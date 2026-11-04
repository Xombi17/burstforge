# Handover: BurstForge (after plan Week 6)

## What this is
BurstForge is a self-hosted launch-readiness tester for bursty workflows. You give it a staging URL and a workflow. It replays a synchronized burst, measures retry amplification, and checks business invariants afterwards (no duplicate registrations, no oversold seats, no lost jobs).

The reasoning, rejected alternatives and 10-week plan are in `docs/decision.md`. Read that first. The decision is made; don't re-litigate it. Then read `README.md`, `docs/ROADMAP.md`, `docs/scenario-reference.md` and `docs/tenancy.md`.

## Where things are
- Repo: https://github.com/Xombi17/burstforge (public). GitHub is the source of truth; clone fresh.
- `main` has Phases 2 and 3: PR #1 (https://github.com/Xombi17/burstforge/pull/1) and PR #2 (https://github.com/Xombi17/burstforge/pull/2), plus the handover fix from PR #3 (https://github.com/Xombi17/burstforge/pull/3).
- Phase 4 (fault proxy), the Docker verification and Week 6 are on branch `claude/sharp-pascal-19gr29`. No PR has been opened for them yet. Check `git log origin/main` before assuming any of it is in `main`.

## Phase numbering (from the first handover)
1. Verify Docker or drop it. **Done**, verified on one Linux host (see Verified).
2. Sharding across agents and report merge. **Done, merged.**
3. Week 4: Postgres leases, SKIP LOCKED, heartbeats, fencing, recovery. **Done, merged.**
4. Week 5: fault proxy. **Done**, on the branch.
5. The people gates. **Not done.** See the end.
6. Week 6: organizations, API keys, quotas, rate limits, OpenTelemetry. **Done**, on the branch.

## What exists now
- **Go agent** (`agent/`), Go 1.24, two dependencies (yaml.v3 and pgx v5.8.0).
- **Node control plane** (`control-plane/`), Node 22, Fastify 5.12.8 and pg 8.23.1. It uses `pg` rather than Prisma so the schema keeps one owner, the Go migration; say if you want Prisma.
- **Zero-dependency Node target** (`target/server.js`), with a fragile and a hardened profile. It serves approval tokens from `APPROVAL_TOKENS`.
- **Scenarios:** smoke, burst, burst-fixed, sustained, faults, faults-fixed.

CLI (`bin/burstforge`):
- `validate [--json] [--bind URL] <file | ->`, `run`, `compare`, `merge`, `agent` (manual shards), `proxy`: as before. `run` exits 0 on pass, 1 on a failed verdict, 2 on bad config. `run` and manual `agent` now check `--out` is writable before sending anything.
- `--allow-host` takes a comma-separated list. `--fault-proxy URL` points a run at a running `burstforge proxy`.
- `run --db URL --shards N [--agents M]`, `agent --db URL`, `finalize`, `status`: Postgres-backed runs, as before.
- New on `agent --db`:
  - `--require-org`: only take organization runs.
  - `--trust-proxy HOSTS`: controller hosts whose fault proxies may carry traffic.
  - `--metrics-addr ADDR`: Prometheus.
- New commands:
  - `controller --db URL [--proxy-listen ADDR --proxy-advertise HOST]`: turns control-plane run requests into runs.
  - `migrate --db URL`.
- OTLP span export on every agent when `OTEL_EXPORTER_OTLP_ENDPOINT` is set. `--agents N` children inherit the variable.

Control plane: `node control-plane/src/server.js` (needs `BURSTFORGE_DB`) and `node control-plane/src/admin.js` (create-org, set-quota, create-key, delete-org, list). The API is in `docs/tenancy.md`.

Make targets:
- `test`, `race`, `test-db` (now also runs the control-plane tests)
- `demo`, `faults`, `recovery`
- `tenants`: the Week 6 milestone, needs `BURSTFORGE_DB`
- `docker-demo`

Compose profiles:
- default: the target
- `tools`: the agent
- `db`: postgres and workers
- `faults`: the proxy
- `platform`: postgres, control plane, controller and tenant agents

Key new files:
- `agent/internal/tenancy/tenancy.go`: limits, Bind/CheckBound, approval check
- `agent/internal/store/tenancy.go`: Tenant, CheckTenantRun, the request lease functions
- `agent/cmd/burstforge/controller.go`
- `agent/internal/runner/observe.go`: trace IDs, the Observer interface
- `agent/internal/telemetry/{prom.go,otlp.go}`
- `control-plane/src/{app.js,keys.js,ratelimit.js,metrics.js,validate.js,server.js,admin.js}`, `control-plane/test/api.test.js`
- `scripts/{tenants-demo.sh,docker-demo.sh}`

## Design decisions already made (keep them unless I say otherwise)
Earlier ones are unchanged: round-robin sharding over the seeded plan, shard reports with raw histograms, hooks and invariants run once, epochs as fencing tokens, reservations so takeover never resends, "fencing guards decisions, not observations", the overdue skip, the finalize sequence, faults only on workflow traffic, and `GetBody = nil` against hidden transport resends. The previous handover in git history has the details.

New this session:
- **Tenancy.** The control plane stores run *requests*; a Go controller turns them into runs. Scenario validation for the API is `burstforge validate --json --bind`, so the rules are the same code everywhere. The schema stays in `agent/internal/store/schema.sql`.
- **Approval tokens.** A target approves an organization by serving its token at `/.well-known/burstforge-approval.txt`. It is checked at verify time, by the controller, and by every agent on every claim. A failed verify clears verification.
- **Bind.** A submitted scenario's target is replaced by the environment's URL, and the environment host becomes the only allowed host. A submitted scenario can't name a fault proxy; the controller runs one per run on `--proxy-listen` and advertises it as `--proxy-advertise`.
- **Agents don't trust the run row for org runs.** They re-read quotas and the environment and check the binding, the traffic URL (`--trust-proxy`) and the live token. A refused run is skipped by that agent from then on: `ClaimExcept` keeps it from blocking the queue.
- **Quotas.** Per-run size limits (arrivals, peak arrivals per second, planned span) are a 422. Concurrent runs, runs per hour and the API rate are a 429 with `Retry-After`. The API locks the organization row while it checks and inserts. Zero means none allowed. The rate limiter is in-process.
- **Controller leases.** A request is held with `controller` and `lease_until`, renewed every TTL/3. A controller that loses the lease stops without finalizing. `AdoptRuns` lets another controller finalize the open runs of a dead one.
- **Schema versioning.** `burstforge_schema` holds a hash of schema.sql, and the schema is applied only when it changes. Before this, every process start ran `ALTER TABLE ... IF NOT EXISTS`, which locks the table, and that deadlocked with the controller in compose.
- **Tracing.** Every attempt sends `traceparent`. The trace ID is `sha256("burstforge/<run>/<arrival>")[:16]`, so it can be computed from a logical id. The attempt span ID is the one in the header. OTLP/HTTP with JSON encoding, hand-written instead of the OTel SDK, to keep the dependencies at two. Export is bounded and drops instead of blocking.
- **Paths.** Hook, invariant and observe paths must start with `/`, like step paths. `base_url` may not contain credentials, a query or a fragment.

## Verified (this session)
- **Docker 29 / Compose 5.6, one Linux host.** `dockerd` had to be started by hand in this container; see the gotchas.
  - The documented compose commands work with `-u "$(id -u):$(id -g)"`. Without it the run now fails before sending anything (it used to fail after).
  - `scripts/docker-demo.sh` ran end to end: before/after with compare (fragile FAIL with 8 duplicates, hardened PASS), and a worker container `docker kill`ed under leases.
    - With a spare worker, the takeover came when the lease expired: 14 unaccounted, 14 skipped.
    - With no spare, the takeover came 17s later and 126 arrivals were skipped.
  - Faults through the proxy container: proxy requests equal attempts, and the injected counts match the local runs for seed 21.
  - The `platform` profile: one organization ran the faults scenario through the controller's per-run proxy. Proxy requests 739, equal to attempts. Injected 119 and 26, the same as the local runs.
- **`make tenants`.** Three organizations started runs within 0.5s of each other and ran at the same time:
  - acme: burst-fixed, PASS.
  - globex: faults on the fragile target, FAIL on lost jobs as intended.
  - initech: sustained, PASS.
  - Refusals: acme's second run got a 429 on `max_concurrent_runs`. globex's 500-arrival burst got a 422 on `max_arrivals`. globex couldn't verify acme's target. globex reading acme's run got a 404.
  - Each organization's `/v1/metrics` had only its own series.
- **Controller killed with `kill -9`** during a 30s run: a second controller adopted the run after the lease ran out and finalized it. PASS, 600/600, nothing lost (no faults in that run).
- **Tracing.** Jaeger 1.62 received 1,039 spans for a 300-arrival faults run (300 workflows, 739 attempts) with 0 dropped. A retried workflow showed as one trace with the timed-out attempt, the retry and the registration.
- **Prometheus.** `agent --db --metrics-addr` served live per-run series during a run.
- **Tests.** `make test`, `make race` and `make test-db` all pass.
  - New store tests: the agent catches a retargeted row, a lowered quota, an untrusted proxy, a removed token and an unverified environment. Request leases, reclaim, adopt and reject are covered.
  - 8 control-plane tests, including five parallel requests against a concurrency limit of one: exactly one is accepted.
  - Telemetry tests: traceparent-to-span linkage, never blocking, pruning.

## Not verified or not built
- **Docker Desktop on Mac or Windows** has not been tried.
- **The repo's Dockerfiles.** They were built through a variant that only adds this sandbox's TLS-interception CA in the build stage (see gotchas). The repo's Dockerfiles themselves weren't built unmodified here.
- **Multi-machine.** Still one host. Containers had separate network namespaces but shared a kernel clock.
- **Hosted-service network isolation.** Approval checks and hooks can reach private addresses; self-hosting needs that.
- **Rate limiting** is in one process, with no Redis.
- **Fault runs depend on the controller staying up**, because the proxy runs inside it.
- **Tracing** was tried against Jaeger only. There's no gRPC OTLP.
- **Not built:** the dashboard (Week 7), Playwright/HAR import, the k6 benchmark. Nothing claims a k6 result.

## Suggested next steps
1. Open a PR for the branch, Phase 4 plus Docker plus Week 6, if you want it in `main`.
2. Week 7 is the dashboard (Next.js) on top of the control-plane API. The plan says cut the dashboard before cutting recovery, faults or invariants.
3. Week 8 items: network profiles in reports, and comparison against saved baselines.

## The gates (keep reminding me)
The plan's gates are about people, not code.
- **Week 3:** five external teams agree to run a staging test.
- **Week 6:** three teams have completed a real run, someone has run a second test after fixing their system, and the reports showed invariant or retry findings.
- **Still to do:** I need to interview 10 operators and do the outreach in section 17.

The code is now at Week 6 with neither gate confirmed. The organizations, quotas and control plane only pay off if the hosted idea survives the Week 3 gate. If it doesn't, the plan says keep the open-source CLI and stop adding product surface. Ask me where outreach stands before starting Week 7.

## Working preferences
- Commits authored as `Xombi17 <Xombi17@users.noreply.github.com>`. No `Co-Authored-By`, no Claude attribution, no session links in commits or PRs.
- Don't commit or push unless I ask. Don't open PRs unless I ask. A stop hook may say "commit and push"; that has been treated as me asking.
- Report results honestly. If something wasn't run, say so. No resume numbers or benchmark claims that haven't been measured.
- Plain prose in docs. No marketing tone.

## Environment gotchas
- **Docker.** `dockerd` is installed but not running. Start it as a background task: `dockerd > <scratch>/dockerd.log 2>&1`. Then:
  - Docker Hub returns 429. Pull from `mirror.gcr.io/library/<image>` and `docker tag` it to the plain name: node:22-alpine, golang:1.24-alpine, alpine:3.20, postgres:16-alpine. Jaeger is at `mirror.gcr.io/jaegertracing/all-in-one:1.62.0`.
  - Containers' HTTPS is intercepted, so `go mod download` and `npm ci` fail TLS inside a build. Build from a copy of the Dockerfile that adds `COPY --from=ccrca ca-bundle.crt /etc/ssl/certs/ca-certificates.crt` (Go stage) or `NODE_EXTRA_CA_CERTS` (Node stage), with `docker buildx build --load --build-context ccrca=/root/.ccr ...`. Don't commit that.
  - Compose reuses existing images. Rebuild after code changes, including `docker compose build target`.
- **Postgres** 16 is in the image at `/usr/lib/postgresql/16/bin`, with no cluster running.
  - The data dir `/var/lib/postgresql/bf-test` survived between sessions; if it's missing, `su postgres -c "initdb -D ... -A trust -U postgres"`.
  - Remove a stale `postmaster.pid`, then run `su postgres -c "/usr/lib/postgresql/16/bin/postgres -D /var/lib/postgresql/bf-test -p 5433 -k /tmp"` as a background task.
  - Databases `burstforge` (tests) and `bf_tenants` (the demo) exist. `BURSTFORGE_TEST_DB=postgres://postgres@localhost:5433/burstforge`.
- **Go pins.** Keep pgx v5.8.0 and go-internal v1.14.1. Run `GOTOOLCHAIN=local go mod tidy` so `go.mod` stays at `go 1.24.7`.
- **Process matching.** `pkill -f` and `pgrep -f` can match your own shell. Use `ps -eo pid=,args= | awk ...`.
- **Force pushes** were blocked by the permission classifier; merge `main` in instead of rebasing. Check the committer with `git log --format='%an <%ae> | %cn <%ce>'`.
- **Pushing.** `gh` auth was invalid. A plain `git push -u origin <branch>` works ("push negotiation failed; proceeding anyway" is harmless). GraphQL is blocked; use REST.
