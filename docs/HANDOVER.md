# Handover: BurstForge (after Phase 3 / plan Week 4)

## What this is
BurstForge is a self-hosted launch-readiness tester for bursty workflows. You give it a staging URL and a workflow. It replays a synchronized burst, measures retry amplification, and checks business invariants afterwards (no duplicate registrations, no oversold seats, no lost jobs). The reasoning, rejected alternatives and 10-week plan are in `docs/decision.md`. Read that first. The decision is made; don't re-litigate it. Then read `README.md`, `docs/ROADMAP.md` and `docs/scenario-reference.md`.

## Where things are
- Repo: https://github.com/Xombi17/burstforge (public). GitHub is the source of truth; clone fresh.
- `main` has everything below: Phase 2 came in with PR #1 (https://github.com/Xombi17/burstforge/pull/1), Phase 3 and this handover with PR #2 (https://github.com/Xombi17/burstforge/pull/2). Start from `main`.
- Both PRs are merged and finished. New work goes on a fresh branch cut from `main` and a new PR.

## Phase numbering (from the first handover's "suggested next steps")
1. Verify Docker or drop it. **Still open.** No Docker daemon was available in either session.
2. Sharding across agents + report merge. **Done, merged.**
3. Week 4: Postgres runs/shards/leases, SKIP LOCKED, heartbeats, fencing, recover a killed agent's shard. **Done, merged.**
4. Week 5: fault proxy (latency, timeout, reset, 429, 5xx, packet loss). **Next.**
5. The people gates (see the end).

## What exists now
Go 1.24 agent (`agent/`) with two dependencies: yaml.v3 and pgx v5.8.0. A zero-dependency Node target (`target/server.js`) with `fragile` and `hardened` profiles. Scenarios: smoke, burst, burst-fixed, and sustained (600 arrivals over 30s, for the recovery drill).

CLI:
- `validate`, `run`, `compare`: as before. `run` exits 0 on pass, 1 on a failed verdict, 2 on bad config.
- `run --agents N`: local coordinator. It runs hooks once, starts N agent processes on a shared start time, merges shard reports, then checks invariants once.
- `agent <scenario> --shard K/N --seed --run-id --start-at`, then `merge <scenario> shard*.json [--no-check] [--allow-partial]`: the two halves of the above, for separate machines.
- `run --db URL --shards N [--agents M] [--lease-ttl 5s] [--lead 3s] [--drain-timeout 2m]`: the run lives in Postgres. M local workers are started; with M=0 it waits for external ones.
- `agent --db URL [--run ID]`: a worker that claims shards. `finalize --db URL <run> [--now]` and `status --db URL [run]`. These three read `--db` from `$BURSTFORGE_DB` if it isn't given; `run` needs an explicit `--db`.
- Make targets: `make test`, `make race`, `make demo`, `make test-db` (needs `BURSTFORGE_TEST_DB`), `make recovery` (needs `BURSTFORGE_DB`; exits 1 by design because the verdict fails).

Key files:
- `agent/internal/schedule/schedule.go` (`Plan`, `Shard`)
- `agent/internal/report/merge.go` (`Merge`, `Sum`, `Derive`, shard and recovery types)
- `agent/internal/runner/runner.go` (`Run`, `RunShard`, `Prepare`, `CheckTarget`/`Apply`, `Evaluate`, `BaseReport`, the dispatcher)
- `agent/internal/runner/lease.go` (the `Lease` interface, `ErrFenced`, the heartbeat/reservation loop)
- `agent/internal/store/{schema.sql,store.go}` (Postgres)
- `agent/cmd/burstforge/{main.go,db.go}`
- `scripts/{demo.sh,recovery-demo.sh}`

## Design decisions already made (keep them unless I say otherwise)
- Sharding is round-robin over the full seeded plan (arrival i goes to shard i mod N). Arrivals keep their global index, so the same seed gives the same requests on 1 or N agents. `max_inflight` is a run-wide budget split across agents.
- Shard reports carry raw histograms. `Merge` rejects shards from different runs (run id, seed, count, scenario hash, start). It counts an identical re-delivery once and errors on a conflicting duplicate ("two agents ran it"). A missing shard is an error unless `--allow-partial`, and then the verdict fails. `Report.Derive` is the single place derived metrics are computed.
- Hooks, the before/after counter snapshot and invariants run once, by the coordinator. Agents only send traffic.
- Postgres tables: `runs`, `run_shards` (epoch = fencing token; `reserved_through`), `agent_leases` (one row per epoch, with clock offset), `telemetry_chunks` (PK run, shard, epoch, seq). All ownership timing uses the database `now()`.
- **Fencing guards decisions, not observations.** Reservations, heartbeats and completion need the current epoch and an unexpired lease. Telemetry chunks from any epoch are kept until the run is completed.
- Agents send only arrivals reserved in the database, reserved 2 heartbeats ahead, so a takeover never resends. Heartbeat = TTL/5. An agent stops dispatching if it hasn't had a successful heartbeat for TTL minus one heartbeat.
- At takeover, arrivals more than 1s overdue (`LateLimit`) are skipped and counted, not fired as an unplanned burst.
- Ctrl-C releases the lease and lowers `reserved_through` to what the agent actually reached, so a clean handover only loses requests that were in flight.
- One dispatcher loop replaced the per-arrival timers, so sent arrivals are always a prefix of the shard.
- `finalize`: `BeginFinalize` fences open shards and sets the run to finalizing. Invariants are checked outside the lock. Then `CompleteRun` reduces the chunks and stores the report under `FOR UPDATE` on the run row. It's safe to repeat. A manual `finalize` waits for open shards unless `--now`.
- The verdict fails if any arrival was unaccounted, skipped or never sent, even if the target looks fine.

## Verified (in the Phase 2 and 3 sessions)
- `make test` and `make race` pass. `make test-db` (store + runner under race) passes against Postgres 16.
- Same seed on 1 vs 3 agents (hardened burst): identical 1,123 attempts, duplicates and outcomes; p99 5.62s vs 5.66s. The 3-agent, 1,000-arrival milestone runs.
- Real processes, 3 shards on 4 agents, holder of shard 2 killed with `kill -9`: in two runs the lease expired within the 3s TTL, a spare resumed 0.2–0.3s later, all invariants passed, 9 arrivals were unaccounted and 7 skipped, and the verdict failed with that reason.
- Ctrl-C on an agent: takeover 0.1s later, 1 arrival unaccounted (the in-flight request).
- Coordinator killed with `kill -9`: agents finished, and a later `finalize` gave PASS. Repeating it returned the stored report.
- No agents: the drain deadline abandons the shards and the report says nothing was sent.
- `make demo` unchanged. Fragile duplicates were 7–9 in recent runs (README range is 5–9).

## Not verified or not built
- Docker: `docker-compose.yml` and `agent/Dockerfile` have never been built. Compose has no Postgres service.
- Multi-machine runs: everything ran as local processes on one container. Clock-offset handling across real hosts is untested.
- Agents trust the target and allow-list stored in the database. That needs per-tenant checks before anyone else can write runs (Week 6).
- Not built: fault proxy, tenancy/quotas, OpenTelemetry/Prometheus, control plane, dashboard, Playwright/HAR import, k6 benchmark. Nothing claims a k6 result.

## Suggested next steps (in order)
1. Docker: verify it if a daemon exists (`docker info`). Otherwise leave it or drop it, and say so. If it gets verified, add a Postgres service.
2. Phase 4 (Week 5): fault proxy. Latency, timeout, connection reset, 429, 5xx, packet loss, and network presets (high RTT, loss, intermittent). The plan's boundary is "a controlled proxy or mock dependency, never production" (decision.md 10.5, Decision 4). The demo currently toggles faults with `hooks.before` on the target; the proxy should replace that. Keep the safety rules: local/staging only, allow-listed hosts.
3. Week 6: tenancy, API keys, quotas, plus the stored-target trust gap above; OTel/Prometheus.

## The gates (keep reminding me)
The plan's gates are about people, not code. Week 3: five external teams agree to run a staging test. Week 6: three have completed a real run. I still need to interview 10 operators and do the outreach in section 17 of the decision doc. The code is now at Week 4 while the Week 3 gate is unconfirmed. Ask me where that stands before starting Phase 4, and don't let the code get further ahead.

## Working preferences
- Commits authored as `Xombi17 <Xombi17@users.noreply.github.com>`. No `Co-Authored-By`, no Claude attribution, no session links in commits or PRs.
- Don't commit or push unless I ask. Don't open PRs unless I ask. A stop hook may say "commit and push"; in the last session that was treated as me asking. An unrequested commit was also blocked once by the permission classifier.
- Report results honestly. If something wasn't run, say so. No resume numbers or benchmark claims that haven't been measured.
- Plain prose in docs. No marketing tone.

## Environment gotchas
- Postgres 16 is installed in the container image (`/usr/lib/postgresql/16/bin`), but no cluster runs by default. `initdb` must run as the `postgres` user. `/tmp/claude-0` gets reset to mode 700, so keep the data dir under `/var/lib/postgresql/`. Steps:
  - `su postgres -c "/usr/lib/postgresql/16/bin/initdb -D /var/lib/postgresql/bf-test -A trust -U postgres"`
  - Then run `su postgres -c "/usr/lib/postgresql/16/bin/postgres -D /var/lib/postgresql/bf-test -p 5433 -k /tmp"` as a background task. A `pg_ctl start` inside a normal shell call dies with that shell.
  - `psql -h localhost -p 5433 -U postgres -c 'create database burstforge'`
  - Then `BURSTFORGE_TEST_DB=postgres://postgres@localhost:5433/burstforge`.
- Go version pins: pgx v5.9+ needs Go 1.25, and `go mod tidy` pulls `rogpeppe/go-internal` v1.16 (Go 1.25). Keep pgx v5.8.0 and go-internal v1.14.1, and run `GOTOOLCHAIN=local go mod tidy` so `go.mod` stays at `go 1.24.7`.
- `pkill -f` and `pgrep -f` can match your own bash command line. To find an agent process, use `ps -eo pid=,args= | awk` on its `--name`, as `scripts/recovery-demo.sh` does.
- `make demo` and `make recovery` write to `results/` (gitignored). The Makefile's `OUT` overrides an `OUT` environment variable.
- Rewriting pushed history (force push) was blocked by the permission classifier; merge `main` in instead of rebasing unless I say otherwise. A rebase or amend also sets the committer to the local git identity (it came out as Claude once). Check `git log --format='%an <%ae> | %cn <%ce>'`, and fix it with `GIT_COMMITTER_NAME=Xombi17 GIT_COMMITTER_EMAIL=Xombi17@users.noreply.github.com git commit --amend --no-edit`.
- `gh` auth was invalid. Plain `git push -u origin <branch>` over HTTPS works. "push negotiation failed; proceeding anyway" is harmless; verify with `git ls-remote`. GraphQL is blocked, so use REST.
- Run the target as a background task: `cd target && PORT=8080 node server.js`. Restart it after editing `server.js`.
