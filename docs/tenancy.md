# Control plane, organizations and quotas

This is Week 6 of the plan: several teams share one BurstForge installation without being able to see each other's runs, exceed their limits, or point load at a target they don't control.

A single team doesn't need any of this. `burstforge run` and `run --db` work as before and ignore organizations entirely.

## The pieces

```
team ──HTTP──> control plane (Node + Fastify) ──> Postgres <── controller (Go) ──> target (hooks, invariants)
                API keys, quotas, run requests        ^                │
                                                      │                └─ per-run fault proxy
                                                      └── agents (Go, --require-org) ──> target or the proxy
```

- **Control plane** (`control-plane/`): the HTTP API. It authenticates API keys, enforces quotas and rate limits, and stores run *requests*. It never sends load. It validates scenarios by calling the Go binary (`burstforge validate --json --bind URL`), so validation is the same code everywhere, and it applies the schema with `burstforge migrate` on startup.
- **Controller** (`burstforge controller --db URL`): turns requests into runs. It claims a queued request with `SKIP LOCKED` and binds the scenario to the organization's environment. Then it checks quotas and the approval token, runs the before hooks, and stores the run for agents. When the shards are done, it finalizes the run. It holds a lease on each request it drives. If it dies, another controller adopts its open runs once the lease (default 10s) runs out and finalizes them.
- **Agents** (`burstforge agent --db URL --require-org`): claim shards as before. For an organization's run they don't trust the run row: see [Safety](#safety).

The schema is in `agent/internal/store/schema.sql`: `organizations`, `api_keys`, `environments` and `run_requests`, plus `organization_id` and `environment` on `runs`.

## Getting started

```bash
make build
cd control-plane && npm ci && cd ..
export BURSTFORGE_DB=postgres://user@localhost:5432/burstforge

node control-plane/src/server.js &              # API on 127.0.0.1:8090
bin/burstforge controller --db $BURSTFORGE_DB &
bin/burstforge agent --db $BURSTFORGE_DB --require-org --trust-proxy 127.0.0.1 &

node control-plane/src/admin.js create-org acme --name "Acme" --max-concurrent-runs 2
# prints the owner key once: bf_k..._...
```

With Docker: `docker compose --profile platform up -d --scale tenant-agent=3`, then `docker compose exec control-plane node src/admin.js create-org ...`.

`make tenants` runs the whole thing with three organizations at once. See `scripts/tenants-demo.sh`.

## Admin commands

`node control-plane/src/admin.js` talks to the database directly (it needs `BURSTFORGE_DB`):

| command | |
|---|---|
| `create-org <id> --name NAME [quotas]` | creates the organization and prints an owner key |
| `set-quota <id> [quotas]` | changes quotas |
| `create-key <id> --name NAME --role R` | another key |
| `delete-org <id>` | deletes the organization with its keys, environments and runs |
| `list` | organizations and their quotas |

Quota flags: `--max-concurrent-runs`, `--max-runs-per-hour`, `--max-arrivals`, `--max-arrival-rate`, `--max-duration-ms`, `--api-requests-per-min`.

## Quotas

| quota | default | checked by | when exceeded |
|---|---:|---|---|
| `max_arrivals` | 1000 | API, controller, agents | 422, the run is too big |
| `max_arrival_rate` | 500 | API, controller, agents | 422. Arrivals in the busiest one-second window of the seeded plan |
| `max_duration_ms` | 300000 | API, controller, agents | 422. The span of the planned arrivals |
| `max_concurrent_runs` | 1 | API | 429 with `Retry-After`. Queued, starting, running and finalizing runs count |
| `max_runs_per_hour` | 20 | API | 429 with `Retry-After`. Requests in the last hour, not counting rejected ones |
| `api_requests_per_min` | 120 | API | 429 with `Retry-After`. A token bucket per organization |

Zero means none allowed, not unlimited. The API takes a lock on the organization's row while it checks and inserts a request, so parallel requests can't both slip under `max_concurrent_runs` (a test fires five at once with a limit of one). The rate limiter lives in the control-plane process. With more than one control-plane instance, each would enforce its own budget. The plan's Redis-backed limiter isn't built.

Every refusal names the quota, the limit and what was asked for:

```json
{"error":"quota_exceeded","message":"max_concurrent_runs is 1 and the organization is at 1",
 "quota":"max_concurrent_runs","limit":1,"current":1}
```

## Roles

A key has one role: `viewer` (read runs, environments, metrics), `operator` (also request runs and verify environments) or `owner` (also manage keys and environments). Keys look like `bf_k1a2b3c4d5e6_<32 characters>`. Only a SHA-256 of the secret half is stored, and a key is shown once, when it's created.

## API

All `/v1/` routes need `Authorization: Bearer <key>` and only ever see the key's own organization. Another organization's run is a 404, not a 403.

| | role | |
|---|---|---|
| `GET /v1/me` | viewer | organization, role, quotas, current usage |
| `POST /v1/keys` `{name, role}` | owner | new key, shown once |
| `GET /v1/keys`, `DELETE /v1/keys/:id` | owner | list, revoke |
| `POST /v1/environments` `{name, base_url}` | owner | registers a target and returns its approval token |
| `GET /v1/environments` | viewer | |
| `POST /v1/environments/:name/verify` | operator | fetches the approval file; a failure clears verification |
| `POST /v1/runs` `{environment, scenario, shards?, seed?}` | operator | 202 with the request id; `scenario` is the YAML as a string |
| `GET /v1/runs`, `GET /v1/runs/:id` | viewer | state (`queued`, `starting`, `running`, `finalizing`, `completed`, `rejected`), the reason if rejected, and the report when completed |
| `GET /v1/metrics` | viewer | Prometheus text, this organization's series only |
| `GET /metrics` | metrics token | every organization; disabled unless `BURSTFORGE_METRICS_TOKEN` is set |
| `GET /healthz` | none | |

```bash
curl -H "Authorization: Bearer $KEY" -H 'Content-Type: application/json' \
  -d '{"name":"staging","base_url":"https://staging.acme.dev"}' localhost:8090/v1/environments
# serve the returned approval_token on a line of its own at
# https://staging.acme.dev/.well-known/burstforge-approval.txt, then:
curl -X POST -H "Authorization: Bearer $KEY" localhost:8090/v1/environments/staging/verify

jq -Rs '{environment: "staging", scenario: ., shards: 2}' checkout.yaml |
  curl -H "Authorization: Bearer $KEY" -H 'Content-Type: application/json' -d @- localhost:8090/v1/runs
```

## Safety

The decision doc's rule for tenants is quotas, approval tokens and no production targets. Here's how a run is held to it.

- **Approval token.** An environment can only be used once its target serves the organization's token at `/.well-known/burstforge-approval.txt`. The control plane checks this on `verify`, the controller checks it again before it starts the run, and every agent checks it again before it sends a shard. Redirects aren't followed. A target can approve several organizations, one token per line. Removing the token stops new shards.
- **Bound scenarios.** Whatever `target` a submitted scenario names is replaced with the environment's URL, and the environment's host becomes the only allowed host. Hook, invariant and observe paths must start with `/`, so they can't reach another host. A submitted scenario can't name its own fault proxy; the controller runs one per run.
- **Agents don't trust the run row.** For an organization's run, an agent re-reads the organization's quotas and environment and checks that the stored scenario still targets exactly that environment. It also checks the traffic URL (a fault proxy) is on a host it was told to trust (`--trust-proxy`), and fetches the approval token itself. If anything fails, it releases the shard, logs why, and skips that run from then on. Other runs behind it in the queue aren't held up. `--require-org` makes an agent refuse runs that belong to no organization, for a pool that only serves tenants.
- **No production.** `target.environment` must still be `local` or `staging`.

What this doesn't cover:
- Anyone with write access to the database can create organizations and approve themselves for a target. They still can't make an agent load a host that doesn't serve their token. The database is trusted infrastructure, like the agents.
- The approval check and the controller's hooks make requests to tenant-supplied URLs, including private addresses, which a self-hosted installation needs. A hosted service would also have to keep tenants away from its own internal network. That isn't built.
- A run with faults sends its traffic through a proxy inside the controller. If that controller dies mid-run, the run's requests fail until it ends. Another controller adopts and finalizes the run, but the fault counts are lost: the adopting controller logs it, and the report has no fault section.

## Metrics

The control plane's series all carry an `org` label:

| metric | type | |
|---|---|---|
| `burstforge_api_requests_total{org,route,code}` | counter | authenticated API requests |
| `burstforge_api_rate_limited_total{org}` | counter | |
| `burstforge_run_requests_total{org,result}` | counter | `queued`, `quota`, `invalid`, `bad_environment` |
| `burstforge_quota_refusals_total{org,quota}` | counter | |
| `burstforge_runs{org,state}` | gauge | requests by current state |
| `burstforge_runs_finished_total{org,verdict}` | counter | from stored reports |
| `burstforge_workflows_total{org,result}`, `burstforge_http_attempts_total{org}` | counter | from stored reports |

The API counters live in the process and restart from zero with it. The run series are recomputed from the database on each scrape.

Agents started with `--metrics-addr` serve live series per run, labelled `run`, `org` and `shard`:
- `burstforge_agent_attempts_total{...,step,kind,class}`
- `burstforge_agent_attempt_duration_seconds{...,step}` (histogram)
- `burstforge_agent_workflows_total{...,result}`
- `burstforge_agent_workflow_duration_seconds` (histogram)
- `burstforge_agent_spans_total{result}`

A run's series disappear 15 minutes after its last event.

## Tracing

Every attempt carries `traceparent: 00-<trace>-<span>-01`, unless the scenario sets one itself. The trace ID is the first 16 bytes of `sha256("burstforge/<run id>/<arrival>")`, with the arrival 0-based, so it can be computed from a logical id. With `OTEL_EXPORTER_OTLP_ENDPOINT` (or `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`) set, agents export a `workflow` span per arrival and a client span per attempt over OTLP/HTTP with JSON encoding. `OTEL_EXPORTER_OTLP_HEADERS` and `OTEL_SERVICE_NAME` are honoured. gRPC isn't supported. Spans go through a bounded buffer: when it is full or the collector fails, spans are dropped and counted, never waited for. The agent logs the totals, and `--metrics-addr` exposes them.

Checked against Jaeger 1.62: a 300-arrival faults run exported 1,039 spans (300 workflows, 739 attempts) with none dropped. A retried workflow showed up as one trace: the first `refresh_token` attempt timed out at 1.5s under the injected SSO latency, the retry succeeded in 40ms, and registration returned 201.
