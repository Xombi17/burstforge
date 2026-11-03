# BurstForge

Launch-readiness testing for bursty workflows. Point it at a staging URL and one critical workflow (register, check out, upload). It replays a synchronized burst, counts retry amplification, and checks business invariants afterwards: no duplicate registrations, no oversold seats, no lost jobs.

Most load tests answer "how many requests per second can this endpoint take?". The outages that hurt (Codeforces, IWAG, GitHub, Convex, Trigger.dev, UPI) came from a different place: synchronized arrivals, slow auth, database locks, and clients that retry immediately. The HTTP error rate can read 0% while the data is wrong. BurstForge checks the data.

The reasoning behind the project, the candidates that were rejected, and the 10-week plan are in [docs/decision.md](docs/decision.md).

## Try it

Needs Go 1.24+ and Node 20+. No other dependencies.

```bash
make build
make demo
```

`make demo` starts a deliberately fragile event-registration app, then:

1. runs 10 users one at a time (passes, everything looks healthy),
2. fires 500 users inside one second at 300 seats, with 1.5 s SSO latency and a client that retries immediately,
3. reruns the same burst against the hardened profile with polite retries,
4. prints a before/after comparison.

Typical numbers from the fragile run (they move a little between runs because the races are timing-dependent):

| | fragile | hardened |
|---|---:|---:|
| HTTP error rate | 0% | 0% |
| Duplicate registrations | 5–9 | 0 |
| Oversold seats | 0–3 | 0 |
| Confirmation jobs lost | ~50 | 0 |
| Latency p99 | ~9–10 s | ~5.5 s |
| Retry amplification | ~1.47x | ~1.13x |
| SSO calls per workflow | ~3.0–3.3 | 1.0 |

The first row is the point. An endpoint-only test calls the fragile run healthy.

The hardened run's 1.13x is the floor for this scenario, not waste: 25% of users double-tap the final step, and those extra requests count. Its retry count is zero.

## Using it on your own system

Write a scenario (see [docs/scenario-reference.md](docs/scenario-reference.md)):

```yaml
name: checkout-burst
target:
  base_url: https://staging.example.com
  environment: staging
  allow_hosts: [staging.example.com]
workflow:
  steps:
    - name: login
      method: POST
      path: /login
      capture: { token: $.token }
    - name: pay
      method: POST
      path: /orders
      headers: { Authorization: "Bearer {{token}}", Idempotency-Key: "{{logical_id}}" }
      retry: { max_attempts: 3, backoff: exponential, base_delay: 300ms, jitter: full }
traffic: { profile: burst, arrivals: 500, window: 1s, double_submit: 0.1 }
invariants:
  - { name: no duplicate orders, path: /internal/invariants, json: $.duplicate_orders, op: eq, value: 0 }
thresholds: { p99_ms: 4000, retry_amplification: 1.5 }
```

```bash
bin/burstforge validate checkout.yaml
bin/burstforge run checkout.yaml
bin/burstforge compare results/<before>/report.json results/<after>/report.json
```

`run` exits 0 on pass, 1 on a failed verdict and 2 on bad config, so it works as a CI gate. Each run writes `report.json` and a shareable `report.md`.

### Several agents

One agent process tops out somewhere (CPU, sockets, one network path). To split a run:

```bash
bin/burstforge run checkout.yaml --agents 3
```

This runs the `before` hooks once, starts three agent processes that all schedule from one shared start time, merges their shard reports, and then checks invariants once. The report gets an agents table with per-shard arrivals, attempts and arrival skew.

Arrivals are dealt round-robin from the full plan (arrival `i` goes to shard `i mod N`), so each agent fires the same burst shape at a third of the density and keeps the global arrival number. With the same `--seed`, a 3-agent run sends the same logical requests, retries and double submits as a 1-agent run. `max_inflight` is a budget for the whole run and is split between agents.

For agents on separate machines, run the two halves yourself. Every agent needs the same seed, run ID, start time and scenario flags:

```bash
bin/burstforge agent checkout.yaml --shard 1/3 --seed 42 --run-id launch-1 --start-at 2026-11-10T10:00:00Z
bin/burstforge merge checkout.yaml results/launch-1/shard-*.json
```

`agent` only sends traffic. It runs no hooks and checks no invariants, because those act on the whole target. `merge` refuses shards from different runs or scenarios, counts a shard delivered twice once, and errors if the same shard comes back with different results (two agents ran it). A missing shard is an error. With `--allow-partial` it merges anyway and fails the verdict. Run `merge` right after the agents finish, because it checks invariants against the target as it is now (`--no-check` skips that and marks them unchecked). It has no "before" snapshot, so it doesn't diff target counters. `run --agents` does.

Limits for now: arrival skew is measured on each agent's own clock, so clock offset between machines isn't measured yet (that needs the Week 4 clock-offset handshake). There are no leases either. If an agent dies, its shard is reported missing, not re-run.

Business invariants need a read-only endpoint on your staging app that exposes the counts you care about (see `GET /_bf/invariants` in [target/server.js](target/server.js) for a 20-line example). Without one, BurstForge still reports latency, failures and amplification.

## What it measures

- **Retry amplification**: HTTP attempts per intended request. Retries and double submits count.
- **Target counters**: any numeric counters you expose, diffed across the run and divided by workflows. "One request became 3 SSO calls and 4.5 queries" comes from here.
- **Arrival skew**: planned versus actual start time of each arrival. If the agent can't keep up, this shows it instead of quietly claiming a synchronized burst.
- **Latency**: mergeable log-bucket histograms (about 2% error), per step and per workflow.
- **Invariants**: polled with an optional `eventually` window, so "every queued job completes" can wait for a queue to drain.
- **Timeline**: attempts, retries and failures per second.

## Safety

BurstForge sends real load. It refuses to run unless:

- `target.environment` is `local` or `staging` (there is no production mode),
- the host is loopback, a private address, or listed in `target.allow_hosts`,
- `traffic.arrivals` is under `--max-arrivals` (default 10,000).

Unknown YAML fields are rejected, so a typo can't silently disable a limit.

## Layout

```
agent/        Go: scenario parser, schedule planner, runner, histograms, reports, CLI
target/       Node, zero dependencies: the fragile registration app (fragile and hardened profiles)
scenarios/    smoke, burst and burst-fixed scenarios used by the demo
docs/         decision doc, roadmap, scenario reference
scripts/      demo.sh
```

```bash
make test     # go vet + unit tests
make race     # same, under the race detector
```

## Status

This is the Week 1–3 slice of the plan: a single agent, or several agents coordinated from one command or by hand. See [docs/ROADMAP.md](docs/ROADMAP.md) for what's done and what's next. Not built yet: Postgres-backed leases and shard recovery, the fault-injection proxy, the control plane and dashboard, Playwright import, the k6 benchmark.

The Docker files (`docker-compose.yml`, `agent/Dockerfile`) are written but have not been built or run.
