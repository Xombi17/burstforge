# BurstForge

Launch-readiness testing for bursty workflows. Point it at a staging URL and one critical workflow (register, check out, upload). It replays a synchronized burst, counts retry amplification, and checks business invariants afterwards: no duplicate registrations, no oversold seats, no lost jobs.

Most load tests answer "how many requests per second can this endpoint take?". The outages that hurt (Codeforces, IWAG, GitHub, Convex, Trigger.dev, UPI) came from a different place: synchronized arrivals, slow auth, database locks, and clients that retry immediately. The HTTP error rate can read 0% while the data is wrong. BurstForge checks the data.

The reasoning behind the project, the candidates that were rejected, and the 10-week plan are in [docs/decision.md](docs/decision.md).

## Try it

Needs Go 1.24+ and Node 20+. Lease-based runs (`--db`) also need PostgreSQL (tested on 16).

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
| Duplicate registrations | 5–12 | 0 |
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

In this mode arrival skew is measured on each agent's own clock, so clock offset between machines isn't measured, and if an agent dies its shard is reported missing, not re-run. The next section fixes both.

### Injecting faults

Add a `faults:` block to a scenario and the workflow traffic goes through a proxy that adds latency, error statuses, connection resets or timeouts, per path and at a given rate (see [docs/scenario-reference.md](docs/scenario-reference.md#faults)). With `phase: after`, the target handles the request and then the response is delayed or dropped. That's the case where a server did the work, the client never heard back, and a retry finds out whether the endpoint is idempotent.

The coordinator starts the proxy itself on 127.0.0.1. Agents on other machines need a standalone one:

```bash
bin/burstforge proxy --upstream http://staging.internal:8080 --listen 0.0.0.0:8081 --allow-host staging.internal
```

and `faults.proxy: http://<proxy-host>:8081` in the scenario. The report lists every rule with how many requests it matched and how many it faulted, and the proxy's request count should equal the run's HTTP attempts.

`make faults` runs the Week 5 drill twice with the same seed: 300 arrivals, 30% of token refreshes answered 2s late after the SSO did the work, and 10% of registration responses lost after the registration went through. One pair of runs here:

| | fragile, immediate retries | hardened, backoff + idempotency key |
|---|---:|---:|
| SSO calls per workflow | 2.46 | 1.00 |
| Retry amplification | 1.23x | 1.05x |
| Users told "already registered" after their own registration succeeded | 31 | 0 |
| Lost responses answered from the idempotency record | 0 | 32 |
| Latency p50 | 247ms | 930ms |

The hardened client is slower at the median because it waits up to 3s for a token instead of giving up at 1.5s. The fragile run also loses confirmation jobs (33–34 here), but its queue does that under load without any injected faults, so don't read that row as caused by the proxy.

Building the proxy turned up a measurement bug that affected every earlier phase. Go's HTTP client silently resends a request after a connection error on a reused connection if it considers the request replayable, which includes a POST with an `Idempotency-Key` header. Those resends bypassed the retry policy and weren't counted. Requests with a body are no longer resent, and any resend of a bodiless request is counted as a `transport_retry` attempt. The earlier demo runs had no connection errors, so their numbers weren't affected.

### Surviving agent failures (Postgres)

With `--db`, the run lives in Postgres and agents take shards under leases:

```bash
bin/burstforge run checkout.yaml --db $BURSTFORGE_DB --shards 3 --agents 4
```

`--agents 4` starts four local workers for three shards, so one is spare. Leave `--agents` out and start workers yourself, on any machine that can reach the database:

```bash
bin/burstforge agent --db $BURSTFORGE_DB            # works on any open run
bin/burstforge status --db $BURSTFORGE_DB [run-id]  # shards, leases, who holds what
bin/burstforge finalize --db $BURSTFORGE_DB <run-id>
```

How it holds together:

- An agent claims a shard with `FOR UPDATE SKIP LOCKED`, which bumps the shard's epoch. Every heartbeat, reservation and completion is conditional on that epoch and on an unexpired lease, using the database clock. A stalled agent that comes back after losing its lease can't reserve or complete anything.
- An agent only sends arrivals it has reserved in the database, a couple of heartbeats ahead of the schedule. Whoever takes the shard over starts after the last reserved arrival, so no arrival is sent twice.
- If an agent stops heartbeating for `--lease-ttl` (default 5s), a spare claims the shard and resumes it. Arrivals that are more than 1s overdue by then are skipped and counted, because sending them all at once would add a burst the plan never had. An agent stopped with Ctrl-C releases its lease and hands back the arrivals it didn't reach, so the takeover is immediate.
- Telemetry goes up as chunks with sequence numbers and is stored at least once and idempotently. Chunks from an agent that lost its lease are still kept until the run is finalized, because they describe traffic that really went out.
- When the agent claims a shard, it measures its offset to the database clock and schedules from the shared start time on that clock. It refuses to claim if the measurement is uncertain by more than 100ms.
- If the coordinator dies, agents finish their shards anyway, and `finalize` closes the run later. It waits for open shards, checks invariants once and stores the report. Running it twice gives the same report.

A run where any arrival didn't go as planned fails its verdict, and the report says how many were unaccounted for (reserved by an agent that died before reporting them), skipped as overdue, or never sent. The target may well be fine; the load just wasn't what the plan said, so rerun before trusting the numbers.

`make recovery` runs the drill: 600 registrations over 30 seconds on three shards and four agents, with the agent holding shard 2 killed by SIGKILL partway through. In two runs here the lease expired within the 3s TTL of the kill, a spare resumed the shard 0.2–0.3s later, and 9 arrivals were unaccounted and 7 skipped. The verdict fails for that reason, so `make recovery` exits 1.

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
agent/        Go: scenario parser, schedule planner, runner, histograms, reports, Postgres store, CLI
target/       Node, zero dependencies: the fragile registration app (fragile and hardened profiles)
scenarios/    smoke, burst and burst-fixed for the demo; faults and faults-fixed for the fault drill;
              sustained for the recovery drill
docs/         decision doc, roadmap, scenario reference
scripts/      demo.sh
```

```bash
make test     # go vet + unit tests
make race     # same, under the race detector
BURSTFORGE_TEST_DB=postgres://... make test-db   # lease and recovery tests against a real Postgres
```

## Status

This is the Week 1–5 slice of the plan: a single agent, several agents coordinated from one command or by hand, or agents working under Postgres leases with takeover of a dead agent's shard, with faults injected by a proxy in front of the target. See [docs/ROADMAP.md](docs/ROADMAP.md) for what's done and what's next. Not built yet: tenancy and quotas, OpenTelemetry, the control plane and dashboard, Playwright import, the k6 benchmark.

The Docker files (`docker-compose.yml`, `agent/Dockerfile`) are written but have not been built or run.
