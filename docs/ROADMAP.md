# Roadmap

Tracks the 10-week plan in [decision.md](decision.md). `[x]` is done and tested, `[ ]` is open.

## Gates (from the plan)

- **Week 3:** at least five external teams agree to run a staging test. If not, keep BurstForge as an open-source CLI and drop the hosted SaaS idea.
- **Week 6:** at least three external teams have completed a real run, someone ran a second test after fixing their system, and reports show invariant or retry findings, not only metrics.

These are about people, not code. The code below is ahead of the user work: Week 6 was built before the Week 3 gate was confirmed, and the hosted-SaaS parts (organizations, quotas) only matter if that gate passes.

## Week 1: target and first numbers

- [x] Fragile registration target (SSO with a concurrency cap, DB pool, seat row lock, job queue, hardened profile)
- [x] CLI sends requests and reports p50/p95/p99 and error rate
- [ ] Interview 10 operators (GDG CRCE, Bit N Build, partner universities, hackathon teams)
- [x] Docker Compose verified end to end on one Linux host: before/after, a worker container killed under Postgres leases, faults through a proxy container (`scripts/docker-demo.sh`), and the control-plane profile. Not tried on Docker Desktop
- [x] Found and fixed: the agent container couldn't write `./results` on Linux, and the run only noticed after sending all its traffic. Runs now check the output directory first

## Week 2: scenarios

- [x] YAML scenario format with validation that reports every problem at once
- [x] Multi-step API workflows with captured variables
- [x] Retry policies: fixed and exponential backoff, full jitter, `Retry-After`
- [ ] Playwright/API import (HAR or recorded flow to scenario)

## Week 3: agent and schedules

- [x] Go agent with deterministic seeded burst, ramp and sustained schedules
- [x] Arrival-skew measurement
- [x] Retry amplification and double-submit modelling
- [x] Business invariants with `eventually` polling
- [x] Deterministic sharding across multiple agents (round-robin over the seeded plan; `run --agents N`, or `agent` + `merge` across machines)
- [x] Merge reports from several agents: shard reports carry histograms; the reducer rejects mismatched runs and conflicting duplicates and flags missing shards
- [x] Milestone check: 3 local agents, 1,000 arrivals, report with per-agent skew and merged latency. Same seed on 1 vs 3 agents gave identical attempt counts (1,123) and outcomes on the hardened burst.

## Week 4: durable run state

- [x] Postgres schema: runs, run_shards, agent_leases, telemetry_chunks (`agent/internal/store/schema.sql`)
- [x] `FOR UPDATE SKIP LOCKED` shard claiming, heartbeats, fencing epochs
- [x] Arrival reservations, so a takeover never resends an arrival; overdue arrivals are skipped and counted
- [x] Idempotent telemetry chunks with sequence numbers (pulled forward from Week 6)
- [x] Kill an agent mid-run and recover its shard (`make recovery`; tested with SIGKILL, Ctrl-C, coordinator SIGKILL, and no agents at all)
- [x] Clock-offset handshake against the database clock
- [x] Agents no longer trust the run row for organization runs: they re-check the organization's quotas, the environment binding, the traffic URL and the target's approval token themselves (Week 6)

## Week 5: fault injection

- [x] Fault proxy: latency, timeout, connection reset, 429, 5xx, per path and rate, before or after the target handles the request (embedded, or `burstforge proxy` for remote agents)
- [x] Network presets (high-rtt, lossy, intermittent, mobile-3g). Loss is approximated at the HTTP level; real packet loss needs tc/netem
- [x] Same seed, same faulted requests, so before/after runs are comparable
- [x] Milestone: `make faults` injects SSO latency and shows retry amplification (2.46 vs 1.00 SSO calls per workflow, 1.23x vs 1.05x)
- [x] Found and fixed: Go's HTTP client silently resent some requests after connection errors, uncounted
- [ ] `finalize` run separately from the coordinator has no fault counts (only the coordinator reads the proxy's stats)

## Week 6: tenancy and telemetry

- [x] Organizations, API keys (hashed, three roles), environments with approval tokens (`docs/tenancy.md`)
- [x] Quotas: arrivals, peak arrival rate and duration per run; concurrent runs and runs per hour; API requests per minute. Refusals name the quota
- [x] Control-plane API in Node + Fastify (pulled forward from Week 7), with `pg` rather than Prisma so the schema keeps one owner (the Go migration)
- [x] `burstforge controller`: claims run requests, binds them to the environment, runs hooks, finalizes; another controller adopts a dead one's runs
- [x] Agents: `--require-org`, `--trust-proxy`; a refused run no longer blocks the runs queued behind it
- [x] Milestone: three organizations run at once with separate targets, quotas and metrics (`make tenants`)
- [x] W3C `traceparent` on every request; OTLP/HTTP span export (checked against Jaeger); Prometheus metrics on agents and the control plane
- [x] Idempotent telemetry chunks with sequence numbers (done in Week 4)
- [x] Found and fixed: hook, invariant and observe paths could point at another host (`@host/...`)
- [x] Found and fixed: every process start re-ran the schema, locking `runs` and deadlocking with live work. The schema now has a version and is applied only when it changes
- [ ] Redis-backed rate limiting for more than one control-plane instance (the limiter is in-process)
- [ ] Keeping tenants away from a hosted installation's internal network (approval checks and hooks reach private addresses, which self-hosting needs)
- [ ] A run with faults depends on its controller staying up (the proxy runs inside it)

## Week 7: dashboard

- [x] Control-plane API (Node + Fastify), done in Week 6
- [ ] Next.js dashboard: launch, watch, read reports

## Week 8: reports

- [x] Before/after comparison (`burstforge compare`)
- [ ] Compare against saved baselines in the dashboard
- [ ] Network profiles in reports

## Week 9: benchmark and chaos

- [ ] Public benchmark against k6 on the same target (goal: under 5% throughput difference)
- [ ] Chaos matrix from section 13.4 of the plan
- [ ] Security review

## Week 10: pilot and launch

- [ ] Three external teams run tests and get reports
- [ ] Hosted dashboard, open-source CLI release
- [ ] Fill in the resume bullets with measured numbers
