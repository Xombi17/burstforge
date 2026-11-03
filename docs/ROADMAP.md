# Roadmap

Tracks the 10-week plan in [decision.md](decision.md). `[x]` is done and tested, `[ ]` is open.

## Gates (from the plan)

- **Week 3:** at least five external teams agree to run a staging test. If not, keep BurstForge as an open-source CLI and drop the hosted SaaS idea.
- **Week 6:** at least three external teams have completed a real run, someone ran a second test after fixing their system, and reports show invariant or retry findings, not only metrics.

These are about people, not code. The code below is ahead of the user work.

## Week 1: target and first numbers

- [x] Fragile registration target (SSO with a concurrency cap, DB pool, seat row lock, job queue, hardened profile)
- [x] CLI sends requests and reports p50/p95/p99 and error rate
- [ ] Interview 10 operators (GDG CRCE, Bit N Build, partner universities, hackathon teams)
- [ ] Docker Compose verified end to end (files exist, not yet built)

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

- [ ] Postgres schema: runs, run_shards, agent_leases
- [ ] `FOR UPDATE SKIP LOCKED` shard claiming, heartbeats, fencing epochs
- [ ] Kill an agent mid-run and recover its shard
- [ ] Clock-offset handshake

## Week 5: fault injection

- [ ] Fault proxy: latency, timeout, connection reset, 429, 5xx, packet loss
- [ ] Network presets (high RTT, loss, intermittent)
- [ ] Until then, `hooks.before` can toggle faults on a target that exposes a control endpoint, which is what the demo does

## Week 6: tenancy and telemetry

- [ ] Organizations, API keys, quotas
- [ ] OpenTelemetry export, Prometheus metrics
- [ ] Idempotent telemetry chunks with sequence numbers

## Week 7: dashboard

- [ ] Control-plane API (Node + Fastify)
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
