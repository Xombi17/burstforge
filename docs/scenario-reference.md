# Scenario reference

A scenario is one YAML file. Unknown fields are errors. Durations use Go syntax (`500ms`, `3s`, `2m`).

## Top level

| Field | Required | Notes |
|---|---|---|
| `name` | no | Defaults to the file name. |
| `description` | no | |
| `target` | yes | Where to send traffic. |
| `hooks` | no | Control calls before and after the run. |
| `workflow` | yes | The steps one user performs. |
| `traffic` | yes | How users arrive. |
| `observe` | no | Target counters to diff across the run. |
| `invariants` | no | Business checks after the run. |
| `thresholds` | no | Pass/fail limits on measured values. |

## target

| Field | Notes |
|---|---|
| `base_url` | `http` or `https`. |
| `environment` | `local` or `staging`. Anything else is rejected. |
| `allow_hosts` | Hosts allowed besides loopback and private addresses. |

## hooks

`before` and `after` are lists of `{ method, path, headers, body }`. Method defaults to `POST`. A non-2xx response in `before` aborts the run. Use them to reset state or switch the target into a fault mode.

## workflow.steps[]

| Field | Default | Notes |
|---|---|---|
| `name` | required | Unique. Used in the report. |
| `method` | `GET` | |
| `path` | required | Starts with `/`. Supports `{{vars}}`. |
| `headers`, `body` | | Strings anywhere inside support `{{vars}}`. Bodies are sent as JSON. |
| `timeout` | `10s` | Per attempt. |
| `ok_status` | any 2xx | List of status codes that count as success, for example `[201, 409]` when "sold out" is a valid answer. |
| `capture` | | `{ var: "$.json.path" }` from a successful response. Later steps can use `{{var}}`. |
| `retry` | one attempt | See below. |

Built-in variables: `{{n}}` (arrival index), `{{logical_id}}` (stable per user, good for idempotency keys), `{{run_id}}`.

### retry

| Field | Default | Notes |
|---|---|---|
| `max_attempts` | 1 | 1 to 10, including the first. |
| `on` | `[timeout, conn, 429, 5xx]` | Which failures are retried. Other 4xx are never retried. |
| `backoff` | `none` | `none`, `fixed`, `exponential`. |
| `base_delay`, `max_delay` | | Required for `fixed` and `exponential`. |
| `jitter` | `none` | `full` picks a random delay between 0 and the computed one. |
| `respect_retry_after` | false | Waits at least as long as the server asks. |

## traffic

| Field | Notes |
|---|---|
| `profile` | `burst`: arrivals land at random inside `window`. `ramp`: rate rises linearly over `duration`. `sustained`: even spacing over `duration`. |
| `arrivals` | Number of users. |
| `window` / `duration` | See profile. |
| `start_delay` | Default `1s`. Gives the agent time to arm its timers. `run --agents` waits at least 1s so the agent processes can start. `agent` ignores it and uses `--start-at`. |
| `double_submit` | Fraction (0 to 1) of users whose final step is sent twice. |
| `double_submit_delay` | Default `100ms`. |
| `max_inflight` | Default 5000. If reached, arrivals wait, and the wait shows up as arrival skew. With several agents this is the total, split evenly between them. |

Schedules depend only on the profile and `--seed`, so a run can be replayed.

## observe

```yaml
observe:
  url: /_bf/stats
  counters: [sso_calls, db_queries]
```

The endpoint is read before and after. Each counter's delta and delta per workflow go in the report.

## invariants[]

```yaml
- name: no duplicate registrations
  path: /_bf/invariants
  json: $.duplicate_registrations
  op: eq          # eq lt lte gt gte
  value: 0
  eventually: 20s # optional: poll until true, up to this long
```

## thresholds

Each is an upper limit. Omit the ones you don't care about.

| Field | Measured value |
|---|---|
| `p99_ms` | p99 end-to-end latency of successful workflows. |
| `error_rate` | Failed workflows divided by started workflows. |
| `retry_amplification` | HTTP attempts divided by intended requests. |
| `arrival_skew_p99_ms` | p99 gap between planned and actual start. |
