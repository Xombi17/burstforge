-- BurstForge durable run state. Applied on connect; every statement is
-- idempotent. Times that decide ownership (lease expiry, run start) come
-- from the database clock, never from an agent's clock.

CREATE TABLE IF NOT EXISTS runs (
    id              text PRIMARY KEY,
    scenario_name   text NOT NULL,
    scenario        jsonb NOT NULL,          -- after defaults and CLI overrides
    scenario_hash   text NOT NULL,
    seed            bigint NOT NULL,
    shard_count     int NOT NULL CHECK (shard_count > 0),
    start_at        timestamptz NOT NULL,    -- shared epoch, database clock
    lease_ttl_ms    int NOT NULL CHECK (lease_ttl_ms > 0),
    status          text NOT NULL DEFAULT 'running'
                    CHECK (status IN ('running', 'finalizing', 'completed')),
    before_counters jsonb,                   -- observe snapshot taken before traffic
    report          jsonb,                   -- final report, set when completed
    created_at      timestamptz NOT NULL DEFAULT now(),
    finished_at     timestamptz
);

-- One row per shard. epoch is the fencing token: every claim and every
-- abandonment bumps it, and every write an agent makes is conditional on it.
-- Added in Week 5: where agents send workflow traffic (a fault proxy), if
-- not the scenario's base_url.
ALTER TABLE runs ADD COLUMN IF NOT EXISTS traffic_url text;

CREATE TABLE IF NOT EXISTS run_shards (
    run_id           text NOT NULL REFERENCES runs (id) ON DELETE CASCADE,
    shard_no         int NOT NULL CHECK (shard_no > 0),
    arrivals         int NOT NULL CHECK (arrivals > 0),
    state            text NOT NULL DEFAULT 'pending'
                     CHECK (state IN ('pending', 'claimed', 'completed', 'abandoned')),
    epoch            bigint NOT NULL DEFAULT 0,
    -- Arrivals [0, reserved_through) of this shard may have been sent. An
    -- agent sends only reserved arrivals, and a new lease holder resumes
    -- here, so no arrival is sent twice.
    reserved_through int NOT NULL DEFAULT 0,
    PRIMARY KEY (run_id, shard_no),
    CHECK (reserved_through BETWEEN 0 AND arrivals)
);

CREATE INDEX IF NOT EXISTS run_shards_open
    ON run_shards (run_id, shard_no) WHERE state IN ('pending', 'claimed');

-- One row per lease, so the history of who held a shard survives.
CREATE TABLE IF NOT EXISTS agent_leases (
    run_id               text NOT NULL,
    shard_no             int NOT NULL,
    epoch                bigint NOT NULL,
    agent_id             text NOT NULL,
    resumed_from         int NOT NULL,       -- reserved_through when claimed
    acquired_at          timestamptz NOT NULL DEFAULT now(),
    renewed_at           timestamptz NOT NULL DEFAULT now(),
    expires_at           timestamptz NOT NULL,
    ended_at             timestamptz,
    outcome              text CHECK (outcome IN ('completed', 'released', 'expired', 'fenced')),
    clock_offset_ms      double precision,   -- database clock minus agent clock
    clock_uncertainty_ms double precision,   -- half the round trip of the best sample
    PRIMARY KEY (run_id, shard_no, epoch),
    FOREIGN KEY (run_id, shard_no) REFERENCES run_shards ON DELETE CASCADE
);

-- At-least-once telemetry: agents retry a chunk with the same seq until it
-- is stored, and duplicates are ignored. Chunks from an old epoch are kept,
-- because they describe traffic that really went out.
CREATE TABLE IF NOT EXISTS telemetry_chunks (
    run_id     text NOT NULL,
    shard_no   int NOT NULL,
    epoch      bigint NOT NULL,
    seq        int NOT NULL,
    agent_id   text NOT NULL,
    data       jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, shard_no, epoch, seq),
    FOREIGN KEY (run_id, shard_no, epoch) REFERENCES agent_leases ON DELETE CASCADE
);

-- ------------------------------------------------------------- tenancy ---
-- Added in Week 6. Runs requested through the control plane belong to an
-- organization and target one of its verified environments. Runs created
-- with `burstforge run --db` have no organization and work as before.

CREATE TABLE IF NOT EXISTS organizations (
    id                   text PRIMARY KEY CHECK (id ~ '^[a-z0-9][a-z0-9-]{1,38}$'),
    name                 text NOT NULL,
    -- Quotas. Zero means none allowed. The control plane checks them when a
    -- run is requested, the controller when it starts it, and agents again
    -- before they send anything.
    max_concurrent_runs  int NOT NULL DEFAULT 1      CHECK (max_concurrent_runs >= 0),
    max_runs_per_hour    int NOT NULL DEFAULT 20     CHECK (max_runs_per_hour >= 0),
    max_arrivals         int NOT NULL DEFAULT 1000   CHECK (max_arrivals >= 0),
    max_arrival_rate     int NOT NULL DEFAULT 500    CHECK (max_arrival_rate >= 0), -- per second
    max_duration_ms      int NOT NULL DEFAULT 300000 CHECK (max_duration_ms >= 0),  -- planned span
    api_requests_per_min int NOT NULL DEFAULT 120    CHECK (api_requests_per_min >= 0),
    created_at           timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS api_keys (
    id              text PRIMARY KEY,           -- public half of the key
    organization_id text NOT NULL REFERENCES organizations ON DELETE CASCADE,
    name            text NOT NULL,
    role            text NOT NULL CHECK (role IN ('owner', 'operator', 'viewer')),
    secret_sha256   bytea NOT NULL,             -- the secret half is never stored
    created_at      timestamptz NOT NULL DEFAULT now(),
    last_used_at    timestamptz,
    revoked_at      timestamptz
);

-- A target an organization may test. It is verified when the target serves
-- approval_token at /.well-known/burstforge-approval.txt; agents check that
-- again before every shard.
CREATE TABLE IF NOT EXISTS environments (
    organization_id text NOT NULL REFERENCES organizations ON DELETE CASCADE,
    name            text NOT NULL CHECK (name ~ '^[a-z0-9][a-z0-9-]{0,38}$'),
    base_url        text NOT NULL,
    approval_token  text NOT NULL,
    verified_at     timestamptz,
    created_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (organization_id, name)
);

-- A run requested through the control plane. A controller claims it, runs
-- the before hooks, and creates the row in runs with the same id. The
-- controller holds a lease on the request for as long as it drives the run.
CREATE TABLE IF NOT EXISTS run_requests (
    id              text PRIMARY KEY,
    organization_id text NOT NULL,
    environment     text NOT NULL,
    scenario_yaml   text NOT NULL,
    seed            bigint NOT NULL,
    shard_count     int NOT NULL CHECK (shard_count > 0),
    status          text NOT NULL DEFAULT 'queued'
                    CHECK (status IN ('queued', 'starting', 'started', 'rejected')),
    reason          text,
    controller      text,
    lease_until     timestamptz,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    FOREIGN KEY (organization_id, environment) REFERENCES environments ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS run_requests_open
    ON run_requests (created_at) WHERE status IN ('queued', 'starting', 'started');
CREATE INDEX IF NOT EXISTS run_requests_org ON run_requests (organization_id, created_at);

ALTER TABLE runs ADD COLUMN IF NOT EXISTS organization_id text REFERENCES organizations ON DELETE CASCADE;
ALTER TABLE runs ADD COLUMN IF NOT EXISTS environment text;
