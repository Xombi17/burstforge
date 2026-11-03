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
