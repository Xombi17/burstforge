// Package store keeps run state in PostgreSQL: runs, shards, leases and
// telemetry chunks.
//
// Agents claim shards with FOR UPDATE SKIP LOCKED. A claim bumps the shard's
// epoch, which fences out the previous holder: every reservation, heartbeat
// and completion is conditional on the epoch and an unexpired lease, using
// the database clock. Telemetry is the exception. Chunks from any epoch are
// stored until the run is finalized, because they record traffic that was
// really sent.
package store

import (
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"burstforge/internal/report"
	"burstforge/internal/runner"
	"burstforge/internal/scenario"
)

//go:embed schema.sql
var schema string

type Store struct {
	pool *pgxpool.Pool
}

// Open connects and applies the schema.
func Open(ctx context.Context, url string) (*Store, error) {
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, err
	}
	s := &Store{pool: pool}
	if err := s.migrate(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() { s.pool.Close() }

func (s *Store) migrate(ctx context.Context) error {
	// Several agents may start at once; CREATE IF NOT EXISTS is not safe
	// under concurrency, so serialize on an advisory lock. The schema is
	// applied only when its version changed: statements like ALTER TABLE ...
	// ADD COLUMN IF NOT EXISTS lock the table even when they do nothing, and
	// every agent that starts would otherwise stall running agents' writes
	// (and could deadlock with them).
	sum := sha256.Sum256([]byte(schema))
	version := hex.EncodeToString(sum[:8])
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext('burstforge-schema'))`); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS burstforge_schema (
			id int PRIMARY KEY CHECK (id = 1), version text NOT NULL, applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
			return err
		}
		var current string
		err := tx.QueryRow(ctx, `SELECT version FROM burstforge_schema WHERE id = 1`).Scan(&current)
		if err == nil && current == version {
			return nil
		}
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		if _, err := tx.Exec(ctx, schema); err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `
			INSERT INTO burstforge_schema (id, version) VALUES (1, $1)
			ON CONFLICT (id) DO UPDATE SET version = $1, applied_at = now()`, version)
		return err
	})
}

// ---------------------------------------------------------------- runs ---

type NewRun struct {
	ID         string
	Scenario   *scenario.Scenario
	Seed       int64
	ShardCount int
	Lead       time.Duration // start_at = database now() + Lead
	LeaseTTL   time.Duration
	Before     map[string]float64
	TrafficURL string // fault proxy agents send workflow traffic to; empty for none

	// Set for runs requested through the control plane. Controller is the
	// controller holding the request; storing the run marks it started.
	Organization string
	Environment  string
	Controller   string
}

// ShardSize is how many arrivals round-robin sharding gives shard k (1-based).
func ShardSize(arrivals, k, n int) int {
	return (arrivals - k + n) / n
}

// CreateRun stores the run and its shards and returns the shared start time.
func (s *Store) CreateRun(ctx context.Context, nr NewRun) (time.Time, error) {
	sc := nr.Scenario
	if nr.ShardCount < 1 || nr.ShardCount > sc.Traffic.Arrivals {
		return time.Time{}, fmt.Errorf("shard count %d must be between 1 and %d arrivals", nr.ShardCount, sc.Traffic.Arrivals)
	}
	scJSON, err := json.Marshal(sc)
	if err != nil {
		return time.Time{}, err
	}
	var before []byte
	if nr.Before != nil {
		if before, err = json.Marshal(nr.Before); err != nil {
			return time.Time{}, err
		}
	}
	var startAt time.Time
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		err := tx.QueryRow(ctx, `
			INSERT INTO runs (id, scenario_name, scenario, scenario_hash, seed, shard_count, start_at, lease_ttl_ms,
			                  before_counters, traffic_url, organization_id, environment)
			VALUES ($1, $2, $3, $4, $5, $6, now() + make_interval(secs => $7), $8, $9, nullif($10, ''), nullif($11, ''), nullif($12, ''))
			RETURNING start_at`,
			nr.ID, sc.Name, scJSON, sc.Hash(), nr.Seed, nr.ShardCount,
			nr.Lead.Seconds(), nr.LeaseTTL.Milliseconds(), before, nr.TrafficURL, nr.Organization, nr.Environment,
		).Scan(&startAt)
		if err != nil {
			return err
		}
		if nr.Organization != "" {
			tag, err := tx.Exec(ctx, `
				UPDATE run_requests SET status = 'started', updated_at = now()
				WHERE id = $1 AND organization_id = $2 AND controller = $3 AND status = 'starting' AND lease_until > now()`,
				nr.ID, nr.Organization, nr.Controller)
			if err != nil {
				return err
			}
			if tag.RowsAffected() != 1 {
				return fmt.Errorf("run request %s is no longer held by controller %s", nr.ID, nr.Controller)
			}
		}
		for k := 1; k <= nr.ShardCount; k++ {
			if _, err := tx.Exec(ctx, `INSERT INTO run_shards (run_id, shard_no, arrivals) VALUES ($1, $2, $3)`,
				nr.ID, k, ShardSize(sc.Traffic.Arrivals, k, nr.ShardCount)); err != nil {
				return err
			}
		}
		return nil
	})
	return startAt, err
}

type Run struct {
	ID           string
	Scenario     *scenario.Scenario
	ScenarioHash string
	Seed         int64
	ShardCount   int
	StartAt      time.Time
	LeaseTTL     time.Duration
	Status       string
	Before       map[string]float64
	CreatedAt    time.Time
	TrafficURL   string
	Organization string // empty for runs created from the CLI
	Environment  string
}

const runCols = `id, scenario, scenario_hash, seed, shard_count, start_at, lease_ttl_ms, status, before_counters, created_at,
	coalesce(traffic_url, ''), coalesce(organization_id, ''), coalesce(environment, '')`

func scanRun(row pgx.Row) (*Run, error) {
	var r Run
	var scJSON, before []byte
	var ttlMs int64
	if err := row.Scan(&r.ID, &scJSON, &r.ScenarioHash, &r.Seed, &r.ShardCount, &r.StartAt, &ttlMs, &r.Status, &before, &r.CreatedAt,
		&r.TrafficURL, &r.Organization, &r.Environment); err != nil {
		return nil, err
	}
	r.LeaseTTL = time.Duration(ttlMs) * time.Millisecond
	r.Scenario = &scenario.Scenario{}
	if err := json.Unmarshal(scJSON, r.Scenario); err != nil {
		return nil, fmt.Errorf("run %s: stored scenario: %w", r.ID, err)
	}
	if before != nil {
		if err := json.Unmarshal(before, &r.Before); err != nil {
			return nil, fmt.Errorf("run %s: stored counters: %w", r.ID, err)
		}
	}
	return &r, nil
}

func (s *Store) GetRun(ctx context.Context, id string) (*Run, error) {
	r, err := scanRun(s.pool.QueryRow(ctx, `SELECT `+runCols+` FROM runs WHERE id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("no run %q", id)
	}
	return r, err
}

type RunRow struct {
	ID, Scenario, Status string
	Shards, Done         int
	CreatedAt            time.Time
}

// ListRuns returns the most recent runs.
func (s *Store) ListRuns(ctx context.Context, limit int) ([]RunRow, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT r.id, r.scenario_name, r.status, r.shard_count, r.created_at,
		       count(*) FILTER (WHERE sh.state IN ('completed', 'abandoned'))
		FROM runs r JOIN run_shards sh ON sh.run_id = r.id
		GROUP BY r.id ORDER BY r.created_at DESC LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (RunRow, error) {
		var r RunRow
		err := row.Scan(&r.ID, &r.Scenario, &r.Status, &r.Shards, &r.CreatedAt, &r.Done)
		return r, err
	})
}

type ShardState struct {
	Shard           int
	Arrivals        int
	State           string
	Epoch           int64
	ReservedThrough int
	Agent           string // current or last lease holder
	ResumedFrom     int    // where that lease started in the shard
	LeaseExpiresIn  time.Duration
}

// Shards reports the current state of each shard of a run.
func (s *Store) Shards(ctx context.Context, runID string) ([]ShardState, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT sh.shard_no, sh.arrivals, sh.state, sh.epoch, sh.reserved_through,
		       coalesce(l.agent_id, ''), coalesce(l.resumed_from, 0), coalesce(extract(epoch FROM l.expires_at - now()), 0)
		FROM run_shards sh
		LEFT JOIN LATERAL (
			SELECT agent_id, resumed_from, expires_at FROM agent_leases
			WHERE run_id = sh.run_id AND shard_no = sh.shard_no ORDER BY epoch DESC LIMIT 1
		) l ON true
		WHERE sh.run_id = $1 ORDER BY sh.shard_no`, runID)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (ShardState, error) {
		var st ShardState
		var secs float64
		err := row.Scan(&st.Shard, &st.Arrivals, &st.State, &st.Epoch, &st.ReservedThrough, &st.Agent, &st.ResumedFrom, &secs)
		st.LeaseExpiresIn = time.Duration(secs * float64(time.Second))
		return st, err
	})
}

// --------------------------------------------------------------- clock ---

// MeasureClock estimates the database clock minus the local clock. It takes
// the sample with the shortest round trip; the uncertainty is half of it.
func (s *Store) MeasureClock(ctx context.Context) (offset, uncertainty time.Duration, err error) {
	best := time.Duration(-1)
	for i := 0; i < 5; i++ {
		t0 := time.Now()
		var db time.Time
		if err := s.pool.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&db); err != nil {
			return 0, 0, err
		}
		rtt := time.Since(t0)
		if best < 0 || rtt < best {
			best = rtt
			offset = db.Sub(t0.Add(rtt / 2))
		}
	}
	return offset, best / 2, nil
}

// --------------------------------------------------------------- claim ---

type Claim struct {
	Run    *Run
	Shard  int
	Epoch  int64
	Resume int // first shard arrival this lease may send
	Lease  *Lease
}

// Claim takes one shard that is pending or whose lease has expired, starting
// a new epoch. It returns nil if there is nothing to claim. With runID set it
// only looks at that run.
func (s *Store) Claim(ctx context.Context, agentID, runID string, clockOffset, clockUncertainty time.Duration) (*Claim, error) {
	return s.ClaimExcept(ctx, agentID, runID, nil, clockOffset, clockUncertainty)
}

// ClaimExcept is Claim, skipping the runs in exclude: the ones this agent
// has refused. Without that, a refused run at the head of the queue would
// keep every agent from reaching the runs behind it.
func (s *Store) ClaimExcept(ctx context.Context, agentID, runID string, exclude []string, clockOffset, clockUncertainty time.Duration) (*Claim, error) {
	if exclude == nil {
		exclude = []string{}
	}
	var c *Claim
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var rid string
		var shard int
		var epoch int64
		var resume int
		err := tx.QueryRow(ctx, `
			SELECT sh.run_id, sh.shard_no, sh.epoch, sh.reserved_through
			FROM run_shards sh
			JOIN runs r ON r.id = sh.run_id
			LEFT JOIN agent_leases l
			       ON l.run_id = sh.run_id AND l.shard_no = sh.shard_no AND l.epoch = sh.epoch
			WHERE r.status = 'running'
			  AND ($1 = '' OR r.id = $1)
			  AND NOT (r.id = ANY($2))
			  AND (sh.state = 'pending' OR (sh.state = 'claimed' AND l.expires_at <= now()))
			ORDER BY r.created_at, sh.shard_no
			LIMIT 1
			FOR UPDATE OF sh SKIP LOCKED`, runID, exclude).Scan(&rid, &shard, &epoch, &resume)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		// The previous holder, if any, let its lease run out.
		if _, err := tx.Exec(ctx, `
			UPDATE agent_leases SET outcome = 'expired', ended_at = expires_at
			WHERE run_id = $1 AND shard_no = $2 AND epoch = $3 AND outcome IS NULL`, rid, shard, epoch); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `
			UPDATE run_shards SET epoch = epoch + 1, state = 'claimed'
			WHERE run_id = $1 AND shard_no = $2 RETURNING epoch`, rid, shard).Scan(&epoch); err != nil {
			return err
		}
		run, err := scanRun(tx.QueryRow(ctx, `SELECT `+runCols+` FROM runs WHERE id = $1`, rid))
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO agent_leases (run_id, shard_no, epoch, agent_id, resumed_from, expires_at, clock_offset_ms, clock_uncertainty_ms)
			VALUES ($1, $2, $3, $4, $5, now() + make_interval(secs => $6), $7, $8)`,
			rid, shard, epoch, agentID, resume, run.LeaseTTL.Seconds(),
			ms(clockOffset), ms(clockUncertainty)); err != nil {
			return err
		}
		c = &Claim{Run: run, Shard: shard, Epoch: epoch, Resume: resume}
		c.Lease = &Lease{s: s, run: rid, shard: shard, epoch: epoch, agent: agentID, ttl: run.LeaseTTL}
		return nil
	})
	return c, err
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }

// --------------------------------------------------------------- lease ---

// Lease implements runner.Lease for one claimed shard epoch.
type Lease struct {
	s     *Store
	run   string
	shard int
	epoch int64
	agent string
	ttl   time.Duration

	mu      sync.Mutex
	nextSeq int
	queue   []queued // chunks not yet acknowledged by the database
	dropped int      // chunks refused because the run was already finalized
}

type queued struct {
	seq  int
	data []byte
}

var _ runner.Lease = (*Lease)(nil)

func (l *Lease) Epoch() int64 { return l.epoch }

// Dropped is how many chunks arrived after the run was finalized.
func (l *Lease) Dropped() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.dropped
}

// enqueue gives a chunk its sequence number once, so a retry after a lost
// acknowledgement sends exactly the same (seq, data) and is deduplicated.
func (l *Lease) enqueue(chunk *report.Report) error {
	if chunk == nil {
		return nil
	}
	b, err := json.Marshal(chunk)
	if err != nil {
		return err
	}
	l.nextSeq++
	l.queue = append(l.queue, queued{seq: l.nextSeq, data: b})
	return nil
}

// flush stores queued chunks inside tx. It returns false if the run is
// already finalized, in which case the chunks are dropped.
func (l *Lease) flush(ctx context.Context, tx pgx.Tx) (open bool, err error) {
	var status string
	if err := tx.QueryRow(ctx, `SELECT status FROM runs WHERE id = $1 FOR SHARE`, l.run).Scan(&status); err != nil {
		return false, err
	}
	if status == "completed" {
		return false, nil
	}
	for _, q := range l.queue {
		if _, err := tx.Exec(ctx, `
			INSERT INTO telemetry_chunks (run_id, shard_no, epoch, seq, agent_id, data)
			VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT DO NOTHING`,
			l.run, l.shard, l.epoch, q.seq, l.agent, q.data); err != nil {
			return false, err
		}
	}
	return true, nil
}

// leaseValid is the fencing condition shared by every ownership write.
const leaseValid = `
	sh.run_id = $1 AND sh.shard_no = $2 AND sh.epoch = $3 AND sh.state = 'claimed'
	AND EXISTS (SELECT 1 FROM agent_leases l
	            WHERE l.run_id = sh.run_id AND l.shard_no = sh.shard_no AND l.epoch = sh.epoch
	              AND l.outcome IS NULL AND l.expires_at > now())`

// tx runs fn after flushing chunks, and clears the queue once the
// transaction commits. fn reports a lost lease by returning fenced=true
// rather than an error, so the transaction still commits the chunks.
func (l *Lease) tx(ctx context.Context, chunk *report.Report, fn func(tx pgx.Tx) (fenced bool, err error)) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.enqueue(chunk); err != nil {
		return err
	}
	var closed, fenced bool
	err := pgx.BeginFunc(ctx, l.s.pool, func(tx pgx.Tx) error {
		open, err := l.flush(ctx, tx)
		if err != nil {
			return err
		}
		if !open {
			closed = true
			return nil
		}
		fenced, err = fn(tx)
		return err
	})
	if err != nil {
		return err // queue kept for the next attempt
	}
	if closed {
		l.dropped += len(l.queue)
	}
	l.queue = nil
	if closed || fenced {
		return runner.ErrFenced
	}
	return nil
}

func (l *Lease) Sync(ctx context.Context, want int, chunk *report.Report) (int, error) {
	var reserved int
	err := l.tx(ctx, chunk, func(tx pgx.Tx) (bool, error) {
		err := tx.QueryRow(ctx, `
			UPDATE run_shards sh SET reserved_through = greatest(sh.reserved_through, least($4, sh.arrivals))
			WHERE `+leaseValid+` RETURNING sh.reserved_through`,
			l.run, l.shard, l.epoch, want).Scan(&reserved)
		if errors.Is(err, pgx.ErrNoRows) {
			return true, nil
		}
		if err != nil {
			return false, err
		}
		_, err = tx.Exec(ctx, `
			UPDATE agent_leases SET expires_at = now() + make_interval(secs => $4), renewed_at = now()
			WHERE run_id = $1 AND shard_no = $2 AND epoch = $3`,
			l.run, l.shard, l.epoch, l.ttl.Seconds())
		return false, err
	})
	return reserved, err
}

func (l *Lease) Complete(ctx context.Context, chunk *report.Report) error {
	return l.tx(ctx, chunk, func(tx pgx.Tx) (bool, error) {
		tag, err := tx.Exec(ctx, `
			UPDATE run_shards sh SET state = 'completed'
			WHERE `+leaseValid+` AND sh.reserved_through = sh.arrivals`,
			l.run, l.shard, l.epoch)
		if err != nil {
			return false, err
		}
		if tag.RowsAffected() == 0 {
			return true, nil
		}
		_, err = tx.Exec(ctx, `
			UPDATE agent_leases SET outcome = 'completed', ended_at = now()
			WHERE run_id = $1 AND shard_no = $2 AND epoch = $3`, l.run, l.shard, l.epoch)
		return false, err
	})
}

func (l *Lease) Release(ctx context.Context, handled int, chunk *report.Report) error {
	err := l.tx(ctx, chunk, func(tx pgx.Tx) (bool, error) {
		// Hand back reserved arrivals this agent never reached. Only the
		// current holder can do this, and it sends nothing after releasing.
		tag, err := tx.Exec(ctx, `
			UPDATE run_shards sh SET reserved_through = least(sh.reserved_through, greatest($4, 0))
			WHERE `+leaseValid,
			l.run, l.shard, l.epoch, handled)
		if err != nil || tag.RowsAffected() == 0 {
			return err == nil, err
		}
		_, err = tx.Exec(ctx, `
			UPDATE agent_leases SET outcome = 'released', ended_at = now(), expires_at = now()
			WHERE run_id = $1 AND shard_no = $2 AND epoch = $3`,
			l.run, l.shard, l.epoch)
		return false, err
	})
	if errors.Is(err, runner.ErrFenced) {
		return nil // nothing left to release
	}
	return err
}

// ------------------------------------------------------------ finalize ---

// BeginFinalize stops the run from handing out shards and fences every shard
// that is not done, so no agent can reserve more arrivals. It is idempotent
// and returns the run as it now stands.
func (s *Store) BeginFinalize(ctx context.Context, runID string) (*Run, error) {
	var run *Run
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var err error
		run, err = scanRun(tx.QueryRow(ctx, `SELECT `+runCols+` FROM runs WHERE id = $1 FOR UPDATE`, runID))
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("no run %q", runID)
		}
		if err != nil || run.Status == "completed" {
			return err
		}
		if _, err := tx.Exec(ctx, `
			UPDATE agent_leases l SET outcome = 'fenced', ended_at = now()
			FROM run_shards sh
			WHERE sh.run_id = $1 AND sh.state = 'claimed' AND l.outcome IS NULL
			  AND l.run_id = sh.run_id AND l.shard_no = sh.shard_no AND l.epoch = sh.epoch`, runID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			UPDATE run_shards SET state = 'abandoned', epoch = epoch + 1
			WHERE run_id = $1 AND state IN ('pending', 'claimed')`, runID); err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `UPDATE runs SET status = 'finalizing' WHERE id = $1`, runID)
		run.Status = "finalizing"
		return err
	})
	return run, err
}

// CompleteRun reduces all stored telemetry into the final report, lets
// decide fill in invariants and the verdict, stores the report and closes
// the run. The run row stays locked throughout, so no chunk can slip in
// between the reduction and the close. On an already completed run it
// returns the stored report and does not call decide.
func (s *Store) CompleteRun(ctx context.Context, runID string, decide func(*report.Report) error) (*report.Report, error) {
	var rep *report.Report
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		run, err := scanRun(tx.QueryRow(ctx, `SELECT `+runCols+` FROM runs WHERE id = $1 FOR UPDATE`, runID))
		if err != nil {
			return err
		}
		if run.Status == "completed" {
			var b []byte
			if err := tx.QueryRow(ctx, `SELECT report FROM runs WHERE id = $1`, runID).Scan(&b); err != nil {
				return err
			}
			rep = &report.Report{}
			return json.Unmarshal(b, rep)
		}
		if run.Status != "finalizing" {
			return fmt.Errorf("run %s is %s; call BeginFinalize first", runID, run.Status)
		}
		if rep, err = reduce(ctx, tx, run); err != nil {
			return err
		}
		if err := decide(rep); err != nil {
			return err
		}
		b, err := json.Marshal(rep)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `UPDATE runs SET status = 'completed', report = $2, finished_at = now() WHERE id = $1`, runID, b)
		return err
	})
	return rep, err
}

type leaseRow struct {
	shard, resumedFrom int
	epoch              int64
	agent, outcome     string
	acquired, ended    time.Time
	offsetMs           float64
}

func reduce(ctx context.Context, tx pgx.Tx, run *Run) (*report.Report, error) {
	base, err := runner.BaseReport(run.Scenario, run.ID, run.Seed)
	if err != nil {
		return nil, err
	}
	base.StartedAt = run.CreatedAt

	type shardRow struct {
		arrivals, reserved int
		state              string
	}
	shards := map[int]shardRow{}
	rows, err := tx.Query(ctx, `SELECT shard_no, arrivals, reserved_through, state FROM run_shards WHERE run_id = $1`, run.ID)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var k int
		var r shardRow
		if err := rows.Scan(&k, &r.arrivals, &r.reserved, &r.state); err != nil {
			return nil, err
		}
		shards[k] = r
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	leases := map[int][]leaseRow{}
	rows, err = tx.Query(ctx, `
		SELECT shard_no, epoch, agent_id, resumed_from, coalesce(outcome, ''), acquired_at,
		       coalesce(ended_at, 'epoch'::timestamptz), coalesce(clock_offset_ms, 0)
		FROM agent_leases WHERE run_id = $1 ORDER BY shard_no, epoch`, run.ID)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var l leaseRow
		if err := rows.Scan(&l.shard, &l.epoch, &l.agent, &l.resumedFrom, &l.outcome, &l.acquired, &l.ended, &l.offsetMs); err != nil {
			return nil, err
		}
		leases[l.shard] = append(leases[l.shard], l)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	chunks := map[int][]*report.Report{}
	rows, err = tx.Query(ctx, `SELECT shard_no, data FROM telemetry_chunks WHERE run_id = $1 ORDER BY shard_no, epoch, seq`, run.ID)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var k int
		var b []byte
		if err := rows.Scan(&k, &b); err != nil {
			return nil, err
		}
		var c report.Report
		if err := json.Unmarshal(b, &c); err != nil {
			return nil, fmt.Errorf("telemetry chunk for shard %d: %w", k, err)
		}
		chunks[k] = append(chunks[k], &c)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	rec := &report.Recovery{}
	at := func(t time.Time) string {
		return fmt.Sprintf("t%+.1fs", t.Sub(run.StartAt).Seconds())
	}
	var shardReports []*report.Report
	for k := 1; k <= run.ShardCount; k++ {
		sh := shards[k]
		ls := leases[k]
		var agents []string
		for _, l := range ls {
			agents = append(agents, l.agent)
		}
		b := *base
		b.Shard = &report.ShardInfo{
			Index: k, Count: run.ShardCount, Arrivals: sh.arrivals,
			ScenarioHash: run.ScenarioHash, StartAt: run.StartAt.UTC(),
			Agent: strings.Join(agents, " > "),
		}
		r := report.Sum(&b, chunks[k])
		r.Shard = b.Shard
		d := &report.ShardDetail{
			Epochs:  len(ls),
			NotSent: sh.arrivals - sh.reserved,
			// Reserved but never reported: sent or not, nobody knows.
			Unaccounted: max(0, sh.reserved-r.Totals.Logical-r.Totals.SkippedOverdue),
		}
		if len(ls) > 0 {
			d.ClockOffsetMs = ls[len(ls)-1].offsetMs
		}
		r.ShardDetail = d
		shardReports = append(shardReports, r)

		rec.Takeovers += max(0, len(ls)-1)
		rec.Unaccounted += d.Unaccounted
		rec.NotSent += d.NotSent
		rec.SkippedOverdue += r.Totals.SkippedOverdue
		for _, l := range ls {
			switch l.outcome {
			case "completed":
				if len(ls) > 1 {
					rec.Events = append(rec.Events, fmt.Sprintf("shard %d lease %d (%s): resumed at arrival %d at %s, completed at %s",
						k, l.epoch, l.agent, l.resumedFrom+1, at(l.acquired), at(l.ended)))
				}
			case "":
				rec.Events = append(rec.Events, fmt.Sprintf("shard %d lease %d (%s): still open at finalize", k, l.epoch, l.agent))
			default:
				rec.Events = append(rec.Events, fmt.Sprintf("shard %d lease %d (%s): taken at %s from arrival %d, %s at %s",
					k, l.epoch, l.agent, at(l.acquired), l.resumedFrom+1, l.outcome, at(l.ended)))
			}
		}
		if sh.state == "abandoned" {
			rec.Events = append(rec.Events, fmt.Sprintf("shard %d abandoned at finalize: %d of %d arrivals never reserved",
				k, d.NotSent, sh.arrivals))
		}
	}

	rep, err := report.Merge(shardReports, false)
	if err != nil {
		return nil, err
	}
	rep.StartedAt = run.CreatedAt
	rep.FinishedAt = time.Now()
	rep.Recovery = rec
	return rep, nil
}

// ---------------------------------------------------------------- work ---

func trusted(run *Run, hosts []string) []string {
	if run.Organization == "" {
		return nil
	}
	return hosts
}

// RunClaim executes a claimed shard. clockOffset (database minus local)
// converts the run's start time to the local clock. It returns
// runner.ErrFenced if another agent took the shard over.
func RunClaim(ctx context.Context, c *Claim, agent string, clockOffset time.Duration, maxArrivals int,
	logf func(string, ...any), obs runner.Observer, trustedProxies []string) (*report.Report, error) {
	run := c.Run
	hb := run.LeaseTTL / 5
	if hb < 50*time.Millisecond {
		hb = 50 * time.Millisecond
	}
	return runner.RunShard(ctx, run.Scenario, runner.Options{
		Seed:        run.Seed,
		RunID:       run.ID,
		MaxArrivals: maxArrivals,
		Logf:        logf,
		Shard:       c.Shard,
		ShardCount:  run.ShardCount,
		StartAt:     run.StartAt.Add(-clockOffset),
		Agent:       agent,
		TrafficURL:  run.TrafficURL,
		Lease:       c.Lease,
		// Only organization runs have had their traffic URL checked
		// against the agent's trusted proxies (CheckTenantRun).
		TrustedProxies: trusted(run, trustedProxies),
		Resume:         c.Resume,
		Heartbeat:      hb,
		LeaseTTL:       run.LeaseTTL,
		Observer:       obs,
	})
}
