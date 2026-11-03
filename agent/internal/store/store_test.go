package store

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"burstforge/internal/report"
	"burstforge/internal/runner"
	"burstforge/internal/scenario"
)

// These tests need a real PostgreSQL. Point BURSTFORGE_TEST_DB at a
// throwaway database, for example:
//
//	BURSTFORGE_TEST_DB=postgres://postgres@localhost:5433/burstforge go test ./internal/store/
func openTest(t *testing.T) *Store {
	t.Helper()
	url := os.Getenv("BURSTFORGE_TEST_DB")
	if url == "" {
		t.Skip("BURSTFORGE_TEST_DB not set")
	}
	s, err := Open(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	return s
}

var runSeq atomic.Int64

func newRun(t *testing.T, s *Store, sc *scenario.Scenario, shards int, ttl, lead time.Duration) string {
	t.Helper()
	id := fmt.Sprintf("test-%s-%d-%d", t.Name(), time.Now().UnixNano(), runSeq.Add(1))
	if _, err := s.CreateRun(context.Background(), NewRun{
		ID: id, Scenario: sc, Seed: 3, ShardCount: shards, Lead: lead, LeaseTTL: ttl,
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = s.pool.Exec(context.Background(), `DELETE FROM runs WHERE id = $1`, id)
	})
	return id
}

func testScenario(url string, arrivals int, duration time.Duration) *scenario.Scenario {
	return &scenario.Scenario{
		Name:   "store-test",
		Target: scenario.Target{BaseURL: url, Environment: "local"},
		Workflow: scenario.Workflow{Steps: []scenario.Step{{
			Name: "act", Method: "POST", Path: "/act", Timeout: 2 * time.Second,
			Retry: scenario.Retry{MaxAttempts: 1, Backoff: "none", Jitter: "none"},
		}}},
		Traffic: scenario.Traffic{
			Profile: "sustained", Arrivals: arrivals, Duration: duration,
			MaxInflight: 100, DoubleSubmitDelay: 5 * time.Millisecond,
		},
	}
}

func chunk(logical int) *report.Report {
	return &report.Report{Totals: report.Totals{Logical: logical, Succeeded: logical, FinalOutcomes: map[string]int{"200": logical}}}
}

func chunkCount(t *testing.T, s *Store, run string) int {
	var n int
	if err := s.pool.QueryRow(context.Background(), `SELECT count(*) FROM telemetry_chunks WHERE run_id = $1`, run).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestConcurrentClaimsGetDistinctShards(t *testing.T) {
	s := openTest(t)
	run := newRun(t, s, testScenario("http://127.0.0.1:1", 50, time.Second), 5, 10*time.Second, time.Second)

	var mu sync.Mutex
	got := map[int]int{}
	var wg sync.WaitGroup
	for a := 0; a < 12; a++ {
		wg.Add(1)
		go func(a int) {
			defer wg.Done()
			c, err := s.Claim(context.Background(), fmt.Sprintf("a%d", a), run, 0, 0)
			if err != nil {
				t.Error(err)
				return
			}
			if c != nil {
				mu.Lock()
				got[c.Shard]++
				mu.Unlock()
			}
		}(a)
	}
	wg.Wait()
	if len(got) != 5 {
		t.Fatalf("claimed shards %v, want each of 1..5 once", got)
	}
	for k, n := range got {
		if n != 1 {
			t.Fatalf("shard %d claimed %d times", k, n)
		}
	}
}

func TestExpiredLeaseIsFencedAndResumed(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()
	run := newRun(t, s, testScenario("http://127.0.0.1:1", 10, time.Second), 1, 300*time.Millisecond, time.Second)

	a, err := s.Claim(ctx, "a", run, 0, 0)
	if err != nil || a == nil {
		t.Fatalf("claim a: %v %v", a, err)
	}
	if n, err := a.Lease.Sync(ctx, 4, chunk(1)); err != nil || n != 4 {
		t.Fatalf("a reserve: %d %v", n, err)
	}
	if c, _ := s.Claim(ctx, "b", run, 0, 0); c != nil {
		t.Fatal("claimed a shard whose lease is still live")
	}

	time.Sleep(400 * time.Millisecond) // a stops heartbeating; its lease expires
	b, err := s.Claim(ctx, "b", run, 0, 0)
	if err != nil || b == nil {
		t.Fatalf("claim b: %v %v", b, err)
	}
	if b.Epoch != a.Epoch+1 || b.Resume != 4 {
		t.Fatalf("b epoch %d resume %d, want %d and 4", b.Epoch, b.Resume, a.Epoch+1)
	}

	// The stale holder can't reserve or complete, but its telemetry is kept.
	if _, err := a.Lease.Sync(ctx, 10, chunk(2)); !errors.Is(err, runner.ErrFenced) {
		t.Fatalf("stale sync err = %v, want ErrFenced", err)
	}
	if err := a.Lease.Complete(ctx, nil); !errors.Is(err, runner.ErrFenced) {
		t.Fatalf("stale complete err = %v, want ErrFenced", err)
	}
	if n := chunkCount(t, s, run); n != 2 {
		t.Fatalf("stored chunks %d, want 2 (both from the stale epoch)", n)
	}

	// b can't complete before every arrival is reserved.
	if err := b.Lease.Complete(ctx, nil); !errors.Is(err, runner.ErrFenced) {
		t.Fatalf("early complete err = %v", err)
	}
	if n, err := b.Lease.Sync(ctx, 10, chunk(6)); err != nil || n != 10 {
		t.Fatalf("b reserve: %d %v", n, err)
	}
	if err := b.Lease.Complete(ctx, nil); err != nil {
		t.Fatal(err)
	}
	st, _ := s.Shards(ctx, run)
	if st[0].State != "completed" || st[0].Epoch != 2 {
		t.Fatalf("shard %+v", st[0])
	}
}

func TestReleasedLeaseIsClaimableAtOnce(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()
	run := newRun(t, s, testScenario("http://127.0.0.1:1", 10, time.Second), 1, time.Minute, time.Second)
	a, _ := s.Claim(ctx, "a", run, 0, 0)
	if _, err := a.Lease.Sync(ctx, 3, nil); err != nil {
		t.Fatal(err)
	}
	// Reserved 3 but only got through 2: the third goes back to the shard.
	if err := a.Lease.Release(ctx, 2, chunk(2)); err != nil {
		t.Fatal(err)
	}
	b, err := s.Claim(ctx, "b", run, 0, 0)
	if err != nil || b == nil || b.Resume != 2 {
		t.Fatalf("claim after release: %+v %v", b, err)
	}
}

func TestRetriedChunkIsStoredOnce(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()
	run := newRun(t, s, testScenario("http://127.0.0.1:1", 10, time.Second), 1, time.Minute, time.Second)
	c, _ := s.Claim(ctx, "a", run, 0, 0)
	l := c.Lease

	// The commit succeeds but the acknowledgement is "lost": the chunk stays
	// queued and goes out again with the same sequence number.
	l.mu.Lock()
	_ = l.enqueue(chunk(5))
	l.mu.Unlock()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := l.flush(ctx, tx); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Sync(ctx, 0, nil); err != nil {
		t.Fatal(err)
	}
	if n := chunkCount(t, s, run); n != 1 {
		t.Fatalf("stored chunks %d, want 1", n)
	}
}

func TestFinalizeFencesOpenShardsAndIsIdempotent(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()
	run := newRun(t, s, testScenario("http://127.0.0.1:1", 20, time.Second), 2, time.Minute, time.Second)

	a, _ := s.Claim(ctx, "a", run, 0, 0) // shard 1, 10 arrivals
	if _, err := a.Lease.Sync(ctx, 6, chunk(4)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.BeginFinalize(ctx, run); err != nil {
		t.Fatal(err)
	}
	if c, _ := s.Claim(ctx, "b", run, 0, 0); c != nil {
		t.Fatal("claimed a shard of a finalizing run")
	}
	// Fenced, but a straggler's telemetry is still accepted until the close.
	if _, err := a.Lease.Sync(ctx, 10, chunk(1)); !errors.Is(err, runner.ErrFenced) {
		t.Fatalf("sync after finalize: %v", err)
	}

	calls := 0
	rep, err := s.CompleteRun(ctx, run, func(r *report.Report) error { calls++; r.Finalize(); return nil })
	if err != nil {
		t.Fatal(err)
	}
	rc := rep.Recovery
	// Shard 1: 6 reserved, 5 reported -> 1 unaccounted, 4 never sent.
	// Shard 2: never claimed -> 10 never sent.
	if rep.Totals.Logical != 5 || rc.Unaccounted != 1 || rc.NotSent != 14 || rep.Verdict.Pass {
		t.Fatalf("logical %d, recovery %+v, verdict %+v", rep.Totals.Logical, rc, rep.Verdict)
	}

	// After the close, telemetry is refused and the stored report is final.
	if _, err := a.Lease.Sync(ctx, 0, chunk(1)); !errors.Is(err, runner.ErrFenced) || a.Lease.Dropped() != 1 {
		t.Fatalf("sync after close: %v, dropped %d", err, a.Lease.Dropped())
	}
	again, err := s.CompleteRun(ctx, run, func(*report.Report) error { calls++; return nil })
	if err != nil || calls != 1 || again.Totals.Logical != 5 {
		t.Fatalf("second CompleteRun: calls %d logical %d err %v", calls, again.Totals.Logical, err)
	}
}

// crashLease forwards to a real lease until crashed, then drops everything,
// the way a killed process would.
type crashLease struct {
	runner.Lease
	crashed atomic.Bool
}

var errCrashed = errors.New("agent is dead")

func (c *crashLease) Sync(ctx context.Context, want int, ch *report.Report) (int, error) {
	if c.crashed.Load() {
		return 0, errCrashed
	}
	return c.Lease.Sync(ctx, want, ch)
}
func (c *crashLease) Complete(ctx context.Context, ch *report.Report) error {
	if c.crashed.Load() {
		return errCrashed
	}
	return c.Lease.Complete(ctx, ch)
}
func (c *crashLease) Release(ctx context.Context, handled int, ch *report.Report) error {
	if c.crashed.Load() {
		return errCrashed
	}
	return c.Lease.Release(ctx, handled, ch)
}

// An agent dies mid-shard; another takes over after the lease expires. No
// arrival may be sent twice, and every planned arrival must be accounted
// for as sent, skipped, unaccounted or never sent.
func TestKilledAgentShardIsRecovered(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()

	var mu sync.Mutex
	seen := map[string]int{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen[r.Header.Get("X-BF-Logical-Id")]++
		mu.Unlock()
	}))
	defer srv.Close()

	sc := testScenario(srv.URL, 60, 3*time.Second) // one arrival every 50ms
	sc.Target.AllowHosts = []string{"127.0.0.1"}
	run := newRun(t, s, sc, 1, 500*time.Millisecond, 200*time.Millisecond)

	a, err := s.Claim(ctx, "a", run, 0, 0)
	if err != nil || a == nil {
		t.Fatalf("claim a: %v", err)
	}
	cl := &crashLease{Lease: a.Lease}
	a.Lease = nil
	actx, kill := context.WithCancel(ctx)
	var aErr error
	done := make(chan struct{})
	go func() {
		defer close(done)
		c := *a
		_, aErr = runner.RunShard(actx, c.Run.Scenario, runner.Options{
			Seed: c.Run.Seed, RunID: c.Run.ID, Shard: 1, ShardCount: 1, StartAt: c.Run.StartAt,
			Agent: "a", Lease: cl, Heartbeat: 100 * time.Millisecond, LeaseTTL: c.Run.LeaseTTL,
		})
	}()
	time.Sleep(1200 * time.Millisecond) // ~1s into the schedule
	cl.crashed.Store(true)
	kill()
	// A real killed process just vanishes; here a's goroutine keeps retrying
	// its handover against the dead lease, so don't wait for it.
	defer func() { <-done; _ = aErr }()

	// b polls until a's lease has expired, like a spare agent would.
	var b *Claim
	for deadline := time.Now().Add(5 * time.Second); b == nil && time.Now().Before(deadline); {
		if b, err = s.Claim(ctx, "b", run, 0, 0); err != nil {
			t.Fatal(err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if b == nil || b.Resume == 0 {
		t.Fatalf("takeover claim %+v", b)
	}
	if _, err := RunClaim(ctx, b, "b", 0, 0, t.Logf); err != nil {
		t.Fatal(err)
	}

	if _, err := s.BeginFinalize(ctx, run); err != nil {
		t.Fatal(err)
	}
	rep, err := s.CompleteRun(ctx, run, func(r *report.Report) error { r.Finalize(); return nil })
	if err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("logical id %s sent %d times", id, n)
		}
	}
	rc := rep.Recovery
	total := rep.Totals.Logical + rc.Unaccounted + rc.SkippedOverdue + rc.NotSent
	if total != 60 || rc.Takeovers != 1 || rc.NotSent != 0 {
		t.Fatalf("logical %d + unaccounted %d + skipped %d + never sent %d = %d, want 60; takeovers %d",
			rep.Totals.Logical, rc.Unaccounted, rc.SkippedOverdue, rc.NotSent, total, rc.Takeovers)
	}
	if rc.SkippedOverdue != 0 {
		t.Fatalf("takeover within the lease TTL should not skip anything; skipped %d", rc.SkippedOverdue)
	}
	if len(seen) < rep.Totals.Logical || len(seen) > rep.Totals.Logical+rc.Unaccounted {
		t.Fatalf("target saw %d ids; report says %d done and %d unaccounted", len(seen), rep.Totals.Logical, rc.Unaccounted)
	}
	t.Logf("resumed at %d; logical %d, unaccounted %d, skipped %d; events %v",
		b.Resume, rep.Totals.Logical, rc.Unaccounted, rc.SkippedOverdue, rc.Events)
}
