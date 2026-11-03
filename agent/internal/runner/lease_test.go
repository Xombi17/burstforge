package runner

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"burstforge/internal/report"
)

// memLease is an in-memory Lease. It grants at most cap arrivals and can be
// fenced from the test.
type memLease struct {
	mu        sync.Mutex
	cap       int
	reserved  int
	fenced    bool
	chunks    []*report.Report
	completed bool
	released  bool
}

func (m *memLease) Sync(_ context.Context, want int, chunk *report.Report) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if chunk != nil {
		m.chunks = append(m.chunks, chunk)
	}
	if m.fenced {
		return 0, ErrFenced
	}
	if want > m.reserved {
		m.reserved = min(want, m.cap)
	}
	return m.reserved, nil
}

func (m *memLease) Complete(ctx context.Context, chunk *report.Report) error {
	if _, err := m.Sync(ctx, 0, chunk); err != nil {
		return err
	}
	m.mu.Lock()
	m.completed = true
	m.mu.Unlock()
	return nil
}

func (m *memLease) Release(ctx context.Context, handled int, chunk *report.Report) error {
	_, err := m.Sync(ctx, 0, chunk)
	m.mu.Lock()
	m.released = true
	m.mu.Unlock()
	return err
}

func (m *memLease) total() (logical, skipped int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, c := range m.chunks {
		logical += c.Totals.Logical
		skipped += c.Totals.SkippedOverdue
	}
	return
}

func countingServer(n *int32) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(n, 1)
	}))
}

func leaseOpts(l Lease) Options {
	return Options{Seed: 1, RunID: "r", Shard: 1, ShardCount: 1, Lease: l,
		Heartbeat: 20 * time.Millisecond, LeaseTTL: 200 * time.Millisecond, LateLimit: time.Second}
}

func TestLeaseCompletesAndChunksAddUp(t *testing.T) {
	var hits int32
	srv := countingServer(&hits)
	defer srv.Close()
	sc := baseScenario(srv.URL)
	sc.Traffic.Profile, sc.Traffic.Duration, sc.Traffic.Arrivals = "sustained", 200*time.Millisecond, 20

	l := &memLease{cap: 1 << 30}
	rep, err := RunShard(context.Background(), sc, leaseOpts(l))
	if err != nil {
		t.Fatal(err)
	}
	logical, _ := l.total()
	if !l.completed || logical != 20 || hits != 20 || rep.Totals.Logical != 20 {
		t.Fatalf("completed %v, chunk logical %d, hits %d, report logical %d", l.completed, logical, hits, rep.Totals.Logical)
	}
	if len(l.chunks) < 2 {
		t.Fatalf("expected several chunks over a 200ms run, got %d", len(l.chunks))
	}
}

func TestNothingIsSentBeyondTheReservation(t *testing.T) {
	var hits int32
	srv := countingServer(&hits)
	defer srv.Close()
	sc := baseScenario(srv.URL)

	// The lease only ever grants 7 arrivals; then the test fences it.
	l := &memLease{cap: 7}
	go func() {
		time.Sleep(300 * time.Millisecond)
		l.mu.Lock()
		l.fenced = true
		l.mu.Unlock()
	}()
	if _, err := RunShard(context.Background(), sc, leaseOpts(l)); err != ErrFenced {
		t.Fatalf("err = %v, want ErrFenced", err)
	}
	if hits != 7 || l.completed {
		t.Fatalf("hits %d (want 7), completed %v", hits, l.completed)
	}
	logical, _ := l.total()
	if logical != 7 {
		t.Fatalf("chunks recorded %d workflows after fencing, want 7", logical)
	}
}

func TestResumeSkipsReservedAndOverdueArrivals(t *testing.T) {
	var hits int32
	srv := countingServer(&hits)
	defer srv.Close()
	sc := baseScenario(srv.URL)
	sc.Traffic.Profile, sc.Traffic.Duration, sc.Traffic.Arrivals = "sustained", 2*time.Second, 20

	// A takeover 1.45s into a 2s run that resumes at arrival index 2.
	// Arrival k is planned at k*100ms, so at takeover it is 1.45-0.1k s late:
	// indexes 2..4 are more than 1s late and are skipped, 5..19 are sent,
	// and 0..1 belonged to the previous lease holder.
	opt := leaseOpts(&memLease{cap: 1 << 30})
	opt.Resume = 2
	opt.StartAt = time.Now().Add(-1450 * time.Millisecond)
	l := opt.Lease.(*memLease)
	if _, err := RunShard(context.Background(), sc, opt); err != nil {
		t.Fatal(err)
	}
	logical, skipped := l.total()
	if hits != 15 || logical != 15 || skipped != 3 {
		t.Fatalf("hits %d, logical %d, skipped %d; want 15, 15, 3", hits, logical, skipped)
	}
}

func TestOverdueArrivalsAreSkipped(t *testing.T) {
	var hits int32
	srv := countingServer(&hits)
	defer srv.Close()
	sc := baseScenario(srv.URL)
	sc.Traffic.Profile, sc.Traffic.Duration, sc.Traffic.Arrivals = "sustained", time.Second, 10

	opt := leaseOpts(&memLease{cap: 1 << 30})
	opt.StartAt = time.Now().Add(-3 * time.Second) // every arrival is >= 2s late
	l := opt.Lease.(*memLease)
	if _, err := RunShard(context.Background(), sc, opt); err != nil {
		t.Fatal(err)
	}
	if _, skipped := l.total(); skipped != 10 || hits != 0 {
		t.Fatalf("skipped %d, hits %d; want all 10 skipped", skipped, hits)
	}
}

func TestInterruptedAgentReleasesItsLease(t *testing.T) {
	var hits int32
	srv := countingServer(&hits)
	defer srv.Close()
	sc := baseScenario(srv.URL)
	sc.Traffic.Profile, sc.Traffic.Duration, sc.Traffic.Arrivals = "sustained", 2*time.Second, 20

	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	l := &memLease{cap: 1 << 30}
	rep, err := RunShard(ctx, sc, leaseOpts(l))
	if err != nil {
		t.Fatal(err)
	}
	if !l.released || l.completed || rep.Totals.Aborted {
		t.Fatalf("released %v completed %v aborted %v", l.released, l.completed, rep.Totals.Aborted)
	}
}
