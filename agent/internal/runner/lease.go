package runner

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"burstforge/internal/report"
)

// ErrFenced means the agent no longer holds the lease on its shard: it
// expired, was released, or a newer epoch took over. The agent must stop
// sending new arrivals. Telemetry it already has is still worth storing.
var ErrFenced = errors.New("lease lost: a newer epoch owns this shard")

// Lease ties one shard's execution to durable run state (see internal/store).
//
// The runner only sends arrivals the lease has reserved for it, so a shard
// taken over by another agent resumes after the last reserved arrival and no
// arrival is sent twice. Chunks are telemetry deltas; the implementation
// must store them at least once and idempotently, and must keep storing them
// after the lease is lost, because they describe traffic that really went out.
type Lease interface {
	// Sync renews the lease, stores chunk (nil if nothing new happened), and
	// asks for the first want arrivals of the shard to be reserved. It
	// returns how many are reserved.
	Sync(ctx context.Context, want int, chunk *report.Report) (reserved int, err error)
	// Complete stores the last chunk and marks the shard done.
	Complete(ctx context.Context, chunk *report.Report) error
	// Release stores the last chunk and gives the lease up early, so another
	// agent can take over without waiting for it to expire. handled is how
	// many shard arrivals this agent sent or skipped; reserved arrivals past
	// it were never sent and go back to the shard.
	Release(ctx context.Context, handled int, chunk *report.Report) error
}

// gate holds the dispatcher back until arrivals are reserved.
type gate struct {
	mu       sync.Mutex
	reserved int
	fenced   bool
	changed  chan struct{}
}

func newGate(reserved int) *gate {
	return &gate{reserved: reserved, changed: make(chan struct{})}
}

func (g *gate) update(reserved int, fenced bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if reserved > g.reserved {
		g.reserved = reserved
	}
	g.fenced = g.fenced || fenced
	close(g.changed)
	g.changed = make(chan struct{})
}

func (g *gate) isFenced() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.fenced
}

// wait blocks until arrival k (0-based within the shard) is reserved. It
// returns false if the lease is lost or ctx ends first.
func (g *gate) wait(ctx context.Context, k int) bool {
	for {
		g.mu.Lock()
		ok, fenced, ch := k < g.reserved, g.fenced, g.changed
		g.mu.Unlock()
		switch {
		case fenced:
			return false
		case ok:
			return true
		}
		select {
		case <-ch:
		case <-ctx.Done():
			return false
		}
	}
}

// leaseLoop heartbeats the lease, extends the reservation ahead of the
// schedule, and ships telemetry chunks.
type leaseLoop struct {
	e      *engine
	lease  Lease
	gate   *gate
	dueAt  []time.Time // planned time of each shard arrival, ascending
	ahead  time.Duration
	lastOK time.Time
	sent   []*report.Report
	stop   chan struct{}
	done   chan struct{}
}

func startLeaseLoop(e *engine, due []time.Time) *leaseLoop {
	l := &leaseLoop{
		e:      e,
		lease:  e.opt.Lease,
		gate:   newGate(e.opt.Resume),
		dueAt:  due,
		ahead:  2 * e.opt.Heartbeat,
		lastOK: time.Now(),
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
	}
	go l.run()
	return l
}

// want is how many shard arrivals are due before now + ahead. Reserving a
// little ahead keeps the dispatcher from stalling between heartbeats, and
// bounds how many arrivals are lost if this agent dies.
func (l *leaseLoop) want() int {
	horizon := time.Now().Add(l.ahead)
	n := sort.Search(len(l.dueAt), func(k int) bool { return l.dueAt[k].After(horizon) })
	return max(n, l.e.opt.Resume)
}

func (l *leaseLoop) run() {
	defer close(l.done)
	t := time.NewTicker(l.e.opt.Heartbeat)
	defer t.Stop()
	warned := false
	for {
		l.sync(&warned)
		select {
		case <-t.C:
		case <-l.stop:
			return
		}
	}
}

func (l *leaseLoop) sync(warned *bool) {
	ctx, cancel := context.WithTimeout(context.Background(), l.e.opt.Heartbeat)
	defer cancel()
	chunk := l.e.col.cut()
	if chunk != nil {
		l.sent = append(l.sent, chunk) // the lease queues it until stored
	}
	want := 0
	if !l.gate.isFenced() {
		want = l.want()
	}
	reserved, err := l.lease.Sync(ctx, want, chunk)
	switch {
	case err == nil:
		l.lastOK = time.Now()
		l.gate.update(reserved, false)
	case errors.Is(err, ErrFenced):
		if !*warned {
			l.e.opt.Logf("%s%v; no new arrivals will be sent", l.e.who(), err)
			*warned = true
		}
		l.gate.update(0, true)
	default:
		l.e.opt.Logf("%sheartbeat failed: %v", l.e.who(), err)
		// Without a successful heartbeat the lease may already belong to
		// someone else. Stop before it can have expired.
		if time.Since(l.lastOK) > l.e.opt.LeaseTTL-l.e.opt.Heartbeat && !l.gate.isFenced() {
			l.e.opt.Logf("%sno successful heartbeat for %s; assuming the lease is lost", l.e.who(), time.Since(l.lastOK).Round(time.Millisecond))
			l.gate.update(0, true)
		}
	}
}

// finish stops heartbeating and hands over the last chunk: Complete if the
// shard ran to the end, Release if this agent was interrupted, or a plain
// flush if the lease was already lost.
func (l *leaseLoop) finish(aborted bool, handled int) error {
	close(l.stop)
	<-l.done
	chunk := l.e.col.cut()
	if chunk != nil {
		l.sent = append(l.sent, chunk)
	}
	var err error
	for attempt := 0; attempt < 5; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		switch {
		case l.gate.isFenced():
			// Store what this agent observed, then report that it no
			// longer owns the shard.
			if _, err = l.lease.Sync(ctx, 0, chunk); err == nil {
				err = ErrFenced
			}
		case aborted:
			err = l.lease.Release(ctx, handled, chunk)
		default:
			err = l.lease.Complete(ctx, chunk)
		}
		cancel()
		if err == nil || errors.Is(err, ErrFenced) {
			return err
		}
		l.e.opt.Logf("%shanding over the last chunk failed (attempt %d): %v", l.e.who(), attempt+1, err)
		chunk = nil // the lease kept it queued
		time.Sleep(time.Second)
	}
	return err
}
