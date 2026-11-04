package runner

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math/rand/v2"
	"time"
)

// Every attempt carries a W3C traceparent header. The trace is the logical
// workflow: its ID is derived from the run ID and the arrival, so the same
// workflow can be found in the target's own traces. Each attempt is a span
// under the workflow's root span.

type TraceID [16]byte
type SpanID [8]byte

func (t TraceID) String() string { return hex.EncodeToString(t[:]) }
func (s SpanID) String() string  { return hex.EncodeToString(s[:]) }

// WorkflowTraceID is the trace ID of arrival i (0-based) of a run.
func WorkflowTraceID(runID string, arrival int) TraceID {
	h := sha256.Sum256([]byte(fmt.Sprintf("burstforge/%s/%d", runID, arrival)))
	var t TraceID
	copy(t[:], h[:16])
	return t
}

func newSpanID() SpanID {
	var s SpanID
	binary.LittleEndian.PutUint64(s[:], rand.Uint64()|1) // never all zeros
	return s
}

func traceparent(t TraceID, s SpanID) string { return "00-" + t.String() + "-" + s.String() + "-01" }

type traceKey struct{}

type traceCtx struct {
	trace TraceID
	root  SpanID
}

func withTrace(ctx context.Context, tc traceCtx) context.Context {
	return context.WithValue(ctx, traceKey{}, tc)
}

func traceFrom(ctx context.Context) (traceCtx, bool) {
	tc, ok := ctx.Value(traceKey{}).(traceCtx)
	return tc, ok
}

// Observer sees attempts and workflows as they happen, for live metrics and
// trace export. Calls come from many goroutines and must not block.
type Observer interface {
	Attempt(AttemptEvent)
	Workflow(WorkflowEvent)
}

type AttemptEvent struct {
	Arrival  int
	Step     string
	Method   string
	URL      string
	N        int    // attempt number within the step
	Kind     string // first, retry, duplicate, transport_retry
	Class    string // ok, timeout, conn_error, http_429, http_5xx, http_4xx, canceled
	Status   int    // HTTP status, 0 if there was no response
	Start    time.Time
	Duration time.Duration // negative when not measured (a transport resend)
	Trace    TraceID
	Span     SpanID
	Parent   SpanID
}

type WorkflowEvent struct {
	Arrival  int
	OK       bool
	Outcome  string
	Start    time.Time
	Duration time.Duration
	Trace    TraceID
	Span     SpanID
}

// Observers fans events out to several observers.
type Observers []Observer

func (o Observers) Attempt(a AttemptEvent) {
	for _, x := range o {
		x.Attempt(a)
	}
}

func (o Observers) Workflow(w WorkflowEvent) {
	for _, x := range o {
		x.Workflow(w)
	}
}
