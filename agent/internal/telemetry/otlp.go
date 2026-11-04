package telemetry

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"burstforge/internal/runner"
)

// OTLP exports spans to an OpenTelemetry collector over OTLP/HTTP with the
// JSON encoding: one span per workflow, with one child span per HTTP
// attempt. The attempt span's ID is the one sent in that request's
// traceparent header, so the target's server spans nest under it.
//
// Export never blocks the load: spans go into a bounded buffer and are
// dropped, and counted, when it is full or the collector fails.
type OTLP struct {
	url      string
	resource []kv
	client   *http.Client
	spans    chan span
	done     chan struct{}
	exported atomic.Uint64
	dropped  atomic.Uint64
	closing  sync.Once
}

// OTLPFromEnv reads the standard OpenTelemetry variables:
// OTEL_EXPORTER_OTLP_TRACES_ENDPOINT (used as is) or
// OTEL_EXPORTER_OTLP_ENDPOINT (+ /v1/traces), OTEL_EXPORTER_OTLP_HEADERS
// and OTEL_SERVICE_NAME. It returns nil if no endpoint is set.
func OTLPFromEnv(agent string) (*OTLP, error) {
	url := os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
	if url == "" {
		if base := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"); base != "" {
			url = strings.TrimRight(base, "/") + "/v1/traces"
		}
	}
	if url == "" {
		return nil, nil
	}
	if !strings.HasPrefix(url, "http://") && !strings.HasPrefix(url, "https://") {
		return nil, fmt.Errorf("OTLP endpoint %q must be an http(s) URL (OTLP/HTTP; gRPC isn't supported)", url)
	}
	service := os.Getenv("OTEL_SERVICE_NAME")
	if service == "" {
		service = "burstforge-agent"
	}
	headers := map[string]string{}
	for _, h := range strings.Split(os.Getenv("OTEL_EXPORTER_OTLP_HEADERS"), ",") {
		if k, v, ok := strings.Cut(h, "="); ok {
			headers[strings.TrimSpace(k)] = strings.TrimSpace(v)
		}
	}
	return NewOTLP(url, service, agent, headers), nil
}

func NewOTLP(url, service, agent string, headers map[string]string) *OTLP {
	o := &OTLP{
		url:      url,
		resource: []kv{str("service.name", service), str("service.version", runner.Version), str("burstforge.agent", agent)},
		client:   &http.Client{Timeout: 10 * time.Second, Transport: headerTransport(headers)},
		spans:    make(chan span, 8192),
		done:     make(chan struct{}),
	}
	go o.loop()
	return o
}

// Stats reports spans exported and dropped so far.
func (o *OTLP) Stats() (exported, dropped uint64) { return o.exported.Load(), o.dropped.Load() }

// Close flushes what is buffered, waiting at most until ctx ends.
func (o *OTLP) Close(ctx context.Context) {
	o.closing.Do(func() { close(o.spans) })
	select {
	case <-o.done:
	case <-ctx.Done():
	}
}

// ForRun returns the observer for one shard of one run.
func (o *OTLP) ForRun(run, org string, shard int) runner.Observer {
	attrs := []kv{str("burstforge.run_id", run), integer("burstforge.shard", int64(shard))}
	if org != "" {
		attrs = append(attrs, str("burstforge.org", org))
	}
	return &otlpRun{o: o, attrs: attrs}
}

type otlpRun struct {
	o     *OTLP
	attrs []kv
}

func (r *otlpRun) Attempt(a runner.AttemptEvent) {
	if a.Duration < 0 {
		return // a transport resend: counted in metrics, no timing for a span
	}
	attrs := append(append([]kv{}, r.attrs...),
		str("http.request.method", a.Method), str("url.full", a.URL),
		integer("burstforge.arrival", int64(a.Arrival)), str("burstforge.step", a.Step),
		integer("burstforge.attempt", int64(a.N)), str("burstforge.attempt.kind", a.Kind), str("burstforge.outcome", a.Class))
	if a.Status > 0 {
		attrs = append(attrs, integer("http.response.status_code", int64(a.Status)))
	}
	s := span{
		TraceID: a.Trace.String(), SpanID: a.Span.String(), ParentSpanID: a.Parent.String(),
		Name: a.Step + " " + a.Kind, Kind: 3, // client
		Start: nanos(a.Start), End: nanos(a.Start.Add(a.Duration)), Attributes: attrs,
	}
	if a.Class != "ok" {
		s.Status = &status{Code: 2, Message: a.Class}
	}
	r.o.enqueue(s)
}

func (r *otlpRun) Workflow(w runner.WorkflowEvent) {
	attrs := append(append([]kv{}, r.attrs...), integer("burstforge.arrival", int64(w.Arrival)), str("burstforge.outcome", w.Outcome))
	s := span{
		TraceID: w.Trace.String(), SpanID: w.Span.String(), Name: "workflow", Kind: 1, // internal
		Start: nanos(w.Start), End: nanos(w.Start.Add(w.Duration)), Attributes: attrs,
	}
	if !w.OK {
		s.Status = &status{Code: 2, Message: w.Outcome}
	}
	r.o.enqueue(s)
}

func (o *OTLP) enqueue(s span) {
	defer func() {
		if recover() != nil { // closed: the agent is shutting down
			o.dropped.Add(1)
		}
	}()
	select {
	case o.spans <- s:
	default:
		o.dropped.Add(1)
	}
}

func (o *OTLP) loop() {
	defer close(o.done)
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	batch := make([]span, 0, 512)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		if err := o.post(batch); err != nil {
			o.dropped.Add(uint64(len(batch)))
		} else {
			o.exported.Add(uint64(len(batch)))
		}
		batch = batch[:0]
	}
	for {
		select {
		case s, ok := <-o.spans:
			if !ok {
				flush()
				return
			}
			batch = append(batch, s)
			if len(batch) == cap(batch) {
				flush()
			}
		case <-tick.C:
			flush()
		}
	}
}

func (o *OTLP) post(batch []span) error {
	body, err := json.Marshal(map[string]any{
		"resourceSpans": []any{map[string]any{
			"resource": map[string]any{"attributes": o.resource},
			"scopeSpans": []any{map[string]any{
				"scope": map[string]any{"name": "burstforge", "version": runner.Version},
				"spans": batch,
			}},
		}},
	})
	if err != nil {
		return err
	}
	resp, err := o.client.Post(o.url, "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("collector returned HTTP %d", resp.StatusCode)
	}
	return nil
}

// OTLP/JSON: IDs are hex, 64-bit integers are decimal strings.
type span struct {
	TraceID      string  `json:"traceId"`
	SpanID       string  `json:"spanId"`
	ParentSpanID string  `json:"parentSpanId,omitempty"`
	Name         string  `json:"name"`
	Kind         int     `json:"kind"`
	Start        string  `json:"startTimeUnixNano"`
	End          string  `json:"endTimeUnixNano"`
	Attributes   []kv    `json:"attributes"`
	Status       *status `json:"status,omitempty"`
}

type status struct {
	Code    int    `json:"code"`
	Message string `json:"message,omitempty"`
}

type kv struct {
	Key   string         `json:"key"`
	Value map[string]any `json:"value"`
}

func str(k, v string) kv { return kv{k, map[string]any{"stringValue": v}} }
func integer(k string, v int64) kv {
	return kv{k, map[string]any{"intValue": strconv.FormatInt(v, 10)}}
}
func nanos(t time.Time) string { return strconv.FormatInt(t.UnixNano(), 10) }

type headerRT map[string]string

func (h headerRT) RoundTrip(r *http.Request) (*http.Response, error) {
	for k, v := range h {
		r.Header.Set(k, v)
	}
	return http.DefaultTransport.RoundTrip(r)
}

func headerTransport(h map[string]string) http.RoundTripper {
	if len(h) == 0 {
		return http.DefaultTransport
	}
	return headerRT(h)
}
