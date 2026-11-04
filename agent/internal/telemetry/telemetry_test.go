package telemetry

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"burstforge/internal/runner"
	"burstforge/internal/scenario"
)

func testScenario(url string, arrivals int) *scenario.Scenario {
	return &scenario.Scenario{
		Name:   "telemetry",
		Target: scenario.Target{BaseURL: url, Environment: "local", AllowHosts: []string{"127.0.0.1"}},
		Workflow: scenario.Workflow{Steps: []scenario.Step{{
			Name: "act", Method: "POST", Path: "/act", Timeout: 2 * time.Second,
			Retry: scenario.Retry{MaxAttempts: 2, Backoff: "none", Jitter: "none", On: []string{"5xx"}},
		}}},
		Traffic: scenario.Traffic{
			Profile: "burst", Arrivals: arrivals, Window: 50 * time.Millisecond,
			StartDelay: 20 * time.Millisecond, MaxInflight: 100, DoubleSubmitDelay: 5 * time.Millisecond,
		},
	}
}

type collected struct {
	TraceID, SpanID, ParentSpanID, Name string
	Status                              *struct{ Code int }
}

// A collector that decodes OTLP/JSON trace exports.
func fakeCollector(t *testing.T) (*httptest.Server, func() []collected) {
	var mu sync.Mutex
	var spans []collected
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/traces" || r.Header.Get("Content-Type") != "application/json" || r.Header.Get("X-Token") != "t" {
			w.WriteHeader(400)
			return
		}
		var req struct {
			ResourceSpans []struct {
				ScopeSpans []struct{ Spans []collected }
			}
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("bad OTLP JSON: %v", err)
		}
		mu.Lock()
		for _, rs := range req.ResourceSpans {
			for _, ss := range rs.ScopeSpans {
				spans = append(spans, ss.Spans...)
			}
		}
		mu.Unlock()
	}))
	t.Cleanup(srv.Close)
	return srv, func() []collected { mu.Lock(); defer mu.Unlock(); return append([]collected(nil), spans...) }
}

func TestTraceparentLinksTargetRequestsToExportedSpans(t *testing.T) {
	var mu sync.Mutex
	headers := map[string]string{} // traceparent -> arrival
	tries := map[string]int{}
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		headers[r.Header.Get("Traceparent")] = r.Header.Get("X-BF-Arrival")
		tries[r.Header.Get("X-BF-Arrival")]++
		if tries[r.Header.Get("X-BF-Arrival")] == 1 && r.Header.Get("X-BF-Arrival") == "3" {
			w.WriteHeader(503) // one retry, so one workflow has two attempt spans
		}
	}))
	defer target.Close()
	coll, spans := fakeCollector(t)

	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", coll.URL)
	t.Setenv("OTEL_EXPORTER_OTLP_HEADERS", "X-Token=t")
	exp, err := OTLPFromEnv("agent-1")
	if err != nil || exp == nil {
		t.Fatalf("exporter: %v %v", exp, err)
	}
	prom := NewPrometheus()
	_, err = runner.Run(context.Background(), testScenario(target.URL, 10), runner.Options{
		Seed: 1, RunID: "run-x", Observer: runner.Observers{exp.ForRun("run-x", "acme", 1), prom.ForRun("run-x", "acme", 1)},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	exp.Close(ctx)
	if ex, dr := exp.Stats(); ex != 21 || dr != 0 {
		t.Fatalf("exported %d dropped %d, want 21 (10 workflows + 11 attempts) and 0", ex, dr)
	}

	got := spans()
	bySpan := map[string]collected{}
	for _, s := range got {
		bySpan[s.SpanID] = s
	}
	if len(headers) != 11 {
		t.Fatalf("target saw %d distinct traceparents, want 11", len(headers))
	}
	for tp, arrival := range headers {
		var n int
		fmt.Sscan(arrival, &n)
		parts := strings.Split(tp, "-")
		if len(parts) != 4 || parts[0] != "00" || parts[3] != "01" {
			t.Fatalf("malformed traceparent %q", tp)
		}
		if want := runner.WorkflowTraceID("run-x", n).String(); parts[1] != want {
			t.Fatalf("arrival %d: trace %s, want %s", n, parts[1], want)
		}
		s, ok := bySpan[parts[2]]
		if !ok {
			t.Fatalf("no exported span for the request's span id %s", parts[2])
		}
		root, ok := bySpan[s.ParentSpanID]
		if !ok || root.Name != "workflow" || root.TraceID != parts[1] {
			t.Fatalf("attempt span %+v has no workflow parent", s)
		}
	}
	failed := 0
	for _, s := range got {
		if s.Status != nil && s.Status.Code == 2 {
			failed++
		}
	}
	if failed != 1 {
		t.Fatalf("%d spans marked as errors, want 1 (the 503)", failed)
	}

	rec := httptest.NewRecorder()
	prom.ServeHTTP(rec, nil)
	body, _ := io.ReadAll(rec.Body)
	for _, want := range []string{
		`burstforge_agent_attempts_total{run="run-x",org="acme",shard="1",step="act",kind="first",class="ok"} 9`,
		`burstforge_agent_attempts_total{run="run-x",org="acme",shard="1",step="act",kind="first",class="http_5xx"} 1`,
		`burstforge_agent_attempts_total{run="run-x",org="acme",shard="1",step="act",kind="retry",class="ok"} 1`,
		`burstforge_agent_workflows_total{run="run-x",org="acme",shard="1",result="ok"} 10`,
		`burstforge_agent_attempt_duration_seconds_count{run="run-x",org="acme",shard="1",step="act"} 11`,
		`burstforge_agent_workflow_duration_seconds_bucket{run="run-x",org="acme",shard="1",le="+Inf"} 10`,
	} {
		if !strings.Contains(string(body), want) {
			t.Errorf("missing %s in:\n%s", want, body)
		}
	}
}

func TestExportNeverBlocksAndCountsDrops(t *testing.T) {
	block := make(chan struct{})
	coll := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-block }))
	defer coll.Close()
	defer close(block)
	exp := NewOTLP(coll.URL+"/v1/traces", "s", "a", nil)
	obs := exp.ForRun("r", "", 1)
	start := time.Now()
	for i := 0; i < 20000; i++ {
		obs.Workflow(runner.WorkflowEvent{Arrival: i, Start: time.Now()})
	}
	if time.Since(start) > time.Second {
		t.Fatalf("enqueueing 20000 spans took %s; export must not block the load", time.Since(start))
	}
	if _, dropped := exp.Stats(); dropped == 0 {
		t.Fatal("a full buffer must drop and count spans")
	}
}

func TestPrometheusDropsQuietRuns(t *testing.T) {
	p := NewPrometheus()
	now := time.Now()
	p.now = func() time.Time { return now }
	p.ForRun("old", "", 1).Workflow(runner.WorkflowEvent{OK: true})
	now = now.Add(20 * time.Minute)
	p.ForRun("new", "", 1).Workflow(runner.WorkflowEvent{OK: true})
	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, nil)
	if s := rec.Body.String(); strings.Contains(s, `run="old"`) || !strings.Contains(s, `run="new"`) {
		t.Fatalf("pruning:\n%s", s)
	}
}
