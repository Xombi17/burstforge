package runner

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"burstforge/internal/faultproxy"
	"burstforge/internal/scenario"
)

// A response lost after the target handled the request makes the client
// retry, so the target does the work twice. The run must show that, and
// hooks and invariants must reach the target without faults.
func TestFaultProxyResetAfterCausesRepeatWork(t *testing.T) {
	var mu sync.Mutex
	seen := map[string]int{}
	var hooks, invChecks int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch r.URL.Path {
		case "/hook":
			hooks++
		case "/inv":
			invChecks++
			w.Write([]byte(`{"bad": 0}`))
		default:
			seen[r.Header.Get("X-BF-Logical-Id")]++
		}
	}))
	defer srv.Close()

	sc := baseScenario(srv.URL)
	sc.Traffic.Arrivals = 40
	sc.Workflow.Steps[0].Retry.MaxAttempts = 3
	// An idempotency key and a body make Go's transport consider the request
	// replayable; it must still not resend it behind the runner's back.
	sc.Workflow.Steps[0].Headers = map[string]string{"Idempotency-Key": "{{logical_id}}"}
	sc.Workflow.Steps[0].Body = map[string]any{"n": "{{n}}"}
	sc.Hooks.Before = []scenario.Call{{Method: "POST", Path: "/hook"}}
	sc.Invariants = []scenario.Invariant{{Name: "ok", Path: "/inv", JSON: "$.bad", Op: "eq", Value: 0}}
	sc.Faults = scenario.Faults{Rules: []faultproxy.Rule{
		{Reset: true, Phase: "after", Rate: 0.5}, // every path, including /hook and /inv if they went through it
	}}

	rep, err := Run(context.Background(), sc, Options{Seed: 5, RunID: "f"})
	if err != nil {
		t.Fatal(err)
	}
	f := rep.Faults
	if f == nil || f.Proxy != "embedded" || len(f.Rules) != 1 {
		t.Fatalf("fault report %+v", f)
	}
	mu.Lock()
	defer mu.Unlock()
	if hooks != 1 || invChecks == 0 || !rep.Invariants[0].Pass {
		t.Fatalf("hooks %d, invariant checks %d, invariant %+v", hooks, invChecks, rep.Invariants[0])
	}
	repeats := 0
	for _, n := range seen {
		repeats += n - 1
	}
	// Every injected reset after the target handled the request is one
	// extra time the target saw it (the retry), except where retries ran out.
	if f.Rules[0].Injected == 0 || repeats == 0 || int64(repeats) > f.Rules[0].Injected {
		t.Fatalf("injected %d, target repeats %d", f.Rules[0].Injected, repeats)
	}
	if f.Requests != int64(rep.Attempts.Total) {
		t.Fatalf("proxy saw %d requests, runner counted %d attempts: some were sent without being counted", f.Requests, rep.Attempts.Total)
	}
	if rep.Attempts.ByClass["conn_error"] != int(f.Rules[0].Injected) {
		t.Fatalf("client saw %d conn errors, proxy injected %d", rep.Attempts.ByClass["conn_error"], f.Rules[0].Injected)
	}
	if !strings.Contains(rep.Text(), "faults") || !strings.Contains(rep.Markdown(), "## Injected faults") {
		t.Fatal("fault section missing from rendering")
	}
}

func TestSameSeedSameFaults(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer srv.Close()
	sc := baseScenario(srv.URL)
	sc.Traffic.Arrivals = 60
	sc.Faults = scenario.Faults{Rules: []faultproxy.Rule{{Status: 503, Rate: 0.3}}}
	var got []int
	for i := 0; i < 2; i++ {
		// Different run IDs, as two CLI runs would have.
		rep, err := Run(context.Background(), sc, Options{Seed: 9, RunID: fmt.Sprintf("run-%d", i)})
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, rep.Attempts.ByClass["http_5xx"])
	}
	if got[0] != got[1] || got[0] == 0 {
		t.Fatalf("5xx counts %v; want equal and non-zero", got)
	}
}

func TestTrafficURLMustBeAllowed(t *testing.T) {
	sc := baseScenario("http://127.0.0.1:1")
	_, err := RunShard(context.Background(), sc, Options{Seed: 1, RunID: "r", Shard: 1, ShardCount: 1,
		TrafficURL: "http://example.com:8081", StartAt: time.Now()})
	if err == nil || !strings.Contains(err.Error(), "traffic URL") {
		t.Fatalf("err = %v", err)
	}
}

// Bodiless requests can still be resent by the transport; those resends
// must show up as attempts so the proxy and the runner agree.
func TestHiddenResendsOfBodilessRequestsAreCounted(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer srv.Close()
	sc := baseScenario(srv.URL)
	sc.Traffic.Arrivals = 60
	sc.Workflow.Steps[0].Method = "GET"
	sc.Workflow.Steps[0].Retry.MaxAttempts = 2
	sc.Faults = scenario.Faults{Rules: []faultproxy.Rule{{Reset: true, Phase: "after", Rate: 0.5}}}
	rep, err := Run(context.Background(), sc, Options{Seed: 3, RunID: "g"})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Faults.Requests != int64(rep.Attempts.Total) {
		t.Fatalf("proxy saw %d requests, runner counted %d (by kind %v)", rep.Faults.Requests, rep.Attempts.Total, rep.Attempts.ByKind)
	}
	t.Logf("attempts by kind %v", rep.Attempts.ByKind)
}
