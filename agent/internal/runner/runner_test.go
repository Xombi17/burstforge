package runner

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"burstforge/internal/report"
	"burstforge/internal/scenario"
)

func ptr(f float64) *float64 { return &f }

func baseScenario(url string) *scenario.Scenario {
	return &scenario.Scenario{
		Name:   "test",
		Target: scenario.Target{BaseURL: url, Environment: "local", AllowHosts: []string{"127.0.0.1"}},
		Workflow: scenario.Workflow{Steps: []scenario.Step{{
			Name: "act", Method: "POST", Path: "/act", Timeout: 2 * time.Second,
			Retry: scenario.Retry{MaxAttempts: 1, Backoff: "none", Jitter: "none", On: []string{"timeout", "conn", "429", "5xx"}},
		}}},
		Traffic: scenario.Traffic{
			Profile: "burst", Arrivals: 20, Window: 50 * time.Millisecond,
			StartDelay: 20 * time.Millisecond, MaxInflight: 100, DoubleSubmitDelay: 5 * time.Millisecond,
		},
	}
}

type reportAlias = report.Report

func run(t *testing.T, sc *scenario.Scenario) (rep interface{ Text() string }, r *reportAlias) {
	t.Helper()
	out, err := Run(context.Background(), sc, Options{Seed: 1, RunID: "t"})
	if err != nil {
		t.Fatal(err)
	}
	return out, out
}

func TestRetriesAreCountedAndAmplificationMeasured(t *testing.T) {
	// Every user's first attempt gets a 503; the retry succeeds.
	var mu sync.Mutex
	seen := map[string]int{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-BF-Logical-Id")
		mu.Lock()
		seen[id]++
		n := seen[id]
		mu.Unlock()
		if n == 1 {
			w.WriteHeader(503)
			return
		}
		w.WriteHeader(200)
	}))
	defer srv.Close()

	sc := baseScenario(srv.URL)
	sc.Workflow.Steps[0].Retry.MaxAttempts = 3
	_, rep := run(t, sc)

	if rep.Totals.Succeeded != 20 || rep.Totals.Failed != 0 {
		t.Fatalf("totals: %+v", rep.Totals)
	}
	if rep.Attempts.Total != 40 || rep.Attempts.ByKind["first"] != 20 || rep.Attempts.ByKind["retry"] != 20 {
		t.Fatalf("attempts: %+v", rep.Attempts)
	}
	if rep.Attempts.RetryAmplification != 2.0 {
		t.Fatalf("amplification = %v, want 2.0", rep.Attempts.RetryAmplification)
	}
	if rep.Attempts.ByClass["http_5xx"] != 20 {
		t.Fatalf("classes: %+v", rep.Attempts.ByClass)
	}
}

func TestNoRetryWhenNotConfigured(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(500) }))
	defer srv.Close()
	_, rep := run(t, baseScenario(srv.URL))
	if rep.Totals.Failed != 20 || rep.Attempts.Total != 20 || rep.Attempts.RetryAmplification != 1.0 {
		t.Fatalf("got %+v %+v", rep.Totals, rep.Attempts)
	}
	if rep.Verdict.Pass {
		// no thresholds set, so this passes unless something else is wrong
		t.Log("verdict passes without thresholds, as designed")
	}
}

func TestFourOhFourIsNotRetried(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(404)
	}))
	defer srv.Close()
	sc := baseScenario(srv.URL)
	sc.Workflow.Steps[0].Retry.MaxAttempts = 3
	_, rep := run(t, sc)
	if hits.Load() != 20 || rep.Totals.Failed != 20 {
		t.Fatalf("hits=%d totals=%+v", hits.Load(), rep.Totals)
	}
}

func TestTimeoutsAreClassified(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(300 * time.Millisecond)
	}))
	defer srv.Close()
	sc := baseScenario(srv.URL)
	sc.Traffic.Arrivals = 5
	sc.Workflow.Steps[0].Timeout = 50 * time.Millisecond
	_, rep := run(t, sc)
	if rep.Attempts.ByClass["timeout"] != 5 || rep.Totals.FinalOutcomes["timeout"] != 5 {
		t.Fatalf("got %+v %+v", rep.Attempts, rep.Totals)
	}
}

func TestDoubleSubmitAddsDuplicateAttempts(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(200)
	}))
	defer srv.Close()
	sc := baseScenario(srv.URL)
	sc.Traffic.DoubleSubmit = 1.0
	_, rep := run(t, sc)
	if rep.Attempts.ByKind["duplicate"] != 20 || hits.Load() != 40 {
		t.Fatalf("by kind %+v, server hits %d", rep.Attempts.ByKind, hits.Load())
	}
	if rep.Attempts.RetryAmplification != 2.0 {
		t.Fatalf("amplification = %v", rep.Attempts.RetryAmplification)
	}
}

func TestCaptureFeedsLaterSteps(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/login":
			_ = json.NewEncoder(w).Encode(map[string]any{"token": "tok-" + r.Header.Get("X-BF-Logical-Id")})
		case "/act":
			want := "Bearer tok-" + r.Header.Get("X-BF-Logical-Id")
			if r.Header.Get("Authorization") != want {
				w.WriteHeader(401)
			}
		}
	}))
	defer srv.Close()
	sc := baseScenario(srv.URL)
	sc.Workflow.Steps = []scenario.Step{
		{Name: "login", Method: "POST", Path: "/login", Timeout: time.Second, Capture: map[string]string{"token": "$.token"},
			Retry: scenario.Retry{MaxAttempts: 1, Backoff: "none", Jitter: "none"}},
		{Name: "act", Method: "POST", Path: "/act", Timeout: time.Second,
			Headers: map[string]string{"Authorization": "Bearer {{token}}"},
			Retry:   scenario.Retry{MaxAttempts: 1, Backoff: "none", Jitter: "none"}},
	}
	_, rep := run(t, sc)
	if rep.Totals.Succeeded != 20 {
		t.Fatalf("totals %+v outcomes %+v", rep.Totals, rep.Totals.FinalOutcomes)
	}
}

func TestInvariantEventuallyPolls(t *testing.T) {
	start := time.Now()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/state" {
			pending := 1
			if time.Since(start) > 700*time.Millisecond {
				pending = 0
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"pending": pending})
		}
	}))
	defer srv.Close()
	sc := baseScenario(srv.URL)
	sc.Invariants = []scenario.Invariant{
		{Name: "drains", Path: "/state", JSON: "$.pending", Op: "eq", Value: 0, Eventually: 3 * time.Second},
		{Name: "never true", Path: "/state", JSON: "$.pending", Op: "gt", Value: 5, Eventually: 300 * time.Millisecond},
	}
	_, rep := run(t, sc)
	if !rep.Invariants[0].Pass {
		t.Errorf("drains should pass once state settles: %+v", rep.Invariants[0])
	}
	if rep.Invariants[1].Pass || rep.Invariants[1].WaitedMs < 250 {
		t.Errorf("unsatisfiable invariant must fail after waiting: %+v", rep.Invariants[1])
	}
	if rep.Verdict.Pass {
		t.Error("a failed invariant must fail the verdict")
	}
	if len(rep.Verdict.Failures) != 1 || !strings.Contains(rep.Verdict.Failures[0], "never true") {
		t.Errorf("failures: %v", rep.Verdict.Failures)
	}
}

func TestThresholdsAndTargetCounters(t *testing.T) {
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/stats" {
			_ = json.NewEncoder(w).Encode(map[string]any{"downstream": calls.Load()})
			return
		}
		calls.Add(3) // each request fans out to 3 downstream calls
	}))
	defer srv.Close()
	sc := baseScenario(srv.URL)
	sc.Observe = scenario.Observe{URL: "/stats", Counters: []string{"downstream"}}
	sc.Thresholds = scenario.Thresholds{RetryAmplification: ptr(0.5)} // impossible
	_, rep := run(t, sc)
	if rep.TargetStats == nil || rep.TargetStats.Delta["downstream"] != 60 || rep.TargetStats.PerLogical["downstream"] != 3 {
		t.Fatalf("target stats: %+v", rep.TargetStats)
	}
	if rep.Verdict.Pass {
		t.Fatal("amplification 1.0 > limit 0.5 must fail")
	}
}

func TestRefusesNonAllowedHostAndArrivalCap(t *testing.T) {
	sc := baseScenario("https://app.example.com")
	sc.Target.AllowHosts = nil
	if _, err := Run(context.Background(), sc, Options{Seed: 1, RunID: "t"}); err == nil || !strings.Contains(err.Error(), "refusing") {
		t.Fatalf("expected refusal, got %v", err)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer srv.Close()
	sc = baseScenario(srv.URL)
	if _, err := Run(context.Background(), sc, Options{Seed: 1, RunID: "t", MaxArrivals: 10}); err == nil || !strings.Contains(err.Error(), "safety cap") {
		t.Fatalf("expected cap error, got %v", err)
	}
}

func TestBackoffMath(t *testing.T) {
	r := scenario.Retry{Backoff: "exponential", BaseDelay: 100 * time.Millisecond, MaxDelay: 350 * time.Millisecond, Jitter: "none"}
	want := []time.Duration{100, 200, 350, 350}
	for i, w := range want {
		if got := backoff(r, i+1, nil, 0); got != w*time.Millisecond {
			t.Errorf("attempt %d: %v want %v", i+1, got, w*time.Millisecond)
		}
	}
	r.RespectRetryAfter = true
	if got := backoff(r, 1, nil, 2*time.Second); got != 2*time.Second {
		t.Errorf("Retry-After should win: %v", got)
	}
}
