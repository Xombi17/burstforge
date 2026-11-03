package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
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

// Three shards of one run, merged through their JSON form, should send the
// same requests and report the same totals as one agent with the same seed.
func TestShardedRunMatchesSingleAgent(t *testing.T) {
	var mu sync.Mutex
	seen := map[string]int{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-BF-Logical-Id")
		mu.Lock()
		seen[id]++
		n := seen[id]
		mu.Unlock()
		if n == 1 && strings.HasSuffix(id, "7") {
			w.WriteHeader(503) // ids ending in 7 need one retry
			return
		}
		w.WriteHeader(200)
	}))
	defer srv.Close()

	sc := baseScenario(srv.URL)
	sc.Traffic.Arrivals = 90
	sc.Traffic.DoubleSubmit = 0.2
	sc.Workflow.Steps[0].Retry.MaxAttempts = 2

	single, err := Run(context.Background(), sc, Options{Seed: 9, RunID: "r"})
	if err != nil {
		t.Fatal(err)
	}
	singleSeen := seen
	seen = map[string]int{}

	dir := t.TempDir()
	startAt := time.Now().Add(100 * time.Millisecond)
	var wg sync.WaitGroup
	paths := make([]string, 3)
	errs := make([]error, 3)
	for k := 1; k <= 3; k++ {
		wg.Add(1)
		go func(k int) {
			defer wg.Done()
			rep, err := RunShard(context.Background(), sc, Options{
				Seed: 9, RunID: "r", Shard: k, ShardCount: 3, StartAt: startAt, Agent: fmt.Sprintf("a%d", k),
			})
			if err != nil {
				errs[k-1] = err
				return
			}
			paths[k-1] = filepath.Join(dir, fmt.Sprintf("shard-%d.json", k))
			errs[k-1] = rep.Save(paths[k-1])
		}(k)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}

	var shards []*report.Report
	for _, p := range paths {
		r, err := report.Load(p)
		if err != nil {
			t.Fatal(err)
		}
		if r.Shard.Arrivals != 30 || r.Raw == nil {
			t.Fatalf("shard %d: arrivals %d, raw %v", r.Shard.Index, r.Shard.Arrivals, r.Raw != nil)
		}
		shards = append(shards, r)
	}
	merged, err := report.Merge(shards, false)
	if err != nil {
		t.Fatal(err)
	}
	Evaluate(sc, merged)

	if fmt.Sprint(seen) != fmt.Sprint(singleSeen) {
		t.Fatalf("sharded run sent different requests per logical id:\nsharded %v\nsingle  %v", seen, singleSeen)
	}
	if merged.Totals.Logical != single.Totals.Logical || merged.Totals.Succeeded != single.Totals.Succeeded {
		t.Fatalf("totals: merged %+v, single %+v", merged.Totals, single.Totals)
	}
	if fmt.Sprint(merged.Attempts.ByKind) != fmt.Sprint(single.Attempts.ByKind) ||
		merged.Attempts.RetryAmplification != single.Attempts.RetryAmplification {
		t.Fatalf("attempts: merged %+v, single %+v", merged.Attempts, single.Attempts)
	}
	if merged.Latency.WorkflowOK.Count != 90 || merged.ArrivalSkew.Count != 90 {
		t.Fatalf("merged histogram counts: workflow %d, skew %d", merged.Latency.WorkflowOK.Count, merged.ArrivalSkew.Count)
	}
	if len(merged.Shards) != 3 || merged.Raw != nil || !merged.Verdict.Pass {
		t.Fatalf("merged report: shards %d, raw %v, verdict %+v", len(merged.Shards), merged.Raw != nil, merged.Verdict)
	}
	sum := 0
	for _, p := range merged.Timeline {
		sum += p.Attempts
	}
	if sum != merged.Attempts.Total {
		t.Fatalf("timeline sums to %d attempts, total is %d", sum, merged.Attempts.Total)
	}
}

func TestShardSplitsInflightBudget(t *testing.T) {
	var cur, peak int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&cur, 1)
		for {
			p := atomic.LoadInt32(&peak)
			if n <= p || atomic.CompareAndSwapInt32(&peak, p, n) {
				break
			}
		}
		time.Sleep(30 * time.Millisecond)
		atomic.AddInt32(&cur, -1)
	}))
	defer srv.Close()

	sc := baseScenario(srv.URL)
	sc.Traffic.Arrivals = 40
	sc.Traffic.MaxInflight = 8
	if _, err := RunShard(context.Background(), sc, Options{Seed: 1, RunID: "r", Shard: 1, ShardCount: 4}); err != nil {
		t.Fatal(err)
	}
	if peak > 2 {
		t.Fatalf("one of 4 shards with max_inflight 8 reached %d in flight, want <= 2", peak)
	}
}

func TestShardRunsNoHooks(t *testing.T) {
	var hooks int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/hook" {
			atomic.AddInt32(&hooks, 1)
		}
	}))
	defer srv.Close()
	sc := baseScenario(srv.URL)
	sc.Hooks.Before = []scenario.Call{{Method: "POST", Path: "/hook"}}
	sc.Hooks.After = []scenario.Call{{Method: "POST", Path: "/hook"}}
	if _, err := RunShard(context.Background(), sc, Options{Seed: 1, RunID: "r", Shard: 2, ShardCount: 2}); err != nil {
		t.Fatal(err)
	}
	if hooks != 0 {
		t.Fatalf("shard ran %d hooks; hooks belong to the coordinator", hooks)
	}
	if _, err := RunShard(context.Background(), sc, Options{Seed: 1, RunID: "r", Shard: 3, ShardCount: 2}); err == nil {
		t.Fatal("expected an out-of-range shard error")
	}
}
