package faultproxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// setup starts an upstream that counts requests and a proxy in front of it.
func setup(t *testing.T, rules ...Rule) (proxyURL string, hits *atomic.Int64, p *Proxy) {
	t.Helper()
	hits = &atomic.Int64{}
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("X-Up", "yes")
		w.WriteHeader(201)
		fmt.Fprint(w, "created "+r.URL.Path)
	}))
	t.Cleanup(up.Close)
	p, err := New(up.URL)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Configure(Config{Seed: 1, StartAt: time.Now(), Rules: rules}); err != nil {
		t.Fatal(err)
	}
	e, err := Start(p, "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	return e.URL, hits, p
}

func get(t *testing.T, url string, id int, timeout time.Duration) (*http.Response, string, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "POST", url, strings.NewReader("{}"))
	req.Header.Set("X-BF-Logical-Id", fmt.Sprintf("run-%d", id))
	req.Header.Set("X-BF-Attempt", "1")
	resp, err := (&http.Client{Transport: &http.Transport{DisableKeepAlives: true}}).Do(req)
	if err != nil {
		return nil, "", err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp, string(b), nil
}

func TestPassThroughWithoutRules(t *testing.T) {
	u, hits, _ := setup(t)
	resp, body, err := get(t, u+"/register?x=1", 1, time.Second)
	if err != nil || resp.StatusCode != 201 || body != "created /register" || resp.Header.Get("X-Up") != "yes" || hits.Load() != 1 {
		t.Fatalf("resp %v body %q err %v hits %d", resp, body, err, hits.Load())
	}
}

func TestStatusFaultAnswersInsteadOfTarget(t *testing.T) {
	u, hits, p := setup(t, Rule{Path: "/register", Status: 429, RetryAfter: 1500 * time.Millisecond})
	resp, _, err := get(t, u+"/register", 1, time.Second)
	if err != nil || resp.StatusCode != 429 || resp.Header.Get("Retry-After") != "2" || hits.Load() != 0 {
		t.Fatalf("resp %v err %v hits %d", resp, err, hits.Load())
	}
	// Other paths are untouched.
	if resp, _, _ := get(t, u+"/auth", 2, time.Second); resp.StatusCode != 201 {
		t.Fatalf("/auth got %d", resp.StatusCode)
	}
	if s := p.Stats(); s.Rules[0].Matched != 1 || s.Rules[0].Injected != 1 || s.Requests != 2 {
		t.Fatalf("stats %+v", s)
	}
}

func TestResetBeforeNeverReachesTarget(t *testing.T) {
	u, hits, _ := setup(t, Rule{Reset: true})
	if _, _, err := get(t, u+"/register", 1, time.Second); err == nil {
		t.Fatal("expected a connection error")
	}
	if hits.Load() != 0 {
		t.Fatalf("target saw %d requests", hits.Load())
	}
}

func TestResetAfterReachesTargetButLosesResponse(t *testing.T) {
	u, hits, _ := setup(t, Rule{Reset: true, Phase: "after"})
	_, _, err := get(t, u+"/register", 1, time.Second)
	if err == nil || hits.Load() != 1 {
		t.Fatalf("err %v hits %d; want a connection error after the target handled it", err, hits.Load())
	}
}

func TestTimeoutHoldsUntilClientGivesUp(t *testing.T) {
	u, hits, _ := setup(t, Rule{Timeout: true, Phase: "after"})
	start := time.Now()
	_, _, err := get(t, u+"/register", 1, 300*time.Millisecond)
	if !errors.Is(err, context.DeadlineExceeded) || hits.Load() != 1 {
		t.Fatalf("err %v hits %d", err, hits.Load())
	}
	if d := time.Since(start); d < 250*time.Millisecond || d > 2*time.Second {
		t.Fatalf("took %s", d)
	}
}

func TestLatencyBeforeAndAfter(t *testing.T) {
	for _, phase := range []string{"before", "after"} {
		u, hits, _ := setup(t, Rule{Latency: 200 * time.Millisecond, Phase: phase})
		start := time.Now()
		resp, _, err := get(t, u+"/x", 1, 2*time.Second)
		if err != nil || resp.StatusCode != 201 || hits.Load() != 1 {
			t.Fatalf("%s: resp %v err %v", phase, resp, err)
		}
		if d := time.Since(start); d < 190*time.Millisecond {
			t.Fatalf("%s: took only %s", phase, d)
		}
	}
}

func TestRateIsDeterministicPerRequest(t *testing.T) {
	rule := Rule{Status: 503, Rate: 0.3}
	outcome := func() []int {
		u, _, _ := setup(t, rule)
		var codes []int
		for i := 0; i < 200; i++ {
			resp, _, err := get(t, u+"/x", i, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			codes = append(codes, resp.StatusCode)
		}
		return codes
	}
	a, b := outcome(), outcome()
	if fmt.Sprint(a) != fmt.Sprint(b) {
		t.Fatal("same seed and requests gave different faults")
	}
	n := 0
	for _, c := range a {
		if c == 503 {
			n++
		}
	}
	if n < 40 || n > 80 {
		t.Fatalf("%d of 200 faulted at rate 0.3", n)
	}
}

func TestWindowedRule(t *testing.T) {
	r := Rule{Reset: true, Every: 10 * time.Second, For: time.Second}
	req := httptest.NewRequest("GET", "/x", nil)
	start := time.Now()
	for _, c := range []struct {
		at   time.Duration
		want bool
	}{{500 * time.Millisecond, true}, {1500 * time.Millisecond, false}, {10200 * time.Millisecond, true}, {-time.Second, false}} {
		if got := r.active(req, start.Add(c.at), start); got != c.want {
			t.Fatalf("at %s: active %v, want %v", c.at, got, c.want)
		}
	}
}

func TestControlRequestsAreNotFaultedOrForwarded(t *testing.T) {
	u, hits, _ := setup(t, Rule{Reset: true})
	c := &Client{Base: u}
	s, err := c.Stats(context.Background())
	if err != nil || hits.Load() != 0 || len(s.Rules) != 1 {
		t.Fatalf("stats %+v err %v hits %d", s, err, hits.Load())
	}
	if err := c.Configure(context.Background(), Config{Rules: []Rule{{Status: 200}}}); err == nil {
		t.Fatal("accepted an invalid rule")
	}
	if err := c.Clear(context.Background()); err != nil {
		t.Fatal(err)
	}
	if resp, _, err := get(t, u+"/x", 1, time.Second); err != nil || resp.StatusCode != 201 {
		t.Fatalf("after clear: %v %v", resp, err)
	}
}

func TestRuleValidation(t *testing.T) {
	bad := []Rule{
		{},                                  // no action
		{Latency: time.Second, Reset: true}, // two actions
		{Status: 302},                       // not an error status
		{Status: 503, Phase: "after"},       // status can't be after
		{Reset: true, Rate: 1.5},            // rate
		{Reset: true, Path: "register"},     // path
		{Latency: time.Second, Jitter: 2 * time.Second},
		{Reset: true, Every: time.Second}, // every without for
		{Reset: true, Every: time.Second, For: 2 * time.Second},
		{Reset: true, RetryAfter: time.Second},
	}
	for i, r := range bad {
		if r.Validate() == nil {
			t.Fatalf("bad rule %d passed validation: %+v", i, r)
		}
	}
	if _, err := Expand("nope", nil); err == nil {
		t.Fatal("unknown preset accepted")
	}
	for name, rules := range Presets {
		for _, r := range rules {
			if err := r.Validate(); err != nil {
				t.Fatalf("preset %s: %v", name, err)
			}
		}
	}
}
