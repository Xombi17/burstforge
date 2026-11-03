// Package runner executes a scenario against a target and builds the report.
package runner

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"burstforge/internal/hist"
	"burstforge/internal/jsonpath"
	"burstforge/internal/report"
	"burstforge/internal/scenario"
	"burstforge/internal/schedule"
)

const Version = "0.1.0"

type Options struct {
	Seed        int64
	RunID       string
	MaxArrivals int
	Logf        func(format string, args ...any)
}

type engine struct {
	sc     *scenario.Scenario
	opt    Options
	client *http.Client
	base   string
	start  time.Time // monotonic anchor for the schedule
	sem    chan struct{}
	wg     sync.WaitGroup
	col    *collector
}

// Run drives the scenario to completion (or until ctx is cancelled).
func Run(ctx context.Context, sc *scenario.Scenario, opt Options) (*report.Report, error) {
	if opt.Logf == nil {
		opt.Logf = func(string, ...any) {}
	}
	if err := sc.CheckTarget(); err != nil {
		return nil, err
	}
	if opt.MaxArrivals > 0 && sc.Traffic.Arrivals > opt.MaxArrivals {
		return nil, fmt.Errorf("traffic.arrivals %d exceeds the safety cap of %d (raise it with --max-arrivals)",
			sc.Traffic.Arrivals, opt.MaxArrivals)
	}

	offsets, err := schedule.Plan(schedule.Profile{
		Kind:     sc.Traffic.Profile,
		Arrivals: sc.Traffic.Arrivals,
		Window:   sc.Traffic.Window,
		Duration: sc.Traffic.Duration,
	}, opt.Seed)
	if err != nil {
		return nil, err
	}

	e := &engine{
		sc:   sc,
		opt:  opt,
		base: strings.TrimRight(sc.Target.BaseURL, "/"),
		sem:  make(chan struct{}, sc.Traffic.MaxInflight),
		col:  newCollector(),
		client: &http.Client{
			Transport: &http.Transport{
				MaxIdleConns:        sc.Traffic.MaxInflight,
				MaxIdleConnsPerHost: sc.Traffic.MaxInflight,
				MaxConnsPerHost:     0,
				IdleConnTimeout:     30 * time.Second,
			},
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
	}
	defer e.client.CloseIdleConnections()

	startedAt := time.Now()

	for _, h := range sc.Hooks.Before {
		if err := e.hook(ctx, h); err != nil {
			return nil, fmt.Errorf("before hook %s %s: %w", h.Method, h.Path, err)
		}
	}

	var before map[string]float64
	if sc.Observe.URL != "" {
		before, err = e.snapshot(ctx)
		if err != nil {
			return nil, fmt.Errorf("observe snapshot (before): %w", err)
		}
	}

	e.start = time.Now().Add(sc.Traffic.StartDelay)
	e.col.setStart(e.start)
	opt.Logf("firing %d arrivals (%s) in %s, planned span %s",
		len(offsets), sc.Traffic.Profile, sc.Traffic.StartDelay.Round(time.Millisecond), offsets[len(offsets)-1].Round(time.Millisecond))

	for i, off := range offsets {
		i, off := i, off
		e.wg.Add(1)
		time.AfterFunc(time.Until(e.start.Add(off)), func() {
			defer e.wg.Done()
			e.workflow(ctx, i, off)
		})
	}

	done := make(chan struct{})
	go func() { e.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
		e.col.setAborted()
		<-done // workflows observe ctx and return quickly
	}
	endedAt := time.Now()

	// Post-run checks use a fresh context so Ctrl-C still produces a report.
	cctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	var after map[string]float64
	if sc.Observe.URL != "" {
		after, err = e.snapshot(cctx)
		if err != nil {
			opt.Logf("observe snapshot (after) failed: %v", err)
		}
	}

	invs := make([]report.InvariantCheck, 0, len(sc.Invariants))
	for _, inv := range sc.Invariants {
		invs = append(invs, e.checkInvariant(cctx, inv))
	}

	for _, h := range sc.Hooks.After {
		if err := e.hook(cctx, h); err != nil {
			opt.Logf("after hook %s %s failed: %v", h.Method, h.Path, err)
		}
	}

	rep := e.col.build(sc, opt, startedAt, endedAt, offsets)
	if before != nil && after != nil {
		rep.TargetStats = targetStats(before, after, rep.Totals.Logical)
	}
	rep.Invariants = invs
	rep.Thresholds = evalThresholds(sc.Thresholds, rep)
	rep.Finalize()
	return rep, nil
}

// ------------------------------------------------------------- workflow ---

func (e *engine) workflow(ctx context.Context, i int, planned time.Duration) {
	select {
	case e.sem <- struct{}{}:
	case <-ctx.Done():
		return
	}
	defer func() { <-e.sem }()

	begin := time.Now()
	// Skew is measured after the in-flight cap, so agent saturation shows up
	// as skew instead of being hidden.
	e.col.recordSkew(begin.Sub(e.start.Add(planned)))

	runID := e.opt.RunID
	vars := map[string]string{
		"n":          strconv.Itoa(i),
		"logical_id": fmt.Sprintf("%s-%d", runID, i),
		"run_id":     runID,
	}
	rng := rand.New(rand.NewSource(e.opt.Seed + int64(i)*7919))
	double := rng.Float64() < e.sc.Traffic.DoubleSubmit

	steps := e.sc.Workflow.Steps
	ok, outcome := true, ""
	for si, st := range steps {
		res := e.runStep(ctx, st, vars, rng, double && si == len(steps)-1)
		outcome = res.final
		if !res.ok {
			ok = false
			break
		}
		for name, path := range st.Capture {
			var doc any
			var val any
			found := json.Unmarshal(res.body, &doc) == nil
			if found {
				val, found = jsonpath.Get(doc, path)
			}
			if !found {
				ok, outcome = false, "capture_failed"
				break
			}
			vars[name] = jsonpath.String(val)
		}
		if !ok {
			break
		}
	}
	if ctx.Err() != nil {
		return
	}
	e.col.finishWorkflow(ok, outcome, time.Since(begin))
}

type stepResult struct {
	ok    bool
	final string
	body  []byte
}

type request struct {
	method  string
	url     string
	headers map[string]string
	body    []byte
}

func (e *engine) buildRequest(st scenario.Step, vars map[string]string) request {
	r := request{
		method:  st.Method,
		url:     e.base + scenario.Render(st.Path, vars),
		headers: map[string]string{},
	}
	for k, v := range st.Headers {
		r.headers[k] = scenario.Render(v, vars)
	}
	if st.Body != nil {
		b, _ := json.Marshal(scenario.RenderValue(st.Body, vars))
		r.body = b
		if _, ok := r.headers["Content-Type"]; !ok {
			r.headers["Content-Type"] = "application/json"
		}
	}
	r.headers["X-BF-Run"] = vars["run_id"]
	r.headers["X-BF-Logical-Id"] = vars["logical_id"]
	return r
}

func (e *engine) runStep(ctx context.Context, st scenario.Step, vars map[string]string, rng *rand.Rand, doubleSubmit bool) stepResult {
	req := e.buildRequest(st, vars)

	if doubleSubmit {
		// The user clicks again. Fire an identical request without waiting on it.
		e.wg.Add(1)
		time.AfterFunc(e.sc.Traffic.DoubleSubmitDelay, func() {
			defer e.wg.Done()
			if ctx.Err() == nil {
				e.attempt(ctx, st, req, 1, "duplicate")
			}
		})
	}

	var last attemptResult
	for n := 1; n <= st.Retry.MaxAttempts; n++ {
		kind := "first"
		if n > 1 {
			kind = "retry"
		}
		last = e.attempt(ctx, st, req, n, kind)
		if last.ok {
			return stepResult{ok: true, final: last.label, body: last.body}
		}
		if n == st.Retry.MaxAttempts || ctx.Err() != nil || !retryable(st.Retry, last.class) {
			break
		}
		if wait := backoff(st.Retry, n, rng, last.retryAfter); wait > 0 {
			select {
			case <-time.After(wait):
			case <-ctx.Done():
			}
		}
	}
	return stepResult{ok: false, final: last.label}
}

type attemptResult struct {
	ok         bool
	class      string // ok timeout conn_error http_429 http_5xx http_4xx canceled
	label      string // "201", "timeout", ...
	body       []byte
	retryAfter time.Duration
}

func (e *engine) attempt(ctx context.Context, st scenario.Step, r request, n int, kind string) attemptResult {
	actx, cancel := context.WithTimeout(ctx, st.Timeout)
	defer cancel()

	hr, err := http.NewRequestWithContext(actx, r.method, r.url, bytes.NewReader(r.body))
	if err != nil {
		return attemptResult{class: "conn_error", label: "bad_request"}
	}
	for k, v := range r.headers {
		hr.Header.Set(k, v)
	}
	hr.Header.Set("X-BF-Attempt", strconv.Itoa(n))

	t0 := time.Now()
	res := attemptResult{}
	resp, err := e.client.Do(hr)
	if err == nil {
		var body []byte
		body, err = io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if err == nil {
			res.body = body
			res.label = strconv.Itoa(resp.StatusCode)
			res.ok, res.class = classify(st, resp.StatusCode)
			if ra := resp.Header.Get("Retry-After"); ra != "" {
				if secs, perr := strconv.Atoi(ra); perr == nil {
					res.retryAfter = time.Duration(secs) * time.Second
				}
			}
		}
	}
	if err != nil {
		switch {
		case errors.Is(actx.Err(), context.DeadlineExceeded):
			res.class, res.label = "timeout", "timeout"
		case ctx.Err() != nil:
			res.class, res.label = "canceled", "canceled"
		default:
			res.class, res.label = "conn_error", "conn_error"
		}
	}
	lat := time.Since(t0)
	e.col.recordAttempt(st.Name, kind, res.class, lat, t0.Sub(e.start))
	return res
}

func classify(st scenario.Step, code int) (ok bool, class string) {
	if len(st.OKStatus) > 0 {
		for _, c := range st.OKStatus {
			if c == code {
				return true, "ok"
			}
		}
	} else if code >= 200 && code < 300 {
		return true, "ok"
	}
	switch {
	case code == 429:
		return false, "http_429"
	case code >= 500:
		return false, "http_5xx"
	default:
		return false, "http_4xx"
	}
}

func retryable(r scenario.Retry, class string) bool {
	want := map[string]string{"timeout": "timeout", "conn_error": "conn", "http_429": "429", "http_5xx": "5xx"}[class]
	if want == "" {
		return false
	}
	for _, on := range r.On {
		if on == want {
			return true
		}
	}
	return false
}

func backoff(r scenario.Retry, attempt int, rng *rand.Rand, retryAfter time.Duration) time.Duration {
	var d time.Duration
	switch r.Backoff {
	case "fixed":
		d = r.BaseDelay
	case "exponential":
		shift := attempt - 1
		if shift > 16 {
			shift = 16
		}
		d = r.BaseDelay << shift
		if r.MaxDelay > 0 && d > r.MaxDelay {
			d = r.MaxDelay
		}
	}
	if r.Jitter == "full" && d > 0 {
		d = time.Duration(rng.Int63n(int64(d) + 1))
	}
	if r.RespectRetryAfter && retryAfter > d {
		d = retryAfter
	}
	return d
}

// ---------------------------------------------------- control + checking ---

func (e *engine) call(ctx context.Context, method, path string, headers map[string]string, body any) (int, []byte, error) {
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return 0, nil, err
		}
		rd = bytes.NewReader(b)
	}
	cctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(cctx, method, e.base+path, rd)
	if err != nil {
		return 0, nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := e.client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	return resp.StatusCode, b, err
}

func (e *engine) hook(ctx context.Context, c scenario.Call) error {
	code, _, err := e.call(ctx, c.Method, c.Path, c.Headers, c.Body)
	if err != nil {
		return err
	}
	if code < 200 || code >= 300 {
		return fmt.Errorf("status %d", code)
	}
	return nil
}

func (e *engine) fetchValue(ctx context.Context, path, jp string) (float64, map[string]any, error) {
	code, body, err := e.call(ctx, "GET", path, nil, nil)
	if err != nil {
		return 0, nil, err
	}
	if code != 200 {
		return 0, nil, fmt.Errorf("GET %s returned %d", path, code)
	}
	var doc any
	if err := json.Unmarshal(body, &doc); err != nil {
		return 0, nil, fmt.Errorf("GET %s: invalid JSON: %w", path, err)
	}
	v, ok := jsonpath.Get(doc, jp)
	if !ok {
		return 0, nil, fmt.Errorf("GET %s: %q not found in response", path, jp)
	}
	f, ok := jsonpath.Float(v)
	if !ok {
		return 0, nil, fmt.Errorf("GET %s: %q is not numeric", path, jp)
	}
	m, _ := doc.(map[string]any)
	return f, m, nil
}

func (e *engine) snapshot(ctx context.Context) (map[string]float64, error) {
	out := map[string]float64{}
	for _, c := range e.sc.Observe.Counters {
		v, _, err := e.fetchValue(ctx, e.sc.Observe.URL, c)
		if err != nil {
			return nil, err
		}
		out[strings.TrimPrefix(strings.TrimPrefix(c, "$"), ".")] = v
	}
	return out, nil
}

func targetStats(before, after map[string]float64, logical int) *report.TargetStats {
	ts := &report.TargetStats{Before: before, After: after, Delta: map[string]float64{}, PerLogical: map[string]float64{}}
	for k, a := range after {
		d := a - before[k]
		ts.Delta[k] = d
		if logical > 0 {
			ts.PerLogical[k] = d / float64(logical)
		}
	}
	return ts
}

func (e *engine) checkInvariant(ctx context.Context, inv scenario.Invariant) report.InvariantCheck {
	res := report.InvariantCheck{Name: inv.Name, Op: inv.Op, Expected: inv.Value}
	t0 := time.Now()
	deadline := t0.Add(inv.Eventually)
	for {
		v, _, err := e.fetchValue(ctx, inv.Path, inv.JSON)
		res.Observed, res.Error = v, ""
		if err != nil {
			res.Error = err.Error()
		}
		res.Pass = err == nil && compare(v, inv.Op, inv.Value)
		if res.Pass || !time.Now().Before(deadline) || ctx.Err() != nil {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	res.WaitedMs = float64(time.Since(t0).Microseconds()) / 1000
	return res
}

func compare(v float64, op string, want float64) bool {
	switch op {
	case "eq":
		return v == want
	case "lt":
		return v < want
	case "lte":
		return v <= want
	case "gt":
		return v > want
	case "gte":
		return v >= want
	}
	return false
}

func evalThresholds(t scenario.Thresholds, r *report.Report) []report.ThresholdCheck {
	var out []report.ThresholdCheck
	add := func(name string, limit *float64, observed float64) {
		if limit != nil {
			out = append(out, report.ThresholdCheck{Name: name, Pass: observed <= *limit, Observed: observed, Limit: *limit})
		}
	}
	errRate := 0.0
	if r.Totals.Logical > 0 {
		errRate = float64(r.Totals.Failed) / float64(r.Totals.Logical)
	}
	add("p99_ms", t.P99Ms, r.Latency.WorkflowOK.P99Ms)
	add("error_rate", t.ErrorRate, errRate)
	add("retry_amplification", t.RetryAmplification, r.Attempts.RetryAmplification)
	add("arrival_skew_p99_ms", t.ArrivalSkewP99Ms, r.ArrivalSkew.P99Ms)
	return out
}

// ------------------------------------------------------------ collector ---

type bucket struct{ attempts, ok, failed, retries int }

type collector struct {
	mu sync.Mutex

	start   time.Time
	aborted bool

	logical, succeeded, failed int
	outcomes                   map[string]int
	lastDone                   time.Duration

	e2eOK, e2eAll *hist.Histogram
	steps         map[string]*hist.Histogram
	skew          *hist.Histogram

	byKind, byClass map[string]int
	attempts        int
	timeline        map[int]*bucket
}

func newCollector() *collector {
	return &collector{
		outcomes: map[string]int{},
		e2eOK:    &hist.Histogram{},
		e2eAll:   &hist.Histogram{},
		steps:    map[string]*hist.Histogram{},
		skew:     &hist.Histogram{},
		byKind:   map[string]int{},
		byClass:  map[string]int{},
		timeline: map[int]*bucket{},
	}
}

func (c *collector) setStart(t time.Time) { c.mu.Lock(); c.start = t; c.mu.Unlock() }
func (c *collector) setAborted()          { c.mu.Lock(); c.aborted = true; c.mu.Unlock() }

func (c *collector) recordSkew(d time.Duration) {
	if d < 0 {
		d = 0
	}
	c.mu.Lock()
	c.skew.Record(d)
	c.mu.Unlock()
}

func (c *collector) recordAttempt(step, kind, class string, lat time.Duration, at time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	h := c.steps[step]
	if h == nil {
		h = &hist.Histogram{}
		c.steps[step] = h
	}
	h.Record(lat)
	c.attempts++
	c.byKind[kind]++
	c.byClass[class]++

	sec := int(at / time.Second)
	if sec < 0 {
		sec = 0
	}
	b := c.timeline[sec]
	if b == nil {
		b = &bucket{}
		c.timeline[sec] = b
	}
	b.attempts++
	if class == "ok" {
		b.ok++
	} else {
		b.failed++
	}
	if kind != "first" {
		b.retries++
	}
}

func (c *collector) finishWorkflow(ok bool, outcome string, lat time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.logical++
	c.outcomes[outcome]++
	c.e2eAll.Record(lat)
	if ok {
		c.succeeded++
		c.e2eOK.Record(lat)
	} else {
		c.failed++
	}
	if !c.start.IsZero() {
		if since := time.Since(c.start); since > c.lastDone {
			c.lastDone = since
		}
	}
}

func lat(h *hist.Histogram) report.Latency {
	f := func(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }
	return report.Latency{
		Count: h.Count(), MeanMs: f(h.Mean()),
		P50Ms: f(h.Quantile(0.50)), P95Ms: f(h.Quantile(0.95)), P99Ms: f(h.Quantile(0.99)), MaxMs: f(h.Max()),
	}
}

func (c *collector) build(sc *scenario.Scenario, opt Options, started, ended time.Time, offsets []time.Duration) *report.Report {
	c.mu.Lock()
	defer c.mu.Unlock()

	r := &report.Report{
		Schema:      report.SchemaVersion,
		Tool:        "burstforge " + Version,
		Scenario:    sc.Name,
		RunID:       opt.RunID,
		StartedAt:   started,
		FinishedAt:  ended,
		Seed:        opt.Seed,
		Target:      sc.Target.BaseURL,
		Environment: sc.Target.Environment,
		Traffic: report.Traffic{
			Profile:      sc.Traffic.Profile,
			Arrivals:     sc.Traffic.Arrivals,
			PlannedSpan:  float64(offsets[len(offsets)-1].Microseconds()) / 1000,
			DoubleSubmit: sc.Traffic.DoubleSubmit,
		},
		Totals: report.Totals{
			Logical: c.logical, Succeeded: c.succeeded, Failed: c.failed,
			FinalOutcomes: c.outcomes, Aborted: c.aborted,
		},
		Attempts: report.Attempts{Total: c.attempts, ByKind: c.byKind, ByClass: c.byClass},
		Latency: report.LatencySet{
			WorkflowOK:  lat(c.e2eOK),
			WorkflowAll: lat(c.e2eAll),
			Steps:       map[string]report.Latency{},
		},
		ArrivalSkew: lat(c.skew),
	}
	// Amplification is attempts per intended request: every step a user
	// reaches counts once as a "first" attempt, and retries and double
	// submits are the extra load on top of that.
	if first := c.byKind["first"]; first > 0 {
		r.Attempts.RetryAmplification = float64(c.attempts) / float64(first)
	}
	for name, h := range c.steps {
		r.Latency.Steps[name] = lat(h)
	}

	span := c.lastDone.Seconds()
	if span <= 0 {
		span = ended.Sub(started).Seconds()
	}
	r.Throughput = report.Throughput{
		DurationSec:    span,
		AttemptsPerSec: float64(c.attempts) / span,
		GoodputPerSec:  float64(c.succeeded) / span,
	}

	last := -1
	for s := range c.timeline {
		if s > last {
			last = s
		}
	}
	for s := 0; s <= last; s++ {
		b := c.timeline[s]
		if b == nil {
			b = &bucket{}
		}
		r.Timeline = append(r.Timeline, report.TimelinePoint{
			Second: s, Attempts: b.attempts, OK: b.ok, Failed: b.failed, Retries: b.retries,
		})
	}
	return r
}
