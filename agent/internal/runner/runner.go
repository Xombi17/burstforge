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
	"net/http/httptrace"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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

	// Sharding. Shard is 1-based; ShardCount 0 means the whole run.
	Shard      int
	ShardCount int
	// StartAt is the shared epoch the schedule is anchored to. Agents on
	// different processes or hosts use the same value so their arrivals line
	// up. Zero means now + traffic.start_delay.
	StartAt time.Time
	Agent   string // name recorded in shard reports

	// TrafficURL, if set, receives the workflow requests instead of
	// target.base_url: a fault proxy in front of the target. Its host must
	// pass the same check as the target, or be in TrustedProxies (hosts the
	// agent's operator allowed with --trust-proxy).
	TrafficURL     string
	TrustedProxies []string

	// Lease-based execution (run --db). With a Lease, the shard starts at
	// arrival Resume, sends only reserved arrivals, and skips arrivals that
	// are more than LateLimit overdue when it gets to them.
	Lease     Lease
	Resume    int
	Heartbeat time.Duration // default 1s
	LeaseTTL  time.Duration // default 5s
	LateLimit time.Duration // default 1s

	// Observer, if set, sees every attempt and workflow (live metrics,
	// trace export).
	Observer Observer
}

func (o *Options) sharded() bool { return o.ShardCount > 0 }

type engine struct {
	*control
	opt   Options
	start time.Time // monotonic anchor for the schedule
	sem   chan struct{}
	wg    sync.WaitGroup
	col   *collector
}

// control makes the non-load requests: hooks, counter snapshots, invariants.
type control struct {
	sc     *scenario.Scenario
	client *http.Client
	base   string
}

func newControl(sc *scenario.Scenario, conns int) *control {
	return &control{
		sc:   sc,
		base: strings.TrimRight(sc.Target.BaseURL, "/"),
		client: &http.Client{
			Transport: &http.Transport{
				MaxIdleConns:        conns,
				MaxIdleConnsPerHost: conns,
				MaxConnsPerHost:     0,
				IdleConnTimeout:     30 * time.Second,
			},
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
	}
}

func checkSafety(sc *scenario.Scenario, opt Options) error {
	if err := sc.CheckTarget(); err != nil {
		return err
	}
	if opt.MaxArrivals > 0 && sc.Traffic.Arrivals > opt.MaxArrivals {
		return fmt.Errorf("traffic.arrivals %d exceeds the safety cap of %d (raise it with --max-arrivals)",
			sc.Traffic.Arrivals, opt.MaxArrivals)
	}
	if opt.TrafficURL != "" {
		allow := append(append([]string{}, sc.Target.AllowHosts...), opt.TrustedProxies...)
		if err := scenario.CheckHost(opt.TrafficURL, allow); err != nil {
			return fmt.Errorf("traffic URL: %w", err)
		}
	}
	return nil
}

func withDefaults(opt Options) Options {
	if opt.Logf == nil {
		opt.Logf = func(string, ...any) {}
	}
	if opt.Heartbeat <= 0 {
		opt.Heartbeat = time.Second
	}
	if opt.LeaseTTL <= 0 {
		opt.LeaseTTL = 5 * time.Second
	}
	if opt.LateLimit <= 0 {
		opt.LateLimit = time.Second
	}
	return opt
}

func (e *engine) who() string {
	if !e.opt.sharded() {
		return ""
	}
	return fmt.Sprintf("shard %d/%d: ", e.opt.Shard, e.opt.ShardCount)
}

// Run drives the whole scenario as a single agent: before hooks, traffic,
// invariants, thresholds.
func Run(ctx context.Context, sc *scenario.Scenario, opt Options) (*report.Report, error) {
	opt = withDefaults(opt)
	opt.Shard, opt.ShardCount, opt.Lease = 0, 0, nil
	if err := checkSafety(sc, opt); err != nil {
		return nil, err
	}
	startedAt := time.Now()
	fs, err := StartFaults(ctx, sc, "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	defer fs.Close()
	before, err := Prepare(ctx, sc)
	if err != nil {
		return nil, err
	}
	if fs != nil {
		if opt.StartAt.IsZero() {
			opt.StartAt = time.Now().Add(sc.Traffic.StartDelay)
		}
		if err := fs.Apply(ctx, opt.Seed, opt.StartAt); err != nil {
			return nil, err
		}
		opt.TrafficURL = fs.URL
	}
	rep, err := traffic(ctx, sc, opt)
	if err != nil {
		return nil, err
	}
	rep.StartedAt = startedAt
	if fs != nil {
		rep.Faults = fs.Collect(opt.Logf)
	}
	Check(sc, rep, before, opt.Logf)
	Evaluate(sc, rep)
	return rep, nil
}

// RunShard sends only this agent's share of the traffic and returns a shard
// report for Merge. It runs no hooks, snapshots or invariants: those act on
// the whole target and belong to whoever coordinates the run.
func RunShard(ctx context.Context, sc *scenario.Scenario, opt Options) (*report.Report, error) {
	opt = withDefaults(opt)
	if !opt.sharded() || opt.Shard < 1 || opt.Shard > opt.ShardCount {
		return nil, fmt.Errorf("shard %d/%d is out of range", opt.Shard, opt.ShardCount)
	}
	if opt.Lease == nil && opt.Resume != 0 {
		return nil, fmt.Errorf("resume needs a lease")
	}
	if err := checkSafety(sc, opt); err != nil {
		return nil, err
	}
	return traffic(ctx, sc, opt)
}

// Prepare runs the before hooks and takes the "before" counter snapshot.
func Prepare(ctx context.Context, sc *scenario.Scenario) (map[string]float64, error) {
	c := newControl(sc, 4)
	defer c.client.CloseIdleConnections()
	for _, h := range sc.Hooks.Before {
		if err := c.hook(ctx, h); err != nil {
			return nil, fmt.Errorf("before hook %s %s: %w", h.Method, h.Path, err)
		}
	}
	if sc.Observe.URL == "" {
		return nil, nil
	}
	before, err := c.snapshot(ctx)
	if err != nil {
		return nil, fmt.Errorf("observe snapshot (before): %w", err)
	}
	return before, nil
}

// TargetCheck is the result of checking the target after load.
type TargetCheck struct {
	After      map[string]float64
	Invariants []report.InvariantCheck
}

// CheckTarget takes the "after" snapshot (if there is a "before" one), polls
// the invariants and runs the after hooks. It uses its own context so an
// interrupted run still gets checked.
func CheckTarget(sc *scenario.Scenario, before map[string]float64, logf func(string, ...any)) TargetCheck {
	if logf == nil {
		logf = func(string, ...any) {}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	c := newControl(sc, 4)
	defer c.client.CloseIdleConnections()

	var tc TargetCheck
	if sc.Observe.URL != "" && before != nil {
		after, err := c.snapshot(ctx)
		if err != nil {
			logf("observe snapshot (after) failed: %v", err)
		} else {
			tc.After = after
		}
	}
	tc.Invariants = make([]report.InvariantCheck, 0, len(sc.Invariants))
	for _, inv := range sc.Invariants {
		tc.Invariants = append(tc.Invariants, c.checkInvariant(ctx, inv))
	}
	for _, h := range sc.Hooks.After {
		if err := c.hook(ctx, h); err != nil {
			logf("after hook %s %s failed: %v", h.Method, h.Path, err)
		}
	}
	return tc
}

// Apply puts the check results into rep. Counter deltas are divided by the
// report's workflow count, so apply to the final report.
func (tc TargetCheck) Apply(rep *report.Report, before map[string]float64) {
	if before != nil && tc.After != nil {
		rep.TargetStats = targetStats(before, tc.After, rep.Totals.Logical)
	}
	rep.Invariants = tc.Invariants
}

// Check is CheckTarget followed by Apply.
func Check(sc *scenario.Scenario, rep *report.Report, before map[string]float64, logf func(string, ...any)) {
	CheckTarget(sc, before, logf).Apply(rep, before)
}

// BaseReport is an empty report carrying the run's identity and plan. The
// lease-based reducer starts each shard from it.
func BaseReport(sc *scenario.Scenario, runID string, seed int64) (*report.Report, error) {
	offsets, err := schedule.Plan(schedule.Profile{
		Kind: sc.Traffic.Profile, Arrivals: sc.Traffic.Arrivals,
		Window: sc.Traffic.Window, Duration: sc.Traffic.Duration,
	}, seed)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	return newCollector().build(sc, Options{RunID: runID, Seed: seed}, now, now, offsets), nil
}

// Evaluate applies the scenario's thresholds and computes the verdict.
func Evaluate(sc *scenario.Scenario, rep *report.Report) {
	rep.Thresholds = evalThresholds(sc.Thresholds, rep)
	rep.Finalize()
}

// traffic fires this agent's arrivals and waits for their workflows.
func traffic(ctx context.Context, sc *scenario.Scenario, opt Options) (*report.Report, error) {
	offsets, err := schedule.Plan(schedule.Profile{
		Kind:     sc.Traffic.Profile,
		Arrivals: sc.Traffic.Arrivals,
		Window:   sc.Traffic.Window,
		Duration: sc.Traffic.Duration,
	}, opt.Seed)
	if err != nil {
		return nil, err
	}

	// The shard is always cut from the full plan, so every agent agrees on
	// which arrival is which.
	var mine []int
	inflight := sc.Traffic.MaxInflight
	if opt.sharded() {
		if mine, err = schedule.Shard(len(offsets), opt.Shard, opt.ShardCount); err != nil {
			return nil, err
		}
		// max_inflight is a budget for the whole run, split across agents.
		inflight = (inflight + opt.ShardCount - 1) / opt.ShardCount
	} else {
		mine = make([]int, len(offsets))
		for i := range mine {
			mine[i] = i
		}
	}

	e := &engine{
		control: newControl(sc, inflight),
		opt:     opt,
		sem:     make(chan struct{}, inflight),
		col:     newCollector(),
	}
	if opt.TrafficURL != "" {
		e.base = strings.TrimRight(opt.TrafficURL, "/")
	}
	defer e.client.CloseIdleConnections()

	startedAt := time.Now()
	startAt := opt.StartAt
	if startAt.IsZero() {
		startAt = startedAt.Add(sc.Traffic.StartDelay)
	}
	// Re-derive the anchor from time.Now so it carries a monotonic reading;
	// a StartAt parsed from a flag only has wall-clock time.
	lead := time.Until(startAt)
	e.start = time.Now().Add(lead)
	e.col.setStart(e.start)

	who := e.who()
	if lead < 0 {
		opt.Logf("%sstart time passed %s ago; arrivals will fire late and the lateness shows up as arrival skew",
			who, (-lead).Round(time.Millisecond))
	}
	if opt.Resume > 0 {
		opt.Logf("%sresuming at arrival %d of %d (earlier ones were reserved by a previous lease holder)",
			who, opt.Resume+1, len(mine))
	}
	opt.Logf("%sfiring %d of %d arrivals (%s) in %s, planned span %s",
		who, len(mine)-opt.Resume, len(offsets), sc.Traffic.Profile, lead.Round(time.Millisecond), offsets[len(offsets)-1].Round(time.Millisecond))

	var loop *leaseLoop
	var g *gate
	if opt.Lease != nil {
		due := make([]time.Time, len(mine))
		for k, i := range mine {
			due[k] = e.start.Add(offsets[i])
		}
		loop = startLeaseLoop(e, due)
		g = loop.gate
	}

	handled := e.dispatch(ctx, mine, offsets, g)
	// Under a lease, an interrupted agent hands its shard to another agent,
	// so the interruption is a recovery event, not an aborted run.
	if ctx.Err() != nil && loop == nil {
		e.col.setAborted()
	}
	e.wg.Wait() // workflows observe ctx and return quickly
	endedAt := time.Now()

	info := &report.ShardInfo{
		Index:        opt.Shard,
		Count:        opt.ShardCount,
		Arrivals:     len(mine),
		ScenarioHash: sc.Hash(),
		StartAt:      startAt.Round(0).UTC(),
		Agent:        opt.Agent,
	}
	if loop != nil {
		ferr := loop.finish(ctx.Err() != nil, handled)
		base := newCollector().build(sc, opt, startedAt, endedAt, offsets)
		base.Shard = info
		rep := report.Sum(base, loop.sent)
		if ferr != nil && !errors.Is(ferr, ErrFenced) {
			return rep, fmt.Errorf("handing the shard back: %w", ferr)
		}
		return rep, ferr
	}

	rep := e.col.build(sc, opt, startedAt, endedAt, offsets)
	if opt.sharded() {
		rep.Shard = info
	} else {
		rep.Raw = nil
	}
	return rep, nil
}

// dispatch starts each arrival's workflow at its planned time, in schedule
// order. One loop (instead of a timer per arrival) means the arrivals sent so
// far are always a prefix of the shard, which is what reservations count.
// It returns how many shard arrivals it handled (sent or skipped).
func (e *engine) dispatch(ctx context.Context, mine []int, offsets []time.Duration, g *gate) int {
	timer := time.NewTimer(time.Hour)
	timer.Stop()
	k := e.opt.Resume
	for ; k < len(mine); k++ {
		if g != nil && !g.wait(ctx, k) {
			return k
		}
		i, off := mine[k], offsets[mine[k]]
		due := e.start.Add(off)
		if g != nil && time.Since(due) > e.opt.LateLimit {
			// Too late to send without changing the load shape.
			e.col.recordSkipped()
			continue
		}
		if d := time.Until(due); d > 0 {
			timer.Reset(d)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				return k
			}
		}
		if ctx.Err() != nil {
			return k
		}
		e.wg.Add(1)
		go func() {
			defer e.wg.Done()
			e.workflow(ctx, i, off)
		}()
	}
	return k
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
	tc := traceCtx{trace: WorkflowTraceID(runID, i), root: newSpanID()}
	ctx = withTrace(ctx, tc)
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
	took := time.Since(begin)
	e.col.finishWorkflow(ok, outcome, took)
	if e.opt.Observer != nil {
		e.opt.Observer.Workflow(WorkflowEvent{
			Arrival: i, OK: ok, Outcome: outcome, Start: begin, Duration: took, Trace: tc.trace, Span: tc.root,
		})
	}
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
	r.headers["X-BF-Arrival"] = vars["n"] // stable across runs with the same seed
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

	// Go's transport silently resends a "replayable" request after a
	// connection error on a reused connection, and a POST with an
	// Idempotency-Key header counts as replayable. Those resends would bypass
	// the retry policy and the attempt count. Without GetBody a request with
	// a body can't be replayed; bodiless ones can, so count their writes.
	var writes atomic.Int32
	actx = httptrace.WithClientTrace(actx, &httptrace.ClientTrace{
		WroteRequest: func(httptrace.WroteRequestInfo) { writes.Add(1) },
	})
	hr, err := http.NewRequestWithContext(actx, r.method, r.url, bytes.NewReader(r.body))
	if err != nil {
		return attemptResult{class: "conn_error", label: "bad_request"}
	}
	if len(r.body) > 0 {
		hr.GetBody = nil
	}
	for k, v := range r.headers {
		hr.Header.Set(k, v)
	}
	hr.Header.Set("X-BF-Attempt", strconv.Itoa(n))
	tc, traced := traceFrom(ctx)
	span := newSpanID()
	if traced && hr.Header.Get("Traceparent") == "" { // the scenario may set its own
		hr.Header.Set("Traceparent", traceparent(tc.trace, span))
	}

	t0 := time.Now()
	res := attemptResult{}
	status := 0
	resp, err := e.client.Do(hr)
	if err == nil {
		status = resp.StatusCode
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
	arrival, _ := strconv.Atoi(r.headers["X-BF-Arrival"])
	for k := writes.Load(); k > 1; k-- {
		e.col.recordAttempt(st.Name, "transport_retry", "conn_error", -1, t0.Sub(e.start))
		if e.opt.Observer != nil {
			e.opt.Observer.Attempt(AttemptEvent{Arrival: arrival, Step: st.Name, Method: r.method, URL: r.url, N: n, Kind: "transport_retry",
				Class: "conn_error", Start: t0, Duration: -1, Trace: tc.trace, Parent: tc.root})
		}
	}
	e.col.recordAttempt(st.Name, kind, res.class, lat, t0.Sub(e.start))
	if e.opt.Observer != nil {
		e.opt.Observer.Attempt(AttemptEvent{
			Arrival: arrival, Step: st.Name, Method: r.method, URL: r.url, N: n, Kind: kind, Class: res.class,
			Status: status, Start: t0, Duration: lat, Trace: tc.trace, Span: span, Parent: tc.root,
		})
	}
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

func (c *control) call(ctx context.Context, method, path string, headers map[string]string, body any) (int, []byte, error) {
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
	req, err := http.NewRequestWithContext(cctx, method, c.base+path, rd)
	if err != nil {
		return 0, nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	return resp.StatusCode, b, err
}

func (c *control) hook(ctx context.Context, h scenario.Call) error {
	code, _, err := c.call(ctx, h.Method, h.Path, h.Headers, h.Body)
	if err != nil {
		return err
	}
	if code < 200 || code >= 300 {
		return fmt.Errorf("status %d", code)
	}
	return nil
}

func (c *control) fetchValue(ctx context.Context, path, jp string) (float64, map[string]any, error) {
	code, body, err := c.call(ctx, "GET", path, nil, nil)
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

func (c *control) snapshot(ctx context.Context) (map[string]float64, error) {
	out := map[string]float64{}
	for _, ctr := range c.sc.Observe.Counters {
		v, _, err := c.fetchValue(ctx, c.sc.Observe.URL, ctr)
		if err != nil {
			return nil, err
		}
		out[strings.TrimPrefix(strings.TrimPrefix(ctr, "$"), ".")] = v
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

func (c *control) checkInvariant(ctx context.Context, inv scenario.Invariant) report.InvariantCheck {
	res := report.InvariantCheck{Name: inv.Name, Op: inv.Op, Expected: inv.Value}
	t0 := time.Now()
	deadline := t0.Add(inv.Eventually)
	for {
		v, _, err := c.fetchValue(ctx, inv.Path, inv.JSON)
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
	skipped         int
}

func (c *collector) recordSkipped() { c.mu.Lock(); c.skipped++; c.mu.Unlock() }

// cut returns everything recorded since the last cut as a chunk and resets
// the counters. It returns nil if nothing happened.
func (c *collector) cut() *report.Report {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.attempts == 0 && c.logical == 0 && c.skipped == 0 && c.skew.Count() == 0 {
		return nil
	}
	r := c.buildLocked(nil, Options{}, time.Time{}, time.Time{}, nil)
	fresh := newCollector()
	fresh.start, fresh.aborted = c.start, c.aborted
	c.logical, c.succeeded, c.failed, c.skipped, c.attempts = 0, 0, 0, 0, 0
	c.outcomes, c.e2eOK, c.e2eAll, c.steps, c.skew = fresh.outcomes, fresh.e2eOK, fresh.e2eAll, fresh.steps, fresh.skew
	c.byKind, c.byClass, c.timeline = fresh.byKind, fresh.byClass, fresh.timeline
	c.lastDone = 0
	return r
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
	if lat >= 0 { // negative: an uncounted resend with no latency of its own
		h.Record(lat)
	}
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

func (c *collector) build(sc *scenario.Scenario, opt Options, started, ended time.Time, offsets []time.Duration) *report.Report {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buildLocked(sc, opt, started, ended, offsets)
}

// buildLocked turns the counters into a report. With a nil scenario it
// builds a bare telemetry chunk: counters, histograms and timeline only.
func (c *collector) buildLocked(sc *scenario.Scenario, opt Options, started, ended time.Time, offsets []time.Duration) *report.Report {
	r := &report.Report{
		Schema:     report.SchemaVersion,
		Tool:       "burstforge " + Version,
		RunID:      opt.RunID,
		StartedAt:  started,
		FinishedAt: ended,
		Seed:       opt.Seed,
		Totals: report.Totals{
			Logical: c.logical, Succeeded: c.succeeded, Failed: c.failed,
			FinalOutcomes: c.outcomes, Aborted: c.aborted, SkippedOverdue: c.skipped,
		},
		Attempts: report.Attempts{Total: c.attempts, ByKind: c.byKind, ByClass: c.byClass},
		Raw: &report.Raw{
			WorkflowOK:  c.e2eOK,
			WorkflowAll: c.e2eAll,
			ArrivalSkew: c.skew,
			Steps:       c.steps,
			LastDoneMs:  float64(c.lastDone.Microseconds()) / 1000,
		},
	}

	if sc != nil {
		r.Scenario = sc.Name
		r.Target = sc.Target.BaseURL
		r.Environment = sc.Target.Environment
		r.Traffic = report.Traffic{
			Profile:      sc.Traffic.Profile,
			Arrivals:     sc.Traffic.Arrivals,
			PlannedSpan:  float64(offsets[len(offsets)-1].Microseconds()) / 1000,
			DoubleSubmit: sc.Traffic.DoubleSubmit,
		}
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
	r.Derive()
	return r
}
