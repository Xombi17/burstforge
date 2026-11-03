package faultproxy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ControlPrefix is where the proxy's own API lives. Requests under it are
// never forwarded and never faulted.
const ControlPrefix = "/_bfproxy/"

// maxHold bounds how long a timeout fault holds a request open.
const maxHold = 2 * time.Minute

// Config is what the coordinator sends before a run.
type Config struct {
	Seed    int64     `json:"seed"`
	StartAt time.Time `json:"start_at"` // every/for windows count from here
	Rules   []Rule    `json:"rules"`
}

type RuleStats struct {
	Rule     string `json:"rule"`
	Matched  int64  `json:"matched"`  // requests the rule applied to (path, method, window)
	Injected int64  `json:"injected"` // requests that actually got the fault
}

type Stats struct {
	Requests       int64       `json:"requests"`
	UpstreamErrors int64       `json:"upstream_errors"` // the target itself failed to answer
	Rules          []RuleStats `json:"rules"`
}

type state struct {
	cfg      Config
	matched  []atomic.Int64
	injected []atomic.Int64
	requests atomic.Int64
	upErrs   atomic.Int64
	counter  atomic.Uint64 // keys requests that carry no logical id
}

type Proxy struct {
	upstream *url.URL
	client   *http.Client
	mu       sync.RWMutex
	st       *state
}

// New returns a proxy that forwards to upstream with no faults configured.
func New(upstream string) (*Proxy, error) {
	u, err := url.Parse(upstream)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("upstream must be an http(s) URL, got %q", upstream)
	}
	p := &Proxy{
		upstream: u,
		client: &http.Client{
			Transport: &http.Transport{
				MaxIdleConns:        2000,
				MaxIdleConnsPerHost: 2000,
				IdleConnTimeout:     30 * time.Second,
			},
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
	}
	if err := p.Configure(Config{}); err != nil {
		return nil, err
	}
	return p, nil
}

// Configure replaces the fault rules and resets the counters.
func (p *Proxy) Configure(cfg Config) error {
	for i, r := range cfg.Rules {
		if err := r.Validate(); err != nil {
			return fmt.Errorf("rule %d (%s): %w", i+1, r, err)
		}
	}
	st := &state{cfg: cfg, matched: make([]atomic.Int64, len(cfg.Rules)), injected: make([]atomic.Int64, len(cfg.Rules))}
	p.mu.Lock()
	p.st = st
	p.mu.Unlock()
	return nil
}

func (p *Proxy) state() *state {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.st
}

func (p *Proxy) Stats() Stats {
	st := p.state()
	out := Stats{Requests: st.requests.Load(), UpstreamErrors: st.upErrs.Load(), Rules: []RuleStats{}}
	for i, r := range st.cfg.Rules {
		out.Rules = append(out.Rules, RuleStats{Rule: r.String(), Matched: st.matched[i].Load(), Injected: st.injected[i].Load()})
	}
	return out
}

// roll maps (seed, rule, request key, salt) to [0, 1). Requests carry their
// arrival number and attempt number, so the same seed faults the same
// requests in a before/after comparison, even though run IDs differ.
func roll(seed int64, rule int, key, salt string) float64 {
	h := fnv.New64a()
	fmt.Fprintf(h, "%d|%d|%s|%s", seed, rule, key, salt)
	return float64(h.Sum64()>>11) / float64(1<<53)
}

func (st *state) key(r *http.Request) string {
	id := r.Header.Get("X-BF-Arrival")
	if id == "" {
		id = r.Header.Get("X-BF-Logical-Id")
	}
	if id == "" {
		return "n" + strconv.FormatUint(st.counter.Add(1), 10)
	}
	return id + "|" + r.Header.Get("X-BF-Attempt") + "|" + r.Method + " " + r.URL.Path
}

func (r Rule) active(req *http.Request, now, start time.Time) bool {
	if r.Path != "" && !strings.HasPrefix(req.URL.Path, r.Path) {
		return false
	}
	if r.Method != "" && req.Method != r.Method {
		return false
	}
	if r.Every > 0 {
		since := now.Sub(start)
		if since < 0 || since%r.Every >= r.For {
			return false
		}
	}
	return true
}

type plan struct {
	before, after time.Duration // added latency
	terminal      *Rule         // status, reset or timeout; the first one that fires
}

func (st *state) plan(r *http.Request) plan {
	var pl plan
	key := st.key(r)
	now := time.Now()
	for i := range st.cfg.Rules {
		rule := &st.cfg.Rules[i]
		if !rule.active(r, now, st.cfg.StartAt) {
			continue
		}
		st.matched[i].Add(1)
		if roll(st.cfg.Seed, i, key, "fire") >= rule.rate() {
			continue
		}
		if rule.Latency > 0 {
			d := rule.Latency
			if rule.Jitter > 0 {
				d += time.Duration((roll(st.cfg.Seed, i, key, "jitter")*2 - 1) * float64(rule.Jitter))
			}
			if rule.after() {
				pl.after += d
			} else {
				pl.before += d
			}
			st.injected[i].Add(1)
			continue
		}
		if pl.terminal == nil {
			pl.terminal = rule
			st.injected[i].Add(1)
		}
	}
	return pl
}

func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, ControlPrefix) {
		p.control(w, r)
		return
	}
	st := p.state()
	st.requests.Add(1)
	pl := st.plan(r)
	ctx := r.Context()

	if !sleep(ctx, pl.before) {
		return
	}
	if t := pl.terminal; t != nil && !t.after() {
		switch {
		case t.Status != 0:
			if t.RetryAfter > 0 {
				w.Header().Set("Retry-After", strconv.Itoa(int((t.RetryAfter+time.Second-1)/time.Second)))
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(t.Status)
			fmt.Fprintf(w, `{"error":"injected by burstforge proxy","status":%d}`+"\n", t.Status)
		case t.Reset:
			reset(w)
		case t.Timeout:
			hold(ctx)
			reset(w)
		}
		return
	}

	resp, body, err := p.forward(r)
	if err != nil {
		st.upErrs.Add(1)
		if ctx.Err() == nil {
			http.Error(w, "burstforge proxy: upstream error: "+err.Error(), http.StatusBadGateway)
		}
		return
	}
	if t := pl.terminal; t != nil {
		// phase: after. The target handled the request; the client won't hear.
		if t.Timeout {
			hold(ctx)
		}
		reset(w)
		return
	}
	if !sleep(ctx, pl.after) {
		return
	}
	for k, vs := range resp.Header {
		if hopHeader(k) {
			continue
		}
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = w.Write(body)
}

func sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func hold(ctx context.Context) {
	t := time.NewTimer(maxHold)
	defer t.Stop()
	select {
	case <-t.C:
	case <-ctx.Done():
	}
}

// reset closes the client connection with a TCP RST.
func reset(w http.ResponseWriter) {
	hj, ok := w.(http.Hijacker)
	if !ok {
		panic(http.ErrAbortHandler)
	}
	conn, _, err := hj.Hijack()
	if err != nil {
		panic(http.ErrAbortHandler)
	}
	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.SetLinger(0)
	}
	_ = conn.Close()
}

var hopHeaders = map[string]bool{
	"Connection": true, "Keep-Alive": true, "Proxy-Authenticate": true, "Proxy-Authorization": true,
	"Te": true, "Trailer": true, "Transfer-Encoding": true, "Upgrade": true, "Content-Length": true,
}

func hopHeader(k string) bool { return hopHeaders[http.CanonicalHeaderKey(k)] }

// forward sends r to the upstream and reads the whole response, so a fault
// in phase "after" can still drop or delay it.
func (p *Proxy) forward(r *http.Request) (*http.Response, []byte, error) {
	u := *p.upstream
	u.Path = strings.TrimRight(p.upstream.Path, "/") + r.URL.Path
	u.RawQuery = r.URL.RawQuery
	out, err := http.NewRequestWithContext(r.Context(), r.Method, u.String(), r.Body)
	if err != nil {
		return nil, nil, err
	}
	out.ContentLength = r.ContentLength
	for k, vs := range r.Header {
		if !hopHeader(k) {
			out.Header[k] = vs
		}
	}
	resp, err := p.client.Do(out)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	return resp, body, err
}

// ---------------------------------------------------------------- control ---

func (p *Proxy) control(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.URL.Path == ControlPrefix+"faults" && r.Method == http.MethodPut:
		var cfg Config
		if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&cfg); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := p.Configure(cfg); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	case r.URL.Path == ControlPrefix+"faults" && r.Method == http.MethodDelete:
		_ = p.Configure(Config{})
		w.WriteHeader(http.StatusNoContent)
	case r.URL.Path == ControlPrefix+"stats" && r.Method == http.MethodGet:
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(p.Stats())
	case r.URL.Path == ControlPrefix+"healthz":
		w.WriteHeader(http.StatusOK)
	default:
		http.NotFound(w, r)
	}
}

// Client drives a proxy's control API.
type Client struct {
	Base string
	HTTP *http.Client
}

func (c *Client) do(ctx context.Context, method, path string, body any, out any) error {
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(c.Base, "/")+ControlPrefix+path, rd)
	if err != nil {
		return err
	}
	hc := c.HTTP
	if hc == nil {
		hc = &http.Client{Timeout: 10 * time.Second}
	}
	resp, err := hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode >= 300 {
		return fmt.Errorf("fault proxy %s %s: %d %s", method, path, resp.StatusCode, strings.TrimSpace(string(b)))
	}
	if out != nil {
		return json.Unmarshal(b, out)
	}
	return nil
}

func (c *Client) Configure(ctx context.Context, cfg Config) error {
	return c.do(ctx, http.MethodPut, "faults", cfg, nil)
}

func (c *Client) Clear(ctx context.Context) error {
	return c.do(ctx, http.MethodDelete, "faults", nil, nil)
}

func (c *Client) Stats(ctx context.Context) (Stats, error) {
	var s Stats
	err := c.do(ctx, http.MethodGet, "stats", nil, &s)
	return s, err
}

// Embedded is a proxy running inside this process on a local port.
type Embedded struct {
	URL string
	srv *http.Server
}

// Start runs p on addr (for example 127.0.0.1:0).
func Start(p *Proxy, addr string) (*Embedded, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	srv := &http.Server{Handler: p, ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = srv.Serve(ln) }()
	return &Embedded{URL: "http://" + ln.Addr().String(), srv: srv}, nil
}

func (e *Embedded) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := e.srv.Shutdown(ctx); err != nil {
		return e.srv.Close()
	}
	return nil
}
