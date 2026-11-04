// Package scenario defines the YAML workflow format and validates it.
package scenario

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"burstforge/internal/faultproxy"
)

type Scenario struct {
	Name        string `yaml:"name"`
	Description string `yaml:"description"`

	Target     Target      `yaml:"target"`
	Hooks      Hooks       `yaml:"hooks"`
	Workflow   Workflow    `yaml:"workflow"`
	Traffic    Traffic     `yaml:"traffic"`
	Observe    Observe     `yaml:"observe"`
	Thresholds Thresholds  `yaml:"thresholds"`
	Invariants []Invariant `yaml:"invariants"`
	Faults     Faults      `yaml:"faults"`
}

// Faults are injected by a proxy between the agents and the target. Hooks,
// counter snapshots and invariant checks go to the target directly.
type Faults struct {
	// Proxy is the URL of a running `burstforge proxy`. Empty means the
	// coordinator starts one on 127.0.0.1, which only local agents can reach.
	Proxy  string            `yaml:"proxy"`
	Preset string            `yaml:"preset"` // high-rtt | lossy | intermittent | mobile-3g
	Rules  []faultproxy.Rule `yaml:"rules"`
}

func (f Faults) Enabled() bool { return f.Preset != "" || len(f.Rules) > 0 }

type Target struct {
	BaseURL     string   `yaml:"base_url"`
	Environment string   `yaml:"environment"` // local | staging
	AllowHosts  []string `yaml:"allow_hosts"`
}

type Hooks struct {
	Before []Call `yaml:"before"`
	After  []Call `yaml:"after"`
}

// Call is a one-off control request (reset state, inject a fault, ...).
type Call struct {
	Method  string            `yaml:"method"`
	Path    string            `yaml:"path"`
	Headers map[string]string `yaml:"headers"`
	Body    any               `yaml:"body"`
}

type Workflow struct {
	Steps []Step `yaml:"steps"`
}

type Step struct {
	Name     string            `yaml:"name"`
	Method   string            `yaml:"method"`
	Path     string            `yaml:"path"`
	Headers  map[string]string `yaml:"headers"`
	Body     any               `yaml:"body"`
	Timeout  time.Duration     `yaml:"timeout"`
	OKStatus []int             `yaml:"ok_status"` // default: any 2xx
	Capture  map[string]string `yaml:"capture"`   // var -> json path in response
	Retry    Retry             `yaml:"retry"`
}

type Retry struct {
	MaxAttempts       int           `yaml:"max_attempts"`
	On                []string      `yaml:"on"`      // timeout, conn, 429, 5xx
	Backoff           string        `yaml:"backoff"` // none | fixed | exponential
	BaseDelay         time.Duration `yaml:"base_delay"`
	MaxDelay          time.Duration `yaml:"max_delay"`
	Jitter            string        `yaml:"jitter"` // none | full
	RespectRetryAfter bool          `yaml:"respect_retry_after"`
}

type Traffic struct {
	Profile           string        `yaml:"profile"` // burst | ramp | sustained
	Arrivals          int           `yaml:"arrivals"`
	Window            time.Duration `yaml:"window"`   // burst
	Duration          time.Duration `yaml:"duration"` // ramp, sustained
	StartDelay        time.Duration `yaml:"start_delay"`
	DoubleSubmit      float64       `yaml:"double_submit"` // fraction of users who click twice
	DoubleSubmitDelay time.Duration `yaml:"double_submit_delay"`
	MaxInflight       int           `yaml:"max_inflight"`
}

// Observe names a target endpoint whose numeric counters are diffed across the run.
type Observe struct {
	URL      string   `yaml:"url"`
	Counters []string `yaml:"counters"`
}

type Thresholds struct {
	P99Ms              *float64 `yaml:"p99_ms"`
	ErrorRate          *float64 `yaml:"error_rate"`
	RetryAmplification *float64 `yaml:"retry_amplification"`
	ArrivalSkewP99Ms   *float64 `yaml:"arrival_skew_p99_ms"`
}

// Invariant is a business-correctness check run against the target after load.
type Invariant struct {
	Name       string        `yaml:"name"`
	Path       string        `yaml:"path"`
	JSON       string        `yaml:"json"`
	Op         string        `yaml:"op"` // eq lt lte gt gte
	Value      float64       `yaml:"value"`
	Eventually time.Duration `yaml:"eventually"` // poll until true, up to this long
}

// Builtin template variables available in every step.
var builtinVars = []string{"n", "logical_id", "run_id"}

var varRe = regexp.MustCompile(`\{\{\s*([A-Za-z_][A-Za-z0-9_]*)\s*\}\}`)

// Load reads, defaults and validates a scenario file.
func Load(path string) (*Scenario, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	sc, err := Parse(raw, strings.TrimSuffix(filepath.Base(path), filepath.Ext(path)))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return sc, nil
}

// Parse decodes, defaults and validates a scenario. name is used if the
// scenario doesn't set one.
func Parse(raw []byte, name string) (*Scenario, error) {
	sc, err := Decode(raw, name)
	if err != nil {
		return nil, err
	}
	if err := sc.Validate(); err != nil {
		return nil, err
	}
	return sc, nil
}

// Decode decodes and defaults a scenario without validating it, for callers
// that change fields (the target, for example) before validating.
func Decode(raw []byte, name string) (*Scenario, error) {
	var sc Scenario
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true)
	if err := dec.Decode(&sc); err != nil {
		return nil, err
	}
	if sc.Name == "" {
		sc.Name = name
	}
	sc.applyDefaults()
	return &sc, nil
}

func (s *Scenario) applyDefaults() {
	for i := range s.Workflow.Steps {
		st := &s.Workflow.Steps[i]
		if st.Method == "" {
			st.Method = "GET"
		}
		st.Method = strings.ToUpper(st.Method)
		if st.Timeout == 0 {
			st.Timeout = 10 * time.Second
		}
		if st.Retry.MaxAttempts == 0 {
			st.Retry.MaxAttempts = 1
		}
		if st.Retry.Backoff == "" {
			st.Retry.Backoff = "none"
		}
		if st.Retry.Jitter == "" {
			st.Retry.Jitter = "none"
		}
		if len(st.Retry.On) == 0 {
			st.Retry.On = []string{"timeout", "conn", "429", "5xx"}
		}
	}
	for i := range s.Hooks.Before {
		s.Hooks.Before[i].Method = defaultMethod(s.Hooks.Before[i].Method)
	}
	for i := range s.Hooks.After {
		s.Hooks.After[i].Method = defaultMethod(s.Hooks.After[i].Method)
	}
	if s.Traffic.MaxInflight == 0 {
		s.Traffic.MaxInflight = 5000
	}
	if s.Traffic.DoubleSubmitDelay == 0 {
		s.Traffic.DoubleSubmitDelay = 100 * time.Millisecond
	}
	if s.Traffic.StartDelay == 0 {
		s.Traffic.StartDelay = time.Second
	}
	for i := range s.Invariants {
		if s.Invariants[i].Op == "" {
			s.Invariants[i].Op = "eq"
		}
	}
}

func defaultMethod(m string) string {
	if m == "" {
		return "POST"
	}
	return strings.ToUpper(m)
}

// Validate reports every problem it finds, not just the first.
func (s *Scenario) Validate() error {
	var errs []error
	add := func(format string, a ...any) { errs = append(errs, fmt.Errorf(format, a...)) }

	u, err := url.Parse(s.Target.BaseURL)
	switch {
	case s.Target.BaseURL == "":
		add("target.base_url is required")
	case err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "":
		add("target.base_url must be an http(s) URL, got %q", s.Target.BaseURL)
	case u.User != nil || u.RawQuery != "" || u.Fragment != "":
		add("target.base_url must not contain credentials, a query or a fragment")
	}
	// Every other URL is base_url + path. A path that doesn't start with /
	// could change the host ("@other-host/...").
	for i, h := range s.Hooks.Before {
		if !strings.HasPrefix(h.Path, "/") {
			add("hooks.before[%d].path must start with /", i)
		}
	}
	for i, h := range s.Hooks.After {
		if !strings.HasPrefix(h.Path, "/") {
			add("hooks.after[%d].path must start with /", i)
		}
	}
	if s.Observe.URL != "" && !strings.HasPrefix(s.Observe.URL, "/") {
		add("observe.url must be a path on the target starting with /")
	}
	if s.Target.Environment != "local" && s.Target.Environment != "staging" {
		add("target.environment must be \"local\" or \"staging\" (production targets are not supported)")
	}

	if len(s.Workflow.Steps) == 0 {
		add("workflow.steps needs at least one step")
	}
	known := map[string]bool{}
	for _, v := range builtinVars {
		known[v] = true
	}
	seen := map[string]bool{}
	validMethods := map[string]bool{"GET": true, "POST": true, "PUT": true, "PATCH": true, "DELETE": true}
	validOn := map[string]bool{"timeout": true, "conn": true, "429": true, "5xx": true}
	for i, st := range s.Workflow.Steps {
		at := fmt.Sprintf("workflow.steps[%d]", i)
		if st.Name == "" {
			add("%s.name is required", at)
		} else if seen[st.Name] {
			add("%s.name %q is duplicated", at, st.Name)
		}
		seen[st.Name] = true
		if !validMethods[st.Method] {
			add("%s.method %q is not supported", at, st.Method)
		}
		if !strings.HasPrefix(st.Path, "/") {
			add("%s.path must start with /", at)
		}
		if st.Retry.MaxAttempts < 1 || st.Retry.MaxAttempts > 10 {
			add("%s.retry.max_attempts must be 1..10", at)
		}
		switch st.Retry.Backoff {
		case "none", "fixed", "exponential":
		default:
			add("%s.retry.backoff must be none, fixed or exponential", at)
		}
		if st.Retry.Backoff != "none" && st.Retry.BaseDelay <= 0 {
			add("%s.retry.base_delay is required when backoff is %s", at, st.Retry.Backoff)
		}
		if st.Retry.Jitter != "none" && st.Retry.Jitter != "full" {
			add("%s.retry.jitter must be none or full", at)
		}
		for _, on := range st.Retry.On {
			if !validOn[on] {
				add("%s.retry.on contains unknown value %q", at, on)
			}
		}
		for _, name := range referencedVars(st.Path, st.Headers, st.Body) {
			if !known[name] {
				add("%s references {{%s}} which is not a builtin or an earlier capture", at, name)
			}
		}
		for name := range st.Capture {
			known[name] = true
		}
	}

	t := s.Traffic
	switch t.Profile {
	case "burst":
		if t.Window < 0 {
			add("traffic.window must not be negative")
		}
	case "ramp", "sustained":
		if t.Duration <= 0 {
			add("traffic.duration is required for the %s profile", t.Profile)
		}
	default:
		add("traffic.profile must be burst, ramp or sustained")
	}
	if t.Arrivals <= 0 {
		add("traffic.arrivals must be > 0")
	}
	if t.DoubleSubmit < 0 || t.DoubleSubmit > 1 {
		add("traffic.double_submit must be between 0 and 1")
	}

	if len(s.Observe.Counters) > 0 && s.Observe.URL == "" {
		add("observe.url is required when observe.counters is set")
	}
	for i, inv := range s.Invariants {
		at := fmt.Sprintf("invariants[%d]", i)
		if inv.Name == "" || inv.Path == "" || inv.JSON == "" {
			add("%s needs name, path and json", at)
		} else if !strings.HasPrefix(inv.Path, "/") {
			add("%s.path must start with /", at)
		}
		switch inv.Op {
		case "eq", "lt", "lte", "gt", "gte":
		default:
			add("%s.op must be eq, lt, lte, gt or gte", at)
		}
	}
	if f := s.Faults; f.Proxy != "" || f.Enabled() {
		if f.Proxy != "" {
			if u, err := url.Parse(f.Proxy); err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
				add("faults.proxy must be an http(s) URL, got %q", f.Proxy)
			}
			if !f.Enabled() {
				add("faults.proxy is set but there is no preset and no rules")
			}
		}
		if _, err := faultproxy.Expand(f.Preset, nil); err != nil {
			add("faults.preset: %v", err)
		}
		for i, r := range f.Rules {
			if err := r.Validate(); err != nil {
				add("faults.rules[%d]: %v", i, err)
			}
		}
	}
	return errors.Join(errs...)
}

// CheckTarget enforces the safety rules: explicit environment, and a host that
// is loopback, private, or listed in target.allow_hosts.
func (s *Scenario) CheckTarget() error {
	if err := CheckHost(s.Target.BaseURL, s.Target.AllowHosts); err != nil {
		return err
	}
	if s.Faults.Proxy != "" {
		if err := CheckHost(s.Faults.Proxy, s.Target.AllowHosts); err != nil {
			return fmt.Errorf("faults.proxy: %w", err)
		}
	}
	return nil
}

// CheckHost refuses URLs whose host is not loopback, private or allowed.
func CheckHost(raw string, allow []string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return err
	}
	host := u.Hostname()
	for _, h := range allow {
		if strings.EqualFold(h, host) {
			return nil
		}
	}
	if strings.EqualFold(host, "localhost") || strings.HasSuffix(strings.ToLower(host), ".localhost") {
		return nil
	}
	if ip := net.ParseIP(host); ip != nil && (ip.IsLoopback() || ip.IsPrivate()) {
		return nil
	}
	return fmt.Errorf("refusing to test %q: host is not loopback or private and is not listed in target.allow_hosts. "+
		"BurstForge only drives load at targets you have explicitly allowed", host)
}

// Render replaces {{var}} placeholders in a string.
func Render(s string, vars map[string]string) string {
	return varRe.ReplaceAllStringFunc(s, func(m string) string {
		name := varRe.FindStringSubmatch(m)[1]
		if v, ok := vars[name]; ok {
			return v
		}
		return m
	})
}

// RenderValue deep-copies v, rendering every string it finds.
func RenderValue(v any, vars map[string]string) any {
	switch t := v.(type) {
	case string:
		return Render(t, vars)
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, val := range t {
			out[k] = RenderValue(val, vars)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, val := range t {
			out[i] = RenderValue(val, vars)
		}
		return out
	default:
		return v
	}
}

func referencedVars(path string, headers map[string]string, body any) []string {
	set := map[string]bool{}
	collect := func(s string) {
		for _, m := range varRe.FindAllStringSubmatch(s, -1) {
			set[m[1]] = true
		}
	}
	collect(path)
	for _, h := range headers {
		collect(h)
	}
	var walk func(any)
	walk = func(v any) {
		switch t := v.(type) {
		case string:
			collect(t)
		case map[string]any:
			for _, val := range t {
				walk(val)
			}
		case []any:
			for _, val := range t {
				walk(val)
			}
		}
	}
	walk(body)
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// Hash fingerprints the scenario after defaults and command-line overrides.
// Shards of one run must carry the same hash before their reports merge.
func (s *Scenario) Hash() string {
	b, err := json.Marshal(s)
	if err != nil {
		// Bodies come from YAML, so they are always JSON-encodable maps,
		// slices and scalars. Fall back to %#v rather than panic.
		b = []byte(fmt.Sprintf("%#v", s))
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:8])
}
