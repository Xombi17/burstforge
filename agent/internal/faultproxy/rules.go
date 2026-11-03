// Package faultproxy is an HTTP proxy that injects faults between the load
// agents and a staging target: latency, error statuses, connection resets
// and timeouts, per path, at a given rate.
//
// It works at the HTTP level. It cannot drop individual packets; the "lossy"
// presets approximate what loss does to a request (a retransmission-sized
// delay for some, a reset for a few). For real packet loss use tc/netem.
package faultproxy

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// Rule injects one kind of fault into matching requests. Exactly one of
// Latency, Status, Reset and Timeout is set.
type Rule struct {
	Name   string  `yaml:"name" json:"name,omitempty"`
	Path   string  `yaml:"path" json:"path,omitempty"`     // prefix; empty matches every path
	Method string  `yaml:"method" json:"method,omitempty"` // empty matches every method
	Rate   float64 `yaml:"rate" json:"rate"`               // fraction of matching requests; 0 means 1

	// Phase "before" (default) acts before the target sees the request.
	// "after" forwards it first, then delays or drops the response: the
	// target did the work but the client doesn't find out.
	Phase string `yaml:"phase" json:"phase,omitempty"`

	Latency    time.Duration `yaml:"latency" json:"latency,omitempty"`
	Jitter     time.Duration `yaml:"jitter" json:"jitter,omitempty"` // latency +/- up to this much
	Status     int           `yaml:"status" json:"status,omitempty"`
	RetryAfter time.Duration `yaml:"retry_after" json:"retry_after,omitempty"`
	Reset      bool          `yaml:"reset" json:"reset,omitempty"`
	Timeout    bool          `yaml:"timeout" json:"timeout,omitempty"` // hold until the client gives up

	// With Every and For set, the rule is active for For out of every Every,
	// counted from the run's start time.
	Every time.Duration `yaml:"every" json:"every,omitempty"`
	For   time.Duration `yaml:"for" json:"for,omitempty"`
}

func (r Rule) rate() float64 {
	if r.Rate == 0 {
		return 1
	}
	return r.Rate
}

func (r Rule) after() bool { return r.Phase == "after" }

// Validate reports every problem with the rule.
func (r Rule) Validate() error {
	var errs []error
	add := func(f string, a ...any) { errs = append(errs, fmt.Errorf(f, a...)) }
	actions := 0
	if r.Latency > 0 {
		actions++
	}
	if r.Status != 0 {
		actions++
	}
	if r.Reset {
		actions++
	}
	if r.Timeout {
		actions++
	}
	if actions != 1 {
		add("set exactly one of latency, status, reset, timeout")
	}
	if r.Rate < 0 || r.Rate > 1 {
		add("rate must be between 0 and 1")
	}
	if r.Path != "" && !strings.HasPrefix(r.Path, "/") {
		add("path must start with /")
	}
	if r.Method != "" && r.Method != strings.ToUpper(r.Method) {
		add("method must be upper case")
	}
	switch r.Phase {
	case "", "before":
	case "after":
		if r.Status != 0 {
			add("status faults answer instead of the target, so they can't use phase: after")
		}
	default:
		add("phase must be before or after")
	}
	if r.Status != 0 && (r.Status < 400 || r.Status > 599) {
		add("status must be 4xx or 5xx")
	}
	if r.RetryAfter != 0 && r.Status == 0 {
		add("retry_after only applies to status faults")
	}
	if r.Jitter < 0 || (r.Jitter > 0 && r.Jitter > r.Latency) {
		add("jitter must be between 0 and latency")
	}
	if (r.Every == 0) != (r.For == 0) {
		add("every and for go together")
	} else if r.Every != 0 && (r.For <= 0 || r.For > r.Every) {
		add("for must be between 0 and every")
	}
	return errors.Join(errs...)
}

// String describes the rule for reports.
func (r Rule) String() string {
	where := r.Path
	if where == "" {
		where = "all paths"
	}
	if r.Method != "" {
		where = r.Method + " " + where
	}
	var what string
	switch {
	case r.Latency > 0:
		what = "+" + r.Latency.String()
		if r.Jitter > 0 {
			what += " ±" + r.Jitter.String()
		}
	case r.Status != 0:
		what = fmt.Sprintf("HTTP %d", r.Status)
		if r.RetryAfter > 0 {
			what += fmt.Sprintf(" (Retry-After %s)", r.RetryAfter)
		}
	case r.Reset:
		what = "connection reset"
	case r.Timeout:
		what = "no response (timeout)"
	}
	if r.after() {
		what += " after the target handled it"
	}
	if r.rate() < 1 {
		what += fmt.Sprintf(", %g%% of requests", r.rate()*100)
	}
	if r.Every > 0 {
		what += fmt.Sprintf(", for %s of every %s", r.For, r.Every)
	}
	s := where + ": " + what
	if r.Name != "" {
		s = r.Name + " (" + s + ")"
	}
	return s
}

// Presets are network profiles. They are approximations at the HTTP level.
var Presets = map[string][]Rule{
	"high-rtt": {
		{Name: "high RTT", Latency: 300 * time.Millisecond, Jitter: 100 * time.Millisecond},
	},
	"lossy": {
		{Name: "loss: retransmission delay", Latency: time.Second, Jitter: 200 * time.Millisecond, Rate: 0.02},
		{Name: "loss: connection reset", Reset: true, Rate: 0.005},
	},
	"intermittent": {
		{Name: "intermittent outage", Reset: true, Every: 10 * time.Second, For: time.Second},
	},
	"mobile-3g": {
		{Name: "3G RTT", Latency: 200 * time.Millisecond, Jitter: 100 * time.Millisecond},
		{Name: "loss: retransmission delay", Latency: time.Second, Jitter: 200 * time.Millisecond, Rate: 0.02},
		{Name: "loss: connection reset", Reset: true, Rate: 0.005},
	},
}

// Expand returns the preset's rules followed by the explicit ones.
func Expand(preset string, rules []Rule) ([]Rule, error) {
	if preset == "" {
		return rules, nil
	}
	p, ok := Presets[preset]
	if !ok {
		names := make([]string, 0, len(Presets))
		for n := range Presets {
			names = append(names, n)
		}
		return nil, fmt.Errorf("unknown fault preset %q (have %s)", preset, strings.Join(sortStrings(names), ", "))
	}
	return append(append([]Rule{}, p...), rules...), nil
}

func sortStrings(s []string) []string {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
	return s
}
