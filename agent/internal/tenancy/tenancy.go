// Package tenancy holds the checks for runs that belong to an organization:
// per-run limits, binding a scenario to the organization's environment, and
// the approval token a target must serve before anyone may load it.
//
// The control plane, the controller and every agent run these checks. An
// agent does not trust the run row: it re-reads the organization's limits
// and environment and asks the target for the approval token itself.
package tenancy

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"burstforge/internal/scenario"
	"burstforge/internal/schedule"
)

// ApprovalPath is where a target publishes the approval tokens of the
// organizations allowed to test it, one per line.
const ApprovalPath = "/.well-known/burstforge-approval.txt"

// Limits are an organization's quotas. Zero means zero, not unlimited.
type Limits struct {
	MaxConcurrentRuns int
	MaxRunsPerHour    int
	MaxArrivals       int           // per run
	MaxArrivalRate    int           // arrivals in any one-second window
	MaxDuration       time.Duration // planned span of the arrivals
}

// Environment is a target an organization may test.
type Environment struct {
	Org           string
	Name          string
	BaseURL       string
	ApprovalToken string
	Verified      bool
}

// Shape is what a scenario asks of the target, computed from its seeded plan.
type Shape struct {
	Arrivals    int
	PeakPerSec  int
	PlannedSpan time.Duration
}

// Measure computes the scenario's shape for this seed.
func Measure(sc *scenario.Scenario, seed int64) (Shape, error) {
	offsets, err := schedule.Plan(schedule.Profile{
		Kind: sc.Traffic.Profile, Arrivals: sc.Traffic.Arrivals,
		Window: sc.Traffic.Window, Duration: sc.Traffic.Duration,
	}, seed)
	if err != nil {
		return Shape{}, err
	}
	return Shape{Arrivals: len(offsets), PeakPerSec: peak(offsets, time.Second), PlannedSpan: span(offsets)}, nil
}

// peak is the most offsets that fall in any half-open window of length w.
func peak(offsets []time.Duration, w time.Duration) int {
	s := append([]time.Duration(nil), offsets...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	best, lo := 0, 0
	for hi := range s {
		for s[hi]-s[lo] >= w {
			lo++
		}
		best = max(best, hi-lo+1)
	}
	return best
}

func span(offsets []time.Duration) time.Duration {
	var m time.Duration
	for _, o := range offsets {
		m = max(m, o)
	}
	return m
}

// CheckLimits reports every limit the shape exceeds.
func CheckLimits(s Shape, l Limits) error {
	var errs []error
	if s.Arrivals > l.MaxArrivals {
		errs = append(errs, fmt.Errorf("quota max_arrivals: the run has %d arrivals, the limit is %d", s.Arrivals, l.MaxArrivals))
	}
	if s.PeakPerSec > l.MaxArrivalRate {
		errs = append(errs, fmt.Errorf("quota max_arrival_rate: the run peaks at %d arrivals in one second, the limit is %d", s.PeakPerSec, l.MaxArrivalRate))
	}
	if s.PlannedSpan > l.MaxDuration {
		errs = append(errs, fmt.Errorf("quota max_duration: the arrivals span %s, the limit is %s", s.PlannedSpan, l.MaxDuration))
	}
	return errors.Join(errs...)
}

// origin is scheme://host[:port], lower-cased, with default ports dropped.
func origin(raw string) (scheme, host string, err error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", "", err
	}
	if u.Scheme != "http" && u.Scheme != "https" || u.Host == "" || u.User != nil {
		return "", "", fmt.Errorf("%q is not a plain http(s) URL", raw)
	}
	host = strings.ToLower(u.Hostname())
	if p := u.Port(); p != "" && !(u.Scheme == "http" && p == "80") && !(u.Scheme == "https" && p == "443") {
		host += ":" + p
	}
	return u.Scheme, host, nil
}

// Bind points the scenario at the environment: its base_url becomes the
// environment's, and the environment's host is the only allowed host.
// Whatever target the scenario named is ignored. Faults run through a proxy
// the controller starts, so a scenario may not name its own.
func Bind(sc *scenario.Scenario, env Environment) error {
	if sc.Faults.Proxy != "" {
		return errors.New("faults.proxy can't be set for runs submitted to the control plane; the controller runs the proxy")
	}
	u, err := url.Parse(env.BaseURL)
	if err != nil {
		return err
	}
	sc.Target.BaseURL = strings.TrimRight(env.BaseURL, "/")
	sc.Target.AllowHosts = []string{u.Hostname()}
	return nil
}

// CheckBound verifies that a stored scenario still targets only the
// environment. Agents run it on every claim.
func CheckBound(sc *scenario.Scenario, env Environment) error {
	ws, wh, err := origin(env.BaseURL)
	if err != nil {
		return fmt.Errorf("environment %s: %w", env.Name, err)
	}
	gs, gh, err := origin(sc.Target.BaseURL)
	if err != nil {
		return err
	}
	if gs != ws || gh != wh {
		return fmt.Errorf("the run targets %s://%s but environment %s is %s://%s", gs, gh, env.Name, ws, wh)
	}
	eu, _ := url.Parse(env.BaseURL) // origin parsed it already
	envHost := eu.Hostname()
	for _, h := range sc.Target.AllowHosts {
		if !strings.EqualFold(h, envHost) {
			return fmt.Errorf("the run allows host %q, which is not environment %s", h, env.Name)
		}
	}
	if sc.Faults.Proxy != "" {
		return errors.New("the run names its own fault proxy")
	}
	return nil
}

// CheckApproval asks the target for its approval file and looks for the
// token on a line of its own. It doesn't follow redirects.
func CheckApproval(ctx context.Context, baseURL, token string) error {
	if token == "" {
		return errors.New("the environment has no approval token")
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	u := strings.TrimRight(baseURL, "/") + ApprovalPath
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return err
	}
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("fetching %s: %w", u, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s returned HTTP %d; the target must serve the approval token there", u, resp.StatusCode)
	}
	sc := bufio.NewScanner(io.LimitReader(resp.Body, 64<<10))
	for sc.Scan() {
		if strings.TrimSpace(sc.Text()) == token {
			return nil
		}
	}
	return fmt.Errorf("%s doesn't list this organization's approval token", u)
}
