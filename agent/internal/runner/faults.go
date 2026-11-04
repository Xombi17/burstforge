package runner

import (
	"context"
	"fmt"
	"time"

	"burstforge/internal/faultproxy"
	"burstforge/internal/report"
	"burstforge/internal/scenario"
)

// FaultSession puts a fault proxy between the agents and the target for one
// run. Workflow traffic goes to URL; hooks, snapshots and invariants keep
// going to the target directly, so faults never distort the checks.
type FaultSession struct {
	URL      string // where agents send workflow traffic
	where    string
	client   *faultproxy.Client
	embedded *faultproxy.Embedded
	rules    []faultproxy.Rule
}

// StartFaults returns nil if the scenario injects no faults. Otherwise it
// starts an embedded proxy on listen (or checks the configured one is up).
func StartFaults(ctx context.Context, sc *scenario.Scenario, listen string) (*FaultSession, error) {
	if !sc.Faults.Enabled() {
		return nil, nil
	}
	rules, err := faultproxy.Expand(sc.Faults.Preset, sc.Faults.Rules)
	if err != nil {
		return nil, err
	}
	fs := &FaultSession{rules: rules}
	if sc.Faults.Proxy != "" {
		fs.URL, fs.where = sc.Faults.Proxy, sc.Faults.Proxy
		fs.client = &faultproxy.Client{Base: sc.Faults.Proxy}
		if _, err := fs.client.Stats(ctx); err != nil {
			return nil, fmt.Errorf("fault proxy at %s is not answering: %w", sc.Faults.Proxy, err)
		}
		return fs, nil
	}
	p, err := faultproxy.New(sc.Target.BaseURL)
	if err != nil {
		return nil, err
	}
	if fs.embedded, err = faultproxy.Start(p, listen); err != nil {
		return nil, fmt.Errorf("starting the fault proxy: %w", err)
	}
	fs.URL, fs.where = fs.embedded.URL, "embedded"
	fs.client = &faultproxy.Client{Base: fs.embedded.URL}
	return fs, nil
}

// Apply loads the rules. Fault decisions depend on seed, so the same seed
// faults the same requests; every/for windows count from startAt.
func (fs *FaultSession) Apply(ctx context.Context, seed int64, startAt time.Time) error {
	return fs.client.Configure(ctx, faultproxy.Config{Seed: seed, StartAt: startAt, Rules: fs.rules})
}

// Collect reads what was injected and clears the rules.
func (fs *FaultSession) Collect(logf func(string, ...any)) *report.FaultReport {
	if fs == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	st, err := fs.client.Stats(ctx)
	if err != nil {
		logf("reading fault proxy stats: %v", err)
		return nil
	}
	if err := fs.client.Clear(ctx); err != nil {
		logf("clearing fault rules: %v", err)
	}
	out := &report.FaultReport{Proxy: fs.where, Requests: st.Requests, UpstreamErrors: st.UpstreamErrors}
	for _, r := range st.Rules {
		out.Rules = append(out.Rules, report.FaultRuleStat{Rule: r.Rule, Matched: r.Matched, Injected: r.Injected})
	}
	return out
}

func (fs *FaultSession) Close() {
	if fs != nil && fs.embedded != nil {
		_ = fs.embedded.Close()
	}
}

// Embedded reports whether the proxy runs inside this process (and so is
// only reachable from this machine).
func (fs *FaultSession) Embedded() bool { return fs.embedded != nil }
