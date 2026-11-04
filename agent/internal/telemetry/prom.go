// Package telemetry exports what agents do while a run is going: live
// Prometheus metrics, and spans over OTLP/HTTP.
package telemetry

import (
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"burstforge/internal/runner"
)

// Buckets for attempt and workflow durations, in seconds.
var buckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30}

// Prometheus collects per-run metrics on an agent. Series carry run, org
// and shard labels; a run's series are dropped 15 minutes after its last
// event so a long-lived agent doesn't grow without bound.
type Prometheus struct {
	mu      sync.Mutex
	now     func() time.Time
	counter map[string]map[string]float64 // family -> label string -> value
	hist    map[string]map[string]*histogram
	lastRun map[string]time.Time
	extra   []func(w io.Writer)
}

type histogram struct {
	counts []uint64 // per bucket, cumulative at render time
	count  uint64
	sum    float64
}

func NewPrometheus() *Prometheus {
	return &Prometheus{
		now:     time.Now,
		counter: map[string]map[string]float64{},
		hist:    map[string]map[string]*histogram{},
		lastRun: map[string]time.Time{},
	}
}

var help = map[string]string{
	"burstforge_agent_attempts_total":            "HTTP attempts sent by this agent, by run, step, kind and outcome class.",
	"burstforge_agent_attempt_duration_seconds":  "HTTP attempt latency, by run and step.",
	"burstforge_agent_workflows_total":           "Workflows finished by this agent, by run and result.",
	"burstforge_agent_workflow_duration_seconds": "Workflow latency, by run.",
}

// ForRun returns the observer for one shard of one run.
func (p *Prometheus) ForRun(run, org string, shard int) runner.Observer {
	return &promRun{p: p, run: run, base: fmt.Sprintf(`run="%s",org="%s",shard="%d"`, esc(run), esc(org), shard)}
}

// Also adds lines (other gauges) to every scrape.
func (p *Prometheus) Also(f func(w io.Writer)) { p.extra = append(p.extra, f) }

type promRun struct {
	p    *Prometheus
	run  string
	base string
}

func (r *promRun) Attempt(a runner.AttemptEvent) {
	p := r.p
	p.mu.Lock()
	defer p.mu.Unlock()
	p.lastRun[r.run] = p.now()
	p.inc("burstforge_agent_attempts_total",
		fmt.Sprintf(`%s,step="%s",kind="%s",class="%s"`, r.base, esc(a.Step), a.Kind, a.Class), 1)
	if a.Duration >= 0 {
		p.observe("burstforge_agent_attempt_duration_seconds", fmt.Sprintf(`%s,step="%s"`, r.base, esc(a.Step)), a.Duration.Seconds())
	}
}

func (r *promRun) Workflow(w runner.WorkflowEvent) {
	p := r.p
	p.mu.Lock()
	defer p.mu.Unlock()
	p.lastRun[r.run] = p.now()
	result := "ok"
	if !w.OK {
		result = "failed"
	}
	p.inc("burstforge_agent_workflows_total", fmt.Sprintf(`%s,result="%s"`, r.base, result), 1)
	p.observe("burstforge_agent_workflow_duration_seconds", r.base, w.Duration.Seconds())
}

func (p *Prometheus) inc(fam, labels string, v float64) {
	m := p.counter[fam]
	if m == nil {
		m = map[string]float64{}
		p.counter[fam] = m
	}
	m[labels] += v
}

func (p *Prometheus) observe(fam, labels string, v float64) {
	m := p.hist[fam]
	if m == nil {
		m = map[string]*histogram{}
		p.hist[fam] = m
	}
	h := m[labels]
	if h == nil {
		h = &histogram{counts: make([]uint64, len(buckets))}
		m[labels] = h
	}
	for i, b := range buckets {
		if v <= b {
			h.counts[i]++
			break
		}
	}
	h.count++
	h.sum += v
}

// prune drops the series of runs that have been quiet for 15 minutes.
func (p *Prometheus) prune() {
	cutoff := p.now().Add(-15 * time.Minute)
	for run, t := range p.lastRun {
		if t.After(cutoff) {
			continue
		}
		prefix := fmt.Sprintf(`run="%s",`, esc(run))
		for _, m := range p.counter {
			for k := range m {
				if strings.HasPrefix(k, prefix) {
					delete(m, k)
				}
			}
		}
		for _, m := range p.hist {
			for k := range m {
				if strings.HasPrefix(k, prefix) {
					delete(m, k)
				}
			}
		}
		delete(p.lastRun, run)
	}
}

// ServeHTTP renders the text exposition format.
func (p *Prometheus) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	p.mu.Lock()
	p.prune()
	var b strings.Builder
	for _, fam := range sortedKeys(p.counter) {
		fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s counter\n", fam, help[fam], fam)
		for _, l := range sortedKeys(p.counter[fam]) {
			fmt.Fprintf(&b, "%s{%s} %g\n", fam, l, p.counter[fam][l])
		}
	}
	for _, fam := range sortedKeys(p.hist) {
		fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s histogram\n", fam, help[fam], fam)
		for _, l := range sortedKeys(p.hist[fam]) {
			h := p.hist[fam][l]
			var cum uint64
			for i, ub := range buckets {
				cum += h.counts[i]
				fmt.Fprintf(&b, "%s_bucket{%s,le=\"%g\"} %d\n", fam, l, ub, cum)
			}
			fmt.Fprintf(&b, "%s_bucket{%s,le=\"+Inf\"} %d\n", fam, l, h.count)
			fmt.Fprintf(&b, "%s_sum{%s} %g\n%s_count{%s} %d\n", fam, l, h.sum, fam, l, h.count)
		}
	}
	p.mu.Unlock()
	for _, f := range p.extra {
		f(&b)
	}
	_, _ = io.WriteString(w, b.String())
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func esc(s string) string {
	return strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`).Replace(s)
}
