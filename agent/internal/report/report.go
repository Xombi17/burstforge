// Package report holds the run result schema and its renderers.
package report

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"
)

const SchemaVersion = 1

type Report struct {
	Schema      int       `json:"schema"`
	Tool        string    `json:"tool"`
	Scenario    string    `json:"scenario"`
	RunID       string    `json:"run_id"`
	StartedAt   time.Time `json:"started_at"`
	FinishedAt  time.Time `json:"finished_at"`
	Seed        int64     `json:"seed"`
	Target      string    `json:"target"`
	Environment string    `json:"environment"`

	Traffic     Traffic          `json:"traffic"`
	Totals      Totals           `json:"totals"`
	Attempts    Attempts         `json:"attempts"`
	Throughput  Throughput       `json:"throughput"`
	Latency     LatencySet       `json:"latency"`
	ArrivalSkew Latency          `json:"arrival_skew"`
	TargetStats *TargetStats     `json:"target_stats,omitempty"`
	Invariants  []InvariantCheck `json:"invariants"`
	Thresholds  []ThresholdCheck `json:"thresholds"`
	Timeline    []TimelinePoint  `json:"timeline"`
	Verdict     Verdict          `json:"verdict"`
}

type Traffic struct {
	Profile      string  `json:"profile"`
	Arrivals     int     `json:"arrivals"`
	PlannedSpan  float64 `json:"planned_span_ms"`
	DoubleSubmit float64 `json:"double_submit"`
}

type Totals struct {
	Logical       int            `json:"logical"`
	Succeeded     int            `json:"succeeded"`
	Failed        int            `json:"failed"`
	FinalOutcomes map[string]int `json:"final_outcomes"`
	Aborted       bool           `json:"aborted"`
}

type Attempts struct {
	Total              int            `json:"total"`
	ByKind             map[string]int `json:"by_kind"`
	ByClass            map[string]int `json:"by_class"`
	RetryAmplification float64        `json:"retry_amplification"`
}

type Throughput struct {
	DurationSec    float64 `json:"duration_sec"`
	AttemptsPerSec float64 `json:"attempts_per_sec"`
	GoodputPerSec  float64 `json:"goodput_per_sec"`
}

type Latency struct {
	Count  uint64  `json:"count"`
	MeanMs float64 `json:"mean_ms"`
	P50Ms  float64 `json:"p50_ms"`
	P95Ms  float64 `json:"p95_ms"`
	P99Ms  float64 `json:"p99_ms"`
	MaxMs  float64 `json:"max_ms"`
}

type LatencySet struct {
	WorkflowOK  Latency            `json:"workflow_ok"`
	WorkflowAll Latency            `json:"workflow_all"`
	Steps       map[string]Latency `json:"steps"`
}

// TargetStats are counters read from the target before and after the run.
type TargetStats struct {
	Before     map[string]float64 `json:"before"`
	After      map[string]float64 `json:"after"`
	Delta      map[string]float64 `json:"delta"`
	PerLogical map[string]float64 `json:"per_logical"`
}

type InvariantCheck struct {
	Name     string  `json:"name"`
	Pass     bool    `json:"pass"`
	Observed float64 `json:"observed"`
	Op       string  `json:"op"`
	Expected float64 `json:"expected"`
	WaitedMs float64 `json:"waited_ms"`
	Error    string  `json:"error,omitempty"`
}

type ThresholdCheck struct {
	Name     string  `json:"name"`
	Pass     bool    `json:"pass"`
	Observed float64 `json:"observed"`
	Limit    float64 `json:"limit"`
}

type TimelinePoint struct {
	Second   int `json:"second"`
	Attempts int `json:"attempts"`
	OK       int `json:"ok"`
	Failed   int `json:"failed"`
	Retries  int `json:"retries"`
}

type Verdict struct {
	Pass     bool     `json:"pass"`
	Failures []string `json:"failures"`
}

// Finalize computes the verdict from invariants, thresholds and abort state.
func (r *Report) Finalize() {
	var fails []string
	if r.Totals.Aborted {
		fails = append(fails, "run was aborted before completion")
	}
	for _, t := range r.Thresholds {
		if !t.Pass {
			fails = append(fails, fmt.Sprintf("threshold %s: observed %.2f, limit %.2f", t.Name, t.Observed, t.Limit))
		}
	}
	for _, i := range r.Invariants {
		if !i.Pass {
			if i.Error != "" {
				fails = append(fails, fmt.Sprintf("invariant %q could not be checked: %s", i.Name, i.Error))
			} else {
				fails = append(fails, fmt.Sprintf("invariant %q: observed %s, expected %s %s",
					i.Name, num(i.Observed), i.Op, num(i.Expected)))
			}
		}
	}
	r.Verdict = Verdict{Pass: len(fails) == 0, Failures: fails}
}

func (r *Report) Save(path string) error {
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o644)
}

func Load(path string) (*Report, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var r Report
	if err := json.Unmarshal(b, &r); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return &r, nil
}

// ------------------------------------------------------------- rendering ---

func num(f float64) string {
	if f == float64(int64(f)) {
		return fmt.Sprintf("%d", int64(f))
	}
	return fmt.Sprintf("%.2f", f)
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func mark(ok bool) string {
	if ok {
		return "PASS"
	}
	return "FAIL"
}

// Text renders the terminal summary.
func (r *Report) Text() string {
	var b strings.Builder
	w := func(format string, a ...any) { fmt.Fprintf(&b, format+"\n", a...) }

	w("")
	w("BurstForge  %s  (run %s)", r.Scenario, r.RunID)
	w("target %s [%s]   profile %s   seed %d", r.Target, r.Environment, r.Traffic.Profile, r.Seed)
	w("")
	w("  workflows     %d started, %d ok, %d failed", r.Totals.Logical, r.Totals.Succeeded, r.Totals.Failed)
	w("  outcomes      %s", outcomes(r.Totals.FinalOutcomes))
	w("  attempts      %d total (%s)", r.Attempts.Total, kinds(r.Attempts.ByKind))
	w("  amplification %.2fx  (HTTP attempts per intended request)", r.Attempts.RetryAmplification)
	w("  throughput    %.0f req/s offered, %.0f ok workflows/s, over %.1fs",
		r.Throughput.AttemptsPerSec, r.Throughput.GoodputPerSec, r.Throughput.DurationSec)
	if r.Latency.WorkflowOK.Count == 0 {
		w("  latency       no workflow succeeded")
	} else {
		w("  latency       p50 %s  p95 %s  p99 %s  max %s   (successful workflows)",
			ms(r.Latency.WorkflowOK.P50Ms), ms(r.Latency.WorkflowOK.P95Ms),
			ms(r.Latency.WorkflowOK.P99Ms), ms(r.Latency.WorkflowOK.MaxMs))
	}
	w("  arrival skew  p50 %s  p99 %s  max %s", ms(r.ArrivalSkew.P50Ms), ms(r.ArrivalSkew.P99Ms), ms(r.ArrivalSkew.MaxMs))

	if ts := r.TargetStats; ts != nil && len(ts.Delta) > 0 {
		w("")
		w("  target counters (delta, per workflow)")
		for _, k := range sortedKeys(ts.Delta) {
			w("    %-22s %8s   %6.2f", k, num(ts.Delta[k]), ts.PerLogical[k])
		}
	}
	if len(r.Invariants) > 0 {
		w("")
		w("  invariants")
		for _, i := range r.Invariants {
			w("    [%s] %-40s observed %s (want %s %s)", mark(i.Pass), i.Name, num(i.Observed), i.Op, num(i.Expected))
		}
	}
	if len(r.Thresholds) > 0 {
		w("")
		w("  thresholds")
		for _, t := range r.Thresholds {
			w("    [%s] %-40s observed %.2f (limit %.2f)", mark(t.Pass), t.Name, t.Observed, t.Limit)
		}
	}
	w("")
	if r.Verdict.Pass {
		w("VERDICT: PASS")
	} else {
		w("VERDICT: FAIL")
		for _, f := range r.Verdict.Failures {
			w("  - %s", f)
		}
	}
	w("")
	return b.String()
}

// Markdown renders the shareable launch-readiness report.
func (r *Report) Markdown() string {
	var b strings.Builder
	w := func(format string, a ...any) { fmt.Fprintf(&b, format+"\n", a...) }

	w("# Launch-readiness report: %s", r.Scenario)
	w("")
	w("**Verdict: %s**", mark(r.Verdict.Pass))
	for _, f := range r.Verdict.Failures {
		w("- %s", f)
	}
	w("")
	w("| | |")
	w("|---|---|")
	w("| Run | `%s` |", r.RunID)
	w("| Started | %s |", r.StartedAt.Format(time.RFC3339))
	w("| Target | %s (%s) |", r.Target, r.Environment)
	w("| Traffic | %s, %d arrivals, seed %d, %.0f%% double-submit |", r.Traffic.Profile, r.Traffic.Arrivals, r.Seed, r.Traffic.DoubleSubmit*100)
	w("")
	w("## Summary")
	w("")
	w("| Metric | Value |")
	w("|---|---:|")
	w("| Workflows started | %d |", r.Totals.Logical)
	w("| Workflows succeeded | %d |", r.Totals.Succeeded)
	w("| Workflows failed | %d |", r.Totals.Failed)
	w("| HTTP attempts | %d |", r.Attempts.Total)
	w("| **Retry amplification** | **%.2fx** |", r.Attempts.RetryAmplification)
	w("| Offered load | %.0f req/s |", r.Throughput.AttemptsPerSec)
	w("| Goodput | %.0f ok workflows/s |", r.Throughput.GoodputPerSec)
	w("| Latency p50 / p95 / p99 / max | %s / %s / %s / %s |",
		ms(r.Latency.WorkflowOK.P50Ms), ms(r.Latency.WorkflowOK.P95Ms), ms(r.Latency.WorkflowOK.P99Ms), ms(r.Latency.WorkflowOK.MaxMs))
	w("| Arrival skew p50 / p99 / max | %s / %s / %s |", ms(r.ArrivalSkew.P50Ms), ms(r.ArrivalSkew.P99Ms), ms(r.ArrivalSkew.MaxMs))
	w("")
	w("Final outcomes: %s.", outcomes(r.Totals.FinalOutcomes))
	w("Attempts by kind: %s. By class: %s.", kinds(r.Attempts.ByKind), kinds(r.Attempts.ByClass))
	w("")

	if len(r.Invariants) > 0 {
		w("## Business invariants")
		w("")
		w("| Invariant | Result | Observed | Expected | Waited |")
		w("|---|---|---:|---|---:|")
		for _, i := range r.Invariants {
			w("| %s | %s | %s | %s %s | %s |", i.Name, mark(i.Pass), num(i.Observed), i.Op, num(i.Expected), ms(i.WaitedMs))
		}
		w("")
	}
	if len(r.Thresholds) > 0 {
		w("## Thresholds")
		w("")
		w("| Threshold | Result | Observed | Limit |")
		w("|---|---|---:|---:|")
		for _, t := range r.Thresholds {
			w("| %s | %s | %.2f | %.2f |", t.Name, mark(t.Pass), t.Observed, t.Limit)
		}
		w("")
	}
	if ts := r.TargetStats; ts != nil && len(ts.Delta) > 0 {
		w("## Target counters")
		w("")
		w("| Counter | Delta | Per workflow |")
		w("|---|---:|---:|")
		for _, k := range sortedKeys(ts.Delta) {
			w("| `%s` | %s | %.2f |", k, num(ts.Delta[k]), ts.PerLogical[k])
		}
		w("")
	}
	if len(r.Latency.Steps) > 0 {
		w("## Per-step attempt latency")
		w("")
		w("| Step | Attempts | p50 | p95 | p99 | max |")
		w("|---|---:|---:|---:|---:|---:|")
		for _, k := range sortedKeys(r.Latency.Steps) {
			l := r.Latency.Steps[k]
			w("| %s | %d | %s | %s | %s | %s |", k, l.Count, ms(l.P50Ms), ms(l.P95Ms), ms(l.P99Ms), ms(l.MaxMs))
		}
		w("")
	}
	if len(r.Timeline) > 0 {
		w("## Timeline")
		w("")
		w("Attempts started per second. Retries are the share of attempts that were not a user's first try.")
		w("")
		w("```")
		peak := 1
		for _, p := range r.Timeline {
			if p.Attempts > peak {
				peak = p.Attempts
			}
		}
		for _, p := range r.Timeline {
			bar := strings.Repeat("#", p.Attempts*40/peak)
			w("t+%2ds %6d att %6d retry %6d fail  %s", p.Second, p.Attempts, p.Retries, p.Failed, bar)
		}
		w("```")
		w("")
	}
	return b.String()
}

func ms(v float64) string {
	switch {
	case v >= 1000:
		return fmt.Sprintf("%.2fs", v/1000)
	case v >= 10:
		return fmt.Sprintf("%.0fms", v)
	default:
		return fmt.Sprintf("%.1fms", v)
	}
}

func outcomes(m map[string]int) string {
	if len(m) == 0 {
		return "none"
	}
	parts := make([]string, 0, len(m))
	for _, k := range sortedKeys(m) {
		parts = append(parts, fmt.Sprintf("%s x%d", k, m[k]))
	}
	return strings.Join(parts, ", ")
}

func kinds(m map[string]int) string {
	if len(m) == 0 {
		return "none"
	}
	parts := make([]string, 0, len(m))
	for _, k := range sortedKeys(m) {
		parts = append(parts, fmt.Sprintf("%s %d", k, m[k]))
	}
	return strings.Join(parts, ", ")
}

// --------------------------------------------------------------- compare ---

type metric struct {
	label       string
	get         func(*Report) float64
	lowerBetter bool
	fmt         func(float64) string
}

var compareMetrics = []metric{
	{"Workflows failed", func(r *Report) float64 { return float64(r.Totals.Failed) }, true, num},
	{"Retry amplification (x)", func(r *Report) float64 { return r.Attempts.RetryAmplification }, true, func(f float64) string { return fmt.Sprintf("%.2f", f) }},
	{"HTTP attempts", func(r *Report) float64 { return float64(r.Attempts.Total) }, true, num},
	{"Latency p50", func(r *Report) float64 { return r.Latency.WorkflowOK.P50Ms }, true, ms},
	{"Latency p99", func(r *Report) float64 { return r.Latency.WorkflowOK.P99Ms }, true, ms},
	{"Latency max", func(r *Report) float64 { return r.Latency.WorkflowOK.MaxMs }, true, ms},
	{"Goodput (ok workflows/s)", func(r *Report) float64 { return r.Throughput.GoodputPerSec }, false, func(f float64) string { return fmt.Sprintf("%.0f", f) }},
}

// Compare renders a before/after table, including invariants and target counters.
func Compare(before, after *Report) string {
	var b strings.Builder
	w := func(format string, a ...any) { fmt.Fprintf(&b, format+"\n", a...) }

	w("")
	w("BurstForge compare")
	w("  before  %s  run %s  verdict %s", before.Scenario, before.RunID, mark(before.Verdict.Pass))
	w("  after   %s  run %s  verdict %s", after.Scenario, after.RunID, mark(after.Verdict.Pass))
	w("")
	w("  %-28s %14s %14s   %s", "metric", "before", "after", "change")
	for _, m := range compareMetrics {
		bv, av := m.get(before), m.get(after)
		w("  %-28s %14s %14s   %s", m.label, m.fmt(bv), m.fmt(av), change(bv, av, m.lowerBetter))
	}

	if before.TargetStats != nil && after.TargetStats != nil {
		w("")
		w("  %-28s %14s %14s   %s", "target calls per workflow", "before", "after", "change")
		for _, k := range sortedKeys(before.TargetStats.PerLogical) {
			bv := before.TargetStats.PerLogical[k]
			av, ok := after.TargetStats.PerLogical[k]
			if !ok {
				continue
			}
			w("  %-28s %14.2f %14.2f   %s", k, bv, av, change(bv, av, true))
		}
	}

	names := map[string]bool{}
	bi, ai := map[string]InvariantCheck{}, map[string]InvariantCheck{}
	for _, i := range before.Invariants {
		bi[i.Name] = i
		names[i.Name] = true
	}
	for _, i := range after.Invariants {
		ai[i.Name] = i
		names[i.Name] = true
	}
	if len(names) > 0 {
		w("")
		w("  %-44s %-18s %-18s", "invariant", "before", "after")
		for _, n := range sortedKeys(names) {
			w("  %-44s %-18s %-18s", n, invCell(bi, n), invCell(ai, n))
		}
	}
	w("")
	return b.String()
}

func invCell(m map[string]InvariantCheck, name string) string {
	i, ok := m[name]
	if !ok {
		return "-"
	}
	return fmt.Sprintf("%s (%s)", mark(i.Pass), num(i.Observed))
}

func change(before, after float64, lowerBetter bool) string {
	if before == after {
		return "no change"
	}
	if before == 0 {
		return fmt.Sprintf("%+.2f", after-before)
	}
	pct := (after - before) / before * 100
	better := (after < before) == lowerBetter
	tag := "worse"
	if better {
		tag = "better"
	}
	return fmt.Sprintf("%+.0f%%  %s", pct, tag)
}
