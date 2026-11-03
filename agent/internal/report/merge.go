package report

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"burstforge/internal/hist"
)

// ShardInfo identifies the slice of a run that one agent executed.
type ShardInfo struct {
	Index        int       `json:"index"` // 1-based
	Count        int       `json:"count"`
	Arrivals     int       `json:"arrivals"` // arrivals owned by this shard
	ScenarioHash string    `json:"scenario_hash"`
	StartAt      time.Time `json:"start_at"` // shared epoch every shard schedules from
	Agent        string    `json:"agent"`
}

// ShardSummary is one row of the per-agent table in a merged report.
type ShardSummary struct {
	Index            int     `json:"index"`
	Count            int     `json:"count"`
	Agent            string  `json:"agent,omitempty"`
	Arrivals         int     `json:"arrivals"`
	Logical          int     `json:"logical"`
	Attempts         int     `json:"attempts"`
	ArrivalSkewP99Ms float64 `json:"arrival_skew_p99_ms"`
	Aborted          bool    `json:"aborted,omitempty"`
	Missing          bool    `json:"missing,omitempty"`
}

// Raw carries the mergeable state behind the summary numbers. Shard reports
// include it so a reducer can combine them without losing precision.
type Raw struct {
	WorkflowOK  *hist.Histogram            `json:"workflow_ok"`
	WorkflowAll *hist.Histogram            `json:"workflow_all"`
	ArrivalSkew *hist.Histogram            `json:"arrival_skew"`
	Steps       map[string]*hist.Histogram `json:"steps"`
	LastDoneMs  float64                    `json:"last_done_ms"` // since StartAt
}

func newRaw() *Raw {
	return &Raw{
		WorkflowOK: &hist.Histogram{}, WorkflowAll: &hist.Histogram{},
		ArrivalSkew: &hist.Histogram{}, Steps: map[string]*hist.Histogram{},
	}
}

// FromHist summarizes a histogram in milliseconds.
func FromHist(h *hist.Histogram) Latency {
	f := func(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }
	return Latency{
		Count: h.Count(), MeanMs: f(h.Mean()),
		P50Ms: f(h.Quantile(0.50)), P95Ms: f(h.Quantile(0.95)), P99Ms: f(h.Quantile(0.99)), MaxMs: f(h.Max()),
	}
}

// Derive fills latency, skew, amplification and throughput from Raw and the
// counters. The single-agent runner and Merge both use it, so the two paths
// cannot compute these numbers differently.
func (r *Report) Derive() {
	raw := r.Raw
	r.Latency = LatencySet{
		WorkflowOK:  FromHist(raw.WorkflowOK),
		WorkflowAll: FromHist(raw.WorkflowAll),
		Steps:       map[string]Latency{},
	}
	for name, h := range raw.Steps {
		r.Latency.Steps[name] = FromHist(h)
	}
	r.ArrivalSkew = FromHist(raw.ArrivalSkew)

	// Amplification is attempts per intended request: every step a user
	// reaches counts once as a "first" attempt, and retries and double
	// submits are the extra load on top of that.
	r.Attempts.RetryAmplification = 0
	if first := r.Attempts.ByKind["first"]; first > 0 {
		r.Attempts.RetryAmplification = float64(r.Attempts.Total) / float64(first)
	}

	span := raw.LastDoneMs / 1000
	if span <= 0 {
		span = r.FinishedAt.Sub(r.StartedAt).Seconds()
	}
	r.Throughput = Throughput{DurationSec: span}
	if span > 0 {
		r.Throughput.AttemptsPerSec = float64(r.Attempts.Total) / span
		r.Throughput.GoodputPerSec = float64(r.Totals.Succeeded) / span
	}
}

// Merge reduces shard reports from one run into a single report.
//
// Shards must agree on run ID, seed, shard count, scenario hash and start
// epoch. A shard delivered twice with identical content is counted once. The
// same shard reported twice with different content means two agents both
// executed it, and the load they sent can't be undone, so that is an error.
//
// Missing shards are an error unless allowPartial is set; then they appear
// in Shards with Missing set and Finalize fails the verdict.
//
// Thresholds and invariants are not evaluated here: they need the scenario
// and, for invariants, the target. The caller does that and calls Finalize.
func Merge(shards []*Report, allowPartial bool) (*Report, error) {
	if len(shards) == 0 {
		return nil, fmt.Errorf("no shard reports to merge")
	}
	for i, s := range shards {
		if s.Shard == nil || s.Raw == nil {
			return nil, fmt.Errorf("report %d (run %s) is not a shard report; produce shard reports with `burstforge agent`", i+1, s.RunID)
		}
	}

	ref := shards[0]
	count := ref.Shard.Count
	byIndex := map[int]*Report{}
	fingerprint := map[int][]byte{}
	var notes []string
	for _, s := range shards {
		if err := sameRun(ref, s); err != nil {
			return nil, err
		}
		b, err := json.Marshal(s)
		if err != nil {
			return nil, err
		}
		idx := s.Shard.Index
		if prev, ok := fingerprint[idx]; ok {
			if bytes.Equal(prev, b) {
				notes = append(notes, fmt.Sprintf("shard %d/%d was delivered more than once; counted once", idx, count))
				continue
			}
			return nil, fmt.Errorf("shard %d/%d was reported twice with different results (agents %q and %q): two agents executed the same shard",
				idx, count, byIndex[idx].Shard.Agent, s.Shard.Agent)
		}
		fingerprint[idx] = b
		byIndex[idx] = s
	}

	var missing []int
	for i := 1; i <= count; i++ {
		if byIndex[i] == nil {
			missing = append(missing, i)
		}
	}
	if len(missing) > 0 && !allowPartial {
		return nil, fmt.Errorf("missing shard(s) %v of %d", missing, count)
	}

	out := &Report{
		Schema:      SchemaVersion,
		Tool:        ref.Tool,
		Scenario:    ref.Scenario,
		RunID:       ref.RunID,
		Seed:        ref.Seed,
		Target:      ref.Target,
		Environment: ref.Environment,
		Traffic:     ref.Traffic,
		Totals:      Totals{FinalOutcomes: map[string]int{}},
		Attempts:    Attempts{ByKind: map[string]int{}, ByClass: map[string]int{}},
		Raw:         newRaw(),
		Notes:       notes,
	}

	var timeline []TimelinePoint
	for i := 1; i <= count; i++ {
		s := byIndex[i]
		if s == nil {
			out.Shards = append(out.Shards, ShardSummary{Index: i, Count: count, Missing: true})
			continue
		}
		if out.StartedAt.IsZero() || s.StartedAt.Before(out.StartedAt) {
			out.StartedAt = s.StartedAt
		}
		if s.FinishedAt.After(out.FinishedAt) {
			out.FinishedAt = s.FinishedAt
		}

		out.Totals.Logical += s.Totals.Logical
		out.Totals.Succeeded += s.Totals.Succeeded
		out.Totals.Failed += s.Totals.Failed
		out.Totals.Aborted = out.Totals.Aborted || s.Totals.Aborted
		addCounts(out.Totals.FinalOutcomes, s.Totals.FinalOutcomes)

		out.Attempts.Total += s.Attempts.Total
		addCounts(out.Attempts.ByKind, s.Attempts.ByKind)
		addCounts(out.Attempts.ByClass, s.Attempts.ByClass)

		out.Raw.WorkflowOK.Merge(s.Raw.WorkflowOK)
		out.Raw.WorkflowAll.Merge(s.Raw.WorkflowAll)
		out.Raw.ArrivalSkew.Merge(s.Raw.ArrivalSkew)
		for name, h := range s.Raw.Steps {
			if out.Raw.Steps[name] == nil {
				out.Raw.Steps[name] = &hist.Histogram{}
			}
			out.Raw.Steps[name].Merge(h)
		}
		if s.Raw.LastDoneMs > out.Raw.LastDoneMs {
			out.Raw.LastDoneMs = s.Raw.LastDoneMs
		}

		// Timeline seconds are relative to the shared start epoch.
		for _, p := range s.Timeline {
			for len(timeline) <= p.Second {
				timeline = append(timeline, TimelinePoint{Second: len(timeline)})
			}
			t := &timeline[p.Second]
			t.Attempts += p.Attempts
			t.OK += p.OK
			t.Failed += p.Failed
			t.Retries += p.Retries
		}

		out.Shards = append(out.Shards, ShardSummary{
			Index: i, Count: count, Agent: s.Shard.Agent,
			Arrivals: s.Shard.Arrivals, Logical: s.Totals.Logical, Attempts: s.Attempts.Total,
			ArrivalSkewP99Ms: s.ArrivalSkew.P99Ms, Aborted: s.Totals.Aborted,
		})
	}
	out.Timeline = timeline
	out.Derive()
	out.Raw = nil // a merged report is a final report
	sort.Slice(out.Shards, func(a, b int) bool { return out.Shards[a].Index < out.Shards[b].Index })
	return out, nil
}

func sameRun(a, b *Report) error {
	mismatch := func(field string, x, y any) error {
		return fmt.Errorf("shard %d/%d does not belong with shard %d/%d: %s differs (%v vs %v)",
			b.Shard.Index, b.Shard.Count, a.Shard.Index, a.Shard.Count, field, y, x)
	}
	switch {
	case a.Schema != b.Schema:
		return mismatch("schema", a.Schema, b.Schema)
	case a.RunID != b.RunID:
		return mismatch("run_id", a.RunID, b.RunID)
	case a.Seed != b.Seed:
		return mismatch("seed", a.Seed, b.Seed)
	case a.Shard.Count != b.Shard.Count:
		return mismatch("shard count", a.Shard.Count, b.Shard.Count)
	case a.Shard.ScenarioHash != b.Shard.ScenarioHash:
		return mismatch("scenario hash", a.Shard.ScenarioHash, b.Shard.ScenarioHash)
	case !a.Shard.StartAt.Equal(b.Shard.StartAt):
		return mismatch("start_at", a.Shard.StartAt.Format(time.RFC3339Nano), b.Shard.StartAt.Format(time.RFC3339Nano))
	case a.Target != b.Target:
		return mismatch("target", a.Target, b.Target)
	}
	if b.Shard.Index < 1 || b.Shard.Index > b.Shard.Count {
		return fmt.Errorf("shard index %d is out of range 1..%d", b.Shard.Index, b.Shard.Count)
	}
	return nil
}

func addCounts(dst, src map[string]int) {
	for k, v := range src {
		dst[k] += v
	}
}
