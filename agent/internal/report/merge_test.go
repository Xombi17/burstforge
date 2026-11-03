package report

import (
	"strings"
	"testing"
	"time"

	"burstforge/internal/hist"
)

var epoch = time.Date(2026, 11, 3, 10, 0, 0, 0, time.UTC)

func shard(i, n int, latencies ...time.Duration) *Report {
	raw := newRaw()
	for _, l := range latencies {
		raw.WorkflowOK.Record(l)
		raw.WorkflowAll.Record(l)
		raw.ArrivalSkew.Record(time.Millisecond)
	}
	raw.Steps["act"] = &hist.Histogram{}
	raw.LastDoneMs = 2000
	k := len(latencies)
	return &Report{
		Schema: SchemaVersion, RunID: "r", Seed: 7, Scenario: "s", Target: "http://t",
		StartedAt: epoch, FinishedAt: epoch.Add(2 * time.Second),
		Traffic:  Traffic{Profile: "burst", Arrivals: 9},
		Totals:   Totals{Logical: k, Succeeded: k, FinalOutcomes: map[string]int{"200": k}},
		Attempts: Attempts{Total: 2 * k, ByKind: map[string]int{"first": k, "retry": k}, ByClass: map[string]int{"ok": 2 * k}},
		Timeline: []TimelinePoint{{Second: 0, Attempts: k}, {Second: 1, Attempts: k}},
		Shard:    &ShardInfo{Index: i, Count: n, Arrivals: k, ScenarioHash: "h", StartAt: epoch, Agent: "a"},
		Raw:      raw,
	}
}

func ms3() []time.Duration {
	return []time.Duration{10 * time.Millisecond, 20 * time.Millisecond, 30 * time.Millisecond}
}

func TestMergeSumsCountersAndHistograms(t *testing.T) {
	m, err := Merge([]*Report{shard(2, 3, ms3()...), shard(1, 3, ms3()...), shard(3, 3, ms3()...)}, false)
	if err != nil {
		t.Fatal(err)
	}
	if m.Totals.Logical != 9 || m.Attempts.Total != 18 || m.Totals.FinalOutcomes["200"] != 9 {
		t.Fatalf("totals %+v attempts %+v", m.Totals, m.Attempts)
	}
	if m.Attempts.RetryAmplification != 2 {
		t.Fatalf("amplification %v", m.Attempts.RetryAmplification)
	}
	if m.Latency.WorkflowOK.Count != 9 || m.Latency.WorkflowOK.MaxMs != 30 {
		t.Fatalf("latency %+v", m.Latency.WorkflowOK)
	}
	if len(m.Timeline) != 2 || m.Timeline[0].Attempts != 9 {
		t.Fatalf("timeline %+v", m.Timeline)
	}
	if m.Throughput.DurationSec != 2 || m.Throughput.GoodputPerSec != 4.5 {
		t.Fatalf("throughput %+v", m.Throughput)
	}
	if m.Shards[0].Index != 1 || m.Shards[2].Index != 3 || m.Raw != nil {
		t.Fatalf("shards %+v", m.Shards)
	}
	m.Finalize()
	if !m.Verdict.Pass {
		t.Fatalf("verdict %+v", m.Verdict)
	}
	for _, s := range []string{m.Text(), m.Markdown()} {
		if !strings.Contains(s, "shard 2/3") && !strings.Contains(s, "| 2/3 |") {
			t.Fatalf("rendering lacks the agents table:\n%s", s)
		}
	}
}

func TestMergeIgnoresIdenticalRedelivery(t *testing.T) {
	a := shard(1, 2, ms3()...)
	m, err := Merge([]*Report{a, shard(2, 2, ms3()...), a}, false)
	if err != nil {
		t.Fatal(err)
	}
	if m.Totals.Logical != 6 || len(m.Notes) != 1 {
		t.Fatalf("logical %d notes %v", m.Totals.Logical, m.Notes)
	}
}

func TestMergeRejectsConflictingDuplicate(t *testing.T) {
	b := shard(1, 2, time.Millisecond)
	b.Shard.Agent = "other"
	_, err := Merge([]*Report{shard(1, 2, ms3()...), shard(2, 2, ms3()...), b}, false)
	if err == nil || !strings.Contains(err.Error(), "two agents executed the same shard") {
		t.Fatalf("err = %v", err)
	}
}

func TestMergeRejectsShardsFromDifferentRuns(t *testing.T) {
	cases := map[string]func(*Report){
		"seed":          func(r *Report) { r.Seed = 8 },
		"run_id":        func(r *Report) { r.RunID = "other" },
		"scenario hash": func(r *Report) { r.Shard.ScenarioHash = "x" },
		"start_at":      func(r *Report) { r.Shard.StartAt = epoch.Add(time.Second) },
		"shard count":   func(r *Report) { r.Shard.Count = 3 },
	}
	for field, mutate := range cases {
		b := shard(2, 2, ms3()...)
		mutate(b)
		_, err := Merge([]*Report{shard(1, 2, ms3()...), b}, false)
		if err == nil || !strings.Contains(err.Error(), field) {
			t.Fatalf("%s: err = %v", field, err)
		}
	}
	plain := shard(1, 1)
	plain.Shard = nil
	if _, err := Merge([]*Report{plain}, false); err == nil {
		t.Fatal("merged a report that is not a shard report")
	}
}

func TestMissingShardFailsOrIsFlagged(t *testing.T) {
	in := []*Report{shard(1, 3, ms3()...), shard(3, 3, ms3()...)}
	if _, err := Merge(in, false); err == nil || !strings.Contains(err.Error(), "missing shard(s) [2]") {
		t.Fatalf("err = %v", err)
	}
	m, err := Merge(in, true)
	if err != nil {
		t.Fatal(err)
	}
	m.Finalize()
	if m.Verdict.Pass || !m.Shards[1].Missing || m.Totals.Logical != 6 {
		t.Fatalf("partial merge: verdict %+v shards %+v", m.Verdict, m.Shards)
	}
}
