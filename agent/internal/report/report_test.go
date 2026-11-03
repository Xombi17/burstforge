package report

import (
	"path/filepath"
	"strings"
	"testing"
)

func sample(pass bool) *Report {
	r := &Report{
		Scenario: "s", RunID: "r",
		Totals:   Totals{Logical: 100, Succeeded: 100, FinalOutcomes: map[string]int{"201": 100}},
		Attempts: Attempts{Total: 150, RetryAmplification: 1.5, ByKind: map[string]int{"first": 100, "retry": 50}, ByClass: map[string]int{"ok": 100}},
		Latency:  LatencySet{WorkflowOK: Latency{Count: 100, P99Ms: 900}, Steps: map[string]Latency{}},
	}
	r.Invariants = []InvariantCheck{{Name: "no dupes", Pass: pass, Observed: 3, Op: "eq", Expected: 0}}
	r.Finalize()
	return r
}

func TestVerdictFollowsInvariants(t *testing.T) {
	if !sample(true).Verdict.Pass {
		t.Error("passing invariants should pass")
	}
	bad := sample(false)
	if bad.Verdict.Pass || len(bad.Verdict.Failures) != 1 || !strings.Contains(bad.Verdict.Failures[0], "no dupes") {
		t.Errorf("verdict: %+v", bad.Verdict)
	}
}

func TestAbortedRunNeverPasses(t *testing.T) {
	r := sample(true)
	r.Totals.Aborted = true
	r.Finalize()
	if r.Verdict.Pass {
		t.Error("aborted run must not pass")
	}
}

func TestSaveLoadRoundTrip(t *testing.T) {
	p := filepath.Join(t.TempDir(), "r.json")
	if err := sample(false).Save(p); err != nil {
		t.Fatal(err)
	}
	got, err := Load(p)
	if err != nil || got.Attempts.Total != 150 || got.Verdict.Pass {
		t.Fatalf("round trip: %+v %v", got, err)
	}
}

func TestCompareAndMarkdownRender(t *testing.T) {
	a, b := sample(false), sample(true)
	b.Attempts.RetryAmplification = 1.0
	out := Compare(a, b)
	for _, want := range []string{"Retry amplification", "-33%", "better", "no dupes", "FAIL (3)", "PASS (3)"} {
		if !strings.Contains(out, want) {
			t.Errorf("compare output missing %q:\n%s", want, out)
		}
	}
	md := a.Markdown()
	if !strings.Contains(md, "Verdict: FAIL") || !strings.Contains(md, "Business invariants") {
		t.Errorf("markdown:\n%s", md)
	}
}
