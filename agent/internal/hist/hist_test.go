package hist

import (
	"math/rand"
	"sort"
	"testing"
	"time"
)

func TestQuantilesWithinTolerance(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	var h Histogram
	vals := make([]time.Duration, 20000)
	for i := range vals {
		vals[i] = time.Duration(rng.ExpFloat64()*80+1) * time.Millisecond
		h.Record(vals[i])
	}
	sort.Slice(vals, func(i, j int) bool { return vals[i] < vals[j] })
	for _, q := range []float64{0.5, 0.95, 0.99} {
		want := vals[int(q*float64(len(vals)))-1]
		got := h.Quantile(q)
		diff := float64(got-want) / float64(want)
		if diff < -0.03 || diff > 0.03 {
			t.Errorf("q%.2f: got %v want %v (off by %.1f%%)", q, got, want, diff*100)
		}
	}
}

func TestMergeMatchesSingleHistogram(t *testing.T) {
	rng := rand.New(rand.NewSource(2))
	var all, a, b Histogram
	for i := 0; i < 5000; i++ {
		d := time.Duration(rng.Intn(3000)+1) * time.Millisecond
		all.Record(d)
		if i%2 == 0 {
			a.Record(d)
		} else {
			b.Record(d)
		}
	}
	a.Merge(&b)
	if a.Count() != all.Count() || a.Max() != all.Max() {
		t.Fatalf("count/max differ: %d/%v vs %d/%v", a.Count(), a.Max(), all.Count(), all.Max())
	}
	for _, q := range []float64{0.5, 0.9, 0.99} {
		if a.Quantile(q) != all.Quantile(q) {
			t.Errorf("q%.2f differs after merge: %v vs %v", q, a.Quantile(q), all.Quantile(q))
		}
	}
}

func TestEmptyAndClamp(t *testing.T) {
	var h Histogram
	if h.Quantile(0.99) != 0 || h.Mean() != 0 {
		t.Fatal("empty histogram should report zero")
	}
	h.Record(5 * time.Millisecond)
	if h.Quantile(0.99) > h.Max() {
		t.Fatal("quantile must not exceed observed max")
	}
}
