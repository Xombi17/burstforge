// Package hist is a mergeable log-bucketed latency histogram.
//
// Buckets grow by 2%, so any reported quantile is within ~2% of the true
// value. Histograms from different agents merge by adding bucket counts,
// which is what makes at-least-once telemetry safe to combine later.
package hist

import (
	"encoding/json"
	"fmt"
	"math"
	"time"
)

const (
	growth     = 1.02
	numBuckets = 1200 // covers 1us .. ~2e4 s
)

var lnGrowth = math.Log(growth)

type Histogram struct {
	counts [numBuckets]uint64
	n      uint64
	sumUs  float64
	minUs  int64
	maxUs  int64
}

func index(us int64) int {
	if us < 1 {
		return 0
	}
	i := 1 + int(math.Log(float64(us))/lnGrowth)
	if i >= numBuckets {
		return numBuckets - 1
	}
	return i
}

func upperBoundUs(i int) float64 {
	if i == 0 {
		return 1
	}
	return math.Pow(growth, float64(i))
}

func (h *Histogram) Record(d time.Duration) {
	us := d.Microseconds()
	if us < 0 {
		us = 0
	}
	h.counts[index(us)]++
	h.sumUs += float64(us)
	if h.n == 0 || us < h.minUs {
		h.minUs = us
	}
	if us > h.maxUs {
		h.maxUs = us
	}
	h.n++
}

// Merge adds o into h.
func (h *Histogram) Merge(o *Histogram) {
	if o == nil || o.n == 0 {
		return
	}
	for i, c := range o.counts {
		h.counts[i] += c
	}
	if h.n == 0 || o.minUs < h.minUs {
		h.minUs = o.minUs
	}
	if o.maxUs > h.maxUs {
		h.maxUs = o.maxUs
	}
	h.n += o.n
	h.sumUs += o.sumUs
}

func (h *Histogram) Count() uint64 { return h.n }

func (h *Histogram) Max() time.Duration { return time.Duration(h.maxUs) * time.Microsecond }

func (h *Histogram) Mean() time.Duration {
	if h.n == 0 {
		return 0
	}
	return time.Duration(h.sumUs/float64(h.n)) * time.Microsecond
}

// Quantile returns the q-th quantile (0..1), clamped to the observed max.
func (h *Histogram) Quantile(q float64) time.Duration {
	if h.n == 0 {
		return 0
	}
	target := uint64(math.Ceil(q * float64(h.n)))
	if target < 1 {
		target = 1
	}
	var cum uint64
	for i, c := range h.counts {
		cum += c
		if cum >= target {
			us := upperBoundUs(i)
			if us > float64(h.maxUs) {
				us = float64(h.maxUs)
			}
			return time.Duration(us) * time.Microsecond
		}
	}
	return h.Max()
}

// wire is the JSON form: only non-empty buckets, as [index, count] pairs.
type wire struct {
	N       uint64      `json:"n"`
	SumUs   float64     `json:"sum_us"`
	MinUs   int64       `json:"min_us"`
	MaxUs   int64       `json:"max_us"`
	Buckets [][2]uint64 `json:"buckets"`
}

func (h *Histogram) MarshalJSON() ([]byte, error) {
	w := wire{N: h.n, SumUs: h.sumUs, MinUs: h.minUs, MaxUs: h.maxUs, Buckets: [][2]uint64{}}
	for i, c := range h.counts {
		if c > 0 {
			w.Buckets = append(w.Buckets, [2]uint64{uint64(i), c})
		}
	}
	return json.Marshal(w)
}

func (h *Histogram) UnmarshalJSON(b []byte) error {
	var w wire
	if err := json.Unmarshal(b, &w); err != nil {
		return err
	}
	var out Histogram
	var total uint64
	for _, bc := range w.Buckets {
		if bc[0] >= numBuckets {
			return fmt.Errorf("histogram bucket %d out of range", bc[0])
		}
		out.counts[bc[0]] += bc[1]
		total += bc[1]
	}
	if total != w.N {
		return fmt.Errorf("histogram buckets sum to %d, header says %d", total, w.N)
	}
	out.n, out.sumUs, out.minUs, out.maxUs = w.N, w.SumUs, w.MinUs, w.MaxUs
	*h = out
	return nil
}
