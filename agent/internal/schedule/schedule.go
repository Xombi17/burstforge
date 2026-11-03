// Package schedule turns a traffic profile into arrival offsets.
//
// The plan is a pure function of (profile, seed), so a run can be reproduced
// exactly and, later, split into shards across agents.
package schedule

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"time"
)

type Profile struct {
	Kind     string // burst | ramp | sustained
	Arrivals int
	Window   time.Duration // burst: arrivals land inside this window
	Duration time.Duration // ramp, sustained
}

// Plan returns Arrivals offsets from t0, sorted ascending.
func Plan(p Profile, seed int64) ([]time.Duration, error) {
	if p.Arrivals <= 0 {
		return nil, fmt.Errorf("arrivals must be > 0")
	}
	out := make([]time.Duration, p.Arrivals)
	switch p.Kind {
	case "burst":
		rng := rand.New(rand.NewSource(seed))
		for i := range out {
			out[i] = time.Duration(rng.Float64() * float64(p.Window))
		}
	case "ramp":
		if p.Duration <= 0 {
			return nil, fmt.Errorf("ramp needs a duration")
		}
		// Rate rises linearly from 0, so the arrival CDF is (t/D)^2.
		// Placing arrivals at evenly spaced quantiles gives t = D*sqrt(u).
		for i := range out {
			u := (float64(i) + 0.5) / float64(p.Arrivals)
			out[i] = time.Duration(float64(p.Duration) * math.Sqrt(u))
		}
	case "sustained":
		if p.Duration <= 0 {
			return nil, fmt.Errorf("sustained needs a duration")
		}
		step := float64(p.Duration) / float64(p.Arrivals)
		for i := range out {
			out[i] = time.Duration(float64(i) * step)
		}
	default:
		return nil, fmt.Errorf("unknown profile %q", p.Kind)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}

// Shard returns the global arrival indexes owned by shard (1-based) of count.
//
// Assignment is round-robin over the sorted plan: arrival i belongs to shard
// i%count+1. Every agent therefore gets the same burst shape at 1/count of
// the density, instead of one agent getting all the early arrivals. Arrivals
// keep their global index, so logical IDs and per-arrival randomness match a
// single-agent run with the same seed.
func Shard(arrivals, shard, count int) ([]int, error) {
	if count < 1 || shard < 1 || shard > count {
		return nil, fmt.Errorf("shard %d/%d is out of range", shard, count)
	}
	if count > arrivals {
		return nil, fmt.Errorf("%d shards for %d arrivals would leave a shard empty", count, arrivals)
	}
	out := make([]int, 0, arrivals/count+1)
	for i := shard - 1; i < arrivals; i += count {
		out = append(out, i)
	}
	return out, nil
}
