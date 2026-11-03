package schedule

import (
	"fmt"
	"reflect"
	"testing"
	"time"
)

func TestBurstIsDeterministicAndBounded(t *testing.T) {
	p := Profile{Kind: "burst", Arrivals: 500, Window: time.Second}
	a, _ := Plan(p, 42)
	b, _ := Plan(p, 42)
	c, _ := Plan(p, 43)
	if !reflect.DeepEqual(a, b) {
		t.Fatal("same seed must give the same plan")
	}
	if reflect.DeepEqual(a, c) {
		t.Fatal("different seeds should differ")
	}
	for i, off := range a {
		if off < 0 || off >= time.Second {
			t.Fatalf("offset %d out of window: %v", i, off)
		}
		if i > 0 && off < a[i-1] {
			t.Fatal("plan must be sorted")
		}
	}
}

func TestRampGetsDenser(t *testing.T) {
	out, err := Plan(Profile{Kind: "ramp", Arrivals: 1000, Duration: 10 * time.Second}, 1)
	if err != nil {
		t.Fatal(err)
	}
	firstHalf := 0
	for _, o := range out {
		if o < 5*time.Second {
			firstHalf++
		}
	}
	// Linear ramp: a quarter of arrivals land in the first half.
	if firstHalf < 240 || firstHalf > 260 {
		t.Fatalf("expected ~250 arrivals in first half, got %d", firstHalf)
	}
}

func TestSustainedIsEven(t *testing.T) {
	out, _ := Plan(Profile{Kind: "sustained", Arrivals: 10, Duration: 10 * time.Second}, 1)
	for i, o := range out {
		if o != time.Duration(i)*time.Second {
			t.Fatalf("arrival %d at %v", i, o)
		}
	}
}

func TestErrors(t *testing.T) {
	if _, err := Plan(Profile{Kind: "burst"}, 1); err == nil {
		t.Error("zero arrivals should fail")
	}
	if _, err := Plan(Profile{Kind: "nope", Arrivals: 1}, 1); err == nil {
		t.Error("unknown profile should fail")
	}
	if _, err := Plan(Profile{Kind: "ramp", Arrivals: 1}, 1); err == nil {
		t.Error("ramp without duration should fail")
	}
}

func TestShardsPartitionThePlan(t *testing.T) {
	const n = 1000
	for _, count := range []int{1, 2, 3, 7} {
		seen := make([]int, n)
		minLen, maxLen := n, 0
		for s := 1; s <= count; s++ {
			idx, err := Shard(n, s, count)
			if err != nil {
				t.Fatal(err)
			}
			again, _ := Shard(n, s, count)
			if fmt.Sprint(idx) != fmt.Sprint(again) {
				t.Fatalf("shard %d/%d is not deterministic", s, count)
			}
			for _, i := range idx {
				seen[i]++
			}
			minLen, maxLen = min(minLen, len(idx)), max(maxLen, len(idx))
		}
		for i, c := range seen {
			if c != 1 {
				t.Fatalf("count %d: arrival %d owned by %d shards", count, i, c)
			}
		}
		if maxLen-minLen > 1 {
			t.Fatalf("count %d: shard sizes range %d..%d", count, minLen, maxLen)
		}
	}
}

func TestShardsKeepTheBurstShape(t *testing.T) {
	// Each shard of a burst should span the whole window, not a slice of it.
	offsets, _ := Plan(Profile{Kind: "burst", Arrivals: 900, Window: time.Second}, 3)
	for s := 1; s <= 3; s++ {
		idx, _ := Shard(len(offsets), s, 3)
		first, last := offsets[idx[0]], offsets[idx[len(idx)-1]]
		if first > 20*time.Millisecond || last < 980*time.Millisecond {
			t.Fatalf("shard %d spans %v..%v", s, first, last)
		}
	}
}

func TestShardErrors(t *testing.T) {
	for _, c := range [][3]int{{10, 0, 3}, {10, 4, 3}, {10, 1, 0}, {2, 1, 3}} {
		if _, err := Shard(c[0], c[1], c[2]); err == nil {
			t.Fatalf("Shard%v: expected an error", c)
		}
	}
}
