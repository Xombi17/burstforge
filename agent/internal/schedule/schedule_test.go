package schedule

import (
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
