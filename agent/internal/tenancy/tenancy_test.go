package tenancy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"burstforge/internal/scenario"
)

func scen(t *testing.T, traffic string) *scenario.Scenario {
	t.Helper()
	sc, err := scenario.Parse([]byte(`
target: { base_url: "http://somewhere-else:9000", environment: staging, allow_hosts: [somewhere-else] }
workflow: { steps: [ { name: a, path: /a } ] }
traffic: `+traffic), "t")
	if err != nil {
		t.Fatal(err)
	}
	return sc
}

func TestMeasureAndLimits(t *testing.T) {
	sc := scen(t, `{ profile: burst, arrivals: 300, window: 1s }`)
	s, err := Measure(sc, 1)
	if err != nil {
		t.Fatal(err)
	}
	if s.Arrivals != 300 || s.PeakPerSec != 300 || s.PlannedSpan > time.Second {
		t.Fatalf("shape %+v", s)
	}
	ok := Limits{MaxArrivals: 300, MaxArrivalRate: 300, MaxDuration: time.Second}
	if err := CheckLimits(s, ok); err != nil {
		t.Fatal(err)
	}
	err = CheckLimits(s, Limits{MaxArrivals: 100, MaxArrivalRate: 50, MaxDuration: time.Millisecond})
	for _, want := range []string{"max_arrivals", "max_arrival_rate", "max_duration"} {
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("missing %s in %v", want, err)
		}
	}

	// 600 arrivals spread over 30s peak far below 600 a second.
	s, _ = Measure(scen(t, `{ profile: sustained, arrivals: 600, duration: 30s }`), 1)
	if s.PeakPerSec < 15 || s.PeakPerSec > 30 {
		t.Fatalf("sustained peak %d", s.PeakPerSec)
	}
}

func TestBindReplacesTheTarget(t *testing.T) {
	sc := scen(t, `{ profile: burst, arrivals: 5, window: 1s }`)
	env := Environment{Org: "acme", Name: "staging", BaseURL: "http://acme-staging:8080/"}
	if err := CheckBound(sc, env); err == nil {
		t.Fatal("an unbound scenario passed")
	}
	if err := Bind(sc, env); err != nil {
		t.Fatal(err)
	}
	if sc.Target.BaseURL != "http://acme-staging:8080" || len(sc.Target.AllowHosts) != 1 {
		t.Fatalf("target %+v", sc.Target)
	}
	if err := sc.Validate(); err != nil {
		t.Fatal(err)
	}
	if err := sc.CheckTarget(); err != nil {
		t.Fatalf("bound scenario fails the CLI safety check: %v", err)
	}
	if err := CheckBound(sc, env); err != nil {
		t.Fatal(err)
	}
	// Tampering with the stored scenario is caught.
	sc.Target.AllowHosts = append(sc.Target.AllowHosts, "evil.example")
	if err := CheckBound(sc, env); err == nil {
		t.Fatal("extra allowed host passed")
	}
	sc.Target.AllowHosts = sc.Target.AllowHosts[:1]
	sc.Target.BaseURL = "https://acme-staging:8080"
	if err := CheckBound(sc, env); err == nil {
		t.Fatal("scheme change passed")
	}
	sc.Target.BaseURL = "http://acme-staging:8080"
	sc.Faults.Proxy = "http://p:1"
	if err := CheckBound(sc, env); err == nil {
		t.Fatal("own fault proxy passed")
	}
	if err := Bind(sc, env); err == nil {
		t.Fatal("Bind accepted a scenario with its own fault proxy")
	}
}

func TestCheckApproval(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case ApprovalPath:
			w.Write([]byte("# teams allowed to test this target\nbf-approve-other-1\n  bf-approve-acme-2  \n"))
		case "/redirect" + ApprovalPath:
			http.Redirect(w, r, ApprovalPath, http.StatusFound)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	ctx := context.Background()
	if err := CheckApproval(ctx, srv.URL, "bf-approve-acme-2"); err != nil {
		t.Fatal(err)
	}
	if err := CheckApproval(ctx, srv.URL, "bf-approve-acme"); err == nil {
		t.Fatal("a prefix of a token passed")
	}
	if err := CheckApproval(ctx, srv.URL+"/nope", "bf-approve-acme-2"); err == nil {
		t.Fatal("404 passed")
	}
	if err := CheckApproval(ctx, srv.URL+"/redirect", "bf-approve-acme-2"); err == nil {
		t.Fatal("followed a redirect")
	}
	if err := CheckApproval(ctx, srv.URL, ""); err == nil {
		t.Fatal("empty token passed")
	}
}
