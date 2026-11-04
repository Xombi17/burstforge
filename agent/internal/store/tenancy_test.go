package store

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"burstforge/internal/tenancy"
)

// newOrg creates an organization with one environment pointing at baseURL.
func newOrg(t *testing.T, s *Store, baseURL, token string, verified bool) string {
	t.Helper()
	ctx := context.Background()
	org := fmt.Sprintf("t%d-%d", time.Now().UnixNano()%1e12, runSeq.Add(1))
	if _, err := s.pool.Exec(ctx, `
		INSERT INTO organizations (id, name, max_concurrent_runs, max_arrivals, max_arrival_rate, max_duration_ms)
		VALUES ($1, $1, 2, 100, 100, 10000)`, org); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = s.pool.Exec(context.Background(), `DELETE FROM organizations WHERE id = $1`, org) })
	if _, err := s.pool.Exec(ctx, `
		INSERT INTO environments (organization_id, name, base_url, approval_token, verified_at)
		VALUES ($1, 'staging', $2, $3, CASE WHEN $4 THEN now() END)`, org, baseURL, token, verified); err != nil {
		t.Fatal(err)
	}
	return org
}

func newRequest(t *testing.T, s *Store, org string) string {
	t.Helper()
	id := fmt.Sprintf("req-%d-%d", time.Now().UnixNano(), runSeq.Add(1))
	if _, err := s.pool.Exec(context.Background(), `
		INSERT INTO run_requests (id, organization_id, environment, scenario_yaml, seed, shard_count)
		VALUES ($1, $2, 'staging', 'name: x', 3, 2)`, id, org); err != nil {
		t.Fatal(err)
	}
	return id
}

// clearRequests removes requests left by earlier runs of the tests, since
// ClaimRequest takes the oldest one in the whole database.
func clearRequests(t *testing.T, s *Store) {
	if _, err := s.pool.Exec(context.Background(), `DELETE FROM run_requests WHERE status IN ('queued', 'starting')`); err != nil {
		t.Fatal(err)
	}
}

func approvalServer(t *testing.T, tokens ...string) *httptest.Server {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == tenancy.ApprovalPath {
			fmt.Fprintln(w, strings.Join(tokens, "\n"))
			return
		}
		w.WriteHeader(200)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestAgentChecksTheTenantNotTheRunRow(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()
	srv := approvalServer(t, "tok-a")
	org := newOrg(t, s, srv.URL, "tok-a", true)

	_, env, err := s.Tenant(ctx, org, "staging")
	if err != nil {
		t.Fatal(err)
	}
	sc := testScenario("http://ignored:1", 50, time.Second)
	if err := tenancy.Bind(sc, env); err != nil {
		t.Fatal(err)
	}
	clearRequests(t, s)
	id := newRequest(t, s, org)
	if req, err := s.ClaimRequest(ctx, "ctl-1", 5*time.Second); err != nil || req == nil || req.ID != id {
		t.Fatalf("claim request: %+v %v", req, err)
	}
	if _, err := s.CreateRun(ctx, NewRun{
		ID: id, Scenario: sc, Seed: 3, ShardCount: 2, Lead: time.Second, LeaseTTL: 5 * time.Second,
		Organization: org, Environment: "staging", Controller: "ctl-1",
	}); err != nil {
		t.Fatal(err)
	}
	run, err := s.GetRun(ctx, id)
	if err != nil || run.Organization != org || run.Environment != "staging" {
		t.Fatalf("run %+v err %v", run, err)
	}
	if err := s.CheckTenantRun(ctx, run, nil); err != nil {
		t.Fatalf("a valid run was refused: %v", err)
	}

	// Each of these is a change an agent must catch on its own.
	check := func(what, want string, mutate func(r *Run), undo func()) {
		t.Helper()
		r := *run
		sc2 := *run.Scenario
		r.Scenario = &sc2
		if mutate != nil {
			mutate(&r)
		}
		err := s.CheckTenantRun(ctx, &r, []string{"proxy-host"})
		if undo != nil {
			undo()
		}
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: got %v, want an error containing %q", what, err, want)
		}
	}
	check("retargeted run row", "targets", func(r *Run) { r.Scenario.Target.BaseURL = "http://127.0.0.1:1" }, nil)
	_, _ = s.pool.Exec(ctx, `UPDATE organizations SET max_arrivals = 10 WHERE id = $1`, org)
	check("quota lowered after the run was created", "max_arrivals", nil, nil)
	_, _ = s.pool.Exec(ctx, `UPDATE organizations SET max_arrivals = 100 WHERE id = $1`, org)
	check("untrusted traffic URL", "doesn't trust", func(r *Run) { r.TrafficURL = "http://elsewhere:8081" }, nil)
	_, _ = s.pool.Exec(ctx, `UPDATE environments SET approval_token = 'tok-b' WHERE organization_id = $1`, org)
	check("target stopped approving", "approval token", nil, nil)
	_, _ = s.pool.Exec(ctx, `UPDATE environments SET verified_at = NULL WHERE organization_id = $1`, org)
	check("unverified environment", "not verified", nil, nil)
}

func TestRequestLeases(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()
	srv := approvalServer(t, "tok")
	org := newOrg(t, s, srv.URL, "tok", true)
	clearRequests(t, s)
	a, b := newRequest(t, s, org), newRequest(t, s, org)

	// Two controllers never get the same request.
	var mu sync.Mutex
	got := map[string]string{}
	var wg sync.WaitGroup
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			name := fmt.Sprintf("ctl-%d", i)
			r, err := s.ClaimRequest(ctx, name, 300*time.Millisecond)
			if err != nil {
				t.Error(err)
				return
			}
			if r != nil {
				mu.Lock()
				defer mu.Unlock()
				if prev, dup := got[r.ID]; dup {
					t.Errorf("%s claimed by %s and %s", r.ID, prev, name)
				}
				got[r.ID] = name
			}
		}(i)
	}
	wg.Wait()
	if len(got) != 2 || got[a] == "" || got[b] == "" {
		t.Fatalf("claims %v", got)
	}

	// A controller that stops renewing loses the request, and can no longer
	// turn it into a run.
	time.Sleep(400 * time.Millisecond)
	held, err := s.RenewRequests(ctx, got[a], []string{a}, time.Second)
	if err != nil || held[a] {
		t.Fatalf("renewed an expired lease: %v %v", held, err)
	}
	r, err := s.ClaimRequest(ctx, "ctl-new", 5*time.Second)
	if err != nil || r == nil {
		t.Fatalf("expired request not reclaimed: %v", err)
	}
	sc := testScenario(srv.URL, 10, time.Second)
	if _, err := s.CreateRun(ctx, NewRun{
		ID: r.ID, Scenario: sc, Seed: 3, ShardCount: 1, Lead: time.Second, LeaseTTL: time.Second,
		Organization: org, Environment: "staging", Controller: got[r.ID],
	}); err == nil {
		t.Fatal("the old controller created a run for a request it lost")
	}
	if _, err := s.CreateRun(ctx, NewRun{
		ID: r.ID, Scenario: sc, Seed: 3, ShardCount: 1, Lead: time.Second, LeaseTTL: time.Second,
		Organization: org, Environment: "staging", Controller: "ctl-new",
	}); err != nil {
		t.Fatal(err)
	}

	// If ctl-new dies, another controller adopts the open run.
	_, _ = s.pool.Exec(ctx, `UPDATE run_requests SET lease_until = now() - interval '1s' WHERE id = $1`, r.ID)
	ids, err := s.AdoptRuns(ctx, "ctl-adopter", 5*time.Second)
	if err != nil || len(ids) != 1 || ids[0] != r.ID {
		t.Fatalf("adopt: %v %v", ids, err)
	}
	if ids, _ := s.AdoptRuns(ctx, "ctl-other", 5*time.Second); len(ids) != 0 {
		t.Fatalf("adopted a run with a live lease: %v", ids)
	}
	if err := s.ReleaseRequest(ctx, r.ID, "ctl-adopter"); err != nil {
		t.Fatal(err)
	}

	// The other request is rejected with a reason.
	other := a
	if r.ID == a {
		other = b
	}
	_, _ = s.pool.Exec(ctx, `UPDATE run_requests SET controller = 'ctl-x', lease_until = now() + interval '5s', status = 'starting' WHERE id = $1`, other)
	if err := s.RejectRequest(ctx, other, "ctl-x", "quota max_arrivals: too many"); err != nil {
		t.Fatal(err)
	}
	var status, reason string
	_ = s.pool.QueryRow(ctx, `SELECT status, reason FROM run_requests WHERE id = $1`, other).Scan(&status, &reason)
	if status != "rejected" || !strings.Contains(reason, "max_arrivals") {
		t.Fatalf("status %s reason %q", status, reason)
	}
}
