package store

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"burstforge/internal/tenancy"
)

// Tenant loads an organization's limits and one of its environments.
func (s *Store) Tenant(ctx context.Context, org, env string) (tenancy.Limits, tenancy.Environment, error) {
	var l tenancy.Limits
	var e tenancy.Environment
	var durMs int64
	var verified *time.Time
	err := s.pool.QueryRow(ctx, `
		SELECT o.max_concurrent_runs, o.max_runs_per_hour, o.max_arrivals, o.max_arrival_rate, o.max_duration_ms,
		       e.name, e.base_url, e.approval_token, e.verified_at
		FROM organizations o JOIN environments e ON e.organization_id = o.id
		WHERE o.id = $1 AND e.name = $2`, org, env).Scan(
		&l.MaxConcurrentRuns, &l.MaxRunsPerHour, &l.MaxArrivals, &l.MaxArrivalRate, &durMs,
		&e.Name, &e.BaseURL, &e.ApprovalToken, &verified)
	if errors.Is(err, pgx.ErrNoRows) {
		return l, e, fmt.Errorf("organization %q has no environment %q", org, env)
	}
	l.MaxDuration = time.Duration(durMs) * time.Millisecond
	e.Org = org
	e.Verified = verified != nil
	return l, e, err
}

// CheckTenantRun is what an agent checks before it sends any traffic for a
// run that belongs to an organization. It reads the organization's limits
// and environment again instead of trusting the run row, and asks the
// target for the approval token itself. trustedProxies are the hosts this
// agent may send traffic through when the run has a fault proxy.
func (s *Store) CheckTenantRun(ctx context.Context, run *Run, trustedProxies []string) error {
	lim, env, err := s.Tenant(ctx, run.Organization, run.Environment)
	if err != nil {
		return err
	}
	if !env.Verified {
		return fmt.Errorf("environment %s of %s is not verified", env.Name, env.Org)
	}
	if err := tenancy.CheckBound(run.Scenario, env); err != nil {
		return err
	}
	shape, err := tenancy.Measure(run.Scenario, run.Seed)
	if err != nil {
		return err
	}
	if err := tenancy.CheckLimits(shape, lim); err != nil {
		return err
	}
	if run.TrafficURL != "" {
		u, err := url.Parse(run.TrafficURL)
		if err != nil {
			return fmt.Errorf("traffic URL: %w", err)
		}
		ok := false
		for _, h := range trustedProxies {
			ok = ok || strings.EqualFold(h, u.Hostname())
		}
		if !ok {
			return fmt.Errorf("the run sends traffic through %s, which this agent doesn't trust (see --trust-proxy)", u.Host)
		}
	}
	return tenancy.CheckApproval(ctx, env.BaseURL, env.ApprovalToken)
}

// RunRequest is a run requested through the control plane.
type RunRequest struct {
	ID           string
	Organization string
	Environment  string
	ScenarioYAML string
	Seed         int64
	ShardCount   int
}

// ClaimRequest takes the oldest queued request, or one left in 'starting'
// by a controller whose lease ran out, and holds it for ttl.
func (s *Store) ClaimRequest(ctx context.Context, controller string, ttl time.Duration) (*RunRequest, error) {
	var r RunRequest
	err := s.pool.QueryRow(ctx, `
		UPDATE run_requests SET status = 'starting', controller = $1,
		       lease_until = now() + make_interval(secs => $2), updated_at = now()
		WHERE id = (
			SELECT id FROM run_requests
			WHERE status = 'queued' OR (status = 'starting' AND lease_until < now())
			ORDER BY created_at LIMIT 1
			FOR UPDATE SKIP LOCKED)
		RETURNING id, organization_id, environment, scenario_yaml, seed, shard_count`,
		controller, ttl.Seconds()).Scan(&r.ID, &r.Organization, &r.Environment, &r.ScenarioYAML, &r.Seed, &r.ShardCount)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &r, nil
}

// RejectRequest records why the controller won't start a request.
func (s *Store) RejectRequest(ctx context.Context, id, controller, reason string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE run_requests SET status = 'rejected', reason = $3, lease_until = NULL, updated_at = now()
		WHERE id = $1 AND controller = $2 AND status = 'starting'`, id, controller, reason)
	return err
}

// RenewRequests extends this controller's lease on the requests it is
// starting or driving and returns the ones it still holds.
func (s *Store) RenewRequests(ctx context.Context, controller string, ids []string, ttl time.Duration) (map[string]bool, error) {
	rows, err := s.pool.Query(ctx, `
		UPDATE run_requests SET lease_until = now() + make_interval(secs => $3)
		WHERE controller = $1 AND id = ANY($2) AND status IN ('starting', 'started') AND lease_until > now()
		RETURNING id`, controller, ids, ttl.Seconds())
	if err != nil {
		return nil, err
	}
	held := map[string]bool{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		held[id] = true
	}
	return held, rows.Err()
}

// AdoptRuns takes over started runs that are still open but whose
// controller stopped renewing its lease, so they still get finalized.
func (s *Store) AdoptRuns(ctx context.Context, controller string, ttl time.Duration) ([]string, error) {
	rows, err := s.pool.Query(ctx, `
		UPDATE run_requests q SET controller = $1, lease_until = now() + make_interval(secs => $2)
		WHERE q.id IN (
			SELECT q2.id FROM run_requests q2 JOIN runs r ON r.id = q2.id
			WHERE q2.status = 'started' AND r.status IN ('running', 'finalizing') AND q2.lease_until < now()
			FOR UPDATE OF q2 SKIP LOCKED)
		RETURNING q.id`, controller, ttl.Seconds())
	if err != nil {
		return nil, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// ReleaseRequest drops the controller's lease once its run is finalized.
func (s *Store) ReleaseRequest(ctx context.Context, id, controller string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE run_requests SET lease_until = NULL, updated_at = now()
		WHERE id = $1 AND controller = $2`, id, controller)
	return err
}
