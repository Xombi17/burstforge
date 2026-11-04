package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"burstforge/internal/runner"
	"burstforge/internal/scenario"
	"burstforge/internal/store"
	"burstforge/internal/tenancy"
)

// The controller turns run requests from the control plane into runs. For
// each request it binds the scenario to the organization's environment,
// checks the quotas and the target's approval token, runs the before hooks,
// stores the run for agents to claim, and finalizes it when the shards are
// done. Several controllers can share a database: requests are claimed with
// SKIP LOCKED and held with a lease, and a run whose controller stopped
// renewing is adopted and finalized by another.
type controller struct {
	st             *store.Store
	name           string
	leaseTTL       time.Duration // for agents' shard leases
	requestTTL     time.Duration // for this controller's hold on a request
	lead           time.Duration
	drain          time.Duration
	proxyListen    string
	proxyAdvertise string

	mu   sync.Mutex // guards held and serializes log lines
	held map[string]context.CancelFunc
}

func cmdController(args []string) int {
	fs := flag.NewFlagSet("controller", flag.ContinueOnError)
	url := dbURLFlag(fs)
	c := &controller{held: map[string]context.CancelFunc{}}
	fs.StringVar(&c.name, "name", "", "controller name (default host-pid)")
	fs.DurationVar(&c.leaseTTL, "lease-ttl", 5*time.Second, "agents' shard lease TTL for the runs this controller starts")
	fs.DurationVar(&c.requestTTL, "request-ttl", 10*time.Second, "how long this controller's hold on a request lasts without renewal")
	fs.DurationVar(&c.lead, "lead", 3*time.Second, "time between storing a run and its first arrival")
	fs.DurationVar(&c.drain, "drain-timeout", 2*time.Minute, "after a run's planned end, wait this long for open shards")
	fs.StringVar(&c.proxyListen, "proxy-listen", "127.0.0.1:0", "address for per-run fault proxies; port 0 gives each run its own port")
	fs.StringVar(&c.proxyAdvertise, "proxy-advertise", "", "host agents use to reach the fault proxies (default: the listen host)")
	maxRuns := fs.Int("max-runs", 10, "requests this controller starts or drives at once")
	poll := fs.Duration("poll", 500*time.Millisecond, "how often to look for requests")
	pos, err := parse(fs, args)
	if err != nil || len(pos) != 0 || *url == "" {
		fmt.Fprintln(os.Stderr, "usage: burstforge controller --db URL [--proxy-listen ADDR --proxy-advertise HOST] [--max-runs N]")
		return 2
	}
	if c.name == "" {
		host, _ := os.Hostname()
		c.name = fmt.Sprintf("%s-%d", host, os.Getpid())
	}
	if c.requestTTL < time.Second {
		fmt.Fprintln(os.Stderr, "--request-ttl must be at least 1s")
		return 2
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	c.st, err = store.Open(ctx, *url)
	if err != nil {
		fmt.Fprintln(os.Stderr, "opening the database:", err)
		return 2
	}
	defer c.st.Close()
	logf("controller %s: waiting for run requests", c.name)

	go c.renewLoop(ctx)
	var wg sync.WaitGroup
	tick := time.NewTicker(*poll)
	defer tick.Stop()
	for ctx.Err() == nil {
		if c.count() < *maxRuns {
			ids, err := c.st.AdoptRuns(ctx, c.name, c.requestTTL)
			if err != nil && ctx.Err() == nil {
				logf("adopting runs: %v", err)
			}
			for _, id := range ids {
				rctx := c.hold(ctx, id)
				wg.Add(1)
				go func(id string) {
					defer wg.Done()
					defer c.drop(id)
					c.runLog(id)("adopted from a controller that stopped renewing; fault counts, if any, are lost")
					c.drive(rctx, id, nil)
				}(id)
			}
		}
		if c.count() < *maxRuns {
			req, err := c.st.ClaimRequest(ctx, c.name, c.requestTTL)
			if err != nil && ctx.Err() == nil {
				logf("claiming a request: %v", err)
			}
			if req != nil {
				rctx := c.hold(ctx, req.ID)
				wg.Add(1)
				go func() {
					defer wg.Done()
					defer c.drop(req.ID)
					c.start(rctx, req)
				}()
				continue // look for the next one right away
			}
		}
		select {
		case <-ctx.Done():
		case <-tick.C:
		}
	}
	if n := c.count(); n > 0 {
		logf("stopping; %d request(s) will be adopted by another controller once the lease (%s) runs out", n, c.requestTTL)
	}
	wg.Wait()
	return 0
}

func (c *controller) hold(parent context.Context, id string) context.Context {
	ctx, cancel := context.WithCancel(parent)
	c.mu.Lock()
	c.held[id] = cancel
	c.mu.Unlock()
	return ctx
}

func (c *controller) drop(id string) {
	c.mu.Lock()
	if cancel, ok := c.held[id]; ok {
		cancel()
		delete(c.held, id)
	}
	c.mu.Unlock()
}

func (c *controller) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.held)
}

// renewLoop extends the leases on held requests and cancels the work on any
// this controller has lost, so it never finalizes a run another controller
// has adopted.
func (c *controller) renewLoop(ctx context.Context) {
	t := time.NewTicker(c.requestTTL / 3)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		c.mu.Lock()
		ids := make([]string, 0, len(c.held))
		for id := range c.held {
			ids = append(ids, id)
		}
		c.mu.Unlock()
		if len(ids) == 0 {
			continue
		}
		rctx, cancel := context.WithTimeout(ctx, c.requestTTL/3)
		still, err := c.st.RenewRequests(rctx, c.name, ids, c.requestTTL)
		cancel()
		if err != nil {
			logf("renewing request leases: %v", err)
			continue
		}
		for _, id := range ids {
			if !still[id] {
				c.runLog(id)("lost the lease on this request; stopping work on it")
				c.mu.Lock()
				if cancel, ok := c.held[id]; ok {
					cancel()
				}
				c.mu.Unlock()
			}
		}
	}
}

func (c *controller) runLog(id string) func(string, ...any) {
	return func(f string, a ...any) {
		c.mu.Lock()
		defer c.mu.Unlock()
		fmt.Printf("  [%s] "+f+"\n", append([]any{id}, a...)...)
	}
}

// start checks a request and turns it into a run, or rejects it with the
// reason. Every check here is repeated by the agents.
func (c *controller) start(ctx context.Context, req *store.RunRequest) {
	log := c.runLog(req.ID)
	reject := func(reason string) {
		log("rejected: %s", reason)
		if err := c.st.RejectRequest(context.Background(), req.ID, c.name, reason); err != nil {
			log("recording the rejection: %v", err)
		}
	}
	lim, env, err := c.st.Tenant(ctx, req.Organization, req.Environment)
	if err != nil {
		reject(err.Error())
		return
	}
	if !env.Verified {
		reject(fmt.Sprintf("environment %s is not verified; the target must serve its approval token at %s", env.Name, tenancy.ApprovalPath))
		return
	}
	sc, err := scenario.Decode([]byte(req.ScenarioYAML), "run")
	if err == nil {
		err = tenancy.Bind(sc, env)
	}
	if err == nil {
		err = sc.Validate()
	}
	if err == nil {
		err = sc.CheckTarget()
	}
	if err == nil && (req.ShardCount < 1 || req.ShardCount > sc.Traffic.Arrivals) {
		err = fmt.Errorf("shards must be between 1 and the %d arrivals", sc.Traffic.Arrivals)
	}
	if err != nil {
		reject("scenario: " + err.Error())
		return
	}
	shape, err := tenancy.Measure(sc, req.Seed)
	if err == nil {
		err = tenancy.CheckLimits(shape, lim)
	}
	if err != nil {
		reject(err.Error())
		return
	}
	if err := tenancy.CheckApproval(ctx, env.BaseURL, env.ApprovalToken); err != nil {
		reject(err.Error())
		return
	}

	faults, err := runner.StartFaults(ctx, sc, c.proxyListen)
	if err != nil {
		reject("starting the fault proxy: " + err.Error())
		return
	}
	defer faults.Close()
	trafficURL := ""
	if faults != nil {
		trafficURL = c.advertise(faults.URL)
	}
	before, err := runner.Prepare(ctx, sc)
	if err != nil {
		if ctx.Err() == nil {
			reject(err.Error())
		}
		return
	}
	startAt, err := c.st.CreateRun(ctx, store.NewRun{
		ID: req.ID, Scenario: sc, Seed: req.Seed, ShardCount: req.ShardCount,
		Lead: max(c.lead, sc.Traffic.StartDelay), LeaseTTL: c.leaseTTL, Before: before, TrafficURL: trafficURL,
		Organization: req.Organization, Environment: req.Environment, Controller: c.name,
	})
	if err != nil {
		// Most likely the lease ran out and another controller has the
		// request; it will run the hooks again and store the run.
		log("storing the run: %v", err)
		return
	}
	log("%s/%s: %d arrivals on %d shard(s), first at %s on the database clock",
		req.Organization, req.Environment, shape.Arrivals, req.ShardCount, startAt.Format("15:04:05.000"))
	if faults != nil {
		if err := faults.Apply(ctx, req.Seed, startAt); err != nil {
			log("configuring the fault proxy: %v", err)
		} else {
			log("workflow traffic goes through the fault proxy at %s", trafficURL)
		}
	}
	c.drive(ctx, req.ID, faults)
}

// drive waits for a run's shards and finalizes it. If the lease on the
// request is lost meanwhile, it stops without finalizing.
func (c *controller) drive(ctx context.Context, runID string, faults *runner.FaultSession) {
	log := c.runLog(runID)
	run, err := c.st.GetRun(ctx, runID)
	if err != nil {
		log("%v", err)
		return
	}
	base, err := runner.BaseReport(run.Scenario, run.ID, run.Seed)
	if err != nil {
		log("%v", err)
		return
	}
	deadline := run.StartAt.Add(time.Duration(base.Traffic.PlannedSpan*float64(time.Millisecond)) + c.drain)
	waitForShards(ctx, c.st, runID, deadline, &sync.Mutex{}, log)
	if ctx.Err() != nil {
		return
	}
	rep, err := finalizeRun(c.st, runID, false, faults.Collect(log), log)
	if err != nil {
		log("finalizing: %v", err)
		return
	}
	if err := c.st.ReleaseRequest(context.Background(), runID, c.name); err != nil {
		log("releasing the request: %v", err)
	}
	verdict := "PASS"
	if !rep.Verdict.Pass {
		verdict = "FAIL"
	}
	log("completed: %s, %d workflows, %d attempts, amplification %.2fx", verdict,
		rep.Totals.Logical, rep.Attempts.Total, rep.Attempts.RetryAmplification)
}

// advertise swaps the proxy's listen host for the one agents should use.
func (c *controller) advertise(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	host := c.proxyAdvertise
	if host == "" {
		host = u.Hostname()
	}
	u.Host = net.JoinHostPort(host, u.Port())
	return u.String()
}
