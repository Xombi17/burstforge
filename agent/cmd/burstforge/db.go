package main

// Lease-based runs: the coordinator stores the run in Postgres, agents claim
// shards with leases, and a dead agent's shard is taken over by another.

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"time"

	"burstforge/internal/report"
	"burstforge/internal/runner"
	"burstforge/internal/scenario"
	"burstforge/internal/store"
)

type dbFlags struct {
	url          string
	shards       int
	leaseTTL     time.Duration
	lead         time.Duration
	drainTimeout time.Duration
}

// addDBFlags registers the run --db flags. run only switches to Postgres
// with an explicit --db, so a stray $BURSTFORGE_DB never changes what a
// plain `run` does.
func addDBFlags(fs *flag.FlagSet) *dbFlags {
	f := &dbFlags{}
	fs.StringVar(&f.url, "db", "", "Postgres URL; store the run there and hand shards to agents under leases")
	fs.IntVar(&f.shards, "shards", 1, "with --db: number of shards")
	fs.DurationVar(&f.leaseTTL, "lease-ttl", 5*time.Second, "with --db: a shard is taken over after its agent misses heartbeats for this long")
	fs.DurationVar(&f.lead, "lead", 3*time.Second, "with --db: time between storing the run and its first arrival, for agents to claim shards")
	fs.DurationVar(&f.drainTimeout, "drain-timeout", 2*time.Minute, "with --db: after the planned end, wait this long for shards before abandoning them")
	return f
}

func dbURLFlag(fs *flag.FlagSet) *string {
	return fs.String("db", os.Getenv("BURSTFORGE_DB"), "Postgres URL (default $BURSTFORGE_DB)")
}

func coordinateDB(ctx context.Context, path string, sc *scenario.Scenario, sf *scenarioFlags, df *dbFlags,
	agents int, seed int64, runID string) (*report.Report, error) {
	if err := preflight(sc, sf); err != nil {
		return nil, err
	}
	if df.leaseTTL < 250*time.Millisecond {
		return nil, fmt.Errorf("--lease-ttl must be at least 250ms")
	}
	base, err := runner.BaseReport(sc, runID, seed)
	if err != nil {
		return nil, err
	}
	st, err := store.Open(ctx, df.url)
	if err != nil {
		return nil, fmt.Errorf("opening the database: %w", err)
	}
	defer st.Close()

	fs, err := runner.StartFaults(ctx, sc, "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	defer fs.Close()
	trafficURL := ""
	if fs != nil {
		trafficURL = fs.URL
		if fs.Embedded() && agents == 0 {
			logf("note: the fault proxy is embedded on %s, which agents on other machines can't reach; "+
				"run `burstforge proxy` and set faults.proxy for remote agents", fs.URL)
		}
	}
	before, err := runner.Prepare(ctx, sc)
	if err != nil {
		return nil, err
	}
	lead := max(df.lead, sc.Traffic.StartDelay)
	startAt, err := st.CreateRun(ctx, store.NewRun{
		ID: runID, Scenario: sc, Seed: seed, ShardCount: df.shards,
		Lead: lead, LeaseTTL: df.leaseTTL, Before: before, TrafficURL: trafficURL,
	})
	if err != nil {
		return nil, fmt.Errorf("storing the run: %w", err)
	}
	logf("run stored; first arrival at %s on the database clock (in %s)", startAt.Format("15:04:05.000"), lead)
	if fs != nil {
		// Same machine as the embedded proxy, so the database start time
		// is close enough for every/for windows.
		if err := fs.Apply(ctx, seed, startAt); err != nil {
			return nil, err
		}
		logf("workflow traffic goes through the fault proxy at %s", fs.URL)
	}
	if agents == 0 {
		logf("waiting for agents. Start them with:  burstforge agent --db <url> --run %s", runID)
	}

	// Local workers. They exit by themselves once the run stops handing out shards.
	var wg sync.WaitGroup
	var mu sync.Mutex
	var procs []*exec.Cmd
	if agents > 0 {
		exe, err := os.Executable()
		if err != nil {
			return nil, err
		}
		for k := 1; k <= agents; k++ {
			cmd := exec.Command(exe, "agent", "--db", df.url, "--run", runID,
				"--name", fmt.Sprintf("local-%d", k), "--max-arrivals", strconv.Itoa(sf.maxArrivals))
			pipe, err := cmd.StdoutPipe()
			if err != nil {
				return nil, err
			}
			cmd.Stderr = cmd.Stdout
			if err := cmd.Start(); err != nil {
				return nil, fmt.Errorf("starting agent %d: %w", k, err)
			}
			procs = append(procs, cmd)
			wg.Add(1)
			go func(k int) {
				defer wg.Done()
				prefixLines(pipe, fmt.Sprintf("  [agent %d pid %d] ", k, cmd.Process.Pid), &mu)
				_ = cmd.Wait()
			}(k)
		}
	}

	deadline := startAt.Add(time.Duration(base.Traffic.PlannedSpan*float64(time.Millisecond)) + df.drainTimeout)
	aborted := waitForShards(ctx, st, runID, deadline, &mu)

	var faults *report.FaultReport
	if fs != nil {
		faults = fs.Collect(logf)
	}
	rep, err := finalizeRun(st, runID, aborted, faults)

	// Workers notice the run is closed within a poll interval; nudge any
	// that don't.
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		for _, p := range procs {
			_ = p.Process.Signal(os.Interrupt)
		}
		<-done
	}
	return rep, err
}

// waitForShards polls shard state, printing changes, until every shard is
// completed or abandoned, the deadline passes, or ctx ends. It reports
// whether the run was interrupted.
func waitForShards(ctx context.Context, st *store.Store, runID string, deadline time.Time, mu *sync.Mutex) bool {
	type seen struct {
		state string
		epoch int64
	}
	last := map[int]seen{}
	t := time.NewTicker(250 * time.Millisecond)
	defer t.Stop()
	for {
		shards, err := st.Shards(context.Background(), runID)
		if err != nil {
			logf("reading shard state: %v", err)
		}
		open := 0
		for _, s := range shards {
			prev, known := last[s.Shard]
			now := seen{s.State, s.Epoch}
			if !known || prev != now {
				mu.Lock()
				switch {
				case s.State == "claimed" && s.Epoch > 1:
					logf("shard %d: taken over by %s (lease %d), resuming at arrival %d of %d",
						s.Shard, s.Agent, s.Epoch, s.ResumedFrom+1, s.Arrivals)
				case s.State == "claimed":
					logf("shard %d: claimed by %s", s.Shard, s.Agent)
				case s.State == "completed":
					logf("shard %d: completed by %s", s.Shard, s.Agent)
				}
				mu.Unlock()
				last[s.Shard] = now
			}
			if s.State == "pending" || s.State == "claimed" {
				open++
			}
		}
		if err == nil && open == 0 {
			return false
		}
		if time.Now().After(deadline) {
			logf("drain deadline passed with %d shard(s) open; abandoning them", open)
			return false
		}
		select {
		case <-ctx.Done():
			logf("interrupted; finalizing what was recorded")
			return true
		case <-t.C:
		}
	}
}

// finalizeRun closes the run: fence open shards, check the target, reduce
// the telemetry and store the report. Safe to repeat after a crash.
func finalizeRun(st *store.Store, runID string, aborted bool, faults *report.FaultReport) (*report.Report, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	run, err := st.BeginFinalize(ctx, runID)
	if err != nil {
		return nil, err
	}
	if run.Status == "completed" {
		logf("run %s was already finalized; showing the stored report", runID)
		return st.CompleteRun(ctx, runID, nil)
	}
	tc := runner.CheckTarget(run.Scenario, run.Before, logf)
	return st.CompleteRun(ctx, runID, func(rep *report.Report) error {
		tc.Apply(rep, run.Before)
		rep.Faults = faults
		rep.Totals.Aborted = rep.Totals.Aborted || aborted
		runner.Evaluate(run.Scenario, rep)
		return nil
	})
}

func cmdAgentDB(args []string) int {
	fs := flag.NewFlagSet("agent", flag.ContinueOnError)
	url := dbURLFlag(fs)
	runFilter := fs.String("run", "", "only work on this run, and exit when it stops handing out shards")
	name := fs.String("name", "", "agent name (default host-pid)")
	poll := fs.Duration("poll", 250*time.Millisecond, "how often to look for a shard when idle")
	maxArrivals := fs.Int("max-arrivals", 10000, "refuse runs with more arrivals than this")
	maxUncertainty := fs.Duration("max-clock-uncertainty", 100*time.Millisecond,
		"refuse to claim if the clock offset to the database can't be measured more precisely than this")
	pos, err := parse(fs, args)
	if err != nil || len(pos) != 0 || *url == "" {
		fmt.Fprintln(os.Stderr, "usage: burstforge agent --db URL [--run ID] [--name NAME]")
		return 2
	}
	if *name == "" {
		host, _ := os.Hostname()
		*name = fmt.Sprintf("%s-%d", host, os.Getpid())
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	st, err := store.Open(ctx, *url)
	if err != nil {
		fmt.Fprintln(os.Stderr, "opening the database:", err)
		return 2
	}
	defer st.Close()

	sleep := func(d time.Duration) {
		select {
		case <-ctx.Done():
		case <-time.After(d):
		}
	}
	refused := map[string]bool{}
	for ctx.Err() == nil {
		offset, uncertainty, err := st.MeasureClock(ctx)
		if err != nil {
			logf("measuring the database clock: %v", err)
			sleep(time.Second)
			continue
		}
		if uncertainty > *maxUncertainty {
			logf("clock offset to the database is uncertain by %s (limit %s); not claiming", uncertainty, *maxUncertainty)
			sleep(time.Second)
			continue
		}
		c, err := st.Claim(ctx, *name, *runFilter, offset, uncertainty)
		if err != nil {
			if ctx.Err() == nil {
				logf("claiming a shard: %v", err)
				sleep(time.Second)
			}
			continue
		}
		if c == nil {
			if *runFilter != "" {
				if run, err := st.GetRun(ctx, *runFilter); err == nil && run.Status != "running" {
					logf("run %s is %s; exiting", *runFilter, run.Status)
					return 0
				}
			}
			sleep(*poll)
			continue
		}
		if refused[c.Run.ID] {
			_ = c.Lease.Release(ctx, c.Resume, nil)
			sleep(2 * time.Second)
			continue
		}
		logf("claimed shard %d/%d of run %s (lease %d, from arrival %d, clock offset %s ±%s)",
			c.Shard, c.Run.ShardCount, c.Run.ID, c.Epoch, c.Resume+1,
			offset.Round(10*time.Microsecond), uncertainty.Round(10*time.Microsecond))
		rep, err := store.RunClaim(ctx, c, *name, offset, *maxArrivals, logf)
		switch {
		case errors.Is(err, runner.ErrFenced):
			logf("shard %d: lost the lease; another agent owns it now", c.Shard)
		case err != nil && rep == nil:
			// Refused before sending anything (safety checks). Hand the shard back.
			logf("shard %d: %v; releasing it", c.Shard, err)
			refused[c.Run.ID] = true
			_ = c.Lease.Release(context.Background(), c.Resume, nil)
		case err != nil:
			logf("shard %d: %v", c.Shard, err)
		case ctx.Err() != nil:
			logf("shard %d: interrupted after %d workflows; lease released for another agent to resume", c.Shard, rep.Totals.Logical)
		default:
			logf("shard %d/%d done: %d workflows, %d attempts, %d skipped as overdue",
				c.Shard, c.Run.ShardCount, rep.Totals.Logical, rep.Attempts.Total, rep.Totals.SkippedOverdue)
		}
		if n := c.Lease.Dropped(); n > 0 {
			logf("shard %d: %d telemetry chunk(s) arrived after the run was finalized and were dropped", c.Shard, n)
		}
	}
	return 0
}

func cmdFinalize(args []string) int {
	fs := flag.NewFlagSet("finalize", flag.ContinueOnError)
	url := dbURLFlag(fs)
	out := fs.String("out", "burstforge-results", "directory for report files")
	now := fs.Bool("now", false, "don't wait for open shards; abandon them immediately")
	drain := fs.Duration("drain-timeout", 2*time.Minute, "after the planned end, wait this long for open shards")
	pos, err := parse(fs, args)
	if err != nil || len(pos) != 1 || *url == "" {
		fmt.Fprintln(os.Stderr, "usage: burstforge finalize --db URL <run-id> [--now]")
		return 2
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	st, err := store.Open(ctx, *url)
	if err != nil {
		fmt.Fprintln(os.Stderr, "opening the database:", err)
		return 2
	}
	defer st.Close()

	run, err := st.GetRun(ctx, pos[0])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	if run.Status == "running" && !*now {
		// Agents may still be working; finalizing now would fence them and
		// count their in-flight arrivals as unaccounted.
		base, err := runner.BaseReport(run.Scenario, run.ID, run.Seed)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 2
		}
		deadline := run.StartAt.Add(time.Duration(base.Traffic.PlannedSpan*float64(time.Millisecond)) + *drain)
		logf("waiting for open shards (until %s; --now to skip)", deadline.Local().Format("15:04:05"))
		// Ctrl-C here means "stop waiting", not "the run was aborted".
		_ = waitForShards(ctx, st, run.ID, deadline, &sync.Mutex{})
	}
	if run.Scenario.Faults.Enabled() {
		logf("fault counts are only recorded by the coordinator that ran the proxy; this report won't have them")
	}
	rep, err := finalizeRun(st, run.ID, false, nil)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		return 2
	}
	return finish(rep, filepath.Join(*out, rep.RunID))
}

func cmdStatus(args []string) int {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	url := dbURLFlag(fs)
	pos, err := parse(fs, args)
	if err != nil || len(pos) > 1 || *url == "" {
		fmt.Fprintln(os.Stderr, "usage: burstforge status --db URL [run-id]")
		return 2
	}
	ctx := context.Background()
	st, err := store.Open(ctx, *url)
	if err != nil {
		fmt.Fprintln(os.Stderr, "opening the database:", err)
		return 2
	}
	defer st.Close()
	if len(pos) == 0 {
		runs, err := st.ListRuns(ctx, 20)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 2
		}
		fmt.Printf("%-24s %-28s %-11s %s\n", "run", "scenario", "status", "shards done")
		for _, r := range runs {
			fmt.Printf("%-24s %-28s %-11s %d/%d\n", r.ID, r.Scenario, r.Status, r.Done, r.Shards)
		}
		return 0
	}
	run, err := st.GetRun(ctx, pos[0])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	shards, err := st.Shards(ctx, run.ID)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	fmt.Printf("run %s  %s  status %s  start %s  lease ttl %s\n",
		run.ID, run.Scenario.Name, run.Status, run.StartAt.Format(time.RFC3339Nano), run.LeaseTTL)
	fmt.Printf("  %-6s %-10s %-6s %-14s %-24s %s\n", "shard", "state", "lease", "reserved", "agent", "lease expires")
	for _, s := range shards {
		exp := ""
		if s.State == "claimed" {
			exp = fmt.Sprintf("in %s", s.LeaseExpiresIn.Round(time.Millisecond))
			if s.LeaseExpiresIn <= 0 {
				exp = fmt.Sprintf("%s ago (claimable)", (-s.LeaseExpiresIn).Round(time.Millisecond))
			}
		}
		fmt.Printf("  %-6d %-10s %-6d %-14s %-24s %s\n", s.Shard, s.State, s.Epoch,
			fmt.Sprintf("%d/%d", s.ReservedThrough, s.Arrivals), s.Agent, exp)
	}
	return 0
}
