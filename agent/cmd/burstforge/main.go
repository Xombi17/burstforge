// Command burstforge replays bursty workflows against a staging target and
// reports retry amplification, latency and business-invariant failures.
package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"burstforge/internal/report"
	"burstforge/internal/runner"
	"burstforge/internal/scenario"
)

const usage = `burstforge %s

Usage:
  burstforge validate <scenario.yaml>
  burstforge run <scenario.yaml> [--agents N] [--out DIR] [--seed N] [scenario flags]
  burstforge agent <scenario.yaml> --shard K/N --seed N --run-id ID --start-at TIME
                                   [--out DIR] [--name NAME] [scenario flags]
  burstforge merge <scenario.yaml> <shard.json>... [--out DIR] [--no-check]
                                   [--allow-partial] [scenario flags]
  burstforge compare <before/report.json> <after/report.json>

Scenario flags: [--arrivals N] [--max-arrivals N] [--target URL] [--allow-host HOST]

run --agents N splits the run across N local agent processes and merges their
reports. agent and merge are the same two halves, for running agents on
separate machines.

Exit codes: 0 pass, 1 verdict failed, 2 bad usage or config.
`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, usage, runner.Version)
		os.Exit(2)
	}
	var code int
	switch os.Args[1] {
	case "validate":
		code = cmdValidate(os.Args[2:])
	case "run":
		code = cmdRun(os.Args[2:])
	case "agent":
		code = cmdAgent(os.Args[2:])
	case "merge":
		code = cmdMerge(os.Args[2:])
	case "compare":
		code = cmdCompare(os.Args[2:])
	case "version", "--version":
		fmt.Println("burstforge", runner.Version)
	case "help", "-h", "--help":
		fmt.Printf(usage, runner.Version)
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n", os.Args[1])
		fmt.Fprintf(os.Stderr, usage, runner.Version)
		code = 2
	}
	os.Exit(code)
}

// parse lets flags appear before or after the positional arguments.
func parse(fs *flag.FlagSet, args []string) ([]string, error) {
	var pos []string
	for {
		if err := fs.Parse(args); err != nil {
			return nil, err
		}
		if fs.NArg() == 0 {
			return pos, nil
		}
		pos = append(pos, fs.Arg(0))
		args = fs.Args()[1:]
	}
}

// scenarioFlags are the overrides shared by run, agent and merge. Agents and
// the merge step must use the same ones, or the scenario hashes won't match.
type scenarioFlags struct {
	arrivals    int
	maxArrivals int
	target      string
	allowHost   string
}

func addScenarioFlags(fs *flag.FlagSet) *scenarioFlags {
	f := &scenarioFlags{}
	fs.IntVar(&f.arrivals, "arrivals", 0, "override traffic.arrivals")
	fs.IntVar(&f.maxArrivals, "max-arrivals", 10000, "safety cap on traffic.arrivals")
	fs.StringVar(&f.target, "target", "", "override target.base_url")
	fs.StringVar(&f.allowHost, "allow-host", "", "add a host to target.allow_hosts (for example a docker service name)")
	return f
}

func (f *scenarioFlags) load(path string) (*scenario.Scenario, error) {
	sc, err := scenario.Load(path)
	if err != nil {
		return nil, err
	}
	if f.arrivals > 0 {
		sc.Traffic.Arrivals = f.arrivals
	}
	if f.target != "" {
		sc.Target.BaseURL = f.target
	}
	if f.allowHost != "" {
		sc.Target.AllowHosts = append(sc.Target.AllowHosts, f.allowHost)
	}
	if err := sc.Validate(); err != nil {
		return nil, err
	}
	return sc, nil
}

// args reproduces the overrides for a child agent process.
func (f *scenarioFlags) args() []string {
	a := []string{"--max-arrivals", strconv.Itoa(f.maxArrivals)}
	if f.arrivals > 0 {
		a = append(a, "--arrivals", strconv.Itoa(f.arrivals))
	}
	if f.target != "" {
		a = append(a, "--target", f.target)
	}
	if f.allowHost != "" {
		a = append(a, "--allow-host", f.allowHost)
	}
	return a
}

func cmdValidate(args []string) int {
	fs := flag.NewFlagSet("validate", flag.ContinueOnError)
	pos, err := parse(fs, args)
	if err != nil || len(pos) != 1 {
		fmt.Fprintln(os.Stderr, "usage: burstforge validate <scenario.yaml>")
		return 2
	}
	sc, err := scenario.Load(pos[0])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	if err := sc.CheckTarget(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	fmt.Printf("ok: %s (%d steps, %d %s arrivals, %d invariants)\n",
		sc.Name, len(sc.Workflow.Steps), sc.Traffic.Arrivals, sc.Traffic.Profile, len(sc.Invariants))
	return 0
}

func logf(f string, a ...any) { fmt.Printf("  "+f+"\n", a...) }

func cmdRun(args []string) int {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	out := fs.String("out", "burstforge-results", "directory for report files")
	seed := fs.Int64("seed", 0, "random seed (default: time-based; printed so you can replay it)")
	agents := fs.Int("agents", 1, "split the run across this many local agent processes")
	sf := addScenarioFlags(fs)
	pos, err := parse(fs, args)
	if err != nil || len(pos) != 1 {
		fmt.Fprintln(os.Stderr, "usage: burstforge run <scenario.yaml> [flags]")
		return 2
	}

	sc, err := sf.load(pos[0])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	if *agents < 1 || *agents > sc.Traffic.Arrivals {
		fmt.Fprintf(os.Stderr, "--agents must be between 1 and traffic.arrivals (%d)\n", sc.Traffic.Arrivals)
		return 2
	}
	if *seed == 0 {
		*seed = time.Now().UnixNano() % 1_000_000
	}
	runID := time.Now().Format("20060102-150405") + "-" + shortID()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	fmt.Printf("run %s  scenario %q  seed %d", runID, sc.Name, *seed)
	if *agents > 1 {
		fmt.Printf("  agents %d", *agents)
	}
	fmt.Println()

	var rep *report.Report
	if *agents == 1 {
		rep, err = runner.Run(ctx, sc, runner.Options{
			Seed: *seed, RunID: runID, MaxArrivals: sf.maxArrivals, Logf: logf,
		})
	} else {
		rep, err = coordinate(ctx, pos[0], sc, sf, *agents, *seed, runID, *out)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		return 2
	}
	return finish(rep, filepath.Join(*out, runID))
}

// coordinate runs the hooks once, fans the traffic out to N local agent
// processes anchored to one start time, merges their shard reports, then
// checks invariants once against the target.
func coordinate(ctx context.Context, path string, sc *scenario.Scenario, sf *scenarioFlags,
	n int, seed int64, runID, out string) (*report.Report, error) {
	// Same safety checks a single agent does, before any hook touches the target.
	if err := sc.CheckTarget(); err != nil {
		return nil, err
	}
	if sf.maxArrivals > 0 && sc.Traffic.Arrivals > sf.maxArrivals {
		return nil, fmt.Errorf("traffic.arrivals %d exceeds the safety cap of %d (raise it with --max-arrivals)",
			sc.Traffic.Arrivals, sf.maxArrivals)
	}
	exe, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("locating the burstforge binary for agent processes: %w", err)
	}

	startedAt := time.Now()
	before, err := runner.Prepare(ctx, sc)
	if err != nil {
		return nil, err
	}

	// Leave enough lead time for the agent processes to start and arm their timers.
	lead := sc.Traffic.StartDelay
	if lead < time.Second {
		lead = time.Second
	}
	startAt := time.Now().Add(lead).UTC()
	logf("starting %d agents, shared start %s (in %s)", n, startAt.Format("15:04:05.000"), lead)

	dir := filepath.Join(out, runID)
	var mu sync.Mutex // serializes agent output lines
	var wg sync.WaitGroup
	failures := make([]error, n)
	for k := 1; k <= n; k++ {
		cmd := exec.CommandContext(ctx, exe, append([]string{
			"agent", path,
			"--shard", fmt.Sprintf("%d/%d", k, n),
			"--seed", strconv.FormatInt(seed, 10),
			"--run-id", runID,
			"--start-at", startAt.Format(time.RFC3339Nano),
			"--out", out,
			"--name", fmt.Sprintf("local-%d", k),
		}, sf.args()...)...)
		// On Ctrl-C, ask agents to stop and write a partial shard report
		// rather than killing them outright.
		cmd.Cancel = func() error { return cmd.Process.Signal(os.Interrupt) }
		cmd.WaitDelay = 30 * time.Second
		pipe, err := cmd.StdoutPipe()
		if err != nil {
			return nil, err
		}
		cmd.Stderr = cmd.Stdout
		if err := cmd.Start(); err != nil {
			return nil, fmt.Errorf("starting agent %d: %w", k, err)
		}
		wg.Add(1)
		go func(k int) {
			defer wg.Done()
			prefixLines(pipe, fmt.Sprintf("  [agent %d] ", k), &mu)
			if err := cmd.Wait(); err != nil {
				failures[k-1] = err
			}
		}(k)
	}
	wg.Wait()

	var shards []*report.Report
	var notes []string
	for k := 1; k <= n; k++ {
		if err := failures[k-1]; err != nil {
			var ee *exec.ExitError
			// Exit code 1 means the agent wrote a report for an aborted shard.
			if !errors.As(err, &ee) || ee.ExitCode() != 1 {
				notes = append(notes, fmt.Sprintf("agent %d failed: %v", k, err))
			}
		}
		r, err := report.Load(shardPath(dir, k, n))
		if err != nil {
			continue // reported as a missing shard by Merge
		}
		if r.Shard == nil || r.Shard.ScenarioHash != sc.Hash() {
			return nil, fmt.Errorf("agent %d ran a different scenario than the coordinator loaded (was %s edited during the run?)", k, path)
		}
		shards = append(shards, r)
	}
	if len(shards) == 0 {
		return nil, fmt.Errorf("no agent produced a shard report: %s", strings.Join(notes, "; "))
	}
	rep, err := report.Merge(shards, true)
	if err != nil {
		return nil, err
	}
	rep.StartedAt = startedAt
	rep.Notes = append(rep.Notes, notes...)
	runner.Check(sc, rep, before, logf)
	runner.Evaluate(sc, rep)
	return rep, nil
}

func prefixLines(r io.Reader, prefix string, mu *sync.Mutex) {
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		mu.Lock()
		fmt.Println(prefix + strings.TrimLeft(sc.Text(), " "))
		mu.Unlock()
	}
}

func shardPath(dir string, k, n int) string {
	return filepath.Join(dir, fmt.Sprintf("shard-%d-of-%d.json", k, n))
}

func cmdAgent(args []string) int {
	fs := flag.NewFlagSet("agent", flag.ContinueOnError)
	out := fs.String("out", "burstforge-results", "directory for report files")
	shardArg := fs.String("shard", "", "this agent's shard, as K/N (1-based)")
	seed := fs.Int64("seed", 0, "seed shared by every shard of the run (required)")
	runID := fs.String("run-id", "", "run ID shared by every shard of the run (required)")
	startArg := fs.String("start-at", "", "shared start time: RFC 3339 or unix milliseconds (required)")
	name := fs.String("name", "", "agent name recorded in the report (default host-pid)")
	sf := addScenarioFlags(fs)
	pos, err := parse(fs, args)
	if err != nil || len(pos) != 1 {
		fmt.Fprintln(os.Stderr, "usage: burstforge agent <scenario.yaml> --shard K/N --seed N --run-id ID --start-at TIME [flags]")
		return 2
	}
	set := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { set[f.Name] = true })
	for _, req := range []string{"shard", "seed", "run-id", "start-at"} {
		if !set[req] {
			fmt.Fprintf(os.Stderr, "agent: --%s is required; every shard of a run must use the same value\n", req)
			return 2
		}
	}
	k, n, err := parseShard(*shardArg)
	if err != nil {
		fmt.Fprintln(os.Stderr, "agent:", err)
		return 2
	}
	startAt, err := parseTime(*startArg)
	if err != nil {
		fmt.Fprintln(os.Stderr, "agent:", err)
		return 2
	}
	if *name == "" {
		host, _ := os.Hostname()
		*name = fmt.Sprintf("%s-%d", host, os.Getpid())
	}

	sc, err := sf.load(pos[0])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	rep, err := runner.RunShard(ctx, sc, runner.Options{
		Seed: *seed, RunID: *runID, MaxArrivals: sf.maxArrivals, Logf: logf,
		Shard: k, ShardCount: n, StartAt: startAt, Agent: *name,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		return 2
	}
	dir := filepath.Join(*out, *runID)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	path := shardPath(dir, k, n)
	if err := rep.Save(path); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	fmt.Printf("shard %d/%d: %d workflows, %d attempts, skew p99 %.1fms -> %s\n",
		k, n, rep.Totals.Logical, rep.Attempts.Total, rep.ArrivalSkew.P99Ms, path)
	if rep.Totals.Aborted {
		return 1
	}
	return 0
}

func cmdMerge(args []string) int {
	fs := flag.NewFlagSet("merge", flag.ContinueOnError)
	out := fs.String("out", "burstforge-results", "directory for the merged report")
	noCheck := fs.Bool("no-check", false, "don't call the target to check invariants (they are then marked unchecked)")
	allowPartial := fs.Bool("allow-partial", false, "merge even if some shards are missing (the verdict fails)")
	sf := addScenarioFlags(fs)
	pos, err := parse(fs, args)
	if err != nil || len(pos) < 2 {
		fmt.Fprintln(os.Stderr, "usage: burstforge merge <scenario.yaml> <shard.json>... [flags]")
		return 2
	}
	sc, err := sf.load(pos[0])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	var shards []*report.Report
	for _, p := range pos[1:] {
		r, err := report.Load(p)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 2
		}
		if r.Shard != nil && r.Shard.ScenarioHash != sc.Hash() {
			fmt.Fprintf(os.Stderr, "%s was produced from a different scenario (hash %s, this one is %s); pass the same file and flags the agents used\n",
				p, r.Shard.ScenarioHash, sc.Hash())
			return 2
		}
		shards = append(shards, r)
	}
	rep, err := report.Merge(shards, *allowPartial)
	if err != nil {
		fmt.Fprintln(os.Stderr, "merge:", err)
		return 2
	}
	if *noCheck {
		for _, inv := range sc.Invariants {
			rep.Invariants = append(rep.Invariants, report.InvariantCheck{
				Name: inv.Name, Op: inv.Op, Expected: inv.Value, Error: "skipped with --no-check",
			})
		}
	} else {
		if err := sc.CheckTarget(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 2
		}
		runner.Check(sc, rep, nil, logf)
	}
	if sc.Observe.URL != "" {
		rep.Notes = append(rep.Notes, "target counters were not diffed: merge has no before snapshot (run --agents takes one)")
	}
	runner.Evaluate(sc, rep)
	return finish(rep, filepath.Join(*out, rep.RunID))
}

// finish writes report.json and report.md, prints the summary and returns
// the exit code for the verdict.
func finish(rep *report.Report, dir string) int {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	if err := rep.Save(filepath.Join(dir, "report.json")); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	if err := os.WriteFile(filepath.Join(dir, "report.md"), []byte(rep.Markdown()), 0o644); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	fmt.Print(rep.Text())
	fmt.Printf("reports written to %s\n", dir)
	if !rep.Verdict.Pass {
		return 1
	}
	return 0
}

func parseShard(s string) (k, n int, err error) {
	a, b, ok := strings.Cut(s, "/")
	if ok {
		k, err = strconv.Atoi(a)
		if err == nil {
			n, err = strconv.Atoi(b)
		}
	}
	if !ok || err != nil || n < 1 || k < 1 || k > n {
		return 0, 0, fmt.Errorf("--shard must look like 2/3 (1-based), got %q", s)
	}
	return k, n, nil
}

func parseTime(s string) (time.Time, error) {
	if ms, err := strconv.ParseInt(s, 10, 64); err == nil {
		return time.UnixMilli(ms), nil
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}, fmt.Errorf("--start-at must be RFC 3339 (2026-11-03T10:00:00Z) or unix milliseconds, got %q", s)
	}
	return t, nil
}

func cmdCompare(args []string) int {
	fs := flag.NewFlagSet("compare", flag.ContinueOnError)
	pos, err := parse(fs, args)
	if err != nil || len(pos) != 2 {
		fmt.Fprintln(os.Stderr, "usage: burstforge compare <before/report.json> <after/report.json>")
		return 2
	}
	a, err := report.Load(pos[0])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	b, err := report.Load(pos[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	fmt.Print(report.Compare(a, b))
	return 0
}

func shortID() string {
	b := make([]byte, 2)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
