// Command burstforge replays bursty workflows against a staging target and
// reports retry amplification, latency and business-invariant failures.
package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"burstforge/internal/faultproxy"
	"burstforge/internal/report"
	"burstforge/internal/runner"
	"burstforge/internal/scenario"
	"burstforge/internal/store"
	"burstforge/internal/telemetry"
	"burstforge/internal/tenancy"
)

const usage = `burstforge %s

Usage:
  burstforge validate [--json] [--bind URL] <scenario.yaml | ->
  burstforge run <scenario.yaml> [--agents N] [--out DIR] [--seed N] [scenario flags]
  burstforge agent <scenario.yaml> --shard K/N --seed N --run-id ID --start-at TIME
                                   [--via URL] [--out DIR] [--name NAME] [scenario flags]
  burstforge merge <scenario.yaml> <shard.json>... [--out DIR] [--no-check]
                                   [--allow-partial] [scenario flags]
  burstforge compare <before/report.json> <after/report.json>
  burstforge proxy --upstream URL [--listen ADDR] [--allow-host HOSTS]

With Postgres (leases, heartbeats, takeover of a dead agent's shard):
  burstforge run <scenario.yaml> --db URL --shards N [--agents M] [--lease-ttl D]
                                 [--lead D] [--drain-timeout D] [scenario flags]
  burstforge agent --db URL [--run ID] [--name NAME] [--max-arrivals N]
  burstforge finalize --db URL <run-id> [--out DIR]
  burstforge status --db URL [run-id]

For the control plane (organizations, API keys, quotas; see docs/tenancy.md):
  burstforge migrate --db URL
  burstforge controller --db URL [--proxy-listen ADDR] [--proxy-advertise HOST]
  burstforge agent --db URL --require-org [--trust-proxy HOSTS]

Scenario flags: [--arrivals N] [--max-arrivals N] [--target URL]
                [--allow-host HOST,...] [--fault-proxy URL]

run --agents N splits the run across N local agent processes and merges their
reports. agent and merge are the same two halves, for running agents on
separate machines. With --db, run stores the run in Postgres and agents claim
shards from it; --db defaults to $BURSTFORGE_DB.

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
	case "finalize":
		code = cmdFinalize(os.Args[2:])
	case "status":
		code = cmdStatus(os.Args[2:])
	case "proxy":
		code = cmdProxy(os.Args[2:])
	case "controller":
		code = cmdController(os.Args[2:])
	case "migrate":
		code = cmdMigrate(os.Args[2:])
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
	faultProxy  string
}

func addScenarioFlags(fs *flag.FlagSet) *scenarioFlags {
	f := &scenarioFlags{}
	fs.IntVar(&f.arrivals, "arrivals", 0, "override traffic.arrivals")
	fs.IntVar(&f.maxArrivals, "max-arrivals", 10000, "safety cap on traffic.arrivals")
	fs.StringVar(&f.target, "target", "", "override target.base_url")
	fs.StringVar(&f.allowHost, "allow-host", "", "add hosts, comma-separated, to target.allow_hosts (for example docker service names)")
	fs.StringVar(&f.faultProxy, "fault-proxy", "", "use this running `burstforge proxy` (overrides faults.proxy)")
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
	sc.Target.AllowHosts = append(sc.Target.AllowHosts, splitList(f.allowHost)...)
	if f.faultProxy != "" {
		sc.Faults.Proxy = f.faultProxy
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
	if f.faultProxy != "" {
		a = append(a, "--fault-proxy", f.faultProxy)
	}
	return a
}

// startOTLP starts the span exporter if OTEL_EXPORTER_OTLP_ENDPOINT (or
// ..._TRACES_ENDPOINT) is set. The returned func flushes and reports.
func startOTLP(agent string) (*telemetry.OTLP, func()) {
	exp, err := telemetry.OTLPFromEnv(agent)
	if err != nil {
		logf("not exporting traces: %v", err)
	}
	if exp == nil {
		return nil, func() {}
	}
	return exp, func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		exp.Close(ctx)
		ex, dr := exp.Stats()
		logf("traces: %d spans exported to the OTLP collector, %d dropped", ex, dr)
	}
}

// observers combines the exporters that are on, or returns nil.
func observers(spans *telemetry.OTLP, prom *telemetry.Prometheus, run, org string, shard int) runner.Observer {
	var obs runner.Observers
	if spans != nil {
		obs = append(obs, spans.ForRun(run, org, shard))
	}
	if prom != nil {
		obs = append(obs, prom.ForRun(run, org, shard))
	}
	if len(obs) == 0 {
		return nil
	}
	return obs
}

// splitList splits a comma-separated flag value, dropping empty entries.
func splitList(s string) []string {
	var out []string
	for _, v := range strings.Split(s, ",") {
		if v = strings.TrimSpace(v); v != "" {
			out = append(out, v)
		}
	}
	return out
}

// writable fails early when reports can't be written to dir, so a run
// doesn't send its traffic and then lose the report.
func writable(dir string) error {
	fail := func(err error) error {
		return fmt.Errorf("can't write reports to %s: %w (pick another directory with --out)", dir, err)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fail(err)
	}
	f, err := os.CreateTemp(dir, ".burstforge-write-check-*")
	if err != nil {
		return fail(err)
	}
	_ = f.Close()
	return os.Remove(f.Name())
}

func cmdValidate(args []string) int {
	fs := flag.NewFlagSet("validate", flag.ContinueOnError)
	asJSON := fs.Bool("json", false, "print the result as JSON, including the traffic shape (for the control plane)")
	bind := fs.String("bind", "", "point the scenario at this environment URL first, as the controller does")
	seed := fs.Int64("seed", 1, "seed for the traffic shape (--json)")
	pos, err := parse(fs, args)
	if err != nil || len(pos) != 1 {
		fmt.Fprintln(os.Stderr, "usage: burstforge validate [--json] [--bind URL] [--seed N] <scenario.yaml | ->")
		return 2
	}
	sc, err := validateScenario(pos[0], *bind)
	if *asJSON {
		out := struct {
			OK            bool     `json:"ok"`
			Errors        []string `json:"errors,omitempty"`
			Name          string   `json:"name,omitempty"`
			Profile       string   `json:"profile,omitempty"`
			Arrivals      int      `json:"arrivals,omitempty"`
			PeakPerSec    int      `json:"peak_per_sec,omitempty"`
			PlannedSpanMs int64    `json:"planned_span_ms,omitempty"`
			Faults        bool     `json:"faults,omitempty"`
		}{OK: err == nil}
		if err == nil {
			var shape tenancy.Shape
			if shape, err = tenancy.Measure(sc, *seed); err == nil {
				out.Name, out.Profile, out.Faults = sc.Name, sc.Traffic.Profile, sc.Faults.Enabled()
				out.Arrivals, out.PeakPerSec, out.PlannedSpanMs = shape.Arrivals, shape.PeakPerSec, shape.PlannedSpan.Milliseconds()
			}
		}
		if err != nil {
			out.OK = false
			for _, line := range strings.Split(err.Error(), "\n") {
				if line = strings.TrimSpace(line); line != "" {
					out.Errors = append(out.Errors, line)
				}
			}
		}
		_ = json.NewEncoder(os.Stdout).Encode(out)
		if !out.OK {
			return 2
		}
		return 0
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	fmt.Printf("ok: %s (%d steps, %d %s arrivals, %d invariants)\n",
		sc.Name, len(sc.Workflow.Steps), sc.Traffic.Arrivals, sc.Traffic.Profile, len(sc.Invariants))
	return 0
}

// validateScenario loads a scenario from a file or stdin ("-"), optionally
// binds it to an environment URL, and runs every check the CLI runs.
func validateScenario(path, bind string) (*scenario.Scenario, error) {
	var raw []byte
	var err error
	name := strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
	if path == "-" {
		raw, err = io.ReadAll(io.LimitReader(os.Stdin, 1<<20))
		name = "scenario"
	} else {
		raw, err = os.ReadFile(path)
	}
	if err != nil {
		return nil, err
	}
	sc, err := scenario.Decode(raw, name)
	if err != nil {
		return nil, err
	}
	if bind != "" {
		if err := tenancy.Bind(sc, tenancy.Environment{BaseURL: bind}); err != nil {
			return nil, err
		}
	}
	if err := sc.Validate(); err != nil {
		return nil, err
	}
	return sc, sc.CheckTarget()
}

func cmdMigrate(args []string) int {
	fs := flag.NewFlagSet("migrate", flag.ContinueOnError)
	url := dbURLFlag(fs)
	if _, err := parse(fs, args); err != nil || *url == "" {
		fmt.Fprintln(os.Stderr, "usage: burstforge migrate --db URL")
		return 2
	}
	st, err := store.Open(context.Background(), *url)
	if err != nil {
		fmt.Fprintln(os.Stderr, "migrating:", err)
		return 2
	}
	st.Close()
	fmt.Println("schema is up to date")
	return 0
}

func logf(f string, a ...any) { fmt.Printf("  "+f+"\n", a...) }

func cmdRun(args []string) int {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	out := fs.String("out", "burstforge-results", "directory for report files")
	seed := fs.Int64("seed", 0, "random seed (default: time-based; printed so you can replay it)")
	agents := fs.Int("agents", 0, "split the run across this many local agent processes (with --db: local workers to start)")
	df := addDBFlags(fs)
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
	if df.url == "" && *agents == 0 {
		*agents = 1
	}
	if *agents < 0 || *agents > sc.Traffic.Arrivals {
		fmt.Fprintf(os.Stderr, "--agents must be between 1 and traffic.arrivals (%d)\n", sc.Traffic.Arrivals)
		return 2
	}
	if df.url != "" && (df.shards < 1 || df.shards > sc.Traffic.Arrivals) {
		fmt.Fprintf(os.Stderr, "--shards must be between 1 and traffic.arrivals (%d)\n", sc.Traffic.Arrivals)
		return 2
	}
	if err := writable(*out); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	if *seed == 0 {
		*seed = time.Now().UnixNano() % 1_000_000
	}
	runID := time.Now().Format("20060102-150405") + "-" + shortID()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	fmt.Printf("run %s  scenario %q  seed %d", runID, sc.Name, *seed)
	if df.url != "" {
		fmt.Printf("  shards %d  local agents %d  (leases in Postgres)", df.shards, *agents)
	} else if *agents > 1 {
		fmt.Printf("  agents %d", *agents)
	}
	fmt.Println()

	var rep *report.Report
	if df.url != "" {
		rep, err = coordinateDB(ctx, pos[0], sc, sf, df, *agents, *seed, runID)
	} else if *agents == 1 {
		spans, closeSpans := startOTLP("local")
		rep, err = runner.Run(ctx, sc, runner.Options{
			Seed: *seed, RunID: runID, MaxArrivals: sf.maxArrivals, Logf: logf,
			Observer: observers(spans, nil, runID, "", 0),
		})
		closeSpans()
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
	if err := preflight(sc, sf); err != nil {
		return nil, err
	}
	exe, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("locating the burstforge binary for agent processes: %w", err)
	}

	startedAt := time.Now()
	fs, err := runner.StartFaults(ctx, sc, "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	defer fs.Close()
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
	var via []string
	if fs != nil {
		if err := fs.Apply(ctx, seed, startAt); err != nil {
			return nil, err
		}
		via = []string{"--via", fs.URL}
		logf("workflow traffic goes through the fault proxy at %s", fs.URL)
	}

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
		}, append(via, sf.args()...)...)...)
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
	if fs != nil {
		rep.Faults = fs.Collect(logf)
	}
	runner.Check(sc, rep, before, logf)
	runner.Evaluate(sc, rep)
	return rep, nil
}

// preflight runs the safety checks a single agent does, before any hook
// touches the target.
func preflight(sc *scenario.Scenario, sf *scenarioFlags) error {
	if err := sc.CheckTarget(); err != nil {
		return err
	}
	if sf.maxArrivals > 0 && sc.Traffic.Arrivals > sf.maxArrivals {
		return fmt.Errorf("traffic.arrivals %d exceeds the safety cap of %d (raise it with --max-arrivals)",
			sc.Traffic.Arrivals, sf.maxArrivals)
	}
	return nil
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
	for _, a := range args {
		if a == "--db" || a == "-db" || strings.HasPrefix(a, "--db=") || strings.HasPrefix(a, "-db=") {
			return cmdAgentDB(args)
		}
	}
	fs := flag.NewFlagSet("agent", flag.ContinueOnError)
	out := fs.String("out", "burstforge-results", "directory for report files")
	shardArg := fs.String("shard", "", "this agent's shard, as K/N (1-based)")
	seed := fs.Int64("seed", 0, "seed shared by every shard of the run (required)")
	runID := fs.String("run-id", "", "run ID shared by every shard of the run (required)")
	startArg := fs.String("start-at", "", "shared start time: RFC 3339 or unix milliseconds (required)")
	name := fs.String("name", "", "agent name recorded in the report (default host-pid)")
	via := fs.String("via", "", "send workflow traffic here instead of target.base_url (a fault proxy)")
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

	if err := writable(*out); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	spans, closeSpans := startOTLP(*name)
	rep, err := runner.RunShard(ctx, sc, runner.Options{
		Seed: *seed, RunID: *runID, MaxArrivals: sf.maxArrivals, Logf: logf,
		Shard: k, ShardCount: n, StartAt: startAt, Agent: *name, TrafficURL: *via,
		Observer: observers(spans, nil, *runID, "", k),
	})
	closeSpans()
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
	err := os.MkdirAll(dir, 0o755)
	if err == nil {
		err = rep.Save(filepath.Join(dir, "report.json"))
	}
	if err == nil {
		err = os.WriteFile(filepath.Join(dir, "report.md"), []byte(rep.Markdown()), 0o644)
	}
	fmt.Print(rep.Text())
	if err != nil {
		// The traffic has been sent; at least show the summary.
		fmt.Fprintln(os.Stderr, "writing the report:", err)
		return 2
	}
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

func cmdProxy(args []string) int {
	fs := flag.NewFlagSet("proxy", flag.ContinueOnError)
	upstream := fs.String("upstream", "", "the staging target to forward to (required)")
	listen := fs.String("listen", "127.0.0.1:8081", "address to listen on")
	allowHost := fs.String("allow-host", "", "allow upstream hosts (comma-separated) that are not loopback or private")
	pos, err := parse(fs, args)
	if err != nil || len(pos) != 0 || *upstream == "" {
		fmt.Fprintln(os.Stderr, "usage: burstforge proxy --upstream URL [--listen ADDR] [--allow-host HOSTS]")
		return 2
	}
	allow := splitList(*allowHost)
	if err := scenario.CheckHost(*upstream, allow); err != nil {
		fmt.Fprintf(os.Stderr, "--upstream: %v (for the proxy, allow a host with --allow-host)\n", err)
		return 2
	}
	p, err := faultproxy.New(*upstream)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	e, err := faultproxy.Start(p, *listen)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	fmt.Printf("fault proxy on %s -> %s (no faults until a run configures them)\n", e.URL, *upstream)
	use := e.URL
	if host, port, err := net.SplitHostPort(strings.TrimPrefix(e.URL, "http://")); err == nil {
		if ip := net.ParseIP(host); ip != nil && ip.IsUnspecified() {
			use = "http://<this host>:" + port
		}
	}
	fmt.Printf("point agents at it with --fault-proxy %s (or faults.proxy in the scenario)\n", use)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()
	_ = e.Close()
	return 0
}
