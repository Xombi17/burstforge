// Command burstforge replays bursty workflows against a staging target and
// reports retry amplification, latency and business-invariant failures.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"burstforge/internal/report"
	"burstforge/internal/runner"
	"burstforge/internal/scenario"
)

const usage = `burstforge %s

Usage:
  burstforge validate <scenario.yaml>
  burstforge run <scenario.yaml> [--out DIR] [--seed N] [--arrivals N] [--max-arrivals N]
                                 [--target URL] [--allow-host HOST]
  burstforge compare <before/report.json> <after/report.json>

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

func cmdRun(args []string) int {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	out := fs.String("out", "burstforge-results", "directory for report files")
	seed := fs.Int64("seed", 0, "random seed (default: time-based; printed so you can replay it)")
	arrivals := fs.Int("arrivals", 0, "override traffic.arrivals")
	maxArrivals := fs.Int("max-arrivals", 10000, "safety cap on traffic.arrivals")
	target := fs.String("target", "", "override target.base_url")
	allowHost := fs.String("allow-host", "", "add a host to target.allow_hosts (for example a docker service name)")
	pos, err := parse(fs, args)
	if err != nil || len(pos) != 1 {
		fmt.Fprintln(os.Stderr, "usage: burstforge run <scenario.yaml> [flags]")
		return 2
	}

	sc, err := scenario.Load(pos[0])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	if *arrivals > 0 {
		sc.Traffic.Arrivals = *arrivals
	}
	if *target != "" {
		sc.Target.BaseURL = *target
	}
	if *allowHost != "" {
		sc.Target.AllowHosts = append(sc.Target.AllowHosts, *allowHost)
	}
	if err := sc.Validate(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	if *seed == 0 {
		*seed = time.Now().UnixNano() % 1_000_000
	}
	runID := time.Now().Format("20060102-150405") + "-" + shortID()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	fmt.Printf("run %s  scenario %q  seed %d\n", runID, sc.Name, *seed)
	rep, err := runner.Run(ctx, sc, runner.Options{
		Seed:        *seed,
		RunID:       runID,
		MaxArrivals: *maxArrivals,
		Logf:        func(f string, a ...any) { fmt.Printf("  "+f+"\n", a...) },
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		return 2
	}

	dir := filepath.Join(*out, runID)
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
