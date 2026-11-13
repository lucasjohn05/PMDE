// Command pmde-opens works on what cmd/pmde-scan recorded.
//
//	pmde-opens backfill -config opens.yaml -data-dir data -since 2026-10-15
//
// fetches, for every captured market and every market of the
// allowlisted series settled since -since, how it settled and its
// earliest prices, into <data-dir>/opens/backfill.jsonl: one row per
// market. It only reads Kalshi's public REST API, so it needs no key.
//
//	pmde-opens report -data-dir data -deciles
//
// reads those rows and prints, per series, a table ranked by score
// (miscalibration x modal opening size x opens per day), and writes the
// same table as CSV to <data-dir>/opens/report.csv. It reads no network.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/lucasjohn05/pmde/internal/kalshi"
	"github.com/lucasjohn05/pmde/internal/opens"
)

func main() {
	log.SetFlags(0)
	log.SetPrefix("pmde-opens: ")
	if len(os.Args) < 2 {
		usage()
	}
	switch os.Args[1] {
	case "backfill":
		backfill(os.Args[2:])
	case "report":
		report(os.Args[2:])
	default:
		usage()
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: pmde-opens backfill [flags]\n       pmde-opens report [flags]\n(-h after either for its flags)")
	os.Exit(2)
}

func backfill(args []string) {
	fs := flag.NewFlagSet("backfill", flag.ExitOnError)
	configPath := fs.String("config", "opens.yaml", "open scanner config (YAML); its series list is what gets backfilled")
	dataDir := fs.String("data-dir", "data", "data directory; rows go to its opens/backfill.jsonl")
	sinceFlag := fs.String("since", "", "take the allowlisted series' markets settled since this UTC date, YYYY-MM-DD (default: 30 days ago)")
	horizon := fs.Duration("horizon", time.Hour, "how long after a market's open to look for its first two-sided quote and first trade")
	kalshiAPI := fs.String("kalshi-api", kalshi.RESTBase, "Kalshi REST base")
	fs.Parse(args)

	cfg, err := opens.LoadConfig(*configPath)
	if err != nil {
		log.Fatal(err)
	}
	since := time.Now().Add(-30 * 24 * time.Hour)
	if *sinceFlag != "" {
		if since, err = time.Parse("2006-01-02", *sinceFlag); err != nil {
			log.Fatalf("-since: %v", err)
		}
	}
	if *horizon < time.Minute {
		log.Fatalf("-horizon must be at least a minute")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	b := &opens.Backfill{
		REST:    &kalshi.REST{Base: *kalshiAPI, Logf: log.Printf},
		Dir:     filepath.Join(*dataDir, "opens"),
		Series:  cfg.Series,
		Since:   since,
		Horizon: *horizon,
		Logf:    log.Printf,
	}
	stats, err := b.Run(ctx)
	log.Printf("%d row(s) in %s: %d fetched this run (%d from captures, %d from candlesticks, %d with no two-sided quote), %d failed",
		stats.Rows, filepath.Join(b.Dir, "backfill.jsonl"), stats.Built, stats.Captured, stats.Candles, stats.NoQuote, stats.Failed)
	if err != nil {
		log.Fatal(err)
	}
}

func report(args []string) {
	fs := flag.NewFlagSet("report", flag.ExitOnError)
	dataDir := fs.String("data-dir", "data", "data directory; rows are read from its opens/backfill.jsonl")
	csvPath := fs.String("csv", "", "where to write the table as CSV (default: <data-dir>/opens/report.csv; \"-\" for none)")
	minOpens := fs.Int("min-opens", 1, "leave out series with fewer opens than this")
	deciles := fs.Bool("deciles", false, "also print each series' calibration table by opening-mid decile")
	fs.Parse(args)

	dir := filepath.Join(*dataDir, "opens")
	rows, err := opens.LoadRows(dir)
	if err != nil {
		log.Fatal(err)
	}
	if len(rows) == 0 {
		log.Fatalf("no rows in %s; run pmde-opens backfill first", filepath.Join(dir, "backfill.jsonl"))
	}
	reps := opens.Report(rows, *minOpens)
	if err := opens.WriteText(os.Stdout, reps, *deciles); err != nil {
		log.Fatal(err)
	}
	if *csvPath == "-" {
		return
	}
	if *csvPath == "" {
		*csvPath = filepath.Join(dir, "report.csv")
	}
	f, err := os.Create(*csvPath)
	if err != nil {
		log.Fatal(err)
	}
	if err := opens.WriteCSV(f, reps); err != nil {
		f.Close()
		log.Fatal(err)
	}
	if err := f.Close(); err != nil {
		log.Fatal(err)
	}
	log.Printf("wrote %s", *csvPath)
}
