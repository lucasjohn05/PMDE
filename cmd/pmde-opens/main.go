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
//
//	pmde-opens series -data-dir data -frequency hourly,daily > candidates.txt
//	pmde-opens triage -data-dir data -series-file candidates.txt -min-quoted-share 0.5 -out opens.yaml
//
// are the cheap first pass. series lists candidate series from the
// latest series snapshot (fetching one if there is none); triage reads
// their opening minutes from Kalshi's candlesticks and prints one line
// per series, and can write the ones that pass as a scanner config.
// Neither needs a key or captures.
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
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
	case "triage":
		triage(os.Args[2:])
	case "series":
		series(os.Args[2:])
	default:
		usage()
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: pmde-opens backfill [flags]\n       pmde-opens report [flags]\n       pmde-opens triage [flags]\n       pmde-opens series [flags]\n(-h after any of them for its flags)")
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
		Rule:    cfg.Quote,
		Logf:    log.Printf,
	}
	log.Printf("quote rule: %v", cfg.Quote)
	stats, err := b.Run(ctx)
	log.Printf("%d row(s) in %s: %d fetched this run (%d from captures, %d from candlesticks, %d with no two-sided quote), %d failed",
		stats.Rows, filepath.Join(b.Dir, "backfill.jsonl"), stats.Built, stats.Captured, stats.Candles, stats.NoQuote, stats.Failed)
	if err != nil {
		log.Fatal(err)
	}
}

func report(args []string) {
	fs := flag.NewFlagSet("report", flag.ExitOnError)
	configPath := fs.String("config", "opens.yaml", "open scanner config (YAML), for its quote rule; \"\" for the defaults")
	dataDir := fs.String("data-dir", "data", "data directory; rows are read from its opens/backfill.jsonl")
	csvPath := fs.String("csv", "", "where to write the table as CSV (default: <data-dir>/opens/report.csv; \"-\" for none)")
	minOpens := fs.Int("min-opens", 1, "leave out series with fewer opens than this")
	deciles := fs.Bool("deciles", false, "also print each series' calibration table by opening-mid decile")
	seed := fs.Uint64("seed", 1, "seed for the bootstrap and the null simulation; the same rows and seed give the same report")
	resamples := fs.Int("resamples", opens.DefaultResamples, "bootstrap resamples and null simulations per series")
	feesPath := fs.String("fees", "fees.yaml", "fee rates (YAML), for the net edge; \"\" reports without net figures")
	fs.Parse(args)

	cfg := loadConfig(*configPath)
	dir := filepath.Join(*dataDir, "opens")
	rows, err := opens.LoadRows(dir)
	if err != nil {
		log.Fatal(err)
	}
	if len(rows) == 0 {
		log.Fatalf("no rows in %s; run pmde-opens backfill first", filepath.Join(dir, "backfill.jsonl"))
	}
	fees, err := opens.FeesOrNil(*feesPath, dir)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("quote rule: %v", cfg.Quote)
	if n := opens.BuiltUnderOtherRule(rows, cfg.Quote); n > 0 {
		log.Printf("%d of %d rows were built under a different quote rule; their quotes are held to this one, but rerun the backfill to rebuild them", n, len(rows))
	}
	reps := opens.Report(rows, opens.Options{MinOpens: *minOpens, Seed: *seed, Resamples: *resamples, Fees: fees, Rule: cfg.Quote})
	noteFees(fees, reps)
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

// loadConfig reads the scanner config at path, or returns the defaults
// if path is "".
func loadConfig(path string) opens.Config {
	if path == "" {
		return opens.DefaultConfig()
	}
	cfg, err := opens.LoadConfig(path)
	if err != nil {
		log.Fatal(err)
	}
	return cfg
}

// noteFees says where the fee inputs came from, and which series have
// no fee and so no net figures.
func noteFees(fees *opens.Fees, reps []opens.SeriesReport) {
	if fees == nil {
		return
	}
	snapshot := "no series snapshot"
	if fees.SnapshotDay != "" {
		snapshot = "the " + fees.SnapshotDay + " series snapshot"
	}
	log.Printf("fees: taker rate %d ppm (fees.yaml, checked %s) x each series' fee_multiplier from %s", fees.TakerRatePPM, fees.Checked, snapshot)
	var missing []string
	for _, r := range reps {
		if r.Quoted > 0 && !r.HasFee {
			missing = append(missing, r.Series)
		}
	}
	if len(missing) > 0 {
		log.Printf("no fee for %d series (not in the snapshot with a fee_type, or a fee type with no formula here), so no net edge: %v", len(missing), missing)
	}
}

func triage(args []string) {
	fs := flag.NewFlagSet("triage", flag.ExitOnError)
	configPath := fs.String("config", "", "open scanner config (YAML), for its quote rule; \"\" for the defaults")
	seriesFlag := fs.String("series", "", "series tickers to triage, comma-separated")
	seriesFile := fs.String("series-file", "", "file of series tickers, one per line (what `pmde-opens series` prints); \"-\" for stdin")
	dataDir := fs.String("data-dir", "data", "data directory; fetched rows are kept in its opens/triage.jsonl, so a rerun fetches only what is new")
	sinceFlag := fs.String("since", "", "take markets settled since this UTC date, YYYY-MM-DD (default: Kalshi's historical cutoff, as far back as these endpoints reach)")
	horizon := fs.Duration("horizon", 10*time.Minute, "how long after a market's open to look for its opening quote")
	sample := fs.Int("sample-events", 20, "events per series whose candles are read, evenly spaced over the period; -1 for every event")
	minShare := fs.Float64("min-quoted-share", -1, "if 0 or more: write the series with at least this share (0-1) of opens quoted two-sided in the first minute to -out, as a scanner config")
	outPath := fs.String("out", "opens-triage.yaml", "where -min-quoted-share writes the config")
	kalshiAPI := fs.String("kalshi-api", kalshi.RESTBase, "Kalshi REST base")
	fs.Parse(args)

	var list []string
	for _, s := range strings.Split(*seriesFlag, ",") {
		if s = strings.TrimSpace(s); s != "" {
			list = append(list, s)
		}
	}
	if *seriesFile != "" {
		f := os.Stdin
		if *seriesFile != "-" {
			var err error
			if f, err = os.Open(*seriesFile); err != nil {
				log.Fatal(err)
			}
			defer f.Close()
		}
		fromFile, err := opens.ReadSeriesList(f)
		if err != nil {
			log.Fatalf("%s: %v", *seriesFile, err)
		}
		list = append(list, fromFile...)
	}
	list, err := opens.ReadSeriesList(strings.NewReader(strings.Join(list, "\n"))) // validates and dedupes
	if err != nil {
		log.Fatal(err)
	}
	if len(list) == 0 {
		log.Fatal("no series: give -series A,B or -series-file FILE")
	}
	var since time.Time
	if *sinceFlag != "" {
		if since, err = time.Parse("2006-01-02", *sinceFlag); err != nil {
			log.Fatalf("-since: %v", err)
		}
	}
	if *horizon < time.Minute || *horizon > 2*time.Hour {
		log.Fatal("-horizon must be between 1m and 2h")
	}
	if *minShare > 1 {
		log.Fatal("-min-quoted-share is a share between 0 and 1")
	}
	cfg := loadConfig(*configPath)
	log.Printf("quote rule: %v (candles have no sizes: the spread cap only)", cfg.Quote)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	t := &opens.Triage{
		REST:         &kalshi.REST{Base: *kalshiAPI, Logf: log.Printf},
		Dir:          filepath.Join(*dataDir, "opens"),
		Series:       list,
		Since:        since,
		Horizon:      *horizon,
		SampleEvents: *sample,
		Rule:         cfg.Quote,
		Logf:         log.Printf,
	}
	rows, runErr := t.Run(ctx)
	opens.SortTriage(rows)
	out := bufio.NewWriter(os.Stdout)
	if err := opens.WriteTriage(out, rows); err != nil {
		log.Fatal(err)
	}
	out.Flush()
	if runErr != nil {
		// What was fetched is on file; the table above covers the series
		// reached. No allowlist from a partial run.
		log.Fatalf("stopped after %d of %d series: %v", len(rows), len(list), runErr)
	}
	if *minShare < 0 {
		return
	}
	passing := opens.Passing(rows, *minShare)
	note := fmt.Sprintf("%d of %d series triaged on %s had at least %.0f%% of their sampled opens\nquoted two-sided within the first minute.",
		len(passing), len(list), time.Now().UTC().Format("2006-01-02"), *minShare*100)
	f, err := os.Create(*outPath)
	if err != nil {
		log.Fatal(err)
	}
	if err := opens.WriteAllowlist(f, passing, note, cfg.Quote); err != nil {
		f.Close()
		log.Fatal(err)
	}
	if err := f.Close(); err != nil {
		log.Fatal(err)
	}
	log.Printf("wrote %s: %d of %d series", *outPath, len(passing), len(list))
}

func series(args []string) {
	fs := flag.NewFlagSet("series", flag.ExitOnError)
	dataDir := fs.String("data-dir", "data", "data directory; the series snapshots are under its opens/series")
	freq := fs.String("frequency", "", "only series with one of these frequencies, comma-separated (Kalshi's values: fifteen_min, hourly, daily, weekly, monthly, quarterly, annual, one_off, custom); empty for all")
	fetch := fs.Bool("fetch", false, "fetch a fresh snapshot of Kalshi's series list first (done anyway if there is none)")
	tickersOnly := fs.Bool("tickers-only", false, "print the ticker alone, without the frequency and title")
	kalshiAPI := fs.String("kalshi-api", kalshi.RESTBase, "Kalshi REST base")
	fs.Parse(args)

	var freqs []string
	for _, f := range strings.Split(*freq, ",") {
		if f = strings.TrimSpace(f); f != "" {
			freqs = append(freqs, f)
		}
	}
	dir := filepath.Join(*dataDir, "opens")
	list, day, err := opens.Candidates(dir, freqs)
	if err != nil {
		log.Fatal(err)
	}
	if *fetch || day == "" {
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		// The scanner's own series poll: it writes the day's snapshot
		// and, if there was an earlier one, logs the series that are
		// new, exactly as pmde-scan would.
		d := &opens.Discovery{REST: &kalshi.REST{Base: *kalshiAPI, Logf: log.Printf}, Dir: dir, Logf: log.Printf}
		if err := d.Load(); err != nil {
			log.Fatal(err)
		}
		if err := d.PollSeries(ctx); err != nil {
			log.Fatal(err)
		}
		if list, day, err = opens.Candidates(dir, freqs); err != nil {
			log.Fatal(err)
		}
	}
	out := bufio.NewWriter(os.Stdout)
	defer out.Flush()
	// Comments and the trailing fields are ignored by triage's
	// -series-file, so this output can be fed straight to it.
	fmt.Fprintf(out, "# %d series from the %s snapshot", len(list), day)
	if len(freqs) > 0 {
		fmt.Fprintf(out, ", frequency %s", strings.Join(freqs, " or "))
	}
	fmt.Fprintln(out)
	for _, s := range list {
		if *tickersOnly {
			fmt.Fprintln(out, s.Ticker)
		} else {
			fmt.Fprintf(out, "%s\t%s\t%s\n", s.Ticker, s.Frequency, s.Title)
		}
	}
}
