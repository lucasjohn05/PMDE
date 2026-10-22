// Command pmde-analyze reports on the data cmd/pmde stored: per pair, how
// much of the time both books were trusted, the mid divergence, the
// executable gap before and after fees, and lead/lag between the venues.
//
//	pmde-analyze -data-dir data -pair fed-dec-2026-hold -from 2026-10-22
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/lucasjohn05/pmde/internal/analysis"
	"github.com/lucasjohn05/pmde/internal/divergence"
)

func main() {
	log.SetFlags(0)
	log.SetPrefix("pmde-analyze: ")
	def := analysis.DefaultOptions()
	dataDir := flag.String("data-dir", "data", "directory cmd/pmde stored ticks and runs.jsonl in")
	configPath := flag.String("config", "pairs.yaml", "venue pair config (YAML)")
	feesPath := flag.String("fees", "fees.yaml", "fee rates and their sources (YAML)")
	pairName := flag.String("pair", "", "pair to report on (default: every pair in the config)")
	fromFlag := flag.String("from", "", "start: YYYY-MM-DD (UTC midnight) or RFC 3339 (default: earliest data)")
	toFlag := flag.String("to", "", "end, exclusive: RFC 3339, or YYYY-MM-DD for the end of that UTC day (default: latest data)")
	asJSON := flag.Bool("json", false, "write JSON instead of text")
	episodes := flag.Int("episodes", 20, "crossing episodes to list per pair in text output (the longest)")
	moveBps := flag.Int("move-bps", def.MoveBps, "smallest mid move counted for lead/lag, in bps")
	window := flag.Duration("follow-window", def.FollowWindow, "how long after one venue's move the other's still counts as following")
	maxLag := flag.Duration("corr-max-lag", def.CorrMaxLag, "largest lag for the 1s cross-correlation, each direction")
	flag.Parse()

	from, err := parseTime(*fromFlag, false)
	if err != nil {
		log.Fatalf("-from: %v", err)
	}
	to, err := parseTime(*toFlag, true)
	if err != nil {
		log.Fatalf("-to: %v", err)
	}
	if !from.IsZero() && !to.IsZero() && !to.After(from) {
		log.Fatalf("-to must be after -from")
	}
	if *moveBps <= 0 || *window <= 0 || *maxLag < 0 {
		log.Fatalf("-move-bps and -follow-window must be positive")
	}
	pairs, err := divergence.LoadPairs(*configPath)
	if err != nil {
		log.Fatal(err)
	}
	fees, err := analysis.LoadFees(*feesPath)
	if err != nil {
		log.Fatal(err)
	}
	if *pairName != "" {
		var found []divergence.Pair
		for _, p := range pairs {
			if p.Name == *pairName {
				found = append(found, p)
			}
		}
		if len(found) == 0 {
			log.Fatalf("no pair %q in %s", *pairName, *configPath)
		}
		pairs = found
	}

	var reports []analysis.Report
	for _, p := range pairs {
		k, poly, runs, err := analysis.Load(*dataDir, p, from, to)
		if err != nil {
			log.Fatal(err)
		}
		f, t := from, to
		if f.IsZero() || t.IsZero() {
			df, dt, ok := analysis.DefaultRange(p.Name, k, poly, runs)
			if !ok {
				log.Printf("%s: no data in %s", p.Name, *dataDir)
				continue
			}
			if f.IsZero() {
				f = df
			}
			if t.IsZero() {
				t = dt
			}
		}
		reports = append(reports, analysis.Analyze(analysis.Input{
			Pair: p, Kalshi: k, Polymarket: poly, Runs: runs, From: f, To: t, Fees: fees,
			Opts: analysis.Options{MoveBps: *moveBps, FollowWindow: *window, CorrMaxLag: *maxLag},
		}))
	}

	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(reports); err != nil {
			log.Fatal(err)
		}
		return
	}
	for i, r := range reports {
		if i > 0 {
			fmt.Println()
		}
		if err := analysis.WriteText(os.Stdout, r, *episodes); err != nil {
			log.Fatal(err)
		}
	}
}

// parseTime accepts RFC 3339 or a UTC date. A date as an end means the
// end of that day, so -from 2026-10-22 -to 2026-10-22 is that whole day.
func parseTime(s string, end bool) (time.Time, error) {
	if s == "" {
		return time.Time{}, nil
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t, nil
	}
	d, err := time.Parse("2006-01-02", s)
	if err != nil {
		return time.Time{}, fmt.Errorf("%q is neither YYYY-MM-DD nor RFC 3339", s)
	}
	if end {
		d = d.AddDate(0, 0, 1)
	}
	return d, nil
}
