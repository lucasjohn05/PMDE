package analysis

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/lucasjohn05/pmde/internal/divergence"
	"github.com/lucasjohn05/pmde/internal/storage"
	"github.com/lucasjohn05/pmde/internal/tick"
)

const dayLayout = "2006-01-02"

// Load reads a pair's tick files and the run log from dataDir. With a
// zero from or to, all days on disk count; otherwise the days that
// overlap [from, to), plus the day before from, so the books' state at
// from is known even if neither changed in the first hours of the range.
func Load(dataDir string, pair divergence.Pair, from, to time.Time) (k, p []storage.TickRecord, runs []storage.RunRecord, err error) {
	if k, err = loadTicks(dataDir, tick.Kalshi, pair.Kalshi, from, to); err != nil {
		return nil, nil, nil, err
	}
	if p, err = loadTicks(dataDir, tick.Polymarket, pair.Polymarket, from, to); err != nil {
		return nil, nil, nil, err
	}
	runs, err = storage.ReadRuns(dataDir)
	return k, p, runs, err
}

func loadTicks(dataDir, venue, market string, from, to time.Time) ([]storage.TickRecord, error) {
	dir := filepath.Join(dataDir, filepath.FromSlash(storage.MarketDir(venue, market)))
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil, nil // nothing recorded for this market (yet)
	}
	if err != nil {
		return nil, err
	}
	var days []string
	for _, e := range entries {
		day, ok := strings.CutSuffix(e.Name(), ".ticks.jsonl")
		if !ok {
			continue
		}
		d, err := time.Parse(dayLayout, day)
		if err != nil {
			continue
		}
		if !from.IsZero() && d.Before(from.UTC().Truncate(24*time.Hour).AddDate(0, 0, -1)) {
			continue
		}
		if !to.IsZero() && !d.Before(to) {
			continue
		}
		days = append(days, day)
	}
	slices.Sort(days) // file names sort by date
	var out []storage.TickRecord
	for _, day := range days {
		recs, err := storage.ReadTicks(filepath.Join(dir, day+".ticks.jsonl"))
		if err != nil {
			return nil, fmt.Errorf("%s/%s: %w", venue, market, err)
		}
		out = append(out, recs...)
	}
	return out, nil
}

// DefaultRange is the range to report on when none is given: from the
// earliest to the latest thing known about the pair, a tick on either
// venue or a run (start to last heartbeat) that included the pair.
func DefaultRange(pair string, k, p []storage.TickRecord, runs []storage.RunRecord) (from, to time.Time, ok bool) {
	lo, hi := int64(1<<62), int64(-1<<62)
	see := func(t int64) { lo, hi = min(lo, t), max(hi, t) }
	for _, recs := range [][]storage.TickRecord{k, p} {
		for _, r := range recs {
			see(r.T)
		}
	}
	for _, s := range runSpans(runs, pair) {
		see(s.start)
		see(s.end)
	}
	if lo > hi {
		return time.Time{}, time.Time{}, false
	}
	return time.UnixMilli(lo).UTC(), time.UnixMilli(hi + 1).UTC(), true
}
