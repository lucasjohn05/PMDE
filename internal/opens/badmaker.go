package opens

import (
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"text/tabwriter"
	"time"

	"github.com/lucasjohn05/pmde/internal/kalshi"
	"github.com/lucasjohn05/pmde/internal/metrics"
	"github.com/lucasjohn05/pmde/internal/storage"
)

// Files of the bad-maker evaluation, under the opens directory.
const (
	badMakersFile = "bad_makers.json"       // the latest evaluation, rewritten each run
	badFlagsFile  = "bad_maker_flags.jsonl" // one line per notification
)

// BadMakerRule is when a series is flagged as a bad-maker candidate: one
// maker, quoting by script, at size, often enough to measure, and wrong.
// Every threshold is configurable (bad_maker in opens.yaml).
type BadMakerRule struct {
	WindowDays        int     // trailing days evaluated
	MinQuotedShare    float64 // of captured opens, the share with a qualifying quote
	MinMedianCapacity float64 // median daily capacity, contracts
	MinModalShare     float64 // share of opening quotes that are the one most common size/size/spread
	MinEvents         int     // settled events
	WingInterval      float64 // central probability of the wing test's interval
	NotifyEveryDays   int     // at most one notification per series this often
}

// DefaultBadMakerRule is the rule as first specified.
func DefaultBadMakerRule() BadMakerRule {
	return BadMakerRule{
		WindowDays: 30, MinQuotedShare: 0.7, MinMedianCapacity: 200, MinModalShare: 0.7,
		MinEvents: 30, WingInterval: 0.9, NotifyEveryDays: 7,
	}
}

// Thresholds is the rule in one line, for the alert's labels and the
// summaries.
func (r BadMakerRule) Thresholds() string {
	return fmt.Sprintf("%dd: quoted >= %.0f%%, capacity >= %.0f, one quote >= %.0f%%, events >= %d, edge lower bound > 0 or wings outside %.0f%%",
		r.WindowDays, r.MinQuotedShare*100, r.MinMedianCapacity, r.MinModalShare*100, r.MinEvents, r.WingInterval*100)
}

// BadMakerEval is one series' evaluation: the numbers behind each
// condition, whether each holds, and whether the series is a candidate.
type BadMakerEval struct {
	Series string `json:"series"`
	Title  string `json:"title,omitempty"`

	Opens        int `json:"opens"`         // in the window
	CaptureOpens int `json:"capture_opens"` // of those, captured

	// 1. A qualifying quote on at least MinQuotedShare of opens. Only
	// captured opens have sizes, so the share is of those.
	QuotedCaptures int     `json:"quoted_captures"`
	QuotedShare    float64 `json:"quoted_share"`
	QuotedOK       bool    `json:"quoted_ok"`

	// 2. Median daily capacity, contracts.
	CapDays        int     `json:"cap_days"`
	MedianCapacity float64 `json:"median_capacity"`
	CapacityOK     bool    `json:"capacity_ok"`

	// 3. One size and spread combination on at least MinModalShare of
	// the captured opening quotes.
	ComboBidSz     int64   `json:"combo_bid_sz"`
	ComboAskSz     int64   `json:"combo_ask_sz"`
	ComboSpreadBps int     `json:"combo_spread_bps"`
	ComboShare     float64 `json:"combo_share"`
	OneMakerOK     bool    `json:"one_maker_ok"`

	// 4. Settled events.
	SettledEvents int  `json:"settled_events"`
	EventsOK      bool `json:"events_ok"`

	// 5. The quotes are wrong: a net edge above zero whose lower bound
	// (the bootstrap interval's low end, less the null, half the
	// spread and the fee) is above zero too; or wing rungs settling
	// against their opening price more often than the top of the wing
	// interval.
	HasNet       bool    `json:"has_net"`
	NetEdgeBps   float64 `json:"net_edge_bps"`
	NetEdgeLoBps float64 `json:"net_edge_lo_bps"`
	EdgeOK       bool    `json:"edge_ok"`
	WingEvents   int     `json:"wing_events"`
	WingAgainst  int     `json:"wing_against"`
	WingExpected float64 `json:"wing_expected"`
	WingHi       float64 `json:"wing_hi"`
	WingOK       bool    `json:"wing_ok"`

	Candidate bool `json:"candidate"`

	// NotifiedMs is the last time the series was put up for an email,
	// and AlertUntilMs how long the alert fires for it: until a day
	// before the next notification may go out, so that the alert has
	// resolved by then and a renewed one is a new email.
	NotifiedMs   int64 `json:"notified_ms,omitempty"`
	AlertUntilMs int64 `json:"alert_until_ms,omitempty"`

	// Test marks the made-up candidate of a delivery test.
	Test bool `json:"test,omitempty"`
}

// BadMakers is bad_makers.json: one evaluation of every series.
type BadMakers struct {
	EvaluatedMs int64          `json:"evaluated_ms"`
	Rule        BadMakerRule   `json:"rule"`
	Quote       QuoteRule      `json:"quote"`
	Series      []BadMakerEval `json:"series"`
}

// Candidates returns the candidates, in the file's order.
func (b BadMakers) Candidates() []BadMakerEval {
	var out []BadMakerEval
	for _, e := range b.Series {
		if e.Candidate {
			out = append(out, e)
		}
	}
	return out
}

// EvaluateBadMakers evaluates every series with rows opened in the
// rule's window up to now. prev is the last evaluation, for when each
// series was last notified; titles are the series' titles.
func EvaluateBadMakers(rows []OpenRow, now time.Time, rule BadMakerRule, opts Options, titles map[string]string, prev BadMakers) BadMakers {
	from := now.Add(-time.Duration(rule.WindowDays) * 24 * time.Hour).UnixMilli()
	var window []OpenRow
	for _, r := range rows {
		if r.OpenMs >= from && r.OpenMs <= now.UnixMilli() {
			window = append(window, r)
		}
	}
	opts.MinOpens, opts.WingInterval = 1, rule.WingInterval
	notified := map[string]int64{}
	for _, e := range prev.Series {
		if !e.Test {
			notified[e.Series] = e.NotifiedMs
		}
	}
	every := time.Duration(rule.NotifyEveryDays) * 24 * time.Hour
	out := BadMakers{EvaluatedMs: now.UnixMilli(), Rule: rule, Quote: opts.Rule}
	for _, r := range Report(window, opts) {
		e := BadMakerEval{
			Series: r.Series, Title: titles[r.Series], Opens: r.Opens, CaptureOpens: r.CaptureOpens,
			QuotedCaptures: r.Captured, CapDays: r.CapDays, MedianCapacity: r.Contracts,
			ComboBidSz: r.ComboBidSz, ComboAskSz: r.ComboAskSz, ComboSpreadBps: r.ComboSpreadBps, ComboShare: r.ComboShare,
			SettledEvents: r.CalEvents, HasNet: r.HasNet, NetEdgeBps: r.NetEdgeBps,
			WingEvents: r.Wing.Events, WingAgainst: r.Wing.Against, WingExpected: r.Wing.Expected, WingHi: r.Wing.Hi,
			NotifiedMs: notified[r.Series],
		}
		if r.CaptureOpens > 0 {
			e.QuotedShare = float64(r.Captured) / float64(r.CaptureOpens)
		}
		if r.HasNet {
			e.NetEdgeLoBps = r.LoBps - r.NullBps - r.SpreadBps/2 - r.FeeBps
		}
		e.QuotedOK = r.CaptureOpens > 0 && e.QuotedShare >= rule.MinQuotedShare
		e.CapacityOK = r.CapDays > 0 && e.MedianCapacity >= rule.MinMedianCapacity
		e.OneMakerOK = r.Captured > 0 && e.ComboShare >= rule.MinModalShare
		e.EventsOK = e.SettledEvents >= rule.MinEvents
		e.EdgeOK = e.HasNet && e.NetEdgeBps > 0 && e.NetEdgeLoBps > 0
		e.WingOK = e.WingEvents > 0 && float64(e.WingAgainst) > e.WingHi
		e.Candidate = e.QuotedOK && e.CapacityOK && e.OneMakerOK && e.EventsOK && (e.EdgeOK || e.WingOK)
		if e.Candidate {
			if e.NotifiedMs == 0 || now.Sub(time.UnixMilli(e.NotifiedMs)) >= every {
				e.NotifiedMs = now.UnixMilli()
			}
			e.AlertUntilMs = time.UnixMilli(e.NotifiedMs).Add(every - 24*time.Hour).UnixMilli()
		}
		out.Series = append(out.Series, e)
	}
	slices.SortFunc(out.Series, func(a, b BadMakerEval) int {
		if a.Candidate != b.Candidate {
			if a.Candidate {
				return -1
			}
			return 1
		}
		return cmp.Compare(a.Series, b.Series)
	})
	return out
}

// TestCandidate is a made-up candidate, for checking that the alert's
// email arrives: it fires the real rule with plausible numbers.
func TestCandidate(now time.Time, rule BadMakerRule) BadMakerEval {
	return BadMakerEval{
		Series: "TEST-BAD-MAKER", Title: "Test of the bad-maker alert, not a real series", Test: true,
		Opens: 300, CaptureOpens: 300, QuotedCaptures: 270, QuotedShare: 0.9, QuotedOK: true,
		CapDays: 30, MedianCapacity: 1200, CapacityOK: true,
		ComboBidSz: 10000, ComboAskSz: 10000, ComboSpreadBps: 400, ComboShare: 0.85, OneMakerOK: true,
		SettledEvents: 45, EventsOK: true,
		HasNet: true, NetEdgeBps: 120, NetEdgeLoBps: 35, EdgeOK: true,
		WingEvents: 40, WingAgainst: 9, WingExpected: 3.1, WingHi: 6, WingOK: true,
		Candidate: true, NotifiedMs: now.UnixMilli(),
		AlertUntilMs: now.Add(time.Duration(rule.NotifyEveryDays)*24*time.Hour - 24*time.Hour).UnixMilli(),
	}
}

// Metrics returns the candidates as metrics.SetBadMakers takes them.
func (b BadMakers) Metrics() []metrics.BadMaker {
	var out []metrics.BadMaker
	for _, e := range b.Candidates() {
		out = append(out, metrics.BadMaker{
			Series: e.Series, Title: e.Title, Thresholds: b.Rule.Thresholds(), Until: time.UnixMilli(e.AlertUntilMs),
			Stats: map[string]float64{
				"opens":            float64(e.Opens),
				"capture_opens":    float64(e.CaptureOpens),
				"quoted_share":     e.QuotedShare,
				"median_capacity":  e.MedianCapacity,
				"combo_share":      e.ComboShare,
				"combo_bid_size":   float64(e.ComboBidSz) / 100,
				"combo_ask_size":   float64(e.ComboAskSz) / 100,
				"combo_spread_bps": float64(e.ComboSpreadBps),
				"settled_events":   float64(e.SettledEvents),
				"net_edge_bps":     e.NetEdgeBps,
				"net_edge_lo_bps":  e.NetEdgeLoBps,
				"wing_events":      float64(e.WingEvents),
				"wing_against":     float64(e.WingAgainst),
				"wing_expected":    e.WingExpected,
				"wing_hi":          e.WingHi,
			},
		})
	}
	return out
}

// LoadBadMakers reads bad_makers.json; a missing file is an empty
// evaluation.
func LoadBadMakers(dir string) (BadMakers, error) {
	data, err := os.ReadFile(filepath.Join(dir, badMakersFile))
	if errors.Is(err, os.ErrNotExist) {
		return BadMakers{}, nil
	}
	if err != nil {
		return BadMakers{}, fmt.Errorf("opens: %w", err)
	}
	var b BadMakers
	if err := json.Unmarshal(data, &b); err != nil {
		return BadMakers{}, fmt.Errorf("opens: %s: %w", badMakersFile, err)
	}
	return b, nil
}

// SaveBadMakers writes b as bad_makers.json, atomically, and appends a
// line to bad_maker_flags.jsonl for each candidate notified at
// b.EvaluatedMs.
func SaveBadMakers(dir string, b BadMakers) error {
	data, err := json.MarshalIndent(b, "", "  ")
	if err != nil {
		return err
	}
	path := filepath.Join(dir, badMakersFile)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, append(data, '\n'), 0o644); err != nil {
		return fmt.Errorf("opens: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("opens: %w", err)
	}
	for _, e := range b.Series {
		if e.Candidate && e.NotifiedMs == b.EvaluatedMs {
			if err := storage.AppendLine(filepath.Join(dir, badFlagsFile), e); err != nil {
				return err
			}
		}
	}
	return nil
}

// SeriesTitles maps each series in the latest series snapshot under dir
// to its title.
func SeriesTitles(dir string) (map[string]string, error) {
	path, _, err := LatestSnapshot(dir)
	if err != nil || path == "" {
		return map[string]string{}, err
	}
	snap, err := storage.ReadJSONL[kalshi.Series](path)
	if err != nil {
		return nil, fmt.Errorf("opens: %w", err)
	}
	out := make(map[string]string, len(snap))
	for _, s := range snap {
		out[s.Ticker] = s.Title
	}
	return out, nil
}

// WriteBadMakers prints one line per series: the numbers behind each
// condition, a * where the condition holds.
func WriteBadMakers(w io.Writer, b BadMakers) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', tabwriter.AlignRight)
	fmt.Fprintln(tw, "series\topens\tcaptured\tquoted\tcap/day\tone quote\tevents\tnet edge [lo]\twing against/exp (hi)\tcandidate\t")
	star := func(ok bool) string {
		if ok {
			return "*"
		}
		return ""
	}
	for _, e := range b.Series {
		net := "-"
		if e.HasNet {
			net = fmt.Sprintf("%.0f [%.0f]", e.NetEdgeBps, e.NetEdgeLoBps)
		}
		combo := "-"
		if e.QuotedCaptures > 0 {
			combo = fmt.Sprintf("%.0f%% %sx%s@%d", e.ComboShare*100, centi(e.ComboBidSz), centi(e.ComboAskSz), e.ComboSpreadBps)
		}
		cand := ""
		if e.Candidate {
			cand = "CANDIDATE"
		}
		fmt.Fprintf(tw, "%s\t%d\t%d\t%.0f%%%s\t%.0f%s\t%s%s\t%d%s\t%s%s\t%d/%.1f (%.0f)%s\t%s\t\n",
			e.Series, e.Opens, e.CaptureOpens,
			e.QuotedShare*100, star(e.QuotedOK), e.MedianCapacity, star(e.CapacityOK), combo, star(e.OneMakerOK),
			e.SettledEvents, star(e.EventsOK), net, star(e.EdgeOK),
			e.WingAgainst, e.WingExpected, e.WingHi, star(e.WingOK), cand)
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	_, err := fmt.Fprintf(w, "\n*: the condition holds. Rule: %s. Quote rule: %v.\n", b.Rule.Thresholds(), b.Quote)
	return err
}
