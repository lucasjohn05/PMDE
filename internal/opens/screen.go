package opens

import (
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
)

// ScreenRule is the screening band: series worth a closer look, as
// opposed to the bad-maker alert's evidence that a maker is wrong. A
// series is a candidate when its window notional per day is within
// [MinWindowNotional, MaxWindowNotional] dollars (enough flow to matter,
// not so much that it is crowded), its largest quoter holds at least
// MinTopQuoterShare of the quoted size (one maker to compete with), and
// its median opening spread is at least MinSpreadBps (room inside it).
type ScreenRule struct {
	MinWindowNotional float64 `json:"min_window_notional"`
	MaxWindowNotional float64 `json:"max_window_notional"`
	MinTopQuoterShare float64 `json:"min_top_quoter_share"`
	MinSpreadBps      float64 `json:"min_spread_bps"`
}

// DefaultScreenRule is $1,000 to $50,000 a day, 70%, 300 bps.
func DefaultScreenRule() ScreenRule {
	return ScreenRule{MinWindowNotional: 1000, MaxWindowNotional: 50000, MinTopQuoterShare: 0.7, MinSpreadBps: 300}
}

func (r ScreenRule) String() string {
	return fmt.Sprintf("window notional $%.0f-$%.0f/day, top quoter >= %.0f%%, median spread >= %.0f bps",
		r.MinWindowNotional, r.MaxWindowNotional, r.MinTopQuoterShare*100, r.MinSpreadBps)
}

// ScreenInputs are what the screen needs; a Has* false is unknown.
type ScreenInputs struct {
	WindowNotional float64 // dollars a day
	HasNotional    bool
	TopQuoterShare float64
	HasQuoters     bool
	SpreadBps      float64
	HasSpread      bool
}

// Screen is one series' result. Candidate needs all three conditions
// known and met; Unknown lists what could not be judged.
type Screen struct {
	Candidate                      bool
	NotionalOK, QuoterOK, SpreadOK bool
	Unknown                        []string
}

// Evaluate applies the rule.
func (r ScreenRule) Evaluate(in ScreenInputs) Screen {
	var s Screen
	if in.HasNotional {
		s.NotionalOK = in.WindowNotional >= r.MinWindowNotional && in.WindowNotional <= r.MaxWindowNotional
	} else {
		s.Unknown = append(s.Unknown, "notional")
	}
	if in.HasQuoters {
		s.QuoterOK = in.TopQuoterShare >= r.MinTopQuoterShare
	} else {
		s.Unknown = append(s.Unknown, "quoters")
	}
	if in.HasSpread {
		s.SpreadOK = in.SpreadBps >= r.MinSpreadBps
	} else {
		s.Unknown = append(s.Unknown, "spread")
	}
	s.Candidate = len(s.Unknown) == 0 && s.NotionalOK && s.QuoterOK && s.SpreadOK
	return s
}

// String is the table cell: "CANDIDATE", what failed ("no: $ q"), or
// what was unknown ("?: quoters").
func (s Screen) String() string {
	if s.Candidate {
		return "CANDIDATE"
	}
	var failed []string
	known := func(name string) bool {
		for _, u := range s.Unknown {
			if u == name {
				return false
			}
		}
		return true
	}
	if known("notional") && !s.NotionalOK {
		failed = append(failed, "$")
	}
	if known("quoters") && !s.QuoterOK {
		failed = append(failed, "q")
	}
	if known("spread") && !s.SpreadOK {
		failed = append(failed, "sp")
	}
	out := ""
	if len(failed) > 0 {
		out = "no: " + strings.Join(failed, " ")
	}
	if len(s.Unknown) > 0 {
		if out != "" {
			out += "; "
		}
		out += "?: " + strings.Join(s.Unknown, " ")
	}
	return out
}

// screenReport screens a report line.
func screenReport(r SeriesReport, rule ScreenRule) Screen {
	return rule.Evaluate(ScreenInputs{
		WindowNotional: r.WindowNotionalPerDay, HasNotional: r.WindowDays > 0,
		TopQuoterShare: r.Quoters.TopShare(), HasQuoters: r.HasQuoters,
		SpreadBps: r.SpreadBps, HasSpread: r.Quoted > 0,
	})
}

// WriteCandidates prints the screen's candidates, and how many series
// could not be judged, under the report's table.
func WriteCandidates(w io.Writer, reps []SeriesReport, rule ScreenRule) error {
	var cands []SeriesReport
	unknown := 0
	for _, r := range reps {
		if r.Screen.Candidate {
			cands = append(cands, r)
		}
		if len(r.Screen.Unknown) > 0 {
			unknown++
		}
	}
	fmt.Fprintf(w, "\nScreen (%v): %d candidate(s); %d series not fully judged (no notional yet, no captured quoters, or no quotes).\n", rule, len(cands), unknown)
	if len(cands) == 0 {
		return nil
	}
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', tabwriter.AlignRight)
	fmt.Fprintln(tw, "series\twindow $/day\tlife $/day\tquoters\ttop quoter\tspread\t")
	for _, r := range cands {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%d\t%s\t%.0f\t\n", r.Series, dollars(r.WindowNotionalPerDay),
			na(r.LifeDays > 0, "%s", dollars(r.LifeNotionalPerDay)), r.Quoters.Count(), topQuoter(r), r.SpreadBps)
	}
	return tw.Flush()
}
