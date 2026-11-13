package opens

import (
	"cmp"
	"slices"
	"sync"
	"time"

	"github.com/lucasjohn05/pmde/internal/kalshi"
)

// Opening is one market waiting for its open_time.
type Opening struct {
	Market kalshi.Market
	Series string
}

// doneKeep is how long after its open_time a market that came due is
// remembered, so a poll that still lists it as unopened (Kalshi's
// responses are cached for a few seconds) doesn't schedule it again.
const doneKeep = time.Hour

// Schedule holds the unopened markets of the watched series, keyed by
// market ticker. Discovery fills it; the capture loop takes markets out
// as they come due. Safe for concurrent use.
type Schedule struct {
	mu      sync.Mutex
	pending map[string]Opening
	done    map[string]time.Time // ticker -> open_time, for markets already taken
}

func NewSchedule() *Schedule {
	return &Schedule{pending: map[string]Opening{}, done: map[string]time.Time{}}
}

// Update replaces what is scheduled for series with unopened, the
// series' current unopened markets. A market's open_time can move; the
// latest one wins. vanished is how many scheduled markets are no longer
// in the list: they opened earlier than scheduled or were removed, and
// either way their open was not captured.
func (s *Schedule) Update(series string, unopened []kalshi.Market) (added, vanished int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	listed := make(map[string]bool, len(unopened))
	for _, m := range unopened {
		listed[m.Ticker] = true
		if _, taken := s.done[m.Ticker]; taken {
			continue
		}
		if _, ok := s.pending[m.Ticker]; !ok {
			added++
		}
		s.pending[m.Ticker] = Opening{Market: m, Series: series}
	}
	for ticker, o := range s.pending {
		if o.Series == series && !listed[ticker] {
			delete(s.pending, ticker)
			vanished++
		}
	}
	return added, vanished
}

// Drop removes every scheduled market of a series that is no longer
// watched. Nothing is counted: they were not going to be captured.
func (s *Schedule) Drop(series string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for ticker, o := range s.pending {
		if o.Series == series {
			delete(s.pending, ticker)
		}
	}
}

// EventOpening is the rungs of one event that open together: what one
// capture connection subscribes to.
type EventOpening struct {
	Series string
	Event  string    // the event ticker; the market ticker for a market without one
	Rungs  []Opening // by open_time, then ticker; never empty
}

// First and Last are the earliest and latest open_time among the rungs.
func (e EventOpening) First() time.Time { return e.Rungs[0].Market.Open }
func (e EventOpening) Last() time.Time  { return e.Rungs[len(e.Rungs)-1].Market.Open }

// eventKey groups a market with the other rungs of its event.
func eventKey(o Opening) string {
	if o.Market.Event == "" {
		return o.Series + "/" + o.Market.Ticker
	}
	return o.Series + "/" + o.Market.Event
}

// Due takes out every event with a rung whose open_time is within lead
// of now, whole: all of the scheduled rungs of that event that open
// within span of its earliest one go with it, so a ladder is one
// EventOpening however its open_times are staggered inside that span.
// (Rungs opening later than that stay scheduled and come due as a
// group of their own.)
//
// Events whose earliest rung is still ahead of its open_time are
// returned in due, soonest first. Events already past it come back in
// late: subscribing now would record a book that is no longer the
// opening one.
func (s *Schedule) Due(now time.Time, lead, span time.Duration) (due, late []EventOpening) {
	s.mu.Lock()
	defer s.mu.Unlock()
	dueKeys := map[string]bool{}
	for _, o := range s.pending {
		if o.Market.Open.Sub(now) <= lead {
			dueKeys[eventKey(o)] = true
		}
	}
	if len(dueKeys) > 0 {
		groups := map[string][]Opening{}
		for _, o := range s.pending {
			if k := eventKey(o); dueKeys[k] {
				groups[k] = append(groups[k], o)
			}
		}
		for _, rungs := range groups {
			sortOpenings(rungs)
			first := rungs[0].Market.Open
			n := 0
			for n < len(rungs) && rungs[n].Market.Open.Sub(first) <= span {
				n++
			}
			rungs = rungs[:n]
			for _, o := range rungs {
				delete(s.pending, o.Market.Ticker)
				s.done[o.Market.Ticker] = o.Market.Open
			}
			ev := EventOpening{Series: rungs[0].Series, Event: rungs[0].Market.Event, Rungs: rungs}
			if ev.Event == "" {
				ev.Event = rungs[0].Market.Ticker
			}
			if now.After(first) {
				late = append(late, ev)
			} else {
				due = append(due, ev)
			}
		}
	}
	for ticker, open := range s.done {
		if now.Sub(open) > doneKeep {
			delete(s.done, ticker)
		}
	}
	sortEvents(due)
	sortEvents(late)
	return due, late
}

func sortEvents(es []EventOpening) {
	slices.SortFunc(es, func(a, b EventOpening) int {
		return cmp.Or(a.First().Compare(b.First()), cmp.Compare(a.Series, b.Series), cmp.Compare(a.Event, b.Event))
	})
}

// Len is how many markets are scheduled.
func (s *Schedule) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.pending)
}

// Next is the earliest scheduled open_time; ok is false if nothing is
// scheduled.
func (s *Schedule) Next() (t time.Time, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, o := range s.pending {
		if !ok || o.Market.Open.Before(t) {
			t, ok = o.Market.Open, true
		}
	}
	return t, ok
}
