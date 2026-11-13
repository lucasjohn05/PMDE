package opens

import (
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

// Due takes out every market whose open_time is within lead of now.
// Markets still ahead of their open_time are returned in due, soonest
// first. Markets already past it come back in late: subscribing now
// would record a book that is no longer the opening one.
func (s *Schedule) Due(now time.Time, lead time.Duration) (due, late []Opening) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for ticker, o := range s.pending {
		if o.Market.Open.Sub(now) > lead {
			continue
		}
		delete(s.pending, ticker)
		s.done[ticker] = o.Market.Open
		if now.After(o.Market.Open) {
			late = append(late, o)
		} else {
			due = append(due, o)
		}
	}
	for ticker, open := range s.done {
		if now.Sub(open) > doneKeep {
			delete(s.done, ticker)
		}
	}
	sortOpenings(due)
	sortOpenings(late)
	return due, late
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
