package opens

import (
	"cmp"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/lucasjohn05/pmde/internal/storage"
)

// pairWindowMs is how close in time a bid and an ask added to one
// market's book must be to count as one two-sided placement. In the
// captures of 2026-11-13/14 most such pairs were under 50ms apart; past
// 250ms they thin out into unrelated orders.
const pairWindowMs = 250

// Quoter is one cluster of two-sided placements: every placement with
// the same bid size, ask size and width, which repeats across markets or
// days. Kalshi gives no account IDs, so this is a guess at one quoter:
// see docs/open-scanner.md, "Quoters".
type Quoter struct {
	BidSz, AskSz int64 // hundredths of a contract, as placed
	SpreadBps    int

	Placements int
	Markets    int
	Days       int // UTC days of the markets' opens

	// Volume is the size placed, bid plus ask, over all its placements
	// (hundredths of a contract); Share its share of the series' total.
	Volume int64
	Share  float64

	// DelayMs is the median time from open_time to the placement.
	DelayMs float64
}

// Quoters is one series' quoter estimate from its captured books.
type Quoters struct {
	Markets    int   // markets with captured book events read
	Placements int   // two-sided placements meeting the quote rule
	Volume     int64 // their size, bid plus ask, hundredths of a contract

	// Quoters are the clusters that repeat (across at least two
	// markets or two days), largest volume first. OneOffs are the
	// placements whose size and width never repeat, with their share of
	// Volume: unattributed.
	Quoters     []Quoter
	OneOffs     int
	OneOffShare float64
}

// Count is the number of quoters, the repeating clusters.
func (q Quoters) Count() int { return len(q.Quoters) }

// TopShare is the largest quoter's share of the series' quoted volume,
// one-offs included in the total; 0 with none.
func (q Quoters) TopShare() float64 {
	if len(q.Quoters) == 0 {
		return 0
	}
	return q.Quoters[0].Share
}

// placement is one two-sided placement.
type placement struct {
	bidSz, askSz int64
	spread       int
	market       string
	day          int64
	delayMs      int64
}

// placements finds a market's two-sided placements in its book events
// (seq order): a size added at a yes price and one added at a no price
// within pairWindowMs, the bid below the ask. A record with a hole (a
// stale marker other than the pre-open "unsynced") ends the market: a
// book with a missing delta is not read past it.
func placements(events []EventRecord, rule QuoteRule) []placement {
	type add struct {
		ms    int64
		price int // the yes bid or the yes ask, bps
		size  int64
	}
	var out []placement
	pending := map[string][]add{} // side -> unpaired adds, oldest first
	for _, r := range events {
		if r.Stale != "" && r.Stale != "unsynced" {
			break
		}
		if r.Type != "delta" || r.Price == nil || r.Delta == nil || *r.Delta <= 0 {
			continue
		}
		ms := r.TsMs
		if ms == 0 {
			ms = r.RecvMs
		}
		a := add{ms: ms, price: *r.Price, size: *r.Delta}
		if r.Side == "no" {
			a.price = 10000 - *r.Price
		}
		other := "no"
		if r.Side == "no" {
			other = "yes"
		}
		// Drop what has aged out, then pair with the newest opposite add.
		for _, s := range []string{"yes", "no"} {
			pending[s] = slices.DeleteFunc(pending[s], func(p add) bool { return ms-p.ms > pairWindowMs })
		}
		paired := false
		if opp := pending[other]; len(opp) > 0 {
			p := opp[len(opp)-1]
			bid, ask := a, p
			if r.Side == "no" {
				bid, ask = p, a
			}
			if bid.price < ask.price {
				pending[other] = opp[:len(opp)-1]
				paired = true
				pl := placement{bidSz: bid.size, askSz: ask.size, spread: ask.price - bid.price,
					market: r.Market, day: utcDay(r.OpenMs), delayMs: max(bid.ms, ask.ms) - r.OpenMs}
				if rule.MeetsSpread(bid.price, ask.price) && bid.size >= rule.MinSizeCenti && ask.size >= rule.MinSizeCenti {
					out = append(out, pl)
				}
			}
		}
		if !paired {
			pending[r.Side] = append(pending[r.Side], a)
		}
	}
	return out
}

// clusterQuoters groups placements into quoters.
func clusterQuoters(ps []placement, markets int) Quoters {
	q := Quoters{Markets: markets, Placements: len(ps)}
	type key struct {
		bid, ask int64
		spread   int
	}
	groups := map[key][]placement{}
	for _, p := range ps {
		k := key{p.bidSz, p.askSz, p.spread}
		groups[k] = append(groups[k], p)
		q.Volume += p.bidSz + p.askSz
	}
	var oneOff int64
	for k, g := range groups {
		ms, days := map[string]bool{}, map[int64]bool{}
		var vol int64
		var delays []float64
		for _, p := range g {
			ms[p.market], days[p.day] = true, true
			vol += p.bidSz + p.askSz
			delays = append(delays, float64(p.delayMs))
		}
		if len(ms) < 2 && len(days) < 2 {
			q.OneOffs += len(g)
			oneOff += vol
			continue
		}
		q.Quoters = append(q.Quoters, Quoter{BidSz: k.bid, AskSz: k.ask, SpreadBps: k.spread,
			Placements: len(g), Markets: len(ms), Days: len(days), Volume: vol, DelayMs: median(delays)})
	}
	if q.Volume > 0 {
		q.OneOffShare = float64(oneOff) / float64(q.Volume)
		for i := range q.Quoters {
			q.Quoters[i].Share = float64(q.Quoters[i].Volume) / float64(q.Volume)
		}
	}
	slices.SortFunc(q.Quoters, func(a, b Quoter) int {
		return cmp.Or(cmp.Compare(b.Volume, a.Volume), cmp.Compare(a.BidSz, b.BidSz), cmp.Compare(a.AskSz, b.AskSz), cmp.Compare(a.SpreadBps, b.SpreadBps))
	})
	return q
}

// LoadQuoters estimates every captured series' quoters from the book
// events of the markets that opened in [from, to], under the quote rule.
func LoadQuoters(dir string, from, to time.Time, rule QuoteRule) (map[string]Quoters, error) {
	if rule == (QuoteRule{}) {
		rule = DefaultQuoteRule()
	}
	seriesDirs, err := os.ReadDir(filepath.Join(dir, eventsDir))
	if os.IsNotExist(err) {
		return map[string]Quoters{}, nil
	}
	if err != nil {
		return nil, err
	}
	// Event files are by the UTC day a record was received: from the day
	// before from (subscriptions start before the open) to the day after
	// to (a long capture window can run past midnight).
	lo := from.UTC().Add(-24 * time.Hour).Format("2006-01-02")
	hi := to.UTC().Add(24 * time.Hour).Format("2006-01-02")
	out := map[string]Quoters{}
	for _, sd := range seriesDirs {
		if !sd.IsDir() {
			continue
		}
		files, err := os.ReadDir(filepath.Join(dir, eventsDir, sd.Name()))
		if err != nil {
			return nil, err
		}
		byMarket := map[string][]EventRecord{}
		for _, f := range files {
			day := strings.TrimSuffix(f.Name(), ".jsonl")
			if f.IsDir() || day == f.Name() || day < lo || day > hi {
				continue
			}
			recs, err := storage.ReadJSONL[EventRecord](filepath.Join(dir, eventsDir, sd.Name(), f.Name()))
			if err != nil {
				return nil, err
			}
			for _, r := range recs {
				if r.OpenMs >= from.UnixMilli() && r.OpenMs <= to.UnixMilli() {
					byMarket[r.Market] = append(byMarket[r.Market], r)
				}
			}
		}
		if len(byMarket) == 0 {
			continue
		}
		var ps []placement
		for _, recs := range byMarket {
			slices.SortStableFunc(recs, func(a, b EventRecord) int { return cmp.Compare(a.Seq, b.Seq) })
			ps = append(ps, placements(recs, rule)...)
		}
		out[sd.Name()] = clusterQuoters(ps, len(byMarket))
	}
	return out, nil
}
