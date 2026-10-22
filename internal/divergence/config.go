package divergence

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"go.yaml.in/yaml/v3"
)

// Pair is one hand-written mapping: a Kalshi market and the Polymarket
// asset that quote the same outcome of the same event.
type Pair struct {
	// Name labels the pair in logs and metrics.
	Name string `yaml:"name"`

	// Kalshi is the Kalshi market ticker. Its "yes" side is compared.
	Kalshi string `yaml:"kalshi"`

	// Polymarket is the asset (token) ID of the Polymarket outcome that
	// means the same as Kalshi's "yes". Polymarket markets have two
	// tokens; picking the wrong one compares a price against its
	// complement.
	Polymarket string `yaml:"polymarket"`

	// Notes is free text for whoever maintains the pair: typically how
	// the two venues' resolution rules differ. Optional; the code doesn't
	// read it.
	Notes string `yaml:"notes"`

	// KalshiLastTrade is when trading on the Kalshi market ends, as RFC
	// 3339 with an explicit offset ("2026-10-28T13:55:00-04:00").
	// Optional. pmde-analyze excludes the pair from this time on: after
	// it, Kalshi's book is frozen while Polymarket's still trades, so the
	// divergence compares a closed market with an open one. A string, not
	// a time.Time, so that a bare date (which YAML would read as midnight
	// UTC) is an error instead; see LastTrade.
	KalshiLastTrade string `yaml:"kalshi_last_trade"`
}

// LastTrade parses KalshiLastTrade. ok is false if it isn't set.
// ParsePairs has already rejected values that don't parse.
func (p Pair) LastTrade() (t time.Time, ok bool) {
	if p.KalshiLastTrade == "" {
		return time.Time{}, false
	}
	t, err := time.Parse(time.RFC3339, p.KalshiLastTrade)
	return t, err == nil
}

type config struct {
	Pairs []Pair `yaml:"pairs"`
}

// LoadPairs reads and validates a pair config file.
func LoadPairs(path string) ([]Pair, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading pair config: %w", err)
	}
	pairs, err := ParsePairs(data)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return pairs, nil
}

// ParsePairs parses and validates a pair config:
//
//	pairs:
//	  - name: fed-oct-2026-hold
//	    kalshi: KXFEDDECISION-26OCT-H0
//	    polymarket: "1110619025448142662..."
//
// Unknown keys are errors rather than ignored, so a typo ("polymarkt")
// fails loudly instead of leaving a field empty.
func ParsePairs(data []byte) ([]Pair, error) {
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	var c config
	if err := dec.Decode(&c); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, errors.New("no pairs configured")
		}
		return nil, fmt.Errorf("parsing pair config: %w", err)
	}
	if len(c.Pairs) == 0 {
		return nil, errors.New("no pairs configured")
	}

	names := map[string]bool{}
	markets := map[[2]string]string{}
	for i, p := range c.Pairs {
		switch {
		case p.Name == "":
			return nil, fmt.Errorf("pair %d: name is required", i+1)
		case p.Kalshi == "":
			return nil, fmt.Errorf("pair %q: kalshi ticker is required", p.Name)
		case p.Polymarket == "":
			return nil, fmt.Errorf("pair %q: polymarket asset ID is required", p.Name)
		case !allDigits(p.Polymarket):
			// Asset (token) IDs are long decimal integers. The likely
			// mistake is pasting the market's condition ID (0x...) or
			// its slug instead.
			return nil, fmt.Errorf("pair %q: polymarket %q is not an asset (token) ID; those are long decimal numbers, not the 0x... condition ID", p.Name, p.Polymarket)
		case p.KalshiLastTrade != "" && !validLastTrade(p.KalshiLastTrade):
			return nil, fmt.Errorf("pair %q: kalshi_last_trade %q must be RFC 3339 with an explicit offset, like 2026-10-28T13:55:00-04:00", p.Name, p.KalshiLastTrade)
		case names[p.Name]:
			return nil, fmt.Errorf("pair %q: duplicate name", p.Name)
		}
		names[p.Name] = true
		key := [2]string{p.Kalshi, p.Polymarket}
		if other, dup := markets[key]; dup {
			return nil, fmt.Errorf("pair %q: same markets as pair %q", p.Name, other)
		}
		markets[key] = p.Name
	}
	return c.Pairs, nil
}

// validLastTrade reports whether s is an RFC 3339 time with a numeric
// offset. "Z" is refused too: the venues' rules are written in ET, and a
// UTC time typed by hand is the likelier mistake.
func validLastTrade(s string) bool {
	_, err := time.Parse(time.RFC3339, s)
	return err == nil && len(s) >= 6 && (s[len(s)-6] == '+' || s[len(s)-6] == '-')
}

func allDigits(s string) bool {
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return s != ""
}
