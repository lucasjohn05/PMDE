package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
)

// IndexFile is the index's name in the data directory.
const IndexFile = "index.json"

// IndexEntry describes one market directory: what it holds, and which
// configured pairs it's part of. Polymarket directories are named by
// 77-digit asset IDs, so this is how a person finds the right one.
type IndexEntry struct {
	Venue  string   `json:"venue"`
	Market string   `json:"market"`
	Pairs  []string `json:"pairs"`
}

// MarketDir is a market's directory relative to the data directory, as
// used for index keys: always '/'-separated.
func MarketDir(venue, market string) string {
	return venue + "/" + market
}

// UpdateIndex merges entries (keyed by MarketDir) into dir's index.json.
// Entries already there are kept and their pair lists unioned, so
// directories written under an older config stay explained after the
// config changes. The file is replaced atomically, by writing a temporary
// file and renaming it over the old one.
func UpdateIndex(dir string, entries map[string]IndexEntry) error {
	path := filepath.Join(dir, IndexFile)
	index := map[string]IndexEntry{}
	data, err := os.ReadFile(path)
	switch {
	case err == nil:
		if err := json.Unmarshal(data, &index); err != nil {
			return fmt.Errorf("storage: reading %s: %w", path, err)
		}
	case !errors.Is(err, fs.ErrNotExist):
		return fmt.Errorf("storage: %w", err)
	}

	for key, e := range entries {
		if !safeComponent(e.Venue) || !safeComponent(e.Market) || key != MarketDir(e.Venue, e.Market) {
			return fmt.Errorf("storage: bad index entry %q: %+v", key, e)
		}
		old, ok := index[key]
		if ok {
			e.Pairs = append(e.Pairs, old.Pairs...)
		}
		slices.Sort(e.Pairs)
		e.Pairs = slices.Compact(e.Pairs)
		index[key] = e
	}

	out, err := json.MarshalIndent(index, "", "  ") // map keys come out sorted
	if err != nil {
		return fmt.Errorf("storage: encoding index: %w", err)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("storage: %w", err)
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, append(out, '\n'), 0o644); err != nil {
		return fmt.Errorf("storage: writing index: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("storage: replacing index: %w", err)
	}
	return nil
}
