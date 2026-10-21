package storage

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
)

// segment is one append-only JSON Lines file: one record kind for one
// market for one UTC day. Writes go through a buffer; flush pushes them
// to the OS and fsyncs.
type segment struct {
	day  string
	path string
	f    *os.File
	w    *bufio.Writer

	dirty bool // written since the last flush
	bad   bool // the last open/write/flush failed and was logged
}

// openSegment opens path for appending, creating it if needed. If a crash
// left the file ending in a partial line (no trailing newline), that
// partial line is cut off first: it was never a complete record, and
// appending after it would glue the next record onto it. Returns how
// many bytes were cut.
func openSegment(path string) (*os.File, int64, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, 0, err
	}
	size, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		f.Close()
		return nil, 0, err
	}
	keep, err := completeLength(f, size)
	if err != nil {
		f.Close()
		return nil, 0, err
	}
	if keep < size {
		if err := f.Truncate(keep); err != nil {
			f.Close()
			return nil, 0, err
		}
		if _, err := f.Seek(keep, io.SeekStart); err != nil {
			f.Close()
			return nil, 0, err
		}
	}
	return f, size - keep, nil
}

// completeLength returns the length of f's prefix that ends in a newline:
// everything up to and including the last '\n', or 0 if there is none.
func completeLength(f *os.File, size int64) (int64, error) {
	const chunk = 4096
	buf := make([]byte, chunk)
	for end := size; end > 0; {
		start := max(end-chunk, 0)
		n, err := f.ReadAt(buf[:end-start], start)
		if err != nil && !errors.Is(err, io.EOF) {
			return 0, err
		}
		if i := bytes.LastIndexByte(buf[:n], '\n'); i >= 0 {
			return start + int64(i) + 1, nil
		}
		end = start
	}
	return 0, nil
}

// TickRecord is one line of a .ticks.jsonl file. Prices and sizes are
// present only when OK.
type TickRecord struct {
	T       int64  `json:"t"`            // local receive time, Unix ms (UTC)
	VT      int64  `json:"vt,omitempty"` // venue timestamp, Unix ms; omitted if the venue gave none
	Bid     *int   `json:"bid,omitempty"`
	Ask     *int   `json:"ask,omitempty"`
	BidSize *int64 `json:"bid_sz,omitempty"`
	AskSize *int64 `json:"ask_sz,omitempty"`
	OK      bool   `json:"ok"`
}

// BarRecord is one line of a .1s/.1m/.1h.jsonl file: a summary of the
// top of book over one window. Bid and Ask are [open, high, low, close]
// over the window's trusted periods, nil (null) if it had none.
type BarRecord struct {
	T       int64   `json:"t"` // window start, Unix ms (UTC), on the local receive clock
	N       int     `json:"n"` // Ticks in the window
	Bid     *[4]int `json:"bid"`
	Ask     *[4]int `json:"ask"`
	StaleMs int64   `json:"stale_ms"` // time in the window with no trusted book
	OK      bool    `json:"ok"`       // book trusted at the window's end
	// Partial marks a window this process didn't see whole: the first one
	// after startup (the state before it is unknown) and the one open at
	// shutdown.
	Partial bool `json:"partial,omitempty"`
}

// ReadTicks reads a .ticks.jsonl file.
func ReadTicks(path string) ([]TickRecord, error) {
	return readLines[TickRecord](path)
}

// ReadBars reads a .1s/.1m/.1h.jsonl file.
func ReadBars(path string) ([]BarRecord, error) {
	return readLines[BarRecord](path)
}

// readLines decodes a JSON Lines file. A final line that doesn't parse is
// skipped: it's a record a crash cut short (the writer trims these when it
// reopens the file, but a reader may get there first). A bad line
// anywhere else is corruption and an error.
func readLines[T any](path string) ([]T, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	lines := strings.Split(string(data), "\n")
	var out []T
	for i, line := range lines {
		if line == "" {
			continue
		}
		var rec T
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			if i == len(lines)-1 {
				break // torn final line, no trailing newline
			}
			return nil, fmt.Errorf("%s: line %d: %w", path, i+1, err)
		}
		out = append(out, rec)
	}
	return out, nil
}
