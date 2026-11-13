package storage

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// Appender appends records to one JSON Lines file, with the same
// guarantees as the tick segments it shares its code with: a line a
// crash left torn is cut off when the file is opened, writes are
// buffered, and Flush pushes them to the OS and fsyncs. It is for
// record streams other than Ticks (the open scanner's). Not safe for
// concurrent use.
type Appender struct {
	seg segment
}

// OpenAppender opens path for appending, creating it and its directory
// if needed. cut is how many bytes of a torn final line were removed.
func OpenAppender(path string) (a *Appender, cut int64, err error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, 0, fmt.Errorf("storage: %w", err)
	}
	f, cut, err := openSegment(path)
	if err != nil {
		return nil, 0, fmt.Errorf("storage: opening %s: %w", path, err)
	}
	return &Appender{seg: segment{path: path, f: f, w: bufio.NewWriter(f)}}, cut, nil
}

// Path is the file the Appender writes.
func (a *Appender) Path() string { return a.seg.path }

// Append buffers rec as one line.
func (a *Appender) Append(rec any) error {
	line, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("storage: encoding a record for %s: %w", a.seg.path, err)
	}
	if _, err := a.seg.w.Write(append(line, '\n')); err != nil {
		return fmt.Errorf("storage: writing %s: %w", a.seg.path, err)
	}
	a.seg.dirty = true
	return nil
}

// Flush writes out what is buffered and fsyncs, if anything was appended
// since the last Flush.
func (a *Appender) Flush() error {
	if !a.seg.dirty {
		return nil
	}
	if err := a.seg.w.Flush(); err != nil {
		return fmt.Errorf("storage: writing %s: %w", a.seg.path, err)
	}
	if err := a.seg.f.Sync(); err != nil {
		return fmt.Errorf("storage: syncing %s: %w", a.seg.path, err)
	}
	a.seg.dirty = false
	return nil
}

// Close flushes and closes the file.
func (a *Appender) Close() error {
	err := a.Flush()
	if cerr := a.seg.f.Close(); err == nil && cerr != nil {
		err = fmt.Errorf("storage: closing %s: %w", a.seg.path, cerr)
	}
	return err
}

// AppendLine appends one record to path and fsyncs: for logs that get a
// line now and then, where holding the file open isn't worth it.
func AppendLine(path string, rec any) error {
	a, _, err := OpenAppender(path)
	if err != nil {
		return err
	}
	if err := a.Append(rec); err != nil {
		a.Close()
		return err
	}
	return a.Close()
}

// ReadJSONL decodes a JSON Lines file of T. As with ReadTicks, a torn
// final line is skipped and a bad line anywhere else is an error. A
// missing file is an error (os.IsNotExist).
func ReadJSONL[T any](path string) ([]T, error) {
	return readLines[T](path)
}

// WriteJSONL replaces path with recs, one per line. It writes a
// temporary file next to path, fsyncs it and renames it over path, so a
// reader (or a crash) sees the old file or the new one, never half.
func WriteJSONL[T any](path string, recs []T) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("storage: %w", err)
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".tmp*")
	if err != nil {
		return fmt.Errorf("storage: %w", err)
	}
	defer os.Remove(tmp.Name()) // no-op once renamed
	w := bufio.NewWriter(tmp)
	enc := json.NewEncoder(w)
	for _, r := range recs {
		if err := enc.Encode(r); err != nil {
			tmp.Close()
			return fmt.Errorf("storage: writing %s: %w", path, err)
		}
	}
	if err := w.Flush(); err == nil {
		err = tmp.Sync()
	}
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Rename(tmp.Name(), path)
	}
	if err != nil {
		return fmt.Errorf("storage: writing %s: %w", path, err)
	}
	return nil
}

// SafeComponent reports whether s can be used as one directory or file
// name under the data directory: letters, digits, '.', '_' and '-'
// only, and not "." or "..".
func SafeComponent(s string) bool {
	return safeComponent(s)
}
