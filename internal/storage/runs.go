package storage

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// RunsFile is the run log's name in the data directory.
const RunsFile = "runs.jsonl"

// HeartbeatInterval is how often a running pmde appends an "alive" line.
const HeartbeatInterval = time.Minute

// RunRecord is one line of runs.jsonl. Every process appends a "start"
// line, an "alive" line every HeartbeatInterval, and a "stop" line on a
// clean shutdown, all tagged with its run ID. The tick files are silent
// both when a market is quiet and when pmde isn't running; this is how a
// reader tells the two apart. A run is known to be up from its start line
// to its stop line or, after a crash, its last alive line.
type RunRecord struct {
	Ev  string `json:"ev"`  // "start", "alive" or "stop"
	T   int64  `json:"t"`   // local time, Unix ms (UTC); the clock ticks' t uses
	Run string `json:"run"` // random ID shared by one process's lines

	// On "start" only.
	Commit       string   `json:"commit,omitempty"`        // git commit pmde was built from, "-dirty" if modified; "unknown" if not recorded
	Config       string   `json:"config,omitempty"`        // pair config path
	ConfigSHA256 string   `json:"config_sha256,omitempty"` // of the config file's bytes
	Pairs        []string `json:"pairs,omitempty"`         // pair names in the config
	HeartbeatMs  int64    `json:"heartbeat_ms,omitempty"`  // interval between alive lines
}

// RunLog appends one process's lines to runs.jsonl. Each line is written
// and fsynced on its own: it's one line a minute, and a heartbeat that
// sits in a buffer through a crash proves nothing.
type RunLog struct {
	mu   sync.Mutex
	f    *os.File
	id   string
	logf func(format string, args ...any)
}

// OpenRunLog opens dir's runs.jsonl for appending, cutting off a line a
// crash left torn, and writes start (with its Ev, T, Run and HeartbeatMs
// filled in).
func OpenRunLog(dir string, start RunRecord, logf func(format string, args ...any)) (*RunLog, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("storage: %w", err)
	}
	path := filepath.Join(dir, RunsFile)
	f, cut, err := openSegment(path)
	if err != nil {
		return nil, fmt.Errorf("storage: opening %s: %w", path, err)
	}
	if cut > 0 && logf != nil {
		logf("storage: %s: cut a torn last line (%d bytes)", path, cut)
	}
	var id [8]byte
	rand.Read(id[:])
	l := &RunLog{f: f, id: hex.EncodeToString(id[:]), logf: logf}
	start.Ev, start.Run, start.HeartbeatMs = "start", l.id, HeartbeatInterval.Milliseconds()
	start.T = time.Now().UnixMilli()
	if err := l.write(start); err != nil {
		f.Close()
		return nil, err
	}
	return l, nil
}

// ID is this run's ID.
func (l *RunLog) ID() string { return l.id }

// Heartbeat appends an alive line every HeartbeatInterval until ctx is
// done. A failed write is logged and retried at the next beat.
func (l *RunLog) Heartbeat(ctx context.Context) {
	t := time.NewTicker(HeartbeatInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			l.event("alive", now)
		}
	}
}

// Stop appends the stop line and closes the file. Call it once
// everything else has been written, so that a stop line means the run's
// data is on disk.
func (l *RunLog) Stop() {
	l.event("stop", time.Now())
	l.mu.Lock()
	defer l.mu.Unlock()
	l.f.Close()
	l.f = nil
}

func (l *RunLog) event(ev string, now time.Time) {
	if err := l.write(RunRecord{Ev: ev, T: now.UnixMilli(), Run: l.id}); err != nil && l.logf != nil {
		l.logf("%v", err)
	}
}

func (l *RunLog) write(rec RunRecord) error {
	line, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("storage: encoding run record: %w", err)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.f == nil {
		return nil // stopped; a heartbeat raced the stop line
	}
	if _, err := l.f.Write(append(line, '\n')); err != nil {
		return fmt.Errorf("storage: writing %s: %w", RunsFile, err)
	}
	if err := l.f.Sync(); err != nil {
		return fmt.Errorf("storage: syncing %s: %w", RunsFile, err)
	}
	return nil
}

// ReadRuns reads dir's runs.jsonl. A missing file is no runs, not an
// error: data written before the run log existed has none.
func ReadRuns(dir string) ([]RunRecord, error) {
	recs, err := readLines[RunRecord](filepath.Join(dir, RunsFile))
	if os.IsNotExist(err) {
		return nil, nil
	}
	return recs, err
}
