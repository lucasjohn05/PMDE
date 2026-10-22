package storage

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestRunLog(t *testing.T) {
	dir := t.TempDir()
	start := RunRecord{Commit: "abc123", Config: "pairs.yaml", ConfigSHA256: "deadbeef", Pairs: []string{"a", "b"}}

	before := time.Now().UnixMilli()
	l, err := OpenRunLog(dir, start, t.Logf)
	if err != nil {
		t.Fatal(err)
	}
	l.event("alive", time.UnixMilli(before+60_000))
	l.Stop()
	l.event("alive", time.Now()) // after Stop: dropped, no panic, no error

	// A second run appends to the same file with its own ID.
	l2, err := OpenRunLog(dir, start, t.Logf)
	if err != nil {
		t.Fatal(err)
	}
	l2.Stop()

	recs, err := ReadRuns(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 5 {
		t.Fatalf("got %d records, want 5: %+v", len(recs), recs)
	}
	s := recs[0]
	if s.Ev != "start" || s.Run != l.ID() || s.Commit != "abc123" || s.Config != "pairs.yaml" ||
		s.ConfigSHA256 != "deadbeef" || len(s.Pairs) != 2 || s.HeartbeatMs != 60_000 || s.T < before {
		t.Errorf("start = %+v", s)
	}
	if a := recs[1]; a.Ev != "alive" || a.Run != l.ID() || a.T != before+60_000 || a.Commit != "" {
		t.Errorf("alive = %+v", a)
	}
	if st := recs[2]; st.Ev != "stop" || st.Run != l.ID() || st.T < s.T {
		t.Errorf("stop = %+v", st)
	}
	if recs[3].Ev != "start" || recs[4].Ev != "stop" || recs[3].Run != l2.ID() || l2.ID() == l.ID() {
		t.Errorf("second run = %+v, %+v", recs[3], recs[4])
	}
}

// TestRunLogTornTail: a crash mid-line leaves a partial last line; the
// next run cuts it off rather than gluing its start line onto it.
func TestRunLogTornTail(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, RunsFile)
	good := `{"ev":"start","t":1,"run":"r1"}` + "\n" + `{"ev":"alive","t":2,"run":"r1"}` + "\n"
	if err := os.WriteFile(path, []byte(good+`{"ev":"ali`), 0o644); err != nil {
		t.Fatal(err)
	}
	l, err := OpenRunLog(dir, RunRecord{Commit: "x"}, t.Logf)
	if err != nil {
		t.Fatal(err)
	}
	l.Stop()
	recs, err := ReadRuns(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 4 || recs[1].Ev != "alive" || recs[2].Ev != "start" || recs[3].Ev != "stop" {
		t.Fatalf("records = %+v", recs)
	}
}

func TestReadRunsMissing(t *testing.T) {
	recs, err := ReadRuns(t.TempDir())
	if err != nil || recs != nil {
		t.Fatalf("ReadRuns on empty dir = %v, %v; want nil, nil", recs, err)
	}
}
