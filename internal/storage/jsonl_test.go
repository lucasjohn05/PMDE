package storage

import (
	"os"
	"path/filepath"
	"testing"
)

type jsonlRec struct {
	N int    `json:"n"`
	S string `json:"s,omitempty"`
}

func TestAppenderRepairsTornLineAndRoundTrips(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sub", "x.jsonl")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	// A complete line, then one a crash cut short.
	if err := os.WriteFile(path, []byte("{\"n\":1}\n{\"n\":2,\"s\":\"to"), 0o644); err != nil {
		t.Fatal(err)
	}
	a, cut, err := OpenAppender(path)
	if err != nil {
		t.Fatal(err)
	}
	if cut != int64(len("{\"n\":2,\"s\":\"to")) {
		t.Errorf("cut %d bytes", cut)
	}
	if err := a.Append(jsonlRec{N: 3, S: "three"}); err != nil {
		t.Fatal(err)
	}
	// Buffered: not on disk until Flush.
	if got, _ := ReadJSONL[jsonlRec](path); len(got) != 1 {
		t.Errorf("before Flush: %d records on disk, want 1", len(got))
	}
	if err := a.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if err := AppendLine(path, jsonlRec{N: 4}); err != nil {
		t.Fatal(err)
	}
	got, err := ReadJSONL[jsonlRec](path)
	if err != nil {
		t.Fatal(err)
	}
	want := []jsonlRec{{N: 1}, {N: 3, S: "three"}, {N: 4}}
	if len(got) != len(want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("record %d: got %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestWriteJSONLReplacesAtomically(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "deep", "rows.jsonl")
	if err := WriteJSONL(path, []jsonlRec{{N: 1}, {N: 2}}); err != nil {
		t.Fatal(err)
	}
	if err := WriteJSONL(path, []jsonlRec{{N: 9}}); err != nil {
		t.Fatal(err)
	}
	got, err := ReadJSONL[jsonlRec](path)
	if err != nil || len(got) != 1 || got[0].N != 9 {
		t.Fatalf("got %+v, err %v", got, err)
	}
	entries, err := os.ReadDir(filepath.Dir(path))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Errorf("%d files left in the directory, want only rows.jsonl", len(entries))
	}
}
