package kalshi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestCloseTime(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/markets/KXFEDDECISION-26OCT-H0":
			// Trimmed from the live response, 2026-11-10.
			w.Write([]byte(`{"market":{"ticker":"KXFEDDECISION-26OCT-H0","status":"finalized","close_time":"2026-10-28T17:59:00Z"}}`))
		case "/markets/WRONG":
			w.Write([]byte(`{"market":{"ticker":"OTHER","close_time":"2026-10-28T17:59:00Z"}}`))
		case "/markets/NOCLOSE":
			w.Write([]byte(`{"market":{"ticker":"NOCLOSE"}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	ctx := context.Background()

	got, err := CloseTime(ctx, srv.Client(), srv.URL, "KXFEDDECISION-26OCT-H0")
	want := time.Date(2026, 10, 28, 17, 59, 0, 0, time.UTC)
	if err != nil || !got.Equal(want) {
		t.Fatalf("CloseTime = %v, %v; want %v", got, err, want)
	}
	for _, ticker := range []string{"MISSING", "WRONG", "NOCLOSE"} {
		if got, err := CloseTime(ctx, srv.Client(), srv.URL, ticker); err == nil {
			t.Errorf("%s: CloseTime = %v, want an error", ticker, got)
		}
	}
}
