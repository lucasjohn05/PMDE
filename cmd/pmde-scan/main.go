// Command pmde-scan is the open scanner's recorder: it watches Kalshi
// for markets about to open in the configured series (and in newly
// listed series), and records each one's order book from a few seconds
// before its open_time until a minute after, under <data-dir>/opens.
// It records only; it has no order-placing code.
//
//	KALSHI_KEY_PATH=... KALSHI_KEY_ID=... pmde-scan -config opens.yaml -data-dir data
//
// cmd/pmde-opens backfills results and reports on what it recorded.
package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/lucasjohn05/pmde/internal/kalshi"
	"github.com/lucasjohn05/pmde/internal/metrics"
	"github.com/lucasjohn05/pmde/internal/opens"
)

const kalshiURL = "wss://external-api-ws.kalshi.com/trade-api/ws/v2"

func main() {
	configPath := flag.String("config", "opens.yaml", "open scanner config (YAML)")
	dataDir := flag.String("data-dir", "data", "data directory; the scanner writes under its opens/ subdirectory")
	metricsAddr := flag.String("metrics-addr", ":9101", "address to serve Prometheus /metrics on")
	kalshiAPI := flag.String("kalshi-api", kalshi.RESTBase, "Kalshi REST base")
	flag.Parse()

	cfg, err := opens.LoadConfig(*configPath)
	if err != nil {
		log.Fatal(err)
	}
	keyPath, keyID := os.Getenv("KALSHI_KEY_PATH"), os.Getenv("KALSHI_KEY_ID")
	if keyPath == "" || keyID == "" {
		log.Fatalf("KALSHI_KEY_PATH and KALSHI_KEY_ID must be set")
	}
	privKey, err := kalshi.LoadPrivateKey(keyPath)
	if err != nil {
		log.Fatalf("%v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	reg := metrics.NewRegistry()
	m := metrics.NewOpens(reg)
	addr, err := metrics.Serve(ctx, *metricsAddr, reg, log.Printf)
	if err != nil {
		log.Fatalf("metrics: %v", err)
	}
	log.Printf("metrics: serving http://%s/metrics", addr)

	dir := filepath.Join(*dataDir, "opens")
	schedule := opens.NewSchedule()
	discovery := &opens.Discovery{
		REST:     &kalshi.REST{Base: *kalshiAPI, Logf: log.Printf, OnResponse: m.RESTResponse},
		Dir:      dir,
		Cfg:      cfg,
		Schedule: schedule,
		Metrics:  m,
		Logf:     log.Printf,
	}
	if err := discovery.Load(); err != nil {
		log.Fatal(err)
	}
	newQuotes := &opens.NewSeriesQuotes{Dir: dir, For: cfg.NewSeriesFor, Metrics: m, Logf: log.Printf}
	if err := newQuotes.Load(time.Now()); err != nil {
		log.Fatal(err)
	}
	recorder := opens.NewRecorder(dir, m, log.Printf)
	capturer := &opens.Capturer{
		URL:      kalshiURL,
		Header:   func() (http.Header, error) { return kalshi.SignedHeader(privKey, keyID) },
		Cfg:      cfg,
		Schedule: schedule,
		Recorder: recorder,
		Dir:      dir,
		Metrics:  m,
		Logf:     log.Printf,

		NewSeries: discovery.NewSeries,
		NewQuotes: newQuotes,
	}
	log.Printf("scanning %d allowlisted series (new series: %v); subscribing %v before each open, recording %v after, at most %d at once; quote rule %v; storing to %s",
		len(cfg.Series), cfg.IncludeNewSeries, cfg.Lead, cfg.Window, cfg.MaxConcurrent, cfg.Quote, dir)
	for _, sw := range cfg.SeriesWindows {
		log.Printf("recording %s* series for %v after each open (series_windows)", sw.Prefix, sw.Window)
	}

	recorderDone := make(chan struct{})
	go func() {
		defer close(recorderDone)
		recorder.Run(ctx)
	}()
	var wg sync.WaitGroup
	wg.Go(func() { discovery.Run(ctx) })
	wg.Go(func() { capturer.Run(ctx) })
	wg.Go(func() {
		// New series age out of the alert's metric.
		t := time.NewTicker(10 * time.Minute)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-t.C:
				newQuotes.Expire(now)
			}
		}
	})
	wg.Go(func() { showBadMakers(ctx, dir, m) })
	wg.Wait()
	<-recorderDone // the recorder flushes and closes its files on the way out
}

// showBadMakers exposes the candidates of the latest pmde-opens
// badmakers run (opens/bad_makers.json, written by the daily cron job)
// as metrics, for the Grafana alert, and keeps them current.
func showBadMakers(ctx context.Context, dir string, m *metrics.Opens) {
	var last int64 = -1
	t := time.NewTicker(time.Minute)
	defer t.Stop()
	for {
		b, err := opens.LoadBadMakers(dir)
		switch {
		case err != nil:
			log.Printf("bad makers: %v", err)
		case b.EvaluatedMs != last:
			last = b.EvaluatedMs
			m.SetBadMakers(b.Metrics())
			if n := len(b.Candidates()); n > 0 {
				log.Printf("bad makers: %d candidate(s) in the evaluation of %s", n, time.UnixMilli(b.EvaluatedMs).UTC().Format(time.RFC3339))
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
