// Command pmde runs a venue client for every market in the pair config,
// tracks each pair's divergence, and stores every Tick with 1s/1m/1h
// rollups.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime/debug"
	"sync"
	"syscall"

	"github.com/lucasjohn05/pmde/internal/divergence"
	"github.com/lucasjohn05/pmde/internal/kalshi"
	"github.com/lucasjohn05/pmde/internal/metrics"
	"github.com/lucasjohn05/pmde/internal/polymarket"
	"github.com/lucasjohn05/pmde/internal/storage"
	"github.com/lucasjohn05/pmde/internal/tick"
)

const (
	kalshiURL     = "wss://external-api-ws.kalshi.com/trade-api/ws/v2"
	polymarketURL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
)

// commit is the git commit pmde was built from, set at build time with
// -ldflags "-X main.commit=<hash>" (the Dockerfile does this, since .git
// isn't in its build context). Left empty, it comes from the VCS stamp
// that go build records when run inside the repo.
var commit string

func main() {
	configPath := flag.String("config", "pairs.yaml", "venue pair config (YAML)")
	metricsAddr := flag.String("metrics-addr", ":9100", "address to serve Prometheus /metrics on")
	dataDir := flag.String("data-dir", "data", "directory to store ticks and rollups in")
	flag.Parse()

	config, err := os.ReadFile(*configPath)
	if err != nil {
		log.Fatalf("reading pair config: %v", err)
	}
	pairs, err := divergence.ParsePairs(config)
	if err != nil {
		log.Fatalf("%s: %v", *configPath, err)
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
	m := metrics.New(reg)
	addr, err := metrics.Serve(ctx, *metricsAddr, reg, log.Printf)
	if err != nil {
		log.Fatalf("metrics: %v", err)
	}
	log.Printf("metrics: serving http://%s/metrics", addr)

	store, err := storage.NewWriter(*dataDir, m, log.Printf)
	if err != nil {
		log.Fatalf("%v", err)
	}
	if err := storage.UpdateIndex(*dataDir, indexEntries(pairs)); err != nil {
		log.Fatalf("%v", err)
	}
	sum := sha256.Sum256(config)
	start := storage.RunRecord{
		Commit:       buildCommit(),
		Config:       *configPath,
		ConfigSHA256: hex.EncodeToString(sum[:]),
	}
	for _, p := range pairs {
		start.Pairs = append(start.Pairs, p.Name)
	}
	runs, err := storage.OpenRunLog(*dataDir, start, log.Printf)
	if err != nil {
		log.Fatalf("%v", err)
	}
	log.Printf("run %s: commit %s, config sha256 %s", runs.ID(), start.Commit, start.ConfigSHA256[:12])
	go runs.Heartbeat(ctx)

	// The fan-out: every client's book goroutine sends each Tick to the
	// divergence tracker and to the storage writer. Both sends block when
	// their queue is full (CLAUDE.md: stages downstream of the book
	// block), except on shutdown, when the stage may have stopped reading.
	// A blocked send holds up that client's book goroutine; see the
	// README's Storage section for where that leads.
	ticks := make(chan tick.Tick, divergence.TickQueueSize)
	send := func(t tick.Tick) {
		select {
		case ticks <- t:
		case <-ctx.Done():
			return
		}
		store.Send(ctx, t)
	}

	// One client (one connection) per distinct market: a market in
	// several pairs is subscribed once.
	var wg sync.WaitGroup
	seen := map[string]bool{}
	for _, p := range pairs {
		if !seen[tick.Kalshi+"/"+p.Kalshi] {
			seen[tick.Kalshi+"/"+p.Kalshi] = true
			c := &kalshi.Client{
				URL:     kalshiURL,
				Ticker:  p.Kalshi,
				Header:  func() (http.Header, error) { return kalshi.SignedHeader(privKey, keyID) },
				OnTick:  send,
				Logf:    prefixed(p.Kalshi),
				Metrics: m.Market(tick.Kalshi, p.Kalshi),
			}
			wg.Go(func() { c.Run(ctx) })
		}
		if !seen[tick.Polymarket+"/"+p.Polymarket] {
			seen[tick.Polymarket+"/"+p.Polymarket] = true
			c := &polymarket.Client{
				URL:     polymarketURL,
				AssetID: p.Polymarket,
				OnTick:  send,
				Logf:    prefixed(short(p.Polymarket)),
				Metrics: m.Market(tick.Polymarket, p.Polymarket),
			}
			wg.Go(func() { c.Run(ctx) })
		}
	}
	log.Printf("tracking %d pair(s) over %d connection(s); storing to %s", len(pairs), len(seen), *dataDir)

	storeDone := make(chan struct{})
	go func() {
		defer close(storeDone)
		store.Run(ctx)
	}()
	divergence.NewTracker(pairs, m, log.Printf).Run(ctx, ticks)
	wg.Wait()
	<-storeDone // the writer flushes and closes its files on the way out
	runs.Stop() // last, so a stop line means the run's data is on disk
}

// buildCommit is the commit to record in the run log: commit if set at
// build time, else go build's VCS stamp ("-dirty" if the tree had
// uncommitted changes), else "unknown".
func buildCommit() string {
	if commit != "" {
		return commit
	}
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}
	var rev, modified string
	for _, s := range info.Settings {
		switch s.Key {
		case "vcs.revision":
			rev = s.Value
		case "vcs.modified":
			modified = s.Value
		}
	}
	if rev == "" {
		return "unknown"
	}
	if modified == "true" {
		rev += "-dirty"
	}
	return rev
}

// indexEntries maps each market directory to the pairs it's part of.
func indexEntries(pairs []divergence.Pair) map[string]storage.IndexEntry {
	entries := map[string]storage.IndexEntry{}
	add := func(venue, market, pair string) {
		key := storage.MarketDir(venue, market)
		e := entries[key]
		e.Venue, e.Market, e.Pairs = venue, market, append(e.Pairs, pair)
		entries[key] = e
	}
	for _, p := range pairs {
		add(tick.Kalshi, p.Kalshi, p.Name)
		add(tick.Polymarket, p.Polymarket, p.Name)
	}
	return entries
}

// prefixed returns a Logf that tags each line with the market, since
// several clients of the same venue share the log.
func prefixed(market string) func(string, ...any) {
	return func(format string, args ...any) {
		log.Printf("["+market+"] "+format, args...)
	}
}

// short abbreviates a Polymarket asset ID (~77 digits) for log lines.
func short(assetID string) string {
	if len(assetID) <= 12 {
		return assetID
	}
	return assetID[:6] + "…" + assetID[len(assetID)-6:]
}
