package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/lucasjohn05/pmde/internal/kalshi"
	"github.com/lucasjohn05/pmde/internal/metrics"
	"github.com/lucasjohn05/pmde/internal/tick"
)

const wsURL = "wss://external-api-ws.kalshi.com/trade-api/ws/v2"

const marketTicker = "KXFEDDECISION-26SEP-H25"

func main() {
	metricsAddr := flag.String("metrics-addr", ":9101", "address to serve Prometheus /metrics on")
	flag.Parse()

	keyPath := os.Getenv("KALSHI_KEY_PATH")
	if keyPath == "" {
		log.Fatalf("KALSHI_KEY_PATH is not set")
	}

	keyID := os.Getenv("KALSHI_KEY_ID")
	if keyID == "" {
		log.Fatalf("KALSHI_KEY_ID is not set")
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

	client := &kalshi.Client{
		URL:    wsURL,
		Ticker: marketTicker,
		Header: func() (http.Header, error) { return kalshi.SignedHeader(privKey, keyID) },
		OnTick: func(t tick.Tick) {
			if t.OK {
				log.Printf("kalshi: best yes: bid=%d ask=%d bps", t.BidBps, t.AskBps)
			} else {
				log.Printf("kalshi: best yes: none")
			}
		},
		Logf:    log.Printf,
		Metrics: m.Market(tick.Kalshi, marketTicker),
	}
	client.Run(ctx)
}
