package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/lucasjohn05/pmde/internal/metrics"
	"github.com/lucasjohn05/pmde/internal/polymarket"
	"github.com/lucasjohn05/pmde/internal/tick"
)

const wsURL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

const assetID = "63842529068710005716169325380315470359047749786610778647370693404952498013178"

func main() {
	metricsAddr := flag.String("metrics-addr", ":9102", "address to serve Prometheus /metrics on")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	reg := metrics.NewRegistry()
	m := metrics.New(reg)
	addr, err := metrics.Serve(ctx, *metricsAddr, reg, log.Printf)
	if err != nil {
		log.Fatalf("metrics: %v", err)
	}
	log.Printf("metrics: serving http://%s/metrics", addr)

	client := &polymarket.Client{
		URL:     wsURL,
		AssetID: assetID,
		OnTick: func(t tick.Tick) {
			if t.OK {
				log.Printf("polymarket: best: bid=%d ask=%d bps", t.BidBps, t.AskBps)
			} else {
				log.Printf("polymarket: best: none")
			}
		},
		Logf:    log.Printf,
		Metrics: m.Market(tick.Polymarket, assetID),
	}
	client.Run(ctx)
}
