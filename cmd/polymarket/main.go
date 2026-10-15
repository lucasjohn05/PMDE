package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/lucasjohn05/pmde/internal/polymarket"
)

const wsURL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

const assetID = "63842529068710005716169325380315470359047749786610778647370693404952498013178"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	client := &polymarket.Client{
		URL:     wsURL,
		AssetID: assetID,
		OnBest: func(bid, ask int, ok bool) {
			if ok {
				log.Printf("polymarket: best: bid=%d ask=%d bps", bid, ask)
			} else {
				log.Printf("polymarket: best: none")
			}
		},
		Logf: log.Printf,
	}
	client.Run(ctx)
}
