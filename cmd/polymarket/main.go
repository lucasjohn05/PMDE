package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"

	"github.com/lucasjohn05/pmde/internal/polymarket"
)

const wsURL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

const assetID = "63842529068710005716169325380315470359047749786610778647370693404952498013178"

const subscribeMsg = `{"assets_ids":["` + assetID + `"],"type":"market"}`

func main() {
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		log.Fatalf("dial error: %v", err)
	}
	fmt.Println("connected")

	var writeMu sync.Mutex

	writeMu.Lock()
	err = conn.WriteMessage(websocket.TextMessage, []byte(subscribeMsg))
	writeMu.Unlock()
	if err != nil {
		log.Fatalf("subscribe write error: %v", err)
	}

	stopHeartbeat := make(chan struct{})
	var hbWg sync.WaitGroup
	hbWg.Add(1)
	go func() {
		defer hbWg.Done()
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				writeMu.Lock()
				err := conn.WriteMessage(websocket.TextMessage, []byte("PING"))
				writeMu.Unlock()
				if err != nil {
					fmt.Printf("heartbeat write error: %v\n", err)
					return
				}
			case <-stopHeartbeat:
				return
			}
		}
	}()

	// Ensure a clean close (and thus a deterministic "connection closed"
	// line + read-loop exit) on Ctrl+C.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		conn.Close()
	}()

	feed := polymarket.NewFeed(assetID)
	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			fmt.Printf("read error: %v\n", err)
			break
		}
		if string(msg) == "PONG" {
			continue
		}

		wasStale := feed.Stale()
		changed, err := feed.Handle(msg)
		if err != nil {
			log.Printf("polymarket: %v", err)
		}
		switch {
		case !wasStale && feed.Stale():
			log.Printf("polymarket: book stale, ignoring price_change until the next book event")
		case wasStale && !feed.Stale():
			log.Printf("polymarket: book synced from book event")
		}
		if changed {
			bid, ask, ok := feed.Best()
			if ok {
				log.Printf("polymarket: best: bid=%d ask=%d bps", bid, ask)
			} else {
				log.Printf("polymarket: best: none")
			}
		}
	}

	close(stopHeartbeat)
	hbWg.Wait()
	fmt.Println("connection closed")
}
