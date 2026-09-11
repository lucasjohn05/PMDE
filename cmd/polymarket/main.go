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
)

const wsURL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

const subscribeMsg = `{"assets_ids":["63842529068710005716169325380315470359047749786610778647370693404952498013178"],"type":"market"}`

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

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			fmt.Printf("read error: %v\n", err)
			break
		}
		if string(msg) == "PONG" {
			fmt.Println("heartbeat: got PONG")
			continue
		}
		fmt.Println(string(msg))
	}

	close(stopHeartbeat)
	hbWg.Wait()
	fmt.Println("connection closed")
}
