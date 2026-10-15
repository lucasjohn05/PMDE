package main

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/gorilla/websocket"

	"github.com/lucasjohn05/pmde/internal/kalshi"
)

const wsURL = "wss://external-api-ws.kalshi.com/trade-api/ws/v2"
const wsPath = "/trade-api/ws/v2"

const subscribeMsg = `{"id":1,"cmd":"subscribe","params":{"channels":["orderbook_delta"],"market_ticker":"KXFEDDECISION-26SEP-H25"}}`

func main() {
	keyPath := os.Getenv("KALSHI_KEY_PATH")
	if keyPath == "" {
		log.Fatalf("KALSHI_KEY_PATH is not set")
	}

	keyID := os.Getenv("KALSHI_KEY_ID")
	if keyID == "" {
		log.Fatalf("KALSHI_KEY_ID is not set")
	}

	keyBytes, err := os.ReadFile(keyPath)
	if err != nil {
		log.Fatalf("reading key file: %v", err)
	}

	block, _ := pem.Decode(keyBytes)
	if block == nil {
		log.Fatalf("failed to decode PEM block from %s", keyPath)
	}

	var privKey *rsa.PrivateKey
	switch block.Type {
	case "RSA PRIVATE KEY":
		privKey, err = x509.ParsePKCS1PrivateKey(block.Bytes)
		if err != nil {
			log.Fatalf("parsing PKCS1 private key: %v", err)
		}
	case "PRIVATE KEY":
		parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
		if err != nil {
			log.Fatalf("parsing PKCS8 private key: %v", err)
		}
		rsaKey, ok := parsed.(*rsa.PrivateKey)
		if !ok {
			log.Fatalf("PKCS8 key is not an RSA private key")
		}
		privKey = rsaKey
	default:
		log.Fatalf("unsupported PEM block type: %s", block.Type)
	}

	timestamp := strconv.FormatInt(time.Now().UnixMilli(), 10)
	message := timestamp + "GET" + wsPath

	hash := sha256.Sum256([]byte(message))
	sig, err := rsa.SignPSS(rand.Reader, privKey, crypto.SHA256, hash[:], &rsa.PSSOptions{
		SaltLength: rsa.PSSSaltLengthEqualsHash,
		Hash:       crypto.SHA256,
	})
	if err != nil {
		log.Fatalf("signing request: %v", err)
	}
	sigB64 := base64.StdEncoding.EncodeToString(sig)

	header := http.Header{}
	header.Set("KALSHI-ACCESS-KEY", keyID)
	header.Set("KALSHI-ACCESS-SIGNATURE", sigB64)
	header.Set("KALSHI-ACCESS-TIMESTAMP", timestamp)

	conn, resp, err := websocket.DefaultDialer.Dial(wsURL, header)
	if err != nil {
		if resp != nil {
			body, _ := io.ReadAll(resp.Body)
			fmt.Printf("dial failed: status=%d body=%s\n", resp.StatusCode, string(body))
		}
		log.Fatalf("dial error: %v", err)
	}
	fmt.Println("connected")

	if err := conn.WriteMessage(websocket.TextMessage, []byte(subscribeMsg)); err != nil {
		log.Fatalf("subscribe write error: %v", err)
	}

	q := kalshi.NewQueue(kalshi.QueueSize)
	bookDone := make(chan struct{})
	go func() {
		defer close(bookDone)
		runBook(q)
	}()

	// The reader does nothing but read and hand off, so it keeps pace with
	// the socket however slow the book goroutine gets. Offer never blocks;
	// if the queue is full the message is dropped and the book goes stale.
	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			fmt.Printf("read error: %v\n", err)
			break
		}
		q.Offer(msg)
	}

	q.Close()
	<-bookDone
	log.Printf("kalshi: %d message(s) dropped in total", q.Drops())
}

// runBook is the book goroutine: the only goroutine that touches the
// Feed. It applies queued messages until the reader closes the queue and
// it drains.
func runBook(q *kalshi.Queue) {
	var feed kalshi.Feed
	for more := true; more; {
		wasStale := feed.Stale()
		var changed bool
		var err error
		changed, more, err = feed.Next(q)
		if err != nil {
			log.Printf("kalshi: %v", err)
		}
		switch {
		case !wasStale && feed.Stale():
			log.Printf("kalshi: book stale, ignoring deltas until a new snapshot")
		case wasStale && !feed.Stale():
			log.Printf("kalshi: book synced from snapshot (seq %d)", feed.Book().LastSeq())
		}
		if changed {
			bid, ask, ok := feed.Best()
			if ok {
				log.Printf("kalshi: best yes: bid=%d ask=%d bps", bid, ask)
			} else {
				log.Printf("kalshi: best yes: none")
			}
		}
	}
}
