package main

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/lucasjohn05/pmde/internal/kalshi"
	"github.com/lucasjohn05/pmde/internal/metrics"
)

const wsURL = "wss://external-api-ws.kalshi.com/trade-api/ws/v2"
const wsPath = "/trade-api/ws/v2"

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
		Header: func() (http.Header, error) { return signedHeader(privKey, keyID) },
		OnBest: func(bid, ask int, ok bool) {
			if ok {
				log.Printf("kalshi: best yes: bid=%d ask=%d bps", bid, ask)
			} else {
				log.Printf("kalshi: best yes: none")
			}
		},
		Logf:    log.Printf,
		Metrics: m.Venue("kalshi"),
	}
	client.Run(ctx)
}

// signedHeader builds Kalshi's auth headers. The signature covers the
// current timestamp, so this runs on every dial, reconnects included.
func signedHeader(privKey *rsa.PrivateKey, keyID string) (http.Header, error) {
	timestamp := strconv.FormatInt(time.Now().UnixMilli(), 10)
	message := timestamp + "GET" + wsPath

	hash := sha256.Sum256([]byte(message))
	sig, err := rsa.SignPSS(rand.Reader, privKey, crypto.SHA256, hash[:], &rsa.PSSOptions{
		SaltLength: rsa.PSSSaltLengthEqualsHash,
		Hash:       crypto.SHA256,
	})
	if err != nil {
		return nil, fmt.Errorf("signing request: %w", err)
	}

	header := http.Header{}
	header.Set("KALSHI-ACCESS-KEY", keyID)
	header.Set("KALSHI-ACCESS-SIGNATURE", base64.StdEncoding.EncodeToString(sig))
	header.Set("KALSHI-ACCESS-TIMESTAMP", timestamp)
	return header, nil
}
