package kalshi

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"
)

// wsPath is the path Kalshi's signature covers for the websocket
// endpoint.
const wsPath = "/trade-api/ws/v2"

// LoadPrivateKey reads an RSA private key in PEM form, PKCS#1 ("RSA
// PRIVATE KEY") or PKCS#8 ("PRIVATE KEY"), as Kalshi issues it.
func LoadPrivateKey(path string) (*rsa.PrivateKey, error) {
	keyBytes, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading key file: %w", err)
	}
	block, _ := pem.Decode(keyBytes)
	if block == nil {
		return nil, fmt.Errorf("failed to decode PEM block from %s", path)
	}
	switch block.Type {
	case "RSA PRIVATE KEY":
		key, err := x509.ParsePKCS1PrivateKey(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("parsing PKCS1 private key: %w", err)
		}
		return key, nil
	case "PRIVATE KEY":
		parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("parsing PKCS8 private key: %w", err)
		}
		key, ok := parsed.(*rsa.PrivateKey)
		if !ok {
			return nil, fmt.Errorf("PKCS8 key in %s is not an RSA private key", path)
		}
		return key, nil
	default:
		return nil, fmt.Errorf("unsupported PEM block type: %s", block.Type)
	}
}

// SignedHeader builds Kalshi's auth headers for the websocket endpoint.
// The signature covers the current timestamp, so it has to be called on
// every dial, reconnects included; it fits Client.Header directly.
func SignedHeader(privKey *rsa.PrivateKey, keyID string) (http.Header, error) {
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
