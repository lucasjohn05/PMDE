package kalshi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

// RESTBase is Kalshi's public trade API. GET /markets/{ticker} needs no
// authentication.
const RESTBase = "https://api.elections.kalshi.com/trade-api/v2"

// CloseTime fetches a market's close_time: when trading on it ends. On
// the FOMC markets this is 1:59 PM ET on decision day, four minutes after
// the 1:55 PM last trade the contract terms state; on Oct 28 2026 trades
// ran until 13:58:58, so close_time is the one that matches the book.
//
// base is RESTBase outside tests. client may be nil (http.DefaultClient);
// ctx bounds the request.
func CloseTime(ctx context.Context, client *http.Client, base, ticker string) (time.Time, error) {
	if client == nil {
		client = http.DefaultClient
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/markets/"+url.PathEscape(ticker), nil)
	if err != nil {
		return time.Time{}, fmt.Errorf("kalshi: market %s: %w", ticker, err)
	}
	resp, err := client.Do(req)
	if err != nil {
		return time.Time{}, fmt.Errorf("kalshi: market %s: %w", ticker, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return time.Time{}, fmt.Errorf("kalshi: market %s: reading response: %w", ticker, err)
	}
	if resp.StatusCode != http.StatusOK {
		return time.Time{}, fmt.Errorf("kalshi: market %s: HTTP %d", ticker, resp.StatusCode)
	}
	var m struct {
		Market struct {
			Ticker    string `json:"ticker"`
			CloseTime string `json:"close_time"`
		} `json:"market"`
	}
	if err := json.Unmarshal(body, &m); err != nil {
		return time.Time{}, fmt.Errorf("kalshi: market %s: decoding response: %w", ticker, err)
	}
	if m.Market.Ticker != ticker {
		return time.Time{}, fmt.Errorf("kalshi: market %s: response is for %q", ticker, m.Market.Ticker)
	}
	if m.Market.CloseTime == "" {
		return time.Time{}, errors.New("kalshi: market " + ticker + ": no close_time in response")
	}
	t, err := time.Parse(time.RFC3339, m.Market.CloseTime)
	if err != nil {
		return time.Time{}, fmt.Errorf("kalshi: market %s: close_time: %w", ticker, err)
	}
	return t, nil
}
