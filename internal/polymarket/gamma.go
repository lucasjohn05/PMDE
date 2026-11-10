package polymarket

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
)

// GammaBase is Polymarket's public market-metadata API.
const GammaBase = "https://gamma-api.polymarket.com"

// Resolved reports whether the market an asset belongs to has closed and
// resolved, according to the Gamma API: closed, with
// umaResolutionStatus "resolved". A market that's only closed (its
// resolution still proposed or disputed) isn't resolved yet.
//
// Gamma's /markets leaves closed markets out unless asked, so the query
// asks for closed ones only; an open market comes back as no match,
// which is false, not an error.
//
// The CLOB's /book isn't usable for this: it answers 404 "No orderbook
// exists" for a resolved asset and for an ID that never existed alike.
//
// base is GammaBase outside tests. client may be nil (http.DefaultClient).
func Resolved(ctx context.Context, client *http.Client, base, assetID string) (bool, error) {
	if client == nil {
		client = http.DefaultClient
	}
	q := url.Values{"clob_token_ids": {assetID}, "closed": {"true"}}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/markets?"+q.Encode(), nil)
	if err != nil {
		return false, fmt.Errorf("polymarket: gamma: %w", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		return false, fmt.Errorf("polymarket: gamma: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return false, fmt.Errorf("polymarket: gamma: reading response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return false, fmt.Errorf("polymarket: gamma: HTTP %d", resp.StatusCode)
	}
	var markets []struct {
		Closed              bool   `json:"closed"`
		UMAResolutionStatus string `json:"umaResolutionStatus"`
		// A JSON array encoded as a string: "[\"111...\", \"420...\"]".
		ClobTokenIDs string `json:"clobTokenIds"`
	}
	if err := json.Unmarshal(body, &markets); err != nil {
		return false, fmt.Errorf("polymarket: gamma: decoding response: %w", err)
	}
	for _, m := range markets {
		var ids []string
		if err := json.Unmarshal([]byte(m.ClobTokenIDs), &ids); err != nil {
			return false, fmt.Errorf("polymarket: gamma: decoding clobTokenIds %q: %w", m.ClobTokenIDs, err)
		}
		if slices.Contains(ids, assetID) {
			return m.Closed && m.UMAResolutionStatus == "resolved", nil
		}
	}
	return false, nil
}
