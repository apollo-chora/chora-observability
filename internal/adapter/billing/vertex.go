// vertex.go — real Cloud Billing API client for the daily reconciliation
// harness.
//
// The client targets Google's Cloud Billing API
// (`https://cloudbilling.googleapis.com/v1`) using bearer-token auth via
// a `TokenSource` seam. Production wires
// `golang.org/x/oauth2/google.DefaultTokenSource` (Application Default
// Credentials → Workload Identity Federation per `secrets-and-env`); tests
// inject a static-token TokenSource against an httptest.Server.
//
// Authn / authz (per `secrets-and-env` + `infra-gcp-compute`):
//
//   - Production: WIF impersonates a billing-reader SA on chora-489812
//     (`roles/billing.viewer` on the billing account that funds the
//     project). NO SA key files travel.
//   - Local dev: `GOOGLE_APPLICATION_CREDENTIALS` points at the local SA
//     key (`~/.config/gcloud/sa-keys/dale-cli-chora-489812.json` per
//     CLAUDE.md §9). Same code path; different credential source.
//   - Tests: TokenSource is a static-token stub and BaseURL points at
//     httptest.Server.
//
// Failure mode: the TokenSource returning an error or the API replying
// with non-2xx is surfaced verbatim. The Cloud Run Job retries via Cloud
// Run's `--max-retries` rather than baking retry logic into the client.
package billing

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// TokenSource produces an OAuth 2 bearer token for outbound calls. The
// real production wiring satisfies this with
// `oauth2.TokenSource.Token()`; tests inject a static-value stub. The
// minimal interface intentionally avoids importing
// `golang.org/x/oauth2` from this adapter to keep the dep graph small;
// the cmd/reconcile binary owns the dep + adapts the real
// `oauth2.TokenSource.Token()` value-tuple into a string return.
type TokenSource interface {
	// Token returns a fresh bearer token. Implementations are expected
	// to cache + refresh internally; this adapter calls Token() once
	// per request.
	Token() (string, error)
}

// VertexConfig is the constructor input for the real client.
type VertexConfig struct {
	// BaseURL is the Cloud Billing API base — production uses
	// `https://cloudbilling.googleapis.com`. Tests substitute an
	// httptest.Server URL.
	BaseURL string

	// TokenSource produces the bearer token for each request.
	TokenSource TokenSource

	// Timeout is the per-request HTTP timeout. Default 10s.
	Timeout time.Duration

	// HTTPClient is optional; defaults to `&http.Client{Timeout: Timeout}`.
	HTTPClient *http.Client

	// MaxRetries is the per-call retry budget on 5xx + transport errors.
	// Default 3. 4xx is never retried (the call returns the error
	// verbatim — programming or auth issue, not transient).
	MaxRetries int

	// RetryBackoff is the initial exponential backoff between retries.
	// Doubles each attempt. Default 100ms.
	RetryBackoff time.Duration

	// CircuitBreakerThreshold is the number of CONSECUTIVE failed
	// calls (across retries) before the breaker opens. Default 5.
	// Once open, subsequent calls fail FAST with ErrBreakerOpen until
	// CircuitBreakerCooldown elapses.
	CircuitBreakerThreshold int

	// CircuitBreakerCooldown is how long the breaker stays open after
	// tripping. Default 60s. After cooldown the next call is a
	// half-open probe; on success the breaker closes.
	CircuitBreakerCooldown time.Duration
}

// ErrBreakerOpen is returned when the circuit breaker is in OPEN state
// and we fail-fast without hitting the network.
var ErrBreakerOpen = errors.New("billing.vertex: circuit breaker open")

// VertexClient queries aggregated cost from the Cloud Billing API.
// Implements reconcile.BillingClient + the in-package Client interface.
type VertexClient struct {
	cfg     VertexConfig
	baseURL *url.URL

	// Circuit breaker state — accessed under breakerMu.
	breakerMu        sync.Mutex
	consecutiveFails int
	breakerOpenedAt  time.Time
}

// NewVertexClient constructs a real Cloud Billing API client.
func NewVertexClient(cfg VertexConfig) (*VertexClient, error) {
	if strings.TrimSpace(cfg.BaseURL) == "" {
		return nil, errors.New("billing.NewVertexClient: BaseURL required")
	}
	if cfg.TokenSource == nil {
		return nil, errors.New("billing.NewVertexClient: TokenSource required")
	}
	u, err := url.Parse(cfg.BaseURL)
	if err != nil {
		return nil, fmt.Errorf("billing.NewVertexClient: invalid BaseURL: %w", err)
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 10 * time.Second
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = &http.Client{Timeout: cfg.Timeout}
	}
	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = 3
	}
	if cfg.RetryBackoff <= 0 {
		cfg.RetryBackoff = 100 * time.Millisecond
	}
	if cfg.CircuitBreakerThreshold <= 0 {
		cfg.CircuitBreakerThreshold = 5
	}
	if cfg.CircuitBreakerCooldown <= 0 {
		cfg.CircuitBreakerCooldown = 60 * time.Second
	}
	return &VertexClient{cfg: cfg, baseURL: u}, nil
}

// breakerAllow returns true when the breaker permits a call. Call it
// once per call. When it returns false the caller MUST fail fast with
// ErrBreakerOpen.
func (c *VertexClient) breakerAllow() bool {
	c.breakerMu.Lock()
	defer c.breakerMu.Unlock()
	if c.consecutiveFails < c.cfg.CircuitBreakerThreshold {
		return true
	}
	// Breaker is OPEN. Has cooldown elapsed?
	if time.Since(c.breakerOpenedAt) >= c.cfg.CircuitBreakerCooldown {
		// Half-open probe — let one call through. If it succeeds the
		// next breakerOnSuccess() will reset the counter.
		return true
	}
	return false
}

// breakerOnSuccess records a successful call; resets the counter so
// the breaker stays CLOSED.
func (c *VertexClient) breakerOnSuccess() {
	c.breakerMu.Lock()
	defer c.breakerMu.Unlock()
	c.consecutiveFails = 0
	c.breakerOpenedAt = time.Time{}
}

// breakerOnFailure records a failed call; trips the breaker when the
// threshold is reached.
func (c *VertexClient) breakerOnFailure() {
	c.breakerMu.Lock()
	defer c.breakerMu.Unlock()
	c.consecutiveFails++
	if c.consecutiveFails >= c.cfg.CircuitBreakerThreshold {
		c.breakerOpenedAt = time.Now()
	}
}

// queryRequest is the on-wire shape POSTed to the Billing API. Mirrors
// the canonical `aggregateCost` query body — the production endpoint
// path (`/v1/billingAccounts/{billing_account}/projects/{project}:aggregateCost`)
// is constructed at request time but kept simple here.
type queryRequest struct {
	ProjectID      string `json:"projectId"`
	WindowStartIso string `json:"windowStartIso"`
	WindowEndIso   string `json:"windowEndIso"`
}

// queryResponse mirrors the Cloud Billing aggregated-cost response.
type queryResponse struct {
	CostMicros   int64  `json:"costMicros"`
	CurrencyCode string `json:"currencyCode"`
}

// QueryAggregatedCostMicrosForWindow implements reconcile.BillingClient.
//
// Wire shape: POST {BaseURL}/v1/projects/{project}:aggregateCost with
// the (project, window) tuple in the JSON body.
//
// Resilience:
//
//   - Circuit breaker: when N consecutive calls fail, the breaker
//     trips and subsequent calls fail FAST with ErrBreakerOpen for
//     CircuitBreakerCooldown so the daily cron does not exhaust its
//     timeout retrying a permanently broken endpoint. After cooldown
//     a half-open probe is allowed; a successful probe closes it.
//
//   - Retry: 5xx + transport errors retry up to MaxRetries with
//     exponential backoff. 4xx surfaces verbatim — programming /
//     auth issue, not transient.
//
//   - Context honoured — `ctx.Err()` short-circuits the retry loop.
func (c *VertexClient) QueryAggregatedCostMicrosForWindow(ctx context.Context, projectID string, windowStart, windowEnd time.Time) (int64, error) {
	if !c.breakerAllow() {
		return 0, ErrBreakerOpen
	}

	body, err := json.Marshal(queryRequest{
		ProjectID:      projectID,
		WindowStartIso: windowStart.UTC().Format(time.RFC3339),
		WindowEndIso:   windowEnd.UTC().Format(time.RFC3339),
	})
	if err != nil {
		return 0, fmt.Errorf("billing.marshal: %w", err)
	}
	endpoint := strings.TrimRight(c.baseURL.String(), "/") +
		"/v1/projects/" + url.PathEscape(projectID) + ":aggregateCost"

	backoff := c.cfg.RetryBackoff
	var lastErr error
	for attempt := 1; attempt <= c.cfg.MaxRetries; attempt++ {
		// Re-fetch token each attempt — short-lived ADC tokens may be
		// stale by the time a retry runs (>5min slow path).
		tok, err := c.cfg.TokenSource.Token()
		if err != nil {
			c.breakerOnFailure()
			return 0, fmt.Errorf("billing.token: %w", err)
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
		if err != nil {
			c.breakerOnFailure()
			return 0, fmt.Errorf("billing.newrequest: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json")
		req.Header.Set("Authorization", "Bearer "+tok)

		resp, err := c.cfg.HTTPClient.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("billing.do: %w", err)
			if attempt < c.cfg.MaxRetries {
				if waitErr := sleepCtx(ctx, backoff); waitErr != nil {
					c.breakerOnFailure()
					return 0, waitErr
				}
				backoff *= 2
				continue
			}
			c.breakerOnFailure()
			return 0, lastErr
		}

		// 2xx — happy path.
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			var out queryResponse
			err := json.NewDecoder(resp.Body).Decode(&out)
			_ = resp.Body.Close()
			if err != nil {
				c.breakerOnFailure()
				return 0, fmt.Errorf("billing.decode: %w", err)
			}
			c.breakerOnSuccess()
			return out.CostMicros, nil
		}

		// 4xx — surface immediately, no retry. Counts as a breaker
		// failure so persistent auth issues trip the breaker too.
		if resp.StatusCode >= 400 && resp.StatusCode < 500 {
			buf, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<14))
			_ = resp.Body.Close()
			c.breakerOnFailure()
			return 0, fmt.Errorf("billing.status=%d body=%s", resp.StatusCode, string(buf))
		}

		// 5xx — backoff + retry.
		buf, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<14))
		_ = resp.Body.Close()
		lastErr = fmt.Errorf("billing.status=%d body=%s", resp.StatusCode, string(buf))
		if attempt < c.cfg.MaxRetries {
			if waitErr := sleepCtx(ctx, backoff); waitErr != nil {
				c.breakerOnFailure()
				return 0, waitErr
			}
			backoff *= 2
			continue
		}
	}
	c.breakerOnFailure()
	if lastErr == nil {
		lastErr = errors.New("billing.retries: exhausted with no error captured")
	}
	return 0, lastErr
}

// sleepCtx waits for d unless ctx is cancelled.
func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
