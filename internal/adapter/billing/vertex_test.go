// Package billing_test exercises the real Vertex Billing HTTP client.
//
// The client targets the Cloud Billing API (`cloudbilling.googleapis.com/v1`)
// using bearer-token auth sourced from a TokenSource. Production wires
// `golang.org/x/oauth2/google.DefaultTokenSource` (Application Default
// Credentials → Workload Identity Federation per `secrets-and-env`); tests
// inject a static-token TokenSource against an httptest.Server.
//
// Per ai-cost-tracking ("daily reconciliation"), this client returns the
// aggregated cost (in micros) for a (project, window) tuple. The call
// shape is intentionally minimal — the Runner only consumes the int64
// result via QueryAggregatedCostMicrosForWindow.
package billing_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/apollo-chora/chora-observability/internal/adapter/billing"
)

// staticTokenSource is a tiny TokenSource that always returns the same
// access token. Test-only — production uses ADC/WIF.
type staticTokenSource struct{ token string }

func (s *staticTokenSource) Token() (string, error) {
	if s.token == "" {
		return "", errors.New("staticTokenSource: blank token")
	}
	return s.token, nil
}

func TestVertexClient_QueryAggregatedCostMicrosForWindow_ReturnsBillingTotal(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s; want POST", r.Method)
		}
		// The Cloud Billing API uses POST for query/list filtered by service.
		// Authorisation header MUST be present + bear the static token.
		auth := r.Header.Get("Authorization")
		if !strings.HasPrefix(auth, "Bearer ") {
			t.Errorf("Authorization missing/malformed: %q", auth)
		}
		if !strings.HasSuffix(auth, " test-token-fixture") {
			t.Errorf("Authorization did not carry token: %q", auth)
		}

		// Skeleton response: total cost reported in micro-USD.
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"costMicros":     int64(2_345_000),
			"currencyCode":   "USD",
			"projectId":      "chora-489812",
			"windowStartIso": "2026-05-09T00:00:00Z",
			"windowEndIso":   "2026-05-10T00:00:00Z",
		})
	}))
	defer srv.Close()

	c, err := billing.NewVertexClient(billing.VertexConfig{
		BaseURL:     srv.URL,
		TokenSource: &staticTokenSource{token: "test-token-fixture"},
		Timeout:     2 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewVertexClient: %v", err)
	}

	ws := time.Date(2026, 5, 9, 0, 0, 0, 0, time.UTC)
	we := ws.AddDate(0, 0, 1)
	got, err := c.QueryAggregatedCostMicrosForWindow(context.Background(), "chora-489812", ws, we)
	if err != nil {
		t.Fatalf("QueryAggregatedCostMicrosForWindow: %v", err)
	}
	if got != 2_345_000 {
		t.Errorf("got = %d; want 2_345_000", got)
	}
}

func TestVertexClient_PropagatesProjectIDInRequest(t *testing.T) {
	t.Parallel()

	var capturedBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&capturedBody)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"costMicros": int64(0)})
	}))
	defer srv.Close()

	c, _ := billing.NewVertexClient(billing.VertexConfig{
		BaseURL:     srv.URL,
		TokenSource: &staticTokenSource{token: "tok"},
	})
	ws := time.Date(2026, 5, 9, 0, 0, 0, 0, time.UTC)
	we := ws.AddDate(0, 0, 1)
	if _, err := c.QueryAggregatedCostMicrosForWindow(context.Background(), "chora-content", ws, we); err != nil {
		t.Fatalf("err = %v", err)
	}
	if capturedBody["projectId"] != "chora-content" {
		t.Errorf("projectId = %v; want chora-content", capturedBody["projectId"])
	}
}

func TestVertexClient_RequiresBaseURL(t *testing.T) {
	t.Parallel()
	_, err := billing.NewVertexClient(billing.VertexConfig{
		BaseURL:     "",
		TokenSource: &staticTokenSource{token: "tok"},
	})
	if err == nil {
		t.Fatal("expected err when BaseURL blank")
	}
}

func TestVertexClient_RequiresTokenSource(t *testing.T) {
	t.Parallel()
	_, err := billing.NewVertexClient(billing.VertexConfig{
		BaseURL:     "https://cloudbilling.googleapis.com",
		TokenSource: nil,
	})
	if err == nil {
		t.Fatal("expected err when TokenSource nil")
	}
}

func TestVertexClient_SurfacesNon200(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "permission denied", http.StatusForbidden)
	}))
	defer srv.Close()

	c, _ := billing.NewVertexClient(billing.VertexConfig{
		BaseURL:     srv.URL,
		TokenSource: &staticTokenSource{token: "tok"},
	})
	_, err := c.QueryAggregatedCostMicrosForWindow(context.Background(),
		"chora-489812", time.Now(), time.Now().Add(time.Hour))
	if err == nil {
		t.Fatalf("expected error on 403")
	}
}

func TestVertexClient_SurfacesTokenSourceFailure(t *testing.T) {
	t.Parallel()
	c, _ := billing.NewVertexClient(billing.VertexConfig{
		BaseURL:     "http://example.invalid",
		TokenSource: &staticTokenSource{token: ""}, // produces error
	})
	_, err := c.QueryAggregatedCostMicrosForWindow(context.Background(),
		"chora-489812", time.Now(), time.Now().Add(time.Hour))
	if err == nil {
		t.Fatalf("expected error from TokenSource failure")
	}
}
