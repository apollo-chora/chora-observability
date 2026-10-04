// vertex_resilience_test.go — RED-phase TDD specs for Vertex Billing
// client resilience per the production directive:
//
//   - Exponential backoff retry on 5xx + transport errors.
//   - Circuit breaker: after N consecutive failures, fail fast for
//     COOLDOWN duration so the daily reconciliation Cloud Run Job
//     does not exhaust its 60-minute timeout retrying a permanently
//     broken billing endpoint.
//   - Per-attempt context budget honoured (don't blow the cron's
//     overall timeout on a single retry sequence).
//
// Per `data-consistency` the reconciliation event sink is a separate
// concern (DLQ + alerting); this file focuses on outbound-call
// resilience inside the adapter itself.
package billing_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apollo-chora/chora-observability/internal/adapter/billing"
)

// staticTok is reused across tests in this file.
type staticTok struct{ v string }

func (s *staticTok) Token() (string, error) { return s.v, nil }

func TestVertexClient_RetriesOn5xx(t *testing.T) {
	t.Parallel()
	calls := &atomic.Int32{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		if n < 3 {
			http.Error(w, "transient 503", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"costMicros":12345}`))
	}))
	defer srv.Close()

	c, err := billing.NewVertexClient(billing.VertexConfig{
		BaseURL:      srv.URL,
		TokenSource:  &staticTok{v: "tok"},
		MaxRetries:   5,
		RetryBackoff: 5 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewVertexClient: %v", err)
	}
	got, err := c.QueryAggregatedCostMicrosForWindow(context.Background(),
		"chora-489812", time.Now(), time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("QueryAggregatedCostMicrosForWindow: %v", err)
	}
	if got != 12345 {
		t.Errorf("got = %d; want 12345", got)
	}
	if calls.Load() != 3 {
		t.Errorf("expected 3 attempts (2 retries); got %d", calls.Load())
	}
}

func TestVertexClient_DoesNotRetry4xx(t *testing.T) {
	t.Parallel()
	calls := &atomic.Int32{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		http.Error(w, "permission denied", http.StatusForbidden)
	}))
	defer srv.Close()

	c, _ := billing.NewVertexClient(billing.VertexConfig{
		BaseURL:      srv.URL,
		TokenSource:  &staticTok{v: "tok"},
		MaxRetries:   5,
		RetryBackoff: 5 * time.Millisecond,
	})
	_, err := c.QueryAggregatedCostMicrosForWindow(context.Background(),
		"chora-489812", time.Now(), time.Now().Add(time.Hour))
	if err == nil {
		t.Fatalf("expected 403 error")
	}
	if calls.Load() != 1 {
		t.Errorf("4xx must not retry; got %d attempts", calls.Load())
	}
}

func TestVertexClient_CircuitBreaker_OpensAfterConsecutiveFailures(t *testing.T) {
	t.Parallel()
	calls := &atomic.Int32{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		http.Error(w, "down", http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	c, err := billing.NewVertexClient(billing.VertexConfig{
		BaseURL:                 srv.URL,
		TokenSource:             &staticTok{v: "tok"},
		MaxRetries:              1, // 1 attempt per call (no in-call retry)
		CircuitBreakerThreshold: 3,
		CircuitBreakerCooldown:  100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewVertexClient: %v", err)
	}

	// Three failed calls open the breaker.
	for i := 0; i < 3; i++ {
		_, _ = c.QueryAggregatedCostMicrosForWindow(context.Background(),
			"chora-489812", time.Now(), time.Now().Add(time.Hour))
	}
	failsBeforeBreaker := calls.Load()

	// Subsequent calls fail FAST without hitting the network.
	_, err = c.QueryAggregatedCostMicrosForWindow(context.Background(),
		"chora-489812", time.Now(), time.Now().Add(time.Hour))
	if err == nil {
		t.Fatalf("expected circuit-breaker error")
	}
	if calls.Load() != failsBeforeBreaker {
		t.Errorf("breaker open should NOT hit the network; got extra %d calls",
			calls.Load()-failsBeforeBreaker)
	}
}

func TestVertexClient_CircuitBreaker_ClosesAfterCooldown(t *testing.T) {
	t.Parallel()

	// Server: first 3 calls 503; subsequent calls 200.
	calls := &atomic.Int32{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		if n <= 3 {
			http.Error(w, "down", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"costMicros":99}`))
	}))
	defer srv.Close()

	c, _ := billing.NewVertexClient(billing.VertexConfig{
		BaseURL:                 srv.URL,
		TokenSource:             &staticTok{v: "tok"},
		MaxRetries:              1,
		CircuitBreakerThreshold: 3,
		CircuitBreakerCooldown:  50 * time.Millisecond,
	})
	for i := 0; i < 3; i++ {
		_, _ = c.QueryAggregatedCostMicrosForWindow(context.Background(),
			"chora-489812", time.Now(), time.Now().Add(time.Hour))
	}

	// Wait past cooldown.
	time.Sleep(80 * time.Millisecond)

	// Half-open probe should succeed (server is now healthy).
	got, err := c.QueryAggregatedCostMicrosForWindow(context.Background(),
		"chora-489812", time.Now(), time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("after cooldown: %v", err)
	}
	if got != 99 {
		t.Errorf("got = %d; want 99", got)
	}
}
