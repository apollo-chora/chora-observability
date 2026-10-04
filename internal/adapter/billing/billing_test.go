// Package billing_test exercises the Vertex Billing API client adapter
// (mock implementation only — real Vertex Billing API call lives behind
// google-cloud-go billing client and is exercised in integration tests).
package billing_test

import (
	"context"
	"testing"
	"time"

	"github.com/apollo-chora/chora-observability/internal/adapter/billing"
)

func TestMockClient_Returns_SeededValue(t *testing.T) {
	t.Parallel()
	mc := billing.NewMockClient(map[string]int64{
		"chora-489812@2026-05-09": 1_000_000,
	})
	ws := time.Date(2026, 5, 9, 0, 0, 0, 0, time.UTC)
	we := ws.AddDate(0, 0, 1)
	got, err := mc.QueryAggregatedCostMicros(context.Background(), billing.Query{
		ProjectID:   "chora-489812",
		WindowStart: ws,
		WindowEnd:   we,
	})
	if err != nil {
		t.Fatalf("QueryAggregatedCostMicros: %v", err)
	}
	if got != 1_000_000 {
		t.Errorf("got = %d; want 1_000_000", got)
	}
}

func TestMockClient_ReturnsZeroForUnknownProject(t *testing.T) {
	t.Parallel()
	mc := billing.NewMockClient(nil)
	got, err := mc.QueryAggregatedCostMicros(context.Background(), billing.Query{
		ProjectID:   "chora-content",
		WindowStart: time.Now(),
		WindowEnd:   time.Now().Add(24 * time.Hour),
	})
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if got != 0 {
		t.Errorf("unknown should return 0; got %d", got)
	}
}

func TestMockClient_ConfigurableError(t *testing.T) {
	t.Parallel()
	mc := billing.NewMockClient(nil)
	mc.SimulateError("simulated downstream failure")
	_, err := mc.QueryAggregatedCostMicros(context.Background(), billing.Query{
		ProjectID:   "chora-489812",
		WindowStart: time.Now(),
		WindowEnd:   time.Now().Add(24 * time.Hour),
	})
	if err == nil {
		t.Errorf("expected simulated error")
	}
}
