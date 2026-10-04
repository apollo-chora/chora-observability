// Package billing_test covers the reconcile-facing MockClient facade
// (QueryAggregatedCostMicrosForWindow), distinct from the raw
// QueryAggregatedCostMicros surface exercised in billing_test.go.
package billing_test

import (
	"context"
	"testing"
	"time"

	"github.com/5007-Capstone/chora/services/chora-observability/internal/adapter/billing"
)

func TestMockClient_QueryAggregatedCostMicrosForWindow(t *testing.T) {
	t.Parallel()
	mc := billing.NewMockClient(map[string]int64{
		"chora-489812@2026-05-09": 1_234_500,
	})
	ws := time.Date(2026, 5, 9, 0, 0, 0, 0, time.UTC)
	we := ws.AddDate(0, 0, 1)

	got, err := mc.QueryAggregatedCostMicrosForWindow(context.Background(), "chora-489812", ws, we)
	if err != nil {
		t.Fatalf("QueryAggregatedCostMicrosForWindow: %v", err)
	}
	if got != 1_234_500 {
		t.Errorf("sum = %d; want 1234500", got)
	}

	// unset tuple -> 0
	got, err = mc.QueryAggregatedCostMicrosForWindow(context.Background(), "other", ws, we)
	if err != nil {
		t.Fatalf("unset tuple: %v", err)
	}
	if got != 0 {
		t.Errorf("unset sum = %d; want 0", got)
	}

	// simulated downstream failure
	fail := billing.NewMockClient(nil)
	fail.SimulateError("billing API down")
	if _, err := fail.QueryAggregatedCostMicrosForWindow(context.Background(), "chora-489812", ws, we); err == nil {
		t.Fatal("expected simulated error")
	}
}