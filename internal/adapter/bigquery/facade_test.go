// Package bigquery_test covers the reconcile-facing MockClient facade
// (SumLedgerCostMicrosForWindow) — the minimal port the reconcile Runner
// consumes, distinct from the raw SumLedgerCostMicros surface.
package bigquery_test

import (
	"context"
	"testing"
	"time"

	"github.com/5007-Capstone/chora/services/chora-observability/internal/adapter/bigquery"
)

func TestMockClient_SumLedgerCostMicrosForWindow(t *testing.T) {
	t.Parallel()
	mc := bigquery.NewMockClient(map[string]int64{
		"chora-489812@2026-05-09":         400_000,
		"chora-489812@2026-05-09@is_eval": 60_000,
	})
	ws := time.Date(2026, 5, 9, 0, 0, 0, 0, time.UTC)
	we := ws.AddDate(0, 0, 1)

	// eval runs included
	got, err := mc.SumLedgerCostMicrosForWindow(context.Background(), "chora-489812", ws, we, false)
	if err != nil {
		t.Fatalf("SumLedgerCostMicrosForWindow: %v", err)
	}
	if got != 460_000 {
		t.Errorf("sum = %d; want 460_000 (eval included)", got)
	}

	// eval runs excluded
	got, err = mc.SumLedgerCostMicrosForWindow(context.Background(), "chora-489812", ws, we, true)
	if err != nil {
		t.Fatalf("SumLedgerCostMicrosForWindow(exclude): %v", err)
	}
	if got != 400_000 {
		t.Errorf("sum = %d; want 400_000 (eval excluded)", got)
	}
}