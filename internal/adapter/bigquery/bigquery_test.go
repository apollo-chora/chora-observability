// Package bigquery_test exercises the BigQuery ledger-sum adapter.
//
// Production: a real google-cloud-go bigquery client runs the SUM query
// against the chora_observability_analytics.token_usage_ledger table.
//
// Test / M10 skeleton: a MockClient seeded with (project, window) → micros.
package bigquery_test

import (
	"context"
	"testing"
	"time"

	"github.com/apollo-chora/chora-observability/internal/adapter/bigquery"
)

func TestMockClient_Returns_SeededValue(t *testing.T) {
	t.Parallel()
	mc := bigquery.NewMockClient(map[string]int64{
		"chora-489812@2026-05-09": 999_950,
	})
	ws := time.Date(2026, 5, 9, 0, 0, 0, 0, time.UTC)
	we := ws.AddDate(0, 0, 1)
	got, err := mc.SumLedgerCostMicros(context.Background(), bigquery.Query{
		ProjectID:   "chora-489812",
		WindowStart: ws,
		WindowEnd:   we,
	})
	if err != nil {
		t.Fatalf("SumLedgerCostMicros: %v", err)
	}
	if got != 999_950 {
		t.Errorf("got = %d; want 999_950", got)
	}
}

func TestMockClient_FilteredByEvalRun(t *testing.T) {
	t.Parallel()
	// Eval-runs are excluded from the production reconciliation query (per
	// pricing.yaml: cost_center='eval' rows don't bill against tenant).
	// MockClient honours the IsEvalRun flag.
	mc := bigquery.NewMockClient(map[string]int64{
		"chora-489812@2026-05-09":         500_000,
		"chora-489812@2026-05-09@is_eval": 100_000,
	})
	ws := time.Date(2026, 5, 9, 0, 0, 0, 0, time.UTC)
	we := ws.AddDate(0, 0, 1)
	got, _ := mc.SumLedgerCostMicros(context.Background(), bigquery.Query{
		ProjectID:       "chora-489812",
		WindowStart:     ws,
		WindowEnd:       we,
		ExcludeEvalRuns: true,
	})
	if got != 500_000 {
		t.Errorf("eval-excluded sum = %d; want 500_000 (production rows only)", got)
	}
}

func TestMockClient_SimulateError(t *testing.T) {
	t.Parallel()
	mc := bigquery.NewMockClient(nil)
	mc.SimulateError("BQ down")
	_, err := mc.SumLedgerCostMicros(context.Background(), bigquery.Query{
		ProjectID:   "chora-489812",
		WindowStart: time.Now(),
		WindowEnd:   time.Now().Add(24 * time.Hour),
	})
	if err == nil {
		t.Errorf("expected simulated error")
	}
}
