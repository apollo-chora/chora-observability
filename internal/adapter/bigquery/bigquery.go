// Package bigquery is the BigQuery analytics adapter for the daily
// reconciliation harness.
//
// Production: a real google-cloud-go bigquery client runs the SUM query
// against chora_observability_analytics.token_usage_ledger table (declared
// in chora-infra/terraform/modules/bigquery — already provisioned).
//
// Reconciliation query template (production):
//
//	SELECT SUM(cost_usd_micros) AS sum_micros
//	FROM `${project}.chora_observability_analytics.token_usage_ledger`
//	WHERE recorded_at >= @window_start
//	  AND recorded_at <  @window_end
//	  [AND COALESCE(is_eval_run, FALSE) = FALSE  -- when ExcludeEvalRuns]
//
// Test / M10 skeleton: a MockClient seeded with (project, window) → micros.
package bigquery

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Query is the input to SumLedgerCostMicros.
type Query struct {
	ProjectID       string
	WindowStart     time.Time
	WindowEnd       time.Time
	ExcludeEvalRuns bool // production reconciliation always sets true.
}

// Client is the abstract BigQuery analytics contract.
type Client interface {
	SumLedgerCostMicros(ctx context.Context, q Query) (int64, error)
}

// MockClient is the in-memory implementation seeded for tests + M10 skeleton.
//
// Seed key formats:
//
//	"<project_id>@<yyyy-mm-dd>"           — production rows (default)
//	"<project_id>@<yyyy-mm-dd>@is_eval"   — eval-run rows (excluded when ExcludeEvalRuns=true)
type MockClient struct {
	mu     sync.RWMutex
	seed   map[string]int64
	errMsg string
}

// NewMockClient constructs a MockClient with the given seed map.
func NewMockClient(seed map[string]int64) *MockClient {
	if seed == nil {
		seed = make(map[string]int64)
	}
	return &MockClient{seed: seed}
}

// SimulateError configures the next SumLedgerCostMicros call to return an
// error with the given message.
func (m *MockClient) SimulateError(msg string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.errMsg = msg
}

// SumLedgerCostMicros returns the seeded sum honouring ExcludeEvalRuns.
func (m *MockClient) SumLedgerCostMicros(_ context.Context, q Query) (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.errMsg != "" {
		return 0, errors.New(m.errMsg)
	}
	prodKey := fmt.Sprintf("%s@%s", q.ProjectID, q.WindowStart.Format("2006-01-02"))
	evalKey := prodKey + "@is_eval"
	prod := m.seed[prodKey]
	if q.ExcludeEvalRuns {
		return prod, nil
	}
	return prod + m.seed[evalKey], nil
}

// SumLedgerCostMicrosForWindow implements reconcile.BigQueryClient — the
// minimal port the Runner consumes. Wraps SumLedgerCostMicros so the same
// MockClient satisfies both surfaces.
func (m *MockClient) SumLedgerCostMicrosForWindow(ctx context.Context, projectID string, windowStart, windowEnd time.Time, excludeEvalRuns bool) (int64, error) {
	return m.SumLedgerCostMicros(ctx, Query{
		ProjectID:       projectID,
		WindowStart:     windowStart,
		WindowEnd:       windowEnd,
		ExcludeEvalRuns: excludeEvalRuns,
	})
}
