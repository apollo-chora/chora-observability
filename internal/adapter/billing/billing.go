// Package billing is the Vertex AI Billing API adapter for the daily
// reconciliation harness.
//
// Production: a real google-cloud-go billing client wraps the Vertex
// `cloudbilling.googleapis.com/v1` API + Cloud Billing BigQuery export.
//
// Test / M10 skeleton: a MockClient that returns seeded values keyed by
// (project_id + window_start_iso8601_date). The Cloud Run Job (cmd/reconcile)
// instantiates the production client; integration tests substitute MockClient.
//
// API key handling: NEVER inline. The production client reads from
// secret-manager via env var BILLING_API_KEY (or, preferred: Workload Identity
// Federation impersonating a billing-reader SA so no key file travels). See
// secrets-and-env skill.
package billing

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Query is the input to QueryAggregatedCostMicros.
type Query struct {
	ProjectID   string
	WindowStart time.Time
	WindowEnd   time.Time
}

// Client is the abstract billing-API contract.
type Client interface {
	QueryAggregatedCostMicros(ctx context.Context, q Query) (int64, error)
}

// MockClient is the in-memory implementation seeded for tests + the M10
// skeleton. Map key format: "<project_id>@<window_start_yyyy-mm-dd>".
type MockClient struct {
	mu      sync.RWMutex
	seed    map[string]int64
	errMsg  string
}

// NewMockClient constructs a MockClient with the given seed map.
func NewMockClient(seed map[string]int64) *MockClient {
	if seed == nil {
		seed = make(map[string]int64)
	}
	return &MockClient{seed: seed}
}

// SimulateError configures the next QueryAggregatedCostMicros call to return
// an error with the given message. Used to test the reconciliation harness's
// downstream-failure handling.
func (m *MockClient) SimulateError(msg string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.errMsg = msg
}

// QueryAggregatedCostMicros returns the seeded value or 0 when the (project,
// window) tuple isn't in the seed map.
func (m *MockClient) QueryAggregatedCostMicros(_ context.Context, q Query) (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.errMsg != "" {
		return 0, errors.New(m.errMsg)
	}
	key := fmt.Sprintf("%s@%s", q.ProjectID, q.WindowStart.Format("2006-01-02"))
	return m.seed[key], nil
}

// QueryAggregatedCostMicrosForWindow implements reconcile.BillingClient —
// the minimal port the Runner consumes. Wraps QueryAggregatedCostMicros.
func (m *MockClient) QueryAggregatedCostMicrosForWindow(ctx context.Context, projectID string, windowStart, windowEnd time.Time) (int64, error) {
	return m.QueryAggregatedCostMicros(ctx, Query{
		ProjectID:   projectID,
		WindowStart: windowStart,
		WindowEnd:   windowEnd,
	})
}
