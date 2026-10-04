// Package reconcile_test exercises the Runner which orchestrates the daily
// reconciliation: BQ query → Vertex billing query → Check → emit anomaly
// event when drift exceeds tolerance.
package reconcile_test

import (
	"context"
	"testing"
	"time"

	"github.com/5007-Capstone/chora/services/chora-observability/internal/adapter/bigquery"
	"github.com/5007-Capstone/chora/services/chora-observability/internal/adapter/billing"
	"github.com/5007-Capstone/chora/services/chora-observability/internal/domain/reconcile"
)

// stubEventSink records emitted events.
type stubEventSink struct {
	emitted []reconcile.AnomalyEvent
	err     error
}

func (s *stubEventSink) Emit(_ context.Context, ev reconcile.AnomalyEvent) error {
	if s.err != nil {
		return s.err
	}
	s.emitted = append(s.emitted, ev)
	return nil
}

func TestRunner_NoDrift_NoEvent(t *testing.T) {
	t.Parallel()
	ws := time.Date(2026, 5, 9, 0, 0, 0, 0, time.UTC)
	bq := bigquery.NewMockClient(map[string]int64{"chora-489812@2026-05-09": 1_000_000})
	bl := billing.NewMockClient(map[string]int64{"chora-489812@2026-05-09": 1_000_000})
	sink := &stubEventSink{}
	r := reconcile.NewRunner(reconcile.RunnerConfig{
		ProjectID:         "chora-489812",
		BigQueryClient:    bq,
		BillingClient:     bl,
		EventSink:         sink,
		ToleranceFraction: 0.0001,
	})
	v, err := r.Run(context.Background(), ws, ws.AddDate(0, 0, 1))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if v.IsAnomaly {
		t.Errorf("expected no anomaly")
	}
	if len(sink.emitted) != 0 {
		t.Errorf("emitted = %d; want 0", len(sink.emitted))
	}
}

func TestRunner_DriftExceedsTolerance_EmitsEvent(t *testing.T) {
	t.Parallel()
	ws := time.Date(2026, 5, 9, 0, 0, 0, 0, time.UTC)
	bq := bigquery.NewMockClient(map[string]int64{"chora-489812@2026-05-09": 1_000_000})
	bl := billing.NewMockClient(map[string]int64{"chora-489812@2026-05-09": 1_000_500}) // 0.05%
	sink := &stubEventSink{}
	r := reconcile.NewRunner(reconcile.RunnerConfig{
		ProjectID:         "chora-489812",
		BigQueryClient:    bq,
		BillingClient:     bl,
		EventSink:         sink,
		ToleranceFraction: 0.0001, // 0.01%
	})
	v, err := r.Run(context.Background(), ws, ws.AddDate(0, 0, 1))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !v.IsAnomaly {
		t.Errorf("expected anomaly")
	}
	if len(sink.emitted) != 1 {
		t.Errorf("emitted = %d; want 1", len(sink.emitted))
	}
	if sink.emitted[0].Topic != "chora.governance.payment_reconciliation.anomaly.v1" {
		t.Errorf("topic = %s", sink.emitted[0].Topic)
	}
}

func TestRunner_BQError_Surfaced(t *testing.T) {
	t.Parallel()
	bq := bigquery.NewMockClient(nil)
	bq.SimulateError("BQ unavailable")
	bl := billing.NewMockClient(nil)
	r := reconcile.NewRunner(reconcile.RunnerConfig{
		ProjectID:      "chora-489812",
		BigQueryClient: bq, BillingClient: bl, EventSink: &stubEventSink{},
	})
	_, err := r.Run(context.Background(), time.Now(), time.Now().Add(24*time.Hour))
	if err == nil {
		t.Errorf("expected BQ error")
	}
}

func TestRunner_BillingError_Surfaced(t *testing.T) {
	t.Parallel()
	bq := bigquery.NewMockClient(nil)
	bl := billing.NewMockClient(nil)
	bl.SimulateError("billing unavailable")
	r := reconcile.NewRunner(reconcile.RunnerConfig{
		ProjectID:      "chora-489812",
		BigQueryClient: bq, BillingClient: bl, EventSink: &stubEventSink{},
	})
	_, err := r.Run(context.Background(), time.Now(), time.Now().Add(24*time.Hour))
	if err == nil {
		t.Errorf("expected billing error")
	}
}

func TestRunner_RejectsMissingDeps(t *testing.T) {
	t.Parallel()
	r := reconcile.NewRunner(reconcile.RunnerConfig{ProjectID: "p"})
	_, err := r.Run(context.Background(), time.Now(), time.Now().Add(24*time.Hour))
	if err == nil {
		t.Errorf("expected missing-deps error")
	}
}
