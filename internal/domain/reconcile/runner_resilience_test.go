// runner_resilience_test.go — RED-phase TDD specs for the
// reconciliation Runner's degraded-pipeline emission per the
// resilience directive. The Runner must surface a DegradedEvent to
// Cloud Monitoring when upstream (BigQuery / Vertex Billing) is
// unreachable, NOT silently swallow the error.
package reconcile_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/apollo-chora/chora-observability/internal/domain/reconcile"
)

type stubBQErr struct{}

func (stubBQErr) SumLedgerCostMicrosForWindow(_ context.Context, _ string, _, _ time.Time, _ bool) (int64, error) {
	return 0, errors.New("bigquery quota exhausted")
}

type stubBillingErr struct{}

func (stubBillingErr) QueryAggregatedCostMicrosForWindow(_ context.Context, _ string, _, _ time.Time) (int64, error) {
	return 0, errors.New("billing.vertex: circuit breaker open")
}

type stubBQOK struct{}

func (stubBQOK) SumLedgerCostMicrosForWindow(_ context.Context, _ string, _, _ time.Time, _ bool) (int64, error) {
	return 1_000_000, nil
}

type stubBillingOK struct{}

func (stubBillingOK) QueryAggregatedCostMicrosForWindow(_ context.Context, _ string, _, _ time.Time) (int64, error) {
	return 1_000_000, nil
}

type stubAnomalySink struct{ emitted []reconcile.AnomalyEvent }

func (s *stubAnomalySink) Emit(_ context.Context, ev reconcile.AnomalyEvent) error {
	s.emitted = append(s.emitted, ev)
	return nil
}

type stubDegradedSink struct{ emitted []reconcile.DegradedEvent }

func (s *stubDegradedSink) EmitDegraded(_ context.Context, ev reconcile.DegradedEvent) error {
	s.emitted = append(s.emitted, ev)
	return nil
}

func TestRunner_EmitsDegradedOnBigQueryFailure(t *testing.T) {
	t.Parallel()
	deg := &stubDegradedSink{}
	runner := reconcile.NewRunner(reconcile.RunnerConfig{
		ProjectID:      "chora-489812",
		BigQueryClient: stubBQErr{},
		BillingClient:  stubBillingOK{}, // never reached
		EventSink:      &stubAnomalySink{},
		DegradedSink:   deg,
	})
	_, err := runner.Run(context.Background(),
		time.Date(2026, 5, 9, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 5, 10, 0, 0, 0, 0, time.UTC))
	if err == nil {
		t.Fatal("expected bigquery error to propagate")
	}
	if len(deg.emitted) != 1 {
		t.Fatalf("expected 1 degraded event; got %d", len(deg.emitted))
	}
	got := deg.emitted[0]
	if got.UpstreamComponent != "bigquery" {
		t.Errorf("upstream_component = %s; want bigquery", got.UpstreamComponent)
	}
	if got.Topic != reconcile.CanonicalReconcileDegradedTopic {
		t.Errorf("topic = %s; want canonical degraded", got.Topic)
	}
}

func TestRunner_EmitsDegradedOnBillingFailure(t *testing.T) {
	t.Parallel()
	deg := &stubDegradedSink{}
	runner := reconcile.NewRunner(reconcile.RunnerConfig{
		ProjectID:      "chora-489812",
		BigQueryClient: stubBQOK{},
		BillingClient:  stubBillingErr{},
		EventSink:      &stubAnomalySink{},
		DegradedSink:   deg,
	})
	_, err := runner.Run(context.Background(),
		time.Date(2026, 5, 9, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 5, 10, 0, 0, 0, 0, time.UTC))
	if err == nil {
		t.Fatal("expected billing error to propagate")
	}
	if len(deg.emitted) != 1 {
		t.Fatalf("expected 1 degraded event; got %d", len(deg.emitted))
	}
	got := deg.emitted[0]
	if got.UpstreamComponent != "vertex_billing" {
		t.Errorf("upstream_component = %s; want vertex_billing", got.UpstreamComponent)
	}
}

func TestRunner_DegradedSinkOptional_DoesNotPanic(t *testing.T) {
	t.Parallel()
	// Without DegradedSink, the Runner must STILL surface the upstream
	// error — it just doesn't emit the degraded event.
	runner := reconcile.NewRunner(reconcile.RunnerConfig{
		ProjectID:      "chora-489812",
		BigQueryClient: stubBQErr{},
		BillingClient:  stubBillingErr{},
		EventSink:      &stubAnomalySink{},
		// DegradedSink: nil — explicitly omitted.
	})
	_, err := runner.Run(context.Background(), time.Now(), time.Now().Add(time.Hour))
	if err == nil {
		t.Fatalf("expected bigquery error")
	}
}
