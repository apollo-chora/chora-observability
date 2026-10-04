// Package reconcile_test exercises the daily payment-reconciliation harness:
//
//	BigQuery SUM(token_usage_ledger.cost_usd_micros)  vs
//	Vertex Billing API aggregated_cost (per-day, per-project)
//
//	tolerance: ±0.01% (1 part per 10,000)
//
// Drift exceeding the tolerance → publishes
//
//	chora.governance.payment_reconciliation.anomaly.v1
//
// Per ai-cost-tracking skill ("nightly reconciliation … ±0.01% tolerance").
//
// The Reconciler is PURE: given two int64 micros figures and the tolerance,
// returns a deterministic ReconciliationVerdict. The Cloud Run Job adapter
// pulls the figures from BigQuery + Vertex Billing API and emits the event.
package reconcile_test

import (
	"testing"
	"time"

	"github.com/5007-Capstone/chora/services/chora-observability/internal/domain/reconcile"
)

func TestReconcile_NoDrift_NoAnomaly(t *testing.T) {
	t.Parallel()
	v := reconcile.Check(reconcile.CheckInput{
		LedgerSumMicros:        1_000_000,
		VertexBillingMicros:    1_000_000,
		ToleranceFraction:      0.0001, // 0.01%
		WindowStart:            time.Date(2026, 5, 9, 0, 0, 0, 0, time.UTC),
		WindowEnd:              time.Date(2026, 5, 10, 0, 0, 0, 0, time.UTC),
	})
	if v.IsAnomaly {
		t.Errorf("zero drift should not fire; got %+v", v)
	}
	if v.DriftFraction != 0.0 {
		t.Errorf("drift = %v; want 0.0", v.DriftFraction)
	}
}

func TestReconcile_WithinTolerance_NoAnomaly(t *testing.T) {
	t.Parallel()
	// 50 micros drift on 1M = 0.005% — within 0.01%.
	v := reconcile.Check(reconcile.CheckInput{
		LedgerSumMicros:     1_000_000,
		VertexBillingMicros: 1_000_050,
		ToleranceFraction:   0.0001,
	})
	if v.IsAnomaly {
		t.Errorf("0.005%% drift should NOT fire (under 0.01%%); got %+v", v)
	}
}

func TestReconcile_ExceedsTolerance_FiresAnomaly(t *testing.T) {
	t.Parallel()
	// 200 micros drift on 1M = 0.02% — exceeds 0.01%.
	v := reconcile.Check(reconcile.CheckInput{
		LedgerSumMicros:     1_000_000,
		VertexBillingMicros: 1_000_200,
		ToleranceFraction:   0.0001,
	})
	if !v.IsAnomaly {
		t.Errorf("0.02%% drift should fire; got %+v", v)
	}
	if v.DriftMicros != 200 {
		t.Errorf("drift_micros = %d; want 200", v.DriftMicros)
	}
}

func TestReconcile_NegativeDrift_AbsoluteValueUsed(t *testing.T) {
	t.Parallel()
	// Ledger > Vertex (we recorded more than we were billed) — still drift.
	v := reconcile.Check(reconcile.CheckInput{
		LedgerSumMicros:     1_001_000,
		VertexBillingMicros: 1_000_000,
		ToleranceFraction:   0.0001,
	})
	if !v.IsAnomaly {
		t.Errorf("0.1%% drift in either direction should fire; got %+v", v)
	}
	if v.DriftMicros != 1_000 && v.DriftMicros != -1_000 {
		t.Errorf("drift_micros = %d; want ±1000", v.DriftMicros)
	}
}

func TestReconcile_ZeroDenominator_NoAnomaly(t *testing.T) {
	t.Parallel()
	// No ledger sum + no Vertex bill = no traffic = no anomaly.
	v := reconcile.Check(reconcile.CheckInput{
		LedgerSumMicros:     0,
		VertexBillingMicros: 0,
		ToleranceFraction:   0.0001,
	})
	if v.IsAnomaly {
		t.Errorf("0/0 should not fire; got %+v", v)
	}
}

func TestReconcile_ZeroLedgerWithVertexBilling_FiresAnomaly(t *testing.T) {
	t.Parallel()
	// We were billed but our ledger is empty — biggest drift possible.
	v := reconcile.Check(reconcile.CheckInput{
		LedgerSumMicros:     0,
		VertexBillingMicros: 1_000_000,
		ToleranceFraction:   0.0001,
	})
	if !v.IsAnomaly {
		t.Errorf("ledger missing entire bill must fire; got %+v", v)
	}
}

func TestReconcile_DefaultToleranceWhenZero(t *testing.T) {
	t.Parallel()
	// ToleranceFraction = 0 → use default 0.0001 (0.01%).
	v := reconcile.Check(reconcile.CheckInput{
		LedgerSumMicros:     1_000_000,
		VertexBillingMicros: 1_000_050, // 0.005%
	})
	if v.IsAnomaly {
		t.Errorf("default tolerance 0.01%% should not fire on 0.005%%; got %+v", v)
	}
}

func TestReconcile_AsEvent_CanonicalShape(t *testing.T) {
	t.Parallel()
	v := reconcile.Check(reconcile.CheckInput{
		LedgerSumMicros:     1_000_000,
		VertexBillingMicros: 1_000_500,
		ToleranceFraction:   0.0001,
		WindowStart:         time.Date(2026, 5, 9, 0, 0, 0, 0, time.UTC),
		WindowEnd:           time.Date(2026, 5, 10, 0, 0, 0, 0, time.UTC),
	})
	if !v.IsAnomaly {
		t.Fatalf("expected anomaly")
	}
	ev := v.AsEvent()
	if ev.Topic != "chora.governance.payment_reconciliation.anomaly.v1" {
		t.Errorf("topic = %s; want canonical", ev.Topic)
	}
	if ev.ChoraImdaDimension != "accountability" {
		t.Errorf("imda_dimension = %s; want accountability", ev.ChoraImdaDimension)
	}
}

func TestReconcile_NegativeInputsRejected(t *testing.T) {
	t.Parallel()
	// Negative micros are illegal — IsAnomaly = true with InvalidInput reason.
	v := reconcile.Check(reconcile.CheckInput{
		LedgerSumMicros:     -1,
		VertexBillingMicros: 0,
		ToleranceFraction:   0.0001,
	})
	if !v.IsAnomaly {
		t.Errorf("negative input should fire as anomaly")
	}
}
