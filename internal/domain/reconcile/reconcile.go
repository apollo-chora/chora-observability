// Package reconcile is the daily payment-reconciliation domain.
//
// Per ai-cost-tracking skill ("Reconciliation harness"):
//
//	BigQuery SUM(token_usage_ledger.cost_usd_micros)  vs
//	Vertex Billing API aggregated_cost (per-day, per-project)
//
//	tolerance: ±0.01% (1 part per 10,000)
//
// Drift exceeding the tolerance → publish
// chora.governance.payment_reconciliation.anomaly.v1 (per S0 reconciliation
// canonical topic mapping).
//
// Check is PURE: pass in the two int64 micros figures (BigQuery sum + Vertex
// billing total) and the tolerance fraction, get a deterministic verdict.
// The Cloud Run Job adapter (cmd/reconcile) is responsible for fetching the
// figures + emitting the event.
package reconcile

import (
	"math"
	"time"
)

// CanonicalReconcileAnomalyTopic is the Pub/Sub topic the adapter publishes
// when Verdict.IsAnomaly is true.
const CanonicalReconcileAnomalyTopic = "chora.governance.payment_reconciliation.anomaly.v1"

// CanonicalReconcileDegradedTopic is the Pub/Sub topic the adapter publishes
// when the Vertex Billing API circuit breaker is OPEN (persistent
// outage). Used by Cloud Monitoring alerting + on-call routing per the
// resilience directive ("on 5+ consecutive failures, emit
// chora.ai_kernel.cost-reconciliation-degraded.v1").
//
// Topic naming: `chora.{domain}.{aggregate}.{event_type}.v{N}` per
// pub-sub-topology. The aggregate is the reconciliation pipeline and
// the event type signals SERVICE health (NOT a billing anomaly), so
// it is owned by Observability (chora_observability DB).
const CanonicalReconcileDegradedTopic = "chora.observability.payment_reconciliation.degraded.v1"

// DefaultToleranceFraction = 0.0001 (0.01% = 1 part per 10,000) per
// ai-cost-tracking skill.
const DefaultToleranceFraction = 0.0001

// IMDADimensionAccountability is the ADR-141 canonical label — payment
// reconciliation is D1 evidence.
const IMDADimensionAccountability = "accountability"

// CheckInput is the reconciliation domain input.
type CheckInput struct {
	// LedgerSumMicros is SUM(token_usage_ledger.cost_usd_micros) over the
	// window from BigQuery (chora_observability_analytics.token_usage_ledger
	// table — populated via Pub/Sub→BQ subscription).
	LedgerSumMicros int64

	// VertexBillingMicros is the Vertex AI billing total for the same window
	// from the Vertex Billing API (in cost_usd_micros — int64 to avoid float
	// drift in billing-grade comparisons).
	VertexBillingMicros int64

	// ToleranceFraction is the ±drift threshold (e.g. 0.0001 = 0.01%). Zero
	// uses DefaultToleranceFraction.
	ToleranceFraction float64

	// WindowStart + WindowEnd are echoed onto the event for traceability.
	WindowStart time.Time
	WindowEnd   time.Time
}

// Verdict is the reconciliation domain output.
type Verdict struct {
	// IsAnomaly = true when |drift| > tolerance OR negative inputs.
	IsAnomaly bool `json:"is_anomaly"`

	// DriftFraction = (vertex - ledger) / max(vertex, ledger). Signed.
	DriftFraction float64 `json:"drift_fraction"`

	// DriftMicros = vertex - ledger (signed).
	DriftMicros int64 `json:"drift_micros"`

	// Echo of inputs for traceability.
	LedgerSumMicros     int64     `json:"ledger_sum_micros"`
	VertexBillingMicros int64     `json:"vertex_billing_micros"`
	WindowStart         time.Time `json:"window_start"`
	WindowEnd           time.Time `json:"window_end"`
}

// AnomalyEvent is the canonical event shape published when IsAnomaly = true.
type AnomalyEvent struct {
	Topic               string    `json:"topic"`
	EventType           string    `json:"event_type"`
	LedgerSumMicros     int64     `json:"ledger_sum_micros"`
	VertexBillingMicros int64     `json:"vertex_billing_micros"`
	DriftFraction       float64   `json:"drift_fraction"`
	DriftMicros         int64     `json:"drift_micros"`
	WindowStart         time.Time `json:"window_start"`
	WindowEnd           time.Time `json:"window_end"`
	ChoraImdaDimension  string    `json:"chora_imda_dimension"`
	ImdaLifecycleStage  string    `json:"imda_lifecycle_stage"`
	OccurredAt          time.Time `json:"occurred_at"`
}

// DegradedEvent is the canonical event shape published when the
// reconciliation pipeline cannot reach upstream (Vertex Billing API
// circuit breaker open, BigQuery quota, etc.). Per the resilience
// directive, this drives Cloud Monitoring alerting on persistent
// outages BEFORE silent drift accumulates.
//
// Distinction from AnomalyEvent:
//   - AnomalyEvent  = "we DID reconcile and the numbers diverge".
//   - DegradedEvent = "we COULD NOT reconcile because upstream is down".
type DegradedEvent struct {
	Topic              string    `json:"topic"`
	EventType          string    `json:"event_type"`
	Reason             string    `json:"reason"`
	UpstreamComponent  string    `json:"upstream_component"`
	WindowStart        time.Time `json:"window_start"`
	WindowEnd          time.Time `json:"window_end"`
	ChoraImdaDimension string    `json:"chora_imda_dimension"`
	ImdaLifecycleStage string    `json:"imda_lifecycle_stage"`
	OccurredAt         time.Time `json:"occurred_at"`
}

// Check runs the reconciliation and returns the deterministic verdict.
//
// Edge cases:
//   - Both = 0 → no traffic → not an anomaly.
//   - Negative input → invalid → ALWAYS an anomaly.
//   - Ledger = 0 + Vertex > 0 → ALWAYS an anomaly (we missed ledger writes).
//   - |drift| <= tolerance → not an anomaly.
//   - |drift|  > tolerance → anomaly.
func Check(in CheckInput) Verdict {
	tol := in.ToleranceFraction
	if tol <= 0 {
		tol = DefaultToleranceFraction
	}

	v := Verdict{
		LedgerSumMicros:     in.LedgerSumMicros,
		VertexBillingMicros: in.VertexBillingMicros,
		WindowStart:         in.WindowStart,
		WindowEnd:           in.WindowEnd,
	}

	// Negative inputs → invalid → anomaly.
	if in.LedgerSumMicros < 0 || in.VertexBillingMicros < 0 {
		v.IsAnomaly = true
		return v
	}

	// Both zero → no traffic — not an anomaly.
	if in.LedgerSumMicros == 0 && in.VertexBillingMicros == 0 {
		return v
	}

	v.DriftMicros = in.VertexBillingMicros - in.LedgerSumMicros
	denom := in.VertexBillingMicros
	if in.LedgerSumMicros > denom {
		denom = in.LedgerSumMicros
	}
	if denom == 0 {
		// One side is zero — can't normalize; treat as anomaly to surface.
		v.IsAnomaly = true
		return v
	}
	v.DriftFraction = float64(v.DriftMicros) / float64(denom)
	if math.Abs(v.DriftFraction) > tol {
		v.IsAnomaly = true
	}
	return v
}

// AsEvent returns the canonical AnomalyEvent payload. Caller is responsible
// for envelope construction + Pub/Sub publish.
func (v Verdict) AsEvent() AnomalyEvent {
	return AnomalyEvent{
		Topic:               CanonicalReconcileAnomalyTopic,
		EventType:           "payment_reconciliation.anomaly.v1",
		LedgerSumMicros:     v.LedgerSumMicros,
		VertexBillingMicros: v.VertexBillingMicros,
		DriftFraction:       v.DriftFraction,
		DriftMicros:         v.DriftMicros,
		WindowStart:         v.WindowStart,
		WindowEnd:           v.WindowEnd,
		ChoraImdaDimension:  IMDADimensionAccountability,
		ImdaLifecycleStage:  "post_deploy",
		OccurredAt:          time.Now().UTC(),
	}
}
