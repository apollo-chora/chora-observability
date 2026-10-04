// runner.go — Runner orchestrates the daily reconciliation:
//
//  1. Pull BigQuery SUM(token_usage_ledger.cost_usd_micros) for the window.
//  2. Pull Vertex Billing API aggregated cost for the same window.
//  3. Run reconcile.Check (pure domain logic).
//  4. If IsAnomaly: emit chora.governance.payment_reconciliation.anomaly.v1
//     via EventSink (Pub/Sub publisher in production; recording stub in
//     tests).
//
// The Runner is the bridge between the pure domain (Check) and the cmd/
// reconcile Cloud Run Job entrypoint. It is itself testable via mocked
// adapters (see runner_test.go).
package reconcile

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// BigQueryClient is the read port for the ledger sum. The Runner only
// needs (project, window) → micros — adapters that have richer query
// surfaces (e.g. eval-run filtering) wrap their richer call into this
// minimal contract.
type BigQueryClient interface {
	SumLedgerCostMicrosForWindow(ctx context.Context, projectID string, windowStart, windowEnd time.Time, excludeEvalRuns bool) (int64, error)
}

// BillingClient is the read port for the Vertex billing aggregate.
type BillingClient interface {
	QueryAggregatedCostMicrosForWindow(ctx context.Context, projectID string, windowStart, windowEnd time.Time) (int64, error)
}

// EventSink is the write port for the anomaly event.
type EventSink interface {
	Emit(ctx context.Context, ev AnomalyEvent) error
}

// DegradedSink is an OPTIONAL write port for the degraded-pipeline
// event. When upstream BigQuery / Vertex Billing is unreachable
// (circuit breaker open, repeated 5xx) the Runner emits a
// DegradedEvent so Cloud Monitoring can alert before silent drift
// accumulates. When unset (production wiring runtime path), the
// Runner falls back to the canonical EventSink with a synthesised
// AnomalyEvent shape.
type DegradedSink interface {
	EmitDegraded(ctx context.Context, ev DegradedEvent) error
}

// RunnerConfig captures the Runner's dependencies + tunables.
type RunnerConfig struct {
	ProjectID         string         // GCP project (e.g. chora-489812)
	BigQueryClient    BigQueryClient // SUM source
	BillingClient     BillingClient  // truth source
	EventSink         EventSink      // anomaly publisher
	DegradedSink      DegradedSink   // OPTIONAL — degraded-pipeline publisher
	ToleranceFraction float64        // ±drift threshold; 0 = default 0.0001
}

// Runner is the reconciliation orchestrator.
type Runner struct {
	cfg RunnerConfig
}

// NewRunner constructs a Runner.
func NewRunner(cfg RunnerConfig) *Runner {
	return &Runner{cfg: cfg}
}

// Run performs one reconciliation pass for the given window.
//
// Returns the Verdict for traceability (caller may log / surface to
// dashboards). Errors from BQ / Billing / Sink are surfaced; the caller
// (Cloud Run Job) decides whether to retry (Cloud Run handles this via
// max_retries).
func (r *Runner) Run(ctx context.Context, windowStart, windowEnd time.Time) (Verdict, error) {
	if r == nil || r.cfg.BigQueryClient == nil || r.cfg.BillingClient == nil || r.cfg.EventSink == nil {
		return Verdict{}, errors.New("reconcile.Runner: missing deps (bq, billing, sink)")
	}

	ledgerSum, err := r.cfg.BigQueryClient.SumLedgerCostMicrosForWindow(
		ctx, r.cfg.ProjectID, windowStart, windowEnd, true /* excludeEvalRuns — production always */)
	if err != nil {
		// Upstream BQ failure → emit DegradedEvent so Cloud Monitoring
		// alerting fires before silent drift accumulates.
		r.emitDegraded(ctx, "bigquery", err.Error(), windowStart, windowEnd)
		return Verdict{}, fmt.Errorf("bq sum: %w", err)
	}

	vertexBill, err := r.cfg.BillingClient.QueryAggregatedCostMicrosForWindow(
		ctx, r.cfg.ProjectID, windowStart, windowEnd)
	if err != nil {
		r.emitDegraded(ctx, "vertex_billing", err.Error(), windowStart, windowEnd)
		return Verdict{}, fmt.Errorf("billing: %w", err)
	}

	v := Check(CheckInput{
		LedgerSumMicros:     ledgerSum,
		VertexBillingMicros: vertexBill,
		ToleranceFraction:   r.cfg.ToleranceFraction,
		WindowStart:         windowStart,
		WindowEnd:           windowEnd,
	})

	if v.IsAnomaly {
		ev := v.AsEvent()
		if err := r.cfg.EventSink.Emit(ctx, ev); err != nil {
			return v, fmt.Errorf("emit anomaly: %w", err)
		}
	}
	return v, nil
}

// emitDegraded is the best-effort fan-out when the pipeline cannot
// reach upstream. Logs but does not error — the caller already has
// the original error to propagate; we only want to make sure alert
// signal escapes.
func (r *Runner) emitDegraded(ctx context.Context, component, reason string, windowStart, windowEnd time.Time) {
	if r.cfg.DegradedSink == nil {
		return
	}
	_ = r.cfg.DegradedSink.EmitDegraded(ctx, DegradedEvent{
		Topic:              CanonicalReconcileDegradedTopic,
		EventType:          "payment_reconciliation.degraded.v1",
		Reason:             reason,
		UpstreamComponent:  component,
		WindowStart:        windowStart,
		WindowEnd:          windowEnd,
		ChoraImdaDimension: IMDADimensionAccountability,
		ImdaLifecycleStage: "post_deploy",
		OccurredAt:         time.Now().UTC(),
	})
}
