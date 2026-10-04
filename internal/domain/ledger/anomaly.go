// anomaly.go — statistical 3σ cost anomaly detector.
//
// Per ai-cost-tracking skill ("Statistical anomaly detection"):
//
//	Cloud Monitoring metric: rolling 1h cost per agent vs 7d baseline;
//	alarm at 3σ. On alarm: emit chora.governance.cost_anomaly.detected.v1.
//
// Detector is PURE — given a 7d baseline (mean, stddev) + an hourly
// observation, returns a verdict + canonical event. Side-effects (Pub/Sub
// publish) live in the adapter layer.
//
// Below-baseline anomalies are NOT surfaced — cost dropping is good news.
// Only positive deviations >= 3σ from mean trigger.
package ledger

import (
	"time"
)

// AnomalySeverity is the closed enum of anomaly grades.
type AnomalySeverity string

// AnomalySeverity enum.
const (
	// AnomalySeverityNone — within tolerance.
	AnomalySeverityNone AnomalySeverity = "none"
	// AnomalySeverityWarning — between WarningSigma and CriticalSigma.
	AnomalySeverityWarning AnomalySeverity = "warning"
	// AnomalySeverityCritical — at or above CriticalSigma.
	AnomalySeverityCritical AnomalySeverity = "critical"
)

// CanonicalCostAnomalyTopic is the Pub/Sub topic the adapter publishes on
// when AnomalyResult.IsAnomaly is true.
const CanonicalCostAnomalyTopic = "chora.governance.cost_anomaly.detected.v1"

// AnomalyInput is the detector input.
type AnomalyInput struct {
	AgentID              string
	TenantID             string
	BaselineMeanMicros   int64
	BaselineStdDevMicros int64
	ObservedMicros       int64
	ObservedAt           time.Time
}

// AnomalyResult is the detector output.
type AnomalyResult struct {
	IsAnomaly     bool            `json:"is_anomaly"`
	Severity      AnomalySeverity `json:"severity"`
	SigmaDistance float64         `json:"sigma_distance"`

	// Echo of inputs for traceability:
	AgentID            string `json:"agent_id"`
	TenantID           string `json:"tenant_id"`
	BaselineMeanMicros int64  `json:"baseline_mean_micros"`
	ObservedMicros     int64  `json:"observed_micros"`
}

// AnomalyEvent is the canonical event payload published when IsAnomaly = true.
type AnomalyEvent struct {
	Topic              string          `json:"topic"`
	EventType          string          `json:"event_type"`
	AgentID            string          `json:"agent_id"`
	TenantID           string          `json:"tenant_id"`
	Severity           AnomalySeverity `json:"severity"`
	SigmaDistance      float64         `json:"sigma_distance"`
	BaselineMeanMicros int64           `json:"baseline_mean_micros"`
	ObservedMicros     int64           `json:"observed_micros"`
	ChoraImdaDimension string          `json:"chora_imda_dimension"` // accountability per ADR-141
	ImdaLifecycleStage string          `json:"imda_lifecycle_stage"` // post_deploy
	OccurredAt         time.Time       `json:"occurred_at"`
}

// AnomalyDetector is the configured detector. Defaults are 3σ warning + 6σ
// critical; both are tunable.
type AnomalyDetector struct {
	WarningSigma  float64
	CriticalSigma float64
}

// NewAnomalyDetector constructs a detector with sensible defaults.
func NewAnomalyDetector() *AnomalyDetector {
	return &AnomalyDetector{
		WarningSigma:  3.0,
		CriticalSigma: 6.0,
	}
}

// Detect evaluates the input against the configured thresholds.
//
// Edge cases:
//   - BaselineStdDev = 0 → cannot compute sigma → never fires (no baseline).
//   - Observed <= mean → never fires (cost dropping is not an anomaly).
//   - 0 <= sigma < WarningSigma → IsAnomaly = false.
//   - WarningSigma <= sigma < CriticalSigma → warning.
//   - sigma >= CriticalSigma → critical.
func (d *AnomalyDetector) Detect(in AnomalyInput) AnomalyResult {
	out := AnomalyResult{
		AgentID:            in.AgentID,
		TenantID:           in.TenantID,
		BaselineMeanMicros: in.BaselineMeanMicros,
		ObservedMicros:     in.ObservedMicros,
		Severity:           AnomalySeverityNone,
	}
	if in.BaselineStdDevMicros == 0 {
		return out
	}
	if in.ObservedMicros <= in.BaselineMeanMicros {
		return out
	}
	delta := float64(in.ObservedMicros - in.BaselineMeanMicros)
	sigma := delta / float64(in.BaselineStdDevMicros)
	out.SigmaDistance = sigma

	if sigma >= d.CriticalSigma {
		out.IsAnomaly = true
		out.Severity = AnomalySeverityCritical
		return out
	}
	if sigma >= d.WarningSigma {
		out.IsAnomaly = true
		out.Severity = AnomalySeverityWarning
		return out
	}
	return out
}

// AsEvent returns the canonical AnomalyEvent payload. Caller (adapter) is
// responsible for envelope construction + Pub/Sub publish.
func (r AnomalyResult) AsEvent() AnomalyEvent {
	return AnomalyEvent{
		Topic:              CanonicalCostAnomalyTopic,
		EventType:          "cost_anomaly.detected.v1",
		AgentID:            r.AgentID,
		TenantID:           r.TenantID,
		Severity:           r.Severity,
		SigmaDistance:      r.SigmaDistance,
		BaselineMeanMicros: r.BaselineMeanMicros,
		ObservedMicros:     r.ObservedMicros,
		ChoraImdaDimension: IMDADimensionAccountability,
		ImdaLifecycleStage: "post_deploy",
		OccurredAt:         time.Now().UTC(),
	}
}
