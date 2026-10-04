// Package ledger_test exercises the cost AnomalyDetector — statistical
// rolling-window detector that fires chora.governance.cost_anomaly.detected.v1
// when an agent's hourly cost diverges by 3σ from its 7-day baseline.
//
// Per ai-cost-tracking skill: "Statistical anomaly detection — Cloud
// Monitoring metric: rolling 1h cost per agent vs 7d baseline; alarm at 3σ".
//
// Detector is PURE — given a 7-day baseline (mean, stddev) and the current
// observation, returns Detect{is_anomaly, sigma_distance, severity}.
package ledger_test

import (
	"testing"

	"github.com/5007-Capstone/chora/services/chora-observability/internal/domain/ledger"
)

func TestAnomalyDetector_NoBaseline_NoAlert(t *testing.T) {
	t.Parallel()
	// Baseline=0 (no history yet) → never fires.
	det := ledger.NewAnomalyDetector()
	out := det.Detect(ledger.AnomalyInput{
		AgentID:           "agent-x",
		BaselineMeanMicros: 0,
		BaselineStdDevMicros: 0,
		ObservedMicros:    1_000_000,
	})
	if out.IsAnomaly {
		t.Errorf("zero baseline should never fire; got %+v", out)
	}
}

func TestAnomalyDetector_Within2Sigma_NoAlert(t *testing.T) {
	t.Parallel()
	det := ledger.NewAnomalyDetector()
	out := det.Detect(ledger.AnomalyInput{
		AgentID:              "agent-x",
		BaselineMeanMicros:   100_000,
		BaselineStdDevMicros: 10_000,
		ObservedMicros:       115_000, // 1.5σ above mean
	})
	if out.IsAnomaly {
		t.Errorf("within 2σ should not fire; got %+v", out)
	}
}

func TestAnomalyDetector_Above3Sigma_FiresWarning(t *testing.T) {
	t.Parallel()
	det := ledger.NewAnomalyDetector()
	out := det.Detect(ledger.AnomalyInput{
		AgentID:              "agent-x",
		BaselineMeanMicros:   100_000,
		BaselineStdDevMicros: 10_000,
		ObservedMicros:       135_000, // 3.5σ above mean
	})
	if !out.IsAnomaly {
		t.Errorf("3.5σ should fire; got %+v", out)
	}
	if out.Severity != ledger.AnomalySeverityWarning {
		t.Errorf("severity = %s; want warning", out.Severity)
	}
	if out.SigmaDistance < 3.0 {
		t.Errorf("sigma_distance = %v; should be >= 3.0", out.SigmaDistance)
	}
}

func TestAnomalyDetector_Above6Sigma_FiresCritical(t *testing.T) {
	t.Parallel()
	det := ledger.NewAnomalyDetector()
	out := det.Detect(ledger.AnomalyInput{
		AgentID:              "agent-x",
		BaselineMeanMicros:   100_000,
		BaselineStdDevMicros: 10_000,
		ObservedMicros:       170_000, // 7σ above mean
	})
	if !out.IsAnomaly {
		t.Errorf("7σ should fire")
	}
	if out.Severity != ledger.AnomalySeverityCritical {
		t.Errorf("severity = %s; want critical", out.Severity)
	}
}

func TestAnomalyDetector_BelowMean_NeverFires(t *testing.T) {
	t.Parallel()
	// Cost dropping is good news, not an anomaly to alert on (it's tracked
	// for analytics but not surfaced as a budget anomaly event).
	det := ledger.NewAnomalyDetector()
	out := det.Detect(ledger.AnomalyInput{
		AgentID:              "agent-x",
		BaselineMeanMicros:   100_000,
		BaselineStdDevMicros: 10_000,
		ObservedMicros:       50_000, // 5σ BELOW mean
	})
	if out.IsAnomaly {
		t.Errorf("below-mean should not fire as anomaly; got %+v", out)
	}
}

func TestAnomalyDetector_BuildsCanonicalEvent(t *testing.T) {
	t.Parallel()
	det := ledger.NewAnomalyDetector()
	out := det.Detect(ledger.AnomalyInput{
		AgentID:              "agent-validator-01",
		TenantID:             "tenant-a",
		BaselineMeanMicros:   100_000,
		BaselineStdDevMicros: 10_000,
		ObservedMicros:       170_000,
	})
	if !out.IsAnomaly {
		t.Fatalf("expected anomaly")
	}
	ev := out.AsEvent()
	if ev.Topic != "chora.governance.cost_anomaly.detected.v1" {
		t.Errorf("topic = %s; want canonical", ev.Topic)
	}
	if ev.ChoraImdaDimension != "accountability" {
		t.Errorf("imda_dimension = %s; want accountability", ev.ChoraImdaDimension)
	}
}

func TestAnomalyDetector_CustomThresholds(t *testing.T) {
	t.Parallel()
	// Custom warning threshold = 2σ; critical = 4σ.
	det := ledger.NewAnomalyDetector()
	det.WarningSigma = 2.0
	det.CriticalSigma = 4.0
	out := det.Detect(ledger.AnomalyInput{
		AgentID:              "agent-x",
		BaselineMeanMicros:   100_000,
		BaselineStdDevMicros: 10_000,
		ObservedMicros:       125_000, // 2.5σ
	})
	if !out.IsAnomaly {
		t.Errorf("2.5σ above 2σ threshold should fire; got %+v", out)
	}
	if out.Severity != ledger.AnomalySeverityWarning {
		t.Errorf("severity = %s; want warning", out.Severity)
	}
}
