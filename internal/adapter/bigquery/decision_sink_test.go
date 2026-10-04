// Package bigquery exercises the decision-sink projection — the pure
// toDecisionBQRow mapping plus the constructor/Insert paths that are
// reachable without a live BigQuery (the streaming Put against a real
// inserter is deliberately left untested: it requires a live client).
package bigquery

import (
	"context"
	"strings"
	"testing"

	"github.com/5007-Capstone/chora/services/chora-observability/internal/domain/decision"
)

func TestToDecisionBQRow_FullProjection(t *testing.T) {
	rs, err := decision.NewReasoningSummary(decision.ReasoningParams{
		Input: "in", Output: "out", LatencyMs: 5, Summary: "pii-detected-input",
	})
	if err != nil {
		t.Fatalf("NewReasoningSummary: %v", err)
	}
	log, err := decision.New(decision.NewParams{
		TenantID: "t-1", Agid: "a-1", ModelID: "gemini-3-pro",
		DecisionType:  decision.TypeEscalate,
		Reason:        "needs human",
		RiskTier:      decision.TierHigh,
		CorrelationID: "c-1",
		Traceparent:   "00-00000000000000000000000000000001-0000000000000001-01",
		Reasoning:     rs,
		CrewID:        "crew-7",
		GuardrailOutcome: "block",
		AutonomyLevel: "advisory",
		Verdict:       "accepted",
	})
	if err != nil {
		t.Fatalf("decision.New: %v", err)
	}

	row := toDecisionBQRow(log)
	if row == nil {
		t.Fatal("toDecisionBQRow returned nil")
	}
	if row.LogID != log.LogID || row.TenantID != "t-1" || row.Agid != "a-1" || row.AgentID != "a-1" {
		t.Errorf("identity fields: %+v", row)
	}
	if row.RunID != "crew-7" {
		t.Errorf("run_id = %q; want crew-7", row.RunID)
	}
	// Verdict overrides the domain decision_type in the BQ projection
	if row.DecisionType != "accepted" {
		t.Errorf("decision_type = %q; want verdict 'accepted'", row.DecisionType)
	}
	if row.GuardrailVerdict != "block" || row.AutonomyLevel != "advisory" {
		t.Errorf("guardrail/autonomy = %q/%q", row.GuardrailVerdict, row.AutonomyLevel)
	}
	if row.RiskTier != "high" || row.CorrelationID != "c-1" {
		t.Errorf("risk/correlation = %q/%q", row.RiskTier, row.CorrelationID)
	}
	if row.ReasoningSummary != "pii-detected-input" {
		t.Errorf("reasoning_summary = %q", row.ReasoningSummary)
	}
	if row.InputHash == "" || row.OutputHash == "" {
		t.Error("expected input/output hashes from the reasoning summary")
	}
	if row.DecidedAt.IsZero() {
		t.Error("expected decided_at stamp")
	}
}

func TestToDecisionBQRow_MinimalProjection(t *testing.T) {
	log, err := decision.New(decision.NewParams{
		TenantID: "t-1", Agid: "a-1",
		DecisionType:  decision.TypeRoute,
		RiskTier:      decision.TierLow,
		CorrelationID: "c-1",
	})
	if err != nil {
		t.Fatalf("decision.New: %v", err)
	}
	row := toDecisionBQRow(log)
	if row.ReasoningSummary != "" || row.InputHash != "" || row.OutputHash != "" {
		t.Errorf("nil reasoning should leave hash fields empty: %+v", row)
	}
	if row.DecisionType != "route" {
		t.Errorf("decision_type = %q; want 'route' (no verdict)", row.DecisionType)
	}
}

func TestDecisionSink_InsertNilLog(t *testing.T) {
	sink := &DecisionSink{}
	if err := sink.Insert(context.Background(), nil); err != nil {
		t.Fatalf("Insert(nil) = %v; want nil", err)
	}
}

func TestNewDecisionSink_ConstructsAndCloses(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping BigQuery client construction in -short mode")
	}
	sink, err := NewDecisionSink(context.Background(), "proj", "dataset", "agent_decision_log", "asia-southeast1")
	if err != nil {
		if strings.Contains(err.Error(), "credentials") || strings.Contains(err.Error(), "google:") {
			t.Skipf("no Application Default Credentials in environment: %v", err)
		}
		t.Fatalf("NewDecisionSink: %v", err)
	}
	if sink.schema == nil || len(sink.schema) == 0 {
		t.Error("expected inferred BigQuery schema from decisionBQRow")
	}
	if sink.bq == nil {
		t.Error("expected underlying bigquery client")
	} else if sink.bq.Location != "asia-southeast1" {
		t.Errorf("client location = %q; want asia-southeast1", sink.bq.Location)
	}
	if sink.inserter == nil {
		t.Error("expected wired dataset/table inserter")
	}
	if err := sink.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestNewEvidenceClient_ConstructsAndCloses(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping BigQuery client construction in -short mode")
	}
	c, err := NewEvidenceClient(context.Background(), "proj", "dataset", "agent_eval_evidence", "asia-southeast1")
	if err != nil {
		if strings.Contains(err.Error(), "credentials") || strings.Contains(err.Error(), "google:") {
			t.Skipf("no Application Default Credentials in environment: %v", err)
		}
		t.Fatalf("NewEvidenceClient: %v", err)
	}
	if c.table != "`proj.dataset.agent_eval_evidence`" {
		t.Errorf("table = %q; want fully-qualified back-quoted ref", c.table)
	}
	if c.bq == nil {
		t.Error("expected underlying bigquery client")
	} else if c.bq.Location != "asia-southeast1" {
		t.Errorf("client location = %q; want asia-southeast1", c.bq.Location)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}