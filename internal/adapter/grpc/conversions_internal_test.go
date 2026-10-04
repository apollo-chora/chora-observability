// Package grpcadapter exercises the unexported proto <-> domain conversion
// helpers in observability_server.go. The nil-entry and unspecified-enum
// branches are unreachable through the public RPC surface (the domain
// constructors reject them), so they are tested directly here.
package grpcadapter

import (
	"testing"
	"time"

	observabilityv1 "github.com/locoroco-git/Chora-LMS/chora-contracts/gen/go/chora/services/observability/v1"

	"github.com/5007-Capstone/chora/services/chora-observability/internal/domain/correlation"
	"github.com/5007-Capstone/chora/services/chora-observability/internal/domain/decision"
	"github.com/5007-Capstone/chora/services/chora-observability/internal/domain/ledger"
)

func TestTokenUsageEntryToProto_Nil(t *testing.T) {
	if got := tokenUsageEntryToProto(nil); got != nil {
		t.Fatalf("tokenUsageEntryToProto(nil) = %v; want nil", got)
	}
}

func TestTokenUsageEntryToProto_RoundTrip(t *testing.T) {
	e, err := ledger.New(ledger.NewParams{
		TenantID: "t-1", Gcid: "g-1", Agid: "a-1", ModelID: "m-1",
		PromptTokens: 11, CompletionTokens: 7, CostUsdMicros: 555,
		TraceID: "00000000000000000000000000000001", SpanID: "0000000000000001",
		RecordedAt: time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("ledger.New: %v", err)
	}
	p := tokenUsageEntryToProto(e)
	if p == nil {
		t.Fatal("expected proto entry")
	}
	if p.LedgerId != e.LedgerID || p.TenantId != "t-1" || p.Gcid != "g-1" || p.Agid != "a-1" {
		t.Errorf("identity fields not mapped: %+v", p)
	}
	if p.PromptTokens != 11 || p.CompletionTokens != 7 || p.CostUsdMicros != 555 {
		t.Errorf("numeric fields not mapped: %+v", p)
	}
	if p.RecordedAt.AsTime().Unix() != e.RecordedAt.Unix() {
		t.Errorf("recorded_at not mapped: %v vs %v", p.RecordedAt.AsTime(), e.RecordedAt)
	}
}

func TestAgentDecisionEntryToProto_Nil(t *testing.T) {
	if got := agentDecisionEntryToProto(nil); got != nil {
		t.Fatalf("agentDecisionEntryToProto(nil) = %v; want nil", got)
	}
}

func TestAgentDecisionEntryToProto_RoundTrip(t *testing.T) {
	d, err := decision.New(decision.NewParams{
		TenantID: "t-1", Agid: "a-1",
		DecisionType: decision.TypeRefuse, Reason: "because",
		RiskTier:     decision.TierCritical,
		CorrelationID: "c-1",
		Reasoning: mustReasoning(t),
	})
	if err != nil {
		t.Fatalf("decision.New: %v", err)
	}
	p := agentDecisionEntryToProto(d)
	if p == nil {
		t.Fatal("expected proto entry")
	}
	if p.LogId != d.LogID || p.DecisionType != observabilityv1.AgentDecisionType_AGENT_DECISION_TYPE_REFUSE {
		t.Errorf("type mapping wrong: %+v", p)
	}
	if p.RiskTier != observabilityv1.AgentRiskTier_AGENT_RISK_TIER_CRITICAL {
		t.Errorf("risk tier mapping wrong: %v", p.RiskTier)
	}
	if p.Reasoning == nil || p.Reasoning.LatencyMs != 42 || p.Reasoning.Summary != "s" {
		t.Errorf("reasoning not mapped: %+v", p.Reasoning)
	}
}

func TestAgentDecisionEntryToProto_NoReasoning(t *testing.T) {
	d, err := decision.New(decision.NewParams{
		TenantID: "t-1", Agid: "a-1",
		DecisionType: decision.TypeRoute, RiskTier: decision.TierLow,
		CorrelationID: "c-1",
	})
	if err != nil {
		t.Fatalf("decision.New: %v", err)
	}
	p := agentDecisionEntryToProto(d)
	if p.Reasoning != nil {
		t.Fatalf("expected nil reasoning; got %+v", p.Reasoning)
	}
}

func TestCorrelationEntryToProto_Nil(t *testing.T) {
	if got := correlationEntryToProto(nil); got != nil {
		t.Fatalf("correlationEntryToProto(nil) = %v; want nil", got)
	}
}

func TestCorrelationEntryToProto_RoundTrip(t *testing.T) {
	c, err := correlation.New(correlation.NewParams{
		TenantID: "t-1", CorrelationID: "c-1",
		TraceID: "00000000000000000000000000000001", ParentSpanID: "0000000000000001",
	})
	if err != nil {
		t.Fatalf("correlation.New: %v", err)
	}
	_ = c.AttachChildSpan("0000000000000002")
	p := correlationEntryToProto(c)
	if p == nil {
		t.Fatal("expected proto entry")
	}
	if p.TraceId != "00000000000000000000000000000001" || len(p.ChildSpans) != 1 {
		t.Errorf("correlation fields wrong: %+v", p)
	}
	if p.ChildSpans[0] != "0000000000000002" {
		t.Errorf("child span = %q; want 0000000000000002", p.ChildSpans[0])
	}
}

func TestDecisionTypeEnumConversions_Unspecified(t *testing.T) {
	// Unspecified -> empty domain sentinel -> rejected by decision.New.
	if got := decisionTypeFromProto(observabilityv1.AgentDecisionType_AGENT_DECISION_TYPE_UNSPECIFIED); got != decision.Type("") {
		t.Fatalf("decisionTypeFromProto(unspecified) = %q; want empty", got)
	}
	// Unknown domain type -> UNSPECIFIED proto.
	if got := decisionTypeToProto(decision.Type("bogus")); got != observabilityv1.AgentDecisionType_AGENT_DECISION_TYPE_UNSPECIFIED {
		t.Fatalf("decisionTypeToProto(bogus) = %v; want UNSPECIFIED", got)
	}
	// Round-trip the four valid values.
	valid := map[decision.Type]observabilityv1.AgentDecisionType{
		decision.TypeRoute:    observabilityv1.AgentDecisionType_AGENT_DECISION_TYPE_ROUTE,
		decision.TypeEscalate: observabilityv1.AgentDecisionType_AGENT_DECISION_TYPE_ESCALATE,
		decision.TypeRefuse:   observabilityv1.AgentDecisionType_AGENT_DECISION_TYPE_REFUSE,
		decision.TypeRespond:  observabilityv1.AgentDecisionType_AGENT_DECISION_TYPE_RESPOND,
	}
	for dt, pt := range valid {
		if got := decisionTypeFromProto(pt); got != dt {
			t.Errorf("decisionTypeFromProto(%v) = %q; want %q", pt, got, dt)
		}
		if got := decisionTypeToProto(dt); got != pt {
			t.Errorf("decisionTypeToProto(%q) = %v; want %v", dt, got, pt)
		}
	}
}

func TestRiskTierEnumConversions_Unspecified(t *testing.T) {
	if got := riskTierFromProto(observabilityv1.AgentRiskTier_AGENT_RISK_TIER_UNSPECIFIED); got != decision.RiskTier("") {
		t.Fatalf("riskTierFromProto(unspecified) = %q; want empty", got)
	}
	if got := riskTierToProto(decision.RiskTier("bogus")); got != observabilityv1.AgentRiskTier_AGENT_RISK_TIER_UNSPECIFIED {
		t.Fatalf("riskTierToProto(bogus) = %v; want UNSPECIFIED", got)
	}
	valid := map[decision.RiskTier]observabilityv1.AgentRiskTier{
		decision.TierLow:      observabilityv1.AgentRiskTier_AGENT_RISK_TIER_LOW,
		decision.TierMedium:   observabilityv1.AgentRiskTier_AGENT_RISK_TIER_MEDIUM,
		decision.TierHigh:     observabilityv1.AgentRiskTier_AGENT_RISK_TIER_HIGH,
		decision.TierCritical: observabilityv1.AgentRiskTier_AGENT_RISK_TIER_CRITICAL,
	}
	for rt, pt := range valid {
		if got := riskTierFromProto(pt); got != rt {
			t.Errorf("riskTierFromProto(%v) = %q; want %q", pt, got, rt)
		}
		if got := riskTierToProto(rt); got != pt {
			t.Errorf("riskTierToProto(%q) = %v; want %v", rt, got, pt)
		}
	}
}

func mustReasoning(t *testing.T) *decision.ReasoningSummary {
	t.Helper()
	rs, err := decision.NewReasoningSummary(decision.ReasoningParams{
		Input: "in", Output: "out", LatencyMs: 42, Summary: "s",
	})
	if err != nil {
		t.Fatalf("NewReasoningSummary: %v", err)
	}
	return rs
}