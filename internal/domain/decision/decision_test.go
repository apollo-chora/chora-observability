// Package decision_test exercises the AgentDecisionLog aggregate.
//
// AgentDecisionLog is APPEND-ONLY per .claude/rules/ddd-enforcement.md
// (mirrors AtomRevision). Mutators are absent. Risk tier is one of
// low/medium/high/critical per ai-runtime-guardrails skill stub.
package decision_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/5007-Capstone/chora/services/chora-observability/internal/domain/decision"
)

const (
	tenantA = "01970000-0000-7000-8000-000000000001"
	agidA   = "01970000-0000-7000-a000-000000000001"
	corrA   = "01970000-0000-7000-b000-000000000001"
	tpA     = "00-00000000000000000000000000000001-0000000000000001-01"
)

func TestNewDecisionLog_Valid(t *testing.T) {
	t.Parallel()

	d, err := decision.New(decision.NewParams{
		TenantID:      tenantA,
		Agid:          agidA,
		DecisionType:  decision.TypeRoute,
		Reason:        "matched cheap-route policy",
		RiskTier:      decision.TierLow,
		CorrelationID: corrA,
		Traceparent:   tpA,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if d.LogID == "" {
		t.Errorf("LogID empty")
	}
	if d.CreatedAt.IsZero() {
		t.Errorf("CreatedAt zero")
	}
}

// TestNewDecisionLog_CarriesQuestionType proves the per-question-type tag
// (proto field-21 attributes["question_type"], "mcq"|"oe") round-trips through
// the constructor + serializes — the split the O+ /o/agents qgen crew tiles
// (qgen-mcq / qgen-OE) are built from.
func TestNewDecisionLog_CarriesQuestionType(t *testing.T) {
	t.Parallel()

	d, err := decision.New(decision.NewParams{
		TenantID:      tenantA,
		Agid:          agidA,
		DecisionType:  decision.TypeRespond,
		Reason:        "generated MCQ",
		RiskTier:      decision.TierLow,
		CorrelationID: corrA,
		Traceparent:   tpA,
		QuestionType:  "mcq",
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if d.QuestionType != "mcq" {
		t.Errorf("QuestionType = %q; want mcq", d.QuestionType)
	}
	b, err := json.Marshal(d)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !strings.Contains(string(b), `"question_type":"mcq"`) {
		t.Errorf("serialized log missing question_type: %s", b)
	}
}

// TestNewDecisionLog_QuestionTypeOmittedWhenEmpty proves a non-question
// decision (routing-only) emits no question_type key (omitempty) rather than
// a misleading empty string.
func TestNewDecisionLog_QuestionTypeOmittedWhenEmpty(t *testing.T) {
	t.Parallel()

	d, err := decision.New(decision.NewParams{
		TenantID:      tenantA,
		Agid:          agidA,
		DecisionType:  decision.TypeRoute,
		Reason:        "routed",
		RiskTier:      decision.TierLow,
		CorrelationID: corrA,
		Traceparent:   tpA,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if d.QuestionType != "" {
		t.Errorf("QuestionType = %q; want empty", d.QuestionType)
	}
	b, _ := json.Marshal(d)
	if strings.Contains(string(b), "question_type") {
		t.Errorf("empty question_type should be omitted: %s", b)
	}
}

// TestNewDecisionLog_CarriesVerdict proves the per-decision verdict
// (accepted | rejected | completed_with_warning | refused | retry — the qgen
// quality-gate outcome, distinct from the 4-value decision_type ENUM) round-
// trips through the constructor + serializes as `verdict`. The O+ Decision
// Traces DECISION TYPE column + reasoning-panel step render this instead of the
// base `respond` (CHO-1700 follow-up).
func TestNewDecisionLog_CarriesVerdict(t *testing.T) {
	t.Parallel()

	d, err := decision.New(decision.NewParams{
		TenantID:      tenantA,
		Agid:          agidA,
		DecisionType:  decision.TypeRespond,
		Reason:        "critique passed",
		RiskTier:      decision.TierLow,
		CorrelationID: corrA,
		Traceparent:   tpA,
		Verdict:       "accepted",
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if d.Verdict != "accepted" {
		t.Errorf("Verdict = %q; want accepted", d.Verdict)
	}
	b, err := json.Marshal(d)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !strings.Contains(string(b), `"verdict":"accepted"`) {
		t.Errorf("serialized log missing verdict: %s", b)
	}
}

// TestNewDecisionLog_VerdictOmittedWhenEmpty proves a decision with no verdict
// (e.g. a routing-only event) emits no verdict key (omitempty) so the gateway
// falls back to decision_type rather than rendering an empty column.
func TestNewDecisionLog_VerdictOmittedWhenEmpty(t *testing.T) {
	t.Parallel()

	d, err := decision.New(decision.NewParams{
		TenantID:      tenantA,
		Agid:          agidA,
		DecisionType:  decision.TypeRoute,
		Reason:        "routed",
		RiskTier:      decision.TierLow,
		CorrelationID: corrA,
		Traceparent:   tpA,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if d.Verdict != "" {
		t.Errorf("Verdict = %q; want empty", d.Verdict)
	}
	b, _ := json.Marshal(d)
	if strings.Contains(string(b), "verdict") {
		t.Errorf("empty verdict should be omitted: %s", b)
	}
}

func TestNewDecisionLog_AcceptsAllDecisionTypes(t *testing.T) {
	t.Parallel()

	for _, dt := range []decision.Type{
		decision.TypeRoute,
		decision.TypeEscalate,
		decision.TypeRefuse,
		decision.TypeRespond,
	} {
		t.Run(string(dt), func(t *testing.T) {
			_, err := decision.New(decision.NewParams{
				TenantID:      tenantA,
				Agid:          agidA,
				DecisionType:  dt,
				Reason:        "test",
				RiskTier:      decision.TierMedium,
				CorrelationID: corrA,
				Traceparent:   tpA,
			})
			if err != nil {
				t.Errorf("type %s: %v", dt, err)
			}
		})
	}
}

func TestNewDecisionLog_AcceptsAllRiskTiers(t *testing.T) {
	t.Parallel()

	for _, rt := range []decision.RiskTier{
		decision.TierLow,
		decision.TierMedium,
		decision.TierHigh,
		decision.TierCritical,
	} {
		t.Run(string(rt), func(t *testing.T) {
			_, err := decision.New(decision.NewParams{
				TenantID:      tenantA,
				Agid:          agidA,
				DecisionType:  decision.TypeRoute,
				Reason:        "test",
				RiskTier:      rt,
				CorrelationID: corrA,
				Traceparent:   tpA,
			})
			if err != nil {
				t.Errorf("tier %s: %v", rt, err)
			}
		})
	}
}

func TestNewDecisionLog_RejectsInvalidDecisionType(t *testing.T) {
	t.Parallel()

	_, err := decision.New(decision.NewParams{
		TenantID:      tenantA,
		Agid:          agidA,
		DecisionType:  decision.Type("hallucinate"),
		Reason:        "test",
		RiskTier:      decision.TierLow,
		CorrelationID: corrA,
		Traceparent:   tpA,
	})
	if err == nil {
		t.Errorf("expected error for invalid decision_type")
	}
}

func TestNewDecisionLog_RejectsInvalidRiskTier(t *testing.T) {
	t.Parallel()

	_, err := decision.New(decision.NewParams{
		TenantID:      tenantA,
		Agid:          agidA,
		DecisionType:  decision.TypeRoute,
		Reason:        "test",
		RiskTier:      decision.RiskTier("nuclear"),
		CorrelationID: corrA,
		Traceparent:   tpA,
	})
	if err == nil {
		t.Errorf("expected error for invalid risk_tier")
	}
}

func TestNewDecisionLog_RejectsMissingTenant(t *testing.T) {
	t.Parallel()

	_, err := decision.New(decision.NewParams{
		TenantID:      "",
		Agid:          agidA,
		DecisionType:  decision.TypeRoute,
		Reason:        "test",
		RiskTier:      decision.TierLow,
		CorrelationID: corrA,
		Traceparent:   tpA,
	})
	if err == nil {
		t.Errorf("expected error for missing tenant")
	}
}

func TestNewDecisionLog_RejectsMissingAgid(t *testing.T) {
	t.Parallel()

	_, err := decision.New(decision.NewParams{
		TenantID:      tenantA,
		Agid:          "",
		DecisionType:  decision.TypeRoute,
		Reason:        "test",
		RiskTier:      decision.TierLow,
		CorrelationID: corrA,
		Traceparent:   tpA,
	})
	if err == nil {
		t.Errorf("expected error for missing agid")
	}
}

// TestNewDecisionLog_CarriesPlus9ProjectionFields proves the CHO-1560 +9-field
// projection (model_id / confidence / autonomy_level / cost_usd / crew_* /
// token counts / guardrail_outcome) round-trips through the constructor onto
// the Log aggregate.
func TestNewDecisionLog_CarriesPlus9ProjectionFields(t *testing.T) {
	t.Parallel()

	conf := float32(0.87)
	cost := int64(1234)
	d, err := decision.New(decision.NewParams{
		TenantID:         tenantA,
		Agid:             agidA,
		DecisionType:     decision.TypeRespond,
		Reason:           "PASS",
		RiskTier:         decision.TierLow,
		CorrelationID:    corrA,
		Traceparent:      tpA,
		ModelID:          "vertex_ai/gemini-2.5-flash",
		Confidence:       &conf,
		AutonomyLevel:    "hootl",
		CostUsdMicros:    &cost,
		CrewName:         "mcq_ai_assist",
		CrewID:           "crew-1",
		PromptTokens:     100,
		CompletionTokens: 50,
		CachedTokens:     10,
		GuardrailOutcome: "pass",
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if d.ModelID != "vertex_ai/gemini-2.5-flash" {
		t.Errorf("ModelID = %q", d.ModelID)
	}
	if d.Confidence == nil || *d.Confidence != 0.87 {
		t.Errorf("Confidence = %v; want 0.87", d.Confidence)
	}
	if d.AutonomyLevel != "hootl" {
		t.Errorf("AutonomyLevel = %q", d.AutonomyLevel)
	}
	if d.CostUsdMicros == nil || *d.CostUsdMicros != 1234 {
		t.Errorf("CostUsdMicros = %v; want 1234", d.CostUsdMicros)
	}
	if d.CrewName != "mcq_ai_assist" || d.CrewID != "crew-1" {
		t.Errorf("crew = %q/%q", d.CrewName, d.CrewID)
	}
	if d.PromptTokens != 100 || d.CompletionTokens != 50 || d.CachedTokens != 10 {
		t.Errorf("tokens = %d/%d/%d", d.PromptTokens, d.CompletionTokens, d.CachedTokens)
	}
	if d.GuardrailOutcome != "pass" {
		t.Errorf("GuardrailOutcome = %q", d.GuardrailOutcome)
	}
}

// TestDecisionLog_JSONKeysMatchGatewayContract locks the serialized JSON keys
// to EXACTLY what chora-gateway's upstream.AgentDecisionRow reads
// (model_id / confidence / cost_usd / autonomy_level / acted_upon). A drift
// here re-blanks the O+ Decision Traces columns even when the data is present.
func TestDecisionLog_JSONKeysMatchGatewayContract(t *testing.T) {
	t.Parallel()

	conf := float64(0.5)
	costUSD := float64(0.001234)
	d := &decision.Log{
		LogID:         "log-1",
		TenantID:      tenantA,
		Agid:          "qgen_critic",
		ModelID:       "vertex_ai/gemini-2.5-flash",
		ConfidenceOut: &conf,
		CostUSD:       &costUSD,
		AutonomyLevel: "hootl",
		ActedUpon:     "true",
		CrewName:      "mcq_ai_assist",
	}
	b, err := json.Marshal(d)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for _, key := range []string{
		"model_id", "confidence", "cost_usd", "autonomy_level",
		"acted_upon", "crew_name",
	} {
		if _, ok := m[key]; !ok {
			t.Errorf("serialized Log missing JSON key %q (gateway reads it); got keys %v", key, keysOf(m))
		}
	}
	if m["model_id"] != "vertex_ai/gemini-2.5-flash" {
		t.Errorf("model_id = %v", m["model_id"])
	}
	if m["cost_usd"] != 0.001234 {
		t.Errorf("cost_usd = %v; want 0.001234", m["cost_usd"])
	}
}

// TestNewDecisionLog_RejectsInvalidGuardrailOutcome covers the guardrail
// domain validation (must match the DB CHECK: pass|block|redact|"").
func TestNewDecisionLog_RejectsInvalidGuardrailOutcome(t *testing.T) {
	t.Parallel()
	for _, bad := range []string{"allow", "deny", "unspecified", "BLOCK"} {
		t.Run(bad, func(t *testing.T) {
			_, err := decision.New(decision.NewParams{
				TenantID: tenantA, Agid: agidA, DecisionType: decision.TypeRespond,
				RiskTier: decision.TierLow, CorrelationID: corrA,
				GuardrailOutcome: bad,
			})
			if err == nil {
				t.Errorf("expected error for guardrail_outcome %q", bad)
			}
		})
	}
	// Accepted values + empty must NOT error.
	for _, ok := range []string{"", "pass", "block", "redact"} {
		t.Run("ok_"+ok, func(t *testing.T) {
			_, err := decision.New(decision.NewParams{
				TenantID: tenantA, Agid: agidA, DecisionType: decision.TypeRespond,
				RiskTier: decision.TierLow, CorrelationID: corrA,
				GuardrailOutcome: ok,
			})
			if err != nil {
				t.Errorf("guardrail_outcome %q: %v", ok, err)
			}
		})
	}
}

// TestNewDecisionLog_RejectsConfidenceOutOfRange covers the [0,1] guard.
func TestNewDecisionLog_RejectsConfidenceOutOfRange(t *testing.T) {
	t.Parallel()
	for _, bad := range []float32{-0.1, 1.5} {
		c := bad
		_, err := decision.New(decision.NewParams{
			TenantID: tenantA, Agid: agidA, DecisionType: decision.TypeRespond,
			RiskTier: decision.TierLow, CorrelationID: corrA,
			Confidence: &c,
		})
		if err == nil {
			t.Errorf("expected error for confidence %v", bad)
		}
	}
}

// TestNewDecisionLog_RejectsNegativeTokens covers the token-count guard.
func TestNewDecisionLog_RejectsNegativeTokens(t *testing.T) {
	t.Parallel()
	_, err := decision.New(decision.NewParams{
		TenantID: tenantA, Agid: agidA, DecisionType: decision.TypeRespond,
		RiskTier: decision.TierLow, CorrelationID: corrA,
		PromptTokens: -5,
	})
	if err == nil {
		t.Error("expected error for negative prompt_tokens")
	}
}

// TestLog_PopulateProjectionOut_BridgesStorageToSerialization covers the
// micros→USD + float32→float64 bridge, including the nil-stays-nil paths.
func TestLog_PopulateProjectionOut_BridgesStorageToSerialization(t *testing.T) {
	t.Parallel()

	// nil receiver is a no-op (must not panic).
	var nilLog *decision.Log
	nilLog.PopulateProjectionOut()

	conf := float32(0.5)
	cost := int64(1234)
	l := &decision.Log{Confidence: &conf, CostUsdMicros: &cost}
	l.PopulateProjectionOut()
	if l.ConfidenceOut == nil || *l.ConfidenceOut != 0.5 {
		t.Errorf("ConfidenceOut = %v; want 0.5", l.ConfidenceOut)
	}
	if l.CostUSD == nil || *l.CostUSD != 0.001234 {
		t.Errorf("CostUSD = %v; want 0.001234", l.CostUSD)
	}

	// Absent storage → absent serialization (no fabricated zero).
	empty := &decision.Log{}
	empty.PopulateProjectionOut()
	if empty.ConfidenceOut != nil {
		t.Errorf("ConfidenceOut = %v; want nil", empty.ConfidenceOut)
	}
	if empty.CostUSD != nil {
		t.Errorf("CostUSD = %v; want nil", empty.CostUSD)
	}
}

func keysOf(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func TestNewDecisionLog_RejectsLongReason(t *testing.T) {
	t.Parallel()

	_, err := decision.New(decision.NewParams{
		TenantID:      tenantA,
		Agid:          agidA,
		DecisionType:  decision.TypeRoute,
		Reason:        strings.Repeat("x", 2049),
		RiskTier:      decision.TierLow,
		CorrelationID: corrA,
		Traceparent:   tpA,
	})
	if err == nil {
		t.Errorf("expected error for reason >2048")
	}
}
