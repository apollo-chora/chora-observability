// agent_decision_binding_test.go — RED→GREEN unit tests for the
// chora.observability.agent_decision.logged.v1 StreamingPull binding
// (ADR-167 read-model hydration, 2026-05-29).
//
// Verifies:
//   - buildAgentDecisionHandler proto.Unmarshal the v2 AgentDecisionLogged
//     payload + maps it into AgentDecisionLoggedEvent + persists via
//     AgentDecisionConsumer.Handle.
//   - DecisionKind → decision.Type mapping (CRITIQUE→respond, BLOCK→refuse,
//     CLASSIFY→route).
//   - guardrail_outcome → risk_tier derivation (block→high).
//   - correlation_id falls back through invocation/crew/decision id so a
//     real decision is never rejected for a blank correlation.
//   - FAIL LOUD on malformed proto bytes (NACK→DLQ; no JSON fallback).
//   - startAgentDecisionSubscriber returns nil when client/cons unwired.
package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonenvelope "github.com/apollo-chora/chora-common/envelope"
	"github.com/apollo-chora/chora-common/idempotent"
	cgcpubsub "github.com/apollo-chora/chora-common/pubsub"
	commonv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/common/v1"
	observabilityv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/observability/v1"
	"github.com/apollo-chora/chora-observability/internal/adapter/events"
	"github.com/apollo-chora/chora-observability/internal/adapter/inmem"
	"github.com/apollo-chora/chora-observability/internal/domain/decision"
)

// pricingYAMLPathForTest resolves the service's config/pricing.yaml relative to
// the cmd/server test cwd (../../config/pricing.yaml).
func pricingYAMLPathForTest(t *testing.T) string {
	t.Helper()
	p := filepath.Join("..", "..", "config", "pricing.yaml")
	if _, err := os.Stat(p); err != nil {
		t.Fatalf("pricing.yaml not found at %s: %v", p, err)
	}
	return p
}

// TestDecisionCostCalculator_ResolvesTheModelIDProductionSends guards CHO-2220's
// second defect. config/pricing.yaml was keyed "vertex_ai/gemini-2.5-flash"
// while every producer emits a BARE model id ("gemini-2.5-flash" — straight
// from the agent's logical_model_id / RECOMMENDER_MODEL / QGEN_IMAGE_MODEL_ID).
// Nothing in the codebase ever constructed the provider-qualified form; only
// the test fixtures did. That is precisely why the mismatch survived unnoticed:
// the fixtures supplied a key shape production never sends, so every lookup hit
// in CI and missed 100% of the time in production, leaving the O+ cost column
// blank for every model since it was written.
//
// Drive this with the model ids that actually appear on the wire — never with a
// shape invented here.
func TestDecisionCostCalculator_ResolvesTheModelIDProductionSends(t *testing.T) {
	calc, err := newDecisionCostCalculatorFromFile(pricingYAMLPathForTest(t))
	if err != nil {
		t.Fatalf("load pricing.yaml: %v", err)
	}
	// Every one of these is a model id observed in live Vertex traffic in the
	// 25d to 2026-07-16, in the exact form the gateway sends it.
	for _, model := range []string{
		"gemini-2.5-flash",
		"gemini-2.5-pro",
		"gemini-3.5-flash",
		"gemini-2.5-flash-image",
		"gemini-3-pro-image",
	} {
		micros, ok := calc.ComputeMicros(model, 1_000, 500, 0)
		if !ok {
			t.Errorf("ComputeMicros(%q) not priced — pricing.yaml key format does not "+
				"match the model id production sends", model)
			continue
		}
		if micros <= 0 {
			t.Errorf("ComputeMicros(%q) = %d micros, want > 0", model, micros)
		}
	}
}

// TestDecisionCostCalculator_UnknownModelStaysBlank pins the deliberate
// null-vs-zero distinction: an unpriced model must leave cost nil (blank in O+)
// rather than record $0 as if it were real. gemini-3.1-pro-preview has no SKU
// in the Vertex catalogue, so it is intentionally absent from pricing.yaml.
func TestDecisionCostCalculator_UnknownModelStaysBlank(t *testing.T) {
	calc, err := newDecisionCostCalculatorFromFile(pricingYAMLPathForTest(t))
	if err != nil {
		t.Fatalf("load pricing.yaml: %v", err)
	}
	if _, ok := calc.ComputeMicros("gemini-3.1-pro-preview", 1_000, 500, 0); ok {
		t.Error("gemini-3.1-pro-preview reported a priced cost — it has no SKU; " +
			"a fabricated rate is worse than a blank cell")
	}
}

// TestDecisionCostCalculator_CachedIsDiscountedNotFree keeps pricing.yaml in
// step with the gateway's table, which is the only reason the two do not drift.
// Both billed cached tokens at ZERO until 2026-07-16; Vertex publishes real
// "... Input Caching" SKUs, so zero was an under-bill, not a simplification.
func TestDecisionCostCalculator_CachedIsDiscountedNotFree(t *testing.T) {
	calc, err := newDecisionCostCalculatorFromFile(pricingYAMLPathForTest(t))
	if err != nil {
		t.Fatalf("load pricing.yaml: %v", err)
	}
	for _, model := range []string{"gemini-2.5-flash", "gemini-2.5-pro", "gemini-3.5-flash"} {
		cached, ok := calc.ComputeMicros(model, 1_000_000, 0, 1_000_000) // all cached
		if !ok {
			t.Errorf("%s not priced", model)
			continue
		}
		fresh, _ := calc.ComputeMicros(model, 1_000_000, 0, 0)
		if cached == 0 {
			t.Errorf("%s: fully-cached prompt bills 0 — cached tokens are discounted, not free", model)
		}
		if cached >= fresh {
			t.Errorf("%s: cached %d >= fresh %d — the cache discount has inverted", model, cached, fresh)
		}
	}
}

// TestDecisionCostCalculator_MatchesTheGatewayTable is the anti-drift guard.
// pricing.yaml (O+ cost column) and the gateway's geminiPricing (the ledger +
// budget) price the same models from two places — the exact duplication that
// caused CHO-2220, where the two had silently drifted 2x apart and the accurate
// copy was the inert one. Until they are unified onto one descriptor, this test
// is what keeps them honest. Rates are per 1k here, per 1M there.
func TestDecisionCostCalculator_MatchesTheGatewayTable(t *testing.T) {
	calc, err := newDecisionCostCalculatorFromFile(pricingYAMLPathForTest(t))
	if err != nil {
		t.Fatalf("load pricing.yaml: %v", err)
	}
	// (model, input $/1M, output $/1M, cached $/1M) — must equal
	// services/chora-model-gateway/internal/adapter/vendors/gemini/gemini.go:geminiPricing.
	gateway := []struct {
		model             string
		inPer1M, outPer1M float64
		cachePer1M        float64
	}{
		{"gemini-2.5-flash", 0.30, 2.50, 0.03},
		{"gemini-2.5-pro", 1.25, 10.00, 0.13},
		{"gemini-2.5-flash-lite", 0.10, 0.40, 0.01},
		{"gemini-3.5-flash", 1.50, 9.00, 0.15},
		{"gemini-3.1-flash-lite", 0.25, 1.50, 0.025},
		{"gemini-2.5-flash-image", 0.30, 30.00, 0.03},
		{"gemini-3.1-flash-image", 0.50, 60.00, 0.05},
		{"gemini-3-pro-image", 2.00, 120.00, 0.20},
	}
	for _, g := range gateway {
		// 1M input-only ⇒ cost == input rate, in micros.
		gotIn, ok := calc.ComputeMicros(g.model, 1_000_000, 0, 0)
		if !ok {
			t.Errorf("%s missing from pricing.yaml but present in the gateway table", g.model)
			continue
		}
		if wantIn := int64(g.inPer1M * 1_000_000 / 1_000_000 * 1_000_000); gotIn != wantIn {
			t.Errorf("%s input: pricing.yaml=%d micros/1M, gateway=%d micros/1M — THE TABLES HAVE DRIFTED",
				g.model, gotIn, wantIn)
		}
		gotOut, _ := calc.ComputeMicros(g.model, 0, 1_000_000, 0)
		if wantOut := int64(g.outPer1M * 1_000_000); gotOut != wantOut {
			t.Errorf("%s output: pricing.yaml=%d micros/1M, gateway=%d micros/1M — THE TABLES HAVE DRIFTED",
				g.model, gotOut, wantOut)
		}
		gotCache, _ := calc.ComputeMicros(g.model, 1_000_000, 0, 1_000_000)
		if wantCache := int64(g.cachePer1M * 1_000_000); gotCache != wantCache {
			t.Errorf("%s cached: pricing.yaml=%d micros/1M, gateway=%d micros/1M — THE TABLES HAVE DRIFTED",
				g.model, gotCache, wantCache)
		}
	}
}

func newAgentDecisionFixture(t *testing.T) (*events.AgentDecisionConsumer, *inmem.DecisionRepository) {
	t.Helper()
	repo := inmem.NewDecisionRepository()
	cons := events.NewAgentDecisionConsumer(events.AgentDecisionConsumerConfig{
		Repo:  repo,
		Inbox: idempotent.NewMemoryStore(),
	})
	return cons, repo
}

func goodDecisionProto() *observabilityv1.AgentDecisionLogged {
	at := time.Date(2026, 5, 29, 9, 33, 55, 0, time.UTC)
	return &observabilityv1.AgentDecisionLogged{
		Envelope: &commonv1.EventEnvelope{
			EventId:       "evt-dec-01HABC",
			TenantId:      "platform",
			Gcid:          "gcid-1",
			OccurredAt:    timestamppb.New(at),
			Traceparent:   "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
			SourceService: "chora-ai-kernel-orchestrator",
			SchemaVersion: 2,
		},
		DecisionId:   "dec-1",
		InvocationId: "assist-job-abc",
		Agid:         "qgen_critic",
		// BARE model id — the form every producer actually emits (the agent's
		// logical_model_id). This fixture said "vertex_ai/gemini-2.5-flash"
		// until CHO-2220; nothing in production has ever produced that shape, so
		// the pricing lookup below hit in CI and missed 100% in prod.
		ModelId:       "gemini-2.5-flash",
		DecisionKind:  observabilityv1.DecisionKind_DECISION_KIND_CRITIQUE,
		InputSummary:  "candidate MCQ",
		OutputSummary: "PASS — well-formed",
		CrewName:      "mcq_ai_assist",
		CrewId:        "crew-1",
		DecidedAt:     timestamppb.New(at),
	}
}

func decisionProtoMessage(t *testing.T, m *observabilityv1.AgentDecisionLogged) *cgcpubsub.Message {
	t.Helper()
	data, err := proto.Marshal(m)
	if err != nil {
		t.Fatalf("marshal proto: %v", err)
	}
	env := m.GetEnvelope()
	return &cgcpubsub.Message{
		Topic: events.TopicAgentDecisionLogged,
		Envelope: commonenvelope.Envelope{
			EventID:       env.GetEventId(),
			TenantID:      env.GetTenantId(),
			GCID:          env.GetGcid(),
			OccurredAt:    env.GetOccurredAt().AsTime(),
			Traceparent:   env.GetTraceparent(),
			SchemaVersion: env.GetSchemaVersion(),
		},
		Payload:         data,
		DeliveryAttempt: 1,
	}
}

func TestBuildAgentDecisionHandler_DecodesAndPersists(t *testing.T) {
	cons, repo := newAgentDecisionFixture(t)
	h := buildAgentDecisionHandler(cons)

	if err := h(context.Background(), decisionProtoMessage(t, goodDecisionProto())); err != nil {
		t.Fatalf("handler returned error: %v", err)
	}

	// tenant "platform" persists under the platform sentinel (the inmem repo
	// does not normalise to nil-uuid — that's the pg adapter's RLS concern —
	// so List by "platform" returns the row here).
	logs, err := repo.List(context.Background(), "platform", decision.ListFilter{})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(logs) != 1 {
		t.Fatalf("logs = %d; want 1", len(logs))
	}
	l := logs[0]
	if l.Agid != "qgen_critic" {
		t.Errorf("agid = %q; want qgen_critic", l.Agid)
	}
	if l.DecisionType != decision.TypeRespond {
		t.Errorf("decision_type = %q; want respond (CRITIQUE→respond)", l.DecisionType)
	}
	if l.RiskTier != decision.TierLow {
		t.Errorf("risk_tier = %q; want low (no guardrail block)", l.RiskTier)
	}
	if l.CorrelationID != "assist-job-abc" {
		t.Errorf("correlation_id = %q; want assist-job-abc (invocation_id)", l.CorrelationID)
	}
	if !l.CreatedAt.Equal(time.Date(2026, 5, 29, 9, 33, 55, 0, time.UTC)) {
		t.Errorf("recorded_at = %v; want event DecidedAt", l.CreatedAt)
	}
}

// TestBuildAgentDecisionHandler_ExtractsPlus9Fields proves the CHO-1560
// projection fields (model_id / confidence / crew_* / token counts /
// guardrail_outcome) are extracted from the proto into the persisted row.
func TestBuildAgentDecisionHandler_ExtractsPlus9Fields(t *testing.T) {
	cons, repo := newAgentDecisionFixture(t)
	h := buildAgentDecisionHandler(cons)

	m := goodDecisionProto()
	m.Confidence = 0.91
	m.GuardrailOutcome = "pass"
	m.PromptTokens = 1000
	m.CompletionTokens = 200
	m.CachedTokens = 100

	if err := h(context.Background(), decisionProtoMessage(t, m)); err != nil {
		t.Fatalf("handler: %v", err)
	}
	logs, _ := repo.List(context.Background(), "platform", decision.ListFilter{})
	if len(logs) != 1 {
		t.Fatalf("logs = %d; want 1", len(logs))
	}
	l := logs[0]
	if l.ModelID != "gemini-2.5-flash" {
		t.Errorf("ModelID = %q; want gemini-2.5-flash", l.ModelID)
	}
	if l.Confidence == nil || *l.Confidence < 0.9099 || *l.Confidence > 0.9101 {
		t.Errorf("Confidence = %v; want ~0.91", l.Confidence)
	}
	if l.CrewName != "mcq_ai_assist" || l.CrewID != "crew-1" {
		t.Errorf("crew = %q/%q; want mcq_ai_assist/crew-1", l.CrewName, l.CrewID)
	}
	if l.PromptTokens != 1000 || l.CompletionTokens != 200 || l.CachedTokens != 100 {
		t.Errorf("tokens = %d/%d/%d; want 1000/200/100", l.PromptTokens, l.CompletionTokens, l.CachedTokens)
	}
	if l.GuardrailOutcome != "pass" {
		t.Errorf("GuardrailOutcome = %q; want pass", l.GuardrailOutcome)
	}
}

// TestBuildAgentDecisionHandler_ExtractsQuestionType proves the binding reads
// question_type from the proto field-21 attributes map (ADR-167 D5) and
// persists it — the per-(agent,question_type) split the O+ /o/agents qgen crew
// tiles (qgen-mcq / qgen-OE) are built from. Stray whitespace is trimmed.
func TestBuildAgentDecisionHandler_ExtractsQuestionType(t *testing.T) {
	cases := []struct {
		name string
		attr map[string]string
		want string
	}{
		{"mcq", map[string]string{"question_type": "mcq"}, "mcq"},
		{"oe", map[string]string{"question_type": "oe"}, "oe"},
		{"trimmed", map[string]string{"question_type": " mcq "}, "mcq"},
		{"absent_key", map[string]string{"attempt_count": "2"}, ""},
		{"nil_map", nil, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cons, repo := newAgentDecisionFixture(t)
			h := buildAgentDecisionHandler(cons)
			m := goodDecisionProto()
			m.Agid = "qgen_question"
			m.Attributes = tc.attr
			if err := h(context.Background(), decisionProtoMessage(t, m)); err != nil {
				t.Fatalf("handler: %v", err)
			}
			logs, _ := repo.List(context.Background(), "platform", decision.ListFilter{})
			if len(logs) != 1 {
				t.Fatalf("logs = %d; want 1", len(logs))
			}
			if logs[0].QuestionType != tc.want {
				t.Errorf("QuestionType = %q; want %q", logs[0].QuestionType, tc.want)
			}
		})
	}
}

// TestBuildAgentDecisionHandler_ComputesCostFromTokens proves the binding
// derives cost_usd_micros from token counts x the pricing config (the proto
// carries NO cost field). With gemini-2.5-flash @ input 0.00030/1k,
// output 0.00250/1k, cache 0.00003/1k and 1000 prompt (100 cached) + 200
// completion tokens:
//
//	fresh_input = 900 → 900/1000 * 0.00030 = 0.00027   USD
//	cached      = 100 → 100/1000 * 0.00003 = 0.000003  USD
//	output      = 200 → 200/1000 * 0.00250 = 0.0005    USD
//	total = 0.000773 USD = 773 micros
//
// CHO-2220, three revisions deep — worth reading as a unit:
//   - 258: Gemini 2.5 Flash's pre-GA rates ($0.15/$0.60) + a 25%-of-input cache
//     guess. It could only run at all because the fixture fed a "vertex_ai/"-
//     prefixed model id that no producer emits.
//   - 770: GA rates, but cached tokens still billed at ZERO.
//   - 773: cached billed at the published $0.03/1M.
//
// Every one of those assertions was faithful to the config of its day, and the
// config was wrong every time — so the test stayed green while the ledger
// under-billed. Re-derive from the published price list, never from whatever
// the code currently returns.
func TestBuildAgentDecisionHandler_ComputesCostFromTokens(t *testing.T) {
	cons, repo := newAgentDecisionFixture(t)
	calc, err := newDecisionCostCalculatorFromFile(pricingYAMLPathForTest(t))
	if err != nil {
		t.Fatalf("load pricing: %v", err)
	}
	h := buildAgentDecisionHandlerWithCost(cons, calc)

	m := goodDecisionProto()
	m.ModelId = "gemini-2.5-flash"
	m.PromptTokens = 1000
	m.CompletionTokens = 200
	m.CachedTokens = 100

	if err := h(context.Background(), decisionProtoMessage(t, m)); err != nil {
		t.Fatalf("handler: %v", err)
	}
	logs, _ := repo.List(context.Background(), "platform", decision.ListFilter{})
	if len(logs) != 1 {
		t.Fatalf("logs = %d; want 1", len(logs))
	}
	l := logs[0]
	if l.CostUsdMicros == nil {
		t.Fatalf("CostUsdMicros nil; want computed from tokens x pricing")
	}
	if *l.CostUsdMicros != 773 {
		t.Errorf("CostUsdMicros = %d; want 773 (0.000773 USD)", *l.CostUsdMicros)
	}
	// Serialization view must be USD.
	if l.CostUSD == nil || *l.CostUSD < 0.000772 || *l.CostUSD > 0.000774 {
		t.Errorf("CostUSD = %v; want ~0.000773", l.CostUSD)
	}
}

// TestBuildAgentDecisionHandler_NoCostCalculatorLeavesCostNil proves the
// binding does NOT invent a cost when no pricing calculator is wired — cost
// stays nil (blank in O+), never a fabricated value.
func TestBuildAgentDecisionHandler_NoCostCalculatorLeavesCostNil(t *testing.T) {
	cons, repo := newAgentDecisionFixture(t)
	h := buildAgentDecisionHandler(cons) // no cost calculator

	m := goodDecisionProto()
	m.PromptTokens = 1000
	m.CompletionTokens = 200
	if err := h(context.Background(), decisionProtoMessage(t, m)); err != nil {
		t.Fatalf("handler: %v", err)
	}
	logs, _ := repo.List(context.Background(), "platform", decision.ListFilter{})
	if len(logs) != 1 {
		t.Fatalf("logs = %d; want 1", len(logs))
	}
	if logs[0].CostUsdMicros != nil {
		t.Errorf("CostUsdMicros = %v; want nil (no calculator → no fabricated cost)", logs[0].CostUsdMicros)
	}
}

// TestBuildAgentDecisionHandler_ExtractsVerdict proves the binding reads the
// qgen quality-gate verdict from the proto field-21 attributes map (key
// "decision", e.g. accepted|rejected|completed_with_warning) and persists it as
// Log.Verdict — the value the O+ DECISION TYPE column renders instead of the
// base `respond` (CHO-1700 follow-up).
func TestBuildAgentDecisionHandler_ExtractsVerdict(t *testing.T) {
	cases := []struct {
		name string
		attr map[string]string
		want string
	}{
		{"accepted", map[string]string{"decision": "accepted"}, "accepted"},
		{"rejected", map[string]string{"decision": "rejected"}, "rejected"},
		{"warning", map[string]string{"decision": "completed_with_warning"}, "completed_with_warning"},
		{"trimmed", map[string]string{"decision": " accepted "}, "accepted"},
		{"absent_key", map[string]string{"attempt_count": "2"}, ""},
		{"nil_map", nil, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cons, repo := newAgentDecisionFixture(t)
			h := buildAgentDecisionHandler(cons)
			m := goodDecisionProto()
			m.Attributes = tc.attr
			if err := h(context.Background(), decisionProtoMessage(t, m)); err != nil {
				t.Fatalf("handler: %v", err)
			}
			logs, _ := repo.List(context.Background(), "platform", decision.ListFilter{})
			if len(logs) != 1 {
				t.Fatalf("logs = %d; want 1", len(logs))
			}
			if logs[0].Verdict != tc.want {
				t.Errorf("Verdict = %q; want %q", logs[0].Verdict, tc.want)
			}
		})
	}
}

// TestBuildAgentDecisionHandler_ExtractsPromptConditions proves the binding
// reads the durable prompt-explainability conditions from the proto field-21
// attributes map (ADR-197 M-A.4): every attribute whose key is prefixed
// `prompt_conditions.` is collected into Log.PromptConditions with the prefix
// stripped (e.g. attributes["prompt_conditions.intent"]="new_question" →
// PromptConditions["intent"]="new_question"). Non-prefixed attributes (incl.
// the question_type/decision/hash keys) are NOT collected; absent → empty map.
func TestBuildAgentDecisionHandler_ExtractsPromptConditions(t *testing.T) {
	cases := []struct {
		name string
		attr map[string]string
		want map[string]string
	}{
		{
			"single",
			map[string]string{"prompt_conditions.intent": "new_question"},
			map[string]string{"intent": "new_question"},
		},
		{
			"multi_and_ignores_others",
			map[string]string{
				"prompt_conditions.intent":     "new_question",
				"prompt_conditions.difficulty": "hard",
				"question_type":                "mcq", // not a prompt condition
				"decision":                     "accepted",
				"input_hash":                   "deadbeef",
			},
			map[string]string{"intent": "new_question", "difficulty": "hard"},
		},
		{"absent", map[string]string{"question_type": "mcq"}, nil},
		{"nil_map", nil, nil},
		{"empty_suffix_ignored", map[string]string{"prompt_conditions.": "x"}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cons, repo := newAgentDecisionFixture(t)
			h := buildAgentDecisionHandler(cons)
			m := goodDecisionProto()
			m.Attributes = tc.attr
			if err := h(context.Background(), decisionProtoMessage(t, m)); err != nil {
				t.Fatalf("handler: %v", err)
			}
			logs, _ := repo.List(context.Background(), "platform", decision.ListFilter{})
			if len(logs) != 1 {
				t.Fatalf("logs = %d; want 1", len(logs))
			}
			got := logs[0].PromptConditions
			if len(got) != len(tc.want) {
				t.Fatalf("PromptConditions = %v; want %v", got, tc.want)
			}
			for k, v := range tc.want {
				if got[k] != v {
					t.Errorf("PromptConditions[%q] = %q; want %q", k, got[k], v)
				}
			}
		})
	}
}

// TestBuildAgentDecisionHandler_ExtractsCitationHashes proves the binding reads
// the producer-computed sha256 citation hashes from the attributes map
// (input_hash / output_hash) and persists them onto the ReasoningSummary, so
// the O+ reasoning panel renders a real PII-safe citation instead of all-zeros.
func TestBuildAgentDecisionHandler_ExtractsCitationHashes(t *testing.T) {
	cons, repo := newAgentDecisionFixture(t)
	h := buildAgentDecisionHandler(cons)
	m := goodDecisionProto()
	ih := strings.Repeat("a", 64)
	oh := strings.Repeat("b", 64)
	m.Attributes = map[string]string{"input_hash": ih, "output_hash": oh}
	if err := h(context.Background(), decisionProtoMessage(t, m)); err != nil {
		t.Fatalf("handler: %v", err)
	}
	logs, _ := repo.List(context.Background(), "platform", decision.ListFilter{})
	if len(logs) != 1 {
		t.Fatalf("logs = %d; want 1", len(logs))
	}
	if logs[0].Reasoning == nil {
		t.Fatal("Reasoning nil; want citation hashes from attributes")
	}
	if logs[0].Reasoning.InputHash != ih || logs[0].Reasoning.OutputHash != oh {
		t.Errorf("hashes = %q/%q; want %q/%q",
			logs[0].Reasoning.InputHash, logs[0].Reasoning.OutputHash, ih, oh)
	}
}

func TestBuildAgentDecisionHandler_KindAndRiskMapping(t *testing.T) {
	cases := []struct {
		kind     observabilityv1.DecisionKind
		guardOut string
		wantType decision.Type
		wantRisk decision.RiskTier
	}{
		{observabilityv1.DecisionKind_DECISION_KIND_BLOCK, "block", decision.TypeRefuse, decision.TierHigh},
		{observabilityv1.DecisionKind_DECISION_KIND_CLASSIFY, "", decision.TypeRoute, decision.TierLow},
		{observabilityv1.DecisionKind_DECISION_KIND_GENERATE, "redact", decision.TypeRespond, decision.TierMedium},
	}
	for _, tc := range cases {
		t.Run(tc.kind.String(), func(t *testing.T) {
			cons, repo := newAgentDecisionFixture(t)
			h := buildAgentDecisionHandler(cons)
			m := goodDecisionProto()
			m.DecisionKind = tc.kind
			m.GuardrailOutcome = tc.guardOut
			if err := h(context.Background(), decisionProtoMessage(t, m)); err != nil {
				t.Fatalf("handler: %v", err)
			}
			logs, _ := repo.List(context.Background(), "platform", decision.ListFilter{})
			if len(logs) != 1 {
				t.Fatalf("logs = %d; want 1", len(logs))
			}
			if logs[0].DecisionType != tc.wantType {
				t.Errorf("decision_type = %q; want %q", logs[0].DecisionType, tc.wantType)
			}
			if logs[0].RiskTier != tc.wantRisk {
				t.Errorf("risk_tier = %q; want %q", logs[0].RiskTier, tc.wantRisk)
			}
		})
	}
}

func TestBuildAgentDecisionHandler_FailsLoudOnBadProto(t *testing.T) {
	cons, _ := newAgentDecisionFixture(t)
	h := buildAgentDecisionHandler(cons)
	msg := &cgcpubsub.Message{
		Topic:           events.TopicAgentDecisionLogged,
		Payload:         []byte{0xff, 0xff, 0xff, 0xff}, // not valid protobuf
		DeliveryAttempt: 1,
	}
	if err := h(context.Background(), msg); err == nil {
		t.Fatal("expected decode error (FAIL LOUD → NACK → DLQ); got nil")
	}
}

func TestStartAgentDecisionSubscriber_NilWhenUnwired(t *testing.T) {
	cons, _ := newAgentDecisionFixture(t)
	if done := startAgentDecisionSubscriber(context.Background(), nil, "", buildAgentDecisionHandler(cons)); done != nil {
		t.Error("expected nil done channel when Pub/Sub client is nil")
	}
}
