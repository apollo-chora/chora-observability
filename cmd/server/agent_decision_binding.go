// agent_decision_binding.go — JetStream binding for the
// chora.observability.agent_decision.logged.v1 consumer (ADR-167 read-model
// hydration, 2026-05-29).
//
// Mirrors token_usage_binding.go. The producer is the AI Kernel orchestrator
// + content-agent crews (qgen_question / qgen_critic). The subscription
// `chora-observability.observability-agent_decision-logged` is provisioned
// in chora-infra terraform; the v2 topic schema is Schema-Registry-bound with
// encoding=BINARY (field-21 `attributes` map per ADR-167 D5).
//
// ADR-167 Phase 2: proto.Unmarshal failure FAILS LOUD (NAK → broker retry →
// DLQ); no JSON fallback, no silent drop.
//
// D6 4-pillar contract (consumer-side): identical to the token_usage
// subscriber — at-least-once delivery + idempotent.Store dedupe on event_id +
// per-tenant decision.Append (RLS-scoped via WithTenantTx) + W3C traceparent
// persisted on the row.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/apollo-chora/chora-common/eventbus"
	observabilityv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/observability/v1"
	"github.com/apollo-chora/chora-observability/internal/adapter/events"
	"github.com/apollo-chora/chora-observability/internal/domain/decision"
	"github.com/apollo-chora/chora-observability/internal/domain/pricing"
)

// decisionCostCalculator derives a per-decision cost (in 1e-6 USD micros) from
// the proto's token counts (prompt/completion/cached) + model_id, using the
// versioned pricing config (config/pricing.yaml). The proto AgentDecisionLogged
// carries NO cost field — cost is computed here at projection time, matching
// how the Model Gateway computes ledger cost (pricing is data, not code).
//
// ComputeMicros returns (nil, false) when the model is unknown to the pricing
// table or all token counts are zero — the binding then leaves cost_usd nil
// (blank in O+) rather than fabricating a value.
type decisionCostCalculator struct {
	cfg *pricing.Config
}

// newDecisionCostCalculatorFromFile loads the pricing YAML at path.
func newDecisionCostCalculatorFromFile(path string) (*decisionCostCalculator, error) {
	cfg, err := pricing.LoadFile(path)
	if err != nil {
		return nil, err
	}
	return &decisionCostCalculator{cfg: cfg}, nil
}

// ComputeMicros returns the cost micros for the given model + token counts.
// The bool is false when no priced cost can be computed (unknown model, or
// zero tokens) — caller leaves cost nil rather than recording $0 as if real.
func (c *decisionCostCalculator) ComputeMicros(modelID string, prompt, completion, cached int64) (int64, bool) {
	if c == nil || c.cfg == nil || modelID == "" {
		return 0, false
	}
	if prompt == 0 && completion == 0 && cached == 0 {
		return 0, false
	}
	price, ok := c.cfg.Prices[modelID]
	if !ok {
		return 0, false
	}
	micros, err := pricing.ComputeCostMicros(price, prompt, completion, cached)
	if err != nil {
		return 0, false
	}
	return micros, true
}

// DefaultAgentDecisionSubscription is the canonical subscription short-name.
const DefaultAgentDecisionSubscription = "chora-observability.observability-agent_decision-logged"

// decisionKindToType maps the proto DecisionKind enum to the canonical
// decision.Type domain value (route | escalate | refuse | respond).
//
//	CLASSIFY / ROUTE        → route    (classifier / router agents)
//	GENERATE / CRITIQUE / REFINE → respond (produced an output / verdict)
//	BLOCK                   → refuse   (guardrail block)
//	UNSPECIFIED / default   → respond
func decisionKindToType(k observabilityv1.DecisionKind) decision.Type {
	switch k {
	case observabilityv1.DecisionKind_DECISION_KIND_CLASSIFY,
		observabilityv1.DecisionKind_DECISION_KIND_ROUTE:
		return decision.TypeRoute
	case observabilityv1.DecisionKind_DECISION_KIND_BLOCK:
		return decision.TypeRefuse
	default:
		// GENERATE / CRITIQUE / REFINE / UNSPECIFIED → respond
		return decision.TypeRespond
	}
}

// riskTierFromGuardrail derives a decision.RiskTier from the guardrail
// outcome. A BLOCK is a high-risk event; everything else defaults to low.
// (The proto core carries no explicit risk_tier — D5 attributes may, but the
// generic projection stays conservative.)
func riskTierFromGuardrail(outcome string) decision.RiskTier {
	switch outcome {
	case "block":
		return decision.TierHigh
	case "redact":
		return decision.TierMedium
	default:
		return decision.TierLow
	}
}

// buildAgentDecisionHandler returns a handler with NO cost calculator wired —
// cost_usd stays nil (blank in O+) for every decision. Used by dev / test
// paths where pricing.yaml is not loaded. Production wires
// buildAgentDecisionHandlerWithCost in main.go.
func buildAgentDecisionHandler(cons *events.AgentDecisionConsumer) eventbus.Handler {
	return buildAgentDecisionHandlerWithCost(cons, nil)
}

// buildAgentDecisionHandlerWithCost returns an eventbus.Handler that
// proto.Unmarshal the BINARY-protobuf AgentDecisionLogged payload into the
// consumer's typed event + calls AgentDecisionConsumer.Handle. Extracted as a
// free function so a unit test can call it without a broker connection or
// goroutine.
//
// CHO-1560: extracts the +9 projection fields (model_id / confidence / crew_* /
// token counts / guardrail_outcome) and, when calc != nil, derives
// cost_usd_micros from the token counts x pricing. calc == nil → cost stays nil
// (never fabricated).
func buildAgentDecisionHandlerWithCost(cons *events.AgentDecisionConsumer, calc *decisionCostCalculator) eventbus.Handler {
	if cons == nil {
		panic("agent_decision_binding: nil AgentDecisionConsumer")
	}
	return func(ctx context.Context, msg eventbus.Message) error {
		var rec observabilityv1.AgentDecisionLogged
		if err := proto.Unmarshal(msg.Payload, &rec); err != nil {
			return fmt.Errorf("agent_decision_binding: proto decode payload: %w", err)
		}

		penv := rec.GetEnvelope()
		attrs := msg.Envelope

		eventID := firstNonBlank(penv.GetEventId(), rec.GetDecisionId(), attrs.EventID)
		tenantID := firstNonBlank(penv.GetTenantId(), attrs.TenantID)
		traceparent := firstNonBlank(penv.GetTraceparent(), attrs.Traceparent)
		// correlation_id is required by decision.New — fall back through the
		// invocation/crew/decision identifiers so a real decision is never
		// rejected for a blank correlation.
		correlationID := firstNonBlank(rec.GetInvocationId(), rec.GetCrewId(), rec.GetDecisionId(), eventID)

		var decidedAt time.Time
		if t := rec.GetDecidedAt(); t != nil {
			decidedAt = t.AsTime()
		} else if t := penv.GetOccurredAt(); t != nil {
			decidedAt = t.AsTime()
		} else {
			decidedAt = attrs.OccurredAt
		}

		// confidence: proto f10 is a float32 — preserve null-distinction by
		// only setting a pointer when the field is meaningfully present. The
		// proto3 scalar default is 0.0; we treat exact-0.0 as "absent" so a
		// non-confidence decision doesn't render a misleading 0% (a real 0.0
		// confidence is indistinguishable from unset on the wire for a proto3
		// non-optional scalar — documented limitation).
		var confidence *float32
		if c := rec.GetConfidence(); c != 0 {
			cc := c
			confidence = &cc
		}

		prompt := rec.GetPromptTokens()
		completion := rec.GetCompletionTokens()
		cached := rec.GetCachedTokens()

		// cost: the proto carries no cost field — derive from tokens x pricing
		// when a calculator is wired + the model is priced. Otherwise leave nil
		// (blank cost in O+; never a fabricated value).
		var costMicros *int64
		if calc != nil {
			if micros, ok := calc.ComputeMicros(rec.GetModelId(), prompt, completion, cached); ok {
				costMicros = &micros
			}
		}

		// question_type: the qgen crew rides its per-question-type tag in the
		// proto field-21 attributes map (ADR-167 D5) under key "question_type"
		// ("mcq"|"oe"). Reading a nil map is safe in Go (returns ""); trim to
		// guard against stray whitespace. Empty for non-question decisions —
		// the O+ qgen crew renders qgen-mcq / qgen-OE from this split.
		questionType := strings.TrimSpace(rec.GetAttributes()["question_type"])

		// verdict + citation hashes ride the SAME field-21 attributes map
		// (ADR-167 D5). The verdict (attributes["decision"], e.g.
		// accepted|rejected|completed_with_warning) is the human-meaningful
		// outcome the O+ DECISION TYPE column renders — distinct from the
		// 4-value decision_type ENUM. input_hash/output_hash are the
		// producer-computed PII-safe sha256 citation. Reading a nil map is safe
		// (returns ""); trim guards stray whitespace. CHO-1700 follow-up.
		attrMap := rec.GetAttributes()
		verdict := strings.TrimSpace(attrMap["decision"])
		inputHash := strings.TrimSpace(attrMap["input_hash"])
		outputHash := strings.TrimSpace(attrMap["output_hash"])

		// prompt_conditions: the durable prompt-explainability conditions ride
		// the SAME field-21 attributes map (ADR-197 M-A.4) under keys prefixed
		// `prompt_conditions.` (e.g. "prompt_conditions.intent"="new_question").
		// Collect them into a map with the prefix stripped. Absent → nil map
		// (blank conditions in O+; never a fabricated value).
		promptConditions := extractPromptConditions(attrMap)

		ev := events.AgentDecisionLoggedEvent{
			EventID:       eventID,
			TenantID:      tenantID,
			Agid:          rec.GetAgid(),
			DecisionType:  string(decisionKindToType(rec.GetDecisionKind())),
			RiskTier:      string(riskTierFromGuardrail(rec.GetGuardrailOutcome())),
			CorrelationID: correlationID,
			Reason:        rec.GetOutputSummary(),
			InputSummary:  rec.GetInputSummary(),
			OutputSummary: rec.GetOutputSummary(),
			Traceparent:   traceparent,
			DecidedAt:     decidedAt,
			// --- CHO-1560 +9-field projection ----------------------------
			ModelID:          rec.GetModelId(),
			Confidence:       confidence,
			CostUsdMicros:    costMicros,
			CrewName:         rec.GetCrewName(),
			CrewID:           rec.GetCrewId(),
			PromptTokens:     prompt,
			CompletionTokens: completion,
			CachedTokens:     cached,
			GuardrailOutcome: normalizeGuardrailOutcome(rec.GetGuardrailOutcome()),
			QuestionType:     questionType,
			Verdict:          verdict,
			InputHash:        inputHash,
			OutputHash:       outputHash,
			PromptConditions: promptConditions,
			// AutonomyLevel: FLAGGED — proto has no autonomy field; left empty
			// so the gateway's deriveHITLStatus reads "" rather than a fake tag.
		}
		return cons.Handle(ctx, ev)
	}
}

// promptConditionsPrefix is the attribute-key prefix the producer uses to carry
// durable prompt-explainability conditions in the proto field-21 attributes map
// (ADR-197 M-A.4). The binding strips this prefix when building the conditions map.
const promptConditionsPrefix = "prompt_conditions."

// extractPromptConditions collects every attribute whose key is prefixed
// `prompt_conditions.` into a map with the prefix stripped (ADR-197 M-A.4).
// Reading a nil map is safe in Go. Returns nil (not an empty map) when no
// prompt-condition keys are present, so the read model records SQL NULL rather
// than a fabricated empty object. A bare `prompt_conditions.` key (empty suffix)
// is ignored — it carries no condition name.
func extractPromptConditions(attrs map[string]string) map[string]string {
	var out map[string]string
	for k, v := range attrs {
		if !strings.HasPrefix(k, promptConditionsPrefix) {
			continue
		}
		key := strings.TrimSpace(strings.TrimPrefix(k, promptConditionsPrefix))
		if key == "" {
			continue
		}
		if out == nil {
			out = make(map[string]string)
		}
		out[key] = v
	}
	return out
}

// normalizeGuardrailOutcome maps the proto guardrail verdict onto the domain's
// accepted set (pass|block|redact|""). Unknown / "unspecified" values collapse
// to "" so decision.New (which validates against the DB CHECK domain) accepts
// the row rather than NACKing a real decision over a stray verdict string.
func normalizeGuardrailOutcome(s string) string {
	switch s {
	case "pass", "block", "redact":
		return s
	default:
		return ""
	}
}

// startAgentDecisionSubscriber starts a JetStream consume-loop goroutine
// bound to the canonical agent_decision subscription. Returns nil when the
// event bus or the handler is unwired (tests).
//
// The supplied handler is the ADR-167 Plane-4 quarantine-wrapped
// buildAgentDecisionHandler (see main.go) — malformed inbound events fail
// loud (local dead-letter + error log + governance alert + broker Nak).
func startAgentDecisionSubscriber(
	ctx context.Context,
	bus eventbus.Subscriber,
	subscription string,
	handler eventbus.Handler,
) chan struct{} {
	if bus == nil || handler == nil {
		return nil
	}
	if subscription == "" {
		subscription = DefaultAgentDecisionSubscription
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		log.Printf(
			"observability: agent_decision subscriber started (subscription=%s, topic=%s)",
			subscription, events.TopicAgentDecisionLogged,
		)
		err := bus.Subscribe(ctx, consumerConfig(subscription, events.TopicAgentDecisionLogged), handler)
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			log.Printf("observability: agent_decision subscriber exited: %v", err)
		}
	}()
	return done
}
