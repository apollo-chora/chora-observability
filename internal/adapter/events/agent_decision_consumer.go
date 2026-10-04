// AgentDecisionConsumer projects inbound
// `chora.observability.agent_decision.logged.v1` events into the local
// `agent_decision_log` append-only table.
//
// This is the SUBSCRIBER half of the IMDA D1/D2 transparency audit trail —
// every routing / generate / critique / guardrail-block decision an agent
// makes is persisted so the O+ surface auditor view + chora-governance can
// reconstruct "what the AI did". chora-observability OWNS the
// AgentDecisionLog aggregate; the producers are the AI Kernel orchestrator +
// the content-agent crews (qgen_question / qgen_critic emit one decision per
// generate / critique hop).
//
// Hexagonal:
//   - INBOUND ADAPTER from Pub/Sub (binding lives in cmd/server/agent_decision_binding.go)
//   - depends on decision.Repository (local-domain only — cross-DB queries
//     forbidden per ddd-enforcement HARD RULE)
//   - idempotency via idempotent.Store keyed on event_id (at-least-once
//     delivery means subscribers see duplicates)
//
// ADR-167 Phase 2: the topic is Schema-Registry-bound with encoding=BINARY;
// the binding proto.Unmarshal the v2 AgentDecisionLogged payload (field-21
// `attributes` map carries agent-specific extensions). Decode failure FAILS
// LOUD (NACK → broker retry → DLQ); no JSON fallback, no silent drop.
package events

import (
	"context"
	"errors"
	"fmt"
	stdlog "log"
	"strings"
	"time"

	"github.com/apollo-chora/chora-common/idempotent"
	"github.com/apollo-chora/chora-observability/internal/domain/decision"
)

// TopicAgentDecisionLogged is the canonical inbound topic name.
const TopicAgentDecisionLogged = "chora.observability.agent_decision.logged.v1"

// AgentDecisionInboxTTL mirrors the Pub/Sub max redelivery window.
const AgentDecisionInboxTTL = 7 * 24 * time.Hour

// AgentDecisionLoggedEvent is the language-agnostic representation of an
// inbound `chora.observability.agent_decision.logged.v1` message. Fields map
// from the proto `chora.observability.v1.AgentDecisionLogged` schema; the
// Pub/Sub binding builds one of these per message before calling Handle.
type AgentDecisionLoggedEvent struct {
	EventID       string    // envelope.event_id (dedupe key)
	TenantID      string    // envelope.tenant_id (may be the "platform" sentinel)
	Agid          string    // deciding agent AGID
	DecisionType  string    // canonical decision.Type ("route"|"escalate"|"refuse"|"respond")
	RiskTier      string    // canonical decision.RiskTier ("low"|"medium"|"high"|"critical")
	CorrelationID string    // invocation/crew correlation (non-empty)
	Reason        string    // bounded reason / output summary
	InputSummary  string    // hashed/summarised input (transparency)
	OutputSummary string    // hashed/summarised output (transparency)
	Traceparent   string    // W3C trace context from the envelope
	DecidedAt     time.Time // event-time clock (producer's UTC)

	// --- CHO-1560 +9-field projection (extracted by the binding) ----------
	ModelID          string   // proto f6 — concrete model (→ FE `model`)
	Confidence       *float32 // proto f10 — self-reported [0..1]; nil = absent
	AutonomyLevel    string   // FLAGGED: no proto source yet (empty today)
	CostUsdMicros    *int64   // derived by binding from tokens x pricing; nil = no calc
	CrewName         string   // proto f12
	CrewID           string   // proto f13
	PromptTokens     int64    // proto f18
	CompletionTokens int64    // proto f19
	CachedTokens     int64    // proto f20
	GuardrailOutcome string   // proto f17 — pass|block|redact|""
	QuestionType     string   // proto f21 attributes["question_type"] — mcq|oe|""

	// Verdict is the qgen quality-gate outcome (proto f21 attributes["decision"]
	// — accepted|rejected|completed_with_warning|refused|retry). Distinct from
	// DecisionType (the 4-value ENUM); surfaced in the O+ DECISION TYPE column +
	// reasoning-panel step (CHO-1700 follow-up). Empty for non-verdict decisions.
	Verdict string
	// InputHash / OutputHash are the producer-computed sha256 hex citation
	// hashes (proto f21 attributes["input_hash"]/["output_hash"]). PII-safe —
	// raw content never carried. Empty → consumer keeps the zero sentinel.
	InputHash  string
	OutputHash string

	// PromptConditions is the durable prompt-explainability map (ADR-197 M-A.4):
	// the binding collects every proto field-21 attribute whose key is prefixed
	// `prompt_conditions.` into this map with the prefix stripped. nil/empty =
	// no recorded conditions (never fabricated).
	PromptConditions map[string]string
}

// DecisionBQSink mirrors each persisted decision into the BigQuery analytics
// table (chora_observability_analytics.agent_decision_log) so the O+ "View in
// BigQuery" deep-link + auditor BQ queries see the SAME audit trail the
// Postgres read-model serves — closing the "BQ mirror empty" gap. Best-effort
// by contract: a sink failure must NEVER fail the Pub/Sub ack (Postgres is the
// canonical store). nil = disabled (the pre-export behaviour).
type DecisionBQSink interface {
	Insert(ctx context.Context, l *decision.Log) error
}

// AgentDecisionConsumerConfig wires the consumer.
type AgentDecisionConsumerConfig struct {
	// Repo is the append-only agent-decision repository (local-DB only).
	Repo decision.Repository

	// Inbox provides exactly-once subscriber semantics keyed on event_id.
	// nil → defaults to a fresh in-memory store (dev / test).
	Inbox idempotent.Store

	// BQSink mirrors each persisted decision into BigQuery (best-effort).
	// nil → BQ mirroring disabled.
	BQSink DecisionBQSink
}

// AgentDecisionConsumer projects inbound events into the decision log.
type AgentDecisionConsumer struct {
	repo   decision.Repository
	inbox  idempotent.Store
	bqSink DecisionBQSink
}

// NewAgentDecisionConsumer constructs the consumer. Repo is required.
func NewAgentDecisionConsumer(cfg AgentDecisionConsumerConfig) *AgentDecisionConsumer {
	if cfg.Repo == nil {
		panic("events: AgentDecisionConsumer requires Repo")
	}
	if cfg.Inbox == nil {
		cfg.Inbox = idempotent.NewMemoryStore()
	}
	return &AgentDecisionConsumer{repo: cfg.Repo, inbox: cfg.Inbox, bqSink: cfg.BQSink}
}

// SubscribedTopic returns the canonical inbound topic for binding wiring.
func (c *AgentDecisionConsumer) SubscribedTopic() string {
	return TopicAgentDecisionLogged
}

// Handle processes one inbound event. Idempotent on EventID via the inbox;
// double-delivery is a no-op. Returns an error only when validation fails or
// the persistence layer rejects the write — the caller NACKs so the broker
// retries + bounces to DLQ.
func (c *AgentDecisionConsumer) Handle(ctx context.Context, ev AgentDecisionLoggedEvent) error {
	if err := validateAgentDecisionEvent(ev); err != nil {
		return err
	}
	return c.inbox.Process(ctx, "agent_decision_logged:"+ev.EventID, AgentDecisionInboxTTL, func() error {
		return c.persist(ctx, ev)
	})
}

func (c *AgentDecisionConsumer) persist(ctx context.Context, ev AgentDecisionLoggedEvent) error {
	// Reasoning is optional; attach the bounded output summary + the
	// producer-computed sha256 citation hashes when present so the O+ auditor
	// view + chora-governance retain "what the AI did" AND a PII-safe citation
	// (raw input/output text is never carried — only the one-way hashes). The
	// hashes alone create a reasoning record (a guardrail-blocked decision may
	// cite input/output without any summary text).
	var reasoning *decision.ReasoningSummary
	if ev.InputSummary != "" || ev.OutputSummary != "" || ev.InputHash != "" || ev.OutputHash != "" {
		reasoning = &decision.ReasoningSummary{
			Summary:    ev.OutputSummary,
			InputHash:  ev.InputHash,
			OutputHash: ev.OutputHash,
		}
	}

	log, err := decision.New(decision.NewParams{
		TenantID:         ev.TenantID,
		Agid:             ev.Agid,
		DecisionType:     decision.Type(ev.DecisionType),
		Reason:           ev.Reason,
		RiskTier:         decision.RiskTier(ev.RiskTier),
		CorrelationID:    ev.CorrelationID,
		Traceparent:      ev.Traceparent,
		Reasoning:        reasoning,
		ModelID:          ev.ModelID,
		Confidence:       ev.Confidence,
		AutonomyLevel:    ev.AutonomyLevel,
		CostUsdMicros:    ev.CostUsdMicros,
		CrewName:         ev.CrewName,
		CrewID:           ev.CrewID,
		PromptTokens:     ev.PromptTokens,
		CompletionTokens: ev.CompletionTokens,
		CachedTokens:     ev.CachedTokens,
		GuardrailOutcome: ev.GuardrailOutcome,
		QuestionType:     ev.QuestionType,
		Verdict:          ev.Verdict,
		PromptConditions: ev.PromptConditions,
	})
	if err != nil {
		return fmt.Errorf("agent_decision_consumer.decision.New: %w", err)
	}
	// Bridge storage (raw confidence + cost micros) → serialization fields so
	// the read model emits float confidence + cost_usd regardless of which
	// repository (pg / inmem) round-trips the row.
	log.PopulateProjectionOut()
	// Preserve event-time as recorded_at (decision.New defaults CreatedAt to
	// ingestion time). The Append maps CreatedAt → recorded_at.
	if !ev.DecidedAt.IsZero() {
		log.CreatedAt = ev.DecidedAt
	}
	if err := c.repo.Append(ctx, log); err != nil {
		return fmt.Errorf("agent_decision_consumer.repo.Append: %w", err)
	}
	// Best-effort BigQuery mirror — Postgres is the canonical store, so a sink
	// failure is logged but NEVER fails the ack (would otherwise DLQ a decision
	// that IS persisted). Disabled when no sink is wired (bqSink == nil).
	if c.bqSink != nil {
		if err := c.bqSink.Insert(ctx, log); err != nil {
			stdlog.Printf("agent_decision_consumer: BQ mirror failed (log_id=%s, persisted in pg): %v", log.LogID, err)
		}
	}
	return nil
}

// validateAgentDecisionEvent rejects clearly-malformed inbound events early
// (cheap, no I/O) so a NACK + DLQ on a bad event happens fast.
func validateAgentDecisionEvent(ev AgentDecisionLoggedEvent) error {
	if strings.TrimSpace(ev.EventID) == "" {
		return errors.New("agent_decision_consumer: event_id required")
	}
	if strings.TrimSpace(ev.TenantID) == "" {
		return errors.New("agent_decision_consumer: tenant_id required")
	}
	if strings.TrimSpace(ev.Agid) == "" {
		return errors.New("agent_decision_consumer: agid required")
	}
	if !decision.Type(ev.DecisionType).Valid() {
		return fmt.Errorf("agent_decision_consumer: invalid decision_type %q", ev.DecisionType)
	}
	if !decision.RiskTier(ev.RiskTier).Valid() {
		return fmt.Errorf("agent_decision_consumer: invalid risk_tier %q", ev.RiskTier)
	}
	if strings.TrimSpace(ev.CorrelationID) == "" {
		return errors.New("agent_decision_consumer: correlation_id required")
	}
	return nil
}
