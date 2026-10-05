// TokenUsageConsumer projects inbound `chora.observability.token_usage.recorded.v1`
// events into the local `token_usage_ledger` append-only table.
//
// Per [[ai-cost-tracking]] this is the SUBSCRIBER half of the dual-axis
// cost-tracking pattern — the canonical billing-grade ledger writes that
// BigQuery streaming + pre-aggregations downstream consume. The chora-
// observability service OWNS the ledger aggregate; the chora-ai-kernel-
// orchestrator is one of the PRODUCERS (qgen 2-agent crew emits one row
// per generate / critique trace hop).
//
// Hexagonal:
//   - INBOUND ADAPTER from Pub/Sub (binding lives in cmd/server/main.go)
//   - depends on ledger.Repository (local-domain only — no cross-DB queries
//     per ddd-enforcement HARD RULE)
//   - idempotency via idempotent.Store keyed on event_id (per [[data-
//     consistency]]: at-least-once delivery means subscribers see duplicates).
//
// W3C trace-context handling per
// `.claude/skills/ai-observability-cloud-trace`:
//
//   - Producer-side emitter stamps `traceparent` on the event envelope.
//   - Subscriber parses the 32-hex trace_id + 16-hex span_id and persists
//     them on the ledger row for OTLP cost-trace correlation.
//   - Malformed / absent traceparent → synthesise non-zero stubs (the
//     domain rejects all-zero W3C reserved values).
//
// Per the proto §61-71: `gcid` may carry either a learner GCID or an agent
// AGID. The subscriber preserves the value verbatim; chora_identity lookup
// disambiguates downstream. The proto's `agent_role` field maps to the
// ledger Entry's `Agid` field for agent-attributed cost.
package events

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"strings"
	"time"

	"github.com/apollo-chora/chora-common/idempotent"
	"github.com/apollo-chora/chora-observability/internal/domain/ledger"
)

// TopicTokenUsageRecorded is the canonical inbound topic name per
// `docs/m13/contracts-reconciliation-2026-05-09.md` §1 +
// `ledger.CanonicalTokenUsageTopic`.
const TopicTokenUsageRecorded = "chora.observability.token_usage.recorded.v1"

// TokenUsageInboxTTL is the dedupe-key retention window for the consumer's
// inbox. 7d mirrors Pub/Sub max redelivery window.
const TokenUsageInboxTTL = 7 * 24 * time.Hour

// TokenUsageRecordedEvent is the language-agnostic representation of an
// inbound `chora.observability.token_usage.recorded.v1` message. The
// Pub/Sub adapter (Cloud or in-memory) builds one of these per message
// before calling Handle — fields map 1:1 to the proto
// `chora.observability.v1.TokenUsageRecorded` schema in
// `chora-contracts/proto/events/observability/token_usage.proto`.
type TokenUsageRecordedEvent struct {
	EventID      string    // envelope.event_id (UUIDv7 — dedupe key)
	TenantID     string    // envelope.tenant_id
	GCID         string    // envelope.gcid (may carry AGID — disambiguate via chora_identity)
	ModelID      string    // e.g. "vertex_ai/gemini-2.5-flash", "vertex_ai/gemini-2.5-pro"
	AgentRole    string    // crew role — "qgen_question" | "qgen_critic" | "" for direct calls
	InputTokens  int64     // prompt tokens
	OutputTokens int64     // completion tokens
	CachedTokens int64     // discounted-billing prompt cache
	CostMicros   int64     // canonical cost in 1e-6 USD
	InvocationID string    // correlation back to AI Kernel invocation chain (assist_id)
	Traceparent  string    // W3C trace context propagated from producer
	Tracestate   string    // W3C vendor-specific extension; optional
	OccurredAt   time.Time // event-time clock (publisher's UTC)
}

// TokenUsageConsumerConfig wires the consumer.
type TokenUsageConsumerConfig struct {
	// Repo is the append-only ledger repository (local-DB only).
	Repo ledger.Repository

	// Inbox provides exactly-once subscriber semantics keyed on event_id.
	// nil → defaults to a fresh in-memory store (dev / test).
	Inbox idempotent.Store

	// Now is the wall-clock injection for tests. nil → time.Now().UTC().
	Now func() time.Time
}

// TokenUsageConsumer projects inbound events into the ledger.
type TokenUsageConsumer struct {
	repo  ledger.Repository
	inbox idempotent.Store
	now   func() time.Time
}

// NewTokenUsageConsumer constructs the consumer. Repo is required; the
// other fields default per TokenUsageConsumerConfig.
func NewTokenUsageConsumer(cfg TokenUsageConsumerConfig) *TokenUsageConsumer {
	if cfg.Repo == nil {
		panic("events: TokenUsageConsumer requires Repo")
	}
	if cfg.Inbox == nil {
		cfg.Inbox = idempotent.NewMemoryStore()
	}
	if cfg.Now == nil {
		cfg.Now = func() time.Time { return time.Now().UTC() }
	}
	return &TokenUsageConsumer{
		repo:  cfg.Repo,
		inbox: cfg.Inbox,
		now:   cfg.Now,
	}
}

// SubscribedTopic returns the canonical inbound topic for binding wiring.
func (c *TokenUsageConsumer) SubscribedTopic() string {
	return TopicTokenUsageRecorded
}

// Handle processes one inbound event. Idempotent on EventID via the inbox;
// double-delivery is a no-op. Returns an error only when the underlying
// persistence layer fails — validation errors are returned to the caller
// so it can NACK the Pub/Sub message + bounce to DLQ.
func (c *TokenUsageConsumer) Handle(ctx context.Context, ev TokenUsageRecordedEvent) error {
	if err := validateTokenUsageEvent(ev); err != nil {
		return err
	}
	return c.inbox.Process(ctx, "token_usage_recorded:"+ev.EventID, TokenUsageInboxTTL, func() error {
		return c.persist(ctx, ev)
	})
}

func (c *TokenUsageConsumer) persist(ctx context.Context, ev TokenUsageRecordedEvent) error {
	traceID, spanID := deriveTraceIDs(ev.Traceparent, ev.EventID)

	recordedAt := ev.OccurredAt
	if recordedAt.IsZero() {
		recordedAt = c.now()
	}

	// AGID slot on the ledger row carries the crew agent_role for agent-
	// attributed cost (per proto §61-71 + ledger Entry.Agid semantics).
	// When the producer didn't stamp a role (direct user call), AGID stays
	// empty — ledger.New rejects only zero-trace, never empty AGID.
	entry, err := ledger.New(ledger.NewParams{
		TenantID:         ev.TenantID,
		Gcid:             ev.GCID,
		Agid:             ev.AgentRole,
		ModelID:          ev.ModelID,
		PromptTokens:     int(ev.InputTokens),
		CompletionTokens: int(ev.OutputTokens),
		CachedTokens:     int(ev.CachedTokens),
		CostUsdMicros:    ev.CostMicros,
		TraceID:          traceID,
		SpanID:           spanID,
		RecordedAt:       recordedAt,
	})
	if err != nil {
		return fmt.Errorf("token_usage_consumer.ledger.New: %w", err)
	}
	if err := c.repo.Append(ctx, entry); err != nil {
		return fmt.Errorf("token_usage_consumer.repo.Append: %w", err)
	}
	return nil
}

// validateTokenUsageEvent rejects clearly-malformed inbound events early.
// Validation is intentionally cheap (no I/O) so a NACK + DLQ on a bad
// event happens fast.
func validateTokenUsageEvent(ev TokenUsageRecordedEvent) error {
	if strings.TrimSpace(ev.EventID) == "" {
		return errors.New("token_usage_consumer: event_id required")
	}
	if strings.TrimSpace(ev.TenantID) == "" {
		return errors.New("token_usage_consumer: tenant_id required")
	}
	if strings.TrimSpace(ev.GCID) == "" {
		return errors.New("token_usage_consumer: gcid required")
	}
	if strings.TrimSpace(ev.ModelID) == "" {
		return errors.New("token_usage_consumer: model_id required")
	}
	if ev.InputTokens < 0 {
		return fmt.Errorf("token_usage_consumer: input_tokens negative: %d", ev.InputTokens)
	}
	if ev.OutputTokens < 0 {
		return fmt.Errorf("token_usage_consumer: output_tokens negative: %d", ev.OutputTokens)
	}
	if ev.CostMicros < 0 {
		return fmt.Errorf("token_usage_consumer: cost_micros negative: %d", ev.CostMicros)
	}
	return nil
}

// deriveTraceIDs parses the W3C traceparent header. On parse failure (or
// missing header), synthesise non-zero 32-hex / 16-hex stubs from a hash of
// the event_id so the ledger domain's all-zero-rejecting invariant is
// honoured AND replay is stable (same event_id → same stub).
//
// W3C format: `00-{trace_id_32hex}-{span_id_16hex}-{flags_2hex}`
func deriveTraceIDs(traceparent, eventID string) (string, string) {
	parts := strings.Split(traceparent, "-")
	if len(parts) == 4 {
		tid, sid := strings.ToLower(parts[1]), strings.ToLower(parts[2])
		if ledger.ValidateTraceID(tid) == nil && ledger.ValidateSpanID(sid) == nil {
			return tid, sid
		}
	}
	return synthesiseTraceID(eventID), synthesiseSpanID(eventID)
}

// synthesiseTraceID builds a deterministic 32-hex stub from event_id. Two
// 64-bit FNV-1a hashes concatenated; collisions across distinct event_ids
// are astronomically unlikely. Guaranteed non-zero (we OR a marker byte
// to dodge the all-zero W3C-reserved sentinel).
func synthesiseTraceID(eventID string) string {
	h1 := fnv.New64a()
	_, _ = h1.Write([]byte("trace:" + eventID))
	h2 := fnv.New64a()
	_, _ = h2.Write([]byte("trace2:" + eventID))
	v1 := h1.Sum64() | 1 // dodge zero
	v2 := h2.Sum64() | 1
	return fmt.Sprintf("%016x%016x", v1, v2)
}

func synthesiseSpanID(eventID string) string {
	h := fnv.New64a()
	_, _ = h.Write([]byte("span:" + eventID))
	v := h.Sum64() | 1
	return fmt.Sprintf("%016x", v)
}
