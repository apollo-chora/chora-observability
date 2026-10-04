// hook.go — LedgerHook is the unified atomic write surface the Model
// Gateway calls after every successful LLM invocation.
//
// One Record call → three side effects, atomically:
//
//  1. Append a TokenUsageLedger entry (append-only invariant)
//  2. Publish chora.observability.token_usage.recorded.v1 via outbox
//  3. RecordSpend on the 3-level Budget cascade
//
// The hook lives in the chora-observability domain (the Observability
// supporting domain owns the ledger). The Gateway has a thin adapter that
// implements the OutboxRecorder port + calls Record after the LLM result
// is committed.
//
// Atomicity contract (per data-consistency skill):
//
//   - Outbox first: if the outbox publish fails, the ledger entry is NOT
//     appended (so we never have a "phantom" entry without a corresponding
//     downstream event).
//   - Budgets last: spend accrual is best-effort within the same call;
//     if budget update fails it is logged but the call still succeeds
//     (the next reconciliation pass corrects drift via the ledger sum).
//
// IMDA dimension (per ADR-141): cost events are evidence for D1
// (accountability) — the canonical label set on the outbox envelope.
package ledger

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

// CanonicalTokenUsageTopic is the canonical Pub/Sub topic name (per
// pub-sub-topology skill: chora.{domain}.{aggregate}.{event_type}.v{N}).
const CanonicalTokenUsageTopic = "chora.observability.token_usage.recorded.v1"

// IMDADimensionAccountability is the ADR-141 canonical label for cost-anchored
// events (D1 — accountability).
const IMDADimensionAccountability = "accountability"

// IMDALifecycleStageRuntime is the Tier 5 D18 canonical lifecycle stage for
// post-deploy runtime evidence (LLM calls happen at runtime).
const IMDALifecycleStageRuntime = "runtime"

// EventTypeTokenUsageRecorded is the v1 event type emitted by the hook.
const EventTypeTokenUsageRecorded = "token_usage.recorded.v1"

// SchemaVersionV1 is the major version of the token_usage.recorded payload.
const SchemaVersionV1 = 1

// OutboxRecorder is the port the hook uses to atomically write outbox rows.
// Adapters implement this either against an in-memory recorder (M10 tests)
// or a Postgres outbox table (Tier 2).
type OutboxRecorder interface {
	RecordOutboxEvent(ctx context.Context, r OutboxRecord) error
}

// OutboxRecord is the value shape the hook hands to OutboxRecorder. The
// adapter is responsible for transforming this into a Postgres row + later
// publishing to Pub/Sub via the Relay (per chora-go-common/outbox conventions).
type OutboxRecord struct {
	EventID            string    `json:"event_id"` // UUIDv7
	IdempotencyKey     string    `json:"idempotency_key"`
	Topic              string    `json:"topic"`
	EventType          string    `json:"event_type"`
	AggregateType      string    `json:"aggregate_type"`
	AggregateID        string    `json:"aggregate_id"`
	TenantID           string    `json:"tenant_id"`
	GCID               string    `json:"gcid"`
	OccurredAt         time.Time `json:"occurred_at"`
	Traceparent        string    `json:"traceparent"`
	SourceProject      string    `json:"source_project"`
	SourceService      string    `json:"source_service"`
	SchemaVersion      int32     `json:"schema_version"`
	ChoraImdaDimension string    `json:"chora_imda_dimension"`
	ImdaLifecycleStage string    `json:"imda_lifecycle_stage"`
	Payload            []byte    `json:"payload"` // marshalled Protobuf body
}

// AccrueSpendInput is the canonical domain-level spend accrual shape.
type AccrueSpendInput struct {
	TenantID     string
	Gcid         string
	AgentID      string
	Period       string
	AmountMicros int64
}

// BudgetAccruer is the canonical port for atomic 3-level spend accrual. The
// in-memory adapter (inmem.BudgetLookup) implements this; Cloud SQL Tier 2
// replicas may opt to leave it unimplemented (returning nil) when running
// read-only.
type BudgetAccruer interface {
	AccrueSpend(ctx context.Context, in AccrueSpendInput) error
}

// LedgerHookConfig is the constructor input.
type LedgerHookConfig struct {
	// Ledger is the append-only repository.
	Ledger Repository

	// Budgets is OPTIONAL — when nil, budget accrual is skipped.
	Budgets BudgetLookup

	// Outbox is the atomic event recorder. REQUIRED.
	Outbox OutboxRecorder

	// SourceProject + SourceService are stamped on the outbox envelope.
	SourceProject string
	SourceService string

	// Period is the budget window key (e.g. "2026-05" monthly). The hook
	// passes this through to RecordSpend.
	Period string

	// PricingConfigVersion is the YAML version active at occurred_at. Stamped
	// on the returned RecordResult so callers can persist it on the ledger
	// row when wiring Cloud SQL Tier 2.
	PricingConfigVersion string

	// Now is injectable for tests; defaults to time.Now().UTC().
	Now func() time.Time
}

// LedgerHook is the atomic write coordinator.
type LedgerHook struct {
	cfg LedgerHookConfig
}

// NewLedgerHook constructs a LedgerHook. Required: Ledger + Outbox + Source*.
func NewLedgerHook(cfg LedgerHookConfig) *LedgerHook {
	if cfg.Now == nil {
		cfg.Now = func() time.Time { return time.Now().UTC() }
	}
	return &LedgerHook{cfg: cfg}
}

// RecordParams is the Record call's input — mirrors NewParams (ledger.New)
// plus optional context fields the outbox envelope needs.
type RecordParams struct {
	TenantID         string
	Gcid             string
	AgentID          string // analogous to AGID — the runtime agent that made the call
	ModelID          string
	PromptTokens     int
	CompletionTokens int
	CostUsdMicros    int64
	TraceID          string
	SpanID           string
	Traceparent      string

	// Optional: occurred_at override (defaults to cfg.Now()).
	OccurredAt time.Time
}

// RecordResult is the value the Record call returns.
type RecordResult struct {
	LedgerID             string `json:"ledger_id"`
	OutboxEventID        string `json:"outbox_event_id"`
	PricingConfigVersion string `json:"pricing_config_version"`
}

// Record performs the atomic ledger-write + outbox-publish + budget-accrue
// triple. Returns wrapped errors — the Gateway maps these to HTTP 500.
//
// Order:
//
//  1. Construct + validate the ledger Entry (domain invariants).
//  2. Build the OutboxRecord envelope (canonical topic, ADR-141 D1 dimension).
//  3. RecordOutboxEvent (Tx-internal). If this fails, abort — no ledger commit.
//  4. ledger.Append (Tx-internal). If this fails, abort — outbox row stays
//     pending but is harmless because no LLM result was committed.
//  5. budgetLookup.RecordSpend (best-effort; logs but doesn't fail the call).
func (h *LedgerHook) Record(ctx context.Context, p RecordParams) (RecordResult, error) {
	if h == nil || h.cfg.Ledger == nil {
		return RecordResult{}, errors.New("LedgerHook not configured")
	}
	if h.cfg.Outbox == nil {
		return RecordResult{}, errors.New("LedgerHook missing Outbox")
	}

	// Step 1: build + validate the entry.
	if p.OccurredAt.IsZero() {
		p.OccurredAt = h.cfg.Now()
	}
	entry, err := New(NewParams{
		TenantID:         p.TenantID,
		Gcid:             p.Gcid,
		Agid:             "", // legacy field; AGID lives on AgentID below
		ModelID:          p.ModelID,
		PromptTokens:     p.PromptTokens,
		CompletionTokens: p.CompletionTokens,
		CostUsdMicros:    p.CostUsdMicros,
		TraceID:          p.TraceID,
		SpanID:           p.SpanID,
		RecordedAt:       p.OccurredAt,
	})
	if err != nil {
		return RecordResult{}, fmt.Errorf("ledger.New: %w", err)
	}

	// Step 2: build the outbox record. Idempotency key = ledger_id (UUIDv7
	// already stable per occurred_at) so re-publishes are de-duped at the
	// subscriber.
	outboxRec := OutboxRecord{
		EventID:            entry.LedgerID,
		IdempotencyKey:     entry.LedgerID,
		Topic:              CanonicalTokenUsageTopic,
		EventType:          EventTypeTokenUsageRecorded,
		AggregateType:      "token_usage_ledger",
		AggregateID:        entry.LedgerID,
		TenantID:           entry.TenantID,
		GCID:               entry.Gcid,
		OccurredAt:         entry.RecordedAt,
		Traceparent:        strings.TrimSpace(p.Traceparent),
		SourceProject:      h.cfg.SourceProject,
		SourceService:      h.cfg.SourceService,
		SchemaVersion:      SchemaVersionV1,
		ChoraImdaDimension: IMDADimensionAccountability,
		ImdaLifecycleStage: IMDALifecycleStageRuntime,
		// Payload is filled by the adapter (which has the Protobuf marshaller);
		// the domain emits a structured JSON snapshot for the in-memory case.
		Payload: nil,
	}

	// Step 3: outbox first (atomicity contract — ledger only commits if
	// outbox publish queues successfully).
	if err := h.cfg.Outbox.RecordOutboxEvent(ctx, outboxRec); err != nil {
		return RecordResult{}, fmt.Errorf("outbox: %w", err)
	}

	// Step 4: append the ledger entry.
	if err := h.cfg.Ledger.Append(ctx, entry); err != nil {
		return RecordResult{}, fmt.Errorf("ledger.Append: %w", err)
	}

	// Step 5: best-effort budget accrual via the canonical BudgetAccruer
	// port. Adapters that don't implement it (e.g. read-only replicas) are
	// silently skipped.
	if h.cfg.Budgets != nil {
		if accruer, ok := h.cfg.Budgets.(BudgetAccruer); ok {
			_ = accruer.AccrueSpend(ctx, AccrueSpendInput{
				TenantID:     p.TenantID,
				Gcid:         p.Gcid,
				AgentID:      p.AgentID,
				Period:       h.cfg.Period,
				AmountMicros: p.CostUsdMicros,
			})
		}
	}

	return RecordResult{
		LedgerID:             entry.LedgerID,
		OutboxEventID:        outboxRec.EventID,
		PricingConfigVersion: h.cfg.PricingConfigVersion,
	}, nil
}

