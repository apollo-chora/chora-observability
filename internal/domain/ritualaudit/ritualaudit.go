// Package ritualaudit holds the O+ auditor projection of Grimoire Ritual runs
// (ADR-215 auditor projection / ADR-219 CHO-2016).
//
// chora-consumption publishes chora.consumption.familiar.ritual_run_completed.v1
// when a learner-composed Ritual run reaches a terminal state; chora-
// observability projects each run into the ritual_run_audit table so O+
// auditors get the full run record — terminal status + mana charge + sink +
// the per-step ADR-197 decision stamps.
//
// chora-observability is the canonical owner of this audit projection; this
// package defines the domain row + repository port so adapter packages build
// pgx-backed implementations + in-memory test doubles. Cross-DB queries
// forbidden — the projection reads/writes only chora_observability.
package ritualaudit

import (
	"context"
	"errors"
	"time"
)

// TopicFamiliarRitualRunCompleted is the LEGACY inbound topic this projection
// binds to (published by chora-consumption's RitualRunner.publishCompleted).
// Declared locally — the observability projection never imports the
// chora-consumption domain (a cross-service domain import would break the
// hexagonal boundary; the familiar-growth audit vertical declares its topics
// the same way).
//
// ADR-254 renamed the aggregate to `companion`; the producer now emits
// TopicCompanionRitualRunCompleted. Both are consumed — the companion subject
// carries the live traffic, the familiar subject is retained for any
// straggler producer still on the legacy name.
const TopicFamiliarRitualRunCompleted = "chora.consumption.familiar.ritual_run_completed.v1"

// TopicCompanionRitualRunCompleted is the ADR-254 canonical inbound topic
// (chora-contracts proto/events/consumption/ritual.proto).
const TopicCompanionRitualRunCompleted = "chora.consumption.companion.ritual_run_completed.v1"

// RitualRunAuditRow mirrors one ritual_run_audit row — one row per inbound
// ritual_run_completed event, idempotent on source_event_id.
//
// Stamps is the per-step ADR-197 decision-stamp array (the O+ full record).
// It mirrors chora-consumption familiar_ritual_runs.decision_stamp (migration
// 0071: JSONB DEFAULT '[]') + the RitualRun.Stamps []StepStamp wire type — a
// JSON ARRAY of per-step objects, NOT a single object.
type RitualRunAuditRow struct {
	AuditID       string           `json:"audit_id"`
	TenantID      string           `json:"tenant_id"`
	SourceTopic   string           `json:"source_topic"`
	SourceEventID string           `json:"source_event_id"`
	RunID         string           `json:"run_id"`
	RitualID      string           `json:"ritual_id,omitempty"`
	FamiliarID    string           `json:"familiar_id,omitempty"`
	OwnerGCID     string           `json:"owner_gcid,omitempty"`
	RevisionNo    int32            `json:"revision_no"`
	TriggerSource string           `json:"trigger_source,omitempty"`
	Status        string           `json:"status"`
	ManaCharged   int32            `json:"mana_charged"`
	SinkRef       string           `json:"sink_ref,omitempty"`
	ErrorText     string           `json:"error_text,omitempty"`
	Stamps        []map[string]any `json:"stamps"`
	OccurredAt    time.Time        `json:"occurred_at,omitempty"`
	ReceivedAt    time.Time        `json:"received_at"`
}

// ErrDuplicateSourceEvent signals a UNIQUE collision on source_event_id — the
// consumer treats it as a no-op (idempotent replay).
var ErrDuplicateSourceEvent = errors.New("ritualaudit: duplicate source_event_id")

// ErrInvalidArgument is returned for malformed inputs (blank required fields).
var ErrInvalidArgument = errors.New("ritualaudit: invalid argument")

// Repository is the persistence port for ritual_run_audit. Adapters implement
// it against pgx (production) or in-memory (tests + no-DSN dev).
type Repository interface {
	// Ingest inserts one audit row. Returns ErrDuplicateSourceEvent when
	// SourceEventID already exists — the caller MAY treat it as a no-op.
	Ingest(ctx context.Context, row RitualRunAuditRow) error

	// List returns the tenant's most-recent ritual-run audit rows, ordered
	// received_at DESC, capped at limit.
	List(ctx context.Context, tenantID string, limit int) ([]RitualRunAuditRow, error)
}
