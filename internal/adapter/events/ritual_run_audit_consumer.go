// ritual_run_audit_consumer.go — projects inbound
// `chora.consumption.familiar.ritual_run_completed.v1` events into the local
// `ritual_run_audit` table (ADR-215 auditor projection / ADR-219 CHO-2016).
//
// chora-consumption's RitualRunner.publishCompleted emits one event per
// terminal ritual run (completed / failed / blocked / skipped_budget) carrying
// the terminal projection fields + the per-step ADR-197 decision stamps (the
// O+ full record). chora-observability OWNS the audit projection.
//
// PULL consumer — the service subscribes to its own subscription (NOT a
// gateway push). Mirrors events.TokenUsageConsumer /
// events.AgentDecisionConsumer; the payload is canonical BINARY protobuf
// (consumptionv1.RitualRunCompleted — chora-consumption's outbox encoder,
// CHO-2136) with a JSON fallback for legacy rows still draining at cutover,
// decoded like events.ClosurePullHandler.
//
// Hexagonal:
//   - INBOUND ADAPTER from the NATS JetStream event bus (binding lives in
//     cmd/server).
//   - depends on ritualaudit.Repository (local-DB only — no cross-DB queries
//     per ddd-enforcement HARD RULE).
//   - idempotency via idempotent.Store keyed on event_id (at-least-once
//     delivery → subscribers see duplicates); the audit table's
//     UNIQUE(source_event_id) is the durable second guard.
package events

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	consumptionv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/consumption/v1"

	"github.com/apollo-chora/chora-common/eventbus"
	"github.com/apollo-chora/chora-common/idempotent"
	ra "github.com/apollo-chora/chora-observability/internal/domain/ritualaudit"
)

// RitualRunAuditInboxTTL is the dedupe-key retention window for the consumer's
// inbox. 7d mirrors the event bus max redelivery window.
const RitualRunAuditInboxTTL = 7 * 24 * time.Hour

// RitualRunCompletedEvent is the language-agnostic representation of an inbound
// ritual_run_completed event: the envelope-sourced identity fields + the
// terminal ritual-run projection fields from the JSON payload body.
type RitualRunCompletedEvent struct {
	SourceTopic   string // envelope topic
	SourceEventID string // envelope.event_id (dedupe key)
	TenantID      string // envelope.tenant_id
	OwnerGCID     string // envelope.gcid (payload owner_gcid as fallback)
	FamiliarID    string

	// Terminal ritual-run projection fields (JSON payload body).
	RunID         string
	RitualID      string
	RevisionNo    int32
	TriggerSource string
	Status        string
	ManaCharged   int32
	SinkRef       string
	ErrorText     string

	// Stamps is the per-step ADR-197 decision-stamp array (O+ full record).
	Stamps []map[string]any

	OccurredAt  time.Time
	Traceparent string
}

// RitualRunAuditConsumerConfig wires the consumer.
type RitualRunAuditConsumerConfig struct {
	// Repo is the ritual-run audit repository (local-DB only). Required.
	Repo ra.Repository

	// Inbox provides at-least-once dedupe keyed on event_id. nil → a fresh
	// in-memory store (dev / test).
	Inbox idempotent.Store

	// Now is the wall-clock injection for tests. nil → time.Now().UTC().
	Now func() time.Time

	// IDGen mints the audit_id (UUIDv7). nil → uuid.NewV7.
	IDGen func() (string, error)
}

// RitualRunAuditConsumer projects inbound ritual_run_completed events into the
// ritual_run_audit table.
type RitualRunAuditConsumer struct {
	repo  ra.Repository
	inbox idempotent.Store
	now   func() time.Time
	idGen func() (string, error)
}

// NewRitualRunAuditConsumer constructs the consumer. Repo is required; the
// other fields default per RitualRunAuditConsumerConfig.
func NewRitualRunAuditConsumer(cfg RitualRunAuditConsumerConfig) *RitualRunAuditConsumer {
	if cfg.Repo == nil {
		panic("events: RitualRunAuditConsumer requires Repo")
	}
	if cfg.Inbox == nil {
		cfg.Inbox = idempotent.NewMemoryStore()
	}
	if cfg.Now == nil {
		cfg.Now = func() time.Time { return time.Now().UTC() }
	}
	if cfg.IDGen == nil {
		cfg.IDGen = func() (string, error) {
			id, err := uuid.NewV7()
			if err != nil {
				return "", err
			}
			return id.String(), nil
		}
	}
	return &RitualRunAuditConsumer{
		repo:  cfg.Repo,
		inbox: cfg.Inbox,
		now:   cfg.Now,
		idGen: cfg.IDGen,
	}
}

// SubscribedTopic returns the canonical inbound topic for binding wiring.
func (c *RitualRunAuditConsumer) SubscribedTopic() string {
	return ra.TopicFamiliarRitualRunCompleted
}

// Handle processes one inbound event. Idempotent on SourceEventID via the
// inbox; double-delivery is a no-op. Returns an error only when validation or
// the underlying persistence fails — the caller NACKs so the broker retries →
// DLQ.
func (c *RitualRunAuditConsumer) Handle(ctx context.Context, ev RitualRunCompletedEvent) error {
	if err := validateRitualRunEvent(ev); err != nil {
		return err
	}
	return c.inbox.Process(ctx, "ritual_run_completed:"+ev.SourceEventID, RitualRunAuditInboxTTL, func() error {
		return c.persist(ctx, ev)
	})
}

func (c *RitualRunAuditConsumer) persist(ctx context.Context, ev RitualRunCompletedEvent) error {
	auditID, err := c.idGen()
	if err != nil {
		return fmt.Errorf("events: ritual audit id gen: %w", err)
	}
	row := ra.RitualRunAuditRow{
		AuditID:       auditID,
		TenantID:      ev.TenantID,
		SourceTopic:   ev.SourceTopic,
		SourceEventID: ev.SourceEventID,
		RunID:         ev.RunID,
		RitualID:      ev.RitualID,
		FamiliarID:    ev.FamiliarID,
		OwnerGCID:     ev.OwnerGCID,
		RevisionNo:    ev.RevisionNo,
		TriggerSource: ev.TriggerSource,
		Status:        ev.Status,
		ManaCharged:   ev.ManaCharged,
		SinkRef:       ev.SinkRef,
		ErrorText:     ev.ErrorText,
		Stamps:        ev.Stamps,
		OccurredAt:    ev.OccurredAt,
		ReceivedAt:    c.now(),
	}
	if err := c.repo.Ingest(ctx, row); err != nil {
		if errors.Is(err, ra.ErrDuplicateSourceEvent) {
			// Repo-level idempotent replay (race-safe under concurrent
			// dispatchers) — no-op.
			return nil
		}
		return fmt.Errorf("events: ritual audit ingest: %w", err)
	}
	return nil
}

// validateRitualRunEvent rejects clearly-malformed inbound events early (cheap,
// no I/O — a NACK + DLQ on a bad event happens fast). Both the legacy familiar.*
// and the ADR-254 canonical companion.* subjects are accepted — the producer
// now emits the companion subject; the familiar one is retained for stragglers.
func validateRitualRunEvent(ev RitualRunCompletedEvent) error {
	if strings.TrimSpace(ev.SourceTopic) == "" {
		return errors.New("ritual_run_audit_consumer: source_topic required")
	}
	if ev.SourceTopic != ra.TopicFamiliarRitualRunCompleted &&
		ev.SourceTopic != ra.TopicCompanionRitualRunCompleted {
		return fmt.Errorf("ritual_run_audit_consumer: unknown source_topic %q", ev.SourceTopic)
	}
	if strings.TrimSpace(ev.SourceEventID) == "" {
		return errors.New("ritual_run_audit_consumer: source_event_id required")
	}
	if strings.TrimSpace(ev.TenantID) == "" {
		return errors.New("ritual_run_audit_consumer: tenant_id required")
	}
	if strings.TrimSpace(ev.RunID) == "" {
		return errors.New("ritual_run_audit_consumer: run_id required")
	}
	return nil
}

// ritualRunCompletedPayload is the decoded body of a ritual_run_completed
// event (chora-consumption RitualRunner.publishCompleted) — populated from the
// canonical BINARY protobuf (consumptionv1.RitualRunCompleted) or, for legacy
// in-flight rows, its JSON shape (CHO-2136 cutover). Envelope fields
// (event_id / tenant_id / gcid / occurred_at / traceparent) arrive on the
// eventbus message envelope, not here.
type ritualRunCompletedPayload struct {
	RunID         string           `json:"run_id"`
	RitualID      string           `json:"ritual_id"`
	FamiliarID    string           `json:"familiar_id"`
	OwnerGCID     string           `json:"owner_gcid"`
	RevisionNo    int32            `json:"revision_no"`
	TriggerSource string           `json:"trigger_source"`
	Status        string           `json:"status"`
	ManaCharged   int32            `json:"mana_charged"`
	SinkRef       string           `json:"sink_ref"`
	Error         string           `json:"error"`
	Stamps        []map[string]any `json:"stamps"`
}

// decodeRitualRunCompletedPayload decodes a payload body proto-first with a
// JSON fallback (CHO-2136): chora-consumption's outbox emits canonical binary
// protobuf; JSON remains for legacy rows still draining at cutover. The first
// byte is a deterministic discriminator — a JSON object begins with '{'
// (0x7B), while the binary message's first field tags as 0x0A — and is
// stricter than try-both (proto.Unmarshal is permissive enough that malformed
// JSON could half-parse as proto instead of failing loud).
func decodeRitualRunCompletedPayload(body []byte) (ritualRunCompletedPayload, error) {
	var p ritualRunCompletedPayload
	if len(body) == 0 {
		return p, errors.New("empty payload")
	}
	if body[0] == '{' {
		if err := json.Unmarshal(body, &p); err != nil {
			return p, err
		}
		return p, nil
	}
	var msg consumptionv1.RitualRunCompleted
	if err := proto.Unmarshal(body, &msg); err != nil {
		return p, err
	}
	return ritualRunCompletedPayload{
		RunID:         msg.GetRunId(),
		RitualID:      msg.GetRitualId(),
		FamiliarID:    msg.GetCompanionId(),
		OwnerGCID:     msg.GetOwnerGcid(),
		RevisionNo:    msg.GetRevisionNo(),
		TriggerSource: msg.GetTriggerSource(),
		Status:        ritualRunStatusString(msg.GetStatus()),
		ManaCharged:   msg.GetManaCharged(),
		SinkRef:       msg.GetSinkRef(),
		Error:         msg.GetError(),
		Stamps:        ritualStampsFromProto(msg.GetStamps()),
	}, nil
}

// ritualRunStatusString maps the proto enum back to the domain's canonical
// status string. UNSPECIFIED maps to "" — validateRitualRunEvent then rejects
// it loudly rather than persisting a fabricated status.
func ritualRunStatusString(s consumptionv1.RitualRunStatus) string {
	switch s {
	case consumptionv1.RitualRunStatus_RITUAL_RUN_STATUS_RUNNING:
		return "running"
	case consumptionv1.RitualRunStatus_RITUAL_RUN_STATUS_COMPLETED:
		return "completed"
	case consumptionv1.RitualRunStatus_RITUAL_RUN_STATUS_FAILED:
		return "failed"
	case consumptionv1.RitualRunStatus_RITUAL_RUN_STATUS_SKIPPED_BUDGET:
		return "skipped_budget"
	case consumptionv1.RitualRunStatus_RITUAL_RUN_STATUS_BLOCKED:
		return "blocked"
	default:
		return ""
	}
}

// ritualStampsFromProto projects the proto stamps into the canonical
// snake_case map shape the audit row's decision_stamp JSONB stores.
func ritualStampsFromProto(in []*consumptionv1.RitualStepStamp) []map[string]any {
	if len(in) == 0 {
		return nil
	}
	out := make([]map[string]any, 0, len(in))
	for _, s := range in {
		out = append(out, map[string]any{
			"step_index":     s.GetStepIndex(),
			"skill_key":      s.GetSkillKey(),
			"prompt_version": s.GetPromptVersion(),
			"prompt_hash":    s.GetPromptHash(),
			"tools_invoked":  s.GetToolsInvoked(),
			"citations":      s.GetCitations(),
		})
	}
	return out
}

// RitualRunAuditPullHandler adapts the consumer to an eventbus.Handler for the
// JetStream binding. It decodes the payload body (binary proto first, JSON
// fallback — CHO-2136) + reads the identity fields from the reconstructed
// envelope, then calls Handle. Mirrors events.ClosurePullHandler. The
// JetStream consume loop acks on nil, naks on error (ack-after-processing per
// D6.2).
func RitualRunAuditPullHandler(c *RitualRunAuditConsumer) eventbus.Handler {
	return func(ctx context.Context, msg eventbus.Message) error {
		if c == nil {
			return errors.New("events: ritual_run_audit pull handler not initialised")
		}
		p, err := decodeRitualRunCompletedPayload(msg.Payload)
		if err != nil {
			return fmt.Errorf("events: ritual_run_completed payload decode: %w", err)
		}
		env := msg.Envelope
		topic := msg.Subject
		if topic == "" {
			topic = ra.TopicFamiliarRitualRunCompleted
		}
		ownerGCID := env.GCID
		if ownerGCID == "" {
			ownerGCID = p.OwnerGCID
		}
		return c.Handle(ctx, RitualRunCompletedEvent{
			SourceTopic:   topic,
			SourceEventID: env.EventID,
			TenantID:      env.TenantID,
			OwnerGCID:     ownerGCID,
			FamiliarID:    p.FamiliarID,
			RunID:         p.RunID,
			RitualID:      p.RitualID,
			RevisionNo:    p.RevisionNo,
			TriggerSource: p.TriggerSource,
			Status:        p.Status,
			ManaCharged:   p.ManaCharged,
			SinkRef:       p.SinkRef,
			ErrorText:     p.Error,
			Stamps:        p.Stamps,
			OccurredAt:    env.OccurredAt,
			Traceparent:   env.Traceparent,
		})
	}
}
