// Package subscribers wires Pub/Sub inbound adapters for chora-observability.
//
// FamiliarGrowthAuditSubscriber binds to the 6 ADR-149 Familiar Growth event
// topics and projects each one into the 4 chora_observability audit tables:
//
//   - familiar_growth_audit_ledger    (every event, idempotent on source_event_id)
//   - familiar_growth_daily_metrics   (exp_awarded / stage_up rollup)
//   - breed_roll_audit                (IMDA D2 transparency — breed_revealed)
//   - egg_funnel_metrics              (egg_purchased -> breed_revealed funnel)
//
// IMDA D1 evidence emit: every audit ingest publishes a
// chora.governance.evidence.recorded.v1 outbox row so the chora-governance
// projector (the canonical owner of all IMDA evidence per Tier 5 D17)
// surfaces these rows in O+ dashboards.
//
// Hexagonal:
//   - INBOUND ADAPTER from Pub/Sub (binding lives in cmd/server/main.go)
//   - depends on familiargrowth.Repository (local DB only — no cross-DB)
//   - depends on a small EvidencePublisher port for the IMDA emit
//
// OTLP cardinality: per-event audit spans MAY stamp familiar_id (low
// cardinality per request); aggregated metrics spans MUST NOT.
package subscribers

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/5007-Capstone/chora/libs/chora-go-common/idempotent"
	fg "github.com/5007-Capstone/chora/services/chora-observability/internal/domain/familiargrowth"
)

// InboxTTL is the dedupe-key retention window for inbound events. 7d is the
// Pub/Sub default max redelivery window; we mirror that here.
const InboxTTL = 7 * 24 * time.Hour

// IMDAEvidenceTopic is the canonical topic for IMDA evidence emission per
// chora-identity's governance publisher pattern (Tier 5 D17).
const IMDAEvidenceTopic = "chora.governance.evidence.recorded.v1"

// EvidencePublisher is the port the subscriber uses to emit IMDA evidence
// to the chora-governance projector. The adapter implementation lives in
// cmd/server/main.go and wraps the existing observability outbox.
type EvidencePublisher interface {
	// PublishEvidence emits one IMDA evidence event. Returns an error only
	// when the producer-side write fails; the subscriber stops processing
	// the source event on error so it can be retried.
	PublishEvidence(ctx context.Context, ev EvidenceEmit) error
}

// EvidenceEmit is the payload the subscriber hands to EvidencePublisher.
type EvidenceEmit struct {
	EvidenceID      string         // UUIDv7
	TenantID        string         // tenant_id (UUID string)
	GCID            string         // owner gcid (UUID string; may be empty for system events)
	EvidenceType    string         // e.g. "familiar_growth.exp_awarded"
	SourceTopic     string         // canonical inbound topic
	SourceEventID   string         // UUIDv7 of the inbound event
	IMDADimension   string         // accountability | transparency
	LifecycleStage  string         // runtime
	Traceparent     string         // W3C traceparent propagated from inbound
	OccurredAt      time.Time
	AdditionalFields map[string]any
}

// SpanRecorder is the optional OTLP span attribute sink. Adapters wire the
// real OTel SDK; tests inject a recorder for assertions.
type SpanRecorder interface {
	// Record stamps the supplied attributes on the in-flight subscriber
	// span. familiar_id MUST NOT be passed when isMetricSpan is true.
	Record(ctx context.Context, name string, attrs map[string]any)
}

// FamiliarGrowthEvent is the language-agnostic representation of an inbound
// Familiar Growth event payload. The Pub/Sub adapter (Cloud or in-memory)
// builds one of these per message before calling Handle.
//
// Fields cover the union of the 6 source-topic shapes; the subscriber reads
// only the subset relevant to SourceTopic.
type FamiliarGrowthEvent struct {
	SourceTopic   string
	SourceEventID string // envelope event_id
	TenantID      string
	OwnerGCID     string
	FamiliarID    string
	Traceparent   string

	// Common audit-ledger payload (passed through to the audit_ledger row).
	Payload map[string]any

	// exp_awarded specific.
	ExpDelta    int32
	ExpSource   string  // atom_session | ebbinghaus_review | conv_turn | ...
	OccurredAt  time.Time

	// stage_up specific.
	StageFrom int32
	StageTo   int32

	// breed_revealed specific (IMDA D2).
	EggSKU               string
	Species              string
	Rarity               string
	Shiny                bool
	RolledProbability    float64
	DistributionSnapshot map[string]any

	// egg_purchased specific.
	PurchaseSource string  // purchase | subscription_inclusion | tenant_grant | trial

	// payment_succeeded specific.
	AmountCents int64
	Currency    string

	// source_revelation specific.
	WindowDurationSeconds int32
}

// FamiliarGrowthAuditSubscriber projects 6 inbound topics into the 4
// audit tables.
type FamiliarGrowthAuditSubscriber struct {
	repo     fg.Repository
	evidence EvidencePublisher
	inbox    idempotent.Store
	spans    SpanRecorder // optional
	now      func() time.Time
	idGen    func() (string, error)
}

// Config wires the subscriber.
type Config struct {
	Repo     fg.Repository
	Evidence EvidencePublisher
	Inbox    idempotent.Store
	Spans    SpanRecorder // optional
	Now      func() time.Time
	IDGen    func() (string, error)
}

// New constructs a FamiliarGrowthAuditSubscriber.
func New(cfg Config) *FamiliarGrowthAuditSubscriber {
	if cfg.Repo == nil {
		panic("subscribers: Repo required")
	}
	if cfg.Evidence == nil {
		panic("subscribers: Evidence required")
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
	return &FamiliarGrowthAuditSubscriber{
		repo:     cfg.Repo,
		evidence: cfg.Evidence,
		inbox:    cfg.Inbox,
		spans:    cfg.Spans,
		now:      cfg.Now,
		idGen:    cfg.IDGen,
	}
}

// SubscribedTopics returns the inbound topics this subscriber binds to.
func (s *FamiliarGrowthAuditSubscriber) SubscribedTopics() []string {
	return []string{
		fg.TopicExpAwarded,
		fg.TopicStageUp,
		fg.TopicBreedRevealed,
		fg.TopicHatched,
		fg.TopicSourceRevelation,
		fg.TopicEggPurchased,
		fg.TopicPaymentSucceeded,
	}
}

// Handle processes one inbound event. Idempotent on (source_topic,
// source_event_id) via the inbox; double-delivery is a no-op.
func (s *FamiliarGrowthAuditSubscriber) Handle(ctx context.Context, ev FamiliarGrowthEvent) error {
	if err := validateEvent(ev); err != nil {
		return err
	}
	dedupeKey := ev.SourceTopic + ":" + ev.SourceEventID
	return s.inbox.Process(ctx, dedupeKey, InboxTTL, func() error {
		return s.process(ctx, ev)
	})
}

func (s *FamiliarGrowthAuditSubscriber) process(ctx context.Context, ev FamiliarGrowthEvent) error {
	auditID, err := s.idGen()
	if err != nil {
		return fmt.Errorf("subscribers: id gen: %w", err)
	}
	receivedAt := s.now()
	occurredAt := ev.OccurredAt
	if occurredAt.IsZero() {
		occurredAt = receivedAt
	}

	eventType := deriveEventType(ev.SourceTopic)
	row := fg.AuditLedgerRow{
		AuditID:       auditID,
		TenantID:      ev.TenantID,
		SourceTopic:   ev.SourceTopic,
		SourceEventID: ev.SourceEventID,
		FamiliarID:    ev.FamiliarID,
		OwnerGCID:     ev.OwnerGCID,
		EventType:     eventType,
		Payload:       cloneMap(ev.Payload),
		ReceivedAt:    receivedAt,
	}

	req := fg.IngestRequest{Ledger: row}
	dimension := fg.IMDADimensionAccountability

	switch ev.SourceTopic {
	case fg.TopicExpAwarded:
		req.MetricsDelta = &fg.DailyMetricsDelta{
			DayBucket:       truncateToDay(occurredAt),
			Source:          fallbackString(ev.ExpSource, "unknown"),
			ExpAwardedDelta: int64(ev.ExpDelta),
			EventCountDelta: 1,
		}
		s.recordSpan(ctx, "familiar_growth.exp_awarded", map[string]any{
			"chora.tenant_id":   ev.TenantID,
			"chora.familiar_id": ev.FamiliarID,
			"chora.exp.source":  req.MetricsDelta.Source,
			"chora.exp.delta":   ev.ExpDelta,
			"chora.imda.d1":     true,
		})
	case fg.TopicStageUp:
		req.MetricsDelta = &fg.DailyMetricsDelta{
			DayBucket:       truncateToDay(occurredAt),
			Source:          "stage_up",
			EventCountDelta: 1,
			StageUpsDelta:   1,
		}
		s.recordSpan(ctx, "familiar_growth.stage_up", map[string]any{
			"chora.tenant_id":     ev.TenantID,
			"chora.familiar_id":   ev.FamiliarID,
			"chora.growth.stage_to": ev.StageTo,
			"chora.imda.d1":       true,
		})
	case fg.TopicBreedRevealed:
		dimension = fg.IMDADimensionTransparency
		req.BreedRoll = &fg.BreedRollAuditRow{
			AuditID:              auditID,
			TenantID:             ev.TenantID,
			FamiliarID:           ev.FamiliarID,
			OwnerGCID:            ev.OwnerGCID,
			EggSKU:               ev.EggSKU,
			Species:              ev.Species,
			Shiny:                ev.Shiny,
			Rarity:               ev.Rarity,
			RolledProbability:    ev.RolledProbability,
			DistributionSnapshot: cloneMap(ev.DistributionSnapshot),
			RevealedAt:           occurredAt,
		}
		// breed_revealed captures the lootbox roll (species/breed selection
		// with rolled probability). Funnel-hatched count is now sourced from
		// hatched.v1 directly per Fix-D 2026-05-16.
		s.recordSpan(ctx, "familiar_growth.breed_revealed", map[string]any{
			"chora.tenant_id":     ev.TenantID,
			"chora.familiar_id":   ev.FamiliarID,
			"chora.breed.species": ev.Species,
			"chora.breed.rarity":  ev.Rarity,
			"chora.egg_sku":       ev.EggSKU,
			"chora.imda.d2":       true,
		})
	case fg.TopicHatched:
		// hatched.v1 is the user-visible egg-hatch lifecycle transition;
		// IMDA D2 transparency. Drives the funnel hatched-count directly
		// per Fix-D 2026-05-16 (replacing the breed_revealed proxy).
		dimension = fg.IMDADimensionTransparency
		req.FunnelDelta = &fg.EggFunnelDelta{
			DayBucket:    truncateToDay(occurredAt),
			HatchedDelta: 1,
		}
		s.recordSpan(ctx, "familiar_growth.hatched", map[string]any{
			"chora.tenant_id":   ev.TenantID,
			"chora.familiar_id": ev.FamiliarID,
			"chora.owner_gcid":  ev.OwnerGCID,
			"chora.imda.d2":     true,
		})
	case fg.TopicSourceRevelation:
		s.recordSpan(ctx, "familiar_growth.source_revelation", map[string]any{
			"chora.tenant_id":   ev.TenantID,
			"chora.familiar_id": ev.FamiliarID,
			"chora.imda.d1":     true,
		})
	case fg.TopicEggPurchased:
		req.FunnelDelta = &fg.EggFunnelDelta{
			DayBucket:      truncateToDay(occurredAt),
			PurchasedDelta: 1,
		}
		s.recordSpan(ctx, "familiar_growth.egg_purchased", map[string]any{
			"chora.tenant_id":   ev.TenantID,
			"chora.familiar_id": ev.FamiliarID,
			"chora.egg_sku":     ev.EggSKU,
			"chora.imda.d1":     true,
		})
	case fg.TopicPaymentSucceeded:
		s.recordSpan(ctx, "familiar_growth.payment_succeeded", map[string]any{
			"chora.tenant_id": ev.TenantID,
			"chora.egg_sku":   ev.EggSKU,
			"chora.imda.d1":   true,
		})
	default:
		return fmt.Errorf("subscribers: unknown source topic %q", ev.SourceTopic)
	}

	if err := s.repo.Ingest(ctx, req); err != nil {
		if errors.Is(err, fg.ErrDuplicateSourceEvent) {
			// Repo-level idempotent replay (race-safe in concurrent
			// dispatchers). Continue with the evidence emit; the chora-
			// governance projector dedupes on its own.
		} else {
			return fmt.Errorf("subscribers: ingest: %w", err)
		}
	}

	// IMDA D1/D2 evidence emit — every audit row publishes one evidence
	// event to the canonical chora-governance projector.
	evidenceID, err := s.idGen()
	if err != nil {
		return fmt.Errorf("subscribers: evidence id gen: %w", err)
	}
	if err := s.evidence.PublishEvidence(ctx, EvidenceEmit{
		EvidenceID:     evidenceID,
		TenantID:       ev.TenantID,
		GCID:           ev.OwnerGCID,
		EvidenceType:   "familiar_growth." + eventType,
		SourceTopic:    ev.SourceTopic,
		SourceEventID:  ev.SourceEventID,
		IMDADimension:  dimension,
		LifecycleStage: fg.IMDALifecycleStageRuntime,
		Traceparent:    ev.Traceparent,
		OccurredAt:     occurredAt,
		AdditionalFields: map[string]any{
			"familiar_id": ev.FamiliarID,
			"audit_id":    auditID,
		},
	}); err != nil {
		return fmt.Errorf("subscribers: evidence publish: %w", err)
	}
	return nil
}

func (s *FamiliarGrowthAuditSubscriber) recordSpan(ctx context.Context, name string, attrs map[string]any) {
	if s.spans == nil {
		return
	}
	s.spans.Record(ctx, name, attrs)
}

// validateEvent rejects clearly malformed input early.
func validateEvent(ev FamiliarGrowthEvent) error {
	if strings.TrimSpace(ev.SourceTopic) == "" {
		return errors.New("subscribers: source_topic required")
	}
	if strings.TrimSpace(ev.SourceEventID) == "" {
		return errors.New("subscribers: source_event_id required")
	}
	if strings.TrimSpace(ev.TenantID) == "" {
		return errors.New("subscribers: tenant_id required")
	}
	switch ev.SourceTopic {
	case fg.TopicExpAwarded,
		fg.TopicStageUp,
		fg.TopicBreedRevealed,
		fg.TopicHatched,
		fg.TopicSourceRevelation,
		fg.TopicEggPurchased,
		fg.TopicPaymentSucceeded:
		return nil
	default:
		return fmt.Errorf("subscribers: unknown source_topic %q", ev.SourceTopic)
	}
}

// deriveEventType extracts the {event_type} part of the canonical topic.
// e.g. chora.consumption.familiar.exp_awarded.v1 -> exp_awarded
func deriveEventType(topic string) string {
	parts := strings.Split(topic, ".")
	if len(parts) >= 5 {
		return parts[len(parts)-2]
	}
	return topic
}

func truncateToDay(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
}

func fallbackString(v, def string) string {
	if strings.TrimSpace(v) == "" {
		return def
	}
	return v
}

func cloneMap(in map[string]any) map[string]any {
	if in == nil {
		return map[string]any{}
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

// -----------------------------------------------------------------------------
// In-memory EvidencePublisher — test double + dev fallback.
// -----------------------------------------------------------------------------

// InMemoryEvidencePublisher records emitted evidence in memory.
type InMemoryEvidencePublisher struct {
	emitted []EvidenceEmit
}

// NewInMemoryEvidencePublisher returns a fresh in-memory publisher.
func NewInMemoryEvidencePublisher() *InMemoryEvidencePublisher {
	return &InMemoryEvidencePublisher{emitted: make([]EvidenceEmit, 0, 4)}
}

// PublishEvidence records the emit in memory.
func (p *InMemoryEvidencePublisher) PublishEvidence(_ context.Context, ev EvidenceEmit) error {
	p.emitted = append(p.emitted, ev)
	return nil
}

// Emitted returns a defensive copy of all recorded emits.
func (p *InMemoryEvidencePublisher) Emitted() []EvidenceEmit {
	out := make([]EvidenceEmit, len(p.emitted))
	copy(out, p.emitted)
	return out
}
