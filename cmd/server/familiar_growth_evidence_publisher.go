// familiar_growth_evidence_publisher.go — durable IMDA D1/D2 evidence emit for
// the ADR-149 Familiar Growth audit lane (CHO-2257, AC "Evidence durability").
//
// What was wrong
// --------------
// The PROD path wired subscribers.NewInMemoryEvidencePublisher(): every IMDA
// D1/D2 evidence emit was appended to a process-local slice and evaporated on
// the next pod restart. Nothing downstream could ever see it. Governance
// evidence that only exists in one process's heap is not evidence.
//
// What this does
// --------------
// outboxEvidencePublisher writes each emit as a durable row in the
// chora_observability outbox (the same PostgresStore the dispatcher + the
// ADR-167 Plane-4 quarantine already use). The dispatcher then publishes it to
// chora.governance.evidence.recorded.v1 — the canonical owner of all IMDA
// evidence is the chora-governance projector (Tier 5 D17). The row survives
// restart and is replayable.
//
// Wire shape: the topic carries a BINARY protobuf schema
// (chora-governance-evidence-recorded-v1, verified against chora-489812 on
// 2026-07-17), so the payload is a marshalled chora.governance.v1
// .EvidenceRecorded. A JSON payload would be rejected with a 400 AT PUBLISH and
// would never reach a DLQ.
//
// KNOWN DOWNSTREAM GAP (split story, see CHO-2257 completion comment):
// chora.governance.evidence.recorded.v1 currently has ZERO subscriptions, so the
// broker discards what it receives. Creating that subscription belongs to
// chora-governance, not chora-observability. The durable outbox row here is the
// part this domain owns, and it is what makes the evidence replayable once the
// projector's subscription exists.
package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/common/v1"
	governancev1 "github.com/apollo-chora/chora-contracts/gen/go/chora/governance/v1"

	cgctracing "github.com/apollo-chora/chora-common/tracing"
	obsoutbox "github.com/apollo-chora/chora-observability/internal/adapter/outbox"
	"github.com/apollo-chora/chora-observability/internal/adapter/subscribers"
)

// evidenceSchemaVersion is the envelope schema_version stamped on every
// evidence event.
const evidenceSchemaVersion = 1

// evidencePublisherConfig carries the non-secret identity stamped onto every
// evidence envelope. Values come from env at boot (never inline).
type evidencePublisherConfig struct {
	SourceProject string
	SourceService string
	// Now is the wall-clock injection for tests. nil → time.Now().UTC().
	Now func() time.Time
}

// outboxEvidencePublisher implements subscribers.EvidencePublisher by writing a
// durable outbox row per emit.
type outboxEvidencePublisher struct {
	store obsoutbox.Store
	cfg   evidencePublisherConfig
}

// newOutboxEvidencePublisher constructs the durable publisher. Panics on a nil
// store: a publisher that cannot write is a silent evidence sink, which is the
// exact defect this replaces.
func newOutboxEvidencePublisher(store obsoutbox.Store, cfg evidencePublisherConfig) *outboxEvidencePublisher {
	if store == nil {
		panic("familiar_growth_evidence: outbox store required — a publisher that cannot persist would silently drop IMDA evidence")
	}
	if cfg.Now == nil {
		cfg.Now = func() time.Time { return time.Now().UTC() }
	}
	return &outboxEvidencePublisher{store: store, cfg: cfg}
}

// PublishEvidence writes one IMDA evidence event to the outbox.
//
// Idempotency keys on (source_topic, source_event_id) — the SOURCE event, not
// the minted evidence id — so a redelivered growth event never writes a second
// evidence row even though the subscriber mints a fresh evidence id per call.
// A duplicate is the idempotent replay path, not a failure.
func (p *outboxEvidencePublisher) PublishEvidence(ctx context.Context, ev subscribers.EvidenceEmit) error {
	now := p.cfg.Now()
	occurredAt := ev.OccurredAt
	if occurredAt.IsZero() {
		occurredAt = now
	}
	tenantID := strings.TrimSpace(ev.TenantID)
	if tenantID == "" {
		// The subscriber validates tenant_id before it reaches here; this is a
		// defensive floor mirroring PublisherAlertSink rather than a silent
		// default for a mandatory field.
		return errors.New("familiar_growth_evidence: tenant_id is required")
	}
	traceparent := cgctracing.EnsureTraceparent(ev.Traceparent)
	idemKey := "evidence:" + ev.SourceTopic + ":" + ev.SourceEventID

	protoEnv := &commonv1.EventEnvelope{
		EventId:            ev.EvidenceID,
		IdempotencyKey:     idemKey,
		TenantId:           tenantID,
		Gcid:               ev.GCID,
		OccurredAt:         timestamppb.New(occurredAt),
		PublishedAt:        timestamppb.New(now),
		Traceparent:        traceparent,
		Tracestate:         "",
		SourceProject:      p.cfg.SourceProject,
		SourceService:      p.cfg.SourceService,
		SchemaVersion:      evidenceSchemaVersion,
		ChoraImdaDimension: ev.IMDADimension,
		ImdaLifecycleStage: ev.LifecycleStage,
	}
	payload, err := proto.Marshal(&governancev1.EvidenceRecorded{
		Envelope:         protoEnv,
		EvidenceType:     ev.EvidenceType,
		SourceEventType:  ev.SourceTopic,
		RecordedAt:       timestamppb.New(now),
		AdditionalFields: stringifyFields(ev.AdditionalFields, ev.SourceEventID),
	})
	if err != nil {
		return fmt.Errorf("familiar_growth_evidence: marshal EvidenceRecorded: %w", err)
	}

	// The Envelope map is what the dispatcher's reconstructEnvelope reads to
	// rebuild the typed envelope for publish. Every field envelope.Validate
	// requires must be present here, or the row can never leave the outbox.
	row := obsoutbox.Row{
		ID:        ev.EvidenceID,
		TenantID:  tenantID,
		GCID:      ev.GCID,
		EventType: ev.EvidenceType,
		Topic:     subscribers.IMDAEvidenceTopic,
		Payload:   payload,
		Envelope: map[string]string{
			"event_id":             ev.EvidenceID,
			"idempotency_key":      idemKey,
			"tenant_id":            tenantID,
			"gcid":                 ev.GCID,
			"occurred_at":          occurredAt.UTC().Format(time.RFC3339Nano),
			"published_at":         now.UTC().Format(time.RFC3339Nano),
			"traceparent":          traceparent,
			"source_project":       p.cfg.SourceProject,
			"source_service":       p.cfg.SourceService,
			"schema_version":       fmt.Sprintf("%d", evidenceSchemaVersion),
			"chora_imda_dimension": ev.IMDADimension,
			"imda_lifecycle_stage": ev.LifecycleStage,
		},
		IdempotencyKey: idemKey,
		OccurredAt:     occurredAt,
	}

	if err := p.store.Insert(ctx, row); err != nil {
		if errors.Is(err, obsoutbox.ErrDuplicateIdempotencyKey) {
			// This source event's evidence is already durable — the idempotent
			// replay path, not a failure.
			return nil
		}
		return fmt.Errorf("familiar_growth_evidence: outbox insert: %w", err)
	}
	return nil
}

// stringifyFields projects the emit's map[string]any onto the proto's
// map[string]string, and pins source_event_id so the projector can correlate
// evidence back to the source event.
func stringifyFields(in map[string]any, sourceEventID string) map[string]string {
	out := make(map[string]string, len(in)+1)
	for k, v := range in {
		if v == nil {
			continue
		}
		out[k] = fmt.Sprint(v)
	}
	if sourceEventID != "" {
		out["source_event_id"] = sourceEventID
	}
	return out
}

// selectEvidencePublisher picks the IMDA evidence sink for the running mode.
//
// durable reports whether a DB pool is up (i.e. this is a real deployment). In
// that mode the in-memory publisher REFUSES to wire: silently buffering IMDA
// D1/D2 evidence into a process-local slice that evaporates on restart is a
// governance-evidence loss, not an acceptable fallback. Without an outbox store
// there is no durable sink, so wiring fails loudly instead of degrading.
func selectEvidencePublisher(
	store obsoutbox.Store,
	durable bool,
	cfg evidencePublisherConfig,
) (subscribers.EvidencePublisher, error) {
	if durable {
		if store == nil {
			return nil, errors.New(
				"familiar_growth_evidence: a DB pool is up but no outbox store is wired — refusing to fall back to the in-memory evidence publisher (IMDA D1/D2 evidence would evaporate on restart)")
		}
		return newOutboxEvidencePublisher(store, cfg), nil
	}
	// Dev / local: no pool, so nothing durable exists to write to. The
	// in-memory publisher is the honest choice here and only here.
	return subscribers.NewInMemoryEvidencePublisher(), nil
}

// Compile-time check.
var _ subscribers.EvidencePublisher = (*outboxEvidencePublisher)(nil)
