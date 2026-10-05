// Package reconcilepublish is the real event-bus publisher adapter for the
// daily reconciliation harness's anomaly + degraded events.
//
// Before this adapter, cmd/reconcile used a logging-only stub EventSink (the
// M10 skeleton). Tier 2 wiring (ADR-167 / 2026-06-01) replaces it with a real
// publisher on the canonical governance topic:
//
//	chora.governance.payment_reconciliation.anomaly.v1   (reconcile.AnomalyEvent)
//	chora.observability.payment_reconciliation.degraded.v1 (reconcile.DegradedEvent)
//
// It REUSES the same publisher abstraction the main server's outbox dispatcher
// uses — the eventbus Publisher contract (the NATS JetStream bus in prod,
// an in-memory bus in tests) — so there is one publish path across the
// service, not two. Per `feedback_no_inline_config` source_project /
// source_service come from the Config struct (env-sourced at the cmd/reconcile
// composition root); per `feedback_no_stubs_real_wiring` this is real wiring,
// the logging stub remains only as the explicit unconfigured-fallback in
// cmd/reconcile.
package reconcilepublish

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	cgcenvelope "github.com/apollo-chora/chora-common/envelope"
	cgctracing "github.com/apollo-chora/chora-common/tracing"
	"github.com/apollo-chora/chora-observability/internal/domain/reconcile"
)

// Publisher is the minimal publish contract this adapter needs. It is
// satisfied by eventbus.Publisher (the NATS JetStream bus) — the SAME
// abstraction the main server's outbox dispatcher's Bus port uses. Declared
// locally to avoid importing the broker client chain into the reconcile
// binary's unit tests.
type Publisher interface {
	Publish(ctx context.Context, topic string, env cgcenvelope.Envelope, payload []byte) error
}

// Config wires an EventSink.
type Config struct {
	// Publisher is the event-bus publisher. Required.
	Publisher Publisher

	// SourceProject is the GCP project the reconcile job runs in. Defaults to
	// "chora-489812".
	SourceProject string

	// SourceService stamps the envelope source_service. Defaults to
	// "chora-observability".
	SourceService string

	// Now is injectable for tests; defaults to time.Now().UTC.
	Now func() time.Time
}

// EventSink publishes reconciliation anomaly + degraded events via the
// injected Publisher. It satisfies BOTH reconcile.EventSink (anomaly) and
// reconcile.DegradedSink (degraded-pipeline) so the Runner emits both
// surfaces through one real publisher.
type EventSink struct {
	cfg Config
}

// NewEventSink constructs the sink. Panics on a nil Publisher (fail-loud
// at composition time, not on the first daily anomaly).
func NewEventSink(cfg Config) *EventSink {
	if cfg.Publisher == nil {
		panic("reconcilepublish: NewEventSink: Publisher required")
	}
	if cfg.Now == nil {
		cfg.Now = func() time.Time { return time.Now().UTC() }
	}
	if cfg.SourceProject == "" {
		cfg.SourceProject = "chora-489812"
	}
	if cfg.SourceService == "" {
		cfg.SourceService = "chora-observability"
	}
	return &EventSink{cfg: cfg}
}

// Emit satisfies reconcile.EventSink — publishes the payment-reconciliation
// anomaly event on chora.governance.payment_reconciliation.anomaly.v1.
func (s *EventSink) Emit(ctx context.Context, ev reconcile.AnomalyEvent) error {
	payload, err := json.Marshal(ev)
	if err != nil {
		return fmt.Errorf("reconcilepublish: marshal anomaly: %w", err)
	}
	// idempotency: one anomaly per window per service-run. Fold the window so a
	// retried Cloud Run Job for the same window dedupes at the consumer.
	idemKey := fmt.Sprintf("reconcile_anomaly:%s:%s",
		ev.WindowStart.UTC().Format(time.RFC3339), ev.WindowEnd.UTC().Format(time.RFC3339))
	env := s.buildEnvelope(ctx, idemKey, ev.ChoraImdaDimension, ev.ImdaLifecycleStage, ev.OccurredAt)
	if err := s.cfg.Publisher.Publish(ctx, reconcile.CanonicalReconcileAnomalyTopic, env, payload); err != nil {
		return fmt.Errorf("reconcilepublish: publish %s: %w", reconcile.CanonicalReconcileAnomalyTopic, err)
	}
	return nil
}

// EmitDegraded satisfies reconcile.DegradedSink — publishes the degraded-
// pipeline event on chora.observability.payment_reconciliation.degraded.v1.
func (s *EventSink) EmitDegraded(ctx context.Context, ev reconcile.DegradedEvent) error {
	payload, err := json.Marshal(ev)
	if err != nil {
		return fmt.Errorf("reconcilepublish: marshal degraded: %w", err)
	}
	idemKey := fmt.Sprintf("reconcile_degraded:%s:%s:%s",
		ev.UpstreamComponent, ev.WindowStart.UTC().Format(time.RFC3339), ev.WindowEnd.UTC().Format(time.RFC3339))
	env := s.buildEnvelope(ctx, idemKey, ev.ChoraImdaDimension, ev.ImdaLifecycleStage, ev.OccurredAt)
	if err := s.cfg.Publisher.Publish(ctx, reconcile.CanonicalReconcileDegradedTopic, env, payload); err != nil {
		return fmt.Errorf("reconcilepublish: publish %s: %w", reconcile.CanonicalReconcileDegradedTopic, err)
	}
	return nil
}

// buildEnvelope builds the mandatory-field envelope for a reconcile event.
// The reconcile harness is a platform job (no learner GCID + the "platform"
// tenant sentinel). occurredAt is the domain event's clock (falls back to now).
func (s *EventSink) buildEnvelope(ctx context.Context, idemKey, imdaDimension, lifecycleStage string, occurredAt time.Time) cgcenvelope.Envelope {
	now := s.cfg.Now()
	if occurredAt.IsZero() {
		occurredAt = now
	}
	dim := imdaDimension
	if dim == "" {
		dim = "accountability" // ADR-141 D1 — reconciliation is accountability evidence
	}
	stage := lifecycleStage
	if stage == "" {
		stage = "post_deploy"
	}
	return cgcenvelope.Envelope{
		EventID:            idemKey,
		IdempotencyKey:     idemKey,
		TenantID:           "platform", // platform-wide reconciliation job
		GCID:               "",
		OccurredAt:         occurredAt,
		PublishedAt:        now,
		Traceparent:        cgctracing.EnsureTraceparent(cgctracing.TraceparentFromContext(ctx)),
		SourceProject:      s.cfg.SourceProject,
		SourceService:      s.cfg.SourceService,
		SchemaVersion:      1,
		ChoraImdaDimension: dim,
		ImdaLifecycleStage: stage,
	}
}

// Compile-time checks: the sink satisfies both reconcile ports.
var (
	_ reconcile.EventSink    = (*EventSink)(nil)
	_ reconcile.DegradedSink = (*EventSink)(nil)
)
