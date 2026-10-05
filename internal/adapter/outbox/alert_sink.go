// Package outbox — PublisherAlertSink implementation (ADR-167 Plane-4).
//
// PublisherAlertSink satisfies the Dispatcher's AlertSink port by marshalling
// a SinkFailureAlert to JSON and publishing it on
// `chora.observability.sink_failure.recorded.v1` via the SAME Bus
// abstraction the Dispatcher already uses to drain the outbox (CloudPublisher
// in prod, InMemoryBus in dev/tests). No new Pub/Sub client + no second
// connection — the alert rides the existing publisher per
// `feedback_no_stubs_real_wiring` (real wiring, reuse-don't-reinvent).
//
// The alert is governance evidence (IMDA D1 accountability — a dropped
// observability event is an auditable gap), so the envelope is stamped with
// the canonical `accountability` dimension + `post_deploy` lifecycle stage
// per ADR-141.
//
// Per `feedback_no_inline_config`: source_project / source_service are
// injected via the Config struct (env-sourced at the composition root), never
// inlined here.
package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	cgcenvelope "github.com/apollo-chora/chora-common/envelope"
	cgctracing "github.com/apollo-chora/chora-common/tracing"
)

// PublisherAlertSinkConfig wires a PublisherAlertSink.
type PublisherAlertSinkConfig struct {
	// Bus is the Pub/Sub publisher (CloudPublisher in prod, InMemoryBus in
	// dev/tests). Required.
	Bus Bus

	// SourceProject is the source-project stamp on the envelope. Defaults to
	// "chora-local".
	SourceProject string

	// SourceService is the publisher's service name. Defaults to
	// "chora-observability".
	SourceService string

	// Now is injectable for tests; defaults to time.Now().UTC.
	Now func() time.Time
}

// PublisherAlertSink publishes governance sink-failure alerts via a Bus.
type PublisherAlertSink struct {
	cfg PublisherAlertSinkConfig
}

// NewPublisherAlertSink constructs a PublisherAlertSink. Panics on missing
// Bus (fail-loud at composition time, not at first failed publish).
func NewPublisherAlertSink(cfg PublisherAlertSinkConfig) *PublisherAlertSink {
	if cfg.Bus == nil {
		panic("outbox: NewPublisherAlertSink: Bus required")
	}
	if cfg.Now == nil {
		cfg.Now = func() time.Time { return time.Now().UTC() }
	}
	if cfg.SourceProject == "" {
		cfg.SourceProject = "chora-local"
	}
	if cfg.SourceService == "" {
		cfg.SourceService = "chora-observability"
	}
	return &PublisherAlertSink{cfg: cfg}
}

// EmitSinkFailure satisfies AlertSink. Builds a fully-populated envelope +
// publishes the JSON-marshalled alert on the governance sink-failure topic.
func (s *PublisherAlertSink) EmitSinkFailure(ctx context.Context, alert SinkFailureAlert) error {
	payload, err := json.Marshal(alert)
	if err != nil {
		return fmt.Errorf("outbox.PublisherAlertSink: marshal alert: %w", err)
	}

	now := s.cfg.Now()
	// idempotency_key folds the row id + reason so a malformed-then-exhausted
	// double-alert for the same row stays distinguishable while replays of the
	// same (row, reason) dedupe.
	idemKey := "sink_failure:" + alert.RowID + ":" + string(alert.Reason)

	// tenant_id: a dead-lettered row may be platform-emitted (empty/"" tenant).
	// The envelope requires a non-empty tenant_id, so fall back to the
	// "platform" sentinel (blessed by the envelope contract).
	tenantID := alert.TenantID
	if tenantID == "" {
		tenantID = "platform"
	}

	env := cgcenvelope.Envelope{
		EventID:            idemKey,
		IdempotencyKey:     idemKey,
		TenantID:           tenantID,
		GCID:               "", // platform-emitted alert — no learner GCID
		OccurredAt:         now,
		PublishedAt:        now,
		Traceparent:        cgctracing.EnsureTraceparent(cgctracing.TraceparentFromContext(ctx)),
		SourceProject:      s.cfg.SourceProject,
		SourceService:      s.cfg.SourceService,
		SchemaVersion:      1,
		ChoraImdaDimension: "accountability", // ADR-141 D1 — dropped event = audit gap
		ImdaLifecycleStage: "post_deploy",
	}

	if err := s.cfg.Bus.Publish(ctx, TopicObservabilitySinkFailure, env, payload); err != nil {
		return fmt.Errorf("outbox.PublisherAlertSink: publish %s: %w", TopicObservabilitySinkFailure, err)
	}
	return nil
}

// Compile-time check.
var _ AlertSink = (*PublisherAlertSink)(nil)
