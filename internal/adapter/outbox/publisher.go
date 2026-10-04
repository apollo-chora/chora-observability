// Package outbox — OutboxPublisher implementation.
//
// OutboxPublisher satisfies ledger.OutboxRecorder by writing the supplied
// OutboxRecord to outbox_events instead of publishing directly to Pub/Sub.
// The Dispatcher (see dispatcher.go) drains the table to Cloud Pub/Sub on a
// separate goroutine. This decouples observability event emission from
// Pub/Sub availability — a crash between Append and Publish no longer
// loses events.
//
// Drop-in replacement for the legacy inmem.OutboxRecorder; the constructor
// signature is the only swap-out site in main().
//
// Per `feedback_d6_resilience_first_class` B.6.2.a producer-side durable
// emission for chora-observability's
// `chora.observability.token_usage.recorded.v1` stream (and other future
// domain events).
package outbox

import (
	"context"
	"fmt"
	"strconv"
	"time"

	cgctracing "github.com/apollo-chora/chora-common/tracing"
	"github.com/apollo-chora/chora-observability/internal/domain/ledger"
)

// schemaVersion is the major version of the on-wire payload schema.
const schemaVersion = 1

// PublisherConfig wires the OutboxPublisher.
type PublisherConfig struct {
	// Store is the outbox table backend. Required.
	Store Store

	// SourceProject is the GCP project the service runs in (e.g.
	// chora-489812). Defaults to "chora-489812".
	SourceProject string

	// SourceService is the publisher's service name. Defaults to
	// "chora-observability".
	SourceService string

	// Now is injectable for tests; defaults to time.Now().UTC.
	Now func() time.Time
}

// Publisher satisfies ledger.OutboxRecorder by enqueueing the outbox record
// into outbox_events.
type Publisher struct {
	cfg PublisherConfig
}

// NewPublisher constructs an OutboxPublisher.
func NewPublisher(cfg PublisherConfig) *Publisher {
	if cfg.Now == nil {
		cfg.Now = func() time.Time { return time.Now().UTC() }
	}
	if cfg.SourceProject == "" {
		cfg.SourceProject = "chora-489812"
	}
	if cfg.SourceService == "" {
		cfg.SourceService = "chora-observability"
	}
	return &Publisher{cfg: cfg}
}

// RecordOutboxEvent satisfies ledger.OutboxRecorder. Writes the record as
// a pending row in outbox_events. The Dispatcher publishes to Pub/Sub
// asynchronously.
func (p *Publisher) RecordOutboxEvent(ctx context.Context, rec ledger.OutboxRecord) error {
	if p.cfg.Store == nil {
		return fmt.Errorf("outbox: store not wired")
	}

	now := p.cfg.Now()

	// Fill envelope defaults from the record + publisher config.
	sourceProject := rec.SourceProject
	if sourceProject == "" {
		sourceProject = p.cfg.SourceProject
	}
	sourceService := rec.SourceService
	if sourceService == "" {
		sourceService = p.cfg.SourceService
	}

	// Traceparent: prefer the record's value, then context propagation,
	// then mint a fresh one (OTLP-everywhere mandate per CLAUDE.md §6).
	traceparent := rec.Traceparent
	if traceparent == "" {
		traceparent = cgctracing.EnsureTraceparent(cgctracing.TraceparentFromContext(ctx))
	}

	envelope := map[string]string{
		"event_id":             rec.EventID,
		"idempotency_key":      rec.IdempotencyKey,
		"tenant_id":            rec.TenantID,
		"gcid":                 rec.GCID,
		"occurred_at":          rec.OccurredAt.UTC().Format(time.RFC3339Nano),
		"published_at":         now.Format(time.RFC3339Nano),
		"traceparent":          traceparent,
		"tracestate":           "",
		"source_project":       sourceProject,
		"source_service":       sourceService,
		"schema_version":       strconv.Itoa(schemaVersion),
		"chora_imda_dimension": rec.ChoraImdaDimension,
		"imda_lifecycle_stage": rec.ImdaLifecycleStage,
	}

	row := Row{
		ID:             rec.EventID,
		TenantID:       rec.TenantID,
		GCID:           rec.GCID,
		AgentID:        "", // observability events are platform-emitted; agent_id stays empty
		EventType:      rec.EventType,
		Topic:          rec.Topic,
		Payload:        rec.Payload,
		Envelope:       envelope,
		IdempotencyKey: rec.IdempotencyKey,
		OccurredAt:     rec.OccurredAt.UTC(),
	}
	return p.cfg.Store.Insert(ctx, row)
}

// Compile-time port assertion.
var _ ledger.OutboxRecorder = (*Publisher)(nil)
