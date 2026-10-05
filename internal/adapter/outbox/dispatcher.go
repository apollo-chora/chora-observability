// Package outbox — Dispatcher implementation.
//
// Dispatcher drains pending outbox_events rows to the NATS JetStream event
// bus. Composes Store.FetchPending → Bus.Publish → Store.MarkPublished /
// MarkFailed / Deadletter. On max-attempts exhaustion the row lands in
// outbox_dead_letters AND a broker-side DLQ subject
// (eventbus.DLQSubject(topic) = _dlq.<topic>).
//
// Per `feedback_d6_resilience_first_class` B.6.2.a — dispatcher is the
// retry + DLQ ladder for the producer-side outbox. Subscriber-side
// ack-after-processing is a separate concern (B.6.2.b).
//
// ADR-167 Plane-4 fail-loud (2026-06-01): a sink/publish failure must NEVER
// be a silent drop. Whenever a row is dead-lettered — either because the
// envelope is corrupt (malformed) or because the publish retries are
// exhausted — the Dispatcher ALSO emits a sink-failure alert event
// (`chora.observability.sink_failure.recorded.v1`) via the optional
// AlertSink so the O+ / chora-governance surface (and a future Cloud
// Monitoring alert policy on dead-letter rows) sees the failure explicitly.
// The dead-letter row + the error-level log + the alert event are three
// independent surfaces; none of them swallows the failure.
//
// Concurrency: one Dispatcher per pod is fine — Postgres's
// `SELECT ... FOR UPDATE SKIP LOCKED` (in PostgresStore.FetchPending) lets
// multiple replicas drain the same table without colliding on rows.
//
// SourceService stamp on every reconstructed envelope: "chora-observability"
// (canonical).
package outbox

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	cgcenvelope "github.com/apollo-chora/chora-common/envelope"
	cgctracing "github.com/apollo-chora/chora-common/tracing"
)

// TopicObservabilitySinkFailure is the canonical sink-failure alert topic the
// Dispatcher emits on whenever a row is dead-lettered (malformed envelope or
// publish-retries exhausted). The event remains governance evidence (IMDA D1
// accountability — a dropped observability event is an auditable gap; the
// envelope still carries the `accountability` dimension), but it is OWNED by
// the observability domain: the emitting service is chora-observability and the
// failure is an observability sink/pipeline concern.
//
// Event-fabric repair (2026-07-01): the prior name
// `chora.governance.observability_sink_failure.v1` VIOLATED the mandatory
// `chora.{domain}.{aggregate}.{event_type}.v{N}` topic rule — its tail
// `observability_sink_failure` is a single run-on segment, not a clean
// aggregate.event pair, so every emit dead-lettered (a ~854-row cascade). The
// conforming name is `chora.observability.sink_failure.recorded.v1`
// (domain=observability, aggregate=sink_failure, event_type=recorded). ADR-167
// Plane-4 introduces this dedicated topic per the brief's fail-loud directive.
const TopicObservabilitySinkFailure = "chora.observability.sink_failure.recorded.v1"

// SinkFailureReason classifies WHY a dead-letter happened (malformed vs
// publish-exhausted) so the governance alert + Cloud Monitoring policy can
// route on a stable enum rather than free-text.
type SinkFailureReason string

// SinkFailureReason enum.
const (
	// SinkFailureMalformed — the row's envelope could not be reconstructed
	// (un-parseable / structurally invalid). The event can never publish.
	SinkFailureMalformed SinkFailureReason = "malformed"
	// SinkFailurePublishExhausted — the publish retries were exhausted
	// (persistent sink/broker failure). The row is dead-lettered for replay.
	SinkFailurePublishExhausted SinkFailureReason = "publish_exhausted"
)

// SinkFailureAlert is the governance-alert payload emitted on dead-letter.
// It carries enough context for the O+ auditor view + a Cloud Monitoring
// alert policy to act without a cross-DB join (cross-DB queries forbidden).
type SinkFailureAlert struct {
	RowID         string            `json:"row_id"`
	TenantID      string            `json:"tenant_id"`
	Topic         string            `json:"topic"`          // the destination topic that failed
	EventType     string            `json:"event_type"`     // the dead-lettered event's type
	Reason        SinkFailureReason `json:"reason"`         // malformed | publish_exhausted
	FailureDetail string            `json:"failure_detail"` // bounded error string
	AttemptCount  int               `json:"attempt_count"`
	WorkerID      string            `json:"worker_id"`
	OccurredAt    time.Time         `json:"occurred_at"`
}

// AlertSink is the optional write port the Dispatcher uses to emit the
// governance sink-failure alert. Production wires an event-bus-backed
// implementation (PublisherAlertSink, see alert_sink.go); dev / tests use a
// recording stub. When unset the Dispatcher logs the alert at error level
// only (it NEVER silently swallows — the dead-letter row + log remain).
type AlertSink interface {
	EmitSinkFailure(ctx context.Context, alert SinkFailureAlert) error
}

// Bus is the event-bus publisher contract the Dispatcher uses. Matches
// `eventbus.Publisher` so the JetStream bus + the in-memory test bus slot in
// directly.
type Bus interface {
	Publish(ctx context.Context, topic string, env cgcenvelope.Envelope, payload []byte) error
}

// DispatcherConfig tunes a Dispatcher.
type DispatcherConfig struct {
	// Store is the outbox table backend. Required.
	Store Store

	// Bus is the event-bus publisher. Required.
	Bus Bus

	// WorkerID identifies the dispatcher worker that records Deadletter
	// rows. Required. Typically derived from the pod name (HOSTNAME).
	WorkerID string

	// MaxAttempts caps the per-row publish retry count. After
	// MaxAttempts the row transitions to status='deadlettered' and an
	// outbox_dead_letters row is inserted. Defaults to 5.
	MaxAttempts int

	// PollInterval is the sleep between empty drain cycles in Run.
	// Defaults to 250ms.
	PollInterval time.Duration

	// BackoffBase + BackoffCap shape the exponential backoff used by Run
	// between non-empty drain cycles when transient failures are
	// accumulating. Defaults: 50ms / 5s.
	BackoffBase time.Duration
	BackoffCap  time.Duration

	// Logger is the structured logger used for outbox_published /
	// outbox_publish_failed / outbox_deadletter lines. nil → stdlib log.
	Logger Logger

	// AlertSink is the OPTIONAL governance sink-failure publisher (ADR-167
	// Plane-4 fail-loud). When wired, a dead-letter (malformed OR publish-
	// exhausted) emits `chora.observability.sink_failure.recorded.v1`. When
	// nil the failure is still logged at error level + dead-lettered — it is
	// never swallowed; the alert event is the extra escalation surface.
	AlertSink AlertSink

	// Now is injectable for tests; defaults to time.Now().UTC.
	Now func() time.Time
}

// Logger is the minimal contract used by the Dispatcher.
type Logger interface {
	Infof(format string, args ...any)
	Warnf(format string, args ...any)
	Errorf(format string, args ...any)
}

// Dispatcher drains the outbox to a Bus.
type Dispatcher struct {
	cfg DispatcherConfig
}

// NewDispatcher constructs a Dispatcher. Panics on missing WorkerID. Defaults
// MaxAttempts=5, PollInterval=250ms, BackoffBase=50ms, BackoffCap=5s.
func NewDispatcher(cfg DispatcherConfig) *Dispatcher {
	if cfg.WorkerID == "" {
		panic("outbox: NewDispatcher: WorkerID required")
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 5
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 250 * time.Millisecond
	}
	if cfg.BackoffBase <= 0 {
		cfg.BackoffBase = 50 * time.Millisecond
	}
	if cfg.BackoffCap <= 0 {
		cfg.BackoffCap = 5 * time.Second
	}
	if cfg.Now == nil {
		cfg.Now = func() time.Time { return time.Now().UTC() }
	}
	if cfg.Logger == nil {
		cfg.Logger = defaultLogger{}
	}
	if inmem, ok := cfg.Store.(*InMemoryStore); ok {
		inmem.SetWorkerID(cfg.WorkerID)
	}
	return &Dispatcher{cfg: cfg}
}

// MaxAttempts returns the configured retry ceiling. Exposed for tests.
func (d *Dispatcher) MaxAttempts() int { return d.cfg.MaxAttempts }

// DrainOnce drains one batch of pending rows. Returns the count of
// successfully published rows.
//
// Failures are recorded via Store.MarkFailed (transient) or Store.Deadletter
// (max_attempts exhausted) — they do NOT raise. Callers run this in a loop
// with a backoff between calls (see Run).
//
// Context cancellation aborts the drain partway through; rows already
// published are committed.
func (d *Dispatcher) DrainOnce(ctx context.Context, batchSize int) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	rows, err := d.cfg.Store.FetchPending(ctx, batchSize)
	if err != nil {
		return 0, fmt.Errorf("outbox.Dispatcher.DrainOnce: %w", err)
	}

	published := 0
	for _, row := range rows {
		select {
		case <-ctx.Done():
			return published, ctx.Err()
		default:
		}
		if err := d.publishOne(ctx, &row); err == nil {
			published++
		}
	}
	return published, nil
}

// publishOne attempts a single publish + records the outcome via the Store.
//
// ADR-167 Plane-4 fail-loud: every dead-letter path (malformed envelope OR
// publish-retries exhausted) logs at ERROR level AND emits a governance
// sink-failure alert — the dead-letter row alone is not enough escalation.
func (d *Dispatcher) publishOne(ctx context.Context, row *Row) error {
	env, err := reconstructEnvelope(row)
	if err != nil {
		reason := "envelope reconstruct: " + err.Error()
		attempt := row.RetryCount + 1
		if dlErr := d.cfg.Store.Deadletter(ctx, row.ID, reason, attempt); dlErr != nil {
			d.cfg.Logger.Errorf("outbox_deadletter_failed row_id=%s err=%v", row.ID, dlErr)
		}
		d.cfg.Logger.Errorf("outbox_deadletter row_id=%s tenant=%s reason=envelope_corrupt err=%v",
			row.ID, row.TenantID, err)
		d.emitSinkFailure(ctx, row, SinkFailureMalformed, reason, attempt)
		return err
	}

	pubErr := d.cfg.Bus.Publish(ctx, row.Topic, env, row.Payload)
	if pubErr == nil {
		if mErr := d.cfg.Store.MarkPublished(ctx, row.ID); mErr != nil {
			d.cfg.Logger.Warnf("outbox_mark_published_failed row_id=%s err=%v", row.ID, mErr)
			return mErr
		}
		d.cfg.Logger.Infof("outbox_published row_id=%s tenant=%s topic=%s",
			row.ID, row.TenantID, row.Topic)
		return nil
	}

	attempt := row.RetryCount + 1
	if attempt >= d.cfg.MaxAttempts {
		if dlErr := d.cfg.Store.Deadletter(ctx, row.ID, pubErr.Error(), attempt); dlErr != nil {
			d.cfg.Logger.Errorf("outbox_deadletter_failed row_id=%s err=%v", row.ID, dlErr)
		}
		d.cfg.Logger.Errorf("outbox_deadletter row_id=%s tenant=%s attempts=%d topic=%s err=%v",
			row.ID, row.TenantID, attempt, row.Topic, pubErr)
		d.emitSinkFailure(ctx, row, SinkFailurePublishExhausted, pubErr.Error(), attempt)
		return pubErr
	}

	if mErr := d.cfg.Store.MarkFailed(ctx, row.ID, pubErr.Error()); mErr != nil {
		d.cfg.Logger.Warnf("outbox_mark_failed_failed row_id=%s err=%v", row.ID, mErr)
	}
	d.cfg.Logger.Infof("outbox_publish_failed row_id=%s attempts=%d err=%v",
		row.ID, attempt, pubErr)
	return pubErr
}

// emitSinkFailure publishes the governance sink-failure alert (ADR-167
// Plane-4). Best-effort + non-fatal: the dead-letter row + the ERROR log are
// already durable; an alert-publish failure must not crash the drain loop or
// mask the original failure. When AlertSink is unwired (dev) we still log the
// alert intent at error level so the failure is visible in logs.
//
// TODO(infra): Cloud Monitoring alert policy on dead-letter rows
// (outbox_dead_letters) + on the `chora.observability.sink_failure.recorded.v1`
// topic — out of scope here (terraform/infra), tracked separately.
func (d *Dispatcher) emitSinkFailure(ctx context.Context, row *Row, reason SinkFailureReason, detail string, attempt int) {
	alert := SinkFailureAlert{
		RowID:         row.ID,
		TenantID:      row.TenantID,
		Topic:         row.Topic,
		EventType:     row.EventType,
		Reason:        reason,
		FailureDetail: truncate(detail, 1000),
		AttemptCount:  attempt,
		WorkerID:      d.cfg.WorkerID,
		OccurredAt:    d.cfg.Now(),
	}
	if d.cfg.AlertSink == nil {
		d.cfg.Logger.Errorf("observability_sink_failure (alert sink unwired) row_id=%s tenant=%s reason=%s topic=%s",
			row.ID, row.TenantID, reason, row.Topic)
		return
	}
	if err := d.cfg.AlertSink.EmitSinkFailure(ctx, alert); err != nil {
		d.cfg.Logger.Errorf("observability_sink_failure_alert_emit_failed row_id=%s reason=%s err=%v",
			row.ID, reason, err)
	}
}

// Run drives DrainOnce in a loop until ctx is canceled, sleeping
// PollInterval between empty drain cycles. Designed for long-running
// Cloud Run / GKE workers.
//
// Returns context.Canceled / context.DeadlineExceeded when the caller
// stops the dispatcher.
func (d *Dispatcher) Run(ctx context.Context, batchSize int) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, err := d.DrainOnce(ctx, batchSize)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return err
			}
			d.cfg.Logger.Warnf("outbox_drain_error err=%v", err)
		}
		if n == 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(d.cfg.PollInterval):
			}
		}
	}
}

// reconstructEnvelope rebuilds the typed cgcenvelope.Envelope from the
// flat string map persisted in the JSONB column. The producer-side
// Publisher persists the same shape; the Dispatcher reverses it.
//
// SourceService defaults to "chora-observability" when missing (it is the
// canonical stamp for this service).
//
// Traceparent is GUARANTEED non-empty via tracing.EnsureTraceparent (event-
// fabric repair 2026-07-01): a valid persisted W3C traceparent is preserved,
// but a missing/empty/malformed one is replaced with a freshly-minted
// synthetic root. This is the final choke point before Bus.Publish, and the
// mandatory-field envelope contract REJECTS an empty traceparent. The shared
// chora_observability.outbox_events table is drained here for EVERY producer
// (this service's LedgerHook Publisher AND chora-model-gateway's rows, see
// store.go FetchPending) — those cross-writer rows lacked a snake_case
// traceparent and previously dead-lettered ~168 token_usage.recorded events.
// EnsureTraceparent here is defence-in-depth complementing the producer-side
// guarantee, so no un-traced envelope is ever emitted regardless of origin.
func reconstructEnvelope(row *Row) (cgcenvelope.Envelope, error) {
	env := cgcenvelope.Envelope{
		EventID:            row.Envelope["event_id"],
		IdempotencyKey:     row.Envelope["idempotency_key"],
		TenantID:           row.Envelope["tenant_id"],
		GCID:               row.Envelope["gcid"],
		Traceparent:        cgctracing.EnsureTraceparent(row.Envelope["traceparent"]),
		Tracestate:         row.Envelope["tracestate"],
		SourceProject:      row.Envelope["source_project"],
		SourceService:      row.Envelope["source_service"],
		ChoraImdaDimension: row.Envelope["chora_imda_dimension"],
		ImdaLifecycleStage: row.Envelope["imda_lifecycle_stage"],
	}
	if v := row.Envelope["schema_version"]; v != "" {
		var n int32
		_, _ = fmt.Sscanf(v, "%d", &n)
		if n <= 0 {
			n = 1
		}
		env.SchemaVersion = n
	} else {
		env.SchemaVersion = 1
	}

	occurred := parseTime(row.Envelope["occurred_at"], row.OccurredAt)
	published := parseTime(row.Envelope["published_at"], time.Now().UTC())
	env.OccurredAt = occurred
	env.PublishedAt = published

	if env.EventID == "" {
		env.EventID = row.ID
	}
	if env.IdempotencyKey == "" {
		env.IdempotencyKey = row.IdempotencyKey
	}
	if env.TenantID == "" {
		env.TenantID = row.TenantID
	}
	if env.SourceProject == "" {
		env.SourceProject = "chora-local"
	}
	if env.SourceService == "" {
		env.SourceService = "chora-observability"
	}
	return env, nil
}

func parseTime(s string, fallback time.Time) time.Time {
	if s == "" {
		return fallback
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t
	}
	return fallback
}

// -----------------------------------------------------------------------------
// defaultLogger — stdlib log fallback.
// -----------------------------------------------------------------------------

type defaultLogger struct{}

func (defaultLogger) Infof(format string, args ...any) {
	log.Printf("INFO  "+format, args...)
}
func (defaultLogger) Warnf(format string, args ...any) {
	log.Printf("WARN  "+format, args...)
}
func (defaultLogger) Errorf(format string, args ...any) {
	log.Printf("ERROR "+format, args...)
}
