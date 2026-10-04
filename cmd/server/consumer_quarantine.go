// consumer_quarantine.go — ADR-167 Plane-4 consumer-side fail-loud.
//
// The token_usage + agent_decision Pub/Sub bindings proto.Unmarshal an
// inbound message and call the typed consumer's Handle. When the message is
// MALFORMED — un-decodable proto, or a structurally-invalid event the
// consumer's validation rejects — the broker would normally Nack → retry →
// (eventually) route to the Pub/Sub-side DLQ subscription. That is correct,
// but it is INVISIBLE to the chora-observability operator + the O+ /
// chora-governance surface until someone inspects the broker DLQ.
//
// Per the ADR-167 Plane-4 fail-loud directive, a malformed inbound event must
// ALSO be:
//
//   1. routed to the LOCAL dead-letter store (outbox_dead_letters) so it is
//      queryable + replayable from chora_observability without touching the
//      broker, AND
//   2. logged at ERROR level with the event id + reason (never swallowed), AND
//   3. surfaced as a governance alert (chora.observability.sink_failure.recorded.v1)
//      via the same AlertSink the dispatcher uses.
//
// The wrapper still returns the original error to the binding so the broker
// ALSO Nacks (defence in depth — the local quarantine and the broker DLQ are
// independent surfaces; neither is allowed to be the single point of trust).
//
// Why Insert-then-Deadletter: outbox_dead_letters.outbox_event_id is a FK to
// outbox_events(id) (migration 0003), so a dead-letter row requires a parent
// events row. We synthesise a quarantine outbox_events row (status flips to
// 'deadlettered' immediately) rather than altering the schema — migrations are
// out of scope per the task constraints.
package main

import (
	"context"
	"errors"
	"log"
	"time"

	cgcpubsub "github.com/5007-Capstone/chora/libs/chora-go-common/pubsub"
	obsoutbox "github.com/5007-Capstone/chora/services/chora-observability/internal/adapter/outbox"
)

// quarantiningHandler wraps a downstream pubsub.Handler with the ADR-167
// Plane-4 fail-loud quarantine: on handler error it writes a local
// dead-letter row + emits a governance alert + logs at error, then returns
// the original error so the broker also Nacks.
type quarantiningHandler struct {
	consumerName string
	topic        string
	inner        cgcpubsub.Handler
	store        obsoutbox.Store
	alert        obsoutbox.AlertSink
	logger       obsoutbox.Logger
	now          func() time.Time
}

// quarantineDeps wires a quarantiningHandler.
type quarantineDeps struct {
	ConsumerName string
	Topic        string
	Store        obsoutbox.Store
	Alert        obsoutbox.AlertSink
	Logger       obsoutbox.Logger
	Now          func() time.Time
}

// withQuarantine wraps inner with consumer-side fail-loud handling. When the
// store is nil (dev path without a DB pool) the wrapper degrades to
// log-at-error + alert (still never silent); the broker Nack remains.
func withQuarantine(inner cgcpubsub.Handler, deps quarantineDeps) cgcpubsub.Handler {
	if inner == nil {
		panic("consumer_quarantine: nil inner handler")
	}
	now := deps.Now
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	logger := deps.Logger
	if logger == nil {
		logger = defaultQuarantineLogger{}
	}
	h := &quarantiningHandler{
		consumerName: deps.ConsumerName,
		topic:        deps.Topic,
		inner:        inner,
		store:        deps.Store,
		alert:        deps.Alert,
		logger:       logger,
		now:          now,
	}
	return h.handle
}

func (q *quarantiningHandler) handle(ctx context.Context, msg *cgcpubsub.Message) error {
	err := q.inner(ctx, msg)
	if err == nil {
		return nil
	}
	// Malformed / un-decodable / validation-rejected inbound event. FAIL LOUD.
	rowID, tenantID := quarantineIdentity(q.consumerName, msg)
	reason := q.consumerName + " inbound event rejected: " + err.Error()

	q.logger.Errorf(
		"consumer_quarantine consumer=%s topic=%s event_id=%s tenant=%s reason=%v",
		q.consumerName, q.topic, rowID, tenantID, err,
	)

	q.quarantineToStore(ctx, rowID, tenantID, msg, reason)
	q.emitAlert(ctx, rowID, tenantID, reason)

	// Return the ORIGINAL error so the broker Nacks → retry → broker-side DLQ
	// (defence in depth alongside the local dead-letter row above).
	return err
}

// quarantineToStore best-effort writes a local dead-letter row. The malformed
// payload is preserved in the synthetic outbox_events row so it is replayable.
// Best-effort: a quarantine-store failure is logged but never masks the
// original error (the broker Nack still fires).
func (q *quarantiningHandler) quarantineToStore(ctx context.Context, rowID, tenantID string, msg *cgcpubsub.Message, reason string) {
	if q.store == nil {
		// Dev path (no DB pool) — the error log + alert above are the surfaces.
		return
	}
	now := q.now()
	// Topic is set to the governance sink-failure topic (NOT the inbound
	// topic) so that even if the shared producer-side dispatcher claims this
	// synthetic row in the sub-millisecond window between Insert (status
	// 'pending') and Deadletter (status 'deadlettered'), it would re-publish a
	// benign alert-shaped event onto the alert topic rather than re-injecting
	// the malformed payload back onto the inbound topic. In the common path
	// Deadletter flips status first, so the dispatcher never sees the row.
	row := obsoutbox.Row{
		ID:             rowID,
		TenantID:       tenantID,
		EventType:      q.consumerName + ".malformed",
		Topic:          obsoutbox.TopicObservabilitySinkFailure,
		Payload:        clonePayload(msg),
		Envelope:       map[string]string{"quarantine_reason": truncateStr(reason, 500)},
		IdempotencyKey: "quarantine:" + q.consumerName + ":" + rowID,
		OccurredAt:     now,
	}
	if err := q.store.Insert(ctx, row); err != nil {
		// A duplicate idempotency_key means we already quarantined this exact
		// malformed event — that's the idempotent happy path, not a failure.
		if isDuplicate(err) {
			return
		}
		q.logger.Errorf("consumer_quarantine_store_insert_failed consumer=%s event_id=%s err=%v",
			q.consumerName, rowID, err)
		return
	}
	if err := q.store.Deadletter(ctx, row.ID, reason, 1); err != nil {
		q.logger.Errorf("consumer_quarantine_deadletter_failed consumer=%s event_id=%s err=%v",
			q.consumerName, rowID, err)
	}
}

// emitAlert publishes the governance sink-failure alert for the malformed
// inbound event. Best-effort + non-fatal.
func (q *quarantiningHandler) emitAlert(ctx context.Context, rowID, tenantID, reason string) {
	if q.alert == nil {
		q.logger.Errorf("consumer_quarantine_alert_unwired consumer=%s event_id=%s", q.consumerName, rowID)
		return
	}
	alert := obsoutbox.SinkFailureAlert{
		RowID:         rowID,
		TenantID:      tenantID,
		Topic:         q.topic,
		EventType:     q.consumerName + ".malformed",
		Reason:        obsoutbox.SinkFailureMalformed,
		FailureDetail: truncateStr(reason, 1000),
		AttemptCount:  1,
		WorkerID:      outboxWorkerID(),
		OccurredAt:    q.now(),
	}
	if err := q.alert.EmitSinkFailure(ctx, alert); err != nil {
		q.logger.Errorf("consumer_quarantine_alert_emit_failed consumer=%s event_id=%s err=%v",
			q.consumerName, rowID, err)
	}
}

// quarantineIdentity extracts a stable (event_id, tenant_id) for the
// quarantine row from the message routing attributes. A malformed proto body
// may still carry valid routing attrs; when even those are absent we synthesise
// a time-based row id so the quarantine never collides + is never blank.
func quarantineIdentity(consumerName string, msg *cgcpubsub.Message) (rowID, tenantID string) {
	if msg != nil {
		rowID = msg.Envelope.EventID
		tenantID = msg.Envelope.TenantID
	}
	if rowID == "" {
		rowID = "quarantine-" + consumerName + "-" + time.Now().UTC().Format("20060102T150405.000000000")
	}
	if tenantID == "" {
		tenantID = "platform"
	}
	return rowID, tenantID
}

func clonePayload(msg *cgcpubsub.Message) []byte {
	if msg == nil || msg.Payload == nil {
		return []byte{}
	}
	return append([]byte(nil), msg.Payload...)
}

func isDuplicate(err error) bool {
	return err != nil && errors.Is(err, obsoutbox.ErrDuplicateIdempotencyKey)
}

func truncateStr(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

// defaultQuarantineLogger is the stdlib-log fallback satisfying
// obsoutbox.Logger when no structured logger is injected.
type defaultQuarantineLogger struct{}

func (defaultQuarantineLogger) Infof(format string, args ...any)  { log.Printf("INFO  "+format, args...) }
func (defaultQuarantineLogger) Warnf(format string, args ...any)  { log.Printf("WARN  "+format, args...) }
func (defaultQuarantineLogger) Errorf(format string, args ...any) { log.Printf("ERROR "+format, args...) }
