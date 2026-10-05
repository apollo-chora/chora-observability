// ritual_run_audit_binding.go — JetStream binding for the
// chora.consumption.familiar.ritual_run_completed.v1 consumer (O+ auditor
// projection, ADR-215 / ADR-219 CHO-2016).
//
// The decode-binding (JSON payload → typed event → consumer.Handle) lives in
// internal/adapter/events (RitualRunAuditPullHandler) so it is unit-testable
// without a broker connection. This file only owns the canonical subscription
// name + the consume-loop goroutine, mirroring startTokenUsageSubscriber.
//
// The JetStream consume loop handles ack/nak on the broker side: handler nil →
// ack; handler error → nak → broker retry → DLQ (per the stream's retry + dead-
// letter policy). The consumer-side quarantine wrap (withQuarantine in
// main.go) additionally dead-letters malformed events locally + alerts — the
// same ADR-167 Plane-4 fail-loud as token_usage.
package main

import (
	"context"
	"errors"
	"log"

	"github.com/apollo-chora/chora-common/eventbus"
	"github.com/apollo-chora/chora-observability/internal/domain/ritualaudit"
)

// DefaultRitualAuditSubscription is the canonical subscription name,
// preserved verbatim from the Pub/Sub era as the NATS durable consumer name.
const DefaultRitualAuditSubscription = "chora-observability.observability-ritual_run_completed"

// startRitualRunAuditSubscriber starts a JetStream consume-loop goroutine
// bound to the canonical ritual-audit subscription. Mirrors
// startTokenUsageSubscriber: the goroutine exits cleanly when ctx is canceled
// (graceful shutdown). Returns nil when the event bus or the handler is
// unwired (tests).
func startRitualRunAuditSubscriber(
	ctx context.Context,
	bus eventbus.Subscriber,
	subscription string,
	handler eventbus.Handler,
) chan struct{} {
	if bus == nil || handler == nil {
		return nil
	}
	if subscription == "" {
		subscription = DefaultRitualAuditSubscription
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		log.Printf(
			"observability: ritual_run_audit subscriber started (subscription=%s, topic=%s)",
			subscription, ritualaudit.TopicFamiliarRitualRunCompleted,
		)
		err := bus.Subscribe(ctx, consumerConfig(subscription, ritualaudit.TopicFamiliarRitualRunCompleted), handler)
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			log.Printf(
				"observability: ritual_run_audit subscriber exited: %v", err,
			)
		}
	}()
	return done
}
