// ritual_run_audit_binding.go — Pub/Sub StreamingPull binding for the
// chora.consumption.familiar.ritual_run_completed.v1 consumer (O+ auditor
// projection, ADR-215 / ADR-219 CHO-2016).
//
// The decode-binding (JSON payload → typed event → consumer.Handle) lives in
// internal/adapter/events (RitualRunAuditPullHandler) so it is unit-testable
// without the Cloud Pub/Sub client. This file only owns the canonical
// subscription short-name + the StreamingPull goroutine, mirroring
// startTokenUsageSubscriber.
//
// The CloudSubscriber handles ack/nack on the broker side: handler nil → ack;
// handler error → nack → broker retry → DLQ (per the subscription's
// retry_policy + dead_letter_policy). The consumer-side quarantine wrap
// (withQuarantine in main.go) additionally dead-letters malformed events
// locally + alerts — the same ADR-167 Plane-4 fail-loud as token_usage.
package main

import (
	"context"
	"log"

	cgcpubsub "github.com/apollo-chora/chora-common/pubsub"
	"github.com/apollo-chora/chora-observability/internal/domain/ritualaudit"
)

// DefaultRitualAuditSubscription is the canonical subscription short-name; the
// full resource path resolves against the configured CHORA_PUBSUB_PROJECT
// (same as the token_usage / agent_decision consumers).
const DefaultRitualAuditSubscription = "chora-observability.observability-ritual_run_completed"

// startRitualRunAuditSubscriber starts a CloudSubscriber goroutine bound to the
// canonical ritual-audit subscription. Mirrors startTokenUsageSubscriber: the
// goroutine exits cleanly when ctx is canceled (graceful shutdown). Returns nil
// when the Pub/Sub client is unwired (CHORA_PUBSUB_PROJECT unset — dev / local
// tests fall back to the in-memory bus, which has no streaming-pull surface).
func startRitualRunAuditSubscriber(
	ctx context.Context,
	client cgcpubsub.CloudPubSubClient,
	subscription string,
	handler cgcpubsub.Handler,
) chan struct{} {
	if client == nil || handler == nil {
		return nil
	}
	if subscription == "" {
		subscription = DefaultRitualAuditSubscription
	}
	sub := cgcpubsub.NewCloudSubscriber(client)
	done := make(chan struct{})
	go func() {
		defer close(done)
		log.Printf(
			"observability: ritual_run_audit subscriber started (subscription=%s, topic=%s)",
			subscription, ritualaudit.TopicFamiliarRitualRunCompleted,
		)
		if err := sub.Subscribe(ctx, subscription, handler); err != nil &&
			err != context.Canceled && err != context.DeadlineExceeded {
			log.Printf(
				"observability: ritual_run_audit subscriber exited: %v", err,
			)
		}
	}()
	return done
}
