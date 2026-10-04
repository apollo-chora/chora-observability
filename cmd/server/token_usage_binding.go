// token_usage_binding.go — Pub/Sub StreamingPull binding for the
// chora.observability.token_usage.recorded.v1 consumer (Gate #7 WIRE1
// 2026-05-17).
//
// Per [[ai-cost-tracking]] + the [[event-driven]] + [[pub-sub-topology]]
// skills, the chora-observability service is the canonical consumer of
// its own TokenUsageLedger ingestion topic. The producer is the
// chora-ai-kernel-orchestrator's TokenUsageLedgerOutboxWriter (gate #7
// producer-side, landed at commit 1b94c12f → composition root wired in
// the same patch as this file).
//
// Subscription resource name per chora-infra/terraform/environments/
// dev/main.tf §1538-1541:
//
//	"chora-observability.observability-token_usage-recorded" = {
//	  subscriber = "chora-observability"
//	  topic      = "chora.observability.token_usage.recorded.v1"
//	}
//
// The CloudSubscriber adapter handles ack/nack on the broker side per
// the retry_policy + dead_letter_policy provisioned in the same
// terraform module. Handler errors → Nack → broker retries → DLQ.
//
// D6 4-pillar contract (consumer-side):
//
//   - P1 pod-death survival — Pub/Sub at-least-once delivery + the
//     ledger.Repository.Append happens in chora_observability DB on the
//     same pgxpool the rest of the service uses, so pod death between
//     ack + persist is impossible (Append is the durable side-effect).
//   - P2 delivery resilience — idempotent.Store (in-memory dev, Postgres
//     prod via bootstrapInbox) dedupes on event_id across retries. The
//     broker DLQ handles persistent failures.
//   - P3 multi-tenant isolation — TenantID is in envelope + payload;
//     ledger.Append writes to per-tenant rows; no cross-tenant
//     cross-pollination.
//   - P4 Cloud Trace attribution — Traceparent carried in the envelope is
//     persisted on the ledger row via TokenUsageConsumer.persist.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"google.golang.org/protobuf/proto"

	cgcpubsub "github.com/apollo-chora/chora-common/pubsub"
	observabilityv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/observability/v1"
	"github.com/apollo-chora/chora-observability/internal/adapter/events"
)

// DefaultTokenUsageSubscription is the canonical subscription resource
// short-name. The full resource path is built from the same
// CHORA_PUBSUB_PROJECT env var that publishes use; the broker library
// accepts a short name + resolves against the configured project.
//
// Provisioned in chora-infra/terraform/environments/dev/main.tf §1538.
const DefaultTokenUsageSubscription = "chora-observability.observability-token_usage-recorded"

// buildTokenUsageHandler returns a pubsub.Handler that proto.Unmarshal the
// producer's BINARY-protobuf TokenUsageRecorded payload into the consumer's
// typed event + calls TokenUsageConsumer.Handle.
//
// ADR-167 Phase 2 (JSON→Protobuf migration): the topic is
// Schema-Registry-bound with encoding=BINARY. proto.Unmarshal failure
// returns an error (FAIL LOUD) so the CloudSubscriber NACKs → broker
// retries → DLQ. NO JSON fallback; NO silent drop; Protobuf only.
//
// Envelope precedence: the proto EventEnvelope (field 1) is AUTHORITATIVE.
// The routing attrs (cgcpubsub.Message.Envelope, reconstructed from Pub/Sub
// message attributes) are read only as a fallback when the proto envelope
// is absent / blank.
//
// Field mapping (proto ⇄ TokenUsageRecordedEvent):
//
//	envelope.event_id      → EventID
//	envelope.tenant_id     → TenantID (proto tenant_id field 3 as 2nd fallback)
//	envelope.gcid          → GCID
//	model_id               → ModelID
//	agent_role             → AgentRole (→ ledger AGID slot)
//	input_tokens           → InputTokens
//	output_tokens          → OutputTokens
//	cached_tokens          → CachedTokens
//	cost_micros            → CostMicros
//	invocation_id          → InvocationID
//	envelope.traceparent   → Traceparent
//	envelope.tracestate    → Tracestate
//	recorded_at            → OccurredAt (envelope.occurred_at as fallback)
//
// Extracted as a free function so a unit test can call it without
// spinning up the Cloud Pub/Sub client or a goroutine.
func buildTokenUsageHandler(cons *events.TokenUsageConsumer) cgcpubsub.Handler {
	if cons == nil {
		panic("token_usage_binding: nil TokenUsageConsumer")
	}
	return func(ctx context.Context, msg *cgcpubsub.Message) error {
		if msg == nil {
			return fmt.Errorf("token_usage_binding: nil message")
		}
		var rec observabilityv1.TokenUsageRecorded
		if err := proto.Unmarshal(msg.Payload, &rec); err != nil {
			return fmt.Errorf("token_usage_binding: proto decode payload: %w", err)
		}

		// Proto envelope wins; routing attrs are the fallback.
		penv := rec.GetEnvelope()
		attrs := msg.Envelope

		eventID := firstNonBlank(penv.GetEventId(), attrs.EventID)
		// tenant_id: proto envelope → proto tenant_id field 3 → routing attrs.
		tenantID := firstNonBlank(penv.GetTenantId(), rec.GetTenantId(), attrs.TenantID)
		gcid := firstNonBlank(penv.GetGcid(), rec.GetGcid(), attrs.GCID)
		traceparent := firstNonBlank(penv.GetTraceparent(), attrs.Traceparent)
		tracestate := firstNonBlank(penv.GetTracestate(), attrs.Tracestate)

		// recorded_at is the canonical clock; fall back to the proto
		// envelope occurred_at, then the routing-attr occurred_at.
		var occurredAt time.Time
		if t := rec.GetRecordedAt(); t != nil {
			occurredAt = t.AsTime()
		} else if t := penv.GetOccurredAt(); t != nil {
			occurredAt = t.AsTime()
		} else {
			occurredAt = attrs.OccurredAt
		}

		ev := events.TokenUsageRecordedEvent{
			EventID:      eventID,
			TenantID:     tenantID,
			GCID:         gcid,
			ModelID:      rec.GetModelId(),
			AgentRole:    rec.GetAgentRole(),
			InputTokens:  rec.GetInputTokens(),
			OutputTokens: rec.GetOutputTokens(),
			CachedTokens: rec.GetCachedTokens(),
			CostMicros:   rec.GetCostMicros(),
			InvocationID: rec.GetInvocationId(),
			Traceparent:  traceparent,
			Tracestate:   tracestate,
			OccurredAt:   occurredAt,
		}
		return cons.Handle(ctx, ev)
	}
}

// firstNonBlank returns the first non-empty (trim-aware) string.
func firstNonBlank(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

// startTokenUsageSubscriber starts a CloudSubscriber goroutine bound to
// the canonical token_usage subscription. The goroutine exits when ctx
// is canceled (graceful shutdown) or the broker returns a permanent
// error (logged + retried per Pub/Sub client defaults).
//
// The supplied handler is the ADR-167 Plane-4 quarantine-wrapped
// buildTokenUsageHandler (see main.go) so a malformed inbound event
// fails loud: local dead-letter row + error log + governance alert, plus
// the broker Nack the handler error triggers.
//
// Returns nil when the Pub/Sub client is unwired (CHORA_PUBSUB_PROJECT
// unset — dev / local tests fall back to the in-memory bus, which does
// NOT have a streaming-pull surface; the analytics in-process
// subscriber covers the dev path).
func startTokenUsageSubscriber(
	ctx context.Context,
	client cgcpubsub.CloudPubSubClient,
	subscription string,
	handler cgcpubsub.Handler,
) chan struct{} {
	if client == nil || handler == nil {
		return nil
	}
	if subscription == "" {
		subscription = DefaultTokenUsageSubscription
	}
	sub := cgcpubsub.NewCloudSubscriber(client)
	done := make(chan struct{})
	go func() {
		defer close(done)
		log.Printf(
			"observability: token_usage subscriber started (subscription=%s, topic=%s)",
			subscription, events.TopicTokenUsageRecorded,
		)
		if err := sub.Subscribe(ctx, subscription, handler); err != nil &&
			err != context.Canceled && err != context.DeadlineExceeded {
			log.Printf(
				"observability: token_usage subscriber exited: %v", err,
			)
		}
	}()
	return done
}
