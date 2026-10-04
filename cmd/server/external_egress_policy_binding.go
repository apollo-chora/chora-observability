// external_egress_policy_binding.go — Pub/Sub StreamingPull binding for the
// chora.tenancy.external_egress_policy.updated.v1 projection (CHO-2148).
//
// chora-tenancy owns the tenant external web-egress entitlement. The
// model-gateway enforces it fail-closed on every grounded call, but it reads
// from chora_observability.external_egress_policy — a different database.
// Cross-DB writes are forbidden, so THIS SUBSCRIBER IS THE ONLY BRIDGE.
//
// Subscription provisioned out-of-band via gcloud on 2026-07-14 and recorded in
// chora-infra/terraform/environments/dev/main.tf:
//
//	"chora-observability.tenancy-external_egress_policy-updated" = {
//	  subscriber = "chora-observability"
//	  topic      = "chora.tenancy.external_egress_policy.updated.v1"
//	}
//
// The topic is Schema-Registry-bound with encoding=BINARY, so the payload is
// binary protobuf. proto.Unmarshal failure returns an error (FAIL LOUD) →
// CloudSubscriber NACKs → broker retries → DLQ. NO JSON fallback, NO silent
// drop — a dropped egress event leaves the gateway serving a stale entitlement
// with nothing to alert on.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"google.golang.org/protobuf/proto"

	cgcpubsub "github.com/apollo-chora/chora-common/pubsub"
	tenancyv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/tenancy/v1"
	"github.com/apollo-chora/chora-observability/internal/adapter/events"
	"github.com/apollo-chora/chora-observability/internal/domain/externalegress"
)

// DefaultExternalEgressSubscription is the canonical subscription short-name.
const DefaultExternalEgressSubscription = "chora-observability.tenancy-external_egress_policy-updated"

// buildExternalEgressPolicyHandler decodes the BINARY-protobuf
// ExternalEgressPolicyUpdated payload and hands it to the projection consumer.
//
// Envelope precedence matches the token_usage binding: the proto EventEnvelope
// (field 1) is AUTHORITATIVE; the Pub/Sub routing attributes are read only as a
// fallback when the proto envelope is absent or blank.
//
// Extracted as a free function so a unit test can drive it without a Pub/Sub
// client or a goroutine.
func buildExternalEgressPolicyHandler(cons *events.ExternalEgressPolicyConsumer) cgcpubsub.Handler {
	if cons == nil {
		panic("external_egress_policy_binding: nil ExternalEgressPolicyConsumer")
	}
	return func(ctx context.Context, msg *cgcpubsub.Message) error {
		if msg == nil {
			return fmt.Errorf("external_egress_policy_binding: nil message")
		}

		var rec tenancyv1.ExternalEgressPolicyUpdated
		if err := proto.Unmarshal(msg.Payload, &rec); err != nil {
			return fmt.Errorf("external_egress_policy_binding: proto decode payload: %w", err)
		}

		penv := rec.GetEnvelope()
		attrs := msg.Envelope

		eventID := firstNonBlank(penv.GetEventId(), attrs.EventID)
		tenantID := firstNonBlank(penv.GetTenantId(), rec.GetTenantId(), attrs.TenantID)
		actor := firstNonBlank(rec.GetUpdatedByGcid(), penv.GetGcid(), attrs.GCID)

		// updated_at is the canonical clock for the audit trail; fall back to the
		// envelope's occurred_at, then the routing attr.
		var occurredAt time.Time
		if t := rec.GetUpdatedAt(); t != nil {
			occurredAt = t.AsTime()
		} else if t := penv.GetOccurredAt(); t != nil {
			occurredAt = t.AsTime()
		} else {
			occurredAt = attrs.OccurredAt
		}

		ev := externalegress.PolicyChanged{
			EventID:                  eventID,
			TenantID:                 tenantID,
			EgressEnabled:            rec.GetEgressEnabled(),
			DailyCallCeiling:         rec.GetDailyCallCeiling(),
			Version:                  rec.GetVersion(),
			UpdatedByGCID:            actor,
			PreviousEgressEnabled:    rec.GetPreviousEgressEnabled(),
			PreviousDailyCallCeiling: rec.GetPreviousDailyCallCeiling(),
			OccurredAt:               occurredAt,
		}
		return cons.Handle(ctx, ev)
	}
}

// startExternalEgressPolicySubscriber starts the StreamingPull goroutine.
// Returns nil when the Pub/Sub client is unwired (dev / local tests).
func startExternalEgressPolicySubscriber(
	ctx context.Context,
	client cgcpubsub.CloudPubSubClient,
	subscription string,
	handler cgcpubsub.Handler,
) chan struct{} {
	if client == nil || handler == nil {
		return nil
	}
	if subscription == "" {
		subscription = DefaultExternalEgressSubscription
	}
	sub := cgcpubsub.NewCloudSubscriber(client)
	done := make(chan struct{})
	go func() {
		defer close(done)
		log.Printf(
			"observability: external_egress projection subscriber started (subscription=%s, topic=%s)",
			subscription, events.TopicExternalEgressPolicyUpdated,
		)
		if err := sub.Subscribe(ctx, subscription, handler); err != nil &&
			err != context.Canceled && err != context.DeadlineExceeded {
			log.Printf("observability: external_egress projection subscriber exited: %v", err)
		}
	}()
	return done
}
