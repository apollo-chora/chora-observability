// familiar_growth_audit_consumer.go — StreamingPull decode-binding for the 7
// ADR-149 Familiar Growth audit topics (CHO-2257).
//
// chora-observability OWNS the Familiar Growth audit projection. chora-
// consumption (6 topics) and chora-tenancy (1 topic) publish the source events;
// this binding decodes one inbound message and hands a typed
// subscribers.FamiliarGrowthEvent to the already-built
// FamiliarGrowthAuditSubscriber, which projects it into the 4 audit tables.
//
// PULL / StreamingPull — the service subscribes to its OWN subscriptions (NOT a
// gateway push). Mirrors events.RitualRunAuditPullHandler / TokenUsageConsumer /
// AgentDecisionConsumer; the StreamingPull binding lives in cmd/server.
//
// Why the topic is BOUND rather than read off the message
// -------------------------------------------------------
// A pull consumer is bound to one subscription, and a subscription is bound to
// exactly one topic, so the topic is known statically at wire time. The push
// handler this replaces derived the topic from msg.Attributes["topic"] and
// silently ack-and-dropped anything it could not match. That attribute is
// stamped by CloudPublisher (chora-common/pubsub.Publish) but was added
// AFTER the oldest events on these subscriptions were published, so an
// attribute-derived topic is not safe for the existing backlog: it would have
// discarded precisely the events this lane exists to audit.
//
// The delivered topic is therefore used only as a CROSS-CHECK. When it is
// present and disagrees with the bound topic the message is genuinely misrouted
// — that fails loud (nack → broker retry → DLQ), never a silent ack.
//
// Hexagonal:
//   - INBOUND ADAPTER from Pub/Sub (the StreamingPull binding lives in cmd/server).
//   - depends on the subscribers package (local projection only — no cross-DB
//     queries per the ddd-enforcement HARD RULE).
//   - idempotency is the subscriber's inbox, keyed (source_topic, event_id);
//     the audit table's UNIQUE(source_event_id) is the durable second guard.
package events

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	cgcpubsub "github.com/apollo-chora/chora-common/pubsub"
	"github.com/apollo-chora/chora-observability/internal/adapter/events/protodecode"
	"github.com/apollo-chora/chora-observability/internal/adapter/subscribers"
	fg "github.com/apollo-chora/chora-observability/internal/domain/familiargrowth"
)

// FamiliarGrowthAuditPullHandler adapts the FamiliarGrowthAuditSubscriber to a
// cgcpubsub.Handler for the StreamingPull binding on ONE subscription.
//
// boundTopic is the topic the target subscription is bound to and MUST be one
// of sub.SubscribedTopics() — the valid set is derived from the subscriber's own
// contract so a topic added there can never silently lack a handler.
//
// Construction errors are returned (not panicked) so cmd/server can fail loudly
// at boot with the offending topic named, rather than discovering the problem on
// the first message — or, as in CHO-2257, never discovering it at all.
//
// The CloudSubscriber acks on nil and nacks on error (ack-after-processing per
// D6.2); the broker then retries and routes to the subscription's DLQ.
func FamiliarGrowthAuditPullHandler(
	sub *subscribers.FamiliarGrowthAuditSubscriber,
	boundTopic string,
) (cgcpubsub.Handler, error) {
	if sub == nil {
		return nil, errors.New("events: familiar_growth_audit pull handler requires a subscriber")
	}
	boundTopic = strings.TrimSpace(boundTopic)
	if boundTopic == "" {
		return nil, errors.New("events: familiar_growth_audit pull handler requires a bound topic")
	}
	if !isSubscribedGrowthTopic(sub, boundTopic) {
		return nil, fmt.Errorf(
			"events: familiar_growth_audit bound topic %q is not one of the subscriber's topics %v",
			boundTopic, sub.SubscribedTopics(),
		)
	}

	return func(ctx context.Context, msg *cgcpubsub.Message) error {
		if msg == nil {
			return errors.New("events: familiar_growth_audit received a nil message")
		}
		// Cross-check only — see the file header on why the BOUND topic wins.
		if msg.Topic != "" && msg.Topic != boundTopic {
			return fmt.Errorf(
				"events: familiar_growth_audit misrouted message: delivered topic %q on the subscription bound to %q",
				msg.Topic, boundTopic,
			)
		}
		ev, err := buildFamiliarGrowthEvent(boundTopic, msg)
		if err != nil {
			return fmt.Errorf("events: familiar_growth_audit decode (%s): %w", boundTopic, err)
		}
		if err := sub.Handle(ctx, ev); err != nil {
			return fmt.Errorf("events: familiar_growth_audit handle (%s): %w", boundTopic, err)
		}
		return nil
	}, nil
}

// isSubscribedGrowthTopic reports whether topic is one the subscriber handles.
// Derived from SubscribedTopics() rather than a hand-written list — a parallel
// list drifts silently.
func isSubscribedGrowthTopic(sub *subscribers.FamiliarGrowthAuditSubscriber, topic string) bool {
	for _, t := range sub.SubscribedTopics() {
		if t == topic {
			return true
		}
	}
	return false
}

// buildFamiliarGrowthEvent hydrates the subscriber's input struct from the
// delivered message.
//
// Identity (event_id / tenant_id / gcid / traceparent / occurred_at) comes from
// the transport ENVELOPE, which CloudSubscriber reconstructs from the canonical
// publisher attributes; the payload body supplies only the topic-specific
// fields. The payload is decoded binary-proto-first with a JSON fallback
// (protodecode), so both the current binary wire shape and any legacy JSON rows
// still draining decode correctly.
func buildFamiliarGrowthEvent(boundTopic string, msg *cgcpubsub.Message) (subscribers.FamiliarGrowthEvent, error) {
	env := msg.Envelope
	// No event_id ⇒ no dedupe key ⇒ the inbox cannot protect the audit table
	// from at-least-once redelivery. Refuse rather than project a row that
	// cannot be deduplicated.
	if strings.TrimSpace(env.EventID) == "" {
		return subscribers.FamiliarGrowthEvent{}, errors.New("envelope event_id is empty")
	}

	payload, err := protodecode.DecodePayloadMap(boundTopic, msg.Payload)
	if err != nil {
		return subscribers.FamiliarGrowthEvent{}, err
	}

	ev := subscribers.FamiliarGrowthEvent{
		SourceTopic:   boundTopic,
		SourceEventID: env.EventID,
		TenantID:      firstNonEmpty(env.TenantID, strField(payload, "tenant_id")),
		OwnerGCID:     firstNonEmpty(env.GCID, strField(payload, "owner_gcid")),
		Traceparent:   env.Traceparent,
		FamiliarID:    strField(payload, "familiar_id"),
		Payload:       payload,
		OccurredAt:    env.OccurredAt,
	}
	if ev.OccurredAt.IsZero() {
		if v := strField(payload, "occurred_at"); v != "" {
			if t, perr := time.Parse(time.RFC3339Nano, v); perr == nil {
				ev.OccurredAt = t
			}
		}
	}

	// Topic-specific payload projection. An absent field passes through as its
	// zero value; the subscriber's per-topic switch reads only the subset
	// relevant to SourceTopic.
	switch boundTopic {
	case fg.TopicExpAwarded:
		ev.ExpDelta = int32Field(payload, "exp_delta")
		ev.ExpSource = strField(payload, "exp_source")
	case fg.TopicStageUp:
		ev.StageFrom = int32Field(payload, "stage_from")
		ev.StageTo = int32Field(payload, "stage_to")
	case fg.TopicBreedRevealed:
		ev.EggSKU = strField(payload, "egg_sku")
		ev.Species = strField(payload, "species")
		ev.Rarity = strField(payload, "rarity")
		ev.Shiny = boolField(payload, "shiny")
		ev.RolledProbability = floatField(payload, "rolled_probability")
		if v, ok := payload["distribution_snapshot"].(map[string]any); ok {
			ev.DistributionSnapshot = v
		}
	case fg.TopicHatched:
		// hatched.v1 drives the funnel hatched count directly (Fix-D
		// 2026-05-16); no topic-specific scalar projection is needed.
	case fg.TopicSourceRevelation:
		ev.WindowDurationSeconds = int32Field(payload, "window_duration_seconds")
	case fg.TopicEggPurchased:
		ev.EggSKU = strField(payload, "egg_sku")
		ev.PurchaseSource = strField(payload, "purchase_source")
	case fg.TopicPaymentSucceeded:
		ev.EggSKU = strField(payload, "egg_sku")
		ev.AmountCents = int64Field(payload, "amount_cents")
		ev.Currency = strField(payload, "currency")
	default:
		// Unreachable: the bound topic is validated at construction. Kept as a
		// fail-loud backstop rather than a silent pass-through.
		return subscribers.FamiliarGrowthEvent{}, fmt.Errorf("unhandled bound topic %q", boundTopic)
	}
	return ev, nil
}

// ---------------------------------------------------------------------------
// Payload helpers — project protodecode's wire-format-tolerant snake_case map
// into the subscriber's typed struct. A wrong-typed field reads as its zero
// value; the subscriber validates what it requires.
// ---------------------------------------------------------------------------

func strField(m map[string]any, key string) string {
	if v, ok := m[key].(string); ok {
		return v
	}
	return ""
}

func boolField(m map[string]any, key string) bool {
	if v, ok := m[key].(bool); ok {
		return v
	}
	return false
}

func floatField(m map[string]any, key string) float64 {
	switch v := m[key].(type) {
	case float64:
		return v
	case float32:
		return float64(v)
	}
	return 0
}

// int32Field tolerates both the JSON path (float64) and the binary-proto
// projection path (int), which protodecode emits for integer fields.
func int32Field(m map[string]any, key string) int32 {
	switch v := m[key].(type) {
	case float64:
		return int32(v)
	case int:
		return int32(v)
	case int32:
		return v
	case int64:
		return int32(v)
	}
	return 0
}

func int64Field(m map[string]any, key string) int64 {
	switch v := m[key].(type) {
	case float64:
		return int64(v)
	case int:
		return int64(v)
	case int32:
		return int64(v)
	case int64:
		return v
	}
	return 0
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}
