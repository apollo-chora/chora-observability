// familiar_growth_audit_binding.go — JetStream bindings for the 7
// ADR-149 Familiar Growth audit source topics (CHO-2257), plus the STRUCTURAL
// GUARD that keeps an ingress and its subscription from drifting apart again.
//
// What went wrong (CHO-2257)
// --------------------------
// The service declared an HTTP PUSH ingress (POST
// /internal/pubsub/familiar-growth-audit) while all 7 source subscriptions were
// PULL-shaped with no push endpoint. Nothing ever called the endpoint. ~492
// events sat undelivered while the suite stayed green, because the declared
// ingress was never compared against the subscription it was meant to serve.
//
// The guard
// ---------
// familiarGrowthAuditBindings below is the SINGLE declaration of that mapping,
// and validateFamiliarGrowthAuditBindings checks it against the subscriber's own
// SubscribedTopics() contract. It runs in the unit suite AND at boot, where an
// invalid table is fatal. It rejects: a subscribed topic with no ingress (the
// CHO-2257 defect), a push-shaped ingress, a duplicate topic or subscription, an
// ingress for a topic the subscriber does not handle, and a blank subscription
// name (a silently-inert consumer).
//
// The decode-binding (message → typed event → subscriber.Handle) lives in
// internal/adapter/events (FamiliarGrowthAuditPullHandler) so it is unit-testable
// without a broker connection. This file owns only the canonical binding
// table + the consume-loop goroutines, mirroring startRitualRunAuditSubscriber.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"

	"github.com/apollo-chora/chora-common/eventbus"
	"github.com/apollo-chora/chora-observability/internal/adapter/events"
	"github.com/apollo-chora/chora-observability/internal/adapter/subscribers"
	fg "github.com/apollo-chora/chora-observability/internal/domain/familiargrowth"
)

// ingressShape is how a consumer receives a topic's events. It exists so the
// declared shape can be CHECKED rather than assumed.
type ingressShape string

const (
	// ingressPull is a StreamingPull consumer: the service subscribes to its
	// own subscription. The chora-observability convention for every consumer.
	ingressPull ingressShape = "pull"
	// ingressPush is a gateway push to an HTTP endpoint. Declared only so the
	// guard can name and reject it — no familiar-growth subscription is
	// push-shaped.
	ingressPush ingressShape = "push"
)

// familiarGrowthAuditBinding maps one source topic to the subscription that
// carries it and the shape of the ingress consuming it.
type familiarGrowthAuditBinding struct {
	Topic        string
	Subscription string
	Shape        ingressShape
}

// familiarGrowthAuditBindings is the canonical ADR-149 ingress declaration.
//
// Subscription names are pinned against deployed reality (verified
// 2026-07-17 in chora-489812: all 7 exist, all have an empty
// pushConfig.pushEndpoint, all carry a dead_letter_policy with
// maxDeliveryAttempts=5 onto chora.dlq.<topic>, and each DLQ topic has a .pull
// drain subscription). The names are preserved verbatim from the Pub/Sub era
// as the NATS durable consumer names, as with the token_usage / ritual_audit
// consumers.
var familiarGrowthAuditBindings = []familiarGrowthAuditBinding{
	{Topic: fg.TopicExpAwarded, Subscription: "chora-observability.consumption-familiar-exp_awarded", Shape: ingressPull},
	{Topic: fg.TopicStageUp, Subscription: "chora-observability.consumption-familiar-stage_up", Shape: ingressPull},
	{Topic: fg.TopicBreedRevealed, Subscription: "chora-observability.consumption-familiar-breed_revealed", Shape: ingressPull},
	{Topic: fg.TopicHatched, Subscription: "chora-observability.consumption-familiar-hatched", Shape: ingressPull},
	{Topic: fg.TopicSourceRevelation, Subscription: "chora-observability.consumption-familiar-source_revelation", Shape: ingressPull},
	{Topic: fg.TopicEggPurchased, Subscription: "chora-observability.consumption-familiar-egg_purchased", Shape: ingressPull},
	{Topic: fg.TopicPaymentSucceeded, Subscription: "chora-observability.tenancy-familiar_egg-payment_succeeded", Shape: ingressPull},
}

// validateFamiliarGrowthAuditBindings is the structural guard. subscribedTopics
// MUST be the subscriber's own SubscribedTopics() — the expectation is derived
// from the contract, never a hand-copied list, because a parallel list drifts
// silently.
//
// Returns an error naming every problem found; the caller decides whether that
// is fatal (boot) or a test failure (suite). Both are loud.
func validateFamiliarGrowthAuditBindings(subscribedTopics []string, bindings []familiarGrowthAuditBinding) error {
	if len(bindings) == 0 {
		return errors.New("familiar_growth_audit: no ingress bindings declared — every source event would sit undelivered")
	}

	var problems []string

	known := make(map[string]bool, len(subscribedTopics))
	for _, t := range subscribedTopics {
		known[t] = true
	}

	seenTopic := make(map[string]int, len(bindings))
	seenSub := make(map[string]string, len(bindings))
	for _, b := range bindings {
		topic := strings.TrimSpace(b.Topic)
		if topic == "" {
			problems = append(problems, "a binding declares an empty topic")
			continue
		}
		if !known[topic] {
			problems = append(problems, fmt.Sprintf(
				"topic %q has an ingress but the subscriber does not handle it (its events would nack-loop to the DLQ)", topic))
		}
		if b.Shape != ingressPull {
			problems = append(problems, fmt.Sprintf(
				"topic %q declares a %q ingress, but its subscription is PULL-shaped — a push ingress is never called (CHO-2257)",
				topic, b.Shape))
		}
		if strings.TrimSpace(b.Subscription) == "" {
			problems = append(problems, fmt.Sprintf(
				"topic %q has a blank subscription name — the consumer would be silently inert", topic))
		} else if prev, dup := seenSub[b.Subscription]; dup {
			problems = append(problems, fmt.Sprintf(
				"subscription %q is bound to both %q and %q — one subscription cannot serve two topics",
				b.Subscription, prev, topic))
		} else {
			seenSub[b.Subscription] = topic
		}
		seenTopic[topic]++
	}

	for topic, n := range seenTopic {
		if n > 1 {
			problems = append(problems, fmt.Sprintf(
				"topic %q has %d ingress bindings — every event would be projected %d times", topic, n, n))
		}
	}

	// The CHO-2257 defect itself: a topic the subscriber claims that nothing
	// consumes.
	for _, topic := range subscribedTopics {
		if seenTopic[topic] == 0 {
			problems = append(problems, fmt.Sprintf(
				"topic %q is in SubscribedTopics() but has NO ingress binding — its events would sit undelivered until the subscription expires (CHO-2257)",
				topic))
		}
	}

	if len(problems) > 0 {
		return fmt.Errorf("familiar_growth_audit ingress is invalid: %s", strings.Join(problems, "; "))
	}
	return nil
}

// growthHandlerWrapper decorates a per-topic handler (the ADR-167 Plane-4
// quarantine wrap in main.go). nil ⇒ no decoration.
type growthHandlerWrapper func(inner eventbus.Handler, consumerName, topic string) eventbus.Handler

// startFamiliarGrowthAuditSubscribers validates the binding table, then starts
// one JetStream consume-loop goroutine per binding. Each goroutine exits
// cleanly when ctx is canceled (graceful shutdown).
//
// The binding table is validated BEFORE anything starts, so an invalid
// declaration never yields a partially-wired lane — the caller fails loudly
// instead. Returns (nil, nil) when the event bus is unwired (tests).
func startFamiliarGrowthAuditSubscribers(
	ctx context.Context,
	bus eventbus.Subscriber,
	sub *subscribers.FamiliarGrowthAuditSubscriber,
	bindings []familiarGrowthAuditBinding,
	wrap growthHandlerWrapper,
) ([]chan struct{}, error) {
	if sub == nil {
		return nil, errors.New("familiar_growth_audit: subscriber required")
	}
	if err := validateFamiliarGrowthAuditBindings(sub.SubscribedTopics(), bindings); err != nil {
		return nil, err
	}
	if bus == nil {
		return nil, nil
	}

	// Build every handler BEFORE starting any goroutine: a construction error
	// must fail the boot, not surface as one quietly-missing consumer.
	handlers := make([]eventbus.Handler, 0, len(bindings))
	for _, b := range bindings {
		h, err := events.FamiliarGrowthAuditPullHandler(sub, b.Topic)
		if err != nil {
			return nil, fmt.Errorf("familiar_growth_audit: bind %s: %w", b.Topic, err)
		}
		if wrap != nil {
			h = wrap(h, "familiar_growth_audit", b.Topic)
		}
		handlers = append(handlers, h)
	}

	done := make([]chan struct{}, 0, len(bindings))
	for i, b := range bindings {
		b, h := b, handlers[i]
		ch := make(chan struct{})
		go func() {
			defer close(ch)
			log.Printf(
				"observability: familiar_growth_audit subscriber started (subscription=%s, topic=%s, shape=%s)",
				b.Subscription, b.Topic, b.Shape,
			)
			err := bus.Subscribe(ctx, consumerConfig(b.Subscription, b.Topic), h)
			if err != nil &&
				!errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				log.Printf(
					"observability: familiar_growth_audit subscriber exited (subscription=%s, topic=%s): %v",
					b.Subscription, b.Topic, err,
				)
			}
		}()
		done = append(done, ch)
	}
	return done, nil
}
