// familiar_growth_audit_binding_test.go — RED→GREEN unit tests for the ADR-149
// Familiar Growth audit StreamingPull bindings + the STRUCTURAL GUARD that
// makes CHO-2257 non-repeatable.
//
// CHO-2257: the service declared an HTTP PUSH ingress while all 7 source
// subscriptions were PULL-shaped. Nothing ever called the push endpoint, ~492
// events sat undelivered for weeks, and the suite stayed green the whole time
// because no test ever compared the declared ingress against the subscription
// it was supposed to serve. These tests are that comparison.
package main

import (
	"strings"
	"testing"

	"github.com/apollo-chora/chora-observability/internal/adapter/inmem"
	"github.com/apollo-chora/chora-observability/internal/adapter/subscribers"
	fg "github.com/apollo-chora/chora-observability/internal/domain/familiargrowth"
)

func growthSubscriberForBindings(t *testing.T) *subscribers.FamiliarGrowthAuditSubscriber {
	t.Helper()
	return subscribers.New(subscribers.Config{
		Repo:     inmem.NewFamiliarGrowthRepository(),
		Evidence: subscribers.NewInMemoryEvidencePublisher(),
	})
}

// --- The guard, applied to the canonical declaration -------------------------

// TestFamiliarGrowthAuditBindings_CanonicalDeclarationIsValid is the guard
// running against the real, shipped binding table. If anyone edits
// familiarGrowthAuditBindings into an inconsistent state, this fails.
func TestFamiliarGrowthAuditBindings_CanonicalDeclarationIsValid(t *testing.T) {
	sub := growthSubscriberForBindings(t)
	if err := validateFamiliarGrowthAuditBindings(sub.SubscribedTopics(), familiarGrowthAuditBindings); err != nil {
		t.Fatalf("the canonical ADR-149 binding table is invalid: %v", err)
	}
}

// TestFamiliarGrowthAuditBindings_TotalOverSubscribedTopics derives the
// expectation from the SUBSCRIBER'S OWN CONTRACT rather than a hand-copied
// list. A hand-maintained parallel list drifts silently; SubscribedTopics() is
// the single source of truth.
func TestFamiliarGrowthAuditBindings_TotalOverSubscribedTopics(t *testing.T) {
	sub := growthSubscriberForBindings(t)
	topics := sub.SubscribedTopics()

	bound := make(map[string]int, len(familiarGrowthAuditBindings))
	for _, b := range familiarGrowthAuditBindings {
		bound[b.Topic]++
	}
	for _, topic := range topics {
		switch bound[topic] {
		case 1: // exactly one ingress — correct
		case 0:
			t.Errorf("topic %q is in SubscribedTopics() but has NO ingress binding — its events would sit undelivered (the CHO-2257 defect)", topic)
		default:
			t.Errorf("topic %q has %d ingress bindings — duplicate ingress double-projects every event", topic, bound[topic])
		}
	}
	if len(familiarGrowthAuditBindings) != len(topics) {
		t.Errorf("binding count %d != SubscribedTopics count %d — the tables have drifted apart",
			len(familiarGrowthAuditBindings), len(topics))
	}
}

// TestFamiliarGrowthAuditBindings_AllPullShaped pins the shape against
// deployed reality: every one of the 7 subscriptions has an empty
// pushConfig.pushEndpoint (verified 2026-07-17 via gcloud).
func TestFamiliarGrowthAuditBindings_AllPullShaped(t *testing.T) {
	for _, b := range familiarGrowthAuditBindings {
		if b.Shape != ingressPull {
			t.Errorf("binding for %q declares shape %q; the deployed subscription is PULL-shaped", b.Topic, b.Shape)
		}
	}
}

// TestFamiliarGrowthAuditBindings_SubscriptionNamesMatchDeployedReality pins
// the canonical subscription short-names against the names that actually exist
// in the NATS deployment (verified 2026-07-17).
// A typo here is a silently-inert consumer.
func TestFamiliarGrowthAuditBindings_SubscriptionNamesMatchDeployedReality(t *testing.T) {
	deployed := map[string]string{
		fg.TopicExpAwarded:       "chora-observability.consumption-familiar-exp_awarded",
		fg.TopicStageUp:          "chora-observability.consumption-familiar-stage_up",
		fg.TopicBreedRevealed:    "chora-observability.consumption-familiar-breed_revealed",
		fg.TopicHatched:          "chora-observability.consumption-familiar-hatched",
		fg.TopicSourceRevelation: "chora-observability.consumption-familiar-source_revelation",
		fg.TopicEggPurchased:     "chora-observability.consumption-familiar-egg_purchased",
		fg.TopicPaymentSucceeded: "chora-observability.tenancy-familiar_egg-payment_succeeded",
	}
	for _, b := range familiarGrowthAuditBindings {
		want, ok := deployed[b.Topic]
		if !ok {
			t.Errorf("binding declares topic %q which has no deployed subscription", b.Topic)
			continue
		}
		if b.Subscription != want {
			t.Errorf("topic %q binds subscription %q; the deployed subscription is %q", b.Topic, b.Subscription, want)
		}
	}
}

// --- The guard rejects each way an ingress can mismatch ----------------------

func TestValidateFamiliarGrowthAuditBindings_RejectsMissingIngress(t *testing.T) {
	// A topic the subscriber claims but nothing consumes — CHO-2257 exactly.
	topics := []string{fg.TopicExpAwarded, fg.TopicStageUp}
	bindings := []familiarGrowthAuditBinding{
		{Topic: fg.TopicExpAwarded, Subscription: "sub-a", Shape: ingressPull},
	}
	err := validateFamiliarGrowthAuditBindings(topics, bindings)
	if err == nil {
		t.Fatal("a subscribed topic with no ingress must fail loudly; got nil")
	}
	if !strings.Contains(err.Error(), fg.TopicStageUp) {
		t.Errorf("the error must name the unserved topic; got: %v", err)
	}
}

// TestValidateFamiliarGrowthAuditBindings_RejectsPushShape is the direct
// regression for CHO-2257: a push-shaped ingress against a pull-shaped
// subscription must not boot.
func TestValidateFamiliarGrowthAuditBindings_RejectsPushShape(t *testing.T) {
	topics := []string{fg.TopicExpAwarded}
	bindings := []familiarGrowthAuditBinding{
		{Topic: fg.TopicExpAwarded, Subscription: "sub-a", Shape: ingressPush},
	}
	err := validateFamiliarGrowthAuditBindings(topics, bindings)
	if err == nil {
		t.Fatal("a PUSH ingress against a PULL subscription must fail loudly; got nil")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "push") {
		t.Errorf("the error must name the offending shape; got: %v", err)
	}
}

func TestValidateFamiliarGrowthAuditBindings_RejectsDuplicateTopic(t *testing.T) {
	topics := []string{fg.TopicExpAwarded}
	bindings := []familiarGrowthAuditBinding{
		{Topic: fg.TopicExpAwarded, Subscription: "sub-a", Shape: ingressPull},
		{Topic: fg.TopicExpAwarded, Subscription: "sub-b", Shape: ingressPull},
	}
	if err := validateFamiliarGrowthAuditBindings(topics, bindings); err == nil {
		t.Fatal("two ingresses for one topic must fail loudly (double projection); got nil")
	}
}

func TestValidateFamiliarGrowthAuditBindings_RejectsDuplicateSubscription(t *testing.T) {
	topics := []string{fg.TopicExpAwarded, fg.TopicStageUp}
	bindings := []familiarGrowthAuditBinding{
		{Topic: fg.TopicExpAwarded, Subscription: "same-sub", Shape: ingressPull},
		{Topic: fg.TopicStageUp, Subscription: "same-sub", Shape: ingressPull},
	}
	if err := validateFamiliarGrowthAuditBindings(topics, bindings); err == nil {
		t.Fatal("one subscription bound to two topics must fail loudly; got nil")
	}
}

func TestValidateFamiliarGrowthAuditBindings_RejectsUnknownTopic(t *testing.T) {
	topics := []string{fg.TopicExpAwarded}
	bindings := []familiarGrowthAuditBinding{
		{Topic: fg.TopicExpAwarded, Subscription: "sub-a", Shape: ingressPull},
		{Topic: "chora.consumption.familiar.ghost.v1", Subscription: "sub-ghost", Shape: ingressPull},
	}
	err := validateFamiliarGrowthAuditBindings(topics, bindings)
	if err == nil {
		t.Fatal("an ingress for a topic the subscriber does not handle must fail loudly; got nil")
	}
	if !strings.Contains(err.Error(), "ghost") {
		t.Errorf("the error must name the unknown topic; got: %v", err)
	}
}

func TestValidateFamiliarGrowthAuditBindings_RejectsEmptySubscription(t *testing.T) {
	topics := []string{fg.TopicExpAwarded}
	bindings := []familiarGrowthAuditBinding{
		{Topic: fg.TopicExpAwarded, Subscription: "   ", Shape: ingressPull},
	}
	if err := validateFamiliarGrowthAuditBindings(topics, bindings); err == nil {
		t.Fatal("a blank subscription name must fail loudly (silently-inert consumer); got nil")
	}
}

func TestValidateFamiliarGrowthAuditBindings_RejectsEmptyBindings(t *testing.T) {
	topics := []string{fg.TopicExpAwarded}
	if err := validateFamiliarGrowthAuditBindings(topics, nil); err == nil {
		t.Fatal("no bindings at all must fail loudly; got nil")
	}
}

// --- The JetStream starter ----------------------------------------------------

func TestStartFamiliarGrowthAuditSubscribers_NilBusIsNotWired(t *testing.T) {
	// Dev path: no event bus ⇒ no goroutines, and the caller logs it.
	sub := growthSubscriberForBindings(t)
	done, err := startFamiliarGrowthAuditSubscribers(t.Context(), nil, sub, familiarGrowthAuditBindings, nil)
	if err != nil {
		t.Fatalf("an unwired bus is the dev path, not an error: %v", err)
	}
	if len(done) != 0 {
		t.Fatalf("expected no subscriber goroutines without a bus, got %d", len(done))
	}
}

func TestStartFamiliarGrowthAuditSubscribers_InvalidBindingsRefuseToStart(t *testing.T) {
	sub := growthSubscriberForBindings(t)
	bad := []familiarGrowthAuditBinding{
		{Topic: fg.TopicExpAwarded, Subscription: "sub-a", Shape: ingressPush},
	}
	_, err := startFamiliarGrowthAuditSubscribers(t.Context(), stubSubscriber{}, sub, bad, nil)
	if err == nil {
		t.Fatal("starting with an invalid binding table must return an error, never start a partial lane")
	}
}
