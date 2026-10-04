// Package reconcilepublish exercises the unexported buildEnvelope helper
// (occurred-at fallback) and the degraded-publish error propagation.
package reconcilepublish

import (
	"context"
	"errors"
	"testing"
	"time"

	cgcenvelope "github.com/5007-Capstone/chora/libs/chora-go-common/envelope"

	"github.com/5007-Capstone/chora/services/chora-observability/internal/domain/reconcile"
)

func TestBuildEnvelope_OccurredAtFallback(t *testing.T) {
	t.Parallel()
	sink := NewPubSubEventSink(Config{
		Publisher:     &alwaysFailPublisher{},
		SourceProject: "chora-489812",
		SourceService: "chora-observability",
		Now:           func() time.Time { return time.Date(2026, 5, 9, 4, 0, 0, 0, time.UTC) },
	})

	// zero occurredAt -> now
	env := sink.buildEnvelope(context.Background(), "key", "accountability", "runtime", time.Time{})
	if env.OccurredAt.IsZero() {
		t.Error("expected occurred_at to fall back to Now()")
	}
	if env.IdempotencyKey != "key" {
		t.Errorf("idempotency key = %q; want key", env.IdempotencyKey)
	}
	if env.SourceProject != "chora-489812" {
		t.Errorf("source project = %q", env.SourceProject)
	}
	if env.TenantID != "platform" {
		t.Errorf("tenant = %q; want platform", env.TenantID)
	}
	if env.ChoraImdaDimension != "accountability" || env.ImdaLifecycleStage != "runtime" {
		t.Errorf("imda fields: %+v", env)
	}

	// explicit occurredAt preserved
	explicit := time.Date(2026, 5, 8, 4, 0, 0, 0, time.UTC)
	env2 := sink.buildEnvelope(context.Background(), "k2", "", "", explicit)
	if !env2.OccurredAt.Equal(explicit) {
		t.Errorf("occurred_at = %v; want %v", env2.OccurredAt, explicit)
	}
	_ = cgcenvelope.Envelope{}
}

type alwaysFailPublisher struct{}

func (a *alwaysFailPublisher) Publish(_ context.Context, _ string, _ cgcenvelope.Envelope, _ []byte) error {
	return errors.New("pubsub down")
}

func TestPubSubEventSink_EmitDegraded_PublishError(t *testing.T) {
	t.Parallel()
	sink := NewPubSubEventSink(Config{Publisher: &alwaysFailPublisher{}})
	err := sink.EmitDegraded(context.Background(), reconcile.DegradedEvent{
		UpstreamComponent: "bigquery",
		WindowStart:       time.Date(2026, 5, 9, 0, 0, 0, 0, time.UTC),
		WindowEnd:         time.Date(2026, 5, 10, 0, 0, 0, 0, time.UTC),
	})
	if err == nil {
		t.Fatal("expected publish error")
	}
}

func TestPubSubEventSink_Emit_PublishError(t *testing.T) {
	t.Parallel()
	sink := NewPubSubEventSink(Config{Publisher: &alwaysFailPublisher{}})
	if err := sink.Emit(context.Background(), reconcile.AnomalyEvent{
		WindowStart: time.Date(2026, 5, 9, 0, 0, 0, 0, time.UTC),
		WindowEnd:   time.Date(2026, 5, 10, 0, 0, 0, 0, time.UTC),
	}); err == nil {
		t.Fatal("expected publish error")
	}
}