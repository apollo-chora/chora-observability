// token_usage_binding_test.go — RED→GREEN unit tests for the
// chora.observability.token_usage.recorded.v1 JetStream binding
// (Gate #7 WIRE1 2026-05-17 + ADR-167 Phase 2 JSON→Protobuf migration).
//
// Wire shape (ADR-167): the event bus message payload is a BINARY-protobuf
// `chora.observability.v1.TokenUsageRecorded` whose EventEnvelope at field
// 1 is AUTHORITATIVE. The eventbus.Message.Envelope (routing envelope
// reconstructed from the publisher's NATS headers) is read only as a fallback
// when the proto envelope is absent / blank.
//
// Verifies:
//
//   - buildTokenUsageHandler proto.Unmarshal the payload into
//     TokenUsageRecorded + maps it into TokenUsageRecordedEvent + calls
//     TokenUsageConsumer.Handle.
//   - Proto envelope (field 1) is canonical; the routing envelope is the fallback.
//   - Multi-tenant isolation — different envelope.TenantID values produce
//     distinct ledger rows (D6 P3).
//   - Trace context propagation — Traceparent + Tracestate land on the
//     ledger row (D6 P4).
//   - Idempotency on event_id (D6 P2) — replay is a no-op at the consumer.
//   - FAIL LOUD — malformed proto bytes → error → broker NAK → DLQ. NO
//     JSON fallback (ADR-167 HARD REQUIREMENT #1).
//   - startTokenUsageSubscriber registers the goroutine (returns a non-nil
//     done channel when wired) + skips wiring when bus/handler is nil.
package main

import (
	"context"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonenvelope "github.com/apollo-chora/chora-common/envelope"
	"github.com/apollo-chora/chora-common/eventbus"
	"github.com/apollo-chora/chora-common/idempotent"
	commonv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/common/v1"
	observabilityv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/observability/v1"
	"github.com/apollo-chora/chora-observability/internal/adapter/events"
	"github.com/apollo-chora/chora-observability/internal/adapter/inmem"
	"github.com/apollo-chora/chora-observability/internal/domain/ledger"
)

func newTokenUsageFixture(t *testing.T) (*events.TokenUsageConsumer, *inmem.LedgerRepository) {
	t.Helper()
	repo := inmem.NewLedgerRepository()
	cons := events.NewTokenUsageConsumer(events.TokenUsageConsumerConfig{
		Repo:  repo,
		Inbox: idempotent.NewMemoryStore(),
	})
	return cons, repo
}

// goodTokenUsageProto builds a canonical TokenUsageRecorded proto with the
// authoritative EventEnvelope at field 1. engine_resource maps to the proto
// `model_id`; occurred_at maps to `recorded_at` per the ADR-167 mapping.
func goodTokenUsageProto() *observabilityv1.TokenUsageRecorded {
	at := time.Date(2026, 5, 17, 10, 0, 1, 0, time.UTC)
	return &observabilityv1.TokenUsageRecorded{
		Envelope: &commonv1.EventEnvelope{
			EventId:        "evt-01HABC",
			IdempotencyKey: "token_usage.tenant-a.assist-job-abc.qgen_question.attempt_1",
			TenantId:       "tenant-a",
			Gcid:           "gcid-1",
			OccurredAt:     timestamppb.New(at),
			PublishedAt:    timestamppb.New(at.Add(time.Second)),
			Traceparent:    "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
			Tracestate:     "vendor=chora",
			SourceProject:  "chora-489812",
			SourceService:  "chora-ai-kernel-orchestrator",
			SchemaVersion:  1,
		},
		UsageId:      "usage-1",
		TenantId:     "tenant-a",
		Gcid:         "gcid-1",
		ModelId:      "projects/chora-489812/locations/us-central1/reasoningEngines/123",
		AgentRole:    "qgen_question",
		InputTokens:  120,
		OutputTokens: 75,
		CachedTokens: 0,
		CostMicros:   0,
		InvocationId: "assist-job-abc",
		RecordedAt:   timestamppb.New(at),
	}
}

// protoMessage wraps the proto into an eventbus.Message. The eventbus
// envelope mirrors the proto envelope (the publisher stamps both); tests
// that exercise precedence/fallback override one side.
func protoMessage(t *testing.T, m *observabilityv1.TokenUsageRecorded) eventbus.Message {
	t.Helper()
	data, err := proto.Marshal(m)
	if err != nil {
		t.Fatalf("marshal proto: %v", err)
	}
	env := m.GetEnvelope()
	return eventbus.Message{
		Subject: events.TopicTokenUsageRecorded,
		Envelope: commonenvelope.Envelope{
			EventID:        env.GetEventId(),
			IdempotencyKey: env.GetIdempotencyKey(),
			TenantID:       env.GetTenantId(),
			GCID:           env.GetGcid(),
			OccurredAt:     env.GetOccurredAt().AsTime(),
			Traceparent:    env.GetTraceparent(),
			Tracestate:     env.GetTracestate(),
			SchemaVersion:  env.GetSchemaVersion(),
		},
		Payload:         data,
		DeliveryAttempt: 1,
	}
}

func TestBuildTokenUsageHandler_DecodesProtoEnvelopeAndPayload(t *testing.T) {
	cons, repo := newTokenUsageFixture(t)
	h := buildTokenUsageHandler(cons)
	msg := protoMessage(t, goodTokenUsageProto())

	if err := h(context.Background(), msg); err != nil {
		t.Fatalf("handler returned error: %v", err)
	}

	entries, err := repo.List(context.Background(), "tenant-a", ledger.ListFilter{})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("entries = %d; want 1", len(entries))
	}
	e := entries[0]
	if e.TenantID != "tenant-a" {
		t.Errorf("tenant_id = %q; want tenant-a", e.TenantID)
	}
	if e.Gcid != "gcid-1" {
		t.Errorf("gcid = %q; want gcid-1", e.Gcid)
	}
	if e.ModelID != "projects/chora-489812/locations/us-central1/reasoningEngines/123" {
		t.Errorf("model_id = %q; want engine resource", e.ModelID)
	}
	if e.PromptTokens != 120 {
		t.Errorf("prompt_tokens = %d; want 120", e.PromptTokens)
	}
	if e.CompletionTokens != 75 {
		t.Errorf("completion_tokens = %d; want 75", e.CompletionTokens)
	}
	// AGID = agent_role per ddd-enforcement (proto §61-71).
	if e.Agid != "qgen_question" {
		t.Errorf("agid = %q; want qgen_question", e.Agid)
	}
	// D6 P4 — traceparent → trace_id + span_id parsed.
	if e.TraceID != "0af7651916cd43dd8448eb211c80319c" {
		t.Errorf("trace_id = %q; want 0af7651916cd43dd8448eb211c80319c", e.TraceID)
	}
	if e.SpanID != "b7ad6b7169203331" {
		t.Errorf("span_id = %q; want b7ad6b7169203331", e.SpanID)
	}
}

func TestBuildTokenUsageHandler_ProtoEnvelopeIsCanonical(t *testing.T) {
	// Proto envelope says tenant-x; the routing attrs (stale replay copy)
	// say tenant-y. The proto-envelope-as-truth contract means tenant-x
	// lands in the ledger.
	cons, repo := newTokenUsageFixture(t)
	h := buildTokenUsageHandler(cons)

	m := goodTokenUsageProto()
	m.Envelope.TenantId = "tenant-x"
	m.Envelope.Gcid = "gcid-x"
	m.TenantId = "tenant-x"
	m.Gcid = "gcid-x"
	msg := protoMessage(t, m)
	// Routing attrs disagree (stale).
	msg.Envelope.TenantID = "tenant-y"
	msg.Envelope.GCID = "gcid-y"

	if err := h(context.Background(), msg); err != nil {
		t.Fatalf("handler error: %v", err)
	}
	entries, _ := repo.List(context.Background(), "tenant-x", ledger.ListFilter{})
	if len(entries) != 1 {
		t.Fatalf("tenant-x entries = %d; want 1", len(entries))
	}
	if entries[0].Gcid != "gcid-x" {
		t.Errorf("gcid = %q; want gcid-x (proto-envelope-as-truth)", entries[0].Gcid)
	}
	staleEntries, _ := repo.List(context.Background(), "tenant-y", ledger.ListFilter{})
	if len(staleEntries) != 0 {
		t.Errorf("tenant-y entries = %d; want 0 (cross-tenant isolation)", len(staleEntries))
	}
}

func TestBuildTokenUsageHandler_FallsBackToAttrsWhenProtoEnvelopeAbsent(t *testing.T) {
	// Proto with NO envelope — handler falls back to the routing attrs
	// (eventbus.Message.Envelope) for the envelope-mandatory fields.
	cons, repo := newTokenUsageFixture(t)
	h := buildTokenUsageHandler(cons)

	m := &observabilityv1.TokenUsageRecorded{
		ModelId:      "engine-x",
		AgentRole:    "qgen_critic",
		InputTokens:  10,
		OutputTokens: 5,
		InvocationId: "assist-fallback",
	}
	data, _ := proto.Marshal(m)
	msg := eventbus.Message{
		Subject: events.TopicTokenUsageRecorded,
		Envelope: commonenvelope.Envelope{
			EventID:    "evt-fallback",
			TenantID:   "tenant-zeta",
			GCID:       "gcid-zeta",
			OccurredAt: time.Date(2026, 5, 17, 10, 0, 30, 0, time.UTC),
		},
		Payload: data,
	}
	if err := h(context.Background(), msg); err != nil {
		t.Fatalf("handler error: %v", err)
	}
	entries, _ := repo.List(context.Background(), "tenant-zeta", ledger.ListFilter{})
	if len(entries) != 1 {
		t.Fatalf("entries = %d; want 1 (attrs-fallback path)", len(entries))
	}
	if entries[0].ModelID != "engine-x" {
		t.Errorf("model_id = %q; want engine-x", entries[0].ModelID)
	}
}

func TestBuildTokenUsageHandler_MultiTenantIsolationD6P3(t *testing.T) {
	cons, repo := newTokenUsageFixture(t)
	h := buildTokenUsageHandler(cons)

	mA := goodTokenUsageProto()
	mA.Envelope.EventId = "evt-aaa"
	mA.Envelope.TenantId = "tenant-a"
	mA.TenantId = "tenant-a"
	msgA := protoMessage(t, mA)

	mB := goodTokenUsageProto()
	mB.Envelope.EventId = "evt-bbb"
	mB.Envelope.TenantId = "tenant-b"
	mB.Envelope.Gcid = "gcid-b"
	mB.TenantId = "tenant-b"
	mB.Gcid = "gcid-b"
	msgB := protoMessage(t, mB)

	if err := h(context.Background(), msgA); err != nil {
		t.Fatalf("tenant-a handler: %v", err)
	}
	if err := h(context.Background(), msgB); err != nil {
		t.Fatalf("tenant-b handler: %v", err)
	}

	entA, _ := repo.List(context.Background(), "tenant-a", ledger.ListFilter{})
	entB, _ := repo.List(context.Background(), "tenant-b", ledger.ListFilter{})
	if len(entA) != 1 || len(entB) != 1 {
		t.Fatalf("len(entA)=%d len(entB)=%d; want 1 each", len(entA), len(entB))
	}
}

func TestBuildTokenUsageHandler_IdempotentOnEventID(t *testing.T) {
	cons, repo := newTokenUsageFixture(t)
	h := buildTokenUsageHandler(cons)
	msg := protoMessage(t, goodTokenUsageProto())

	if err := h(context.Background(), msg); err != nil {
		t.Fatalf("first call: %v", err)
	}
	if err := h(context.Background(), msg); err != nil {
		t.Fatalf("second call: %v", err)
	}
	entries, _ := repo.List(context.Background(), "tenant-a", ledger.ListFilter{})
	if len(entries) != 1 {
		t.Fatalf("entries = %d after double-deliver; want 1 (D6 P2 dedupe)", len(entries))
	}
}

// TestBuildTokenUsageHandler_MalformedProtoError asserts the FAIL-LOUD DLQ
// path (ADR-167 HARD REQUIREMENT #1): non-protobuf bytes → error → NAK →
// DLQ. NO JSON fallback.
func TestBuildTokenUsageHandler_MalformedProtoError(t *testing.T) {
	cons, _ := newTokenUsageFixture(t)
	h := buildTokenUsageHandler(cons)
	msg := eventbus.Message{
		Subject:  events.TopicTokenUsageRecorded,
		Envelope: commonenvelope.Envelope{EventID: "x", TenantID: "t", OccurredAt: time.Now()},
		Payload:  []byte{0xFF, 0xFF, 0xFF, 0xFF, 0x0F},
	}
	if err := h(context.Background(), msg); err == nil {
		t.Fatalf("malformed proto must error so broker DLQs after max retries")
	}
}

func TestBuildTokenUsageHandler_PanicsOnNilConsumer(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic on nil consumer; got nil")
		}
	}()
	_ = buildTokenUsageHandler(nil)
}

// -----------------------------------------------------------------------------
// startTokenUsageSubscriber
// -----------------------------------------------------------------------------

// stubSubscriber is a no-op eventbus.Subscriber that blocks on Subscribe
// until ctx is canceled. Lets the test verify the goroutine is registered
// without spinning up a real broker.
type stubSubscriber struct{}

func (stubSubscriber) Subscribe(ctx context.Context, _ eventbus.ConsumerConfig, _ eventbus.Handler) error {
	<-ctx.Done()
	return ctx.Err()
}

func TestStartTokenUsageSubscriber_ReturnsNilWhenBusUnwired(t *testing.T) {
	cons, _ := newTokenUsageFixture(t)
	done := startTokenUsageSubscriber(context.Background(), nil, "", buildTokenUsageHandler(cons))
	if done != nil {
		t.Fatalf("done = %v; want nil when bus unwired", done)
	}
}

func TestStartTokenUsageSubscriber_ReturnsNilWhenHandlerUnwired(t *testing.T) {
	done := startTokenUsageSubscriber(context.Background(), stubSubscriber{}, "", nil)
	if done != nil {
		t.Fatalf("done = %v; want nil when handler unwired", done)
	}
}

func TestStartTokenUsageSubscriber_GoroutineRegisteredWhenWired(t *testing.T) {
	cons, _ := newTokenUsageFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := startTokenUsageSubscriber(ctx, stubSubscriber{}, "", buildTokenUsageHandler(cons))
	if done == nil {
		t.Fatalf("done = nil; want non-nil channel when wired")
	}
	cancel()
	select {
	case <-done:
		// good — goroutine drained
	case <-time.After(2 * time.Second):
		t.Fatalf("goroutine did not exit within 2s after cancel")
	}
}

func TestStartTokenUsageSubscriber_DefaultSubscriptionName(t *testing.T) {
	if DefaultTokenUsageSubscription != "chora-observability.observability-token_usage-recorded" {
		t.Errorf(
			"DefaultTokenUsageSubscription = %q; want canonical terraform name",
			DefaultTokenUsageSubscription,
		)
	}
}

func TestStartTokenUsageSubscriber_CustomSubscriptionPassThrough(t *testing.T) {
	cons, _ := newTokenUsageFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := startTokenUsageSubscriber(ctx, stubSubscriber{}, "custom-sub", buildTokenUsageHandler(cons))
	if done == nil {
		t.Fatalf("done = nil; want non-nil channel for custom name")
	}
	cancel()
	<-done
}
