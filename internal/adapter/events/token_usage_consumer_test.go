// Package events_test exercises the chora.observability.token_usage.recorded.v1
// subscriber. RED→GREEN per [[feedback-strict-tdd]] for Gate #7
// (docs/m13/ack-oe-ai-assist-plan-2026-05-17.md §Step 6).
//
// Subscriber contract:
//
//   - Decodes inbound `chora.observability.token_usage.recorded.v1` events
//     (producer: chora-ai-kernel-orchestrator outbox → Pub/Sub).
//   - Persists one append-only `token_usage_ledger` row per event via
//     `ledger.Repository.Append`.
//   - Idempotent on (event_id) per [[data-consistency]] — double-delivery
//     is a no-op.
//   - Trace IDs are derived from the W3C traceparent envelope attribute
//     when present; absent / malformed → trace_id/span_id stamped as the
//     all-zero ledger-reserved sentinel which the domain rejects, so a
//     synthesised stub MUST be produced (we use the event_id derived
//     32-hex / 16-hex).
package events_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/5007-Capstone/chora/libs/chora-go-common/idempotent"
	"github.com/5007-Capstone/chora/services/chora-observability/internal/adapter/events"
	"github.com/5007-Capstone/chora/services/chora-observability/internal/adapter/inmem"
	"github.com/5007-Capstone/chora/services/chora-observability/internal/domain/ledger"
)

func newFixture(t *testing.T) (*events.TokenUsageConsumer, *inmem.LedgerRepository) {
	t.Helper()
	repo := inmem.NewLedgerRepository()
	cons := events.NewTokenUsageConsumer(events.TokenUsageConsumerConfig{
		Repo:  repo,
		Inbox: idempotent.NewMemoryStore(),
	})
	return cons, repo
}

func goodEvent() events.TokenUsageRecordedEvent {
	return events.TokenUsageRecordedEvent{
		EventID:      "evt-01HABCDEFGHJKMNPQRSTVWXYZ", // not strictly UUIDv7 — just a unique key
		TenantID:     "tenant-a",
		GCID:         "gcid-1",
		ModelID:      "vertex_ai/gemini-2.5-flash",
		AgentRole:    "qgen_question",
		InputTokens:  120,
		OutputTokens: 75,
		CachedTokens: 0,
		CostMicros:   450,
		InvocationID: "assist-job-abc",
		Traceparent:  "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
		Tracestate:   "",
		OccurredAt:   time.Date(2026, 5, 17, 10, 0, 1, 0, time.UTC),
	}
}

func TestTokenUsageConsumer_PersistsLedgerRow(t *testing.T) {
	t.Parallel()
	cons, repo := newFixture(t)

	if err := cons.Handle(context.Background(), goodEvent()); err != nil {
		t.Fatalf("Handle: %v", err)
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
	if e.ModelID != "vertex_ai/gemini-2.5-flash" {
		t.Errorf("model_id = %q; want vertex_ai/gemini-2.5-flash", e.ModelID)
	}
	if e.PromptTokens != 120 {
		t.Errorf("prompt_tokens = %d; want 120", e.PromptTokens)
	}
	if e.CompletionTokens != 75 {
		t.Errorf("completion_tokens = %d; want 75", e.CompletionTokens)
	}
	if e.CostUsdMicros != 450 {
		t.Errorf("cost_usd_micros = %d; want 450", e.CostUsdMicros)
	}
	// AGID = agent_role for agent-attributed events (per ddd-enforcement
	// AGID is distinct from GCID — and the proto field 'agent_role'
	// carries the crew role, which is the canonical AGID for cost
	// attribution).
	if e.Agid != "qgen_question" {
		t.Errorf("agid = %q; want qgen_question", e.Agid)
	}
	// Trace IDs derived from the W3C traceparent.
	if e.TraceID != "0af7651916cd43dd8448eb211c80319c" {
		t.Errorf("trace_id = %q; want 0af7651916cd43dd8448eb211c80319c", e.TraceID)
	}
	if e.SpanID != "b7ad6b7169203331" {
		t.Errorf("span_id = %q; want b7ad6b7169203331", e.SpanID)
	}
	if e.RecordedAt.IsZero() {
		t.Errorf("recorded_at zero")
	}
}

func TestTokenUsageConsumer_IdempotentOnEventID(t *testing.T) {
	t.Parallel()
	cons, repo := newFixture(t)
	ev := goodEvent()

	if err := cons.Handle(context.Background(), ev); err != nil {
		t.Fatalf("first Handle: %v", err)
	}
	if err := cons.Handle(context.Background(), ev); err != nil {
		t.Fatalf("second Handle: %v", err)
	}

	entries, _ := repo.List(context.Background(), "tenant-a", ledger.ListFilter{})
	if len(entries) != 1 {
		t.Fatalf("entries = %d; want 1 (idempotent)", len(entries))
	}
}

func TestTokenUsageConsumer_RejectsMissingTenant(t *testing.T) {
	t.Parallel()
	cons, _ := newFixture(t)
	ev := goodEvent()
	ev.TenantID = ""

	err := cons.Handle(context.Background(), ev)
	if err == nil {
		t.Fatal("expected error; got nil")
	}
	if !strings.Contains(err.Error(), "tenant_id") {
		t.Errorf("error = %q; want tenant_id mention", err.Error())
	}
}

func TestTokenUsageConsumer_RejectsMissingEventID(t *testing.T) {
	t.Parallel()
	cons, _ := newFixture(t)
	ev := goodEvent()
	ev.EventID = ""

	err := cons.Handle(context.Background(), ev)
	if err == nil {
		t.Fatal("expected error; got nil")
	}
}

func TestTokenUsageConsumer_RejectsMissingGCID(t *testing.T) {
	t.Parallel()
	cons, _ := newFixture(t)
	ev := goodEvent()
	ev.GCID = ""

	err := cons.Handle(context.Background(), ev)
	if err == nil {
		t.Fatal("expected error; got nil")
	}
	if !strings.Contains(err.Error(), "gcid") {
		t.Errorf("error = %q; want gcid mention", err.Error())
	}
}

func TestTokenUsageConsumer_RejectsMissingModelID(t *testing.T) {
	t.Parallel()
	cons, _ := newFixture(t)
	ev := goodEvent()
	ev.ModelID = ""

	err := cons.Handle(context.Background(), ev)
	if err == nil {
		t.Fatal("expected error; got nil")
	}
}

func TestTokenUsageConsumer_MalformedTraceparent_SynthesisesStub(t *testing.T) {
	t.Parallel()
	cons, repo := newFixture(t)
	ev := goodEvent()
	ev.Traceparent = "not-a-valid-traceparent"

	if err := cons.Handle(context.Background(), ev); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	entries, _ := repo.List(context.Background(), "tenant-a", ledger.ListFilter{})
	if len(entries) != 1 {
		t.Fatalf("entries = %d; want 1", len(entries))
	}
	// Synthesised stub: 32 hex chars for trace, 16 for span. Non-zero
	// (W3C reserves all-zero as invalid; ledger.ValidateTraceID rejects).
	got := entries[0]
	if err := ledger.ValidateTraceID(got.TraceID); err != nil {
		t.Errorf("trace_id stub invalid: %v (got %q)", err, got.TraceID)
	}
	if err := ledger.ValidateSpanID(got.SpanID); err != nil {
		t.Errorf("span_id stub invalid: %v (got %q)", err, got.SpanID)
	}
}

func TestTokenUsageConsumer_PropagatesRepoError(t *testing.T) {
	t.Parallel()
	cons := events.NewTokenUsageConsumer(events.TokenUsageConsumerConfig{
		Repo:  &failingRepo{},
		Inbox: idempotent.NewMemoryStore(),
	})
	err := cons.Handle(context.Background(), goodEvent())
	if err == nil {
		t.Fatal("expected error; got nil")
	}
}

type failingRepo struct{}

func (failingRepo) Append(_ context.Context, _ *ledger.Entry) error {
	return errors.New("simulated DB failure")
}
func (failingRepo) List(_ context.Context, _ string, _ ledger.ListFilter) ([]*ledger.Entry, error) {
	return nil, nil
}
func (failingRepo) SumCost(_ context.Context, _ string, _ ledger.ListFilter) (int64, int, error) {
	return 0, 0, nil
}

func TestTokenUsageConsumer_SubscribedTopic(t *testing.T) {
	t.Parallel()
	if got := events.TopicTokenUsageRecorded; got != "chora.observability.token_usage.recorded.v1" {
		t.Errorf("topic = %q; want chora.observability.token_usage.recorded.v1", got)
	}
}

func TestTokenUsageConsumer_SubscribedTopic_Method(t *testing.T) {
	t.Parallel()
	cons, _ := newFixture(t)
	if got := cons.SubscribedTopic(); got != events.TopicTokenUsageRecorded {
		t.Errorf("SubscribedTopic = %q; want %q", got, events.TopicTokenUsageRecorded)
	}
}

func TestTokenUsageConsumer_PanicsOnMissingRepo(t *testing.T) {
	t.Parallel()
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on missing Repo")
		}
	}()
	_ = events.NewTokenUsageConsumer(events.TokenUsageConsumerConfig{})
}

func TestTokenUsageConsumer_ZeroOccurredAtUsesNowFunc(t *testing.T) {
	t.Parallel()
	repo := inmem.NewLedgerRepository()
	fixedTime := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	cons := events.NewTokenUsageConsumer(events.TokenUsageConsumerConfig{
		Repo:  repo,
		Inbox: idempotent.NewMemoryStore(),
		Now:   func() time.Time { return fixedTime },
	})
	ev := goodEvent()
	ev.OccurredAt = time.Time{} // zero — exercises the c.now() fallback

	if err := cons.Handle(context.Background(), ev); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	entries, _ := repo.List(context.Background(), "tenant-a", ledger.ListFilter{})
	if len(entries) != 1 {
		t.Fatalf("entries = %d; want 1", len(entries))
	}
	if !entries[0].RecordedAt.Equal(fixedTime) {
		t.Errorf("recorded_at = %v; want %v (from Now func)", entries[0].RecordedAt, fixedTime)
	}
}

func TestTokenUsageConsumer_RejectsNegativeInputTokens(t *testing.T) {
	t.Parallel()
	cons, _ := newFixture(t)
	ev := goodEvent()
	ev.InputTokens = -1

	err := cons.Handle(context.Background(), ev)
	if err == nil {
		t.Fatal("expected error; got nil")
	}
	if !strings.Contains(err.Error(), "input_tokens negative") {
		t.Errorf("error = %q; want input_tokens negative mention", err.Error())
	}
}

func TestTokenUsageConsumer_RejectsNegativeOutputTokens(t *testing.T) {
	t.Parallel()
	cons, _ := newFixture(t)
	ev := goodEvent()
	ev.OutputTokens = -5

	err := cons.Handle(context.Background(), ev)
	if err == nil {
		t.Fatal("expected error; got nil")
	}
}

func TestTokenUsageConsumer_RejectsNegativeCostMicros(t *testing.T) {
	t.Parallel()
	cons, _ := newFixture(t)
	ev := goodEvent()
	ev.CostMicros = -100

	err := cons.Handle(context.Background(), ev)
	if err == nil {
		t.Fatal("expected error; got nil")
	}
}

func TestTokenUsageConsumer_PropagatesGCIDFromAGID(t *testing.T) {
	// When the producer-side event carries an AGID-style identifier in the
	// gcid slot (per token_usage.proto §61-71 envelope semantics), the
	// ledger row preserves it verbatim — chora_identity lookup
	// disambiguates downstream.
	t.Parallel()
	cons, repo := newFixture(t)
	ev := goodEvent()
	ev.GCID = "agid-system-1"

	if err := cons.Handle(context.Background(), ev); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	entries, _ := repo.List(context.Background(), "tenant-a", ledger.ListFilter{})
	if len(entries) != 1 {
		t.Fatalf("entries = %d; want 1", len(entries))
	}
	if entries[0].Gcid != "agid-system-1" {
		t.Errorf("gcid = %q; want agid-system-1 (preserved verbatim)", entries[0].Gcid)
	}
}
