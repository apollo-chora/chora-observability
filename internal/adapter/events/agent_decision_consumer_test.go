// agent_decision_consumer_test.go — RED→GREEN per [[feedback-strict-tdd]]
// for the ADR-167 agent_decision read-model hydration.
//
// Subscriber contract:
//   - Decodes inbound chora.observability.agent_decision.logged.v1 events.
//   - Persists one append-only agent_decision_log row per event via
//     decision.Repository.Append.
//   - Idempotent on event_id — double-delivery is a no-op.
//   - Validation rejects (NACK→DLQ) malformed events fast.
package events_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apollo-chora/chora-common/idempotent"
	"github.com/apollo-chora/chora-observability/internal/adapter/events"
	"github.com/apollo-chora/chora-observability/internal/adapter/inmem"
	"github.com/apollo-chora/chora-observability/internal/domain/decision"
)

func newDecisionFixture(t *testing.T) (*events.AgentDecisionConsumer, *inmem.DecisionRepository) {
	t.Helper()
	repo := inmem.NewDecisionRepository()
	cons := events.NewAgentDecisionConsumer(events.AgentDecisionConsumerConfig{
		Repo:  repo,
		Inbox: idempotent.NewMemoryStore(),
	})
	return cons, repo
}

func goodDecisionEvent() events.AgentDecisionLoggedEvent {
	return events.AgentDecisionLoggedEvent{
		EventID:       "evt-dec-01HABCDEFGHJKMNPQRSTVWXYZ",
		TenantID:      "tenant-a",
		Agid:          "qgen_critic",
		DecisionType:  string(decision.TypeRespond),
		RiskTier:      string(decision.TierLow),
		CorrelationID: "assist-job-abc",
		Reason:        "critique passed",
		OutputSummary: "PASS — question is well-formed",
		Traceparent:   "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
		DecidedAt:     time.Date(2026, 5, 29, 9, 33, 55, 0, time.UTC),
	}
}

// fakeDecisionBQSink records the decisions mirrored into BigQuery and can be
// configured to fail, proving the best-effort contract.
type fakeDecisionBQSink struct {
	mu       sync.Mutex
	inserted []*decision.Log
	err      error
}

func (f *fakeDecisionBQSink) Insert(_ context.Context, l *decision.Log) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.inserted = append(f.inserted, l)
	return f.err
}

func (f *fakeDecisionBQSink) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.inserted)
}

func TestAgentDecisionConsumer_MirrorsToBigQuery(t *testing.T) {
	t.Parallel()
	repo := inmem.NewDecisionRepository()
	sink := &fakeDecisionBQSink{}
	cons := events.NewAgentDecisionConsumer(events.AgentDecisionConsumerConfig{
		Repo:   repo,
		Inbox:  idempotent.NewMemoryStore(),
		BQSink: sink,
	})

	if err := cons.Handle(context.Background(), goodDecisionEvent()); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if got := sink.count(); got != 1 {
		t.Fatalf("BQ mirror inserted = %d; want 1", got)
	}
	if sink.inserted[0].Agid != "qgen_critic" {
		t.Errorf("mirrored agid = %q; want qgen_critic", sink.inserted[0].Agid)
	}
}

func TestAgentDecisionConsumer_BQSinkFailureDoesNotFailAck(t *testing.T) {
	t.Parallel()
	repo := inmem.NewDecisionRepository()
	sink := &fakeDecisionBQSink{err: errors.New("bq down")}
	cons := events.NewAgentDecisionConsumer(events.AgentDecisionConsumerConfig{
		Repo:   repo,
		Inbox:  idempotent.NewMemoryStore(),
		BQSink: sink,
	})

	// Postgres is canonical: a BQ-mirror failure must NOT fail Handle (the ack),
	// else a decision that IS persisted would needlessly DLQ.
	if err := cons.Handle(context.Background(), goodDecisionEvent()); err != nil {
		t.Fatalf("Handle errored on BQ-sink failure; want nil (best-effort): %v", err)
	}
	logs, _ := repo.List(context.Background(), "tenant-a", decision.ListFilter{})
	if len(logs) != 1 {
		t.Fatalf("decision must still persist to pg; logs = %d want 1", len(logs))
	}
}

func TestAgentDecisionConsumer_PersistsDecisionRow(t *testing.T) {
	t.Parallel()
	cons, repo := newDecisionFixture(t)

	if err := cons.Handle(context.Background(), goodDecisionEvent()); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	logs, err := repo.List(context.Background(), "tenant-a", decision.ListFilter{})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(logs) != 1 {
		t.Fatalf("logs = %d; want 1", len(logs))
	}
	l := logs[0]
	if l.Agid != "qgen_critic" {
		t.Errorf("agid = %q; want qgen_critic", l.Agid)
	}
	if l.DecisionType != decision.TypeRespond {
		t.Errorf("decision_type = %q; want respond", l.DecisionType)
	}
	if !l.CreatedAt.Equal(time.Date(2026, 5, 29, 9, 33, 55, 0, time.UTC)) {
		t.Errorf("recorded_at = %v; want event DecidedAt (event-time preserved)", l.CreatedAt)
	}
}

// TestAgentDecisionConsumer_PersistsVerdict proves the qgen quality-gate
// verdict (attributes[decision] on the wire → ev.Verdict) round-trips onto the
// persisted row so the O+ DECISION TYPE column can show accepted/rejected
// instead of the base `respond` (CHO-1700 follow-up).
func TestAgentDecisionConsumer_PersistsVerdict(t *testing.T) {
	t.Parallel()
	cons, repo := newDecisionFixture(t)
	ev := goodDecisionEvent()
	ev.Verdict = "rejected"

	if err := cons.Handle(context.Background(), ev); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	logs, err := repo.List(context.Background(), "tenant-a", decision.ListFilter{})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(logs) != 1 {
		t.Fatalf("logs = %d; want 1", len(logs))
	}
	if logs[0].Verdict != "rejected" {
		t.Errorf("Verdict = %q; want rejected", logs[0].Verdict)
	}
}

// TestAgentDecisionConsumer_PersistsCitationHashes proves the producer-computed
// sha256 citation hashes (attributes[input_hash]/[output_hash] → ev fields)
// land on the persisted ReasoningSummary so the O+ reasoning panel renders a
// real PII-safe citation instead of the all-zeros sentinel (CHO-1700 follow-up).
func TestAgentDecisionConsumer_PersistsCitationHashes(t *testing.T) {
	t.Parallel()
	cons, repo := newDecisionFixture(t)
	ev := goodDecisionEvent()
	ev.InputHash = strings.Repeat("a", 64)
	ev.OutputHash = strings.Repeat("b", 64)

	if err := cons.Handle(context.Background(), ev); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	logs, err := repo.List(context.Background(), "tenant-a", decision.ListFilter{})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(logs) != 1 {
		t.Fatalf("logs = %d; want 1", len(logs))
	}
	if logs[0].Reasoning == nil {
		t.Fatal("Reasoning nil; want citation hashes populated")
	}
	if logs[0].Reasoning.InputHash != strings.Repeat("a", 64) {
		t.Errorf("InputHash = %q; want 64×a", logs[0].Reasoning.InputHash)
	}
	if logs[0].Reasoning.OutputHash != strings.Repeat("b", 64) {
		t.Errorf("OutputHash = %q; want 64×b", logs[0].Reasoning.OutputHash)
	}
}

// TestAgentDecisionConsumer_CitationHashesAloneCreateReasoning proves the
// reasoning record is built from hashes even when both summaries are empty
// (a guardrail-blocked decision may carry only the citation, no output text).
func TestAgentDecisionConsumer_CitationHashesAloneCreateReasoning(t *testing.T) {
	t.Parallel()
	cons, repo := newDecisionFixture(t)
	ev := goodDecisionEvent()
	ev.Reason = ""
	ev.OutputSummary = ""
	ev.InputSummary = ""
	ev.InputHash = strings.Repeat("c", 64)
	ev.OutputHash = strings.Repeat("d", 64)

	if err := cons.Handle(context.Background(), ev); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	logs, _ := repo.List(context.Background(), "tenant-a", decision.ListFilter{})
	if len(logs) != 1 {
		t.Fatalf("logs = %d; want 1", len(logs))
	}
	if logs[0].Reasoning == nil {
		t.Fatal("Reasoning nil; hashes alone must still create a reasoning record")
	}
	if logs[0].Reasoning.InputHash != strings.Repeat("c", 64) {
		t.Errorf("InputHash = %q; want 64×c", logs[0].Reasoning.InputHash)
	}
}

func TestAgentDecisionConsumer_IdempotentOnEventID(t *testing.T) {
	t.Parallel()
	cons, repo := newDecisionFixture(t)
	ev := goodDecisionEvent()

	for i := 0; i < 3; i++ {
		if err := cons.Handle(context.Background(), ev); err != nil {
			t.Fatalf("Handle #%d: %v", i, err)
		}
	}
	logs, err := repo.List(context.Background(), "tenant-a", decision.ListFilter{})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(logs) != 1 {
		t.Fatalf("logs = %d; want 1 (idempotent on event_id)", len(logs))
	}
}

func TestAgentDecisionConsumer_ValidationRejects(t *testing.T) {
	t.Parallel()
	cases := map[string]func(e *events.AgentDecisionLoggedEvent){
		"blank event_id":      func(e *events.AgentDecisionLoggedEvent) { e.EventID = "" },
		"blank tenant_id":     func(e *events.AgentDecisionLoggedEvent) { e.TenantID = "" },
		"blank agid":          func(e *events.AgentDecisionLoggedEvent) { e.Agid = "" },
		"invalid type":        func(e *events.AgentDecisionLoggedEvent) { e.DecisionType = "frobnicate" },
		"invalid risk":        func(e *events.AgentDecisionLoggedEvent) { e.RiskTier = "spicy" },
		"blank correlationid": func(e *events.AgentDecisionLoggedEvent) { e.CorrelationID = "" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			cons, repo := newDecisionFixture(t)
			ev := goodDecisionEvent()
			mutate(&ev)
			if err := cons.Handle(context.Background(), ev); err == nil {
				t.Fatal("expected validation error; got nil (would ack a bad event)")
			}
			logs, _ := repo.List(context.Background(), "tenant-a", decision.ListFilter{})
			if len(logs) != 0 {
				t.Errorf("logs = %d; want 0 (rejected event must not persist)", len(logs))
			}
		})
	}
}
