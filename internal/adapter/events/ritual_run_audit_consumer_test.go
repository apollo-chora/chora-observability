// ritual_run_audit_consumer_test.go — RitualRunAuditConsumer projection tests
// (ADR-215 / ADR-219 CHO-2016). Strict TDD: the consumer's core contract
// (validate → dedupe via idempotent.Store → map → repo.Ingest) plus the JSON
// StreamingPull decode-binding.
package events_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	consumptionv1 "github.com/locoroco-git/Chora-LMS/chora-contracts/gen/go/chora/consumption/v1"

	"github.com/5007-Capstone/chora/libs/chora-go-common/envelope"
	"github.com/5007-Capstone/chora/libs/chora-go-common/idempotent"
	cgcpubsub "github.com/5007-Capstone/chora/libs/chora-go-common/pubsub"
	"github.com/5007-Capstone/chora/services/chora-observability/internal/adapter/events"
	"github.com/5007-Capstone/chora/services/chora-observability/internal/adapter/inmem"
	ra "github.com/5007-Capstone/chora/services/chora-observability/internal/domain/ritualaudit"
)

const (
	ritTenantID = "01980000-0000-7000-8000-000000000001"
	ritGCID     = "01980000-0000-7000-8000-000000000002"
	ritFamiliar = "01980000-0000-7000-8000-000000000003"
	ritRunID    = "01980000-0000-7000-8000-000000000004"
	ritRitualID = "01980000-0000-7000-8000-000000000005"
	ritEventID1 = "01980000-0000-7000-8000-0000000000aa"
	ritEventID2 = "01980000-0000-7000-8000-0000000000bb"
)

func newRitualConsumer(t *testing.T) (*events.RitualRunAuditConsumer, *inmem.RitualAuditRepository) {
	t.Helper()
	repo := inmem.NewRitualAuditRepository()
	c := events.NewRitualRunAuditConsumer(events.RitualRunAuditConsumerConfig{
		Repo:  repo,
		Inbox: idempotent.NewMemoryStore(),
		Now:   func() time.Time { return time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC) },
	})
	return c, repo
}

func sampleRitualEvent() events.RitualRunCompletedEvent {
	return events.RitualRunCompletedEvent{
		SourceTopic:   ra.TopicFamiliarRitualRunCompleted,
		SourceEventID: ritEventID1,
		TenantID:      ritTenantID,
		OwnerGCID:     ritGCID,
		FamiliarID:    ritFamiliar,
		RunID:         ritRunID,
		RitualID:      ritRitualID,
		RevisionNo:    3,
		TriggerSource: "manual",
		Status:        "completed",
		ManaCharged:   20,
		SinkRef:       "atom://draft/42",
		Stamps: []map[string]any{
			{"StepIndex": float64(0), "SkillKey": "explain", "PromptVersion": "v3"},
			{"StepIndex": float64(1), "SkillKey": "quiz_generative"},
		},
		OccurredAt:  time.Date(2026, 7, 8, 11, 30, 0, 0, time.UTC),
		Traceparent: "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
	}
}

// (a) happy path maps ALL payload fields into the ingested row.
func TestRitualRunAuditConsumer_HappyPath_MapsAllFields(t *testing.T) {
	t.Parallel()
	c, repo := newRitualConsumer(t)
	ev := sampleRitualEvent()
	if err := c.Handle(context.Background(), ev); err != nil {
		t.Fatalf("handle: %v", err)
	}
	rows, err := repo.List(context.Background(), ritTenantID, 0)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row; got %d", len(rows))
	}
	got := rows[0]
	if got.AuditID == "" {
		t.Fatalf("audit_id not minted")
	}
	if got.TenantID != ev.TenantID || got.SourceEventID != ev.SourceEventID ||
		got.SourceTopic != ev.SourceTopic || got.RunID != ev.RunID ||
		got.RitualID != ev.RitualID || got.FamiliarID != ev.FamiliarID ||
		got.OwnerGCID != ev.OwnerGCID {
		t.Fatalf("identity fields mismatch: %+v", got)
	}
	if got.RevisionNo != 3 || got.TriggerSource != "manual" || got.Status != "completed" ||
		got.ManaCharged != 20 || got.SinkRef != "atom://draft/42" || got.ErrorText != "" {
		t.Fatalf("projection fields mismatch: %+v", got)
	}
	if !got.OccurredAt.Equal(ev.OccurredAt) {
		t.Fatalf("occurred_at: got %v want %v", got.OccurredAt, ev.OccurredAt)
	}
	if !got.ReceivedAt.Equal(time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC)) {
		t.Fatalf("received_at: got %v", got.ReceivedAt)
	}
	if len(got.Stamps) != 2 || got.Stamps[0]["SkillKey"] != "explain" || got.Stamps[1]["SkillKey"] != "quiz_generative" {
		t.Fatalf("stamps not preserved: %+v", got.Stamps)
	}
}

// (b) double-delivery of the same source_event_id ingests exactly once.
func TestRitualRunAuditConsumer_IdempotentDoubleDelivery(t *testing.T) {
	t.Parallel()
	c, repo := newRitualConsumer(t)
	ev := sampleRitualEvent()
	for i := 0; i < 3; i++ {
		if err := c.Handle(context.Background(), ev); err != nil {
			t.Fatalf("handle iter %d: %v", i, err)
		}
	}
	rows, _ := repo.List(context.Background(), ritTenantID, 0)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row after 3 deliveries (idempotent); got %d", len(rows))
	}
}

// (c) missing required fields → error, no ingest.
func TestRitualRunAuditConsumer_RejectsMissingRequired(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		mutate func(*events.RitualRunCompletedEvent)
	}{
		{"missing event_id", func(e *events.RitualRunCompletedEvent) { e.SourceEventID = "" }},
		{"missing tenant_id", func(e *events.RitualRunCompletedEvent) { e.TenantID = "" }},
		{"missing run_id", func(e *events.RitualRunCompletedEvent) { e.RunID = "" }},
		{"missing topic", func(e *events.RitualRunCompletedEvent) { e.SourceTopic = "" }},
		{"unknown topic", func(e *events.RitualRunCompletedEvent) { e.SourceTopic = "chora.bogus.v1" }},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c, repo := newRitualConsumer(t)
			ev := sampleRitualEvent()
			tc.mutate(&ev)
			if err := c.Handle(context.Background(), ev); err == nil {
				t.Fatalf("expected error")
			}
			rows, _ := repo.List(context.Background(), ritTenantID, 0)
			if len(rows) != 0 {
				t.Fatalf("expected no ingest on invalid event; got %d rows", len(rows))
			}
		})
	}
}

// dupIngestRepo always returns ErrDuplicateSourceEvent from Ingest.
type dupIngestRepo struct{ ingestN int }

func (d *dupIngestRepo) Ingest(context.Context, ra.RitualRunAuditRow) error {
	d.ingestN++
	return ra.ErrDuplicateSourceEvent
}
func (d *dupIngestRepo) List(context.Context, string, int) ([]ra.RitualRunAuditRow, error) {
	return nil, nil
}

// (d) repo ErrDuplicateSourceEvent is swallowed (no error out of Handle).
func TestRitualRunAuditConsumer_SwallowsRepoDuplicate(t *testing.T) {
	t.Parallel()
	repo := &dupIngestRepo{}
	c := events.NewRitualRunAuditConsumer(events.RitualRunAuditConsumerConfig{
		Repo:  repo,
		Inbox: idempotent.NewMemoryStore(),
	})
	if err := c.Handle(context.Background(), sampleRitualEvent()); err != nil {
		t.Fatalf("expected repo ErrDuplicateSourceEvent to be swallowed; got %v", err)
	}
	if repo.ingestN != 1 {
		t.Fatalf("expected exactly 1 ingest attempt; got %d", repo.ingestN)
	}
}

func TestRitualRunAuditConsumer_SubscribedTopic(t *testing.T) {
	t.Parallel()
	c, _ := newRitualConsumer(t)
	if c.SubscribedTopic() != ra.TopicFamiliarRitualRunCompleted {
		t.Fatalf("subscribed topic: %q", c.SubscribedTopic())
	}
}

// The StreamingPull JSON decode-binding hydrates the event from the payload
// body + the reconstructed envelope (envelope gcid wins over payload
// owner_gcid), then projects it.
func TestRitualRunAuditPullHandler_DecodesJSONPayload(t *testing.T) {
	t.Parallel()
	c, repo := newRitualConsumer(t)
	payload := map[string]any{
		"run_id":         ritRunID,
		"ritual_id":      ritRitualID,
		"familiar_id":    ritFamiliar,
		"owner_gcid":     "should-be-overridden-by-envelope",
		"revision_no":    3,
		"trigger_source": "on_map_open",
		"status":         "completed",
		"mana_charged":   35,
		"sink_ref":       "atom://draft/99",
		"error":          "",
		"stamps": []map[string]any{
			{"StepIndex": 0, "SkillKey": "explain"},
		},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	msg := &cgcpubsub.Message{
		Topic: ra.TopicFamiliarRitualRunCompleted,
		Envelope: envelope.Envelope{
			EventID:     ritEventID2,
			TenantID:    ritTenantID,
			GCID:        ritGCID,
			OccurredAt:  time.Date(2026, 7, 8, 10, 0, 0, 0, time.UTC),
			Traceparent: "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
		},
		Payload: body,
	}
	if err := events.RitualRunAuditPullHandler(c)(context.Background(), msg); err != nil {
		t.Fatalf("pull handle: %v", err)
	}
	rows, _ := repo.List(context.Background(), ritTenantID, 0)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row; got %d", len(rows))
	}
	got := rows[0]
	if got.SourceEventID != ritEventID2 || got.TenantID != ritTenantID {
		t.Fatalf("envelope fields mismatch: %+v", got)
	}
	if got.OwnerGCID != ritGCID {
		t.Fatalf("expected envelope gcid to win; got owner_gcid=%q", got.OwnerGCID)
	}
	if got.RunID != ritRunID || got.RevisionNo != 3 || got.ManaCharged != 35 ||
		got.TriggerSource != "on_map_open" || got.Status != "completed" || got.SinkRef != "atom://draft/99" {
		t.Fatalf("payload fields mismatch: %+v", got)
	}
	if !got.OccurredAt.Equal(time.Date(2026, 7, 8, 10, 0, 0, 0, time.UTC)) {
		t.Fatalf("occurred_at from envelope not applied: %v", got.OccurredAt)
	}
	if len(got.Stamps) != 1 || got.Stamps[0]["SkillKey"] != "explain" {
		t.Fatalf("stamps not decoded: %+v", got.Stamps)
	}
}

// CHO-2136: chora-consumption's outbox now emits canonical BINARY protobuf for
// ritual_run_completed.v1 — the pull handler must proto-first decode (JSON
// stays as the fallback for in-flight legacy rows during the cutover). Binary
// bytes hitting the old json.Unmarshal path NACK → redelivery loop → DLQ, so
// this is the consumer-first half of the co-deploy.
func TestRitualRunAuditPullHandler_DecodesBinaryProtoPayload(t *testing.T) {
	t.Parallel()
	c, repo := newRitualConsumer(t)
	body, err := proto.Marshal(&consumptionv1.RitualRunCompleted{
		RunId:         ritRunID,
		RitualId:      ritRitualID,
		CompanionId:    ritFamiliar,
		OwnerGcid:     "should-be-overridden-by-envelope",
		RevisionNo:    3,
		TriggerSource: "on_map_open",
		Status:        consumptionv1.RitualRunStatus_RITUAL_RUN_STATUS_COMPLETED,
		ManaCharged:   35,
		SinkRef:       "atom://draft/99",
		Stamps: []*consumptionv1.RitualStepStamp{
			{
				StepIndex:     0,
				SkillKey:      "explain_anew",
				PromptVersion: "v3",
				PromptHash:    "sha256:abc",
				ToolsInvoked:  []string{"cite_atom"},
				Citations:     []string{"atom://a1"},
			},
		},
	})
	if err != nil {
		t.Fatalf("marshal binary payload: %v", err)
	}
	msg := &cgcpubsub.Message{
		Topic: ra.TopicFamiliarRitualRunCompleted,
		Envelope: envelope.Envelope{
			EventID:     ritEventID2,
			TenantID:    ritTenantID,
			GCID:        ritGCID,
			OccurredAt:  time.Date(2026, 7, 8, 10, 0, 0, 0, time.UTC),
			Traceparent: "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
		},
		Payload: body,
	}
	if err := events.RitualRunAuditPullHandler(c)(context.Background(), msg); err != nil {
		t.Fatalf("binary payload must decode (proto-first dual decode): %v", err)
	}
	rows, _ := repo.List(context.Background(), ritTenantID, 0)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row; got %d", len(rows))
	}
	got := rows[0]
	if got.RunID != ritRunID || got.RevisionNo != 3 || got.ManaCharged != 35 ||
		got.TriggerSource != "on_map_open" || got.Status != "completed" || got.SinkRef != "atom://draft/99" {
		t.Fatalf("payload fields mismatch: %+v", got)
	}
	if got.OwnerGCID != ritGCID {
		t.Fatalf("expected envelope gcid to win; got owner_gcid=%q", got.OwnerGCID)
	}
	if len(got.Stamps) != 1 {
		t.Fatalf("stamps = %d, want 1", len(got.Stamps))
	}
	s0 := got.Stamps[0]
	if s0["skill_key"] != "explain_anew" || s0["prompt_version"] != "v3" {
		t.Fatalf("stamps must land with canonical snake_case keys: %+v", s0)
	}
}

// A malformed (neither proto nor JSON) payload fails loud so the broker
// NACKs → DLQ.
func TestRitualRunAuditPullHandler_MalformedPayloadErrors(t *testing.T) {
	t.Parallel()
	c, _ := newRitualConsumer(t)
	msg := &cgcpubsub.Message{
		Topic:    ra.TopicFamiliarRitualRunCompleted,
		Envelope: envelope.Envelope{EventID: ritEventID1, TenantID: ritTenantID},
		Payload:  []byte("{not-json"),
	}
	if err := events.RitualRunAuditPullHandler(c)(context.Background(), msg); err == nil {
		t.Fatalf("expected decode error on malformed payload")
	}
}
