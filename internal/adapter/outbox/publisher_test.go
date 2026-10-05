// Package outbox_test — OutboxPublisher adapter tests for chora-observability.
//
// OutboxPublisher satisfies ledger.OutboxRecorder by writing the
// token-usage-recorded event to the canonical outbox_events table (via the
// Store port) instead of publishing directly to Pub/Sub. A separate
// Dispatcher drains the outbox to Cloud Pub/Sub. This decouples emission
// from Pub/Sub availability — a crash between ledger.Append and Publish no
// longer drops the event because the row is durably committed to
// chora_observability before the HTTP request returns.
//
// Per `feedback_d6_resilience_first_class` B.6.2.a — producer-side durable
// emission for chora-observability's
// `chora.observability.token_usage.recorded.v1` stream (and any future
// observability domain events).
package outbox_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/apollo-chora/chora-observability/internal/adapter/outbox"
	"github.com/apollo-chora/chora-observability/internal/domain/ledger"
)

func makeOutboxRecord(eventID, tenant, gcid string, occurred time.Time) ledger.OutboxRecord {
	return ledger.OutboxRecord{
		EventID:            eventID,
		IdempotencyKey:     eventID,
		Topic:              ledger.CanonicalTokenUsageTopic,
		EventType:          ledger.EventTypeTokenUsageRecorded,
		AggregateType:      "token_usage_ledger",
		AggregateID:        eventID,
		TenantID:           tenant,
		GCID:               gcid,
		OccurredAt:         occurred,
		Traceparent:        "",
		SourceProject:      "chora-local",
		SourceService:      "chora-observability",
		SchemaVersion:      ledger.SchemaVersionV1,
		ChoraImdaDimension: ledger.IMDADimensionAccountability,
		ImdaLifecycleStage: ledger.IMDALifecycleStageRuntime,
		Payload:            []byte(`{"prompt_tokens":1}`),
	}
}

func TestOutboxPublisher_RecordOutboxEvent_WritesRowToStore(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	pub := outbox.NewPublisher(outbox.PublisherConfig{
		Store:         store,
		SourceProject: "chora-local",
		SourceService: "chora-observability",
	})

	rec := makeOutboxRecord("01900000-0000-7000-8000-000000000001", "t1", "g1", time.Now().UTC())
	if err := pub.RecordOutboxEvent(context.Background(), rec); err != nil {
		t.Fatalf("RecordOutboxEvent: %v", err)
	}
	rows, _ := store.FetchPending(context.Background(), 10)
	if len(rows) != 1 {
		t.Fatalf("store rows = %d; want 1", len(rows))
	}
	row := rows[0]
	if row.Topic != ledger.CanonicalTokenUsageTopic {
		t.Errorf("row.Topic = %q; want %q", row.Topic, ledger.CanonicalTokenUsageTopic)
	}
	if row.TenantID != rec.TenantID {
		t.Errorf("row.TenantID = %q; want %q", row.TenantID, rec.TenantID)
	}
	if row.GCID != rec.GCID {
		t.Errorf("row.GCID = %q; want %q", row.GCID, rec.GCID)
	}
	if row.IdempotencyKey != rec.IdempotencyKey {
		t.Errorf("row.IdempotencyKey = %q; want %q", row.IdempotencyKey, rec.IdempotencyKey)
	}
	if row.EventType == "" {
		t.Errorf("row.EventType empty")
	}
	if string(row.Payload) != string(rec.Payload) {
		t.Errorf("row.Payload mismatch")
	}
}

func TestOutboxPublisher_RecordOutboxEvent_StampsEnvelopeFields(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	now := time.Date(2026, 5, 12, 10, 0, 0, 0, time.UTC)
	pub := outbox.NewPublisher(outbox.PublisherConfig{
		Store:         store,
		SourceProject: "chora-local",
		SourceService: "chora-observability",
		Now:           func() time.Time { return now },
	})
	rec := makeOutboxRecord("01900000-0000-7000-8000-000000000002", "t2", "g2", now.Add(-1*time.Minute))
	if err := pub.RecordOutboxEvent(context.Background(), rec); err != nil {
		t.Fatalf("RecordOutboxEvent: %v", err)
	}
	rows, _ := store.FetchPending(context.Background(), 1)
	if len(rows) != 1 {
		t.Fatalf("rows = %d; want 1", len(rows))
	}
	env := rows[0].Envelope

	for _, key := range []string{
		"event_id", "idempotency_key", "tenant_id", "occurred_at", "published_at",
		"traceparent", "source_project", "source_service", "schema_version",
	} {
		if env[key] == "" {
			t.Errorf("envelope.%s empty; want non-empty (mandatory per CLAUDE.md §6)", key)
		}
	}
	if env["source_project"] != "chora-local" {
		t.Errorf("envelope.source_project = %q; want chora-local", env["source_project"])
	}
	if env["source_service"] != "chora-observability" {
		t.Errorf("envelope.source_service = %q; want chora-observability", env["source_service"])
	}
	if env["schema_version"] != "1" {
		t.Errorf("envelope.schema_version = %q; want 1", env["schema_version"])
	}
	if env["chora_imda_dimension"] != ledger.IMDADimensionAccountability {
		t.Errorf("envelope.chora_imda_dimension = %q; want accountability", env["chora_imda_dimension"])
	}
	if env["imda_lifecycle_stage"] != ledger.IMDALifecycleStageRuntime {
		t.Errorf("envelope.imda_lifecycle_stage = %q; want runtime", env["imda_lifecycle_stage"])
	}
	if env["tenant_id"] != rec.TenantID {
		t.Errorf("envelope.tenant_id = %q; want %q", env["tenant_id"], rec.TenantID)
	}
	if env["idempotency_key"] != rec.IdempotencyKey {
		t.Errorf("envelope.idempotency_key = %q; want %q", env["idempotency_key"], rec.IdempotencyKey)
	}
}

func TestOutboxPublisher_RecordOutboxEvent_RejectsMissingStore(t *testing.T) {
	t.Parallel()
	pub := outbox.NewPublisher(outbox.PublisherConfig{})
	rec := makeOutboxRecord("01900000-0000-7000-8000-000000000003", "t", "g", time.Now().UTC())
	err := pub.RecordOutboxEvent(context.Background(), rec)
	if err == nil {
		t.Errorf("RecordOutboxEvent without store = nil err; want error")
	}
}

func TestOutboxPublisher_RecordOutboxEvent_DefaultsSourceProjectAndService(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	pub := outbox.NewPublisher(outbox.PublisherConfig{Store: store})
	// Note: empty SourceProject/SourceService on the record means the publisher
	// fills defaults.
	rec := makeOutboxRecord("01900000-0000-7000-8000-000000000004", "t", "g", time.Now().UTC())
	rec.SourceProject = ""
	rec.SourceService = ""
	if err := pub.RecordOutboxEvent(context.Background(), rec); err != nil {
		t.Fatalf("RecordOutboxEvent: %v", err)
	}
	rows, _ := store.FetchPending(context.Background(), 1)
	if rows[0].Envelope["source_project"] != "chora-local" {
		t.Errorf("default source_project = %q; want chora-local", rows[0].Envelope["source_project"])
	}
	if rows[0].Envelope["source_service"] != "chora-observability" {
		t.Errorf("default source_service = %q; want chora-observability", rows[0].Envelope["source_service"])
	}
}

func TestOutboxPublisher_RecordOutboxEvent_PayloadPreservedVerbatim(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	pub := outbox.NewPublisher(outbox.PublisherConfig{Store: store, SourceProject: "chora-local"})

	customPayload := []byte(`{"verdict":"recorded","tokens":42}`)
	rec := makeOutboxRecord("01900000-0000-7000-8000-000000000005", "t", "g", time.Now().UTC())
	rec.Payload = customPayload

	if err := pub.RecordOutboxEvent(context.Background(), rec); err != nil {
		t.Fatalf("RecordOutboxEvent: %v", err)
	}
	rows, _ := store.FetchPending(context.Background(), 1)
	if len(rows) != 1 {
		t.Fatalf("rows = %d; want 1", len(rows))
	}
	var pl map[string]any
	if err := json.Unmarshal(rows[0].Payload, &pl); err != nil {
		t.Fatalf("payload not JSON: %v (%s)", err, string(rows[0].Payload))
	}
	if pl["verdict"] != "recorded" {
		t.Errorf("payload.verdict = %v; want recorded", pl["verdict"])
	}
	if pl["tokens"].(float64) != 42 {
		t.Errorf("payload.tokens = %v; want 42", pl["tokens"])
	}
}

func TestOutboxPublisher_RecordOutboxEvent_RejectsDuplicateIdempotencyKey(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	pub := outbox.NewPublisher(outbox.PublisherConfig{Store: store, SourceProject: "chora-local"})
	rec1 := makeOutboxRecord("ev-aaa", "t", "g", time.Now().UTC())
	rec1.IdempotencyKey = "dup-key"
	rec2 := makeOutboxRecord("ev-bbb", "t", "g", time.Now().UTC())
	rec2.IdempotencyKey = "dup-key"

	if err := pub.RecordOutboxEvent(context.Background(), rec1); err != nil {
		t.Fatalf("first RecordOutboxEvent: %v", err)
	}
	err := pub.RecordOutboxEvent(context.Background(), rec2)
	if err == nil {
		t.Errorf("expected duplicate idempotency_key error on second call")
	}
	if !errors.Is(err, outbox.ErrDuplicateIdempotencyKey) {
		t.Errorf("err = %v; want ErrDuplicateIdempotencyKey", err)
	}
}

func TestOutboxPublisher_TopicMatchesCanonical(t *testing.T) {
	t.Parallel()
	if !strings.HasPrefix(ledger.CanonicalTokenUsageTopic, "chora.observability.") {
		t.Errorf("topic should be in chora.observability.* namespace; got %q",
			ledger.CanonicalTokenUsageTopic)
	}
}

// Compile-time check that Publisher satisfies ledger.OutboxRecorder.
var _ ledger.OutboxRecorder = (*outbox.Publisher)(nil)
