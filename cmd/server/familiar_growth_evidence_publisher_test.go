// familiar_growth_evidence_publisher_test.go — RED→GREEN tests for the durable
// IMDA D1/D2 evidence emit (CHO-2257, AC "Evidence durability").
//
// Before CHO-2257 the PROD path wired subscribers.NewInMemoryEvidencePublisher()
// — every IMDA D1/D2 evidence emit went into a process-local slice and
// evaporated on the next restart. These tests pin the durable replacement and
// the refusal that stops the in-memory publisher reaching prod again.
package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	governancev1 "github.com/apollo-chora/chora-contracts/gen/go/chora/governance/v1"

	cgcenvelope "github.com/apollo-chora/chora-common/envelope"
	obsoutbox "github.com/apollo-chora/chora-observability/internal/adapter/outbox"
	"github.com/apollo-chora/chora-observability/internal/adapter/subscribers"
)

const evTenant = "11111111-1111-7111-8111-111111111111"

func sampleEmit() subscribers.EvidenceEmit {
	return subscribers.EvidenceEmit{
		EvidenceID:     "99999999-0000-7000-8000-000000000001",
		TenantID:       evTenant,
		GCID:           "22222222-2222-7222-8222-222222222222",
		EvidenceType:   "familiar_growth.exp_awarded",
		SourceTopic:    "chora.consumption.familiar.exp_awarded.v1",
		SourceEventID:  "aaaaaaaa-0000-7000-8000-000000000001",
		IMDADimension:  "accountability",
		LifecycleStage: "runtime",
		Traceparent:    "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		OccurredAt:     time.Date(2026, 7, 17, 10, 0, 0, 0, time.UTC),
		AdditionalFields: map[string]any{
			"familiar_id": "33333333-3333-7333-8333-333333333333",
			"audit_id":    "44444444-4444-7444-8444-444444444444",
		},
	}
}

func evidenceCfg() evidencePublisherConfig {
	return evidencePublisherConfig{SourceProject: "chora-489812", SourceService: "chora-observability"}
}

// validatingBus mirrors what CloudPublisher does in prod: it runs the REAL
// envelope validation before "publishing". A row whose envelope cannot validate
// can never leave the outbox, so a test bus that skips validation would prove
// nothing.
type validatingBus struct {
	published []struct {
		topic   string
		env     cgcenvelope.Envelope
		payload []byte
	}
}

func (b *validatingBus) Publish(_ context.Context, topic string, env cgcenvelope.Envelope, payload []byte) error {
	if err := cgcenvelope.Validate(env); err != nil {
		return err
	}
	b.published = append(b.published, struct {
		topic   string
		env     cgcenvelope.Envelope
		payload []byte
	}{topic, env, payload})
	return nil
}

// --- Durability: the emit lands in the outbox and survives the process --------

func TestOutboxEvidencePublisher_InsertsDurableOutboxRow(t *testing.T) {
	store := obsoutbox.NewInMemoryStore()
	pub := newOutboxEvidencePublisher(store, evidenceCfg())

	if err := pub.PublishEvidence(context.Background(), sampleEmit()); err != nil {
		t.Fatalf("PublishEvidence: %v", err)
	}

	rows, err := store.FetchPending(context.Background(), 10)
	if err != nil {
		t.Fatalf("FetchPending: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected exactly 1 durable outbox row, got %d", len(rows))
	}
	if rows[0].Topic != subscribers.IMDAEvidenceTopic {
		t.Errorf("topic = %q, want %q", rows[0].Topic, subscribers.IMDAEvidenceTopic)
	}
	if rows[0].TenantID != evTenant {
		t.Errorf("tenant_id = %q, want %q", rows[0].TenantID, evTenant)
	}
}

// TestOutboxEvidencePublisher_DrainsThroughTheRealDispatcher is the end-to-end
// proof: the row this publisher writes must survive the REAL dispatcher —
// reconstructEnvelope + envelope validation included — and reach the bus. An
// incomplete envelope map would strand the row in the outbox forever, which is
// exactly the kind of silent gap CHO-2257 is about.
func TestOutboxEvidencePublisher_DrainsThroughTheRealDispatcher(t *testing.T) {
	store := obsoutbox.NewInMemoryStore()
	pub := newOutboxEvidencePublisher(store, evidenceCfg())
	if err := pub.PublishEvidence(context.Background(), sampleEmit()); err != nil {
		t.Fatalf("PublishEvidence: %v", err)
	}

	bus := &validatingBus{}
	d := obsoutbox.NewDispatcher(obsoutbox.DispatcherConfig{
		Store: store, Bus: bus, WorkerID: "test-worker",
	})
	n, err := d.DrainOnce(context.Background(), 10)
	if err != nil {
		t.Fatalf("DrainOnce: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected 1 row dispatched, got %d", n)
	}
	if len(bus.published) != 1 {
		t.Fatalf("the evidence row did not reach the bus — its envelope failed validation and it would sit in the outbox forever")
	}
	got := bus.published[0]
	if got.topic != subscribers.IMDAEvidenceTopic {
		t.Errorf("published topic = %q, want %q", got.topic, subscribers.IMDAEvidenceTopic)
	}
	if err := cgcenvelope.ValidateStrict(got.env); err != nil {
		t.Errorf("envelope must satisfy the strict (ADR-141 canonical IMDA label) check: %v", err)
	}
	if got.env.ChoraImdaDimension != "accountability" {
		t.Errorf("chora_imda_dimension = %q, want accountability", got.env.ChoraImdaDimension)
	}
}

// TestOutboxEvidencePublisher_PayloadIsBinaryProto pins the wire shape against
// deployed reality: chora.governance.evidence.recorded.v1 carries a BINARY
// proto schema (schema chora-governance-evidence-recorded-v1, verified
// 2026-07-17). A JSON payload would be rejected with a 400 AT PUBLISH and never
// reach a DLQ.
func TestOutboxEvidencePublisher_PayloadIsBinaryProto(t *testing.T) {
	store := obsoutbox.NewInMemoryStore()
	pub := newOutboxEvidencePublisher(store, evidenceCfg())
	if err := pub.PublishEvidence(context.Background(), sampleEmit()); err != nil {
		t.Fatalf("PublishEvidence: %v", err)
	}
	rows, _ := store.FetchPending(context.Background(), 10)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}

	var m governancev1.EvidenceRecorded
	if err := proto.Unmarshal(rows[0].Payload, &m); err != nil {
		t.Fatalf("payload must be binary proto matching the registered schema: %v", err)
	}
	if m.GetEvidenceType() != "familiar_growth.exp_awarded" {
		t.Errorf("evidence_type = %q", m.GetEvidenceType())
	}
	if m.GetSourceEventType() != "chora.consumption.familiar.exp_awarded.v1" {
		t.Errorf("source_event_type = %q, want the source topic", m.GetSourceEventType())
	}
	if m.GetEnvelope().GetTenantId() != evTenant {
		t.Errorf("proto envelope tenant_id = %q", m.GetEnvelope().GetTenantId())
	}
	if m.GetEnvelope().GetChoraImdaDimension() != "accountability" {
		t.Errorf("proto envelope chora_imda_dimension = %q", m.GetEnvelope().GetChoraImdaDimension())
	}
	if m.GetAdditionalFields()["familiar_id"] != "33333333-3333-7333-8333-333333333333" {
		t.Errorf("additional_fields.familiar_id missing: %+v", m.GetAdditionalFields())
	}
	if m.GetRecordedAt() == nil {
		t.Error("recorded_at must be stamped")
	}
}

// --- Idempotency + fail-loud -------------------------------------------------

func TestOutboxEvidencePublisher_DuplicateEmitIsIdempotent(t *testing.T) {
	store := obsoutbox.NewInMemoryStore()
	pub := newOutboxEvidencePublisher(store, evidenceCfg())
	for i := 0; i < 3; i++ {
		if err := pub.PublishEvidence(context.Background(), sampleEmit()); err != nil {
			t.Fatalf("emit %d: a redelivered evidence emit is a replay, not a failure: %v", i+1, err)
		}
	}
	rows, _ := store.FetchPending(context.Background(), 10)
	if len(rows) != 1 {
		t.Fatalf("3 identical emits must write exactly 1 row, got %d", len(rows))
	}
}

type failingOutboxStore struct{ obsoutbox.Store }

var errInsertDown = errors.New("outbox insert failed")

func (failingOutboxStore) Insert(context.Context, obsoutbox.Row) error { return errInsertDown }

func TestOutboxEvidencePublisher_InsertFailurePropagates(t *testing.T) {
	pub := newOutboxEvidencePublisher(failingOutboxStore{}, evidenceCfg())
	err := pub.PublishEvidence(context.Background(), sampleEmit())
	if err == nil {
		t.Fatal("an outbox insert failure must propagate so the subscriber nacks; got nil (evidence must never be silently dropped)")
	}
	if !errors.Is(err, errInsertDown) {
		t.Errorf("the underlying error must be wrapped, not replaced: %v", err)
	}
}

func TestOutboxEvidencePublisher_NilStoreIsRejected(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("a nil store must fail loudly at construction, never yield a silently-dropping publisher")
		}
	}()
	_ = newOutboxEvidencePublisher(nil, evidenceCfg())
}

// --- The refusal: the in-memory publisher must never reach prod --------------

func TestSelectEvidencePublisher_RefusesInMemoryInProd(t *testing.T) {
	// durable=true (a DB pool is up ⇒ prod) but no outbox store ⇒ the only
	// remaining option is the process-local buffer. That must REFUSE, not
	// silently buffer IMDA evidence into a slice.
	_, err := selectEvidencePublisher(nil, true, evidenceCfg())
	if err == nil {
		t.Fatal("with a DB pool up and no outbox store, wiring must fail loudly rather than fall back to the in-memory publisher")
	}
}

func TestSelectEvidencePublisher_ProdUsesDurableOutbox(t *testing.T) {
	got, err := selectEvidencePublisher(obsoutbox.NewInMemoryStore(), true, evidenceCfg())
	if err != nil {
		t.Fatalf("selectEvidencePublisher: %v", err)
	}
	if _, ok := got.(*outboxEvidencePublisher); !ok {
		t.Fatalf("prod must wire the durable outbox publisher, got %T", got)
	}
}

func TestSelectEvidencePublisher_DevUsesInMemory(t *testing.T) {
	// durable=false (no DB pool ⇒ dev/local): the in-memory publisher is the
	// honest choice, and nothing durable exists to write to anyway.
	got, err := selectEvidencePublisher(nil, false, evidenceCfg())
	if err != nil {
		t.Fatalf("the dev path must wire cleanly: %v", err)
	}
	if _, ok := got.(*subscribers.InMemoryEvidencePublisher); !ok {
		t.Fatalf("dev must wire the in-memory publisher, got %T", got)
	}
}
