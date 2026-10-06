package events_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/common/v1"
	consumptionv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/consumption/v1"
	tenancyv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/tenancy/v1"

	"github.com/apollo-chora/chora-common/envelope"
	"github.com/apollo-chora/chora-common/eventbus"
	"github.com/apollo-chora/chora-observability/internal/adapter/events"
	"github.com/apollo-chora/chora-observability/internal/adapter/inmem"
	"github.com/apollo-chora/chora-observability/internal/adapter/subscribers"
	fg "github.com/apollo-chora/chora-observability/internal/domain/familiargrowth"
)

const (
	growthTenantID = "11111111-1111-7111-8111-111111111111"
	growthGCID     = "22222222-2222-7222-8222-222222222222"
	growthFamiliar = "33333333-3333-7333-8333-333333333333"
)

// newTestSubscriber builds a real FamiliarGrowthAuditSubscriber over the
// in-memory repo so the assertions observe REAL projection, not a mock's
// call log.
func newTestSubscriber(t *testing.T) (*subscribers.FamiliarGrowthAuditSubscriber, *inmem.FamiliarGrowthRepository) {
	t.Helper()
	repo := inmem.NewFamiliarGrowthRepository()
	sub := subscribers.New(subscribers.Config{
		Repo:     repo,
		Evidence: subscribers.NewInMemoryEvidencePublisher(),
	})
	return sub, repo
}

func testEnvelope(eventID string) envelope.Envelope {
	return envelope.Envelope{
		EventID:     eventID,
		TenantID:    growthTenantID,
		GCID:        growthGCID,
		OccurredAt:  time.Date(2026, 7, 17, 10, 0, 0, 0, time.UTC),
		PublishedAt: time.Date(2026, 7, 17, 10, 0, 1, 0, time.UTC),
		Traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
	}
}

// expAwardedProto builds the REAL binary-proto wire shape chora-consumption
// publishes onto chora.consumption.familiar.exp_awarded.v1.
func expAwardedProto(t *testing.T, eventID string, delta int32, source string) []byte {
	t.Helper()
	m := &consumptionv1.CompanionExpAwarded{
		Envelope: &commonv1.EventEnvelope{
			EventId:    eventID,
			TenantId:   growthTenantID,
			Gcid:       growthGCID,
			OccurredAt: timestamppb.New(time.Date(2026, 7, 17, 10, 0, 0, 0, time.UTC)),
		},
		CompanionId: growthFamiliar,
		OwnerGcid:   growthGCID,
		ExpDelta:    delta,
		Source:      source,
	}
	b, err := proto.Marshal(m)
	if err != nil {
		t.Fatalf("marshal exp_awarded: %v", err)
	}
	return b
}

func ledgerRows(t *testing.T, repo *inmem.FamiliarGrowthRepository) []fg.AuditLedgerRow {
	t.Helper()
	rows, err := repo.ListLedger(context.Background(), growthTenantID, fg.AuditFilter{Limit: 100})
	if err != nil {
		t.Fatalf("ListLedger: %v", err)
	}
	return rows
}

// --- Construction: fail loud at BOOT, not at message time --------------------

func TestFamiliarGrowthAuditPullHandler_NilSubscriberIsRejected(t *testing.T) {
	_, err := events.FamiliarGrowthAuditPullHandler(nil, fg.TopicExpAwarded)
	if err == nil {
		t.Fatal("expected an error for a nil subscriber; got nil (a nil subscriber must fail at boot, never silently no-op)")
	}
}

func TestFamiliarGrowthAuditPullHandler_UnknownBoundTopicIsRejected(t *testing.T) {
	sub, _ := newTestSubscriber(t)
	for _, topic := range []string{"", "chora.consumption.familiar.not_a_real_topic.v1"} {
		if _, err := events.FamiliarGrowthAuditPullHandler(sub, topic); err == nil {
			t.Fatalf("expected an error for bound topic %q; got nil", topic)
		}
	}
}

func TestFamiliarGrowthAuditPullHandler_AcceptsEverySubscribedTopic(t *testing.T) {
	sub, _ := newTestSubscriber(t)
	// Derived from the contract, not a hand-written list — a new topic on the
	// subscriber must not silently lack a pull handler.
	for _, topic := range sub.SubscribedTopics() {
		if _, err := events.FamiliarGrowthAuditPullHandler(sub, topic); err != nil {
			t.Fatalf("topic %q is in SubscribedTopics but the pull handler rejected it: %v", topic, err)
		}
	}
}

// --- Projection: the real binary-proto wire shape ----------------------------

func TestFamiliarGrowthAuditPullHandler_ProjectsBinaryProtoExpAwarded(t *testing.T) {
	sub, repo := newTestSubscriber(t)
	h, err := events.FamiliarGrowthAuditPullHandler(sub, fg.TopicExpAwarded)
	if err != nil {
		t.Fatalf("construct: %v", err)
	}

	msg := eventbus.Message{
		Subject:  fg.TopicExpAwarded,
		Envelope: testEnvelope("aaaaaaaa-0000-7000-8000-000000000001"),
		Payload:  expAwardedProto(t, "aaaaaaaa-0000-7000-8000-000000000001", 40, "atom_session"),
	}
	if err := h(context.Background(), msg); err != nil {
		t.Fatalf("handler returned error: %v", err)
	}

	rows := ledgerRows(t, repo)
	if len(rows) != 1 {
		t.Fatalf("expected 1 audit ledger row, got %d", len(rows))
	}
	if rows[0].SourceTopic != fg.TopicExpAwarded {
		t.Errorf("source_topic = %q, want %q", rows[0].SourceTopic, fg.TopicExpAwarded)
	}
	if rows[0].FamiliarID != growthFamiliar {
		t.Errorf("familiar_id = %q, want %q", rows[0].FamiliarID, growthFamiliar)
	}
	if rows[0].EventType != "exp_awarded" {
		t.Errorf("event_type = %q, want exp_awarded", rows[0].EventType)
	}

	metrics, err := repo.ListMetrics(context.Background(), growthTenantID, fg.AuditFilter{Limit: 100})
	if err != nil {
		t.Fatalf("ListMetrics: %v", err)
	}
	if len(metrics) != 1 || metrics[0].TotalExpAwarded != 40 {
		t.Fatalf("expected one daily-metrics row with exp_awarded=40, got %+v", metrics)
	}
}

// TestFamiliarGrowthAuditPullHandler_ProjectsWhenTopicAttributeAbsent is the
// REGRESSION test for the second, latent defect on this lane.
//
// cloud_pubsub.go sets Message.Topic from attributes["topic"]. That attribute
// was added to CloudPublisher LATER than the oldest backlogged events, so a
// message already sitting on the subscription may carry NO topic attribute at
// all — Message.Topic == "". The push handler this consumer replaces read the
// topic from that attribute and silently ACK-AND-DROPPED anything it could not
// match, which would have destroyed exactly the backlog we are here to drain.
//
// A pull consumer is bound to ONE subscription, which is bound to ONE topic, so
// the bound topic is authoritative and the attribute is never needed.
func TestFamiliarGrowthAuditPullHandler_ProjectsWhenTopicAttributeAbsent(t *testing.T) {
	sub, repo := newTestSubscriber(t)
	h, err := events.FamiliarGrowthAuditPullHandler(sub, fg.TopicExpAwarded)
	if err != nil {
		t.Fatalf("construct: %v", err)
	}

	msg := eventbus.Message{
		Subject:  "", // legacy publish — no topic attribute on the wire
		Envelope: testEnvelope("aaaaaaaa-0000-7000-8000-000000000002"),
		Payload:  expAwardedProto(t, "aaaaaaaa-0000-7000-8000-000000000002", 10, "atom_session"),
	}
	if err := h(context.Background(), msg); err != nil {
		t.Fatalf("a message without a topic attribute must still project via the bound topic; got error: %v", err)
	}
	if rows := ledgerRows(t, repo); len(rows) != 1 {
		t.Fatalf("expected the attribute-less message to project 1 row, got %d", len(rows))
	}
}

// --- Fail loud: never a silent ack-and-drop ---------------------------------

func TestFamiliarGrowthAuditPullHandler_MisroutedMessageFailsLoud(t *testing.T) {
	sub, repo := newTestSubscriber(t)
	h, err := events.FamiliarGrowthAuditPullHandler(sub, fg.TopicExpAwarded)
	if err != nil {
		t.Fatalf("construct: %v", err)
	}

	// A message stamped with a DIFFERENT topic than the subscription is bound
	// to is a real broker/fan-out misconfiguration. It must nack → DLQ, never
	// ack-and-drop.
	msg := eventbus.Message{
		Subject:  fg.TopicStageUp,
		Envelope: testEnvelope("aaaaaaaa-0000-7000-8000-000000000003"),
		Payload:  expAwardedProto(t, "aaaaaaaa-0000-7000-8000-000000000003", 5, "atom_session"),
	}
	err = h(context.Background(), msg)
	if err == nil {
		t.Fatal("a misrouted message must return an error (nack → DLQ), not be silently acked")
	}
	if !strings.Contains(err.Error(), fg.TopicStageUp) || !strings.Contains(err.Error(), fg.TopicExpAwarded) {
		t.Errorf("error must name both the delivered and bound topics for diagnosis; got: %v", err)
	}
	if rows := ledgerRows(t, repo); len(rows) != 0 {
		t.Errorf("a misrouted message must not project; got %d rows", len(rows))
	}
}

func TestFamiliarGrowthAuditPullHandler_UndecodablePayloadFailsLoud(t *testing.T) {
	sub, _ := newTestSubscriber(t)
	h, err := events.FamiliarGrowthAuditPullHandler(sub, fg.TopicExpAwarded)
	if err != nil {
		t.Fatalf("construct: %v", err)
	}
	msg := eventbus.Message{
		Subject:  fg.TopicExpAwarded,
		Envelope: testEnvelope("aaaaaaaa-0000-7000-8000-000000000004"),
		Payload:  []byte("{not json and not proto"),
	}
	if err := h(context.Background(), msg); err == nil {
		t.Fatal("an undecodable payload must return an error (nack → DLQ), not be silently acked")
	}
}

func TestFamiliarGrowthAuditPullHandler_EmptyPayloadFailsLoud(t *testing.T) {
	sub, _ := newTestSubscriber(t)
	h, err := events.FamiliarGrowthAuditPullHandler(sub, fg.TopicExpAwarded)
	if err != nil {
		t.Fatalf("construct: %v", err)
	}
	msg := eventbus.Message{
		Subject:  fg.TopicExpAwarded,
		Envelope: testEnvelope("aaaaaaaa-0000-7000-8000-000000000005"),
		Payload:  nil,
	}
	if err := h(context.Background(), msg); err == nil {
		t.Fatal("an empty payload must return an error, not be silently acked")
	}
}

// --- Identity comes from the ENVELOPE, not the payload body ------------------

func TestFamiliarGrowthAuditPullHandler_IdentityFromEnvelope(t *testing.T) {
	sub, repo := newTestSubscriber(t)
	h, err := events.FamiliarGrowthAuditPullHandler(sub, fg.TopicExpAwarded)
	if err != nil {
		t.Fatalf("construct: %v", err)
	}

	// Envelope carries the canonical tenant; the payload body omits it.
	env := testEnvelope("aaaaaaaa-0000-7000-8000-000000000006")
	m := &consumptionv1.CompanionExpAwarded{
		CompanionId: growthFamiliar,
		OwnerGcid:   growthGCID,
		ExpDelta:    7,
		Source:      "ebbinghaus_review",
	}
	payload, merr := proto.Marshal(m)
	if merr != nil {
		t.Fatalf("marshal: %v", merr)
	}

	msg := eventbus.Message{Subject: fg.TopicExpAwarded, Envelope: env, Payload: payload}
	if err := h(context.Background(), msg); err != nil {
		t.Fatalf("handler returned error: %v", err)
	}
	rows := ledgerRows(t, repo)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if rows[0].TenantID != growthTenantID {
		t.Errorf("tenant_id must come from the envelope; got %q", rows[0].TenantID)
	}
	if rows[0].SourceEventID != env.EventID {
		t.Errorf("source_event_id must come from the envelope; got %q want %q", rows[0].SourceEventID, env.EventID)
	}
}

func TestFamiliarGrowthAuditPullHandler_MissingEnvelopeEventIDFailsLoud(t *testing.T) {
	sub, _ := newTestSubscriber(t)
	h, err := events.FamiliarGrowthAuditPullHandler(sub, fg.TopicExpAwarded)
	if err != nil {
		t.Fatalf("construct: %v", err)
	}
	env := testEnvelope("")
	msg := eventbus.Message{
		Subject:  fg.TopicExpAwarded,
		Envelope: env,
		Payload:  expAwardedProto(t, "", 1, "atom_session"),
	}
	// No event_id ⇒ no dedupe key ⇒ the inbox cannot protect the audit table.
	// That must fail loud rather than project an un-deduplicable row.
	if err := h(context.Background(), msg); err == nil {
		t.Fatal("a message with no envelope event_id must fail loud")
	}
}

// --- Idempotency: at-least-once delivery must not duplicate ------------------

func TestFamiliarGrowthAuditPullHandler_RedeliveryIsIdempotent(t *testing.T) {
	sub, repo := newTestSubscriber(t)
	h, err := events.FamiliarGrowthAuditPullHandler(sub, fg.TopicExpAwarded)
	if err != nil {
		t.Fatalf("construct: %v", err)
	}
	newMsg := func() eventbus.Message {
		return eventbus.Message{
			Subject:  fg.TopicExpAwarded,
			Envelope: testEnvelope("aaaaaaaa-0000-7000-8000-000000000007"),
			Payload:  expAwardedProto(t, "aaaaaaaa-0000-7000-8000-000000000007", 25, "atom_session"),
		}
	}
	for i := 0; i < 3; i++ {
		if err := h(context.Background(), newMsg()); err != nil {
			t.Fatalf("delivery %d returned error: %v", i+1, err)
		}
	}
	if rows := ledgerRows(t, repo); len(rows) != 1 {
		t.Fatalf("3 deliveries of the same event_id must project exactly 1 row, got %d", len(rows))
	}
}

// --- Every subscribed topic decodes end-to-end (no unexercised path) ---------

func TestFamiliarGrowthAuditPullHandler_StageUpProjects(t *testing.T) {
	sub, repo := newTestSubscriber(t)
	h, err := events.FamiliarGrowthAuditPullHandler(sub, fg.TopicStageUp)
	if err != nil {
		t.Fatalf("construct: %v", err)
	}
	m := &consumptionv1.CompanionStageUp{
		Envelope:    &commonv1.EventEnvelope{EventId: "bbbbbbbb-0000-7000-8000-000000000001", TenantId: growthTenantID},
		CompanionId: growthFamiliar,
		OwnerGcid:   growthGCID,
		StageFrom:   2,
		StageTo:     3,
	}
	payload, merr := proto.Marshal(m)
	if merr != nil {
		t.Fatalf("marshal: %v", merr)
	}
	msg := eventbus.Message{
		Subject:  fg.TopicStageUp,
		Envelope: testEnvelope("bbbbbbbb-0000-7000-8000-000000000001"),
		Payload:  payload,
	}
	if err := h(context.Background(), msg); err != nil {
		t.Fatalf("stage_up handler returned error: %v", err)
	}
	rows := ledgerRows(t, repo)
	if len(rows) != 1 || rows[0].EventType != "stage_up" {
		t.Fatalf("expected 1 stage_up row, got %+v", rows)
	}
}

func TestFamiliarGrowthAuditPullHandler_HatchedProjectsFunnel(t *testing.T) {
	sub, repo := newTestSubscriber(t)
	h, err := events.FamiliarGrowthAuditPullHandler(sub, fg.TopicHatched)
	if err != nil {
		t.Fatalf("construct: %v", err)
	}
	m := &consumptionv1.CompanionHatched{
		Envelope:    &commonv1.EventEnvelope{EventId: "cccccccc-0000-7000-8000-000000000001", TenantId: growthTenantID},
		CompanionId: growthFamiliar,
		OwnerGcid:   growthGCID,
	}
	payload, merr := proto.Marshal(m)
	if merr != nil {
		t.Fatalf("marshal: %v", merr)
	}
	msg := eventbus.Message{
		Subject:  fg.TopicHatched,
		Envelope: testEnvelope("cccccccc-0000-7000-8000-000000000001"),
		Payload:  payload,
	}
	if err := h(context.Background(), msg); err != nil {
		t.Fatalf("hatched handler returned error: %v", err)
	}
	funnel, ferr := repo.ListEggFunnel(context.Background(), growthTenantID, fg.AuditFilter{Limit: 100})
	if ferr != nil {
		t.Fatalf("ListEggFunnel: %v", ferr)
	}
	if len(funnel) != 1 || funnel[0].EggsHatched != 1 {
		t.Fatalf("hatched must drive the funnel hatched count; got %+v", funnel)
	}
}

// --- A repo failure must propagate (nack), never be swallowed ---------------

type failingGrowthRepo struct{ fg.Repository }

var errRepoDown = errors.New("repo down")

func (failingGrowthRepo) Ingest(context.Context, fg.IngestRequest) error { return errRepoDown }

func TestFamiliarGrowthAuditPullHandler_RepoFailurePropagates(t *testing.T) {
	sub := subscribers.New(subscribers.Config{
		Repo:     failingGrowthRepo{},
		Evidence: subscribers.NewInMemoryEvidencePublisher(),
	})
	h, err := events.FamiliarGrowthAuditPullHandler(sub, fg.TopicExpAwarded)
	if err != nil {
		t.Fatalf("construct: %v", err)
	}
	msg := eventbus.Message{
		Subject:  fg.TopicExpAwarded,
		Envelope: testEnvelope("dddddddd-0000-7000-8000-000000000001"),
		Payload:  expAwardedProto(t, "dddddddd-0000-7000-8000-000000000001", 1, "atom_session"),
	}
	if err := h(context.Background(), msg); err == nil {
		t.Fatal("a repository failure must propagate so the broker nacks → DLQ; got nil")
	}
}

// --- Every one of the 7 bound topics decodes end-to-end ----------------------

// TestFamiliarGrowthAuditPullHandler_EveryBoundTopicProjects exercises a REAL
// binary-proto message on every topic the subscriber claims. An unexercised
// decode path is not evidence it works: breed_revealed, egg_purchased and
// payment_succeeded all have events waiting in the live backlog, and a decode
// bug on any of them would nack-loop those events to the DLQ.
func TestFamiliarGrowthAuditPullHandler_EveryBoundTopicProjects(t *testing.T) {
	env := func(id string) *commonv1.EventEnvelope {
		return &commonv1.EventEnvelope{EventId: id, TenantId: growthTenantID, Gcid: growthGCID}
	}
	cases := []struct {
		topic   string
		eventID string
		msg     proto.Message
		verify  func(t *testing.T, repo *inmem.FamiliarGrowthRepository)
	}{
		{
			topic:   fg.TopicBreedRevealed,
			eventID: "eeee0001-0000-7000-8000-000000000001",
			msg: &consumptionv1.CompanionBreedRevealed{
				Envelope:          env("eeee0001-0000-7000-8000-000000000001"),
				CompanionId:       growthFamiliar,
				OwnerGcid:         growthGCID,
				Species:           consumptionv1.CompanionSpecies_COMPANION_SPECIES_FOX,
				ShinyVariant:      true,
				Rarity:            "legendary",
				EggSku:            "egg_standard_v1",
				RolledProbability: 0.0125,
			},
			verify: func(t *testing.T, repo *inmem.FamiliarGrowthRepository) {
				rolls, err := repo.ListBreedRolls(context.Background(), growthTenantID, fg.AuditFilter{Limit: 10})
				if err != nil {
					t.Fatalf("ListBreedRolls: %v", err)
				}
				if len(rolls) != 1 {
					t.Fatalf("expected 1 breed-roll audit row, got %d", len(rolls))
				}
				// IMDA D2 transparency: the rolled probability + shiny flag are
				// the auditable lootbox record. floatField/boolField decode them.
				if rolls[0].RolledProbability != 0.0125 {
					t.Errorf("rolled_probability = %v, want 0.0125 (float decode)", rolls[0].RolledProbability)
				}
				if !rolls[0].Shiny {
					t.Error("shiny = false, want true (bool decode)")
				}
				if rolls[0].Rarity != "legendary" {
					t.Errorf("rarity = %q", rolls[0].Rarity)
				}
			},
		},
		{
			topic:   fg.TopicEggPurchased,
			eventID: "eeee0002-0000-7000-8000-000000000001",
			msg: &consumptionv1.CompanionEggPurchased{
				Envelope:    env("eeee0002-0000-7000-8000-000000000001"),
				CompanionId: growthFamiliar,
				OwnerGcid:   growthGCID,
				EggSku:      "egg_standard_v1",
				Source:      "purchase",
			},
			verify: func(t *testing.T, repo *inmem.FamiliarGrowthRepository) {
				funnel, err := repo.ListEggFunnel(context.Background(), growthTenantID, fg.AuditFilter{Limit: 10})
				if err != nil {
					t.Fatalf("ListEggFunnel: %v", err)
				}
				if len(funnel) != 1 || funnel[0].EggsPurchased != 1 {
					t.Fatalf("egg_purchased must drive the funnel purchased count; got %+v", funnel)
				}
			},
		},
		{
			topic:   fg.TopicSourceRevelation,
			eventID: "eeee0003-0000-7000-8000-000000000001",
			msg: &consumptionv1.CompanionSourceRevelation{
				Envelope:              env("eeee0003-0000-7000-8000-000000000001"),
				CompanionId:           growthFamiliar,
				OwnerGcid:             growthGCID,
				WindowDurationSeconds: 900,
			},
			verify: func(t *testing.T, repo *inmem.FamiliarGrowthRepository) {
				if rows := ledgerRows(t, repo); len(rows) != 1 || rows[0].EventType != "source_revelation" {
					t.Fatalf("expected 1 source_revelation ledger row, got %+v", rows)
				}
			},
		},
		{
			topic:   fg.TopicPaymentSucceeded,
			eventID: "eeee0004-0000-7000-8000-000000000001",
			msg: &tenancyv1.CompanionEggPaymentSucceeded{
				Envelope:        env("eeee0004-0000-7000-8000-000000000001"),
				EggSku:          "egg_standard_v1",
				AmountCentsPaid: 1299,
				Currency:        "SGD",
			},
			verify: func(t *testing.T, repo *inmem.FamiliarGrowthRepository) {
				rows := ledgerRows(t, repo)
				if len(rows) != 1 || rows[0].EventType != "payment_succeeded" {
					t.Fatalf("expected 1 payment_succeeded ledger row, got %+v", rows)
				}
				// int64Field decodes amount_cents off the proto projection.
				if v, ok := rows[0].Payload["amount_cents"]; !ok || fmt.Sprint(v) != "1299" {
					t.Errorf("payload amount_cents = %v (want 1299 — int64 decode)", rows[0].Payload["amount_cents"])
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.topic, func(t *testing.T) {
			sub, repo := newTestSubscriber(t)
			h, err := events.FamiliarGrowthAuditPullHandler(sub, tc.topic)
			if err != nil {
				t.Fatalf("construct %s: %v", tc.topic, err)
			}
			payload, merr := proto.Marshal(tc.msg)
			if merr != nil {
				t.Fatalf("marshal: %v", merr)
			}
			msg := eventbus.Message{
				Subject:  tc.topic,
				Envelope: testEnvelope(tc.eventID),
				Payload:  payload,
			}
			if err := h(context.Background(), msg); err != nil {
				t.Fatalf("%s handler returned error: %v", tc.topic, err)
			}
			tc.verify(t, repo)
		})
	}
}

// --- ADR-254 companion rename: the live producer lane ------------------------

// TestFamiliarGrowthAuditPullHandler_CompanionTopicsProject is the consumer
// half of the ADR-254 familiar->companion rename: the producer emits the SAME
// binary proto messages on the companion.* subjects, so each companion topic
// must decode + project exactly like its familiar.* twin, recording the
// companion subject as source_topic.
func TestFamiliarGrowthAuditPullHandler_CompanionTopicsProject(t *testing.T) {
	t.Parallel()
	env := func(id string) *commonv1.EventEnvelope {
		return &commonv1.EventEnvelope{EventId: id, TenantId: growthTenantID, Gcid: growthGCID}
	}
	cases := []struct {
		topic   string
		eventID string
		msg     proto.Message
		// wantFamiliar is the companion_id the proto carries; empty means the
		// event type has no companion identity on the wire (payment_succeeded).
		wantFamiliar string
	}{
		{
			topic:   fg.TopicExpAwardedCompanion,
			eventID: "ffff0001-0000-7000-8000-000000000001",
			msg: &consumptionv1.CompanionExpAwarded{
				Envelope:    env("ffff0001-0000-7000-8000-000000000001"),
				CompanionId: growthFamiliar,
				OwnerGcid:   growthGCID,
				ExpDelta:    40,
				Source:      "atom_session",
			},
			wantFamiliar: growthFamiliar,
		},
		{
			topic:   fg.TopicStageUpCompanion,
			eventID: "ffff0002-0000-7000-8000-000000000001",
			msg: &consumptionv1.CompanionStageUp{
				Envelope:    env("ffff0002-0000-7000-8000-000000000001"),
				CompanionId: growthFamiliar,
				OwnerGcid:   growthGCID,
				StageFrom:   2,
				StageTo:     3,
			},
			wantFamiliar: growthFamiliar,
		},
		{
			topic:   fg.TopicBreedRevealedCompanion,
			eventID: "ffff0003-0000-7000-8000-000000000001",
			msg: &consumptionv1.CompanionBreedRevealed{
				Envelope:          env("ffff0003-0000-7000-8000-000000000001"),
				CompanionId:       growthFamiliar,
				OwnerGcid:         growthGCID,
				Species:           consumptionv1.CompanionSpecies_COMPANION_SPECIES_FOX,
				ShinyVariant:      true,
				Rarity:            "legendary",
				EggSku:            "egg_standard_v1",
				RolledProbability: 0.0125,
			},
			wantFamiliar: growthFamiliar,
		},
		{
			topic:   fg.TopicHatchedCompanion,
			eventID: "ffff0004-0000-7000-8000-000000000001",
			msg: &consumptionv1.CompanionHatched{
				Envelope:    env("ffff0004-0000-7000-8000-000000000001"),
				CompanionId: growthFamiliar,
				OwnerGcid:   growthGCID,
			},
			wantFamiliar: growthFamiliar,
		},
		{
			topic:   fg.TopicSourceRevelationCompanion,
			eventID: "ffff0005-0000-7000-8000-000000000001",
			msg: &consumptionv1.CompanionSourceRevelation{
				Envelope:              env("ffff0005-0000-7000-8000-000000000001"),
				CompanionId:           growthFamiliar,
				OwnerGcid:             growthGCID,
				WindowDurationSeconds: 900,
			},
			wantFamiliar: growthFamiliar,
		},
		{
			topic:   fg.TopicEggPurchasedCompanion,
			eventID: "ffff0006-0000-7000-8000-000000000001",
			msg: &consumptionv1.CompanionEggPurchased{
				Envelope:    env("ffff0006-0000-7000-8000-000000000001"),
				CompanionId: growthFamiliar,
				OwnerGcid:   growthGCID,
				EggSku:      "egg_standard_v1",
				Source:      "purchase",
			},
			wantFamiliar: growthFamiliar,
		},
		{
			topic:   fg.TopicPaymentSucceededCompanion,
			eventID: "ffff0007-0000-7000-8000-000000000001",
			msg: &tenancyv1.CompanionEggPaymentSucceeded{
				Envelope:        env("ffff0007-0000-7000-8000-000000000001"),
				EggSku:          "egg_standard_v1",
				AmountCentsPaid: 1299,
				Currency:        "SGD",
			},
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.topic, func(t *testing.T) {
			t.Parallel()
			sub, repo := newTestSubscriber(t)
			h, err := events.FamiliarGrowthAuditPullHandler(sub, tc.topic)
			if err != nil {
				t.Fatalf("construct %s: %v", tc.topic, err)
			}
			payload, merr := proto.Marshal(tc.msg)
			if merr != nil {
				t.Fatalf("marshal: %v", merr)
			}
			msg := eventbus.Message{
				Subject:  tc.topic,
				Envelope: testEnvelope(tc.eventID),
				Payload:  payload,
			}
			if err := h(context.Background(), msg); err != nil {
				t.Fatalf("%s handler returned error: %v", tc.topic, err)
			}
			rows := ledgerRows(t, repo)
			if len(rows) != 1 {
				t.Fatalf("expected 1 ledger row for %s; got %d", tc.topic, len(rows))
			}
			if rows[0].SourceTopic != tc.topic {
				t.Errorf("source_topic = %q, want the companion subject %q", rows[0].SourceTopic, tc.topic)
			}
			if tc.wantFamiliar != "" && rows[0].FamiliarID != tc.wantFamiliar {
				t.Errorf("familiar_id = %q, want %q", rows[0].FamiliarID, tc.wantFamiliar)
			}
		})
	}
}
