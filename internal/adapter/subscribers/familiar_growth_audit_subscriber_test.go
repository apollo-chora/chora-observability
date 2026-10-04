// Tests for FamiliarGrowthAuditSubscriber — ADR-149 audit ledger + IMDA
// D1/D2 evidence emission.
package subscribers_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/5007-Capstone/chora/services/chora-observability/internal/adapter/inmem"
	"github.com/5007-Capstone/chora/services/chora-observability/internal/adapter/subscribers"
	fg "github.com/5007-Capstone/chora/services/chora-observability/internal/domain/familiargrowth"
)

const (
	testTenantID = "01970000-0000-7000-8000-000000000001"
	testGCID     = "01970000-0000-7000-8000-000000000002"
	testFamiliar = "01970000-0000-7000-8000-000000000003"
	testEventID1 = "01970000-0000-7000-8000-0000000000aa"
	testEventID2 = "01970000-0000-7000-8000-0000000000bb"
	testEventID3 = "01970000-0000-7000-8000-0000000000cc"
)

// recordedSpan captures a single SpanRecorder.Record call.
type recordedSpan struct {
	Name  string
	Attrs map[string]any
}

type fakeSpanRecorder struct{ spans []recordedSpan }

func (f *fakeSpanRecorder) Record(_ context.Context, name string, attrs map[string]any) {
	f.spans = append(f.spans, recordedSpan{Name: name, Attrs: attrs})
}

func newSubscriber(t *testing.T) (*subscribers.FamiliarGrowthAuditSubscriber, *inmem.FamiliarGrowthRepository, *subscribers.InMemoryEvidencePublisher, *fakeSpanRecorder) {
	t.Helper()
	repo := inmem.NewFamiliarGrowthRepository()
	pub := subscribers.NewInMemoryEvidencePublisher()
	spans := &fakeSpanRecorder{}
	sub := subscribers.New(subscribers.Config{
		Repo:     repo,
		Evidence: pub,
		Spans:    spans,
		Now:      func() time.Time { return time.Date(2026, 5, 13, 12, 0, 0, 0, time.UTC) },
	})
	return sub, repo, pub, spans
}

func TestSubscriber_ExpAwarded_HappyPath(t *testing.T) {
	t.Parallel()
	sub, repo, pub, spans := newSubscriber(t)
	ev := subscribers.FamiliarGrowthEvent{
		SourceTopic:   fg.TopicExpAwarded,
		SourceEventID: testEventID1,
		TenantID:      testTenantID,
		OwnerGCID:     testGCID,
		FamiliarID:    testFamiliar,
		ExpDelta:      25,
		ExpSource:     "atom_session",
		OccurredAt:    time.Date(2026, 5, 13, 10, 30, 0, 0, time.UTC),
		Payload:       map[string]any{"exp_total_after": 125},
	}
	if err := sub.Handle(context.Background(), ev); err != nil {
		t.Fatalf("handle: %v", err)
	}
	ledger, err := repo.ListLedger(context.Background(), testTenantID, fg.AuditFilter{})
	if err != nil {
		t.Fatalf("list ledger: %v", err)
	}
	if len(ledger) != 1 {
		t.Fatalf("expected 1 ledger row; got %d", len(ledger))
	}
	if ledger[0].EventType != "exp_awarded" {
		t.Fatalf("event_type: %q", ledger[0].EventType)
	}
	metrics, err := repo.ListMetrics(context.Background(), testTenantID, fg.AuditFilter{})
	if err != nil {
		t.Fatalf("list metrics: %v", err)
	}
	if len(metrics) != 1 {
		t.Fatalf("expected 1 daily metric; got %d", len(metrics))
	}
	if metrics[0].TotalExpAwarded != 25 || metrics[0].EventCount != 1 || metrics[0].Source != "atom_session" {
		t.Fatalf("metric row: %+v", metrics[0])
	}
	emitted := pub.Emitted()
	if len(emitted) != 1 {
		t.Fatalf("expected 1 evidence emit; got %d", len(emitted))
	}
	if emitted[0].IMDADimension != "accountability" {
		t.Fatalf("expected accountability dimension; got %q", emitted[0].IMDADimension)
	}
	if emitted[0].EvidenceType != "familiar_growth.exp_awarded" {
		t.Fatalf("evidence_type: %q", emitted[0].EvidenceType)
	}
	if len(spans.spans) != 1 || spans.spans[0].Name != "familiar_growth.exp_awarded" {
		t.Fatalf("expected 1 span; got %d", len(spans.spans))
	}
	if spans.spans[0].Attrs["chora.exp.delta"] != int32(25) {
		t.Fatalf("span attr exp.delta: %v", spans.spans[0].Attrs["chora.exp.delta"])
	}
}

func TestSubscriber_StageUp_RollsUpMetrics(t *testing.T) {
	t.Parallel()
	sub, repo, _, _ := newSubscriber(t)
	ev := subscribers.FamiliarGrowthEvent{
		SourceTopic:   fg.TopicStageUp,
		SourceEventID: testEventID1,
		TenantID:      testTenantID,
		OwnerGCID:     testGCID,
		FamiliarID:    testFamiliar,
		StageFrom:     2,
		StageTo:       3,
		OccurredAt:    time.Date(2026, 5, 13, 10, 30, 0, 0, time.UTC),
	}
	if err := sub.Handle(context.Background(), ev); err != nil {
		t.Fatalf("handle: %v", err)
	}
	metrics, _ := repo.ListMetrics(context.Background(), testTenantID, fg.AuditFilter{})
	if len(metrics) != 1 {
		t.Fatalf("expected 1 stage_up metric; got %d", len(metrics))
	}
	if metrics[0].StageUpsCount != 1 || metrics[0].Source != "stage_up" {
		t.Fatalf("stage_up metric: %+v", metrics[0])
	}
}

func TestSubscriber_BreedRevealed_RecordsD2Audit(t *testing.T) {
	t.Parallel()
	sub, repo, pub, _ := newSubscriber(t)
	ev := subscribers.FamiliarGrowthEvent{
		SourceTopic:   fg.TopicBreedRevealed,
		SourceEventID: testEventID1,
		TenantID:      testTenantID,
		OwnerGCID:     testGCID,
		FamiliarID:    testFamiliar,
		EggSKU:        "egg.standard.v1",
		Species:       "dragon",
		Rarity:        "legendary",
		Shiny:         false,
		RolledProbability: 5.0,
		DistributionSnapshot: map[string]any{
			"dragon":  5.0,
			"owl":     25.0,
			"fox":     35.0,
			"rabbit":  35.0,
		},
		OccurredAt: time.Date(2026, 5, 13, 11, 0, 0, 0, time.UTC),
	}
	if err := sub.Handle(context.Background(), ev); err != nil {
		t.Fatalf("handle: %v", err)
	}
	rolls, err := repo.ListBreedRolls(context.Background(), testTenantID, fg.AuditFilter{})
	if err != nil {
		t.Fatalf("list rolls: %v", err)
	}
	if len(rolls) != 1 {
		t.Fatalf("expected 1 roll; got %d", len(rolls))
	}
	if rolls[0].Species != "dragon" || rolls[0].EggSKU != "egg.standard.v1" {
		t.Fatalf("roll row: %+v", rolls[0])
	}
	if rolls[0].DistributionSnapshot == nil || rolls[0].DistributionSnapshot["dragon"] != 5.0 {
		t.Fatalf("distribution snapshot missing: %+v", rolls[0].DistributionSnapshot)
	}
	// Per Fix-D 2026-05-16: breed_revealed must NOT increment EggsHatched
	// (the hatched-funnel count is now sourced from hatched.v1 directly).
	funnel, err := repo.ListEggFunnel(context.Background(), testTenantID, fg.AuditFilter{})
	if err != nil {
		t.Fatalf("list funnel: %v", err)
	}
	if len(funnel) != 0 {
		t.Fatalf("expected zero funnel rows from breed_revealed (proxy removed); got %+v", funnel)
	}
	emitted := pub.Emitted()
	if len(emitted) != 1 {
		t.Fatalf("expected 1 evidence emit; got %d", len(emitted))
	}
	if emitted[0].IMDADimension != "transparency" {
		t.Fatalf("expected transparency dim for breed_revealed; got %q", emitted[0].IMDADimension)
	}
}

// TestFamiliarGrowthAuditSubscriber_ProcessesHatched verifies the new
// hatched.v1 direct handler — Fix-D 2026-05-16 replaces the breed_revealed
// proxy with a real subscriber for the hatched lifecycle event.
func TestFamiliarGrowthAuditSubscriber_ProcessesHatched(t *testing.T) {
	t.Parallel()
	sub, repo, pub, spans := newSubscriber(t)
	ev := subscribers.FamiliarGrowthEvent{
		SourceTopic:   fg.TopicHatched,
		SourceEventID: testEventID1,
		TenantID:      testTenantID,
		OwnerGCID:     testGCID,
		FamiliarID:    testFamiliar,
		OccurredAt:    time.Date(2026, 5, 13, 12, 30, 0, 0, time.UTC),
	}
	if err := sub.Handle(context.Background(), ev); err != nil {
		t.Fatalf("handle: %v", err)
	}

	// Ledger row recorded with event_type "hatched".
	ledger, err := repo.ListLedger(context.Background(), testTenantID, fg.AuditFilter{})
	if err != nil {
		t.Fatalf("list ledger: %v", err)
	}
	if len(ledger) != 1 {
		t.Fatalf("expected 1 ledger row; got %d", len(ledger))
	}
	if ledger[0].EventType != "hatched" {
		t.Fatalf("event_type: %q", ledger[0].EventType)
	}
	if ledger[0].SourceTopic != fg.TopicHatched {
		t.Fatalf("source_topic: %q", ledger[0].SourceTopic)
	}

	// Funnel row recorded with EggsHatched=1 (sourced directly from hatched.v1).
	funnel, err := repo.ListEggFunnel(context.Background(), testTenantID, fg.AuditFilter{})
	if err != nil {
		t.Fatalf("list funnel: %v", err)
	}
	if len(funnel) != 1 || funnel[0].EggsHatched != 1 {
		t.Fatalf("expected 1 funnel row with EggsHatched=1; got %+v", funnel)
	}

	// Evidence emit with IMDA D2 transparency dimension.
	emitted := pub.Emitted()
	if len(emitted) != 1 {
		t.Fatalf("expected 1 evidence emit; got %d", len(emitted))
	}
	if emitted[0].IMDADimension != "transparency" {
		t.Fatalf("expected transparency dimension for hatched; got %q", emitted[0].IMDADimension)
	}
	if emitted[0].EvidenceType != "familiar_growth.hatched" {
		t.Fatalf("evidence_type: %q", emitted[0].EvidenceType)
	}
	if emitted[0].SourceTopic != fg.TopicHatched {
		t.Fatalf("evidence source_topic: %q", emitted[0].SourceTopic)
	}

	// Span recorded with the canonical hatched name + IMDA D2 attribute.
	if len(spans.spans) != 1 || spans.spans[0].Name != "familiar_growth.hatched" {
		t.Fatalf("expected 1 hatched span; got %+v", spans.spans)
	}
	if spans.spans[0].Attrs["chora.imda.d2"] != true {
		t.Fatalf("expected chora.imda.d2=true on hatched span; got %+v", spans.spans[0].Attrs)
	}
	if spans.spans[0].Attrs["chora.familiar_id"] != testFamiliar {
		t.Fatalf("expected familiar_id on hatched span; got %+v", spans.spans[0].Attrs)
	}
}

// TestFamiliarGrowthAuditSubscriber_BreedRevealedNoLongerIncrementsHatchedDelta
// is the regression test that locks in the Fix-D 2026-05-16 invariant: the
// breed_revealed handler must NOT touch the EggsHatched funnel column. The
// canonical source for hatched count is now hatched.v1 directly.
func TestFamiliarGrowthAuditSubscriber_BreedRevealedNoLongerIncrementsHatchedDelta(t *testing.T) {
	t.Parallel()
	sub, repo, _, _ := newSubscriber(t)
	ev := subscribers.FamiliarGrowthEvent{
		SourceTopic:   fg.TopicBreedRevealed,
		SourceEventID: testEventID1,
		TenantID:      testTenantID,
		OwnerGCID:     testGCID,
		FamiliarID:    testFamiliar,
		EggSKU:        "egg.standard.v1",
		Species:       "owl",
		Rarity:        "common",
		Shiny:         false,
		RolledProbability: 35.0,
		OccurredAt:    time.Date(2026, 5, 13, 11, 0, 0, 0, time.UTC),
	}
	if err := sub.Handle(context.Background(), ev); err != nil {
		t.Fatalf("handle: %v", err)
	}
	funnel, err := repo.ListEggFunnel(context.Background(), testTenantID, fg.AuditFilter{})
	if err != nil {
		t.Fatalf("list funnel: %v", err)
	}
	if len(funnel) != 0 {
		t.Fatalf("breed_revealed must NOT write to egg_funnel_metrics (proxy removed Fix-D 2026-05-16); got %+v", funnel)
	}
	// Breed-roll audit row must still be recorded (D2 transparency).
	rolls, err := repo.ListBreedRolls(context.Background(), testTenantID, fg.AuditFilter{})
	if err != nil {
		t.Fatalf("list rolls: %v", err)
	}
	if len(rolls) != 1 {
		t.Fatalf("breed_revealed must still write breed_roll_audit; got %+v", rolls)
	}
}

func TestSubscriber_EggPurchased_AddsToFunnel(t *testing.T) {
	t.Parallel()
	sub, repo, _, _ := newSubscriber(t)
	ev := subscribers.FamiliarGrowthEvent{
		SourceTopic:   fg.TopicEggPurchased,
		SourceEventID: testEventID1,
		TenantID:      testTenantID,
		OwnerGCID:     testGCID,
		FamiliarID:    testFamiliar,
		EggSKU:        "egg.standard.v1",
		PurchaseSource: "purchase",
		OccurredAt:    time.Date(2026, 5, 13, 9, 0, 0, 0, time.UTC),
	}
	if err := sub.Handle(context.Background(), ev); err != nil {
		t.Fatalf("handle: %v", err)
	}
	funnel, _ := repo.ListEggFunnel(context.Background(), testTenantID, fg.AuditFilter{})
	if len(funnel) != 1 || funnel[0].EggsPurchased != 1 {
		t.Fatalf("funnel: %+v", funnel)
	}
}

func TestSubscriber_PaymentSucceeded(t *testing.T) {
	t.Parallel()
	sub, repo, pub, _ := newSubscriber(t)
	ev := subscribers.FamiliarGrowthEvent{
		SourceTopic:   fg.TopicPaymentSucceeded,
		SourceEventID: testEventID1,
		TenantID:      testTenantID,
		OwnerGCID:     testGCID,
		EggSKU:        "egg.standard.v1",
		AmountCents:   1999,
		Currency:      "USD",
		OccurredAt:    time.Date(2026, 5, 13, 8, 0, 0, 0, time.UTC),
	}
	if err := sub.Handle(context.Background(), ev); err != nil {
		t.Fatalf("handle: %v", err)
	}
	ledger, _ := repo.ListLedger(context.Background(), testTenantID, fg.AuditFilter{})
	if len(ledger) != 1 {
		t.Fatalf("expected 1 ledger row; got %d", len(ledger))
	}
	if pub.Emitted()[0].IMDADimension != "accountability" {
		t.Fatalf("expected accountability for payment_succeeded")
	}
}

func TestSubscriber_SourceRevelation(t *testing.T) {
	t.Parallel()
	sub, repo, _, _ := newSubscriber(t)
	ev := subscribers.FamiliarGrowthEvent{
		SourceTopic:   fg.TopicSourceRevelation,
		SourceEventID: testEventID1,
		TenantID:      testTenantID,
		FamiliarID:    testFamiliar,
		WindowDurationSeconds: 86400,
	}
	if err := sub.Handle(context.Background(), ev); err != nil {
		t.Fatalf("handle: %v", err)
	}
	ledger, _ := repo.ListLedger(context.Background(), testTenantID, fg.AuditFilter{})
	if len(ledger) != 1 || ledger[0].EventType != "source_revelation" {
		t.Fatalf("ledger: %+v", ledger)
	}
}

func TestSubscriber_IdempotentReplay(t *testing.T) {
	t.Parallel()
	sub, repo, pub, _ := newSubscriber(t)
	ev := subscribers.FamiliarGrowthEvent{
		SourceTopic:   fg.TopicExpAwarded,
		SourceEventID: testEventID1,
		TenantID:      testTenantID,
		FamiliarID:    testFamiliar,
		ExpDelta:      10,
		ExpSource:     "atom_session",
		OccurredAt:    time.Date(2026, 5, 13, 10, 30, 0, 0, time.UTC),
	}
	for i := 0; i < 3; i++ {
		if err := sub.Handle(context.Background(), ev); err != nil {
			t.Fatalf("handle iter %d: %v", i, err)
		}
	}
	ledger, _ := repo.ListLedger(context.Background(), testTenantID, fg.AuditFilter{})
	if len(ledger) != 1 {
		t.Fatalf("expected 1 ledger row (idempotent replay); got %d", len(ledger))
	}
	if len(pub.Emitted()) != 1 {
		t.Fatalf("expected 1 evidence emit (idempotent replay); got %d", len(pub.Emitted()))
	}
}

func TestSubscriber_MultipleEventsAccumulate(t *testing.T) {
	t.Parallel()
	sub, repo, _, _ := newSubscriber(t)
	day := time.Date(2026, 5, 13, 10, 30, 0, 0, time.UTC)
	for i, eid := range []string{testEventID1, testEventID2, testEventID3} {
		ev := subscribers.FamiliarGrowthEvent{
			SourceTopic:   fg.TopicExpAwarded,
			SourceEventID: eid,
			TenantID:      testTenantID,
			FamiliarID:    testFamiliar,
			ExpDelta:      int32(10 + i),
			ExpSource:     "atom_session",
			OccurredAt:    day,
		}
		if err := sub.Handle(context.Background(), ev); err != nil {
			t.Fatalf("handle %d: %v", i, err)
		}
	}
	metrics, _ := repo.ListMetrics(context.Background(), testTenantID, fg.AuditFilter{})
	if len(metrics) != 1 {
		t.Fatalf("expected 1 daily-bucket; got %d", len(metrics))
	}
	if metrics[0].TotalExpAwarded != 33 {
		t.Fatalf("expected 33 total exp; got %d", metrics[0].TotalExpAwarded)
	}
	if metrics[0].EventCount != 3 {
		t.Fatalf("expected 3 events; got %d", metrics[0].EventCount)
	}
}

func TestSubscriber_RejectsInvalidEvent(t *testing.T) {
	t.Parallel()
	sub, _, _, _ := newSubscriber(t)
	cases := []struct {
		name string
		ev   subscribers.FamiliarGrowthEvent
	}{
		{
			name: "missing topic",
			ev:   subscribers.FamiliarGrowthEvent{SourceEventID: testEventID1, TenantID: testTenantID},
		},
		{
			name: "missing event_id",
			ev:   subscribers.FamiliarGrowthEvent{SourceTopic: fg.TopicExpAwarded, TenantID: testTenantID},
		},
		{
			name: "missing tenant",
			ev:   subscribers.FamiliarGrowthEvent{SourceTopic: fg.TopicExpAwarded, SourceEventID: testEventID1},
		},
		{
			name: "unknown topic",
			ev:   subscribers.FamiliarGrowthEvent{SourceTopic: "chora.bogus.v1", SourceEventID: testEventID1, TenantID: testTenantID},
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			if err := sub.Handle(context.Background(), c.ev); err == nil {
				t.Fatalf("expected error")
			}
		})
	}
}

// fakeEvidencePublisher that returns errors.
type failingEvidence struct{ err error }

func (f failingEvidence) PublishEvidence(_ context.Context, _ subscribers.EvidenceEmit) error {
	return f.err
}

func TestSubscriber_EvidenceFailurePropagates(t *testing.T) {
	t.Parallel()
	repo := inmem.NewFamiliarGrowthRepository()
	sub := subscribers.New(subscribers.Config{
		Repo:     repo,
		Evidence: failingEvidence{err: errors.New("pub failed")},
	})
	err := sub.Handle(context.Background(), subscribers.FamiliarGrowthEvent{
		SourceTopic:   fg.TopicExpAwarded,
		SourceEventID: testEventID1,
		TenantID:      testTenantID,
		FamiliarID:    testFamiliar,
		ExpDelta:      10,
		ExpSource:     "atom_session",
		OccurredAt:    time.Date(2026, 5, 13, 9, 0, 0, 0, time.UTC),
	})
	if err == nil {
		t.Fatalf("expected error from evidence publisher")
	}
}

func TestSubscriber_SubscribedTopics(t *testing.T) {
	t.Parallel()
	sub, _, _, _ := newSubscriber(t)
	topics := sub.SubscribedTopics()
	// 7 topics post-Fix-D 2026-05-16 (added hatched.v1).
	if len(topics) != 7 {
		t.Fatalf("expected 7 subscribed topics; got %d (%v)", len(topics), topics)
	}
	var foundHatched bool
	for _, tp := range topics {
		if tp == fg.TopicHatched {
			foundHatched = true
		}
	}
	if !foundHatched {
		t.Fatalf("expected SubscribedTopics to include hatched.v1; got %v", topics)
	}
}
