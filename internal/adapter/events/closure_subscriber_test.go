// Tests for the federated-closure-saga subscriber.
//
// Per Tier 3 D11 + the federated saga contract:
//
//	chora.closure.requested.v1 (closure orchestrator)
//	  → chora.observability.pii.pseudonymise.requested.v1 (per-domain fan-out)
//	    → chora-observability ClosureSubscriber.Handle
//	      → apply PII_Closure_Map.yaml fields to repo
//	      → emit chora.observability.account.pseudonymised.v1
package events_test

import (
	"context"
	"strings"
	"testing"

	"github.com/apollo-chora/chora-observability/internal/adapter/events"
	"github.com/apollo-chora/chora-observability/internal/config"
)

const (
	testTenantID   = "01970000-0000-7000-8000-0000000000aa"
	testGCID       = "01970000-0000-7000-8000-0000000000bb"
	testSagaID     = "01970000-0000-7000-8000-0000000000cc"
	testTrace      = "00-1234567890abcdef1234567890abcdef-1234567890abcdef-01"
	testTracestate = "vendor=test"
)

func newPIIMap(domainName string) *config.PIIClosureMap {
	pii := &config.PIIClosureMap{
		Domain:  domainName,
		Version: "1.0",
		FieldsToTokenize: []config.TableSpec{
			{
				Table: "t1",
				Columns: []config.ColumnSpec{
					{Column: "c1", Strategy: "tombstone_string", Value: "Former member"},
				},
			},
		},
		OnCreatorClosure: config.CreatorClosureSpec{
			Strategy:         "tokenise_authorship_keep_atom",
			ShowAuthorshipAs: "Former member",
		},
	}
	pii.AGIDApplicable = (domainName == "chora_a2a")
	return pii
}

func TestClosureSubscriber_HappyPath(t *testing.T) {
	t.Parallel()
	pub := events.NewInMemoryClosurePublisher()
	repo := events.NewInMemoryClosureRepo()
	repo.SeedSubject(testTenantID, testGCID)

	sub := events.NewClosureSubscriber(repo, pub, newPIIMap("chora_observability"), nil)

	payload := events.PseudonymiseRequestedPayload{
		SagaID: testSagaID, Gcid: testGCID, TenantID: testTenantID,
		Traceparent: testTrace, Tracestate: testTracestate,
	}
	if err := sub.Handle(context.Background(), payload); err != nil {
		t.Fatalf("handle: %v", err)
	}

	emitted := pub.ClosureRecordedByTopic(events.TopicPseudonymiseCompleted)
	if len(emitted) != 1 {
		t.Fatalf("expected 1 completed event; got %d", len(emitted))
	}
	if emitted[0].TenantID != testTenantID {
		t.Fatalf("envelope tenant: %q", emitted[0].TenantID)
	}
	got := emitted[0].Payload
	if got["saga_id"] != testSagaID {
		t.Fatalf("saga_id: %v", got["saga_id"])
	}
	if got["domain"] != "observability" {
		t.Fatalf("domain: %v", got["domain"])
	}
	if got["chora_imda_dimension"] != "accountability" {
		t.Fatalf("imda dimension: %v", got["chora_imda_dimension"])
	}
	if got["imda_lifecycle_stage"] != "runtime" {
		t.Fatalf("imda lifecycle: %v", got["imda_lifecycle_stage"])
	}
	pseudonymised, err := repo.IsPseudonymised(context.Background(), testTenantID, testGCID)
	if err != nil {
		t.Fatalf("IsPseudonymised: %v", err)
	}
	if !pseudonymised {
		t.Fatalf("subject not pseudonymised in repo")
	}
}

func TestClosureSubscriber_IdempotentReplay(t *testing.T) {
	t.Parallel()
	pub := events.NewInMemoryClosurePublisher()
	repo := events.NewInMemoryClosureRepo()
	repo.SeedSubject(testTenantID, testGCID)

	sub := events.NewClosureSubscriber(repo, pub, newPIIMap("chora_observability"), nil)

	payload := events.PseudonymiseRequestedPayload{
		SagaID: testSagaID, Gcid: testGCID, TenantID: testTenantID,
	}
	if err := sub.Handle(context.Background(), payload); err != nil {
		t.Fatalf("first handle: %v", err)
	}
	if err := sub.Handle(context.Background(), payload); err != nil {
		t.Fatalf("replay should be no-op: %v", err)
	}
	emitted := pub.ClosureRecordedByTopic(events.TopicPseudonymiseCompleted)
	if len(emitted) != 1 {
		t.Fatalf("expected 1 completed event after replay; got %d", len(emitted))
	}
}

func TestClosureSubscriber_AGIDHandling(t *testing.T) {
	t.Parallel()
	pub := events.NewInMemoryClosurePublisher()
	repo := events.NewInMemoryClosureRepo()
	pii := newPIIMap("chora_observability")

	sub := events.NewClosureSubscriber(repo, pub, pii, nil)

	agid := "0197a000-0000-0000-0000-000000000001"
	payload := events.PseudonymiseRequestedPayload{
		SagaID: testSagaID, Gcid: agid, TenantID: testTenantID, SubjectKind: "agid",
	}
	err := sub.Handle(context.Background(), payload)
	if err != nil {
		t.Fatalf("handle should not fail for AGID: %v", err)
	}
	emitted := pub.ClosureRecordedByTopic(events.TopicPseudonymiseCompleted)
	if len(emitted) != 1 {
		t.Fatalf("expected 1 ack event for AGID; got %d", len(emitted))
	}
	// Domain-specific: AGID-applicable domains process; non-applicable skip.
	if pii.AGIDApplicable {
		if emitted[0].Payload["status"] != "ok" {
			t.Fatalf("AGID-applicable domain should ack ok; got %v", emitted[0].Payload["status"])
		}
	} else {
		if emitted[0].Payload["status"] != "skipped_agid_closure" {
			t.Fatalf("non-AGID domain should ack skipped; got %v", emitted[0].Payload["status"])
		}
	}
}

func TestClosureSubscriber_RejectsMissingFields(t *testing.T) {
	t.Parallel()
	pub := events.NewInMemoryClosurePublisher()
	repo := events.NewInMemoryClosureRepo()
	sub := events.NewClosureSubscriber(repo, pub, newPIIMap("chora_observability"), nil)
	ctx := context.Background()

	tests := map[string]events.PseudonymiseRequestedPayload{
		"empty_saga_id": {Gcid: testGCID, TenantID: testTenantID},
		"empty_gcid":    {SagaID: testSagaID, TenantID: testTenantID},
		"empty_tenant":  {SagaID: testSagaID, Gcid: testGCID},
	}
	for name, p := range tests {
		t.Run(name, func(t *testing.T) {
			err := sub.Handle(ctx, p)
			if err == nil {
				t.Fatalf("expected validation error for %s", name)
			}
			if !strings.Contains(err.Error(), "required") {
				t.Fatalf("expected 'required' error for %s; got %v", name, err)
			}
		})
	}
}

func TestClosureSubscriber_TopicConstants(t *testing.T) {
	t.Parallel()
	if events.TopicPseudonymiseRequested != "chora.observability.pii.pseudonymise.requested.v1" {
		t.Fatalf("requested topic: %q", events.TopicPseudonymiseRequested)
	}
	if events.TopicPseudonymiseCompleted != "chora.observability.account.pseudonymised.v1" {
		t.Fatalf("completed topic: %q", events.TopicPseudonymiseCompleted)
	}
	if events.TopicPseudonymiseFailed != "chora.observability.pii.pseudonymise.failed.v1" {
		t.Fatalf("failed topic: %q", events.TopicPseudonymiseFailed)
	}
}

func TestClosureSubscriber_RepoFailureEmitsCompensation(t *testing.T) {
	t.Parallel()
	pub := events.NewInMemoryClosurePublisher()
	repo := events.NewInMemoryClosureRepo()
	repo.SetFailNext(true)

	sub := events.NewClosureSubscriber(repo, pub, newPIIMap("chora_observability"), nil)

	payload := events.PseudonymiseRequestedPayload{
		SagaID: testSagaID, Gcid: testGCID, TenantID: testTenantID,
	}
	err := sub.Handle(context.Background(), payload)
	if err == nil {
		t.Fatalf("expected error from repo")
	}
	failed := pub.ClosureRecordedByTopic(events.TopicPseudonymiseFailed)
	if len(failed) != 1 {
		t.Fatalf("expected 1 failed event; got %d", len(failed))
	}
}

func TestClosureSubscriber_NilGuards(t *testing.T) {
	t.Parallel()
	if err := (&events.ClosureSubscriber{}).Handle(context.Background(), events.PseudonymiseRequestedPayload{}); err == nil {
		t.Fatalf("nil sub should error")
	}
}

func TestClosureSubscriber_SubscribedTopic(t *testing.T) {
	t.Parallel()
	pub := events.NewInMemoryClosurePublisher()
	repo := events.NewInMemoryClosureRepo()
	sub := events.NewClosureSubscriber(repo, pub, &config.PIIClosureMap{Domain: "x"}, nil)
	if got := sub.SubscribedTopic(); got != events.TopicPseudonymiseRequested {
		t.Fatalf("SubscribedTopic = %q want %q", got, events.TopicPseudonymiseRequested)
	}
}

func TestInMemoryClosureRepo_IsPseudonymised_TenantScoped(t *testing.T) {
	t.Parallel()
	repo := events.NewInMemoryClosureRepo()
	ctx := context.Background()
	const otherTenant = "01970000-0000-7000-8000-0000000000dd"

	// Pseudonymise the GCID under testTenantID only.
	if _, err := repo.Pseudonymise(ctx, testTenantID, testGCID, nil); err != nil {
		t.Fatalf("Pseudonymise: %v", err)
	}

	got, err := repo.IsPseudonymised(ctx, testTenantID, testGCID)
	if err != nil {
		t.Fatalf("IsPseudonymised(testTenantID): %v", err)
	}
	if !got {
		t.Fatalf("expected pseudonymised=true for (testTenantID, testGCID)")
	}

	// The SAME gcid under a DIFFERENT tenant must NOT read as pseudonymised
	// — a pre-fix cross-tenant collision (the map keyed on gcid alone).
	got, err = repo.IsPseudonymised(ctx, otherTenant, testGCID)
	if err != nil {
		t.Fatalf("IsPseudonymised(otherTenant): %v", err)
	}
	if got {
		t.Fatalf("cross-tenant collision: gcid pseudonymised in %q leaked into %q", testTenantID, otherTenant)
	}
}

func TestInMemoryClosurePublisher_RecordedAll(t *testing.T) {
	t.Parallel()
	pub := events.NewInMemoryClosurePublisher()
	if err := pub.Publish("chora.test.x.v1", "t", "g", "tp", map[string]interface{}{"k": "v"}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if err := pub.Publish("", "t", "g", "tp", map[string]interface{}{}); err == nil {
		t.Fatalf("expected error for empty topic")
	}
	all := pub.ClosureRecorded()
	if len(all) != 1 {
		t.Fatalf("expected 1 recorded; got %d", len(all))
	}
}
