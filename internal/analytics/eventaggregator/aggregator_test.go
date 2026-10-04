// Package eventaggregator_test exercises the cross-domain event aggregator.
//
// The aggregator subscribes (read-only) to ALL `chora.*.v1` Pub/Sub events
// and rolls them into per-period (hour / day / week / month) counters keyed
// by tenant + domain + aggregate + event_type.
//
// Tests follow strict TDD (per .claude/rules/development-execution.md
// §"TDD Enforcement (mandatory)"): RED → GREEN → REFACTOR.
//
// Migrated from services/chora-analytics/internal/domain/event_aggregator
// at M12.2.E.4 — chora-analytics consolidated into chora-observability.
package eventaggregator_test

import (
	"testing"
	"time"

	"github.com/apollo-chora/chora-observability/internal/analytics/eventaggregator"
)

func mustNewEnvelope(t *testing.T, tenant, gcid, topic string, occurred time.Time) eventaggregator.Envelope {
	t.Helper()
	env, err := eventaggregator.NewEnvelope(eventaggregator.EnvelopeParams{
		EventID:        "01900000-0000-7000-8000-000000000001",
		IdempotencyKey: "01900000-0000-7000-8000-000000000001",
		TenantID:       tenant,
		GCID:           gcid,
		Topic:          topic,
		OccurredAt:     occurred,
		PublishedAt:    occurred,
		SchemaVersion:  1,
	})
	if err != nil {
		t.Fatalf("NewEnvelope: %v", err)
	}
	return env
}

func TestNewEnvelope_RejectsBlankTenant(t *testing.T) {
	_, err := eventaggregator.NewEnvelope(eventaggregator.EnvelopeParams{
		EventID:        "01900000-0000-7000-8000-000000000001",
		IdempotencyKey: "01900000-0000-7000-8000-000000000001",
		TenantID:       "",
		GCID:           "01900000-0000-7000-8000-000000000002",
		Topic:          "chora.consumption.session.completed.v1",
		OccurredAt:     time.Now().UTC(),
		PublishedAt:    time.Now().UTC(),
		SchemaVersion:  1,
	})
	if err == nil {
		t.Fatal("expected error for blank tenant_id")
	}
}

func TestNewEnvelope_RejectsBadTopic(t *testing.T) {
	_, err := eventaggregator.NewEnvelope(eventaggregator.EnvelopeParams{
		EventID:        "01900000-0000-7000-8000-000000000001",
		IdempotencyKey: "01900000-0000-7000-8000-000000000001",
		TenantID:       "t1",
		GCID:           "01900000-0000-7000-8000-000000000002",
		Topic:          "not-a-chora-topic",
		OccurredAt:     time.Now().UTC(),
		PublishedAt:    time.Now().UTC(),
		SchemaVersion:  1,
	})
	if err == nil {
		t.Fatal("expected error for malformed topic")
	}
}

func TestNewEnvelope_ParsesTopicSegments(t *testing.T) {
	env := mustNewEnvelope(t, "t1", "01900000-0000-7000-8000-000000000002",
		"chora.consumption.session.completed.v1", time.Now().UTC())
	if env.Domain != "consumption" {
		t.Errorf("Domain = %q want consumption", env.Domain)
	}
	if env.Aggregate != "session" {
		t.Errorf("Aggregate = %q want session", env.Aggregate)
	}
	if env.EventType != "completed" {
		t.Errorf("EventType = %q want completed", env.EventType)
	}
}

func TestAggregator_IngestThenQuery_PerDay(t *testing.T) {
	agg := eventaggregator.NewAggregator()
	day := time.Date(2026, 5, 8, 10, 0, 0, 0, time.UTC)

	env := mustNewEnvelope(t, "t1", "01900000-0000-7000-8000-000000000002",
		"chora.consumption.session.completed.v1", day)
	if err := agg.Ingest(env); err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	bucket, err := agg.Query(eventaggregator.QueryParams{
		TenantID: "t1",
		Period:   eventaggregator.PeriodDay,
		At:       day,
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	got := bucket.CountFor("consumption", "session", "completed")
	if got != 1 {
		t.Errorf("CountFor=%d want 1", got)
	}
}

func TestAggregator_IsIdempotent_OnReingest(t *testing.T) {
	agg := eventaggregator.NewAggregator()
	day := time.Date(2026, 5, 8, 10, 0, 0, 0, time.UTC)

	env := mustNewEnvelope(t, "t1", "01900000-0000-7000-8000-000000000002",
		"chora.consumption.session.completed.v1", day)
	for i := 0; i < 5; i++ {
		if err := agg.Ingest(env); err != nil {
			t.Fatalf("Ingest #%d: %v", i, err)
		}
	}

	bucket, err := agg.Query(eventaggregator.QueryParams{
		TenantID: "t1",
		Period:   eventaggregator.PeriodDay,
		At:       day,
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	got := bucket.CountFor("consumption", "session", "completed")
	if got != 1 {
		t.Errorf("idempotent re-ingest leaked: count=%d want 1", got)
	}
}

func TestAggregator_DistinctTenants_AreIsolated(t *testing.T) {
	agg := eventaggregator.NewAggregator()
	day := time.Date(2026, 5, 8, 10, 0, 0, 0, time.UTC)

	envA, _ := eventaggregator.NewEnvelope(eventaggregator.EnvelopeParams{
		EventID:        "01900000-0000-7000-8000-00000000000a",
		IdempotencyKey: "01900000-0000-7000-8000-00000000000a",
		TenantID:       "t-A",
		GCID:           "01900000-0000-7000-8000-00000000000b",
		Topic:          "chora.creation.atom.published.v1",
		OccurredAt:     day,
		PublishedAt:    day,
		SchemaVersion:  1,
	})
	envB, _ := eventaggregator.NewEnvelope(eventaggregator.EnvelopeParams{
		EventID:        "01900000-0000-7000-8000-00000000000c",
		IdempotencyKey: "01900000-0000-7000-8000-00000000000c",
		TenantID:       "t-B",
		GCID:           "01900000-0000-7000-8000-00000000000d",
		Topic:          "chora.creation.atom.published.v1",
		OccurredAt:     day,
		PublishedAt:    day,
		SchemaVersion:  1,
	})
	_ = agg.Ingest(envA)
	_ = agg.Ingest(envB)

	bA, _ := agg.Query(eventaggregator.QueryParams{TenantID: "t-A", Period: eventaggregator.PeriodDay, At: day})
	bB, _ := agg.Query(eventaggregator.QueryParams{TenantID: "t-B", Period: eventaggregator.PeriodDay, At: day})
	if bA.CountFor("creation", "atom", "published") != 1 {
		t.Errorf("tenant A bleed: %d", bA.CountFor("creation", "atom", "published"))
	}
	if bB.CountFor("creation", "atom", "published") != 1 {
		t.Errorf("tenant B bleed: %d", bB.CountFor("creation", "atom", "published"))
	}
}

func TestAggregator_PeriodTruncation_Hour_Day_Week_Month(t *testing.T) {
	agg := eventaggregator.NewAggregator()
	t0 := time.Date(2026, 5, 8, 10, 30, 45, 0, time.UTC)
	t1 := time.Date(2026, 5, 8, 11, 15, 0, 0, time.UTC)  // same day, next hour
	t2 := time.Date(2026, 5, 8, 23, 59, 59, 0, time.UTC) // same day
	t3 := time.Date(2026, 5, 9, 1, 0, 0, 0, time.UTC)    // next day, same week
	t4 := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)    // next month

	for _, ts := range []time.Time{t0, t1, t2, t3, t4} {
		env := mustNewEnvelope(t, "t1", "g", "chora.sharing.post.created.v1", ts)
		// override IdempotencyKey to avoid collisions
		env.EventID = ts.Format(time.RFC3339Nano)
		env.IdempotencyKey = env.EventID
		if err := agg.Ingest(env); err != nil {
			t.Fatalf("Ingest: %v", err)
		}
	}

	tests := []struct {
		name   string
		period eventaggregator.Period
		at     time.Time
		want   int
	}{
		{"hour t0 isolated", eventaggregator.PeriodHour, t0, 1},
		{"day 5/8 has 3", eventaggregator.PeriodDay, t0, 3},
		{"day 5/9 has 1", eventaggregator.PeriodDay, t3, 1},
		{"month 5/2026 has 4", eventaggregator.PeriodMonth, t0, 4},
		{"month 6/2026 has 1", eventaggregator.PeriodMonth, t4, 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b, err := agg.Query(eventaggregator.QueryParams{TenantID: "t1", Period: tc.period, At: tc.at})
			if err != nil {
				t.Fatalf("Query: %v", err)
			}
			got := b.CountFor("sharing", "post", "created")
			if got != tc.want {
				t.Errorf("count=%d want %d", got, tc.want)
			}
		})
	}
}

func TestAggregator_QueryRejectsUnknownPeriod(t *testing.T) {
	agg := eventaggregator.NewAggregator()
	_, err := agg.Query(eventaggregator.QueryParams{TenantID: "t1", Period: eventaggregator.Period("decade"), At: time.Now().UTC()})
	if err == nil {
		t.Fatal("expected error for unknown period")
	}
}

func TestAggregator_IngestRejectsBlankTenant(t *testing.T) {
	agg := eventaggregator.NewAggregator()
	err := agg.Ingest(eventaggregator.Envelope{})
	if err == nil {
		t.Fatal("expected error ingesting zero-value envelope")
	}
}

func TestNewEnvelope_RejectsBlankEventID(t *testing.T) {
	_, err := eventaggregator.NewEnvelope(eventaggregator.EnvelopeParams{
		EventID: "", IdempotencyKey: "k", TenantID: "t",
		Topic: "chora.consumption.session.completed.v1",
	})
	if err == nil {
		t.Error("expected error for blank event_id")
	}
}

func TestNewEnvelope_RejectsBlankIdempotencyKey(t *testing.T) {
	_, err := eventaggregator.NewEnvelope(eventaggregator.EnvelopeParams{
		EventID: "id", IdempotencyKey: "", TenantID: "t",
		Topic: "chora.consumption.session.completed.v1",
	})
	if err == nil {
		t.Error("expected error for blank idempotency_key")
	}
}

func TestNewEnvelope_RejectsBadVersion(t *testing.T) {
	cases := []string{
		"chora.consumption.session.completed.x1", // bad prefix
		"chora.consumption.session.completed.v0", // n < 1
		"chora.consumption.session.completed.vX", // not numeric
		"chora.consumption.session..v1",          // empty event_type
		"chora.unknown_domain.x.y.v1",            // bad domain
		"chora.consumption.session.completed",    // wrong segment count
	}
	for _, topic := range cases {
		_, err := eventaggregator.NewEnvelope(eventaggregator.EnvelopeParams{
			EventID: "id", IdempotencyKey: "id", TenantID: "t", Topic: topic,
		})
		if err == nil {
			t.Errorf("topic %q should be rejected", topic)
		}
	}
}

func TestAggregator_IngestRejectsTopiclessEnvelope(t *testing.T) {
	agg := eventaggregator.NewAggregator()
	err := agg.Ingest(eventaggregator.Envelope{
		IdempotencyKey: "k",
		TenantID:       "t1",
	})
	if err == nil {
		t.Error("expected error: missing parsed topic segments")
	}
}

func TestAggregator_QueryRejectsBlankTenant(t *testing.T) {
	agg := eventaggregator.NewAggregator()
	if _, err := agg.Query(eventaggregator.QueryParams{TenantID: "", Period: eventaggregator.PeriodDay, At: time.Now()}); err == nil {
		t.Error("expected error blank tenant")
	}
}

func TestBucket_CountFor_NilCounts(t *testing.T) {
	b := eventaggregator.Bucket{}
	if b.CountFor("a", "b", "c") != 0 {
		t.Error("expected 0 from zero-value bucket")
	}
}

func TestAggregator_PeriodWeek(t *testing.T) {
	agg := eventaggregator.NewAggregator()
	// Wednesday + Sunday of the same week → same Mon-anchored week bucket.
	wed := time.Date(2026, 5, 6, 12, 0, 0, 0, time.UTC)
	sun := time.Date(2026, 5, 10, 23, 0, 0, 0, time.UTC)
	for i, ts := range []time.Time{wed, sun} {
		env, err := eventaggregator.NewEnvelope(eventaggregator.EnvelopeParams{
			EventID:        "01900000-0000-7000-8000-00000000000" + string(rune('0'+i)),
			IdempotencyKey: "01900000-0000-7000-8000-00000000000" + string(rune('0'+i)),
			TenantID:       "t1",
			Topic:          "chora.creation.atom.published.v1",
			OccurredAt:     ts,
			PublishedAt:    ts,
		})
		if err != nil {
			t.Fatalf("NewEnvelope: %v", err)
		}
		if err := agg.Ingest(env); err != nil {
			t.Fatalf("Ingest: %v", err)
		}
	}
	b, err := agg.Query(eventaggregator.QueryParams{TenantID: "t1", Period: eventaggregator.PeriodWeek, At: wed})
	if err != nil {
		t.Fatal(err)
	}
	if b.CountFor("creation", "atom", "published") != 2 {
		t.Errorf("week count=%d want 2", b.CountFor("creation", "atom", "published"))
	}
}
