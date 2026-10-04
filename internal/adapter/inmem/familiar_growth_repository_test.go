// Package inmem_test exercises the in-memory FamiliarGrowthRepository — the
// hermetic test/dev implementation of familiargrowth.Repository. Verifies
// the ingest side-effects (breed-roll, daily metrics, egg funnel), the
// idempotency guard, and the four list surfaces with their filters + paging.
package inmem_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/apollo-chora/chora-observability/internal/adapter/inmem"
	fg "github.com/apollo-chora/chora-observability/internal/domain/familiargrowth"
)

const (
	fgTenant  = "01970000-0000-7000-8000-000000000001"
	fgAuditID = "01970000-0000-7000-8000-000000000101"
)

func fgDay() time.Time { return time.Date(2026, 5, 10, 0, 0, 0, 0, time.UTC) }

func fgLedgerRow(auditID, tenantID, eventID string) fg.AuditLedgerRow {
	return fg.AuditLedgerRow{
		AuditID:       auditID,
		TenantID:      tenantID,
		SourceTopic:   fg.TopicExpAwarded,
		SourceEventID: eventID,
		FamiliarID:    "fam-1",
		OwnerGCID:     "gcid-1",
		EventType:     "exp_awarded",
		ReceivedAt:    fgDay(),
	}
}

func TestFamiliarGrowthRepository_IngestLedgerOnly(t *testing.T) {
	repo := inmem.NewFamiliarGrowthRepository()

	err := repo.Ingest(context.Background(), fg.IngestRequest{
		Ledger: fgLedgerRow(fgAuditID, fgTenant, "evt-1"),
	})
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	rows, err := repo.ListLedger(context.Background(), fgTenant, fg.AuditFilter{})
	if err != nil {
		t.Fatalf("ListLedger: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("ledger rows = %d; want 1", len(rows))
	}
	if rows[0].AuditID != fgAuditID || rows[0].EventType != "exp_awarded" {
		t.Errorf("row = %+v", rows[0])
	}

	// tenant isolation
	other, _ := repo.ListLedger(context.Background(), "other-tenant", fg.AuditFilter{})
	if len(other) != 0 {
		t.Errorf("other tenant saw %d rows", len(other))
	}
}

func TestFamiliarGrowthRepository_IngestWithBreedRoll(t *testing.T) {
	repo := inmem.NewFamiliarGrowthRepository()

	err := repo.Ingest(context.Background(), fg.IngestRequest{
		Ledger:    fgLedgerRow(fgAuditID, fgTenant, "evt-2"),
		BreedRoll: &fg.BreedRollAuditRow{TenantID: fgTenant, FamiliarID: "fam-1", RevealedAt: fgDay()},
	})
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	rolls, err := repo.ListBreedRolls(context.Background(), fgTenant, fg.AuditFilter{})
	if err != nil {
		t.Fatalf("ListBreedRolls: %v", err)
	}
	if len(rolls) != 1 {
		t.Fatalf("breed rolls = %d; want 1", len(rolls))
	}
	// blank AuditID on the roll inherits the ledger audit_id
	if rolls[0].AuditID != fgAuditID {
		t.Errorf("roll audit id = %q; want %q", rolls[0].AuditID, fgAuditID)
	}
	if rolls[0].TenantID != fgTenant {
		t.Errorf("roll tenant = %q; want %q", rolls[0].TenantID, fgTenant)
	}
}

func TestFamiliarGrowthRepository_IngestMetricsDelta(t *testing.T) {
	repo := inmem.NewFamiliarGrowthRepository()

	req := func(eventID string) fg.IngestRequest {
		return fg.IngestRequest{
			Ledger: fgLedgerRow(fgAuditID, fgTenant, eventID),
			MetricsDelta: &fg.DailyMetricsDelta{
				DayBucket: fgDay(), Source: fg.TopicExpAwarded,
				ExpAwardedDelta: 100, EventCountDelta: 1, StageUpsDelta: 2,
			},
		}
	}
	if err := repo.Ingest(context.Background(), req("evt-3a")); err != nil {
		t.Fatalf("Ingest #1: %v", err)
	}
	// second ingest with same tenant/day/source drives the existing-row branch
	if err := repo.Ingest(context.Background(), req("evt-3b")); err != nil {
		t.Fatalf("Ingest #2: %v", err)
	}

	metrics, err := repo.ListMetrics(context.Background(), fgTenant, fg.AuditFilter{})
	if err != nil {
		t.Fatalf("ListMetrics: %v", err)
	}
	if len(metrics) != 1 {
		t.Fatalf("metric rows = %d; want 1", len(metrics))
	}
	if metrics[0].TotalExpAwarded != 200 {
		t.Errorf("exp awarded = %d; want 200 (rolled up twice)", metrics[0].TotalExpAwarded)
	}
	if metrics[0].EventCount != 2 || metrics[0].StageUpsCount != 4 {
		t.Errorf("counts = (%d, %d); want (2, 4)", metrics[0].EventCount, metrics[0].StageUpsCount)
	}
}

func TestFamiliarGrowthRepository_IngestFunnelDelta(t *testing.T) {
	repo := inmem.NewFamiliarGrowthRepository()

	req := func(eventID string) fg.IngestRequest {
		return fg.IngestRequest{
			Ledger: fgLedgerRow(fgAuditID, fgTenant, eventID),
			FunnelDelta: &fg.EggFunnelDelta{
				DayBucket: fgDay(), PurchasedDelta: 2, HatchedDelta: 1,
			},
		}
	}
	if err := repo.Ingest(context.Background(), req("evt-4a")); err != nil {
		t.Fatalf("Ingest #1: %v", err)
	}
	if err := repo.Ingest(context.Background(), req("evt-4b")); err != nil {
		t.Fatalf("Ingest #2: %v", err)
	}

	funnel, err := repo.ListEggFunnel(context.Background(), fgTenant, fg.AuditFilter{})
	if err != nil {
		t.Fatalf("ListEggFunnel: %v", err)
	}
	if len(funnel) != 1 {
		t.Fatalf("funnel rows = %d; want 1", len(funnel))
	}
	if funnel[0].EggsPurchased != 4 || funnel[0].EggsHatched != 2 {
		t.Errorf("funnel = (%d, %d); want (4, 2)", funnel[0].EggsPurchased, funnel[0].EggsHatched)
	}
}

func TestFamiliarGrowthRepository_IngestValidationAndIdempotency(t *testing.T) {
	repo := inmem.NewFamiliarGrowthRepository()

	bad := fgLedgerRow(fgAuditID, fgTenant, "evt-5")
	bad.AuditID = ""
	if err := repo.Ingest(context.Background(), fg.IngestRequest{Ledger: bad}); !errors.Is(err, fg.ErrInvalidArgument) {
		t.Fatalf("blank audit_id err = %v; want ErrInvalidArgument", err)
	}
	bad = fgLedgerRow(fgAuditID, "", "evt-5")
	if err := repo.Ingest(context.Background(), fg.IngestRequest{Ledger: bad}); !errors.Is(err, fg.ErrInvalidArgument) {
		t.Fatalf("blank tenant err = %v; want ErrInvalidArgument", err)
	}
	bad = fgLedgerRow(fgAuditID, fgTenant, "")
	if err := repo.Ingest(context.Background(), fg.IngestRequest{Ledger: bad}); !errors.Is(err, fg.ErrInvalidArgument) {
		t.Fatalf("blank source_event_id err = %v; want ErrInvalidArgument", err)
	}

	if err := repo.Ingest(context.Background(), fg.IngestRequest{
		Ledger: fgLedgerRow(fgAuditID, fgTenant, "evt-5"),
	}); err != nil {
		t.Fatalf("first ingest: %v", err)
	}
	if err := repo.Ingest(context.Background(), fg.IngestRequest{
		Ledger: fgLedgerRow("other-id", fgTenant, "evt-5"),
	}); !errors.Is(err, fg.ErrDuplicateSourceEvent) {
		t.Fatalf("duplicate err = %v; want ErrDuplicateSourceEvent", err)
	}
}

func TestFamiliarGrowthRepository_ListLedgerFiltersAndPaging(t *testing.T) {
	repo := inmem.NewFamiliarGrowthRepository()

	base := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	eventIDs := []string{"evt-a", "evt-b", "evt-c", "evt-d"}
	for i, eventID := range eventIDs {
		_ = repo.Ingest(context.Background(), fg.IngestRequest{
			Ledger: fg.AuditLedgerRow{
				AuditID:       "aid-" + eventID,
				TenantID:      fgTenant,
				SourceTopic:   fg.TopicStageUp,
				SourceEventID: eventID,
				FamiliarID:    "fam-1",
				EventType:     "stage_up",
				ReceivedAt:    base.Add(time.Duration(i) * time.Hour),
			},
		})
	}

	// window filter excludes the first (base) and last (base+3h) rows
	rows, err := repo.ListLedger(context.Background(), fgTenant, fg.AuditFilter{
		From:       base.Add(30 * time.Minute),
		To:         base.Add(2*time.Hour + 30*time.Minute),
		FamiliarID: "fam-1",
	})
	if err != nil {
		t.Fatalf("ListLedger: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("window rows = %d; want 2", len(rows))
	}
	// newest-first ordering
	if rows[0].ReceivedAt.Before(rows[1].ReceivedAt) {
		t.Error("expected received_at DESC ordering")
	}

	// source filter excludes everything (rows use TopicStageUp)
	filtered, _ := repo.ListLedger(context.Background(), fgTenant, fg.AuditFilter{Source: fg.TopicExpAwarded})
	if len(filtered) != 0 {
		t.Errorf("source-filtered rows = %d; want 0", len(filtered))
	}

	// familiar filter that excludes all rows
	noFam, _ := repo.ListLedger(context.Background(), fgTenant, fg.AuditFilter{FamiliarID: "fam-other"})
	if len(noFam) != 0 {
		t.Errorf("non-matching familiar rows = %d; want 0", len(noFam))
	}

	// familiar filter
	byFam, _ := repo.ListLedger(context.Background(), fgTenant, fg.AuditFilter{FamiliarID: "fam-1"})
	if len(byFam) != 4 {
		t.Errorf("familiar-filtered rows = %d; want 4", len(byFam))
	}

	// paging: offset into the list + limit
	page, _ := repo.ListLedger(context.Background(), fgTenant, fg.AuditFilter{Limit: 2, Offset: 1})
	if len(page) != 2 {
		t.Errorf("paged rows = %d; want 2", len(page))
	}
	// offset beyond list -> empty
	beyond, _ := repo.ListLedger(context.Background(), fgTenant, fg.AuditFilter{Offset: 99})
	if len(beyond) != 0 {
		t.Errorf("beyond rows = %d; want 0", len(beyond))
	}
}

func TestFamiliarGrowthRepository_ListMetricsSortAndFilters(t *testing.T) {
	repo := inmem.NewFamiliarGrowthRepository()

	day1 := time.Date(2026, 5, 10, 0, 0, 0, 0, time.UTC)
	day2 := time.Date(2026, 5, 11, 0, 0, 0, 0, time.UTC)
	ingest := func(day time.Time, source, eventID string) {
		_ = repo.Ingest(context.Background(), fg.IngestRequest{
			Ledger: fg.AuditLedgerRow{
				AuditID: "aid-" + eventID, TenantID: fgTenant,
				SourceTopic: source, SourceEventID: eventID,
				ReceivedAt: day,
			},
			MetricsDelta: &fg.DailyMetricsDelta{DayBucket: day, Source: source, EventCountDelta: 1},
		})
	}
	ingest(day2, fg.TopicExpAwarded, "evt-m1")
	ingest(day1, fg.TopicExpAwarded, "evt-m2")
	ingest(day1, fg.TopicStageUp, "evt-m3") // same day as m2, different source

	// deterministic sort: day DESC, then source ASC
	metrics, err := repo.ListMetrics(context.Background(), fgTenant, fg.AuditFilter{})
	if err != nil {
		t.Fatalf("ListMetrics: %v", err)
	}
	if len(metrics) != 3 {
		t.Fatalf("metric rows = %d; want 3", len(metrics))
	}
	if !metrics[0].DayBucket.Equal(day2) {
		t.Errorf("first row day = %v; want day2", metrics[0].DayBucket)
	}
	lastPair := metrics[1:]
	if lastPair[0].DayBucket.Equal(lastPair[1].DayBucket) && lastPair[0].Source >= lastPair[1].Source {
		t.Errorf("expected same-day rows sorted by source ASC: %v vs %v", lastPair[0].Source, lastPair[1].Source)
	}

	// window filter: only day1 rows survive
	day1Only, _ := repo.ListMetrics(context.Background(), fgTenant, fg.AuditFilter{
		From: day1.Add(-time.Minute), To: day2.Add(-time.Minute),
	})
	if len(day1Only) != 2 {
		t.Errorf("day1 window rows = %d; want 2", len(day1Only))
	}

	sourceOnly, _ := repo.ListMetrics(context.Background(), fgTenant, fg.AuditFilter{Source: fg.TopicStageUp})
	if len(sourceOnly) != 1 || sourceOnly[0].Source != fg.TopicStageUp {
		t.Errorf("source-filtered rows = %+v; want exactly TopicStageUp", sourceOnly)
	}
}

func TestFamiliarGrowthRepository_ListBreedRollsFilters(t *testing.T) {
	repo := inmem.NewFamiliarGrowthRepository()

	base := time.Date(2026, 5, 10, 0, 0, 0, 0, time.UTC)
	for i, sku := range []string{"egg-a", "egg-b"} {
		_ = repo.Ingest(context.Background(), fg.IngestRequest{
			Ledger: fg.AuditLedgerRow{
				AuditID: "roll-" + sku, TenantID: fgTenant,
				SourceTopic: fg.TopicBreedRevealed, SourceEventID: "evt-" + sku,
				ReceivedAt: base.Add(time.Duration(i) * time.Hour),
			},
			BreedRoll: &fg.BreedRollAuditRow{
				AuditID: "roll-" + sku, TenantID: fgTenant,
				FamiliarID: "fam-1", EggSKU: sku, RevealedAt: base.Add(time.Duration(i) * time.Hour),
			},
		})
	}

	bySKU, err := repo.ListBreedRolls(context.Background(), fgTenant, fg.AuditFilter{EggSKU: "egg-b"})
	if err != nil {
		t.Fatalf("ListBreedRolls: %v", err)
	}
	if len(bySKU) != 1 || bySKU[0].EggSKU != "egg-b" {
		t.Errorf("sku-filtered rolls = %+v; want egg-b only", bySKU)
	}

	windowed, _ := repo.ListBreedRolls(context.Background(), fgTenant, fg.AuditFilter{
		From: base.Add(30 * time.Minute), To: base.Add(90 * time.Minute),
	})
	if len(windowed) != 1 {
		t.Errorf("windowed rolls = %d; want 1", len(windowed))
	}
}

func TestFamiliarGrowthRepository_ListEggFunnelWindow(t *testing.T) {
	repo := inmem.NewFamiliarGrowthRepository()

	day1 := time.Date(2026, 5, 10, 0, 0, 0, 0, time.UTC)
	day2 := time.Date(2026, 5, 11, 0, 0, 0, 0, time.UTC)
	for _, day := range []time.Time{day1, day2} {
		_ = repo.Ingest(context.Background(), fg.IngestRequest{
			Ledger: fg.AuditLedgerRow{
				AuditID: "funnel-" + day.Format("20060102"), TenantID: fgTenant,
				SourceTopic: fg.TopicEggPurchased, SourceEventID: "evt-" + day.Format("20060102"),
				ReceivedAt: day,
			},
			FunnelDelta: &fg.EggFunnelDelta{DayBucket: day, PurchasedDelta: 1},
		})
	}

	funnel, err := repo.ListEggFunnel(context.Background(), fgTenant, fg.AuditFilter{})
	if err != nil {
		t.Fatalf("ListEggFunnel: %v", err)
	}
	if len(funnel) != 2 {
		t.Fatalf("funnel rows = %d; want 2", len(funnel))
	}
	if !funnel[0].DayBucket.After(funnel[1].DayBucket) {
		t.Error("expected day DESC ordering")
	}

	day1Only, _ := repo.ListEggFunnel(context.Background(), fgTenant, fg.AuditFilter{
		From: day1.Add(-time.Minute), To: day2.Add(-time.Minute),
	})
	if len(day1Only) != 1 {
		t.Errorf("day1 window rows = %d; want 1", len(day1Only))
	}
}
