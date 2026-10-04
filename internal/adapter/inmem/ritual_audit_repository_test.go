// Package inmem_test exercises the in-memory RitualAuditRepository — the
// hermetic implementation of ritualaudit.Repository. Verifies ingest
// validation + idempotency and the tenant-scoped DESC list with limit cap.
package inmem_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/5007-Capstone/chora/services/chora-observability/internal/adapter/inmem"
	ra "github.com/5007-Capstone/chora/services/chora-observability/internal/domain/ritualaudit"
)

func TestRitualAuditRepository_IngestAndList(t *testing.T) {
	repo := inmem.NewRitualAuditRepository()

	base := time.Date(2026, 5, 10, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 3; i++ {
		row := ra.RitualRunAuditRow{
			AuditID:       "audit-" + string(rune('a'+i)),
			TenantID:      "01970000-0000-7000-8000-000000000001",
			SourceTopic:   ra.TopicFamiliarRitualRunCompleted,
			SourceEventID: "evt-" + string(rune('a'+i)),
			RunID:         "run-1",
			RitualID:      "rit-1",
			Status:        "completed",
			RevisionNo:    3,
			OccurredAt:    base.Add(time.Duration(i) * time.Hour),
			ReceivedAt:    base.Add(time.Duration(i) * time.Hour),
		}
		if err := repo.Ingest(context.Background(), row); err != nil {
			t.Fatalf("Ingest #%d: %v", i, err)
		}
	}

	rows, err := repo.List(context.Background(), "01970000-0000-7000-8000-000000000001", 10)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("rows = %d; want 3", len(rows))
	}
	// received_at DESC
	for i := 1; i < len(rows); i++ {
		if rows[i-1].ReceivedAt.Before(rows[i].ReceivedAt) {
			t.Fatalf("expected DESC ordering at %d", i)
		}
	}
	if rows[0].AuditID != "audit-c" {
		t.Errorf("most recent audit = %q; want audit-c", rows[0].AuditID)
	}
}

func TestRitualAuditRepository_ListLimitCap(t *testing.T) {
	repo := inmem.NewRitualAuditRepository()

	for i := 0; i < 5; i++ {
		_ = repo.Ingest(context.Background(), ra.RitualRunAuditRow{
			AuditID:       "audit-" + string(rune('a'+i)),
			TenantID:      "01970000-0000-7000-8000-000000000001",
			SourceTopic:   ra.TopicFamiliarRitualRunCompleted,
			SourceEventID: "evt-" + string(rune('a'+i)),
			RunID:         "run-2",
		})
	}

	capped, _ := repo.List(context.Background(), "01970000-0000-7000-8000-000000000001", 2)
	if len(capped) != 2 {
		t.Errorf("capped rows = %d; want 2", len(capped))
	}

	// limit <= 0 returns all
	all, _ := repo.List(context.Background(), "01970000-0000-7000-8000-000000000001", 0)
	if len(all) != 5 {
		t.Errorf("uncapped rows = %d; want 5", len(all))
	}

	// tenant isolation
	other, _ := repo.List(context.Background(), "other-tenant", 0)
	if len(other) != 0 {
		t.Errorf("other tenant saw %d rows", len(other))
	}
}

func TestRitualAuditRepository_IngestValidation(t *testing.T) {
	repo := inmem.NewRitualAuditRepository()

	mk := func() ra.RitualRunAuditRow {
		return ra.RitualRunAuditRow{
			AuditID:       "audit-1",
			TenantID:      "01970000-0000-7000-8000-000000000001",
			SourceTopic:   ra.TopicFamiliarRitualRunCompleted,
			SourceEventID: "evt-1",
			RunID:         "run-1",
		}
	}

	bad := mk()
	bad.AuditID = ""
	if err := repo.Ingest(context.Background(), bad); !errors.Is(err, ra.ErrInvalidArgument) {
		t.Fatalf("blank audit_id err = %v; want ErrInvalidArgument", err)
	}
	bad = mk()
	bad.TenantID = ""
	if err := repo.Ingest(context.Background(), bad); !errors.Is(err, ra.ErrInvalidArgument) {
		t.Fatalf("blank tenant err = %v; want ErrInvalidArgument", err)
	}
	bad = mk()
	bad.SourceEventID = ""
	if err := repo.Ingest(context.Background(), bad); !errors.Is(err, ra.ErrInvalidArgument) {
		t.Fatalf("blank source_event_id err = %v; want ErrInvalidArgument", err)
	}
	bad = mk()
	bad.RunID = ""
	if err := repo.Ingest(context.Background(), bad); !errors.Is(err, ra.ErrInvalidArgument) {
		t.Fatalf("blank run_id err = %v; want ErrInvalidArgument", err)
	}

	if err := repo.Ingest(context.Background(), mk()); err != nil {
		t.Fatalf("valid ingest: %v", err)
	}
	dup := mk()
	dup.AuditID = "audit-2" // same source_event_id, different audit_id
	if err := repo.Ingest(context.Background(), dup); !errors.Is(err, ra.ErrDuplicateSourceEvent) {
		t.Fatalf("duplicate err = %v; want ErrDuplicateSourceEvent", err)
	}

	// round-trip the default stamping + stale defaults on a fresh repo
	repo2 := inmem.NewRitualAuditRepository()
	zeroRow := ra.RitualRunAuditRow{
		AuditID: "audit-3", TenantID: "01970000-0000-7000-8000-000000000001",
		SourceTopic: ra.TopicFamiliarRitualRunCompleted, SourceEventID: "evt-3", RunID: "run-3",
	}
	if err := repo2.Ingest(context.Background(), zeroRow); err != nil {
		t.Fatalf("zero-value ingest: %v", err)
	}
	stored, _ := repo2.List(context.Background(), "01970000-0000-7000-8000-000000000001", 1)
	if len(stored) != 1 {
		t.Fatalf("stored rows = %d; want 1", len(stored))
	}
	if stored[0].ReceivedAt.IsZero() {
		t.Error("expected ReceivedAt to be defaulted on ingest")
	}
	if stored[0].Stamps == nil {
		t.Error("expected Stamps slice to be defaulted")
	}
}