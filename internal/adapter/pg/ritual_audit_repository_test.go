// ritual_audit_repository_test.go — pgx adapter tests for
// pg.RitualAuditRepository using the shared stub Querier
// (tenantTxStub / capturedExec, defined in familiar_growth_repository_test.go).
//
// Verifies:
//   - Ingest runs inside WithTenantTx with the row's tenant_id (so the
//     migration-0013 RLS policy — re-keyed to the canonical chora.tenant_id by
//     0015_rls_guc_rekey — passes. (This said app.current_tenant_id until
//     CHO-2140 completion; the stale claim outlived the policy it described.)
//   - The INSERT targets ritual_run_audit.
//   - A UNIQUE(source_event_id) collision maps to ra.ErrDuplicateSourceEvent.
//   - Validation rejects blank required fields (run_id).
package pg_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/apollo-chora/chora-observability/internal/adapter/pg"
	ra "github.com/apollo-chora/chora-observability/internal/domain/ritualaudit"
)

func goodRitualRow() ra.RitualRunAuditRow {
	return ra.RitualRunAuditRow{
		AuditID:       uuid.NewString(),
		TenantID:      uuid.NewString(),
		SourceTopic:   ra.TopicFamiliarRitualRunCompleted,
		SourceEventID: uuid.NewString(),
		RunID:         uuid.NewString(),
		RitualID:      uuid.NewString(),
		FamiliarID:    uuid.NewString(),
		OwnerGCID:     uuid.NewString(),
		RevisionNo:    3,
		TriggerSource: "manual",
		Status:        "completed",
		ManaCharged:   20,
		SinkRef:       "atom://draft/1",
		Stamps:        []map[string]any{{"step_index": float64(0), "skill_key": "explain"}},
		OccurredAt:    time.Date(2026, 7, 8, 11, 0, 0, 0, time.UTC),
	}
}

func TestRitualAuditRepository_Ingest_RunsInTenantTx(t *testing.T) {
	stub := &tenantTxStub{}
	repo := pg.NewRitualAuditRepository(stub)
	row := goodRitualRow()
	if err := repo.Ingest(context.Background(), row); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if stub.txN != 1 {
		t.Fatalf("expected WithTenantTx once; got %d", stub.txN)
	}
	if stub.tenantID != row.TenantID {
		t.Fatalf("tenant GUC: got %q want %q", stub.tenantID, row.TenantID)
	}
	if len(stub.execs) != 1 {
		t.Fatalf("expected 1 exec; got %d", len(stub.execs))
	}
	if !strings.Contains(stub.execs[0].sql, "INSERT INTO ritual_run_audit") {
		t.Fatalf("unexpected sql: %s", stub.execs[0].sql)
	}
}

func TestRitualAuditRepository_Ingest_UniqueViolation(t *testing.T) {
	stub := &tenantTxStub{execErr: errors.New(
		`ERROR: duplicate key value violates unique constraint "ritual_run_audit_source_event_id_key" (SQLSTATE 23505)`)}
	repo := pg.NewRitualAuditRepository(stub)
	if err := repo.Ingest(context.Background(), goodRitualRow()); !errors.Is(err, ra.ErrDuplicateSourceEvent) {
		t.Fatalf("expected ErrDuplicateSourceEvent; got %v", err)
	}
}

func TestRitualAuditRepository_Ingest_ValidatesRequired(t *testing.T) {
	repo := pg.NewRitualAuditRepository(&tenantTxStub{})
	bad := goodRitualRow()
	bad.RunID = ""
	if err := repo.Ingest(context.Background(), bad); !errors.Is(err, ra.ErrInvalidArgument) {
		t.Fatalf("expected ErrInvalidArgument for blank run_id; got %v", err)
	}
}
