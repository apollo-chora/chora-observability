// familiar_growth_repository_test.go — pgx adapter tests using a stub
// Querier. Verifies the ADR-167 Plane-4 wiring:
//   - Ingest runs inside WithTenantTx, so the migration-0007 RLS policy — which
//     0015_rls_guc_rekey re-keyed to the canonical `chora.tenant_id` — passes.
//     (This comment previously asserted the policy keyed on `app.current_tenant_id`.
//     That was true at 0007 and false from 0015 on; the stale claim is what kept
//     CHO-2140's half-done sweep invisible while every write 42501'd. The live
//     policy is now the authority — see rls_guc_coherence_test.go.)
//   - The ledger INSERT SQL + side-effect UPSERTs are emitted in order.
//   - A UNIQUE(source_event_id) collision maps to fg.ErrDuplicateSourceEvent.
//   - Validation rejects blank required fields.
package pg_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/apollo-chora/chora-observability/internal/adapter/pg"
	fg "github.com/apollo-chora/chora-observability/internal/domain/familiargrowth"
)

// tenantTxStub captures Exec calls + the tenant id passed to the canonical
// WithTenantTx seam. execErr (optional) is returned from every Exec to exercise
// error paths (e.g. a unique violation).
type tenantTxStub struct {
	execs    []capturedExec
	execErr  error
	tenantID string
	txN      int
}

type capturedExec struct {
	sql  string
	args []any
}

func (s *tenantTxStub) Exec(_ context.Context, sql string, args ...any) error {
	s.execs = append(s.execs, capturedExec{sql: sql, args: args})
	return s.execErr
}

func (s *tenantTxStub) QueryRow(_ context.Context, _ string, _ ...any) pg.Row {
	return &stubRow{err: pg.ErrNoRows}
}

func (s *tenantTxStub) Query(_ context.Context, _ string, _ ...any) (pg.Rows, error) {
	return nil, errors.New("tenantTxStub: Query not stubbed in this test")
}

func (s *tenantTxStub) WithTenantTx(ctx context.Context, tenantID string, fn func(context.Context, pg.TenantScopedQuerier) error) error {
	s.txN++
	s.tenantID = tenantID
	return fn(ctx, s)
}

func goodIngestReq() fg.IngestRequest {
	return fg.IngestRequest{
		Ledger: fg.AuditLedgerRow{
			AuditID:       uuid.NewString(),
			TenantID:      uuid.NewString(),
			SourceTopic:   fg.TopicExpAwarded,
			SourceEventID: uuid.NewString(),
			FamiliarID:    uuid.NewString(),
			OwnerGCID:     uuid.NewString(),
			EventType:     "exp_awarded",
			Payload:       map[string]any{"exp": 10},
			ReceivedAt:    time.Now().UTC(),
		},
		MetricsDelta: &fg.DailyMetricsDelta{
			DayBucket:       time.Now().UTC(),
			Source:          fg.TopicExpAwarded,
			ExpAwardedDelta: 10,
			EventCountDelta: 1,
		},
	}
}

func TestFamiliarGrowthRepository_Ingest_RunsInTenantTxAndInserts(t *testing.T) {
	t.Parallel()
	req := goodIngestReq()
	q := &tenantTxStub{}
	repo := pg.NewFamiliarGrowthRepository(q)

	if err := repo.Ingest(context.Background(), req); err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	if q.txN != 1 {
		t.Fatalf("WithTenantTx calls = %d; want 1 (atomic ingest)", q.txN)
	}
	if q.tenantID != req.Ledger.TenantID {
		t.Errorf("WithTenantTx tenant = %q; want %q", q.tenantID, req.Ledger.TenantID)
	}
	// Two execs: ledger insert + daily-metrics upsert (no breed/funnel here).
	if len(q.execs) != 2 {
		t.Fatalf("exec count = %d; want 2 (ledger + metrics)", len(q.execs))
	}
	if !contains(q.execs[0].sql, "INSERT INTO familiar_growth_audit_ledger") {
		t.Errorf("exec[0] = %q; want ledger insert", q.execs[0].sql)
	}
	if !contains(q.execs[1].sql, "INSERT INTO familiar_growth_daily_metrics") {
		t.Errorf("exec[1] = %q; want daily-metrics upsert", q.execs[1].sql)
	}
}

func TestFamiliarGrowthRepository_Ingest_BreedAndFunnel(t *testing.T) {
	t.Parallel()
	req := goodIngestReq()
	req.Ledger.SourceTopic = fg.TopicBreedRevealed
	req.MetricsDelta = nil
	req.BreedRoll = &fg.BreedRollAuditRow{
		FamiliarID:           uuid.NewString(),
		EggSKU:               "egg_basic",
		Species:              "dragon",
		Rarity:               "rare",
		RolledProbability:    12.5,
		DistributionSnapshot: map[string]any{"dragon": 0.1},
		RevealedAt:           time.Now().UTC(),
	}
	req.FunnelDelta = &fg.EggFunnelDelta{DayBucket: time.Now().UTC(), HatchedDelta: 1}

	q := &tenantTxStub{}
	repo := pg.NewFamiliarGrowthRepository(q)
	if err := repo.Ingest(context.Background(), req); err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	// ledger + breed_roll + egg_funnel = 3 execs.
	if len(q.execs) != 3 {
		t.Fatalf("exec count = %d; want 3 (ledger + breed + funnel)", len(q.execs))
	}
	if !contains(q.execs[1].sql, "INSERT INTO breed_roll_audit") {
		t.Errorf("exec[1] = %q; want breed_roll insert", q.execs[1].sql)
	}
	if !contains(q.execs[2].sql, "INSERT INTO egg_funnel_metrics") {
		t.Errorf("exec[2] = %q; want egg_funnel upsert", q.execs[2].sql)
	}
}

func TestFamiliarGrowthRepository_Ingest_DuplicateSourceEventMapped(t *testing.T) {
	t.Parallel()
	req := goodIngestReq()
	q := &tenantTxStub{execErr: errors.New(`ERROR: duplicate key value violates unique constraint "familiar_growth_audit_ledger_source_event_id_key" (SQLSTATE 23505)`)}
	repo := pg.NewFamiliarGrowthRepository(q)

	err := repo.Ingest(context.Background(), req)
	if !errors.Is(err, fg.ErrDuplicateSourceEvent) {
		t.Errorf("err = %v; want fg.ErrDuplicateSourceEvent", err)
	}
}

func TestFamiliarGrowthRepository_Ingest_RejectsBlankRequired(t *testing.T) {
	t.Parallel()
	q := &tenantTxStub{}
	repo := pg.NewFamiliarGrowthRepository(q)

	for name, mut := range map[string]func(*fg.IngestRequest){
		"blank audit_id":        func(r *fg.IngestRequest) { r.Ledger.AuditID = "" },
		"blank tenant_id":       func(r *fg.IngestRequest) { r.Ledger.TenantID = "" },
		"blank source_event_id": func(r *fg.IngestRequest) { r.Ledger.SourceEventID = "" },
	} {
		req := goodIngestReq()
		mut(&req)
		if err := repo.Ingest(context.Background(), req); !errors.Is(err, fg.ErrInvalidArgument) {
			t.Errorf("%s: err = %v; want fg.ErrInvalidArgument", name, err)
		}
	}
	if q.txN != 0 {
		t.Errorf("validation must reject BEFORE opening a tx; txN = %d", q.txN)
	}
}

// Compile-time: the pg repo satisfies the domain port.
func TestFamiliarGrowthRepository_SatisfiesPort(t *testing.T) {
	t.Parallel()
	var _ fg.Repository = pg.NewFamiliarGrowthRepository(&tenantTxStub{})
}
