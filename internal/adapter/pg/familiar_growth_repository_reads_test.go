// familiar_growth_repository_reads_test.go — read-path tests for the ADR-149
// audit dashboards (CHO-2140 completion).
//
// Why these exist
// ---------------
// The 4 O+ IMDA D1/D2 dashboard reads had ZERO test coverage while the write
// path had plenty. That asymmetry matters more than it looks: a write against a
// mismatched RLS GUC fails LOUDLY with 42501, but a READ against a mismatched
// GUC returns ZERO ROWS AND NO ERROR. The auditor's dashboard would render an
// empty ledger, forever, and nothing anywhere would report a fault.
//
// So the load-bearing assertion in each test below is not the SQL text — it is
// that the read runs inside WithTenantTx (the seam that sets the canonical
// `chora.tenant_id` GUC the live policies read) and carries the CALLER'S tenant,
// never a parameter-only filter. A `WHERE tenant_id = $1` that is not backed by
// the GUC would still return rows for the owner role and silently return none
// for the app role.
package pg_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/5007-Capstone/chora/services/chora-observability/internal/adapter/pg"
	fg "github.com/5007-Capstone/chora/services/chora-observability/internal/domain/familiargrowth"
)

const readTenant = "11111111-1111-7111-8111-111111111111"

// stubRows serves a fixed number of rows, delegating each Scan to a per-row func.
type stubRows struct {
	n      int
	i      int
	scan   func(i int, dest ...any) error
	closed bool
	err    error
}

func (r *stubRows) Next() bool {
	if r.i >= r.n {
		return false
	}
	r.i++
	return true
}

func (r *stubRows) Scan(dest ...any) error {
	if r.scan != nil {
		return r.scan(r.i-1, dest...)
	}
	return nil
}

func (r *stubRows) Close()     { r.closed = true }
func (r *stubRows) Err() error { return r.err }

// readStub is a Querier whose Query returns canned Rows and which records the
// tenant handed to WithTenantTx.
type readStub struct {
	rows     *stubRows
	queryErr error
	tenantID string
	txN      int
	lastSQL  string
	lastArgs []any
}

func (s *readStub) Exec(context.Context, string, ...any) error { return nil }

func (s *readStub) QueryRow(context.Context, string, ...any) pg.Row {
	return &stubRow{err: pg.ErrNoRows}
}

func (s *readStub) Query(_ context.Context, sql string, args ...any) (pg.Rows, error) {
	s.lastSQL, s.lastArgs = sql, args
	if s.queryErr != nil {
		return nil, s.queryErr
	}
	return s.rows, nil
}

func (s *readStub) WithTenantTx(ctx context.Context, tenantID string, fn func(context.Context, pg.TenantScopedQuerier) error) error {
	s.txN++
	s.tenantID = tenantID
	return fn(ctx, s)
}

// assertCanonicalTenantSeam is the shared, load-bearing check.
func assertCanonicalTenantSeam(t *testing.T, s *readStub) {
	t.Helper()
	if s.txN != 1 {
		t.Fatalf("read must run inside exactly 1 WithTenantTx (the canonical chora.tenant_id seam); got %d", s.txN)
	}
	if s.tenantID != readTenant {
		t.Fatalf("WithTenantTx tenant = %q, want the caller's tenant %q — the GUC must come from the caller, and a read under the wrong GUC returns EMPTY with no error", s.tenantID, readTenant)
	}
}

func TestFamiliarGrowthRepository_ListLedger_ReadsInCanonicalTenantTx(t *testing.T) {
	t.Parallel()
	s := &readStub{rows: &stubRows{n: 1, scan: func(_ int, dest ...any) error {
		*(dest[0].(*string)) = "aaaaaaaa-0000-7000-8000-000000000001" // audit_id
		*(dest[1].(*string)) = readTenant                             // tenant_id
		*(dest[2].(*string)) = fg.TopicExpAwarded                     // source_topic
		*(dest[3].(*string)) = "bbbbbbbb-0000-7000-8000-000000000001"
		*(dest[4].(*string)) = "cccccccc-0000-7000-8000-000000000001"
		*(dest[5].(*string)) = "dddddddd-0000-7000-8000-000000000001"
		*(dest[6].(*string)) = "exp_awarded"
		*(dest[7].(*string)) = `{"exp":10}`
		*(dest[8].(*time.Time)) = time.Date(2026, 7, 17, 10, 0, 0, 0, time.UTC)
		return nil
	}}}
	repo := pg.NewFamiliarGrowthRepository(s)

	rows, err := repo.ListLedger(context.Background(), readTenant, fg.AuditFilter{Limit: 50})
	if err != nil {
		t.Fatalf("ListLedger: %v", err)
	}
	assertCanonicalTenantSeam(t, s)
	if len(rows) != 1 {
		t.Fatalf("expected 1 ledger row, got %d", len(rows))
	}
	if rows[0].TenantID != readTenant || rows[0].EventType != "exp_awarded" {
		t.Errorf("row scanned wrong: %+v", rows[0])
	}
	if rows[0].Payload["exp"] != float64(10) {
		t.Errorf("payload JSON must decode into the map; got %#v", rows[0].Payload)
	}
	if !s.rows.closed {
		t.Error("Rows must be closed before the tx returns")
	}
}

func TestFamiliarGrowthRepository_ListMetrics_ReadsInCanonicalTenantTx(t *testing.T) {
	t.Parallel()
	s := &readStub{rows: &stubRows{n: 1, scan: func(_ int, dest ...any) error {
		*(dest[0].(*string)) = readTenant
		*(dest[1].(*time.Time)) = time.Date(2026, 7, 17, 0, 0, 0, 0, time.UTC)
		*(dest[2].(*string)) = "atom_session"
		*(dest[3].(*int64)) = 40
		*(dest[4].(*int64)) = 2
		*(dest[5].(*int)) = 1
		*(dest[6].(*time.Time)) = time.Date(2026, 7, 17, 10, 0, 0, 0, time.UTC)
		return nil
	}}}
	repo := pg.NewFamiliarGrowthRepository(s)

	rows, err := repo.ListMetrics(context.Background(), readTenant, fg.AuditFilter{Limit: 50})
	if err != nil {
		t.Fatalf("ListMetrics: %v", err)
	}
	assertCanonicalTenantSeam(t, s)
	if len(rows) != 1 || rows[0].TotalExpAwarded != 40 || rows[0].StageUpsCount != 1 {
		t.Fatalf("metrics row scanned wrong: %+v", rows)
	}
}

func TestFamiliarGrowthRepository_ListBreedRolls_ReadsInCanonicalTenantTx(t *testing.T) {
	t.Parallel()
	s := &readStub{rows: &stubRows{n: 1, scan: func(_ int, dest ...any) error {
		*(dest[0].(*string)) = "aaaaaaaa-0000-7000-8000-000000000002"
		*(dest[1].(*string)) = readTenant
		*(dest[2].(*string)) = "cccccccc-0000-7000-8000-000000000002"
		*(dest[3].(*string)) = "dddddddd-0000-7000-8000-000000000002"
		*(dest[4].(*string)) = "egg_standard_v1"
		*(dest[5].(*string)) = "FAMILIAR_SPECIES_FOX"
		*(dest[6].(*bool)) = true
		*(dest[7].(*string)) = "legendary"
		*(dest[8].(*float64)) = 0.0125
		*(dest[9].(*string)) = `{"fox":0.0125}`
		*(dest[10].(*time.Time)) = time.Date(2026, 7, 17, 10, 0, 0, 0, time.UTC)
		return nil
	}}}
	repo := pg.NewFamiliarGrowthRepository(s)

	rows, err := repo.ListBreedRolls(context.Background(), readTenant, fg.AuditFilter{Limit: 50})
	if err != nil {
		t.Fatalf("ListBreedRolls: %v", err)
	}
	assertCanonicalTenantSeam(t, s)
	// IMDA D2 transparency: the rolled probability + shiny flag are the auditable
	// record of the lootbox roll.
	if len(rows) != 1 || rows[0].RolledProbability != 0.0125 || !rows[0].Shiny {
		t.Fatalf("breed-roll row scanned wrong: %+v", rows)
	}
}

func TestFamiliarGrowthRepository_ListEggFunnel_ReadsInCanonicalTenantTx(t *testing.T) {
	t.Parallel()
	s := &readStub{rows: &stubRows{n: 1, scan: func(_ int, dest ...any) error {
		*(dest[0].(*string)) = readTenant
		*(dest[1].(*time.Time)) = time.Date(2026, 7, 17, 0, 0, 0, 0, time.UTC)
		*(dest[2].(*int)) = 2
		*(dest[3].(*int)) = 1
		*(dest[4].(*int)) = 0
		*(dest[5].(*time.Time)) = time.Date(2026, 7, 17, 10, 0, 0, 0, time.UTC)
		return nil
	}}}
	repo := pg.NewFamiliarGrowthRepository(s)

	rows, err := repo.ListEggFunnel(context.Background(), readTenant, fg.AuditFilter{Limit: 50})
	if err != nil {
		t.Fatalf("ListEggFunnel: %v", err)
	}
	assertCanonicalTenantSeam(t, s)
	if len(rows) != 1 || rows[0].EggsPurchased != 2 || rows[0].EggsHatched != 1 {
		t.Fatalf("funnel row scanned wrong: %+v", rows)
	}
}

// A query failure must propagate — an empty result and an error are very
// different things to an auditor, and this lane has already shipped one silent
// empty.
func TestFamiliarGrowthRepository_ListLedger_QueryErrorPropagates(t *testing.T) {
	t.Parallel()
	want := errors.New("connection reset")
	s := &readStub{queryErr: want}
	repo := pg.NewFamiliarGrowthRepository(s)

	_, err := repo.ListLedger(context.Background(), readTenant, fg.AuditFilter{Limit: 10})
	if err == nil {
		t.Fatal("a query failure must surface as an error, never as an empty ledger")
	}
	if !errors.Is(err, want) {
		t.Errorf("underlying error must be wrapped, not replaced: %v", err)
	}
}

// Rows.Err() must be checked — pgx reports mid-iteration failures there, and
// ignoring it truncates the auditor's page into a silently-short answer.
func TestFamiliarGrowthRepository_ListLedger_RowsErrPropagates(t *testing.T) {
	t.Parallel()
	want := errors.New("row stream broke")
	s := &readStub{rows: &stubRows{n: 0, err: want}}
	repo := pg.NewFamiliarGrowthRepository(s)

	_, err := repo.ListLedger(context.Background(), readTenant, fg.AuditFilter{Limit: 10})
	if err == nil {
		t.Fatal("a mid-stream Rows.Err() must surface, never be silently truncated to an empty page")
	}
}

// --- ritual_run_audit read path ---------------------------------------------

// TestRitualAuditRepository_List_ReadsInCanonicalTenantTx covers the ADR-215 O+
// auditor read. This is the table that made CHO-2140 hard to see: it holds 3
// rows written on 2026-07-11, BEFORE 0015 re-keyed its policy, so it looked
// alive while every write since had been refused with 42501. Its read is subject
// to the same policy — under a mismatched GUC it returns an empty page and no
// error.
func TestRitualAuditRepository_List_ReadsInCanonicalTenantTx(t *testing.T) {
	t.Parallel()
	s := &readStub{rows: &stubRows{n: 1, scan: func(_ int, dest ...any) error {
		*(dest[0].(*string)) = "019f500a-a913-7169-bca9-ffdc5482f1a9"
		*(dest[1].(*string)) = readTenant
		*(dest[2].(*string)) = "chora.consumption.familiar.ritual_run_completed.v1"
		*(dest[3].(*string)) = "bbbbbbbb-0000-7000-8000-000000000009"
		*(dest[4].(*string)) = "run-1"
		*(dest[5].(*string)) = "ritual-1"
		*(dest[6].(*string)) = "cccccccc-0000-7000-8000-000000000009"
		*(dest[7].(*string)) = "dddddddd-0000-7000-8000-000000000009"
		*(dest[8].(*int32)) = 2
		*(dest[9].(*string)) = "scheduled"
		*(dest[10].(*string)) = "completed"
		*(dest[11].(*int32)) = 5
		*(dest[12].(*string)) = "sink://x"
		*(dest[13].(*string)) = ""
		*(dest[14].(*string)) = `[{"step":"a","decision":"allow"}]`
		occ := time.Date(2026, 7, 11, 7, 18, 23, 0, time.UTC)
		*(dest[15].(**time.Time)) = &occ
		*(dest[16].(*time.Time)) = time.Date(2026, 7, 11, 7, 18, 23, 0, time.UTC)
		return nil
	}}}
	repo := pg.NewRitualAuditRepository(s)

	rows, err := repo.List(context.Background(), readTenant, 50)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	assertCanonicalTenantSeam(t, s)
	if len(rows) != 1 || rows[0].Status != "completed" {
		t.Fatalf("ritual row scanned wrong: %+v", rows)
	}
	// The ADR-197 per-step decision stamps are the O+ full record — they must
	// survive the jsonb → []map decode.
	if len(rows[0].Stamps) != 1 || rows[0].Stamps[0]["decision"] != "allow" {
		t.Errorf("decision stamps must decode; got %#v", rows[0].Stamps)
	}
	if rows[0].OccurredAt.IsZero() {
		t.Error("occurred_at must be dereferenced from the nullable column")
	}
}

func TestRitualAuditRepository_List_QueryErrorPropagates(t *testing.T) {
	t.Parallel()
	want := errors.New("connection reset")
	repo := pg.NewRitualAuditRepository(&readStub{queryErr: want})
	if _, err := repo.List(context.Background(), readTenant, 10); err == nil {
		t.Fatal("a query failure must surface as an error, never as an empty audit page")
	}
}
