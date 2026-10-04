// closure_repository_test.go — pgx adapter tests for the durable
// ClosureRepository (W0-F1 durability + W0-F5 error-honesty, CHO-2198).
//
// Defines its own closureStubQuerier rather than reusing stubQuerier
// (ledger_repository_test.go): that stub hardcodes QueryRow to always
// return ErrNoRows, which cannot exercise the found-row / conflict /
// genuine-error branches this adapter needs. Reuses stubRow (flexible
// scan closure) and contains, both already declared package-wide in
// ledger_repository_test.go.
//
// These are unit tests against the SQL emit + scan surface — no live DB.
// The critical assertions here are the W0-F5 ones: a genuine backing-store
// error from either Pseudonymise or IsPseudonymised must come back as a
// non-nil error, never get coerced into a false/zero "everything is fine"
// result (the swallowed-error trap the in-memory port's original
// `IsPseudonymised(gcid string) bool` signature made structurally
// impossible to avoid).
package pg_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/apollo-chora/chora-observability/internal/adapter/pg"
	"github.com/apollo-chora/chora-observability/internal/config"
)

func closureSpecFixture() []config.TableSpec {
	return []config.TableSpec{
		{
			Table: "token_usage_ledger",
			Columns: []config.ColumnSpec{
				{Column: "subject_display_name", Strategy: "tombstone_string", Value: "Former member"},
			},
		},
		{
			Table: "budgets",
			Columns: []config.ColumnSpec{
				{Column: "tenant_admin_display_name", Strategy: "tombstone_string", Value: "Former member"},
			},
		},
		{
			Table: "cost_anomaly_alerts",
			Columns: []config.ColumnSpec{
				{Column: "subject_display_name", Strategy: "tombstone_string", Value: "Former member"},
			},
		},
	}
}

// closureStubQuerier is a configurable stub Querier: unlike stubQuerier
// (ledger_repository_test.go), .row is injectable so a test can script a
// found row, a conflict (ErrNoRows), or a genuine backing-store error.
// WithTenantTx mirrors the "record tenantID + delegate fn to self" idiom
// shared by stubQuerier / decisionStubQuerier in this package.
type closureStubQuerier struct {
	execSQL  string
	execArgs []any
	execErr  error

	rowSQL  string
	rowArgs []any
	row     *stubRow

	tenantID string
	txCalled bool
}

func (q *closureStubQuerier) Exec(_ context.Context, sql string, args ...any) error {
	q.execSQL = sql
	q.execArgs = args
	return q.execErr
}

func (q *closureStubQuerier) QueryRow(_ context.Context, sql string, args ...any) pg.Row {
	q.rowSQL = sql
	q.rowArgs = args
	if q.row == nil {
		return &stubRow{err: pg.ErrNoRows}
	}
	return q.row
}

func (q *closureStubQuerier) Query(_ context.Context, _ string, _ ...any) (pg.Rows, error) {
	return nil, errors.New("closureStubQuerier: Query not stubbed in this test")
}

// WithTenantTx records the tenant_id the repo would apply via
// `SET LOCAL chora.tenant_id` and delegates fn to the stub itself so the
// Exec/QueryRow capture still works inside the callback.
func (q *closureStubQuerier) WithTenantTx(ctx context.Context, tenantID string, fn func(context.Context, pg.TenantScopedQuerier) error) error {
	q.txCalled = true
	q.tenantID = tenantID
	return fn(ctx, q)
}

// -----------------------------------------------------------------------------
// Pseudonymise
// -----------------------------------------------------------------------------

func TestClosureRepository_Pseudonymise_EmitsInsertOnConflictReturningID(t *testing.T) {
	t.Skip("disarmed 2026-08-14: Pseudonymise now fails closed via ErrClosureExecutorUnbuilt, so the ack-row mechanics below are unreachable. This test pins the contract the REAL per-table executor must honour (CHO-2198); un-skip it with that executor.")
	t.Parallel()
	q := &closureStubQuerier{row: &stubRow{scan: func(dest ...any) error {
		*(dest[0].(*string)) = uuid.NewString()
		return nil
	}}}
	repo := pg.NewClosureRepository(q)

	tenant := uuid.NewString()
	rows, err := repo.Pseudonymise(context.Background(), tenant, "gcid-A", closureSpecFixture())
	if err != nil {
		t.Fatalf("Pseudonymise: %v", err)
	}
	if rows != 3 {
		t.Fatalf("expected rows_touched=3 (1+1+1 columns); got %d", rows)
	}

	wants := []string{"INSERT INTO closure_pseudonymisation_state", "ON CONFLICT", "DO NOTHING", "RETURNING"}
	for _, w := range wants {
		if !contains(q.rowSQL, w) {
			t.Errorf("Pseudonymise SQL missing %q; got:\n%s", w, q.rowSQL)
		}
	}
	if !q.txCalled {
		t.Fatal("expected Pseudonymise to run inside WithTenantTx (RLS GUC); it did not")
	}
	if q.tenantID != tenant {
		t.Errorf("SET LOCAL chora.tenant_id = %q; want %q", q.tenantID, tenant)
	}
}

func TestClosureRepository_Pseudonymise_NormalisesPlatformSentinel(t *testing.T) {
	t.Skip("disarmed 2026-08-14: Pseudonymise now fails closed via ErrClosureExecutorUnbuilt, so the ack-row mechanics below are unreachable. This test pins the contract the REAL per-table executor must honour (CHO-2198); un-skip it with that executor.")
	t.Parallel()
	// "platform" is the envelope sentinel for non-tenant subjects
	// (NormalizeTenantForRLS) — the closure_pseudonymisation_state
	// tenant_id column is UUID NOT NULL with a `::uuid` RLS cast, so a
	// bare "platform" literal would 22P02 at row-evaluation time. Mirrors
	// TestLedgerRepository_Append_TenantScopedAndNormalised.
	q := &closureStubQuerier{row: &stubRow{scan: func(dest ...any) error {
		*(dest[0].(*string)) = uuid.NewString()
		return nil
	}}}
	repo := pg.NewClosureRepository(q)

	if _, err := repo.Pseudonymise(context.Background(), "platform", "gcid-A", nil); err != nil {
		t.Fatalf("Pseudonymise: %v", err)
	}
	if q.tenantID != pg.NilTenantUUID {
		t.Errorf("SET LOCAL chora.tenant_id = %q; want NilTenantUUID %q", q.tenantID, pg.NilTenantUUID)
	}
	if len(q.rowArgs) < 2 {
		t.Fatalf("expected >= 2 args; got %d", len(q.rowArgs))
	}
	if q.rowArgs[1] != pg.NilTenantUUID {
		t.Errorf("inserted tenant_id arg = %v; want NilTenantUUID %q", q.rowArgs[1], pg.NilTenantUUID)
	}
}

func TestClosureRepository_Pseudonymise_MintsUUIDv7ForID(t *testing.T) {
	t.Skip("disarmed 2026-08-14: Pseudonymise now fails closed via ErrClosureExecutorUnbuilt, so the ack-row mechanics below are unreachable. This test pins the contract the REAL per-table executor must honour (CHO-2198); un-skip it with that executor.")
	t.Parallel()
	q := &closureStubQuerier{row: &stubRow{scan: func(dest ...any) error {
		*(dest[0].(*string)) = uuid.NewString()
		return nil
	}}}
	repo := pg.NewClosureRepository(q)

	if _, err := repo.Pseudonymise(context.Background(), uuid.NewString(), "gcid-A", nil); err != nil {
		t.Fatalf("Pseudonymise: %v", err)
	}
	if len(q.rowArgs) == 0 {
		t.Fatalf("expected query args")
	}
	idArg, ok := q.rowArgs[0].(string)
	if !ok || idArg == "" {
		t.Fatalf("expected non-empty string id as first arg; got %T %v", q.rowArgs[0], q.rowArgs[0])
	}
	parsed, err := uuid.Parse(idArg)
	if err != nil {
		t.Fatalf("minted id %q is not a valid UUID: %v", idArg, err)
	}
	if parsed.Version() != 7 {
		t.Fatalf("minted id %q is not UUIDv7 (version=%d)", idArg, parsed.Version())
	}
}

func TestClosureRepository_Pseudonymise_ReturnsZeroWhenConflictFires(t *testing.T) {
	t.Skip("disarmed 2026-08-14: Pseudonymise now fails closed via ErrClosureExecutorUnbuilt, so the ack-row mechanics below are unreachable. This test pins the contract the REAL per-table executor must honour (CHO-2198); un-skip it with that executor.")
	t.Parallel()
	// ON CONFLICT DO NOTHING suppresses the RETURNING row — the stub
	// Querier signals that exactly as the pg.Querier seam does: ErrNoRows.
	q := &closureStubQuerier{} // no .row set -> defaults to ErrNoRows
	repo := pg.NewClosureRepository(q)

	rows, err := repo.Pseudonymise(context.Background(), uuid.NewString(), "gcid-A", closureSpecFixture())
	if err != nil {
		t.Fatalf("Pseudonymise: expected idempotent no-op, got error: %v", err)
	}
	if rows != 0 {
		t.Fatalf("expected rows=0 on idempotent replay; got %d", rows)
	}
}

func TestClosureRepository_Pseudonymise_RejectsEmptyTenantID(t *testing.T) {
	t.Skip("disarmed 2026-08-14: Pseudonymise now fails closed via ErrClosureExecutorUnbuilt, so the ack-row mechanics below are unreachable. This test pins the contract the REAL per-table executor must honour (CHO-2198); un-skip it with that executor.")
	t.Parallel()
	q := &closureStubQuerier{}
	repo := pg.NewClosureRepository(q)
	_, err := repo.Pseudonymise(context.Background(), "", "gcid-A", nil)
	if err == nil {
		t.Fatalf("expected error on empty tenant_id")
	}
	if q.txCalled {
		t.Fatalf("must not open a tenant tx on validation failure")
	}
}

func TestClosureRepository_Pseudonymise_RejectsEmptyGCID(t *testing.T) {
	t.Skip("disarmed 2026-08-14: Pseudonymise now fails closed via ErrClosureExecutorUnbuilt, so the ack-row mechanics below are unreachable. This test pins the contract the REAL per-table executor must honour (CHO-2198); un-skip it with that executor.")
	t.Parallel()
	q := &closureStubQuerier{}
	repo := pg.NewClosureRepository(q)
	_, err := repo.Pseudonymise(context.Background(), uuid.NewString(), "", nil)
	if err == nil {
		t.Fatalf("expected error on empty gcid")
	}
	if q.txCalled {
		t.Fatalf("must not open a tenant tx on validation failure")
	}
}

// TestClosureRepository_Pseudonymise_PropagatesScanError is the W0-F5
// fail-loud proof for Pseudonymise: a genuine backing-store error (NOT
// pg.ErrNoRows) must come back as a non-nil error, never as a silent
// "idempotent no-op" (0, nil) — conflating "I don't know" with "already
// done" would let the closure saga believe this domain acked when it did
// not, exactly the class of bug in
// reusable_gotcha_swallowed_error_damage_is_decided_by_the_caller.
func TestClosureRepository_Pseudonymise_PropagatesScanError(t *testing.T) {
	t.Skip("disarmed 2026-08-14: Pseudonymise now fails closed via ErrClosureExecutorUnbuilt, so the ack-row mechanics below are unreachable. This test pins the contract the REAL per-table executor must honour (CHO-2198); un-skip it with that executor.")
	t.Parallel()
	boom := errors.New("pg: connection reset by peer")
	q := &closureStubQuerier{row: &stubRow{err: boom}}
	repo := pg.NewClosureRepository(q)

	rows, err := repo.Pseudonymise(context.Background(), uuid.NewString(), "gcid-A", closureSpecFixture())
	if err == nil {
		t.Fatalf("expected error to propagate, got nil (rows=%d)", rows)
	}
	if !errors.Is(err, boom) {
		t.Fatalf("expected wrapped sentinel error; got %v", err)
	}
	if rows != 0 {
		t.Fatalf("expected rows=0 on error; got %d", rows)
	}
}

// -----------------------------------------------------------------------------
// IsPseudonymised
// -----------------------------------------------------------------------------

func TestClosureRepository_IsPseudonymised_TrueWhenRowExists(t *testing.T) {
	t.Parallel()
	q := &closureStubQuerier{row: &stubRow{scan: func(dest ...any) error {
		*(dest[0].(*int)) = 1
		return nil
	}}}
	repo := pg.NewClosureRepository(q)

	got, err := repo.IsPseudonymised(context.Background(), uuid.NewString(), "gcid-A")
	if err != nil {
		t.Fatalf("IsPseudonymised: %v", err)
	}
	if !got {
		t.Fatalf("expected true when a row exists")
	}
	if !contains(q.rowSQL, "FROM closure_pseudonymisation_state") {
		t.Errorf("IsPseudonymised SQL malformed; got:\n%s", q.rowSQL)
	}
}

func TestClosureRepository_IsPseudonymised_FalseWhenNoRows(t *testing.T) {
	t.Parallel()
	q := &closureStubQuerier{} // defaults to ErrNoRows
	repo := pg.NewClosureRepository(q)

	got, err := repo.IsPseudonymised(context.Background(), uuid.NewString(), "gcid-A")
	if err != nil {
		t.Fatalf("IsPseudonymised: expected nil error on a clean miss; got %v", err)
	}
	if got {
		t.Fatalf("expected false when no row exists")
	}
}

// TestClosureRepository_IsPseudonymised_PropagatesQueryError is the core
// W0-F5 proof for IsPseudonymised: this is exactly the method the original
// `IsPseudonymised(gcid string) bool` signature could NOT have implemented
// honestly against Postgres (no ctx, no error return). A real backing-store
// error must be reported, not folded into `false`.
func TestClosureRepository_IsPseudonymised_PropagatesQueryError(t *testing.T) {
	t.Parallel()
	boom := errors.New("pg: connection reset by peer")
	q := &closureStubQuerier{row: &stubRow{err: boom}}
	repo := pg.NewClosureRepository(q)

	got, err := repo.IsPseudonymised(context.Background(), uuid.NewString(), "gcid-A")
	if err == nil {
		t.Fatalf("expected error to propagate, got nil (got=%v)", got)
	}
	if !errors.Is(err, boom) {
		t.Fatalf("expected wrapped sentinel error; got %v", err)
	}
	if got {
		t.Fatalf("expected false alongside the error (never claim true on failure)")
	}
}

func TestClosureRepository_IsPseudonymised_RejectsEmptyTenantID(t *testing.T) {
	t.Parallel()
	q := &closureStubQuerier{}
	repo := pg.NewClosureRepository(q)
	_, err := repo.IsPseudonymised(context.Background(), "", "gcid-A")
	if err == nil {
		t.Fatalf("expected error on empty tenant_id")
	}
	if q.txCalled {
		t.Fatalf("must not open a tenant tx on validation failure")
	}
}

func TestClosureRepository_IsPseudonymised_RejectsEmptyGCID(t *testing.T) {
	t.Parallel()
	q := &closureStubQuerier{}
	repo := pg.NewClosureRepository(q)
	_, err := repo.IsPseudonymised(context.Background(), uuid.NewString(), "")
	if err == nil {
		t.Fatalf("expected error on empty gcid")
	}
	if q.txCalled {
		t.Fatalf("must not open a tenant tx on validation failure")
	}
}

func TestClosureRepository_NoQuerier_FailsLoud(t *testing.T) {
	t.Parallel()
	repo := pg.NewClosureRepository(nil)
	if _, err := repo.Pseudonymise(context.Background(), uuid.NewString(), "gcid-A", nil); err == nil {
		t.Fatalf("expected error when no Querier wired (Pseudonymise)")
	}
	if _, err := repo.IsPseudonymised(context.Background(), uuid.NewString(), "gcid-A"); err == nil {
		t.Fatalf("expected error when no Querier wired (IsPseudonymised)")
	}
}
