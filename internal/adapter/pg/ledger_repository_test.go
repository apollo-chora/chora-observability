// ledger_repository_test.go — pgx adapter tests using a stub Querier.
package pg_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/5007-Capstone/chora/services/chora-observability/internal/adapter/pg"
	"github.com/5007-Capstone/chora/services/chora-observability/internal/domain/ledger"
)

// stubQuerier captures the most recent Exec / QueryRow call + the tenant_id
// passed to WithTenantTx (the SET LOCAL chora.tenant_id GUC value).
type stubQuerier struct {
	execSQL  string
	execArgs []any
	execErr  error
	tenantID string // captured from the most recent WithTenantTx
	txCalled bool
}

func (s *stubQuerier) Exec(_ context.Context, sql string, args ...any) error {
	s.execSQL = sql
	s.execArgs = args
	return s.execErr
}

func (s *stubQuerier) QueryRow(_ context.Context, _ string, _ ...any) pg.Row {
	return &stubRow{err: pg.ErrNoRows}
}

func (s *stubQuerier) Query(_ context.Context, _ string, _ ...any) (pg.Rows, error) {
	return nil, errors.New("stubQuerier: Query not stubbed in this test")
}

// WithTenantTx records the tenant_id the repo would apply via
// `SET LOCAL chora.tenant_id` and delegates fn to the stub itself so the
// Exec/Query/QueryRow capture still works inside the callback.
func (s *stubQuerier) WithTenantTx(ctx context.Context, tenantID string, fn func(context.Context, pg.TenantScopedQuerier) error) error {
	s.txCalled = true
	s.tenantID = tenantID
	return fn(ctx, s)
}

type stubRow struct {
	scan func(dest ...any) error
	err  error
}

func (r *stubRow) Scan(dest ...any) error {
	if r.scan != nil {
		return r.scan(dest...)
	}
	return r.err
}

func TestLedgerRepository_Append_EmitsInsertSQL(t *testing.T) {
	t.Parallel()
	tenant := uuid.NewString()
	gcid := uuid.NewString()

	e, err := ledger.New(ledger.NewParams{
		TenantID: tenant,
		Gcid:     gcid,
		ModelID:  "gemini-2.0-flash",
		PromptTokens: 100, CompletionTokens: 50,
		CostUsdMicros: 1234,
		TraceID:       "00000000000000000000000000000001",
		SpanID:        "0000000000000001",
	})
	if err != nil {
		t.Fatalf("ledger.New: %v", err)
	}

	q := &stubQuerier{}
	repo := pg.NewLedgerRepository(q)

	if err := repo.Append(context.Background(), e); err != nil {
		t.Fatalf("Append: %v", err)
	}

	if !contains(q.execSQL, "INSERT INTO token_usage_ledger") {
		t.Errorf("expected INSERT into token_usage_ledger; got:\n%s", q.execSQL)
	}
	// Spot-check column order: ledger_id, tenant_id, gcid first.
	if len(q.execArgs) < 8 {
		t.Fatalf("expected ≥ 8 args; got %d (sql=%q)", len(q.execArgs), q.execSQL)
	}
	if q.execArgs[0] != e.LedgerID {
		t.Errorf("arg[0] = %v; want LedgerID %q", q.execArgs[0], e.LedgerID)
	}
	if q.execArgs[1] != tenant {
		t.Errorf("arg[1] = %v; want tenant %q", q.execArgs[1], tenant)
	}
}

// TestLedgerRepository_Append_TenantScopedAndNormalised proves the Append
// path runs inside WithTenantTx (so the RLS tenant_isolation policy passes)
// and that the "platform" envelope sentinel is normalised to the nil UUID
// for BOTH the SET LOCAL chora.tenant_id GUC AND the inserted tenant_id
// column. Reproduces the prod defect: a bare pool Exec with tenant="platform"
// is RLS-rejected ("new row violates row-level security policy") + can't
// cast 'platform'::uuid.
func TestLedgerRepository_Append_TenantScopedAndNormalised(t *testing.T) {
	t.Parallel()
	e, err := ledger.New(ledger.NewParams{
		TenantID: "platform",
		Gcid:     uuid.NewString(),
		ModelID:  "vertex_ai/gemini-2.5-pro",
		PromptTokens: 100, CompletionTokens: 50,
		CostUsdMicros: 1234,
		TraceID:       "00000000000000000000000000000001",
		SpanID:        "0000000000000001",
	})
	if err != nil {
		t.Fatalf("ledger.New: %v", err)
	}

	q := &stubQuerier{}
	repo := pg.NewLedgerRepository(q)
	if err := repo.Append(context.Background(), e); err != nil {
		t.Fatalf("Append: %v", err)
	}

	if !q.txCalled {
		t.Fatal("expected Append to run inside WithTenantTx (RLS GUC); it did not")
	}
	if q.tenantID != pg.NilTenantUUID {
		t.Errorf("SET LOCAL chora.tenant_id = %q; want NilTenantUUID %q", q.tenantID, pg.NilTenantUUID)
	}
	if len(q.execArgs) < 2 {
		t.Fatalf("expected ≥ 2 args; got %d", len(q.execArgs))
	}
	if q.execArgs[1] != pg.NilTenantUUID {
		t.Errorf("inserted tenant_id arg = %v; want NilTenantUUID %q", q.execArgs[1], pg.NilTenantUUID)
	}
}

func TestLedgerRepository_Append_NilEntryRejected(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	repo := pg.NewLedgerRepository(q)
	if err := repo.Append(context.Background(), nil); err == nil {
		t.Fatal("expected error for nil entry; got nil")
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
