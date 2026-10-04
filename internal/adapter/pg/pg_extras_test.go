// pg_extras_test.go — completes the pg adapter surfaces reachable through a
// stub Querier (no live DB): the ledger List/SumCost scan + error paths,
// the decision Count rollup, the platform kill-switch Get/Set branches, the
// familiar-growth pagination bounds helper, and the constructor guards.
//
// The *PgxPoolQuerier / txQuerier / pgxPoolRow / pgxPoolRows wrappers in
// runtime.go are deliberately NOT covered here: they require a real
// *pgxpool.Pool / pgx.Tx and are exercised by the live integration suite
// (see the platform-level pgx integration tests).
package pg_test

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/5007-Capstone/chora/services/chora-observability/internal/adapter/pg"
	fg "github.com/5007-Capstone/chora/services/chora-observability/internal/domain/familiargrowth"
	"github.com/5007-Capstone/chora/services/chora-observability/internal/domain/ledger"
)

// extStubQuerier is a flexible pg.Querier stub: it can script Query rows,
// QueryRow scan/errors, and Exec errors, and records the tenant passed to
// WithTenantTx.
type extStubQuerier struct {
	execSQL   string
	execArgs  []any
	execErr   error
	tenantID  string
	txCalled  bool
	queryErr  error
	queryArgs []any
	rows      [][]any
	qrScan    func(dest ...any) error
	qrErr     error
}

func (s *extStubQuerier) Exec(_ context.Context, sql string, args ...any) error {
	s.execSQL = sql
	s.execArgs = args
	return s.execErr
}

func (s *extStubQuerier) QueryRow(_ context.Context, _ string, args ...any) pg.Row {
	if s.qrErr != nil {
		return &extRow{err: s.qrErr}
	}
	return &extRow{scan: s.qrScan}
}

func (s *extStubQuerier) Query(_ context.Context, _ string, args ...any) (pg.Rows, error) {
	s.queryArgs = args
	if s.queryErr != nil {
		return nil, s.queryErr
	}
	return &extRows{rows: s.rows}, nil
}

func (s *extStubQuerier) WithTenantTx(ctx context.Context, tenantID string, fn func(context.Context, pg.TenantScopedQuerier) error) error {
	s.txCalled = true
	s.tenantID = tenantID
	return fn(ctx, s)
}

type extRow struct {
	scan func(dest ...any) error
	err  error
}

func (r *extRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if r.scan == nil {
		return pg.ErrNoRows
	}
	return r.scan(dest...)
}

type extRows struct {
	rows [][]any
	i    int
}

func (r *extRows) Next() bool {
	if r.i >= len(r.rows) {
		return false
	}
	r.i++
	return true
}
func (r *extRows) Scan(dest ...any) error { return assignScan(dest, r.rows[r.i-1]) }
func (r *extRows) Close()                 {}
func (r *extRows) Err() error             { return nil }

// ---------------------------------------------------------------------------
// LedgerRepository
// ---------------------------------------------------------------------------

func TestLedgerRepository_List_ScansRows(t *testing.T) {
	q := &extStubQuerier{
		rows: [][]any{
			{"lid-1", "t-1", "g-1", "ag-1", "m-1", 10, 20, int64(100), "trace-1", "span-1", time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)},
			{"lid-2", "t-1", "g-2", "ag-2", "m-2", 30, 40, int64(200), "trace-2", "span-2", time.Date(2026, 5, 2, 0, 0, 0, 0, time.UTC)},
		},
	}
	repo := pg.NewLedgerRepository(q)

	entries, err := repo.List(context.Background(), "platform", ledger.ListFilter{
		From: time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("entries = %d; want 2", len(entries))
	}
	if entries[0].LedgerID != "lid-1" || entries[1].CostUsdMicros != 200 {
		t.Errorf("scan mismatch: %+v", entries)
	}
	// "platform" must normalise to the nil-UUID sentinel for the RLS GUC
	if !q.txCalled || q.tenantID != pg.NilTenantUUID {
		t.Errorf("WithTenantTx tenant = %q (called=%v); want %q", q.tenantID, q.txCalled, pg.NilTenantUUID)
	}
}

func TestLedgerRepository_List_NoRows(t *testing.T) {
	q := &extStubQuerier{}
	repo := pg.NewLedgerRepository(q)
	entries, err := repo.List(context.Background(), "t-1", ledger.ListFilter{})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("entries = %d; want 0", len(entries))
	}
	// non-zero From -> nullableTime returns the timestamp argument
	if len(q.queryArgs) < 2 || q.queryArgs[1] != nil {
		t.Errorf("expected nil (zero) From arg, got %v", q.queryArgs)
	}
}

func TestLedgerRepository_List_QueryError(t *testing.T) {
	q := &extStubQuerier{queryErr: errors.New("connection reset")}
	_, err := pg.NewLedgerRepository(q).List(context.Background(), "t-1", ledger.ListFilter{})
	if err == nil {
		t.Fatal("expected list error")
	}
}

func TestLedgerRepository_SumCost_ScansAndAggregates(t *testing.T) {
	q := &extStubQuerier{
		rows: [][]any{{int64(100)}, {int64(200)}, {int64(300)}},
	}
	total, count, err := pg.NewLedgerRepository(q).SumCost(context.Background(), "t-1", ledger.ListFilter{
		To: time.Date(2026, 5, 2, 0, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("SumCost: %v", err)
	}
	if total != 600 || count != 3 {
		t.Errorf("sum = (%d, %d); want (600, 3)", total, count)
	}
	if len(q.queryArgs) < 3 || q.queryArgs[2] == nil {
		t.Errorf("expected non-nil To arg, got %v", q.queryArgs)
	}
}

func TestLedgerRepository_SumCost_Errors(t *testing.T) {
	// query failure
	_, _, err := pg.NewLedgerRepository(&extStubQuerier{queryErr: errors.New("reset")}).
		SumCost(context.Background(), "t-1", ledger.ListFilter{})
	if err == nil {
		t.Fatal("expected query error")
	}

	// int64 overflow via the domain SumCost guard
	overflow := pg.NewLedgerRepository(&extStubQuerier{
		rows: [][]any{{int64(math.MaxInt64)}, {int64(1)}},
	})
	if _, _, err := overflow.SumCost(context.Background(), "t-1", ledger.ListFilter{}); err == nil {
		t.Fatal("expected overflow error")
	}
}

func TestLedgerRepository_Append_NilEntryAndExecError(t *testing.T) {
	repo := pg.NewLedgerRepository(&extStubQuerier{})
	if err := repo.Append(context.Background(), nil); err == nil {
		t.Fatal("expected nil-entry error")
	}

	q := &extStubQuerier{execErr: errors.New("boom")}
	err := pg.NewLedgerRepository(q).Append(context.Background(), &ledger.Entry{
		TenantID: "t-1", Gcid: "g-1", ModelID: "m-1",
		PromptTokens: 10, CostUsdMicros: 100, RecordedAt: time.Now(),
	})
	if err == nil {
		t.Fatal("expected exec error")
	}
}

// ---------------------------------------------------------------------------
// DecisionRepository.Count
// ---------------------------------------------------------------------------

func TestDecisionRepository_Count(t *testing.T) {
	q := &extStubQuerier{
		qrScan: func(dest ...any) error {
			*(dest[0].(*int64)) = 42
			return nil
		},
	}
	repo := pg.NewDecisionRepository(q)
	n, err := repo.Count(context.Background(), "t-1", time.Now().Add(-24*time.Hour), time.Now())
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if n != 42 {
		t.Errorf("count = %d; want 42", n)
	}

	// error propagates loudly (per feedback-no-stubs-real-wiring)
	qErr := &extStubQuerier{qrErr: errors.New("db down")}
	if _, err := pg.NewDecisionRepository(qErr).Count(context.Background(), "t-1", time.Time{}, time.Time{}); err == nil {
		t.Fatal("expected count error")
	}
}

// ---------------------------------------------------------------------------
// Platform egress kill switch
// ---------------------------------------------------------------------------

func TestKillSwitchRepo_Get(t *testing.T) {
	t0 := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	happy := &extStubQuerier{
		qrScan: func(dest ...any) error {
			*(dest[0].(*bool)) = true
			*(dest[1].(*string)) = "incident-77"
			*(dest[2].(*string)) = "00000000-0000-7000-8000-000000001999"
			*(dest[3].(*time.Time)) = t0
			return nil
		},
	}
	ks, err := pg.NewKillSwitchRepo(happy).Get(context.Background())
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !ks.Engaged || ks.Reason != "incident-77" || !ks.UpdatedAt.Equal(t0) {
		t.Errorf("kill switch = %+v", ks)
	}

	// missing singleton row -> fail loud
	if _, err := pg.NewKillSwitchRepo(&extStubQuerier{}).Get(context.Background()); !errors.Is(err, pg.ErrNoKillSwitchRow) {
		t.Fatalf("Get no-row err = %v; want ErrNoKillSwitchRow", err)
	}

	// unexpected error -> wrapped
	if _, err := pg.NewKillSwitchRepo(&extStubQuerier{qrErr: errors.New("reset")}).Get(context.Background()); err == nil {
		t.Fatal("expected wrapped get error")
	}
}

func TestKillSwitchRepo_Set(t *testing.T) {
	t0 := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	happy := &extStubQuerier{
		qrScan: func(dest ...any) error {
			*(dest[0].(*bool)) = false
			*(dest[1].(*string)) = ""
			*(dest[2].(*string)) = "00000000-0000-7000-8000-000000001999"
			*(dest[3].(*time.Time)) = t0
			return nil
		},
	}
	ks, err := pg.NewKillSwitchRepo(happy).Set(context.Background(), false, "", "00000000-0000-7000-8000-000000001999")
	if err != nil {
		t.Fatalf("Set: %v", err)
	}
	if ks.Engaged {
		t.Error("expected engaged=false")
	}

	// anonymous flip refused
	if _, err := pg.NewKillSwitchRepo(&extStubQuerier{}).Set(context.Background(), true, "r", ""); !errors.Is(err, pg.ErrKillSwitchNoActor) {
		t.Fatalf("no-actor err = %v; want ErrKillSwitchNoActor", err)
	}

	// missing singleton row -> fail loud
	if _, err := pg.NewKillSwitchRepo(&extStubQuerier{}).Set(context.Background(), true, "r", "00000000-0000-7000-8000-000000001999"); !errors.Is(err, pg.ErrNoKillSwitchRow) {
		t.Fatalf("no-row err = %v; want ErrNoKillSwitchRow", err)
	}

	// unexpected error -> wrapped
	if _, err := pg.NewKillSwitchRepo(&extStubQuerier{qrErr: errors.New("reset")}).Set(context.Background(), true, "r", "00000000-0000-7000-8000-000000001999"); err == nil {
		t.Fatal("expected wrapped set error")
	}
}

// ---------------------------------------------------------------------------
// Familiar-growth pagination bounds + constructor guards
// ---------------------------------------------------------------------------

func TestFamiliarGrowthRepository_ListLedger_BoundsPagination(t *testing.T) {
	q := &extStubQuerier{}
	repo := pg.NewFamiliarGrowthRepository(q)
	_, err := repo.ListLedger(context.Background(), "t-1", fg.AuditFilter{Limit: 5000, Offset: -5})
	if err != nil {
		t.Fatalf("ListLedger: %v", err)
	}
	// boundLimitOffset: 5000 clamps to 1000; -5 raises to 0 — the last two
	// bind args are LIMIT/OFFSET.
	args := q.queryArgs
	if len(args) < 2 {
		t.Fatalf("query args = %v", args)
	}
	if args[len(args)-2] != 1000 || args[len(args)-1] != 0 {
		t.Errorf("LIMIT/OFFSET = %v/%v; want 1000/0", args[len(args)-2], args[len(args)-1])
	}
}

func TestPgConstructorGuards(t *testing.T) {
	for name, fn := range map[string]func(){
		"external egress": func() { pg.NewExternalEgressRepo(nil) },
		"kill switch":     func() { pg.NewKillSwitchRepo(nil) },
	} {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatalf("%s: expected panic on nil Querier", name)
				}
			}()
			fn()
		})
	}
}

// NewPgxPoolQuerier + Pool() wrap a *pgxpool.Pool without touching it.
// pgxpool.New is lazy (no connection until first use), so construction +
// the Pool() accessor are testable offline; the Exec/Query/WithTenantTx
// methods need a live pool and stay in the integration suite.
func TestNewPgxPoolQuerier_PoolAccessor(t *testing.T) {
	pool, err := pgxpool.New(context.Background(), "postgres://user:pass@localhost:5432/chora")
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	defer pool.Close()
	q := pg.NewPgxPoolQuerier(pool)
	if q == nil || q.Pool() != pool {
		t.Fatal("NewPgxPoolQuerier/Pool round-trip failed")
	}
}