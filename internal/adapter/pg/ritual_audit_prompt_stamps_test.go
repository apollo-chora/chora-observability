// ritual_audit_prompt_stamps_test.go - pgx adapter tests for the CHO-2364
// AggregatePromptStamps read (ADR-197 prompt-evidence aggregation over the
// ritual_run_audit stamps JSONB array). Same stub-Querier SQL-smoke idiom as
// the sibling repositories: capture the SQL + tenant-tx seam, serve scripted
// rows for the scan paths.
package pg_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/5007-Capstone/chora/services/chora-observability/internal/adapter/pg"
)

// promptStampsStub scripts a totals row (QueryRow) + version rows (Query) so
// the two statements inside one WithTenantTx can be exercised offline.
type promptStampsStub struct {
	tenantID string
	txN      int

	queryRowSQL string
	querySQL    string

	totalsRow   []any   // scripted (runs_total, last_run_at) row
	versionRows [][]any // scripted (prompt_version, runs, last_seen) rows
	queryErr    error
}

func (s *promptStampsStub) Exec(_ context.Context, _ string, _ ...any) error { return nil }

func (s *promptStampsStub) QueryRow(_ context.Context, sql string, _ ...any) pg.Row {
	s.queryRowSQL = sql
	return &stampScriptRow{vals: s.totalsRow}
}

func (s *promptStampsStub) Query(_ context.Context, sql string, _ ...any) (pg.Rows, error) {
	s.querySQL = sql
	if s.queryErr != nil {
		return nil, s.queryErr
	}
	return &stampScriptRows{rows: s.versionRows}, nil
}

func (s *promptStampsStub) WithTenantTx(ctx context.Context, tenantID string, fn func(context.Context, pg.TenantScopedQuerier) error) error {
	s.txN++
	s.tenantID = tenantID
	return fn(ctx, s)
}

// stampScriptRow / stampScriptRows assign scripted values into the scan
// destinations this aggregation uses (*string, *int64, *time.Time, **time.Time).
type stampScriptRow struct{ vals []any }

func (r *stampScriptRow) Scan(dest ...any) error { return stampAssign(dest, r.vals) }

type stampScriptRows struct {
	rows [][]any
	i    int
}

func (r *stampScriptRows) Next() bool {
	if r.i >= len(r.rows) {
		return false
	}
	r.i++
	return true
}
func (r *stampScriptRows) Scan(dest ...any) error { return stampAssign(dest, r.rows[r.i-1]) }
func (r *stampScriptRows) Close()                 {}
func (r *stampScriptRows) Err() error             { return nil }

func stampAssign(dest, vals []any) error {
	for i := range dest {
		switch d := dest[i].(type) {
		case *string:
			*d = vals[i].(string)
		case *int64:
			*d = vals[i].(int64)
		case *time.Time:
			*d = vals[i].(time.Time)
		case **time.Time:
			if vals[i] == nil {
				*d = nil
			} else {
				v := vals[i].(time.Time)
				*d = &v
			}
		default:
			panic("stampAssign: unhandled dest type")
		}
	}
	return nil
}

// TestRitualAuditRepository_AggregatePromptStamps_SQLShape proves both
// statements (totals + per-version lateral unnest) run inside ONE
// WithTenantTx and read the canonical snake_case stamp key with the legacy
// CamelCase fallback (pre-CHO-2136 JSON rows stored Go field names).
func TestRitualAuditRepository_AggregatePromptStamps_SQLShape(t *testing.T) {
	t.Parallel()
	lastRun := time.Date(2026, 7, 22, 10, 0, 0, 0, time.UTC)
	lastSeen := lastRun.Add(-time.Hour)
	stub := &promptStampsStub{
		totalsRow: []any{int64(3), lastRun},
		versionRows: [][]any{
			{"v3", int64(2), lastSeen},
			{"v4", int64(1), lastRun},
		},
	}
	repo := pg.NewRitualAuditRepository(stub)
	tenant := uuid.NewString()
	sum, err := repo.AggregatePromptStamps(context.Background(), tenant)
	if err != nil {
		t.Fatalf("AggregatePromptStamps: %v", err)
	}
	if stub.txN != 1 {
		t.Fatalf("WithTenantTx calls = %d; want 1 (both statements share the tx)", stub.txN)
	}
	if stub.tenantID != tenant {
		t.Errorf("tenant GUC: got %q want %q", stub.tenantID, tenant)
	}
	for _, want := range []string{"COUNT(*)", "FROM ritual_run_audit"} {
		if !containsSub(stub.queryRowSQL, want) {
			t.Errorf("totals SQL missing %q:\n%s", want, stub.queryRowSQL)
		}
	}
	for _, want := range []string{
		"jsonb_array_elements",
		"'prompt_version'",
		"'PromptVersion'",
		"COUNT(DISTINCT",
		"FROM ritual_run_audit",
	} {
		if !containsSub(stub.querySQL, want) {
			t.Errorf("versions SQL missing %q:\n%s", want, stub.querySQL)
		}
	}
	if sum.RunsTotal != 3 || !sum.LastRunAt.Equal(lastRun) {
		t.Errorf("totals = %d/%s; want 3/%s", sum.RunsTotal, sum.LastRunAt, lastRun)
	}
	if len(sum.Versions) != 2 {
		t.Fatalf("versions = %d; want 2", len(sum.Versions))
	}
	if sum.Versions[0].PromptVersion != "v3" || sum.Versions[0].Runs != 2 || !sum.Versions[0].LastSeen.Equal(lastSeen) {
		t.Errorf("versions[0] = %+v; want v3/2/%s", sum.Versions[0], lastSeen)
	}
	if sum.Versions[1].PromptVersion != "v4" || sum.Versions[1].Runs != 1 || !sum.Versions[1].LastSeen.Equal(lastRun) {
		t.Errorf("versions[1] = %+v; want v4/1/%s", sum.Versions[1], lastRun)
	}
}

// TestRitualAuditRepository_AggregatePromptStamps_EmptyTenant proves the
// zero-run case: NULL MAX scans to a zero LastRunAt, versions stays an empty
// non-nil slice, no error (honest zero).
func TestRitualAuditRepository_AggregatePromptStamps_EmptyTenant(t *testing.T) {
	t.Parallel()
	stub := &promptStampsStub{totalsRow: []any{int64(0), nil}}
	repo := pg.NewRitualAuditRepository(stub)
	sum, err := repo.AggregatePromptStamps(context.Background(), uuid.NewString())
	if err != nil {
		t.Fatalf("AggregatePromptStamps: %v", err)
	}
	if sum.RunsTotal != 0 || !sum.LastRunAt.IsZero() {
		t.Errorf("totals = %d/%s; want 0/zero-time", sum.RunsTotal, sum.LastRunAt)
	}
	if sum.Versions == nil || len(sum.Versions) != 0 {
		t.Errorf("versions = %#v; want empty non-nil slice", sum.Versions)
	}
}

// TestRitualAuditRepository_AggregatePromptStamps_QueryErrorFailsLoud proves a
// version-query failure surfaces as an error (never a silent partial result).
func TestRitualAuditRepository_AggregatePromptStamps_QueryErrorFailsLoud(t *testing.T) {
	t.Parallel()
	stub := &promptStampsStub{
		totalsRow: []any{int64(1), time.Date(2026, 7, 22, 10, 0, 0, 0, time.UTC)},
		queryErr:  errors.New("boom"),
	}
	repo := pg.NewRitualAuditRepository(stub)
	if _, err := repo.AggregatePromptStamps(context.Background(), uuid.NewString()); err == nil {
		t.Fatal("expected error when the versions query fails")
	}
}
