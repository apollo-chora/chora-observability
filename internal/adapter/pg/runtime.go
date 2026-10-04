// Package pg is the pgx-backed adapter for chora-observability repository
// ports defined in internal/domain.
//
// Architecture mirrors chora-identity / chora-notifications:
//
//   - Domain ports (Repository on ledger / decision / correlation) are
//     defined in internal/domain/{ledger,decision,correlation}.
//   - This package implements those ports against a *pgxpool.Pool.
//   - A small `Querier` interface decouples SQL from pgx so unit tests
//     stub the SQL surface without a live DB.
//
// Resilience-priority directive (`feedback_resilience_priority`):
//
//   - All tenant-scoped queries run inside a transaction with `SET LOCAL
//     chora.tenant_id` applied BEFORE the user query.
//   - Append-only invariants (token_usage_ledger, agent_decision_log) are
//     enforced both at the table-trigger level (migrations) and at the
//     repository level (only Append + List/SumCost are exposed).
//
// Cross-DB queries forbidden — chora-observability reads only
// chora_observability. Inter-domain side effects flow through the
// chora_observability outbox (migration 0003).
package pg

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// NilTenantUUID is the canonical sentinel for platform-level / non-tenant
// rows in the UUID-typed tenant_id columns (token_usage_ledger,
// agent_decision_log). It mirrors the repo-wide convention
// (chora-creation question_job_repository preflightTenant +
// chora-consumption engine_warmup warmupSyntheticTenantID).
//
// The event envelope carries the string sentinel "platform" (blessed by
// libs/chora-go-common/rls.ValidateTenantID), but the observability
// read-model columns are UUID NOT NULL with a `current_setting(
// 'chora.tenant_id', true)::uuid` RLS policy — `'platform'::uuid` fails the
// cast. NormalizeTenantForRLS bridges the two at the adapter boundary.
const NilTenantUUID = "00000000-0000-0000-0000-000000000000"

// NormalizeTenantForRLS maps the envelope "platform" sentinel (and the
// empty string) to NilTenantUUID so it satisfies the UUID column + the
// `::uuid` RLS cast. Real tenant UUIDs pass through unchanged.
func NormalizeTenantForRLS(tenantID string) string {
	switch tenantID {
	case "", "platform":
		return NilTenantUUID
	default:
		return tenantID
	}
}

// TenantScopedQuerier is the Exec/Query/QueryRow surface available INSIDE a
// WithTenantTx callback. The `SET LOCAL chora.tenant_id` GUC is already
// applied on the transaction, so reads + writes against RLS-protected tables
// pass the `current_setting('chora.tenant_id', true)::uuid` policy.
type TenantScopedQuerier interface {
	Exec(ctx context.Context, sql string, args ...any) error
	QueryRow(ctx context.Context, sql string, args ...any) Row
	Query(ctx context.Context, sql string, args ...any) (Rows, error)
}

// Querier is the minimal Exec + Query + QueryRow surface this package needs
// from pgx, plus the tenant-scoped transaction runner WithTenantTx.
//
// Resilience-priority + multi-tenant-rls: every RLS-protected table access
// (token_usage_ledger, agent_decision_log, budgets, traces_correlation)
// MUST run inside WithTenantTx — the bare Exec/Query/QueryRow run on the
// pool with NO tenant GUC and are therefore RLS-blocked on those tables.
type Querier interface {
	TenantScopedQuerier
	// WithTenantTx opens a transaction, applies `SET LOCAL chora.tenant_id`
	// (tenantID is normalised + validated by the caller — pass a UUID or
	// NilTenantUUID), invokes fn with the tx-scoped querier, and commits.
	// fn MUST fully consume any Rows before returning (the tx closes on
	// return). A non-nil fn error rolls the tx back.
	WithTenantTx(ctx context.Context, tenantID string, fn func(context.Context, TenantScopedQuerier) error) error
}

// validateTenantIDLiteral rejects values unsafe to interpolate into a
// `SET LOCAL` statement (SET LOCAL is not parameterisable). Allows only
// hex/UUID-shaped strings (alphanum + dash) — mirrors
// libs/chora-go-common/rls.validateSafeIdentifier. The "platform" sentinel
// must be NormalizeTenantForRLS'd to NilTenantUUID before reaching here.
func validateTenantIDLiteral(id string) error {
	if id == "" {
		return errors.New("pg: empty tenant_id for SET LOCAL")
	}
	for _, r := range id {
		switch {
		case r >= '0' && r <= '9', r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r == '-':
			continue
		default:
			return fmt.Errorf("pg: tenant_id %q contains forbidden character for SET LOCAL", id)
		}
	}
	return nil
}

// Row is the minimal Scan surface used by the repos.
type Row interface {
	Scan(dest ...any) error
}

// Rows is the minimal multi-row surface.
type Rows interface {
	Next() bool
	Scan(dest ...any) error
	Close()
	Err() error
}

// ErrNoRows is the package-local sentinel for not-found.
var ErrNoRows = errors.New("pg: no rows in result set")

// PgxPoolQuerier wraps a *pgxpool.Pool with the local Querier shape.
type PgxPoolQuerier struct {
	pool *pgxpool.Pool
}

// NewPgxPoolQuerier wraps a pgxpool.Pool.
func NewPgxPoolQuerier(pool *pgxpool.Pool) *PgxPoolQuerier {
	return &PgxPoolQuerier{pool: pool}
}

// Exec runs a non-returning SQL statement.
func (q *PgxPoolQuerier) Exec(ctx context.Context, sql string, args ...any) error {
	if _, err := q.pool.Exec(ctx, sql, args...); err != nil {
		return fmt.Errorf("pg.Exec: %w", err)
	}
	return nil
}

// QueryRow runs a single-row query.
func (q *PgxPoolQuerier) QueryRow(ctx context.Context, sql string, args ...any) Row {
	return &pgxPoolRow{r: q.pool.QueryRow(ctx, sql, args...)}
}

// Query runs a multi-row query.
func (q *PgxPoolQuerier) Query(ctx context.Context, sql string, args ...any) (Rows, error) {
	rows, err := q.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("pg.Query: %w", err)
	}
	return &pgxPoolRows{r: rows}, nil
}

// Pool returns the underlying pool.
func (q *PgxPoolQuerier) Pool() *pgxpool.Pool {
	return q.pool
}

// WithTenantTx opens a transaction, applies `SET LOCAL chora.tenant_id` so
// RLS-protected reads + writes pass the tenant_isolation policy, runs fn,
// then commits. SET LOCAL (not SET) keeps the GUC transaction-scoped — safe
// under PgBouncer transaction-pooling (no cross-request leak).
func (q *PgxPoolQuerier) WithTenantTx(ctx context.Context, tenantID string, fn func(context.Context, TenantScopedQuerier) error) error {
	if err := validateTenantIDLiteral(tenantID); err != nil {
		return err
	}
	tx, err := q.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("pg.WithTenantTx: begin: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
		}
	}()
	// tenantID is validated above (hex/UUID/dash only) — safe to inline.
	// SET LOCAL is parsed by Postgres, not the prepared-statement path.
	if _, err := tx.Exec(ctx, "SET LOCAL chora.tenant_id = '"+tenantID+"'"); err != nil {
		return fmt.Errorf("pg.WithTenantTx: SET LOCAL chora.tenant_id: %w", err)
	}
	if err := fn(ctx, &txQuerier{tx: tx}); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("pg.WithTenantTx: commit: %w", err)
	}
	committed = true
	return nil
}

// WithAppTenantTx + AppTenantQuerier were DELETED here (CHO-2140 completion,
// via CHO-2257's live proof).
//
// They applied `SET LOCAL app.current_tenant_id`, and their godoc asserted that
// the familiar-growth (0007) + analytics (0004) tables keyed their RLS policy on
// that GUC. That was true when written and FALSE from
// 0015_rls_guc_rekey onward: 0015 re-keyed all 5 familiar-growth +
// ritual_run_audit policies to `chora.tenant_id`, and this seam was never
// updated. A live census on 2026-07-17 found ZERO policies in
// chora_observability reading `app.current_tenant_id` (17 of 17 tenant policies
// read `chora.tenant_id`), so every write through this seam set a GUC nothing
// read: the policy evaluated against NULL and Postgres refused the INSERT with
// 42501. familiar_growth_audit_ledger / _daily_metrics / breed_roll_audit /
// egg_funnel_metrics had never held a single row; ritual_run_audit held exactly
// 3, all written on 2026-07-11 BEFORE the re-key.
//
// There is one canonical tenant GUC — `chora.tenant_id` — and one seam that sets
// it: WithTenantTx above. rls_guc_coherence_test.go enforces that every table a
// repository writes has a policy reading the GUC that repository's seam sets, so
// a second GUC cannot be reintroduced silently.
//
// (Unrelated, do not confuse: outbox_events keys on `app.current_tenant` — no
// `_id` — and is deliberately permissive when unset so the dispatcher can run;
// it is written from a bare pool by design, not through this seam.)

// txQuerier adapts a pgx.Tx to TenantScopedQuerier so repository SQL runs
// on the transaction that carries the SET LOCAL chora.tenant_id GUC.
type txQuerier struct{ tx pgx.Tx }

func (t *txQuerier) Exec(ctx context.Context, sql string, args ...any) error {
	if _, err := t.tx.Exec(ctx, sql, args...); err != nil {
		return fmt.Errorf("pg.tx.Exec: %w", err)
	}
	return nil
}

func (t *txQuerier) QueryRow(ctx context.Context, sql string, args ...any) Row {
	return &pgxPoolRow{r: t.tx.QueryRow(ctx, sql, args...)}
}

func (t *txQuerier) Query(ctx context.Context, sql string, args ...any) (Rows, error) {
	rows, err := t.tx.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("pg.tx.Query: %w", err)
	}
	return &pgxPoolRows{r: rows}, nil
}

// Compile-time checks.
var (
	_ Querier             = (*PgxPoolQuerier)(nil)
	_ TenantScopedQuerier = (*txQuerier)(nil)
)

type pgxPoolRow struct{ r pgx.Row }

func (r *pgxPoolRow) Scan(dest ...any) error {
	err := r.r.Scan(dest...)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNoRows
	}
	return err
}

type pgxPoolRows struct{ r pgx.Rows }

func (r *pgxPoolRows) Next() bool             { return r.r.Next() }
func (r *pgxPoolRows) Scan(dest ...any) error { return r.r.Scan(dest...) }
func (r *pgxPoolRows) Close()                 { r.r.Close() }
func (r *pgxPoolRows) Err() error             { return r.r.Err() }
