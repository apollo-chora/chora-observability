// closure_repository.go — pgx-backed, durable implementation of the
// federated account-closure saga's ClosureRepository port (Tier 3 D11 /
// ADR-184 / ADR-186; W0-F1 durability + W0-F5 error-honesty, CHO-2198).
//
// Replaces events.NewInMemoryClosureRepo — the process-local map whose ack
// state (which (tenant, gcid) pairs this domain has already pseudonymised)
// was lost on every pod restart. That in-memory repo was wired UNGATED:
// gated on `pubsubClient != nil` only, never on pool health (see
// docs/references/w0-f1-inmemory-inventory.md §6 item 4).
//
// SQL contract:
//
//   - Pseudonymise: INSERT ... ON CONFLICT (tenant_id, gcid) DO NOTHING
//     RETURNING id. A returned row means THIS call performed the durable
//     ack (fresh); the conflict-fired path means another call already acked
//     this (tenant, gcid) pair, so this call is an idempotent no-op
//     (mirrors the in-memory repo's `if r.pseudonymed[key] { return 0, nil
//     }` contract, but race-safe: the uniqueness is enforced by Postgres,
//     not a Go-side check-then-set that two concurrent goroutines/pods
//     could both pass).
//   - IsPseudonymised: SELECT 1 ... LIMIT 1. A real backing-store error is
//     returned as an error — NEVER coerced into `false`. This is the
//     entire point of widening the port's signature (see
//     closure_subscriber.go's ClosureRepository doc comment): the original
//     `IsPseudonymised(gcid string) bool` had no way to report a DB error,
//     so ANY pg-backed implementation of that old signature would have had
//     to swallow a connection error into "not yet pseudonymised" — a
//     guard read that fails OPEN.
//
// What this adapter does NOT do: it does not execute any per-table UPDATE
// against token_usage_ledger / agent_decision_log / trace_correlation_index
// / budgets / mana_ledger / cost_anomaly_alerts. Like the in-memory repo it
// replaces, `rows_touched` is a declared-intent count derived from the
// PII_Closure_Map.yaml spec (sum of columns), NOT an actual per-row
// UPDATE-affected count. Real per-table redaction is a separate, deeper gap
// — confirmed identical across all 9 closure-saga services (see CHO-2198
// durability report "no-op Pseudonymise" finding), not something this
// migration/adapter implements.
//
// RLS + tenant-tx contract — DEVIATES from the chora-notifications
// reference adapter: chora-notifications' pg.Querier applies the
// `SET LOCAL chora.tenant_id` GUC transparently inside QueryRow whenever
// the ctx carries a tenant (pg.WithTenantID(ctx, tenantID)). chora-
// observability has no such ctx-carried-GUC seam — its
// pg.PgxPoolQuerier.QueryRow runs a bare, un-scoped query regardless of
// ctx. Tenant scoping here instead goes through the
// pg.Querier.WithTenantTx(ctx, tenantID, fn) callback seam (mirrors
// ledger_repository.go / decision_repository.go): WithTenantTx opens a
// transaction, issues `SET LOCAL chora.tenant_id` on it, then invokes fn
// with a tx-scoped pg.TenantScopedQuerier. Every query below runs inside
// that callback — never on the bare Querier — because
// chora_observability_app_rw is NOBYPASSRLS and
// closure_pseudonymisation_state carries the same tenant_isolation RLS
// policy as token_usage_ledger / agent_decision_log
// (reusable_gotcha_rls_tenant_from_ctx_not_param: a bare `WHERE tenant_id =
// $1` is never trusted on its own to satisfy RLS). This table uses the
// canonical `chora.tenant_id` GUC (WithTenantTx), matching
// token_usage_ledger/agent_decision_log's policy.
//
// CHO-2140 completion (2026-07-17): this comment used to add "NOT the
// `app.current_tenant_id` variant (WithAppTenantTx) that the newer
// familiar-growth/analytics tables use". Both halves are now false — 0015
// re-keyed those policies to `chora.tenant_id`, the WithAppTenantTx seam is
// deleted, and there is exactly ONE tenant GUC + ONE seam. This repository was
// never affected; it is one of the controls that proved the diagnosis (it kept
// writing normally throughout, because it already used WithTenantTx).
//
// Cross-DB queries forbidden — this repo reads only chora_observability.
package pg

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"

	"github.com/5007-Capstone/chora/services/chora-observability/internal/config"
)

// ErrClosureExecutorUnbuilt is returned by Pseudonymise until a real per-table
// executor exists.
//
// ⚠ WHY THIS FAILS CLOSED (disarm, 2026-08-14). Pseudonymise executes no
// per-table UPDATE at all: it writes one durable ack row and reports
// rows_touched as a DECLARED-INTENT count, the sum of columns listed in
// PII_Closure_Map.yaml. Measured across the platform: 9 services carry a closure
// repo, 0 of them contain any UPDATE, over 73 declared tables and 123 columns.
// Measured as never exercised: lifetime inserts on closure_pseudonymisation_state
// are 0.
//
// That is worse than dead code, because closure IS reachable from the admin
// account-lifecycle surface. On the first real closure the subscriber would
// publish status "ok" with a non-zero rows_touched under chora_imda_dimension
// "accountability", and the account would reach ACCOUNT_STATE_PSEUDONYMIZED,
// which the contract defines as "PII fields tokenized per PII_Closure_Map",
// having tokenised nothing. A false compliance attestation; absent code would at
// least have failed loudly.
//
// So the saga fails CLOSED rather than reaching a pseudonymised end state on the
// strength of work nobody did. Real per-table redaction is a separate, deeper
// gap (CHO-2198 "no-op Pseudonymise"); when it lands, this error goes with it.
var ErrClosureExecutorUnbuilt = errors.New(
	"closure: per-table PII_Closure_Map executor is not implemented; " +
		"refusing to report a pseudonymisation that did not happen")

// ClosureRepository is the pgx-backed, durable implementation of the
// events.ClosureRepository port.
type ClosureRepository struct {
	q Querier
}

// NewClosureRepository wraps a Querier (typically *PgxPoolQuerier, which
// satisfies Querier via WithTenantTx). Tests inject a stub.
func NewClosureRepository(q Querier) *ClosureRepository {
	return &ClosureRepository{q: q}
}

// sqlClosurePseudonymiseInsert is the idempotent ack INSERT. RETURNING id
// distinguishes a fresh ack (row scanned) from a conflict (ErrNoRows).
const sqlClosurePseudonymiseInsert = `
        INSERT INTO closure_pseudonymisation_state (id, tenant_id, gcid, rows_touched)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (tenant_id, gcid) DO NOTHING
        RETURNING id
    `

// sqlClosureIsPseudonymisedSelect probes for an existing durable ack.
const sqlClosureIsPseudonymisedSelect = `SELECT 1 FROM closure_pseudonymisation_state WHERE tenant_id = $1 AND gcid = $2 LIMIT 1`

// Pseudonymise durably acks that (tenantID, gcid) has been processed by
// this domain's closure subscriber. Idempotent: a replay (same tenant,
// gcid) is a no-op that returns (0, nil), never a duplicate row or an
// error.
//
// Column order matches
// migrations/0017_closure_pseudonymisation_state.up.sql: id, tenant_id,
// gcid, rows_touched.
func (r *ClosureRepository) Pseudonymise(ctx context.Context, tenantID, gcid string, spec []config.TableSpec) (int, error) {
	// Disarmed until a real executor exists: see ErrClosureExecutorUnbuilt.
	// Placed FIRST so no ack row is written and no success is published.
	return 0, ErrClosureExecutorUnbuilt

	if strings.TrimSpace(tenantID) == "" {
		return 0, errors.New("pg.ClosureRepository.Pseudonymise: tenant_id required")
	}
	if strings.TrimSpace(gcid) == "" {
		return 0, errors.New("pg.ClosureRepository.Pseudonymise: gcid required")
	}
	if r.q == nil {
		return 0, errors.New("pg.ClosureRepository.Pseudonymise: no Querier wired")
	}

	id, err := uuid.NewV7()
	if err != nil {
		return 0, fmt.Errorf("pg.ClosureRepository.Pseudonymise: uuidv7: %w", err)
	}

	// Declared-intent row count from the PII map — see package doc: this
	// adapter does not itself touch token_usage_ledger/agent_decision_log/etc.
	rows := 0
	for _, t := range spec {
		rows += len(t.Columns)
	}

	// tenant_id is normalised ("platform"/"" → NilTenantUUID) so it both
	// satisfies the UUID column AND matches the SET LOCAL chora.tenant_id
	// GUC the RLS tenant_isolation policy checks (mirrors
	// ledger_repository.go's Append).
	normTenant := NormalizeTenantForRLS(tenantID)

	conflict := false
	txErr := r.q.WithTenantTx(ctx, normTenant, func(ctx context.Context, tx TenantScopedQuerier) error {
		var insertedID string
		row := tx.QueryRow(ctx, sqlClosurePseudonymiseInsert, id.String(), normTenant, gcid, rows)
		if err := row.Scan(&insertedID); err != nil {
			if errors.Is(err, ErrNoRows) {
				// ON CONFLICT DO NOTHING fired: another call already
				// durably acked this (tenant, gcid) pair. Idempotent
				// no-op — fail loud only on a GENUINE backing-store
				// error (below).
				conflict = true
				return nil
			}
			return err
		}
		return nil
	})
	if txErr != nil {
		return 0, fmt.Errorf("pg.ClosureRepository.Pseudonymise: %w", txErr)
	}
	if conflict {
		return 0, nil
	}
	return rows, nil
}

// IsPseudonymised reports whether (tenantID, gcid) has already been
// durably acked. A real backing-store error is returned as a non-nil
// error and MUST NOT be treated as `false` by any caller — see the
// ClosureRepository port doc comment in closure_subscriber.go.
func (r *ClosureRepository) IsPseudonymised(ctx context.Context, tenantID, gcid string) (bool, error) {
	if strings.TrimSpace(tenantID) == "" {
		return false, errors.New("pg.ClosureRepository.IsPseudonymised: tenant_id required")
	}
	if strings.TrimSpace(gcid) == "" {
		return false, errors.New("pg.ClosureRepository.IsPseudonymised: gcid required")
	}
	if r.q == nil {
		return false, errors.New("pg.ClosureRepository.IsPseudonymised: no Querier wired")
	}

	normTenant := NormalizeTenantForRLS(tenantID)
	found := false
	txErr := r.q.WithTenantTx(ctx, normTenant, func(ctx context.Context, tx TenantScopedQuerier) error {
		var v int
		row := tx.QueryRow(ctx, sqlClosureIsPseudonymisedSelect, normTenant, gcid)
		if err := row.Scan(&v); err != nil {
			if errors.Is(err, ErrNoRows) {
				return nil
			}
			return err
		}
		found = true
		return nil
	})
	if txErr != nil {
		return false, fmt.Errorf("pg.ClosureRepository.IsPseudonymised: %w", txErr)
	}
	return found, nil
}
