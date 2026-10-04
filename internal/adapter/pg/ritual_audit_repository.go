// ritual_audit_repository.go — pgx-backed implementation of
// ritualaudit.Repository (ADR-215 / ADR-219 CHO-2016 ritual run audit).
//
// Backs the ritual_run_audit table in chora_observability (migration 0013):
// one row per inbound chora.consumption.familiar.ritual_run_completed.v1 event,
// UNIQUE on source_event_id.
//
// Mirrors pg.FamiliarGrowthRepository: a Querier abstracts the SQL surface;
// tenant-scoped statements run inside WithTenantTx so the migration-0013 RLS
// policy (keyed on the canonical `chora.tenant_id` GUC) passes.
//
// CHO-2140 completion (2026-07-17): this table was born keyed on the legacy
// `app.current_tenant_id` GUC and this repository used the matching
// WithAppTenantTx seam. 0015_rls_guc_rekey re-keyed the policy to
// `chora.tenant_id` on 2026-07-11 and the seam was NOT updated, so every write
// since has been refused with 42501. The table's 3 rows are the fossil record of
// the pre-re-key era — all written 2026-07-11 07:18–11:32, BEFORE the re-key —
// which is exactly why the lane still looked alive. Nothing has been auditable
// here since.
//
// A UNIQUE collision on source_event_id surfaces as
// ritualaudit.ErrDuplicateSourceEvent (idempotent replay — the consumer treats
// it as a no-op).
//
// Cross-DB queries forbidden — chora-observability reads only
// chora_observability (per ddd-enforcement).
package pg

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	ra "github.com/5007-Capstone/chora/services/chora-observability/internal/domain/ritualaudit"
)

// RitualAuditRepository is the pgx-backed ritualaudit.Repository.
type RitualAuditRepository struct {
	q Querier
}

// NewRitualAuditRepository wraps an Querier (PgxPoolQuerier in
// production).
func NewRitualAuditRepository(q Querier) *RitualAuditRepository {
	return &RitualAuditRepository{q: q}
}

// Ingest writes one ritual-run audit row inside a tenant-scoped transaction.
func (r *RitualAuditRepository) Ingest(ctx context.Context, row ra.RitualRunAuditRow) error {
	if strings.TrimSpace(row.AuditID) == "" ||
		strings.TrimSpace(row.TenantID) == "" ||
		strings.TrimSpace(row.SourceEventID) == "" ||
		strings.TrimSpace(row.RunID) == "" {
		return ra.ErrInvalidArgument
	}
	if row.ReceivedAt.IsZero() {
		row.ReceivedAt = time.Now().UTC()
	}
	stampsJSON, err := json.Marshal(orEmptyStamps(row.Stamps))
	if err != nil {
		return fmt.Errorf("pg.RitualAuditRepository.Ingest: marshal stamps: %w", err)
	}

	tenantID := row.TenantID
	return r.q.WithTenantTx(ctx, tenantID, func(ctx context.Context, tx TenantScopedQuerier) error {
		const insert = `
            INSERT INTO ritual_run_audit (
                audit_id, tenant_id, source_topic, source_event_id, run_id,
                ritual_id, familiar_id, owner_gcid, revision_no, trigger_source,
                status, mana_charged, sink_ref, error_text, stamps,
                occurred_at, received_at
            ) VALUES (
                $1, $2, $3, $4, $5,
                NULLIF($6,'')::uuid, NULLIF($7,'')::uuid, NULLIF($8,'')::uuid, $9, $10,
                $11, $12, $13, $14, $15::jsonb,
                $16::timestamptz, $17
            )`
		if err := tx.Exec(ctx, insert,
			row.AuditID, tenantID, row.SourceTopic, row.SourceEventID, row.RunID,
			row.RitualID, row.FamiliarID, row.OwnerGCID, row.RevisionNo, row.TriggerSource,
			row.Status, row.ManaCharged, row.SinkRef, row.ErrorText, string(stampsJSON),
			nullableTime(row.OccurredAt), row.ReceivedAt,
		); err != nil {
			if isUniqueViolationErr(err) {
				return ra.ErrDuplicateSourceEvent
			}
			return fmt.Errorf("pg.RitualAuditRepository.Ingest: insert: %w", err)
		}
		return nil
	})
}

// List returns the tenant's most-recent ritual-run audit rows, received_at DESC.
func (r *RitualAuditRepository) List(ctx context.Context, tenantID string, limit int) ([]ra.RitualRunAuditRow, error) {
	limit, _ = boundLimitOffset(limit, 0)
	const q = `
        SELECT audit_id, tenant_id, source_topic, source_event_id, run_id,
               COALESCE(ritual_id::text, ''), COALESCE(familiar_id::text, ''),
               COALESCE(owner_gcid::text, ''), COALESCE(revision_no, 0),
               COALESCE(trigger_source, ''), status, COALESCE(mana_charged, 0),
               COALESCE(sink_ref, ''), COALESCE(error_text, ''),
               stamps::text, occurred_at, received_at
        FROM ritual_run_audit
        WHERE tenant_id = $1
        ORDER BY received_at DESC
        LIMIT $2`
	out := make([]ra.RitualRunAuditRow, 0)
	err := r.q.WithTenantTx(ctx, tenantID, func(ctx context.Context, tx TenantScopedQuerier) error {
		rows, err := tx.Query(ctx, q, tenantID, limit)
		if err != nil {
			return fmt.Errorf("pg.RitualAuditRepository.List: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var (
				row        ra.RitualRunAuditRow
				stampsJSON string
				occurredAt *time.Time
			)
			if err := rows.Scan(
				&row.AuditID, &row.TenantID, &row.SourceTopic, &row.SourceEventID, &row.RunID,
				&row.RitualID, &row.FamiliarID, &row.OwnerGCID, &row.RevisionNo,
				&row.TriggerSource, &row.Status, &row.ManaCharged,
				&row.SinkRef, &row.ErrorText, &stampsJSON, &occurredAt, &row.ReceivedAt,
			); err != nil {
				return fmt.Errorf("pg.RitualAuditRepository.List scan: %w", err)
			}
			row.Stamps = decodeJSONArray(stampsJSON)
			if occurredAt != nil {
				row.OccurredAt = *occurredAt
			}
			out = append(out, row)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// -----------------------------------------------------------------------------
// helpers (ritual-audit-local; the map helpers live in
// familiar_growth_repository.go)
// -----------------------------------------------------------------------------

func orEmptyStamps(s []map[string]any) []map[string]any {
	if s == nil {
		return []map[string]any{}
	}
	return s
}

func decodeJSONArray(s string) []map[string]any {
	if strings.TrimSpace(s) == "" {
		return []map[string]any{}
	}
	out := []map[string]any{}
	if err := json.Unmarshal([]byte(s), &out); err != nil {
		return []map[string]any{}
	}
	return out
}

// Compile-time check.
var _ ra.Repository = (*RitualAuditRepository)(nil)
