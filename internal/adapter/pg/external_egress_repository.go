// external_egress_repository.go — the projection of the tenant external
// web-egress entitlement into the model-gateway's fail-closed read-copy, plus
// the platform kill-switch (CHO-2148).
//
// SQL contract:
//
//	Project — ONE tenant-scoped transaction:
//	  1. INSERT the audit row (ON CONFLICT (event_id) DO NOTHING).
//	     Written for EVERY valid event, including one discarded as stale below:
//	     the trail is chronological HISTORY, not current state, and an
//	     out-of-order event is still a real decision that happened in tenancy.
//	  2. UPSERT external_egress_policy ... RETURNING source_version, guarded by
//	     `WHERE existing.source_version < EXCLUDED.source_version`.
//	     No row returned => the event is STALE (a version at least as new is
//	     already projected) => applied=false, nil error. The caller ACKs it.
//	     THIS GUARD IS THE WHOLE POINT: Pub/Sub is at-least-once and not
//	     order-preserving, so without it a late "egress ON" would overwrite a
//	     newer "egress OFF" and silently resurrect a revoked entitlement.
//
//	KillSwitch — no tenant scope. platform_egress_killswitch is a platform-global
//	  singleton (PK `singleton BOOLEAN CHECK (singleton)`) with NO RLS, so it
//	  must NOT run inside WithTenantTx.
//
// RLS: external_egress_policy + external_egress_policy_audit are keyed on
// `chora.tenant_id` (migration 0014; 0015's re-key did NOT touch them — they
// were born correct). So the write path uses WithTenantTx — the one canonical
// seam, since CHO-2140 deleted the legacy-GUC variant.
//
// Getting the GUC wrong is not loud in the same way everywhere: an INSERT is
// refused with 42501, but an UPDATE simply affects ZERO ROWS, silently, and the
// gateway keeps reading a stale entitlement. That asymmetry is why this repo's
// rows (2026-07-14, after the re-key) served as a control proving the
// familiar-growth seam was the broken one, not the policies.
package pg

import (
	"context"
	"errors"
	"fmt"

	"github.com/apollo-chora/chora-observability/internal/domain/externalegress"
)

// ExternalEgressRepo projects policy changes into external_egress_policy.
type ExternalEgressRepo struct {
	q Querier
}

// NewExternalEgressRepo constructs the projection repository. Panics on a nil
// querier so a wiring bug fails loud at boot — an unwired projection would drop
// every egress change silently, with nothing to alert on.
func NewExternalEgressRepo(q Querier) *ExternalEgressRepo {
	if q == nil {
		panic("pg.NewExternalEgressRepo: nil Querier")
	}
	return &ExternalEgressRepo{q: q}
}

const insertExternalEgressAuditSQL = `
    INSERT INTO external_egress_policy_audit (
        tenant_id, event_id, source_version,
        egress_enabled, daily_call_ceiling,
        previous_egress_enabled, previous_daily_call_ceiling,
        updated_by_gcid, occurred_at
    )
    VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, $7, $8::uuid, $9)
    ON CONFLICT (event_id) DO NOTHING
`

// upsertExternalEgressPolicySQL — the monotonic guard.
//
// `WHERE external_egress_policy.source_version < EXCLUDED.source_version` means a
// stale event matches no row, RETURNING yields nothing, and we report
// applied=false. Pre-CHO-2148 hand-seeded rows carry source_version=0, so the
// first real event (version >= 1) always supersedes them.
const upsertExternalEgressPolicySQL = `
    INSERT INTO external_egress_policy (
        tenant_id, egress_enabled, daily_call_ceiling, source_version, updated_by_gcid
    )
    VALUES ($1::uuid, $2, $3, $4, $5::uuid)
    ON CONFLICT (tenant_id) DO UPDATE SET
        egress_enabled     = EXCLUDED.egress_enabled,
        daily_call_ceiling = EXCLUDED.daily_call_ceiling,
        source_version     = EXCLUDED.source_version,
        updated_by_gcid    = EXCLUDED.updated_by_gcid
      WHERE external_egress_policy.source_version < EXCLUDED.source_version
    RETURNING source_version
`

// Project applies a policy change. See the file-level SQL contract.
func (r *ExternalEgressRepo) Project(ctx context.Context, ev externalegress.PolicyChanged) (bool, error) {
	if err := ev.Validate(); err != nil {
		return false, err
	}

	applied := false
	err := r.q.WithTenantTx(ctx, ev.TenantID, func(ctx context.Context, tx TenantScopedQuerier) error {
		// 1. Audit — every valid event, stale or not. History, not state.
		if err := tx.Exec(ctx, insertExternalEgressAuditSQL,
			ev.TenantID,
			ev.EventID,
			ev.Version,
			ev.EgressEnabled,
			ev.DailyCallCeiling,
			ev.PreviousEgressEnabled,
			ev.PreviousDailyCallCeiling,
			ev.UpdatedByGCID,
			ev.OccurredAt,
		); err != nil {
			return fmt.Errorf("insert audit row: %w", err)
		}

		// 2. Projection — only when strictly newer.
		var projectedVersion int64
		err := tx.QueryRow(ctx, upsertExternalEgressPolicySQL,
			ev.TenantID,
			ev.EgressEnabled,
			ev.DailyCallCeiling,
			ev.Version,
			ev.UpdatedByGCID,
		).Scan(&projectedVersion)
		if err != nil {
			if errors.Is(err, ErrNoRows) {
				// Stale: a version at least as new is already projected. Correct
				// outcome, not a fault — the caller ACKs.
				applied = false
				return nil
			}
			return fmt.Errorf("upsert policy: %w", err)
		}
		applied = true
		return nil
	})
	if err != nil {
		return false, fmt.Errorf("pg.ExternalEgressRepo.Project: %w", err)
	}
	return applied, nil
}

// Compile-time check.
var _ externalegress.Repository = (*ExternalEgressRepo)(nil)

// ---------------------------------------------------------------------------
// Kill switch — platform-global singleton, NO RLS, NO tenant tx.
// ---------------------------------------------------------------------------

// ErrNoKillSwitchRow — the singleton seed row is missing. Fail loud: the
// model-gateway reads this row on every grounded call, and a missing row would
// leave the platform with no override at all.
var ErrNoKillSwitchRow = errors.New("pg: platform_egress_killswitch singleton row missing")

// ErrKillSwitchNoActor — refusing an anonymous flip.
var ErrKillSwitchNoActor = errors.New("pg: platform egress kill-switch change requires an actor GCID")

// KillSwitchRepo reads and writes the platform egress kill-switch.
type KillSwitchRepo struct {
	q Querier
}

// NewKillSwitchRepo constructs the kill-switch repository.
func NewKillSwitchRepo(q Querier) *KillSwitchRepo {
	if q == nil {
		panic("pg.NewKillSwitchRepo: nil Querier")
	}
	return &KillSwitchRepo{q: q}
}

const selectKillSwitchSQL = `
    SELECT engaged,
           COALESCE(reason, ''),
           COALESCE(updated_by_gcid::text, ''),
           updated_at
      FROM platform_egress_killswitch
     LIMIT 1
`

const updateKillSwitchSQL = `
    UPDATE platform_egress_killswitch
       SET engaged         = $1,
           reason          = NULLIF($2, ''),
           updated_by_gcid = NULLIF($3, '')::uuid
     WHERE singleton = TRUE
    RETURNING engaged,
              COALESCE(reason, ''),
              COALESCE(updated_by_gcid::text, ''),
              updated_at
`

// Get reads the current kill-switch state.
func (r *KillSwitchRepo) Get(ctx context.Context) (externalegress.KillSwitch, error) {
	var ks externalegress.KillSwitch
	err := r.q.QueryRow(ctx, selectKillSwitchSQL).
		Scan(&ks.Engaged, &ks.Reason, &ks.UpdatedByGCID, &ks.UpdatedAt)
	if err != nil {
		if errors.Is(err, ErrNoRows) {
			return externalegress.KillSwitch{}, ErrNoKillSwitchRow
		}
		return externalegress.KillSwitch{}, fmt.Errorf("pg.KillSwitchRepo.Get: %w", err)
	}
	return ks, nil
}

// Set flips the switch. Engaging it denies grounded egress for EVERY tenant, so
// the change must name its actor.
func (r *KillSwitchRepo) Set(ctx context.Context, engaged bool, reason, actorGCID string) (externalegress.KillSwitch, error) {
	if actorGCID == "" {
		return externalegress.KillSwitch{}, ErrKillSwitchNoActor
	}

	var ks externalegress.KillSwitch
	err := r.q.QueryRow(ctx, updateKillSwitchSQL, engaged, reason, actorGCID).
		Scan(&ks.Engaged, &ks.Reason, &ks.UpdatedByGCID, &ks.UpdatedAt)
	if err != nil {
		if errors.Is(err, ErrNoRows) {
			return externalegress.KillSwitch{}, ErrNoKillSwitchRow
		}
		return externalegress.KillSwitch{}, fmt.Errorf("pg.KillSwitchRepo.Set: %w", err)
	}
	return ks, nil
}

// Compile-time check.
var _ externalegress.KillSwitchRepository = (*KillSwitchRepo)(nil)
