// companion_suspension_repository.go: the ADR-252 operator write path over the
// two containment tables (migration 0018) plus the governance audit event
// (ADR-252 D4), written into outbox_events in the SAME transaction as the row.
//
// SQL contract:
//
//	Engage: ONE transaction (platform scope under the platform sentinel tenant,
//	  tenant scope under SET LOCAL chora.tenant_id = tenant, RLS enforcing):
//	  1. SELECT an identical ENGAGED row (same skill / all-skills) -> if found,
//	     return it: idempotent, no new row, no new event.
//	  2. INSERT the row (id = UUIDv7 minted here, version = 1).
//	  3. INSERT the outbox row for chora.governance.audit.companion_suspension_
//	     changed.v1 (payload = protojson of governance.v1.CompanionSuspensionChanged,
//	     envelope = the flat string map the dispatcher reconstructs into Pub/Sub
//	     attributes; idempotency_key = "<id>:<version>").
//	  Any failure rolls the whole thing back: a containment that cannot be
//	  audited is not engaged silently.
//	Release: ONE transaction: UPDATE ... WHERE engaged AND key matches
//	  SET engaged = FALSE, released_by/at, version = version + 1 RETURNING ...,
//	  then one outbox row per released row. Release is a state change, never a
//	  DELETE (the history survives).
//
// The platform table has NO RLS (a platform control, sibling of
// platform_egress_killswitch); the tenant table is read and written under the
// tenant GUC, which is what keeps one tenant's admin from seeing another's rows.
package pg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/locoroco-git/Chora-LMS/chora-contracts/gen/go/chora/common/v1"
	governancev1 "github.com/locoroco-git/Chora-LMS/chora-contracts/gen/go/chora/governance/v1"

	cgctracing "github.com/5007-Capstone/chora/libs/chora-go-common/tracing"
	cs "github.com/5007-Capstone/chora/services/chora-observability/internal/domain/companionsuspension"

	"github.com/5007-Capstone/chora/libs/chora-go-common/env"
)

// TopicCompanionSuspensionChanged is the ADR-252 D4 audit topic (ADR-254 D4:
// JSON-wire, schema: null in topics.yaml, provisioned at G0).
const TopicCompanionSuspensionChanged = "chora.governance.audit.companion_suspension_changed.v1"

const (
	companionSuspensionSourceService = "chora-observability"
	companionSuspensionSchemaVersion = 1
	// IMDA dimension of a human-oversight control (ADR-141 canonical label).
	companionSuspensionImdaDimension = "fairness_and_human_oversight"
	companionSuspensionImdaStage     = "runtime"
)

//
// Was a hardcoded literal until 2026-09-02 (CHO-2419). No manifest could reach
// it, so a second org stamped every event with chora-489812 and any consumer
// filtering on source_project would have been filtering on a lie.
// CHORA_SOURCE_PROJECT is now set on every event-emitting service; the literal
// stays as the fallback so this estate is provably unchanged.
var companionSuspensionSourceProject = env.GetOrDefault("CHORA_SOURCE_PROJECT", "chora-489812")

// CompanionSuspensionRepoOptions injects the clock and the id minter (tests pin
// both; production leaves them nil).
type CompanionSuspensionRepoOptions struct {
	// NewID mints a UUIDv7 for new rows and events. nil -> uuid.NewV7.
	NewID func() (string, error)
	// Now is the write clock. nil -> time.Now().UTC.
	Now func() time.Time
}

// CompanionSuspensionRepo implements companionsuspension.Repository.
type CompanionSuspensionRepo struct {
	q     Querier
	newID func() (string, error)
	now   func() time.Time
}

// NewCompanionSuspensionRepo constructs the repository. Panics on a nil querier
// so a wiring bug fails loud at boot: an unwired write path would leave the
// operator with no containment control and nothing to alert on.
func NewCompanionSuspensionRepo(q Querier, opts CompanionSuspensionRepoOptions) *CompanionSuspensionRepo {
	if q == nil {
		panic("pg.NewCompanionSuspensionRepo: nil Querier")
	}
	r := &CompanionSuspensionRepo{q: q, newID: opts.NewID, now: opts.Now}
	if r.newID == nil {
		r.newID = func() (string, error) {
			id, err := uuid.NewV7()
			if err != nil {
				return "", err
			}
			return id.String(), nil
		}
	}
	if r.now == nil {
		r.now = func() time.Time { return time.Now().UTC() }
	}
	return r
}

// ---------------------------------------------------------------------------
// SQL
// ---------------------------------------------------------------------------

const selectEngagedPlatformSQL = `
    SELECT id::text, reason, engaged_by::text, engaged_at, version
      FROM platform_companion_suspension
     WHERE engaged = TRUE
       AND skill_key IS NOT DISTINCT FROM NULLIF($1, '')
     ORDER BY engaged_at DESC
     LIMIT 1`

const selectEngagedTenantSQL = `
    SELECT id::text, reason, engaged_by::text, engaged_at, version
      FROM companion_suspension_policy
     WHERE tenant_id = $1::uuid
       AND engaged = TRUE
       AND skill_key IS NOT DISTINCT FROM NULLIF($2, '')
     ORDER BY engaged_at DESC
     LIMIT 1`

const insertPlatformSQL = `
    INSERT INTO platform_companion_suspension (id, skill_key, engaged, reason, engaged_by, engaged_at, version)
    VALUES ($1::uuid, NULLIF($2, ''), TRUE, $3, $4::uuid, $5, 1)
    RETURNING engaged_at`

const insertTenantSQL = `
    INSERT INTO companion_suspension_policy (id, tenant_id, skill_key, engaged, reason, engaged_by, engaged_at, version)
    VALUES ($1::uuid, $2::uuid, NULLIF($3, ''), TRUE, $4, $5::uuid, $6, 1)
    RETURNING engaged_at`

const releasePlatformSQL = `
    UPDATE platform_companion_suspension
       SET engaged = FALSE, released_by = $1::uuid, released_at = $2, version = version + 1
     WHERE engaged = TRUE
       AND skill_key IS NOT DISTINCT FROM NULLIF($3, '')
    RETURNING id::text, COALESCE(skill_key, ''), reason, engaged_by::text, engaged_at, released_at, version`

const releaseTenantSQL = `
    UPDATE companion_suspension_policy
       SET engaged = FALSE, released_by = $1::uuid, released_at = $2, version = version + 1
     WHERE tenant_id = $3::uuid
       AND engaged = TRUE
       AND skill_key IS NOT DISTINCT FROM NULLIF($4, '')
    RETURNING id::text, COALESCE(skill_key, ''), reason, engaged_by::text, engaged_at, released_at, version`

const listPlatformSQL = `
    SELECT id::text, COALESCE(skill_key, ''), engaged, reason, engaged_by::text, engaged_at,
           COALESCE(released_by::text, ''), COALESCE(released_at, 'epoch'::timestamptz), version
      FROM platform_companion_suspension
     ORDER BY engaged DESC, engaged_at DESC
     LIMIT 200`

const listTenantSQL = `
    SELECT id::text, COALESCE(skill_key, ''), engaged, reason, engaged_by::text, engaged_at,
           COALESCE(released_by::text, ''), COALESCE(released_at, 'epoch'::timestamptz), version
      FROM companion_suspension_policy
     WHERE tenant_id = $1::uuid
     ORDER BY engaged DESC, engaged_at DESC
     LIMIT 200`

// insertOutboxSQL mirrors outbox.PostgresStore.Insert column for column, so the
// shared dispatcher drains this row like any other (status pending, published
// by row.Topic).
const insertOutboxSQL = `
    INSERT INTO outbox_events (
        id, tenant_id, gcid, agid, event_type, topic,
        payload, envelope, idempotency_key, occurred_at,
        aggregate_type, aggregate_id, status
    ) VALUES (
        $1, $2, $3, '', $4, $5, $6, $7::jsonb, $8, $9, 'companion_suspension', $10, 'pending'
    )
    ON CONFLICT (idempotency_key) DO NOTHING`

// ---------------------------------------------------------------------------
// Engage
// ---------------------------------------------------------------------------

// Engage: see the file-level SQL contract.
func (r *CompanionSuspensionRepo) Engage(ctx context.Context, req cs.EngageRequest) (cs.Suspension, error) {
	if err := req.Validate(); err != nil {
		return cs.Suspension{}, err
	}
	txTenant := NilTenantUUID
	if req.Scope == cs.ScopeTenant {
		txTenant = req.TenantID
	}
	now := r.now()
	var out cs.Suspension
	err := r.q.WithTenantTx(ctx, txTenant, func(ctx context.Context, tx TenantScopedQuerier) error {
		// 1. Identical engaged row -> idempotent return.
		var existing cs.Suspension
		var scanErr error
		if req.Scope == cs.ScopePlatform {
			scanErr = tx.QueryRow(ctx, selectEngagedPlatformSQL, req.SkillKey).
				Scan(&existing.ID, &existing.Reason, &existing.EngagedBy, &existing.EngagedAt, &existing.Version)
		} else {
			scanErr = tx.QueryRow(ctx, selectEngagedTenantSQL, req.TenantID, req.SkillKey).
				Scan(&existing.ID, &existing.Reason, &existing.EngagedBy, &existing.EngagedAt, &existing.Version)
		}
		switch {
		case scanErr == nil:
			existing.Scope = req.Scope
			existing.TenantID = req.TenantID
			existing.SkillKey = req.SkillKey
			existing.Engaged = true
			out = existing
			return nil
		case errors.Is(scanErr, ErrNoRows):
			// fall through to insert
		default:
			return fmt.Errorf("select engaged suspension: %w", scanErr)
		}

		// 2. Insert the row.
		id, err := r.newID()
		if err != nil {
			return fmt.Errorf("mint suspension id: %w", err)
		}
		var engagedAt time.Time
		if req.Scope == cs.ScopePlatform {
			err = tx.QueryRow(ctx, insertPlatformSQL, id, req.SkillKey, req.Reason, req.ActorGCID, now).Scan(&engagedAt)
		} else {
			err = tx.QueryRow(ctx, insertTenantSQL, id, req.TenantID, req.SkillKey, req.Reason, req.ActorGCID, now).Scan(&engagedAt)
		}
		if err != nil {
			return fmt.Errorf("insert suspension: %w", err)
		}
		out = cs.Suspension{
			ID: id, Scope: req.Scope, TenantID: req.TenantID, SkillKey: req.SkillKey,
			Engaged: true, Reason: req.Reason, EngagedBy: req.ActorGCID, EngagedAt: engagedAt, Version: 1,
		}

		// 3. Audit event in the same transaction.
		return r.enqueueChanged(ctx, tx, cs.ChangedEventFor(out, req.Reason, req.ActorGCID, "", now))
	})
	if err != nil {
		return cs.Suspension{}, fmt.Errorf("pg.CompanionSuspensionRepo.Engage: %w", err)
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// Release
// ---------------------------------------------------------------------------

// Release: see the file-level SQL contract.
func (r *CompanionSuspensionRepo) Release(ctx context.Context, req cs.ReleaseRequest) ([]cs.Suspension, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}
	txTenant := NilTenantUUID
	if req.Scope == cs.ScopeTenant {
		txTenant = req.TenantID
	}
	now := r.now()
	var released []cs.Suspension
	err := r.q.WithTenantTx(ctx, txTenant, func(ctx context.Context, tx TenantScopedQuerier) error {
		var (
			rows Rows
			err  error
		)
		if req.Scope == cs.ScopePlatform {
			rows, err = tx.Query(ctx, releasePlatformSQL, req.ActorGCID, now, req.SkillKey)
		} else {
			rows, err = tx.Query(ctx, releaseTenantSQL, req.ActorGCID, now, req.TenantID, req.SkillKey)
		}
		if err != nil {
			return fmt.Errorf("release suspensions: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			s := cs.Suspension{Scope: req.Scope, TenantID: req.TenantID, Engaged: false, ReleasedBy: req.ActorGCID}
			if err := rows.Scan(&s.ID, &s.SkillKey, &s.Reason, &s.EngagedBy, &s.EngagedAt, &s.ReleasedAt, &s.Version); err != nil {
				return fmt.Errorf("scan released row: %w", err)
			}
			released = append(released, s)
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("iterate released rows: %w", err)
		}
		for _, s := range released {
			if err := r.enqueueChanged(ctx, tx, cs.ChangedEventFor(s, req.Reason, req.ActorGCID, "", now)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("pg.CompanionSuspensionRepo.Release: %w", err)
	}
	return released, nil
}

// ---------------------------------------------------------------------------
// List
// ---------------------------------------------------------------------------

// ListPlatform: platform rows, engaged first, newest first (no RLS).
func (r *CompanionSuspensionRepo) ListPlatform(ctx context.Context) ([]cs.Suspension, error) {
	var out []cs.Suspension
	err := r.q.WithTenantTx(ctx, NilTenantUUID, func(ctx context.Context, tx TenantScopedQuerier) error {
		rows, err := tx.Query(ctx, listPlatformSQL)
		if err != nil {
			return err
		}
		defer rows.Close()
		out, err = scanSuspensions(rows, cs.ScopePlatform, "")
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("pg.CompanionSuspensionRepo.ListPlatform: %w", err)
	}
	return out, nil
}

// ListTenant: the tenant's rows under RLS, engaged first, newest first.
func (r *CompanionSuspensionRepo) ListTenant(ctx context.Context, tenantID string) ([]cs.Suspension, error) {
	if tenantID == "" {
		return nil, cs.ErrEmptyTenantID
	}
	var out []cs.Suspension
	err := r.q.WithTenantTx(ctx, tenantID, func(ctx context.Context, tx TenantScopedQuerier) error {
		rows, err := tx.Query(ctx, listTenantSQL, tenantID)
		if err != nil {
			return err
		}
		defer rows.Close()
		out, err = scanSuspensions(rows, cs.ScopeTenant, tenantID)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("pg.CompanionSuspensionRepo.ListTenant: %w", err)
	}
	return out, nil
}

func scanSuspensions(rows Rows, scope cs.Scope, tenantID string) ([]cs.Suspension, error) {
	var out []cs.Suspension
	for rows.Next() {
		s := cs.Suspension{Scope: scope, TenantID: tenantID}
		var releasedAt time.Time
		if err := rows.Scan(&s.ID, &s.SkillKey, &s.Engaged, &s.Reason, &s.EngagedBy, &s.EngagedAt, &s.ReleasedBy, &releasedAt, &s.Version); err != nil {
			return nil, fmt.Errorf("scan suspension: %w", err)
		}
		if !s.Engaged {
			s.ReleasedAt = releasedAt
		}
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate suspensions: %w", err)
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// Audit event (same transaction)
// ---------------------------------------------------------------------------

func (r *CompanionSuspensionRepo) enqueueChanged(ctx context.Context, tx TenantScopedQuerier, ev cs.ChangedEvent) error {
	if ev.EventID == "" {
		id, err := r.newID()
		if err != nil {
			return fmt.Errorf("mint audit event id: %w", err)
		}
		ev.EventID = id
	}
	envTenant := ev.TenantID
	if envTenant == "" {
		envTenant = NilTenantUUID
	}
	traceparent := cgctracing.EnsureTraceparent(cgctracing.TraceparentFromContext(ctx))

	msg := &governancev1.CompanionSuspensionChanged{
		Envelope: &commonv1.EventEnvelope{
			EventId:            ev.EventID,
			IdempotencyKey:     ev.IdempotencyKey(),
			TenantId:           envTenant,
			Gcid:               ev.ActorGCID,
			OccurredAt:         timestamppb.New(ev.ChangedAt),
			PublishedAt:        timestamppb.New(ev.ChangedAt),
			Traceparent:        traceparent,
			SourceProject:      companionSuspensionSourceProject,
			SourceService:      companionSuspensionSourceService,
			SchemaVersion:      companionSuspensionSchemaVersion,
			ChoraImdaDimension: companionSuspensionImdaDimension,
			ImdaLifecycleStage: companionSuspensionImdaStage,
		},
		SuspensionId: ev.SuspensionID,
		Scope:        string(ev.Scope),
		TenantId:     ev.TenantID,
		SkillKey:     ev.SkillKey,
		Engaged:      ev.Engaged,
		Version:      ev.Version,
		Reason:       ev.Reason,
		ActorGcid:    ev.ActorGCID,
		ChangedAt:    timestamppb.New(ev.ChangedAt),
	}
	// EmitUnpopulated: the JSON wire must say `"engaged": false` on a release
	// explicitly (protojson otherwise omits default values, and an absent field
	// is exactly the ambiguity a projection must not have to resolve).
	payload, err := protojson.MarshalOptions{EmitUnpopulated: true}.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal companion_suspension_changed: %w", err)
	}
	envelope := map[string]string{
		"event_id":             ev.EventID,
		"idempotency_key":      ev.IdempotencyKey(),
		"tenant_id":            envTenant,
		"gcid":                 ev.ActorGCID,
		"occurred_at":          ev.ChangedAt.UTC().Format(time.RFC3339Nano),
		"published_at":         ev.ChangedAt.UTC().Format(time.RFC3339Nano),
		"traceparent":          traceparent,
		"tracestate":           "",
		"source_project":       companionSuspensionSourceProject,
		"source_service":       companionSuspensionSourceService,
		"schema_version":       strconv.Itoa(companionSuspensionSchemaVersion),
		"chora_imda_dimension": companionSuspensionImdaDimension,
		"imda_lifecycle_stage": companionSuspensionImdaStage,
		"event_topic":          TopicCompanionSuspensionChanged,
	}
	envJSON, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("marshal envelope: %w", err)
	}
	if err := tx.Exec(ctx, insertOutboxSQL,
		ev.EventID, envTenant, ev.ActorGCID,
		TopicCompanionSuspensionChanged, TopicCompanionSuspensionChanged,
		payload, string(envJSON), ev.IdempotencyKey(), ev.ChangedAt.UTC(),
		ev.SuspensionID,
	); err != nil {
		return fmt.Errorf("enqueue companion_suspension_changed: %w", err)
	}
	return nil
}

// Compile-time check.
var _ cs.Repository = (*CompanionSuspensionRepo)(nil)
