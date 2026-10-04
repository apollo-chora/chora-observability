// external_egress_repository_test.go — pgx adapter specs for the external-egress
// projection + platform kill-switch (CHO-2148).
//
// The monotonic guard is the load-bearing behaviour. Pub/Sub is at-least-once
// and not order-preserving, so the projection MUST discard any event that is not
// strictly newer than what it already holds — otherwise a late "egress ON" could
// silently resurrect an entitlement a tenant just revoked, and the model-gateway
// would keep letting that tenant's learners reach the open web.
package pg_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/5007-Capstone/chora/services/chora-observability/internal/adapter/pg"
	"github.com/5007-Capstone/chora/services/chora-observability/internal/domain/externalegress"
)

const (
	egTenant = "11111111-1111-7111-8111-111111111111"
	egActor  = "00000000-0000-7000-8000-000000001999"
	egEvent  = "01980000-0000-7000-8000-aaaaaaaaaaaa"
)

// --- stub tenant-tx querier ------------------------------------------------

type egStubQuerier struct {
	tenantID  string
	txCalled  int
	execs     []string
	execArgs  [][]any
	rowSQL    []string
	rowScan   []func(dest ...any) error
	bareRow   func(dest ...any) error
	bareExecs []string
}

func (s *egStubQuerier) Exec(_ context.Context, sql string, args ...any) error {
	s.execs = append(s.execs, sql)
	s.execArgs = append(s.execArgs, args)
	return nil
}

func (s *egStubQuerier) QueryRow(_ context.Context, sql string, args ...any) pg.Row {
	s.rowSQL = append(s.rowSQL, sql)
	if len(s.rowScan) > 0 {
		f := s.rowScan[0]
		s.rowScan = s.rowScan[1:]
		return &egStubRow{fn: f}
	}
	if s.bareRow != nil {
		return &egStubRow{fn: s.bareRow}
	}
	return &egStubRow{fn: func(_ ...any) error { return pg.ErrNoRows }}
}

func (s *egStubQuerier) Query(_ context.Context, _ string, _ ...any) (pg.Rows, error) {
	return nil, nil
}

func (s *egStubQuerier) WithTenantTx(ctx context.Context, tenantID string, fn func(context.Context, pg.TenantScopedQuerier) error) error {
	s.txCalled++
	s.tenantID = tenantID
	return fn(ctx, s)
}

type egStubRow struct{ fn func(dest ...any) error }

func (r *egStubRow) Scan(dest ...any) error { return r.fn(dest...) }

func scanVersion(v int64) func(dest ...any) error {
	return func(dest ...any) error {
		if len(dest) > 0 {
			if p, ok := dest[0].(*int64); ok {
				*p = v
			}
		}
		return nil
	}
}

func validEvent() externalegress.PolicyChanged {
	return externalegress.PolicyChanged{
		EventID:                  egEvent,
		TenantID:                 egTenant,
		EgressEnabled:            true,
		DailyCallCeiling:         50,
		Version:                  3,
		UpdatedByGCID:            egActor,
		PreviousEgressEnabled:    false,
		PreviousDailyCallCeiling: 50,
		OccurredAt:               time.Now().UTC(),
	}
}

// --- Project ---------------------------------------------------------------

func TestExternalEgressRepo_Project_NewerEvent_AppliesAndAudits(t *testing.T) {
	t.Parallel()
	q := &egStubQuerier{rowScan: []func(dest ...any) error{scanVersion(3)}}
	repo := pg.NewExternalEgressRepo(q)

	applied, err := repo.Project(context.Background(), validEvent())
	if err != nil {
		t.Fatalf("Project: %v", err)
	}
	if !applied {
		t.Fatal("a strictly-newer event must be APPLIED")
	}
	if q.txCalled != 1 {
		t.Fatalf("WithTenantTx called %d times, want 1 — the projection and the audit "+
			"row must share ONE transaction", q.txCalled)
	}
	if q.tenantID != egTenant {
		t.Errorf("tx tenant = %q, want %q (SET LOCAL chora.tenant_id scopes the RLS write)",
			q.tenantID, egTenant)
	}

	// The audit row is written for every valid event.
	if len(q.execs) != 1 || !strings.Contains(q.execs[0], "external_egress_policy_audit") {
		t.Fatalf("expected exactly one audit INSERT, got %v", q.execs)
	}
	// The policy upsert RETURNs the version (the OCC / staleness probe).
	if len(q.rowSQL) != 1 || !strings.Contains(q.rowSQL[0], "external_egress_policy") {
		t.Fatalf("expected the policy upsert as a QueryRow, got %v", q.rowSQL)
	}
	if !strings.Contains(q.rowSQL[0], "source_version") {
		t.Error("the upsert must guard on source_version — without it, an out-of-order " +
			"redelivery could resurrect a revoked entitlement")
	}
}

// The upsert's `WHERE existing.source_version < EXCLUDED.source_version` matched
// no row: a newer (or equal) policy is already projected.
func TestExternalEgressRepo_Project_StaleEvent_NotApplied_NoError(t *testing.T) {
	t.Parallel()
	q := &egStubQuerier{rowScan: []func(dest ...any) error{
		func(_ ...any) error { return pg.ErrNoRows },
	}}
	repo := pg.NewExternalEgressRepo(q)

	applied, err := repo.Project(context.Background(), validEvent())
	if err != nil {
		t.Fatalf("a stale event must NOT be an error (it gets ACKed): %v", err)
	}
	if applied {
		t.Fatal("a stale event must NOT be applied — it would overwrite a newer policy")
	}
}

// Even a stale (out-of-order) event is a REAL decision that happened in
// chora-tenancy. The trail is chronological history, not current state, so it
// must still be recorded — deduped on event_id.
func TestExternalEgressRepo_Project_StaleEvent_StillAudited(t *testing.T) {
	t.Parallel()
	q := &egStubQuerier{rowScan: []func(dest ...any) error{
		func(_ ...any) error { return pg.ErrNoRows },
	}}
	repo := pg.NewExternalEgressRepo(q)

	if _, err := repo.Project(context.Background(), validEvent()); err != nil {
		t.Fatalf("Project: %v", err)
	}
	if len(q.execs) != 1 || !strings.Contains(q.execs[0], "external_egress_policy_audit") {
		t.Fatalf("a stale-but-valid event must still be audited — the trail is history, "+
			"not current state; got %v", q.execs)
	}
}

func TestExternalEgressRepo_Project_AuditCarriesActorAndBeforeAfter(t *testing.T) {
	t.Parallel()
	q := &egStubQuerier{rowScan: []func(dest ...any) error{scanVersion(3)}}
	repo := pg.NewExternalEgressRepo(q)

	if _, err := repo.Project(context.Background(), validEvent()); err != nil {
		t.Fatalf("Project: %v", err)
	}

	var joined strings.Builder
	for _, a := range q.execArgs[0] {
		if s, ok := a.(string); ok {
			joined.WriteString(s)
			joined.WriteString("|")
		}
	}
	if !strings.Contains(joined.String(), egActor) {
		t.Errorf("the audit row must name the actor GCID (IMDA D1); args = %s", joined.String())
	}
	if !strings.Contains(joined.String(), egEvent) {
		t.Errorf("the audit row must carry event_id for dedupe; args = %s", joined.String())
	}
}

// --- KillSwitch ------------------------------------------------------------

func TestKillSwitchRepo_Get_ReadsSingleton(t *testing.T) {
	t.Parallel()
	q := &egStubQuerier{bareRow: func(dest ...any) error {
		// engaged, reason, updated_by_gcid, updated_at
		*(dest[0].(*bool)) = true
		*(dest[1].(*string)) = "incident-42"
		*(dest[2].(*string)) = egActor
		*(dest[3].(*time.Time)) = time.Now().UTC()
		return nil
	}}
	repo := pg.NewKillSwitchRepo(q)

	ks, err := repo.Get(context.Background())
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !ks.Engaged {
		t.Error("Engaged = false, want true")
	}
	if ks.Reason != "incident-42" {
		t.Errorf("Reason = %q, want incident-42", ks.Reason)
	}
	if q.txCalled != 0 {
		t.Error("the kill-switch is a platform-global singleton with NO RLS — " +
			"it must not run inside a tenant-scoped tx")
	}
}

func TestKillSwitchRepo_Set_RequiresActor(t *testing.T) {
	t.Parallel()
	q := &egStubQuerier{}
	repo := pg.NewKillSwitchRepo(q)

	_, err := repo.Set(context.Background(), true, "incident", "")
	if err == nil {
		t.Fatal("engaging the kill-switch with no actor must be refused — disabling " +
			"web egress for the ENTIRE platform is never anonymous")
	}
}
