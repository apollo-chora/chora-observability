// companion_suspension_repository_test.go: pgx adapter specs for the ADR-252
// operator write path (ADR-254 D7/D11). The load-bearing behaviour is that the
// suspension row and its governance audit event are written in ONE transaction
// (the outbox row rides the same tx, the dispatcher publishes it by topic), and
// that an identical engaged row is returned rather than duplicated.
package pg_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/5007-Capstone/chora/services/chora-observability/internal/adapter/pg"
	cs "github.com/5007-Capstone/chora/services/chora-observability/internal/domain/companionsuspension"
)

const (
	csTenant = "11111111-1111-7111-8111-111111111111"
	csActor  = "00000000-0000-7000-8000-000000001999"
	csID     = "01990000-0000-7000-8000-00000000c0de"
	csTopic  = "chora.governance.audit.companion_suspension_changed.v1"
)

// --- stub querier ---------------------------------------------------------------

type csStubRow struct{ fn func(dest ...any) error }

func (r *csStubRow) Scan(dest ...any) error { return r.fn(dest...) }

type csStubRows struct {
	rows [][]any
	i    int
}

func (r *csStubRows) Next() bool { return r.i < len(r.rows) }
func (r *csStubRows) Scan(dest ...any) error {
	row := r.rows[r.i]
	r.i++
	for k := range dest {
		switch d := dest[k].(type) {
		case *string:
			*d = row[k].(string)
		case *bool:
			*d = row[k].(bool)
		case *int64:
			*d = row[k].(int64)
		case *time.Time:
			*d = row[k].(time.Time)
		}
	}
	return nil
}
func (r *csStubRows) Close()     {}
func (r *csStubRows) Err() error { return nil }

type csStubQuerier struct {
	txTenant string
	txCalled int
	execSQL  []string
	execArgs [][]any
	rowSQL   []string
	rowArgs  [][]any
	rowScan  []func(dest ...any) error // consumed in order; default ErrNoRows
	querySQL []string
	queryRes []*csStubRows // consumed in order
	execErr  error
}

func (s *csStubQuerier) Exec(_ context.Context, sql string, args ...any) error {
	s.execSQL = append(s.execSQL, sql)
	s.execArgs = append(s.execArgs, args)
	return s.execErr
}

func (s *csStubQuerier) QueryRow(_ context.Context, sql string, args ...any) pg.Row {
	s.rowSQL = append(s.rowSQL, sql)
	s.rowArgs = append(s.rowArgs, args)
	if len(s.rowScan) > 0 {
		f := s.rowScan[0]
		s.rowScan = s.rowScan[1:]
		return &csStubRow{fn: f}
	}
	return &csStubRow{fn: func(_ ...any) error { return pg.ErrNoRows }}
}

func (s *csStubQuerier) Query(_ context.Context, sql string, _ ...any) (pg.Rows, error) {
	s.querySQL = append(s.querySQL, sql)
	if len(s.queryRes) > 0 {
		r := s.queryRes[0]
		s.queryRes = s.queryRes[1:]
		return r, nil
	}
	return &csStubRows{}, nil
}

func (s *csStubQuerier) WithTenantTx(ctx context.Context, tenantID string, fn func(context.Context, pg.TenantScopedQuerier) error) error {
	s.txCalled++
	s.txTenant = tenantID
	return fn(ctx, s)
}

func newCSRepo(q *csStubQuerier) *pg.CompanionSuspensionRepo {
	return pg.NewCompanionSuspensionRepo(q, pg.CompanionSuspensionRepoOptions{
		NewID: func() (string, error) { return csID, nil },
		Now:   func() time.Time { return time.Date(2026, 8, 22, 15, 0, 0, 0, time.UTC) },
	})
}

// scanEngagedAt answers the INSERT ... RETURNING engaged_at read.
func scanEngagedAt(at time.Time) func(dest ...any) error {
	return func(dest ...any) error {
		if p, ok := dest[0].(*time.Time); ok {
			*p = at
		}
		return nil
	}
}

func outboxExecs(q *csStubQuerier) [][]any {
	var out [][]any
	for i, sql := range q.execSQL {
		if strings.Contains(sql, "INSERT INTO outbox_events") {
			out = append(out, q.execArgs[i])
		}
	}
	return out
}

func decodePayload(t *testing.T, args []any) map[string]any {
	t.Helper()
	var payload []byte
	for _, a := range args {
		if b, ok := a.([]byte); ok {
			payload = b
		}
	}
	if payload == nil {
		t.Fatal("outbox insert carries no []byte payload")
	}
	var m map[string]any
	if err := json.Unmarshal(payload, &m); err != nil {
		t.Fatalf("payload is not JSON (ADR-254 D4 JSON-wire): %v", err)
	}
	return m
}

// --- Engage ---------------------------------------------------------------------

func TestCompanionSuspensionRepo_Engage_Platform_WritesRowAndAuditInOneTx(t *testing.T) {
	q := &csStubQuerier{rowScan: []func(dest ...any) error{
		func(_ ...any) error { return pg.ErrNoRows }, // no identical engaged row
		scanEngagedAt(time.Date(2026, 8, 22, 15, 0, 0, 0, time.UTC)),
	}}
	repo := newCSRepo(q)

	s, err := repo.Engage(context.Background(), cs.EngageRequest{Scope: cs.ScopePlatform, Reason: "incident 42", ActorGCID: csActor})
	if err != nil {
		t.Fatalf("Engage: %v", err)
	}
	if q.txCalled != 1 || q.txTenant != pg.NilTenantUUID {
		t.Fatalf("platform scope must run in ONE tx under the platform sentinel, got calls=%d tenant=%q", q.txCalled, q.txTenant)
	}
	if s.ID != csID || s.Scope != cs.ScopePlatform || !s.Engaged || s.Reason != "incident 42" || s.EngagedBy != csActor || s.Version != 1 {
		t.Fatalf("returned suspension wrong: %+v", s)
	}
	if len(q.rowSQL) < 2 || !strings.Contains(q.rowSQL[1], "INSERT INTO platform_companion_suspension") {
		t.Fatalf("expected the platform INSERT, got %v", q.rowSQL)
	}
	ob := outboxExecs(q)
	if len(ob) != 1 {
		t.Fatalf("exactly one outbox row per engage, got %d", len(ob))
	}
	var sawTopic bool
	for _, a := range ob[0] {
		if str, ok := a.(string); ok && str == csTopic {
			sawTopic = true
		}
	}
	if !sawTopic {
		t.Fatalf("outbox row must carry topic %s, args=%v", csTopic, ob[0])
	}
	p := decodePayload(t, ob[0])
	if p["scope"] != "platform" || p["engaged"] != true || p["reason"] != "incident 42" || p["actorGcid"] != csActor || p["suspensionId"] != csID {
		t.Fatalf("audit payload missing fields: %v", p)
	}
}

func TestCompanionSuspensionRepo_Engage_Tenant_RunsUnderTenantTxWithTenantColumn(t *testing.T) {
	q := &csStubQuerier{rowScan: []func(dest ...any) error{
		func(_ ...any) error { return pg.ErrNoRows },
		scanEngagedAt(time.Now().UTC()),
	}}
	repo := newCSRepo(q)

	s, err := repo.Engage(context.Background(), cs.EngageRequest{Scope: cs.ScopeTenant, TenantID: csTenant, SkillKey: "companion_chat_turn_basic", Reason: "drift", ActorGCID: csActor})
	if err != nil {
		t.Fatalf("Engage: %v", err)
	}
	if q.txTenant != csTenant {
		t.Fatalf("tenant scope must run under SET LOCAL chora.tenant_id = tenant, got %q", q.txTenant)
	}
	if !strings.Contains(q.rowSQL[1], "INSERT INTO companion_suspension_policy") {
		t.Fatalf("expected the tenant INSERT, got %q", q.rowSQL[1])
	}
	if s.TenantID != csTenant || s.SkillKey != "companion_chat_turn_basic" {
		t.Fatalf("tenant/skill not carried: %+v", s)
	}
	p := decodePayload(t, outboxExecs(q)[0])
	if p["tenantId"] != csTenant || p["skillKey"] != "companion_chat_turn_basic" {
		t.Fatalf("audit payload must name tenant + skill: %v", p)
	}
}

func TestCompanionSuspensionRepo_Engage_IdenticalEngagedRow_IsIdempotent(t *testing.T) {
	existingAt := time.Date(2026, 8, 22, 14, 0, 0, 0, time.UTC)
	q := &csStubQuerier{rowScan: []func(dest ...any) error{
		func(dest ...any) error { // an identical engaged row already exists
			*(dest[0].(*string)) = "01990000-0000-7000-8000-0000000000aa"
			*(dest[1].(*string)) = "earlier reason"
			*(dest[2].(*string)) = csActor
			*(dest[3].(*time.Time)) = existingAt
			*(dest[4].(*int64)) = 1
			return nil
		},
	}}
	repo := newCSRepo(q)

	s, err := repo.Engage(context.Background(), cs.EngageRequest{Scope: cs.ScopePlatform, Reason: "again", ActorGCID: csActor})
	if err != nil {
		t.Fatalf("Engage: %v", err)
	}
	if s.ID != "01990000-0000-7000-8000-0000000000aa" || s.Reason != "earlier reason" {
		t.Fatalf("must return the existing engaged row, got %+v", s)
	}
	if len(q.rowSQL) != 1 || len(outboxExecs(q)) != 0 {
		t.Fatalf("idempotent engage must write nothing: rowSQL=%d outbox=%d", len(q.rowSQL), len(outboxExecs(q)))
	}
}

func TestCompanionSuspensionRepo_Engage_InvalidRequest_NoTx(t *testing.T) {
	q := &csStubQuerier{}
	repo := newCSRepo(q)
	_, err := repo.Engage(context.Background(), cs.EngageRequest{Scope: cs.ScopeTenant, Reason: "x", ActorGCID: csActor})
	if !errors.Is(err, cs.ErrEmptyTenantID) {
		t.Fatalf("want ErrEmptyTenantID, got %v", err)
	}
	if q.txCalled != 0 {
		t.Fatal("an invalid request must never open a transaction")
	}
}

func TestCompanionSuspensionRepo_Engage_OutboxFailure_FailsTheWrite(t *testing.T) {
	q := &csStubQuerier{
		rowScan: []func(dest ...any) error{func(_ ...any) error { return pg.ErrNoRows }, scanEngagedAt(time.Now().UTC())},
		execErr: errors.New("outbox_events: disk full"),
	}
	repo := newCSRepo(q)
	_, err := repo.Engage(context.Background(), cs.EngageRequest{Scope: cs.ScopePlatform, Reason: "incident", ActorGCID: csActor})
	if err == nil {
		t.Fatal("an audit row that cannot be written must fail the engage (same tx), never engage silently")
	}
}

// --- Release --------------------------------------------------------------------

func TestCompanionSuspensionRepo_Release_ReleasesEveryMatchingRowAndAuditsEach(t *testing.T) {
	at := time.Date(2026, 8, 22, 15, 0, 0, 0, time.UTC)
	q := &csStubQuerier{queryRes: []*csStubRows{{rows: [][]any{
		{"01990000-0000-7000-8000-0000000000a1", "", "incident", csActor, at.Add(-time.Hour), at, int64(2)},
		{"01990000-0000-7000-8000-0000000000a2", "companion_skill_cite_atom", "leak", csActor, at.Add(-2 * time.Hour), at, int64(2)},
	}}}}
	repo := newCSRepo(q)

	released, err := repo.Release(context.Background(), cs.ReleaseRequest{Scope: cs.ScopePlatform, Reason: "resolved", ActorGCID: csActor})
	if err != nil {
		t.Fatalf("Release: %v", err)
	}
	if len(released) != 2 {
		t.Fatalf("want 2 released rows, got %d", len(released))
	}
	if released[0].Engaged || released[0].ReleasedBy != csActor || released[0].Version != 2 {
		t.Fatalf("released row state wrong: %+v", released[0])
	}
	if len(q.querySQL) != 1 || !strings.Contains(q.querySQL[0], "UPDATE platform_companion_suspension") || !strings.Contains(q.querySQL[0], "RETURNING") {
		t.Fatalf("expected one UPDATE ... RETURNING on the platform table, got %v", q.querySQL)
	}
	ob := outboxExecs(q)
	if len(ob) != 2 {
		t.Fatalf("one audit event per released row, got %d", len(ob))
	}
	p := decodePayload(t, ob[1])
	if p["engaged"] != false || p["skillKey"] != "companion_skill_cite_atom" || p["reason"] != "resolved" {
		t.Fatalf("release payload wrong: %v", p)
	}
}

func TestCompanionSuspensionRepo_Release_NothingEngaged_NoAudit(t *testing.T) {
	q := &csStubQuerier{}
	repo := newCSRepo(q)
	released, err := repo.Release(context.Background(), cs.ReleaseRequest{Scope: cs.ScopeTenant, TenantID: csTenant, Reason: "resolved", ActorGCID: csActor})
	if err != nil {
		t.Fatalf("Release: %v", err)
	}
	if len(released) != 0 || len(outboxExecs(q)) != 0 {
		t.Fatalf("nothing to release must write no audit rows")
	}
	if q.txTenant != csTenant {
		t.Fatalf("tenant release must run under the tenant tx, got %q", q.txTenant)
	}
}

// --- List -----------------------------------------------------------------------

func TestCompanionSuspensionRepo_ListTenant_UnderTenantTx(t *testing.T) {
	at := time.Date(2026, 8, 22, 15, 0, 0, 0, time.UTC)
	q := &csStubQuerier{queryRes: []*csStubRows{{rows: [][]any{
		{"01990000-0000-7000-8000-0000000000b1", "", true, "drift", csActor, at, "", time.Time{}, int64(1)},
	}}}}
	repo := newCSRepo(q)
	got, err := repo.ListTenant(context.Background(), csTenant)
	if err != nil {
		t.Fatalf("ListTenant: %v", err)
	}
	if len(got) != 1 || got[0].TenantID != csTenant || !got[0].Engaged || got[0].Scope != cs.ScopeTenant {
		t.Fatalf("list wrong: %+v", got)
	}
	if q.txTenant != csTenant {
		t.Fatalf("tenant list must run under the tenant tx (RLS), got %q", q.txTenant)
	}
}

func TestNewCompanionSuspensionRepo_NilQuerierPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("nil Querier must panic at wiring time (fail loud at boot)")
		}
	}()
	pg.NewCompanionSuspensionRepo(nil, pg.CompanionSuspensionRepoOptions{})
}
