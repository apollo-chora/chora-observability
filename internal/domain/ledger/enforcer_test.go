// Package ledger_test exercises the BudgetEnforcer's 3-level cascade per
// ai-cost-tracking skill:
//
//	Level 1 — per-tenant   HARD CAP at the Router
//	Level 2 — per-user     FAIRNESS slice riding tenant cap
//	Level 3 — per-agent    KILL-SWITCH at 100x baseline
//
// AllowDecision returns the most restrictive verdict across all 3 levels.
// AllowDecision.Reason names which level vetoed when allowed=false.
package ledger_test

import (
	"context"
	"testing"

	"github.com/apollo-chora/chora-observability/internal/domain/ledger"
)

const (
	enfTenant = "01970000-0000-7000-8000-000000000001"
	enfGcid   = "01970000-0000-7000-9000-000000000001"
	enfAgent  = "agent-validator-01"
	enfPeriod = "2026-05"
)

// stubLookup is the test fixture for BudgetEnforcer.Check —
// implements the BudgetLookup port so we can pre-seed budgets.
type stubLookup struct {
	tenantBudget *ledger.Budget
	userBudget   *ledger.Budget
	agentBudget  *ledger.Budget
}

func (s *stubLookup) GetTenantBudget(_ context.Context, _, _ string) (*ledger.Budget, error) {
	if s.tenantBudget == nil {
		return nil, ledger.ErrBudgetNotFound
	}
	return s.tenantBudget, nil
}

func (s *stubLookup) GetUserBudget(_ context.Context, _, _, _ string) (*ledger.Budget, error) {
	if s.userBudget == nil {
		return nil, ledger.ErrBudgetNotFound
	}
	return s.userBudget, nil
}

func (s *stubLookup) GetAgentBudget(_ context.Context, _, _, _ string) (*ledger.Budget, error) {
	if s.agentBudget == nil {
		return nil, ledger.ErrBudgetNotFound
	}
	return s.agentBudget, nil
}

func TestEnforcer_Allow_NoLimitsConfigured(t *testing.T) {
	t.Parallel()
	// No budgets set anywhere → no cap to violate → ALLOW.
	enf := ledger.NewBudgetEnforcer(&stubLookup{})
	dec := enf.Check(context.Background(), ledger.CheckRequest{
		TenantID:            enfTenant,
		Gcid:                enfGcid,
		AgentID:             enfAgent,
		Period:              enfPeriod,
		ProjectedCostMicros: 100_000,
	})
	if !dec.Allowed {
		t.Errorf("expected ALLOW; got %+v", dec)
	}
}

func TestEnforcer_Allow_TenantUnderCap(t *testing.T) {
	t.Parallel()
	t1, _ := ledger.NewBudget(ledger.BudgetParams{
		TenantID: enfTenant, Period: enfPeriod, CapUsdMicros: 1_000_000,
	})
	t1.RecordSpend(200_000) // 20% used; projected 100k → 30% total.
	enf := ledger.NewBudgetEnforcer(&stubLookup{tenantBudget: t1})
	dec := enf.Check(context.Background(), ledger.CheckRequest{
		TenantID:            enfTenant,
		Gcid:                enfGcid,
		AgentID:             enfAgent,
		Period:              enfPeriod,
		ProjectedCostMicros: 100_000,
	})
	if !dec.Allowed {
		t.Errorf("expected ALLOW; got %+v", dec)
	}
	if dec.ThresholdRemaining != 700_000 {
		t.Errorf("threshold_remaining = %d; want 700_000", dec.ThresholdRemaining)
	}
}

func TestEnforcer_Block_TenantHardCap(t *testing.T) {
	t.Parallel()
	t1, _ := ledger.NewBudget(ledger.BudgetParams{
		TenantID: enfTenant, Period: enfPeriod, CapUsdMicros: 1_000_000,
	})
	t1.RecordSpend(950_000) // 95% used.
	enf := ledger.NewBudgetEnforcer(&stubLookup{tenantBudget: t1})
	// Projected 100k would push to 105% → 100% hard-block.
	dec := enf.Check(context.Background(), ledger.CheckRequest{
		TenantID:            enfTenant,
		Gcid:                enfGcid,
		AgentID:             enfAgent,
		Period:              enfPeriod,
		ProjectedCostMicros: 100_000,
	})
	if dec.Allowed {
		t.Errorf("expected BLOCK; got %+v", dec)
	}
	if dec.Reason != ledger.ReasonTenantCapExceeded {
		t.Errorf("reason = %s; want %s", dec.Reason, ledger.ReasonTenantCapExceeded)
	}
}

func TestEnforcer_Block_UserCap(t *testing.T) {
	t.Parallel()
	// Tenant has plenty; user is over.
	t1, _ := ledger.NewBudget(ledger.BudgetParams{
		TenantID: enfTenant, Period: enfPeriod, CapUsdMicros: 100_000_000,
	})
	t1.RecordSpend(100_000)
	u1, _ := ledger.NewBudget(ledger.BudgetParams{
		TenantID: enfTenant, Period: enfPeriod, CapUsdMicros: 50_000,
	})
	u1.RecordSpend(40_000)
	enf := ledger.NewBudgetEnforcer(&stubLookup{
		tenantBudget: t1,
		userBudget:   u1,
	})
	// User: 40k spent of 50k cap; projected 20k → 60k = 120%.
	dec := enf.Check(context.Background(), ledger.CheckRequest{
		TenantID:            enfTenant,
		Gcid:                enfGcid,
		AgentID:             enfAgent,
		Period:              enfPeriod,
		ProjectedCostMicros: 20_000,
	})
	if dec.Allowed {
		t.Errorf("expected BLOCK; got %+v", dec)
	}
	if dec.Reason != ledger.ReasonUserCapExceeded {
		t.Errorf("reason = %s; want %s", dec.Reason, ledger.ReasonUserCapExceeded)
	}
}

func TestEnforcer_Block_AgentKillSwitch(t *testing.T) {
	t.Parallel()
	t1, _ := ledger.NewBudget(ledger.BudgetParams{
		TenantID: enfTenant, Period: enfPeriod, CapUsdMicros: 100_000_000,
	})
	a1, _ := ledger.NewBudget(ledger.BudgetParams{
		TenantID: enfTenant, Period: enfPeriod, CapUsdMicros: 1_000,
	})
	a1.RecordSpend(100_000) // 10000% (exactly 100x baseline) — kill-switch fires.
	enf := ledger.NewBudgetEnforcer(&stubLookup{
		tenantBudget: t1,
		agentBudget:  a1,
	})
	// Even projecting +1 should fire kill-switch since spent already >= cap*100.
	dec := enf.Check(context.Background(), ledger.CheckRequest{
		TenantID:            enfTenant,
		Gcid:                enfGcid,
		AgentID:             enfAgent,
		Period:              enfPeriod,
		ProjectedCostMicros: 1,
	})
	if dec.Allowed {
		t.Errorf("expected BLOCK; got %+v", dec)
	}
	if dec.Reason != ledger.ReasonAgentKillSwitch {
		t.Errorf("reason = %s; want %s", dec.Reason, ledger.ReasonAgentKillSwitch)
	}
}

func TestEnforcer_Allow_WithThresholdsCrossed_DoesNotBlock(t *testing.T) {
	t.Parallel()
	// Tenant at 70% — 80% threshold would NOT fire from this projection,
	// but had it crossed, throttle is advisory not blocking.
	t1, _ := ledger.NewBudget(ledger.BudgetParams{
		TenantID: enfTenant, Period: enfPeriod, CapUsdMicros: 1_000_000,
	})
	t1.RecordSpend(700_000) // 70% used, Threshold50 already crossed.
	enf := ledger.NewBudgetEnforcer(&stubLookup{tenantBudget: t1})
	dec := enf.Check(context.Background(), ledger.CheckRequest{
		TenantID:            enfTenant,
		Gcid:                enfGcid,
		AgentID:             enfAgent,
		Period:              enfPeriod,
		ProjectedCostMicros: 50_000, // → 75%
	})
	if !dec.Allowed {
		t.Errorf("75%% should ALLOW (only hard-cap blocks); got %+v", dec)
	}
}

func TestEnforcer_TenantTakesPrecedenceOverUser(t *testing.T) {
	t.Parallel()
	// Both tenant and user over-cap → tenant reason wins (hard-cap is the
	// most-restrictive at the platform level).
	t1, _ := ledger.NewBudget(ledger.BudgetParams{
		TenantID: enfTenant, Period: enfPeriod, CapUsdMicros: 1_000_000,
	})
	t1.RecordSpend(1_000_000) // 100%
	u1, _ := ledger.NewBudget(ledger.BudgetParams{
		TenantID: enfTenant, Period: enfPeriod, CapUsdMicros: 50_000,
	})
	u1.RecordSpend(50_000)
	enf := ledger.NewBudgetEnforcer(&stubLookup{
		tenantBudget: t1,
		userBudget:   u1,
	})
	dec := enf.Check(context.Background(), ledger.CheckRequest{
		TenantID:            enfTenant,
		Gcid:                enfGcid,
		AgentID:             enfAgent,
		Period:              enfPeriod,
		ProjectedCostMicros: 1,
	})
	if dec.Allowed {
		t.Errorf("expected BLOCK")
	}
	if dec.Reason != ledger.ReasonTenantCapExceeded {
		t.Errorf("reason = %s; want %s", dec.Reason, ledger.ReasonTenantCapExceeded)
	}
}

func TestEnforcer_Check_ZeroProjectionAllowed(t *testing.T) {
	t.Parallel()
	t1, _ := ledger.NewBudget(ledger.BudgetParams{
		TenantID: enfTenant, Period: enfPeriod, CapUsdMicros: 1_000_000,
	})
	t1.RecordSpend(999_999)
	enf := ledger.NewBudgetEnforcer(&stubLookup{tenantBudget: t1})
	// Even at 99.9% spent, a zero-projection check is informational only.
	dec := enf.Check(context.Background(), ledger.CheckRequest{
		TenantID:            enfTenant,
		Gcid:                enfGcid,
		AgentID:             enfAgent,
		Period:              enfPeriod,
		ProjectedCostMicros: 0,
	})
	if !dec.Allowed {
		t.Errorf("zero projection must allow")
	}
}

func TestEnforcer_Check_NegativeProjectionRejected(t *testing.T) {
	t.Parallel()
	enf := ledger.NewBudgetEnforcer(&stubLookup{})
	dec := enf.Check(context.Background(), ledger.CheckRequest{
		TenantID:            enfTenant,
		Gcid:                enfGcid,
		AgentID:             enfAgent,
		Period:              enfPeriod,
		ProjectedCostMicros: -1,
	})
	// Negative projection is a programming error; we DENY defensively rather
	// than treat as "free credit" — Reason names it.
	if dec.Allowed {
		t.Errorf("negative projection must deny")
	}
	if dec.Reason != ledger.ReasonInvalidProjection {
		t.Errorf("reason = %s; want %s", dec.Reason, ledger.ReasonInvalidProjection)
	}
}

func TestEnforcer_Check_RetryAfterPopulatedOnHardBlock(t *testing.T) {
	t.Parallel()
	// On 100% hard-block, RetryAfterSeconds should give the period boundary
	// hint (28800 = 8h conservative default, or the period-end if known).
	t1, _ := ledger.NewBudget(ledger.BudgetParams{
		TenantID: enfTenant, Period: enfPeriod, CapUsdMicros: 1_000_000,
	})
	t1.RecordSpend(1_000_000)
	enf := ledger.NewBudgetEnforcer(&stubLookup{tenantBudget: t1})
	dec := enf.Check(context.Background(), ledger.CheckRequest{
		TenantID:            enfTenant,
		Gcid:                enfGcid,
		AgentID:             enfAgent,
		Period:              enfPeriod,
		ProjectedCostMicros: 1,
	})
	if dec.RetryAfterSeconds <= 0 {
		t.Errorf("retry_after must be > 0 on hard-block; got %d", dec.RetryAfterSeconds)
	}
}

func TestEnforcer_Check_LookupErrorTreatedAsAllow(t *testing.T) {
	t.Parallel()
	// If the BudgetLookup returns an error other than ErrBudgetNotFound,
	// the enforcer must FAIL OPEN (allow) and surface a warning rather
	// than block real traffic on a side-channel infrastructure issue.
	// (The Router will still see the error in its log/trace.)
	enf := ledger.NewBudgetEnforcer(&errStubLookup{})
	dec := enf.Check(context.Background(), ledger.CheckRequest{
		TenantID:            enfTenant,
		Gcid:                enfGcid,
		AgentID:             enfAgent,
		Period:              enfPeriod,
		ProjectedCostMicros: 100_000,
	})
	if !dec.Allowed {
		t.Errorf("infrastructure error must fail-open; got %+v", dec)
	}
	if dec.Reason != ledger.ReasonInfrastructureFailOpen {
		t.Errorf("reason = %s; want %s", dec.Reason, ledger.ReasonInfrastructureFailOpen)
	}
}

// errStubLookup returns a non-ErrBudgetNotFound error to simulate transient
// infrastructure failure.
type errStubLookup struct{}

var errStubFail = newStubErr("simulated infra failure")

func (errStubLookup) GetTenantBudget(_ context.Context, _, _ string) (*ledger.Budget, error) {
	return nil, errStubFail
}

func (errStubLookup) GetUserBudget(_ context.Context, _, _, _ string) (*ledger.Budget, error) {
	return nil, errStubFail
}

func (errStubLookup) GetAgentBudget(_ context.Context, _, _, _ string) (*ledger.Budget, error) {
	return nil, errStubFail
}

type stubErr struct{ msg string }

func (e *stubErr) Error() string { return e.msg }
func newStubErr(s string) error  { return &stubErr{msg: s} }
