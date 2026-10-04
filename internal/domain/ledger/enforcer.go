// enforcer.go — 3-level Budget enforcement cascade per ai-cost-tracking skill.
//
// Cascade order (most-restrictive wins):
//
//	Level 1 — per-tenant   HARD CAP (Reason=ReasonTenantCapExceeded)
//	Level 2 — per-user     FAIRNESS slice (Reason=ReasonUserCapExceeded)
//	Level 3 — per-agent    KILL-SWITCH at 100x baseline (Reason=ReasonAgentKillSwitch)
//
// AllowDecision is the verdict the Gateway / Router consumes BEFORE invoking
// the LLM. On {Allowed: false} the caller returns 429 GATEWAY_BUDGET_EXHAUSTED
// + Retry-After header.
//
// FAIL-OPEN: any infrastructure error (other than ErrBudgetNotFound) returns
// Allowed=true with Reason=ReasonInfrastructureFailOpen so a transient
// dependency hiccup never accidentally takes real traffic offline. The Router
// logs the underlying error (not surfaced via Reason).
package ledger

import (
	"context"
	"errors"
)

// AllowReason is a closed enum of the allow-decision rationales.
type AllowReason string

// AllowReason enum.
const (
	// ReasonAllowed — the call passed all 3 levels.
	ReasonAllowed AllowReason = "allowed"
	// ReasonTenantCapExceeded — Level 1 hard-cap blocked.
	ReasonTenantCapExceeded AllowReason = "tenant_cap_exceeded"
	// ReasonUserCapExceeded — Level 2 fairness slice blocked.
	ReasonUserCapExceeded AllowReason = "user_cap_exceeded"
	// ReasonAgentKillSwitch — Level 3 kill-switch (>=100x baseline) blocked.
	ReasonAgentKillSwitch AllowReason = "agent_kill_switch"
	// ReasonInvalidProjection — caller passed a negative projected cost.
	ReasonInvalidProjection AllowReason = "invalid_projection"
	// ReasonInfrastructureFailOpen — lookup error → fail-open (allowed).
	ReasonInfrastructureFailOpen AllowReason = "infrastructure_fail_open"
)

// AllowDecision is the verdict returned to the Gateway pre-call check.
type AllowDecision struct {
	Allowed            bool        `json:"allowed"`
	Reason             AllowReason `json:"reason"`
	ThresholdRemaining int64       `json:"threshold_remaining_micros"`
	RetryAfterSeconds  int         `json:"retry_after_seconds,omitempty"`
}

// CheckRequest is the Enforcer.Check input.
type CheckRequest struct {
	TenantID            string
	Gcid                string
	AgentID             string
	Period              string // e.g. "2026-05" (monthly) or "2026-05-09" (daily)
	ProjectedCostMicros int64  // estimate for THIS LLM call
}

// BudgetLookup is the read-side port for fetching the 3 budget levels.
// Hexagonal: domain owns the interface; adapters (in-memory, Cloud SQL)
// implement it.
type BudgetLookup interface {
	GetTenantBudget(ctx context.Context, tenantID, period string) (*Budget, error)
	GetUserBudget(ctx context.Context, tenantID, gcid, period string) (*Budget, error)
	GetAgentBudget(ctx context.Context, tenantID, agentID, period string) (*Budget, error)
}

// BudgetEnforcer is the 3-level cascade decision-maker.
type BudgetEnforcer struct {
	lookup BudgetLookup

	// AgentKillMultiplier is the multiplier above which the agent budget
	// fires the kill-switch. Default is 100x baseline.
	AgentKillMultiplier int64

	// HardBlockRetryAfterSeconds is the Retry-After hint sent on hard-block.
	// Default 28800 (8h conservative period-rollover).
	HardBlockRetryAfterSeconds int
}

// NewBudgetEnforcer constructs an Enforcer with sensible defaults. nil lookup
// is treated as "no limits" — Check always returns Allowed.
func NewBudgetEnforcer(lookup BudgetLookup) *BudgetEnforcer {
	return &BudgetEnforcer{
		lookup:                     lookup,
		AgentKillMultiplier:        100,
		HardBlockRetryAfterSeconds: 28800,
	}
}

// Check runs the 3-level cascade. Returns the most-restrictive verdict.
//
// Cascade order:
//  1. Negative projection → DENY (programming error)
//  2. Per-tenant hard-cap (would exceed)
//  3. Per-user cap (would exceed)
//  4. Per-agent kill-switch (already past 100x baseline)
//  5. ALLOW with ThresholdRemaining = tenant cap remaining
func (e *BudgetEnforcer) Check(ctx context.Context, req CheckRequest) AllowDecision {
	if req.ProjectedCostMicros < 0 {
		return AllowDecision{
			Allowed: false,
			Reason:  ReasonInvalidProjection,
		}
	}
	if e == nil || e.lookup == nil {
		// No enforcer wired = no limits.
		return AllowDecision{Allowed: true, Reason: ReasonAllowed}
	}

	// ── Level 1: per-tenant hard-cap ──────────────────────────────────────
	tenantB, err := e.lookup.GetTenantBudget(ctx, req.TenantID, req.Period)
	if err != nil && !errors.Is(err, ErrBudgetNotFound) {
		return e.failOpen()
	}
	if tenantB != nil {
		projected := tenantB.SpentUsdMicros + req.ProjectedCostMicros
		if projected > tenantB.CapUsdMicros {
			return AllowDecision{
				Allowed:           false,
				Reason:            ReasonTenantCapExceeded,
				RetryAfterSeconds: e.HardBlockRetryAfterSeconds,
			}
		}
	}

	// ── Level 2: per-user fairness slice ──────────────────────────────────
	userB, err := e.lookup.GetUserBudget(ctx, req.TenantID, req.Gcid, req.Period)
	if err != nil && !errors.Is(err, ErrBudgetNotFound) {
		return e.failOpen()
	}
	if userB != nil {
		projected := userB.SpentUsdMicros + req.ProjectedCostMicros
		if projected > userB.CapUsdMicros {
			return AllowDecision{
				Allowed:           false,
				Reason:            ReasonUserCapExceeded,
				RetryAfterSeconds: e.HardBlockRetryAfterSeconds,
			}
		}
	}

	// ── Level 3: per-agent kill-switch ────────────────────────────────────
	agentB, err := e.lookup.GetAgentBudget(ctx, req.TenantID, req.AgentID, req.Period)
	if err != nil && !errors.Is(err, ErrBudgetNotFound) {
		return e.failOpen()
	}
	if agentB != nil {
		killThreshold := agentB.CapUsdMicros * e.AgentKillMultiplier
		// Already past kill-switch (regardless of projection).
		if agentB.SpentUsdMicros >= killThreshold {
			return AllowDecision{
				Allowed:           false,
				Reason:            ReasonAgentKillSwitch,
				RetryAfterSeconds: e.HardBlockRetryAfterSeconds,
			}
		}
	}

	// ── ALLOW ─────────────────────────────────────────────────────────────
	var remaining int64
	if tenantB != nil {
		remaining = tenantB.CapUsdMicros - tenantB.SpentUsdMicros - req.ProjectedCostMicros
		if remaining < 0 {
			remaining = 0
		}
	}
	return AllowDecision{
		Allowed:            true,
		Reason:             ReasonAllowed,
		ThresholdRemaining: remaining,
	}
}

func (e *BudgetEnforcer) failOpen() AllowDecision {
	return AllowDecision{
		Allowed: true,
		Reason:  ReasonInfrastructureFailOpen,
	}
}
