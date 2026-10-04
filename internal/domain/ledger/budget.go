// budget.go — per-period spend ceiling aggregate for the TokenUsageLedger.
//
// Budget is the per-tenant "this billing window's cap" record. RecordSpend is
// the only mutator: it adds to SpentUsdMicros (int64 micros — never float64)
// and returns the threshold tiers crossed by THIS call (50%, 80%, 100%, 110%
// over-cap). Each threshold fires exactly once per period.
//
// 3-level cascade per ai-cost-tracking skill:
//
//	Level 1 — per-tenant   (this aggregate; HARD CAP at the Router)
//	Level 2 — per-user     (BudgetPerUser; FAIRNESS slice riding tenant cap)
//	Level 3 — per-agent    (BudgetPerAgent; KILL-SWITCH at 100x baseline)
//
// Level 2 + Level 3 share the threshold-crossing semantics — see Enforcer
// (enforcer.go) for the cascading check that consults all three levels.
//
// Aligned with Tier 3 D10 (atomic ledger + budgets) + ai-cost-tracking skill.
package ledger

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
)

// ErrBudgetNotFound is the canonical sentinel for "no budget set for the
// (tenant, period) pair".
var ErrBudgetNotFound = errors.New("budget not found")

// BudgetThreshold is one of the 4 fixed warning tiers.
type BudgetThreshold string

// Threshold tiers — each fires exactly once per period when crossed.
const (
	// Threshold50 = 50% spent (warning). Notify; no enforcement action.
	Threshold50 BudgetThreshold = "50%"
	// Threshold80 = 80% spent (throttle suggested). Cost-anomaly hint.
	Threshold80 BudgetThreshold = "80%"
	// Threshold100 = 100% spent (hard-block at Router).
	Threshold100 BudgetThreshold = "100%"
	// Threshold110 = 110% spent (kill-switch — over-spend already happened).
	Threshold110 BudgetThreshold = "110%"
)

// BudgetThresholdsAsc returns the closed vocabulary of thresholds in
// ascending percent order. Used by tests + the Enforcer cascade.
func BudgetThresholdsAsc() []BudgetThreshold {
	return []BudgetThreshold{Threshold50, Threshold80, Threshold100, Threshold110}
}

// thresholdRatio maps each threshold to its (numerator, denominator) ratio.
// Using int math avoids float drift at exactly the cap.
func thresholdRatio(t BudgetThreshold) (int64, int64) {
	switch t {
	case Threshold50:
		return 50, 100
	case Threshold80:
		return 80, 100
	case Threshold100:
		return 100, 100
	case Threshold110:
		return 110, 100
	}
	return 0, 1
}

// Budget is the per-tenant per-period cap aggregate. It is mutable through
// RecordSpend ONLY; threshold-crossings accumulate idempotently.
type Budget struct {
	BudgetID          string            `json:"budget_id"`
	TenantID          string            `json:"tenant_id"`
	Period            string            `json:"period"` // e.g. "2026-05" or "2026-05-09"
	CapUsdMicros      int64             `json:"cap_usd_micros"`
	SpentUsdMicros    int64             `json:"spent_usd_micros"`
	ThresholdsCrossed []BudgetThreshold `json:"thresholds_crossed"`
}

// BudgetParams is the constructor input.
type BudgetParams struct {
	TenantID     string
	Period       string
	CapUsdMicros int64
}

// NewBudget constructs a fresh per-tenant per-period budget. Cap must be > 0.
func NewBudget(p BudgetParams) (*Budget, error) {
	if strings.TrimSpace(p.TenantID) == "" {
		return nil, errors.New("tenant_id is required")
	}
	if strings.TrimSpace(p.Period) == "" {
		return nil, errors.New("period is required")
	}
	if p.CapUsdMicros <= 0 {
		return nil, fmt.Errorf("cap_usd_micros must be > 0: %d", p.CapUsdMicros)
	}
	id, err := uuid.NewV7()
	if err != nil {
		return nil, fmt.Errorf("uuidv7: %w", err)
	}
	return &Budget{
		BudgetID:          id.String(),
		TenantID:          strings.TrimSpace(p.TenantID),
		Period:            strings.TrimSpace(p.Period),
		CapUsdMicros:      p.CapUsdMicros,
		SpentUsdMicros:    0,
		ThresholdsCrossed: nil,
	}, nil
}

// RecordSpend adds amountMicros to SpentUsdMicros and returns the threshold
// tiers crossed by THIS call (in ascending percent order). Each threshold
// fires exactly once per period — already-crossed thresholds are not
// re-emitted on subsequent spend.
//
// Panics on negative amount (programming error — caller must validate).
func (b *Budget) RecordSpend(amountMicros int64) []BudgetThreshold {
	if amountMicros < 0 {
		panic(fmt.Sprintf("Budget.RecordSpend: negative amount %d", amountMicros))
	}

	prev := b.SpentUsdMicros
	b.SpentUsdMicros += amountMicros

	// Early-exit: zero spend never crosses anything.
	if amountMicros == 0 || b.CapUsdMicros == 0 {
		return nil
	}

	// alreadyCrossed is the set of thresholds already accumulated.
	already := make(map[BudgetThreshold]struct{}, len(b.ThresholdsCrossed))
	for _, t := range b.ThresholdsCrossed {
		already[t] = struct{}{}
	}

	var newlyCrossed []BudgetThreshold
	for _, t := range BudgetThresholdsAsc() {
		num, den := thresholdRatio(t)
		// threshold_micros = cap * num / den (int math; trunc).
		thMicros := b.CapUsdMicros * num / den
		// Crossed iff prev < thMicros && new >= thMicros AND not already crossed.
		if _, seen := already[t]; seen {
			continue
		}
		if prev < thMicros && b.SpentUsdMicros >= thMicros {
			newlyCrossed = append(newlyCrossed, t)
		}
	}

	if len(newlyCrossed) > 0 {
		b.ThresholdsCrossed = append(b.ThresholdsCrossed, newlyCrossed...)
	}
	return newlyCrossed
}

// PercentSpent returns spent / cap * 100 as a float64, capped at no upper
// bound (so 110% over-spend reports 110.0). Returns 0.0 when cap is 0
// (defensive — NewBudget rejects 0 cap, but RecordSpend can be called on a
// pointer with hand-set fields).
func (b *Budget) PercentSpent() float64 {
	if b.CapUsdMicros <= 0 {
		return 0.0
	}
	return float64(b.SpentUsdMicros) / float64(b.CapUsdMicros) * 100.0
}

// Remaining returns CapUsdMicros - SpentUsdMicros, clamped at zero. Negative
// remaining (over-spend) is reported as zero so subscribers don't double-count.
func (b *Budget) Remaining() int64 {
	r := b.CapUsdMicros - b.SpentUsdMicros
	if r < 0 {
		return 0
	}
	return r
}

// IsOverCap reports whether SpentUsdMicros > CapUsdMicros (strict).
func (b *Budget) IsOverCap() bool {
	return b.SpentUsdMicros > b.CapUsdMicros
}

// BudgetRepository is the persistence port for per-tenant per-period budgets.
//
// Hexagonal: domain owns the interface. The in-memory adapter
// (internal/adapter/inmem) is the M10 implementation; Postgres comes Tier 2.
type BudgetRepository interface {
	// Set persists or replaces the budget for (tenant_id, period).
	Set(ctx context.Context, b *Budget) error

	// Get returns the budget for (tenant_id, period) or ErrBudgetNotFound.
	Get(ctx context.Context, tenantID, period string) (*Budget, error)
}
