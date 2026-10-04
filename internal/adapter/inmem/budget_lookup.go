// budget_lookup.go — in-memory 3-level Budget cascade adapter.
//
// Implements ledger.BudgetLookup (the Enforcer.Check read port) for the M10
// skeleton. Cloud SQL adapter is deferred to Tier 2 — the schema is already
// locked in services/chora-observability/migrations/0002_schema_lockdown.sql.
//
// Key shape:
//
//	tenant   = (tenant_id + period)
//	user     = (tenant_id + gcid + period)
//	agent    = (tenant_id + agent_id + period)
//
// All three levels are independently optional; a missing level returns
// ErrBudgetNotFound which the Enforcer treats as "no limit at this level".
package inmem

import (
	"context"
	"strings"
	"sync"

	"github.com/5007-Capstone/chora/services/chora-observability/internal/domain/ledger"
)

// BudgetLookup is the goroutine-safe in-memory 3-level adapter.
type BudgetLookup struct {
	mu      sync.RWMutex
	tenant  map[string]*ledger.Budget // key = tenantID + "::" + period
	user    map[string]*ledger.Budget // key = tenantID + "::" + gcid + "::" + period
	agent   map[string]*ledger.Budget // key = tenantID + "::" + agentID + "::" + period
}

// NewBudgetLookup constructs an initialised 3-level lookup.
func NewBudgetLookup() *BudgetLookup {
	return &BudgetLookup{
		tenant: make(map[string]*ledger.Budget),
		user:   make(map[string]*ledger.Budget),
		agent:  make(map[string]*ledger.Budget),
	}
}

func key2(a, b string) string             { return a + "::" + b }
func key3(a, b, c string) string          { return a + "::" + b + "::" + c }

// SetTenantBudget persists or replaces the per-tenant budget. Defensively
// deep-copies so external mutation does not leak.
func (l *BudgetLookup) SetTenantBudget(_ context.Context, b *ledger.Budget) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.tenant[key2(b.TenantID, b.Period)] = cloneBudget(b)
	return nil
}

// SetUserBudget persists or replaces the per-user budget for (tenantID, gcid).
func (l *BudgetLookup) SetUserBudget(_ context.Context, gcid string, b *ledger.Budget) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.user[key3(b.TenantID, gcid, b.Period)] = cloneBudget(b)
	return nil
}

// SetAgentBudget persists or replaces the per-agent budget for
// (tenantID, agentID).
func (l *BudgetLookup) SetAgentBudget(_ context.Context, agentID string, b *ledger.Budget) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.agent[key3(b.TenantID, agentID, b.Period)] = cloneBudget(b)
	return nil
}

// GetTenantBudget returns the per-tenant budget or ErrBudgetNotFound.
func (l *BudgetLookup) GetTenantBudget(_ context.Context, tenantID, period string) (*ledger.Budget, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	b, ok := l.tenant[key2(strings.TrimSpace(tenantID), strings.TrimSpace(period))]
	if !ok {
		return nil, ledger.ErrBudgetNotFound
	}
	return cloneBudget(b), nil
}

// GetUserBudget returns the per-user budget or ErrBudgetNotFound.
func (l *BudgetLookup) GetUserBudget(_ context.Context, tenantID, gcid, period string) (*ledger.Budget, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	b, ok := l.user[key3(strings.TrimSpace(tenantID), strings.TrimSpace(gcid), strings.TrimSpace(period))]
	if !ok {
		return nil, ledger.ErrBudgetNotFound
	}
	return cloneBudget(b), nil
}

// GetAgentBudget returns the per-agent budget or ErrBudgetNotFound.
func (l *BudgetLookup) GetAgentBudget(_ context.Context, tenantID, agentID, period string) (*ledger.Budget, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	b, ok := l.agent[key3(strings.TrimSpace(tenantID), strings.TrimSpace(agentID), strings.TrimSpace(period))]
	if !ok {
		return nil, ledger.ErrBudgetNotFound
	}
	return cloneBudget(b), nil
}

// RecordSpendInput captures the call-site values RecordSpend needs to update
// all configured levels atomically.
type RecordSpendInput struct {
	TenantID     string
	Gcid         string
	AgentID      string
	Period       string
	AmountMicros int64
}

// RecordSpend applies the given spend to ALL 3 levels (skipping missing ones).
// Atomic w.r.t. the in-memory map (single mutex scope). The caller is the
// post-LLM-call ledger writer at the Gateway.
//
// Returns nil even when no levels are configured (degenerate but valid case
// — the call simply records nothing).
func (l *BudgetLookup) RecordSpend(_ context.Context, in RecordSpendInput) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if b, ok := l.tenant[key2(in.TenantID, in.Period)]; ok {
		b.RecordSpend(in.AmountMicros)
	}
	if b, ok := l.user[key3(in.TenantID, in.Gcid, in.Period)]; ok {
		b.RecordSpend(in.AmountMicros)
	}
	if b, ok := l.agent[key3(in.TenantID, in.AgentID, in.Period)]; ok {
		b.RecordSpend(in.AmountMicros)
	}
	return nil
}

// AccrueSpend implements ledger.BudgetAccruer — the canonical domain port
// the LedgerHook calls. Internally re-uses RecordSpend.
func (l *BudgetLookup) AccrueSpend(ctx context.Context, in ledger.AccrueSpendInput) error {
	return l.RecordSpend(ctx, RecordSpendInput{
		TenantID:     in.TenantID,
		Gcid:         in.Gcid,
		AgentID:      in.AgentID,
		Period:       in.Period,
		AmountMicros: in.AmountMicros,
	})
}

// cloneBudget returns a deep copy of the budget so external callers can mutate
// without leaking into storage.
func cloneBudget(b *ledger.Budget) *ledger.Budget {
	if b == nil {
		return nil
	}
	clone := *b
	clone.ThresholdsCrossed = append([]ledger.BudgetThreshold(nil), b.ThresholdsCrossed...)
	return &clone
}
