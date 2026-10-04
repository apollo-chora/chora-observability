// Package ledger_test exercises the LedgerHook — the unified ledger write +
// outbox publish + budget RecordSpend triple that the Gateway calls after
// every successful LLM invocation.
//
// LedgerHook gives the Gateway adapter a SINGLE method (Record) that does
// the right thing under hexagonal: append the ledger entry, atomically
// publish chora.observability.token_usage.recorded.v1 via outbox, and
// update the 3-level budget cascade.
//
// Atomicity contract (per data-consistency skill):
//
//	Same Tx ─► Append(ledger entry) → Publish(outbox row) → RecordSpend(budgets)
//	         └─ Tx commit/rollback is the unit of consistency
//
// In the M10 in-memory implementation Tx is a no-op (single goroutine, lock
// scope == Tx scope). When wired to Cloud SQL Tier 2, Tx is a *sql.Tx.
package ledger_test

import (
	"context"
	"errors"
	"testing"

	"github.com/5007-Capstone/chora/services/chora-observability/internal/adapter/inmem"
	"github.com/5007-Capstone/chora/services/chora-observability/internal/domain/ledger"
)

// stubOutbox records calls so we can assert the publish side-effect.
type stubOutbox struct {
	calls []ledger.OutboxRecord
	err   error
}

func (s *stubOutbox) RecordOutboxEvent(_ context.Context, r ledger.OutboxRecord) error {
	if s.err != nil {
		return s.err
	}
	s.calls = append(s.calls, r)
	return nil
}

func TestLedgerHook_Record_AppendsAndPublishesAndAccrues(t *testing.T) {
	t.Parallel()
	ledgerRepo := inmem.NewLedgerRepository()
	budgetLookup := inmem.NewBudgetLookup()
	tb, _ := ledger.NewBudget(ledger.BudgetParams{
		TenantID: enfTenant, Period: enfPeriod, CapUsdMicros: 10_000_000,
	})
	_ = budgetLookup.SetTenantBudget(context.Background(), tb)

	outbox := &stubOutbox{}
	hook := ledger.NewLedgerHook(ledger.LedgerHookConfig{
		Ledger:        ledgerRepo,
		Budgets:       budgetLookup,
		Outbox:        outbox,
		SourceProject: "chora-489812",
		SourceService: "chora-model-broker-gateway",
		Period:        enfPeriod,
	})

	rec, err := hook.Record(context.Background(), ledger.RecordParams{
		TenantID:         enfTenant,
		Gcid:             enfGcid,
		AgentID:          enfAgent,
		ModelID:          "vertex_ai/gemini-2.5-flash",
		PromptTokens:     1000,
		CompletionTokens: 500,
		CostUsdMicros:    450,
		TraceID:          "00000000000000000000000000000001",
		SpanID:           "0000000000000001",
	})
	if err != nil {
		t.Fatalf("Record: %v", err)
	}

	// 1. Ledger entry appended.
	entries, _ := ledgerRepo.List(context.Background(), enfTenant, ledger.ListFilter{})
	if len(entries) != 1 {
		t.Fatalf("ledger entries = %d; want 1", len(entries))
	}
	if entries[0].CostUsdMicros != 450 {
		t.Errorf("entry cost = %d", entries[0].CostUsdMicros)
	}

	// 2. Outbox event published with correct topic + IMDA dimension.
	if len(outbox.calls) != 1 {
		t.Fatalf("outbox calls = %d; want 1", len(outbox.calls))
	}
	c := outbox.calls[0]
	if c.Topic != "chora.observability.token_usage.recorded.v1" {
		t.Errorf("topic = %s; want canonical", c.Topic)
	}
	if c.ChoraImdaDimension != "accountability" {
		t.Errorf("imda dimension = %s; want accountability (D1 per ADR-141)", c.ChoraImdaDimension)
	}
	if c.SourceService != "chora-model-broker-gateway" {
		t.Errorf("source_service = %s", c.SourceService)
	}

	// 3. Tenant budget accrued the spend.
	tb2, _ := budgetLookup.GetTenantBudget(context.Background(), enfTenant, enfPeriod)
	if tb2.SpentUsdMicros != 450 {
		t.Errorf("tenant spent = %d; want 450", tb2.SpentUsdMicros)
	}

	// 4. Returned record carries the ledger_id + outbox_event_id.
	if rec.LedgerID == "" {
		t.Errorf("LedgerID empty")
	}
	if rec.OutboxEventID == "" {
		t.Errorf("OutboxEventID empty")
	}
}

func TestLedgerHook_Record_ValidationError(t *testing.T) {
	t.Parallel()
	hook := ledger.NewLedgerHook(ledger.LedgerHookConfig{
		Ledger:        inmem.NewLedgerRepository(),
		Budgets:       inmem.NewBudgetLookup(),
		Outbox:        &stubOutbox{},
		SourceProject: "chora-489812",
		SourceService: "chora-model-broker-gateway",
		Period:        enfPeriod,
	})
	_, err := hook.Record(context.Background(), ledger.RecordParams{
		// missing TenantID — ledger.New rejects.
		Gcid: enfGcid, ModelID: "m", CostUsdMicros: 1,
		TraceID: "00000000000000000000000000000001", SpanID: "0000000000000001",
	})
	if err == nil {
		t.Errorf("expected validation error")
	}
}

func TestLedgerHook_Record_OutboxFailureRollsBack(t *testing.T) {
	t.Parallel()
	// If outbox publish fails, the ledger entry must NOT be committed
	// (atomicity contract). The in-memory hook achieves this by writing
	// to the outbox FIRST and only appending the ledger if outbox succeeds.
	ledgerRepo := inmem.NewLedgerRepository()
	budgetLookup := inmem.NewBudgetLookup()
	tb, _ := ledger.NewBudget(ledger.BudgetParams{
		TenantID: enfTenant, Period: enfPeriod, CapUsdMicros: 10_000_000,
	})
	_ = budgetLookup.SetTenantBudget(context.Background(), tb)
	outbox := &stubOutbox{err: errors.New("pubsub down")}
	hook := ledger.NewLedgerHook(ledger.LedgerHookConfig{
		Ledger: ledgerRepo, Budgets: budgetLookup, Outbox: outbox,
		SourceProject: "chora-489812",
		SourceService: "chora-model-broker-gateway",
		Period:        enfPeriod,
	})
	_, err := hook.Record(context.Background(), ledger.RecordParams{
		TenantID: enfTenant, Gcid: enfGcid, AgentID: enfAgent,
		ModelID: "m", CostUsdMicros: 100,
		TraceID: "00000000000000000000000000000001", SpanID: "0000000000000001",
	})
	if err == nil {
		t.Errorf("expected outbox error")
	}
	// Ledger should be empty — no entry committed when outbox fails.
	entries, _ := ledgerRepo.List(context.Background(), enfTenant, ledger.ListFilter{})
	if len(entries) != 0 {
		t.Errorf("ledger committed despite outbox failure: %d entries", len(entries))
	}
	// Budget should be unchanged.
	tb2, _ := budgetLookup.GetTenantBudget(context.Background(), enfTenant, enfPeriod)
	if tb2.SpentUsdMicros != 0 {
		t.Errorf("budget accrued despite outbox failure: %d", tb2.SpentUsdMicros)
	}
}

func TestLedgerHook_Record_PassesPricingVersionThrough(t *testing.T) {
	t.Parallel()
	hook := ledger.NewLedgerHook(ledger.LedgerHookConfig{
		Ledger:               inmem.NewLedgerRepository(),
		Budgets:              inmem.NewBudgetLookup(),
		Outbox:               &stubOutbox{},
		SourceProject:        "chora-489812",
		SourceService:        "chora-model-broker-gateway",
		Period:               enfPeriod,
		PricingConfigVersion: "2026.05.09-1",
	})
	rec, err := hook.Record(context.Background(), ledger.RecordParams{
		TenantID: enfTenant, Gcid: enfGcid, AgentID: enfAgent,
		ModelID: "m", CostUsdMicros: 100,
		TraceID: "00000000000000000000000000000001", SpanID: "0000000000000001",
	})
	if err != nil {
		t.Fatalf("Record: %v", err)
	}
	if rec.PricingConfigVersion != "2026.05.09-1" {
		t.Errorf("pricing version = %s", rec.PricingConfigVersion)
	}
}

func TestLedgerHook_Record_SkipsBudgetWhenNotConfigured(t *testing.T) {
	t.Parallel()
	// Hook with nil Budgets: ledger + outbox must still work.
	hook := ledger.NewLedgerHook(ledger.LedgerHookConfig{
		Ledger:        inmem.NewLedgerRepository(),
		Budgets:       nil,
		Outbox:        &stubOutbox{},
		SourceProject: "chora-489812",
		SourceService: "chora-model-broker-gateway",
		Period:        enfPeriod,
	})
	_, err := hook.Record(context.Background(), ledger.RecordParams{
		TenantID: enfTenant, Gcid: enfGcid, AgentID: enfAgent,
		ModelID: "m", CostUsdMicros: 100,
		TraceID: "00000000000000000000000000000001", SpanID: "0000000000000001",
	})
	if err != nil {
		t.Errorf("expected no error; got %v", err)
	}
}
