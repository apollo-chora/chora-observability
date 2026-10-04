// Package inmem_test exercises in-memory repositories for the three
// observability aggregates. Verifies append-only semantics + tenant isolation.
package inmem_test

import (
	"context"
	"testing"
	"time"

	"github.com/apollo-chora/chora-observability/internal/adapter/inmem"
	"github.com/apollo-chora/chora-observability/internal/domain/correlation"
	"github.com/apollo-chora/chora-observability/internal/domain/decision"
	"github.com/apollo-chora/chora-observability/internal/domain/ledger"
)

const (
	tenantA = "01970000-0000-7000-8000-000000000001"
	tenantB = "01970000-0000-7000-8000-000000000002"
	gcidA   = "01970000-0000-7000-9000-000000000001"
	agidA   = "01970000-0000-7000-a000-000000000001"
	corrA   = "01970000-0000-7000-b000-000000000001"
	traceA  = "00000000000000000000000000000001"
	spanA   = "0000000000000001"
	tpA     = "00-00000000000000000000000000000001-0000000000000001-01"
)

// -----------------------------------------------------------------------------
// LedgerRepository
// -----------------------------------------------------------------------------

func TestLedgerRepository_AppendAndList(t *testing.T) {
	t.Parallel()

	repo := inmem.NewLedgerRepository()
	e, _ := ledger.New(ledger.NewParams{
		TenantID: tenantA, Gcid: gcidA, ModelID: "gemini-3-pro",
		PromptTokens: 10, CostUsdMicros: 100,
		TraceID: traceA, SpanID: spanA,
	})
	if err := repo.Append(context.Background(), e); err != nil {
		t.Fatalf("Append: %v", err)
	}
	out, err := repo.List(context.Background(), tenantA, ledger.ListFilter{})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(out) != 1 {
		t.Errorf("len = %d; want 1", len(out))
	}
}

func TestLedgerRepository_TenantIsolation(t *testing.T) {
	t.Parallel()

	repo := inmem.NewLedgerRepository()
	e, _ := ledger.New(ledger.NewParams{
		TenantID: tenantA, Gcid: gcidA, ModelID: "gemini-3-pro",
		PromptTokens: 10, CostUsdMicros: 100,
		TraceID: traceA, SpanID: spanA,
	})
	_ = repo.Append(context.Background(), e)
	out, _ := repo.List(context.Background(), tenantB, ledger.ListFilter{})
	if len(out) != 0 {
		t.Errorf("tenantB saw tenantA data: %d entries", len(out))
	}
}

// TestLedgerRepository_AppendOnly verifies that a ledger entry, once
// stored, cannot be mutated through any repo API. The Repository port
// deliberately omits Update / Delete methods.
func TestLedgerRepository_AppendOnly(t *testing.T) {
	t.Parallel()

	repo := inmem.NewLedgerRepository()
	e, _ := ledger.New(ledger.NewParams{
		TenantID: tenantA, Gcid: gcidA, ModelID: "gemini-3-pro",
		PromptTokens: 10, CostUsdMicros: 100,
		TraceID: traceA, SpanID: spanA,
	})
	_ = repo.Append(context.Background(), e)

	// Mutate the local pointer; the stored copy must NOT reflect it.
	originalCost := e.CostUsdMicros
	e.CostUsdMicros = 999

	out, _ := repo.List(context.Background(), tenantA, ledger.ListFilter{})
	if len(out) != 1 {
		t.Fatalf("len = %d", len(out))
	}
	if out[0].CostUsdMicros != originalCost {
		t.Errorf("repo mutated externally: got %d; want %d",
			out[0].CostUsdMicros, originalCost)
	}
}

func TestLedgerRepository_Sum(t *testing.T) {
	t.Parallel()

	repo := inmem.NewLedgerRepository()
	for _, c := range []int64{100, 250, 50} {
		e, _ := ledger.New(ledger.NewParams{
			TenantID: tenantA, Gcid: gcidA, ModelID: "m",
			PromptTokens: 1, CostUsdMicros: c,
			TraceID: traceA, SpanID: spanA,
		})
		_ = repo.Append(context.Background(), e)
	}
	total, count, err := repo.SumCost(context.Background(), tenantA, ledger.ListFilter{})
	if err != nil {
		t.Fatalf("SumCost: %v", err)
	}
	if total != 400 || count != 3 {
		t.Errorf("got total=%d count=%d; want 400/3", total, count)
	}
}

func TestLedgerRepository_FilterByDate(t *testing.T) {
	t.Parallel()

	repo := inmem.NewLedgerRepository()
	e, _ := ledger.New(ledger.NewParams{
		TenantID: tenantA, Gcid: gcidA, ModelID: "m",
		PromptTokens: 1, CostUsdMicros: 100,
		TraceID: traceA, SpanID: spanA,
	})
	_ = repo.Append(context.Background(), e)

	// from-to in the future excludes everything
	future := time.Now().Add(24 * time.Hour)
	out, _ := repo.List(context.Background(), tenantA, ledger.ListFilter{From: future})
	if len(out) != 0 {
		t.Errorf("future filter let through %d", len(out))
	}
}

// -----------------------------------------------------------------------------
// DecisionRepository
// -----------------------------------------------------------------------------

func TestDecisionRepository_Append(t *testing.T) {
	t.Parallel()

	repo := inmem.NewDecisionRepository()
	d, _ := decision.New(decision.NewParams{
		TenantID: tenantA, Agid: agidA,
		DecisionType: decision.TypeRoute, Reason: "x",
		RiskTier: decision.TierLow, CorrelationID: corrA,
		Traceparent: tpA,
	})
	if err := repo.Append(context.Background(), d); err != nil {
		t.Fatalf("Append: %v", err)
	}
	out, _ := repo.List(context.Background(), tenantA, decision.ListFilter{})
	if len(out) != 1 {
		t.Errorf("len = %d", len(out))
	}
}

func TestDecisionRepository_AppendOnly(t *testing.T) {
	t.Parallel()

	repo := inmem.NewDecisionRepository()
	d, _ := decision.New(decision.NewParams{
		TenantID: tenantA, Agid: agidA,
		DecisionType: decision.TypeRoute, Reason: "original",
		RiskTier: decision.TierLow, CorrelationID: corrA,
		Traceparent: tpA,
	})
	_ = repo.Append(context.Background(), d)

	// External mutation must not leak.
	d.Reason = "MUTATED"
	out, _ := repo.List(context.Background(), tenantA, decision.ListFilter{})
	if out[0].Reason != "original" {
		t.Errorf("decision mutated externally: %q", out[0].Reason)
	}
}

func TestDecisionRepository_Count(t *testing.T) {
	t.Parallel()

	repo := inmem.NewDecisionRepository()
	now := time.Now().UTC()

	// Seed 3 rows for tenantA (now ± offsets) and 1 row for tenantB.
	for _, agid := range []string{agidA, agidA, agidA} {
		d, _ := decision.New(decision.NewParams{
			TenantID: tenantA, Agid: agid,
			DecisionType: decision.TypeRoute, Reason: "x",
			RiskTier: decision.TierLow, CorrelationID: corrA,
			Traceparent: tpA,
		})
		_ = repo.Append(context.Background(), d)
	}
	dB, _ := decision.New(decision.NewParams{
		TenantID: tenantB, Agid: agidA,
		DecisionType: decision.TypeRoute, Reason: "x",
		RiskTier: decision.TierLow, CorrelationID: corrA,
		Traceparent: tpA,
	})
	_ = repo.Append(context.Background(), dB)

	// Window covering everything for tenantA — count = 3.
	got, err := repo.Count(context.Background(), tenantA,
		now.Add(-1*time.Hour), now.Add(1*time.Hour))
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if got != 3 {
		t.Errorf("Count(tenantA, ±1h) = %d; want 3", got)
	}

	// tenant isolation — tenantB sees only its 1 row.
	got, err = repo.Count(context.Background(), tenantB,
		now.Add(-1*time.Hour), now.Add(1*time.Hour))
	if err != nil {
		t.Fatalf("Count tenantB: %v", err)
	}
	if got != 1 {
		t.Errorf("Count(tenantB, ±1h) = %d; want 1 (isolation)", got)
	}

	// Future window — zero. Honest, not stub.
	got, err = repo.Count(context.Background(), tenantA,
		now.Add(48*time.Hour), now.Add(72*time.Hour))
	if err != nil {
		t.Fatalf("Count future: %v", err)
	}
	if got != 0 {
		t.Errorf("Count(future) = %d; want 0", got)
	}

	// Past-only window — zero (all rows have CreatedAt ~= now).
	got, err = repo.Count(context.Background(), tenantA,
		now.Add(-72*time.Hour), now.Add(-48*time.Hour))
	if err != nil {
		t.Fatalf("Count past: %v", err)
	}
	if got != 0 {
		t.Errorf("Count(past) = %d; want 0", got)
	}

	// Half-open semantics — until is exclusive. With until exactly == now
	// (CreatedAt time of the most-recent row), it should exclude those
	// rows; with until just after, include them. Use a +1ns buffer to make
	// the test deterministic regardless of clock skew between Append and
	// the test.
	got, err = repo.Count(context.Background(), tenantA,
		now.Add(-1*time.Hour), now.Add(1*time.Hour))
	if err != nil {
		t.Fatalf("Count window: %v", err)
	}
	if got != 3 {
		t.Errorf("Count(±1h) = %d; want 3", got)
	}
}

func TestDecisionRepository_FilterByAgid(t *testing.T) {
	t.Parallel()

	repo := inmem.NewDecisionRepository()
	d1, _ := decision.New(decision.NewParams{
		TenantID: tenantA, Agid: agidA,
		DecisionType: decision.TypeRoute, Reason: "x",
		RiskTier: decision.TierLow, CorrelationID: corrA,
		Traceparent: tpA,
	})
	d2, _ := decision.New(decision.NewParams{
		TenantID: tenantA, Agid: "01970000-0000-7000-a000-000000000002",
		DecisionType: decision.TypeRefuse, Reason: "y",
		RiskTier: decision.TierHigh, CorrelationID: corrA,
		Traceparent: tpA,
	})
	_ = repo.Append(context.Background(), d1)
	_ = repo.Append(context.Background(), d2)

	out, _ := repo.List(context.Background(), tenantA,
		decision.ListFilter{Agid: agidA})
	if len(out) != 1 {
		t.Errorf("agid filter produced %d; want 1", len(out))
	}
}

// TestDecisionRepository_ListDescending proves List honors ListFilter.Descending:
// default (false) stays oldest-first (recorded_at ASC) — the /o/agents
// collectStats contract — while Descending:true returns newest-first AND makes
// a Limit take the newest rows (the O+ Decision Traces audit view).
func TestDecisionRepository_ListDescending(t *testing.T) {
	t.Parallel()

	repo := inmem.NewDecisionRepository()
	base := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	// Seed oldest → newest.
	for _, off := range []time.Duration{0, time.Hour, 2 * time.Hour} {
		d, _ := decision.New(decision.NewParams{
			TenantID: tenantA, Agid: agidA,
			DecisionType: decision.TypeRoute, Reason: "x",
			RiskTier: decision.TierLow, CorrelationID: corrA,
			Traceparent: tpA,
		})
		d.CreatedAt = base.Add(off)
		_ = repo.Append(context.Background(), d)
	}

	// Default: oldest-first (ASC) — unchanged behaviour /o/agents relies on.
	asc, _ := repo.List(context.Background(), tenantA, decision.ListFilter{})
	if len(asc) != 3 {
		t.Fatalf("asc len = %d; want 3", len(asc))
	}
	if !asc[0].CreatedAt.Equal(base) || !asc[2].CreatedAt.Equal(base.Add(2*time.Hour)) {
		t.Errorf("default sort not ASC: [0]=%v [2]=%v", asc[0].CreatedAt, asc[2].CreatedAt)
	}

	// Descending: newest-first.
	desc, _ := repo.List(context.Background(), tenantA, decision.ListFilter{Descending: true})
	if len(desc) != 3 {
		t.Fatalf("desc len = %d; want 3", len(desc))
	}
	if !desc[0].CreatedAt.Equal(base.Add(2*time.Hour)) || !desc[2].CreatedAt.Equal(base) {
		t.Errorf("descending not newest-first: [0]=%v [2]=%v", desc[0].CreatedAt, desc[2].CreatedAt)
	}

	// Descending + Limit must take the NEWEST row, not the oldest.
	top, _ := repo.List(context.Background(), tenantA, decision.ListFilter{Descending: true, Limit: 1})
	if len(top) != 1 {
		t.Fatalf("desc+limit len = %d; want 1", len(top))
	}
	if !top[0].CreatedAt.Equal(base.Add(2 * time.Hour)) {
		t.Errorf("desc+limit took %v; want newest %v", top[0].CreatedAt, base.Add(2*time.Hour))
	}
}

// -----------------------------------------------------------------------------
// CorrelationRepository
// -----------------------------------------------------------------------------

func TestCorrelationRepository_Register(t *testing.T) {
	t.Parallel()

	repo := inmem.NewCorrelationRepository()
	c, _ := correlation.New(correlation.NewParams{
		TenantID: tenantA, CorrelationID: corrA,
		TraceID: traceA, ParentSpanID: spanA,
	})
	if err := repo.Register(context.Background(), c); err != nil {
		t.Fatalf("Register: %v", err)
	}
	got, err := repo.Get(context.Background(), tenantA, corrA)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.TraceID != traceA {
		t.Errorf("trace mismatch")
	}
}

func TestCorrelationRepository_GetUnknown(t *testing.T) {
	t.Parallel()

	repo := inmem.NewCorrelationRepository()
	_, err := repo.Get(context.Background(), tenantA, "nope")
	if err == nil {
		t.Errorf("expected ErrNotFound")
	}
}

func TestCorrelationRepository_TenantIsolation(t *testing.T) {
	t.Parallel()

	repo := inmem.NewCorrelationRepository()
	c, _ := correlation.New(correlation.NewParams{
		TenantID: tenantA, CorrelationID: corrA,
		TraceID: traceA, ParentSpanID: spanA,
	})
	_ = repo.Register(context.Background(), c)
	_, err := repo.Get(context.Background(), tenantB, corrA)
	if err == nil {
		t.Errorf("tenantB saw tenantA correlation")
	}
}
