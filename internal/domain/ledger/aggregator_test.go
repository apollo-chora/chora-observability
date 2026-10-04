// Package ledger_test exercises the TokenUsageLedger aggregator.
//
// Aggregator groups append-only ledger entries by model_id / agid / gcid and
// returns deterministic totals (no float drift; int64 micros). Aggregation
// must be PURE (input slice -> output map) so it can be reused by the in-memory
// repository AND the eventual Cloud SQL adapter.
package ledger_test

import (
	"testing"
	"time"

	"github.com/apollo-chora/chora-observability/internal/domain/ledger"
)

func mustEntry(t *testing.T, p ledger.NewParams) *ledger.Entry {
	t.Helper()
	e, err := ledger.New(p)
	if err != nil {
		t.Fatalf("ledger.New: %v", err)
	}
	return e
}

func TestAggregateByModel_Deterministic(t *testing.T) {
	t.Parallel()
	const traceA = "00000000000000000000000000000001"
	const spanA = "0000000000000001"
	in := []*ledger.Entry{
		mustEntry(t, ledger.NewParams{TenantID: "t1", Gcid: "g1", ModelID: "gemini-3-pro",
			PromptTokens: 100, CompletionTokens: 50, CostUsdMicros: 250000,
			TraceID: traceA, SpanID: spanA}),
		mustEntry(t, ledger.NewParams{TenantID: "t1", Gcid: "g1", ModelID: "gemini-3-pro",
			PromptTokens: 200, CompletionTokens: 50, CostUsdMicros: 100000,
			TraceID: traceA, SpanID: spanA}),
		mustEntry(t, ledger.NewParams{TenantID: "t1", Gcid: "g1", ModelID: "gemma-tenant-lora:01H",
			PromptTokens: 50, CompletionTokens: 25, CostUsdMicros: 50000,
			TraceID: traceA, SpanID: spanA}),
	}
	groups := ledger.AggregateBy(in, ledger.GroupByModel)
	if len(groups) != 2 {
		t.Fatalf("group count = %d; want 2", len(groups))
	}
	// Run again — must be deterministic (same key=>same totals)
	groups2 := ledger.AggregateBy(in, ledger.GroupByModel)
	if len(groups2) != 2 {
		t.Fatalf("non-deterministic: %d != 2", len(groups2))
	}
	// Find gemini-3-pro group
	var got *ledger.AggregateGroup
	for i, g := range groups {
		if g.Group == "gemini-3-pro" {
			got = &groups[i]
			break
		}
	}
	if got == nil {
		t.Fatalf("gemini-3-pro group missing: %v", groups)
	}
	if got.TotalCostUsdMicros != 350000 {
		t.Errorf("gemini total cost = %d; want 350000", got.TotalCostUsdMicros)
	}
	if got.PromptTokens != 300 {
		t.Errorf("prompt tokens = %d; want 300", got.PromptTokens)
	}
	if got.CompletionTokens != 100 {
		t.Errorf("completion tokens = %d; want 100", got.CompletionTokens)
	}
	if got.EntryCount != 2 {
		t.Errorf("entry count = %d; want 2", got.EntryCount)
	}
}

func TestAggregateByAgent_GroupsByAgid(t *testing.T) {
	t.Parallel()
	const traceA = "00000000000000000000000000000001"
	const spanA = "0000000000000001"
	in := []*ledger.Entry{
		mustEntry(t, ledger.NewParams{TenantID: "t1", Gcid: "g1", Agid: "a1",
			ModelID: "gemini-3-pro", CostUsdMicros: 100,
			TraceID: traceA, SpanID: spanA}),
		mustEntry(t, ledger.NewParams{TenantID: "t1", Gcid: "g1", Agid: "a2",
			ModelID: "gemini-3-pro", CostUsdMicros: 200,
			TraceID: traceA, SpanID: spanA}),
	}
	groups := ledger.AggregateBy(in, ledger.GroupByAgent)
	if len(groups) != 2 {
		t.Errorf("len = %d; want 2", len(groups))
	}
}

func TestAggregateByGcid_GroupsByGcid(t *testing.T) {
	t.Parallel()
	const traceA = "00000000000000000000000000000001"
	const spanA = "0000000000000001"
	in := []*ledger.Entry{
		mustEntry(t, ledger.NewParams{TenantID: "t1", Gcid: "g1",
			ModelID: "gemini-3-pro", CostUsdMicros: 100,
			TraceID: traceA, SpanID: spanA}),
		mustEntry(t, ledger.NewParams{TenantID: "t1", Gcid: "g2",
			ModelID: "gemini-3-pro", CostUsdMicros: 200,
			TraceID: traceA, SpanID: spanA}),
		mustEntry(t, ledger.NewParams{TenantID: "t1", Gcid: "g1",
			ModelID: "gemini-3-pro", CostUsdMicros: 50,
			TraceID: traceA, SpanID: spanA}),
	}
	groups := ledger.AggregateBy(in, ledger.GroupByGcid)
	if len(groups) != 2 {
		t.Fatalf("len = %d; want 2", len(groups))
	}
	// Find g1 — should sum 150
	var g1 *ledger.AggregateGroup
	for i := range groups {
		if groups[i].Group == "g1" {
			g1 = &groups[i]
		}
	}
	if g1 == nil || g1.TotalCostUsdMicros != 150 {
		t.Errorf("g1 sum = %v; want 150", g1)
	}
}

func TestAggregateBy_EmptyInput(t *testing.T) {
	t.Parallel()
	groups := ledger.AggregateBy(nil, ledger.GroupByModel)
	if len(groups) != 0 {
		t.Errorf("empty input should yield empty groups; got %d", len(groups))
	}
}

func TestAggregateBy_UnknownDimensionFallsBackToModel(t *testing.T) {
	t.Parallel()
	const traceA = "00000000000000000000000000000001"
	const spanA = "0000000000000001"
	in := []*ledger.Entry{
		mustEntry(t, ledger.NewParams{TenantID: "t1", Gcid: "g1",
			ModelID: "gemini-3-pro", CostUsdMicros: 100,
			TraceID: traceA, SpanID: spanA, RecordedAt: time.Now()}),
	}
	groups := ledger.AggregateBy(in, ledger.GroupBy("bogus"))
	if len(groups) != 1 {
		t.Errorf("bogus group_by should fall back to model; got %d", len(groups))
	}
}

func TestAggregateBy_StableOrder(t *testing.T) {
	t.Parallel()
	// Aggregation output MUST be sorted by Group ascending so callers get
	// deterministic JSON output even across map-iteration noise.
	const traceA = "00000000000000000000000000000001"
	const spanA = "0000000000000001"
	in := []*ledger.Entry{
		mustEntry(t, ledger.NewParams{TenantID: "t1", Gcid: "g1",
			ModelID: "z-model", CostUsdMicros: 100,
			TraceID: traceA, SpanID: spanA}),
		mustEntry(t, ledger.NewParams{TenantID: "t1", Gcid: "g1",
			ModelID: "a-model", CostUsdMicros: 200,
			TraceID: traceA, SpanID: spanA}),
	}
	groups := ledger.AggregateBy(in, ledger.GroupByModel)
	if groups[0].Group != "a-model" || groups[1].Group != "z-model" {
		t.Errorf("order not stable: %v", groups)
	}
}
