// Package ledger_test exercises the TokenUsageLedger aggregate.
//
// TokenUsageLedger is APPEND-ONLY per .claude/rules/ddd-enforcement.md
// invariant #4 (mirrors AtomRevision append-only). Mutators are deliberately
// absent. Cost is stored as int64 micros (1e-6 USD) to avoid float drift —
// per the canonical doc Tier 3 D12 + ai-cost-tracking skill.
package ledger_test

import (
	"strings"
	"testing"

	"github.com/apollo-chora/chora-observability/internal/domain/ledger"
)

const (
	tenantA = "01970000-0000-7000-8000-000000000001"
	gcidA   = "01970000-0000-7000-9000-000000000001"
	agidA   = "01970000-0000-7000-a000-000000000001"
	traceA  = "00000000000000000000000000000001"
	spanA   = "0000000000000001"
)

func TestNewLedgerEntry_Valid(t *testing.T) {
	t.Parallel()

	e, err := ledger.New(ledger.NewParams{
		TenantID:         tenantA,
		Gcid:             gcidA,
		Agid:             agidA,
		ModelID:          "gemini-3-pro",
		PromptTokens:     100,
		CompletionTokens: 50,
		CostUsdMicros:    250000,
		TraceID:          traceA,
		SpanID:           spanA,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if e.LedgerID == "" {
		t.Errorf("LedgerID empty")
	}
	if e.TenantID != tenantA {
		t.Errorf("tenant mismatch: %s", e.TenantID)
	}
	if e.RecordedAt.IsZero() {
		t.Errorf("RecordedAt is zero")
	}
}

func TestNewLedgerEntry_AgidNullable(t *testing.T) {
	t.Parallel()

	// User-direct call (no agent) — Agid is empty string (nullable).
	e, err := ledger.New(ledger.NewParams{
		TenantID:         tenantA,
		Gcid:             gcidA,
		Agid:             "", // explicitly empty
		ModelID:          "gemini-3-pro",
		PromptTokens:     100,
		CompletionTokens: 50,
		CostUsdMicros:    250000,
		TraceID:          traceA,
		SpanID:           spanA,
	})
	if err != nil {
		t.Fatalf("Agid nullable expected, got error: %v", err)
	}
	if e.Agid != "" {
		t.Errorf("Agid should be empty: %q", e.Agid)
	}
}

func TestNewLedgerEntry_RejectsMissingTenant(t *testing.T) {
	t.Parallel()

	_, err := ledger.New(ledger.NewParams{
		TenantID:      "",
		Gcid:          gcidA,
		ModelID:       "gemini-3-pro",
		PromptTokens:  10,
		CostUsdMicros: 100,
		TraceID:       traceA,
		SpanID:        spanA,
	})
	if err == nil {
		t.Errorf("expected error for missing tenant")
	}
}

func TestNewLedgerEntry_RejectsMissingGcid(t *testing.T) {
	t.Parallel()

	_, err := ledger.New(ledger.NewParams{
		TenantID:      tenantA,
		Gcid:          "",
		ModelID:       "gemini-3-pro",
		PromptTokens:  10,
		CostUsdMicros: 100,
		TraceID:       traceA,
		SpanID:        spanA,
	})
	if err == nil {
		t.Errorf("expected error for missing gcid")
	}
}

func TestNewLedgerEntry_RejectsMissingModelID(t *testing.T) {
	t.Parallel()

	_, err := ledger.New(ledger.NewParams{
		TenantID:      tenantA,
		Gcid:          gcidA,
		ModelID:       "",
		PromptTokens:  10,
		CostUsdMicros: 100,
		TraceID:       traceA,
		SpanID:        spanA,
	})
	if err == nil {
		t.Errorf("expected error for empty model_id")
	}
}

func TestNewLedgerEntry_RejectsNegativeTokens(t *testing.T) {
	t.Parallel()

	_, err := ledger.New(ledger.NewParams{
		TenantID:      tenantA,
		Gcid:          gcidA,
		ModelID:       "gemini-3-pro",
		PromptTokens:  -1,
		CostUsdMicros: 100,
		TraceID:       traceA,
		SpanID:        spanA,
	})
	if err == nil {
		t.Errorf("expected error for negative prompt_tokens")
	}
}

func TestNewLedgerEntry_RejectsNegativeCompletionTokens(t *testing.T) {
	t.Parallel()

	_, err := ledger.New(ledger.NewParams{
		TenantID:         tenantA,
		Gcid:             gcidA,
		ModelID:          "gemini-3-pro",
		PromptTokens:     10,
		CompletionTokens: -1,
		CostUsdMicros:    100,
		TraceID:          traceA,
		SpanID:           spanA,
	})
	if err == nil {
		t.Errorf("expected error for negative completion_tokens")
	}
}

func TestNewLedgerEntry_RejectsNegativeCost(t *testing.T) {
	t.Parallel()

	_, err := ledger.New(ledger.NewParams{
		TenantID:      tenantA,
		Gcid:          gcidA,
		ModelID:       "gemini-3-pro",
		PromptTokens:  10,
		CostUsdMicros: -1,
		TraceID:       traceA,
		SpanID:        spanA,
	})
	if err == nil {
		t.Errorf("expected error for negative cost")
	}
}

func TestNewLedgerEntry_RejectsBadTraceID(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		traceID string
	}{
		{"too short", "abc"},
		{"too long", strings.Repeat("a", 33)},
		{"non-hex", strings.Repeat("z", 32)},
		{"all zeros", strings.Repeat("0", 32)}, // W3C reserved as invalid
		{"empty", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ledger.New(ledger.NewParams{
				TenantID:      tenantA,
				Gcid:          gcidA,
				ModelID:       "gemini-3-pro",
				PromptTokens:  10,
				CostUsdMicros: 100,
				TraceID:       tc.traceID,
				SpanID:        spanA,
			})
			if err == nil {
				t.Errorf("expected error for trace_id %q", tc.traceID)
			}
		})
	}
}

func TestNewLedgerEntry_RejectsBadSpanID(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		spanID string
	}{
		{"too short", "abc"},
		{"too long", strings.Repeat("a", 17)},
		{"non-hex", strings.Repeat("z", 16)},
		{"all zeros", strings.Repeat("0", 16)},
		{"empty", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ledger.New(ledger.NewParams{
				TenantID:      tenantA,
				Gcid:          gcidA,
				ModelID:       "gemini-3-pro",
				PromptTokens:  10,
				CostUsdMicros: 100,
				TraceID:       traceA,
				SpanID:        tc.spanID,
			})
			if err == nil {
				t.Errorf("expected error for span_id %q", tc.spanID)
			}
		})
	}
}

func TestValidateTraceID_LowercaseHex(t *testing.T) {
	t.Parallel()

	if err := ledger.ValidateTraceID("00000000000000000000000000000001"); err != nil {
		t.Errorf("lowercase 32-hex with one non-zero should be valid: %v", err)
	}
	if err := ledger.ValidateTraceID("0123456789ABCDEF0123456789ABCDEF"); err != nil {
		// W3C says lowercase preferred but uppercase tolerated.
		t.Errorf("uppercase hex: %v", err)
	}
}

func TestSumCost_HandlesOverflow(t *testing.T) {
	t.Parallel()

	// MaxInt64 - 1 + 5 must overflow-detect.
	const max = int64(1<<63 - 1)
	_, err := ledger.SumCost([]int64{max - 1, 5})
	if err == nil {
		t.Errorf("expected overflow error on int64 sum")
	}
}

func TestSumCost_NoOverflow(t *testing.T) {
	t.Parallel()

	v, err := ledger.SumCost([]int64{100, 250, 50})
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if v != 400 {
		t.Errorf("sum = %d; want 400", v)
	}
}

func TestSumCost_RejectsNegative(t *testing.T) {
	t.Parallel()

	_, err := ledger.SumCost([]int64{100, -1})
	if err == nil {
		t.Errorf("expected error for negative entry")
	}
}

func TestSumCost_EmptyIsZero(t *testing.T) {
	t.Parallel()

	v, err := ledger.SumCost(nil)
	if err != nil {
		t.Errorf("nil slice should return 0 + nil; got %v", err)
	}
	if v != 0 {
		t.Errorf("sum = %d; want 0", v)
	}
}
