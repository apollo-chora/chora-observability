// Package ledger is the TokenUsageLedger aggregate of the Observability domain.
//
// TokenUsageLedger is APPEND-ONLY (per ddd-enforcement invariant #4 mirror).
// Each entry records a single LLM call (or batch) with token counts + cost
// stored as int64 micros (1e-6 USD) — never float64, to avoid drift in
// billing-grade numbers per Tier 3 D12 + ai-cost-tracking skill.
//
// W3C trace context is stored alongside each entry so cost can be correlated
// back to a Cloud Trace span.
//
// Recursion warning: chora-observability emits OTLP traces for ITS OWN
// requests. Those self-emitted traces do NOT generate a TokenUsageLedger
// entry because no LLM call was made — the recording API only persists
// LLM-cost data passed in by callers (Model Gateway, etc.). The Model
// Gateway is responsible for filtering its own self-recursion.
package ledger

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Entry is the append-only TokenUsageLedger record.
type Entry struct {
	LedgerID         string    `json:"ledger_id"`
	TenantID         string    `json:"tenant_id"`
	Gcid             string    `json:"gcid"`
	Agid             string    `json:"agid,omitempty"` // nullable: empty = user-direct
	ModelID          string    `json:"model_id"`
	PromptTokens     int       `json:"prompt_tokens"`
	CompletionTokens int       `json:"completion_tokens"`
	CachedTokens     int       `json:"cached_tokens"`
	CostUsdMicros    int64     `json:"cost_usd_micros"` // 1e-6 USD
	TraceID          string    `json:"trace_id"`        // 32 hex (W3C)
	SpanID           string    `json:"span_id"`         // 16 hex (W3C)
	RecordedAt       time.Time `json:"recorded_at"`
}

// NewParams is the constructor input.
type NewParams struct {
	TenantID         string
	Gcid             string
	Agid             string // nullable
	ModelID          string
	PromptTokens     int
	CompletionTokens int
	CachedTokens     int
	CostUsdMicros    int64
	TraceID          string
	SpanID           string

	// RecordedAt overrides the default time.Now().UTC() — useful for tests
	// that need a deterministic timestamp. Zero value defaults to now().
	RecordedAt time.Time
}

// New constructs a fresh ledger entry. Returns an error on invariant
// violation. Append-only: there is no Update / Patch on the returned entry.
func New(p NewParams) (*Entry, error) {
	if strings.TrimSpace(p.TenantID) == "" {
		return nil, errors.New("tenant_id is required")
	}
	if strings.TrimSpace(p.Gcid) == "" {
		return nil, errors.New("gcid is required")
	}
	if strings.TrimSpace(p.ModelID) == "" {
		return nil, errors.New("model_id is required")
	}
	if p.PromptTokens < 0 {
		return nil, fmt.Errorf("prompt_tokens negative: %d", p.PromptTokens)
	}
	if p.CompletionTokens < 0 {
		return nil, fmt.Errorf("completion_tokens negative: %d", p.CompletionTokens)
	}
	if p.CachedTokens < 0 {
		return nil, fmt.Errorf("cached_tokens negative: %d", p.CachedTokens)
	}
	if p.CostUsdMicros < 0 {
		return nil, fmt.Errorf("cost_usd_micros negative: %d", p.CostUsdMicros)
	}
	if err := ValidateTraceID(p.TraceID); err != nil {
		return nil, fmt.Errorf("trace_id: %w", err)
	}
	if err := ValidateSpanID(p.SpanID); err != nil {
		return nil, fmt.Errorf("span_id: %w", err)
	}

	id, err := uuid.NewV7()
	if err != nil {
		return nil, fmt.Errorf("uuidv7: %w", err)
	}

	recordedAt := p.RecordedAt
	if recordedAt.IsZero() {
		recordedAt = time.Now().UTC()
	}

	return &Entry{
		LedgerID:         id.String(),
		TenantID:         p.TenantID,
		Gcid:             p.Gcid,
		Agid:             p.Agid,
		ModelID:          p.ModelID,
		PromptTokens:     p.PromptTokens,
		CompletionTokens: p.CompletionTokens,
		CachedTokens:     p.CachedTokens,
		CostUsdMicros:    p.CostUsdMicros,
		TraceID:          strings.ToLower(p.TraceID),
		SpanID:           strings.ToLower(p.SpanID),
		RecordedAt:       recordedAt,
	}, nil
}

// ValidateTraceID checks the input is a valid W3C trace_id: exactly 32 hex
// chars, not all zeros (W3C reserves all-zero as invalid).
func ValidateTraceID(t string) error {
	if len(t) != 32 {
		return fmt.Errorf("must be 32 hex chars, got %d", len(t))
	}
	if !isHex(t) {
		return errors.New("must be hex")
	}
	if strings.Count(t, "0") == 32 {
		return errors.New("must not be all zeros (W3C reserved)")
	}
	return nil
}

// ValidateSpanID checks the input is a valid W3C span_id: exactly 16 hex
// chars, not all zeros.
func ValidateSpanID(s string) error {
	if len(s) != 16 {
		return fmt.Errorf("must be 16 hex chars, got %d", len(s))
	}
	if !isHex(s) {
		return errors.New("must be hex")
	}
	if strings.Count(s, "0") == 16 {
		return errors.New("must not be all zeros (W3C reserved)")
	}
	return nil
}

func isHex(s string) bool {
	for _, c := range s {
		switch {
		case c >= '0' && c <= '9':
		case c >= 'a' && c <= 'f':
		case c >= 'A' && c <= 'F':
		default:
			return false
		}
	}
	return true
}

// SumCost returns the int64 sum of costs in micros. Returns an error if
// any entry is negative or the running sum would overflow int64. Empty
// input returns (0, nil).
func SumCost(costs []int64) (int64, error) {
	var total int64
	for i, c := range costs {
		if c < 0 {
			return 0, fmt.Errorf("entry %d negative: %d", i, c)
		}
		// Overflow guard: total + c > MaxInt64 ⇔ c > MaxInt64 - total.
		if c > math.MaxInt64-total {
			return 0, fmt.Errorf("overflow at entry %d (running=%d c=%d)", i, total, c)
		}
		total += c
	}
	return total, nil
}

// ListFilter is the query filter for repository List/SumCost.
type ListFilter struct {
	From   time.Time // inclusive; zero = no lower bound
	To     time.Time // exclusive; zero = no upper bound
	Limit  int       // 0 = default (100); cap 1000
	Offset int
}
