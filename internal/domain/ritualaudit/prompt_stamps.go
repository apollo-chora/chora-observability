// prompt_stamps.go - CHO-2364 (ADR-197 read slice) prompt-evidence
// aggregation port over the ritual_run_audit stamps JSONB array.
//
// Each ritual run row stores a JSON ARRAY of per-step decision stamps
// (migration 0013). The canonical stamp key for the per-step prompt version
// is snake_case 'prompt_version' (the CHO-2136 wire shape); rows ingested
// from the pre-CHO-2136 legacy JSON wire stored Go field names, so readers
// must also accept 'PromptVersion' or those runs silently vanish from the
// evidence.
package ritualaudit

import (
	"context"
	"time"
)

// StampVersionCount is one distinct non-empty stamp prompt version with the
// number of DISTINCT runs it appeared in (a version repeated across steps of
// one run counts once) and the most recent run time it was seen at.
type StampVersionCount struct {
	PromptVersion string
	Runs          int64
	LastSeen      time.Time
}

// PromptStampSummary is the familiar-agent prompt evidence: total terminal
// runs + the most recent run time + the per-version run counts. LastRunAt is
// the zero time when the tenant has no runs (the caller omits the field,
// never fabricates).
type PromptStampSummary struct {
	RunsTotal int64
	LastRunAt time.Time
	Versions  []StampVersionCount
}

// PromptStampRepository is the read port for the ritual stamp aggregation.
// Implemented by the pg and inmem ritual-audit repositories; the
// agent-prompts endpoint 503s loudly when its ritual repository does not
// provide it.
type PromptStampRepository interface {
	// AggregatePromptStamps aggregates the tenant's ritual_run_audit rows.
	// A tenant with no runs returns a zero summary with an empty (non-nil)
	// Versions slice, never an error.
	AggregatePromptStamps(ctx context.Context, tenantID string) (PromptStampSummary, error)
}
