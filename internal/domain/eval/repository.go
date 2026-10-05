package eval

import "context"

// Repository reads agent-eval evidence from the analytics store. All reads are
// PLATFORM-scoped — there is no tenant filter (the store has no tenant_id
// column).
//
// Implementations: none wired; per-test fakes satisfy the port.
type Repository interface {
	// ListRuns returns crew runs (grouped by candidate_label), most-recent
	// first, each with per-member autorater + adversarial summaries.
	ListRuns(ctx context.Context, f RunFilter) ([]Run, error)

	// ListEvidence returns the per-row evidence for a single crew run
	// (f.CandidateLabel), ordered by member + kind + row_index.
	ListEvidence(ctx context.Context, f EvidenceFilter) ([]EvidenceRow, error)
}
