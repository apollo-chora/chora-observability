package eval

import "context"

// Repository reads agent-eval evidence from the analytics store (the BigQuery
// agent_eval_evidence view). All reads are PLATFORM-scoped — there is no
// tenant filter (the view has no tenant_id column).
//
// Implementations: adapter/bigquery.EvidenceClient (production, real BigQuery)
// and per-test fakes.
type Repository interface {
	// ListRuns returns crew runs (grouped by candidate_label), most-recent
	// first, each with per-member autorater + adversarial summaries.
	ListRuns(ctx context.Context, f RunFilter) ([]Run, error)

	// ListEvidence returns the per-row evidence for a single crew run
	// (f.CandidateLabel), ordered by member + kind + row_index.
	ListEvidence(ctx context.Context, f EvidenceFilter) ([]EvidenceRow, error)
}
