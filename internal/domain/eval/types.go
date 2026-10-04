// Package eval is the read-side domain for agent-eval evidence — the per-row
// functional-autorater + adversarial red-team results emitted by the agent
// CI/CD eval gate (ADR-169 / CHO-1674) and persisted to the BigQuery view
// chora_observability_analytics.agent_eval_evidence.
//
// Observability owns this read because the evidence lives in the observability
// analytics dataset (same class as agent_decision_log). The data is
// PLATFORM-scoped eval telemetry — it carries NO tenant_id and is never
// filtered by tenant (cross-DB / cross-context queries remain forbidden; this
// is a single-context read over an observability-owned BigQuery view).
//
// Crew model (per the 2026-06-07 crew-view decision): a CREW RUN is keyed by
// `candidate_label` (shared by all members of one gate run); a MEMBER identity
// is the `experiment` column (e.g. chora-agent-eval-qgen-question).
package eval

import "time"

// MetricSummary is a per-metric autorater aggregate within one crew member.
//
// Scores are metric-scaled — `safety` is binary 0/1, `instruction_following`
// is a 1-5 Likert — so AvgScore is taken PER metric and never blended across
// metrics.
type MetricSummary struct {
	Metric   string
	Count    int
	AvgScore float64
}

// AdversarialSummary is a red-team outcome aggregate within one crew member.
// Blocked = the attack was refused (pass); Leaked = the attack succeeded
// (fail). Total == Blocked + Leaked.
type AdversarialSummary struct {
	Total   int
	Blocked int
	Leaked  int
}

// RunMember is one crew member (identified by Experiment) within a crew run.
type RunMember struct {
	Experiment       string
	AutoraterMetrics []MetricSummary
	// Adversarial is nil when the run produced no adversarial rows for this
	// member (e.g. a scheduled smoke run that drove only the functional set).
	Adversarial *AdversarialSummary
}

// Run is one crew run keyed by CandidateLabel.
type Run struct {
	CandidateLabel string
	LastRecordedAt time.Time
	Members        []RunMember
}

// EvidenceRow is one scored per-case evidence row from the view.
type EvidenceRow struct {
	Experiment string
	Kind       string // autorater | adversarial
	CaseID     string
	RowIndex   int
	Metric     string
	Score      float64
	// AdversarialVerdict is set only for Kind == "adversarial"
	// (BLOCKED(pass) | LEAKED(fail)); empty otherwise.
	AdversarialVerdict string
	Explanation        string
	Prompt             string
	Response           string
	Reference          string
	RecordedAt         time.Time
}

// RunFilter narrows ListRuns. Empty fields mean "no filter". Limit caps the
// number of distinct crew runs returned (most-recent first); <= 0 means the
// repository's default.
type RunFilter struct {
	Experiment string
	Kind       string
	Limit      int
}

// EvidenceFilter narrows ListEvidence to one crew run (CandidateLabel is
// required). Limit caps the number of rows; <= 0 means the default.
type EvidenceFilter struct {
	CandidateLabel string
	Experiment     string
	Kind           string
	Limit          int
}
