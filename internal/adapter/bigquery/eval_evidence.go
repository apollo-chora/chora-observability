// eval_evidence.go — the production BigQuery read adapter for agent-eval
// evidence (eval.Repository), over the view
// chora_observability_analytics.agent_eval_evidence (per-row functional
// autorater + adversarial red-team results from the agent CI/CD eval gate,
// ADR-169 / CHO-1674).
//
// This is the first REAL google-cloud-go BigQuery read in Chora's Go services
// (the existing MockClient in this package serves the reconcile SUM skeleton).
// The client uses Application Default Credentials → Workload Identity
// Federation in-cluster (no SA key files; consistent with secrets-and-env).
//
// The view is PLATFORM-scoped (no tenant_id column) — none of these queries
// filter by tenant. The pure helpers (buildListRunsQuery / buildEvidenceQuery
// / assembleRuns / toEvidenceRow) carry all the logic and are unit-tested
// without a live BigQuery.
package bigquery

import (
	"context"
	"fmt"
	"sort"
	"time"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"

	"github.com/apollo-chora/chora-observability/internal/domain/eval"
)

// EvidenceClient reads the agent_eval_evidence view. Implements eval.Repository.
type EvidenceClient struct {
	bq    *bigquery.Client
	table string // fully-qualified, back-quoted `project.dataset.view`
}

// NewEvidenceClient constructs a real BigQuery-backed eval.Repository.
//
// project/dataset/view identify the agent_eval_evidence view; location is the
// dataset's BigQuery location (asia-southeast1) — REQUIRED for a single-region
// dataset or the job 404s on the multi-region default. Credentials come from
// ADC (Workload Identity in-cluster).
func NewEvidenceClient(ctx context.Context, project, dataset, view, location string) (*EvidenceClient, error) {
	c, err := bigquery.NewClient(ctx, project)
	if err != nil {
		return nil, fmt.Errorf("bigquery.NewClient: %w", err)
	}
	if location != "" {
		c.Location = location
	}
	return &EvidenceClient{
		bq:    c,
		table: fmt.Sprintf("`%s.%s.%s`", project, dataset, view),
	}, nil
}

// Close releases the underlying BigQuery client.
func (c *EvidenceClient) Close() error { return c.bq.Close() }

// evalAggRow is the scan target for the grouped ListRuns query and the input
// to assembleRuns. With GROUP BY, COUNT/COUNTIF/MAX are non-null; AVG(score)
// is guarded with NullFloat64 for the all-null edge case.
type evalAggRow struct {
	CandidateLabel string               `bigquery:"candidate_label"`
	Experiment     string               `bigquery:"experiment"`
	Kind           string               `bigquery:"kind"`
	Metric         string               `bigquery:"metric"`
	N              int64                `bigquery:"n"`
	AvgScore       bigquery.NullFloat64 `bigquery:"avg_score"`
	Blocked        int64                `bigquery:"blocked"`
	LastAt         time.Time            `bigquery:"last_at"`
}

// buildListRunsQuery builds the grouped crew-run aggregate query + params.
// Aggregates at (candidate_label, experiment, kind, metric); assembleRuns
// folds the flat rows into the nested crew→member shape.
func buildListRunsQuery(table string, f eval.RunFilter) (string, []bigquery.QueryParameter) {
	var params []bigquery.QueryParameter
	where := "WHERE TRUE"
	if f.Experiment != "" {
		where += "\n  AND experiment = @experiment"
		params = append(params, bigquery.QueryParameter{Name: "experiment", Value: f.Experiment})
	}
	if f.Kind != "" {
		where += "\n  AND kind = @kind"
		params = append(params, bigquery.QueryParameter{Name: "kind", Value: f.Kind})
	}
	q := fmt.Sprintf(`SELECT
  candidate_label AS candidate_label,
  experiment AS experiment,
  kind AS kind,
  metric AS metric,
  COUNT(*) AS n,
  AVG(score) AS avg_score,
  COUNTIF(STARTS_WITH(adversarial_verdict, 'BLOCKED')) AS blocked,
  MAX(recorded_at) AS last_at
FROM %s
%s
GROUP BY candidate_label, experiment, kind, metric
ORDER BY last_at DESC`, table, where)
	return q, params
}

// assembleRuns folds flat aggregate rows into crew runs (grouped by
// candidate_label) → members (grouped by experiment). Autorater rows become
// per-metric summaries; adversarial rows accumulate into a block/leak summary.
// Runs are ordered most-recent-first (by max recorded_at, then label) and
// capped to limit (when > 0). Pure — unit-tested without BigQuery.
func assembleRuns(rows []evalAggRow, limit int) []eval.Run {
	type memberAcc struct {
		experiment string
		metrics    []eval.MetricSummary
		advTotal   int
		advBlocked int
		hasAdv     bool
	}
	type runAcc struct {
		label   string
		lastAt  time.Time
		members map[string]*memberAcc
	}
	runs := map[string]*runAcc{}
	var order []string
	for _, r := range rows {
		ra, ok := runs[r.CandidateLabel]
		if !ok {
			ra = &runAcc{label: r.CandidateLabel, members: map[string]*memberAcc{}}
			runs[r.CandidateLabel] = ra
			order = append(order, r.CandidateLabel)
		}
		if r.LastAt.After(ra.lastAt) {
			ra.lastAt = r.LastAt
		}
		ma, ok := ra.members[r.Experiment]
		if !ok {
			ma = &memberAcc{experiment: r.Experiment}
			ra.members[r.Experiment] = ma
		}
		if r.Kind == "adversarial" {
			ma.hasAdv = true
			ma.advTotal += int(r.N)
			ma.advBlocked += int(r.Blocked)
			continue
		}
		// autorater (functional) — per-metric summary.
		avg := 0.0
		if r.AvgScore.Valid {
			avg = r.AvgScore.Float64
		}
		ma.metrics = append(ma.metrics, eval.MetricSummary{
			Metric:   r.Metric,
			Count:    int(r.N),
			AvgScore: avg,
		})
	}

	out := make([]eval.Run, 0, len(runs))
	for _, label := range order {
		ra := runs[label]
		members := make([]eval.RunMember, 0, len(ra.members))
		for _, ma := range ra.members {
			sort.Slice(ma.metrics, func(i, j int) bool { return ma.metrics[i].Metric < ma.metrics[j].Metric })
			m := eval.RunMember{Experiment: ma.experiment, AutoraterMetrics: ma.metrics}
			if ma.hasAdv {
				m.Adversarial = &eval.AdversarialSummary{
					Total:   ma.advTotal,
					Blocked: ma.advBlocked,
					Leaked:  ma.advTotal - ma.advBlocked,
				}
			}
			members = append(members, m)
		}
		sort.Slice(members, func(i, j int) bool { return members[i].Experiment < members[j].Experiment })
		out = append(out, eval.Run{
			CandidateLabel: ra.label,
			LastRecordedAt: ra.lastAt,
			Members:        members,
		})
	}
	sort.SliceStable(out, func(i, j int) bool {
		if !out[i].LastRecordedAt.Equal(out[j].LastRecordedAt) {
			return out[i].LastRecordedAt.After(out[j].LastRecordedAt)
		}
		return out[i].CandidateLabel < out[j].CandidateLabel
	})
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out
}

// ListRuns runs the grouped aggregate query and folds it into crew runs.
func (c *EvidenceClient) ListRuns(ctx context.Context, f eval.RunFilter) ([]eval.Run, error) {
	sql, params := buildListRunsQuery(c.table, f)
	q := c.bq.Query(sql)
	q.Parameters = params
	it, err := q.Read(ctx)
	if err != nil {
		return nil, fmt.Errorf("eval list-runs read: %w", err)
	}
	var rows []evalAggRow
	for {
		var r evalAggRow
		err := it.Next(&r)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("eval list-runs next: %w", err)
		}
		rows = append(rows, r)
	}
	return assembleRuns(rows, f.Limit), nil
}

// evalEvidenceScanRow is the scan target for the per-row drill-down query.
// Most columns are NULLABLE in the view, so each uses a Null wrapper.
type evalEvidenceScanRow struct {
	Experiment         string               `bigquery:"experiment"`
	Kind               string               `bigquery:"kind"`
	CaseID             bigquery.NullString  `bigquery:"case_id"`
	RowIndex           bigquery.NullInt64   `bigquery:"row_index"`
	Metric             bigquery.NullString  `bigquery:"metric"`
	Score              bigquery.NullFloat64 `bigquery:"score"`
	AdversarialVerdict bigquery.NullString  `bigquery:"adversarial_verdict"`
	Explanation        bigquery.NullString  `bigquery:"explanation"`
	Prompt             bigquery.NullString  `bigquery:"prompt"`
	Response           bigquery.NullString  `bigquery:"response"`
	Reference          bigquery.NullString  `bigquery:"reference"`
	RecordedAt         time.Time            `bigquery:"recorded_at"`
}

// toEvidenceRow maps a BigQuery scan row to the domain EvidenceRow, unwrapping
// the Null types to their zero values when absent. Pure — unit-tested.
func toEvidenceRow(r evalEvidenceScanRow) eval.EvidenceRow {
	return eval.EvidenceRow{
		Experiment:         r.Experiment,
		Kind:               r.Kind,
		CaseID:             r.CaseID.StringVal,
		RowIndex:           int(r.RowIndex.Int64),
		Metric:             r.Metric.StringVal,
		Score:              r.Score.Float64,
		AdversarialVerdict: r.AdversarialVerdict.StringVal,
		Explanation:        r.Explanation.StringVal,
		Prompt:             r.Prompt.StringVal,
		Response:           r.Response.StringVal,
		Reference:          r.Reference.StringVal,
		RecordedAt:         r.RecordedAt,
	}
}

// buildEvidenceQuery builds the per-row drill-down query + params for one crew
// run (f.CandidateLabel required).
func buildEvidenceQuery(table string, f eval.EvidenceFilter) (string, []bigquery.QueryParameter) {
	params := []bigquery.QueryParameter{{Name: "candidate_label", Value: f.CandidateLabel}}
	where := "WHERE candidate_label = @candidate_label"
	if f.Experiment != "" {
		where += "\n  AND experiment = @experiment"
		params = append(params, bigquery.QueryParameter{Name: "experiment", Value: f.Experiment})
	}
	if f.Kind != "" {
		where += "\n  AND kind = @kind"
		params = append(params, bigquery.QueryParameter{Name: "kind", Value: f.Kind})
	}
	limit := f.Limit
	if limit <= 0 {
		limit = 200
	}
	params = append(params, bigquery.QueryParameter{Name: "lim", Value: int64(limit)})
	q := fmt.Sprintf(`SELECT
  experiment, kind, case_id, row_index, metric, score,
  adversarial_verdict, explanation, prompt, response, reference, recorded_at
FROM %s
%s
ORDER BY experiment, kind, row_index, metric
LIMIT @lim`, table, where)
	return q, params
}

// ListEvidence runs the per-row drill-down query for one crew run.
func (c *EvidenceClient) ListEvidence(ctx context.Context, f eval.EvidenceFilter) ([]eval.EvidenceRow, error) {
	sql, params := buildEvidenceQuery(c.table, f)
	q := c.bq.Query(sql)
	q.Parameters = params
	it, err := q.Read(ctx)
	if err != nil {
		return nil, fmt.Errorf("eval evidence read: %w", err)
	}
	var out []eval.EvidenceRow
	for {
		var r evalEvidenceScanRow
		err := it.Next(&r)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("eval evidence next: %w", err)
		}
		out = append(out, toEvidenceRow(r))
	}
	return out, nil
}

var _ eval.Repository = (*EvidenceClient)(nil)
