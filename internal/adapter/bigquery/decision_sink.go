// decision_sink.go — streams persisted agent decisions into the BigQuery
// analytics table chora_observability_analytics.agent_decision_log.
//
// Closes the "BQ mirror empty" gap: the Postgres agent_decision_log is the
// canonical store (read by the O+ BFF), but the BigQuery table — provisioned by
// chora-infra/terraform/modules/bigquery — had no writer, so the mirror was
// empty (0 rows) and the O+ "View in BigQuery" + auditor BQ queries saw
// nothing. This sink mirrors each persisted decision via the BigQuery streaming
// inserter, keyed on log_id for best-effort de-dupe.
//
// Best-effort by contract (the consumer never fails the ack on a sink error);
// Postgres remains authoritative. Credentials come from ADC (Workload Identity
// in-cluster). Implements events.DecisionBQSink.
package bigquery

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/bigquery"

	"github.com/apollo-chora/chora-observability/internal/domain/decision"
)

// DecisionSink streams agent decisions into BigQuery.
type DecisionSink struct {
	bq       *bigquery.Client
	inserter *bigquery.Inserter
	schema   bigquery.Schema
}

// NewDecisionSink constructs a real BigQuery-backed decision sink. project /
// dataset / table identify the agent_decision_log table; location is the
// dataset's BigQuery location (asia-southeast1 — REQUIRED for a single-region
// dataset). Credentials come from ADC (Workload Identity in-cluster).
func NewDecisionSink(ctx context.Context, project, dataset, table, location string) (*DecisionSink, error) {
	c, err := bigquery.NewClient(ctx, project)
	if err != nil {
		return nil, fmt.Errorf("bigquery.NewClient: %w", err)
	}
	if location != "" {
		c.Location = location
	}
	schema, err := bigquery.InferSchema(decisionBQRow{})
	if err != nil {
		_ = c.Close()
		return nil, fmt.Errorf("bigquery.InferSchema(decisionBQRow): %w", err)
	}
	return &DecisionSink{
		bq:       c,
		inserter: c.Dataset(dataset).Table(table).Inserter(),
		schema:   schema,
	}, nil
}

// Close releases the underlying BigQuery client.
func (s *DecisionSink) Close() error { return s.bq.Close() }

// decisionBQRow maps a decision.Log onto the agent_decision_log BQ schema.
// Fields the Postgres read-model does not carry (model_version, prompt_id,
// chora_imda_dimension, imda_lifecycle_stage, accountability_owner, latency_ms)
// are intentionally omitted — they stay NULL until a producer emits them.
type decisionBQRow struct {
	LogID            string    `bigquery:"log_id"`
	TenantID         string    `bigquery:"tenant_id"`
	Agid             string    `bigquery:"agid"`
	AgentID          string    `bigquery:"agent_id"`
	RunID            string    `bigquery:"run_id"`
	DecisionType     string    `bigquery:"decision_type"`
	ReasoningSummary string    `bigquery:"reasoning_summary"`
	InputHash        string    `bigquery:"input_hash"`
	OutputHash       string    `bigquery:"output_hash"`
	ModelID          string    `bigquery:"model_id"`
	GuardrailVerdict string    `bigquery:"guardrail_verdict"`
	AutonomyLevel    string    `bigquery:"autonomy_level"`
	RiskTier         string    `bigquery:"risk_tier"`
	CorrelationID    string    `bigquery:"correlation_id"`
	Traceparent      string    `bigquery:"traceparent"`
	DecidedAt        time.Time `bigquery:"decided_at"`
}

// toDecisionBQRow projects a decision.Log to the BQ row. decision_type prefers
// the qgen quality-gate verdict (the human-meaningful "what did the agent
// decide") when present, mirroring the O+ BFF mapping.
func toDecisionBQRow(l *decision.Log) *decisionBQRow {
	row := &decisionBQRow{
		LogID:            l.LogID,
		TenantID:         l.TenantID,
		Agid:             l.Agid,
		AgentID:          l.Agid,
		RunID:            l.CrewID,
		DecisionType:     string(l.DecisionType),
		ModelID:          l.ModelID,
		GuardrailVerdict: l.GuardrailOutcome,
		AutonomyLevel:    l.AutonomyLevel,
		RiskTier:         string(l.RiskTier),
		CorrelationID:    l.CorrelationID,
		Traceparent:      l.Traceparent,
		DecidedAt:        l.CreatedAt,
	}
	if l.Reasoning != nil {
		row.ReasoningSummary = l.Reasoning.Summary
		row.InputHash = l.Reasoning.InputHash
		row.OutputHash = l.Reasoning.OutputHash
	}
	if l.Verdict != "" {
		row.DecisionType = l.Verdict
	}
	return row
}

// Insert streams one decision row into BigQuery, keyed on log_id so
// at-least-once redelivery de-dupes within BigQuery's best-effort window.
func (s *DecisionSink) Insert(ctx context.Context, l *decision.Log) error {
	if l == nil {
		return nil
	}
	saver := &bigquery.StructSaver{
		Struct:   toDecisionBQRow(l),
		Schema:   s.schema,
		InsertID: l.LogID,
	}
	if err := s.inserter.Put(ctx, saver); err != nil {
		return fmt.Errorf("bigquery decision insert (log_id=%s): %w", l.LogID, err)
	}
	return nil
}
