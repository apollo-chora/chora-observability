// decision_repository.go — pgx-backed implementation of decision.Repository.
//
// SQL contract:
//
//   - Append: INSERT INTO agent_decision_log (...). The migration installs
//     append-only triggers — UPDATE/DELETE are rejected at the table level.
//   - List: filter by tenant_id + optional agid + half-open [from, to) window;
//     ORDER BY recorded_at — ASC by default (the /o/agents collectStats
//     contract), DESC when ListFilter.Descending is set (newest-first audit
//     list) — + LIMIT/OFFSET pagination.
//   - GetByID: single-row read by log_id scoped to tenant; ErrNotFound when
//     no row matches.
//   - Count: aggregate over the half-open window [since, until). Used by the
//     O+ Dashboard `recent_decisions_24h` rollup. Zero is a legitimate
//     answer; the caller treats it as honest "no recent activity". Query
//     errors are surfaced loudly per [[feedback-no-stubs-real-wiring]].
//
// Tenant isolation: agent_decision_log has RLS enabled (migration 0001) with
// the canonical `current_setting('chora.tenant_id', true)::uuid` policy. The
// tenant_id parameter is passed for callers that explicitly scope the query;
// production deploys MUST also wrap calls in a transaction that runs
// `SET LOCAL chora.tenant_id` first (the pg.SetTenantID helper handles this
// when wired into a transaction-aware Querier). The repository methods stay
// pool-friendly so dev/test paths without RLS still work.
package pg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/apollo-chora/chora-observability/internal/domain/decision"
)

// DecisionRepository is the pgx-backed implementation of decision.Repository.
type DecisionRepository struct {
	q Querier
}

// NewDecisionRepository wraps a Querier.
func NewDecisionRepository(q Querier) *DecisionRepository {
	return &DecisionRepository{q: q}
}

// Append persists the decision log. Column order per migrations/0001_initial.sql:
//
//	log_id, tenant_id, agid, decision_type, reasoning_summary,
//	risk_tier, correlation_id, input_hash, output_hash, latency_ms,
//	traceparent, recorded_at
func (r *DecisionRepository) Append(ctx context.Context, l *decision.Log) error {
	if l == nil {
		return errors.New("pg.DecisionRepository.Append: nil log")
	}
	inputHash := ""
	outputHash := ""
	latencyMs := 0
	reasoningSummary := ""
	if l.Reasoning != nil {
		inputHash = l.Reasoning.InputHash
		outputHash = l.Reasoning.OutputHash
		latencyMs = l.Reasoning.LatencyMs
		reasoningSummary = l.Reasoning.Summary
	}
	// Log.Agid carries the crew agent_role (e.g. "qgen_critic") — a STRING,
	// not a UUID. It belongs in the agent_id VARCHAR column. Unlike
	// token_usage_ledger.agid (nullable), agent_decision_log.agid is uuid
	// NOT NULL, so it gets the nil-uuid sentinel (role-only crew agents have
	// no real AGID). Writing the role into agid::uuid is the latent defect
	// that kept this table empty ("invalid input syntax for type uuid").
	//
	// CHO-1560 +9-field projection columns appended after recorded_at:
	//   model_id (0002), confidence (0009), cost_usd_micros (0009),
	//   crew_name (0009), crew_id (0009), prompt/completion/cached_tokens
	//   (0009), guardrail_verdict (0002; <- proto guardrail_outcome),
	//   autonomy_level (0002 enum). guardrail_verdict + autonomy_level take
	//   NULLIF + ::cast so an empty string lands as SQL NULL (the CHECK +
	//   enum reject ''). confidence / cost / tokens bind as *float32 / *int64
	//   so a nil pointer is SQL NULL (legitimate for non-LLM decisions).
	//
	// CHO question_type (0010) is appended last — NULLIF($23,'') so a
	// non-question decision lands SQL NULL. The qgen crew's per-question-type
	// tag ("mcq"|"oe") feeds the O+ /o/agents qgen-mcq / qgen-OE tile split.
	//
	// verdict (0011) — the qgen quality-gate outcome (accepted | rejected |
	// completed_with_warning | refused | retry). NULLIF($24,'') so a
	// non-verdict decision lands SQL NULL. Distinct from decision_type (the
	// 4-value ENUM); the O+ DECISION TYPE column prefers it (CHO-1700 follow-up).
	//
	// prompt_conditions (0012) — the durable prompt-explainability map (ADR-197
	// M-A.4), marshalled to a JSON object string here. NULLIF($25,'')::jsonb so
	// a decision with no recorded conditions lands SQL NULL (never a fabricated
	// empty object).
	q := `
        INSERT INTO agent_decision_log (
            log_id, tenant_id, agid, agent_id, decision_type, reasoning_summary,
            risk_tier, correlation_id, input_hash, output_hash, latency_ms,
            traceparent, recorded_at,
            model_id, confidence, cost_usd_micros, crew_name, crew_id,
            prompt_tokens, completion_tokens, cached_tokens,
            guardrail_verdict, autonomy_level, question_type, verdict,
            prompt_conditions
        )
        VALUES ($1, $2, '00000000-0000-0000-0000-000000000000', NULLIF($3, ''), $4, $5,
                $6, $7, $8, $9, $10,
                NULLIF($11, ''), $12,
                NULLIF($13, ''), $14, $15, NULLIF($16, ''), NULLIF($17, ''),
                $18, $19, $20,
                NULLIF($21, ''), NULLIF($22, '')::autonomy_level, NULLIF($23, ''),
                NULLIF($24, ''), NULLIF($25, '')::jsonb)
    `
	// Marshal the conditions map to a JSON object string ('' when empty so the
	// NULLIF lands SQL NULL — never a fabricated value). json.Marshal of a
	// nil/empty map yields "{}" which we collapse to "" to keep the column NULL.
	promptConditionsJSON, err := marshalPromptConditions(l.PromptConditions)
	if err != nil {
		return fmt.Errorf("pg.DecisionRepository.Append: marshal prompt_conditions: %w", err)
	}
	// tenant_id normalised ("platform"/"" → NilTenantUUID) to satisfy the
	// UUID column + the SET LOCAL chora.tenant_id GUC the RLS policy checks.
	tenantID := NormalizeTenantForRLS(l.TenantID)
	return r.q.WithTenantTx(ctx, tenantID, func(ctx context.Context, tx TenantScopedQuerier) error {
		return tx.Exec(ctx, q,
			l.LogID,
			tenantID,
			l.Agid,
			string(l.DecisionType),
			reasoningSummary,
			string(l.RiskTier),
			l.CorrelationID,
			emptyHash(inputHash),
			emptyHash(outputHash),
			latencyMs,
			l.Traceparent,
			l.CreatedAt,
			l.ModelID,            // $13
			l.Confidence,         // $14 (*float32 → REAL/NULL)
			l.CostUsdMicros,      // $15 (*int64 → BIGINT/NULL)
			l.CrewName,           // $16
			l.CrewID,             // $17
			l.PromptTokens,       // $18
			l.CompletionTokens,   // $19
			l.CachedTokens,       // $20
			l.GuardrailOutcome,   // $21 → guardrail_verdict
			l.AutonomyLevel,      // $22 → autonomy_level enum
			l.QuestionType,       // $23 → question_type (NULLIF '' → NULL)
			l.Verdict,            // $24 → verdict (NULLIF '' → NULL)
			promptConditionsJSON, // $25 → prompt_conditions (NULLIF ''::jsonb → NULL)
		)
	})
}

// List returns decision logs matching the filter, sorted recorded_at ASC by
// default; when f.Descending is set the order is recorded_at DESC (newest-first,
// used by the O+ Decision Traces audit list). Default ASC is the /o/agents
// collectStats contract — do NOT flip it.
func (r *DecisionRepository) List(ctx context.Context, tenantID string, f decision.ListFilter) ([]*decision.Log, error) {
	limit := f.Limit
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}
	offset := f.Offset
	if offset < 0 {
		offset = 0
	}
	// order is a controlled literal — never user input — so it is safe to
	// interpolate. The $1..$6 params + LIMIT/OFFSET are unchanged.
	order := "ASC"
	if f.Descending {
		order = "DESC"
	}
	q := fmt.Sprintf(`
        SELECT log_id, tenant_id, COALESCE(agent_id, ''), decision_type::text,
               reasoning_summary, risk_tier::text, correlation_id,
               input_hash, output_hash, latency_ms,
               COALESCE(traceparent, ''), recorded_at,
               COALESCE(model_id, ''), confidence, cost_usd_micros,
               COALESCE(crew_name, ''), COALESCE(crew_id, ''),
               COALESCE(prompt_tokens, 0), COALESCE(completion_tokens, 0),
               COALESCE(cached_tokens, 0),
               COALESCE(guardrail_verdict, ''), COALESCE(autonomy_level::text, ''),
               COALESCE(question_type, ''), COALESCE(verdict, ''),
               COALESCE(prompt_conditions::text, '')
        FROM agent_decision_log
        WHERE tenant_id = $1
          AND ($2::text = '' OR agent_id = $2)
          AND ($3::timestamptz IS NULL OR recorded_at >= $3)
          AND ($4::timestamptz IS NULL OR recorded_at <  $4)
        ORDER BY recorded_at %s
        LIMIT $5 OFFSET $6
    `, order)
	// Reads are RLS-scoped too — without the tenant GUC the policy filters
	// every row to zero regardless of the WHERE clause.
	tenantID = NormalizeTenantForRLS(tenantID)
	out := make([]*decision.Log, 0)
	err := r.q.WithTenantTx(ctx, tenantID, func(ctx context.Context, tx TenantScopedQuerier) error {
		rows, err := tx.Query(ctx, q,
			tenantID,
			f.Agid,
			nullableTime(f.From),
			nullableTime(f.To),
			limit, offset,
		)
		if err != nil {
			return fmt.Errorf("pg.DecisionRepository.List: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			l := &decision.Log{}
			var (
				decType             string
				riskTier            string
				summary             string
				inputHash           string
				outputHash          string
				latencyMs           int
				promptConditionsRaw string
			)
			if err := rows.Scan(
				&l.LogID,
				&l.TenantID,
				&l.Agid,
				&decType,
				&summary,
				&riskTier,
				&l.CorrelationID,
				&inputHash,
				&outputHash,
				&latencyMs,
				&l.Traceparent,
				&l.CreatedAt,
				&l.ModelID,
				&l.Confidence,
				&l.CostUsdMicros,
				&l.CrewName,
				&l.CrewID,
				&l.PromptTokens,
				&l.CompletionTokens,
				&l.CachedTokens,
				&l.GuardrailOutcome,
				&l.AutonomyLevel,
				&l.QuestionType,
				&l.Verdict,
				&promptConditionsRaw,
			); err != nil {
				return fmt.Errorf("pg.DecisionRepository.List scan: %w", err)
			}
			if pc, err := unmarshalPromptConditions(promptConditionsRaw); err != nil {
				return fmt.Errorf("pg.DecisionRepository.List prompt_conditions: %w", err)
			} else {
				l.PromptConditions = pc
			}
			l.DecisionType = decision.Type(decType)
			l.RiskTier = decision.RiskTier(riskTier)
			if inputHash != "" || outputHash != "" || latencyMs > 0 || summary != "" {
				l.Reasoning = &decision.ReasoningSummary{
					InputHash:  inputHash,
					OutputHash: outputHash,
					LatencyMs:  latencyMs,
					Summary:    summary,
				}
			}
			// Bridge storage (raw confidence + micros) → serialization fields
			// (float64 confidence + float64 USD) the gateway reads.
			l.PopulateProjectionOut()
			out = append(out, l)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// GetByID returns the log with the given ID for the tenant.
func (r *DecisionRepository) GetByID(ctx context.Context, tenantID, logID string) (*decision.Log, error) {
	q := `
        SELECT log_id, tenant_id, COALESCE(agent_id, ''), decision_type::text,
               reasoning_summary, risk_tier::text, correlation_id,
               input_hash, output_hash, latency_ms,
               COALESCE(traceparent, ''), recorded_at,
               COALESCE(model_id, ''), confidence, cost_usd_micros,
               COALESCE(crew_name, ''), COALESCE(crew_id, ''),
               COALESCE(prompt_tokens, 0), COALESCE(completion_tokens, 0),
               COALESCE(cached_tokens, 0),
               COALESCE(guardrail_verdict, ''), COALESCE(autonomy_level::text, ''),
               COALESCE(question_type, ''), COALESCE(verdict, ''),
               COALESCE(prompt_conditions::text, '')
        FROM agent_decision_log
        WHERE tenant_id = $1 AND log_id = $2
        LIMIT 1
    `
	tenantID = NormalizeTenantForRLS(tenantID)
	l := &decision.Log{}
	var (
		decType             string
		riskTier            string
		summary             string
		inputHash           string
		outputHash          string
		latencyMs           int
		promptConditionsRaw string
	)
	if err := r.q.WithTenantTx(ctx, tenantID, func(ctx context.Context, tx TenantScopedQuerier) error {
		row := tx.QueryRow(ctx, q, tenantID, logID)
		if err := row.Scan(
			&l.LogID,
			&l.TenantID,
			&l.Agid,
			&decType,
			&summary,
			&riskTier,
			&l.CorrelationID,
			&inputHash,
			&outputHash,
			&latencyMs,
			&l.Traceparent,
			&l.CreatedAt,
			&l.ModelID,
			&l.Confidence,
			&l.CostUsdMicros,
			&l.CrewName,
			&l.CrewID,
			&l.PromptTokens,
			&l.CompletionTokens,
			&l.CachedTokens,
			&l.GuardrailOutcome,
			&l.AutonomyLevel,
			&l.QuestionType,
			&l.Verdict,
			&promptConditionsRaw,
		); err != nil {
			if errors.Is(err, ErrNoRows) {
				return decision.ErrNotFound
			}
			return fmt.Errorf("pg.DecisionRepository.GetByID: %w", err)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	pc, err := unmarshalPromptConditions(promptConditionsRaw)
	if err != nil {
		return nil, fmt.Errorf("pg.DecisionRepository.GetByID prompt_conditions: %w", err)
	}
	l.PromptConditions = pc
	l.DecisionType = decision.Type(decType)
	l.RiskTier = decision.RiskTier(riskTier)
	if inputHash != "" || outputHash != "" || latencyMs > 0 || summary != "" {
		l.Reasoning = &decision.ReasoningSummary{
			InputHash:  inputHash,
			OutputHash: outputHash,
			LatencyMs:  latencyMs,
			Summary:    summary,
		}
	}
	l.PopulateProjectionOut()
	return l, nil
}

// Count returns the number of decision logs for the tenant in the half-open
// window [since, until). Backs the O+ dashboard `recent_decisions_24h` rollup.
//
// SQL: SELECT COUNT(*) FROM agent_decision_log
//
//	WHERE tenant_id = $1
//	  AND ($2::timestamptz IS NULL OR recorded_at >= $2)
//	  AND ($3::timestamptz IS NULL OR recorded_at <  $3)
//
// Per [[feedback-no-stubs-real-wiring]] — a zero-count result is honest;
// any query error surfaces loudly. Caller is responsible for choosing the
// window (handler defaults to last 24h).
func (r *DecisionRepository) Count(ctx context.Context, tenantID string, since, until time.Time) (int64, error) {
	q := `
        SELECT COUNT(*)
        FROM agent_decision_log
        WHERE tenant_id = $1
          AND ($2::timestamptz IS NULL OR recorded_at >= $2)
          AND ($3::timestamptz IS NULL OR recorded_at <  $3)
    `
	tenantID = NormalizeTenantForRLS(tenantID)
	var n int64
	if err := r.q.WithTenantTx(ctx, tenantID, func(ctx context.Context, tx TenantScopedQuerier) error {
		row := tx.QueryRow(ctx, q,
			tenantID,
			nullableTime(since),
			nullableTime(until),
		)
		if err := row.Scan(&n); err != nil {
			return fmt.Errorf("pg.DecisionRepository.Count: %w", err)
		}
		return nil
	}); err != nil {
		return 0, err
	}
	return n, nil
}

// marshalPromptConditions encodes the durable prompt-explainability map
// (ADR-197 M-A.4) to a JSON object string for the prompt_conditions JSONB
// column. An empty/nil map returns "" so the Append NULLIF(”)::jsonb lands SQL
// NULL — a decision with no recorded conditions never persists a fabricated
// empty object.
func marshalPromptConditions(m map[string]string) (string, error) {
	if len(m) == 0 {
		return "", nil
	}
	b, err := json.Marshal(m)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// unmarshalPromptConditions decodes the prompt_conditions JSONB column text
// (COALESCE'd to ” when NULL) back into a map. Empty text → nil map (never a
// fabricated value). A malformed JSON object surfaces loudly (the caller NACKs /
// returns the error) rather than silently dropping the conditions.
func unmarshalPromptConditions(raw string) (map[string]string, error) {
	if raw == "" {
		return nil, nil
	}
	var m map[string]string
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		return nil, err
	}
	if len(m) == 0 {
		return nil, nil
	}
	return m, nil
}

// emptyHash returns a zero-pad-equivalent for the CHAR(64) input_hash /
// output_hash columns when the caller hasn't supplied a real hash. The
// schema's CHAR(64) NOT NULL constraint forbids NULLs; we emit 64 zero hex
// chars as the canonical "no IO captured" sentinel. Real decisions always
// populate hashes via decision.NewReasoningSummary.
func emptyHash(h string) string {
	if h == "" {
		return "0000000000000000000000000000000000000000000000000000000000000000"
	}
	return h
}

// Compile-time check.
var _ decision.Repository = (*DecisionRepository)(nil)
