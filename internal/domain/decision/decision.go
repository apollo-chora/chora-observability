// Package decision is the AgentDecisionLog aggregate of the Observability
// domain.
//
// AgentDecisionLog is APPEND-ONLY (per ddd-enforcement invariant #4 mirror).
// Each entry records a single agent routing/escalate/refuse/respond decision
// with the LLM-provided reason + per-agent risk tier (per ai-runtime-guardrails
// stub). Used by the Governance Gatekeeper / O+ surface auditor view.
package decision

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Type is one of the four canonical agent decision types.
type Type string

const (
	TypeRoute    Type = "route"    // routed to another agent / model
	TypeEscalate Type = "escalate" // escalated to human or higher tier
	TypeRefuse   Type = "refuse"   // refused (guardrail block)
	TypeRespond  Type = "respond"  // produced a final response
)

// Valid reports whether the type is a known decision type.
func (t Type) Valid() bool {
	switch t {
	case TypeRoute, TypeEscalate, TypeRefuse, TypeRespond:
		return true
	}
	return false
}

// RiskTier matches the per-agent risk YAML stub from ai-runtime-guardrails.
type RiskTier string

const (
	TierLow      RiskTier = "low"
	TierMedium   RiskTier = "medium"
	TierHigh     RiskTier = "high"
	TierCritical RiskTier = "critical"
)

// Valid reports whether the tier is one of low|medium|high|critical.
func (r RiskTier) Valid() bool {
	switch r {
	case TierLow, TierMedium, TierHigh, TierCritical:
		return true
	}
	return false
}

// Log is the append-only AgentDecisionLog record.
//
// The CHO-1560 +9-field projection adds the columns the O+ Decision Traces tab
// renders (Model / Confidence / Cost / autonomy / crew / token counts /
// guardrail). The JSON keys below match EXACTLY what chora-gateway's
// upstream.AgentDecisionRow + mapAgentDecision read — drift here re-blanks the
// O+ columns even when the data is present.
//
// Two representations coexist on purpose:
//   - Storage fields (Confidence *float32, CostUsdMicros *int64, PromptTokens…)
//     are what the Pub/Sub binding sets + the repo Append writes to the DB
//     columns. They are NOT serialized (json:"-").
//   - Serialization fields (ConfidenceOut *float64 `json:"confidence"`,
//     CostUSD *float64 `json:"cost_usd"`) are what the repo List/GetByID
//     populate on read + what the HTTP serializer hands to the gateway. The
//     gateway wants confidence + cost in float (USD, not micros).
//
// PopulateProjectionOut() bridges storage → serialization (micros→USD, float32
// →float64) and is called by the read paths.
type Log struct {
	LogID         string            `json:"log_id"`
	TenantID      string            `json:"tenant_id"`
	Agid          string            `json:"agid"`
	DecisionType  Type              `json:"decision_type"`
	Reason        string            `json:"reason"`
	RiskTier      RiskTier          `json:"risk_tier"`
	CorrelationID string            `json:"correlation_id"`
	Traceparent   string            `json:"traceparent"`
	Reasoning     *ReasoningSummary `json:"reasoning,omitempty"`
	CreatedAt     time.Time         `json:"created_at"`

	// --- CHO-1560 +9-field projection -------------------------------------
	// ModelID is the concrete model that produced the decision (proto f6).
	// Maps to FE `model`. Empty for non-LLM (routing-only) decisions.
	ModelID string `json:"model_id,omitempty"`
	// AutonomyLevel is the HOOTL / HOTL / HITL-L0..L2 tag. Currently FLAGGED:
	// the proto carries NO autonomy field, so this stays empty until the
	// producer emits it (see binding comment + handoff FLAGS). Serialized so
	// the gateway's deriveHITLStatus reads a (currently-empty) value rather
	// than a missing key.
	AutonomyLevel string `json:"autonomy_level,omitempty"`
	// ActedUpon is the HITL gate-completion flag. Also FLAGGED — no proto
	// source today. Serialized for forward-compat with the gateway contract.
	ActedUpon string `json:"acted_upon,omitempty"`
	// CrewName is the crew the agent acted in (proto f12). Maps to FE
	// `crew_name`.
	CrewName string `json:"crew_name,omitempty"`
	// CrewID is the crew instance / orchestration id (proto f13). Powers the
	// Decision-Traces drill-down grouping.
	CrewID string `json:"crew_id,omitempty"`
	// GuardrailOutcome is the Cloud Model Armor verdict (proto f17): one of
	// pass | block | redact (or empty when guardrails were not invoked).
	GuardrailOutcome string `json:"guardrail_outcome,omitempty"`
	// QuestionType is the per-question-type tag the qgen crew emits in the
	// proto field-21 attributes map under key "question_type": one of
	// "mcq" | "oe" (or empty for non-question decisions). The O+ /o/agents
	// qgen crew splits the qgen_question agent into qgen-mcq + qgen-OE tiles
	// keyed on this value; it is aggregated together with agent_id in the
	// per-(agent, question_type) tile stats.
	QuestionType string `json:"question_type,omitempty"`

	// Verdict is the agent's per-decision outcome — for the qgen quality gate
	// one of accepted | rejected | completed_with_warning | refused | retry
	// (carried on the wire in the proto field-21 attributes map under key
	// "decision"). It is DISTINCT from DecisionType (the 4-value route |
	// escalate | refuse | respond ENUM): the verdict is the human-meaningful
	// "what did the agent decide" the O+ Decision Traces DECISION TYPE column +
	// reasoning-panel step render. Empty for non-verdict decisions — the
	// gateway then falls back to decision_type. The JSON key `verdict` is read
	// by chora-gateway's upstream.AgentDecisionRow + preferred by
	// mapAgentDecision (CHO-1700 follow-up).
	Verdict string `json:"verdict,omitempty"`

	// PromptConditions is the durable prompt-explainability map (ADR-197 M-A.4):
	// the set of producer-supplied conditions that shaped the prompt (e.g.
	// {"intent":"new_question","difficulty":"hard"}). It rides the proto
	// field-21 attributes map under keys prefixed `prompt_conditions.` — the
	// binding strips the prefix before populating this map. Stored as a JSONB
	// column + serialized verbatim (the gateway/O+ render the conditions that
	// produced a decision). Empty for decisions with no recorded conditions —
	// never a fabricated value.
	PromptConditions map[string]string `json:"prompt_conditions,omitempty"`

	// ConfidenceOut is the serialization view of Confidence (proto f10),
	// widened to float64 for the gateway. Pointer so 0.0 vs "absent" is
	// preserved (zero is a legitimate confidence).
	ConfidenceOut *float64 `json:"confidence,omitempty"`
	// CostUSD is the serialization view of CostUsdMicros, converted to whole
	// USD for the gateway (it formats with formatUSD). Pointer so a genuine
	// $0.00 (e.g. cached-only / pass-through) differs from "no cost computed".
	CostUSD *float64 `json:"cost_usd,omitempty"`

	// --- storage-only (NOT serialized; written by Append, read into the
	//     *Out fields by List/GetByID) -------------------------------------
	// Confidence is the raw proto f10 confidence (float32). Stored verbatim
	// in the `confidence REAL` column.
	Confidence *float32 `json:"-"`
	// CostUsdMicros is the per-decision cost in 1e-6 USD, derived at
	// projection time from token counts x pricing.yaml (the proto carries no
	// cost field). Stored in `cost_usd_micros BIGINT`.
	CostUsdMicros *int64 `json:"-"`
	// PromptTokens / CompletionTokens / CachedTokens are gen_ai.usage.* token
	// counts (proto f18-20). Stored so the cost is reproducible + auditable.
	PromptTokens     int64 `json:"-"`
	CompletionTokens int64 `json:"-"`
	CachedTokens     int64 `json:"-"`
}

// PopulateProjectionOut bridges the storage representation (raw float32
// confidence + int64 cost micros) into the serialization fields the gateway
// reads (float64 confidence + float64 USD cost). Idempotent. Called by the
// repository read paths so GET /api/agent-decisions emits confidence + cost_usd
// in the float shape upstream.AgentDecisionRow expects.
func (l *Log) PopulateProjectionOut() {
	if l == nil {
		return
	}
	if l.Confidence != nil {
		v := float64(*l.Confidence)
		l.ConfidenceOut = &v
	}
	if l.CostUsdMicros != nil {
		usd := float64(*l.CostUsdMicros) / 1_000_000.0
		l.CostUSD = &usd
	}
}

// NewParams is the constructor input.
type NewParams struct {
	TenantID      string
	Agid          string
	DecisionType  Type
	Reason        string
	RiskTier      RiskTier
	CorrelationID string
	Traceparent   string

	// Reasoning is optional — when set, the input/output hashes + latency
	// + chain summary are attached to the log. Constructor enforces no
	// further validation beyond what NewReasoningSummary already did
	// upstream — pass a nil *ReasoningSummary if not captured.
	Reasoning *ReasoningSummary

	// --- CHO-1560 +9-field projection (all optional) ----------------------
	// ModelID is the concrete model (proto f6). Empty for routing-only.
	ModelID string
	// Confidence is the raw proto f10 confidence in [0..1]. nil = absent.
	Confidence *float32
	// AutonomyLevel — FLAGGED: no proto source yet (see binding). Empty today.
	AutonomyLevel string
	// CostUsdMicros is the per-decision cost in 1e-6 USD, derived by the
	// binding from token counts x pricing.yaml. nil = no cost computed.
	CostUsdMicros *int64
	// CrewName / CrewID (proto f12 / f13).
	CrewName string
	CrewID   string
	// PromptTokens / CompletionTokens / CachedTokens (proto f18-20).
	PromptTokens     int64
	CompletionTokens int64
	CachedTokens     int64
	// GuardrailOutcome is the Cloud Model Armor verdict (proto f17):
	// pass | block | redact | "" (guardrails not invoked).
	GuardrailOutcome string
	// QuestionType is the qgen crew's per-question-type tag from the proto
	// field-21 attributes map: "mcq" | "oe" | "" (non-question decision).
	QuestionType string
	// Verdict is the qgen quality-gate outcome (accepted | rejected |
	// completed_with_warning | refused | retry) from the proto field-21
	// attributes map under key "decision". Distinct from DecisionType. Empty
	// for non-verdict decisions.
	Verdict string
	// PromptConditions is the durable prompt-explainability map (ADR-197 M-A.4)
	// extracted by the binding from the proto field-21 attributes keys prefixed
	// `prompt_conditions.` (prefix stripped). nil/empty = no recorded conditions.
	PromptConditions map[string]string
}

// MaxReasonLen caps reason length to keep payloads bounded.
const MaxReasonLen = 2048

// MaxReasoningLen caps the bounded summary text appended on a reasoning
// summary (separate from Reason — reasoning_summary is a richer chain).
const MaxReasoningLen = 2048

// ReasoningSummary is the bounded "input/output + chain summary" record
// attached to an AgentDecisionLog. Hashes are sha256 hex over the canonical
// (utf-8) form of input/output.
type ReasoningSummary struct {
	InputHash  string `json:"input_hash"`
	OutputHash string `json:"output_hash"`
	LatencyMs  int    `json:"latency_ms"`
	Summary    string `json:"reasoning_summary"`
}

// ReasoningParams is the constructor input.
type ReasoningParams struct {
	Input     string
	Output    string
	LatencyMs int
	Summary   string
}

// NewReasoningSummary computes hashes + validates the summary. Empty IO is
// allowed (refused decisions may have no IO captured); the hash is still
// computed deterministically over the empty string.
func NewReasoningSummary(p ReasoningParams) (*ReasoningSummary, error) {
	if p.LatencyMs < 0 {
		return nil, fmt.Errorf("latency_ms must be >= 0; got %d", p.LatencyMs)
	}
	if len(p.Summary) > MaxReasoningLen {
		return nil, fmt.Errorf("summary too long: %d > %d", len(p.Summary), MaxReasoningLen)
	}
	return &ReasoningSummary{
		InputHash:  sha256Hex(p.Input),
		OutputHash: sha256Hex(p.Output),
		LatencyMs:  p.LatencyMs,
		Summary:    p.Summary,
	}, nil
}

// sha256Hex returns the lowercase hex sha256 of s. Deterministic; same-input
// always yields same-hash.
func sha256Hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// New constructs a fresh AgentDecisionLog. Returns an error on invariant
// violation. Append-only: there is no Update / Patch.
func New(p NewParams) (*Log, error) {
	if strings.TrimSpace(p.TenantID) == "" {
		return nil, errors.New("tenant_id is required")
	}
	if strings.TrimSpace(p.Agid) == "" {
		return nil, errors.New("agid is required")
	}
	if !p.DecisionType.Valid() {
		return nil, fmt.Errorf("invalid decision_type: %q", string(p.DecisionType))
	}
	if !p.RiskTier.Valid() {
		return nil, fmt.Errorf("invalid risk_tier: %q", string(p.RiskTier))
	}
	if strings.TrimSpace(p.CorrelationID) == "" {
		return nil, errors.New("correlation_id is required")
	}
	if len(p.Reason) > MaxReasonLen {
		return nil, fmt.Errorf("reason too long: %d > %d", len(p.Reason), MaxReasonLen)
	}
	if !validGuardrailOutcome(p.GuardrailOutcome) {
		return nil, fmt.Errorf("invalid guardrail_outcome: %q (want pass|block|redact or empty)", p.GuardrailOutcome)
	}
	if p.Confidence != nil && (*p.Confidence < 0 || *p.Confidence > 1) {
		return nil, fmt.Errorf("confidence out of [0,1]: %v", *p.Confidence)
	}
	if p.PromptTokens < 0 || p.CompletionTokens < 0 || p.CachedTokens < 0 {
		return nil, fmt.Errorf("negative token count: prompt=%d completion=%d cached=%d",
			p.PromptTokens, p.CompletionTokens, p.CachedTokens)
	}

	id, err := uuid.NewV7()
	if err != nil {
		return nil, fmt.Errorf("uuidv7: %w", err)
	}

	return &Log{
		LogID:            id.String(),
		TenantID:         p.TenantID,
		Agid:             p.Agid,
		DecisionType:     p.DecisionType,
		Reason:           p.Reason,
		RiskTier:         p.RiskTier,
		CorrelationID:    p.CorrelationID,
		Traceparent:      p.Traceparent,
		Reasoning:        p.Reasoning,
		CreatedAt:        time.Now().UTC(),
		ModelID:          p.ModelID,
		Confidence:       p.Confidence,
		AutonomyLevel:    p.AutonomyLevel,
		CostUsdMicros:    p.CostUsdMicros,
		CrewName:         p.CrewName,
		CrewID:           p.CrewID,
		PromptTokens:     p.PromptTokens,
		CompletionTokens: p.CompletionTokens,
		CachedTokens:     p.CachedTokens,
		GuardrailOutcome: p.GuardrailOutcome,
		QuestionType:     p.QuestionType,
		Verdict:          p.Verdict,
		PromptConditions: p.PromptConditions,
	}, nil
}

// validGuardrailOutcome reports whether the guardrail verdict matches the DB
// CHECK constraint domain (migration 0002 guardrail_verdict): pass|block|redact
// or empty (guardrails not invoked).
func validGuardrailOutcome(s string) bool {
	switch s {
	case "", "pass", "block", "redact":
		return true
	}
	return false
}

// ListFilter is the query filter for repository List.
type ListFilter struct {
	Agid       string    // empty = no filter
	From       time.Time // inclusive; zero = no lower bound
	To         time.Time // exclusive; zero = no upper bound
	Limit      int       // 0 = default (100); cap 1000
	Offset     int
	Descending bool // false = recorded_at ASC (default, used by /o/agents); true = DESC (newest-first)
}
