// Package httpadapter wires the Observability domain HTTP routes.
//
// Endpoints (per the task contract — OpenAPI codegen lives in
// chora-contracts post-M11.4):
//
//	GET    /healthz, /healthz/, /readyz
//	POST   /api/token-usage             — append entry; 201 with ledger_id
//	GET    /api/token-usage             — list (paginated, filtered)
//	GET    /api/token-usage/cost        — aggregated cost (sum of micros)
//	GET    /api/token-usage/aggregate   — group by model/agent/gcid
//	POST   /api/token-usage/budget      — set per-tenant period cap
//	GET    /api/token-usage/budget      — current spend vs cap
//	POST   /api/agent-decisions         — append AgentDecisionLog
//	GET    /api/agent-decisions         — list (paginated, filtered by agid)
//	GET    /api/agent-decisions/{id}    — fetch by log_id
//	POST   /api/correlations            — register a TraceCorrelation
//	GET    /api/correlations/{id}       — fetch (returns trace_id + spans)
//	GET    /api/cost/cumulative         — Sequel-comic running ticker
//	GET    /api/cost/by-act             — by-model breakdown (also per Sequel)
//	POST   /api/traces/export           — Spanstore range export (trace-store read)
//	GET    /api/v1/observability/spans  — Spanstore query API for O+ Decision-Log Explorer
//
// Recursion warning: traces emitted by chora-observability for ITS OWN HTTP
// requests do NOT generate self-referencing TokenUsageLedger entries. The
// ledger is only written when callers (Model Gateway, etc.) explicitly POST
// /api/token-usage; the OTLP self-tracing stays in the trace store and never
// loops back.
package httpadapter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/apollo-chora/chora-observability/internal/domain/agents"
	cs "github.com/apollo-chora/chora-observability/internal/domain/companionsuspension"
	"github.com/apollo-chora/chora-observability/internal/domain/correlation"
	"github.com/apollo-chora/chora-observability/internal/domain/decision"
	"github.com/apollo-chora/chora-observability/internal/domain/eval"
	ee "github.com/apollo-chora/chora-observability/internal/domain/externalegress"
	fg "github.com/apollo-chora/chora-observability/internal/domain/familiargrowth"
	"github.com/apollo-chora/chora-observability/internal/domain/ledger"
	ra "github.com/apollo-chora/chora-observability/internal/domain/ritualaudit"
)

// Handler is the HTTP-layer dispatcher.
type Handler struct {
	ledgers      ledger.Repository
	decisions    decision.Repository
	correlations correlation.Repository
	budgets      ledger.BudgetRepository // optional — nil means budget endpoints 503
	budgetLookup ledger.BudgetLookup     // optional — nil means budget-check endpoint 503
	traceExport  TraceExporter           // optional — nil means traces endpoint 503

	// ledgerHook (optional) is the atomic ledger-write + outbox-publish
	// + budget-accrue coordinator. When set, POST /api/token-usage
	// delegates to the hook so every recorded entry queues a
	// chora.observability.token_usage.recorded.v1 outbox row. When nil
	// the handler falls back to the M10 baseline (direct Append).
	ledgerHook *ledger.LedgerHook

	// analytics (optional) holds the analytics composite store wired by
	// WithAnalyticsStore. Subsumed from the retired chora-analytics
	// service at M12.2.E.4 — when nil, /api/analytics/* routes return 503.
	analytics *analyticsHandler

	// familiarGrowth (optional) projects the ADR-149 Familiar Growth audit
	// ledger for O+ IMDA D1/D2 governance dashboards (PROD-H). When nil
	// the /v1/audit/familiar-growth/* routes return 503. Wiring in
	// familiar_growth_wiring.go. Ingestion is a PULL/StreamingPull consumer
	// wired in cmd/server, NOT a gateway push (CHO-2257: the push ingress this
	// replaced was never called — all 7 source subscriptions are pull-shaped).
	familiarGrowth fg.Repository

	// ritualAudit (optional) projects the Grimoire Ritual run audit for O+
	// auditors (ADR-215 / ADR-219 CHO-2016). When nil the /v1/audit/ritual-runs
	// route returns 503. Wiring in ritual_audit_wiring.go. Ingestion is a
	// PULL/StreamingPull consumer wired in cmd/server, NOT a gateway push.
	ritualAudit ra.Repository

	// egressKillSwitch (optional) is the O+ platform egress kill-switch
	// (CHO-2148 / ADR-231 D6) — the operator's override on Far Sight web egress.
	// Engaging it makes the model-gateway deny EVERY grounded call, for every
	// tenant, with no deploy. When nil the route returns 503 (never a silent
	// "not engaged", which would be indistinguishable from a real answer and
	// would hide that the platform has no working override). Wiring in
	// external_egress_killswitch_wiring.go.
	egressKillSwitch ee.KillSwitchRepository

	// companionSuspension (optional) is the O+ Learning Companion containment
	// control (ADR-252 via ADR-254 D7): an operator contains the companion
	// platform-wide, per tenant, or per skill without a deploy; the model
	// gateway reads the result on every companion turn. When nil the route
	// returns 503 (never a silent "nobody is suspended"). Wiring in
	// companion_suspension_wiring.go.
	companionSuspension cs.Repository

	// agentsRegistry (optional) carries the static metadata for the O+
	// /api/v1/observability/agents endpoint (Phase B of the O+ hydration
	// plan atomic-napping-spring.md). When nil the route returns 503.
	agentsRegistry *agents.Registry

	// evalEvidence (optional) reads the agent-eval evidence analytics store for
	// the O+ Agent-Eval drill-down (IMDA D2 transparency). When nil the
	// /api/v1/observability/eval-runs[...] routes return 503.
	evalEvidence eval.Repository
}

// TraceExporter is the read-side Spanstore port (defined here as an interface
// to keep the http layer decoupled from the trace-store adapter — the
// adapter satisfies this implicitly).
type TraceExporter interface {
	Export(ctx context.Context, req TraceExportRequest) (TraceExportResponse, error)
}

// TraceExportRequest is the cross-package shape for trace export.
type TraceExportRequest struct {
	TenantID string
	TraceID  string
	Since    time.Time
	Until    time.Time
}

// TraceExportResponse is the cross-package shape returned to the API caller.
type TraceExportResponse struct {
	ExportID string    `json:"export_id"`
	Status   string    `json:"status"`
	Endpoint string    `json:"endpoint"`
	Mock     bool      `json:"mock"`
	Since    time.Time `json:"since"`
	Until    time.Time `json:"until"`
	QueuedAt time.Time `json:"queued_at"`

	// Spans carries the spans read back from the trace store (Tempo). Empty
	// when the store holds no spans for the request. SpanCount mirrors
	// len(Spans) for a quick summary without decoding the array.
	Spans     []TraceSpan `json:"spans,omitempty"`
	SpanCount int         `json:"span_count,omitempty"`
}

// TraceSpan is one span in a trace-store read response.
type TraceSpan struct {
	TraceID      string            `json:"trace_id"`
	SpanID       string            `json:"span_id"`
	ParentSpanID string            `json:"parent_span_id,omitempty"`
	Name         string            `json:"name"`
	StartTime    time.Time         `json:"start_time"`
	EndTime      time.Time         `json:"end_time"`
	Attributes   map[string]string `json:"attributes,omitempty"`
}

// NewRouter wires the public mux with the minimum-required ports. Optional
// ports (budgets, trace-export) can be added via Options post-construction.
//
// Returns an http.Handler ready for ServeMux embedding or direct
// ListenAndServe.
func NewRouter(
	ledgers ledger.Repository,
	decisions decision.Repository,
	correlations correlation.Repository,
	opts ...Option,
) http.Handler {
	h := &Handler{ledgers: ledgers, decisions: decisions, correlations: correlations}
	for _, o := range opts {
		o(h)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", healthz)
	mux.HandleFunc("/healthz/", healthz)
	mux.HandleFunc("/health", healthz)
	mux.HandleFunc("/readyz", h.readyz)
	mux.HandleFunc("/", h.indexHandler)

	mux.HandleFunc("/api/token-usage", h.tokenUsageRoot)
	mux.HandleFunc("/api/token-usage/", h.tokenUsageSub)
	mux.HandleFunc("/api/agent-decisions", h.agentDecisions)
	mux.HandleFunc("/api/agent-decisions/", h.agentDecisionItem)
	mux.HandleFunc("/api/correlations", h.correlationsCollection)
	mux.HandleFunc("/api/correlations/", h.correlationsItem)

	mux.HandleFunc("/api/cost/cumulative", h.costCumulative)
	mux.HandleFunc("/api/cost/by-act", h.costByAct)
	mux.HandleFunc("/api/traces/export", h.tracesExport)

	// Spanstore query API for O+ Decision-Log Explorer (Stage 6).
	mux.HandleFunc("/api/v1/observability/spans", h.spanstoreQuery)
	mux.HandleFunc("/api/v1/observability/agent-decisions", h.spanstoreAgentDecisions)

	// Recent-decisions rollup for the O+ Dashboard `recent_decisions_24h`
	// counter (chora-gateway BFF consumer; replaces the prior TODO(M12)
	// omission in handlers_oplus.go Dashboard).
	mux.HandleFunc("/api/v1/observability/agent-decisions/count", h.agentDecisionsCount)

	// Phase B O+ hydration (atomic-napping-spring.md) — crews + agents
	// hierarchy. Source: agent_decision_log aggregated by agid (last-24h
	// window) joined to chora-infra/agents-cli/registry.json static metadata.
	// Per [[feedback-no-stubs-real-wiring]] returns invocations_24h=0 + null
	// stats when no rows exist — NEVER fabricates.
	ah := newAgentsHandler(decisions, h.agentsRegistry)
	mux.HandleFunc("/api/v1/observability/agents", ah.serveOPlusAgents)

	// CHO-2364 (ADR-197 read slice) - per-agent prompt-evidence aggregation
	// for the O+ prompts view. The aggregation ports ride the SAME repos the
	// router already receives: both pg and inmem implementations satisfy
	// decision.PromptUsageRepository + ritualaudit.PromptStampRepository, so
	// no extra wiring is needed in cmd/server. A repo that lacks a port
	// leaves it nil and the route 503s loudly (never a fabricated payload).
	promptUsage, _ := decisions.(decision.PromptUsageRepository)
	promptStamps, _ := h.ritualAudit.(ra.PromptStampRepository)
	pph := newAgentPromptsHandler(promptUsage, promptStamps)
	mux.HandleFunc("/api/v1/observability/agent-prompts", pph.serveAgentPrompts)

	// O+ Agent-Eval evidence drill-down (IMDA D2 transparency). Source: the
	// analytics store via eval.Repository. The exact path serves the crew-run
	// index; the subtree serves a single run's per-row drill-down. Both 503
	// when the repo is not wired (WithEvalEvidenceRepo).
	mux.HandleFunc(evalRunsPath, h.evalRunsList)
	mux.HandleFunc(evalRunsPrefix, h.evalRunEvidence)

	// Analytics slice (consolidated from chora-analytics at M12.2.E.4).
	// Routes 503 when the analytics composite store is not wired.
	mux.HandleFunc("/api/analytics/dashboards/learner", h.analyticsLearnerDashboard)
	mux.HandleFunc("/api/analytics/dashboards/instructor", h.analyticsInstructorDashboard)
	mux.HandleFunc("/api/analytics/dashboards/admin", h.analyticsAdminDashboard)
	mux.HandleFunc("/api/analytics/cohorts", h.analyticsCohortsRoot)
	mux.HandleFunc("/api/analytics/cohorts/", h.analyticsCohortsSub)

	// Phase 6: GetAuditEvents endpoint — GET /events?tenant_id=X projects
	// AgentDecisionLog entries as audit events for the named tenant. Added
	// so the chora-gateway HTTPUpstream.GetAuditEvents method has a
	// concrete downstream target.
	mux.Handle("/events", newPhase6EventsHandler(decisions))

	// PROD-H: ADR-149 Familiar Growth audit endpoints for O+ IMDA D1/D2
	// governance dashboards. The 4 routes 503 when the repo is not wired.
	// Route definitions live in familiar_growth_wiring.go.
	h.MountFamiliarGrowthRoutes(mux)

	// ADR-215 / ADR-219 CHO-2016 — Grimoire Ritual run audit read route (O+
	// auditor projection). 503s when the repo is not wired. Route definition
	// lives in ritual_audit_wiring.go. Ingestion is a PULL/StreamingPull
	// consumer wired in cmd/server, NOT a gateway push.
	h.MountRitualAuditRoutes(mux)
	h.MountEgressKillSwitchRoutes(mux)
	h.MountCompanionSuspensionRoutes(mux)

	return logging(tenantContext(traceparent(mux)))
}

// Option mutates a Handler during construction.
type Option func(*Handler)

// WithBudgetRepo enables /api/token-usage/budget.
func WithBudgetRepo(b ledger.BudgetRepository) Option {
	return func(h *Handler) { h.budgets = b }
}

// WithTraceExporter enables /api/traces/export + Spanstore query.
func WithTraceExporter(t TraceExporter) Option {
	return func(h *Handler) { h.traceExport = t }
}

// WithLedgerHook wires the atomic ledger.LedgerHook into POST
// /api/token-usage so every successful entry also queues a canonical
// `chora.observability.token_usage.recorded.v1` outbox row. When unset,
// the handler stays on the M10 baseline (direct Append; no outbox).
func WithLedgerHook(hook *ledger.LedgerHook) Option {
	return func(h *Handler) { h.ledgerHook = hook }
}

// WithAgentsRegistry wires the static crews + agents metadata source for
// GET /api/v1/observability/agents. Per Phase B of the O+ hydration plan
// atomic-napping-spring.md the registry is loaded from
// chora-infra/agents-cli/registry.json (mounted as config/registry.json
// in the service container). When unset the agents endpoint returns 503.
func WithAgentsRegistry(r *agents.Registry) Option {
	return func(h *Handler) { h.agentsRegistry = r }
}

// WithEvalEvidenceRepo wires the agent-eval evidence reader for
// GET /api/v1/observability/eval-runs[...] (the O+ Agent-Eval drill-down,
// IMDA D2). When unset those routes return 503.
func WithEvalEvidenceRepo(r eval.Repository) Option {
	return func(h *Handler) { h.evalEvidence = r }
}

// -----------------------------------------------------------------------------
// Health + readiness + index
// -----------------------------------------------------------------------------

func healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"status": "ok",
		"time":   time.Now().UTC().Format(time.RFC3339),
	})
}

func (h *Handler) readyz(w http.ResponseWriter, _ *http.Request) {
	if h.ledgers == nil || h.decisions == nil || h.correlations == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"status": "repos-uninitialised"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "ready"})
}

func (h *Handler) indexHandler(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"service": "chora-observability",
		"surface": "O+ Observability+",
		"domain":  "Observability (supporting)",
		"project": projectFromEnv(),
		"warning": "self-emitted OTLP traces do NOT generate TokenUsageLedger entries (recursion guard)",
	})
}

// -----------------------------------------------------------------------------
// Token usage — /api/token-usage
// -----------------------------------------------------------------------------

func (h *Handler) tokenUsageRoot(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		h.appendTokenUsage(w, r)
	case http.MethodGet:
		h.listTokenUsage(w, r)
	default:
		writeError(w, http.StatusMethodNotAllowed, "OBS_METHOD_NOT_ALLOWED",
			"only GET and POST are supported on /api/token-usage")
	}
}

func (h *Handler) tokenUsageSub(w http.ResponseWriter, r *http.Request) {
	// /api/token-usage/{cost|budget|aggregate} are the supported sub-paths.
	sub := strings.TrimPrefix(r.URL.Path, "/api/token-usage/")
	sub = strings.TrimSuffix(sub, "/")
	switch sub {
	case "cost":
		if r.Method != http.MethodGet {
			writeError(w, http.StatusMethodNotAllowed, "OBS_METHOD_NOT_ALLOWED",
				"only GET is supported on /api/token-usage/cost")
			return
		}
		h.costTokenUsage(w, r)
	case "budget":
		switch r.Method {
		case http.MethodPost:
			h.postBudget(w, r)
		case http.MethodGet:
			h.getBudget(w, r)
		default:
			writeError(w, http.StatusMethodNotAllowed, "OBS_METHOD_NOT_ALLOWED",
				"only GET and POST are supported on /api/token-usage/budget")
		}
	case "aggregate":
		if r.Method != http.MethodGet {
			writeError(w, http.StatusMethodNotAllowed, "OBS_METHOD_NOT_ALLOWED",
				"only GET is supported on /api/token-usage/aggregate")
			return
		}
		h.aggregateTokenUsage(w, r)
	case "budget-check":
		h.budgetCheck(w, r)
	default:
		writeError(w, http.StatusNotFound, "OBS_NOT_FOUND", "unknown sub-resource")
	}
}

type appendTokenUsageRequest struct {
	Gcid             string `json:"gcid"`
	Agid             string `json:"agid,omitempty"`
	ModelID          string `json:"model_id"`
	PromptTokens     int    `json:"prompt_tokens"`
	CompletionTokens int    `json:"completion_tokens"`
	CostUsdMicros    int64  `json:"cost_usd_micros"`
	TraceID          string `json:"trace_id"`
	SpanID           string `json:"span_id"`
}

func (h *Handler) appendTokenUsage(w http.ResponseWriter, r *http.Request) {
	var req appendTokenUsageRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "OBS_INVALID_BODY", err.Error())
		return
	}
	tenantID := tenantFromContext(r.Context())

	// When the LedgerHook is wired, delegate to it so the ledger.Append +
	// outbox.RecordOutboxEvent pair runs atomically (outbox first per the
	// data-consistency invariant). Per ai-cost-tracking the canonical
	// topic is chora.observability.token_usage.recorded.v1 — the hook
	// stamps it on the outbox envelope for the Outbox Relay to pick up.
	if h.ledgerHook != nil {
		res, err := h.ledgerHook.Record(r.Context(), ledger.RecordParams{
			TenantID:         tenantID,
			Gcid:             req.Gcid,
			AgentID:          req.Agid,
			ModelID:          req.ModelID,
			PromptTokens:     req.PromptTokens,
			CompletionTokens: req.CompletionTokens,
			CostUsdMicros:    req.CostUsdMicros,
			TraceID:          req.TraceID,
			SpanID:           req.SpanID,
			Traceparent:      r.Header.Get("Traceparent"),
		})
		if err != nil {
			log.Printf("ledger hook record: %v", err)
			writeError(w, http.StatusInternalServerError, "OBS_LEDGER_HOOK_ERROR",
				"failed to atomically persist ledger + outbox event")
			return
		}
		writeJSON(w, http.StatusCreated, map[string]any{
			"ledger_id":              res.LedgerID,
			"outbox_event_id":        res.OutboxEventID,
			"pricing_config_version": res.PricingConfigVersion,
		})
		return
	}

	// Backwards-compat path (M10 baseline): direct Append without outbox.
	e, err := ledger.New(ledger.NewParams{
		TenantID:         tenantID,
		Gcid:             req.Gcid,
		Agid:             req.Agid,
		ModelID:          req.ModelID,
		PromptTokens:     req.PromptTokens,
		CompletionTokens: req.CompletionTokens,
		CostUsdMicros:    req.CostUsdMicros,
		TraceID:          req.TraceID,
		SpanID:           req.SpanID,
	})
	if err != nil {
		writeError(w, http.StatusBadRequest, "OBS_INVALID_LEDGER", err.Error())
		return
	}
	if err := h.ledgers.Append(r.Context(), e); err != nil {
		log.Printf("ledger append: %v", err)
		writeError(w, http.StatusInternalServerError, "OBS_REPO_ERROR", "failed to persist ledger")
		return
	}
	writeJSON(w, http.StatusCreated, e)
}

type listTokenUsageResponse struct {
	Items []*ledger.Entry `json:"items"`
	Total int             `json:"total"`
}

func (h *Handler) listTokenUsage(w http.ResponseWriter, r *http.Request) {
	tenantID := tenantFromContext(r.Context())
	f, err := parseListFilter(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "OBS_INVALID_QUERY", err.Error())
		return
	}
	items, err := h.ledgers.List(r.Context(), tenantID, f)
	if err != nil {
		log.Printf("ledger list: %v", err)
		writeError(w, http.StatusInternalServerError, "OBS_REPO_ERROR", "failed to list ledger")
		return
	}
	writeJSON(w, http.StatusOK, listTokenUsageResponse{Items: items, Total: len(items)})
}

type costResponse struct {
	TotalCostUsdMicros int64  `json:"total_cost_usd_micros"`
	TotalCostUsd       string `json:"total_cost_usd"`
	EntryCount         int    `json:"entry_count"`
}

func (h *Handler) costTokenUsage(w http.ResponseWriter, r *http.Request) {
	tenantID := tenantFromContext(r.Context())
	f, err := parseListFilter(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "OBS_INVALID_QUERY", err.Error())
		return
	}
	total, count, err := h.ledgers.SumCost(r.Context(), tenantID, f)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "OBS_AGGREGATE_ERROR", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, costResponse{
		TotalCostUsdMicros: total,
		TotalCostUsd:       fmt.Sprintf("%.6f", float64(total)/1_000_000.0),
		EntryCount:         count,
	})
}

// -----------------------------------------------------------------------------
// Agent decisions — /api/agent-decisions
// -----------------------------------------------------------------------------

func (h *Handler) agentDecisions(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		h.appendDecision(w, r)
	case http.MethodGet:
		h.listDecisions(w, r)
	default:
		writeError(w, http.StatusMethodNotAllowed, "OBS_METHOD_NOT_ALLOWED",
			"only GET and POST are supported on /api/agent-decisions")
	}
}

type appendDecisionRequest struct {
	Agid          string `json:"agid"`
	DecisionType  string `json:"decision_type"`
	Reason        string `json:"reason"`
	RiskTier      string `json:"risk_tier"`
	CorrelationID string `json:"correlation_id"`
	Traceparent   string `json:"traceparent"`

	// Reasoning fields (optional). When any of the reasoning fields are
	// non-zero, the handler computes hashes + attaches a ReasoningSummary.
	InputText        string `json:"input_text,omitempty"`
	OutputText       string `json:"output_text,omitempty"`
	LatencyMs        int    `json:"latency_ms,omitempty"`
	ReasoningSummary string `json:"reasoning_summary,omitempty"`
}

func (h *Handler) appendDecision(w http.ResponseWriter, r *http.Request) {
	var req appendDecisionRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "OBS_INVALID_BODY", err.Error())
		return
	}
	tenantID := tenantFromContext(r.Context())

	tp := req.Traceparent
	if tp == "" {
		tp = traceparentFromContext(r.Context())
	}

	var rs *decision.ReasoningSummary
	if req.InputText != "" || req.OutputText != "" || req.LatencyMs != 0 || req.ReasoningSummary != "" {
		built, err := decision.NewReasoningSummary(decision.ReasoningParams{
			Input:     req.InputText,
			Output:    req.OutputText,
			LatencyMs: req.LatencyMs,
			Summary:   req.ReasoningSummary,
		})
		if err != nil {
			writeError(w, http.StatusBadRequest, "OBS_INVALID_DECISION", err.Error())
			return
		}
		rs = built
	}

	d, err := decision.New(decision.NewParams{
		TenantID:      tenantID,
		Agid:          req.Agid,
		DecisionType:  decision.Type(req.DecisionType),
		Reason:        req.Reason,
		RiskTier:      decision.RiskTier(req.RiskTier),
		CorrelationID: req.CorrelationID,
		Traceparent:   tp,
		Reasoning:     rs,
	})
	if err != nil {
		writeError(w, http.StatusBadRequest, "OBS_INVALID_DECISION", err.Error())
		return
	}
	if err := h.decisions.Append(r.Context(), d); err != nil {
		log.Printf("decision append: %v", err)
		writeError(w, http.StatusInternalServerError, "OBS_REPO_ERROR", "failed to persist decision")
		return
	}
	writeJSON(w, http.StatusCreated, d)
}

// agentDecisionItem dispatches /api/agent-decisions/{id}.
func (h *Handler) agentDecisionItem(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/api/agent-decisions/")
	id = strings.TrimSuffix(id, "/")
	if id == "" || strings.Contains(id, "/") {
		writeError(w, http.StatusNotFound, "OBS_NOT_FOUND", "unknown sub-resource")
		return
	}
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "OBS_METHOD_NOT_ALLOWED",
			"only GET is supported on /api/agent-decisions/{id}")
		return
	}
	tenantID := tenantFromContext(r.Context())
	d, err := h.decisions.GetByID(r.Context(), tenantID, id)
	if err != nil {
		if errors.Is(err, decision.ErrNotFound) {
			writeError(w, http.StatusNotFound, "OBS_DECISION_NOT_FOUND", "decision not found")
			return
		}
		log.Printf("decision get: %v", err)
		writeError(w, http.StatusInternalServerError, "OBS_REPO_ERROR", "internal error")
		return
	}
	writeJSON(w, http.StatusOK, d)
}

type listDecisionResponse struct {
	Items []*decision.Log `json:"items"`
	Total int             `json:"total"`
}

func (h *Handler) listDecisions(w http.ResponseWriter, r *http.Request) {
	tenantID := tenantFromContext(r.Context())
	q := r.URL.Query()
	// Decision Traces is a newest-first audit list. (collectStats / /o/agents
	// keeps the default ASC — Descending defaults false there.)
	f := decision.ListFilter{Agid: strings.TrimSpace(q.Get("agid")), Descending: true}
	// `since`/`until` are accepted as aliases for `from`/`to` (the gateway BFF
	// + the count endpoint speak since/until); `from`/`to` win when both given.
	fromRaw := q.Get("from")
	if fromRaw == "" {
		fromRaw = q.Get("since")
	}
	if fromRaw != "" {
		t, err := time.Parse(time.RFC3339, fromRaw)
		if err != nil {
			writeError(w, http.StatusBadRequest, "OBS_INVALID_QUERY",
				"from must be RFC3339")
			return
		}
		f.From = t
	}
	toRaw := q.Get("to")
	if toRaw == "" {
		toRaw = q.Get("until")
	}
	if toRaw != "" {
		t, err := time.Parse(time.RFC3339, toRaw)
		if err != nil {
			writeError(w, http.StatusBadRequest, "OBS_INVALID_QUERY",
				"to must be RFC3339")
			return
		}
		f.To = t
	}
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			writeError(w, http.StatusBadRequest, "OBS_INVALID_QUERY",
				"limit must be int")
			return
		}
		f.Limit = n
	}
	if v := q.Get("offset"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			writeError(w, http.StatusBadRequest, "OBS_INVALID_QUERY",
				"offset must be int")
			return
		}
		f.Offset = n
	}

	items, err := h.decisions.List(r.Context(), tenantID, f)
	if err != nil {
		log.Printf("decision list: %v", err)
		writeError(w, http.StatusInternalServerError, "OBS_REPO_ERROR", "failed to list decisions")
		return
	}
	writeJSON(w, http.StatusOK, listDecisionResponse{Items: items, Total: len(items)})
}

// -----------------------------------------------------------------------------
// Correlations — /api/correlations
// -----------------------------------------------------------------------------

func (h *Handler) correlationsCollection(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "OBS_METHOD_NOT_ALLOWED",
			"only POST is supported on /api/correlations")
		return
	}
	h.registerCorrelation(w, r)
}

func (h *Handler) correlationsItem(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/api/correlations/")
	id = strings.TrimSuffix(id, "/")
	if id == "" || strings.Contains(id, "/") {
		writeError(w, http.StatusNotFound, "OBS_NOT_FOUND", "unknown sub-resource")
		return
	}
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "OBS_METHOD_NOT_ALLOWED",
			"only GET is supported on /api/correlations/{id}")
		return
	}
	h.getCorrelation(w, r, id)
}

type registerCorrelationRequest struct {
	CorrelationID string   `json:"correlation_id"`
	TraceID       string   `json:"trace_id"`
	ParentSpanID  string   `json:"parent_span_id"`
	ChildSpans    []string `json:"child_spans,omitempty"`
}

func (h *Handler) registerCorrelation(w http.ResponseWriter, r *http.Request) {
	var req registerCorrelationRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "OBS_INVALID_BODY", err.Error())
		return
	}
	tenantID := tenantFromContext(r.Context())
	c, err := correlation.New(correlation.NewParams{
		TenantID:      tenantID,
		CorrelationID: req.CorrelationID,
		TraceID:       req.TraceID,
		ParentSpanID:  req.ParentSpanID,
	})
	if err != nil {
		writeError(w, http.StatusBadRequest, "OBS_INVALID_CORRELATION", err.Error())
		return
	}
	for _, span := range req.ChildSpans {
		if err := c.AttachChildSpan(span); err != nil {
			writeError(w, http.StatusBadRequest, "OBS_INVALID_CORRELATION", err.Error())
			return
		}
	}
	if err := h.correlations.Register(r.Context(), c); err != nil {
		log.Printf("correlation register: %v", err)
		writeError(w, http.StatusInternalServerError, "OBS_REPO_ERROR", "failed to register correlation")
		return
	}
	writeJSON(w, http.StatusCreated, c)
}

func (h *Handler) getCorrelation(w http.ResponseWriter, r *http.Request, id string) {
	tenantID := tenantFromContext(r.Context())
	c, err := h.correlations.Get(r.Context(), tenantID, id)
	if err != nil {
		if errors.Is(err, correlation.ErrNotFound) {
			writeError(w, http.StatusNotFound, "OBS_CORRELATION_NOT_FOUND", "correlation not found")
			return
		}
		log.Printf("correlation get: %v", err)
		writeError(w, http.StatusInternalServerError, "OBS_REPO_ERROR", "internal error")
		return
	}
	writeJSON(w, http.StatusOK, c)
}

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------

// parseListFilter parses the common list-query parameters used by the ledger
// endpoints (from, to, limit, offset). Returns a populated ListFilter.
func parseListFilter(r *http.Request) (ledger.ListFilter, error) {
	q := r.URL.Query()
	var f ledger.ListFilter
	if v := q.Get("from"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			return f, fmt.Errorf("from must be RFC3339")
		}
		f.From = t
	}
	if v := q.Get("to"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			return f, fmt.Errorf("to must be RFC3339")
		}
		f.To = t
	}
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return f, fmt.Errorf("limit must be int")
		}
		f.Limit = n
	}
	if v := q.Get("offset"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return f, fmt.Errorf("offset must be int")
		}
		f.Offset = n
	}
	return f, nil
}

func decodeJSON(r *http.Request, v any) error {
	if r.Body == nil {
		return errors.New("empty body")
	}
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return err
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

type errEnvelope struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func writeError(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, errEnvelope{Code: code, Message: msg})
}

// _ keeps context import tidy for future use.
var _ = context.TODO
