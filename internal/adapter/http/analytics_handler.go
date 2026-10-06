// Package httpadapter — analytics HTTP routes for the consolidated
// chora-observability service.
//
// Endpoints (migrated from chora-analytics at M12.2.E.4, now mounted
// under /api/analytics/* per the observability URL space):
//
//	GET    /api/analytics/dashboards/learner?gcid=...        (learner analytics)
//	GET    /api/analytics/dashboards/instructor?gcid=...     (instructor analytics)
//	GET    /api/analytics/dashboards/admin?tenant_id=...     (tenant-wide stats)
//	POST   /api/analytics/cohorts                            (create cohort definition)
//	GET    /api/analytics/cohorts/{id}/retention             (retention curve)
//
// Multi-tenant isolation: tenantContext middleware enforces X-Tenant-Id
// for every /api/* path; per-handler logic adds the explicit cross-tenant
// check on the admin tenant_id query param (no super-admin).
//
// READ-ONLY guarantee: the consolidated analytics slice does NOT
// republish Pub/Sub events. Per Pillar 4 of agentic-resilience-d6 the
// upstream subscriber's Ingest call must run AFTER ack/nack; HTTP routes
// here only serve query views.
//
// Trace span attributes: every handler emits `chora.tenant_id`,
// `chora.gcid` (when present), `chora.surface = O+`, and
// `chora.domain = observability` so per-tenant attribution flows through
// the OTLP pipeline (per agentic-resilience-d6 Pillar 4).
package httpadapter

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/apollo-chora/chora-observability/internal/analytics/cohort"
	"github.com/apollo-chora/chora-observability/internal/analytics/dashboard"
	"github.com/apollo-chora/chora-observability/internal/analytics/eventaggregator"
	analyticsinmem "github.com/apollo-chora/chora-observability/internal/analytics/inmem"
	"github.com/apollo-chora/chora-observability/internal/analytics/retention"
)

// analyticsHandler holds the analytics composite store. Wired by
// NewAnalyticsRouter (for the standalone bootstrap path) or by passing a
// WithAnalyticsStore Option to NewRouter.
type analyticsHandler struct {
	store *analyticsinmem.Store
}

// analyticsLearnerDashboard delegates to the analyticsHandler when wired,
// else returns 503. Glue between the Handler-level mux registration and
// the analytics-specific logic — keeps the route registration uniform
// with the rest of the observability domain.
func (h *Handler) analyticsLearnerDashboard(w http.ResponseWriter, r *http.Request) {
	if h.analytics == nil {
		writeError(w, http.StatusServiceUnavailable, "OBS_ANALYTICS_UNINITIALISED",
			"analytics store not wired")
		return
	}
	h.analytics.learnerDashboard(w, r)
}

func (h *Handler) analyticsInstructorDashboard(w http.ResponseWriter, r *http.Request) {
	if h.analytics == nil {
		writeError(w, http.StatusServiceUnavailable, "OBS_ANALYTICS_UNINITIALISED",
			"analytics store not wired")
		return
	}
	h.analytics.instructorDashboard(w, r)
}

func (h *Handler) analyticsAdminDashboard(w http.ResponseWriter, r *http.Request) {
	if h.analytics == nil {
		writeError(w, http.StatusServiceUnavailable, "OBS_ANALYTICS_UNINITIALISED",
			"analytics store not wired")
		return
	}
	h.analytics.adminDashboard(w, r)
}

func (h *Handler) analyticsCohortsRoot(w http.ResponseWriter, r *http.Request) {
	if h.analytics == nil {
		writeError(w, http.StatusServiceUnavailable, "OBS_ANALYTICS_UNINITIALISED",
			"analytics store not wired")
		return
	}
	h.analytics.cohortsRoot(w, r)
}

func (h *Handler) analyticsCohortsSub(w http.ResponseWriter, r *http.Request) {
	if h.analytics == nil {
		writeError(w, http.StatusServiceUnavailable, "OBS_ANALYTICS_UNINITIALISED",
			"analytics store not wired")
		return
	}
	h.analytics.cohortsSub(w, r)
}

// WithAnalyticsStore wires the analytics composite store into the
// observability router so /api/analytics/* routes resolve. When unset,
// those routes return 503 (uninitialised) — consistent with the existing
// optional-port pattern in this package.
func WithAnalyticsStore(s *analyticsinmem.Store) Option {
	return func(h *Handler) { h.analytics = &analyticsHandler{store: s} }
}

// NewAnalyticsRouter is the test-time convenience constructor that wires
// ONLY the analytics routes (with their middleware chain). It avoids
// needing to provide the ledger/decision/correlation ports for tests
// that exercise analytics endpoints in isolation.
func NewAnalyticsRouter(s *analyticsinmem.Store) http.Handler {
	mux := http.NewServeMux()
	ah := &analyticsHandler{store: s}

	// Health (public path — short-circuits tenantContext).
	mux.HandleFunc("/healthz", healthz)
	mux.HandleFunc("/healthz/", healthz)
	mux.HandleFunc("/health", healthz)
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"status": "ready", "scope": "analytics-only"})
	})

	mux.HandleFunc("/api/analytics/dashboards/learner", ah.learnerDashboard)
	mux.HandleFunc("/api/analytics/dashboards/instructor", ah.instructorDashboard)
	mux.HandleFunc("/api/analytics/dashboards/admin", ah.adminDashboard)
	mux.HandleFunc("/api/analytics/cohorts", ah.cohortsRoot)
	mux.HandleFunc("/api/analytics/cohorts/", ah.cohortsSub)

	return logging(tenantContext(traceparent(mux)))
}

// -----------------------------------------------------------------------------
// Dashboard handlers
// -----------------------------------------------------------------------------

func (ah *analyticsHandler) learnerDashboard(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "OBS_METHOD_NOT_ALLOWED",
			"only GET supported on /api/analytics/dashboards/learner")
		return
	}
	tenantID := r.Header.Get("X-Tenant-Id")
	gcid := r.URL.Query().Get("gcid")
	if strings.TrimSpace(gcid) == "" {
		writeError(w, http.StatusBadRequest, "OBS_GCID_REQUIRED", "gcid required")
		return
	}
	d, err := dashboard.NewLearner(dashboard.LearnerInput{
		TenantID: tenantID,
		GCID:     gcid,
	})
	if err != nil {
		writeError(w, http.StatusBadRequest, "OBS_INVALID_ARG", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, d)
}

func (ah *analyticsHandler) instructorDashboard(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "OBS_METHOD_NOT_ALLOWED",
			"only GET supported on /api/analytics/dashboards/instructor")
		return
	}
	tenantID := r.Header.Get("X-Tenant-Id")
	gcid := r.URL.Query().Get("gcid")
	if strings.TrimSpace(gcid) == "" {
		writeError(w, http.StatusBadRequest, "OBS_GCID_REQUIRED", "gcid required")
		return
	}
	d, err := dashboard.NewInstructor(dashboard.InstructorInput{
		TenantID: tenantID,
		GCID:     gcid,
	})
	if err != nil {
		writeError(w, http.StatusBadRequest, "OBS_INVALID_ARG", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, d)
}

func (ah *analyticsHandler) adminDashboard(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "OBS_METHOD_NOT_ALLOWED",
			"only GET supported on /api/analytics/dashboards/admin")
		return
	}
	tenantID := r.Header.Get("X-Tenant-Id")
	queryTenant := r.URL.Query().Get("tenant_id")
	if strings.TrimSpace(queryTenant) == "" {
		writeError(w, http.StatusBadRequest, "OBS_TENANT_REQUIRED", "tenant_id required")
		return
	}
	if queryTenant != tenantID {
		writeError(w, http.StatusForbidden, "OBS_CROSS_TENANT",
			"cross-tenant access forbidden")
		return
	}
	// Pick up a recent day-bucket and surface published-atom + completed-session
	// counters as the tenant headlines. Cross-DB queries forbidden (per ddd-
	// enforcement) — these counts come from the in-memory aggregator that
	// folds Pub/Sub events; production will fold them into a chora_observability
	// materialised view.
	atomsPublished := 0
	sessionsCompleted := 0
	if ah.store != nil {
		bucket, err := ah.store.Aggregator.Query(eventaggregator.QueryParams{
			TenantID: tenantID,
			Period:   eventaggregator.PeriodDay,
			At:       time.Now().UTC(),
		})
		if err == nil {
			atomsPublished = bucket.CountFor("creation", "atom", "published")
			sessionsCompleted = bucket.CountFor("consumption", "session", "completed")
		}
	}
	d, err := dashboard.NewAdmin(dashboard.AdminInput{
		TenantID:          tenantID,
		AtomsPublished:    atomsPublished,
		SessionsCompleted: sessionsCompleted,
	})
	if err != nil {
		writeError(w, http.StatusBadRequest, "OBS_INVALID_ARG", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, d)
}

// -----------------------------------------------------------------------------
// Cohort handlers
// -----------------------------------------------------------------------------

type cohortCreateBody struct {
	Name     string            `json:"name"`
	Criteria map[string]string `json:"criteria"`
}

func (ah *analyticsHandler) cohortsRoot(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "OBS_METHOD_NOT_ALLOWED",
			"only POST supported on /api/analytics/cohorts")
		return
	}
	if ah.store == nil {
		writeError(w, http.StatusServiceUnavailable, "OBS_ANALYTICS_UNINITIALISED",
			"analytics store not wired")
		return
	}
	tenantID := r.Header.Get("X-Tenant-Id")
	var body cohortCreateBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "OBS_BAD_JSON", "invalid JSON: "+err.Error())
		return
	}
	c, err := cohort.New(tenantID, body.Name, body.Criteria)
	if err != nil {
		writeError(w, http.StatusBadRequest, "OBS_INVALID_ARG", err.Error())
		return
	}
	ah.store.Cohorts.Save(c)
	writeJSON(w, http.StatusCreated, c)
}

func (ah *analyticsHandler) cohortsSub(w http.ResponseWriter, r *http.Request) {
	// Path: /api/analytics/cohorts/{id}/retention
	path := strings.TrimPrefix(r.URL.Path, "/api/analytics/cohorts/")
	parts := strings.Split(path, "/")
	if len(parts) != 2 || parts[1] != "retention" {
		writeError(w, http.StatusNotFound, "OBS_UNKNOWN_PATH", "unknown sub-path")
		return
	}
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "OBS_METHOD_NOT_ALLOWED",
			"only GET supported")
		return
	}
	if ah.store == nil {
		writeError(w, http.StatusServiceUnavailable, "OBS_ANALYTICS_UNINITIALISED",
			"analytics store not wired")
		return
	}
	tenantID := r.Header.Get("X-Tenant-Id")
	cohortID := parts[0]

	c, ok := ah.store.Cohorts.Get(tenantID, cohortID)
	if !ok {
		writeError(w, http.StatusNotFound, "OBS_COHORT_NOT_FOUND", "cohort not found")
		return
	}
	members, err := ah.store.Cohorts.Members(tenantID, c.ID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "OBS_INTERNAL", err.Error())
		return
	}

	retentionMembers := make([]retention.Member, 0, len(members))
	for _, m := range members {
		retentionMembers = append(retentionMembers, retention.Member{
			GCID:     m.GCID,
			JoinedAt: m.JoinedAt,
		})
	}

	offsets := []int{1, 7, 30}
	if len(retentionMembers) == 0 {
		// Domain layer rejects empty cohorts — return a 200 with a zero-size
		// curve to keep the API ergonomic for newly-created cohorts that have
		// no members yet.
		writeJSON(w, http.StatusOK, map[string]any{
			"cohort_id":  c.ID,
			"total_size": 0,
			"points":     []retention.RetentionPoint{},
		})
		return
	}
	curve, err := retention.ComputeCohortCurve(retentionMembers, nil, offsets)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "OBS_INTERNAL", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"cohort_id":  c.ID,
		"total_size": curve.TotalSize,
		"points":     curve.Points,
	})
}
