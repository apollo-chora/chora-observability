// Tests for the ADR-215/ADR-219 ritual run audit read handler
// (GET /v1/audit/ritual-runs).
package httpadapter_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	httpadapter "github.com/apollo-chora/chora-observability/internal/adapter/http"
	"github.com/apollo-chora/chora-observability/internal/adapter/inmem"
	ra "github.com/apollo-chora/chora-observability/internal/domain/ritualaudit"
)

const (
	rrTenant   = "01980000-0000-7000-8000-0000000000c1"
	rrOtherTen = "01980000-0000-7000-8000-0000000000c2"
	rrFamiliar = "01980000-0000-7000-8000-0000000000c3"
)

func seedRitualRepo(t *testing.T) *inmem.RitualAuditRepository {
	t.Helper()
	repo := inmem.NewRitualAuditRepository()
	base := time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC)
	for i, tenant := range []string{rrTenant, rrTenant, rrOtherTen} {
		row := ra.RitualRunAuditRow{
			AuditID:       "audit-" + string(rune('a'+i)),
			TenantID:      tenant,
			SourceTopic:   ra.TopicFamiliarRitualRunCompleted,
			SourceEventID: "evt-" + string(rune('a'+i)),
			RunID:         "run-" + string(rune('a'+i)),
			FamiliarID:    rrFamiliar,
			Status:        "completed",
			ManaCharged:   20,
			RevisionNo:    int32(i + 1),
			Stamps:        []map[string]any{{"SkillKey": "explain"}},
			ReceivedAt:    base.Add(time.Duration(i) * time.Minute),
		}
		if err := repo.Ingest(context.Background(), row); err != nil {
			t.Fatalf("seed ingest: %v", err)
		}
	}
	return repo
}

// getRitual issues a GET with the X-Tenant-Id header the tenantContext
// middleware requires for non-public paths (empty tenant → no header, to
// exercise the 400 gate).
func getRitual(t *testing.T, url, tenant string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if tenant != "" {
		req.Header.Set("X-Tenant-Id", tenant)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	return resp
}

func TestRitualRuns_HappyPath(t *testing.T) {
	t.Parallel()
	router := httpadapter.NewRouter(nil, nil, nil, httpadapter.WithRitualAuditRepo(seedRitualRepo(t)))
	server := httptest.NewServer(router)
	defer server.Close()

	resp := getRitual(t, server.URL+"/v1/audit/ritual-runs", rrTenant)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: %d", resp.StatusCode)
	}
	var body struct {
		Items []map[string]any `json:"items"`
		Total int              `json:"total"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	// 2 rows for rrTenant; the rrOtherTen row is RLS-scoped out at repo level.
	if body.Total != 2 {
		t.Fatalf("expected 2 tenant-scoped runs; got %d", body.Total)
	}
}

func TestRitualRuns_LimitCap(t *testing.T) {
	t.Parallel()
	router := httpadapter.NewRouter(nil, nil, nil, httpadapter.WithRitualAuditRepo(seedRitualRepo(t)))
	server := httptest.NewServer(router)
	defer server.Close()

	resp := getRitual(t, server.URL+"/v1/audit/ritual-runs?limit=1", rrTenant)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: %d", resp.StatusCode)
	}
	var body struct {
		Total int `json:"total"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if body.Total != 1 {
		t.Fatalf("expected limit=1 to cap at 1; got %d", body.Total)
	}
}

func TestRitualRuns_RequiresTenant(t *testing.T) {
	t.Parallel()
	router := httpadapter.NewRouter(nil, nil, nil, httpadapter.WithRitualAuditRepo(inmem.NewRitualAuditRepository()))
	server := httptest.NewServer(router)
	defer server.Close()

	resp := getRitual(t, server.URL+"/v1/audit/ritual-runs", "") // no X-Tenant-Id
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 (no tenant); got %d", resp.StatusCode)
	}
}

func TestRitualRuns_Unwired503(t *testing.T) {
	t.Parallel()
	router := httpadapter.NewRouter(nil, nil, nil) // no WithRitualAuditRepo
	server := httptest.NewServer(router)
	defer server.Close()

	resp := getRitual(t, server.URL+"/v1/audit/ritual-runs", rrTenant)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 (repo unwired); got %d", resp.StatusCode)
	}
}

func TestRitualRuns_MethodNotAllowed(t *testing.T) {
	t.Parallel()
	router := httpadapter.NewRouter(nil, nil, nil, httpadapter.WithRitualAuditRepo(inmem.NewRitualAuditRepository()))
	server := httptest.NewServer(router)
	defer server.Close()

	req, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/audit/ritual-runs", nil)
	req.Header.Set("X-Tenant-Id", rrTenant)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405; got %d", resp.StatusCode)
	}
}
