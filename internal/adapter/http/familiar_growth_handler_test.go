// Tests for ADR-149 Familiar Growth audit HTTP handlers (PROD-H).
package httpadapter_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	httpadapter "github.com/5007-Capstone/chora/services/chora-observability/internal/adapter/http"
	"github.com/5007-Capstone/chora/services/chora-observability/internal/adapter/inmem"
	"github.com/5007-Capstone/chora/services/chora-observability/internal/adapter/subscribers"
	fg "github.com/5007-Capstone/chora/services/chora-observability/internal/domain/familiargrowth"
)

const (
	fgTenant   = "01970000-0000-7000-8000-0000000000a1"
	fgGCID     = "01970000-0000-7000-8000-0000000000a2"
	fgFamiliar = "01970000-0000-7000-8000-0000000000a3"
)

// fillFGRepo seeds the in-memory repository with a deterministic event set
// via the subscriber so the HTTP layer can be tested end-to-end.
func fillFGRepo(t *testing.T) *inmem.FamiliarGrowthRepository {
	t.Helper()
	repo := inmem.NewFamiliarGrowthRepository()
	pub := subscribers.NewInMemoryEvidencePublisher()
	now := time.Date(2026, 5, 13, 12, 0, 0, 0, time.UTC)
	sub := subscribers.New(subscribers.Config{
		Repo:     repo,
		Evidence: pub,
		Now:      func() time.Time { return now },
	})
	ingestOrFail(t, sub, subscribers.FamiliarGrowthEvent{
		SourceTopic:   fg.TopicExpAwarded,
		SourceEventID: "01970000-0000-7000-8000-000000000001",
		TenantID:      fgTenant,
		OwnerGCID:     fgGCID,
		FamiliarID:    fgFamiliar,
		ExpDelta:      30,
		ExpSource:     "atom_session",
		OccurredAt:    now,
	})
	ingestOrFail(t, sub, subscribers.FamiliarGrowthEvent{
		SourceTopic:   fg.TopicStageUp,
		SourceEventID: "01970000-0000-7000-8000-000000000002",
		TenantID:      fgTenant,
		OwnerGCID:     fgGCID,
		FamiliarID:    fgFamiliar,
		StageFrom:     1,
		StageTo:       2,
		OccurredAt:    now,
	})
	// 100 breed-revealed rolls split 40/30/20/10 against claimed 25/25/25/25.
	for i := 0; i < 100; i++ {
		species := "common"
		switch {
		case i < 40:
			species = "common"
		case i < 70:
			species = "uncommon"
		case i < 90:
			species = "rare"
		default:
			species = "legendary"
		}
		ingestOrFail(t, sub, subscribers.FamiliarGrowthEvent{
			SourceTopic:   fg.TopicBreedRevealed,
			SourceEventID: uuidish(i, 'b'),
			TenantID:      fgTenant,
			OwnerGCID:     fgGCID,
			FamiliarID:    fgFamiliar,
			EggSKU:        "egg.standard.v1",
			Species:       species,
			Rarity:        "common",
			RolledProbability: 25.0,
			DistributionSnapshot: map[string]any{
				"common":    25.0,
				"uncommon":  25.0,
				"rare":      25.0,
				"legendary": 25.0,
			},
			OccurredAt: now,
		})
	}
	// 100 hatched events — eggs_hatched is now sourced from hatched.v1
	// directly per Fix-D 2026-05-16 (was previously proxied from
	// breed_revealed.v1).
	for i := 0; i < 100; i++ {
		ingestOrFail(t, sub, subscribers.FamiliarGrowthEvent{
			SourceTopic:   fg.TopicHatched,
			SourceEventID: uuidish(i, 'h'),
			TenantID:      fgTenant,
			OwnerGCID:     fgGCID,
			FamiliarID:    fgFamiliar,
			OccurredAt:    now,
		})
	}
	// 5 egg-purchased.
	for i := 0; i < 5; i++ {
		ingestOrFail(t, sub, subscribers.FamiliarGrowthEvent{
			SourceTopic:    fg.TopicEggPurchased,
			SourceEventID:  uuidish(i, 'c'),
			TenantID:       fgTenant,
			FamiliarID:     fgFamiliar,
			EggSKU:         "egg.standard.v1",
			PurchaseSource: "purchase",
			OccurredAt:     now,
		})
	}
	return repo
}

func ingestOrFail(t *testing.T, sub *subscribers.FamiliarGrowthAuditSubscriber, ev subscribers.FamiliarGrowthEvent) {
	t.Helper()
	if err := sub.Handle(context.Background(), ev); err != nil {
		t.Fatalf("ingest %s: %v", ev.SourceTopic, err)
	}
}

// uuidish builds a deterministic UUIDv7-shape string for tests.
func uuidish(i int, salt rune) string {
	// e.g. 01970000-0000-7000-8000-0000000000ff (i in last segment).
	return fmt.Sprintf("01970000-0000-7000-8000-00%c000000%04x", salt, i)
}

func TestFG_ListEvents_RequiresTenant(t *testing.T) {
	t.Parallel()
	repo := inmem.NewFamiliarGrowthRepository()
	router := httpadapter.NewRouter(
		nil, nil, nil,
		httpadapter.WithFamiliarGrowthRepo(repo),
	)
	server := httptest.NewServer(router)
	defer server.Close()
	// Issue request WITHOUT setting X-Tenant-Id — the tenantContext
	// middleware rejects /api/* paths without it. /v1/* paths are also
	// non-public; expectation is 400.
	req, err := http.NewRequest(http.MethodGet, server.URL+"/v1/audit/familiar-growth/events", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	resp := doReq(t, req)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 (no tenant); got %d", resp.StatusCode)
	}
}

func TestFG_ListEvents_HappyPath(t *testing.T) {
	t.Parallel()
	repo := fillFGRepo(t)
	router := httpadapter.NewRouter(
		nil, nil, nil,
		httpadapter.WithFamiliarGrowthRepo(repo),
	)
	server := httptest.NewServer(router)
	defer server.Close()
	req := newGet(t, server.URL+"/v1/audit/familiar-growth/events?tenant_id="+fgTenant)
	resp := doReq(t, req)
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
	// 1 exp + 1 stage_up + 100 breed + 100 hatched + 5 egg = 207
	if body.Total != 207 {
		t.Fatalf("expected 207 events; got %d", body.Total)
	}
}

func TestFG_ListEvents_FilterBySource(t *testing.T) {
	t.Parallel()
	repo := fillFGRepo(t)
	router := httpadapter.NewRouter(
		nil, nil, nil,
		httpadapter.WithFamiliarGrowthRepo(repo),
	)
	server := httptest.NewServer(router)
	defer server.Close()
	req := newGet(t, server.URL+"/v1/audit/familiar-growth/events?tenant_id="+fgTenant+"&source="+fg.TopicBreedRevealed)
	resp := doReq(t, req)
	var body struct {
		Total int `json:"total"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Total != 100 {
		t.Fatalf("expected 100 breed events; got %d", body.Total)
	}
}

func TestFG_ListMetrics(t *testing.T) {
	t.Parallel()
	repo := fillFGRepo(t)
	router := httpadapter.NewRouter(
		nil, nil, nil,
		httpadapter.WithFamiliarGrowthRepo(repo),
	)
	server := httptest.NewServer(router)
	defer server.Close()
	req := newGet(t, server.URL+"/v1/audit/familiar-growth/metrics?tenant_id="+fgTenant)
	resp := doReq(t, req)
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
	if body.Total != 2 {
		t.Fatalf("expected 2 metric buckets (atom_session, stage_up); got %d (%+v)", body.Total, body.Items)
	}
}

func TestFG_BreedDistribution_RequiresEggSKU(t *testing.T) {
	t.Parallel()
	repo := fillFGRepo(t)
	router := httpadapter.NewRouter(
		nil, nil, nil,
		httpadapter.WithFamiliarGrowthRepo(repo),
	)
	server := httptest.NewServer(router)
	defer server.Close()
	req := newGet(t, server.URL+"/v1/audit/familiar-growth/breed-distribution?tenant_id="+fgTenant)
	resp := doReq(t, req)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 (missing egg_sku); got %d", resp.StatusCode)
	}
}

func TestFG_BreedDistribution_ReturnsChiSquare(t *testing.T) {
	t.Parallel()
	repo := fillFGRepo(t)
	router := httpadapter.NewRouter(
		nil, nil, nil,
		httpadapter.WithFamiliarGrowthRepo(repo),
	)
	server := httptest.NewServer(router)
	defer server.Close()
	req := newGet(t, server.URL+"/v1/audit/familiar-growth/breed-distribution?tenant_id="+fgTenant+"&egg_sku=egg.standard.v1")
	resp := doReq(t, req)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: %d", resp.StatusCode)
	}
	var body fg.BreedDistributionReport
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.SampleSize != 100 {
		t.Fatalf("expected sample_size=100; got %d", body.SampleSize)
	}
	if body.Empirical["common"] != 40 {
		t.Fatalf("expected 40 common; got %d", body.Empirical["common"])
	}
	if body.ChiSquare == nil {
		t.Fatalf("expected chi-square populated")
	}
	if body.ChiSquare.SampleSize != 100 {
		t.Fatalf("chi-square sample_size: %d", body.ChiSquare.SampleSize)
	}
	// Claimed is 25/25/25/25 (normalised). Observed 40/30/20/10 -> stat = 20.
	// p-value should be very small (~ 0.0002).
	if body.ChiSquare.Statistic < 15 || body.ChiSquare.Statistic > 25 {
		t.Fatalf("expected statistic ~20; got %v", body.ChiSquare.Statistic)
	}
	if body.ChiSquare.PValue > 0.01 {
		t.Fatalf("expected p-value < 0.01; got %v", body.ChiSquare.PValue)
	}
}

func TestFG_EggFunnel(t *testing.T) {
	t.Parallel()
	repo := fillFGRepo(t)
	router := httpadapter.NewRouter(
		nil, nil, nil,
		httpadapter.WithFamiliarGrowthRepo(repo),
	)
	server := httptest.NewServer(router)
	defer server.Close()
	req := newGet(t, server.URL+"/v1/audit/familiar-growth/egg-funnel?tenant_id="+fgTenant)
	resp := doReq(t, req)
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
	if body.Total != 1 {
		t.Fatalf("expected 1 funnel row; got %d", body.Total)
	}
	if int(body.Items[0]["eggs_purchased"].(float64)) != 5 {
		t.Fatalf("expected eggs_purchased=5; got %v", body.Items[0]["eggs_purchased"])
	}
	if int(body.Items[0]["eggs_hatched"].(float64)) != 100 {
		t.Fatalf("expected eggs_hatched=100; got %v", body.Items[0]["eggs_hatched"])
	}
}

func TestFG_503_WhenRepoUnwired(t *testing.T) {
	t.Parallel()
	router := httpadapter.NewRouter(nil, nil, nil) // no WithFamiliarGrowthRepo
	server := httptest.NewServer(router)
	defer server.Close()
	for _, p := range []string{
		"/v1/audit/familiar-growth/events",
		"/v1/audit/familiar-growth/metrics",
		"/v1/audit/familiar-growth/breed-distribution",
		"/v1/audit/familiar-growth/egg-funnel",
	} {
		req := newGet(t, server.URL+p+"?tenant_id="+fgTenant)
		resp := doReq(t, req)
		if resp.StatusCode != http.StatusServiceUnavailable {
			t.Fatalf("%s: expected 503; got %d", p, resp.StatusCode)
		}
	}
}

func TestFG_FilterValidation_BadQueryParams(t *testing.T) {
	t.Parallel()
	repo := fillFGRepo(t)
	router := httpadapter.NewRouter(
		nil, nil, nil,
		httpadapter.WithFamiliarGrowthRepo(repo),
	)
	server := httptest.NewServer(router)
	defer server.Close()
	cases := []string{
		"?from=not-a-date",
		"?to=not-a-date",
		"?limit=abc",
		"?offset=xyz",
	}
	for _, qs := range cases {
		qs := qs
		t.Run(qs, func(t *testing.T) {
			// NOT t.Parallel — share parent's server (closed on parent return).
			req := newGet(t, server.URL+"/v1/audit/familiar-growth/events"+qs)
			resp := doReq(t, req)
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("expected 400 for bogus query; got %d", resp.StatusCode)
			}
		})
	}
}

func TestFG_FilterValidation_AllValidParams(t *testing.T) {
	t.Parallel()
	repo := fillFGRepo(t)
	router := httpadapter.NewRouter(
		nil, nil, nil,
		httpadapter.WithFamiliarGrowthRepo(repo),
	)
	server := httptest.NewServer(router)
	defer server.Close()
	from := "2026-05-12T00:00:00Z"
	to := "2026-05-14T00:00:00Z"
	url := server.URL + "/v1/audit/familiar-growth/events?from=" + from + "&to=" + to +
		"&familiar_id=" + fgFamiliar + "&limit=5&offset=0"
	req := newGet(t, url)
	resp := doReq(t, req)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200; got %d", resp.StatusCode)
	}
	var body struct {
		Total int `json:"total"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Total > 5 {
		t.Fatalf("expected at most 5 (limit); got %d", body.Total)
	}
}

func TestFG_RegisterRoutes_DirectMux(t *testing.T) {
	t.Parallel()
	// Exercise RegisterFamiliarGrowthRoutes directly (no tenantContext
	// middleware) to lift coverage on that path.
	repo := fillFGRepo(t)
	mux := http.NewServeMux()
	httpadapter.RegisterFamiliarGrowthRoutes(mux, repo)
	server := httptest.NewServer(mux)
	defer server.Close()
	req := newGet(t, server.URL+"/v1/audit/familiar-growth/events?tenant_id="+fgTenant)
	resp := doReq(t, req)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("direct mux events: status %d", resp.StatusCode)
	}
	req = newGet(t, server.URL+"/v1/audit/familiar-growth/metrics?tenant_id="+fgTenant)
	resp = doReq(t, req)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("direct mux metrics: status %d", resp.StatusCode)
	}
	req = newGet(t, server.URL+"/v1/audit/familiar-growth/egg-funnel?tenant_id="+fgTenant)
	resp = doReq(t, req)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("direct mux funnel: status %d", resp.StatusCode)
	}
	req = newGet(t, server.URL+"/v1/audit/familiar-growth/breed-distribution?tenant_id="+fgTenant+"&egg_sku=egg.standard.v1")
	resp = doReq(t, req)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("direct mux breed: status %d", resp.StatusCode)
	}
}

func TestFG_AllEndpointsRejectNonGET(t *testing.T) {
	t.Parallel()
	repo := fillFGRepo(t)
	router := httpadapter.NewRouter(
		nil, nil, nil,
		httpadapter.WithFamiliarGrowthRepo(repo),
	)
	server := httptest.NewServer(router)
	defer server.Close()
	for _, p := range []string{
		"/v1/audit/familiar-growth/events",
		"/v1/audit/familiar-growth/metrics",
		"/v1/audit/familiar-growth/breed-distribution",
		"/v1/audit/familiar-growth/egg-funnel",
	} {
		req, err := http.NewRequest(http.MethodPost, server.URL+p+"?tenant_id="+fgTenant, nil)
		if err != nil {
			t.Fatalf("new req: %v", err)
		}
		req.Header.Set("X-Tenant-Id", fgTenant)
		resp := doReq(t, req)
		if resp.StatusCode != http.StatusMethodNotAllowed {
			t.Fatalf("%s: expected 405; got %d", p, resp.StatusCode)
		}
	}
}

func TestFG_NormaliseProbability_BothShapes(t *testing.T) {
	t.Parallel()
	// Construct a tiny repo and seed two breed_revealed events with
	// distribution_snapshot in both percent (25.0) and ratio (0.25) shapes.
	repo := inmem.NewFamiliarGrowthRepository()
	pub := subscribers.NewInMemoryEvidencePublisher()
	now := time.Date(2026, 5, 13, 12, 0, 0, 0, time.UTC)
	sub := subscribers.New(subscribers.Config{
		Repo:     repo,
		Evidence: pub,
		Now:      func() time.Time { return now },
	})
	// Use ratio-shape snapshot.
	ingestOrFail(t, sub, subscribers.FamiliarGrowthEvent{
		SourceTopic:   fg.TopicBreedRevealed,
		SourceEventID: uuidish(1, 'z'),
		TenantID:      fgTenant,
		FamiliarID:    fgFamiliar,
		EggSKU:        "egg.ratio.v1",
		Species:       "alpha",
		Rarity:        "common",
		DistributionSnapshot: map[string]any{
			"alpha": 0.5,
			"beta":  0.5,
		},
		OccurredAt: now,
	})
	router := httpadapter.NewRouter(
		nil, nil, nil,
		httpadapter.WithFamiliarGrowthRepo(repo),
	)
	server := httptest.NewServer(router)
	defer server.Close()
	req := newGet(t, server.URL+"/v1/audit/familiar-growth/breed-distribution?tenant_id="+fgTenant+"&egg_sku=egg.ratio.v1")
	resp := doReq(t, req)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: %d", resp.StatusCode)
	}
	var body fg.BreedDistributionReport
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	// Claimed should be normalised to [0,1] — both shapes accepted.
	if body.Claimed["alpha"] != 0.5 {
		t.Fatalf("expected claimed alpha=0.5; got %v", body.Claimed["alpha"])
	}
}

func TestFG_BreedDistribution_NoRolls(t *testing.T) {
	t.Parallel()
	repo := inmem.NewFamiliarGrowthRepository()
	router := httpadapter.NewRouter(
		nil, nil, nil,
		httpadapter.WithFamiliarGrowthRepo(repo),
	)
	server := httptest.NewServer(router)
	defer server.Close()
	req := newGet(t, server.URL+"/v1/audit/familiar-growth/breed-distribution?tenant_id="+fgTenant+"&egg_sku=egg.unknown.v1")
	resp := doReq(t, req)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200; got %d", resp.StatusCode)
	}
	var body fg.BreedDistributionReport
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.SampleSize != 0 {
		t.Fatalf("expected sample_size=0 (no rolls); got %d", body.SampleSize)
	}
	if body.ChiSquare != nil {
		t.Fatalf("expected nil chi-square when no rolls; got %+v", body.ChiSquare)
	}
}

func TestFG_RejectsNonGET(t *testing.T) {
	t.Parallel()
	repo := fillFGRepo(t)
	router := httpadapter.NewRouter(
		nil, nil, nil,
		httpadapter.WithFamiliarGrowthRepo(repo),
	)
	server := httptest.NewServer(router)
	defer server.Close()
	req, err := http.NewRequest(http.MethodPost, server.URL+"/v1/audit/familiar-growth/events?tenant_id="+fgTenant, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("X-Tenant-Id", fgTenant)
	resp := doReq(t, req)
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405; got %d", resp.StatusCode)
	}
}

// -----------------------------------------------------------------------------
// test helpers
// -----------------------------------------------------------------------------

func newGet(t *testing.T, url string) *http.Request {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("X-Tenant-Id", fgTenant)
	return req
}

func doReq(t *testing.T, req *http.Request) *http.Response {
	t.Helper()
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	return resp
}
