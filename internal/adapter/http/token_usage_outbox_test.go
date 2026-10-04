// Package httpadapter_test exercises the atomic ledger + outbox publish
// path on POST /api/token-usage.
//
// Per ai-cost-tracking + data-consistency: every successful token-usage
// write MUST atomically queue an outbox event for
// `chora.observability.token_usage.recorded.v1` (canonical topic per
// pub-sub-topology). The Dispatcher (separate background goroutine in
// cmd/server/main.go) drains the pending rows to Pub/Sub.
//
// Wiring contract:
//
//   - WithLedgerHook(hook) Option attaches a *ledger.LedgerHook to the
//     Handler. When set, POST /api/token-usage delegates to the hook
//     (ledger.Append + outbox.RecordOutboxEvent atomic pair) instead of
//     calling ledger.Append directly.
//   - When the Option is unset, behaviour is unchanged (M10 backwards-
//     compat — direct Append).
//   - On hook error, the HTTP response is 500 and NEITHER side commits
//     (atomicity invariant).
//
// W2c (M12.3 Wave 2, 2026-05-12): test migrated from the retired
// `inmem.OutboxRecorder` to the canonical `outbox.Publisher` backed by
// the canonical `outbox.InMemoryStore`. The semantics are unchanged; the
// underlying adapter shape now matches the D6.2 producer-side template.
package httpadapter_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	httpadapter "github.com/5007-Capstone/chora/services/chora-observability/internal/adapter/http"
	"github.com/5007-Capstone/chora/services/chora-observability/internal/adapter/inmem"
	"github.com/5007-Capstone/chora/services/chora-observability/internal/adapter/outbox"
	"github.com/5007-Capstone/chora/services/chora-observability/internal/domain/ledger"
)

// failingOutbox is a deterministic-failure OutboxRecorder used to assert
// the LedgerHook's atomicity invariant (outbox first; abort if it fails).
type failingOutbox struct{}

func (failingOutbox) RecordOutboxEvent(_ context.Context, _ ledger.OutboxRecord) error {
	return errors.New("simulated outbox failure")
}

func newOutboxFixture(t *testing.T) (http.Handler, *outbox.InMemoryStore, ledger.Repository) {
	t.Helper()
	ledgerRepo := inmem.NewLedgerRepository()
	decisionRepo := inmem.NewDecisionRepository()
	correlationRepo := inmem.NewCorrelationRepository()

	// Canonical outbox path: InMemoryStore + Publisher.
	store := outbox.NewInMemoryStore()
	publisher := outbox.NewPublisher(outbox.PublisherConfig{
		Store:         store,
		SourceProject: "chora-489812",
		SourceService: "chora-observability",
	})

	hook := ledger.NewLedgerHook(ledger.LedgerHookConfig{
		Ledger:        ledgerRepo,
		Outbox:        publisher,
		SourceProject: "chora-489812",
		SourceService: "chora-observability",
	})

	mux := httpadapter.NewRouter(ledgerRepo, decisionRepo, correlationRepo,
		httpadapter.WithLedgerHook(hook))
	return mux, store, ledgerRepo
}

func TestPostTokenUsage_RecordsLedgerAndOutboxAtomically(t *testing.T) {
	t.Parallel()
	mux, store, _ := newOutboxFixture(t)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	body := map[string]any{
		"gcid":              "gcid-1",
		"agid":              "agid-validator-1",
		"model_id":          "vertex_ai/gemini-2.5-flash",
		"prompt_tokens":     1000,
		"completion_tokens": 500,
		"cost_usd_micros":   int64(450),
		"trace_id":          "00000000000000000000000000000001",
		"span_id":           "0000000000000001",
	}
	bz, _ := json.Marshal(body)
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/api/token-usage", bytes.NewReader(bz))
	req.Header.Set("X-Tenant-ID", "tenant-a")
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d; want 201", resp.StatusCode)
	}

	// Outbox must have ONE pending event.
	pending, _ := store.FetchPending(context.Background(), 10)
	if len(pending) != 1 {
		t.Fatalf("pending outbox = %d; want 1", len(pending))
	}
	got := pending[0]
	if got.Topic != ledger.CanonicalTokenUsageTopic {
		t.Errorf("topic = %s; want %s", got.Topic, ledger.CanonicalTokenUsageTopic)
	}
	if got.EventType != ledger.EventTypeTokenUsageRecorded {
		t.Errorf("event_type = %s; want %s", got.EventType, ledger.EventTypeTokenUsageRecorded)
	}
	if got.TenantID != "tenant-a" {
		t.Errorf("tenant_id = %s; want tenant-a", got.TenantID)
	}
	if got.GCID != "gcid-1" {
		t.Errorf("gcid = %s; want gcid-1", got.GCID)
	}
	if got.Envelope["source_project"] != "chora-489812" {
		t.Errorf("envelope.source_project = %s; want chora-489812", got.Envelope["source_project"])
	}
	if got.Envelope["chora_imda_dimension"] != ledger.IMDADimensionAccountability {
		t.Errorf("envelope.chora_imda_dimension = %s; want %s",
			got.Envelope["chora_imda_dimension"], ledger.IMDADimensionAccountability)
	}
	if got.Envelope["schema_version"] != "1" {
		t.Errorf("envelope.schema_version = %s; want 1", got.Envelope["schema_version"])
	}
	if strings.TrimSpace(got.ID) == "" {
		t.Errorf("row.ID empty")
	}
	if strings.TrimSpace(got.IdempotencyKey) == "" {
		t.Errorf("row.IdempotencyKey empty")
	}
}

func TestPostTokenUsage_BackwardsCompatWithoutHook(t *testing.T) {
	t.Parallel()
	// When the hook Option is NOT supplied, behaviour MUST stay
	// identical to the M10 baseline (direct Append, no outbox).
	ledgerRepo := inmem.NewLedgerRepository()
	decisionRepo := inmem.NewDecisionRepository()
	correlationRepo := inmem.NewCorrelationRepository()
	mux := httpadapter.NewRouter(ledgerRepo, decisionRepo, correlationRepo)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	body := map[string]any{
		"gcid":            "gcid-2",
		"model_id":        "vertex_ai/gemini-2.5-flash",
		"prompt_tokens":   100,
		"cost_usd_micros": int64(45),
		"trace_id":        "00000000000000000000000000000002",
		"span_id":         "0000000000000002",
	}
	bz, _ := json.Marshal(body)
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/api/token-usage", bytes.NewReader(bz))
	req.Header.Set("X-Tenant-ID", "tenant-b")
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d; want 201 (backwards-compat path)", resp.StatusCode)
	}
}

func TestPostTokenUsage_OutboxFailureAborts(t *testing.T) {
	t.Parallel()
	// When the outbox recorder fails, the hook's atomicity contract
	// (outbox first; abort if outbox publish fails) means the HTTP
	// response is 500.
	ledgerRepo := inmem.NewLedgerRepository()
	decisionRepo := inmem.NewDecisionRepository()
	correlationRepo := inmem.NewCorrelationRepository()
	hook := ledger.NewLedgerHook(ledger.LedgerHookConfig{
		Ledger:        ledgerRepo,
		Outbox:        &failingOutbox{},
		SourceProject: "chora-489812",
		SourceService: "chora-observability",
	})
	mux := httpadapter.NewRouter(ledgerRepo, decisionRepo, correlationRepo,
		httpadapter.WithLedgerHook(hook))
	srv := httptest.NewServer(mux)
	defer srv.Close()

	body := map[string]any{
		"gcid":            "gcid-x",
		"model_id":        "vertex_ai/gemini-2.5-flash",
		"prompt_tokens":   1,
		"cost_usd_micros": int64(1),
		"trace_id":        "00000000000000000000000000000003",
		"span_id":         "0000000000000003",
	}
	bz, _ := json.Marshal(body)
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/api/token-usage", bytes.NewReader(bz))
	req.Header.Set("X-Tenant-ID", "tenant-c")
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status = %d; want 500 (outbox-failure abort)", resp.StatusCode)
	}
}
