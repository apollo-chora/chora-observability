// Package tempo_test exercises the Tempo-backed trace-read client against an
// httptest server standing in for Tempo's query API.
package tempo_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	httpadapter "github.com/apollo-chora/chora-observability/internal/adapter/http"
	"github.com/apollo-chora/chora-observability/internal/adapter/tempo"
)

const traceJSON = `{
  "batches": [
    {
      "resource": {"attributes": [{"key": "service.name", "value": {"stringValue": "chora-observability"}}]},
      "scopeSpans": [
        {
          "scope": {"name": "otel"},
          "spans": [
            {
              "traceId": "000000000000000000000000000000aa",
              "spanId": "00000000000000bb",
              "parentSpanId": "",
              "name": "GET /api/token-usage",
              "kind": 2,
              "startTimeUnixNano": "1759000000000000000",
              "endTimeUnixNano": "1759000000500000000",
              "attributes": [{"key": "http.method", "value": {"stringValue": "GET"}}],
              "status": {"code": 1}
            }
          ]
        }
      ]
    }
  ]
}`

const searchJSON = `{
  "traces": [
    {"traceID": "000000000000000000000000000000aa", "rootServiceName": "chora-observability", "rootTraceName": "GET /api/token-usage", "startTimeUnixNano": "1759000000000000000", "durationMs": 500, "spanCount": 1}
  ],
  "metrics": {"totalTraces": 1}
}`

// newTempoClient returns an httptest server serving the trace + search
// fixtures and a client wired to it.
func newTempoClient(t *testing.T) *tempo.Client {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasPrefix(r.URL.Path, "/api/traces/"):
			_, _ = w.Write([]byte(traceJSON))
		case strings.HasPrefix(r.URL.Path, "/api/search"):
			_, _ = w.Write([]byte(searchJSON))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	c, err := tempo.NewClient(tempo.Config{QueryURL: srv.URL})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return c
}

// req builds a TraceExportRequest with a default 24h window.
func req(traceID string) httpadapter.TraceExportRequest {
	since := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	return httpadapter.TraceExportRequest{TraceID: traceID, Since: since, Until: since.Add(24 * time.Hour)}
}

func TestNewClient_RequiresQueryURL(t *testing.T) {
	t.Parallel()
	if _, err := tempo.NewClient(tempo.Config{QueryURL: ""}); err == nil {
		t.Error("expected error for empty QueryURL (no inline config)")
	}
}

func TestExport_FetchTraceByID(t *testing.T) {
	t.Parallel()
	c := newTempoClient(t)
	res, err := c.Export(context.Background(), req("000000000000000000000000000000aa"))
	if err != nil {
		t.Fatalf("Export: %v", err)
	}
	if res.Status != "fetched" {
		t.Errorf("status = %q; want fetched", res.Status)
	}
	if res.Mock {
		t.Error("real Tempo read must not report Mock=true")
	}
	if res.SpanCount != 1 || len(res.Spans) != 1 {
		t.Fatalf("span count = %d (len=%d); want 1", res.SpanCount, len(res.Spans))
	}
	s := res.Spans[0]
	if s.Name != "GET /api/token-usage" {
		t.Errorf("span name = %q; want GET /api/token-usage", s.Name)
	}
	if s.Attributes["http.method"] != "GET" {
		t.Errorf("http.method = %q; want GET", s.Attributes["http.method"])
	}
	if !s.StartTime.Equal(time.Unix(0, 1759000000000000000).UTC()) {
		t.Errorf("start time = %v; want parsed nano", s.StartTime)
	}
}

func TestExport_SearchByWindow(t *testing.T) {
	t.Parallel()
	c := newTempoClient(t)
	res, err := c.Export(context.Background(), req(""))
	if err != nil {
		t.Fatalf("Export: %v", err)
	}
	if res.SpanCount != 1 {
		t.Errorf("span count = %d; want 1 (search hit flattened)", res.SpanCount)
	}
}

func TestExport_RejectsInvertedRange(t *testing.T) {
	t.Parallel()
	c := newTempoClient(t)
	since := time.Date(2026, 10, 2, 0, 0, 0, 0, time.UTC)
	until := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	_, err := c.Export(context.Background(), httpadapter.TraceExportRequest{TraceID: "x", Since: since, Until: until})
	if err == nil {
		t.Error("expected error for inverted range")
	}
}

func TestExport_RejectsZeroSince(t *testing.T) {
	t.Parallel()
	c := newTempoClient(t)
	_, err := c.Export(context.Background(), httpadapter.TraceExportRequest{TraceID: "x"})
	if err == nil {
		t.Error("expected error for zero since")
	}
}

func TestExport_TraceNotFoundYieldsEmptySpans(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}))
	t.Cleanup(srv.Close)
	c, err := tempo.NewClient(tempo.Config{QueryURL: srv.URL})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	res, err := c.Export(context.Background(), req("missing"))
	if err != nil {
		t.Fatalf("Export: %v; want nil (404 -> empty spans)", err)
	}
	if res.SpanCount != 0 {
		t.Errorf("span count = %d; want 0 for a missing trace", res.SpanCount)
	}
}
