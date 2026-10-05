// Package tempo is the Tempo-backed trace-read adapter for the Spanstore
// query API. It replaces the retired Cloud Trace read client: the local stack
// runs Grafana Tempo (OTLP ingest on tempo:4317, query API on tempo:3200),
// so span reads are served by Tempo's query API instead of Cloud Trace.
//
// The client satisfies the httpadapter.TraceExporter port. When the request
// carries a trace ID it fetches that specific trace; otherwise it searches
// Tempo for traces in the requested window and flattens their spans. Every
// read is real — there is no mock mode and no synthetic metadata.
//
// Configuration (env-only per secrets-and-env / no-inline-config):
//
//	TEMPO_QUERY_URL   Tempo query API base (e.g. http://tempo:3200). Required.
//	TEMPO_TIMEOUT     per-request HTTP timeout (default 10s).
package tempo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	httpadapter "github.com/apollo-chora/chora-observability/internal/adapter/http"
)

// errTraceNotFound is returned by getJSON when Tempo answers 404. Callers
// translate it into an empty span list (a missing trace is not an error).
var errTraceNotFound = errors.New("tempo: trace not found")

// Config configures the Tempo client.
type Config struct {
	// QueryURL is the Tempo query API base URL (TEMPO_QUERY_URL), e.g.
	// http://tempo:3200. Required (no inline config).
	QueryURL string

	// Timeout is the per-request HTTP timeout. Default 10s.
	Timeout time.Duration

	// HTTPClient is optional; defaults to &http.Client{Timeout: Timeout}.
	HTTPClient *http.Client

	// MaxTraces caps how many traces a windowed search fetches + flattens.
	// Default 10.
	MaxTraces int
}

// Client reads traces from Tempo. Satisfies httpadapter.TraceExporter.
type Client struct {
	cfg     Config
	baseURL *url.URL
}

// NewClient constructs the Tempo client. Refuses an empty QueryURL
// (no-inline-config rule).
func NewClient(cfg Config) (*Client, error) {
	if strings.TrimSpace(cfg.QueryURL) == "" {
		return nil, errors.New("tempo: QueryURL required (set TEMPO_QUERY_URL)")
	}
	u, err := url.Parse(cfg.QueryURL)
	if err != nil {
		return nil, fmt.Errorf("tempo: invalid QueryURL: %w", err)
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 10 * time.Second
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = &http.Client{Timeout: cfg.Timeout}
	}
	if cfg.MaxTraces <= 0 {
		cfg.MaxTraces = 10
	}
	return &Client{cfg: cfg, baseURL: u}, nil
}

// Export reads traces from Tempo for the request. When req.TraceID is set it
// fetches that specific trace; otherwise it searches Tempo for traces in
// [req.Since, req.Until] and flattens their spans. Satisfies
// httpadapter.TraceExporter.
func (c *Client) Export(ctx context.Context, req httpadapter.TraceExportRequest) (httpadapter.TraceExportResponse, error) {
	if req.Since.IsZero() {
		return httpadapter.TraceExportResponse{}, errors.New("tempo: since is required")
	}
	if req.Until.IsZero() {
		return httpadapter.TraceExportResponse{}, errors.New("tempo: until is required")
	}
	if req.Since.After(req.Until) {
		return httpadapter.TraceExportResponse{}, errors.New("tempo: inverted time range: since > until")
	}

	var spans []httpadapter.TraceSpan
	var err error
	if id := strings.TrimSpace(req.TraceID); id != "" {
		spans, err = c.fetchTrace(ctx, id)
	} else {
		spans, err = c.searchTraces(ctx, req)
	}
	if err != nil {
		return httpadapter.TraceExportResponse{}, err
	}

	return httpadapter.TraceExportResponse{
		ExportID:  strings.TrimSpace(req.TraceID),
		Status:    "fetched",
		Endpoint:  c.cfg.QueryURL,
		Mock:      false,
		Since:     req.Since.UTC(),
		Until:     req.Until.UTC(),
		QueuedAt:  time.Now().UTC(),
		Spans:     spans,
		SpanCount: len(spans),
	}, nil
}

// fetchTrace fetches one trace by ID and flattens its spans. A 404 (trace
// not found) yields an empty span list, not an error.
func (c *Client) fetchTrace(ctx context.Context, traceID string) ([]httpadapter.TraceSpan, error) {
	u := *c.baseURL
	u.Path = strings.TrimRight(u.Path, "/") + "/api/traces/" + url.PathEscape(traceID)
	var out tempoTraceResponse
	err := c.getJSON(ctx, u.String(), &out)
	if errors.Is(err, errTraceNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("tempo: fetch trace %s: %w", traceID, err)
	}
	return flattenBatches(out.Batches), nil
}

// searchTraces searches Tempo for traces in [req.Since, req.Until], fetches
// each (up to MaxTraces), and flattens their spans.
func (c *Client) searchTraces(ctx context.Context, req httpadapter.TraceExportRequest) ([]httpadapter.TraceSpan, error) {
	u := *c.baseURL
	u.Path = strings.TrimRight(u.Path, "/") + "/api/search"
	q := u.Query()
	q.Set("start", strconv.FormatInt(req.Since.Unix(), 10))
	q.Set("end", strconv.FormatInt(req.Until.Unix(), 10))
	q.Set("limit", strconv.Itoa(c.cfg.MaxTraces))
	u.RawQuery = q.Encode()

	var out tempoSearchResponse
	if err := c.getJSON(ctx, u.String(), &out); err != nil {
		return nil, fmt.Errorf("tempo: search: %w", err)
	}
	var spans []httpadapter.TraceSpan
	for _, tr := range out.Traces {
		s, err := c.fetchTrace(ctx, tr.TraceID)
		if err != nil {
			continue // skip traces that fail to fetch; the rest still return
		}
		spans = append(spans, s...)
	}
	return spans, nil
}

// getJSON GETs url and decodes the JSON body into out. Returns
// errTraceNotFound on a 404.
func (c *Client) getJSON(ctx context.Context, url string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := c.cfg.HTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return errTraceNotFound
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		buf, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<14))
		return fmt.Errorf("tempo: status=%d body=%s", resp.StatusCode, string(buf))
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("tempo: decode: %w", err)
	}
	return nil
}

// --- Tempo API wire shapes -------------------------------------------------

// tempoTraceResponse is the GET /api/traces/{traceID} payload.
type tempoTraceResponse struct {
	Batches []tempoBatch `json:"batches"`
}

// tempoBatch is one resource batch in a trace response.
type tempoBatch struct {
	Resource   tempoResource    `json:"resource"`
	ScopeSpans []tempoScopeSpan `json:"scopeSpans"`
}

// tempoResource carries the resource attributes (service.name etc.).
type tempoResource struct {
	Attributes []tempoAttribute `json:"attributes"`
}

// tempoScopeSpan groups spans under one instrumentation scope.
type tempoScopeSpan struct {
	Spans []tempoSpan `json:"spans"`
}

// tempoSpan is one span in a trace.
type tempoSpan struct {
	TraceID           string           `json:"traceId"`
	SpanID            string           `json:"spanId"`
	ParentSpanID      string           `json:"parentSpanId"`
	Name              string           `json:"name"`
	Kind              int              `json:"kind"`
	StartTimeUnixNano string           `json:"startTimeUnixNano"`
	EndTimeUnixNano   string           `json:"endTimeUnixNano"`
	Attributes        []tempoAttribute `json:"attributes"`
	Status            tempoStatus      `json:"status"`
}

// tempoStatus is a span's status.
type tempoStatus struct {
	Code int `json:"code"`
}

// tempoAttribute is a key-value attribute. The value is a oneof; we read the
// common string / int / bool / double forms.
type tempoAttribute struct {
	Key   string     `json:"key"`
	Value tempoValue `json:"value"`
}

// tempoValue is a Tempo attribute value (oneof).
type tempoValue struct {
	StringValue string  `json:"stringValue"`
	IntValue    int64   `json:"intValue"`
	BoolValue   bool    `json:"boolValue"`
	DoubleValue float64 `json:"doubleValue"`
}

// tempoSearchResponse is the GET /api/search payload.
type tempoSearchResponse struct {
	Traces []tempoSearchTrace `json:"traces"`
}

// tempoSearchTrace is one search hit.
type tempoSearchTrace struct {
	TraceID           string `json:"traceID"`
	RootServiceName   string `json:"rootServiceName"`
	RootTraceName     string `json:"rootTraceName"`
	StartTimeUnixNano string `json:"startTimeUnixNano"`
	DurationMs        int64  `json:"durationMs"`
	SpanCount         int    `json:"spanCount"`
}

// flattenBatches folds Tempo batches into a flat span list.
func flattenBatches(batches []tempoBatch) []httpadapter.TraceSpan {
	var out []httpadapter.TraceSpan
	for _, b := range batches {
		for _, ss := range b.ScopeSpans {
			for _, s := range ss.Spans {
				out = append(out, toTraceSpan(s))
			}
		}
	}
	return out
}

// toTraceSpan maps a Tempo span to the port's TraceSpan.
func toTraceSpan(s tempoSpan) httpadapter.TraceSpan {
	return httpadapter.TraceSpan{
		TraceID:      s.TraceID,
		SpanID:       s.SpanID,
		ParentSpanID: s.ParentSpanID,
		Name:         s.Name,
		StartTime:    unixNanoToTime(s.StartTimeUnixNano),
		EndTime:      unixNanoToTime(s.EndTimeUnixNano),
		Attributes:   attributesToMap(s.Attributes),
	}
}

// attributesToMap flattens Tempo attributes to a string map.
func attributesToMap(attrs []tempoAttribute) map[string]string {
	if len(attrs) == 0 {
		return nil
	}
	m := make(map[string]string, len(attrs))
	for _, a := range attrs {
		m[a.Key] = a.Value.String()
	}
	return m
}

// String renders a Tempo attribute value as a string.
func (v tempoValue) String() string {
	switch {
	case v.StringValue != "":
		return v.StringValue
	case v.IntValue != 0:
		return strconv.FormatInt(v.IntValue, 10)
	case v.DoubleValue != 0:
		return strconv.FormatFloat(v.DoubleValue, 'f', -1, 64)
	case v.BoolValue:
		return "true"
	default:
		return ""
	}
}

// unixNanoToTime parses a Unix-nano string into a time. Returns the zero
// time when the value is absent or malformed.
func unixNanoToTime(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return time.Time{}
	}
	return time.Unix(0, n).UTC()
}
