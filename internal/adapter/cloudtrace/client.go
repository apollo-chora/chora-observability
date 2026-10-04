// Package cloudtrace is the Cloud Trace OTLP read/export client adapter.
//
// MVP: this is a Mock client that records ExportRequest invocations against
// the configured endpoint (env-driven, NOT inline) and returns synthetic
// metadata. Real OTLP fanout to Cloud Trace is deferred to the post-Phyllis
// hardening pass — see internal/observability/otlp.go for the direct trace
// EXPORTER. This adapter handles the READ path (Spanstore queries).
//
// The ExportRequest API is what the O+ Decision-Log Explorer Stage S5 will
// call to fetch spans for a given trace_id range. Mock=true is set during
// the Phyllis MVP; production replaces with the real OTLP read client.
package cloudtrace

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
)

// ErrInvertedRange is returned when since > until.
var ErrInvertedRange = errors.New("inverted time range: since > until")

// Config configures the client.
type Config struct {
	// Endpoint is the OTLP/Cloud Trace gRPC endpoint, MUST be non-empty
	// (per CLAUDE.md no-inline-config rule). Sourced from
	// CLOUDTRACE_OTLP_READ_ENDPOINT env var at the call site.
	Endpoint string

	// Mock=true returns synthetic metadata without dialing. Used for tests
	// and the Phyllis MVP demo where Cloud Trace is wired emit-only.
	Mock bool
}

// ExportRequest is the read-side request shape for the Spanstore query API.
type ExportRequest struct {
	TenantID string
	TraceID  string // optional — empty matches any trace
	Since    time.Time
	Until    time.Time
}

// ExportResponse carries the queued export metadata.
type ExportResponse struct {
	ExportID string    `json:"export_id"`
	Status   string    `json:"status"`
	Endpoint string    `json:"endpoint"`
	Mock     bool      `json:"mock"`
	Since    time.Time `json:"since"`
	Until    time.Time `json:"until"`
	QueuedAt time.Time `json:"queued_at"`
}

// Client is the read-side client.
type Client struct {
	cfg         Config
	mu          sync.Mutex
	invocations int
}

// NewClient constructs the read-side Cloud Trace client. Refuses construction
// with an empty endpoint (no-inline-config rule).
func NewClient(cfg Config) (*Client, error) {
	if cfg.Endpoint == "" {
		return nil, fmt.Errorf("cloudtrace: endpoint is required (no inline config — read from env)")
	}
	return &Client{cfg: cfg}, nil
}

// Export queues a span-export request for the given range. Validates the
// range; returns synthetic metadata in mock mode.
func (c *Client) Export(_ context.Context, req ExportRequest) (ExportResponse, error) {
	if req.Since.IsZero() {
		return ExportResponse{}, errors.New("since is required")
	}
	if req.Until.IsZero() {
		return ExportResponse{}, errors.New("until is required")
	}
	if req.Since.After(req.Until) {
		return ExportResponse{}, ErrInvertedRange
	}

	c.mu.Lock()
	c.invocations++
	c.mu.Unlock()

	id, err := uuid.NewV7()
	if err != nil {
		return ExportResponse{}, fmt.Errorf("uuidv7: %w", err)
	}

	return ExportResponse{
		ExportID: id.String(),
		Status:   "queued",
		Endpoint: c.cfg.Endpoint,
		Mock:     c.cfg.Mock,
		Since:    req.Since.UTC(),
		Until:    req.Until.UTC(),
		QueuedAt: time.Now().UTC(),
	}, nil
}

// InvocationCount returns the number of Export calls received.
func (c *Client) InvocationCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.invocations
}
