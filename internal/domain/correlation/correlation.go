// Package correlation is the TraceCorrelation aggregate of the Observability
// domain.
//
// TraceCorrelation links a logical request (correlation_id) to its W3C
// trace_id + parent span. Used to assemble cross-service traces (e.g. when
// a Pub/Sub event spans multiple services and downstream consumers want
// to walk back to the originating request).
//
// Unlike TokenUsageLedger and AgentDecisionLog, TraceCorrelation supports
// AttachChildSpan to grow the child-span list. The append-only invariant
// applies to the trace_id + parent_span_id (those are immutable once set);
// child spans may grow.
package correlation

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/apollo-chora/chora-observability/internal/domain/ledger"
)

// Correlation is the TraceCorrelation aggregate root.
type Correlation struct {
	TenantID      string    `json:"tenant_id"`
	CorrelationID string    `json:"correlation_id"`
	TraceID       string    `json:"trace_id"`
	ParentSpanID  string    `json:"parent_span_id"`
	ChildSpans    []string  `json:"child_spans,omitempty"`
	RecordedAt    time.Time `json:"recorded_at"`
}

// NewParams is the constructor input.
type NewParams struct {
	TenantID      string
	CorrelationID string
	TraceID       string
	ParentSpanID  string
}

// New constructs a fresh TraceCorrelation.
func New(p NewParams) (*Correlation, error) {
	if strings.TrimSpace(p.TenantID) == "" {
		return nil, errors.New("tenant_id is required")
	}
	if strings.TrimSpace(p.CorrelationID) == "" {
		return nil, errors.New("correlation_id is required")
	}
	if err := ledger.ValidateTraceID(p.TraceID); err != nil {
		return nil, fmt.Errorf("trace_id: %w", err)
	}
	if err := ledger.ValidateSpanID(p.ParentSpanID); err != nil {
		return nil, fmt.Errorf("parent_span_id: %w", err)
	}

	return &Correlation{
		TenantID:      p.TenantID,
		CorrelationID: p.CorrelationID,
		TraceID:       strings.ToLower(p.TraceID),
		ParentSpanID:  strings.ToLower(p.ParentSpanID),
		RecordedAt:    time.Now().UTC(),
	}, nil
}

// AttachChildSpan appends a validated child span. The child span is
// validated as a W3C span_id (16 hex chars, not all zeros).
func (c *Correlation) AttachChildSpan(span string) error {
	if err := ledger.ValidateSpanID(span); err != nil {
		return fmt.Errorf("child span: %w", err)
	}
	c.ChildSpans = append(c.ChildSpans, strings.ToLower(span))
	return nil
}
