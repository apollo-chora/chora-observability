// Package correlation_test exercises the TraceCorrelation aggregate.
//
// TraceCorrelation links a logical request (correlation_id) to its W3C
// trace_id + parent span. Used to assemble cross-service traces.
package correlation_test

import (
	"testing"

	"github.com/5007-Capstone/chora/services/chora-observability/internal/domain/correlation"
)

const (
	tenantA = "01970000-0000-7000-8000-000000000001"
	corrA   = "01970000-0000-7000-b000-000000000001"
	traceA  = "00000000000000000000000000000001"
	spanA   = "0000000000000001"
)

func TestNewCorrelation_Valid(t *testing.T) {
	t.Parallel()

	c, err := correlation.New(correlation.NewParams{
		TenantID:      tenantA,
		CorrelationID: corrA,
		TraceID:       traceA,
		ParentSpanID:  spanA,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if c.RecordedAt.IsZero() {
		t.Errorf("RecordedAt zero")
	}
	if c.TraceID != traceA {
		t.Errorf("trace mismatch")
	}
}

func TestNewCorrelation_RejectsBadTraceID(t *testing.T) {
	t.Parallel()

	_, err := correlation.New(correlation.NewParams{
		TenantID:      tenantA,
		CorrelationID: corrA,
		TraceID:       "abc",
		ParentSpanID:  spanA,
	})
	if err == nil {
		t.Errorf("expected error for short trace_id")
	}
}

func TestNewCorrelation_RejectsBadSpanID(t *testing.T) {
	t.Parallel()

	_, err := correlation.New(correlation.NewParams{
		TenantID:      tenantA,
		CorrelationID: corrA,
		TraceID:       traceA,
		ParentSpanID:  "x",
	})
	if err == nil {
		t.Errorf("expected error for short span_id")
	}
}

func TestNewCorrelation_RejectsMissingTenant(t *testing.T) {
	t.Parallel()

	_, err := correlation.New(correlation.NewParams{
		TenantID:      "",
		CorrelationID: corrA,
		TraceID:       traceA,
		ParentSpanID:  spanA,
	})
	if err == nil {
		t.Errorf("expected error for missing tenant")
	}
}

func TestNewCorrelation_RejectsMissingCorrelationID(t *testing.T) {
	t.Parallel()

	_, err := correlation.New(correlation.NewParams{
		TenantID:      tenantA,
		CorrelationID: "",
		TraceID:       traceA,
		ParentSpanID:  spanA,
	})
	if err == nil {
		t.Errorf("expected error for missing correlation_id")
	}
}

func TestAttachChildSpan(t *testing.T) {
	t.Parallel()

	c, err := correlation.New(correlation.NewParams{
		TenantID:      tenantA,
		CorrelationID: corrA,
		TraceID:       traceA,
		ParentSpanID:  spanA,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := c.AttachChildSpan("0000000000000002"); err != nil {
		t.Fatalf("AttachChildSpan: %v", err)
	}
	if len(c.ChildSpans) != 1 {
		t.Errorf("childSpans = %d; want 1", len(c.ChildSpans))
	}
}

func TestAttachChildSpan_RejectsBadSpan(t *testing.T) {
	t.Parallel()

	c, _ := correlation.New(correlation.NewParams{
		TenantID:      tenantA,
		CorrelationID: corrA,
		TraceID:       traceA,
		ParentSpanID:  spanA,
	})
	if err := c.AttachChildSpan("nothex"); err == nil {
		t.Errorf("expected error for invalid child span")
	}
}
