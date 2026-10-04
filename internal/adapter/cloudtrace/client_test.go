// Package cloudtrace_test exercises the Cloud Trace OTLP export client.
//
// The MVP client is a mock that records ExportRequest invocations against
// the configured endpoint (env var, NOT inline). Real OTLP fanout to Cloud
// Trace is deferred to Tier 2 — see internal/observability/otlp.go for the
// direct trace exporter wiring.
package cloudtrace_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/apollo-chora/chora-observability/internal/adapter/cloudtrace"
)

func TestNewClient_RequiresEndpoint(t *testing.T) {
	t.Parallel()
	_, err := cloudtrace.NewClient(cloudtrace.Config{Endpoint: ""})
	if err == nil {
		t.Errorf("expected error for empty endpoint (no inline config)")
	}
}

func TestNewClient_AcceptsEndpoint(t *testing.T) {
	t.Parallel()
	c, err := cloudtrace.NewClient(cloudtrace.Config{
		Endpoint: "trace.example.com:4317",
		Mock:     true,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if c == nil {
		t.Errorf("client nil")
	}
}

func TestClient_Export_ReturnsExportMetadata(t *testing.T) {
	t.Parallel()
	c, _ := cloudtrace.NewClient(cloudtrace.Config{
		Endpoint: "trace.example.com:4317",
		Mock:     true,
	})
	res, err := c.Export(context.Background(), cloudtrace.ExportRequest{
		TenantID: "t1",
		Since:    time.Date(2026, 5, 8, 0, 0, 0, 0, time.UTC),
		Until:    time.Date(2026, 5, 8, 23, 59, 59, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("Export: %v", err)
	}
	if res.ExportID == "" {
		t.Errorf("ExportID empty")
	}
	if !res.Mock {
		t.Errorf("MVP must report Mock=true")
	}
	if res.Endpoint != "trace.example.com:4317" {
		t.Errorf("endpoint = %s; want trace.example.com:4317", res.Endpoint)
	}
}

func TestClient_Export_RejectsInvertedRange(t *testing.T) {
	t.Parallel()
	c, _ := cloudtrace.NewClient(cloudtrace.Config{
		Endpoint: "trace.example.com:4317",
		Mock:     true,
	})
	_, err := c.Export(context.Background(), cloudtrace.ExportRequest{
		TenantID: "t1",
		Since:    time.Date(2026, 5, 9, 0, 0, 0, 0, time.UTC),
		Until:    time.Date(2026, 5, 8, 0, 0, 0, 0, time.UTC),
	})
	if err == nil {
		t.Errorf("expected error for inverted range")
	}
	if !errors.Is(err, cloudtrace.ErrInvertedRange) {
		t.Errorf("err = %v; want ErrInvertedRange", err)
	}
}

func TestClient_Export_RejectsZeroSince(t *testing.T) {
	t.Parallel()
	c, _ := cloudtrace.NewClient(cloudtrace.Config{
		Endpoint: "trace.example.com:4317",
		Mock:     true,
	})
	_, err := c.Export(context.Background(), cloudtrace.ExportRequest{
		TenantID: "t1",
		Until:    time.Date(2026, 5, 8, 0, 0, 0, 0, time.UTC),
	})
	if err == nil {
		t.Errorf("expected error for zero since")
	}
}

func TestClient_Export_RecordsInvocations(t *testing.T) {
	t.Parallel()
	c, _ := cloudtrace.NewClient(cloudtrace.Config{
		Endpoint: "trace.example.com:4317",
		Mock:     true,
	})
	for i := 0; i < 3; i++ {
		_, err := c.Export(context.Background(), cloudtrace.ExportRequest{
			TenantID: "t1",
			Since:    time.Date(2026, 5, 8, 0, 0, 0, 0, time.UTC),
			Until:    time.Date(2026, 5, 8, 23, 59, 59, 0, time.UTC),
		})
		if err != nil {
			t.Fatalf("Export[%d]: %v", i, err)
		}
	}
	if got := c.InvocationCount(); got != 3 {
		t.Errorf("invocation count = %d; want 3", got)
	}
}
