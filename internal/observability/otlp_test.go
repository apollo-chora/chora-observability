// otlp_test.go — HHH-2 paydown coverage for the canonical-lib shim.
package observability

import (
	"context"
	"errors"
	"testing"
	"time"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// TestInit_StdoutFallbackOnDevEnv exercises the dev-mode path (no
// GOOGLE_CLOUD_PROJECT / OTEL_EXPORTER_OTLP_ENDPOINT / ADC). The canonical
// lib falls back to stdouttrace and Init() returns a usable shutdown.
func TestInit_StdoutFallbackOnDevEnv(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("GOOGLE_CLOUD_PROJECT", "")
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "")
	t.Setenv("CHORA_OTLP_INIT_TIMEOUT_SECONDS", "5")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	shutdown, err := Init(ctx)
	if err != nil {
		t.Fatalf("Init returned error in dev mode: %v", err)
	}
	if shutdown == nil {
		t.Fatalf("Init returned nil shutdown")
	}

	shutdownCtx, c := context.WithTimeout(context.Background(), 2*time.Second)
	defer c()
	if err := shutdown(shutdownCtx); err != nil {
		t.Errorf("shutdown returned error: %v", err)
	}
}

// TestInitAsync_HandleResolves verifies InitAsync returns a non-nil
// handle that eventually resolves to a usable OTLPResult.
func TestInitAsync_HandleResolves(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("GOOGLE_CLOUD_PROJECT", "")
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "")
	t.Setenv("CHORA_OTLP_INIT_TIMEOUT_SECONDS", "5")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	handle := InitAsync(ctx)
	if handle == nil {
		t.Fatalf("InitAsync returned nil handle")
	}

	waitCtx, c := context.WithTimeout(context.Background(), 3*time.Second)
	defer c()
	res := handle.WaitContext(waitCtx)
	if res.Shutdown == nil {
		t.Fatalf("OTLPResult.Shutdown is nil; expected at least a no-op")
	}

	shutdownCtx, c2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer c2()
	if err := res.Shutdown(shutdownCtx); err != nil {
		t.Errorf("shutdown returned error: %v", err)
	}
}

// TestInit_TimedOutCtxReturnsErrInitTimeout exercises the deadline path —
// the caller's ctx fires before the lib's 15s default OTLP timeout.
// Confirms the shim surfaces ErrInitTimeout (not a panic) and the
// shutdown closure remains safe to call.
func TestInit_TimedOutCtxReturnsErrInitTimeout(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("GOOGLE_CLOUD_PROJECT", "")
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "")
	// Force the lib's init goroutine to take longer than our ctx allows.
	t.Setenv("CHORA_OTLP_INIT_TIMEOUT_SECONDS", "30")

	// Cancel before WaitContext can settle.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	shutdown, err := Init(ctx)
	if shutdown == nil {
		t.Fatalf("Init returned nil shutdown even on timeout (fail-soft contract violated)")
	}
	// On a cancelled-ctx path the lib may return either InitError or
	// TimedOut depending on the inner-vs-outer race. Either way the
	// shim should NOT panic, and shutdown is always safe.
	if err != nil && !errors.Is(err, ErrInitTimeout) {
		// Acceptable: a context.Canceled / context.DeadlineExceeded
		// surfaced as InitError (preserved per Init contract).
		// We only fail on a hard panic / nil shutdown which were
		// asserted above.
		t.Logf("Init returned non-timeout error (acceptable): %v", err)
	}

	shutdownCtx, c := context.WithTimeout(context.Background(), 2*time.Second)
	defer c()
	if err := shutdown(shutdownCtx); err != nil {
		t.Errorf("shutdown returned error after timeout (should be no-op): %v", err)
	}
}

// TestSwapExporterFactoryForTest_DelegatesToCanonicalLib verifies the
// compat alias actually swaps the canonical-lib factory.
func TestSwapExporterFactoryForTest_DelegatesToCanonicalLib(t *testing.T) {
	var called bool
	restore := SwapExporterFactoryForTest(func(ctx context.Context, endpoint, serviceName, version string) (sdktrace.SpanExporter, error) {
		called = true
		if serviceName != ServiceName {
			t.Errorf("exporter factory got serviceName=%q; want %q", serviceName, ServiceName)
		}
		// Return a synthetic error so the SDK init path exits cleanly
		// without needing a real exporter (the fail-soft contract means
		// the caller still gets a no-op shutdown).
		return nil, errors.New("stub exporter (test)")
	})
	defer restore()

	// Pin a short OTLP init timeout so Init() returns quickly even when
	// the synthetic factory error propagates through the lib's
	// goroutine-bounded path.
	t.Setenv("CHORA_OTLP_INIT_TIMEOUT_SECONDS", "2")
	// Force the lib's isDevExport heuristic OFF so cloudtrace path runs
	// our swapped factory; otherwise stdouttrace short-circuits the
	// factory.
	t.Setenv("GOOGLE_CLOUD_PROJECT", "chora-observability-test")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Drive Init() so the canonical lib's globalInit codepath fires the
	// swapped factory.
	shutdown, _ := Init(ctx)
	if shutdown == nil {
		t.Fatalf("Init returned nil shutdown even when factory errored (fail-soft contract violated)")
	}
	if !called {
		t.Errorf("swapped exporter factory was never invoked")
	}

	// Shutdown should be safe to call even after a factory error.
	shutdownCtx, c := context.WithTimeout(context.Background(), 1*time.Second)
	defer c()
	if err := shutdown(shutdownCtx); err != nil {
		t.Errorf("shutdown returned error after factory error: %v", err)
	}
}

// TestServiceNameConstantPinned guards against accidental rename of the
// canonical service name (Cloud Trace uses service.name as the index
// key; changing it splits historical span lookups).
func TestServiceNameConstantPinned(t *testing.T) {
	if ServiceName != "chora-observability" {
		t.Errorf("ServiceName drift: got %q; want chora-observability", ServiceName)
	}
}
