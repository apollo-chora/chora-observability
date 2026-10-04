// Package observability wires traces direct to Cloud Trace per Tier 3 D12.
// NO Langfuse, NO OTel Collector.
//
// HHH-2 paydown (2026-05-14): the previous bespoke wiring in this file
// hand-rolled the entire OTLP TracerProvider + exporter selection on top
// of cloudtrace.New / stdouttrace.New. LL's Wave B audit (commit
// 533cfbda) flagged chora-observability as the 4th Cloud Trace "dark"
// service — the bespoke adapter rejected the `https://telemetry.googleapis.com:443`
// endpoint scheme (the canonical lib accepts the scheme as a log-only
// hint and reaches Cloud Trace via ADC). Wave B's follow-on F-shim swept
// 11 services but skipped chora-observability intentionally because it's
// the observability service ITSELF — special-cased to its own (older)
// OTLP wiring path.
//
// This file now thinly delegates to chora-common/observability
// (canonical lib) so the bespoke divergence closes for good. Backward-
// compatibility is preserved at the public API level (Init / SwapExporterFactoryForTest)
// so the call site in cmd/server/main.go can either keep the legacy
// shutdown-handle shape or migrate to the new InitAsync handle shape.
//
// Migration semantics:
//
//   - Init(ctx) — sync compat shim returning the legacy
//     (shutdown func(context.Context) error, error) shape. Preferred for
//     test main()s that don't want the async-handle ceremony.
//
//   - InitAsync(ctx) — new entry point returning *bootstrap.OTLPHandle
//     per chora-sharing / chora-tenancy reference migration. Decouples
//     OTLP init from pgx-pool init so a slow Cloud Trace TLS handshake
//     can no longer swallow the pod's bootstrap budget under PgBouncer
//     4-container cold-start. See chora-common/bootstrap/README.md.
//
// Recursion warning (META-LEVEL OBSERVABILITY): chora-observability emits
// traces for ITS OWN HTTP requests. Those self-emitted traces do NOT
// generate self-referencing TokenUsageLedger entries because:
//
//  1. /api/token-usage POSTs only persist the data the caller passes in.
//  2. chora-observability never calls an LLM itself, so it has nothing to
//     ledger about its own work.
//  3. The Model Gateway (which DOES call LLMs) is responsible for not
//     ledger-recording its own internal-self-tracing recursion.
//
// Future-proofing: any tail-sampling layer (deferred to Tier 2) layers
// on top of the canonical lib's Init at the Sampler interface — wrap the
// chora-observability-specific sampler around the canonical SDK's
// TracerProvider rather than re-rolling the exporter.
package observability

import (
	"context"
	"errors"

	"github.com/apollo-chora/chora-common/bootstrap"
	commonobs "github.com/apollo-chora/chora-common/observability"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// ServiceName is the canonical service identifier emitted on every span.
const ServiceName = "chora-observability"

// ServiceVersion follows semver per OpenInference convention. Injected at
// build time when set via -ldflags "-X ...ServiceVersion=..."; empty
// default is acceptable (Cloud Trace will record service.version="" which
// is non-fatal for span ingest).
const ServiceVersion = "0.1.0"

// Init wires the trace exporter and registers a global TracerProvider.
// Returns a shutdown func the caller MUST defer to flush spans on exit.
//
// This is the LEGACY sync entry point preserved for backward compat with
// `cmd/server/main.go` and any test that wires the trace provider in-
// process. Internally it now drives commonobs.InitOTLPAsync + WaitContext
// so the lib's TLS+ADC handling sweeps in; the only difference vs the
// async path is that this blocks until init settles.
//
// Service name is fixed to "chora-observability". Dev fallback path (no
// GOOGLE_CLOUD_PROJECT / OTEL_EXPORTER_OTLP_ENDPOINT / ADC) is provided
// by the canonical lib's `isDevExport` heuristic — spans route to
// stdouttrace.
//
// Returns ErrInitTimeout if the OTLP init didn't settle before the
// caller's ctx deadline. The shutdown closure is always safe to call
// (no-op on timeout / init-error per the lib's fail-soft contract).
func Init(ctx context.Context) (shutdown func(context.Context) error, err error) {
	handle := commonobs.InitOTLPAsync(ctx, ServiceName, ServiceVersion)
	res := handle.WaitContext(ctx)
	if res.InitError != nil {
		// Surface the lib's init error to preserve the legacy Init()
		// error-return contract. The shutdown closure is still safe.
		return res.Shutdown, res.InitError
	}
	if !res.Initialized && res.TimedOut {
		return res.Shutdown, ErrInitTimeout
	}
	return res.Shutdown, nil
}

// InitAsync is the NEW non-blocking entry point per chora-sharing /
// chora-tenancy reference migration (C(a).S1 path (b) — tracker #151).
// Returns immediately with a bootstrap.OTLPHandle the caller blocks on
// via Wait / WaitContext at the time of its choosing. Use this in
// `cmd/server/main.go` to let pgx-pool init proceed with the full
// CHORA_BOOTSTRAP_TIMEOUT_SECONDS budget while OTLP TLS handshakes
// happen in their own goroutine.
//
// Mirrors:
//
//	otlpHandle := observability.InitAsync(ctx)
//	defer func() {
//	    shutdownCtx, c := context.WithTimeout(context.Background(), 5*time.Second)
//	    defer c()
//	    res := otlpHandle.WaitContext(shutdownCtx)
//	    if err := res.Shutdown(shutdownCtx); err != nil {
//	        log.Printf("trace shutdown error: %v", err)
//	    }
//	}()
func InitAsync(ctx context.Context) *bootstrap.OTLPHandle {
	return commonobs.InitOTLPAsync(ctx, ServiceName, ServiceVersion)
}

// ErrInitTimeout is returned by Init when the OTLP init didn't settle
// before the caller's ctx deadline. Surfaced for diagnostics — the
// canonical lib's fail-soft contract guarantees the shutdown closure is
// still safe to call even after a timeout.
var ErrInitTimeout = errors.New("observability: OTLP init timed out (fail-soft; shutdown is no-op)")

// SwapExporterFactoryForTest is a backward-compat shim. The bespoke
// adapter previously had a package-level exporterFactory variable that
// tests swapped to inject a no-op exporter. The canonical lib has its
// own commonobs.SwapExporterFactoryForTest — this thin alias delegates
// so existing test imports keep compiling.
//
// Test-only — never call from production code paths.
func SwapExporterFactoryForTest(fn func(ctx context.Context, endpoint, serviceName, version string) (sdktrace.SpanExporter, error)) func() {
	return commonobs.SwapExporterFactoryForTest(fn)
}
