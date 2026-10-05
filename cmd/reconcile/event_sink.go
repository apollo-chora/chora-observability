// event_sink.go — env-driven EventSink construction for the daily
// reconciliation job (ADR-167 / 2026-06-01 Tier 2 wiring).
//
// Before this, cmd/reconcile always used a logging-only stub. This loader
// resolves a REAL event-bus publisher on
// chora.governance.payment_reconciliation.anomaly.v1 (+ the degraded topic)
// when the broker is configured, reusing the same chora-common/eventbus
// JetStream bus the main server's outbox dispatcher uses.
//
// Configuration (env-only per `secrets-and-env` / no-inline-config):
//
//	NATS_URL                  NATS server URL (e.g. nats://nats:4222). When set,
//	                         a real JetStream bus is wired. When UNSET, the
//	                         logging stub is used (dev / local) UNLESS prod is
//	                         asserted (see below).
//	RECONCILE_REQUIRE_EVENTBUS "1"/"true" → fail loud if NATS_URL is unset or
//	                         the bus cannot init. Set this in the job env so a
//	                         misconfigured prod job crashes instead of silently
//	                         logging anomalies into the void.
//	CHORA_SOURCE_PROJECT     envelope source_project stamp (default chora-489812).
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"os"
	"strings"

	"github.com/apollo-chora/chora-common/eventbus"
	"github.com/apollo-chora/chora-observability/internal/adapter/reconcilepublish"
	"github.com/apollo-chora/chora-observability/internal/domain/reconcile"
)

// errEventBusRequired is returned when RECONCILE_REQUIRE_EVENTBUS is asserted
// but the broker cannot be wired — fail loud rather than silently log.
var errEventBusRequired = errors.New("reconcile: RECONCILE_REQUIRE_EVENTBUS set but the event bus is unavailable")

// loggingEventSink is the fallback that logs anomaly events as structured
// JSON. Retained ONLY for the unconfigured dev path (NATS_URL unset +
// RECONCILE_REQUIRE_EVENTBUS not asserted).
type loggingEventSink struct{}

func (loggingEventSink) Emit(_ context.Context, ev reconcile.AnomalyEvent) error {
	b, _ := json.Marshal(ev)
	log.Printf("anomaly_event (logging fallback — event bus unconfigured): %s", string(b))
	return nil
}

// reconcileSinks bundles the anomaly EventSink + the OPTIONAL DegradedSink so
// the Runner can emit both surfaces. DegradedSink is nil for the logging
// fallback (degraded events only matter when a real broker is wired).
type reconcileSinks struct {
	Anomaly  reconcile.EventSink
	Degraded reconcile.DegradedSink
}

// newEventSinksFromEnv resolves the reconciliation sinks based on env.
//
//   - NATS_URL set    → real JetStream-backed EventSink
//     (satisfies both anomaly + degraded ports).
//   - NATS_URL unset  → logging fallback (dev), UNLESS
//     RECONCILE_REQUIRE_EVENTBUS asserts prod (then fail loud).
//
// shutdown closes the underlying bus when one was created.
func newEventSinksFromEnv(ctx context.Context) (sinks reconcileSinks, shutdown func(), err error) {
	url := strings.TrimSpace(os.Getenv("NATS_URL"))
	requireEventBus := isTruthy(os.Getenv("RECONCILE_REQUIRE_EVENTBUS"))

	if url == "" {
		if requireEventBus {
			return reconcileSinks{}, nil, errEventBusRequired
		}
		log.Printf("reconcile: NATS_URL unset — anomaly events log-only (dev fallback)")
		return reconcileSinks{Anomaly: loggingEventSink{}}, func() {}, nil
	}

	bus, err := eventbus.NewJetStream(eventbus.JetStreamConfig{URL: url})
	if err != nil {
		if requireEventBus {
			return reconcileSinks{}, nil, errors.Join(errEventBusRequired, err)
		}
		// Non-prod: degrade to logging but make the degradation explicit.
		log.Printf("reconcile: JetStream init failed (%v) — falling back to log-only anomaly sink", err)
		return reconcileSinks{Anomaly: loggingEventSink{}}, func() {}, nil
	}

	sink := reconcilepublish.NewEventSink(reconcilepublish.Config{
		Publisher:     bus,
		SourceProject: envOrDefault("CHORA_SOURCE_PROJECT", "chora-489812"),
		SourceService: "chora-observability",
	})
	log.Printf("reconcile: real NATS JetStream anomaly sink wired (url=%s, topic=%s)",
		url, reconcile.CanonicalReconcileAnomalyTopic)
	return reconcileSinks{Anomaly: sink, Degraded: sink}, func() { _ = bus.Close() }, nil
}

// isTruthy treats "1"/"true"/"yes" (case-insensitive) as true.
func isTruthy(v string) bool {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

// envOrDefault returns the env var if set, otherwise the default.
func envOrDefault(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}
