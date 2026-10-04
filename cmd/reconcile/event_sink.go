// event_sink.go — env-driven EventSink construction for the daily
// reconciliation Cloud Run Job (ADR-167 / 2026-06-01 Tier 2 wiring).
//
// Before this, cmd/reconcile always used a logging-only stub. This loader
// resolves a REAL Pub/Sub publisher on
// chora.governance.payment_reconciliation.anomaly.v1 (+ the degraded topic)
// when the broker is configured, reusing the same chora-go-common/pubsub
// CloudPublisher the main server's outbox dispatcher uses.
//
// Configuration (env-only per `secrets-and-env` / no-inline-config):
//
//	CHORA_PUBSUB_PROJECT     GCP project hosting Pub/Sub topics. When set, a
//	                         real CloudPublisher is wired. When UNSET, the
//	                         logging stub is used (dev / local) UNLESS prod is
//	                         asserted (see below).
//	RECONCILE_REQUIRE_PUBSUB "1"/"true" → fail loud if CHORA_PUBSUB_PROJECT is
//	                         unset or the client cannot init. Set this in the
//	                         Cloud Run Job env so a misconfigured prod job
//	                         crashes instead of silently logging anomalies into
//	                         the void.
//	CHORA_SOURCE_PROJECT     envelope source_project stamp (default chora-489812).
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"os"
	"strings"

	cgcpubsub "github.com/5007-Capstone/chora/libs/chora-go-common/pubsub"
	"github.com/5007-Capstone/chora/services/chora-observability/internal/adapter/reconcilepublish"
	"github.com/5007-Capstone/chora/services/chora-observability/internal/domain/reconcile"
)

// errPubSubRequired is returned when RECONCILE_REQUIRE_PUBSUB is asserted but
// the broker cannot be wired — fail loud rather than silently log.
var errPubSubRequired = errors.New("reconcile: RECONCILE_REQUIRE_PUBSUB set but Pub/Sub publisher unavailable")

// loggingEventSink is the fallback that logs anomaly events as structured
// JSON. Retained ONLY for the unconfigured dev path (CHORA_PUBSUB_PROJECT
// unset + RECONCILE_REQUIRE_PUBSUB not asserted).
type loggingEventSink struct{}

func (loggingEventSink) Emit(_ context.Context, ev reconcile.AnomalyEvent) error {
	b, _ := json.Marshal(ev)
	log.Printf("anomaly_event (logging fallback — Pub/Sub unconfigured): %s", string(b))
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
//   - CHORA_PUBSUB_PROJECT set    → real CloudPublisher-backed PubSubEventSink
//     (satisfies both anomaly + degraded ports).
//   - CHORA_PUBSUB_PROJECT unset  → logging fallback (dev), UNLESS
//     RECONCILE_REQUIRE_PUBSUB asserts prod (then fail loud).
//
// shutdown closes the underlying Pub/Sub client when one was created.
func newEventSinksFromEnv(ctx context.Context) (sinks reconcileSinks, shutdown func(), err error) {
	project := strings.TrimSpace(os.Getenv("CHORA_PUBSUB_PROJECT"))
	requirePubSub := isTruthy(os.Getenv("RECONCILE_REQUIRE_PUBSUB"))

	if project == "" {
		if requirePubSub {
			return reconcileSinks{}, nil, errPubSubRequired
		}
		log.Printf("reconcile: CHORA_PUBSUB_PROJECT unset — anomaly events log-only (dev fallback)")
		return reconcileSinks{Anomaly: loggingEventSink{}}, func() {}, nil
	}

	client, err := cgcpubsub.NewGCPClient(ctx, project)
	if err != nil {
		if requirePubSub {
			return reconcileSinks{}, nil, errors.Join(errPubSubRequired, err)
		}
		// Non-prod: degrade to logging but make the degradation explicit.
		log.Printf("reconcile: Pub/Sub client init failed (%v) — falling back to log-only anomaly sink", err)
		return reconcileSinks{Anomaly: loggingEventSink{}}, func() {}, nil
	}

	publisher := cgcpubsub.NewCloudPublisher(client)
	sink := reconcilepublish.NewPubSubEventSink(reconcilepublish.Config{
		Publisher:     publisher,
		SourceProject: envOrDefault("CHORA_SOURCE_PROJECT", "chora-489812"),
		SourceService: "chora-observability",
	})
	log.Printf("reconcile: real Pub/Sub anomaly sink wired (project=%s, topic=%s)",
		project, reconcile.CanonicalReconcileAnomalyTopic)
	return reconcileSinks{Anomaly: sink, Degraded: sink}, func() { _ = client.Close() }, nil
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
