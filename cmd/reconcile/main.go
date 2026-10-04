// Package main is the chora-observability daily reconciliation Cloud Run Job.
//
// Per ai-cost-tracking skill ("Reconciliation harness"):
//
//   - Cron: 04:00 UTC daily (Cloud Scheduler trigger; matches pricing.yaml
//     reconciliation.cron).
//   - Source 1: BigQuery SUM(token_usage_ledger.cost_usd_micros) over the
//     prior 24h window, excluding eval-runs.
//   - Source 2: Vertex Billing API aggregated cost over the same window.
//   - Tolerance: ±0.01% (default; tunable via RECONCILE_TOLERANCE_FRACTION).
//   - Drift exceeding tolerance → publish
//     chora.governance.payment_reconciliation.anomaly.v1 (Pub/Sub).
//
// Configuration (env-only per CLAUDE.md no-inline-config):
//
//	GOOGLE_CLOUD_PROJECT          GCP project (required)
//	BIGQUERY_DATASET              BigQuery analytics dataset
//	                              (default: chora_observability_analytics)
//	RECONCILE_LOOKBACK_DAYS       Days to reconcile (default: 1)
//	RECONCILE_TOLERANCE_FRACTION  Drift threshold (default: 0.0001)
//	BILLING_CLIENT_MODE           "mock" | "vertex" (default: "mock" — M10
//	                              skeleton; "vertex" wires the real google-
//	                              cloud-go billing client at Tier 2)
//	OTEL_EXPORTER_OTLP_ENDPOINT   OTLP endpoint for self-traces
//
// Output: structured JSON log lines + (on anomaly) Pub/Sub event published
// via the EventSink adapter (currently a stub that logs; Tier 2 swaps in the
// chora-go-common/pubsub adapter).
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/5007-Capstone/chora/services/chora-observability/internal/adapter/bigquery"
	"github.com/5007-Capstone/chora/services/chora-observability/internal/domain/reconcile"
)

func main() {
	if err := run(context.Background()); err != nil {
		log.Fatalf("reconcile job failed: %v", err)
	}
}

func run(ctx context.Context) error {
	projectID := os.Getenv("GOOGLE_CLOUD_PROJECT")
	if projectID == "" {
		return errors.New("GOOGLE_CLOUD_PROJECT required")
	}

	lookback := getenvInt("RECONCILE_LOOKBACK_DAYS", 1)
	tolerance := getenvFloat("RECONCILE_TOLERANCE_FRACTION", reconcile.DefaultToleranceFraction)

	now := time.Now().UTC()
	windowEnd := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	windowStart := windowEnd.AddDate(0, 0, -lookback)

	log.Printf("reconcile.start project=%s window=[%s, %s) tolerance=%v",
		projectID, windowStart.Format(time.RFC3339), windowEnd.Format(time.RFC3339), tolerance)

	// BigQuery client stays mocked until the cloud-bigquery client lands;
	// the BillingClient now switches between mock + real Vertex via
	// BILLING_CLIENT_MODE per `secrets-and-env`.
	bq := bigquery.NewMockClient(nil)
	bl, err := newBillingClientFromEnv()
	if err != nil {
		return err
	}
	log.Printf("reconcile.billing_client mode=%s type=%T",
		strings.ToLower(strings.TrimSpace(os.Getenv("BILLING_CLIENT_MODE"))), bl)

	// Real Pub/Sub anomaly + degraded publisher (ADR-167 Tier 2). Logging
	// fallback only when CHORA_PUBSUB_PROJECT is unset; fails loud in prod
	// when RECONCILE_REQUIRE_PUBSUB is asserted but the broker is unavailable.
	sinks, sinkShutdown, err := newEventSinksFromEnv(ctx)
	if err != nil {
		return err
	}
	defer sinkShutdown()

	runner := reconcile.NewRunner(reconcile.RunnerConfig{
		ProjectID:         projectID,
		BigQueryClient:    bq,
		BillingClient:     bl,
		EventSink:         sinks.Anomaly,
		DegradedSink:      sinks.Degraded,
		ToleranceFraction: tolerance,
	})

	v, err := runner.Run(ctx, windowStart, windowEnd)
	if err != nil {
		return err
	}

	out, _ := json.Marshal(v)
	log.Printf("reconcile.complete %s", string(out))
	return nil
}

func getenvInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		log.Printf("invalid %s=%q; using default %d", key, v, def)
		return def
	}
	return n
}

func getenvFloat(key string, def float64) float64 {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		log.Printf("invalid %s=%q; using default %v", key, v, def)
		return def
	}
	return f
}
