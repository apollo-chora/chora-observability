// bootstrap.go — production wiring helpers for chora-observability.
//
// Per `feedback_resilience_priority` + `secrets-and-env`: every production
// dependency is sourced from env vars (Terraform / Workload Identity
// Federation in production). Local dev sees nil pools so the server keeps
// the in-memory adapter fallback working out of the box.
//
// Environment contract:
//
//	CHORA_DB_DSN_SECRET_ID  — secret name resolving to a
//	                          chora_observability DSN (app_rw role). The
//	                          producer-side outbox dispatcher REUSES this
//	                          pool via stdlib.OpenDBFromPool — no separate
//	                          CHORA_OUTBOX_DSN env var is needed.
//	CHORA_DB_DSN            — direct DSN (dev override; takes priority).
//	CHORA_DB_PROJECT        — project stamp for the env-backed secret
//	                          resolver's audit/logging path (default
//	                          chora-local).
//	CHORA_DB_REWRITE_FROM_PORT — bypass PgBouncer until the sidecar lands.
//	CHORA_DB_REWRITE_TO_PORT
//	NATS_URL                — NATS server URL for the JetStream event bus
//	                          (mandatory; the server refuses to start
//	                          without it).
//	CHORA_OUTBOX_WORKER_ID  — worker ID stamped onto deadletter rows;
//	                          defaults to HOSTNAME.
//	CHORA_SOURCE_PROJECT    — source_project envelope stamp (default
//	                          chora-local).
package main

import (
	"context"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	cgcdb "github.com/apollo-chora/chora-common/db"
	"github.com/apollo-chora/chora-common/eventbus"
	cgcsecrets "github.com/apollo-chora/chora-common/secrets"
)

func bootstrapDBPool(ctx context.Context) (*pgxpool.Pool, func()) {
	dsn := os.Getenv("CHORA_DB_DSN")
	secretID := os.Getenv("CHORA_DB_DSN_SECRET_ID")
	if dsn == "" && secretID == "" {
		if envEnabled("CHORA_STRICT_STARTUP", false) {
			log.Fatal("observability: strict startup: CHORA_DB_DSN or CHORA_DB_DSN_SECRET_ID is required; refusing in-memory repository fallback")
		}
		log.Printf("observability: CHORA_DB_DSN / CHORA_DB_DSN_SECRET_ID unset — using in-memory repositories")
		return nil, nil
	}

	project := os.Getenv("CHORA_DB_PROJECT")
	if project == "" {
		project = "chora-local"
	}

	var fetcher cgcdb.SecretFetcher
	var sclient *cgcsecrets.Client
	if secretID != "" && dsn == "" {
		c, err := cgcsecrets.NewClient(ctx, project)
		if err != nil {
			log.Fatalf("observability: secret manager init failed (env set, fail-loud): %v", err)
		}
		sclient = c
		fetcher = c
	}

	rewriteFrom, _ := strconv.Atoi(os.Getenv("CHORA_DB_REWRITE_FROM_PORT"))
	rewriteTo, _ := strconv.Atoi(os.Getenv("CHORA_DB_REWRITE_TO_PORT"))

	// Gate-7 fix: env-driven bootstrap context (default 30s). Under
	// concurrent 11-pod cold-start on GKE, Workload Identity → metadata-
	// server → sqladmin → cloudsql-proxy → Cloud SQL listener saturates
	// the metadata-server and Secret Manager fetch alone can exceed 30s.
	// Set CHORA_BOOTSTRAP_TIMEOUT_SECONDS=90 in the deployment env.
	bootstrapSecs, _ := strconv.Atoi(os.Getenv("CHORA_BOOTSTRAP_TIMEOUT_SECONDS"))
	if bootstrapSecs <= 0 {
		bootstrapSecs = 30
	}
	bootstrapCtx, cancel := context.WithTimeout(ctx, time.Duration(bootstrapSecs)*time.Second)
	defer cancel()

	pool, err := cgcdb.Bootstrap(bootstrapCtx, cgcdb.BootstrapOptions{
		DSN:             dsn,
		SecretID:        secretID,
		SecretFetcher:   fetcher,
		RewriteFromPort: rewriteFrom,
		RewriteToPort:   rewriteTo,
		AppName:         serviceName + "@" + version,
		RuntimeParams:   observabilityDBRuntimeParams(),
	})
	if err != nil {
		if sclient != nil {
			_ = sclient.Close()
		}
		log.Fatalf("observability: pgx pool bootstrap failed (env set, fail-loud — kubelet will CrashLoopBackOff): %v", err)
	}

	shutdown := func() {
		pool.Close()
		if sclient != nil {
			_ = sclient.Close()
		}
	}
	return pool, shutdown
}

// bootstrapEventBus wires the NATS JetStream event bus. NATS is mandatory for
// the server executable — there is no in-memory fallback, because the outbox
// dispatcher and every subscriber require a real broker to drain and consume.
func bootstrapEventBus(ctx context.Context) (eventbus.Bus, func()) {
	url := strings.TrimSpace(os.Getenv("NATS_URL"))
	if url == "" {
		log.Fatal("observability: NATS_URL is required; refusing to start without an event bus")
	}
	bus, err := eventbus.NewJetStream(eventbus.JetStreamConfig{URL: url})
	if err != nil {
		log.Fatalf("observability: JetStream init failed: %v", err)
	}
	shutdown := func() {
		_ = bus.Close()
	}
	return bus, shutdown
}

// observabilityDBRuntimeParams returns the per-connection Postgres GUCs that keep a
// DB write from hanging forever (fail-loud). Platform-wide rollout of the
// CHO-2005 fix (2026-07-04); mirrors chora-consumption's
// consumptionDBRuntimeParams. Set on every pooled connection via
// BootstrapOptions.RuntimeParams.
//
//   - lock_timeout=3s: a statement blocked on a row lock ERRORs ("canceling
//     statement due to lock timeout") instead of waiting indefinitely and
//     leaking the request goroutine — under the gateway's 6s per-call
//     timeout, so this service fails loud (500) before the gateway 504s.
//   - idle_in_transaction_session_timeout=60s: reaps a leaked open
//     transaction so its row locks release.
//
// No statement_timeout (owner steer): long read paths must not be capped.
func observabilityDBRuntimeParams() map[string]string {
	return map[string]string{
		"lock_timeout":                        "3s",
		"idle_in_transaction_session_timeout": "60s",
	}
}
