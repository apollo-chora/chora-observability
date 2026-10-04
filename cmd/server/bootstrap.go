// bootstrap.go — production wiring helpers for chora-observability.
//
// Per `feedback_resilience_priority` + `secrets-and-env`: every production
// dependency is sourced from env vars (Terraform / Workload Identity
// Federation in production). Local dev sees nil pools / nil pubsub clients
// so the server keeps the in-memory adapter fallback working out of the box.
//
// Environment contract:
//
//	CHORA_DB_DSN_SECRET_ID  — Secret Manager secret name resolving to a
//	                          chora_observability DSN (app_rw role). The
//	                          producer-side outbox dispatcher REUSES this
//	                          pool via stdlib.OpenDBFromPool — no separate
//	                          CHORA_OUTBOX_DSN env var is needed.
//	CHORA_DB_DSN            — direct DSN (dev override; takes priority).
//	CHORA_DB_PROJECT        — GCP project for Secret Manager.
//	CHORA_DB_REWRITE_FROM_PORT — bypass PgBouncer until the sidecar lands.
//	CHORA_DB_REWRITE_TO_PORT
//	CHORA_PUBSUB_PROJECT    — GCP project hosting Pub/Sub topics.
//	CHORA_OUTBOX_WORKER_ID  — worker ID stamped onto deadletter rows;
//	                          defaults to HOSTNAME.
//	CHORA_SOURCE_PROJECT    — source_project envelope stamp (default
//	                          chora-489812).
package main

import (
	"context"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	cgcdb "github.com/5007-Capstone/chora/libs/chora-go-common/db"
	cgcpubsub "github.com/5007-Capstone/chora/libs/chora-go-common/pubsub"
	cgcsecrets "github.com/5007-Capstone/chora/libs/chora-go-common/secrets"
)

func bootstrapDBPool(ctx context.Context) (*pgxpool.Pool, func()) {
	dsn := os.Getenv("CHORA_DB_DSN")
	secretID := os.Getenv("CHORA_DB_DSN_SECRET_ID")
	if dsn == "" && secretID == "" {
		log.Printf("observability: CHORA_DB_DSN / CHORA_DB_DSN_SECRET_ID unset — using in-memory repositories")
		return nil, nil
	}

	project := os.Getenv("CHORA_DB_PROJECT")
	if project == "" {
		project = "chora-489812"
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

func bootstrapPubSubClient(ctx context.Context) (cgcpubsub.CloudPubSubClient, func()) {
	project := os.Getenv("CHORA_PUBSUB_PROJECT")
	if project == "" {
		return nil, nil
	}
	if emulator := os.Getenv("PUBSUB_EMULATOR_HOST"); emulator != "" {
		log.Printf("observability: Pub/Sub emulator configured (host=%s project=%s)", emulator, project)
	}
	cli, err := cgcpubsub.NewGCPClient(ctx, project)
	if err != nil {
		log.Printf("observability: pubsub client init failed: %v — falling back to in-memory recorder", err)
		return nil, nil
	}
	shutdown := func() {
		_ = cli.Close()
	}
	return cli, shutdown
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
