// Package main is the chora-observability service entrypoint.
//
// Service: chora-observability (Observability supporting domain, surface O+)
// Project: chora-489812 (Team 3 / Platform)
// Domain DB: chora_observability (Cloud SQL — provisioned at M10)
// Topic prefix: chora.observability.* (canonical token_usage.recorded.v1)
//
// W2c (M12.3 Wave 2, 2026-05-12): wires the D6.2 producer-side
// transactional outbox + dispatcher per
// `.claude/skills/agentic-resilience-d6/SKILL.md` Pillar 2. The legacy
// inmem.OutboxRecorder is retired; the canonical
// outbox.Publisher + outbox.Dispatcher replace it. The analytics
// subscriber now uses an idempotent.Store (memory in dev, Postgres in
// prod when the DB pool is wired) for the consumer-side dual.
//
// Recursion warning: this service emits OTLP traces for ITS OWN HTTP
// requests AND records OTLP usage from other services (meta-level
// observability). Self-emitted traces do NOT generate self-referencing
// TokenUsageLedger entries — see internal/observability/otlp.go for
// the full reasoning.
package main

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"google.golang.org/grpc"
	healthgrpc "google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	observabilityv1 "github.com/locoroco-git/Chora-LMS/chora-contracts/gen/go/chora/services/observability/v1"

	"github.com/5007-Capstone/chora/libs/chora-go-common/durabilityguard"
	"github.com/5007-Capstone/chora/libs/chora-go-common/idempotent"
	cgcpubsub "github.com/5007-Capstone/chora/libs/chora-go-common/pubsub"
	bq "github.com/5007-Capstone/chora/services/chora-observability/internal/adapter/bigquery"
	"github.com/5007-Capstone/chora/services/chora-observability/internal/adapter/cloudtrace"
	"github.com/5007-Capstone/chora/services/chora-observability/internal/adapter/events"
	grpcadapter "github.com/5007-Capstone/chora/services/chora-observability/internal/adapter/grpc"
	httpadapter "github.com/5007-Capstone/chora/services/chora-observability/internal/adapter/http"
	"github.com/5007-Capstone/chora/services/chora-observability/internal/adapter/inmem"
	obsoutbox "github.com/5007-Capstone/chora/services/chora-observability/internal/adapter/outbox"
	"github.com/5007-Capstone/chora/services/chora-observability/internal/adapter/pg"
	"github.com/5007-Capstone/chora/services/chora-observability/internal/adapter/subscribers"
	analyticsinmem "github.com/5007-Capstone/chora/services/chora-observability/internal/analytics/inmem"
	"github.com/5007-Capstone/chora/services/chora-observability/internal/domain/agents"
	"github.com/5007-Capstone/chora/services/chora-observability/internal/domain/companionsuspension"
	"github.com/5007-Capstone/chora/services/chora-observability/internal/domain/decision"
	"github.com/5007-Capstone/chora/services/chora-observability/internal/domain/externalegress"
	fg "github.com/5007-Capstone/chora/services/chora-observability/internal/domain/familiargrowth"
	"github.com/5007-Capstone/chora/services/chora-observability/internal/domain/ledger"
	"github.com/5007-Capstone/chora/services/chora-observability/internal/domain/ritualaudit"
	"github.com/5007-Capstone/chora/services/chora-observability/internal/observability"
)

// cloudtraceAdapter shims the cloudtrace.Client to the httpadapter.TraceExporter
// interface — the http adapter doesn't import the cloudtrace pkg directly to
// avoid a hard dependency on that adapter from the http layer.
type cloudtraceAdapter struct{ client *cloudtrace.Client }

func (a *cloudtraceAdapter) Export(ctx context.Context, req httpadapter.TraceExportRequest) (httpadapter.TraceExportResponse, error) {
	res, err := a.client.Export(ctx, cloudtrace.ExportRequest{
		TenantID: req.TenantID,
		TraceID:  req.TraceID,
		Since:    req.Since,
		Until:    req.Until,
	})
	if err != nil {
		return httpadapter.TraceExportResponse{}, err
	}
	return httpadapter.TraceExportResponse{
		ExportID: res.ExportID,
		Status:   res.Status,
		Endpoint: res.Endpoint,
		Mock:     res.Mock,
		Since:    res.Since,
		Until:    res.Until,
		QueuedAt: res.QueuedAt,
	}, nil
}

const (
	serviceName = "chora-observability"
	version     = "0.1.0"
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// OTLP wiring per Tier 3 D13 — direct to Cloud Trace in prod.
	//
	// HHH-2 paydown (2026-05-14): the bespoke internal/observability
	// adapter previously hand-rolled the OTLP TracerProvider and rejected
	// the https://telemetry.googleapis.com:443 endpoint scheme — LL's
	// Wave B audit (commit 533cfbda) flagged chora-observability as the
	// 4th Cloud Trace "dark" service. Migrated to the canonical
	// commonobs.InitOTLPAsync via internal/observability.InitAsync. The
	// async handle decouples OTLP init from pgx pool init so a slow
	// Cloud Trace TLS handshake can no longer swallow the bootstrap
	// budget under PgBouncer 4-container cold-start. Mirrors chora-
	// sharing (commit 3340c7c3) + chora-tenancy (commit c3205431).
	otlpHandle := observability.InitAsync(ctx)
	defer func() {
		// WaitContext blocks until init settles — usually a no-op by
		// shutdown time because pgx-pool init below already gave OTLP
		// best-effort wall-clock to land.
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		res := otlpHandle.WaitContext(shutdownCtx)
		if err := res.Shutdown(shutdownCtx); err != nil {
			log.Printf("trace shutdown error: %v", err)
		}
	}()

	// ----------------------------------------------------------------------
	// Repository wiring.
	// ----------------------------------------------------------------------
	var ledgerRepo ledger.Repository = inmem.NewLedgerRepository()
	pool, poolShutdown := bootstrapDBPool(ctx)
	if poolShutdown != nil {
		defer poolShutdown()
	}
	if pool != nil {
		ledgerRepo = pg.NewLedgerRepository(pg.NewPgxPoolQuerier(pool))
		log.Printf("observability: pgx LedgerRepository wired (pool=chora_observability)")
	}

	pubsubClient, pubsubShutdown := bootstrapPubSubClient(ctx)
	if pubsubShutdown != nil {
		defer pubsubShutdown()
	}

	// Pub/Sub bus selection: Cloud client when wired, in-memory bus
	// otherwise (dev / tests). Both satisfy obsoutbox.Bus.
	var bus obsoutbox.Bus
	if pubsubClient != nil {
		bus = cgcpubsub.NewCloudPublisher(pubsubClient)
		log.Printf("observability: Cloud Pub/Sub client wired (project=%s)", os.Getenv("CHORA_PUBSUB_PROJECT"))
	} else {
		bus = cgcpubsub.NewInMemoryBus()
		log.Printf("observability: in-memory Pub/Sub bus wired (CHORA_PUBSUB_PROJECT unset)")
	}

	// Federated closure-saga subscriber (CHO-1719 / Tier 3 D11): consumes
	// chora.observability.pii.pseudonymise.requested.v1, applies the
	// per-domain PII_Closure_Map.yaml duty, and acks on
	// chora.observability.account.pseudonymised.v1.
	//
	// Repo seam (CHO-2198, W0-F1 durability + W0-F5 error-honesty): pg on a
	// healthy pool (durable ack/dedup — migration 0017,
	// closure_pseudonymisation_state), in-memory ONLY when the pool is
	// absent, mirroring the chora-payments else-branch shape. Before this
	// fix the repo was UNGATED — gated on pubsubClient only, never on pool
	// health — so the ack/dedup state was lost on every pod restart even
	// with a healthy chora_observability pool (see
	// docs/references/w0-f1-inmemory-inventory.md §6 item 4). Real
	// per-table pg tokenisation (actually redacting token_usage_ledger /
	// agent_decision_log / ... columns) remains separate, deeper M12+
	// debt — this fix is durability of the ack/dedup SIGNAL only, not the
	// redaction itself. Pull subscription is provisioned by infra (closure
	// deploy runbook); override the name via env.
	//
	// closureRepo is hoisted to function scope so the ADR-236 D5 durability
	// guard (below, before the HTTP handlers are built) can classify it
	// alongside the other repos. It stays nil when pubsubClient is absent (or
	// the PII map fails to load) — the guard reports nil as UNKNOWN, never a
	// violation. Mirrors the chora-tenancy / chora-a2a-gateway D5 closureRepo
	// hoist.
	var closureRepo events.ClosureRepository
	if pubsubClient != nil {
		piiPath := os.Getenv("CHORA_PII_CLOSURE_MAP_PATH")
		if piiPath == "" {
			piiPath = "config/PII_Closure_Map.yaml"
		}
		closureAckPub := cgcpubsub.NewClosureAckPublisher(
			cgcpubsub.NewCloudPublisher(pubsubClient),
			os.Getenv("CHORA_PUBSUB_PROJECT"),
			"chora-observability",
		)
		if pool != nil {
			closureRepo = pg.NewClosureRepository(pg.NewPgxPoolQuerier(pool))
			log.Printf("observability: pg ClosureRepository wired (table=closure_pseudonymisation_state)")
		} else {
			closureRepo = events.NewInMemoryClosureRepo()
			log.Printf("observability: CHORA_DB_DSN unset — closure repo uses in-memory store (NOT durable across restart)")
		}
		if closureSub, err := events.BootstrapClosureSubscriber(piiPath, closureRepo, closureAckPub, nil); err != nil {
			log.Printf("observability: closure subscriber DISABLED (PII map load: %v)", err)
		} else {
			closureSubName := os.Getenv("CHORA_CLOSURE_SUBSCRIPTION")
			if closureSubName == "" {
				closureSubName = "chora-observability.closure-pseudonymise"
			}
			go func() {
				log.Printf("observability: closure subscriber binding %s -> %s", closureSubName, events.TopicPseudonymiseRequested)
				if err := cgcpubsub.NewCloudSubscriber(pubsubClient).Subscribe(ctx, closureSubName, events.ClosurePullHandler(closureSub)); err != nil && !errors.Is(err, context.Canceled) {
					log.Printf("observability: closure subscriber exited: %v", err)
				}
			}()
		}
	}

	// ----------------------------------------------------------------------
	// D6.2 producer-side outbox wiring (M12.3 Wave 2, w2c).
	//
	// Publisher writes to outbox_events; Dispatcher drains to the Pub/Sub
	// bus on a background goroutine. The PostgresStore reuses the same
	// pgxpool.Pool that backs the LedgerRepository + Inbox (no duplicate
	// connection pool, no separate DSN env). When the pool is unwired we
	// fall back to the InMemoryStore so the service stays runnable in dev.
	// ----------------------------------------------------------------------
	outboxStore := bootstrapOutboxStore(pool)

	outboxPublisher := obsoutbox.NewPublisher(obsoutbox.PublisherConfig{
		Store:         outboxStore,
		SourceProject: envOrDefault("CHORA_SOURCE_PROJECT", "chora-489812"),
		SourceService: serviceName,
	})

	// ADR-167 Plane-4 fail-loud: the governance sink-failure AlertSink rides
	// the SAME Pub/Sub bus the dispatcher drains to (reuse, don't reinvent —
	// no second client). A dead-letter (malformed envelope OR publish-retries
	// exhausted) emits chora.observability.sink_failure.recorded.v1 so the
	// O+ / chora-governance surface sees the dropped event explicitly.
	sinkAlert := obsoutbox.NewPublisherAlertSink(obsoutbox.PublisherAlertSinkConfig{
		Bus:           bus,
		SourceProject: envOrDefault("CHORA_SOURCE_PROJECT", "chora-489812"),
		SourceService: serviceName,
	})

	dispatcher := obsoutbox.NewDispatcher(obsoutbox.DispatcherConfig{
		Store:        outboxStore,
		Bus:          bus,
		WorkerID:     outboxWorkerID(),
		MaxAttempts:  5,
		PollInterval: 250 * time.Millisecond,
		AlertSink:    sinkAlert,
	})
	go func() {
		if err := dispatcher.Run(ctx, 100); err != nil && err != context.Canceled && err != context.DeadlineExceeded {
			log.Printf("observability: outbox dispatcher exited: %v", err)
		}
	}()

	// ----------------------------------------------------------------------
	// Inbox factory — used by Pub/Sub subscribers (analytics + closure +
	// future). MemoryStore in dev; PostgresStore when the DB pool is wired
	// (against chora_observability.idempotency_keys per migration 0005).
	// ----------------------------------------------------------------------
	inbox := bootstrapInbox(pool)

	// ----------------------------------------------------------------------
	// Gate #7 TokenUsageLedger consumer (WIRE1 — 2026-05-17).
	//
	// Binds the existing events.TokenUsageConsumer to a Pub/Sub StreamingPull
	// goroutine on the canonical subscription
	// `chora-observability.observability-token_usage-recorded` (provisioned
	// in chora-infra/terraform/environments/dev/main.tf §1538). The goroutine
	// exits cleanly when ctx is canceled (graceful shutdown).
	//
	// Producer: chora-ai-kernel-orchestrator's TokenUsageLedgerOutboxWriter
	// (qgen 2-agent crew emits one row per generate / critique trace hop;
	// runner._emit_token_usage_ledger drives it; outbox dispatcher publishes
	// to Pub/Sub).
	// ----------------------------------------------------------------------
	tokenUsageConsumer := events.NewTokenUsageConsumer(events.TokenUsageConsumerConfig{
		Repo:  ledgerRepo,
		Inbox: inbox,
	})
	tokenUsageSubscription := envOrDefault(
		"CHORA_TOKEN_USAGE_SUBSCRIPTION", DefaultTokenUsageSubscription,
	)
	// ADR-167 Plane-4: wrap the decode+handle path so a malformed inbound
	// event is dead-lettered locally + alerted + error-logged (and still
	// Nacked to the broker DLQ).
	tokenUsageHandler := withQuarantine(
		buildTokenUsageHandler(tokenUsageConsumer),
		quarantineDeps{
			ConsumerName: "token_usage",
			Topic:        events.TopicTokenUsageRecorded,
			Store:        outboxStore,
			Alert:        sinkAlert,
		},
	)
	tokenUsageDone := startTokenUsageSubscriber(
		ctx, pubsubClient, tokenUsageSubscription, tokenUsageHandler,
	)
	if tokenUsageDone == nil {
		log.Printf(
			"observability: token_usage subscriber NOT wired (Pub/Sub client unwired — dev path)",
		)
	}

	// ----------------------------------------------------------------------
	// LedgerHook wiring — replaces the legacy inmem.OutboxRecorder with the
	// canonical outbox.Publisher (drop-in for ledger.OutboxRecorder port).
	// ----------------------------------------------------------------------
	ledgerHook := ledger.NewLedgerHook(ledger.LedgerHookConfig{
		Ledger:        ledgerRepo,
		Outbox:        outboxPublisher,
		SourceProject: envOrDefault("CHORA_SOURCE_PROJECT", "chora-489812"),
		SourceService: serviceName,
	})

	var decisionRepo decision.Repository = inmem.NewDecisionRepository()
	if pool != nil {
		decisionRepo = pg.NewDecisionRepository(pg.NewPgxPoolQuerier(pool))
		log.Printf("observability: pgx DecisionRepository wired (pool=chora_observability)")
	}

	// ----------------------------------------------------------------------
	// AgentDecisionLog consumer (ADR-167 read-model hydration, 2026-05-29).
	//
	// Binds events.AgentDecisionConsumer to a Pub/Sub StreamingPull goroutine
	// on the canonical subscription
	// `chora-observability.observability-agent_decision-logged` (v2 protobuf
	// topic). Projects every routing/generate/critique/guardrail decision
	// into agent_decision_log (RLS-scoped via DecisionRepository.WithTenantTx)
	// for the O+ auditor view + chora-governance IMDA D1/D2 evidence.
	//
	// Producer: AI Kernel orchestrator + content-agent crews (qgen_question /
	// qgen_critic emit one decision per generate / critique hop).
	// ----------------------------------------------------------------------
	// BigQuery mirror sink (best-effort) — streams each persisted decision into
	// chora_observability_analytics.agent_decision_log so the BQ mirror is no
	// longer empty (closing the "View in BigQuery" / auditor-query gap). Disabled
	// (nil) when no BQ project resolves or the client fails to init; Postgres is
	// the canonical store and is unaffected either way. Env config mirrors the
	// eval-evidence client.
	var decisionBQSink events.DecisionBQSink
	{
		bqProject := envOrDefault("CHORA_DECISION_BQ_PROJECT",
			envOrDefault("CHORA_PROJECT", envOrDefault("GOOGLE_CLOUD_PROJECT", "chora-489812")))
		bqDataset := envOrDefault("CHORA_DECISION_BQ_DATASET", "chora_observability_analytics")
		bqTable := envOrDefault("CHORA_DECISION_BQ_TABLE", "agent_decision_log")
		bqLocation := envOrDefault("CHORA_DECISION_BQ_LOCATION", "asia-southeast1")
		if sink, err := bq.NewDecisionSink(ctx, bqProject, bqDataset, bqTable, bqLocation); err != nil {
			log.Printf("observability: decision BQ mirror disabled (%v) — Postgres unaffected", err)
		} else {
			decisionBQSink = sink
			defer func() { _ = sink.Close() }()
			log.Printf("observability: decision BQ mirror enabled (%s.%s.%s)", bqProject, bqDataset, bqTable)
		}
	}
	agentDecisionConsumer := events.NewAgentDecisionConsumer(events.AgentDecisionConsumerConfig{
		Repo:   decisionRepo,
		Inbox:  inbox,
		BQSink: decisionBQSink,
	})
	agentDecisionSubscription := envOrDefault(
		"CHORA_AGENT_DECISION_SUBSCRIPTION", DefaultAgentDecisionSubscription,
	)
	// CHO-1560: load the pricing config so the binding can derive per-decision
	// cost_usd_micros from token counts (proto carries no cost field). Path is
	// env-driven (no inline config); absence is non-fatal — cost stays blank
	// rather than crashing the consumer.
	var agentDecisionCost *decisionCostCalculator
	if pricingPath := envOrDefault("CHORA_PRICING_CONFIG_PATH", "config/pricing.yaml"); pricingPath != "" {
		if calc, err := newDecisionCostCalculatorFromFile(pricingPath); err != nil {
			log.Printf("observability: pricing config not loaded (%s): %v — agent_decision cost_usd will be blank", pricingPath, err)
		} else {
			agentDecisionCost = calc
			log.Printf("observability: pricing config loaded (%s) — agent_decision cost_usd projection enabled", pricingPath)
		}
	}
	// ADR-167 Plane-4: same quarantine-wrap as the token_usage subscriber.
	agentDecisionHandler := withQuarantine(
		buildAgentDecisionHandlerWithCost(agentDecisionConsumer, agentDecisionCost),
		quarantineDeps{
			ConsumerName: "agent_decision",
			Topic:        events.TopicAgentDecisionLogged,
			Store:        outboxStore,
			Alert:        sinkAlert,
		},
	)
	agentDecisionDone := startAgentDecisionSubscriber(
		ctx, pubsubClient, agentDecisionSubscription, agentDecisionHandler,
	)
	if agentDecisionDone == nil {
		log.Printf(
			"observability: agent_decision subscriber NOT wired (Pub/Sub client unwired — dev path)",
		)
	}

	correlationRepo := inmem.NewCorrelationRepository()
	budgetRepo := inmem.NewBudgetRepository()
	budgetLookup := inmem.NewBudgetLookup()

	// Analytics composite store (subsumed from chora-analytics at M12.2.E.4).
	// In-memory by default; M12.3 migrates to a chora_observability
	// materialised view (cross-DB queries forbidden — events only per
	// ddd-enforcement).
	//
	// ADR-167 Plane-4 DEFERRED (2026-06-01): analytics has NO repository PORT —
	// unlike the ledger/decision/familiar-growth repos.
	//
	// ⚠ TRAP for whoever wires analytics-pg (verified live 2026-07-17, CHO-2140):
	// migration 0004_analytics.sql is RECORDED APPLIED (2026-05-12) but its
	// tables (analytics_event_buckets / _event_seen / _cohorts / _cohort_members)
	// DO NOT EXIST in the live chora_observability DB — checked across all
	// schemas. This comment used to claim they exist; they do not. Worse, 0004
	// still defines their policies on the RETIRED `app.current_tenant_id` GUC,
	// which 0015_rls_guc_rekey did not touch (0015's own header wrongly claims
	// analytics "was already modern"). So whoever CREATES those tables from 0004
	// inherits policies keyed on a GUC no code sets — every insert would 42501 on
	// the first real write, exactly as familiar-growth did for weeks. Re-key 0004
	// to `chora.tenant_id` BEFORE creating the tables. rls_guc_coherence_test.go
	// guards the seam, but only for tables a repository actually writes — it
	// cannot see a table nobody has built yet.
	//
	// The HTTP handlers (internal/adapter/http/analytics_handler.go)
	// consume the CONCRETE *analyticsinmem.Store + its concrete
	// eventaggregator.Aggregator + cohort.Registry directly (e.g.
	// store.Aggregator.Query(...), store.Cohorts.Save(...)), and the
	// aggregator is an in-process event-FOLD, not a CRUD repo. A Postgres
	// switch here requires (1) defining aggregator + cohort ports, (2)
	// refactoring the 5 analytics handlers + handler.go onto those ports, and
	// (3) building a SQL materialised-view/upsert projection for the bucket
	// fold — a sizeable refactor that does NOT "mirror the ledger/decision
	// wiring". Wired the familiar-growth pg adapter (clean port + migration
	// 0007) above; analytics-pg is left for a dedicated follow-up so the live
	// analytics endpoints stay stable. See task report for the rationale.
	analyticsStore := analyticsinmem.NewStore()

	// PROD-H (m14.iter5g): ADR-149 Familiar Growth audit repository. Wires the
	// 4 O+ IMDA D1/D2 dashboard endpoints at /v1/audit/familiar-growth/* + the
	// Pub/Sub-push subscriber's projection target.
	//
	// ADR-167 Plane-4 wiring (2026-06-01): the pgx-backed
	// pg.FamiliarGrowthRepository is wired when the chora_observability pool
	// is up (mirrors the ledger/decision repos). The in-memory repository is
	// retained ONLY for the no-DSN dev path.
	//
	// CHO-2140 completion (2026-07-17): these 4 tables were born (0007) keyed on
	// the legacy `app.current_tenant_id` GUC, and this line used to LOG that name.
	// 0015_rls_guc_rekey re-keyed them to the canonical `chora.tenant_id`; the
	// seam now matches (WithTenantTx) and the log names the GUC the binary
	// actually sets. A boot log asserting a GUC name is a CHECKABLE CLAIM — an
	// operator greps exactly this line to check the seam, and the container is
	// distroless so the binary cannot be inspected — a stale name here makes a
	// fixed seam look broken forever. rls_guc_coherence_test.go now fails if any
	// string literal names a retired GUC.
	var familiarGrowthRepo fg.Repository = inmem.NewFamiliarGrowthRepository()
	if pool != nil {
		familiarGrowthRepo = pg.NewFamiliarGrowthRepository(pg.NewPgxPoolQuerier(pool))
		log.Printf("observability: pgx FamiliarGrowthRepository wired (pool=chora_observability, RLS guc=chora.tenant_id)")
	} else {
		log.Printf("observability: familiar growth audit repo wired (in-memory; no DB pool)")
	}

	// ADR-149 Familiar Growth audit — the subscriber projects 7 source topics
	// into 4 audit tables and emits IMDA D1/D2 evidence to the chora-governance
	// projector.
	//
	// Inbox: reuses the same idempotent.Store the analytics subscriber uses
	// (Postgres-backed in prod via bootstrapInbox, MemoryStore in dev).
	//
	// CHO-2257 — evidence durability: this used to wire
	// subscribers.NewInMemoryEvidencePublisher() on the PROD path, so every IMDA
	// D1/D2 evidence emit went to a process-local buffer and evaporated on
	// restart. The durable outbox-backed publisher is now the prod sink, and the
	// in-memory publisher REFUSES to wire when a DB pool is up.
	evidencePublisher, evErr := selectEvidencePublisher(outboxStore, pool != nil, evidencePublisherConfig{
		SourceProject: envOrDefault("CHORA_SOURCE_PROJECT", "chora-489812"),
		SourceService: serviceName,
	})
	if evErr != nil {
		log.Fatalf("observability: FATAL familiar_growth_audit evidence sink refused to wire: %v", evErr)
	}
	if pool != nil {
		log.Printf("observability: familiar_growth_audit evidence wired DURABLE (outbox -> %s)", subscribers.IMDAEvidenceTopic)
	} else {
		log.Printf("observability: familiar_growth_audit evidence wired in-memory (no DB pool; dev path — NOT durable)")
	}

	familiarGrowthAuditSub := subscribers.New(subscribers.Config{
		Repo:     familiarGrowthRepo,
		Evidence: evidencePublisher,
		Inbox:    inbox,
	})

	// CHO-2257 — PULL/StreamingPull ingress. This lane previously declared an
	// HTTP PUSH handler while all 7 source subscriptions were PULL-shaped, so
	// nothing ever called it and ~492 events sat undelivered. The service now
	// subscribes to its OWN subscriptions, mirroring the token_usage /
	// agent_decision / ritual_run_audit consumers.
	//
	// startFamiliarGrowthAuditSubscribers validates the binding table against
	// the subscriber's SubscribedTopics() contract BEFORE starting anything — an
	// ingress that does not match its subscription is fatal here rather than
	// silently inert (the structural guard; see familiar_growth_audit_binding.go).
	//
	// ADR-167 Plane-4: the same quarantine wrap as the sibling consumers — a
	// malformed inbound event is dead-lettered locally + alerted + error-logged,
	// and still Nacked to the broker DLQ.
	familiarGrowthDone, fgErr := startFamiliarGrowthAuditSubscribers(
		ctx, pubsubClient, familiarGrowthAuditSub, familiarGrowthAuditBindings,
		func(inner cgcpubsub.Handler, consumerName, topic string) cgcpubsub.Handler {
			return withQuarantine(inner, quarantineDeps{
				ConsumerName: consumerName,
				Topic:        topic,
				Store:        outboxStore,
				Alert:        sinkAlert,
			})
		},
	)
	if fgErr != nil {
		log.Fatalf("observability: FATAL familiar_growth_audit ingress is invalid: %v", fgErr)
	}
	if len(familiarGrowthDone) == 0 {
		log.Printf("observability: familiar_growth_audit subscribers NOT wired (Pub/Sub client unwired — dev path)")
	} else {
		log.Printf("observability: familiar_growth_audit wired: %d StreamingPull consumers (PULL, not push)", len(familiarGrowthDone))
	}

	// ----------------------------------------------------------------------
	// Grimoire Ritual run audit (O+ auditor projection, ADR-215 / ADR-219
	// CHO-2016). chora-consumption publishes ritual_run_completed.v1 (JSON
	// payload) when a learner-composed Ritual run reaches a terminal state;
	// this PULL/StreamingPull consumer projects each run into ritual_run_audit
	// so O+ auditors see the run + its per-step decision stamps. NOT a gateway
	// push — the service subscribes to its OWN subscription (mirrors the
	// token_usage / agent_decision consumers above).
	//
	// The pgx-backed repo is wired when the chora_observability pool is up
	// (RLS guc=chora.tenant_id via WithTenantTx, migration 0013 as re-keyed by
	// 0015); the in-memory repo is retained only for the no-DSN dev path.
	//
	// CHO-2140 completion (2026-07-17): this said `app.current_tenant_id` until
	// the seam was corrected — see the familiar-growth block above on why a boot
	// log naming a GUC is a checkable claim, not decoration.
	// ----------------------------------------------------------------------
	var ritualAuditRepo ritualaudit.Repository = inmem.NewRitualAuditRepository()
	if pool != nil {
		ritualAuditRepo = pg.NewRitualAuditRepository(pg.NewPgxPoolQuerier(pool))
		log.Printf("observability: pgx RitualAuditRepository wired (pool=chora_observability, RLS guc=chora.tenant_id)")
	} else {
		log.Printf("observability: ritual run audit repo wired (in-memory; no DB pool)")
	}

	ritualAuditConsumer := events.NewRitualRunAuditConsumer(events.RitualRunAuditConsumerConfig{
		Repo:  ritualAuditRepo,
		Inbox: inbox,
	})
	ritualAuditSubscription := envOrDefault(
		"CHORA_RITUAL_AUDIT_SUBSCRIPTION", DefaultRitualAuditSubscription,
	)
	// ADR-167 Plane-4: same quarantine-wrap as the token_usage / agent_decision
	// subscribers — a malformed inbound event is dead-lettered locally +
	// alerted + error-logged (and still Nacked to the broker DLQ).
	ritualAuditHandler := withQuarantine(
		events.RitualRunAuditPullHandler(ritualAuditConsumer),
		quarantineDeps{
			ConsumerName: "ritual_run_audit",
			Topic:        ritualaudit.TopicFamiliarRitualRunCompleted,
			Store:        outboxStore,
			Alert:        sinkAlert,
		},
	)
	ritualAuditDone := startRitualRunAuditSubscriber(
		ctx, pubsubClient, ritualAuditSubscription, ritualAuditHandler,
	)
	if ritualAuditDone == nil {
		log.Printf("observability: ritual_run_audit subscriber NOT wired (Pub/Sub client unwired — dev path)")
	}

	// ----------------------------------------------------------------------
	// CHO-2148 — external web-egress entitlement projection + kill-switch.
	//
	// chora-tenancy OWNS the entitlement; the model-gateway ENFORCES it on every
	// grounded call by reading chora_observability.external_egress_policy. Those
	// are two different databases and cross-DB writes are forbidden, so this
	// subscriber is the ONLY bridge between them. There is no in-memory
	// fallback: an unwired projection would silently drop every egress change
	// and leave the gateway serving a stale entitlement with nothing to alert
	// on. With no pool, we log LOUDLY and the routes 503.
	//
	// RLS note: external_egress_policy is keyed on `chora.tenant_id` (migration
	// 0014 — 0015's re-key did NOT touch it because it was born correct), so the
	// repo uses WithTenantTx. Every repo in this service now does: CHO-2140
	// deleted the WithAppTenantTx variant, since no live policy read the GUC it
	// set. This repo was one of the CONTROLS that proved that diagnosis — it kept
	// writing normally (rows on 2026-07-14, after the re-key) while the
	// familiar-growth tables could not write at all.
	// ----------------------------------------------------------------------
	var egressKillSwitchRepo externalegress.KillSwitchRepository
	// ADR-252 via ADR-254 D7: the Learning Companion containment write path
	// (two tables, migration 0018) + its governance audit event through this
	// service's own outbox (same table the dispatcher below drains). pg-or-nil:
	// with no pool the route 503s, never a silent "nobody is suspended".
	var companionSuspensionRepo companionsuspension.Repository
	if pool != nil {
		egressQuerier := pg.NewPgxPoolQuerier(pool)
		egressProjectionRepo := pg.NewExternalEgressRepo(egressQuerier)
		egressKillSwitchRepo = pg.NewKillSwitchRepo(egressQuerier)
		companionSuspensionRepo = pg.NewCompanionSuspensionRepo(egressQuerier, pg.CompanionSuspensionRepoOptions{})
		log.Printf("observability: companion containment write path wired (ADR-252; routes /api/v1/admin/companion/suspension)")

		egressConsumer := events.NewExternalEgressPolicyConsumer(events.ExternalEgressPolicyConsumerConfig{
			Repo:  egressProjectionRepo,
			Inbox: inbox,
		})
		egressSubscription := envOrDefault(
			"CHORA_EXTERNAL_EGRESS_SUBSCRIPTION", DefaultExternalEgressSubscription,
		)
		egressHandler := withQuarantine(
			buildExternalEgressPolicyHandler(egressConsumer),
			quarantineDeps{
				ConsumerName: "external_egress_policy",
				Topic:        events.TopicExternalEgressPolicyUpdated,
				Store:        outboxStore,
				Alert:        sinkAlert,
			},
		)
		egressDone := startExternalEgressPolicySubscriber(
			ctx, pubsubClient, egressSubscription, egressHandler,
		)
		if egressDone == nil {
			log.Printf("observability: external_egress projection subscriber NOT wired " +
				"(Pub/Sub client unwired — dev path); tenant egress changes will NOT reach the gateway")
		}
		log.Printf("observability: external_egress projection + kill-switch repos wired (CHO-2148)")
	} else {
		log.Printf("observability: external_egress projection + kill-switch DISABLED (no pgx pool) — " +
			"kill-switch route will 503 and tenant egress changes will NOT be projected")
	}

	// ADR-236 D5 — report-only runtime durability guard over the composition
	// root (W0-F1 gate, CHO-2198). Classifies each wired repository by SHAPE
	// (holds a live *pgxpool.Pool ⇒ DURABLE; a data map ⇒ IN_MEMORY) and logs a
	// structured, greppable report at boot. Report-only unless
	// CHORA_DURABILITY_GUARD=enforce AND the binding is allow-listed — the nil
	// allow-list matches the chora-payments / chora-identity wirings, so the
	// always-in-memory sites (correlation, budgets, budget_lookup, analytics)
	// surface as VIOLATION rather than being silently accepted. ledger,
	// decision, familiar_growth, ritual_audit are pg-or-inmem (durable on a
	// healthy pool); egress_kill_switch + closure are pg-or-nil/absent and
	// report DURABLE when the pool is up, UNKNOWN when it is absent. analytics
	// is the ADR-167 Plane-4 DEFERRED in-memory site (no repo port yet) and
	// reports IN_MEMORY honestly.
	durabilityguard.Guard("chora-observability", []durabilityguard.Binding{
		{Port: "ledger", Adapter: ledgerRepo},
		{Port: "decision", Adapter: decisionRepo},
		{Port: "correlation", Adapter: correlationRepo},
		{Port: "budgets", Adapter: budgetRepo},
		{Port: "budget_lookup", Adapter: budgetLookup},
		{Port: "analytics", Adapter: analyticsStore},
		{Port: "familiar_growth", Adapter: familiarGrowthRepo},
		{Port: "ritual_audit", Adapter: ritualAuditRepo},
		{Port: "egress_kill_switch", Adapter: egressKillSwitchRepo},
		{Port: "companion_suspension", Adapter: companionSuspensionRepo},
		{Port: "closure", Adapter: closureRepo},
	}, nil)

	opts := []httpadapter.Option{
		httpadapter.WithEgressKillSwitchRepo(egressKillSwitchRepo),
		httpadapter.WithCompanionSuspensionRepo(companionSuspensionRepo),
		httpadapter.WithBudgetRepo(budgetRepo),
		httpadapter.WithBudgetLookup(budgetLookup),
		httpadapter.WithAnalyticsStore(analyticsStore),
		httpadapter.WithLedgerHook(ledgerHook),
		httpadapter.WithFamiliarGrowthRepo(familiarGrowthRepo),
		httpadapter.WithRitualAuditRepo(ritualAuditRepo),
	}

	// Phase B (atomic-napping-spring.md O+ hydration) — load the static
	// crews + agents metadata from chora-infra/agents-cli/registry.json
	// for /api/v1/observability/agents. The endpoint returns 503 when the
	// file is absent or unparseable.
	if registry := loadAgentsRegistry(); registry != nil {
		opts = append(opts, httpadapter.WithAgentsRegistry(registry))
	}

	// O+ Agent-Eval evidence drill-down (IMDA D2 transparency) — a real
	// BigQuery read over chora_observability_analytics.agent_eval_evidence.
	// Enabled when CHORA_EVAL_EVIDENCE_BQ is set (the deployment sets it);
	// absent in local dev so /api/v1/observability/eval-runs 503s rather than
	// dialing BigQuery without ADC. Project/dataset/view/location are env-
	// sourced (secrets-and-env; no inline config) with chora-489812 defaults.
	if os.Getenv("CHORA_EVAL_EVIDENCE_BQ") != "" {
		evalProject := envOrDefault("CHORA_EVAL_BQ_PROJECT",
			envOrDefault("CHORA_PROJECT", envOrDefault("GOOGLE_CLOUD_PROJECT", "chora-489812")))
		evalDataset := envOrDefault("CHORA_EVAL_BQ_DATASET", "chora_observability_analytics")
		evalView := envOrDefault("CHORA_EVAL_BQ_VIEW", "agent_eval_evidence")
		evalLocation := envOrDefault("CHORA_EVAL_BQ_LOCATION", "asia-southeast1")
		evalRepo, err := bq.NewEvidenceClient(ctx, evalProject, evalDataset, evalView, evalLocation)
		if err != nil {
			log.Printf("observability: eval-evidence BigQuery client init failed (non-fatal; /eval-runs will 503): %v", err)
		} else {
			opts = append(opts, httpadapter.WithEvalEvidenceRepo(evalRepo))
			log.Printf("observability: eval-evidence BigQuery reader wired (project=%s dataset=%s view=%s loc=%s)",
				evalProject, evalDataset, evalView, evalLocation)
		}
	}

	// Optional Cloud Trace read client (Spanstore query API).
	if endpoint := os.Getenv("CLOUDTRACE_OTLP_READ_ENDPOINT"); endpoint != "" {
		mock := os.Getenv("CLOUDTRACE_MOCK") == "1"
		ctClient, err := cloudtrace.NewClient(cloudtrace.Config{
			Endpoint: endpoint,
			Mock:     mock,
		})
		if err != nil {
			log.Printf("cloudtrace client init failed (non-fatal): %v", err)
		} else {
			opts = append(opts, httpadapter.WithTraceExporter(&cloudtraceAdapter{client: ctClient}))
		}
	}

	handler := httpadapter.NewRouter(ledgerRepo, decisionRepo, correlationRepo, opts...)

	srv := &http.Server{
		Addr:              ":" + port,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	go func() {
		log.Printf("service=%s version=%s listening on %s", serviceName, version, srv.Addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server error: %v", err)
		}
	}()

	// -------------------------------------------------------------------------
	// gRPC server on :9090 (env-overrideable via CHORA_GRPC_PORT).
	//
	// Wave-1 ADR-140 remediation per docs/m13/grpc-mass-remediation-2026-05-16.md
	// (owner O-FULL). The Observability gRPC surface mirrors the HTTP routes
	// in internal/adapter/http/ — TokenUsageLedger, AgentDecisionLog,
	// TraceCorrelation. Primary callers: chora-gateway BFF + chora-governance
	// (IMDA D1/D2 evidence queries).
	//
	// Per feedback_no_stubs_real_wiring: registers UNCONDITIONALLY — no
	// `if env unset { skip }` shim. If a dependent is not wired yet, that's
	// the consumer's problem. HTTP server stays LIVE in parallel through
	// Wave-3 soak per the same doc §7 risk + rollback ruling.
	//
	// Same ledgerHook + repositories the HTTP handler uses — the two
	// protocol surfaces share state so ledger writes via gRPC are
	// immediately visible to HTTP readers, and vice versa.
	// -------------------------------------------------------------------------
	grpcPort := strings.TrimSpace(os.Getenv("CHORA_GRPC_PORT"))
	if grpcPort == "" {
		grpcPort = "9090"
	}
	grpcLis, err := net.Listen("tcp", ":"+grpcPort)
	if err != nil {
		log.Fatalf("observability: gRPC net.Listen :%s: %v", grpcPort, err)
	}
	grpcSrv := grpc.NewServer()
	obsServer := grpcadapter.NewObservabilityServer(ledgerRepo, decisionRepo, correlationRepo, ledgerHook)
	observabilityv1.RegisterObservabilityServer(grpcSrv, obsServer)

	// gRPC health check — required for Cloud Service Mesh probe routing.
	healthSrv := healthgrpc.NewServer()
	healthpb.RegisterHealthServer(grpcSrv, healthSrv)

	go func() {
		log.Printf("service=%s grpc listening on :%s (Observability + Health bound)", serviceName, grpcPort)
		if err := grpcSrv.Serve(grpcLis); err != nil && err != grpc.ErrServerStopped {
			log.Fatalf("observability: gRPC Serve: %v", err)
		}
	}()

	<-ctx.Done()
	log.Printf("shutting down...")

	// Drain gRPC first — in-flight Observability calls finish + clients see
	// EOF cleanly before the HTTP path drains. Mirrors chora-identity's
	// canonical Wave-1 shutdown ordering.
	grpcShutdownDone := make(chan struct{})
	go func() {
		grpcSrv.GracefulStop()
		close(grpcShutdownDone)
	}()
	select {
	case <-grpcShutdownDone:
		log.Printf("grpc server drained")
	case <-time.After(10 * time.Second):
		log.Printf("grpc graceful-stop deadline exceeded — forcing stop")
		grpcSrv.Stop()
	}

	// Drain the token-usage Pub/Sub subscriber goroutine — broker already
	// saw ctx cancel; this just waits for the in-flight Handle() to settle
	// so the ledger.Append + inbox.Process commit cleanly.
	if tokenUsageDone != nil {
		select {
		case <-tokenUsageDone:
			log.Printf("token_usage subscriber drained")
		case <-time.After(5 * time.Second):
			log.Printf("token_usage subscriber drain deadline exceeded")
		}
	}

	// Drain the agent_decision Pub/Sub subscriber goroutine — same graceful
	// shutdown contract as the token_usage subscriber above.
	if agentDecisionDone != nil {
		select {
		case <-agentDecisionDone:
			log.Printf("agent_decision subscriber drained")
		case <-time.After(5 * time.Second):
			log.Printf("agent_decision subscriber drain deadline exceeded")
		}
	}

	// Drain the ritual_run_audit Pub/Sub subscriber goroutine — same graceful
	// shutdown contract as the token_usage / agent_decision subscribers above.
	if ritualAuditDone != nil {
		select {
		case <-ritualAuditDone:
			log.Printf("ritual_run_audit subscriber drained")
		case <-time.After(5 * time.Second):
			log.Printf("ritual_run_audit subscriber drain deadline exceeded")
		}
	}

	drainCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(drainCtx); err != nil {
		log.Printf("server shutdown error: %v", err)
	}
}

// bootstrapInbox returns the inbox idempotent.Store used by every
// subscriber. Uses idempotent.PostgresStore when the chora_observability
// pool is wired (production); falls back to idempotent.MemoryStore
// otherwise (dev / local tests).
//
// Per `.claude/skills/agentic-resilience-d6/SKILL.md` Pillar 2 — the
// inbox is the consumer-side dual of the outbox. In-process
// map[string]struct{} dedup is INSUFFICIENT under chaos (pod-death loses
// state, multi-replica fragments dedup).
//
// Implementation detail: chora-observability's main path provisions a
// pgxpool.Pool (pgx native); idempotent.PostgresStore takes a database/sql
// SQLDB shim. We bridge via stdlib.OpenDBFromPool which returns a
// *sql.DB that delegates to the pgxpool (no extra connection pool — same
// underlying connections).
func bootstrapInbox(pool *pgxpool.Pool) idempotent.Store {
	if pool == nil {
		log.Printf("observability: inbox MemoryStore wired (no DB pool; not durable across restart)")
		return idempotent.NewMemoryStore()
	}
	sqlDB := stdlib.OpenDBFromPool(pool)
	store := idempotent.NewPostgresStore(idempotentSQLDBAdapter{db: sqlDB})
	log.Printf("observability: inbox PostgresStore wired (pgxpool -> *sql.DB bridge via stdlib.OpenDBFromPool)")
	return store
}

// bootstrapOutboxStore returns the producer-side outbox.Store. Uses the
// canonical PostgresStore when the chora_observability pgxpool is wired
// (production), and falls back to the InMemoryStore otherwise (dev /
// local tests; not durable across restart).
//
// Mirrors bootstrapInbox — both bridge the SAME pgxpool.Pool to a
// *sql.DB via stdlib.OpenDBFromPool, so the outbox dispatcher reuses
// the existing connection pool instead of opening a second one. This
// removes the need for a separate CHORA_OUTBOX_DSN / CHORA_OUTBOX_DSN_SECRET_ID
// env contract (the producer-side outbox writes to outbox_events in the
// SAME database as the domain repositories, so a separate DSN was never
// architecturally required).
func bootstrapOutboxStore(pool *pgxpool.Pool) obsoutbox.Store {
	if pool == nil {
		log.Printf("observability: outbox InMemoryStore wired (no DB pool; not durable across restart)")
		return obsoutbox.NewInMemoryStore()
	}
	sqlDB := stdlib.OpenDBFromPool(pool)
	store := obsoutbox.NewPostgresStore(
		sqlDBAdapter{db: sqlDB},
		obsoutbox.PostgresStoreOptions{WorkerID: outboxWorkerID()},
	)
	log.Printf("observability: outbox PostgresStore wired (pgxpool -> *sql.DB bridge via stdlib.OpenDBFromPool, worker_id=%s)", outboxWorkerID())
	return store
}

// idempotentSQLDBAdapter bridges *sql.DB to the idempotent.SQLDB interface
// (which uses idempotent.SQLRows + idempotent.SQLRow so tests can stub).
type idempotentSQLDBAdapter struct {
	db *sql.DB
}

func (a idempotentSQLDBAdapter) ExecContext(ctx context.Context, q string, args ...interface{}) (sql.Result, error) {
	return a.db.ExecContext(ctx, q, args...)
}

func (a idempotentSQLDBAdapter) QueryContext(ctx context.Context, q string, args ...interface{}) (idempotent.SQLRows, error) {
	return a.db.QueryContext(ctx, q, args...)
}

func (a idempotentSQLDBAdapter) QueryRowContext(ctx context.Context, q string, args ...interface{}) idempotent.SQLRow {
	return a.db.QueryRowContext(ctx, q, args...)
}

// outboxWorkerID derives the dispatcher worker_id from
// CHORA_OUTBOX_WORKER_ID or HOSTNAME. Returns a stable default only when
// both are unset; the dispatcher requires a non-empty worker_id at
// construction time.
func outboxWorkerID() string {
	if v := os.Getenv("CHORA_OUTBOX_WORKER_ID"); v != "" {
		return v
	}
	if v := os.Getenv("HOSTNAME"); v != "" {
		return v
	}
	return "chora-observability-local"
}

// envOrDefault returns the env var if set, otherwise the default.
func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// sqlDBAdapter bridges *sql.DB to obsoutbox.SQLDB (which uses
// obsoutbox.SQLRows so tests can stub).
type sqlDBAdapter struct {
	db *sql.DB
}

func (a sqlDBAdapter) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return a.db.ExecContext(ctx, query, args...)
}

func (a sqlDBAdapter) QueryContext(ctx context.Context, query string, args ...any) (obsoutbox.SQLRows, error) {
	return a.db.QueryContext(ctx, query, args...)
}

// Compile-time check.
var _ obsoutbox.SQLDB = sqlDBAdapter{}

// loadAgentsRegistry probes the canonical search paths for the agents
// registry JSON file (chora-infra/agents-cli/registry.json copy). Returns
// nil on the first decoding failure; the /api/v1/observability/agents
// route degrades to 503 when nil. Per [[secrets-and-env]] +
// [[feedback-no-stubs-real-wiring]] the registry file path is overridable
// via CHORA_AGENTS_REGISTRY_PATH.
func loadAgentsRegistry() *agents.Registry {
	for _, p := range agents.DefaultSearchPaths() {
		if p == "" {
			continue
		}
		if _, statErr := os.Stat(p); statErr != nil {
			// File missing at this candidate is benign — try the next.
			continue
		}
		r, err := agents.Load(p)
		if err != nil {
			log.Printf("observability: agents registry parse failed at %s: %v", p, err)
			continue
		}
		log.Printf("observability: agents registry loaded from %s (%d crews)", p, len(r.Crews))
		return r
	}
	log.Printf("observability: agents registry NOT loaded (config/registry.json absent) — /api/v1/observability/agents will 503")
	return nil
}
