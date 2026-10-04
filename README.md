# chora-observability

Observability supporting domain — TokenUsageLedger + AgentDecisionLog +
TraceCorrelation + 3-level Budget + cost reconciliation.

| Aspect | Value |
|---|---|
| **Service name** | `chora-observability` |
| **Surface** | O+ (Observability+) |
| **Domain** | Observability (supporting) |
| **Project** | `chora-489812` (platform host) |
| **Owning team** | Team 3 — Platform |
| **Database** | `chora_observability` (Cloud SQL Enterprise Plus) |
| **Topic prefix** | `chora.observability.*` (canonical: `chora.observability.token_usage.recorded.v1`) |
| **Module** | `github.com/5007-Capstone/chora/services/chora-observability` |

## Local configuration

The service and the reconciliation job automatically load a dotenv file before reading any configuration.

Create the local file:

```bash
cp .env.example .env
```

Then run the service normally:

```bash
go run ./cmd/server
```

The lookup order is `.env` in the current working directory, then `/app/.env`. Set `CHORA_ENV_FILE` to use another path. Values already present in the process environment always win, so Kubernetes, Cloud Run, Docker, CI, and Secret Manager injection remain authoritative.

For a local Docker stack, use PostgreSQL + the Google Pub/Sub emulator:

```bash
cp .env.example .env
export CHORA_OBSERVABILITY_IMAGE=<your-built-image>
docker compose -f compose.local.yaml up -d
```

`compose.local.yaml` starts PostgreSQL 18, applies the forward database migrations on first initialization, starts the Pub/Sub emulator, creates the service's canonical topics/subscriptions, and then starts observability. It forces `CHORA_TRACING_ENABLED=false` and `CHORA_DECISION_BQ_ENABLED=false`, so local Pub/Sub does not accidentally trigger Cloud Trace or BigQuery ADC calls.

Local Docker also enables `CHORA_STRICT_STARTUP=true`. In strict mode the process refuses to start when the database configuration is absent, the PostgreSQL pool cannot be established, `CHORA_PUBSUB_PROJECT` is absent, or the Pub/Sub client cannot be created. It never silently substitutes in-memory persistence or an in-memory event bus. Startup logs print the resolved strict/tracing/BigQuery/emulator mode before dependency bootstrap.

The application still uses the normal Google Pub/Sub client. Setting `PUBSUB_EMULATOR_HOST` makes that client talk to the emulator, so no separate fake event-bus implementation or changed topic semantics are introduced.

Existing GCP deployments are backward-compatible: both GCP-only feature flags default to enabled when absent, `PUBSUB_EMULATOR_HOST` is optional, Secret Manager remains available, and the existing Cloud Build/GKE deployment files are unchanged.

The local Compose file intentionally consumes an already-built service image. The repository's existing Dockerfile is designed for the original Chora monorepo build context and copies `libs/chora-go-common` plus generated `chora-contracts` from outside this standalone repository. Changing that Dockerfile would break the existing CI build contract. Once an image is built by the existing pipeline (or mirrored to another registry), the runtime itself no longer needs GCP credentials.

The checked-in `.env.example` is only a template; `.env` is ignored by Git and excluded from the Docker build context.

## Aggregates

- **`TokenUsageLedger`** — append-only, billing-grade cost ledger (one row per LLM call). int64 micros (1e-6 USD) throughout.
- **`AgentDecisionLog`** — append-only, IMDA-tagged AI decision audit trail.
- **`TraceCorrelation`** — W3C trace_id ↔ span_id ↔ tenant ↔ correlation_id mapping.
- **`Budget`** — per-tenant per-period cap + threshold-crossings (50/80/100/110%).
- **`AnomalyDetector`** — pure 3σ statistical detector for rolling 1h cost vs 7d baseline.
- **`Reconciler`** — daily BigQuery vs Vertex Billing API drift check (±0.01% tolerance).

## Endpoints

| Method | Path | Description |
|---|---|---|
| GET | `/healthz/`, `/readyz` | Health + readiness |
| POST | `/api/token-usage` | Append a ledger entry |
| GET | `/api/token-usage` | List entries (paginated, filtered) |
| GET | `/api/token-usage/cost` | Aggregate cost (int64 micros sum) |
| GET | `/api/token-usage/aggregate?group_by=model\|agent\|gcid` | Group by dimension |
| POST | `/api/token-usage/budget` | Set per-tenant period cap |
| GET | `/api/token-usage/budget?period=YYYY-MM` | Current spend vs cap |
| **POST** | `/api/token-usage/budget-check` | **3-level cascade pre-check (NEW per S3.3)** |
| POST | `/api/agent-decisions` | Append AgentDecisionLog |
| GET | `/api/agent-decisions` | List decisions |
| GET | `/api/agent-decisions/{id}` | Fetch decision by id |
| POST | `/api/correlations` | Register a TraceCorrelation |
| GET | `/api/correlations/{id}` | Fetch a correlation |
| GET | `/api/cost/cumulative` | Sequel-comic cumulative ticker |
| GET | `/api/cost/by-act` | By-model cost breakdown |
| POST | `/api/traces/export` | Spanstore range export to Cloud Trace |

## 3-level Budget cascade (per S3.3)

Per ai-cost-tracking skill — Gateway calls `/api/token-usage/budget-check` BEFORE every LLM invocation:

1. **Per-tenant** HARD CAP — Reason=`tenant_cap_exceeded`
2. **Per-user** FAIRNESS slice — Reason=`user_cap_exceeded`
3. **Per-agent** KILL-SWITCH (≥100x baseline) — Reason=`agent_kill_switch`

Most-restrictive verdict wins. Hard-block returns `retry_after_seconds`. Infrastructure errors fail-open with `infrastructure_fail_open` reason.

## TokenUsageLedger atomic-write contract (per S3.3)

The Gateway calls the ledger hook AFTER every successful LLM invocation. ONE call → 3 atomic side-effects:

1. **Append** TokenUsageLedger entry (append-only invariant preserved)
2. **Publish** `chora.observability.token_usage.recorded.v1` via outbox (envelope tagged `chora_imda_dimension=accountability` per ADR-141 + `imda_lifecycle_stage=runtime` per Tier 5 D18)
3. **RecordSpend** on the 3-level Budget cascade (best-effort)

Atomicity: outbox first; if publish fails, ledger is NOT appended (no phantom entries).

The Gateway-side adapter is `services/chora-model-broker-gateway/internal/adapter/cost/` (HTTP client; gRPC stub replaces this once `chora-contracts/proto/services/observability` lands at M11.4).

## pricing.yaml v2026.05.09-1 (per S1.2 schema lockdown)

Single source of truth for cost computation: `services/chora-observability/config/pricing.yaml`.

Covers:
- **Vertex AI Gemini family** (managed, per-1k-token pricing)
- **Self-hosted Gemma 4** (GPU-hours-amortized; L4 + A100 SKUs)
- **BYOA** (`pass_through: true` — tenant pays provider directly)

Compute via:
- `pricing.ComputeCostMicros(p, prompt, completion, cached)` — managed models
- `pricing.ComputeGPUCostMicros(p, gpuSeconds)` — self-hosted Gemma

## Reconciliation harness (per S3.3 P6)

Daily Cloud Run Job at 04:00 UTC:

```
BigQuery SUM(token_usage_ledger.cost_usd_micros)  vs
Vertex Billing API aggregated_cost   →   ±0.01% tolerance check
                                              ↓
                                drift > tolerance →
                                chora.governance.payment_reconciliation.anomaly.v1
```

Entrypoint: `cmd/reconcile/main.go`. Adapters: `internal/adapter/bigquery/` + `internal/adapter/billing/`.

Env config (NEVER inline per CLAUDE.md):

| Var | Default | Purpose |
|---|---|---|
| `GOOGLE_CLOUD_PROJECT` | (required) | GCP project the Gateway/Observability run in |
| `BIGQUERY_DATASET` | `chora_observability_analytics` | Analytics dataset |
| `RECONCILE_LOOKBACK_DAYS` | `1` | Days to reconcile (default = previous day) |
| `RECONCILE_TOLERANCE_FRACTION` | `0.0001` | Drift threshold (0.01%) |
| `BILLING_API_KEY` | — | (Tier 2) prefer WIF impersonation; never key file |

## Migrations

Two migration files under `migrations/`:

| File | Status | Source |
|---|---|---|
| `0001_initial.sql` | ✓ applied baseline | M10 schema (TokenUsageLedger + AgentDecisionLog + TraceCorrelation + append-only triggers + RLS policies) |
| `0002_schema_lockdown.sql` | ✓ idempotent (S1.2 — A-Platform-Obs) | Adds `agent_id` / `model_version` / `pricing_config_version` / `traceparent` / `cached_tokens` / 3-level budget tables; ENUMs for `imda_dimension` + `imda_lifecycle_stage` + `autonomy_level`. |

Both migrations use `IF NOT EXISTS` / `IF EXISTS` clauses so they're safe to re-apply.

### Apply path

**M10 dev DB**: not yet provisioned (`gcloud sql instances list` returns 0). Once `chora-infra/terraform/modules/m10-data-plane` is applied:

```bash
# 1. Set up Cloud SQL Auth Proxy or PgBouncer
gcloud sql connect chora-cloudsql-platform --user=chora-observability --database=chora_observability

# 2. Apply migrations (idempotent — safe to re-run)
psql -f services/chora-observability/migrations/0001_initial.sql
psql -f services/chora-observability/migrations/0002_schema_lockdown.sql
```

A migration runner (e.g. `golang-migrate/migrate`) is a Tier 2 follow-up — every other supporting service uses the same pattern.

### Apply via the chora-infra Cloud SQL module (Tier 2)

`chora-infra/terraform/modules/m10-data-plane` provisions the DB; a follow-up `chora-infra/terraform/modules/migrations` job runs the SQL files post-`terraform apply`. Tracked at AIK-OBS-1 (M11.4 / M12).

## Build & test

```bash
GOWORK=off go test -coverprofile=cover.out ./...
GOWORK=off go tool cover -func=cover.out
```

Coverage gates per `.claude/rules/development-execution.md`:

| Layer | Threshold | Current |
|---|---|---|
| Domain (`internal/domain/...`) | 85% | **94.9%** (ledger), 96% (reconcile), 96.8% (pricing), 100% (anomaly) |
| Adapter (`internal/adapter/...`) | 60% | **82.9%** (inmem), 71.3% (http), 89.5% (cloudtrace) |

## References

- `docs/architecture-review-inputs-2026-05-07.md` Tier 3 D10 (cost tracking) + D12 (OTLP-everywhere)
- ADR-141 (IMDA dimension labels reconciliation)
- ai-cost-tracking skill
- ai-observability-cloud-trace skill
- imda-governance-4-dimensions skill
