# chora-observability

## About

chora-observability is a Go service that records and serves Chora observability data, including token usage, agent decisions, trace correlations, cost aggregates, budgets, and audit projections. It exposes an HTTP REST API and a gRPC API, and consumes and publishes events through NATS JetStream. PostgreSQL provides durable repositories when configured, while selected local development paths use in-memory stores.

## Quick start

Prerequisites:

- Go 1.26.1 or newer
- Docker with Compose
- PostgreSQL 18 for durable local development
- NATS 2 with JetStream enabled
- A built `chora-observability` image for the repository's Compose example

For a local container stack, copy the example environment file and build the image:

```bash
cp .env.example .env

docker buildx build \
  --platform=linux/amd64 \
  -f Dockerfile \
  --build-arg SERVICE_NAME=chora-observability \
  --build-arg GIT_SHA=$(git rev-parse --short HEAD) \
  --build-arg BUILD_TIME=$(date -u +%Y-%m-%dT%H:%M:%SZ) \
  -t chora-observability:local \
  --load \
  .

CHORA_OBSERVABILITY_IMAGE=chora-observability:local docker compose -f compose.local.yaml up
```

The example stack starts PostgreSQL, starts NATS with JetStream enabled, applies the forward SQL migrations in `migrations/` on first PostgreSQL initialization, and starts the service with strict startup enabled.

For direct local execution, create an environment file from `.env.example`, provide `NATS_URL`, and leave `CHORA_DB_DSN` unset to use the service's in-memory repository paths. The event bus is mandatory: the server exits when `NATS_URL` is missing.

```bash
cp .env.example .env
# edit .env and set NATS_URL to a reachable JetStream server
go run ./cmd/server
```

## Usage

The HTTP server listens on port `8080` by default. Set `PORT` to change it. The gRPC server listens on `9090` by default; set `CHORA_GRPC_PORT` to change it.

Protected HTTP routes require the `X-Tenant-Id` header. The service also accepts a W3C `traceparent` header and echoes it on the response.

Health endpoints:

| Method | Path | Purpose |
|---|---|---|
| GET | `/healthz` | Liveness |
| GET | `/healthz/` | Liveness |
| GET | `/health` | Liveness |
| GET | `/readyz` | Readiness |

Core HTTP API:

| Method | Path | Purpose |
|---|---|---|
| POST | `/api/token-usage` | Record token usage |
| GET | `/api/token-usage` | List token-usage entries |
| GET | `/api/token-usage/cost` | Sum token-usage cost |
| GET | `/api/token-usage/aggregate` | Group token usage by model, agent, or GCID |
| POST | `/api/token-usage/budget` | Create or set a tenant budget |
| GET | `/api/token-usage/budget` | Read a tenant budget and current spend |
| POST | `/api/token-usage/budget-check` | Check a tenant budget before use |
| POST | `/api/agent-decisions` | Record an agent decision |
| GET | `/api/agent-decisions` | List agent decisions |
| GET | `/api/agent-decisions/{id}` | Fetch an agent decision |
| POST | `/api/correlations` | Register a trace correlation |
| GET | `/api/correlations/{id}` | Fetch a trace correlation |
| GET | `/api/cost/cumulative` | Return cumulative cost |
| GET | `/api/cost/by-act` | Return cost grouped by model |
| POST | `/api/traces/export` | Export a trace-store range |
| GET | `/api/v1/observability/spans` | Query spans from the configured trace store |
| GET | `/api/v1/observability/agent-decisions` | Query decisions by run ID |
| GET | `/api/v1/observability/agent-decisions/count` | Count decisions in a time window |
| GET | `/api/v1/observability/agents` | Return the configured agent and crew view |
| GET | `/api/v1/observability/agent-prompts` | Return per-agent prompt evidence |
| GET | `/api/v1/observability/eval-runs` | List agent-evaluation runs when the evidence repository is wired |
| GET | `/api/v1/observability/eval-runs/{candidateLabel}` | Read evidence rows for one evaluation candidate |
| GET | `/api/analytics/dashboards/learner` | Learner analytics dashboard |
| GET | `/api/analytics/dashboards/instructor` | Instructor analytics dashboard |
| GET | `/api/analytics/dashboards/admin` | Tenant analytics dashboard |
| POST | `/api/analytics/cohorts` | Create an analytics cohort |
| GET | `/api/analytics/cohorts/{id}/retention` | Read cohort retention |
| GET | `/events` | Read audit events for a tenant |
| GET | `/v1/audit/familiar-growth/events` | Read Familiar Growth audit events |
| GET | `/v1/audit/familiar-growth/metrics` | Read Familiar Growth metrics |
| GET | `/v1/audit/familiar-growth/breed-distribution` | Read the breed-distribution report |
| GET | `/v1/audit/familiar-growth/egg-funnel` | Read the egg-funnel rollup |
| GET | `/v1/audit/ritual-runs` | Read ritual-run audit rows |
| GET | `/api/v1/admin/egress/kill-switch` | Read the platform egress kill-switch |
| PATCH | `/api/v1/admin/egress/kill-switch` | Change the platform egress kill-switch |
| GET | `/api/v1/admin/companion/suspension` | Read companion containment state |
| PATCH | `/api/v1/admin/companion/suspension` | Change companion containment state |

The span query endpoints require a Bearer token and an `X-Chora-Role` value of `observer` or `auditor` in the current handler implementation. The platform egress kill-switch is limited to the `platform_operator` mesh role. Companion containment accepts `platform_operator`, `auditor`, `admin`, or `owner` for reads; writes are additionally constrained by scope and actor.

Runtime configuration:

| Variable | Default / requirement | Purpose |
|---|---|---|
| `PORT` | `8080` | HTTP listen port |
| `CHORA_GRPC_PORT` | `9090` | gRPC listen port |
| `CHORA_STRICT_STARTUP` | `false` | Fail on missing durable database instead of using in-memory repositories |
| `CHORA_DB_DSN` | Optional unless strict startup is enabled | PostgreSQL connection string |
| `CHORA_DB_DSN_SECRET_ID` | Optional | Secret-backed PostgreSQL DSN |
| `CHORA_DB_PROJECT` | `chora-local` | Project name used by secret-backed database bootstrap |
| `CHORA_BOOTSTRAP_TIMEOUT_SECONDS` | `30` | Database bootstrap timeout |
| `NATS_URL` | Required | NATS JetStream server URL |
| `CHORA_SOURCE_PROJECT` | `chora-local` | Source-project stamp for outbox envelopes |
| `CHORA_TRACING_ENABLED` | `true` | Enable OTLP trace emission |
| `TEMPO_QUERY_URL` | Optional | Grafana Tempo query endpoint; enables trace-read routes |
| `CHORA_PII_CLOSURE_MAP_PATH` | `config/PII_Closure_Map.yaml` | PII closure map path |
| `CHORA_PRICING_CONFIG_PATH` | `config/pricing.yaml` | Pricing configuration path |
| `CHORA_ENV_FILE` | Optional | Explicit dotenv file path |
| `CHORA_TOKEN_USAGE_SUBSCRIPTION` | Built-in canonical name | JetStream token-usage consumer name |
| `CHORA_AGENT_DECISION_SUBSCRIPTION` | Built-in canonical name | JetStream agent-decision consumer name |
| `CHORA_CLOSURE_SUBSCRIPTION` | Built-in canonical name | JetStream closure subscriber name |
| `CHORA_OUTBOX_WORKER_ID` | `HOSTNAME` | Outbox worker identifier |
| `CHORA_DB_REWRITE_FROM_PORT` | Optional | Database port rewrite source |
| `CHORA_DB_REWRITE_TO_PORT` | Optional | Database port rewrite destination |

When `CHORA_STRICT_STARTUP=true`, PostgreSQL configuration and a working PostgreSQL bootstrap are required. Regardless of startup mode, `NATS_URL` is mandatory because subscribers and the outbox dispatcher use the JetStream bus.

Tracing has two separate controls. `CHORA_TRACING_ENABLED` controls OTLP emission. `TEMPO_QUERY_URL` controls trace reads. When the query URL is unset, `POST /api/traces/export` and `GET /api/v1/observability/spans` return `503`.

## Development

The service is a Go module with the server entry point in `cmd/server`, HTTP and gRPC adapters under `internal/adapter/`, domain logic under `internal/domain/`, analytics code under `internal/analytics/`, PostgreSQL adapters under `internal/adapter/pg/`, and SQL migrations under `migrations/`. Runtime configuration files used by the image are under `config/`.

Run the tests with:

```bash
go test ./...
```

To generate a coverage report:

```bash
go test -coverprofile=cover.out ./...
go tool cover -func=cover.out
```

Build the service binary locally with:

```bash
go build -o chora-observability ./cmd/server
```

Build the container image with:

```bash
docker buildx build \
  --platform=linux/amd64 \
  -f Dockerfile \
  --build-arg SERVICE_NAME=chora-observability \
  --build-arg GIT_SHA=$(git rev-parse --short HEAD) \
  --build-arg BUILD_TIME=$(date -u +%Y-%m-%dT%H:%M:%SZ) \
  -t chora-observability:local \
  --load \
  .
```

The Dockerfile builds the service as a standalone Go module and runs `go mod download` before building `./cmd/server`. The runtime image is a non-root distroless image and includes `config/PII_Closure_Map.yaml` and `config/pricing.yaml`.

The local Compose file is `compose.local.yaml`. Its PostgreSQL initialization script is `deploy/local/init-db.sh`; it applies forward migrations on first database initialization and does not apply migration down files.
