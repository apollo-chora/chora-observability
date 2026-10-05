# chora-observability

Observability service for Chora. It stores token usage, agent decisions, audit projections, budgets, and trace correlations.

The recommended deployment is a local Docker container backed by PostgreSQL and a NATS JetStream event bus. Grafana Tempo is optional (trace reads). No Google Cloud infrastructure is required.

## Local deployment

### Requirements

- Docker with Compose
- PostgreSQL 18
- NATS 2 (JetStream enabled)
- A `walfa/chora-observability` image
- The observability database migrations applied to PostgreSQL

The service exposes:

| Port | Protocol | Purpose |
|---|---|---|
| `8080` | HTTP | REST API, health and readiness |
| `9090` | gRPC | Observability gRPC API + health |

### Recommended Compose configuration

If PostgreSQL and NATS already exist in your Compose stack, add the service like this:

```yaml
chora-observability:
  image: walfa/chora-observability:latest
  container_name: chora-observability
  restart: unless-stopped

  env_file:
    - .env

  environment:
    CHORA_STRICT_STARTUP: "true"

    CHORA_DB_DSN: postgres://${POSTGRES_USER}:${POSTGRES_PASSWORD}@postgres:5432/${POSTGRES_DB}?sslmode=disable

    NATS_URL: nats://nats:4222
    CHORA_SOURCE_PROJECT: chora-local

    CHORA_TRACING_ENABLED: "false"

    PORT: "8080"
    CHORA_GRPC_PORT: "9090"

  ports:
    - "8080:8080"
    - "9090:9090"

  depends_on:
    postgres:
      condition: service_healthy
    nats:
      condition: service_healthy

  stop_grace_period: 30s
```

The hostname in `CHORA_DB_DSN` is the Compose service name, not `localhost`. Likewise, `NATS_URL` must use the NATS service's Compose service name.

## Environment

A minimal local `.env` can look like:

```dotenv
POSTGRES_USER=chora
POSTGRES_PASSWORD=change-me
POSTGRES_DB=chora_observability

NATS_URL=nats://127.0.0.1:4222
CHORA_SOURCE_PROJECT=chora-local
```

The application can also load a dotenv file itself. It checks `.env` and then `/app/.env`. Set `CHORA_ENV_FILE` to use a different path.

Values already injected into the process environment take precedence over values in a dotenv file. This makes `env_file:` and Compose `environment:` overrides safe to use together.

Never bake a real `.env` into the container image. Local dotenv files are ignored by Git and excluded from the Docker build context.

## Strict startup

Local deployments should use:

```dotenv
CHORA_STRICT_STARTUP=true
```

This prevents the service from appearing healthy while silently running without its durable dependencies.

With strict startup enabled, the process exits when:

- database configuration is missing;
- PostgreSQL bootstrap fails;
- `NATS_URL` is missing; or
- the NATS JetStream bus cannot be initialized.

Without strict startup, the legacy development behavior is retained: missing dependencies can fall back to in-memory implementations.

At startup the service logs its resolved runtime mode, for example:

```text
observability: strict startup ENABLED — durable DB and NATS event bus are required; in-memory fallbacks are forbidden
observability: startup config strict=true tracing=false tempo_traces=false
```

## PostgreSQL

For a durable local deployment, set:

```dotenv
CHORA_DB_DSN=postgres://USER:PASSWORD@postgres:5432/chora_observability?sslmode=disable
```

The service uses PostgreSQL for the durable ledger, decision log, inbox/outbox, audit projections, and other persistent repositories.

A single PostgreSQL server can host databases for multiple Chora services. Prefer a dedicated database and non-superuser role for observability rather than sharing one application database between services.

Database migrations live in `migrations/`.

The repository also contains `deploy/local/init-db.sh`, which applies forward observability migrations to a newly initialized local PostgreSQL database. Production-only role grants in `9999_grant_app_roles.sql` are intentionally not applied by that local helper.

## Event bus (NATS JetStream)

The service consumes and publishes events on a NATS JetStream bus. It refuses to start without `NATS_URL` — there is no in-memory fallback, because the outbox dispatcher and every subscriber require a real broker.

Streams and subscriptions are provisioned out-of-band (see `scripts/nats-init.sh` in the platform repository). The service's canonical subscriptions are documented in `cmd/server/*_binding.go`.

## Tracing (Grafana Tempo)

The service emits OTLP traces and can read them back from Grafana Tempo. The local stack runs Tempo with OTLP ingest on `tempo:4317` and the query API on `tempo:3200`.

Set `TEMPO_QUERY_URL` to enable the Spanstore read routes:

```dotenv
TEMPO_QUERY_URL=http://tempo:3200
```

When `TEMPO_QUERY_URL` is unset, the trace-read routes (`/api/traces/export` and `/api/v1/observability/spans`) return 503 honestly ("trace exporter not configured") rather than fabricating data.

`CHORA_TRACING_ENABLED` controls whether the service emits OTLP traces at all. Set it to `false` to disable trace emission.

## Health checks

HTTP health endpoints:

```text
GET /healthz/
GET /readyz
```

The gRPC server also registers the standard gRPC health service on port `9090`.

When using `CHORA_STRICT_STARTUP=true`, a missing required dependency causes the process to exit rather than exposing a misleading healthy service.

## Main HTTP endpoints

| Method | Path | Purpose |
|---|---|---|
| GET | `/healthz/` | Liveness |
| GET | `/readyz` | Readiness |
| POST | `/api/token-usage` | Record token usage |
| GET | `/api/token-usage` | Query token usage |
| GET | `/api/token-usage/cost` | Aggregate cost |
| GET | `/api/token-usage/aggregate` | Aggregate by model, agent, or GCID |
| POST | `/api/token-usage/budget` | Set a budget |
| GET | `/api/token-usage/budget` | Query budget usage |
| POST | `/api/token-usage/budget-check` | Perform budget pre-check |
| POST | `/api/agent-decisions` | Record an agent decision |
| GET | `/api/agent-decisions` | Query agent decisions |
| GET | `/api/agent-decisions/{id}` | Fetch an agent decision |
| POST | `/api/correlations` | Register trace correlation |
| GET | `/api/correlations/{id}` | Fetch trace correlation |
| GET | `/api/cost/cumulative` | Cumulative cost |
| GET | `/api/cost/by-act` | Cost breakdown |
| POST | `/api/traces/export` | Spanstore range export (Tempo) |
| GET | `/api/v1/observability/spans` | Spanstore query (Tempo) |

## Configuration files

The runtime image contains:

```text
/config/PII_Closure_Map.yaml
/config/pricing.yaml
```

Their paths can be overridden with:

```dotenv
CHORA_PII_CLOSURE_MAP_PATH=/config/PII_Closure_Map.yaml
CHORA_PRICING_CONFIG_PATH=/config/pricing.yaml
```

The pricing configuration is used to derive cost information for supported model decisions.

## Local repository stack

For development of this repository itself, `compose.local.yaml` provides an example stack containing PostgreSQL, NATS, and the observability service.

If you already maintain PostgreSQL and NATS services in a larger Chora Compose stack, use those instead. There is no requirement to run the repository's example Compose file.

## Building

The Dockerfile builds from this standalone repository. Shared Chora modules (`chora-common`, `chora-contracts/gen/go`) are resolved through Go modules (pinned pseudo-versions in `go.mod`), so no sibling checkout is required:

```bash
docker buildx build --platform=linux/amd64 \
  -f Dockerfile \
  --build-arg GIT_SHA=$(git rev-parse --short HEAD) \
  --build-arg BUILD_TIME=$(date -u +%Y-%m-%dT%H:%M:%SZ) \
  -t chora-observability:${TAG} .
```

## Tests

```bash
go test -coverprofile=cover.out ./...
go tool cover -func=cover.out
```
