# chora-observability

Observability service for Chora. It stores token usage, agent decisions, audit projections, budgets, and trace correlations.

The recommended deployment is a local Docker container backed by PostgreSQL and the Google Pub/Sub emulator. BigQuery, Cloud Trace, Secret Manager, Cloud SQL, GKE, and other GCP infrastructure are not required for this mode.

## Local deployment

### Requirements

- Docker with Compose
- PostgreSQL 18
- Google Pub/Sub emulator
- A `walfa/chora-observability` image
- The observability database migrations applied to PostgreSQL

The service exposes:

| Port | Protocol | Purpose |
|---|---|---|
| `8080` | HTTP | REST API, health and readiness |
| `9090` | gRPC | Observability gRPC API + health |

### Recommended Compose configuration

If PostgreSQL and the Pub/Sub emulator already exist in your Compose stack, add the service like this:

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

    GOOGLE_CLOUD_PROJECT: ${GOOGLE_CLOUD_PROJECT}
    CHORA_PROJECT: ${GOOGLE_CLOUD_PROJECT}
    CHORA_SOURCE_PROJECT: ${GOOGLE_CLOUD_PROJECT}
    CHORA_PUBSUB_PROJECT: ${GOOGLE_CLOUD_PROJECT}
    PUBSUB_EMULATOR_HOST: pubsub-emulator:8685

    CHORA_TRACING_ENABLED: "false"
    CHORA_DECISION_BQ_ENABLED: "false"
    CHORA_EVAL_EVIDENCE_BQ: ""

    PORT: "8080"
    CHORA_GRPC_PORT: "9090"

  ports:
    - "8080:8080"
    - "9090:9090"

  depends_on:
    postgres:
      condition: service_healthy
    pubsub-init:
      condition: service_completed_successfully

  stop_grace_period: 30s
```

The hostname in `CHORA_DB_DSN` is the Compose service name, not `localhost`. Likewise, `PUBSUB_EMULATOR_HOST` must use the Pub/Sub emulator's Compose service name.

## Environment

A minimal local `.env` can look like:

```dotenv
POSTGRES_USER=chora
POSTGRES_PASSWORD=change-me
POSTGRES_DB=chora_observability

GOOGLE_CLOUD_PROJECT=chora-local
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
- `CHORA_PUBSUB_PROJECT` is missing;
- the Pub/Sub client cannot be initialized; or
- bootstrap unexpectedly leaves the database or Pub/Sub client unwired.

Without strict startup, the legacy development behavior is retained: missing dependencies can fall back to in-memory implementations.

At startup the service logs its resolved runtime mode, for example:

```text
observability: strict startup ENABLED — durable DB and Pub/Sub are required; in-memory fallbacks are forbidden
observability: startup config strict=true tracing=false decision_bigquery=false eval_bigquery=false pubsub_emulator=true
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

## Pub/Sub emulator

The service uses the normal Google Pub/Sub client library. No alternate local event implementation is required.

Point it at the emulator with:

```dotenv
GOOGLE_CLOUD_PROJECT=chora-local
CHORA_PROJECT=chora-local
CHORA_SOURCE_PROJECT=chora-local
CHORA_PUBSUB_PROJECT=chora-local
PUBSUB_EMULATOR_HOST=pubsub-emulator:8685
```

`PUBSUB_EMULATOR_HOST` redirects the client to the emulator. `CHORA_PUBSUB_PROJECT` is still required because the service uses it to construct Pub/Sub resources.

The emulator starts empty. Topics and subscriptions must therefore be created before observability starts.

A helper is provided at:

```text
deploy/local/init-pubsub.sh
```

A Compose initializer can run it:

```yaml
pubsub-init:
  image: gcr.io/google.com/cloudsdktool/google-cloud-cli:slim
  restart: "no"

  environment:
    PUBSUB_EMULATOR_HOST: pubsub-emulator:8685
    CLOUDSDK_CORE_PROJECT: ${GOOGLE_CLOUD_PROJECT}

  volumes:
    - ./pubsub/init.sh:/init.sh:ro

  entrypoint:
    - /bin/bash
    - /init.sh

  depends_on:
    pubsub-emulator:
      condition: service_healthy
```

Then make observability depend on successful completion of that initializer.

The repository helper provisions the canonical inbound subscriptions plus outbound topics needed by the service.

## Disable GCP-only integrations locally

Using a Pub/Sub emulator still requires a project identifier. That does not mean the service should try to use the rest of GCP.

For local Docker deployments use:

```dotenv
CHORA_TRACING_ENABLED=false
CHORA_DECISION_BQ_ENABLED=false
CHORA_EVAL_EVIDENCE_BQ=
```

This keeps:

- Pub/Sub enabled through the emulator;
- PostgreSQL enabled locally;
- Cloud Trace disabled;
- BigQuery decision mirroring disabled; and
- BigQuery evaluation evidence disabled.

No Google Cloud credentials or ADC are required for the normal local runtime with these integrations disabled.

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

For development of this repository itself, `compose.local.yaml` provides an example stack containing:

```text
PostgreSQL
    │
    ├── migrations
    │
chora-observability
    │
    └── Pub/Sub client
             │
             ▼
      Pub/Sub emulator
             │
             └── pubsub-init
```

If you already maintain PostgreSQL and Pub/Sub emulator services in a larger Chora Compose stack, use those instead. There is no requirement to run the repository's example Compose file.

## Existing GCP deployment

The repository still contains the existing GCP deployment path and adapters for backward compatibility, including Cloud Build, GKE/Cloud Deploy configuration, Secret Manager, BigQuery, and Cloud Trace.

Those are not required for local Docker deployment.

The feature flags introduced for local deployment preserve the old behavior when they are absent:

- `CHORA_STRICT_STARTUP` defaults to `false`;
- `CHORA_TRACING_ENABLED` defaults to `true`;
- `CHORA_DECISION_BQ_ENABLED` defaults to `true`; and
- `CHORA_EVAL_EVIDENCE_BQ` remains opt-in.

This allows existing GCP deployments to continue operating while local Docker deployments explicitly select only the dependencies they need.

## Building

The current Dockerfile preserves the original Chora monorepo build contract. Its build context expects:

```text
libs/chora-common
chora-contracts/gen/go
services/chora-observability
```

Therefore this standalone repository is currently best treated as a runtime/deployment repository when using the published `walfa/chora-observability` image.

Changing the Dockerfile to build entirely from this standalone repository requires first making the shared Go library and generated contracts independently resolvable modules.

## Tests

Within the original Chora Go workspace:

```bash
GOWORK=off go test -coverprofile=cover.out ./...
GOWORK=off go tool cover -func=cover.out
```
