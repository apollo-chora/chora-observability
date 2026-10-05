# syntax=docker/dockerfile:1.6
#
# chora-observability Dockerfile — Go service.
# Generated from chora-infra/templates/Dockerfile.go-service.
# DO NOT edit ad-hoc; sync changes back to the template.
#
# Build context = this repository.
# Standard invocation:
#   docker buildx build --platform=linux/amd64 \
#     -f Dockerfile \
#     --build-arg SERVICE_NAME=chora-observability \
#     --build-arg GIT_SHA=$(git rev-parse --short HEAD) \
#     --build-arg BUILD_TIME=$(date -u +%Y-%m-%dT%H:%M:%SZ) \
#     -t chora-observability:${TAG} \
#     --push \
#     .

ARG GO_VERSION=1.26.6
ARG ALPINE_VERSION=3.23
ARG SERVICE_NAME=chora-observability
ARG GIT_SHA=unknown
ARG BUILD_TIME=unknown

############################
# Stage 1 — build
############################
FROM golang:${GO_VERSION}-alpine${ALPINE_VERSION} AS builder

ARG SERVICE_NAME
ARG GIT_SHA
ARG BUILD_TIME

WORKDIR /src

RUN apk add --no-cache ca-certificates git

# The service is standalone; shared Chora modules are resolved through Go modules.
COPY . .
WORKDIR /src

# Pre-fetch standalone module dependencies.
RUN go mod download

ENV CGO_ENABLED=0 \
    GOOS=linux \
    GOARCH=amd64
RUN go build -trimpath \
      -ldflags "-s -w \
        -X main.serviceName=${SERVICE_NAME} \
        -X main.gitSHA=${GIT_SHA} \
        -X main.buildTime=${BUILD_TIME}" \
      -o /out/service \
      ./cmd/server

############################
# Stage 2 — runtime
############################
FROM gcr.io/distroless/static-debian12:nonroot

ARG SERVICE_NAME
ARG GIT_SHA
ARG BUILD_TIME

LABEL org.opencontainers.image.title="${SERVICE_NAME}" \
      org.opencontainers.image.source="https://github.com/apollo-chora/chora-observability" \
      org.opencontainers.image.revision="${GIT_SHA}" \
      org.opencontainers.image.created="${BUILD_TIME}" \
      org.opencontainers.image.vendor="Chora Platform" \
      org.opencontainers.image.licenses="UNLICENSED" \
      io.chora.service="${SERVICE_NAME}" \
      io.chora.git-sha="${GIT_SHA}" \
      io.chora.build-time="${BUILD_TIME}"

WORKDIR /

COPY --from=builder /out/service /service

# Per-domain PII_Closure_Map.yaml (CHO-1719) — the federated closure-saga
# subscriber loads it at the default relative path
# config/PII_Closure_Map.yaml (runtime WORKDIR is /). Without this COPY
# the subscriber boots DISABLED (PII map load error).
COPY --from=builder /src/config/PII_Closure_Map.yaml /config/PII_Closure_Map.yaml

# pricing.yaml (CHO-2220) — same story as the PII map above, and it was missed
# when that one was fixed: the agent-decision cost projector loads it at the
# default relative path config/pricing.yaml (runtime WORKDIR is /). Without this
# COPY the container has no such file, the calculator is never constructed, and
# EVERY agent decision records a blank cost_usd — which is exactly what the
# deployed pod logged on every restart until this line existed:
#   "pricing config not loaded (config/pricing.yaml): ... no such file or
#    directory — agent_decision cost_usd will be blank"
# Absence stays non-fatal by design (blank, never fabricated), so this failure
# is silent apart from that one boot line. Do not drop this COPY.
COPY --from=builder /src/config/pricing.yaml /config/pricing.yaml

USER nonroot:nonroot
ENTRYPOINT ["/service"]
