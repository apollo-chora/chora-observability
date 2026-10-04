# chora-observability — Cloud Deploy ops runbook

> Operational reference for engineers + ops + auditors. Companion to
> services/chora-observability/cloudbuild.yaml + services/chora-observability/clouddeploy/{skaffold,dev,prod}.

## Identity

- Service: chora-observability
- Team: team-3-platform
- Domain: observability
- Cloud Build trigger: chora-observability-main-dev (TF-managed) on services/chora-observability/**
- Cloud Deploy pipeline: chora-observability-pipeline
- Cloud Deploy targets:
  - chora-observability-dev → ns `observability-dev` (auto-deploy)
  - chora-observability-prod → ns `observability` (requireApproval: false — SOFT per user directive 2026-05-17)
- Cloud Deploy Automation: chora-observability-pipeline/automations/advance-dev-to-prod (advanceRolloutRule, wait 60s)
- Cloud Deploy executionTimeout: 7200s (bumped from 1800s 2026-05-18 per commit 383d9458 — see feedback_stale_build_cleanup_2h)
- Runtime GSA: chora-platform-svc@chora-489812.iam.gserviceaccount.com
- Workload Identity binding: chora-489812.svc.id.goog[observability-dev/chora-observability] AND [observability/chora-observability]

## Image

- Registry: asia-southeast1-docker.pkg.dev/chora-489812/chora-services/chora-observability
- Image vulnerability tab: https://console.cloud.google.com/artifacts/docker/chora-489812/asia-southeast1/chora-services/chora-observability?project=chora-489812
- Cosign verify (KMS):
  ```bash
  cosign verify asia-southeast1-docker.pkg.dev/chora-489812/chora-services/chora-observability:<SHORT_SHA> \
    --key=gcpkms://projects/chora-489812/locations/asia-southeast1/keyRings/chora-keys/cryptoKeys/chora-binauthz-signer
  ```

## Evidence

- GCS evidence bucket: gs://chora-489812-cloudbuild-evidence/chora-observability/<SHORT_SHA>/
  - chora-observability-lint-report.txt
  - chora-observability-cover.out
  - chora-observability-cover-integration.out
  - chora-observability-gosec.sarif
  - chora-observability-govulncheck.json + -summary.txt
  - chora-observability-trivy.sarif
  - chora-observability-sbom.spdx.json

## Watch / verify commands

```bash
# Trigger fire history (last 10 builds)
gcloud builds list --region=asia-southeast1 --filter='tags:service-chora-observability' --limit=10 \
  --project=chora-489812

# Latest Cloud Deploy release
gcloud deploy releases list --delivery-pipeline=chora-observability-pipeline \
  --region=asia-southeast1 --limit=1 --project=chora-489812

# Rollout state on the latest release (replace <RELEASE>)
gcloud deploy rollouts list --release=<RELEASE> \
  --delivery-pipeline=chora-observability-pipeline --region=asia-southeast1 \
  --project=chora-489812

# Pod state in dev + prod
kubectl -n observability-dev get pods,deploy,hpa
kubectl -n observability get pods,deploy,hpa

# Smoke smoke (gRPC health check from inside cluster)
kubectl run --rm -i --tty grpcurl --image=fullstorydev/grpcurl:v1.8.9-alpine --restart=Never -- \
  -plaintext chora-observability.observability.svc.cluster.local:9090 grpc.health.v1.Health/Check
```

## Observability

- Cloud Monitoring dashboard (tf-authored, DORMANT until terraform apply m10-platform-services/monitoring):
  https://console.cloud.google.com/monitoring/dashboards/<DASHBOARD_ID>?project=chora-489812
- Cloud Trace service map (filtered): https://console.cloud.google.com/traces/list?project=chora-489812
- Cloud Logging: https://console.cloud.google.com/logs/query?project=chora-489812&query=resource.labels.namespace_name%3D%22observability%22

## Console links

- Trigger: https://console.cloud.google.com/cloud-build/triggers;region=asia-southeast1?project=chora-489812 (filter by chora-observability)
- Build history: https://console.cloud.google.com/cloud-build/builds;region=asia-southeast1?project=chora-489812
- Cloud Deploy pipeline: https://console.cloud.google.com/deploy/delivery-pipelines/asia-southeast1/chora-observability-pipeline?project=chora-489812

## Approval gate posture

SOFT (auto-approve) — `requireApproval: false` on chora-observability-prod target + Automation auto-advance enabled (60s wait after dev SUCCEEDED). Per user directive 2026-05-17 to support concurrent SIT dev. Restore manual approval by flipping `requireApproval: true` in chora-infra/clouddeploy/targets/chora-observability-prod.yaml + re-applying via `gcloud deploy apply`.

## Known runtime gotchas

- **Pool stall + cancel-to-reset**: see `feedback_stale_build_cleanup_2h` memory. If 0 WORKING + non-empty QUEUED for sustained 10+min, cancelling stuck builds may free scheduler slots.
- **NEG quota silently fails Cloud Deploy rollouts**: see `feedback_neg_quota_orphan_zones` memory. Symptom: rollout state FAILED with EXECUTION_FAILED, but `kubectl get pod -n <ns>` shows pod Ready 2/2. Check Cloud Console NEG quota for `asia-southeast1` (default 100/region).
- **Cosmetic FAILURE at trailing PUSH** (`<svc>-cover.out` missing): if go test fails before producing cover.out, the build status FAILS at the artifact-upload PUSH stage even though stages 1-12 succeeded. Cloud Deploy release WAS created at stage 12; deploy proceeds. Check actual pod state via `kubectl get pod -n <ns>`, not the Cloud Build status.

## See also

- Canonical pipeline spec: ../../../docs/architecture.md §6.8
- Cross-service evidence index: ../../../docs/cicd/EVIDENCE_PACK_INDEX_2026-05-17.md
- Cross-service URL cheat sheet: ../../../docs/cicd/EVIDENCE_URL_CHEAT_SHEET.md
