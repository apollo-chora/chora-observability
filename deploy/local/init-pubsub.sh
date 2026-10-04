#!/bin/bash
set -euo pipefail

project="chora-local"

create_topic() {
  gcloud pubsub topics describe "$1" --project="$project" >/dev/null 2>&1 ||
    gcloud pubsub topics create "$1" --project="$project" >/dev/null
}

create_sub() {
  local sub="$1" topic="$2"
  gcloud pubsub subscriptions describe "$sub" --project="$project" >/dev/null 2>&1 ||
    gcloud pubsub subscriptions create "$sub" --topic="$topic" --project="$project" >/dev/null
}

create_topic "chora.observability.token_usage.recorded.v1"
create_sub "chora-observability.observability-token_usage-recorded" "chora.observability.token_usage.recorded.v1"

create_topic "chora.observability.agent_decision.logged.v1"
create_sub "chora-observability.observability-agent_decision-logged" "chora.observability.agent_decision.logged.v1"

create_topic "chora.consumption.familiar.ritual_run_completed.v1"
create_sub "chora-observability.observability-ritual_run_completed" "chora.consumption.familiar.ritual_run_completed.v1"

create_topic "chora.tenancy.external_egress_policy.updated.v1"
create_sub "chora-observability.tenancy-external_egress_policy-updated" "chora.tenancy.external_egress_policy.updated.v1"

create_topic "chora.observability.pii.pseudonymise.requested.v1"
create_sub "chora-observability.closure-pseudonymise" "chora.observability.pii.pseudonymise.requested.v1"

while read -r topic sub; do
  create_topic "$topic"
  create_sub "$sub" "$topic"
done <<'EOF'
chora.consumption.familiar.exp_awarded.v1 chora-observability.consumption-familiar-exp_awarded
chora.consumption.familiar.stage_up.v1 chora-observability.consumption-familiar-stage_up
chora.consumption.familiar.breed_revealed.v1 chora-observability.consumption-familiar-breed_revealed
chora.consumption.familiar.hatched.v1 chora-observability.consumption-familiar-hatched
chora.consumption.familiar.source_revelation.v1 chora-observability.consumption-familiar-source_revelation
chora.consumption.familiar.egg_purchased.v1 chora-observability.consumption-familiar-egg_purchased
chora.tenancy.familiar_egg.payment_succeeded.v1 chora-observability.tenancy-familiar_egg-payment_succeeded
EOF

echo "Pub/Sub emulator topics and subscriptions are ready."
