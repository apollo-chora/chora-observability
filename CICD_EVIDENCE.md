# CI/CD push-trigger evidence log

Each dated line below is a benign touch commit whose push to main fires
`chora-observability-main-dev` (path filter `services/chora-observability/**`).
The commit SHA of each line IS the build's trigger cause; the evidence pack
references it in every capture caption (CHO-2371).

- 2026-07-27 CHO-2371 push-trigger evidence sweep fire
- 2026-07-27 CHO-2371 retry fire: transient sca-govulncheck machinery failure at 3eb5242
- 2026-07-27 CHO-2371 retry 2: root cause was the cosmetic listed-but-absent integration-coverage artifact (all steps SUCCESS both runs); artifact list fixed like payments
