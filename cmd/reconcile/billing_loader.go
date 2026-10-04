// billing_loader.go — env-driven Vertex Billing client construction for
// the daily reconciliation Cloud Run Job.
//
// Per `secrets-and-env`: NO inline URLs/tokens. The cmd/reconcile binary
// resolves the BillingClient via `BILLING_CLIENT_MODE`:
//
//	BILLING_CLIENT_MODE=mock     (default) — in-memory MockClient (M10).
//	BILLING_CLIENT_MODE=vertex             — real VertexClient backed by
//	                                         VERTEX_BILLING_BASE_URL +
//	                                         VERTEX_BILLING_TOKEN.
//
// Production rollout (Tier 2): replace the static-token TokenSource with
// `google.golang.org/x/oauth2/google.DefaultTokenSource` so ADC/WIF
// resolves the bearer token without an SA key file traveling. Until that
// dep is added, the build-out reads a static token from
// VERTEX_BILLING_TOKEN — the Cloud Run Job sources it from Secret Manager
// via the Cloud Run env-var binding (`--update-secrets=VERTEX_BILLING_TOKEN=...`).
package main

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/apollo-chora/chora-observability/internal/adapter/billing"
	"github.com/apollo-chora/chora-observability/internal/domain/reconcile"
)

// errUnknownBillingMode is returned when BILLING_CLIENT_MODE is set to
// a value other than "mock" or "vertex".
var errUnknownBillingMode = errors.New("unknown BILLING_CLIENT_MODE")

// staticEnvTokenSource is the build-out TokenSource — a thin wrapper
// around the VERTEX_BILLING_TOKEN env var. Production swaps this for
// google.DefaultTokenSource (ADC/WIF) without touching the
// VertexClient signature.
type staticEnvTokenSource struct{ token string }

func (s *staticEnvTokenSource) Token() (string, error) {
	if strings.TrimSpace(s.token) == "" {
		return "", errors.New("staticEnvTokenSource: VERTEX_BILLING_TOKEN blank")
	}
	return s.token, nil
}

// newBillingClientFromEnv resolves the BillingClient based on env. The
// returned value satisfies reconcile.BillingClient regardless of mode.
func newBillingClientFromEnv() (reconcile.BillingClient, error) {
	mode := strings.ToLower(strings.TrimSpace(os.Getenv("BILLING_CLIENT_MODE")))
	if mode == "" {
		mode = "mock"
	}
	switch mode {
	case "mock":
		return billing.NewMockClient(nil), nil
	case "vertex":
		baseURL := strings.TrimSpace(os.Getenv("VERTEX_BILLING_BASE_URL"))
		if baseURL == "" {
			return nil, errors.New("VERTEX_BILLING_BASE_URL required when BILLING_CLIENT_MODE=vertex")
		}
		token := strings.TrimSpace(os.Getenv("VERTEX_BILLING_TOKEN"))
		if token == "" {
			return nil, errors.New("VERTEX_BILLING_TOKEN required when BILLING_CLIENT_MODE=vertex")
		}
		c, err := billing.NewVertexClient(billing.VertexConfig{
			BaseURL:     baseURL,
			TokenSource: &staticEnvTokenSource{token: token},
		})
		if err != nil {
			return nil, fmt.Errorf("billing.NewVertexClient: %w", err)
		}
		return c, nil
	default:
		return nil, fmt.Errorf("%w: %q", errUnknownBillingMode, mode)
	}
}
