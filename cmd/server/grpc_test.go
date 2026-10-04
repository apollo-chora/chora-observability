// grpc_test.go — in-process bufconn integration test for the chora-
// observability gRPC server. Verifies end-to-end proto serialization +
// the proto<->domain bridge by running a real *grpc.Server on a
// bufconn.Listener, registering ObservabilityServer + Health, dialing
// it with the bufconn DialContext, and round-tripping the priority RPCs.
//
// This is the canonical Wave-1 ADR-140 remediation test per
// docs/m13/grpc-mass-remediation-2026-05-16.md §3.e — mirrors
// chora-identity's mana_grpc_bufconn_test.go.
//
// Per feedback_strict_tdd: written RED-first (this test exists before the
// gRPC server is registered in main.go) — it validates the gRPC adapter
// in isolation, not the main() bootstrap. The bootstrap_test.go covers
// the pgx + Pub/Sub wiring; here we exercise the proto wire path.
package main

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthgrpc "google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	observabilityv1 "github.com/locoroco-git/Chora-LMS/chora-contracts/gen/go/chora/services/observability/v1"

	grpcadapter "github.com/5007-Capstone/chora/services/chora-observability/internal/adapter/grpc"
	"github.com/5007-Capstone/chora/services/chora-observability/internal/adapter/inmem"
)

const bufconnSize = 1024 * 1024

// startBufconnObservability spins up an in-process *grpc.Server with a real
// ObservabilityServer + Health bound, returning a client + cleanup hook.
// Same wiring cmd/server/main.go uses (modulo the listener — bufconn vs ":9090" tcp).
func startBufconnObservability(t *testing.T) (observabilityv1.ObservabilityClient, healthpb.HealthClient, func()) {
	t.Helper()
	lis := bufconn.Listen(bufconnSize)
	srv := grpc.NewServer()

	ledgers := inmem.NewLedgerRepository()
	decisions := inmem.NewDecisionRepository()
	correlations := inmem.NewCorrelationRepository()

	obs := grpcadapter.NewObservabilityServer(ledgers, decisions, correlations, nil)
	observabilityv1.RegisterObservabilityServer(srv, obs)

	health := healthgrpc.NewServer()
	healthpb.RegisterHealthServer(srv, health)

	go func() {
		if err := srv.Serve(lis); err != nil {
			t.Logf("bufconn observability server stopped: %v", err)
		}
	}()

	//nolint:staticcheck // bufconn dial requires the legacy DialContext API.
	conn, err := grpc.DialContext(
		context.Background(),
		"bufconn",
		grpc.WithContextDialer(func(_ context.Context, _ string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("bufconn dial: %v", err)
	}
	client := observabilityv1.NewObservabilityClient(conn)
	healthClient := healthpb.NewHealthClient(conn)

	cleanup := func() {
		_ = conn.Close()
		srv.GracefulStop()
		_ = lis.Close()
	}
	return client, healthClient, cleanup
}

// TestBufconn_HealthCheck verifies the gRPC health/grpc_health_v1
// service is registered and returns SERVING. Per Cloud Service Mesh
// probe routing this MUST be present on every Chora gRPC server
// (docs/m13/grpc-mass-remediation-2026-05-16.md §3.a).
func TestBufconn_HealthCheck(t *testing.T) {
	t.Parallel()
	_, health, cleanup := startBufconnObservability(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resp, err := health.Check(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("Health.Check: %v", err)
	}
	if resp.Status != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("expected SERVING; got %v", resp.Status)
	}
}

// TestBufconn_AppendListTokenUsage exercises the canonical Model-Gateway
// write path + the O+ cost-dashboard read path end-to-end.
func TestBufconn_AppendListTokenUsage(t *testing.T) {
	t.Parallel()
	client, _, cleanup := startBufconnObservability(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	tenantID := "tenant-bufconn-aplt"
	const (
		traceA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1"
		spanA  = "bbbbbbbbbbbbbbb1"
		traceB = "cccccccccccccccccccccccccccccc02"
		spanB  = "dddddddddddddd02"
	)

	if _, err := client.AppendTokenUsage(ctx, &observabilityv1.AppendTokenUsageRequest{
		TenantId:         tenantID,
		Gcid:             "gcid-1",
		Agid:             "agid-qgen",
		ModelId:          "gemini-2.5-pro",
		PromptTokens:     500,
		CompletionTokens: 200,
		CostUsdMicros:    1_200_000, // $1.20
		TraceId:          traceA,
		SpanId:           spanA,
	}); err != nil {
		t.Fatalf("AppendTokenUsage #1: %v", err)
	}
	if _, err := client.AppendTokenUsage(ctx, &observabilityv1.AppendTokenUsageRequest{
		TenantId:         tenantID,
		Gcid:             "gcid-2",
		ModelId:          "gemini-2.5-flash",
		PromptTokens:     100,
		CompletionTokens: 50,
		CostUsdMicros:    300_000, // $0.30
		TraceId:          traceB,
		SpanId:           spanB,
	}); err != nil {
		t.Fatalf("AppendTokenUsage #2: %v", err)
	}

	listResp, err := client.ListTokenUsage(ctx, &observabilityv1.ListTokenUsageRequest{
		TenantId: tenantID,
	})
	if err != nil {
		t.Fatalf("ListTokenUsage: %v", err)
	}
	if len(listResp.Items) != 2 {
		t.Fatalf("expected 2 entries; got %d", len(listResp.Items))
	}
	if listResp.Items[0].LedgerId == "" {
		t.Fatalf("expected server-stamped ledger_id on item[0]")
	}
	if listResp.Items[0].RecordedAt == nil {
		t.Fatalf("expected server-stamped recorded_at on item[0]")
	}

	sumResp, err := client.SumTokenUsageCost(ctx, &observabilityv1.SumTokenUsageCostRequest{
		TenantId: tenantID,
	})
	if err != nil {
		t.Fatalf("SumTokenUsageCost: %v", err)
	}
	if sumResp.TotalCostUsdMicros != 1_500_000 {
		t.Fatalf("expected total 1_500_000 micros; got %d", sumResp.TotalCostUsdMicros)
	}
	if sumResp.EntryCount != 2 {
		t.Fatalf("expected entry_count 2; got %d", sumResp.EntryCount)
	}
}

// TestBufconn_AppendListGetAgentDecision exercises the Governance Gatekeeper
// write path + the O+ Decision-Log Explorer read paths end-to-end.
func TestBufconn_AppendListGetAgentDecision(t *testing.T) {
	t.Parallel()
	client, _, cleanup := startBufconnObservability(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	tenantID := "tenant-bufconn-decision"

	appendResp, err := client.AppendAgentDecision(ctx, &observabilityv1.AppendAgentDecisionRequest{
		TenantId:      tenantID,
		Agid:          "agid-gatekeeper",
		DecisionType:  observabilityv1.AgentDecisionType_AGENT_DECISION_TYPE_REFUSE,
		Reason:        "guardrail-block: pii-detected",
		RiskTier:      observabilityv1.AgentRiskTier_AGENT_RISK_TIER_HIGH,
		CorrelationId: "corr-123",
		Traceparent:   "00-eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee-ffffffffffffffff-01",
		Reasoning: &observabilityv1.AppendReasoningSummary{
			InputText:  "what is my ssn?",
			OutputText: "refused",
			LatencyMs:  42,
			Summary:    "pii-detected-input",
		},
	})
	if err != nil {
		t.Fatalf("AppendAgentDecision: %v", err)
	}
	if appendResp.Entry.LogId == "" {
		t.Fatalf("expected server-stamped log_id")
	}
	if appendResp.Entry.Reasoning == nil || appendResp.Entry.Reasoning.InputHash == "" {
		t.Fatalf("expected reasoning summary with input_hash; got %+v", appendResp.Entry.Reasoning)
	}

	getResp, err := client.GetAgentDecision(ctx, &observabilityv1.GetAgentDecisionRequest{
		TenantId: tenantID,
		LogId:    appendResp.Entry.LogId,
	})
	if err != nil {
		t.Fatalf("GetAgentDecision: %v", err)
	}
	if getResp.Entry.CorrelationId != "corr-123" {
		t.Fatalf("expected correlation_id=corr-123; got %q", getResp.Entry.CorrelationId)
	}

	// NOT_FOUND on missing log_id maps to gRPC codes.NotFound.
	if _, err := client.GetAgentDecision(ctx, &observabilityv1.GetAgentDecisionRequest{
		TenantId: tenantID,
		LogId:    "nope-no-such-log",
	}); status.Code(err) != codes.NotFound {
		t.Fatalf("expected NotFound; got %v", err)
	}
}

// TestBufconn_GetAuditEvents exercises the primary chora-gateway BFF caller
// path. AppendAgentDecision twice → GetAuditEvents returns both items.
func TestBufconn_GetAuditEvents(t *testing.T) {
	t.Parallel()
	client, _, cleanup := startBufconnObservability(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	tenantID := "tenant-bufconn-audit"
	for i, agid := range []string{"agid-1", "agid-2"} {
		if _, err := client.AppendAgentDecision(ctx, &observabilityv1.AppendAgentDecisionRequest{
			TenantId:      tenantID,
			Agid:          agid,
			DecisionType:  observabilityv1.AgentDecisionType_AGENT_DECISION_TYPE_RESPOND,
			Reason:        "ok",
			RiskTier:      observabilityv1.AgentRiskTier_AGENT_RISK_TIER_LOW,
			CorrelationId: "corr-audit-" + agid,
		}); err != nil {
			t.Fatalf("AppendAgentDecision #%d: %v", i, err)
		}
	}

	resp, err := client.GetAuditEvents(ctx, &observabilityv1.GetAuditEventsRequest{
		TenantId: tenantID,
	})
	if err != nil {
		t.Fatalf("GetAuditEvents: %v", err)
	}
	if len(resp.Items) != 2 {
		t.Fatalf("expected 2 audit events; got %d", len(resp.Items))
	}
	if resp.Total != 2 {
		t.Fatalf("expected total=2; got %d", resp.Total)
	}
}

// TestBufconn_RegisterGetCorrelation exercises the TraceCorrelation register
// + get round-trip.
func TestBufconn_RegisterGetCorrelation(t *testing.T) {
	t.Parallel()
	client, _, cleanup := startBufconnObservability(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	tenantID := "tenant-bufconn-corr"
	const (
		traceID  = "1234567890abcdef1234567890abcdef"
		parentID = "abcdef1234567890"
		childID  = "fedcba9876543210"
	)
	if _, err := client.RegisterCorrelation(ctx, &observabilityv1.RegisterCorrelationRequest{
		TenantId:      tenantID,
		CorrelationId: "corr-roundtrip",
		TraceId:       traceID,
		ParentSpanId:  parentID,
		ChildSpans:    []string{childID},
	}); err != nil {
		t.Fatalf("RegisterCorrelation: %v", err)
	}

	getResp, err := client.GetCorrelation(ctx, &observabilityv1.GetCorrelationRequest{
		TenantId:      tenantID,
		CorrelationId: "corr-roundtrip",
	})
	if err != nil {
		t.Fatalf("GetCorrelation: %v", err)
	}
	if getResp.Entry.TraceId != traceID {
		t.Fatalf("expected trace_id=%s; got %s", traceID, getResp.Entry.TraceId)
	}
	if len(getResp.Entry.ChildSpans) != 1 || getResp.Entry.ChildSpans[0] != childID {
		t.Fatalf("expected child_spans=[%s]; got %v", childID, getResp.Entry.ChildSpans)
	}

	// NOT_FOUND on missing correlation_id.
	if _, err := client.GetCorrelation(ctx, &observabilityv1.GetCorrelationRequest{
		TenantId:      tenantID,
		CorrelationId: "no-such-correlation",
	}); status.Code(err) != codes.NotFound {
		t.Fatalf("expected NotFound; got %v", err)
	}
}

// TestBufconn_AppendTokenUsage_InvalidArgument verifies invalid input
// surfaces as gRPC codes.InvalidArgument (not Internal).
func TestBufconn_AppendTokenUsage_InvalidArgument(t *testing.T) {
	t.Parallel()
	client, _, cleanup := startBufconnObservability(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := client.AppendTokenUsage(ctx, &observabilityv1.AppendTokenUsageRequest{
		// tenant_id intentionally empty → InvalidArgument
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument; got %v", err)
	}
}
