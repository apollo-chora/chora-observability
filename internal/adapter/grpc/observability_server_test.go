// Package grpcadapter_test exercises the v1 Observability gRPC server
// (internal/adapter/grpc). The server is a thin bridge over the three
// domain persistence ports — ledger.Repository, decision.Repository and
// correlation.Repository — so tests drive it through the real in-memory
// repos (internal/adapter/inmem) and through minimal stub repos for the
// error-mapping paths (codes.Internal / codes.InvalidArgument translation).
package grpcadapter_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	observabilityv1 "github.com/locoroco-git/Chora-LMS/chora-contracts/gen/go/chora/services/observability/v1"

	grpcadapter "github.com/5007-Capstone/chora/services/chora-observability/internal/adapter/grpc"
	"github.com/5007-Capstone/chora/services/chora-observability/internal/adapter/inmem"
	"github.com/5007-Capstone/chora/services/chora-observability/internal/domain/correlation"
	"github.com/5007-Capstone/chora/services/chora-observability/internal/domain/decision"
	"github.com/5007-Capstone/chora/services/chora-observability/internal/domain/ledger"
)

const (
	grpcTenant   = "01970000-0000-7000-8000-000000000001"
	grpcGcid     = "01970000-0000-7000-9000-000000000001"
	grpcAgid     = "01970000-0000-7000-a000-000000000001"
	grpcCorrID   = "01970000-0000-7000-b000-000000000001"
	grpcTraceID  = "00000000000000000000000000000001"
	grpcSpanID   = "0000000000000001"
	grpcChild1   = "0000000000000002"
	grpcChild2   = "0000000000000003"
	grpcModel    = "gemini-3-pro"
)

// testHarness bundles the in-memory repos backing a grpc server.
type testHarness struct {
	ledgers  *inmem.LedgerRepository
	decisions *inmem.DecisionRepository
	corrs    *inmem.CorrelationRepository
	srv      *grpcadapter.ObservabilityServer
}

func newTestHarness(t *testing.T) *testHarness {
	t.Helper()
	h := &testHarness{
		ledgers:   inmem.NewLedgerRepository(),
		decisions: inmem.NewDecisionRepository(),
		corrs:     inmem.NewCorrelationRepository(),
	}
	h.srv = grpcadapter.NewObservabilityServer(h.ledgers, h.decisions, h.corrs, nil)
	return h
}

func expectCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error with code %v; got nil", want)
	}
	if got := status.Code(err); got != want {
		t.Fatalf("error code = %v; want %v (err=%v)", got, want, err)
	}
}

// -----------------------------------------------------------------------------
// Stub repositories for the error-mapping paths (the in-memory repos never
// error, so the codes.Internal translation has to be forced with stubs).
// -----------------------------------------------------------------------------

type stubLedgerRepo struct {
	appendErr error
	listErr   error
	sumErr    error
}

func (s *stubLedgerRepo) Append(_ context.Context, _ *ledger.Entry) error { return s.appendErr }
func (s *stubLedgerRepo) List(_ context.Context, _ string, _ ledger.ListFilter) ([]*ledger.Entry, error) {
	return nil, s.listErr
}
func (s *stubLedgerRepo) SumCost(_ context.Context, _ string, _ ledger.ListFilter) (int64, int, error) {
	return 0, 0, s.sumErr
}

type stubDecisionRepo struct {
	appendErr error
	listErr   error
	getErr    error
}

func (s *stubDecisionRepo) Append(_ context.Context, _ *decision.Log) error { return s.appendErr }
func (s *stubDecisionRepo) List(_ context.Context, _ string, _ decision.ListFilter) ([]*decision.Log, error) {
	return nil, s.listErr
}
func (s *stubDecisionRepo) GetByID(_ context.Context, _, _ string) (*decision.Log, error) { return nil, s.getErr }
func (s *stubDecisionRepo) Count(_ context.Context, _ string, _, _ time.Time) (int64, error) { return 0, nil }

type stubCorrelationRepo struct {
	registerErr error
	getErr      error
}

func (s *stubCorrelationRepo) Register(_ context.Context, _ *correlation.Correlation) error { return s.registerErr }
func (s *stubCorrelationRepo) Get(_ context.Context, _, _ string) (*correlation.Correlation, error) {
	return nil, s.getErr
}

// -----------------------------------------------------------------------------
// Constructor
// -----------------------------------------------------------------------------

func TestNewObservabilityServer_RequiresAllRepos(t *testing.T) {
	tests := []struct {
		name      string
		ledgers   ledger.Repository
		decisions decision.Repository
		corrs     correlation.Repository
	}{
		{"nil ledger", nil, inmem.NewDecisionRepository(), inmem.NewCorrelationRepository()},
		{"nil decision", inmem.NewLedgerRepository(), nil, inmem.NewCorrelationRepository()},
		{"nil correlation", inmem.NewLedgerRepository(), inmem.NewDecisionRepository(), nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("expected panic for nil repository")
				}
			}()
			_ = grpcadapter.NewObservabilityServer(tc.ledgers, tc.decisions, tc.corrs, nil)
		})
	}

	h := newTestHarness(t)
	if h.srv == nil {
		t.Fatal("NewObservabilityServer returned nil")
	}
}

// -----------------------------------------------------------------------------
// TokenUsageLedger RPCs
// -----------------------------------------------------------------------------

func TestAppendTokenUsage_DirectAppend(t *testing.T) {
	h := newTestHarness(t)

	res, err := h.srv.AppendTokenUsage(context.Background(), &observabilityv1.AppendTokenUsageRequest{
		TenantId: grpcTenant, Gcid: grpcGcid, Agid: grpcAgid, ModelId: grpcModel,
		PromptTokens: 10, CompletionTokens: 5, CostUsdMicros: 123_456,
		TraceId: grpcTraceID, SpanId: grpcSpanID,
	})
	if err != nil {
		t.Fatalf("AppendTokenUsage: %v", err)
	}
	if res.Entry == nil {
		t.Fatal("expected entry in response")
	}
	if res.Entry.LedgerId == "" {
		t.Error("expected server-stamped ledger_id")
	}
	if res.Entry.TenantId != grpcTenant || res.Entry.ModelId != grpcModel {
		t.Errorf("entry tenant/model = %q/%q; want %q/%q", res.Entry.TenantId, res.Entry.ModelId, grpcTenant, grpcModel)
	}
	if res.OutboxEventId != "" {
		t.Errorf("expected empty outbox_event_id on direct path; got %q", res.OutboxEventId)
	}

	entries, err := h.ledgers.List(context.Background(), grpcTenant, ledger.ListFilter{})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("ledger entries = %d; want 1", len(entries))
	}
}

func TestAppendTokenUsage_ValidationErrors(t *testing.T) {
	h := newTestHarness(t)

	if _, err := h.srv.AppendTokenUsage(context.Background(), nil); err == nil {
		t.Fatal("expected error for nil request")
	} else if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("nil req code = %v; want InvalidArgument", status.Code(err))
	}

	if _, err := h.srv.AppendTokenUsage(context.Background(), &observabilityv1.AppendTokenUsageRequest{
		TenantId: "", Gcid: grpcGcid, ModelId: grpcModel,
		TraceId: grpcTraceID, SpanId: grpcSpanID,
	}); err == nil || status.Code(err) != codes.InvalidArgument {
		t.Fatalf("empty tenant: code = %v; want InvalidArgument", status.Code(err))
	}

	// bad trace_id fails domain validation -> InvalidArgument
	if _, err := h.srv.AppendTokenUsage(context.Background(), &observabilityv1.AppendTokenUsageRequest{
		TenantId: grpcTenant, Gcid: grpcGcid, ModelId: grpcModel,
		PromptTokens: -1, TraceId: grpcTraceID, SpanId: grpcSpanID,
	}); err == nil || status.Code(err) != codes.InvalidArgument {
		t.Fatalf("negative tokens: code = %v; want InvalidArgument", status.Code(err))
	}
}

func TestAppendTokenUsage_DirectAppendRepoFailure(t *testing.T) {
	srv := grpcadapter.NewObservabilityServer(&stubLedgerRepo{appendErr: errors.New("boom")}, inmem.NewDecisionRepository(), inmem.NewCorrelationRepository(), nil)
	_, err := srv.AppendTokenUsage(context.Background(), &observabilityv1.AppendTokenUsageRequest{
		TenantId: grpcTenant, Gcid: grpcGcid, ModelId: grpcModel,
		TraceId: grpcTraceID, SpanId: grpcSpanID,
	})
	expectCode(t, err, codes.Internal)
}

// recordingOutbox implements ledger.OutboxRecorder and captures writes.
type recordingOutbox struct {
	records []ledger.OutboxRecord
	err     error
}

func (o *recordingOutbox) RecordOutboxEvent(_ context.Context, r ledger.OutboxRecord) error {
	if o.err != nil {
		return o.err
	}
	o.records = append(o.records, r)
	return nil
}

func TestAppendTokenUsage_ViaLedgerHook(t *testing.T) {
	ledgerRepo := inmem.NewLedgerRepository()
	outbox := &recordingOutbox{}
	hook := ledger.NewLedgerHook(ledger.LedgerHookConfig{
		Ledger:               ledgerRepo,
		Outbox:               outbox,
		SourceProject:        "chora-obs",
		SourceService:        "chora-observability",
		PricingConfigVersion: "pricing-2026-05-13",
	})
	srv := grpcadapter.NewObservabilityServer(ledgerRepo, inmem.NewDecisionRepository(), inmem.NewCorrelationRepository(), hook)

	res, err := srv.AppendTokenUsage(context.Background(), &observabilityv1.AppendTokenUsageRequest{
		TenantId: grpcTenant, Gcid: grpcGcid, ModelId: grpcModel,
		PromptTokens: 10, CostUsdMicros: 999_950,
		TraceId: grpcTraceID, SpanId: grpcSpanID, Traceparent: " 00-0000-0000 ",
	})
	if err != nil {
		t.Fatalf("AppendTokenUsage: %v", err)
	}
	if res.OutboxEventId == "" {
		t.Fatal("expected outbox_event_id from hook path")
	}
	if res.PricingConfigVersion != "pricing-2026-05-13" {
		t.Errorf("pricing config version = %q; want pricing-2026-05-13", res.PricingConfigVersion)
	}
	if res.Entry == nil || res.Entry.LedgerId == "" {
		t.Fatal("expected stamped ledger entry in response")
	}
	if len(outbox.records) != 1 {
		t.Fatalf("outbox records = %d; want 1", len(outbox.records))
	}
	if outbox.records[0].Topic != ledger.CanonicalTokenUsageTopic {
		t.Errorf("outbox topic = %q; want %q", outbox.records[0].Topic, ledger.CanonicalTokenUsageTopic)
	}
	entries, err := ledgerRepo.List(context.Background(), grpcTenant, ledger.ListFilter{})
	if err != nil || len(entries) != 1 {
		t.Fatalf("hook path should append to the ledger repo (n=%d, err=%v)", len(entries), err)
	}
}

func TestAppendTokenUsage_LedgerHookFailure(t *testing.T) {
	ledgerRepo := inmem.NewLedgerRepository()
	hook := ledger.NewLedgerHook(ledger.LedgerHookConfig{
		Ledger: ledgerRepo,
		Outbox: &recordingOutbox{err: errors.New("outbox down")},
	})
	srv := grpcadapter.NewObservabilityServer(ledgerRepo, inmem.NewDecisionRepository(), inmem.NewCorrelationRepository(), hook)

	_, err := srv.AppendTokenUsage(context.Background(), &observabilityv1.AppendTokenUsageRequest{
		TenantId: grpcTenant, Gcid: grpcGcid, ModelId: grpcModel,
		TraceId: grpcTraceID, SpanId: grpcSpanID,
	})
	if err == nil || status.Code(err) != codes.Internal {
		t.Fatalf("code = %v; want Internal", status.Code(err))
	}
}

func TestListTokenUsage(t *testing.T) {
	h := newTestHarness(t)

	base := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	for i, ts := range []time.Time{base, base.Add(2 * time.Hour), base.Add(5 * time.Hour)} {
		e, err := ledger.New(ledger.NewParams{
			TenantID: grpcTenant, Gcid: grpcGcid, ModelID: grpcModel,
			CostUsdMicros: int64(100 * (i + 1)), TraceID: grpcTraceID, SpanID: grpcSpanID,
			RecordedAt: ts,
		})
		if err != nil {
			t.Fatalf("ledger.New: %v", err)
		}
		if err := h.ledgers.Append(context.Background(), e); err != nil {
			t.Fatalf("append: %v", err)
		}
	}

	res, err := h.srv.ListTokenUsage(context.Background(), &observabilityv1.ListTokenUsageRequest{
		TenantId: grpcTenant,
		Window: &observabilityv1.TimeWindow{
			From:  timestamppb.New(base.Add(time.Hour)),
			Until: timestamppb.New(base.Add(3 * time.Hour)),
		},
		Page: &observabilityv1.PageRequest{Limit: 10, Offset: 0},
	})
	if err != nil {
		t.Fatalf("ListTokenUsage: %v", err)
	}
	if len(res.Items) != 1 {
		t.Fatalf("items = %d; want 1 within window", len(res.Items))
	}
	if res.Total != 1 {
		t.Errorf("total = %d; want 1", res.Total)
	}
}

func TestListTokenUsage_Errors(t *testing.T) {
	empty := &grpcadapter.ObservabilityServer{}
	if _, err := empty.ListTokenUsage(context.Background(), nil); err == nil {
		t.Fatal("expected error for nil request")
	}
	if _, err := empty.ListTokenUsage(context.Background(), &observabilityv1.ListTokenUsageRequest{}); err == nil ||
		status.Code(err) != codes.InvalidArgument {
		t.Fatalf("empty tenant: code = %v; want InvalidArgument", status.Code(err))
	}

	srv := grpcadapter.NewObservabilityServer(&stubLedgerRepo{listErr: errors.New("boom")}, inmem.NewDecisionRepository(), inmem.NewCorrelationRepository(), nil)
	_, err := srv.ListTokenUsage(context.Background(), &observabilityv1.ListTokenUsageRequest{TenantId: grpcTenant})
	if err == nil || status.Code(err) != codes.Internal {
		t.Fatalf("code = %v; want Internal", status.Code(err))
	}
}

func TestSumTokenUsageCost(t *testing.T) {
	h := newTestHarness(t)

	for _, c := range []int64{100, 200, 300} {
		e, _ := ledger.New(ledger.NewParams{
			TenantID: grpcTenant, Gcid: grpcGcid, ModelID: grpcModel,
			CostUsdMicros: c, TraceID: grpcTraceID, SpanID: grpcSpanID,
			RecordedAt: time.Date(2026, 5, 10, 0, 0, 0, 0, time.UTC),
		})
		_ = h.ledgers.Append(context.Background(), e)
	}

	res, err := h.srv.SumTokenUsageCost(context.Background(), &observabilityv1.SumTokenUsageCostRequest{
		TenantId: grpcTenant,
		Window: &observabilityv1.TimeWindow{
			From:  timestamppb.New(time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)),
			Until: timestamppb.New(time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)),
		},
	})
	if err != nil {
		t.Fatalf("SumTokenUsageCost: %v", err)
	}
	if res.TotalCostUsdMicros != 600 {
		t.Errorf("total micros = %d; want 600", res.TotalCostUsdMicros)
	}
	if res.TotalCostUsd != "0.000600" {
		t.Errorf("total usd = %q; want 0.000600", res.TotalCostUsd)
	}
	if res.EntryCount != 3 {
		t.Errorf("entry count = %d; want 3", res.EntryCount)
	}

	// sum over an empty window yields zero
	empty, err := h.srv.SumTokenUsageCost(context.Background(), &observabilityv1.SumTokenUsageCostRequest{TenantId: "other-tenant"})
	if err != nil {
		t.Fatalf("empty sum: %v", err)
	}
	if empty.EntryCount != 0 || empty.TotalCostUsdMicros != 0 {
		t.Errorf("empty sum = (%d, %d); want (0, 0)", empty.TotalCostUsdMicros, empty.EntryCount)
	}
}

func TestSumTokenUsageCost_Errors(t *testing.T) {
	empty := &grpcadapter.ObservabilityServer{}
	if _, err := empty.SumTokenUsageCost(context.Background(), nil); err == nil {
		t.Fatal("expected error for nil request")
	}
	if _, err := empty.SumTokenUsageCost(context.Background(), &observabilityv1.SumTokenUsageCostRequest{}); err == nil ||
		status.Code(err) != codes.InvalidArgument {
		t.Fatalf("empty tenant: code = %v; want InvalidArgument", status.Code(err))
	}

	srv := grpcadapter.NewObservabilityServer(&stubLedgerRepo{sumErr: errors.New("boom")}, inmem.NewDecisionRepository(), inmem.NewCorrelationRepository(), nil)
	_, err := srv.SumTokenUsageCost(context.Background(), &observabilityv1.SumTokenUsageCostRequest{TenantId: grpcTenant})
	if err == nil || status.Code(err) != codes.Internal {
		t.Fatalf("code = %v; want Internal", status.Code(err))
	}
}

// -----------------------------------------------------------------------------
// AgentDecisionLog RPCs
// -----------------------------------------------------------------------------

func TestAppendAgentDecision(t *testing.T) {
	h := newTestHarness(t)

	req := &observabilityv1.AppendAgentDecisionRequest{
		TenantId: grpcTenant, Agid: grpcAgid,
		DecisionType: observabilityv1.AgentDecisionType_AGENT_DECISION_TYPE_ESCALATE,
		Reason:       "pii detected",
		RiskTier:     observabilityv1.AgentRiskTier_AGENT_RISK_TIER_HIGH,
		CorrelationId: grpcCorrID,
		Reasoning: &observabilityv1.AppendReasoningSummary{
			InputText: "prompt text", OutputText: "response text", LatencyMs: 350,
		},
	}
	res, err := h.srv.AppendAgentDecision(context.Background(), req)
	if err != nil {
		t.Fatalf("AppendAgentDecision: %v", err)
	}
	if res.Entry.LogId == "" {
		t.Fatal("expected server-stamped log_id")
	}
	if res.Entry.DecisionType != observabilityv1.AgentDecisionType_AGENT_DECISION_TYPE_ESCALATE {
		t.Errorf("decision type = %v; want ESCALATE", res.Entry.DecisionType)
	}
	if res.Entry.RiskTier != observabilityv1.AgentRiskTier_AGENT_RISK_TIER_HIGH {
		t.Errorf("risk tier = %v; want HIGH", res.Entry.RiskTier)
	}
	if res.Entry.Reasoning == nil {
		t.Fatal("expected reasoning on response entry")
	}
	if res.Entry.Reasoning.InputHash == "" {
		t.Error("expected hashed input")
	}

	logs, err := h.decisions.List(context.Background(), grpcTenant, decision.ListFilter{})
	if err != nil || len(logs) != 1 {
		t.Fatalf("decisions listed = %d (err=%v); want 1", len(logs), err)
	}
}

func TestAppendAgentDecision_NoReasoning(t *testing.T) {
	h := newTestHarness(t)
	res, err := h.srv.AppendAgentDecision(context.Background(), &observabilityv1.AppendAgentDecisionRequest{
		TenantId: grpcTenant, Agid: grpcAgid,
		DecisionType: observabilityv1.AgentDecisionType_AGENT_DECISION_TYPE_ROUTE,
		RiskTier:     observabilityv1.AgentRiskTier_AGENT_RISK_TIER_LOW,
		CorrelationId: grpcCorrID,
	})
	if err != nil {
		t.Fatalf("AppendAgentDecision: %v", err)
	}
	if res.Entry.Reasoning != nil {
		t.Error("expected no reasoning on response entry")
	}
}

func TestAppendAgentDecision_ValidationErrors(t *testing.T) {
	h := newTestHarness(t)

	if _, err := h.srv.AppendAgentDecision(context.Background(), nil); err == nil || status.Code(err) != codes.InvalidArgument {
		t.Fatalf("nil req code = %v; want InvalidArgument", status.Code(err))
	}

	if _, err := h.srv.AppendAgentDecision(context.Background(), &observabilityv1.AppendAgentDecisionRequest{
		Agid: grpcAgid,
		DecisionType: observabilityv1.AgentDecisionType_AGENT_DECISION_TYPE_ROUTE,
		RiskTier:     observabilityv1.AgentRiskTier_AGENT_RISK_TIER_LOW,
		CorrelationId: grpcCorrID,
	}); err == nil || status.Code(err) != codes.InvalidArgument {
		t.Fatalf("empty tenant code = %v; want InvalidArgument", status.Code(err))
	}

	// UNSPECIFIED decision type -> domain rejects -> InvalidArgument
	_, err := h.srv.AppendAgentDecision(context.Background(), &observabilityv1.AppendAgentDecisionRequest{
		TenantId: grpcTenant, Agid: grpcAgid,
		DecisionType: observabilityv1.AgentDecisionType_AGENT_DECISION_TYPE_UNSPECIFIED,
		RiskTier:     observabilityv1.AgentRiskTier_AGENT_RISK_TIER_LOW,
		CorrelationId: grpcCorrID,
	})
	expectCode(t, err, codes.InvalidArgument)

	// invalid reasoning latency -> InvalidArgument
	_, err = h.srv.AppendAgentDecision(context.Background(), &observabilityv1.AppendAgentDecisionRequest{
		TenantId: grpcTenant, Agid: grpcAgid,
		DecisionType: observabilityv1.AgentDecisionType_AGENT_DECISION_TYPE_ROUTE,
		RiskTier:     observabilityv1.AgentRiskTier_AGENT_RISK_TIER_LOW,
		CorrelationId: grpcCorrID,
		Reasoning: &observabilityv1.AppendReasoningSummary{LatencyMs: -1},
	})
	expectCode(t, err, codes.InvalidArgument)
}

func TestAppendAgentDecision_RepoFailure(t *testing.T) {
	srv := grpcadapter.NewObservabilityServer(inmem.NewLedgerRepository(), &stubDecisionRepo{appendErr: errors.New("boom")}, inmem.NewCorrelationRepository(), nil)
	_, err := srv.AppendAgentDecision(context.Background(), &observabilityv1.AppendAgentDecisionRequest{
		TenantId: grpcTenant, Agid: grpcAgid,
		DecisionType: observabilityv1.AgentDecisionType_AGENT_DECISION_TYPE_ROUTE,
		RiskTier:     observabilityv1.AgentRiskTier_AGENT_RISK_TIER_LOW,
		CorrelationId: grpcCorrID,
	})
	expectCode(t, err, codes.Internal)
}

func TestListAgentDecisions(t *testing.T) {
	h := newTestHarness(t)

	for i := 0; i < 3; i++ {
		d, err := decision.New(decision.NewParams{
			TenantID: grpcTenant, Agid: grpcAgid,
			DecisionType: decision.TypeRoute, RiskTier: decision.TierLow,
			CorrelationID: grpcCorrID,
		})
		if err != nil {
			t.Fatalf("decision.New: %v", err)
		}
		if err := h.decisions.Append(context.Background(), d); err != nil {
			t.Fatalf("append: %v", err)
		}
	}

	// Page filter (limit=2) + a permissive window (past -> future) to also
	// exercise the Window From/Until branches.
	res, err := h.srv.ListAgentDecisions(context.Background(), &observabilityv1.ListAgentDecisionsRequest{
		TenantId: grpcTenant, Agid: grpcAgid,
		Page: &observabilityv1.PageRequest{Limit: 2, Offset: 0},
		Window: &observabilityv1.TimeWindow{
			From:  timestamppb.New(time.Now().Add(-time.Hour)),
			Until: timestamppb.New(time.Now().Add(time.Hour)),
		},
	})
	if err != nil {
		t.Fatalf("ListAgentDecisions: %v", err)
	}
	if len(res.Items) != 2 || res.Total != 2 {
		t.Errorf("items/total = %d/%d; want 2/2", len(res.Items), res.Total)
	}
}

func TestListAgentDecisions_Errors(t *testing.T) {
	empty := &grpcadapter.ObservabilityServer{}
	if _, err := empty.ListAgentDecisions(context.Background(), nil); err == nil {
		t.Fatal("expected error for nil request")
	}
	if _, err := empty.ListAgentDecisions(context.Background(), &observabilityv1.ListAgentDecisionsRequest{}); err == nil ||
		status.Code(err) != codes.InvalidArgument {
		t.Fatalf("empty tenant: code = %v; want InvalidArgument", status.Code(err))
	}
	srv := grpcadapter.NewObservabilityServer(inmem.NewLedgerRepository(), &stubDecisionRepo{listErr: errors.New("boom")}, inmem.NewCorrelationRepository(), nil)
	_, err := srv.ListAgentDecisions(context.Background(), &observabilityv1.ListAgentDecisionsRequest{TenantId: grpcTenant})
	expectCode(t, err, codes.Internal)
}

func TestGetAgentDecision(t *testing.T) {
	h := newTestHarness(t)

	d, _ := decision.New(decision.NewParams{
		TenantID: grpcTenant, Agid: grpcAgid,
		DecisionType: decision.TypeRespond, RiskTier: decision.TierMedium,
		CorrelationID: grpcCorrID,
	})
	if err := h.decisions.Append(context.Background(), d); err != nil {
		t.Fatalf("append: %v", err)
	}

	res, err := h.srv.GetAgentDecision(context.Background(), &observabilityv1.GetAgentDecisionRequest{
		TenantId: grpcTenant, LogId: d.LogID,
	})
	if err != nil {
		t.Fatalf("GetAgentDecision: %v", err)
	}
	if res.Entry.LogId != d.LogID {
		t.Errorf("log id = %q; want %q", res.Entry.LogId, d.LogID)
	}

	// not found -> NotFound mapping
	_, err = h.srv.GetAgentDecision(context.Background(), &observabilityv1.GetAgentDecisionRequest{
		TenantId: grpcTenant, LogId: "01970000-0000-7000-8000-ffffffffffff",
	})
	expectCode(t, err, codes.NotFound)
}

func TestGetAgentDecision_Errors(t *testing.T) {
	empty := &grpcadapter.ObservabilityServer{}
	if _, err := empty.GetAgentDecision(context.Background(), nil); err == nil {
		t.Fatal("expected error for nil request")
	}
	if _, err := empty.GetAgentDecision(context.Background(), &observabilityv1.GetAgentDecisionRequest{}); err == nil ||
		status.Code(err) != codes.InvalidArgument {
		t.Fatalf("empty tenant: code = %v; want InvalidArgument", status.Code(err))
	}
	if _, err := empty.GetAgentDecision(context.Background(), &observabilityv1.GetAgentDecisionRequest{
		TenantId: grpcTenant, LogId: "",
	}); err == nil || status.Code(err) != codes.InvalidArgument {
		t.Fatalf("empty log id: code = %v; want InvalidArgument", status.Code(err))
	}

	srv := grpcadapter.NewObservabilityServer(inmem.NewLedgerRepository(), &stubDecisionRepo{getErr: errors.New("boom")}, inmem.NewCorrelationRepository(), nil)
	_, err := srv.GetAgentDecision(context.Background(), &observabilityv1.GetAgentDecisionRequest{
		TenantId: grpcTenant, LogId: "01970000-0000-7000-8000-ffffffffffff",
	})
	expectCode(t, err, codes.Internal)
}

func TestGetAuditEvents(t *testing.T) {
	h := newTestHarness(t)

	// validation: nil + empty tenant
	empty := &grpcadapter.ObservabilityServer{}
	if _, err := empty.GetAuditEvents(context.Background(), nil); err == nil {
		t.Fatal("expected error for nil request")
	}
	if _, err := empty.GetAuditEvents(context.Background(), &observabilityv1.GetAuditEventsRequest{}); err == nil ||
		status.Code(err) != codes.InvalidArgument {
		t.Fatalf("empty tenant: code = %v; want InvalidArgument", status.Code(err))
	}

	for i := 0; i < 2; i++ {
		d, _ := decision.New(decision.NewParams{
			TenantID: grpcTenant, Agid: grpcAgid,
			DecisionType: decision.TypeRefuse, RiskTier: decision.TierCritical,
			CorrelationID: grpcCorrID,
		})
		_ = h.decisions.Append(context.Background(), d)
	}

	res, err := h.srv.GetAuditEvents(context.Background(), &observabilityv1.GetAuditEventsRequest{TenantId: grpcTenant})
	if err != nil {
		t.Fatalf("GetAuditEvents: %v", err)
	}
	if len(res.Items) != 2 || res.Total != 2 {
		t.Errorf("items/total = %d/%d; want 2/2", len(res.Items), res.Total)
	}

	// repository error -> Internal
	errSrv := grpcadapter.NewObservabilityServer(inmem.NewLedgerRepository(), &stubDecisionRepo{listErr: errors.New("boom")}, inmem.NewCorrelationRepository(), nil)
	_, err = errSrv.GetAuditEvents(context.Background(), &observabilityv1.GetAuditEventsRequest{TenantId: grpcTenant})
	expectCode(t, err, codes.Internal)
}

// -----------------------------------------------------------------------------
// TraceCorrelation RPCs
// -----------------------------------------------------------------------------

func TestRegisterCorrelation(t *testing.T) {
	h := newTestHarness(t)

	res, err := h.srv.RegisterCorrelation(context.Background(), &observabilityv1.RegisterCorrelationRequest{
		TenantId: grpcTenant, CorrelationId: grpcCorrID,
		TraceId: grpcTraceID, ParentSpanId: grpcSpanID,
		ChildSpans: []string{grpcChild1, grpcChild2},
	})
	if err != nil {
		t.Fatalf("RegisterCorrelation: %v", err)
	}
	if res.Entry.TraceId != grpcTraceID {
		t.Errorf("trace id = %q; want %q", res.Entry.TraceId, grpcTraceID)
	}
	if len(res.Entry.ChildSpans) != 2 {
		t.Errorf("child spans = %d; want 2", len(res.Entry.ChildSpans))
	}

	got, err := h.corrs.Get(context.Background(), grpcTenant, grpcCorrID)
	if err != nil {
		t.Fatalf("correlation get: %v", err)
	}
	if len(got.ChildSpans) != 2 {
		t.Errorf("persisted child spans = %d; want 2", len(got.ChildSpans))
	}
}

func TestRegisterCorrelation_InvalidChildSpan(t *testing.T) {
	h := newTestHarness(t)
	_, err := h.srv.RegisterCorrelation(context.Background(), &observabilityv1.RegisterCorrelationRequest{
		TenantId: grpcTenant, CorrelationId: grpcCorrID,
		TraceId: grpcTraceID, ParentSpanId: grpcSpanID,
		ChildSpans: []string{"not-hex!"},
	})
	expectCode(t, err, codes.InvalidArgument)
}

func TestRegisterCorrelation_Errors(t *testing.T) {
	h := newTestHarness(t)
	if _, err := h.srv.RegisterCorrelation(context.Background(), nil); err == nil || status.Code(err) != codes.InvalidArgument {
		t.Fatalf("nil req code = %v; want InvalidArgument", status.Code(err))
	}
	if _, err := h.srv.RegisterCorrelation(context.Background(), &observabilityv1.RegisterCorrelationRequest{CorrelationId: grpcCorrID}); err == nil {
		t.Fatal("expected error for empty tenant")
	}
	// invalid trace_id -> correlation.New rejects -> InvalidArgument
	_, err := h.srv.RegisterCorrelation(context.Background(), &observabilityv1.RegisterCorrelationRequest{
		TenantId: grpcTenant, CorrelationId: grpcCorrID,
		TraceId: "bogus", ParentSpanId: grpcSpanID,
	})
	expectCode(t, err, codes.InvalidArgument)

	srv := grpcadapter.NewObservabilityServer(inmem.NewLedgerRepository(), inmem.NewDecisionRepository(), &stubCorrelationRepo{registerErr: errors.New("boom")}, nil)
	_, err = srv.RegisterCorrelation(context.Background(), &observabilityv1.RegisterCorrelationRequest{
		TenantId: grpcTenant, CorrelationId: grpcCorrID,
		TraceId: grpcTraceID, ParentSpanId: grpcSpanID,
	})
	expectCode(t, err, codes.Internal)
}

func TestGetCorrelation(t *testing.T) {
	h := newTestHarness(t)

	c, _ := correlation.New(correlation.NewParams{
		TenantID: grpcTenant, CorrelationID: grpcCorrID,
		TraceID: grpcTraceID, ParentSpanID: grpcSpanID,
	})
	_ = c.AttachChildSpan(grpcChild1)
	_ = h.corrs.Register(context.Background(), c)

	res, err := h.srv.GetCorrelation(context.Background(), &observabilityv1.GetCorrelationRequest{
		TenantId: grpcTenant, CorrelationId: grpcCorrID,
	})
	if err != nil {
		t.Fatalf("GetCorrelation: %v", err)
	}
	if res.Entry.ParentSpanId != grpcSpanID || len(res.Entry.ChildSpans) != 1 {
		t.Errorf("parent/children = %q/%d; want %q/1", res.Entry.ParentSpanId, len(res.Entry.ChildSpans), grpcSpanID)
	}

	// not found
	_, err = h.srv.GetCorrelation(context.Background(), &observabilityv1.GetCorrelationRequest{
		TenantId: grpcTenant, CorrelationId: "missing",
	})
	expectCode(t, err, codes.NotFound)
}

func TestGetCorrelation_Errors(t *testing.T) {
	empty := &grpcadapter.ObservabilityServer{}
	if _, err := empty.GetCorrelation(context.Background(), nil); err == nil {
		t.Fatal("expected error for nil request")
	}
	if _, err := empty.GetCorrelation(context.Background(), &observabilityv1.GetCorrelationRequest{}); err == nil ||
		status.Code(err) != codes.InvalidArgument {
		t.Fatalf("empty tenant: code = %v; want InvalidArgument", status.Code(err))
	}
	if _, err := empty.GetCorrelation(context.Background(), &observabilityv1.GetCorrelationRequest{
		TenantId: grpcTenant, CorrelationId: "",
	}); err == nil || status.Code(err) != codes.InvalidArgument {
		t.Fatalf("empty correlation id: code = %v; want InvalidArgument", status.Code(err))
	}

	srv := grpcadapter.NewObservabilityServer(inmem.NewLedgerRepository(), inmem.NewDecisionRepository(), &stubCorrelationRepo{getErr: errors.New("boom")}, nil)
	_, err := srv.GetCorrelation(context.Background(), &observabilityv1.GetCorrelationRequest{
		TenantId: grpcTenant, CorrelationId: grpcCorrID,
	})
	expectCode(t, err, codes.Internal)
}