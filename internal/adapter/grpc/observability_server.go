// Package grpcadapter — gRPC server for the chora-observability service.
//
// Bridges the v1 Observability proto service to the domain ports owned by
// internal/domain/{ledger, decision, correlation}/. Mirrors the
// chora-identity ManaProtoServer + chora-tenancy gRPC bridges (the canonical
// Wave-1 ADR-140 remediation pattern).
//
// Source-of-truth:
//   - chora-contracts/proto/services/observability/v1/observability.proto
//   - .claude/skills/ai-cost-tracking/SKILL.md
//   - .claude/skills/ai-observability-cloud-trace/SKILL.md
//   - .claude/skills/imda-governance-4-dimensions/SKILL.md
//   - services/chora-observability/internal/adapter/http/handler.go
//
// Per docs/m13/grpc-mass-remediation-2026-05-16.md Wave-1 fail-loud rule:
// the gRPC server registers unconditionally — there is no
// `if env unset { skip }` shim. HTTP handlers stay live in parallel
// through Wave-3 soak per the same doc.
//
// Per feedback_no_stubs_real_wiring: the server is wired directly to the
// real domain repositories — same instance the HTTP handler uses, so the
// two protocol surfaces share state.
package grpcadapter

import (
	"context"
	"errors"
	"fmt"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	observabilityv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/services/observability/v1"

	"github.com/apollo-chora/chora-observability/internal/domain/correlation"
	"github.com/apollo-chora/chora-observability/internal/domain/decision"
	"github.com/apollo-chora/chora-observability/internal/domain/ledger"
)

// ObservabilityServer implements observabilityv1.ObservabilityServer.
//
// Dependencies are the domain persistence ports. The ledgerHook is OPTIONAL;
// when non-nil, AppendTokenUsage delegates to it so every write also queues
// a `chora.observability.token_usage.recorded.v1` outbox row. When nil the
// server falls back to the direct Append path (mirrors the HTTP handler's
// M10 baseline behaviour).
type ObservabilityServer struct {
	observabilityv1.UnimplementedObservabilityServer

	ledgers      ledger.Repository
	decisions    decision.Repository
	correlations correlation.Repository
	ledgerHook   *ledger.LedgerHook // optional
}

// auditEventsCap is the maximum number of audit events GetAuditEvents
// returns in a single call. Mirrors the underlying decision.Repository
// List cap (decision.ListFilter.Limit max 1000 per domain contract).
const auditEventsCap = 1000

// NewObservabilityServer constructs the gRPC server. ledgerHook may be nil.
func NewObservabilityServer(
	ledgers ledger.Repository,
	decisions decision.Repository,
	correlations correlation.Repository,
	ledgerHook *ledger.LedgerHook,
) *ObservabilityServer {
	if ledgers == nil {
		panic("grpcadapter.NewObservabilityServer: ledger repository is required")
	}
	if decisions == nil {
		panic("grpcadapter.NewObservabilityServer: decision repository is required")
	}
	if correlations == nil {
		panic("grpcadapter.NewObservabilityServer: correlation repository is required")
	}
	return &ObservabilityServer{
		ledgers:      ledgers,
		decisions:    decisions,
		correlations: correlations,
		ledgerHook:   ledgerHook,
	}
}

// -----------------------------------------------------------------------------
// TokenUsageLedger RPCs
// -----------------------------------------------------------------------------

// AppendTokenUsage persists a new TokenUsageLedger entry. Delegates to
// LedgerHook (outbox + ledger atomic) when wired; otherwise direct Append.
func (s *ObservabilityServer) AppendTokenUsage(ctx context.Context, req *observabilityv1.AppendTokenUsageRequest) (*observabilityv1.AppendTokenUsageResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if req.GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "tenant_id is required")
	}

	if s.ledgerHook != nil {
		// Pin OccurredAt before delegating so the response can stamp the
		// same RecordedAt the hook persisted (RecordResult does not surface
		// the timestamp; this is the canonical alignment pattern).
		occurredAt := time.Now().UTC()
		res, err := s.ledgerHook.Record(ctx, ledger.RecordParams{
			TenantID:         req.GetTenantId(),
			Gcid:             req.GetGcid(),
			AgentID:          req.GetAgid(),
			ModelID:          req.GetModelId(),
			PromptTokens:     int(req.GetPromptTokens()),
			CompletionTokens: int(req.GetCompletionTokens()),
			CostUsdMicros:    req.GetCostUsdMicros(),
			TraceID:          req.GetTraceId(),
			SpanID:           req.GetSpanId(),
			Traceparent:      req.GetTraceparent(),
			OccurredAt:       occurredAt,
		})
		if err != nil {
			return nil, status.Errorf(codes.Internal, "ledger hook record: %v", err)
		}
		entry := &observabilityv1.TokenUsageEntry{
			LedgerId:         res.LedgerID,
			TenantId:         req.GetTenantId(),
			Gcid:             req.GetGcid(),
			Agid:             req.GetAgid(),
			ModelId:          req.GetModelId(),
			PromptTokens:     req.GetPromptTokens(),
			CompletionTokens: req.GetCompletionTokens(),
			CostUsdMicros:    req.GetCostUsdMicros(),
			TraceId:          req.GetTraceId(),
			SpanId:           req.GetSpanId(),
			RecordedAt:       timestamppb.New(occurredAt),
		}
		return &observabilityv1.AppendTokenUsageResponse{
			Entry:                entry,
			OutboxEventId:        res.OutboxEventID,
			PricingConfigVersion: res.PricingConfigVersion,
		}, nil
	}

	// Direct-Append fallback path.
	e, err := ledger.New(ledger.NewParams{
		TenantID:         req.GetTenantId(),
		Gcid:             req.GetGcid(),
		Agid:             req.GetAgid(),
		ModelID:          req.GetModelId(),
		PromptTokens:     int(req.GetPromptTokens()),
		CompletionTokens: int(req.GetCompletionTokens()),
		CostUsdMicros:    req.GetCostUsdMicros(),
		TraceID:          req.GetTraceId(),
		SpanID:           req.GetSpanId(),
	})
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "ledger validation: %v", err)
	}
	if err := s.ledgers.Append(ctx, e); err != nil {
		return nil, status.Errorf(codes.Internal, "ledger append: %v", err)
	}
	return &observabilityv1.AppendTokenUsageResponse{
		Entry: tokenUsageEntryToProto(e),
	}, nil
}

// ListTokenUsage returns ledger entries for the tenant matching the filter.
func (s *ObservabilityServer) ListTokenUsage(ctx context.Context, req *observabilityv1.ListTokenUsageRequest) (*observabilityv1.ListTokenUsageResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if req.GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "tenant_id is required")
	}
	f := ledger.ListFilter{}
	if w := req.GetWindow(); w != nil {
		if w.GetFrom() != nil {
			f.From = w.GetFrom().AsTime()
		}
		if w.GetUntil() != nil {
			f.To = w.GetUntil().AsTime()
		}
	}
	if p := req.GetPage(); p != nil {
		f.Limit = int(p.GetLimit())
		f.Offset = int(p.GetOffset())
	}

	items, err := s.ledgers.List(ctx, req.GetTenantId(), f)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "ledger list: %v", err)
	}
	out := make([]*observabilityv1.TokenUsageEntry, 0, len(items))
	for _, e := range items {
		out = append(out, tokenUsageEntryToProto(e))
	}
	return &observabilityv1.ListTokenUsageResponse{
		Items: out,
		Total: int32(len(out)),
	}, nil
}

// SumTokenUsageCost aggregates ledger cost over the filter window.
func (s *ObservabilityServer) SumTokenUsageCost(ctx context.Context, req *observabilityv1.SumTokenUsageCostRequest) (*observabilityv1.SumTokenUsageCostResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if req.GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "tenant_id is required")
	}
	f := ledger.ListFilter{}
	if w := req.GetWindow(); w != nil {
		if w.GetFrom() != nil {
			f.From = w.GetFrom().AsTime()
		}
		if w.GetUntil() != nil {
			f.To = w.GetUntil().AsTime()
		}
	}
	total, count, err := s.ledgers.SumCost(ctx, req.GetTenantId(), f)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "ledger sum: %v", err)
	}
	return &observabilityv1.SumTokenUsageCostResponse{
		TotalCostUsdMicros: total,
		TotalCostUsd:       fmt.Sprintf("%.6f", float64(total)/1_000_000.0),
		EntryCount:         int32(count),
	}, nil
}

// -----------------------------------------------------------------------------
// AgentDecisionLog RPCs
// -----------------------------------------------------------------------------

// AppendAgentDecision persists a new AgentDecisionLog entry.
func (s *ObservabilityServer) AppendAgentDecision(ctx context.Context, req *observabilityv1.AppendAgentDecisionRequest) (*observabilityv1.AppendAgentDecisionResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if req.GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "tenant_id is required")
	}

	var rs *decision.ReasoningSummary
	if r := req.GetReasoning(); r != nil {
		built, err := decision.NewReasoningSummary(decision.ReasoningParams{
			Input:     r.GetInputText(),
			Output:    r.GetOutputText(),
			LatencyMs: int(r.GetLatencyMs()),
			Summary:   r.GetSummary(),
		})
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "reasoning summary: %v", err)
		}
		rs = built
	}

	d, err := decision.New(decision.NewParams{
		TenantID:      req.GetTenantId(),
		Agid:          req.GetAgid(),
		DecisionType:  decisionTypeFromProto(req.GetDecisionType()),
		Reason:        req.GetReason(),
		RiskTier:      riskTierFromProto(req.GetRiskTier()),
		CorrelationID: req.GetCorrelationId(),
		Traceparent:   req.GetTraceparent(),
		Reasoning:     rs,
	})
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "decision validation: %v", err)
	}
	if err := s.decisions.Append(ctx, d); err != nil {
		return nil, status.Errorf(codes.Internal, "decision append: %v", err)
	}
	return &observabilityv1.AppendAgentDecisionResponse{
		Entry: agentDecisionEntryToProto(d),
	}, nil
}

// ListAgentDecisions returns decision logs for the tenant matching the filter.
func (s *ObservabilityServer) ListAgentDecisions(ctx context.Context, req *observabilityv1.ListAgentDecisionsRequest) (*observabilityv1.ListAgentDecisionsResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if req.GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "tenant_id is required")
	}
	f := decision.ListFilter{Agid: req.GetAgid()}
	if w := req.GetWindow(); w != nil {
		if w.GetFrom() != nil {
			f.From = w.GetFrom().AsTime()
		}
		if w.GetUntil() != nil {
			f.To = w.GetUntil().AsTime()
		}
	}
	if p := req.GetPage(); p != nil {
		f.Limit = int(p.GetLimit())
		f.Offset = int(p.GetOffset())
	}
	items, err := s.decisions.List(ctx, req.GetTenantId(), f)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "decision list: %v", err)
	}
	out := make([]*observabilityv1.AgentDecisionEntry, 0, len(items))
	for _, d := range items {
		out = append(out, agentDecisionEntryToProto(d))
	}
	return &observabilityv1.ListAgentDecisionsResponse{
		Items: out,
		Total: int32(len(out)),
	}, nil
}

// GetAgentDecision returns a single decision log by ID.
func (s *ObservabilityServer) GetAgentDecision(ctx context.Context, req *observabilityv1.GetAgentDecisionRequest) (*observabilityv1.GetAgentDecisionResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if req.GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "tenant_id is required")
	}
	if req.GetLogId() == "" {
		return nil, status.Error(codes.InvalidArgument, "log_id is required")
	}
	d, err := s.decisions.GetByID(ctx, req.GetTenantId(), req.GetLogId())
	if err != nil {
		if errors.Is(err, decision.ErrNotFound) {
			return nil, status.Error(codes.NotFound, "decision not found")
		}
		return nil, status.Errorf(codes.Internal, "decision get: %v", err)
	}
	return &observabilityv1.GetAgentDecisionResponse{
		Entry: agentDecisionEntryToProto(d),
	}, nil
}

// GetAuditEvents projects AgentDecisionLog entries as audit events for the
// tenant. The gRPC analogue of REST GET /events?tenant_id=X — primary
// chora-gateway BFF caller.
func (s *ObservabilityServer) GetAuditEvents(ctx context.Context, req *observabilityv1.GetAuditEventsRequest) (*observabilityv1.GetAuditEventsResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if req.GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "tenant_id is required")
	}
	items, err := s.decisions.List(ctx, req.GetTenantId(), decision.ListFilter{Limit: auditEventsCap})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "decision list: %v", err)
	}
	out := make([]*observabilityv1.AgentDecisionEntry, 0, len(items))
	for _, d := range items {
		out = append(out, agentDecisionEntryToProto(d))
	}
	return &observabilityv1.GetAuditEventsResponse{
		Items: out,
		Total: int32(len(out)),
	}, nil
}

// -----------------------------------------------------------------------------
// TraceCorrelation RPCs
// -----------------------------------------------------------------------------

// RegisterCorrelation persists a new TraceCorrelation binding.
func (s *ObservabilityServer) RegisterCorrelation(ctx context.Context, req *observabilityv1.RegisterCorrelationRequest) (*observabilityv1.RegisterCorrelationResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if req.GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "tenant_id is required")
	}
	c, err := correlation.New(correlation.NewParams{
		TenantID:      req.GetTenantId(),
		CorrelationID: req.GetCorrelationId(),
		TraceID:       req.GetTraceId(),
		ParentSpanID:  req.GetParentSpanId(),
	})
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "correlation validation: %v", err)
	}
	for _, span := range req.GetChildSpans() {
		if err := c.AttachChildSpan(span); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "child span: %v", err)
		}
	}
	if err := s.correlations.Register(ctx, c); err != nil {
		return nil, status.Errorf(codes.Internal, "correlation register: %v", err)
	}
	return &observabilityv1.RegisterCorrelationResponse{
		Entry: correlationEntryToProto(c),
	}, nil
}

// GetCorrelation returns a TraceCorrelation by (tenant_id, correlation_id).
func (s *ObservabilityServer) GetCorrelation(ctx context.Context, req *observabilityv1.GetCorrelationRequest) (*observabilityv1.GetCorrelationResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if req.GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "tenant_id is required")
	}
	if req.GetCorrelationId() == "" {
		return nil, status.Error(codes.InvalidArgument, "correlation_id is required")
	}
	c, err := s.correlations.Get(ctx, req.GetTenantId(), req.GetCorrelationId())
	if err != nil {
		if errors.Is(err, correlation.ErrNotFound) {
			return nil, status.Error(codes.NotFound, "correlation not found")
		}
		return nil, status.Errorf(codes.Internal, "correlation get: %v", err)
	}
	return &observabilityv1.GetCorrelationResponse{
		Entry: correlationEntryToProto(c),
	}, nil
}

// -----------------------------------------------------------------------------
// Proto <-> domain conversion helpers
// -----------------------------------------------------------------------------

func tokenUsageEntryToProto(e *ledger.Entry) *observabilityv1.TokenUsageEntry {
	if e == nil {
		return nil
	}
	return &observabilityv1.TokenUsageEntry{
		LedgerId:         e.LedgerID,
		TenantId:         e.TenantID,
		Gcid:             e.Gcid,
		Agid:             e.Agid,
		ModelId:          e.ModelID,
		PromptTokens:     int32(e.PromptTokens),
		CompletionTokens: int32(e.CompletionTokens),
		CostUsdMicros:    e.CostUsdMicros,
		TraceId:          e.TraceID,
		SpanId:           e.SpanID,
		RecordedAt:       timestamppb.New(e.RecordedAt),
	}
}

func agentDecisionEntryToProto(d *decision.Log) *observabilityv1.AgentDecisionEntry {
	if d == nil {
		return nil
	}
	entry := &observabilityv1.AgentDecisionEntry{
		LogId:         d.LogID,
		TenantId:      d.TenantID,
		Agid:          d.Agid,
		DecisionType:  decisionTypeToProto(d.DecisionType),
		Reason:        d.Reason,
		RiskTier:      riskTierToProto(d.RiskTier),
		CorrelationId: d.CorrelationID,
		Traceparent:   d.Traceparent,
		CreatedAt:     timestamppb.New(d.CreatedAt),
	}
	if d.Reasoning != nil {
		entry.Reasoning = &observabilityv1.AgentReasoningSummary{
			InputHash:  d.Reasoning.InputHash,
			OutputHash: d.Reasoning.OutputHash,
			LatencyMs:  int32(d.Reasoning.LatencyMs),
			Summary:    d.Reasoning.Summary,
		}
	}
	return entry
}

func correlationEntryToProto(c *correlation.Correlation) *observabilityv1.TraceCorrelationEntry {
	if c == nil {
		return nil
	}
	children := make([]string, len(c.ChildSpans))
	copy(children, c.ChildSpans)
	return &observabilityv1.TraceCorrelationEntry{
		TenantId:      c.TenantID,
		CorrelationId: c.CorrelationID,
		TraceId:       c.TraceID,
		ParentSpanId:  c.ParentSpanID,
		ChildSpans:    children,
		RegisteredAt:  timestamppb.New(c.RecordedAt),
	}
}

func decisionTypeFromProto(t observabilityv1.AgentDecisionType) decision.Type {
	switch t {
	case observabilityv1.AgentDecisionType_AGENT_DECISION_TYPE_ROUTE:
		return decision.TypeRoute
	case observabilityv1.AgentDecisionType_AGENT_DECISION_TYPE_ESCALATE:
		return decision.TypeEscalate
	case observabilityv1.AgentDecisionType_AGENT_DECISION_TYPE_REFUSE:
		return decision.TypeRefuse
	case observabilityv1.AgentDecisionType_AGENT_DECISION_TYPE_RESPOND:
		return decision.TypeRespond
	}
	return decision.Type("") // invalid sentinel; domain New() rejects
}

func decisionTypeToProto(t decision.Type) observabilityv1.AgentDecisionType {
	switch t {
	case decision.TypeRoute:
		return observabilityv1.AgentDecisionType_AGENT_DECISION_TYPE_ROUTE
	case decision.TypeEscalate:
		return observabilityv1.AgentDecisionType_AGENT_DECISION_TYPE_ESCALATE
	case decision.TypeRefuse:
		return observabilityv1.AgentDecisionType_AGENT_DECISION_TYPE_REFUSE
	case decision.TypeRespond:
		return observabilityv1.AgentDecisionType_AGENT_DECISION_TYPE_RESPOND
	}
	return observabilityv1.AgentDecisionType_AGENT_DECISION_TYPE_UNSPECIFIED
}

func riskTierFromProto(t observabilityv1.AgentRiskTier) decision.RiskTier {
	switch t {
	case observabilityv1.AgentRiskTier_AGENT_RISK_TIER_LOW:
		return decision.TierLow
	case observabilityv1.AgentRiskTier_AGENT_RISK_TIER_MEDIUM:
		return decision.TierMedium
	case observabilityv1.AgentRiskTier_AGENT_RISK_TIER_HIGH:
		return decision.TierHigh
	case observabilityv1.AgentRiskTier_AGENT_RISK_TIER_CRITICAL:
		return decision.TierCritical
	}
	return decision.RiskTier("") // invalid sentinel; domain New() rejects
}

func riskTierToProto(t decision.RiskTier) observabilityv1.AgentRiskTier {
	switch t {
	case decision.TierLow:
		return observabilityv1.AgentRiskTier_AGENT_RISK_TIER_LOW
	case decision.TierMedium:
		return observabilityv1.AgentRiskTier_AGENT_RISK_TIER_MEDIUM
	case decision.TierHigh:
		return observabilityv1.AgentRiskTier_AGENT_RISK_TIER_HIGH
	case decision.TierCritical:
		return observabilityv1.AgentRiskTier_AGENT_RISK_TIER_CRITICAL
	}
	return observabilityv1.AgentRiskTier_AGENT_RISK_TIER_UNSPECIFIED
}
