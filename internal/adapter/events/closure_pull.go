// Pull-loop adapter binding the federated closure-saga subscriber to the
// eventbus consume loop (CHO-1719 gap 4). The JetStream consume loop acks on
// nil and naks on error (ack-after-processing per D6.2).
package events

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/apollo-chora/chora-common/eventbus"
)

// ClosurePullHandler adapts the ClosureSubscriber to an eventbus.Handler.
// Payload is the orchestrator's JSON fan-out body; traceparent/tracestate
// fall back to the envelope when absent from the payload.
func ClosurePullHandler(s *ClosureSubscriber) eventbus.Handler {
	return func(ctx context.Context, msg eventbus.Message) error {
		if s == nil {
			return errors.New("events: closure pull handler not initialised")
		}
		var p PseudonymiseRequestedPayload
		if err := json.Unmarshal(msg.Payload, &p); err != nil {
			return errors.Join(errors.New("events: closure payload decode"), err)
		}
		if p.Traceparent == "" {
			p.Traceparent = msg.Envelope.Traceparent
		}
		if p.Tracestate == "" {
			p.Tracestate = msg.Envelope.Tracestate
		}
		return s.Handle(ctx, p)
	}
}
