// fabric_guardrail_test — the Flag-1 CI guardrail from the 2026-07-01
// event-fabric audit, adapted to chora-observability's actual publish path.
//
// It is the durable regression gate against the kg_hexagon_fog class of bug: a
// topic that the service PRODUCES and that is bound to a BINARY Pub/Sub Schema
// Registry schema, but for which no binary encoder exists — so the outbox
// silently JSON-falls-back and the binary schema rejects every publish →
// dead-letter forever.
//
// WHY THIS SERVICE DIFFERS FROM chora-delivery's protomarshal guardrail
// --------------------------------------------------------------------
// chora-delivery owns a per-topic protomarshal.MarshalPayload switch, so its
// guardrail calls MarshalPayload(topic) and asserts it is not
// ErrUnsupportedTopic. chora-observability has NO such switch: its outbox
// Dispatcher is a RELAY — reconstructEnvelope + Bus.Publish emit the
// outbox_events.payload BYTEA verbatim (see dispatcher.go), and the service has
// no local proto encoder in its source at all. Its OWN emitters
// (reconcilepublish, familiar-growth evidence) publish schemaless JSON on topics
// that are deliberately NOT bound to a binary schema. The single binary event it
// PRODUCES, chora.observability.token_usage.recorded.v1, is proto.Marshal'd
// UPSTREAM by chora-model-gateway (buildTokenUsagePayload,
// services/chora-model-gateway/internal/adapter/pg/pg.go), which writes the
// pre-encoded bytes into the SHARED chora_observability.outbox_events table that
// this service's Dispatcher drains.
//
// So the observability guardrail asserts a documented ENCODER MANIFEST instead
// of a local switch: every binary-schema-bound topic the service produces must
// have a manifest entry naming where its binary payload is encoded. A produced
// binary-bound topic ABSENT from the manifest is a NEW un-encoded event — either
// a local proto encoder must be added, the upstream encoder documented here, or
// the topic marked schemaless in m10-data-plane.
//
// Data sources (deployed-reality precedence — CLAUDE.md source hierarchy #1):
//
//   - "bound to a binary schema" = an observability/governance topic literal in
//     the m10-data-plane terraform (which provisions the Schema Registry
//     bindings) MINUS local.schemaless_topics. That terraform is authoritative —
//     NOT chora-infra/topics/topics.yaml, a stale partial catalogue (~97 behind).
//
//   - "chora-observability produces it" = the topic string literal appears as a
//     standalone quoted literal in the service's Go source OUTSIDE the consumer
//     (*_consumer.go), decoder (protodecode), subscriber (subscribers/ +
//     *_subscriber.go), consumer-binding (*_binding.go), consumed-topic
//     catalogue (familiargrowth/) and consumer push-handler surfaces, and tests.
//     What remains is the set of topics the service actually EMITS.
//
// NOTE (per the audit): this is a CI TEST only. It does NOT make the relay
// runtime fail-loud — many observability-domain topics legitimately publish
// schemaless JSON, and a runtime blanket fail-loud would break them.
package outbox_test

import (
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
)

// upstreamBinaryEncoders documents, for each binary-schema-bound topic that
// chora-observability PRODUCES, WHERE the canonical protobuf payload is encoded.
// observability's outbox is a relay with no local MarshalPayload switch, so the
// encoder for a relayed event lives in the upstream producer that writes the
// pre-marshalled bytes into the shared chora_observability.outbox_events table.
//
// A produced binary-bound topic ABSENT from this map fails the gate below: add a
// local proto encoder, document its upstream encoder here, or mark the topic
// schemaless in chora-infra/terraform/modules/m10-data-plane.
var upstreamBinaryEncoders = map[string]string{
	// Encoded upstream by chora-model-gateway buildTokenUsagePayload
	// (proto.Marshal of observability.v1.TokenUsageRecorded) →
	// services/chora-model-gateway/internal/adapter/pg/pg.go, written into the
	// shared chora_observability.outbox_events table and relayed verbatim by
	// this service's Dispatcher (dispatcher.go reconstructEnvelope + Bus.Publish).
	"chora.observability.token_usage.recorded.v1": "chora-model-gateway buildTokenUsagePayload (proto.Marshal → shared chora_observability.outbox_events)",
}

// fabricTopicRe matches a fully-qualified observability- OR governance-domain
// topic that is a STANDALONE double-quoted string literal (a Go source string
// constant, or a TF map key / list element). chora-observability emits into both
// domains (its own observability.* events + governance.* audit/evidence events).
// Requiring the surrounding quotes distinguishes a real literal from a topic
// merely mentioned in a // comment or embedded in a larger log-format string.
var fabricTopicRe = regexp.MustCompile(`"(chora\.(?:observability|governance)\.[a-z0-9_.]+\.v1)"`)

// fabricTopicLiterals returns the set of obs/gov topics quoted as standalone
// string literals in s (capture group 1 of each match).
func fabricTopicLiterals(s string) map[string]bool {
	out := map[string]bool{}
	for _, m := range fabricTopicRe.FindAllStringSubmatch(s, -1) {
		out[m[1]] = true
	}
	return out
}

// fabricRepoRoot walks up from this test's source file (stable at build time,
// independent of the test's working directory) to the monorepo root — the
// directory holding both chora-infra/ and services/chora-observability/.
func fabricRepoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller(0) failed — cannot locate repo root")
	}
	dir := filepath.Dir(file)
	for i := 0; i < 15; i++ {
		infra := filepath.Join(dir, "chora-infra", "terraform", "modules", "m10-data-plane", "main.tf")
		svc := filepath.Join(dir, "services", "chora-observability")
		if fabricFileExists(infra) && fabricDirExists(svc) {
			return dir
		}
		// Stop AT the monorepo root (the directory holding go.work). Without
		// this the walk climbs past a git worktree into whatever checkout
		// contains it, reads THAT tree's copy and reports green on a file this
		// tree does not have: exactly how the missed half of the ADR-254 D9
		// rename hid, because the parent checkout still had the old path.
		if _, gerr := os.Stat(filepath.Join(dir, "go.work")); gerr == nil {
			t.Fatalf("%s not found at the monorepo root %s", infra, dir)
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	t.Fatalf("repo root (chora-infra + services/chora-observability) not found walking up from %s", file)
	return ""
}

func fabricFileExists(p string) bool {
	fi, err := os.Stat(p)
	return err == nil && !fi.IsDir()
}

func fabricDirExists(p string) bool {
	fi, err := os.Stat(p)
	return err == nil && fi.IsDir()
}

// observabilityBinaryBoundTopics returns the set of obs/gov topics bound to a
// binary Schema Registry schema = every obs/gov topic literal in the
// m10-data-plane terraform MINUS the ones in local.schemaless_topics (which
// intentionally publish schemaless JSON).
func observabilityBinaryBoundTopics(t *testing.T, root string) map[string]bool {
	t.Helper()
	tfPath := filepath.Join(root, "chora-infra", "terraform", "modules", "m10-data-plane", "main.tf")
	raw, err := os.ReadFile(tfPath)
	if err != nil {
		t.Fatalf("read m10-data-plane main.tf: %v", err)
	}
	tf := string(raw)

	all := fabricTopicLiterals(tf)

	// Extract the schemaless_topics = toset([ ... ]) block and remove its
	// obs/gov members — those intentionally publish schemaless JSON.
	schemaless := map[string]bool{}
	if start := strings.Index(tf, "schemaless_topics = toset(["); start >= 0 {
		rest := tf[start:]
		if end := strings.Index(rest, "])"); end >= 0 {
			schemaless = fabricTopicLiterals(rest[:end])
		} else {
			t.Fatal("schemaless_topics block has no closing '])' — TF parse assumption broken")
		}
	} else {
		t.Fatal("could not locate local.schemaless_topics in main.tf — TF parse assumption broken")
	}

	bound := map[string]bool{}
	for topic := range all {
		if !schemaless[topic] {
			bound[topic] = true
		}
	}
	if len(bound) == 0 {
		t.Fatal("0 binary-bound obs/gov topics parsed from TF — parse is broken (would make the guardrail vacuously green)")
	}
	return bound
}

// producedObservabilityTopics scans chora-observability's Go source for obs/gov
// topic string literals, EXCLUDING the consumer / decoder / subscriber /
// consumer-binding / consumed-topic-catalogue surfaces and test files. What
// remains is the set of topics the service actually EMITS.
func producedObservabilityTopics(t *testing.T, root string) map[string]bool {
	t.Helper()
	svcRoot := filepath.Join(root, "services", "chora-observability")
	produced := map[string]bool{}
	err := filepath.WalkDir(svcRoot, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		base := d.Name()
		if !strings.HasSuffix(base, ".go") || strings.HasSuffix(base, "_test.go") {
			return nil
		}
		// Consume-only + catalogue surfaces do not count as producers.
		if strings.Contains(path, string(filepath.Separator)+"protodecode"+string(filepath.Separator)) ||
			strings.Contains(path, string(filepath.Separator)+"subscribers"+string(filepath.Separator)) ||
			strings.Contains(path, string(filepath.Separator)+"familiargrowth"+string(filepath.Separator)) ||
			strings.HasSuffix(base, "_consumer.go") ||
			strings.HasSuffix(base, "_subscriber.go") ||
			strings.HasSuffix(base, "_binding.go") ||
			base == "familiar_growth_audit_pubsub_handler.go" {
			return nil
		}
		raw, rerr := os.ReadFile(path)
		if rerr != nil {
			return rerr
		}
		for topic := range fabricTopicLiterals(string(raw)) {
			produced[topic] = true
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk chora-observability source: %v", err)
	}
	if len(produced) == 0 {
		t.Fatal("0 produced obs/gov topics found — source scan is broken (would make the guardrail vacuously green)")
	}
	return produced
}

// TestFabricGuardrail_EveryProducedBinaryTopicHasEncoder is the Flag-1 gate.
// For every obs/gov topic that chora-observability PRODUCES and that is bound to
// a binary Schema Registry schema, an encoder MUST exist — proven here by a
// documented entry in upstreamBinaryEncoders (observability relays; it has no
// local MarshalPayload). A missing entry means the outbox would JSON-fall-back →
// Schema Registry rejects → dead-letter (kg_hexagon_fog class).
func TestFabricGuardrail_EveryProducedBinaryTopicHasEncoder(t *testing.T) {
	root := fabricRepoRoot(t)
	binaryBound := observabilityBinaryBoundTopics(t, root)
	produced := producedObservabilityTopics(t, root)

	checked := 0
	for topic := range produced {
		if !binaryBound[topic] {
			// Schemaless / not schema-bound → a JSON publish is legitimate.
			continue
		}
		if _, ok := upstreamBinaryEncoders[topic]; !ok {
			t.Errorf("topic %q is PRODUCED by chora-observability and bound to a binary "+
				"Pub/Sub schema, but has NO documented binary encoder. observability's outbox "+
				"relays payload bytes verbatim (no local MarshalPayload), so an un-encoded "+
				"payload JSON-falls-back → Schema Registry rejects → dead-letter (kg_hexagon_fog "+
				"class). Add a local proto encoder, document the upstream encoder in "+
				"upstreamBinaryEncoders, or mark the topic schemaless in m10-data-plane.", topic)
			continue
		}
		checked++
	}
	if checked == 0 {
		t.Fatal("guardrail asserted 0 produced binary-bound topics — the TF parse or the source scan is broken")
	}
	t.Logf("Flag-1 guardrail: %d produced binary-bound observability topic(s) all have a documented encoder", checked)
}

// TestFabricGuardrail_NoStaleEncoderManifestEntries keeps upstreamBinaryEncoders
// honest: every documented topic must still be PRODUCED by the service AND still
// be binary-schema-bound. A stale entry (topic retired or moved to schemaless)
// would silently weaken the gate above, so it fails loudly instead.
func TestFabricGuardrail_NoStaleEncoderManifestEntries(t *testing.T) {
	root := fabricRepoRoot(t)
	binaryBound := observabilityBinaryBoundTopics(t, root)
	produced := producedObservabilityTopics(t, root)

	for topic := range upstreamBinaryEncoders {
		if !produced[topic] {
			t.Errorf("upstreamBinaryEncoders documents %q but chora-observability no longer "+
				"produces it — remove the stale manifest entry", topic)
		}
		if !binaryBound[topic] {
			t.Errorf("upstreamBinaryEncoders documents %q but it is no longer binary-schema-bound "+
				"(now schemaless or unprovisioned) — remove the stale manifest entry", topic)
		}
	}
}
