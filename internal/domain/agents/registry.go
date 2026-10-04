// Package agents holds the static metadata for the /api/v1/observability/agents
// endpoint per Phase B of the O+ hydration plan atomic-napping-spring.md.
//
// The registry source-of-truth is chora-infra/agents-cli/registry.json (per
// [[crew-composition]] §8). chora-observability does NOT import chora-infra
// directly — it reads a copy of the registry file mounted into the service
// container at /app/config/registry.json (production) or
// chora-infra/agents-cli/registry.json (dev).
//
// Per [[feedback-no-stubs-real-wiring]]: the registry file is the canonical
// source. No in-memory stubs of agent metadata at the adapter layer.
//
// Per ADR-145 + ADR-148: agent_engine deploys live in us-central1 TEMPORARY
// while asia-southeast1 GA is pending. The deep-link URLs in the handler
// template these directly.
package agents

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Registry is the parsed registry.json payload. Mirrors the layout in
// chora-infra/agents-cli/registry.json (v1 schema).
type Registry struct {
	Schema      string  `json:"$schema,omitempty"`
	Version     string  `json:"version"`
	Description string  `json:"description,omitempty"`
	Crews       []Entry `json:"crews"`
}

// Entry is one row in the crews[] array. Field set mirrors the subset
// chora-observability actually consumes — extra fields parse through via
// the json tags.
type Entry struct {
	Name           string   `json:"name"`
	Pattern        string   `json:"pattern,omitempty"`
	Language       string   `json:"language,omitempty"`
	Framework      string   `json:"framework,omitempty"`
	Module         string   `json:"module,omitempty"`
	Entrypoint     string   `json:"entrypoint,omitempty"`
	Domain         string   `json:"domain,omitempty"`
	OwningTeam     string   `json:"owningTeam,omitempty"`
	Region         string   `json:"region,omitempty"`
	LiveEngine     string   `json:"liveEngine,omitempty"`
	PhyllisStep    string   `json:"phyllisStep,omitempty"`
	Notes          string   `json:"notes,omitempty"`
	MultiAgent     bool     `json:"multiAgent,omitempty"`
	SubAgents      []string `json:"subAgents,omitempty"`
}

// EngineID returns the parsed reasoning engine ID from LiveEngine (the
// final path segment) or "" when LiveEngine is unset / malformed.
//
// Example LiveEngine:
//
//	projects/381315455325/locations/us-central1/reasoningEngines/8635637442075951104
//
// Returns: "8635637442075951104"
func (e Entry) EngineID() string {
	if e.LiveEngine == "" {
		return ""
	}
	idx := strings.LastIndex(e.LiveEngine, "/")
	if idx < 0 || idx == len(e.LiveEngine)-1 {
		return ""
	}
	return e.LiveEngine[idx+1:]
}

// Location returns the parsed location/region from LiveEngine (the segment
// after /locations/) — used by FE deep-link builders.
func (e Entry) Location() string {
	if e.LiveEngine == "" {
		return e.Region
	}
	const marker = "/locations/"
	i := strings.Index(e.LiveEngine, marker)
	if i < 0 {
		return e.Region
	}
	rest := e.LiveEngine[i+len(marker):]
	j := strings.Index(rest, "/")
	if j < 0 {
		return rest
	}
	return rest[:j]
}

// Project returns the parsed project number from LiveEngine.
func (e Entry) Project() string {
	if e.LiveEngine == "" {
		return ""
	}
	const marker = "projects/"
	i := strings.Index(e.LiveEngine, marker)
	if i < 0 {
		return ""
	}
	rest := e.LiveEngine[i+len(marker):]
	j := strings.Index(rest, "/")
	if j < 0 {
		return rest
	}
	return rest[:j]
}

// Load reads + parses a registry.json file from disk. Returns an error on
// either read or parse failure.
func Load(path string) (*Registry, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("agents.Load: path is required")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("agents.Load read: %w", err)
	}
	var r Registry
	if err := json.Unmarshal(data, &r); err != nil {
		return nil, fmt.Errorf("agents.Load parse: %w", err)
	}
	return &r, nil
}

// DefaultSearchPaths returns the canonical lookup paths probed by the
// chora-observability bootstrap. First non-empty + existing path wins.
func DefaultSearchPaths() []string {
	return []string{
		os.Getenv("CHORA_AGENTS_REGISTRY_PATH"),
		filepath.Join("config", "registry.json"),
		filepath.Join("..", "..", "chora-infra", "agents-cli", "registry.json"),
	}
}

// LookupByName returns the entry with the matching name, or nil when not
// present. Case-sensitive.
func (r *Registry) LookupByName(name string) *Entry {
	if r == nil {
		return nil
	}
	for i := range r.Crews {
		if r.Crews[i].Name == name {
			return &r.Crews[i]
		}
	}
	return nil
}
