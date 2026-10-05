// registry_test.go — unit tests for the agents registry loader.
package agents_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/apollo-chora/chora-observability/internal/domain/agents"
)

func writeTempRegistry(t *testing.T, contents string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "registry.json")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	return path
}

func TestLoad_ParsesCanonicalSnippet(t *testing.T) {
	t.Parallel()
	const data = `{
      "version": "v1",
      "crews": [
        {
          "name": "qgen_question",
          "pattern": "P2",
          "language": "go",
          "framework": "adk",
          "domain": "ai-kernel",
          "owningTeam": "content",
          "region": "us-central1",
          "liveEngine": "projects/381315455325/locations/us-central1/reasoningEngines/8635637442075951104",
          "phyllisStep": "3-7 (single-question AI-assist)"
        }
      ]
    }`
	path := writeTempRegistry(t, data)
	r, err := agents.Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if r.Version != "v1" {
		t.Errorf("Version = %q; want v1", r.Version)
	}
	if len(r.Crews) != 1 {
		t.Fatalf("len(Crews) = %d; want 1", len(r.Crews))
	}
	if r.Crews[0].Name != "qgen_question" {
		t.Errorf("Name = %q; want qgen_question", r.Crews[0].Name)
	}
}

func TestLoad_EmptyPathRejected(t *testing.T) {
	t.Parallel()
	if _, err := agents.Load(""); err == nil {
		t.Fatal("expected error for empty path")
	}
}

func TestLoad_ReadErrorPropagates(t *testing.T) {
	t.Parallel()
	if _, err := agents.Load("/does/not/exist/registry.json"); err == nil {
		t.Fatal("expected error for nonexistent file")
	}
}

func TestLoad_ParseErrorPropagates(t *testing.T) {
	t.Parallel()
	path := writeTempRegistry(t, "{ this is not valid json }")
	if _, err := agents.Load(path); err == nil {
		t.Fatal("expected parse error")
	}
}

func TestEntry_EngineID_HappyPath(t *testing.T) {
	t.Parallel()
	e := agents.Entry{LiveEngine: "projects/381315455325/locations/us-central1/reasoningEngines/8635637442075951104"}
	if got := e.EngineID(); got != "8635637442075951104" {
		t.Errorf("EngineID = %q; want 8635637442075951104", got)
	}
}

func TestEntry_EngineID_EmptyLiveEngine(t *testing.T) {
	t.Parallel()
	e := agents.Entry{}
	if got := e.EngineID(); got != "" {
		t.Errorf("EngineID = %q; want empty", got)
	}
}

func TestEntry_Location_FromLiveEngine(t *testing.T) {
	t.Parallel()
	e := agents.Entry{LiveEngine: "projects/381315455325/locations/us-central1/reasoningEngines/8635637442075951104"}
	if got := e.Location(); got != "us-central1" {
		t.Errorf("Location = %q; want us-central1", got)
	}
}

func TestEntry_Location_FallbackToRegion(t *testing.T) {
	t.Parallel()
	e := agents.Entry{Region: "region-a"}
	if got := e.Location(); got != "region-a" {
		t.Errorf("Location = %q; want region-a (fallback)", got)
	}
}

func TestEntry_Project_FromLiveEngine(t *testing.T) {
	t.Parallel()
	e := agents.Entry{LiveEngine: "projects/381315455325/locations/us-central1/reasoningEngines/8635637442075951104"}
	if got := e.Project(); got != "381315455325" {
		t.Errorf("Project = %q; want 381315455325", got)
	}
}

func TestRegistry_LookupByName_Found(t *testing.T) {
	t.Parallel()
	r := &agents.Registry{Crews: []agents.Entry{{Name: "qgen_question"}, {Name: "qgen_critic"}}}
	got := r.LookupByName("qgen_critic")
	if got == nil {
		t.Fatal("LookupByName returned nil for present entry")
	}
	if got.Name != "qgen_critic" {
		t.Errorf("Name = %q; want qgen_critic", got.Name)
	}
}

func TestRegistry_LookupByName_NotFound(t *testing.T) {
	t.Parallel()
	r := &agents.Registry{Crews: []agents.Entry{{Name: "qgen_question"}}}
	if got := r.LookupByName("ghost"); got != nil {
		t.Errorf("LookupByName returned non-nil for absent entry: %v", got)
	}
}

func TestRegistry_LookupByName_NilReceiver(t *testing.T) {
	t.Parallel()
	var r *agents.Registry
	if got := r.LookupByName("anything"); got != nil {
		t.Error("LookupByName on nil registry should return nil")
	}
}

func TestDefaultSearchPaths_ReturnsCanonicalOrder(t *testing.T) {
	// No t.Parallel(): Go 1.26 panics on t.Setenv after t.Parallel().
	t.Setenv("CHORA_AGENTS_REGISTRY_PATH", "/explicit/path")
	paths := agents.DefaultSearchPaths()
	if len(paths) < 3 {
		t.Fatalf("len(paths) = %d; want >= 3", len(paths))
	}
	if paths[0] != "/explicit/path" {
		t.Errorf("paths[0] = %q; want /explicit/path (env override)", paths[0])
	}
}

func TestEntry_Project(t *testing.T) {
	if got := (agents.Entry{}).Project(); got != "" {
		t.Errorf("empty Project = %q; want empty", got)
	}
	if got := (agents.Entry{LiveEngine: "locations/us-central1"}).Project(); got != "" {
		t.Errorf("no projects/ marker = %q; want empty", got)
	}
	if got := (agents.Entry{LiveEngine: "projects/chora-local"}).Project(); got != "chora-local" {
		t.Errorf("no trailing slash = %q; want chora-local", got)
	}
	if got := (agents.Entry{LiveEngine: "projects/chora-local/locations/us-central1"}).Project(); got != "chora-local" {
		t.Errorf("Project = %q; want chora-local", got)
	}
}

func TestEntry_Location(t *testing.T) {
	if got := (agents.Entry{}).Location(); got != "" {
		t.Errorf("empty Location = %q; want empty", got)
	}
	if got := (agents.Entry{LiveEngine: "projects/x/locations/us-central1"}).Location(); got != "us-central1" {
		t.Errorf("Location = %q; want us-central1", got)
	}
	if got := (agents.Entry{LiveEngine: "projects/x/locations/us-central1/engines/e1"}).Location(); got != "us-central1" {
		t.Errorf("Location with suffix = %q; want us-central1", got)
	}
	if got := (agents.Entry{Region: "us-west1", LiveEngine: "projects/x"}).Location(); got != "us-west1" {
		t.Errorf("fallback Region = %q; want us-west1", got)
	}
	if got := (agents.Entry{LiveEngine: "projects/x/locations/"}).Location(); got != "" {
		t.Errorf("empty location segment = %q; want empty", got)
	}
}
