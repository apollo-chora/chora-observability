// Package config_test holds the RED-phase TDD specs for the PII Closure Map
// loader.
package config_test

import (
	"os"
	"strings"
	"testing"

	"github.com/5007-Capstone/chora/services/chora-observability/internal/config"
)

func TestLoadPIIClosureMap_FromBytes_ParsesYAML(t *testing.T) {
	t.Parallel()
	yaml := `
domain: chora_observability
version: "1.0"
fields_to_tokenize:
  - table: t1
    columns:
      - column: c1
        strategy: tombstone_string
        value: "Former member"
retention_days_by_jurisdiction:
  EU: 2557
  default: 2557
on_creator_closure:
  strategy: tokenise_authorship_keep_atom
  show_authorship_as: "Former member"
`
	m, err := config.LoadFromBytes([]byte(yaml))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if m.Domain != "chora_observability" {
		t.Fatalf("domain: %q", m.Domain)
	}
	if m.Version != "1.0" {
		t.Fatalf("version: %q", m.Version)
	}
	if len(m.FieldsToTokenize) != 1 {
		t.Fatalf("expected 1 table; got %d", len(m.FieldsToTokenize))
	}
	if m.RetentionDaysByJurisdiction["EU"] != 2557 {
		t.Fatalf("EU retention: %d", m.RetentionDaysByJurisdiction["EU"])
	}
	if m.OnCreatorClosure.Strategy != "tokenise_authorship_keep_atom" {
		t.Fatalf("creator closure strategy: %q", m.OnCreatorClosure.Strategy)
	}
}

func TestLoadPIIClosureMap_FromFile_DomainManifest(t *testing.T) {
	t.Parallel()
	m, err := config.LoadFromFile("../../config/PII_Closure_Map.yaml")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if m.Domain != "chora_observability" {
		t.Fatalf("expected domain chora_observability, got %q", m.Domain)
	}
	if len(m.FieldsToTokenize) == 0 {
		t.Fatalf("expected at least one tokenize entry")
	}
}

func TestLoadPIIClosureMap_RejectsEmptyDomain(t *testing.T) {
	t.Parallel()
	yaml := `
version: "1.0"
fields_to_tokenize: []
`
	_, err := config.LoadFromBytes([]byte(yaml))
	if err == nil || !strings.Contains(err.Error(), "domain") {
		t.Fatalf("expected domain-required error; got %v", err)
	}
}

func TestLoadPIIClosureMap_RejectsBadStrategy(t *testing.T) {
	t.Parallel()
	yaml := `
domain: chora_observability
version: "1.0"
fields_to_tokenize:
  - table: x
    columns:
      - column: y
        strategy: zap_to_void
        value: ""
`
	_, err := config.LoadFromBytes([]byte(yaml))
	if err == nil || !strings.Contains(err.Error(), "strategy") {
		t.Fatalf("expected strategy validation error; got %v", err)
	}
}

func TestLoadPIIClosureMap_AllowedStrategies(t *testing.T) {
	t.Parallel()
	allowed := []string{
		"tombstone_string", "tombstone_email", "drop", "hash",
		"preserve", "tokenize", "encrypt", "cascade_delete",
	}
	for _, s := range allowed {
		if !config.IsAllowedStrategy(s) {
			t.Errorf("strategy %q should be allowed", s)
		}
	}
	if config.IsAllowedStrategy("zap_to_void") {
		t.Errorf("strategy zap_to_void should NOT be allowed")
	}
}

func TestLoadPIIClosureMap_AGIDApplicable(t *testing.T) {
	t.Parallel()
	yaml := `
domain: chora_observability
version: "1.0"
fields_to_tokenize: []
`
	m, err := config.LoadFromBytes([]byte(yaml))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	expectAGID := (m.Domain == "chora_a2a")
	if m.AGIDApplicable != expectAGID {
		t.Fatalf("AGID applicability mismatch: domain=%s applicable=%v expect=%v", m.Domain, m.AGIDApplicable, expectAGID)
	}
}

func TestLoadPIIClosureMap_FromFile_Errors(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	missing := dir + "/missing.yaml"
	if _, err := config.LoadFromFile(missing); err == nil {
		t.Fatal("expected error for missing file")
	}

	bad := dir + "/bad.yaml"
	_ = os.WriteFile(bad, []byte("{not: yaml: ["), 0o600)
	if _, err := config.LoadFromFile(bad); err == nil {
		t.Fatal("expected error for malformed yaml")
	}

	// validate() rejects a disallowed strategy
	noVersion := dir + "/bad-strategy.yaml"
	_ = os.WriteFile(noVersion, []byte("domain: x\nfields_to_tokenize:\n  - table: t\n    columns:\n      - column: c\n        strategy: nope\n"), 0o600)
	if _, err := config.LoadFromFile(noVersion); err == nil {
		t.Fatal("expected error for disallowed strategy")
	}
}
