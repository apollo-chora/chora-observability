package main

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strings"
)

// loadEnvFiles loads dotenv files into the process environment for the
// reconciliation job, without overwriting variables already supplied by the
// parent process (Kubernetes, Cloud Run, Docker, CI). CHORA_ENV_FILE
// overrides the default search paths.
func loadEnvFiles() {
	paths := []string{}
	if explicit := strings.TrimSpace(os.Getenv("CHORA_ENV_FILE")); explicit != "" {
		paths = append(paths, explicit)
	} else {
		paths = append(paths, ".env", "/app/.env")
	}

	for _, path := range paths {
		if err := loadDotEnvOptional(path); err != nil {
			fmt.Fprintf(os.Stderr, "chora-observability: failed to load env file %s: %v\n", path, err)
			os.Exit(1)
		}
		if _, err := os.Stat(path); err == nil {
			return
		}
	}
}

func loadDotEnvOptional(path string) error {
	if err := loadDotEnv(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	return nil
}

// loadDotEnv loads KEY=VALUE pairs from a dotenv file into the process
// environment, without overwriting variables that were already supplied by
// the parent process. Supports the common dotenv subset: blank lines,
// comments, optional "export " prefixes, quoted values, and inline comments
// outside quotes. Variable expansion is not performed.
func loadDotEnv(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for lineNo := 1; scanner.Scan(); lineNo++ {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, "export ") {
			line = strings.TrimSpace(strings.TrimPrefix(line, "export "))
		}

		key, value, ok := strings.Cut(line, "=")
		if !ok {
			return fmt.Errorf("invalid dotenv entry at %s:%d: missing '='", path, lineNo)
		}

		key = strings.TrimSpace(key)
		if !isEnvKey(key) {
			return fmt.Errorf("invalid dotenv key at %s:%d: %q", path, lineNo, key)
		}
		value = parseDotEnvValue(strings.TrimSpace(value))

		if _, exists := os.LookupEnv(key); exists {
			continue
		}
		if err := os.Setenv(key, value); err != nil {
			return fmt.Errorf("set dotenv key %q: %w", key, err)
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read dotenv file %s: %w", path, err)
	}
	return nil
}

func parseDotEnvValue(value string) string {
	if len(value) >= 2 {
		if (value[0] == '"' && value[len(value)-1] == '"') ||
			(value[0] == 39 && value[len(value)-1] == 39) {
			return value[1 : len(value)-1]
		}
	}

	for i := 0; i < len(value); i++ {
		if value[i] != '#' || (i > 0 && value[i-1] != ' ' && value[i-1] != '\t') {
			continue
		}
		return strings.TrimSpace(value[:i])
	}
	return value
}

func isEnvKey(key string) bool {
	if key == "" {
		return false
	}
	for i, r := range key {
		if !(r == '_' || r >= 'A' && r <= 'Z' || r >= 'a' && r <= 'z' || (i > 0 && r >= '0' && r <= '9')) {
			return false
		}
	}
	return true
}
