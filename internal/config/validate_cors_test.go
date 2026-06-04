package config

import (
	"strings"
	"testing"
)

func TestValidateWildcardCORSWarns(t *testing.T) {
	y := strings.Replace(minimalValid, "  port: 8080",
		"  port: 8080\n  cors_allowed_origins: [\"*\"]", 1)
	_, warnings, err := Parse([]byte(y))
	if err != nil {
		t.Fatalf("Parse: %v (wildcard CORS should warn, not fail)", err)
	}
	found := false
	for _, w := range warnings {
		if strings.Contains(w, "cors_allowed_origins") {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("warnings = %v, want at least one warning mentioning cors_allowed_origins", warnings)
	}
}

func TestValidateExplicitCORSNoWarn(t *testing.T) {
	y := strings.Replace(minimalValid, "  port: 8080",
		"  port: 8080\n  cors_allowed_origins: [\"https://app.example.com\"]", 1)
	_, warnings, err := Parse([]byte(y))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	for _, w := range warnings {
		if strings.Contains(w, "cors_allowed_origins") {
			t.Errorf("unexpected CORS warning for explicit allowlist: %q", w)
		}
	}
}
