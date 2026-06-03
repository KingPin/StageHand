package config

import (
	"os"
	"strings"
	"testing"
)

// minimalValid is a baseline config that passes validation; tests mutate
// copies of it (textually) to probe individual rules.
const minimalValid = `
server:
  port: 8080
vram_pools:
  gpu0:
    grace_period_seconds: 60
    cooldown_seconds: 300
services:
  llm:
    container_name: "llm-box"
    target_url: "http://llm-box:8081"
    health_path: "/health"
    vram_pool: "gpu0"
  embed:
    container_name: "embed-box"
    target_url: "http://embed-box:8082"
    vram_pool: null
routes:
  - path_prefix: "/v1/chat/completions"
    service: "llm"
  - path_prefix: "/v1/embeddings"
    service: "embed"
`

func TestParseValidAppliesDefaults(t *testing.T) {
	cfg, warnings, err := Parse([]byte(minimalValid))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(warnings) != 0 {
		t.Errorf("unexpected warnings: %v", warnings)
	}

	if cfg.Server.Host != DefaultHost {
		t.Errorf("Host = %q, want default %q", cfg.Server.Host, DefaultHost)
	}
	if cfg.Server.DockerSocketPath != DefaultDockerSocketPath {
		t.Errorf("DockerSocketPath = %q, want default", cfg.Server.DockerSocketPath)
	}
	if cfg.Server.MaxQueueSize != DefaultMaxQueueSize {
		t.Errorf("MaxQueueSize = %d, want %d", cfg.Server.MaxQueueSize, DefaultMaxQueueSize)
	}

	embed := cfg.Services["embed"]
	if embed.HealthPath != DefaultHealthPath {
		t.Errorf("embed.HealthPath = %q, want default %q", embed.HealthPath, DefaultHealthPath)
	}
	if embed.StartupTimeoutSeconds != DefaultStartupTimeoutSeconds {
		t.Errorf("embed.StartupTimeoutSeconds = %d, want %d",
			embed.StartupTimeoutSeconds, DefaultStartupTimeoutSeconds)
	}
	if !embed.AlwaysOn() {
		t.Error("embed.AlwaysOn() = false, want true")
	}
	if cfg.Services["llm"].AlwaysOn() {
		t.Error("llm.AlwaysOn() = true, want false")
	}
}

func TestQueueSizeOverride(t *testing.T) {
	yaml := strings.Replace(minimalValid,
		`    health_path: "/health"`,
		"    health_path: \"/health\"\n    max_queue_size: 7", 1)
	cfg, _, err := Parse([]byte(yaml))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if got := cfg.QueueSize(cfg.Services["llm"]); got != 7 {
		t.Errorf("QueueSize(llm) = %d, want 7 (override)", got)
	}
	if got := cfg.QueueSize(cfg.Services["embed"]); got != DefaultMaxQueueSize {
		t.Errorf("QueueSize(embed) = %d, want server default %d", got, DefaultMaxQueueSize)
	}
}

func TestValidationFailures(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(string) string
		wantErr string // substring expected in the aggregated error
	}{
		{
			name:    "unknown top-level key rejected",
			mutate:  func(s string) string { return s + "\ndocker_api_version: \"v1.40\"\n" },
			wantErr: "field docker_api_version not found",
		},
		{
			name:    "port out of range",
			mutate:  func(s string) string { return strings.Replace(s, "port: 8080", "port: 70000", 1) },
			wantErr: "out of range",
		},
		{
			name:    "route references unknown service",
			mutate:  func(s string) string { return strings.Replace(s, `service: "embed"`, `service: "nope"`, 1) },
			wantErr: `service "nope" is not declared`,
		},
		{
			name:    "service references unknown pool",
			mutate:  func(s string) string { return strings.Replace(s, `vram_pool: "gpu0"`, `vram_pool: "gpu9"`, 1) },
			wantErr: `vram_pool "gpu9" is not defined`,
		},
		{
			name: "duplicate container_name",
			mutate: func(s string) string {
				return strings.Replace(s, `container_name: "embed-box"`, `container_name: "llm-box"`, 1)
			},
			wantErr: "share container_name",
		},
		{
			name: "model target must exist",
			mutate: func(s string) string {
				return strings.Replace(s, `    service: "llm"`,
					"    models:\n      \"gpt-x\": \"ghost\"\n    service: \"llm\"", 1)
			},
			wantErr: `targets undeclared service "ghost"`,
		},
		{
			name: "default_service must belong to pool",
			mutate: func(s string) string {
				return strings.Replace(s, "cooldown_seconds: 300",
					"cooldown_seconds: 300\n    default_service: \"embed\"", 1)
			},
			wantErr: "does not belong to this pool",
		},
		{
			name: "default_service must exist",
			mutate: func(s string) string {
				return strings.Replace(s, "cooldown_seconds: 300",
					"cooldown_seconds: 300\n    default_service: \"ghost\"", 1)
			},
			wantErr: "not a declared service",
		},
		{
			name:    "target_url must be absolute",
			mutate:  func(s string) string { return strings.Replace(s, "http://embed-box:8082", "embed-box:8082", 1) },
			wantErr: "must be absolute http(s)",
		},
		{
			name:    "health_path must start with slash",
			mutate:  func(s string) string { return strings.Replace(s, `health_path: "/health"`, `health_path: "health"`, 1) },
			wantErr: "must start with /",
		},
		{
			name:    "path_prefix must start with slash",
			mutate:  func(s string) string { return strings.Replace(s, `path_prefix: "/v1/embeddings"`, `path_prefix: "v1/embeddings"`, 1) },
			wantErr: "must start with /",
		},
		{
			name: "no routes",
			mutate: func(s string) string {
				i := strings.Index(s, "routes:")
				return s[:i] + "routes: []\n"
			},
			wantErr: "at least one route",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := Parse([]byte(tt.mutate(minimalValid)))
			if err == nil {
				t.Fatal("Parse succeeded, want error")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("error %q does not contain %q", err, tt.wantErr)
			}
		})
	}
}

func TestAggregatesMultipleErrors(t *testing.T) {
	bad := strings.Replace(minimalValid, "port: 8080", "port: 70000", 1)
	bad = strings.Replace(bad, `service: "embed"`, `service: "nope"`, 1)
	_, _, err := Parse([]byte(bad))
	if err == nil {
		t.Fatal("Parse succeeded, want error")
	}
	for _, want := range []string{"out of range", `"nope" is not declared`} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("aggregated error missing %q: %v", want, err)
		}
	}
}

func TestLocalhostWarning(t *testing.T) {
	y := strings.Replace(minimalValid, "http://embed-box:8082", "http://localhost:8082", 1)
	cfg, warnings, err := Parse([]byte(y))
	if err != nil {
		t.Fatalf("Parse: %v (localhost should warn, not fail)", err)
	}
	if cfg == nil || len(warnings) != 1 || !strings.Contains(warnings[0], "localhost") {
		t.Errorf("warnings = %v, want one localhost warning", warnings)
	}
}

func TestAuthParses(t *testing.T) {
	y := strings.Replace(minimalValid, "  port: 8080",
		"  port: 8080\n  auth:\n    admin_token: \"a-sufficiently-long-admin-token\"\n    proxy_token: \"proxy-secret\"", 1)
	cfg, warnings, err := Parse([]byte(y))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(warnings) != 0 {
		t.Errorf("unexpected warnings: %v", warnings)
	}
	if cfg.Server.Auth.AdminToken != "a-sufficiently-long-admin-token" {
		t.Errorf("AdminToken = %q", cfg.Server.Auth.AdminToken)
	}
	if cfg.Server.Auth.ProxyToken != "proxy-secret" {
		t.Errorf("ProxyToken = %q", cfg.Server.Auth.ProxyToken)
	}
}

func TestAuthOmittedIsValid(t *testing.T) {
	// No auth block at all: admin token auto-generates at boot, proxy is open.
	cfg, _, err := Parse([]byte(minimalValid))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if cfg.Server.Auth.AdminToken != "" || cfg.Server.Auth.ProxyToken != "" {
		t.Errorf("expected empty auth tokens, got %+v", cfg.Server.Auth)
	}
}

func TestAuthBlankTokenRejected(t *testing.T) {
	y := strings.Replace(minimalValid, "  port: 8080",
		"  port: 8080\n  auth:\n    admin_token: \"   \"", 1)
	_, _, err := Parse([]byte(y))
	if err == nil || !strings.Contains(err.Error(), "must not be blank") {
		t.Fatalf("err = %v, want blank-token error", err)
	}
}

func TestAuthShortTokenWarns(t *testing.T) {
	y := strings.Replace(minimalValid, "  port: 8080",
		"  port: 8080\n  auth:\n    admin_token: \"short\"", 1)
	_, warnings, err := Parse([]byte(y))
	if err != nil {
		t.Fatalf("Parse: %v (short token should warn, not fail)", err)
	}
	if len(warnings) != 1 || !strings.Contains(warnings[0], "shorter than 16") {
		t.Errorf("warnings = %v, want one short-token warning", warnings)
	}
}

func TestExampleConfigIsValid(t *testing.T) {
	raw, err := os.ReadFile("../../config.example.yaml")
	if err != nil {
		t.Fatalf("reading config.example.yaml: %v", err)
	}
	cfg, warnings, err := Parse(raw)
	if err != nil {
		t.Fatalf("example config invalid: %v", err)
	}
	if len(warnings) != 0 {
		t.Errorf("example config produced warnings: %v", warnings)
	}
	if len(cfg.Services) != 4 || len(cfg.Routes) != 5 {
		t.Errorf("example config: %d services, %d routes; want 4, 5",
			len(cfg.Services), len(cfg.Routes))
	}
	pool := cfg.VRAMPools["gpu_0_vram"]
	if pool.DefaultService != nil {
		t.Error("example gpu_0_vram should be a cold pool (default_service null)")
	}
}
