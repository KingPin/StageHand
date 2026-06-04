package server

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/KingPin/StageHand/internal/version"
)

// TestLiveness verifies the unauthenticated /stagehand/healthz endpoint.
// Admin auth is ENABLED (real token) to prove the carve-out bypasses the gate.
func TestLiveness(t *testing.T) {
	const adminToken = "admin-secret-healthz-test"
	rig := newAuthRig(t, adminToken, "", AuthOptions{})

	t.Run("healthz returns 200 without token", func(t *testing.T) {
		resp := rig.do(t, http.MethodGet, "/stagehand/healthz", nil)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("GET /stagehand/healthz: status = %d, want 200", resp.StatusCode)
		}

		var body map[string]string
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			t.Fatalf("decoding response body: %v", err)
		}

		if got := body["status"]; got != "ok" {
			t.Errorf(`body["status"] = %q, want "ok"`, got)
		}
		if got := body["version"]; got == "" {
			t.Errorf(`body["version"] is empty, want non-empty`)
		} else if got != version.Version {
			t.Errorf(`body["version"] = %q, want %q`, got, version.Version)
		}
	})

	t.Run("status still requires token (carve-out is narrow)", func(t *testing.T) {
		resp := rig.do(t, http.MethodGet, "/stagehand/status", nil)
		if resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("GET /stagehand/status (no token): status = %d, want 401", resp.StatusCode)
		}
	})
}
