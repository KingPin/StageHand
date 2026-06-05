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

	t.Run("non-GET healthz requires token (carve-out is GET-only)", func(t *testing.T) {
		// The healthz carve-out in the handler is GET-only; a POST falls
		// through to the admin gate and must require a token.
		resp := rig.do(t, http.MethodPost, "/stagehand/healthz", nil)
		if resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("POST /stagehand/healthz (no token): status = %d, want 401", resp.StatusCode)
		}
	})

	t.Run("status still requires token (carve-out is narrow)", func(t *testing.T) {
		resp := rig.do(t, http.MethodGet, "/stagehand/status", nil)
		if resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("GET /stagehand/status (no token): status = %d, want 401", resp.StatusCode)
		}
	})

	t.Run("healthz 200 when admin auth disabled", func(t *testing.T) {
		// Admin auth explicitly disabled — carve-out must still respond 200.
		noAuthRig := newAuthRig(t, "", "", AuthOptions{AdminDisabled: true})
		resp := noAuthRig.do(t, http.MethodGet, "/stagehand/healthz", nil)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("GET /stagehand/healthz (auth disabled): status = %d, want 200", resp.StatusCode)
		}
		var body map[string]string
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			t.Fatalf("decoding response body: %v", err)
		}
		if got := body["status"]; got != "ok" {
			t.Errorf(`body["status"] = %q, want "ok"`, got)
		}
	})

	t.Run("healthz 200 with proxy token set and no token sent", func(t *testing.T) {
		// Both admin AND proxy tokens set — healthz must still be reachable
		// with no tokens (carve-out sits above the proxy gate too).
		bothRig := newAuthRig(t, "admin-tok", "proxy-tok", AuthOptions{})
		resp := bothRig.do(t, http.MethodGet, "/stagehand/healthz", nil) // no proxy token
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("GET /stagehand/healthz (no tokens, proxy gate active): status = %d, want 200", resp.StatusCode)
		}
		var body map[string]string
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			t.Fatalf("decoding response body: %v", err)
		}
		if got := body["status"]; got != "ok" {
			t.Errorf(`body["status"] = %q, want "ok"`, got)
		}
	})
}
