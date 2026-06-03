package server

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/benbjohnson/clock"

	"github.com/KingPin/StageHand/internal/config"
	"github.com/KingPin/StageHand/internal/dockerctl"
)

// authRig is a minimal one-service server used to exercise the auth gates.
type authRig struct {
	front       *httptest.Server
	gotProxyHdr *atomic.Value // last proxyTokenHeader value the backend saw
	gotAdminHdr *atomic.Value // last adminTokenHeader value the backend saw
}

func newAuthRig(t *testing.T, adminCfgToken, proxyCfgToken string, opts AuthOptions) authRig {
	t.Helper()
	proxyHdr, adminHdr := &atomic.Value{}, &atomic.Value{}
	proxyHdr.Store("")
	adminHdr.Store("")
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proxyHdr.Store(r.Header.Get(proxyTokenHeader))
		adminHdr.Store(r.Header.Get(adminTokenHeader))
		w.WriteHeader(http.StatusOK)
		io.WriteString(w, "ok")
	}))
	t.Cleanup(backend.Close)

	docker := dockerctl.NewFake("gamma-c")
	cfg := &config.Config{
		Server: config.Server{
			Host: "127.0.0.1", Port: 8080,
			CORSAllowedOrigins: []string{"*"},
			MaxQueueSize:       10,
			Auth:               config.Auth{AdminToken: adminCfgToken, ProxyToken: proxyCfgToken},
		},
		Services: map[string]config.Service{
			"gamma": {ContainerName: "gamma-c", TargetURL: backend.URL,
				HealthPath: "/", StartupTimeoutSeconds: 30, VRAMPool: nil},
		},
		Routes: []config.Route{{PathPrefix: "/proxy", Service: "gamma"}},
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv, err := New(cfg, docker, clock.New(), log, opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(srv.Close)
	front := httptest.NewServer(srv.Handler())
	t.Cleanup(front.Close)
	return authRig{front: front, gotProxyHdr: proxyHdr, gotAdminHdr: adminHdr}
}

func (a authRig) do(t *testing.T, method, path string, headers map[string]string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, a.front.URL+path, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request %s %s: %v", method, path, err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

func TestAuthAdminGate(t *testing.T) {
	const token = "admin-secret-token-1234"
	rig := newAuthRig(t, token, "", AuthOptions{})

	cases := []struct {
		name   string
		header map[string]string
		want   int
	}{
		{"no header", nil, http.StatusUnauthorized},
		{"wrong token", map[string]string{adminTokenHeader: "nope"}, http.StatusUnauthorized},
		{"correct token", map[string]string{adminTokenHeader: token}, http.StatusOK},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := rig.do(t, http.MethodGet, "/stagehand/status", tc.header)
			if resp.StatusCode != tc.want {
				t.Errorf("status = %d, want %d", resp.StatusCode, tc.want)
			}
		})
	}
}

func TestAuthAdminDisabledSkipsGate(t *testing.T) {
	rig := newAuthRig(t, "ignored-when-disabled", "", AuthOptions{AdminDisabled: true})
	resp := rig.do(t, http.MethodGet, "/stagehand/status", nil)
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200 (admin auth disabled)", resp.StatusCode)
	}
}

func TestAuthGeneratedFallbackToken(t *testing.T) {
	const gen = "generated-fallback-token-xyz"
	// No config admin_token: the generated fallback must authenticate.
	rig := newAuthRig(t, "", "", AuthOptions{GenAdminToken: gen})

	if resp := rig.do(t, http.MethodGet, "/stagehand/status", nil); resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("no header: status = %d, want 401", resp.StatusCode)
	}
	if resp := rig.do(t, http.MethodGet, "/stagehand/status",
		map[string]string{adminTokenHeader: gen}); resp.StatusCode != http.StatusOK {
		t.Errorf("generated token: status = %d, want 200", resp.StatusCode)
	}
}

func TestAuthConfigTokenOverridesGenerated(t *testing.T) {
	const cfgToken, gen = "config-admin-token-abc", "generated-fallback-token-xyz"
	rig := newAuthRig(t, cfgToken, "", AuthOptions{GenAdminToken: gen})

	// The generated fallback must NOT work once a config token is set.
	if resp := rig.do(t, http.MethodGet, "/stagehand/status",
		map[string]string{adminTokenHeader: gen}); resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("generated token while config set: status = %d, want 401", resp.StatusCode)
	}
	if resp := rig.do(t, http.MethodGet, "/stagehand/status",
		map[string]string{adminTokenHeader: cfgToken}); resp.StatusCode != http.StatusOK {
		t.Errorf("config token: status = %d, want 200", resp.StatusCode)
	}
}

func TestAuthProxyGate(t *testing.T) {
	const token = "proxy-secret-token-1234"
	// Admin disabled so we isolate the proxy gate.
	rig := newAuthRig(t, "", token, AuthOptions{AdminDisabled: true})

	if resp := rig.do(t, http.MethodGet, "/proxy", nil); resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("no proxy token: status = %d, want 401", resp.StatusCode)
	}
	if resp := rig.do(t, http.MethodGet, "/proxy",
		map[string]string{proxyTokenHeader: "wrong"}); resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("wrong proxy token: status = %d, want 401", resp.StatusCode)
	}
	if resp := rig.do(t, http.MethodGet, "/proxy",
		map[string]string{proxyTokenHeader: token}); resp.StatusCode != http.StatusOK {
		t.Errorf("correct proxy token: status = %d, want 200", resp.StatusCode)
	}
}

func TestAuthProxyOpenWhenUnset(t *testing.T) {
	rig := newAuthRig(t, "", "", AuthOptions{AdminDisabled: true})
	if resp := rig.do(t, http.MethodGet, "/proxy", nil); resp.StatusCode != http.StatusOK {
		t.Errorf("proxy with no token configured: status = %d, want 200 (open)", resp.StatusCode)
	}
}

func TestAuthTokenHeadersStrippedFromBackend(t *testing.T) {
	const token = "proxy-secret-token-1234"
	rig := newAuthRig(t, "", token, AuthOptions{AdminDisabled: true})

	// Send both StageHand secrets; neither must reach the backend.
	resp := rig.do(t, http.MethodGet, "/proxy", map[string]string{
		proxyTokenHeader: token,
		adminTokenHeader: "an-admin-token-a-client-happened-to-send",
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if got := rig.gotProxyHdr.Load().(string); got != "" {
		t.Errorf("backend saw %s = %q, want it stripped", proxyTokenHeader, got)
	}
	if got := rig.gotAdminHdr.Load().(string); got != "" {
		t.Errorf("backend saw %s = %q, want it stripped", adminTokenHeader, got)
	}
}

func TestAuthRuntimeRejectedWhenEnabledButNoToken(t *testing.T) {
	// Admin auth enabled (not disabled) but neither a config token nor a
	// generated fallback: New must refuse rather than silently expose
	// /stagehand/*.
	docker := dockerctl.NewFake("gamma-c")
	cfg := &config.Config{
		Server: config.Server{Host: "127.0.0.1", Port: 8080, MaxQueueSize: 10},
		Services: map[string]config.Service{
			"gamma": {ContainerName: "gamma-c", TargetURL: "http://gamma-c:9", HealthPath: "/", StartupTimeoutSeconds: 30},
		},
		Routes: []config.Route{{PathPrefix: "/proxy", Service: "gamma"}},
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	_, err := New(cfg, docker, clock.New(), log, AuthOptions{})
	if err == nil {
		t.Fatal("New succeeded, want error for enabled admin auth with no token")
	}
	if !strings.Contains(err.Error(), "no admin token") {
		t.Errorf("error = %v, want it to mention the missing admin token", err)
	}
}

func TestAuthPreflightNotBlocked(t *testing.T) {
	// OPTIONS preflight must succeed without any token, for both admin and
	// proxy paths, so browsers can complete CORS negotiation.
	rig := newAuthRig(t, "admin-secret-token-1234", "proxy-secret-token-1234", AuthOptions{})
	for _, path := range []string{"/stagehand/status", "/proxy"} {
		resp := rig.do(t, http.MethodOptions, path, map[string]string{"Origin": "https://example.com"})
		if resp.StatusCode != http.StatusOK {
			t.Errorf("OPTIONS %s: status = %d, want 200", path, resp.StatusCode)
		}
	}
}
