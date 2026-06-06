package server

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/benbjohnson/clock"

	"github.com/KingPin/StageHand/internal/config"
	"github.com/KingPin/StageHand/internal/dockerctl"
)

// newBodyCapRig builds a minimal server with one always-on service behind
// /echo and the given request body-size cap (0 disables it).
func newBodyCapRig(t *testing.T, maxRequestBytes int64) *httptest.Server {
	t.Helper()
	be := newBackend(t, "echo")
	docker := dockerctl.NewFake("echo-c")

	cfg := &config.Config{
		Server: config.Server{
			Host: "127.0.0.1", Port: 8080,
			CORSAllowedOrigins: []string{"*"},
			MaxQueueSize:       10,
			MaxRequestBytes:    maxRequestBytes,
		},
		Services: map[string]config.Service{
			"echo": {ContainerName: "echo-c", TargetURL: be.srv.URL,
				HealthPath: "/health", StartupTimeoutSeconds: 30, VRAMPool: nil},
		},
		Routes: []config.Route{
			{PathPrefix: "/echo", Service: "echo"},
		},
	}

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv, err := New(cfg, docker, clock.New(), log, AuthOptions{AdminDisabled: true})
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	t.Cleanup(srv.Close)
	front := httptest.NewServer(srv.Handler())
	t.Cleanup(front.Close)
	return front
}

// TestBodyCapRejectsByContentLength: a declared Content-Length over the cap
// is rejected with 413 before the body is read.
func TestBodyCapRejectsByContentLength(t *testing.T) {
	front := newBodyCapRig(t, 64)

	resp, err := http.Post(front.URL+"/echo", "application/json",
		strings.NewReader(strings.Repeat("x", 128)))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413", resp.StatusCode)
	}
}

// TestBodyCapRejectsChunkedOverflow: a chunked body (no Content-Length)
// that overflows the cap is caught by MaxBytesReader while proxying and
// surfaces as 413.
func TestBodyCapRejectsChunkedOverflow(t *testing.T) {
	front := newBodyCapRig(t, 64)

	// ContentLength = -1 (chunked) by handing http a reader of unknown size.
	req, err := http.NewRequest("POST", front.URL+"/echo",
		io.NopCloser(strings.NewReader(strings.Repeat("y", 256))))
	if err != nil {
		t.Fatal(err)
	}
	req.ContentLength = -1
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413", resp.StatusCode)
	}
}

// TestBodyCapAllowsWithinLimit: a body at/under the cap proxies normally
// with the body intact.
func TestBodyCapAllowsWithinLimit(t *testing.T) {
	front := newBodyCapRig(t, 1024)

	payload := strings.Repeat("z", 256)
	resp, err := http.Post(front.URL+"/echo", "application/json",
		strings.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body within cap)", resp.StatusCode)
	}
	if want := "svc:echo:" + payload; string(body) != want {
		t.Errorf("body = %q, want %q (body forwarded intact)", body, want)
	}
}

// TestBodyCapDisabledAllowsLargeBody: with the cap disabled (0), a large
// body is forwarded untouched.
func TestBodyCapDisabledAllowsLargeBody(t *testing.T) {
	front := newBodyCapRig(t, 0)

	payload := strings.Repeat("w", 4096)
	resp, err := http.Post(front.URL+"/echo", "application/json",
		strings.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200 (cap disabled)", resp.StatusCode)
	}
	if want := "svc:echo:" + payload; string(body) != want {
		t.Errorf("body = %q, want %q", body, want)
	}
}
