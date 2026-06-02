package server

import (
	"bufio"
	"fmt"
	"io"
	"maps"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/KingPin/StageHand/internal/config"
)

func TestReloadAddsRouteForNewRequests(t *testing.T) {
	rig := newRig(t, 10)

	// /v1/extra is unknown before the reload.
	resp, _ := http.Get(rig.front.URL + "/v1/extra")
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("pre-reload status = %d, want 404", resp.StatusCode)
	}

	cfg2 := *rig.cfg
	cfg2.Routes = append(slices.Clone(rig.cfg.Routes),
		config.Route{PathPrefix: "/v1/extra", Service: "gamma"})
	if err := rig.server.Reload(&cfg2); err != nil {
		t.Fatalf("Reload: %v", err)
	}

	resp2, err := http.Get(rig.front.URL + "/v1/extra")
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	body, _ := io.ReadAll(resp2.Body)
	if !strings.HasPrefix(string(body), "svc:gamma") {
		t.Errorf("post-reload body = %q, want gamma response", body)
	}

	// Pre-existing routes keep working.
	resp3, err := http.Get(rig.front.URL + "/v1/embeddings")
	if err != nil {
		t.Fatal(err)
	}
	resp3.Body.Close()
	if resp3.StatusCode != http.StatusOK {
		t.Errorf("old route status = %d after reload, want 200", resp3.StatusCode)
	}
}

func TestReloadRemovedServiceFlushesItsQueue(t *testing.T) {
	rig := newRig(t, 10)
	rig.backends["beta"].healthOK.Store(false) // beta swap hangs

	clientDone := make(chan int, 1)
	go func() {
		resp, err := http.Post(rig.front.URL+"/v1/chat/completions",
			"application/json", strings.NewReader(`{"model":"m-beta"}`))
		if err != nil {
			clientDone <- -1
			return
		}
		defer resp.Body.Close()
		clientDone <- resp.StatusCode
	}()
	waitFor(t, "beta swap in flight", func() bool {
		return slices.Contains(rig.docker.Calls(), "start:beta-c")
	})

	// New config drops beta entirely: the pool membership changes, the
	// pool is rebuilt, and beta's queued waiter must flush with 503.
	cfg2 := *rig.cfg
	cfg2.Services = maps.Clone(rig.cfg.Services)
	delete(cfg2.Services, "beta")
	cfg2.Routes = []config.Route{
		{PathPrefix: "/v1/embeddings", Service: "gamma"},
		{PathPrefix: "/v1/chat/completions", Service: "alpha",
			Models: map[string]string{"m-alpha": "alpha"}},
		{PathPrefix: "/ws", Service: "gamma"},
	}
	if err := rig.server.Reload(&cfg2); err != nil {
		t.Fatalf("Reload: %v", err)
	}

	select {
	case code := <-clientDone:
		if code != http.StatusServiceUnavailable {
			t.Errorf("queued client got %d, want 503 (service removed)", code)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("queued client not flushed after its service was removed")
	}

	// The rebuilt pool serves alpha normally.
	resp, err := http.Get(rig.front.URL + "/v1/chat/completions")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("alpha after reload = %d, want 200", resp.StatusCode)
	}
}

func TestReloadUnchangedPoolPreservesQueuedRequests(t *testing.T) {
	rig := newRig(t, 10)
	rig.backends["beta"].healthOK.Store(false)

	clientDone := make(chan string, 1)
	go func() {
		resp, err := http.Post(rig.front.URL+"/v1/chat/completions",
			"application/json", strings.NewReader(`{"model":"m-beta"}`))
		if err != nil {
			clientDone <- err.Error()
			return
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		clientDone <- fmt.Sprintf("%d:%s", resp.StatusCode, body)
	}()
	waitFor(t, "beta swap in flight", func() bool {
		return slices.Contains(rig.docker.Calls(), "start:beta-c")
	})

	// Reload with the pool untouched (only an unrelated route added):
	// the queued request must survive.
	cfg2 := *rig.cfg
	cfg2.Routes = append(slices.Clone(rig.cfg.Routes),
		config.Route{PathPrefix: "/v1/unrelated", Service: "gamma"})
	if err := rig.server.Reload(&cfg2); err != nil {
		t.Fatalf("Reload: %v", err)
	}

	select {
	case got := <-clientDone:
		t.Fatalf("queued request was flushed by reload (%s); unchanged pool must be reused", got)
	case <-time.After(300 * time.Millisecond):
		// still queued — correct
	}

	rig.backends["beta"].healthOK.Store(true)
	select {
	case got := <-clientDone:
		if !strings.HasPrefix(got, "200:svc:beta") {
			t.Errorf("queued client result = %q, want 200 from beta", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("queued request never completed after health opened")
	}
}

func TestInFlightStreamSurvivesReload(t *testing.T) {
	rig := newRig(t, 10)

	resp, err := http.Get(rig.front.URL + "/v1/embeddings/stream")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	rd := bufio.NewReader(resp.Body)

	read := func(want string) {
		t.Helper()
		for {
			line, err := rd.ReadString('\n')
			if err != nil {
				t.Fatalf("stream read: %v (want %q)", err, want)
			}
			if line = strings.TrimSpace(line); line != "" {
				if line != want {
					t.Fatalf("stream line = %q, want %q", line, want)
				}
				return
			}
		}
	}
	read("data: gamma-chunk-1")

	// Reload mid-stream.
	cfg2 := *rig.cfg
	cfg2.Routes = append(slices.Clone(rig.cfg.Routes),
		config.Route{PathPrefix: "/v1/extra", Service: "gamma"})
	if err := rig.server.Reload(&cfg2); err != nil {
		t.Fatalf("Reload: %v", err)
	}

	close(rig.backends["gamma"].streamRelease)
	read("data: gamma-chunk-2") // the in-flight stream survived
}

func TestReloadRejectsMissingContainerKeepsOldConfig(t *testing.T) {
	rig := newRig(t, 10)

	cfg2 := *rig.cfg
	cfg2.Services = maps.Clone(rig.cfg.Services)
	cfg2.Services["delta"] = config.Service{
		ContainerName: "ghost-c", // not on the (fake) host
		TargetURL:     "http://ghost:1234",
		HealthPath:    "/health", StartupTimeoutSeconds: 30,
	}
	err := rig.server.Reload(&cfg2)
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("Reload err = %v, want missing-container rejection", err)
	}

	// Old config still serves.
	resp, err := http.Get(rig.front.URL + "/v1/embeddings")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("old config broken after rejected reload: %d", resp.StatusCode)
	}
}

func TestReloadEndpointFromFile(t *testing.T) {
	rig := newRig(t, 10)
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")

	// Valid new config: gamma-only.
	yaml := fmt.Sprintf(`
server:
  port: 8080
services:
  gamma:
    container_name: "gamma-c"
    target_url: "%s"
    health_path: "/health"
routes:
  - path_prefix: "/v1/embeddings"
    service: "gamma"
`, rig.backends["gamma"].srv.URL)
	if err := os.WriteFile(path, []byte(yaml), 0o644); err != nil {
		t.Fatal(err)
	}
	rig.server.SetConfigSource(path)

	resp, err := http.Post(rig.front.URL+"/stagehand/reload", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("reload endpoint = %d (%s), want 200", resp.StatusCode, body)
	}

	// The chat route is gone; embeddings remains.
	r404, _ := http.Get(rig.front.URL + "/v1/chat/completions")
	r404.Body.Close()
	if r404.StatusCode != http.StatusNotFound {
		t.Errorf("removed route status = %d, want 404", r404.StatusCode)
	}
	r200, _ := http.Get(rig.front.URL + "/v1/embeddings")
	r200.Body.Close()
	if r200.StatusCode != http.StatusOK {
		t.Errorf("kept route status = %d, want 200", r200.StatusCode)
	}
}

func TestReloadEndpointInvalidConfigKept(t *testing.T) {
	rig := newRig(t, 10)
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte("docker_api_version: \"v1.40\"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	rig.server.SetConfigSource(path)

	resp, err := http.Post(rig.front.URL+"/stagehand/reload", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("reload of invalid config = %d, want 400", resp.StatusCode)
	}

	// A bad reload never drops traffic (PRD §7).
	r, _ := http.Get(rig.front.URL + "/v1/embeddings")
	r.Body.Close()
	if r.StatusCode != http.StatusOK {
		t.Errorf("traffic broken after rejected reload: %d", r.StatusCode)
	}
}
