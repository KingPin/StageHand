package server

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/benbjohnson/clock"

	"github.com/KingPin/StageHand/internal/config"
	"github.com/KingPin/StageHand/internal/dockerctl"
)

// backend simulates one AI service: /health gated 200/503, /ws echoes
// raw lines after an upgrade, anything else identifies itself and echoes
// the request body.
type backend struct {
	name     string
	healthOK atomic.Bool
	srv      *httptest.Server
}

func newBackend(t *testing.T, name string) *backend {
	t.Helper()
	b := &backend{name: name}
	b.healthOK.Store(true)
	b.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/health":
			if b.healthOK.Load() {
				w.WriteHeader(http.StatusOK)
			} else {
				w.WriteHeader(http.StatusServiceUnavailable)
			}
		case strings.HasPrefix(r.URL.Path, "/ws"):
			conn, buf, err := w.(http.Hijacker).Hijack()
			if err != nil {
				return
			}
			defer conn.Close()
			fmt.Fprint(conn, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n")
			for {
				line, err := buf.Reader.ReadString('\n')
				if err != nil {
					return
				}
				fmt.Fprintf(conn, "%s-echo:%s", b.name, line)
			}
		default:
			body, _ := io.ReadAll(r.Body)
			fmt.Fprintf(w, "svc:%s:%s", b.name, body)
		}
	}))
	t.Cleanup(b.srv.Close)
	return b
}

type testRig struct {
	front    *httptest.Server
	server   *Server
	docker   *dockerctl.FakeClient
	backends map[string]*backend
}

func ptr(s string) *string { return &s }

// newRig builds a full server: pooled alpha/beta on gpu0 + always-on
// gamma, with model routing on /v1/chat/completions.
func newRig(t *testing.T, maxQueue int) *testRig {
	t.Helper()
	backends := map[string]*backend{}
	for _, n := range []string{"alpha", "beta", "gamma"} {
		backends[n] = newBackend(t, n)
	}
	docker := dockerctl.NewFake("alpha-c", "beta-c", "gamma-c")

	cfg := &config.Config{
		Server: config.Server{
			Host: "127.0.0.1", Port: 8080,
			CORSAllowedOrigins: []string{"*"},
			MaxQueueSize:       maxQueue,
		},
		VRAMPools: map[string]config.VRAMPool{
			"gpu0": {GracePeriodSeconds: 0, CooldownSeconds: 0},
		},
		Services: map[string]config.Service{
			"alpha": {ContainerName: "alpha-c", TargetURL: backends["alpha"].srv.URL,
				HealthPath: "/health", StartupTimeoutSeconds: 30, VRAMPool: ptr("gpu0")},
			"beta": {ContainerName: "beta-c", TargetURL: backends["beta"].srv.URL,
				HealthPath: "/health", StartupTimeoutSeconds: 30, VRAMPool: ptr("gpu0")},
			"gamma": {ContainerName: "gamma-c", TargetURL: backends["gamma"].srv.URL,
				HealthPath: "/health", StartupTimeoutSeconds: 30, VRAMPool: nil},
		},
		Routes: []config.Route{
			{PathPrefix: "/v1/embeddings", Service: "gamma"},
			{PathPrefix: "/v1/chat/completions", Service: "alpha",
				Models: map[string]string{"m-alpha": "alpha", "m-beta": "beta"}},
			{PathPrefix: "/ws", Service: "gamma"},
		},
	}

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv, err := New(cfg, docker, clock.New(), log)
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	t.Cleanup(srv.Close)

	front := httptest.NewServer(srv.Handler())
	t.Cleanup(front.Close)
	return &testRig{front: front, server: srv, docker: docker, backends: backends}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestPooledServiceColdStartEndToEnd(t *testing.T) {
	rig := newRig(t, 10)

	resp, err := http.Post(rig.front.URL+"/v1/chat/completions",
		"application/json", strings.NewReader(`{"messages":[]}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, body %s", resp.StatusCode, body)
	}
	if want := `svc:alpha:{"messages":[]}`; string(body) != want {
		t.Errorf("body = %q, want %q (fallback service, body intact)", body, want)
	}
	if !slices.Contains(rig.docker.Calls(), "start:alpha-c") {
		t.Error("alpha container was not cold-started")
	}
}

func TestModelRoutingSelectsService(t *testing.T) {
	rig := newRig(t, 10)

	payload := `{"model":"m-beta","messages":["hi"]}`
	resp, err := http.Post(rig.front.URL+"/v1/chat/completions",
		"application/json", strings.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	if want := "svc:beta:" + payload; string(body) != want {
		t.Errorf("body = %q, want %q (model routed to beta, body replayed)", body, want)
	}
	if running := rig.docker.Running(); !slices.Equal(running, []string{"beta-c"}) {
		t.Errorf("running = %v, want only beta-c", running)
	}
}

func TestUnmatchedRouteReturns404WithKnownRoutes(t *testing.T) {
	rig := newRig(t, 10)

	resp, err := http.Get(rig.front.URL + "/v2/never-configured")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
	var payload struct {
		Error       string   `json:"error"`
		KnownRoutes []string `json:"known_routes"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("404 body not JSON: %v", err)
	}
	if len(payload.KnownRoutes) != 3 {
		t.Errorf("known_routes = %v, want 3 entries", payload.KnownRoutes)
	}
}

func TestAlwaysOnBypassesDocker(t *testing.T) {
	rig := newRig(t, 10)

	resp, err := http.Get(rig.front.URL + "/v1/embeddings")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	if !strings.HasPrefix(string(body), "svc:gamma") {
		t.Errorf("body = %q, want gamma response", body)
	}
	if calls := rig.docker.Calls(); len(calls) != 0 {
		t.Errorf("docker calls = %v, want none for always-on service", calls)
	}
}

func TestDockerFailureReturns502WithDetail(t *testing.T) {
	rig := newRig(t, 10)
	rig.docker.SetStartErr("alpha-c", fmt.Errorf("no such image: ghost:latest"))

	resp, err := http.Get(rig.front.URL + "/v1/chat/completions")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", resp.StatusCode)
	}
	var payload map[string]string
	json.NewDecoder(resp.Body).Decode(&payload)
	if !strings.Contains(payload["detail"], "no such image") {
		t.Errorf("502 detail = %q, want docker error detail", payload["detail"])
	}
}

func TestQueueOverflowReturns429WithRetryAfter(t *testing.T) {
	rig := newRig(t, 1) // queue bound of 1
	rig.backends["alpha"].healthOK.Store(false)

	// One request occupies the queue while alpha can't get healthy.
	go http.Get(rig.front.URL + "/v1/chat/completions")
	waitFor(t, "swap in flight", func() bool {
		return slices.Contains(rig.docker.Calls(), "start:alpha-c")
	})

	waitFor(t, "queue full response", func() bool {
		resp, err := http.Get(rig.front.URL + "/v1/chat/completions")
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusTooManyRequests {
			return false
		}
		if ra := resp.Header.Get("Retry-After"); ra == "" {
			t.Fatal("429 missing Retry-After header")
		}
		return true
	})

	rig.backends["alpha"].healthOK.Store(true) // drain the queued request
}

func TestCORSPreflightEchoesRequestedHeaders(t *testing.T) {
	rig := newRig(t, 10)

	req, _ := http.NewRequest(http.MethodOptions, rig.front.URL+"/v1/chat/completions", nil)
	req.Header.Set("Origin", "http://localhost:5173")
	req.Header.Set("Access-Control-Request-Method", "POST")
	req.Header.Set("Access-Control-Request-Headers", "authorization, x-custom")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("preflight status = %d, want 200", resp.StatusCode)
	}
	if got := resp.Header.Get("Access-Control-Allow-Origin"); got != "http://localhost:5173" {
		t.Errorf("Allow-Origin = %q, want the echoed origin", got)
	}
	if got := resp.Header.Get("Access-Control-Allow-Headers"); got != "authorization, x-custom" {
		t.Errorf("Allow-Headers = %q, want echoed request headers", got)
	}
	if got := resp.Header.Get("Access-Control-Allow-Methods"); !strings.Contains(got, "POST") {
		t.Errorf("Allow-Methods = %q, want methods list", got)
	}
	// Preflight must NOT touch docker or backends.
	if calls := rig.docker.Calls(); len(calls) != 0 {
		t.Errorf("preflight triggered docker calls: %v", calls)
	}
}

func TestCORSOriginOnActualResponse(t *testing.T) {
	rig := newRig(t, 10)

	req, _ := http.NewRequest(http.MethodGet, rig.front.URL+"/v1/embeddings", nil)
	req.Header.Set("Origin", "http://localhost:3000")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if got := resp.Header.Get("Access-Control-Allow-Origin"); got != "http://localhost:3000" {
		t.Errorf("Allow-Origin on response = %q, want echoed origin", got)
	}
}

func TestWebSocketThroughFullServer(t *testing.T) {
	rig := newRig(t, 10)

	u, _ := url.Parse(rig.front.URL)
	conn, err := net.DialTimeout("tcp", u.Host, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	fmt.Fprintf(conn, "GET /ws HTTP/1.1\r\nHost: %s\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGVzdA==\r\n\r\n", u.Host)

	rd := bufio.NewReader(conn)
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	status, err := rd.ReadString('\n')
	if err != nil || !strings.Contains(status, "101") {
		t.Fatalf("handshake = %q, %v; want 101", status, err)
	}
	for { // skip handshake headers
		line, err := rd.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		if line == "\r\n" {
			break
		}
	}

	fmt.Fprint(conn, "ping\n")
	line, err := rd.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	if want := "gamma-echo:ping\n"; line != want {
		t.Errorf("ws echo = %q, want %q", line, want)
	}
}
