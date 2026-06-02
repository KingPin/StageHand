package server

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/KingPin/StageHand/internal/dockerctl"
)

type runRig struct {
	baseURL  string
	docker   *dockerctl.FakeClient
	backends map[string]*backend
	cancel   context.CancelFunc
	done     chan error // Run's return value
}

// newRunRig serves a real listener via Server.Run for lifecycle tests.
func newRunRig(t *testing.T, maxQueue int) *runRig {
	t.Helper()
	srv, docker, backends, _ := newRigParts(t, maxQueue)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	rig := &runRig{
		baseURL:  "http://" + ln.Addr().String(),
		docker:   docker,
		backends: backends,
		cancel:   cancel,
		done:     make(chan error, 1),
	}
	go func() { rig.done <- srv.Run(ctx, ln) }()

	// Wait for the listener to answer before returning.
	waitFor(t, "server accepting", func() bool {
		resp, err := http.Get(rig.baseURL + "/stagehand/status")
		if err != nil {
			return false
		}
		resp.Body.Close()
		return true
	})
	return rig
}

func (r *runRig) shutdownAndWait(t *testing.T) {
	t.Helper()
	r.cancel()
	select {
	case err := <-r.done:
		if err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after shutdown")
	}
}

func TestShutdownReleasesQueuedRequestsWith503(t *testing.T) {
	rig := newRunRig(t, 10)
	rig.backends["beta"].healthOK.Store(false) // beta swap will hang

	clientDone := make(chan int, 1)
	go func() {
		resp, err := http.Post(rig.baseURL+"/v1/chat/completions",
			"application/json", strings.NewReader(`{"model":"m-beta"}`))
		if err != nil {
			clientDone <- -1
			return
		}
		defer resp.Body.Close()
		clientDone <- resp.StatusCode
	}()
	waitFor(t, "request queued behind swap", func() bool {
		return slices.Contains(rig.docker.Calls(), "start:beta-c")
	})

	rig.shutdownAndWait(t)

	if code := <-clientDone; code != http.StatusServiceUnavailable {
		t.Errorf("queued client got %d, want clean 503", code)
	}
	// PRD §6: containers are left as-is — no shutdown-triggered stops.
	for _, c := range rig.docker.Calls() {
		if strings.HasPrefix(c, "stop:") {
			t.Errorf("shutdown stopped a container (%s); must leave them running", c)
		}
	}
}

func TestShutdownClosesWebSocketTunnels(t *testing.T) {
	rig := newRunRig(t, 10)

	u, _ := url.Parse(rig.baseURL)
	conn, err := net.DialTimeout("tcp", u.Host, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	fmt.Fprintf(conn, "GET /ws HTTP/1.1\r\nHost: %s\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n", u.Host)

	rd := bufio.NewReader(conn)
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	status, err := rd.ReadString('\n')
	if err != nil || !strings.Contains(status, "101") {
		t.Fatalf("handshake = %q, %v", status, err)
	}
	for {
		line, err := rd.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		if line == "\r\n" {
			break
		}
	}
	// Confirm tunnel live.
	fmt.Fprint(conn, "ping\n")
	if _, err := rd.ReadString('\n'); err != nil {
		t.Fatalf("tunnel not live: %v", err)
	}

	rig.shutdownAndWait(t)

	// The hijacked tunnel must be force-closed by the tracker.
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := rd.ReadString('\n'); err == nil {
		t.Error("websocket still readable after shutdown, want closed")
	}
}

func TestShutdownRefusesNewConnections(t *testing.T) {
	rig := newRunRig(t, 10)
	rig.shutdownAndWait(t)

	if _, err := http.Get(rig.baseURL + "/stagehand/status"); err == nil {
		t.Error("request succeeded after shutdown, want connection refused")
	}
}
