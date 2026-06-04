package proxy

import (
	"bufio"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

// wsEchoBackend is an httptest server that accepts a WebSocket-style
// upgrade by hijacking, answers 101, then echoes raw lines.
func wsEchoBackend(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !IsWebSocketUpgrade(r) {
			http.Error(w, "expected upgrade", http.StatusBadRequest)
			return
		}
		conn, buf, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Errorf("backend hijack: %v", err)
			return
		}
		defer conn.Close()
		fmt.Fprintf(conn, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n")
		for {
			line, err := buf.Reader.ReadString('\n')
			if err != nil {
				return
			}
			fmt.Fprintf(conn, "echo:%s", line)
		}
	}))
}

// dialAndUpgrade opens a raw TCP conn to the front proxy and performs a
// WS-style handshake, returning the conn and reader after the 101.
func dialAndUpgrade(t *testing.T, frontURL string) (net.Conn, *bufio.Reader) {
	t.Helper()
	u, _ := url.Parse(frontURL)
	conn, err := net.DialTimeout("tcp", u.Host, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Fprintf(conn, "GET /ws HTTP/1.1\r\nHost: %s\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGVzdA==\r\nSec-WebSocket-Version: 13\r\n\r\n", u.Host)
	rd := bufio.NewReader(conn)
	status, err := rd.ReadString('\n')
	if err != nil {
		t.Fatalf("reading handshake response: %v", err)
	}
	if !strings.Contains(status, "101") {
		t.Fatalf("handshake status = %q, want 101", status)
	}
	// Skip headers.
	for {
		line, err := rd.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		if line == "\r\n" {
			return conn, rd
		}
	}
}

func newTunnelFront(t *testing.T, backendURL string, tracker *ConnTracker) *httptest.Server {
	t.Helper()
	target := mustParse(t, backendURL)
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := Tunnel(w, r, target, tracker, discardLogger()); err != nil {
			t.Logf("tunnel: %v", err)
		}
	}))
}

func TestTunnelEchoesBidirectionally(t *testing.T) {
	backend := wsEchoBackend(t)
	defer backend.Close()
	tracker := NewConnTracker()
	front := newTunnelFront(t, backend.URL, tracker)
	defer front.Close()

	conn, rd := dialAndUpgrade(t, front.URL)
	defer conn.Close()

	for _, msg := range []string{"hello", "progress-42%", "done"} {
		fmt.Fprintf(conn, "%s\n", msg)
		conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		line, err := rd.ReadString('\n')
		if err != nil {
			t.Fatalf("reading echo of %q: %v", msg, err)
		}
		if want := "echo:" + msg + "\n"; line != want {
			t.Errorf("echo = %q, want %q", line, want)
		}
	}
	if tracker.Len() != 1 {
		t.Errorf("tracker.Len() = %d during tunnel, want 1", tracker.Len())
	}
}

func TestTunnelRelaysBackendRejection(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "no websockets for you", http.StatusForbidden)
	}))
	defer backend.Close()
	front := newTunnelFront(t, backend.URL, nil)
	defer front.Close()

	u, _ := url.Parse(front.URL)
	conn, err := net.DialTimeout("tcp", u.Host, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	fmt.Fprintf(conn, "GET /ws HTTP/1.1\r\nHost: %s\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n", u.Host)

	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	status, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(status, "403") {
		t.Errorf("client saw %q, want backend's 403 relayed", status)
	}
}

func TestConnTrackerCloseAll(t *testing.T) {
	backend := wsEchoBackend(t)
	defer backend.Close()
	tracker := NewConnTracker()
	front := newTunnelFront(t, backend.URL, tracker)
	defer front.Close()

	conn, rd := dialAndUpgrade(t, front.URL)
	defer conn.Close()

	// Confirm live, then force-close via the tracker (graceful shutdown).
	fmt.Fprint(conn, "ping\n")
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := rd.ReadString('\n'); err != nil {
		t.Fatalf("tunnel not live: %v", err)
	}
	tracker.CloseAll()

	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := rd.ReadString('\n'); err == nil {
		t.Error("read succeeded after CloseAll, want closed connection")
	}
	if tracker.Len() != 0 {
		t.Errorf("tracker.Len() = %d after CloseAll, want 0", tracker.Len())
	}
}

func TestIsWebSocketUpgrade(t *testing.T) {
	mk := func(connection, upgrade string) *http.Request {
		r := httptest.NewRequest("GET", "/ws", nil)
		if connection != "" {
			r.Header.Set("Connection", connection)
		}
		if upgrade != "" {
			r.Header.Set("Upgrade", upgrade)
		}
		return r
	}
	if !IsWebSocketUpgrade(mk("Upgrade", "websocket")) {
		t.Error("plain upgrade not detected")
	}
	if !IsWebSocketUpgrade(mk("keep-alive, Upgrade", "WebSocket")) {
		t.Error("multi-token Connection + case variance not detected")
	}
	if IsWebSocketUpgrade(mk("", "")) {
		t.Error("plain request misdetected as upgrade")
	}
	if IsWebSocketUpgrade(mk("keep-alive", "")) {
		t.Error("keep-alive misdetected as upgrade")
	}
}

// panicWriter is a Writer whose Write method always panics.
type panicWriter struct{}

func (panicWriter) Write(_ []byte) (int, error) { panic("write blew up") }

// TestCopyConnRecoversPanic verifies that copyConn catches a panic in the
// underlying Write and sends a non-nil error on errc instead of crashing.
func TestCopyConnRecoversPanic(t *testing.T) {
	errc := make(chan error, 1)
	go copyConn(panicWriter{}, strings.NewReader("data"), errc)

	err := <-errc
	if err == nil {
		t.Fatal("expected non-nil error from panicking writer, got nil")
	}
}

// TestCopyConnNormal verifies that copyConn forwards bytes correctly and
// sends nil on errc when the copy succeeds.
func TestCopyConnNormal(t *testing.T) {
	var buf strings.Builder
	errc := make(chan error, 1)
	go copyConn(&buf, strings.NewReader("hello"), errc)

	if err := <-errc; err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if got := buf.String(); got != "hello" {
		t.Fatalf("buf = %q, want %q", got, "hello")
	}
}
