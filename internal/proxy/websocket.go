package proxy

import (
	"crypto/tls"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// IsWebSocketUpgrade reports whether the request is a WebSocket
// handshake (Connection: Upgrade + Upgrade: websocket).
func IsWebSocketUpgrade(r *http.Request) bool {
	if !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
		return false
	}
	for _, v := range strings.Split(r.Header.Get("Connection"), ",") {
		if strings.EqualFold(strings.TrimSpace(v), "upgrade") {
			return true
		}
	}
	return false
}

// ConnTracker remembers live tunneled connections so graceful shutdown
// can close them (PRD §6). Safe for concurrent use.
type ConnTracker struct {
	mu    sync.Mutex
	conns map[net.Conn]struct{}
}

func NewConnTracker() *ConnTracker {
	return &ConnTracker{conns: map[net.Conn]struct{}{}}
}

func (t *ConnTracker) add(c net.Conn) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.conns[c] = struct{}{}
}

func (t *ConnTracker) remove(c net.Conn) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.conns, c)
}

// Len returns the number of live tunneled connections.
func (t *ConnTracker) Len() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.conns)
}

// CloseAll force-closes every tracked connection.
func (t *ConnTracker) CloseAll() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for c := range t.conns {
		c.Close()
	}
	clear(t.conns)
}

// Tunnel proxies a WebSocket upgrade transparently: it replays the
// client's handshake to the backend, relays the backend's response, and
// on 101 pipes raw bytes bi-directionally until either side closes
// (PRD §4.2). Frames are never parsed — the tunnel is protocol-agnostic.
//
// A non-101 backend response (e.g. 403) is relayed to the client as-is.
// tracker may be nil.
func Tunnel(w http.ResponseWriter, r *http.Request, target *url.URL, tracker *ConnTracker, log *slog.Logger) error {
	backend, err := dialTarget(target)
	if err != nil {
		http.Error(w, "backend unreachable", http.StatusBadGateway)
		return fmt.Errorf("dialing %s: %w", target.Host, err)
	}
	defer backend.Close()

	// Replay the client's handshake against the backend's host.
	out := r.Clone(r.Context())
	out.Host = target.Host
	out.URL.Scheme = ""
	out.URL.Host = ""
	if err := out.Write(backend); err != nil {
		http.Error(w, "backend handshake failed", http.StatusBadGateway)
		return fmt.Errorf("writing handshake: %w", err)
	}

	hj, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "websocket unsupported", http.StatusInternalServerError)
		return fmt.Errorf("response writer is not a hijacker")
	}
	client, clientBuf, err := hj.Hijack()
	if err != nil {
		return fmt.Errorf("hijacking client connection: %w", err)
	}
	defer client.Close()

	if tracker != nil {
		tracker.add(client)
		defer tracker.remove(client)
	}

	// From here on, both conns are raw: relay the backend's handshake
	// response bytes (101 or a rejection) straight through, then pipe.
	// clientBuf.Reader drains any bytes buffered during the hijack before
	// reading the raw conn directly, so no client frames are lost.
	errc := make(chan error, 2)
	go copyConn(client, backend, errc)           // backend -> client
	go copyConn(backend, clientBuf.Reader, errc) // client -> backend

	// First side to error/EOF tears the tunnel down; deferred Closes
	// unblock the other copy goroutine.
	if err := <-errc; err != nil && !isClosedConn(err) {
		log.Debug("websocket tunnel closed", "target", target.Host, "err", err)
	}
	return nil
}

// copyConn relays src into dst and reports the outcome on errc exactly
// once, recovering from any panic so a misbehaving connection can tear
// down the tunnel cleanly instead of crashing the process. The deferred
// recover is a no-op on the normal return (recover() yields nil), so the
// normal send and the panic send are mutually exclusive — never both,
// never neither.
func copyConn(dst io.Writer, src io.Reader, errc chan<- error) {
	defer func() {
		if rec := recover(); rec != nil {
			errc <- fmt.Errorf("panic in websocket copy: %v", rec)
		}
	}()
	_, err := io.Copy(dst, src)
	errc <- err
}

func dialTarget(target *url.URL) (net.Conn, error) {
	host := target.Host
	if target.Port() == "" {
		switch target.Scheme {
		case "https", "wss":
			host = net.JoinHostPort(target.Hostname(), "443")
		default:
			host = net.JoinHostPort(target.Hostname(), "80")
		}
	}
	d := net.Dialer{Timeout: 10 * time.Second}
	if target.Scheme == "https" || target.Scheme == "wss" {
		return tls.DialWithDialer(&d, "tcp", host, nil)
	}
	return d.Dial("tcp", host)
}

func isClosedConn(err error) bool {
	return err == nil || strings.Contains(err.Error(), "use of closed network connection")
}
