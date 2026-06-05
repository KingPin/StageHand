package server

import (
	"bufio"
	"bytes"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/benbjohnson/clock"
)

func TestAccessLogCapturesStatus(t *testing.T) {
	var buf bytes.Buffer
	s := &Server{log: slog.New(slog.NewTextHandler(&buf, nil)), clk: clock.New()}

	h := s.logRequests(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/v1/foo", nil))

	line := buf.String()
	for _, want := range []string{"status=404", "method=GET", "path=/v1/foo", "duration_ms"} {
		if !strings.Contains(line, want) {
			t.Errorf("log line %q missing %q", line, want)
		}
	}
}

// TestAccessLogLogsRecoveredPanic verifies the production middleware order
// (logRequests wrapping recoverPanics): a handler that panics is turned into
// a 500 by recoverPanics and that final status is still access-logged,
// rather than the log line being lost as the panic unwinds.
func TestAccessLogLogsRecoveredPanic(t *testing.T) {
	var buf bytes.Buffer
	s := &Server{log: slog.New(slog.NewTextHandler(&buf, nil)), clk: clock.New()}

	panicking := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("boom")
	})
	h := s.logRequests(recoverPanics(panicking, s.log))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/v1/x", nil))

	// status=500 appears only in the request log line (the panic Error log
	// carries no status field), so finding it proves the request was logged
	// with the recovered status.
	if line := buf.String(); !strings.Contains(line, "status=500") {
		t.Errorf("recovered-panic request not logged with status=500; got %q", line)
	}
}

func TestAccessLogDefaultStatus200(t *testing.T) {
	var buf bytes.Buffer
	s := &Server{log: slog.New(slog.NewTextHandler(&buf, nil)), clk: clock.New()}

	h := s.logRequests(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "hi")
	}))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))

	if line := buf.String(); !strings.Contains(line, "status=200") {
		t.Errorf("log line %q missing status=200", line)
	}
}

type flushSpy struct {
	http.ResponseWriter
	flushed bool
}

func (f *flushSpy) Flush() { f.flushed = true }

func TestStatusRecorderForwardsFlush(t *testing.T) {
	spy := &flushSpy{ResponseWriter: httptest.NewRecorder()}
	sr := &statusRecorder{ResponseWriter: spy}

	var _ http.Flusher = sr // compiles via package-level assertion
	sr.Flush()

	if !spy.flushed {
		t.Fatal("Flush was not forwarded to the underlying writer")
	}
}

type hijackSpy struct {
	http.ResponseWriter
	hijacked bool
}

func (h *hijackSpy) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	h.hijacked = true
	return nil, nil, nil
}

func TestStatusRecorderForwardsHijack(t *testing.T) {
	spy := &hijackSpy{ResponseWriter: httptest.NewRecorder()}
	sr := &statusRecorder{ResponseWriter: spy}

	var _ http.Hijacker = sr // compiles via package-level assertion
	if _, _, err := sr.Hijack(); err != nil {
		t.Fatalf("Hijack returned error: %v", err)
	}
	if !spy.hijacked {
		t.Fatal("Hijack was not forwarded to the underlying writer")
	}

	// A successful hijack records 101 for access-log fidelity: WriteHeader
	// is never called on a hijacked WebSocket connection, so without this
	// the log would misreport the upgrade as status=200.
	if sr.status != http.StatusSwitchingProtocols {
		t.Errorf("after Hijack, sr.status = %d, want %d (101)", sr.status, http.StatusSwitchingProtocols)
	}

	// A writer that is not a Hijacker should fail gracefully (no panic).
	srPlain := &statusRecorder{ResponseWriter: httptest.NewRecorder()}
	if _, _, err := srPlain.Hijack(); err == nil {
		t.Fatal("expected error hijacking a non-Hijacker writer, got nil")
	}
}
