package server

import (
	"bufio"
	"fmt"
	"net"
	"net/http"
)

// statusRecorder wraps an http.ResponseWriter to capture the response status
// code for access logging. It transparently forwards Flush (SSE) and Hijack
// (WebSocket) to the underlying writer so wrapping it never disables
// unbuffered streaming or connection hijacking — both core to StageHand.
type statusRecorder struct {
	http.ResponseWriter
	status  int
	written bool
}

var (
	_ http.Flusher  = (*statusRecorder)(nil)
	_ http.Hijacker = (*statusRecorder)(nil)
)

func (r *statusRecorder) WriteHeader(code int) {
	if !r.written {
		r.status = code
		r.written = true
	}
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	if !r.written {
		r.status = http.StatusOK // implicit 200 on first write
		r.written = true
	}
	return r.ResponseWriter.Write(b)
}

// Flush forwards to the underlying writer when it supports flushing, so
// httputil.ReverseProxy's unbuffered SSE streaming keeps working.
func (r *statusRecorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Hijack forwards to the underlying writer so transparent WebSocket
// tunneling keeps working. On a successful hijack it records the status as
// 101 Switching Protocols — WriteHeader is never called on a hijacked
// connection, so without this the access log would misreport every
// WebSocket upgrade as status=200.
func (r *statusRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hj, ok := r.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, fmt.Errorf("underlying ResponseWriter does not support hijacking")
	}
	conn, rw, err := hj.Hijack()
	if err == nil {
		// Record 101 and lock it so a later spurious WriteHeader can't
		// overwrite it (a hijacked connection is semantically upgraded).
		r.status = http.StatusSwitchingProtocols
		r.written = true
	}
	return conn, rw, err
}

// logRequests logs one structured line per request: method, path, status,
// and wall-clock duration (timed with the injected clock).
func (s *Server) logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := s.clk.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		s.log.Info("request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", rec.status,
			"duration_ms", s.clk.Since(start).Milliseconds())
	})
}
