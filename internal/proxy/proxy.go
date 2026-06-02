// Package proxy provides StageHand's transport layer: a streaming-safe
// HTTP reverse proxy (SSE chunks propagate unbuffered, PRD §4.1), JSON
// request-body model peeking for routing (PRD §2.1), and a transparent
// WebSocket tunnel (PRD §4.2).
package proxy

import (
	"encoding/json"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"time"
)

// sharedTransport is reused by every service proxy: backends are few and
// long-lived, so pooled connections amortize dials. There is deliberately
// no ResponseHeaderTimeout — non-streaming AI backends may generate for
// minutes before the first response byte.
var sharedTransport = &http.Transport{
	DialContext: (&net.Dialer{
		Timeout:   10 * time.Second,
		KeepAlive: 30 * time.Second,
	}).DialContext,
	MaxIdleConnsPerHost: 32,
	IdleConnTimeout:     90 * time.Second,
	ForceAttemptHTTP2:   true,
}

// New builds a streaming-safe reverse proxy for one backend target.
//
// FlushInterval -1 flushes after every write from the backend, which is
// what guarantees the PRD's no-buffering chunk propagation for SSE and
// chunked responses. Response headers (Content-Type, Cache-Control,
// Transfer-Encoding) pass through unmodified by ReverseProxy default.
func New(target *url.URL, log *slog.Logger) *httputil.ReverseProxy {
	return &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(target)
			pr.SetXForwarded()
			// Present the backend's own host, not the proxy's: AI web
			// UIs (ComfyUI, A1111) validate Host against their bind.
			pr.Out.Host = target.Host
		},
		FlushInterval: -1,
		Transport:     sharedTransport,
		ErrorLog:      slog.NewLogLogger(log.Handler(), slog.LevelError),
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			log.Error("upstream round-trip failed",
				"target", target.String(), "path", r.URL.Path, "err", err)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadGateway)
			_ = json.NewEncoder(w).Encode(map[string]string{
				"error":  "upstream request failed",
				"detail": err.Error(),
			})
		},
	}
}
