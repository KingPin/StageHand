package server

import (
	"net/http"
	"slices"
)

// preflight answers OPTIONS immediately (PRD §5.3): the configured
// origin is echoed and the browser's requested headers are allowed
// verbatim — '*' breaks credentialed requests, echoing does not.
func preflight(w http.ResponseWriter, r *http.Request, allowed []string) {
	// Vary is set even when no CORS headers are emitted: caches must
	// never serve an allowed-origin response to a different origin.
	w.Header().Set("Vary", "Origin")
	if origin := allowedOrigin(r, allowed); origin != "" {
		h := w.Header()
		h.Set("Access-Control-Allow-Origin", origin)
		h.Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS, PUT, DELETE")
		if req := r.Header.Get("Access-Control-Request-Headers"); req != "" {
			h.Set("Access-Control-Allow-Headers", req)
		} else {
			h.Set("Access-Control-Allow-Headers", "*")
		}
		h.Set("Access-Control-Max-Age", "600")
	}
	w.WriteHeader(http.StatusOK)
}

// setCORSOrigin marks non-preflight responses so browsers accept them.
func setCORSOrigin(w http.ResponseWriter, r *http.Request, allowed []string) {
	w.Header().Set("Vary", "Origin") // unconditional: see preflight
	if origin := allowedOrigin(r, allowed); origin != "" {
		w.Header().Set("Access-Control-Allow-Origin", origin)
	}
}

// allowedOrigin returns the origin to echo, or "" when CORS headers
// should be omitted (no Origin header, or origin not allowed).
func allowedOrigin(r *http.Request, allowed []string) string {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return ""
	}
	if slices.Contains(allowed, "*") || slices.Contains(allowed, origin) {
		return origin
	}
	return ""
}
