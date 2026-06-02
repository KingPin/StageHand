package server

import (
	"net/http"
	"slices"
)

// preflight answers OPTIONS immediately (PRD §5.3): the configured
// origin is echoed and the browser's requested headers are allowed
// verbatim — '*' breaks credentialed requests, echoing does not.
func (s *Server) preflight(w http.ResponseWriter, r *http.Request) {
	if origin := s.allowedOrigin(r); origin != "" {
		h := w.Header()
		h.Set("Access-Control-Allow-Origin", origin)
		h.Set("Vary", "Origin")
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
func (s *Server) setCORSOrigin(w http.ResponseWriter, r *http.Request) {
	if origin := s.allowedOrigin(r); origin != "" {
		w.Header().Set("Access-Control-Allow-Origin", origin)
		w.Header().Set("Vary", "Origin")
	}
}

// allowedOrigin returns the origin to echo, or "" when CORS headers
// should be omitted (no Origin header, or origin not allowed).
func (s *Server) allowedOrigin(r *http.Request) string {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return ""
	}
	if slices.Contains(s.corsOrigins, "*") || slices.Contains(s.corsOrigins, origin) {
		return origin
	}
	return ""
}
