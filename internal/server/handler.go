package server

import (
	"net/http"
	"strings"

	"github.com/KingPin/StageHand/internal/orchestrator"
	"github.com/KingPin/StageHand/internal/proxy"
)

// handle is the unified request flow (PRD §4): CORS → reserved namespace
// → route match (with body-model peek) → pool admission → forward.
func (s *Server) handle(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		s.preflight(w, r)
		return
	}
	s.setCORSOrigin(w, r)

	if strings.HasPrefix(r.URL.Path, "/stagehand/") {
		s.handleStageHand(w, r)
		return
	}

	match, ok := s.router.Match(r.URL.Path, r.Header, "")
	if ok && match.NeedsModel && r.Method == http.MethodPost && hasJSONBody(r) {
		model, err := proxy.PeekModel(r)
		if err != nil {
			writeError(w, http.StatusBadRequest, "unreadable request body", err.Error())
			return
		}
		if model != "" {
			match, ok = s.router.Match(r.URL.Path, r.Header, model)
		}
	}
	if !ok {
		writeUnmatched(w, s.router.KnownRoutes())
		return
	}

	svc, exists := s.services[match.Service]
	if !exists { // config validation prevents this; defend anyway
		writeError(w, http.StatusInternalServerError, "route target missing",
			"service "+match.Service+" is not configured")
		return
	}

	// Pooled services pass through admission (queue/swap); always-on
	// services forward directly (PRD §3: no VRAM limitation).
	if svc.pool != nil {
		res, err := svc.pool.Admit(r.Context(), svc.name)
		detail := ""
		if err != nil {
			detail = err.Error()
		}
		switch res {
		case orchestrator.AdmitGo:
		case orchestrator.AdmitQueueFull:
			w.Header().Set("Retry-After", "5")
			writeError(w, http.StatusTooManyRequests, "service queue is full", detail)
			return
		case orchestrator.AdmitDockerError:
			writeError(w, http.StatusBadGateway, "docker operation failed", detail)
			return
		case orchestrator.AdmitStartupTimeout:
			writeError(w, http.StatusGatewayTimeout, "service failed to become healthy in time", detail)
			return
		case orchestrator.AdmitShutdown:
			writeError(w, http.StatusServiceUnavailable, "stagehand is shutting down", detail)
			return
		case orchestrator.AdmitCanceled:
			return // client is gone; nothing to write
		default:
			writeError(w, http.StatusInternalServerError, "unexpected admission result", detail)
			return
		}
	}

	if proxy.IsWebSocketUpgrade(r) {
		if err := proxy.Tunnel(w, r, svc.target, s.tracker, s.log); err != nil {
			s.log.Error("websocket tunnel", "service", svc.name, "err", err)
		}
		return
	}
	svc.proxy.ServeHTTP(w, r)
}

// handleStageHand serves the reserved /stagehand/* namespace. The status
// and admin endpoints land in their own commits; unknown paths are 404.
func (s *Server) handleStageHand(w http.ResponseWriter, r *http.Request) {
	writeError(w, http.StatusNotFound, "unknown stagehand endpoint", r.URL.Path)
}

func hasJSONBody(r *http.Request) bool {
	return strings.Contains(r.Header.Get("Content-Type"), "json")
}
