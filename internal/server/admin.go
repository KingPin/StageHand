package server

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/KingPin/StageHand/internal/orchestrator"
	"github.com/KingPin/StageHand/internal/version"
)

// handleStageHand serves the reserved /stagehand/* namespace (PRD §5):
//
//	GET  /stagehand/status            — orchestrator state
//	POST /stagehand/swap/{service}    — pre-warm/force a swap
//	POST /stagehand/pool/{pool}/stop  — force a pool cold
//	POST /stagehand/reload            — hot config reload
func (s *Server) handleStageHand(w http.ResponseWriter, r *http.Request, rt *runtime) {
	path := r.URL.Path
	switch {
	case r.Method == http.MethodGet && path == "/stagehand/status":
		s.handleStatus(w, r, rt)
	case r.Method == http.MethodPost && path == "/stagehand/reload":
		s.handleReload(w)
	case r.Method == http.MethodPost && strings.HasPrefix(path, "/stagehand/swap/"):
		handleAdminSwap(w, rt, strings.TrimPrefix(path, "/stagehand/swap/"))
	case r.Method == http.MethodPost && strings.HasPrefix(path, "/stagehand/pool/") && strings.HasSuffix(path, "/stop"):
		name := strings.TrimSuffix(strings.TrimPrefix(path, "/stagehand/pool/"), "/stop")
		handleAdminPoolStop(w, rt, name)
	default:
		writeError(w, http.StatusNotFound, "unknown stagehand endpoint", path)
	}
}

type poolStatusJSON struct {
	State                string `json:"state"`
	ActiveService        string `json:"active_service,omitempty"`
	SecondsUntilCooldown *int64 `json:"seconds_until_cooldown"`
	QueuedRequestsCount  int    `json:"queued_requests_count"`
}

type statusJSON struct {
	Status          string                    `json:"status"`
	Version         string                    `json:"version"`
	VRAMPools       map[string]poolStatusJSON `json:"vram_pools"`
	AlwaysOnHealthy map[string]string         `json:"always_on_services"`
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request, rt *runtime) {
	out := statusJSON{
		Status:          "healthy",
		Version:         version.Version,
		VRAMPools:       make(map[string]poolStatusJSON, len(rt.pools)),
		AlwaysOnHealthy: map[string]string{},
	}

	for name, p := range rt.pools {
		st := p.Status()
		j := poolStatusJSON{
			State:               st.State.String(),
			ActiveService:       st.ActiveService,
			QueuedRequestsCount: st.QueuedRequests,
		}
		if st.SecondsUntilCooldown >= 0 {
			secs := st.SecondsUntilCooldown
			j.SecondsUntilCooldown = &secs
		}
		out.VRAMPools[name] = j
	}

	// Probe always-on services concurrently; they're meant to be up.
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	var mu sync.Mutex
	var wg sync.WaitGroup
	for name, svc := range rt.services {
		if svc.pool != nil {
			continue
		}
		wg.Add(1)
		go func(name string, svc *service) {
			defer wg.Done()
			state := "healthy"
			if !probeHealth(ctx, svc.healthURL) {
				state = "unhealthy"
			}
			mu.Lock()
			out.AlwaysOnHealthy[name] = state
			mu.Unlock()
		}(name, svc)
	}
	wg.Wait()

	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleReload(w http.ResponseWriter) {
	if err := s.ReloadFromSource(); err != nil {
		writeError(w, http.StatusBadRequest, "reload failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "reloaded"})
}

// probeHealth checks an always-on service's health endpoint.
func probeHealth(ctx context.Context, url string) bool {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return false
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

func handleAdminSwap(w http.ResponseWriter, rt *runtime, serviceName string) {
	svc, ok := rt.services[serviceName]
	if !ok || svc.pool == nil {
		writeError(w, http.StatusNotFound, "unknown pooled service", serviceName)
		return
	}
	outcome := svc.pool.AdminSwap(serviceName)
	status := http.StatusAccepted
	if outcome == orchestrator.AdminUnknown {
		status = http.StatusNotFound
	}
	writeJSON(w, status, map[string]string{"service": serviceName, "result": string(outcome)})
}

func handleAdminPoolStop(w http.ResponseWriter, rt *runtime, poolName string) {
	pool, ok := rt.pools[poolName]
	if !ok {
		writeError(w, http.StatusNotFound, "unknown pool", poolName)
		return
	}
	outcome := pool.AdminStop()
	writeJSON(w, http.StatusAccepted, map[string]string{"pool": poolName, "result": string(outcome)})
}
