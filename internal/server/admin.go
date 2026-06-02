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
func (s *Server) handleStageHand(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	switch {
	case r.Method == http.MethodGet && path == "/stagehand/status":
		s.handleStatus(w, r)
	case r.Method == http.MethodPost && strings.HasPrefix(path, "/stagehand/swap/"):
		s.handleAdminSwap(w, strings.TrimPrefix(path, "/stagehand/swap/"))
	case r.Method == http.MethodPost && strings.HasPrefix(path, "/stagehand/pool/") && strings.HasSuffix(path, "/stop"):
		name := strings.TrimSuffix(strings.TrimPrefix(path, "/stagehand/pool/"), "/stop")
		s.handleAdminPoolStop(w, name)
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

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	out := statusJSON{
		Status:          "healthy",
		Version:         version.Version,
		VRAMPools:       make(map[string]poolStatusJSON, len(s.pools)),
		AlwaysOnHealthy: map[string]string{},
	}

	for name, p := range s.pools {
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
	for name, svc := range s.services {
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

func (s *Server) handleAdminSwap(w http.ResponseWriter, serviceName string) {
	svc, ok := s.services[serviceName]
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

func (s *Server) handleAdminPoolStop(w http.ResponseWriter, poolName string) {
	pool, ok := s.pools[poolName]
	if !ok {
		writeError(w, http.StatusNotFound, "unknown pool", poolName)
		return
	}
	outcome := pool.AdminStop()
	writeJSON(w, http.StatusAccepted, map[string]string{"pool": poolName, "result": string(outcome)})
}
