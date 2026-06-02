// Package server wires StageHand together: routing, CORS, the
// orchestrator pools, and request forwarding (HTTP/SSE/WebSocket).
package server

import (
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"

	"github.com/benbjohnson/clock"

	"github.com/KingPin/StageHand/internal/config"
	"github.com/KingPin/StageHand/internal/dockerctl"
	"github.com/KingPin/StageHand/internal/orchestrator"
	"github.com/KingPin/StageHand/internal/proxy"
	"github.com/KingPin/StageHand/internal/router"
)

// service is the per-backend runtime: its reverse proxy and, for pooled
// services, the owning pool (nil = always-on).
type service struct {
	name   string
	target *url.URL
	proxy  *httputil.ReverseProxy
	pool   *orchestrator.Pool
}

// Server hosts StageHand's HTTP surface.
type Server struct {
	log         *slog.Logger
	corsOrigins []string
	router      *router.Router
	services    map[string]*service
	pools       map[string]*orchestrator.Pool
	watcher     *orchestrator.Watcher
	tracker     *proxy.ConnTracker
}

// New builds the full runtime from a validated config: one orchestrator
// pool per vram_pool, a reverse proxy per service, the events watcher
// (caller runs it), and the route table.
func New(cfg *config.Config, docker dockerctl.Client, clk clock.Clock, log *slog.Logger) (*Server, error) {
	s := &Server{
		log:         log,
		corsOrigins: cfg.Server.CORSAllowedOrigins,
		router:      router.New(cfg.Routes),
		services:    make(map[string]*service, len(cfg.Services)),
		pools:       map[string]*orchestrator.Pool{},
		watcher:     orchestrator.NewWatcher(docker, log),
		tracker:     proxy.NewConnTracker(),
	}

	// Pools first: group member services per vram_pool.
	for poolName, poolCfg := range cfg.VRAMPools {
		var members []orchestrator.MemberConfig
		for svcName, svc := range cfg.Services {
			if svc.VRAMPool == nil || *svc.VRAMPool != poolName {
				continue
			}
			members = append(members, orchestrator.MemberConfig{
				Name:           svcName,
				ContainerName:  svc.ContainerName,
				HealthURL:      strings.TrimSuffix(svc.TargetURL, "/") + svc.HealthPath,
				StartupTimeout: svc.StartupTimeout(),
				MaxQueue:       cfg.QueueSize(svc),
			})
		}
		defaultSvc := ""
		if poolCfg.DefaultService != nil {
			defaultSvc = *poolCfg.DefaultService
		}
		pool := orchestrator.NewPool(orchestrator.PoolConfig{
			Name:           poolName,
			GracePeriod:    poolCfg.GracePeriod(),
			Cooldown:       poolCfg.Cooldown(),
			DefaultService: defaultSvc,
			Members:        members,
		}, docker, clk, log)
		s.pools[poolName] = pool
		for _, m := range members {
			s.watcher.Register(m.ContainerName, pool)
		}
	}

	for name, svc := range cfg.Services {
		target, err := url.Parse(svc.TargetURL)
		if err != nil {
			return nil, fmt.Errorf("service %q target_url: %w", name, err)
		}
		rt := &service{
			name:   name,
			target: target,
			proxy:  proxy.New(target, log.With("service", name)),
		}
		if svc.VRAMPool != nil {
			rt.pool = s.pools[*svc.VRAMPool]
		}
		s.services[name] = rt
	}
	return s, nil
}

// Handler returns the root HTTP handler.
func (s *Server) Handler() http.Handler { return http.HandlerFunc(s.handle) }

// Watcher returns the docker events watcher; the caller owns running it.
func (s *Server) Watcher() *orchestrator.Watcher { return s.watcher }

// Close stops all pool managers and tears down tunneled connections.
func (s *Server) Close() {
	for _, p := range s.pools {
		p.Close()
	}
	s.tracker.CloseAll()
}
