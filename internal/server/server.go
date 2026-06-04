// Package server wires StageHand together: routing, CORS, the
// orchestrator pools, and request forwarding (HTTP/SSE/WebSocket).
package server

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"runtime/debug"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

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
	name          string
	containerName string
	target        *url.URL
	healthURL     string
	proxy         *httputil.ReverseProxy
	pool          *orchestrator.Pool
}

// runtime is everything derived from one config revision. The handler
// reads it through one atomic load, so hot reload is a pointer swap:
// new requests see the new world, in-flight requests finish in the old.
type runtime struct {
	cfg         *config.Config // the revision this runtime was built from
	corsOrigins []string
	router      *router.Router
	services    map[string]*service
	pools       map[string]*orchestrator.Pool
	poolSigs    map[string]string // pool name -> config signature
	// adminToken gates /stagehand/* ("" = admin auth disabled); proxyToken
	// gates all other traffic ("" = open). Resolved per revision so a hot
	// reload can change tokens (PRD §5).
	adminToken string
	proxyToken string
}

// AuthOptions carries the process-level auth decisions made at boot, before
// any config revision: whether admin auth was explicitly disabled via env, and
// the stable fallback admin token to use when no admin_token is configured.
type AuthOptions struct {
	AdminDisabled bool
	GenAdminToken string
}

// Server hosts StageHand's HTTP surface.
type Server struct {
	log     *slog.Logger
	docker  dockerctl.Client
	clk     clock.Clock
	watcher *orchestrator.Watcher
	tracker *proxy.ConnTracker

	rt       atomic.Pointer[runtime]
	reloadMu sync.Mutex // serializes Reload; handlers stay lock-free
	cfgPath  string     // source for ReloadFromSource ("" = disabled)
	adminMux *http.ServeMux
	auth     AuthOptions // process-level auth decisions, fixed for the run
}

// New builds the full runtime from a validated config: one orchestrator
// pool per vram_pool, a reverse proxy per service, the events watcher
// (caller runs it), and the route table.
func New(cfg *config.Config, docker dockerctl.Client, clk clock.Clock, log *slog.Logger, auth AuthOptions) (*Server, error) {
	s := &Server{
		log:     log,
		docker:  docker,
		clk:     clk,
		watcher: orchestrator.NewWatcher(docker, log),
		tracker: proxy.NewConnTracker(),
		auth:    auth,
	}
	rt, err := s.buildRuntime(cfg, nil)
	if err != nil {
		return nil, err
	}
	s.rt.Store(rt)
	s.syncWatcher(rt)
	s.adminMux = s.buildAdminMux()
	return s, nil
}

// SetConfigSource sets the config file path used by ReloadFromSource
// (the /stagehand/reload endpoint and SIGHUP).
func (s *Server) SetConfigSource(path string) { s.cfgPath = path }

// Handler returns the root HTTP handler, wrapped so a panic in the request
// path becomes a 500 rather than a dropped connection.
func (s *Server) Handler() http.Handler {
	return recoverPanics(s.logRequests(http.HandlerFunc(s.handle)), s.log)
}

// recoverPanics converts a panic in the request path into a generic 500.
// http.ErrAbortHandler is re-panicked so net/http's (and the reverse
// proxy's) intentional-abort semantics are preserved. If the response has
// already started streaming when the panic fires, the 500 is best-effort:
// WriteHeader becomes a logged no-op and the partially-sent response is torn
// down by the client — the inherent limit of recover-based middleware.
func recoverPanics(next http.Handler, log *slog.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			rec := recover()
			if rec == nil {
				return
			}
			if rec == http.ErrAbortHandler {
				panic(rec)
			}
			log.Error("panic recovered in request handler",
				"method", r.Method, "path", r.URL.Path, "panic", rec,
				"stack", string(debug.Stack()))
			writeError(w, http.StatusInternalServerError, "internal server error", "")
		}()
		next.ServeHTTP(w, r)
	})
}

// Watcher returns the docker events watcher; the caller owns running it.
func (s *Server) Watcher() *orchestrator.Watcher { return s.watcher }

// Close stops all pool managers and tears down tunneled connections.
// It serializes with Reload so a reload racing shutdown can neither
// resurrect pools nor leave freshly-built ones unclosed.
func (s *Server) Close() {
	s.reloadMu.Lock()
	defer s.reloadMu.Unlock()
	for _, p := range s.rt.Load().pools {
		p.Close()
	}
	s.tracker.CloseAll()
}

// buildRuntime constructs a runtime for cfg. Pools whose configuration
// is unchanged from prev are REUSED — their queues and active container
// survive a reload; changed or removed pools are left for the caller to
// close (flushing their queues).
func (s *Server) buildRuntime(cfg *config.Config, prev *runtime) (*runtime, error) {
	rt := &runtime{
		cfg:         cfg,
		corsOrigins: cfg.Server.CORSAllowedOrigins,
		router:      router.New(cfg.Routes),
		services:    make(map[string]*service, len(cfg.Services)),
		pools:       map[string]*orchestrator.Pool{},
		poolSigs:    map[string]string{},
		adminToken:  s.resolveAdminToken(cfg),
		proxyToken:  cfg.Server.Auth.ProxyToken,
	}

	// Fail safe: an empty adminToken means "admin auth off", which must only
	// happen when it was explicitly disabled. If auth is enabled but nothing
	// resolved (no config token, no generated fallback), refuse to build the
	// runtime rather than silently expose the control plane — this also makes
	// a hot reload that drops admin_token without a fallback get rejected.
	if !s.auth.AdminDisabled && rt.adminToken == "" {
		return nil, fmt.Errorf("admin auth is enabled but no admin token is available: " +
			"set server.auth.admin_token or provide a generated fallback")
	}

	for poolName, poolCfg := range cfg.VRAMPools {
		members := poolMembers(cfg, poolName)
		sig := poolSignature(poolCfg, members)
		rt.poolSigs[poolName] = sig

		if prev != nil && prev.poolSigs[poolName] == sig {
			rt.pools[poolName] = prev.pools[poolName] // unchanged: reuse
			continue
		}
		defaultSvc := ""
		if poolCfg.DefaultService != nil {
			defaultSvc = *poolCfg.DefaultService
		}
		rt.pools[poolName] = orchestrator.NewPool(orchestrator.PoolConfig{
			Name:           poolName,
			GracePeriod:    poolCfg.GracePeriod(),
			Cooldown:       poolCfg.Cooldown(),
			DefaultService: defaultSvc,
			Members:        members,
		}, s.docker, s.clk, s.log)
	}

	for name, svc := range cfg.Services {
		target, err := url.Parse(svc.TargetURL)
		if err != nil {
			return nil, fmt.Errorf("service %q target_url: %w", name, err)
		}
		rt2 := &service{
			name:          name,
			containerName: svc.ContainerName,
			target:        target,
			healthURL:     svc.HealthURL(),
			proxy:         proxy.New(target, s.log.With("service", name)),
		}
		if svc.VRAMPool != nil {
			rt2.pool = rt.pools[*svc.VRAMPool]
		}
		rt.services[name] = rt2
	}
	return rt, nil
}

// resolveAdminToken returns the admin token effective for cfg (PRD §5):
// "" when admin auth was disabled at boot; the configured admin_token when
// set; otherwise the process-stable generated fallback. Resolving it per
// runtime keeps it hot-reload-safe while the generated fallback stays stable
// across reloads.
func (s *Server) resolveAdminToken(cfg *config.Config) string {
	if s.auth.AdminDisabled {
		return ""
	}
	if cfg.Server.Auth.AdminToken != "" {
		return cfg.Server.Auth.AdminToken
	}
	return s.auth.GenAdminToken
}

// Reload applies a new validated configuration (PRD §7). New requests
// route against it immediately; queues on unchanged pools survive;
// changed/removed pools flush their queues with 503. Listener address
// and docker socket changes are ignored with a warning (restart needed).
func (s *Server) Reload(cfg *config.Config) error {
	s.reloadMu.Lock()
	defer s.reloadMu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := dockerctl.ValidateContainers(ctx, s.docker, cfg.ContainerNames()); err != nil {
		return fmt.Errorf("reload rejected: %w", err)
	}

	prev := s.rt.Load()
	warnRestartOnly(s.log, &prev.cfg.Server, &cfg.Server)
	next, err := s.buildRuntime(cfg, prev)
	if err != nil {
		return fmt.Errorf("reload rejected: %w", err)
	}

	s.rt.Store(next)
	s.syncWatcher(next)

	// Close pools that were rebuilt or removed: their queued waiters
	// flush with 503 (the services they wait on may no longer exist).
	closed := 0
	for name, p := range prev.pools {
		if next.pools[name] != p {
			p.Close()
			closed++
		}
	}
	s.log.Info("configuration reloaded",
		"services", len(next.services), "pools", len(next.pools), "pools_rebuilt", closed)
	return nil
}

// ReloadFromSource re-reads the config file (endpoint + SIGHUP path).
func (s *Server) ReloadFromSource() error {
	if s.cfgPath == "" {
		return fmt.Errorf("no config source configured for reload")
	}
	cfg, warnings, err := config.Load(s.cfgPath)
	if err != nil {
		return fmt.Errorf("reload rejected: %w", err)
	}
	for _, w := range warnings {
		s.log.Warn("config warning", "warning", w)
	}
	return s.Reload(cfg)
}

func (s *Server) syncWatcher(rt *runtime) {
	routes := map[string]*orchestrator.Pool{}
	for _, svc := range rt.services {
		if svc.pool != nil {
			routes[svc.containerName] = svc.pool
		}
	}
	s.watcher.ReplaceAll(routes)
}

func poolMembers(cfg *config.Config, poolName string) []orchestrator.MemberConfig {
	var members []orchestrator.MemberConfig
	for svcName, svc := range cfg.Services {
		if svc.VRAMPool == nil || *svc.VRAMPool != poolName {
			continue
		}
		members = append(members, orchestrator.MemberConfig{
			Name:           svcName,
			ContainerName:  svc.ContainerName,
			HealthURL:      svc.HealthURL(),
			StartupTimeout: svc.StartupTimeout(),
			MaxQueue:       cfg.QueueSize(svc),
		})
	}
	slices.SortFunc(members, func(a, b orchestrator.MemberConfig) int {
		return strings.Compare(a.Name, b.Name)
	})
	return members
}

// poolSignature fingerprints everything that affects a pool's behavior;
// equal signatures mean the existing pool can be reused across a reload.
func poolSignature(p config.VRAMPool, members []orchestrator.MemberConfig) string {
	var b strings.Builder
	defaultSvc := ""
	if p.DefaultService != nil {
		defaultSvc = *p.DefaultService
	}
	fmt.Fprintf(&b, "g=%d;c=%d;d=%s", p.GracePeriodSeconds, p.CooldownSeconds, defaultSvc)
	for _, m := range members {
		fmt.Fprintf(&b, "|%s:%s:%s:%s:%d",
			m.Name, m.ContainerName, m.HealthURL, m.StartupTimeout, m.MaxQueue)
	}
	return b.String()
}

// warnRestartOnly logs when a reload changes settings that only a
// restart can apply (PRD §7: documented limitation, surfaced loudly).
func warnRestartOnly(log *slog.Logger, prev, next *config.Server) {
	if prev.Host != next.Host || prev.Port != next.Port {
		log.Warn("reload cannot change the listen address; restart required",
			"active", fmt.Sprintf("%s:%d", prev.Host, prev.Port),
			"requested", fmt.Sprintf("%s:%d", next.Host, next.Port))
	}
	if prev.DockerSocketPath != next.DockerSocketPath {
		log.Warn("reload cannot change docker_socket_path; restart required",
			"active", prev.DockerSocketPath, "requested", next.DockerSocketPath)
	}
}

