// Command stagehand is the StageHand VRAM multiplexer & reverse proxy.
//
// Boot sequence (PRD §2.2): load + validate config, connect to the
// Docker daemon, verify every configured container exists (fail loudly),
// then serve. SIGINT/SIGTERM shut down gracefully; SIGHUP hot-reloads
// the configuration.
package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/benbjohnson/clock"

	"github.com/KingPin/StageHand/internal/config"
	"github.com/KingPin/StageHand/internal/dockerctl"
	"github.com/KingPin/StageHand/internal/server"
	"github.com/KingPin/StageHand/internal/version"
)

// disableAdminAuthEnv is the explicit, env-only escape hatch that turns off
// admin authentication (PRD §5). Env vars are trusted; this is read once at
// boot and cannot be flipped by a config hot-reload.
const disableAdminAuthEnv = "STAGEHAND_DISABLE_ADMIN_AUTH"

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "stagehand:", err)
		os.Exit(1)
	}
}

func run() error {
	cfgPath := flag.String("config", "config.yaml", "path to config.yaml")
	debug := flag.Bool("debug", false, "enable debug logging")
	showVersion := flag.Bool("version", false, "print version and exit")
	healthcheck := flag.Bool("healthcheck", false, "probe /stagehand/healthz and exit (for Docker HEALTHCHECK)")
	flag.Parse()

	if *showVersion {
		fmt.Println("stagehand", version.Version)
		return nil
	}

	if *healthcheck {
		return runHealthcheck(*cfgPath)
	}

	level := slog.LevelInfo
	if *debug {
		level = slog.LevelDebug
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(log)

	// Resolve admin-auth disabling before anything else so the warning banner
	// lands at the very top of the console on every startup (PRD §5).
	adminAuthDisabled := adminAuthDisabledFromEnv(log)
	if adminAuthDisabled {
		printNoAdminAuthBanner()
	}

	cfg, warnings, err := config.Load(*cfgPath)
	if err != nil {
		return fmt.Errorf("loading %s: %w", *cfgPath, err)
	}
	for _, w := range warnings {
		log.Warn("config warning", "warning", w)
	}

	// Admin auth is on by default. Always mint a process-stable fallback token
	// when auth is not disabled, so a hot reload that drops admin_token can
	// never leave the control plane unauthenticated. Only surface it when it's
	// actually the active token (no admin_token configured at boot).
	auth := server.AuthOptions{AdminDisabled: adminAuthDisabled}
	if !adminAuthDisabled {
		token, err := generateToken()
		if err != nil {
			return fmt.Errorf("generating admin token: %w", err)
		}
		auth.GenAdminToken = token
		if cfg.Server.Auth.AdminToken == "" {
			log.Warn("no server.auth.admin_token configured; generated a random admin token for this session",
				"header", "X-Stagehand-Admin-Token", "token", token)
		}
	}

	docker, err := dockerctl.Connect(cfg.Server.DockerSocketPath)
	if err != nil {
		return err
	}

	// Boot validation: every configured container must exist NOW —
	// failing at first request would be far worse (PRD §2.2).
	names := cfg.ContainerNames()
	bootCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	err = dockerctl.ValidateContainers(bootCtx, docker, names)
	cancel()
	if err != nil {
		return fmt.Errorf("boot validation: %w", err)
	}
	log.Info("boot validation passed", "containers", len(names))

	srv, err := server.New(cfg, docker, clock.New(), log, auth)
	if err != nil {
		return err
	}
	srv.SetConfigSource(*cfgPath)

	addr := net.JoinHostPort(cfg.Server.Host, strconv.Itoa(cfg.Server.Port))
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listening on %s: %w", addr, err)
	}

	// SIGINT/SIGTERM end Run gracefully; SIGHUP hot-reloads config.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	hup := make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)
	defer signal.Stop(hup)
	go func() {
		for range hup {
			log.Info("SIGHUP received; reloading configuration")
			if err := srv.ReloadFromSource(); err != nil {
				log.Error("reload failed; keeping previous configuration", "err", err)
			}
		}
	}()

	log.Info("starting stagehand", "version", version.Version, "config", *cfgPath)
	return srv.Run(ctx, ln)
}

// adminAuthDisabledFromEnv reports whether STAGEHAND_DISABLE_ADMIN_AUTH is set
// to a truthy value. A set-but-unparseable value keeps auth enabled (fail
// safe) and is surfaced as a warning.
func adminAuthDisabledFromEnv(log *slog.Logger) bool {
	v, ok := os.LookupEnv(disableAdminAuthEnv)
	if !ok {
		return false
	}
	disabled, err := strconv.ParseBool(v)
	if err != nil {
		log.Warn("ignoring "+disableAdminAuthEnv+": not a boolean; admin auth stays enabled", "value", v)
		return false
	}
	return disabled
}

// printNoAdminAuthBanner writes a hard-to-miss warning to stderr that the
// admin control plane is unauthenticated.
func printNoAdminAuthBanner() {
	const banner = `
########################################################################
#  WARNING: ADMIN AUTHENTICATION IS DISABLED                           #
#  ` + disableAdminAuthEnv + ` is set.
#  The /stagehand/* control plane (status, swap, pool stop, reload)    #
#  is reachable WITHOUT any token. Do not expose this listener on an   #
#  untrusted network.                                                  #
########################################################################
`
	fmt.Fprint(os.Stderr, banner)
}

// generateToken returns a 256-bit URL-safe random token.
func generateToken() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

// runHealthcheck probes the local /stagehand/healthz endpoint.
// It loads the config only to read Server.Port; a 200 response exits 0,
// anything else (non-200, network error, timeout) exits non-zero.
func runHealthcheck(cfgPath string) error {
	cfg, _, err := config.Load(cfgPath)
	if err != nil {
		return fmt.Errorf("healthcheck: loading config %s: %w", cfgPath, err)
	}
	url := "http://127.0.0.1:" + strconv.Itoa(cfg.Server.Port) + "/stagehand/healthz"
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return fmt.Errorf("healthcheck: GET %s: %w", url, err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("healthcheck: %s returned %d", url, resp.StatusCode)
	}
	return nil
}
