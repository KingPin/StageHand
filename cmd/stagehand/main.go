// Command stagehand is the StageHand VRAM multiplexer & reverse proxy.
//
// Boot sequence (PRD §2.2): load + validate config, connect to the
// Docker daemon, verify every configured container exists (fail loudly),
// then serve. SIGINT/SIGTERM shut down gracefully; SIGHUP hot-reloads
// the configuration.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
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
	flag.Parse()

	if *showVersion {
		fmt.Println("stagehand", version.Version)
		return nil
	}

	level := slog.LevelInfo
	if *debug {
		level = slog.LevelDebug
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(log)

	cfg, warnings, err := config.Load(*cfgPath)
	if err != nil {
		return fmt.Errorf("loading %s: %w", *cfgPath, err)
	}
	for _, w := range warnings {
		log.Warn("config warning", "warning", w)
	}

	docker, err := dockerctl.Connect(cfg.Server.DockerSocketPath)
	if err != nil {
		return err
	}

	// Boot validation: every configured container must exist NOW —
	// failing at first request would be far worse (PRD §2.2).
	bootCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	names := make([]string, 0, len(cfg.Services))
	for _, svc := range cfg.Services {
		names = append(names, svc.ContainerName)
	}
	err = dockerctl.ValidateContainers(bootCtx, docker, names)
	cancel()
	if err != nil {
		return fmt.Errorf("boot validation: %w", err)
	}
	log.Info("boot validation passed", "containers", len(names))

	srv, err := server.New(cfg, docker, clock.New(), log)
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
