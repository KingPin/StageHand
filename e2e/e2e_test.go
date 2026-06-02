//go:build e2e

// Package e2e exercises StageHand against a REAL Docker daemon.
//
// Run with:
//
//	STAGEHAND_E2E=1 go test -tags e2e -v ./e2e/
//
// Requirements: a reachable Docker daemon and the tiny traefik/whoami
// image (pulled automatically). Two throwaway containers are created on
// fixed localhost ports and removed afterwards.
package e2e

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/benbjohnson/clock"

	"github.com/KingPin/StageHand/internal/config"
	"github.com/KingPin/StageHand/internal/dockerctl"
	"github.com/KingPin/StageHand/internal/server"
)

const (
	alphaName = "stagehand-e2e-alpha"
	betaName  = "stagehand-e2e-beta"
	alphaPort = "18181"
	betaPort  = "18182"
)

func dockerCLI(t *testing.T, args ...string) string {
	t.Helper()
	out, err := exec.Command("docker", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("docker %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return strings.TrimSpace(string(out))
}

func setup(t *testing.T) (*server.Server, dockerctl.Client) {
	t.Helper()
	if os.Getenv("STAGEHAND_E2E") != "1" {
		t.Skip("set STAGEHAND_E2E=1 to run the real-Docker e2e suite")
	}

	// Created (not started) containers on fixed localhost ports.
	exec.Command("docker", "rm", "-f", alphaName, betaName).Run() // pre-clean
	dockerCLI(t, "create", "--name", alphaName, "-p", "127.0.0.1:"+alphaPort+":80", "traefik/whoami")
	dockerCLI(t, "create", "--name", betaName, "-p", "127.0.0.1:"+betaPort+":80", "traefik/whoami")
	t.Cleanup(func() {
		exec.Command("docker", "rm", "-f", alphaName, betaName).Run()
	})

	docker, err := dockerctl.Connect("/var/run/docker.sock")
	if err != nil {
		t.Fatal(err)
	}

	pool := "gpu0"
	cfg := &config.Config{
		Server: config.Server{Host: "127.0.0.1", Port: 0, MaxQueueSize: 100},
		VRAMPools: map[string]config.VRAMPool{
			pool: {GracePeriodSeconds: 0, CooldownSeconds: 0},
		},
		Services: map[string]config.Service{
			"alpha": {ContainerName: alphaName, TargetURL: "http://127.0.0.1:" + alphaPort,
				HealthPath: "/", StartupTimeoutSeconds: 60, VRAMPool: &pool},
			"beta": {ContainerName: betaName, TargetURL: "http://127.0.0.1:" + betaPort,
				HealthPath: "/", StartupTimeoutSeconds: 60, VRAMPool: &pool},
		},
		Routes: []config.Route{
			{PathPrefix: "/alpha", Service: "alpha"},
			{PathPrefix: "/beta", Service: "beta"},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := dockerctl.ValidateContainers(ctx, docker, []string{alphaName, betaName}); err != nil {
		t.Fatalf("boot validation: %v", err)
	}

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	srv, err := server.New(cfg, docker, clock.New(), log)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Close)

	watchCtx, cancelWatch := context.WithCancel(context.Background())
	t.Cleanup(cancelWatch)
	go srv.Watcher().Run(watchCtx)

	return srv, docker
}

func running(t *testing.T, docker dockerctl.Client, name string) bool {
	t.Helper()
	info, err := docker.InspectByName(context.Background(), name)
	if err != nil {
		t.Fatal(err)
	}
	return info.Running
}

func TestE2ERealSwapAndMutualExclusion(t *testing.T) {
	srv, docker := setup(t)
	front := httptest.NewServer(srv.Handler())
	defer front.Close()

	// Cold start alpha through a real docker start + health poll.
	resp, err := http.Get(front.URL + "/alpha")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || !strings.Contains(string(body), "Hostname") {
		t.Fatalf("alpha response = %d %q, want whoami output", resp.StatusCode, body)
	}
	if !running(t, docker, alphaName) || running(t, docker, betaName) {
		t.Fatal("want exactly alpha running after first request")
	}

	// Real swap: beta in, alpha out.
	resp2, err := http.Get(front.URL + "/beta")
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp2.Body)
	resp2.Body.Close()
	if resp2.StatusCode != 200 {
		t.Fatalf("beta response = %d", resp2.StatusCode)
	}
	if running(t, docker, alphaName) {
		t.Error("alpha still running after swap — mutual exclusion violated")
	}
	if !running(t, docker, betaName) {
		t.Error("beta not running after swap")
	}

	// Concurrent burst back to alpha: all served, one container at end.
	var wg sync.WaitGroup
	codes := make([]int, 5)
	for i := range codes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := http.Get(front.URL + "/alpha")
			if err != nil {
				codes[i] = -1
				return
			}
			io.Copy(io.Discard, r.Body)
			r.Body.Close()
			codes[i] = r.StatusCode
		}()
	}
	wg.Wait()
	for i, c := range codes {
		if c != 200 {
			t.Errorf("concurrent request %d = %d, want 200", i, c)
		}
	}
	if running(t, docker, betaName) || !running(t, docker, alphaName) {
		t.Error("want exactly alpha running after concurrent burst")
	}
}

func TestE2EExternalStopReconciles(t *testing.T) {
	srv, docker := setup(t)
	front := httptest.NewServer(srv.Handler())
	defer front.Close()

	if r, err := http.Get(front.URL + "/alpha"); err == nil {
		io.Copy(io.Discard, r.Body)
		r.Body.Close()
	} else {
		t.Fatal(err)
	}

	// A human stops the active container out-of-band.
	dockerCLI(t, "stop", "-t", "1", alphaName)

	// StageHand notices via the events stream and recovers on demand.
	deadline := time.Now().Add(20 * time.Second)
	for {
		r, err := http.Get(front.URL + "/alpha")
		if err == nil {
			io.Copy(io.Discard, r.Body)
			r.Body.Close()
			if r.StatusCode == 200 {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("alpha never recovered after external stop")
		}
		time.Sleep(500 * time.Millisecond)
	}
	if !running(t, docker, alphaName) {
		t.Error("alpha not running after recovery")
	}
}
