package orchestrator

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
)

// TestAdminStopDuringSwapDoesNotStartTarget is the M1 regression: an admin
// stop issued while a swap is mid-flight must cancel the swap worker so it
// cannot start its target after the stop sweep. Otherwise a container is left
// running while the pool reports IDLE — a transient mutual-exclusion break
// (PRD §3.2). The worker is parked in its sweep (Stop of the active member)
// via a fake hook so the AdminStop lands deterministically before the
// worker's pre-start cancellation check.
func TestAdminStopDuringSwapDoesNotStartTarget(t *testing.T) {
	tp := newTestPool(t, clock.New(), 0, 10) // alpha, beta; both health open
	mustAdmit(t, tp.pool, "alpha")            // alpha active

	parked := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	tp.docker.SetBeforeStop(func(name string) {
		if name != "alpha-c" {
			return
		}
		blocked := false
		once.Do(func() { blocked = true })
		if blocked {
			close(parked) // the swap worker is now parked in its sweep
			<-release
		}
	})

	// Admit beta: with alpha active and grace 0, this starts a swap whose
	// sweep parks on Stop(alpha-c).
	betaDone := make(chan AdmitResult, 1)
	go func() {
		r, _ := tp.pool.Admit(context.Background(), "beta")
		betaDone <- r
	}()

	select {
	case <-parked:
	case <-time.After(5 * time.Second):
		t.Fatal("swap worker never parked in the sweep")
	}

	// Stop the pool while the swap is mid-sweep. supersedeWorker must cancel
	// the in-flight worker's context.
	if out := tp.pool.AdminStop(); out != AdminInitiated {
		t.Fatalf("AdminStop = %v, want AdminInitiated", out)
	}

	close(release) // let the orphaned worker resume; it must bail before start

	if r := <-betaDone; r != AdmitShutdown {
		t.Errorf("beta admit = %v, want AdmitShutdown (pool stopped)", r)
	}

	waitFor(t, "pool idle", func() bool {
		return tp.pool.Snapshot().State == StateIdle
	})
	// Core invariant: the superseded worker must not have started beta-c, and
	// nothing may be left running while the pool reports IDLE.
	waitFor(t, "no containers running", func() bool {
		return len(tp.docker.Running()) == 0
	})
	if slices.Contains(tp.docker.Calls(), "start:beta-c") {
		t.Errorf("beta-c was started by a superseded swap worker; calls=%v", tp.docker.Calls())
	}
}

// TestAdminStopDuringSwapStartsSelfCleansTarget covers the residual TOCTOU
// (issue #7): a supersession that lands AFTER the pre-start cancellation check
// but while the target is being started cannot abort the start. The superseded
// worker must re-check its context after the start succeeds and stop the
// container it just started, rather than leaving it running while the pool
// reports IDLE and relying on the next swap's sweep to heal it. The worker is
// parked at Start(beta-c) via a fake hook so the AdminStop lands deterministically
// inside the post-start window.
func TestAdminStopDuringSwapStartsSelfCleansTarget(t *testing.T) {
	tp := newTestPool(t, clock.New(), 0, 10) // alpha, beta; both health open
	mustAdmit(t, tp.pool, "alpha")            // alpha active

	parked := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	tp.docker.SetBeforeStart(func(name string) {
		if name != "beta-c" {
			return
		}
		blocked := false
		once.Do(func() { blocked = true })
		if blocked {
			close(parked) // the swap worker is now parked at Start(beta-c)
			<-release
		}
	})

	// Admit beta: with alpha active and grace 0, this starts a swap that sweeps
	// alpha-c, passes the pre-start check, then parks inside Start(beta-c).
	betaDone := make(chan AdmitResult, 1)
	go func() {
		r, _ := tp.pool.Admit(context.Background(), "beta")
		betaDone <- r
	}()

	select {
	case <-parked:
	case <-time.After(5 * time.Second):
		t.Fatal("swap worker never parked at Start(beta-c)")
	}

	// Stop the pool while the swap is parked at its target's Start.
	if out := tp.pool.AdminStop(); out != AdminInitiated {
		t.Fatalf("AdminStop = %v, want AdminInitiated", out)
	}

	if r := <-betaDone; r != AdmitShutdown {
		t.Errorf("beta admit = %v, want AdmitShutdown (pool stopped)", r)
	}

	// Wait for the cold sweep to finish (pool IDLE) BEFORE releasing the parked
	// Start. This pins the race: the sweep has already observed beta-c not
	// running and will not re-sweep it, so only the superseded worker's
	// post-start self-check can stop the container it is about to start.
	waitFor(t, "pool idle", func() bool {
		return tp.pool.Snapshot().State == StateIdle
	})

	close(release) // let Start(beta-c) complete; the worker must then self-clean

	// Confirm we actually exercised the post-start window: the worker started
	// beta-c despite the supersession.
	waitFor(t, "beta-c started", func() bool {
		return slices.Contains(tp.docker.Calls(), "start:beta-c")
	})
	// Core invariant: the superseded worker must stop the container it just
	// started — nothing may linger running while the pool reports IDLE.
	waitFor(t, "beta-c self-cleaned", func() bool {
		return slices.Contains(tp.docker.Calls(), "stop:beta-c")
	})
	waitFor(t, "no containers running", func() bool {
		return len(tp.docker.Running()) == 0
	})
}
