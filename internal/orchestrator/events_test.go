package orchestrator

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"slices"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
)

// startWatcher runs a Watcher for the test pool's two containers.
func startWatcher(t *testing.T, tp *testPool) {
	t.Helper()
	w := NewWatcher(tp.docker, slog.New(slog.NewTextHandler(io.Discard, nil)))
	w.Register("alpha-c", tp.pool)
	w.Register("beta-c", tp.pool)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go w.Run(ctx)
}

func TestExternalDeathOfActiveMovesPoolToError(t *testing.T) {
	tp := newTestPool(t, clock.New(), 0, 10)
	startWatcher(t, tp)
	mustAdmit(t, tp.pool, "alpha")

	tp.docker.EmitExternal("alpha-c", "die") // human runs docker stop

	waitFor(t, "pool ERROR", func() bool {
		return tp.pool.Snapshot().State == StateError
	})

	// Next request recovers the pool (ERROR behaves like a cold start).
	mustAdmit(t, tp.pool, "alpha")
	if s := tp.pool.Snapshot(); s.State != StateActive || s.ActiveService != "alpha" {
		t.Errorf("snapshot after recovery = %+v, want ACTIVE alpha", s)
	}
}

// TestSelfEventsAreNotMisclassified is the core disambiguation test:
// a full swap (stop alpha -> start beta) emits die/start events for our
// own operations, and none of them may bounce the state machine.
func TestSelfEventsAreNotMisclassified(t *testing.T) {
	tp := newTestPool(t, clock.New(), 0, 10)
	startWatcher(t, tp)

	mustAdmit(t, tp.pool, "alpha") // self start event
	mustAdmit(t, tp.pool, "beta")  // self stop(alpha) + start(beta) events

	// Give the watcher time to deliver everything it's going to.
	time.Sleep(50 * time.Millisecond)
	if s := tp.pool.Snapshot(); s.State != StateActive || s.ActiveService != "beta" {
		t.Errorf("snapshot = %+v, want ACTIVE beta (self events must be ignored)", s)
	}
	// And no retaliatory stops were issued against beta.
	for _, c := range tp.docker.Calls() {
		if c == "stop:beta-c" {
			t.Error("beta-c was stopped — a self event was treated as external")
		}
	}
}

func TestUnauthorizedStartIsForceStopped(t *testing.T) {
	tp := newTestPool(t, clock.New(), 0, 10)
	startWatcher(t, tp)
	mustAdmit(t, tp.pool, "alpha")

	tp.docker.EmitExternal("beta-c", "start") // intruder violates the pool

	waitFor(t, "intruder stopped", func() bool {
		return slices.Contains(tp.docker.Calls(), "stop:beta-c")
	})
	waitFor(t, "mutual exclusion restored", func() bool {
		return slices.Equal(tp.docker.Running(), []string{"alpha-c"})
	})
	if s := tp.pool.Snapshot(); s.State != StateActive || s.ActiveService != "alpha" {
		t.Errorf("snapshot = %+v, want alpha undisturbed", s)
	}
}

func TestSwapTargetDyingExternallyAbortsSwap(t *testing.T) {
	tp := newTestPool(t, clock.New(), 0, 10, "beta") // beta health held
	startWatcher(t, tp)

	done := make(chan struct{})
	var res AdmitResult
	var admitErr error
	go func() {
		res, admitErr = tp.pool.Admit(context.Background(), "beta")
		close(done)
	}()
	waitFor(t, "beta starting", func() bool {
		return slices.Contains(tp.docker.Calls(), "start:beta-c")
	})

	tp.docker.EmitExternal("beta-c", "die") // dies mid-startup

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("queued admit not flushed after external target death")
	}
	if res != AdmitDockerError || admitErr == nil {
		t.Errorf("Admit = %v, %v; want AdmitDockerError with external-death detail", res, admitErr)
	}
	if s := tp.pool.Snapshot(); s.State != StateError {
		t.Errorf("state = %v, want ERROR", s.State)
	}
}

func TestWatcherResubscribesAfterStreamError(t *testing.T) {
	tp := newTestPool(t, clock.New(), 0, 10)
	startWatcher(t, tp)
	mustAdmit(t, tp.pool, "alpha")

	tp.docker.EmitStreamError(errors.New("stream hiccup"))
	time.Sleep(1100 * time.Millisecond) // watcher backoff is 1s

	tp.docker.EmitExternal("alpha-c", "die")
	waitFor(t, "event routed after resubscribe", func() bool {
		return tp.pool.Snapshot().State == StateError
	})
}

// TestAdminSwapSupersedeReported: replacing a pending admin target is
// last-wins, but the caller must be told the previous one was dropped.
func TestAdminSwapSupersedeReported(t *testing.T) {
	tp := newTestPoolOpts(t, clock.New(), tpOpts{
		services: []string{"alpha", "beta", "gamma"},
		held:     []string{"beta"}, // beta health held: swap hangs
	})
	go tp.pool.Admit(context.Background(), "beta")
	waitFor(t, "swap in flight", func() bool {
		return tp.pool.Snapshot().State == StateSwapping
	})

	if out := tp.pool.AdminSwap("alpha"); out != AdminPending {
		t.Fatalf("first admin swap = %v, want pending", out)
	}
	if out := tp.pool.AdminSwap("alpha"); out != AdminPending {
		t.Fatalf("same-target repeat = %v, want pending (not superseded)", out)
	}
	if out := tp.pool.AdminSwap("gamma"); out != AdminSuperseded {
		t.Fatalf("replacing pending alpha with gamma = %v, want %v", out, AdminSuperseded)
	}
	if out := tp.pool.AdminSwap("beta"); out != AdminPending {
		t.Fatalf("swap to in-flight target = %v, want pending", out)
	}

	// Release the swap: gamma (the last admin target) must win the chain.
	tp.gates["beta"].Open()
	waitFor(t, "gamma active after chain", func() bool {
		s := tp.pool.Snapshot()
		return s.State == StateActive && s.ActiveService == "gamma"
	})
}

func TestOpRegistryCountedMatching(t *testing.T) {
	mock := clock.NewMock()
	r := newOpRegistry(mock)

	// A stop expectation absorbs exactly the kill/die/stop burst.
	r.expect("c1", "stop")
	for _, action := range []string{"kill", "die", "stop"} {
		if !r.isExpected("c1", action) {
			t.Errorf("stop expectation should explain %q", action)
		}
	}
	if r.isExpected("c1", "die") {
		t.Error("4th teardown event must be external — budget exhausted")
	}

	// Non-matching actions don't consume the budget.
	r.expect("c2", "stop")
	if r.isExpected("c2", "start") {
		t.Error("stop expectation must not explain a start")
	}
	if !r.isExpected("c2", "die") {
		t.Error("die should still match after the non-matching start probe")
	}
	if r.isExpected("c9", "die") {
		t.Error("unrelated container must not match")
	}

	// A start expectation absorbs exactly one start.
	r.expect("c3", "start")
	if !r.isExpected("c3", "start") {
		t.Error("start expectation should explain start")
	}
	if r.isExpected("c3", "start") {
		t.Error("second start must be external — budget exhausted")
	}

	// TTL backstop still applies to unconsumed entries.
	r.expect("c4", "stop")
	mock.Add(opTTL + time.Second)
	if r.isExpected("c4", "die") {
		t.Error("expired expectation must not match")
	}
}
