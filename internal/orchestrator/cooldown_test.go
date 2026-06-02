package orchestrator

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
)

func TestColdPoolStopsAfterCooldown(t *testing.T) {
	mock := clock.NewMock()
	tp := newTestPoolOpts(t, mock, tpOpts{cooldown: 5 * time.Minute})
	mustAdmit(t, tp.pool, "alpha")

	advanceUntil(t, mock, 30*time.Second, "cold-pool stop", func() bool {
		return slices.Contains(tp.docker.Calls(), "stop:alpha-c")
	})
	waitFor(t, "pool idle", func() bool {
		return tp.pool.Snapshot().State == StateIdle
	})
	if running := tp.docker.Running(); len(running) != 0 {
		t.Errorf("running = %v after cooldown, want none (0MB VRAM)", running)
	}

	// The pool must wake straight back up on the next request.
	mustAdmit(t, tp.pool, "beta")
	if s := tp.pool.Snapshot(); s.State != StateActive || s.ActiveService != "beta" {
		t.Errorf("snapshot after wake = %+v, want ACTIVE beta", s)
	}
}

func TestActivityResetsCooldown(t *testing.T) {
	mock := clock.NewMock()
	tp := newTestPoolOpts(t, mock, tpOpts{cooldown: 5 * time.Minute})
	mustAdmit(t, tp.pool, "alpha")

	// 4 minutes in (under the 5m cooldown), traffic arrives: the
	// fast-path ping must re-arm the countdown.
	mock.Add(4 * time.Minute)
	mustAdmit(t, tp.pool, "alpha")
	time.Sleep(20 * time.Millisecond) // let the manager process the ping

	// 4 more minutes (8 total, but only 4 since the reset): still warm.
	mock.Add(4 * time.Minute)
	time.Sleep(20 * time.Millisecond)
	if slices.Contains(tp.docker.Calls(), "stop:alpha-c") {
		t.Fatal("pool went cold despite activity resetting the cooldown")
	}
	if s := tp.pool.Snapshot(); s.State != StateActive {
		t.Fatalf("state = %v, want ACTIVE", s.State)
	}

	// Quiet from here: the countdown must complete.
	advanceUntil(t, mock, 30*time.Second, "cooldown after quiet period", func() bool {
		return slices.Contains(tp.docker.Calls(), "stop:alpha-c")
	})
}

func TestCooldownSwapsToDefaultService(t *testing.T) {
	mock := clock.NewMock()
	tp := newTestPoolOpts(t, mock, tpOpts{cooldown: 5 * time.Minute, defaultSvc: "alpha"})
	mustAdmit(t, tp.pool, "beta")

	advanceUntil(t, mock, 30*time.Second, "swap to default", func() bool {
		s := tp.pool.Snapshot()
		return s.State == StateActive && s.ActiveService == "alpha"
	})
	want := []string{"start:beta-c", "stop:beta-c", "start:alpha-c"}
	if calls := tp.docker.Calls(); !slices.Equal(calls, want) {
		t.Errorf("calls = %v, want %v", calls, want)
	}

	// With the default active, no further cooldown actions ever fire.
	before := len(tp.docker.Calls())
	mock.Add(time.Hour)
	time.Sleep(20 * time.Millisecond)
	if after := len(tp.docker.Calls()); after != before {
		t.Errorf("docker calls grew %d -> %d with default active; cooldown must stay disarmed", before, after)
	}
}

// TestCooldownSurvivesCanceledQueuedSwap: a cooldown tick that yields to
// queued waiters must re-arm; if those waiters later cancel, the pool
// must still go cold instead of staying warm forever.
func TestCooldownSurvivesCanceledQueuedSwap(t *testing.T) {
	mock := clock.NewMock()
	tp := newTestPoolOpts(t, mock, tpOpts{
		grace:    60 * time.Second,
		cooldown: 30 * time.Second,
	})
	mustAdmit(t, tp.pool, "alpha")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan AdmitResult, 1)
	go func() {
		r, _ := tp.pool.Admit(ctx, "beta")
		done <- r
	}()
	waitFor(t, "beta queued", func() bool {
		return tp.pool.QueuedCounts()["beta"] == 1
	})

	// Cooldown fires mid-grace-wait and must defer (queue non-empty).
	mock.Add(35 * time.Second)
	time.Sleep(20 * time.Millisecond)

	cancel() // beta gives up before the grace swap happens
	if r := <-done; r != AdmitCanceled {
		t.Fatalf("beta admit = %v, want AdmitCanceled", r)
	}

	// With no traffic and no queue, the pool must still cool down.
	advanceUntil(t, mock, 10*time.Second, "cold pool after cancel", func() bool {
		return tp.pool.Snapshot().State == StateIdle
	})
	if running := tp.docker.Running(); len(running) != 0 {
		t.Errorf("running = %v, want none", running)
	}
}

// TestDefaultServiceRetriesAfterFailedSwap: a default_service pool that
// fails its cooldown swap must retry on the cooldown cadence rather
// than staying cold until traffic arrives.
func TestDefaultServiceRetriesAfterFailedSwap(t *testing.T) {
	mock := clock.NewMock()
	tp := newTestPoolOpts(t, mock, tpOpts{cooldown: 5 * time.Minute, defaultSvc: "alpha"})
	mustAdmit(t, tp.pool, "beta")

	// First cooldown expiry: the swap to default alpha fails to start.
	boom := errors.New("registry unavailable")
	tp.docker.SetStartErr("alpha-c", boom)
	advanceUntil(t, mock, 30*time.Second, "failed default swap", func() bool {
		return slices.Contains(tp.docker.Calls(), "start:alpha-c") &&
			tp.pool.Snapshot().State == StateIdle
	})

	// Docker recovers; the retry tick must bring the default back warm.
	tp.docker.SetStartErr("alpha-c", nil)
	advanceUntil(t, mock, 30*time.Second, "default retried and active", func() bool {
		s := tp.pool.Snapshot()
		return s.State == StateActive && s.ActiveService == "alpha"
	})
	if running := tp.docker.Running(); !slices.Equal(running, []string{"alpha-c"}) {
		t.Errorf("running = %v, want only alpha-c", running)
	}
}

func TestCooldownZeroDisablesIdleHandling(t *testing.T) {
	mock := clock.NewMock()
	tp := newTestPoolOpts(t, mock, tpOpts{cooldown: 0})
	mustAdmit(t, tp.pool, "alpha")

	mock.Add(24 * time.Hour)
	time.Sleep(20 * time.Millisecond)
	if s := tp.pool.Snapshot(); s.State != StateActive || s.ActiveService != "alpha" {
		t.Errorf("snapshot = %+v, want ACTIVE alpha forever (cooldown disabled)", s)
	}
}

// TestCooldownDoesNotPreemptGracePendingSwap is the regression test for
// the epoch-interplay bug: a cooldown reset must not invalidate a pending
// grace tick, and a cooldown firing while waiters are queued must yield
// to the grace-driven swap.
func TestCooldownDoesNotPreemptGracePendingSwap(t *testing.T) {
	mock := clock.NewMock()
	tp := newTestPoolOpts(t, mock, tpOpts{
		grace:    60 * time.Second,
		cooldown: 30 * time.Second, // fires BEFORE grace expires
	})
	mustAdmit(t, tp.pool, "alpha")

	done := make(chan AdmitResult, 1)
	go func() {
		r, _ := tp.pool.Admit(context.Background(), "beta")
		done <- r
	}()
	waitFor(t, "beta queued (still ACTIVE)", func() bool {
		return tp.pool.QueuedCounts()["beta"] == 1
	})

	// Cooldown (30s) elapses while beta waits out alpha's grace (60s):
	// the pool must NOT go cold under the queued request.
	mock.Add(35 * time.Second)
	time.Sleep(20 * time.Millisecond)
	select {
	case r := <-done:
		t.Fatalf("beta admitted early (%v) — grace not honored", r)
	default:
	}
	if s := tp.pool.Snapshot(); s.State != StateActive || s.ActiveService != "alpha" {
		t.Fatalf("snapshot = %+v after cooldown tick, want alpha still ACTIVE", s)
	}

	// Grace expiry must still fire and serve beta.
	advanceUntil(t, mock, 10*time.Second, "grace-driven swap", func() bool {
		select {
		case r := <-done:
			if r != AdmitGo {
				t.Fatalf("beta admit = %v, want AdmitGo", r)
			}
			return true
		default:
			return false
		}
	})
	if running := tp.docker.Running(); !slices.Equal(running, []string{"beta-c"}) {
		t.Errorf("running = %v, want only beta-c", running)
	}
}
