package orchestrator

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/benbjohnson/clock"

	"github.com/KingPin/StageHand/internal/dockerctl"
)

// ctxRecorder wraps a FakeClient and captures the context handed to each
// Events() subscription, so a test can assert the prior subscription's
// context is cancelled when the watcher resubscribes.
type ctxRecorder struct {
	*dockerctl.FakeClient
	mu   sync.Mutex
	ctxs []context.Context
}

func (c *ctxRecorder) Events(ctx context.Context) (<-chan dockerctl.Event, <-chan error) {
	c.mu.Lock()
	c.ctxs = append(c.ctxs, ctx)
	c.mu.Unlock()
	return c.FakeClient.Events(ctx)
}

func (c *ctxRecorder) subscriptions() []context.Context {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]context.Context(nil), c.ctxs...)
}

// TestWatcherCancelsPriorSubscriptionContextOnResubscribe proves the leak
// fix: each Events() subscription gets its own child context that is
// cancelled when the watcher resubscribes, so the docker client's
// per-subscription forwarder goroutine exits instead of leaking (PRD §6).
func TestWatcherCancelsPriorSubscriptionContextOnResubscribe(t *testing.T) {
	fake := dockerctl.NewFake("alpha-c")
	rec := &ctxRecorder{FakeClient: fake}

	mc := clock.NewMock()
	w := NewWatcher(rec, mc, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go w.Run(ctx)

	waitFor(t, "first subscription", func() bool {
		return len(rec.subscriptions()) >= 1
	})

	// Force a resubscribe. The watcher cancels the prior subscription's
	// context, then waits one second (mock clock) before resubscribing.
	fake.EmitStreamError(errors.New("boom"))

	// Advance the mock clock past the backoff so the resubscribe fires
	// without sleeping in real time. We can't observe exactly when the
	// watcher parks on w.clk.After, so we keep nudging the clock forward
	// until the second subscription lands — each Add is a no-op once the
	// timer has already fired.
	waitFor(t, "second subscription after resubscribe", func() bool {
		mc.Add(time.Second)
		return len(rec.subscriptions()) >= 2
	})

	subs := rec.subscriptions()

	// The prior subscription's context MUST be cancelled so its forwarder
	// goroutine can exit.
	select {
	case <-subs[0].Done():
		// good: prior subscription context cancelled on resubscribe
	default:
		t.Fatal("prior subscription context was not cancelled on resubscribe")
	}

	// The live subscription's context MUST NOT be cancelled yet.
	select {
	case <-subs[1].Done():
		t.Fatal("live subscription context was cancelled unexpectedly")
	default:
		// good: live subscription still active
	}
}
