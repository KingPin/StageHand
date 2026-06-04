package orchestrator

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"

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

	w := NewWatcher(rec, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go w.Run(ctx)

	waitFor(t, "first subscription", func() bool {
		return len(rec.subscriptions()) >= 1
	})

	// Force a resubscribe.
	fake.EmitStreamError(errors.New("boom"))

	waitFor(t, "second subscription after resubscribe", func() bool {
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
