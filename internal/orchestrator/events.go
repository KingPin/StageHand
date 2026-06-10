package orchestrator

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/benbjohnson/clock"

	"github.com/KingPin/StageHand/internal/dockerctl"
)

// opTTL bounds how long a registered self-operation explains incoming
// Docker events. Self-events arrive within milliseconds of the API call;
// the TTL only guards against leaked entries from failed operations.
const opTTL = 30 * time.Second

// opRegistry records StageHand-initiated container operations so the
// events watcher can tell self-initiated events from external ones
// (PRD §6). Workers register BEFORE issuing the API call; the resulting
// event can therefore never observe a missing entry.
//
// Expectations are COUNTED, not just time-boxed: a docker stop emits at
// most a kill/die/stop burst (3 events) and a start emits one, so each
// expectation absorbs only that many matching events. The TTL remains
// as a backstop for ops that failed without emitting anything. This
// keeps the window in which a genuinely external event on the same
// container could be misclassified as small as the burst itself.
//
// It is deliberately a tiny mutex-guarded object shared between worker
// goroutines and the watcher — the pool manager's actor state stays
// goroutine-private.
type opRegistry struct {
	mu      sync.Mutex
	clk     clock.Clock
	entries map[string]*opEntry // containerName -> latest expectation
}

type opEntry struct {
	kind      string // "start" | "stop"
	at        time.Time
	remaining int // matching events left to absorb
}

func newOpRegistry(clk clock.Clock) *opRegistry {
	return &opRegistry{clk: clk, entries: map[string]*opEntry{}}
}

// expect records an imminent self-operation on a container.
func (r *opRegistry) expect(container, kind string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	remaining := 1 // "start" emits a single event
	if kind == "stop" {
		remaining = 3 // kill + die + stop burst
	}
	r.entries[container] = &opEntry{kind: kind, at: r.clk.Now(), remaining: remaining}
}

// isExpected reports whether an incoming event is explained by a live
// self-operation, consuming one unit of the expectation's budget.
func (r *opRegistry) isExpected(container, action string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.entries[container]
	if !ok {
		return false
	}
	if r.clk.Since(e.at) > opTTL {
		delete(r.entries, container)
		return false
	}
	var matches bool
	switch e.kind {
	case "stop":
		matches = action == "die" || action == "stop" || action == "kill"
	case "start":
		matches = action == "start"
	}
	if !matches {
		return false
	}
	e.remaining--
	if e.remaining <= 0 {
		delete(r.entries, container)
	}
	return true
}

// Watcher subscribes to the Docker events stream and routes container
// lifecycle events to the pool owning that container. One watcher serves
// all pools (the fake client exposes a single event stream).
type Watcher struct {
	docker dockerctl.Client
	clk    clock.Clock
	log    *slog.Logger

	mu     sync.RWMutex
	routes map[string]*Pool // containerName -> owning pool
}

func NewWatcher(docker dockerctl.Client, clk clock.Clock, log *slog.Logger) *Watcher {
	return &Watcher{docker: docker, clk: clk, log: log, routes: map[string]*Pool{}}
}

// Register routes events for a container to its pool.
func (w *Watcher) Register(containerName string, p *Pool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.routes[containerName] = p
}

// ReplaceAll swaps the entire routing table (hot config reload).
func (w *Watcher) ReplaceAll(routes map[string]*Pool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.routes = make(map[string]*Pool, len(routes))
	for k, v := range routes {
		w.routes[k] = v
	}
}

// Run consumes the events stream until ctx is cancelled, resubscribing
// with backoff if the stream errors. Each subscription gets its own child
// context that is cancelled before resubscribing, so the docker client's
// per-subscription forwarder goroutine exits instead of leaking (PRD §6).
func (w *Watcher) Run(ctx context.Context) {
	for ctx.Err() == nil {
		subCtx, cancel := context.WithCancel(ctx)
		events, errs := w.docker.Events(subCtx)
		w.log.Info("docker events watcher subscribed")
	consume:
		for {
			select {
			case ev, ok := <-events:
				if !ok {
					break consume
				}
				w.route(ev)
			case err, ok := <-errs:
				if !ok {
					break consume
				}
				w.log.Error("docker events stream error; resubscribing", "err", err)
				break consume
			case <-ctx.Done():
				break consume
			}
		}
		cancel() // release this subscription's forwarder before resubscribing
		select {
		case <-w.clk.After(time.Second):
		case <-ctx.Done():
		}
	}
}

func (w *Watcher) route(ev dockerctl.Event) {
	switch ev.Action {
	case "start", "die", "stop", "kill":
	default:
		return // create/attach/exec/... are irrelevant
	}
	w.mu.RLock()
	p := w.routes[ev.ContainerName]
	w.mu.RUnlock()
	if p != nil {
		p.NotifyContainerEvent(ev.ContainerName, ev.Action)
	}
}

// NotifyContainerEvent feeds a container lifecycle event into the pool.
// Self-initiated events (explained by the op registry) are dropped here;
// only genuinely external changes reach the manager.
func (p *Pool) NotifyContainerEvent(container, action string) {
	if p.ops.isExpected(container, action) {
		return
	}
	select {
	case p.dockerEv <- extEvent{container: container, action: action}:
	case <-p.done:
	}
}

// handleExternalEvent reconciles out-of-band container changes (manager
// goroutine). PRD §6: an active container dying externally flushes its
// queue with 502 and marks the pool ERROR; an unauthorized container
// starting is force-stopped to preserve mutual exclusion.
func (p *Pool) handleExternalEvent(ev extEvent) {
	svc := p.memberByContainer(ev.container)
	if svc == nil {
		return
	}

	switch {
	case ev.action == "die" || ev.action == "stop" || ev.action == "kill":
		switch p.state {
		case StateActive:
			if p.active != svc.name {
				return // a sibling we believed stopped; nothing changes
			}
			p.log.Warn("active container stopped outside StageHand", "container", ev.container)
			p.state = StateError
			p.active = ""
			p.bumpEpochs() // disarm cooldown/grace
			p.publish()
			svc.flush(admitReply{AdmitDockerError,
				fmt.Errorf("container %q was stopped outside StageHand", ev.container)})
			p.chainNow() // queued waiters for other services recover now
		case StateSwapping:
			if svc.name != p.target {
				return // sibling noise mid-swap; the sweep handles it
			}
			p.log.Warn("swap target died externally; aborting swap", "container", ev.container)
			p.supersedeWorker() // cancel + orphan the in-flight worker so it can't restart the dead target
			p.state = StateError
			p.active = ""
			p.target = ""
			p.publish()
			svc.flush(admitReply{AdmitDockerError,
				fmt.Errorf("container %q died during startup (external)", ev.container)})
			p.chainNow()
		}
	case ev.action == "start":
		// Our own starts are filtered by the registry; tolerate the swap
		// target (someone "helped" mid-swap) and the active container.
		if p.state == StateSwapping && svc.name == p.target {
			return
		}
		if p.state == StateActive && svc.name == p.active {
			return
		}
		p.log.Warn("unauthorized container start; stopping to preserve pool mutual exclusion",
			"container", ev.container)
		go p.forceStop(ev.container)
	}
}

// forceStop stops an intruder container (registered as a self-op so its
// teardown events don't loop back as external). It is a no-op once the
// pool shuts down — a reload may have handed the container to a new
// pool that legitimately runs it.
func (p *Pool) forceStop(container string) {
	select {
	case <-p.done:
		return
	default:
	}
	p.ops.expect(container, "stop")
	ctx, cancel := p.opCtx(dockerCallDeadline)
	defer cancel()
	if err := p.docker.Stop(ctx, container, gracefulStopTimeout); err != nil {
		p.log.Error("force-stopping unauthorized container", "container", container, "err", err)
	}
}

func (p *Pool) memberByContainer(container string) *member {
	for _, m := range p.members {
		if m.containerName == container {
			return m
		}
	}
	return nil
}
