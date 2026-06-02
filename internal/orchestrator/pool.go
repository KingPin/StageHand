package orchestrator

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/benbjohnson/clock"

	"github.com/KingPin/StageHand/internal/dockerctl"
)

// MemberConfig describes one service participating in a pool.
type MemberConfig struct {
	Name           string
	ContainerName  string
	HealthURL      string // absolute URL polled until 200 after start
	StartupTimeout time.Duration
	MaxQueue       int
}

// PoolConfig describes a VRAM pool and its members.
type PoolConfig struct {
	Name           string
	GracePeriod    time.Duration
	Cooldown       time.Duration // 0 disables (wired in the cooldown commit)
	DefaultService string        // "" = cold pool
	Members        []MemberConfig
}

// Pool is one VRAM mutual-exclusion group, run by a single manager
// goroutine. Public methods are safe for concurrent use; they only read
// the atomic snapshot or exchange messages with the manager.
type Pool struct {
	name   string
	grace  time.Duration
	docker dockerctl.Client
	clk    clock.Clock
	log    *slog.Logger

	snap     atomic.Pointer[PoolSnapshot]
	cmds     chan any       // admitCmd | cancelCmd
	swaps    chan swapMsg   // terminal reports from swap workers
	timers   chan timerTick // epoch-guarded grace/cooldown ticks
	activity chan string    // fast-path pings (cooldown reset, later commit)
	done     chan struct{}
	closing  sync.Once

	// --- manager-goroutine-private state: NEVER touched elsewhere ---
	state     PoolState
	active    string
	startedAt time.Time
	members   map[string]*member
	epoch     uint64 // bumps invalidate in-flight timer ticks
	opSeq     uint64 // bumps identify the current swap worker
	seq       uint64 // waiter ordering across services
}

// NewPool builds the pool and starts its manager goroutine.
func NewPool(cfg PoolConfig, docker dockerctl.Client, clk clock.Clock, log *slog.Logger) *Pool {
	p := &Pool{
		name:     cfg.Name,
		grace:    cfg.GracePeriod,
		docker:   docker,
		clk:      clk,
		log:      log.With("pool", cfg.Name),
		cmds:     make(chan any),
		swaps:    make(chan swapMsg, 4),
		timers:   make(chan timerTick, 8),
		activity: make(chan string, 64),
		done:     make(chan struct{}),
		state:    StateIdle,
		members:  make(map[string]*member, len(cfg.Members)),
	}
	for _, mc := range cfg.Members {
		p.members[mc.Name] = &member{
			name:           mc.Name,
			containerName:  mc.ContainerName,
			healthURL:      mc.HealthURL,
			startupTimeout: mc.StartupTimeout,
			maxQueue:       mc.MaxQueue,
		}
	}
	p.publish()
	go p.run()
	return p
}

// Name returns the pool's configured name.
func (p *Pool) Name() string { return p.name }

// HasMember reports whether the named service belongs to this pool.
func (p *Pool) HasMember(service string) bool {
	_, ok := p.members[service]
	return ok
}

// Snapshot returns the latest published pool state (lock-free).
func (p *Pool) Snapshot() PoolSnapshot { return *p.snap.Load() }

// Close stops the manager; queued waiters receive AdmitShutdown.
func (p *Pool) Close() { p.closing.Do(func() { close(p.done) }) }

// Admit blocks until the target service is active and healthy (AdmitGo),
// or admission fails (queue full, docker error, startup timeout, shutdown,
// client cancellation). The error carries detail for 502/504 payloads.
func (p *Pool) Admit(ctx context.Context, service string) (AdmitResult, error) {
	// Fast path: lock-free snapshot hint. Authority stays with the
	// manager — a stale hit here only means proxying to a container
	// in early teardown, which the grace period makes vanishingly rare.
	if s := p.snap.Load(); s.State == StateActive && s.ActiveService == service {
		select {
		case p.activity <- service:
		default: // a dropped ping only delays one cooldown reset
		}
		return AdmitGo, nil
	}

	reply := make(chan admitReply, 1)
	select {
	case p.cmds <- admitCmd{service: service, reply: reply}:
	case <-ctx.Done():
		return AdmitCanceled, ctx.Err()
	case <-p.done:
		return AdmitShutdown, nil
	}

	select {
	case r := <-reply:
		return r.result, r.err
	case <-ctx.Done():
		// Tell the manager to forget us; it may already have replied
		// into the buffered channel, which is then simply dropped.
		select {
		case p.cmds <- cancelCmd{service: service, reply: reply}:
		case <-p.done:
		}
		return AdmitCanceled, ctx.Err()
	case <-p.done:
		return AdmitShutdown, nil
	}
}

// run is the manager goroutine: the entire state machine, serially.
func (p *Pool) run() {
	for {
		select {
		case c := <-p.cmds:
			switch cmd := c.(type) {
			case admitCmd:
				p.handleAdmit(cmd)
			case cancelCmd:
				if m, ok := p.members[cmd.service]; ok {
					m.removeByReply(cmd.reply)
				}
			}
		case msg := <-p.swaps:
			p.handleSwap(msg)
		case tick := <-p.timers:
			p.handleTick(tick)
		case <-p.activity:
			// Cooldown reset lands in the cooldown commit.
		case <-p.done:
			p.shutdown()
			return
		}
	}
}

func (p *Pool) handleAdmit(c admitCmd) {
	m, ok := p.members[c.service]
	if !ok {
		c.reply <- admitReply{AdmitDockerError,
			fmt.Errorf("service %q is not a member of pool %q", c.service, p.name)}
		return
	}

	// Re-check under serial ownership: the caller's snapshot was a hint.
	if p.state == StateActive && p.active == c.service {
		c.reply <- admitReply{result: AdmitGo}
		return
	}

	p.seq++
	if !m.enqueue(&waiter{seq: p.seq, reply: c.reply}) {
		c.reply <- admitReply{result: AdmitQueueFull}
		return
	}

	switch p.state {
	case StateIdle, StateError:
		// Fast startup path (PRD §3.2): nothing to stop.
		p.startSwap(c.service)
	case StateActive:
		// Different service wants the GPU: respect the grace period.
		if elapsed := p.clk.Since(p.startedAt); elapsed >= p.grace {
			p.startSwap(c.service)
		} else {
			p.armTimer(tickGrace, p.grace-elapsed)
		}
	case StateSwapping:
		// Held until the in-flight swap terminates; then chained.
	}
}

// startSwap transitions to SWAPPING and spawns the worker. The worker is
// guaranteed to deliver exactly one terminal swapMsg, so SWAPPING cannot
// get stuck.
func (p *Pool) startSwap(target string) {
	m := p.members[target]
	stopContainer := ""
	if p.state == StateActive && p.active != "" {
		stopContainer = p.members[p.active].containerName
	}
	p.state = StateSwapping
	p.active = ""
	p.epoch++ // invalidate any pending grace/cooldown ticks
	p.opSeq++
	p.publish()
	p.log.Info("swap started", "target", target, "stopping", stopContainer)
	go p.doSwap(p.opSeq, stopContainer, m)
}

func (p *Pool) handleSwap(msg swapMsg) {
	if msg.op != p.opSeq {
		return // stale worker from a superseded swap
	}
	m := p.members[msg.target]
	switch msg.kind {
	case swapComplete:
		p.state = StateActive
		p.active = msg.target
		p.startedAt = p.clk.Now()
		p.publish()
		p.log.Info("swap complete", "active", msg.target)
		m.flush(admitReply{result: AdmitGo})
		// Other services may have queued during the swap: chain, but
		// honor the fresh grace period so the new container isn't
		// torn down under the requests just flushed into it.
		if next := p.oldestQueued(); next != "" {
			if p.grace > 0 {
				p.armTimer(tickGrace, p.grace)
			} else {
				p.startSwap(next)
			}
		}
	case swapFailed:
		p.state = StateIdle
		p.active = ""
		p.publish()
		p.log.Error("swap failed", "target", msg.target, "err", msg.err)
		m.flush(admitReply{AdmitDockerError, msg.err})
		p.chainNow()
	case swapHealthTimeout:
		// Best-effort teardown of the unhealthy container, off-loop.
		go func(cn string) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if err := p.docker.Stop(ctx, cn, gracefulStopTimeout); err != nil {
				p.log.Error("stopping unhealthy container", "container", cn, "err", err)
			}
		}(m.containerName)
		p.state = StateIdle
		p.active = ""
		p.publish()
		p.log.Error("swap health timeout", "target", msg.target, "budget", m.startupTimeout)
		m.flush(admitReply{AdmitStartupTimeout,
			fmt.Errorf("service %q failed its health check within %s", msg.target, m.startupTimeout)})
		p.chainNow()
	}
}

func (p *Pool) handleTick(tick timerTick) {
	if tick.epoch != p.epoch {
		return // superseded: a transition happened after arming
	}
	if p.state != StateActive {
		return // never act on timers mid-swap
	}
	switch tick.kind {
	case tickGrace:
		if next := p.oldestQueued(); next != "" {
			p.startSwap(next)
		}
	case tickCooldown:
		// Lands in the cooldown commit.
	}
}

// chainNow starts a swap for the longest-waiting service, used after a
// failure when nothing is running (no grace needed).
func (p *Pool) chainNow() {
	if next := p.oldestQueued(); next != "" {
		p.startSwap(next)
	}
}

// oldestQueued returns the member name with the lowest-seq head waiter.
func (p *Pool) oldestQueued() string {
	best := ""
	var bestSeq uint64
	for name, m := range p.members {
		if len(m.queue) == 0 {
			continue
		}
		if best == "" || m.queue[0].seq < bestSeq {
			best, bestSeq = name, m.queue[0].seq
		}
	}
	return best
}

// armTimer schedules an epoch-guarded tick. The callback only enqueues a
// message — all decisions stay in the manager loop.
func (p *Pool) armTimer(kind timerKind, d time.Duration) {
	p.epoch++
	e := p.epoch
	p.clk.AfterFunc(d, func() {
		select {
		case p.timers <- timerTick{kind: kind, epoch: e}:
		case <-p.done:
		}
	})
}

func (p *Pool) publish() {
	p.snap.Store(&PoolSnapshot{State: p.state, ActiveService: p.active})
}

func (p *Pool) shutdown() {
	for _, m := range p.members {
		m.flush(admitReply{result: AdmitShutdown})
	}
	p.log.Info("pool manager stopped")
}
