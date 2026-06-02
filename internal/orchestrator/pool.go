package orchestrator

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
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
	Cooldown       time.Duration // 0 disables idle handling
	DefaultService string        // "" = cold pool (stop everything on cooldown)
	Members        []MemberConfig
}

// Pool is one VRAM mutual-exclusion group, run by a single manager
// goroutine. Public methods are safe for concurrent use; they only read
// the atomic snapshot or exchange messages with the manager.
type Pool struct {
	name       string
	grace      time.Duration
	cooldown   time.Duration
	defaultSvc string
	docker     dockerctl.Client
	clk        clock.Clock
	log        *slog.Logger

	snap     atomic.Pointer[PoolSnapshot]
	cmds     chan any       // admitCmd | cancelCmd | depthCmd
	swaps    chan swapMsg   // terminal reports from swap workers
	timers   chan timerTick // epoch-guarded grace/cooldown ticks
	activity chan string    // fast-path pings (cooldown reset)
	dockerEv chan extEvent  // external container changes (watcher)
	done     chan struct{}
	closing  sync.Once
	ops      *opRegistry // self-initiated op expectations (shared w/ workers)

	// --- manager-goroutine-private state: NEVER touched elsewhere ---
	state        PoolState
	active       string
	target       string // swap target while SWAPPING
	adminPending string // operator-requested swap chained behind one in flight
	startedAt    time.Time
	// lastActivity feeds the cooldown WITHOUT re-arming a timer per
	// request: traffic only writes this field, and the (single) armed
	// cooldown tick re-schedules itself for the remaining idle time.
	// Arming a fresh AfterFunc per request would leak rps×cooldown live
	// timers into the runtime heap.
	lastActivity  time.Time
	cooldownArmed bool
	members       map[string]*member
	epochs        [2]uint64 // per timerKind; bumps invalidate in-flight ticks
	opSeq         uint64    // bumps identify the current swap worker
	seq           uint64    // waiter ordering across services
}

// NewPool builds the pool and starts its manager goroutine.
func NewPool(cfg PoolConfig, docker dockerctl.Client, clk clock.Clock, log *slog.Logger) *Pool {
	p := &Pool{
		name:       cfg.Name,
		grace:      cfg.GracePeriod,
		cooldown:   cfg.Cooldown,
		defaultSvc: cfg.DefaultService,
		docker:     docker,
		clk:        clk,
		log:        log.With("pool", cfg.Name),
		cmds:       make(chan any),
		swaps:      make(chan swapMsg, 4),
		timers:     make(chan timerTick, 8),
		activity:   make(chan string, 64),
		dockerEv:   make(chan extEvent, 16),
		done:       make(chan struct{}),
		ops:        newOpRegistry(clk),
		state:      StateIdle,
		members:    make(map[string]*member, len(cfg.Members)),
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

// QueuedCounts returns per-service queue depths, answered by the manager
// so the numbers are exact, not racy reads.
func (p *Pool) QueuedCounts() map[string]int {
	reply := make(chan map[string]int, 1)
	select {
	case p.cmds <- depthCmd{reply: reply}:
	case <-p.done:
		return nil
	}
	select {
	case depths := <-reply:
		return depths
	case <-p.done:
		return nil
	}
}

// Status reports the pool's state for the status API (PRD §5.1).
func (p *Pool) Status() PoolStatus {
	reply := make(chan PoolStatus, 1)
	select {
	case p.cmds <- statusCmd{reply: reply}:
	case <-p.done:
		return PoolStatus{State: p.Snapshot().State, SecondsUntilCooldown: -1}
	}
	select {
	case st := <-reply:
		return st
	case <-p.done:
		return PoolStatus{State: p.Snapshot().State, SecondsUntilCooldown: -1}
	}
}

// AdminSwap forces a swap to the named service (pre-warm), bypassing the
// grace period; if a swap is in flight, it chains behind it (PRD §5.2).
func (p *Pool) AdminSwap(service string) AdminOutcome {
	reply := make(chan AdminOutcome, 1)
	select {
	case p.cmds <- adminSwapCmd{service: service, reply: reply}:
	case <-p.done:
		return AdminUnknown
	}
	select {
	case out := <-reply:
		return out
	case <-p.done:
		return AdminUnknown
	}
}

// AdminStop forces the pool cold, flushing queues with 503 (PRD §5.2).
func (p *Pool) AdminStop() AdminOutcome {
	reply := make(chan AdminOutcome, 1)
	select {
	case p.cmds <- adminStopCmd{reply: reply}:
	case <-p.done:
		return AdminAlreadyIdle
	}
	select {
	case out := <-reply:
		return out
	case <-p.done:
		return AdminAlreadyIdle
	}
}

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
			case depthCmd:
				depths := make(map[string]int, len(p.members))
				for name, m := range p.members {
					depths[name] = len(m.queue)
				}
				cmd.reply <- depths
			case statusCmd:
				cmd.reply <- p.buildStatus()
			case adminSwapCmd:
				cmd.reply <- p.handleAdminSwap(cmd.service)
			case adminStopCmd:
				cmd.reply <- p.handleAdminStop()
			}
		case msg := <-p.swaps:
			p.handleSwap(msg)
		case tick := <-p.timers:
			p.handleTick(tick)
		case svc := <-p.activity:
			p.touch(svc)
		case ev := <-p.dockerEv:
			p.handleExternalEvent(ev)
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
		p.touch(c.service)
		return
	}

	p.seq++
	if !m.enqueue(&waiter{seq: p.seq, reply: c.reply}) {
		c.reply <- admitReply{result: AdmitQueueFull}
		return
	}

	switch p.state {
	case StateIdle, StateError:
		// Fast startup path (PRD §3.2): nothing known to be running.
		// The swap worker still sweeps other members defensively.
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

// touch records activity on the active service. The cooldown tick reads
// lastActivity when it fires and defers itself — no timer work here.
func (p *Pool) touch(service string) {
	if p.state == StateActive && p.active == service {
		p.lastActivity = p.clk.Now()
	}
}

// startSwap transitions to SWAPPING and spawns the worker. The worker is
// guaranteed to deliver exactly one terminal swapMsg, so SWAPPING cannot
// get stuck.
func (p *Pool) startSwap(target string) {
	m := p.members[target]
	p.state = StateSwapping
	p.active = ""
	p.target = target
	p.bumpEpochs() // invalidate any pending grace/cooldown ticks
	p.opSeq++
	p.publish()
	others := p.otherContainers(target)
	p.log.Info("swap started", "target", target)
	go p.doSwap(p.opSeq, m, others)
}

// startStopAll transitions to SWAPPING for a cold-pool stop (cooldown
// expiry with no default service).
func (p *Pool) startStopAll() {
	p.state = StateSwapping
	p.active = ""
	p.target = ""
	p.bumpEpochs()
	p.opSeq++
	p.publish()
	p.log.Info("cooldown expired, stopping pool (cold)")
	go p.doStopAll(p.opSeq)
}

func (p *Pool) handleSwap(msg swapMsg) {
	if msg.op != p.opSeq {
		return // stale worker from a superseded swap
	}
	switch msg.kind {
	case swapComplete:
		m := p.members[msg.target]
		p.state = StateActive
		p.active = msg.target
		p.target = ""
		p.startedAt = p.clk.Now()
		p.publish()
		p.log.Info("swap complete", "active", msg.target)
		m.flush(admitReply{result: AdmitGo})
		p.chainAfterTerminal(true)
	case swapFailed:
		m := p.members[msg.target]
		p.state = StateIdle
		p.active = ""
		p.target = ""
		p.publish()
		p.log.Error("swap failed", "target", msg.target, "err", msg.err)
		m.flush(admitReply{AdmitDockerError, msg.err})
		p.chainAfterTerminal(false)
	case swapHealthTimeout:
		m := p.members[msg.target]
		// Best-effort teardown of the unhealthy container, off-loop.
		go func(cn string) {
			p.ops.expect(cn, "stop")
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if err := p.docker.Stop(ctx, cn, gracefulStopTimeout); err != nil {
				p.log.Error("stopping unhealthy container", "container", cn, "err", err)
			}
		}(m.containerName)
		p.state = StateIdle
		p.active = ""
		p.target = ""
		p.publish()
		p.log.Error("swap health timeout", "target", msg.target, "budget", m.startupTimeout)
		m.flush(admitReply{AdmitStartupTimeout,
			fmt.Errorf("service %q failed its health check within %s", msg.target, m.startupTimeout)})
		p.chainAfterTerminal(false)
	case poolStopped:
		if msg.err != nil {
			// Containers may straggle; the next swap's defensive sweep
			// re-stops anything still running.
			p.log.Error("cold-pool stop incomplete", "err", msg.err)
		} else {
			p.log.Info("pool is cold (0MB VRAM)")
		}
		p.state = StateIdle
		p.active = ""
		p.target = ""
		p.publish()
		p.chainAfterTerminal(false)
	}
}

// chainAfterTerminal decides what happens after a swap/stop terminates:
// an operator-requested swap wins, then the longest-waiting queued
// service, then (if newly activated and quiet) the idle countdown.
func (p *Pool) chainAfterTerminal(activated bool) {
	if p.adminPending != "" && p.adminPending != p.active {
		next := p.adminPending
		p.adminPending = ""
		p.startSwap(next)
		return
	}
	p.adminPending = ""

	if next := p.oldestQueued(); next != "" {
		if activated && p.grace > 0 {
			// Honor the fresh grace period so the new container isn't
			// torn down under the requests just flushed into it.
			p.armTimer(tickGrace, p.grace)
		} else {
			p.startSwap(next)
		}
		return
	}

	// Quiet pool: begin the idle countdown — unless the default service
	// is the one running (it stays warm indefinitely).
	if activated && p.cooldown > 0 && !(p.defaultSvc != "" && p.active == p.defaultSvc) {
		p.lastActivity = p.clk.Now()
		p.armTimer(tickCooldown, p.cooldown)
		return
	}

	// A failed swap left a default-service pool cold: retry on the
	// cooldown cadence (bounded — one start attempt per period) so the
	// warm-default behavior recovers without traffic. External stops
	// (ERROR state) deliberately do NOT retry: `docker stop` by an
	// operator should stick.
	if !activated && p.state == StateIdle && p.defaultSvc != "" && p.cooldown > 0 {
		p.armTimer(tickCooldown, p.cooldown)
	}
}

// handleAdminSwap forces a swap to a service, bypassing the grace period
// (operator intent is explicit). Mid-swap requests chain at completion.
func (p *Pool) handleAdminSwap(service string) AdminOutcome {
	if _, ok := p.members[service]; !ok {
		return AdminUnknown
	}
	switch p.state {
	case StateActive:
		if p.active == service {
			return AdminAlreadyActive
		}
		p.startSwap(service)
		return AdminInitiated
	case StateSwapping:
		if p.target == service {
			return AdminPending // already heading there
		}
		p.adminPending = service
		return AdminPending
	default: // Idle, Error
		p.startSwap(service)
		return AdminInitiated
	}
}

// handleAdminStop forces the pool cold: every queue flushes with 503 and
// running containers are stopped.
func (p *Pool) handleAdminStop() AdminOutcome {
	p.adminPending = ""
	for _, m := range p.members {
		m.flush(admitReply{result: AdmitShutdown})
	}
	switch p.state {
	case StateIdle:
		return AdminAlreadyIdle
	case StateSwapping:
		p.opSeq++ // orphan the in-flight worker
		p.target = ""
		p.startStopAll()
		return AdminInitiated
	default: // Active, Error
		p.startStopAll()
		return AdminInitiated
	}
}

func (p *Pool) buildStatus() PoolStatus {
	st := PoolStatus{
		State:                p.state,
		ActiveService:        p.active,
		SecondsUntilCooldown: -1,
	}
	for _, m := range p.members {
		st.QueuedRequests += len(m.queue)
	}
	if p.state == StateActive && p.cooldownArmed {
		// Effective expiry tracks last activity, mirroring the tick's
		// self-extension logic.
		if remaining := p.lastActivity.Add(p.cooldown).Sub(p.clk.Now()); remaining > 0 {
			st.SecondsUntilCooldown = int64(remaining.Seconds())
		} else {
			st.SecondsUntilCooldown = 0
		}
	}
	return st
}

func (p *Pool) handleTick(tick timerTick) {
	if tick.epoch != p.epochs[tick.kind] {
		return // superseded: a newer arm or a transition happened
	}
	switch tick.kind {
	case tickGrace:
		if p.state != StateActive {
			return // never act on timers mid-swap
		}
		if next := p.oldestQueued(); next != "" {
			p.startSwap(next)
		}
	case tickCooldown:
		if p.state == StateIdle {
			// Retry tick (armed after a failed swap to the default):
			// bring the warm default back without waiting for traffic.
			if p.defaultSvc != "" && p.oldestQueued() == "" {
				p.log.Info("retrying default service after earlier failure", "default", p.defaultSvc)
				p.startSwap(p.defaultSvc)
			}
			return
		}
		if p.state != StateActive {
			return // never act on timers mid-swap
		}
		if p.oldestQueued() != "" {
			// A grace-driven swap is pending; defer the whole period so
			// the countdown survives even if those waiters cancel.
			p.armTimer(tickCooldown, p.cooldown)
			return
		}
		if idle := p.clk.Since(p.lastActivity); idle < p.cooldown {
			// Traffic arrived since arming: extend by the remainder.
			p.armTimer(tickCooldown, p.cooldown-idle)
			return
		}
		if p.defaultSvc != "" {
			if p.active != p.defaultSvc {
				p.log.Info("cooldown expired, swapping to default", "default", p.defaultSvc)
				p.startSwap(p.defaultSvc)
			}
			return
		}
		p.startStopAll()
	}
}

// chainNow starts a swap for the longest-waiting service, used after a
// failure or cold stop when nothing is running (no grace needed).
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

// otherContainers lists every member container except target's, sorted
// for deterministic stop ordering.
func (p *Pool) otherContainers(target string) []string {
	out := make([]string, 0, len(p.members)-1)
	for name, m := range p.members {
		if name != target {
			out = append(out, m.containerName)
		}
	}
	slices.Sort(out)
	return out
}

// armTimer schedules an epoch-guarded tick. The callback only enqueues a
// message — all decisions stay in the manager loop.
func (p *Pool) armTimer(kind timerKind, d time.Duration) {
	p.epochs[kind]++
	e := p.epochs[kind]
	if kind == tickCooldown {
		p.cooldownArmed = true
	}
	p.clk.AfterFunc(d, func() {
		select {
		case p.timers <- timerTick{kind: kind, epoch: e}:
		case <-p.done:
		}
	})
}

func (p *Pool) bumpEpochs() {
	p.epochs[tickGrace]++
	p.epochs[tickCooldown]++
	p.cooldownArmed = false
}

func (p *Pool) publish() {
	p.snap.Store(&PoolSnapshot{State: p.state, ActiveService: p.active})
}

func (p *Pool) shutdown() {
	// Publish a terminal snapshot FIRST: the lock-free Admit fast path
	// must never see a stale ACTIVE state on a closed pool, or requests
	// in flight across a reload would proxy into an unmanaged container
	// instead of receiving the contractual 503.
	p.state = StateIdle
	p.active = ""
	p.publish()
	for _, m := range p.members {
		m.flush(admitReply{result: AdmitShutdown})
	}
	p.log.Info("pool manager stopped")
}
