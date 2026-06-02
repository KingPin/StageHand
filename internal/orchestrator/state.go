// Package orchestrator implements StageHand's VRAM pool state machine
// (PRD §3). Each pool is an actor: one manager goroutine owns ALL mutable
// pool state and processes every event — request admission, swap results,
// timer ticks, docker events, shutdown — serially from channels. State
// races are structurally impossible because nothing else touches the
// state. The only concession to performance is a lock-free atomic
// snapshot for the request fast path, which is a hint, never authority.
package orchestrator

// PoolState is the lifecycle state of a VRAM pool (PRD §3.1).
type PoolState int

const (
	// StateIdle: no container in the pool is running (cold pool).
	StateIdle PoolState = iota
	// StateActive: exactly one service is running and healthy.
	StateActive
	// StateSwapping: a stop/start/health transition is in flight.
	StateSwapping
	// StateError: external state is inconsistent (e.g. the active
	// container died out-of-band); recovers on next request.
	StateError
)

func (s PoolState) String() string {
	switch s {
	case StateIdle:
		return "IDLE"
	case StateActive:
		return "ACTIVE"
	case StateSwapping:
		return "SWAPPING"
	case StateError:
		return "ERROR"
	default:
		return "UNKNOWN"
	}
}

// PoolSnapshot is the lock-free view published by the manager after every
// transition. The request fast path reads it to skip the command channel
// when the target service is already active.
type PoolSnapshot struct {
	State         PoolState
	ActiveService string
}

// AdmitResult tells a request handler how its admission ended.
type AdmitResult int

const (
	// AdmitGo: the service is active and healthy — proxy now.
	AdmitGo AdmitResult = iota
	// AdmitQueueFull: the service's wait queue is at capacity (429).
	AdmitQueueFull
	// AdmitDockerError: a docker stop/start failed (502).
	AdmitDockerError
	// AdmitStartupTimeout: the service failed health checks within its
	// startup budget (504).
	AdmitStartupTimeout
	// AdmitShutdown: StageHand is shutting down (503).
	AdmitShutdown
	// AdmitCanceled: the client gave up while queued.
	AdmitCanceled
)

// --- internal message types (all consumed by the manager goroutine) ---

type admitReply struct {
	result AdmitResult
	err    error
}

type admitCmd struct {
	service string
	reply   chan admitReply // buffered(1): manager never blocks sending
}

// cancelCmd removes a queued waiter, identified by its reply channel.
type cancelCmd struct {
	service string
	reply   chan admitReply
}

// depthCmd asks the manager for per-service queue depths (status API).
type depthCmd struct {
	reply chan map[string]int
}

type swapKind int

const (
	swapComplete swapKind = iota
	swapFailed
	swapHealthTimeout
	// poolStopped terminates a cold-pool stop transition (cooldown with
	// no default service); target is empty and there is nothing to flush.
	poolStopped
)

// swapMsg is the exactly-once terminal report from a swap worker.
type swapMsg struct {
	op     uint64 // matches Pool.opSeq; stale ops are dropped
	target string
	kind   swapKind
	err    error
}

type timerKind int

const (
	tickGrace timerKind = iota
	tickCooldown
)

// timerTick carries the epoch it was armed with; the manager drops ticks
// whose epoch is stale, which is what makes timer reuse race-free.
//
// Epochs are PER KIND: resetting the cooldown (every request) must not
// invalidate a pending grace tick that queued waiters depend on, and
// arming a grace timer must not cancel the idle cooldown countdown.
// Transitions (swaps) bump both.
type timerTick struct {
	kind  timerKind
	epoch uint64
}
