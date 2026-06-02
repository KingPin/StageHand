package orchestrator

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/benbjohnson/clock"

	"github.com/KingPin/StageHand/internal/dockerctl"
)

// healthGate is a controllable health endpoint: it answers 503 until
// released (Open), then 200. Non-blocking by design so it composes with
// both real and mock clocks.
type healthGate struct {
	ok atomic.Bool
}

func (g *healthGate) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	if g.ok.Load() {
		w.WriteHeader(http.StatusOK)
		return
	}
	w.WriteHeader(http.StatusServiceUnavailable)
}

func (g *healthGate) Open() { g.ok.Store(true) }

type testPool struct {
	pool   *Pool
	docker *dockerctl.FakeClient
	gates  map[string]*healthGate
}

// tpOpts configures newTestPoolOpts; zero values are sensible defaults.
type tpOpts struct {
	grace      time.Duration
	cooldown   time.Duration
	defaultSvc string
	maxQueue   int      // default 10
	services   []string // default ["alpha", "beta"]
	held       []string
}

// newTestPoolOpts builds a two-service pool ("alpha", "beta") whose
// health endpoints start open unless listed in held.
func newTestPoolOpts(t *testing.T, clk clock.Clock, o tpOpts) *testPool {
	t.Helper()
	if o.maxQueue == 0 {
		o.maxQueue = 10
	}
	if len(o.services) == 0 {
		o.services = []string{"alpha", "beta"}
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	containers := make([]string, len(o.services))
	for i, svc := range o.services {
		containers[i] = svc + "-c"
	}
	docker := dockerctl.NewFake(containers...)

	gates := map[string]*healthGate{}
	members := make([]MemberConfig, 0, len(o.services))
	for _, svc := range o.services {
		gate := &healthGate{}
		if !slices.Contains(o.held, svc) {
			gate.Open()
		}
		gates[svc] = gate
		srv := httptest.NewServer(gate)
		t.Cleanup(srv.Close)
		members = append(members, MemberConfig{
			Name:           svc,
			ContainerName:  svc + "-c",
			HealthURL:      srv.URL + "/health",
			StartupTimeout: 30 * time.Second,
			MaxQueue:       o.maxQueue,
		})
	}

	p := NewPool(PoolConfig{
		Name:           "gpu0",
		GracePeriod:    o.grace,
		Cooldown:       o.cooldown,
		DefaultService: o.defaultSvc,
		Members:        members,
	}, docker, clk, log)
	t.Cleanup(p.Close)
	return &testPool{pool: p, docker: docker, gates: gates}
}

func newTestPool(t *testing.T, clk clock.Clock, grace time.Duration, maxQueue int, held ...string) *testPool {
	return newTestPoolOpts(t, clk, tpOpts{grace: grace, maxQueue: maxQueue, held: held})
}

// waitFor polls cond in real time, failing the test after 5 seconds.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// advanceUntil drives a mock clock forward until cond holds.
func advanceUntil(t *testing.T, mock *clock.Mock, step time.Duration, what string, cond func() bool) {
	t.Helper()
	for i := 0; i < 500; i++ {
		if cond() {
			return
		}
		mock.Add(step)
		time.Sleep(time.Millisecond) // let woken goroutines run
	}
	t.Fatalf("mock clock exhausted waiting for %s", what)
}

func mustAdmit(t *testing.T, p *Pool, svc string) {
	t.Helper()
	res, err := p.Admit(context.Background(), svc)
	if res != AdmitGo {
		t.Fatalf("Admit(%s) = %v, %v; want AdmitGo", svc, res, err)
	}
}

func TestIdleFastStartSkipsStop(t *testing.T) {
	tp := newTestPool(t, clock.New(), 0, 10)
	mustAdmit(t, tp.pool, "alpha")

	if calls := tp.docker.Calls(); !slices.Equal(calls, []string{"start:alpha-c"}) {
		t.Errorf("calls = %v, want only [start:alpha-c] (idle pool must skip stop)", calls)
	}
	if s := tp.pool.Snapshot(); s.State != StateActive || s.ActiveService != "alpha" {
		t.Errorf("snapshot = %+v, want ACTIVE alpha", s)
	}
}

func TestActiveFastPathNoNewDockerCalls(t *testing.T) {
	tp := newTestPool(t, clock.New(), 0, 10)
	mustAdmit(t, tp.pool, "alpha")
	before := len(tp.docker.Calls())

	for range 50 {
		mustAdmit(t, tp.pool, "alpha")
	}
	if after := len(tp.docker.Calls()); after != before {
		t.Errorf("docker calls grew %d -> %d on fast-path admits", before, after)
	}
}

func TestSwapStopsActiveThenStartsTarget(t *testing.T) {
	tp := newTestPool(t, clock.New(), 0, 10)
	mustAdmit(t, tp.pool, "alpha")
	mustAdmit(t, tp.pool, "beta")

	want := []string{"start:alpha-c", "stop:alpha-c", "start:beta-c"}
	if calls := tp.docker.Calls(); !slices.Equal(calls, want) {
		t.Errorf("calls = %v, want %v", calls, want)
	}
	if running := tp.docker.Running(); !slices.Equal(running, []string{"beta-c"}) {
		t.Errorf("running = %v, want only beta-c (mutual exclusion)", running)
	}
}

func TestConcurrentAdmitsDuringSwapAllFlushedOneStart(t *testing.T) {
	tp := newTestPool(t, clock.New(), 0, 100, "beta") // beta health held closed

	const n = 25
	var wg sync.WaitGroup
	results := make([]AdmitResult, n)
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i], _ = tp.pool.Admit(context.Background(), "beta")
		}()
	}

	// All n requests pile up while beta can't pass health checks.
	waitFor(t, "swap to start", func() bool {
		return slices.Contains(tp.docker.Calls(), "start:beta-c")
	})
	tp.gates["beta"].Open()
	wg.Wait()

	for i, r := range results {
		if r != AdmitGo {
			t.Errorf("admit[%d] = %v, want AdmitGo", i, r)
		}
	}
	starts := 0
	for _, c := range tp.docker.Calls() {
		if c == "start:beta-c" {
			starts++
		}
	}
	if starts != 1 {
		t.Errorf("start:beta-c called %d times, want exactly 1", starts)
	}
}

func TestQueueOverflowReturns429(t *testing.T) {
	tp := newTestPool(t, clock.New(), 0, 2, "beta")

	// Two admits fill the queue (the first also triggers the swap).
	done := make(chan AdmitResult, 2)
	for range 2 {
		go func() {
			r, _ := tp.pool.Admit(context.Background(), "beta")
			done <- r
		}()
	}
	waitFor(t, "two queued waiters", func() bool {
		return slices.Contains(tp.docker.Calls(), "start:beta-c")
	})
	// Give the second admit a beat to be enqueued by the manager.
	waitFor(t, "queue depth 2", func() bool {
		res, _ := tp.pool.Admit(context.Background(), "beta")
		return res == AdmitQueueFull
	})

	tp.gates["beta"].Open()
	for range 2 {
		if r := <-done; r != AdmitGo {
			t.Errorf("queued admit = %v, want AdmitGo", r)
		}
	}
}

func TestDockerStartFailureFlushes502AndRecovers(t *testing.T) {
	tp := newTestPool(t, clock.New(), 0, 10)
	boom := errors.New("no such image")
	tp.docker.SetStartErr("alpha-c", boom)

	res, err := tp.pool.Admit(context.Background(), "alpha")
	if res != AdmitDockerError {
		t.Fatalf("Admit = %v, want AdmitDockerError", res)
	}
	if !errors.Is(err, boom) {
		t.Errorf("err = %v, want wrapped docker error", err)
	}
	if s := tp.pool.Snapshot(); s.State != StateIdle {
		t.Errorf("state = %v after failure, want IDLE", s.State)
	}

	// Pool recovers once Docker behaves again.
	tp.docker.SetStartErr("alpha-c", nil)
	mustAdmit(t, tp.pool, "alpha")
}

func TestDockerStopFailureFlushes502(t *testing.T) {
	tp := newTestPool(t, clock.New(), 0, 10)
	mustAdmit(t, tp.pool, "alpha")
	boom := errors.New("daemon hung")
	tp.docker.SetStopErr("alpha-c", boom)

	res, err := tp.pool.Admit(context.Background(), "beta")
	if res != AdmitDockerError || !errors.Is(err, boom) {
		t.Fatalf("Admit = %v, %v; want AdmitDockerError wrapping stop failure", res, err)
	}
	if s := tp.pool.Snapshot(); s.State != StateIdle {
		t.Errorf("state = %v, want IDLE", s.State)
	}
}

func TestHealthTimeoutReturns504AndTearsDown(t *testing.T) {
	mock := clock.NewMock()
	tp := newTestPool(t, mock, 0, 10, "beta") // beta never becomes healthy

	done := make(chan struct{})
	var res AdmitResult
	var admitErr error
	go func() {
		res, admitErr = tp.pool.Admit(context.Background(), "beta")
		close(done)
	}()

	waitFor(t, "swap to start", func() bool {
		return slices.Contains(tp.docker.Calls(), "start:beta-c")
	})
	advanceUntil(t, mock, time.Second, "startup timeout", func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	})

	if res != AdmitStartupTimeout {
		t.Fatalf("Admit = %v (%v), want AdmitStartupTimeout", res, admitErr)
	}
	if s := tp.pool.Snapshot(); s.State != StateIdle {
		t.Errorf("state = %v, want IDLE", s.State)
	}
	// Unhealthy container gets a best-effort stop.
	waitFor(t, "teardown stop call", func() bool {
		return slices.Contains(tp.docker.Calls(), "stop:beta-c")
	})
}

func TestGracePeriodDefersSwap(t *testing.T) {
	mock := clock.NewMock()
	tp := newTestPool(t, mock, 60*time.Second, 10)
	mustAdmit(t, tp.pool, "alpha") // alpha just started: grace begins

	done := make(chan AdmitResult, 1)
	go func() {
		r, _ := tp.pool.Admit(context.Background(), "beta")
		done <- r
	}()

	// While inside the grace window, alpha must NOT be stopped.
	waitFor(t, "beta queued", func() bool {
		return tp.pool.QueuedCounts()["beta"] == 1
	})
	mock.Add(30 * time.Second) // halfway through grace
	time.Sleep(20 * time.Millisecond)
	if slices.Contains(tp.docker.Calls(), "stop:alpha-c") {
		t.Fatal("alpha stopped during its grace period")
	}
	select {
	case r := <-done:
		t.Fatalf("beta admitted (%v) during alpha's grace period", r)
	default:
	}

	// Past the grace deadline the swap must proceed.
	advanceUntil(t, mock, 10*time.Second, "grace expiry swap", func() bool {
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

	want := []string{"start:alpha-c", "stop:alpha-c", "start:beta-c"}
	if calls := tp.docker.Calls(); !slices.Equal(calls, want) {
		t.Errorf("calls = %v, want %v", calls, want)
	}
}

func TestCancelWhileQueued(t *testing.T) {
	tp := newTestPool(t, clock.New(), 0, 10, "beta")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var res AdmitResult
	var admitErr error
	go func() {
		res, admitErr = tp.pool.Admit(ctx, "beta")
		close(done)
	}()
	waitFor(t, "swap in flight", func() bool {
		return slices.Contains(tp.docker.Calls(), "start:beta-c")
	})

	cancel()
	<-done
	if res != AdmitCanceled || !errors.Is(admitErr, context.Canceled) {
		t.Errorf("Admit = %v, %v; want AdmitCanceled/context.Canceled", res, admitErr)
	}

	// The pool must keep functioning after the cancellation.
	tp.gates["beta"].Open()
	mustAdmit(t, tp.pool, "beta")
}

func TestChainedSwapServesSecondServiceAfterFirst(t *testing.T) {
	tp := newTestPool(t, clock.New(), 0, 10, "beta")
	mustAdmit(t, tp.pool, "alpha")

	// beta requested while alpha active: swap starts (grace 0) but beta's
	// health is held, so an alpha request arrives mid-swap and queues.
	betaDone := make(chan AdmitResult, 1)
	go func() {
		r, _ := tp.pool.Admit(context.Background(), "beta")
		betaDone <- r
	}()
	waitFor(t, "beta swap started", func() bool {
		return slices.Contains(tp.docker.Calls(), "start:beta-c")
	})

	alphaDone := make(chan AdmitResult, 1)
	go func() {
		r, _ := tp.pool.Admit(context.Background(), "alpha")
		alphaDone <- r
	}()

	tp.gates["beta"].Open()
	if r := <-betaDone; r != AdmitGo {
		t.Fatalf("beta admit = %v, want AdmitGo", r)
	}
	// beta is now active; the queued alpha request chains a second swap.
	if r := <-alphaDone; r != AdmitGo {
		t.Fatalf("alpha admit = %v, want AdmitGo (chained swap)", r)
	}
	if running := tp.docker.Running(); !slices.Equal(running, []string{"alpha-c"}) {
		t.Errorf("running = %v, want only alpha-c", running)
	}
}

// TestClosedPoolRejectsFastPath is the reload-safety regression: a pool
// closed while ACTIVE must not keep admitting via the stale snapshot.
func TestClosedPoolRejectsFastPath(t *testing.T) {
	tp := newTestPool(t, clock.New(), 0, 10)
	mustAdmit(t, tp.pool, "alpha") // snapshot: ACTIVE alpha

	tp.pool.Close()
	waitFor(t, "terminal snapshot", func() bool {
		return tp.pool.Snapshot().State != StateActive
	})

	res, _ := tp.pool.Admit(context.Background(), "alpha")
	if res != AdmitShutdown {
		t.Errorf("Admit on closed pool = %v, want AdmitShutdown", res)
	}
}

func TestShutdownFlushesQueuedWith503(t *testing.T) {
	tp := newTestPool(t, clock.New(), 0, 10, "beta")

	done := make(chan AdmitResult, 1)
	go func() {
		r, _ := tp.pool.Admit(context.Background(), "beta")
		done <- r
	}()
	waitFor(t, "queued waiter", func() bool {
		return slices.Contains(tp.docker.Calls(), "start:beta-c")
	})

	tp.pool.Close()
	if r := <-done; r != AdmitShutdown {
		t.Errorf("queued admit after Close = %v, want AdmitShutdown", r)
	}
}

func TestUnknownServiceRejected(t *testing.T) {
	tp := newTestPool(t, clock.New(), 0, 10)
	res, err := tp.pool.Admit(context.Background(), "ghost")
	if res != AdmitDockerError || err == nil {
		t.Errorf("Admit(ghost) = %v, %v; want AdmitDockerError with error", res, err)
	}
}

// TestMutualExclusionInvariantUnderChaos hammers the pool with
// concurrent admits for both services and asserts the core invariant:
// the fake never has both containers running simultaneously.
func TestMutualExclusionInvariantUnderChaos(t *testing.T) {
	tp := newTestPool(t, clock.New(), 0, 200)

	stop := make(chan struct{})
	violation := make(chan string, 1)
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			if running := tp.docker.Running(); len(running) > 1 {
				select {
				case violation <- "both containers running":
				default:
				}
				return
			}
		}
	}()

	var wg sync.WaitGroup
	for i := range 40 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			svc := "alpha"
			if i%2 == 1 {
				svc = "beta"
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			tp.pool.Admit(ctx, svc)
		}()
	}
	wg.Wait()
	close(stop)

	select {
	case v := <-violation:
		t.Fatal(v)
	default:
	}
}
