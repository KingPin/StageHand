package dockerctl

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"time"
)

// FakeClient is an in-memory Client for tests. It mirrors real Docker
// semantics: Start/Stop mutate container state and emit lifecycle events
// on the events stream (as the daemon would), and EmitExternal simulates
// out-of-band container changes (a human running `docker stop`).
//
// All methods are safe for concurrent use.
type FakeClient struct {
	mu         sync.Mutex
	containers map[string]*ContainerInfo
	calls      []string // chronological op log: "start:name", "stop:name"
	startErr   map[string]error
	stopErr    map[string]error
	events      chan Event
	evErrs      chan error
	beforeStop  func(name string) // test hook, invoked without the lock at Stop entry
	beforeStart func(name string) // test hook, invoked without the lock at Start entry
}

var _ Client = (*FakeClient)(nil)

// NewFake builds a FakeClient with the given containers, all stopped.
func NewFake(names ...string) *FakeClient {
	f := &FakeClient{
		containers: make(map[string]*ContainerInfo, len(names)),
		startErr:   map[string]error{},
		stopErr:    map[string]error{},
		// Generous buffer: fake ops must never block a test that has
		// no events consumer.
		events: make(chan Event, 256),
		evErrs: make(chan error, 1),
	}
	for i, n := range names {
		f.containers[n] = &ContainerInfo{
			ID:    fmt.Sprintf("fake-%d-%s", i, n),
			Name:  n,
			State: "exited",
		}
	}
	return f
}

func (f *FakeClient) InspectByName(_ context.Context, name string) (ContainerInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	c, ok := f.containers[name]
	if !ok {
		return ContainerInfo{}, fmt.Errorf("container %q: %w", name, ErrNotFound)
	}
	return *c, nil
}

func (f *FakeClient) Start(_ context.Context, name string) error {
	f.mu.Lock()
	hook := f.beforeStart
	f.mu.Unlock()
	if hook != nil {
		hook(name) // tests park a swap worker at the start of its target here
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "start:"+name)
	if err := f.startErr[name]; err != nil {
		return err
	}
	c, ok := f.containers[name]
	if !ok {
		return fmt.Errorf("container %q: %w", name, ErrNotFound)
	}
	if c.Running {
		return nil // Docker semantics: starting a running container is a no-op
	}
	c.Running = true
	c.State = "running"
	f.emit(Event{ContainerName: name, Action: "start"})
	return nil
}

func (f *FakeClient) Stop(_ context.Context, name string, _ time.Duration) error {
	f.mu.Lock()
	hook := f.beforeStop
	f.mu.Unlock()
	if hook != nil {
		hook(name) // tests park a swap worker mid-sweep here
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "stop:"+name)
	if err := f.stopErr[name]; err != nil {
		return err
	}
	c, ok := f.containers[name]
	if !ok {
		return fmt.Errorf("container %q: %w", name, ErrNotFound)
	}
	if !c.Running {
		return nil
	}
	c.Running = false
	c.State = "exited"
	f.emit(Event{ContainerName: name, Action: "die"})
	return nil
}

func (f *FakeClient) Events(_ context.Context) (<-chan Event, <-chan error) {
	return f.events, f.evErrs
}

func (f *FakeClient) Ping(context.Context) error { return nil }

// --- test helpers ---

// EmitExternal simulates an out-of-band container change (e.g. a human
// running `docker stop`): it mutates state AND emits the event, exactly
// like Start/Stop, but without recording a StageHand-initiated call.
func (f *FakeClient) EmitExternal(name, action string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if c, ok := f.containers[name]; ok {
		switch action {
		case "start":
			c.Running = true
			c.State = "running"
		case "die", "stop", "kill":
			c.Running = false
			c.State = "exited"
		}
	}
	f.emit(Event{ContainerName: name, Action: action})
}

// EmitStreamError injects an error on the events error channel.
func (f *FakeClient) EmitStreamError(err error) { f.evErrs <- err }

// SetBeforeStop installs a hook invoked (without the lock held) at the start
// of every Stop call, before it records the call or mutates state. Tests use
// it to park a swap worker mid-sweep and drive a concurrent transition.
func (f *FakeClient) SetBeforeStop(fn func(name string)) {
	f.mu.Lock()
	f.beforeStop = fn
	f.mu.Unlock()
}

// SetBeforeStart installs a hook invoked (without the lock held) at the start
// of every Start call, before it records the call or mutates state. Tests use
// it to park a swap worker at the instant it starts its target, to drive a
// concurrent supersession into the post-Start window.
func (f *FakeClient) SetBeforeStart(fn func(name string)) {
	f.mu.Lock()
	f.beforeStart = fn
	f.mu.Unlock()
}

// SetStartErr makes future Start calls for name fail with err.
func (f *FakeClient) SetStartErr(name string, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.startErr[name] = err
}

// SetStopErr makes future Stop calls for name fail with err.
func (f *FakeClient) SetStopErr(name string, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stopErr[name] = err
}

// Running returns the names of currently running containers, sorted.
func (f *FakeClient) Running() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for n, c := range f.containers {
		if c.Running {
			out = append(out, n)
		}
	}
	slices.Sort(out)
	return out
}

// Calls returns the chronological log of Start/Stop operations.
func (f *FakeClient) Calls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.calls)
}

func (f *FakeClient) emit(ev Event) {
	select {
	case f.events <- ev:
	default: // never block an op because no test consumes events
	}
}
