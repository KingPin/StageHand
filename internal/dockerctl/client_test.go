package dockerctl

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"
)

func TestFakeLifecycle(t *testing.T) {
	ctx := context.Background()
	f := NewFake("comfy", "llama")

	// Initially stopped.
	info, err := f.InspectByName(ctx, "comfy")
	if err != nil {
		t.Fatalf("InspectByName: %v", err)
	}
	if info.Running || info.State != "exited" {
		t.Errorf("fresh container = %+v, want stopped/exited", info)
	}

	// Start → running.
	if err := f.Start(ctx, "comfy"); err != nil {
		t.Fatalf("Start: %v", err)
	}
	info, _ = f.InspectByName(ctx, "comfy")
	if !info.Running || info.State != "running" {
		t.Errorf("after Start = %+v, want running", info)
	}
	if got := f.Running(); !slices.Equal(got, []string{"comfy"}) {
		t.Errorf("Running() = %v, want [comfy]", got)
	}

	// Idempotent start.
	if err := f.Start(ctx, "comfy"); err != nil {
		t.Errorf("Start on running container should be no-op success, got %v", err)
	}

	// Stop → exited.
	if err := f.Stop(ctx, "comfy", 10*time.Second); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	info, _ = f.InspectByName(ctx, "comfy")
	if info.Running || info.State != "exited" {
		t.Errorf("after Stop = %+v, want exited", info)
	}

	wantCalls := []string{"start:comfy", "start:comfy", "stop:comfy"}
	if got := f.Calls(); !slices.Equal(got, wantCalls) {
		t.Errorf("Calls() = %v, want %v", got, wantCalls)
	}
}

func TestFakeNotFound(t *testing.T) {
	ctx := context.Background()
	f := NewFake("real")

	if _, err := f.InspectByName(ctx, "ghost"); !errors.Is(err, ErrNotFound) {
		t.Errorf("InspectByName(ghost) err = %v, want ErrNotFound", err)
	}
	if err := f.Start(ctx, "ghost"); !errors.Is(err, ErrNotFound) {
		t.Errorf("Start(ghost) err = %v, want ErrNotFound", err)
	}
	if err := f.Stop(ctx, "ghost", time.Second); !errors.Is(err, ErrNotFound) {
		t.Errorf("Stop(ghost) err = %v, want ErrNotFound", err)
	}
}

func TestFakeInjectedErrors(t *testing.T) {
	ctx := context.Background()
	f := NewFake("flaky")
	boom := errors.New("daemon exploded")

	f.SetStartErr("flaky", boom)
	if err := f.Start(ctx, "flaky"); !errors.Is(err, boom) {
		t.Errorf("Start err = %v, want injected error", err)
	}
	// Failed start must not mark the container running.
	if got := f.Running(); len(got) != 0 {
		t.Errorf("Running() = %v after failed start, want empty", got)
	}

	f.SetStartErr("flaky", nil)
	if err := f.Start(ctx, "flaky"); err != nil {
		t.Fatalf("Start after clearing error: %v", err)
	}
	f.SetStopErr("flaky", boom)
	if err := f.Stop(ctx, "flaky", time.Second); !errors.Is(err, boom) {
		t.Errorf("Stop err = %v, want injected error", err)
	}
	// Failed stop leaves it running.
	if got := f.Running(); !slices.Equal(got, []string{"flaky"}) {
		t.Errorf("Running() = %v after failed stop, want [flaky]", got)
	}
}

func TestFakeEventsStream(t *testing.T) {
	ctx := context.Background()
	f := NewFake("svc")
	events, _ := f.Events(ctx)

	if err := f.Start(ctx, "svc"); err != nil {
		t.Fatalf("Start: %v", err)
	}
	f.EmitExternal("svc", "die") // human runs docker stop

	want := []Event{
		{ContainerName: "svc", Action: "start"},
		{ContainerName: "svc", Action: "die"},
	}
	for i, w := range want {
		select {
		case got := <-events:
			if got != w {
				t.Errorf("event[%d] = %+v, want %+v", i, got, w)
			}
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for event %d", i)
		}
	}

	// EmitExternal must also mutate state (mirrors real Docker).
	info, _ := f.InspectByName(ctx, "svc")
	if info.Running {
		t.Error("container still running after external die event")
	}
}

func TestFakeEventsErrorInjection(t *testing.T) {
	f := NewFake()
	_, errs := f.Events(context.Background())
	boom := errors.New("stream broke")
	f.EmitStreamError(boom)
	select {
	case err := <-errs:
		if !errors.Is(err, boom) {
			t.Errorf("err = %v, want injected", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for stream error")
	}
}
