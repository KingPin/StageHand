// Package dockerctl wraps the Docker Engine API behind a narrow interface
// covering exactly the operations StageHand needs: inspect, start, stop,
// events, and ping. The interface is the seam for the in-memory fake used
// throughout orchestrator and server tests.
package dockerctl

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	cerrdefs "github.com/containerd/errdefs"
	moby "github.com/moby/moby/client"
)

// ErrNotFound reports that a configured container does not exist on the host.
var ErrNotFound = errors.New("container not found")

// ContainerInfo is the subset of container state StageHand cares about.
type ContainerInfo struct {
	ID      string
	Name    string // normalized: no leading "/"
	State   string // "created", "running", "paused", "restarting", "removing", "exited", "dead"
	Running bool
}

// Event is a container lifecycle event from the Docker events stream.
type Event struct {
	ContainerName string
	Action        string // "start", "die", "stop", "kill", ...
}

// Client is the narrow Docker API surface used by StageHand.
type Client interface {
	// InspectByName resolves a container by name. Returns an error
	// wrapping ErrNotFound if the container does not exist.
	InspectByName(ctx context.Context, name string) (ContainerInfo, error)
	// Start starts the named container. Starting an already-running
	// container is a no-op success (Docker semantics).
	Start(ctx context.Context, name string) error
	// Stop gracefully stops the named container, hard-killing after the
	// timeout elapses.
	Stop(ctx context.Context, name string, timeout time.Duration) error
	// Events streams container lifecycle events until ctx is cancelled.
	Events(ctx context.Context) (<-chan Event, <-chan error)
	// Ping verifies daemon connectivity.
	Ping(ctx context.Context) error
}

// realClient implements Client against a live Docker daemon.
type realClient struct {
	cli *moby.Client
}

var _ Client = (*realClient)(nil)

// Connect builds a Client for the Docker daemon at the given unix socket
// path.
//
// API version negotiation is automatic: github.com/moby/moby/client
// enables it by default in moby.New (its WithAPIVersionNegotiation
// option is documented as a deprecated no-op), satisfying the PRD's
// negotiated-compatibility requirement without any extra option.
func Connect(socketPath string) (Client, error) {
	cli, err := moby.New(moby.WithHost("unix://" + socketPath))
	if err != nil {
		return nil, fmt.Errorf("docker client: %w", err)
	}
	return &realClient{cli: cli}, nil
}

func (r *realClient) InspectByName(ctx context.Context, name string) (ContainerInfo, error) {
	res, err := r.cli.ContainerInspect(ctx, name, moby.ContainerInspectOptions{})
	if err != nil {
		return ContainerInfo{}, wrapNotFound(err, name)
	}
	info := ContainerInfo{
		ID:   res.Container.ID,
		Name: strings.TrimPrefix(res.Container.Name, "/"),
	}
	if st := res.Container.State; st != nil {
		info.State = string(st.Status)
		info.Running = st.Running
	}
	return info, nil
}

func (r *realClient) Start(ctx context.Context, name string) error {
	if _, err := r.cli.ContainerStart(ctx, name, moby.ContainerStartOptions{}); err != nil {
		return wrapNotFound(err, name)
	}
	return nil
}

func (r *realClient) Stop(ctx context.Context, name string, timeout time.Duration) error {
	secs := int(timeout.Seconds())
	if _, err := r.cli.ContainerStop(ctx, name, moby.ContainerStopOptions{Timeout: &secs}); err != nil {
		return wrapNotFound(err, name)
	}
	return nil
}

func (r *realClient) Events(ctx context.Context) (<-chan Event, <-chan error) {
	res := r.cli.Events(ctx, moby.EventsListOptions{
		Filters: moby.Filters{}.Add("type", "container"),
	})

	out := make(chan Event)
	go func() {
		defer close(out)
		for msg := range res.Messages {
			ev := Event{
				ContainerName: msg.Actor.Attributes["name"],
				Action:        string(msg.Action),
			}
			select {
			case out <- ev:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, res.Err
}

func (r *realClient) Ping(ctx context.Context) error {
	if _, err := r.cli.Ping(ctx, moby.PingOptions{}); err != nil {
		return fmt.Errorf("docker ping: %w", err)
	}
	return nil
}

// wrapNotFound maps the SDK's not-found classification onto ErrNotFound so
// callers can use errors.Is without importing Docker packages.
func wrapNotFound(err error, name string) error {
	if cerrdefs.IsNotFound(err) {
		return fmt.Errorf("container %q: %w", name, ErrNotFound)
	}
	return err
}
