package orchestrator

import (
	"context"
	"fmt"
	"time"
)

const (
	// gracefulStopTimeout is how long Docker waits for SIGTERM before
	// SIGKILL (PRD §6: prevent model/db corruption).
	gracefulStopTimeout = 10 * time.Second
	// stopConfirmBudget bounds the wait for "exited" confirmation.
	stopConfirmBudget  = 15 * time.Second
	stopConfirmEvery   = 200 * time.Millisecond
	healthProbeEvery   = 250 * time.Millisecond
	dockerCallDeadline = 30 * time.Second
)

// doSwap is the swap worker: stop (optional) → confirm exited → start →
// health poll. It runs OFF the manager goroutine and reports exactly one
// terminal swapMsg — including on panic — so the pool can never wedge in
// SWAPPING.
func (p *Pool) doSwap(op uint64, stopContainer string, m *member) {
	defer func() {
		if r := recover(); r != nil {
			p.deliver(swapMsg{op: op, target: m.name, kind: swapFailed,
				err: fmt.Errorf("swap worker panic: %v", r)})
		}
	}()

	if stopContainer != "" {
		if err := p.stopAndConfirm(stopContainer); err != nil {
			p.deliver(swapMsg{op: op, target: m.name, kind: swapFailed, err: err})
			return
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), dockerCallDeadline)
	err := p.docker.Start(ctx, m.containerName)
	cancel()
	if err != nil {
		p.deliver(swapMsg{op: op, target: m.name, kind: swapFailed,
			err: fmt.Errorf("starting %q: %w", m.containerName, err)})
		return
	}

	deadline := p.clk.Now().Add(m.startupTimeout)
	for {
		if healthOK(m.healthURL) {
			p.deliver(swapMsg{op: op, target: m.name, kind: swapComplete})
			return
		}
		if !p.clk.Now().Before(deadline) {
			p.deliver(swapMsg{op: op, target: m.name, kind: swapHealthTimeout})
			return
		}
		if !p.sleep(healthProbeEvery) {
			return // pool shutting down; manager is gone
		}
	}
}

// stopAndConfirm stops a container gracefully and waits until Docker
// reports it exited (PRD §3.2 step 3).
func (p *Pool) stopAndConfirm(containerName string) error {
	ctx, cancel := context.WithTimeout(context.Background(), dockerCallDeadline)
	err := p.docker.Stop(ctx, containerName, gracefulStopTimeout)
	cancel()
	if err != nil {
		return fmt.Errorf("stopping %q: %w", containerName, err)
	}

	deadline := p.clk.Now().Add(stopConfirmBudget)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), dockerCallDeadline)
		info, err := p.docker.InspectByName(ctx, containerName)
		cancel()
		if err != nil {
			return fmt.Errorf("confirming stop of %q: %w", containerName, err)
		}
		if !info.Running {
			return nil
		}
		if !p.clk.Now().Before(deadline) {
			return fmt.Errorf("container %q still running %s after stop", containerName, stopConfirmBudget)
		}
		if !p.sleep(stopConfirmEvery) {
			return fmt.Errorf("pool shutting down")
		}
	}
}

// sleep waits d on the pool's clock, returning false on pool shutdown.
func (p *Pool) sleep(d time.Duration) bool {
	t := p.clk.Timer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-p.done:
		return false
	}
}

// deliver hands the terminal report to the manager (or drops it if the
// pool is already shut down).
func (p *Pool) deliver(msg swapMsg) {
	select {
	case p.swaps <- msg:
	case <-p.done:
	}
}
