package orchestrator

import (
	"context"
	"fmt"
	"slices"
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

// doSwap is the swap worker: sweep-stop any running pool sibling →
// confirm exited → start target → health poll. It runs OFF the manager
// goroutine and reports exactly one terminal swapMsg — including on
// panic — so the pool can never wedge in SWAPPING.
//
// Sweeping ALL siblings (not just the last known active) makes every
// swap self-healing: containers left running by a failed stop or
// out-of-band meddling are cleaned up before the target starts, which
// is what ultimately enforces the pool's mutual exclusion.
func (p *Pool) doSwap(op uint64, m *member, others []string) {
	defer func() {
		if r := recover(); r != nil {
			p.deliver(swapMsg{op: op, target: m.name, kind: swapFailed,
				err: fmt.Errorf("swap worker panic: %v", r)})
		}
	}()

	for _, cn := range others {
		running, err := p.isRunning(cn)
		if err != nil {
			p.deliver(swapMsg{op: op, target: m.name, kind: swapFailed, err: err})
			return
		}
		if !running {
			continue
		}
		if err := p.stopAndConfirm(cn); err != nil {
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

// doStopAll is the cold-pool worker: stop every running member container
// (cooldown expiry with default_service null).
func (p *Pool) doStopAll(op uint64) {
	defer func() {
		if r := recover(); r != nil {
			p.deliver(swapMsg{op: op, kind: poolStopped,
				err: fmt.Errorf("stop-all worker panic: %v", r)})
		}
	}()

	var firstErr error
	for _, m := range p.sortedMembers() {
		running, err := p.isRunning(m.containerName)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if !running {
			continue
		}
		if err := p.stopAndConfirm(m.containerName); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	p.deliver(swapMsg{op: op, kind: poolStopped, err: firstErr})
}

func (p *Pool) isRunning(containerName string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), dockerCallDeadline)
	defer cancel()
	info, err := p.docker.InspectByName(ctx, containerName)
	if err != nil {
		return false, fmt.Errorf("inspecting %q: %w", containerName, err)
	}
	return info.Running, nil
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
		running, err := p.isRunning(containerName)
		if err != nil {
			return fmt.Errorf("confirming stop: %w", err)
		}
		if !running {
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

// sortedMembers returns members in deterministic name order.
func (p *Pool) sortedMembers() []*member {
	names := make([]string, 0, len(p.members))
	for n := range p.members {
		names = append(names, n)
	}
	slices.Sort(names)
	out := make([]*member, len(names))
	for i, n := range names {
		out[i] = p.members[n]
	}
	return out
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
