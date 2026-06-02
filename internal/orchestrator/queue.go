package orchestrator

import "time"

// waiter is one queued request: a handler goroutine blocked on reply.
// seq orders waiters ACROSS services so chained swaps serve the
// longest-waiting service first (no starvation by declaration order).
type waiter struct {
	seq   uint64
	reply chan admitReply
}

// member is the manager-private per-service state within a pool.
type member struct {
	name           string
	containerName  string
	healthURL      string
	startupTimeout time.Duration
	maxQueue       int
	queue          []*waiter // FIFO
}

// enqueue appends a waiter, reporting false when the queue is full.
func (m *member) enqueue(w *waiter) bool {
	if len(m.queue) >= m.maxQueue {
		return false
	}
	m.queue = append(m.queue, w)
	return true
}

// removeByReply deletes the waiter with the given reply channel identity.
func (m *member) removeByReply(reply chan admitReply) {
	for i, w := range m.queue {
		if w.reply == reply {
			m.queue = append(m.queue[:i], m.queue[i+1:]...)
			return
		}
	}
}

// flush replies to every queued waiter in FIFO order and empties the queue.
func (m *member) flush(r admitReply) {
	for _, w := range m.queue {
		w.reply <- r // buffered(1), never blocks
	}
	m.queue = nil
}
