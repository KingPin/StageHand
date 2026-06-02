package server

import (
	"context"
	"errors"
	"net"
	"net/http"
	"time"
)

// drainTimeout bounds how long in-flight responses (e.g. SSE streams)
// may take to finish after shutdown begins. Queued requests don't wait
// this long — they are released with 503 immediately.
const drainTimeout = 15 * time.Second

// Run serves StageHand on the listener and runs the docker events
// watcher until ctx is cancelled (the caller wires SIGINT/SIGTERM),
// then shuts down gracefully (PRD §6):
//
//  1. stop accepting new connections
//  2. release queued requests with a clean 503 (pool managers close)
//  3. close tunneled WebSocket connections
//  4. drain remaining in-flight responses, bounded by drainTimeout
//
// Running containers are deliberately left as-is — models are expensive
// to reload.
func (s *Server) Run(ctx context.Context, ln net.Listener) error {
	httpSrv := &http.Server{
		Handler: s.Handler(),
		// Generous header read; NO write timeout — SSE streams and
		// 180s+ cold-start queue waits are normal traffic here.
		ReadHeaderTimeout: 30 * time.Second,
	}

	watchCtx, cancelWatch := context.WithCancel(context.Background())
	defer cancelWatch()
	go s.watcher.Run(watchCtx)

	errc := make(chan error, 1)
	go func() { errc <- httpSrv.Serve(ln) }()
	s.log.Info("stagehand listening", "addr", ln.Addr().String())

	select {
	case err := <-errc:
		return err // listener failed outright
	case <-ctx.Done():
	}

	s.log.Info("shutting down: releasing held connections")
	s.Close() // queued admits flush 503; websocket tunnels close

	drainCtx, cancel := context.WithTimeout(context.Background(), drainTimeout)
	defer cancel()
	if err := httpSrv.Shutdown(drainCtx); err != nil {
		s.log.Warn("drain deadline exceeded; forcing close", "err", err)
		httpSrv.Close()
	}
	if err := <-errc; err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	s.log.Info("shutdown complete")
	return nil
}
