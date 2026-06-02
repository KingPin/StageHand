package orchestrator

import (
	"context"
	"net/http"
	"time"
)

// healthClient is deliberately short-fused: a healthy backend answers its
// health endpoint quickly; anything else counts as not-ready and the
// caller retries on its own schedule.
var healthClient = &http.Client{Timeout: 2 * time.Second}

// ProbeHealth reports whether url answers 200 within the client timeout
// (and ctx). Shared by the swap worker's readiness poll and the status
// API's always-on checks so the timeout policy lives in one place.
func ProbeHealth(ctx context.Context, url string) bool {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return false
	}
	resp, err := healthClient.Do(req)
	if err != nil {
		return false
	}
	resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

// healthOK is the worker-loop shorthand.
func healthOK(url string) bool {
	return ProbeHealth(context.Background(), url)
}
