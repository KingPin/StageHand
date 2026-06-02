package orchestrator

import (
	"net/http"
	"time"
)

// healthClient is deliberately short-fused: a healthy backend answers its
// health endpoint quickly; anything else counts as not-ready and the
// worker retries on its own schedule.
var healthClient = &http.Client{Timeout: 2 * time.Second}

// healthOK reports whether the service's health endpoint returns 200.
func healthOK(url string) bool {
	resp, err := healthClient.Get(url)
	if err != nil {
		return false
	}
	resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}
