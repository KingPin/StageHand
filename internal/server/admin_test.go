package server

import (
	"encoding/json"
	"net/http"
	"slices"
	"strings"
	"testing"
)

func getStatus(t *testing.T, rig *testRig) statusJSON {
	t.Helper()
	resp, err := http.Get(rig.front.URL + "/stagehand/status")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status endpoint = %d, want 200", resp.StatusCode)
	}
	var out statusJSON
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("status not JSON: %v", err)
	}
	return out
}

func TestStatusEndpoint(t *testing.T) {
	rig := newRig(t, 10)

	// Cold: pool idle, always-on healthy.
	st := getStatus(t, rig)
	if st.Status != "healthy" || st.Version == "" {
		t.Errorf("status/version = %q/%q", st.Status, st.Version)
	}
	pool, ok := st.VRAMPools["gpu0"]
	if !ok {
		t.Fatalf("vram_pools = %v, want gpu0", st.VRAMPools)
	}
	if pool.State != "IDLE" || pool.ActiveService != "" || pool.QueuedRequestsCount != 0 {
		t.Errorf("cold pool status = %+v, want IDLE/empty", pool)
	}
	if pool.SecondsUntilCooldown != nil {
		t.Errorf("seconds_until_cooldown = %v, want null when disarmed", *pool.SecondsUntilCooldown)
	}
	if got := st.AlwaysOnHealthy["gamma"]; got != "healthy" {
		t.Errorf("gamma = %q, want healthy", got)
	}

	// Activate alpha, then re-check.
	if _, err := http.Get(rig.front.URL + "/v1/chat/completions"); err != nil {
		t.Fatal(err)
	}
	st = getStatus(t, rig)
	pool = st.VRAMPools["gpu0"]
	if pool.State != "ACTIVE" || pool.ActiveService != "alpha" {
		t.Errorf("pool status = %+v, want ACTIVE alpha", pool)
	}

	// Unhealthy always-on is reported, not hidden.
	rig.backends["gamma"].healthOK.Store(false)
	st = getStatus(t, rig)
	if got := st.AlwaysOnHealthy["gamma"]; got != "unhealthy" {
		t.Errorf("gamma = %q, want unhealthy", got)
	}
}

func TestAdminSwapPreWarms(t *testing.T) {
	rig := newRig(t, 10)

	resp, err := http.Post(rig.front.URL+"/stagehand/swap/beta", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("swap status = %d, want 202", resp.StatusCode)
	}
	var out map[string]string
	json.NewDecoder(resp.Body).Decode(&out)
	if out["result"] != "initiated" {
		t.Errorf("result = %q, want initiated", out["result"])
	}

	// Pre-warm completes without any client traffic.
	waitFor(t, "beta active", func() bool {
		st := getStatus(t, rig)
		p := st.VRAMPools["gpu0"]
		return p.State == "ACTIVE" && p.ActiveService == "beta"
	})

	// Swapping to the already-active service reports as such.
	resp2, err := http.Post(rig.front.URL+"/stagehand/swap/beta", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	json.NewDecoder(resp2.Body).Decode(&out)
	if out["result"] != "already-active" {
		t.Errorf("repeat swap result = %q, want already-active", out["result"])
	}
}

func TestAdminSwapUnknownService(t *testing.T) {
	rig := newRig(t, 10)
	resp, err := http.Post(rig.front.URL+"/stagehand/swap/ghost", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404", resp.StatusCode)
	}

	// Always-on services are not swappable either.
	resp2, err := http.Post(rig.front.URL+"/stagehand/swap/gamma", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusNotFound {
		t.Errorf("swap always-on = %d, want 404", resp2.StatusCode)
	}
}

func TestAdminPoolStopForcesIdleAndFlushesQueue(t *testing.T) {
	rig := newRig(t, 10)
	rig.backends["beta"].healthOK.Store(false) // beta swap will hang

	// A queued client waits on beta.
	clientDone := make(chan int, 1)
	go func() {
		resp, err := http.Post(rig.front.URL+"/v1/chat/completions",
			"application/json", strings.NewReader(`{"model":"m-beta"}`))
		if err != nil {
			clientDone <- -1
			return
		}
		defer resp.Body.Close()
		clientDone <- resp.StatusCode
	}()
	waitFor(t, "beta swap in flight", func() bool {
		return slices.Contains(rig.docker.Calls(), "start:beta-c")
	})

	resp, err := http.Post(rig.front.URL+"/stagehand/pool/gpu0/stop", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("pool stop = %d, want 202", resp.StatusCode)
	}

	// The queued client is released with 503.
	if code := <-clientDone; code != http.StatusServiceUnavailable {
		t.Errorf("queued client got %d, want 503", code)
	}
	waitFor(t, "pool idle with nothing running", func() bool {
		st := getStatus(t, rig)
		return st.VRAMPools["gpu0"].State == "IDLE" && len(rig.docker.Running()) == 0
	})
}

func TestAdminPoolStopUnknownPool(t *testing.T) {
	rig := newRig(t, 10)
	resp, err := http.Post(rig.front.URL+"/stagehand/pool/ghost/stop", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404", resp.StatusCode)
	}
}
