package proxy

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func mustParse(t *testing.T, raw string) *url.URL {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

// TestSSEChunksArriveIncrementally proves the no-buffering requirement:
// the client must receive chunk 1 while the backend is still holding
// chunk 2 hostage. If the proxy buffered the response, the first read
// would block until the backend finished and the test would time out.
func TestSSEChunksArriveIncrementally(t *testing.T) {
	release := make(chan struct{})
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		fl := w.(http.Flusher)
		fmt.Fprint(w, "data: chunk-1\n\n")
		fl.Flush()
		<-release // hold chunk 2 until the client confirms chunk 1
		fmt.Fprint(w, "data: chunk-2\n\n")
		fl.Flush()
	}))
	defer backend.Close()

	front := httptest.NewServer(New(mustParse(t, backend.URL), discardLogger()))
	defer front.Close()

	resp, err := http.Get(front.URL + "/v1/chat/completions")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Errorf("Content-Type = %q, want text/event-stream (must pass through)", ct)
	}
	if cc := resp.Header.Get("Cache-Control"); cc != "no-cache" {
		t.Errorf("Cache-Control = %q, want no-cache (must pass through)", cc)
	}

	rd := bufio.NewReader(resp.Body)
	first := readEvent(t, rd)
	if first != "data: chunk-1" {
		t.Fatalf("first event = %q, want chunk-1", first)
	}
	close(release) // only now may the backend send chunk 2
	second := readEvent(t, rd)
	if second != "data: chunk-2" {
		t.Fatalf("second event = %q, want chunk-2", second)
	}
}

// readEvent reads one SSE event line, failing the test on a 5s stall
// (which is what a buffering proxy would cause).
func readEvent(t *testing.T, rd *bufio.Reader) string {
	t.Helper()
	type res struct {
		line string
		err  error
	}
	ch := make(chan res, 1)
	go func() {
		for {
			line, err := rd.ReadString('\n')
			if err != nil {
				ch <- res{"", err}
				return
			}
			line = strings.TrimRight(line, "\n")
			if line != "" {
				ch <- res{line, nil}
				return
			}
		}
	}()
	select {
	case r := <-ch:
		if r.err != nil {
			t.Fatalf("reading event: %v", r.err)
		}
		return r.line
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for SSE event — proxy is buffering")
		return ""
	}
}

func TestUpstreamFailureReturns502JSON(t *testing.T) {
	// Point at a port nothing listens on.
	front := httptest.NewServer(New(mustParse(t, "http://127.0.0.1:1"), discardLogger()))
	defer front.Close()

	resp, err := http.Get(front.URL + "/anything")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadGateway {
		t.Errorf("status = %d, want 502", resp.StatusCode)
	}
	var payload map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("502 body is not JSON: %v", err)
	}
	if payload["error"] == "" || payload["detail"] == "" {
		t.Errorf("502 payload = %v, want error + detail fields", payload)
	}
}

func TestHostAndForwardedHeaders(t *testing.T) {
	var gotHost, gotXFF string
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHost = r.Host
		gotXFF = r.Header.Get("X-Forwarded-For")
	}))
	defer backend.Close()

	front := httptest.NewServer(New(mustParse(t, backend.URL), discardLogger()))
	defer front.Close()

	if _, err := http.Get(front.URL); err != nil {
		t.Fatal(err)
	}
	wantHost := strings.TrimPrefix(backend.URL, "http://")
	if gotHost != wantHost {
		t.Errorf("backend saw Host %q, want backend's own host %q", gotHost, wantHost)
	}
	if gotXFF == "" {
		t.Error("X-Forwarded-For not set")
	}
}

func TestPeekModel(t *testing.T) {
	tests := []struct {
		name      string
		body      string
		wantModel string
	}{
		{"model present", `{"model":"qwen-moe","messages":[]}`, "qwen-moe"},
		{"no model field", `{"messages":[]}`, ""},
		{"model not a string", `{"model":42}`, ""},
		{"not json", `hello world`, ""},
		{"empty body", ``, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("POST", "/v1/chat/completions",
				strings.NewReader(tt.body))
			model, err := PeekModel(req)
			if err != nil {
				t.Fatalf("PeekModel: %v", err)
			}
			if model != tt.wantModel {
				t.Errorf("model = %q, want %q", model, tt.wantModel)
			}
			replayed, _ := io.ReadAll(req.Body)
			if string(replayed) != tt.body {
				t.Errorf("replayed body = %q, want original %q", replayed, tt.body)
			}
		})
	}
}

// TestPeekModelSetsGetBody: the buffered case must be replayable so the
// transport can retry on dead keep-alive conns right after a swap.
func TestPeekModelSetsGetBody(t *testing.T) {
	body := `{"model":"qwen-moe"}`
	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(body))
	if _, err := PeekModel(req); err != nil {
		t.Fatal(err)
	}
	if req.GetBody == nil {
		t.Fatal("GetBody is nil; peeked POSTs are not replayable by the transport")
	}
	for i := range 2 { // replayable more than once
		rc, err := req.GetBody()
		if err != nil {
			t.Fatalf("GetBody[%d]: %v", i, err)
		}
		got, _ := io.ReadAll(rc)
		rc.Close()
		if string(got) != body {
			t.Errorf("GetBody[%d] = %q, want %q", i, got, body)
		}
	}
}

func TestPeekModelNilBody(t *testing.T) {
	req := httptest.NewRequest("GET", "/v1/models", nil)
	model, err := PeekModel(req)
	if err != nil || model != "" {
		t.Errorf("PeekModel(nil body) = %q, %v; want \"\", nil", model, err)
	}
}

func TestPeekModelOversizedBodyReplaysIntact(t *testing.T) {
	// 1 MiB + 64KiB of valid JSON — over the cap, must skip parsing but
	// still deliver every byte downstream.
	big := `{"model":"qwen-moe","pad":"` + strings.Repeat("x", MaxPeekSize+64*1024) + `"}`
	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(big))

	model, err := PeekModel(req)
	if err != nil {
		t.Fatalf("PeekModel: %v", err)
	}
	if model != "" {
		t.Errorf("model = %q, want \"\" (oversized bodies are not parsed)", model)
	}
	replayed, err := io.ReadAll(req.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(replayed, []byte(big)) {
		t.Errorf("replayed %d bytes, want %d identical bytes", len(replayed), len(big))
	}
}

// TestPeekModelEndToEndThroughProxy proves the peeked body reaches the
// backend byte-identical via the real ReverseProxy path.
func TestPeekModelEndToEndThroughProxy(t *testing.T) {
	var backendSaw []byte
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		backendSaw, _ = io.ReadAll(r.Body)
	}))
	defer backend.Close()

	rp := New(mustParse(t, backend.URL), discardLogger())
	front := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		model, err := PeekModel(r)
		if err != nil {
			t.Errorf("PeekModel: %v", err)
		}
		if model != "flux" {
			t.Errorf("model = %q, want flux", model)
		}
		rp.ServeHTTP(w, r)
	}))
	defer front.Close()

	body := `{"model":"flux","prompt":"a cat"}`
	resp, err := http.Post(front.URL+"/v1/images", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if string(backendSaw) != body {
		t.Errorf("backend saw %q, want %q", backendSaw, body)
	}
}
