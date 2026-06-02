package router

import (
	"net/http"
	"strings"
	"testing"

	"github.com/KingPin/StageHand/internal/config"
)

func testRouter() *Router {
	return New([]config.Route{
		{PathPrefix: "/v1/embeddings", Service: "embed"},
		// Header-gated route declared BEFORE a catch-all on the same prefix:
		{PathPrefix: "/v1/chat/completions", Service: "llama-special",
			Headers: map[string]string{"X-Use-Model": "special"}},
		{PathPrefix: "/v1/chat/completions", Service: "llama-moe",
			Models: map[string]string{"qwen-moe": "llama-moe", "flux": "comfyui"}},
		{PathPrefix: "/ws", Service: "comfyui"},
	})
}

func hdr(kv ...string) http.Header {
	h := http.Header{}
	for i := 0; i < len(kv); i += 2 {
		h.Set(kv[i], kv[i+1])
	}
	return h
}

func TestMatch(t *testing.T) {
	r := testRouter()
	tests := []struct {
		name        string
		path        string
		header      http.Header
		model       string
		wantSvc     string
		wantNeeds   bool
		wantMatched bool
	}{
		{
			name: "plain prefix match",
			path: "/v1/embeddings", wantSvc: "embed", wantMatched: true,
		},
		{
			name: "prefix match with longer path",
			path: "/v1/embeddings/extra", wantSvc: "embed", wantMatched: true,
		},
		{
			name: "header gate satisfied takes priority (declared first)",
			path: "/v1/chat/completions", header: hdr("X-Use-Model", "special"),
			wantSvc: "llama-special", wantMatched: true,
		},
		{
			name: "header mismatch falls through to next route",
			path: "/v1/chat/completions", header: hdr("X-Use-Model", "other"),
			wantSvc: "llama-moe", wantNeeds: true, wantMatched: true,
		},
		{
			name: "no header falls through to models route",
			path: "/v1/chat/completions",
			wantSvc: "llama-moe", wantNeeds: true, wantMatched: true,
		},
		{
			name: "model maps to service",
			path: "/v1/chat/completions", model: "flux",
			wantSvc: "comfyui", wantNeeds: true, wantMatched: true,
		},
		{
			name: "unknown model falls back to route service",
			path: "/v1/chat/completions", model: "gpt-9000",
			wantSvc: "llama-moe", wantNeeds: true, wantMatched: true,
		},
		{
			name: "websocket path",
			path: "/ws", wantSvc: "comfyui", wantMatched: true,
		},
		{
			name: "no route matches",
			path: "/v2/nothing", wantMatched: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := tt.header
			if h == nil {
				h = http.Header{}
			}
			m, ok := r.Match(tt.path, h, tt.model)
			if ok != tt.wantMatched {
				t.Fatalf("matched = %v, want %v", ok, tt.wantMatched)
			}
			if !ok {
				return
			}
			if m.Service != tt.wantSvc {
				t.Errorf("Service = %q, want %q", m.Service, tt.wantSvc)
			}
			if m.NeedsModel != tt.wantNeeds {
				t.Errorf("NeedsModel = %v, want %v", m.NeedsModel, tt.wantNeeds)
			}
		})
	}
}

func TestFirstMatchWinsInDeclaredOrder(t *testing.T) {
	r := New([]config.Route{
		{PathPrefix: "/api", Service: "first"},
		{PathPrefix: "/api/deeper", Service: "never-reached"},
	})
	m, ok := r.Match("/api/deeper/x", http.Header{}, "")
	if !ok || m.Service != "first" {
		t.Errorf("Match = %+v ok=%v, want first (declared order, not longest prefix)", m, ok)
	}
}

func TestKnownRoutes(t *testing.T) {
	known := testRouter().KnownRoutes()
	if len(known) != 4 {
		t.Fatalf("KnownRoutes len = %d, want 4", len(known))
	}
	// Models listed deterministically (sorted) for stable 404 payloads.
	want := "/v1/chat/completions -> llama-moe (models: flux, qwen-moe)"
	if known[2] != want {
		t.Errorf("known[2] = %q, want %q", known[2], want)
	}
	for _, k := range known {
		if !strings.Contains(k, " -> ") {
			t.Errorf("route description %q missing service arrow", k)
		}
	}
}
