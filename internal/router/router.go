// Package router implements StageHand's ordered first-match request
// routing (PRD §2.1): path-prefix matching with optional header gating
// and optional JSON-body model-name mapping. It is pure — no I/O — so
// the server layer owns body peeking and hands the extracted model in.
package router

import (
	"fmt"
	"net/http"
	"slices"
	"strings"

	"github.com/KingPin/StageHand/internal/config"
)

// Match is the result of routing a request.
type Match struct {
	// Service is the resolved target service name.
	Service string
	// NeedsModel reports that the matched route declares a models map,
	// so the caller should peek the request body's "model" field and
	// re-match with it for an exact mapping. Service already holds the
	// route's fallback, making the second pass optional on peek failure.
	NeedsModel bool
}

// Router matches requests against an ordered route table.
type Router struct {
	routes []config.Route
	known  []string
}

// New builds a Router from validated config routes (order preserved).
func New(routes []config.Route) *Router {
	known := make([]string, 0, len(routes))
	for _, rt := range routes {
		desc := fmt.Sprintf("%s -> %s", rt.PathPrefix, rt.Service)
		if len(rt.Models) > 0 {
			models := make([]string, 0, len(rt.Models))
			for m := range rt.Models {
				models = append(models, m)
			}
			slices.Sort(models)
			desc += fmt.Sprintf(" (models: %s)", strings.Join(models, ", "))
		}
		known = append(known, desc)
	}
	return &Router{routes: slices.Clone(routes), known: known}
}

// Match finds the first route whose path prefix and header conditions
// match. A route whose headers don't match falls through to later routes.
//
// model is the JSON body "model" field value, or "" when unknown. When
// the matched route declares a models map: a matching entry selects that
// service; otherwise the route's service is the fallback and NeedsModel
// is set so the caller knows peeking the body could refine the match.
func (r *Router) Match(path string, header http.Header, model string) (Match, bool) {
	for _, rt := range r.routes {
		if !strings.HasPrefix(path, rt.PathPrefix) {
			continue
		}
		if !headersMatch(rt.Headers, header) {
			continue // fall through to later routes
		}

		m := Match{Service: rt.Service, NeedsModel: len(rt.Models) > 0}
		if model != "" {
			if svc, ok := rt.Models[model]; ok {
				m.Service = svc
			}
		}
		return m, true
	}
	return Match{}, false
}

// KnownRoutes describes the route table for 404 responses.
func (r *Router) KnownRoutes() []string {
	return slices.Clone(r.known)
}

func headersMatch(want map[string]string, got http.Header) bool {
	for k, v := range want {
		if got.Get(k) != v {
			return false
		}
	}
	return true
}
