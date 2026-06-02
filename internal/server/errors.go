package server

import (
	"encoding/json"
	"net/http"
)

// writeJSON serializes a payload with the right content type.
func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

// writeError emits StageHand's JSON error envelope.
func writeError(w http.ResponseWriter, status int, msg, detail string) {
	body := map[string]string{"error": msg}
	if detail != "" {
		body["detail"] = detail
	}
	writeJSON(w, status, body)
}

// writeUnmatched is the 404 for requests no route matches; listing the
// known routes makes misconfigured clients self-diagnosing (PRD §2.1).
func writeUnmatched(w http.ResponseWriter, routes []string) {
	writeJSON(w, http.StatusNotFound, map[string]any{
		"error":        "no route matched",
		"known_routes": routes,
	})
}
