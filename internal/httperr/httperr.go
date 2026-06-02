// Package httperr defines StageHand's JSON error envelope in one place
// so every layer (server handler, reverse proxy) emits the same shape.
package httperr

import (
	"encoding/json"
	"net/http"
)

// Write emits the {error, detail} envelope with the given status.
// detail is omitted when empty.
func Write(w http.ResponseWriter, status int, msg, detail string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	body := map[string]string{"error": msg}
	if detail != "" {
		body["detail"] = detail
	}
	_ = json.NewEncoder(w).Encode(body)
}
