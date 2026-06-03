package server

import (
	"crypto/subtle"
	"net/http"
)

// Token headers gating StageHand's HTTP surface (PRD §5). Both are
// StageHand-specific and deliberately distinct from Authorization, which is
// reserved for pass-through to AI backends.
const (
	adminTokenHeader = "X-Stagehand-Admin-Token"
	proxyTokenHeader = "X-Stagehand-Token"
)

// checkToken reports whether the request carries the expected token in
// header. The comparison is constant-time to avoid leaking the token through
// response timing; a missing header always fails.
func checkToken(r *http.Request, header, want string) bool {
	got := r.Header.Get(header)
	if got == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(got), []byte(want)) == 1
}
