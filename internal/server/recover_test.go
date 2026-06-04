package server

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// discardLog returns a *slog.Logger that throws away all output.
func discardLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// TestRecoverPanicYields500 checks that a panicking handler results in a 500
// response and that the panic value is NOT echoed in the response body.
func TestRecoverPanicYields500(t *testing.T) {
	panicker := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("boom")
	})
	h := recoverPanics(panicker, discardLog())

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/test", nil))

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
	body := rec.Body.String()
	if strings.Contains(body, "boom") {
		t.Fatalf("response body leaks panic value: %q", body)
	}
}

// TestRecoverPassthrough checks that a well-behaved handler passes through
// untouched: correct status and body.
func TestRecoverPassthrough(t *testing.T) {
	ok := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok")
	})
	h := recoverPanics(ok, discardLog())

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if got := rec.Body.String(); got != "ok" {
		t.Fatalf("body = %q, want %q", got, "ok")
	}
}

// TestRecoverRePanicsAbortHandler verifies that http.ErrAbortHandler is
// re-panicked unchanged so net/http's intentional-abort semantics are preserved.
func TestRecoverRePanicsAbortHandler(t *testing.T) {
	aborter := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic(http.ErrAbortHandler)
	})
	h := recoverPanics(aborter, discardLog())

	defer func() {
		if rec := recover(); rec != http.ErrAbortHandler {
			t.Fatalf("recover = %v, want http.ErrAbortHandler re-panicked", rec)
		}
	}()
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
	t.Fatal("ServeHTTP returned without panicking")
}
