package proxy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// MaxPeekSize caps how much of a request body is buffered for model
// extraction. Chat-completion request bodies are JSON and comfortably
// under this; anything larger is forwarded untouched without parsing.
const MaxPeekSize = 1 << 20 // 1 MiB

// PeekModel extracts the top-level "model" field from a JSON request body
// without consuming it: req.Body is replaced with a reader that replays
// the original bytes to the backend exactly.
//
// maxBuffer caps how much of the body is buffered into memory. When >0 it
// raises the buffering ceiling above MaxPeekSize so an accepted body up to
// the configured request cap is fully buffered — that sets req.GetBody and
// makes the request replayable, which lets http.Transport transparently
// retry on a dead keep-alive connection (the normal case right after a
// container swap, when pooled idle conns to the backend have just gone
// stale). When maxBuffer is 0 the ceiling is MaxPeekSize and a body over
// that is streamed through untouched (and is not replayable). The "model"
// field is only parsed out of bodies within MaxPeekSize.
//
// It returns "" (with the body intact) when there is no body, the body
// exceeds the buffering ceiling, or the body isn't JSON with a string
// "model" field. Only the request body is ever buffered — never a response.
func PeekModel(req *http.Request, maxBuffer int64) (string, error) {
	if req.Body == nil || req.Body == http.NoBody {
		return "", nil
	}

	bufLimit := int64(MaxPeekSize)
	if maxBuffer > bufLimit {
		bufLimit = maxBuffer
	}

	buf, err := io.ReadAll(io.LimitReader(req.Body, bufLimit+1))
	if err != nil {
		req.Body.Close()
		return "", fmt.Errorf("peeking request body: %w", err)
	}

	if int64(len(buf)) > bufLimit {
		// Too large to buffer: replay the buffered prefix + the unread
		// remainder so the backend still receives the full body. Only
		// reachable when uncapped (maxBuffer==0); a configured cap rejects
		// oversized bodies upstream before they reach here. No GetBody, so
		// this request is not replayable after a swap.
		req.Body = replayBody{
			Reader: io.MultiReader(bytes.NewReader(buf), req.Body),
			closer: req.Body,
		}
		return "", nil
	}

	// Fully buffered: the original stream is exhausted; close it and
	// replay from memory. GetBody makes the request replayable.
	req.Body.Close()
	req.Body = io.NopCloser(bytes.NewReader(buf))
	req.GetBody = func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(buf)), nil
	}

	// Only parse bodies within MaxPeekSize: a large-but-capped body is
	// buffered for replay but not worth unmarshaling just for one field.
	if int64(len(buf)) > MaxPeekSize {
		return "", nil
	}

	var probe struct {
		Model string `json:"model"`
	}
	if err := json.Unmarshal(buf, &probe); err != nil {
		return "", nil // not JSON (or unexpected shape) — route by fallback
	}
	return probe.Model, nil
}

// replayBody replays buffered bytes ahead of a still-attached original
// body (the oversized case); closing it closes the original. The fully
// buffered case uses plain io.NopCloser instead.
type replayBody struct {
	io.Reader
	closer io.Closer
}

func (r replayBody) Close() error {
	return r.closer.Close()
}
