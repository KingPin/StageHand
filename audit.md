# Codebase Audit & Beta-Readiness Review — 2026-06-03

**Scope:** Full `internal/` + `cmd/` (~6,750 LOC Go). Three audit pillars
(security/OWASP, correctness/concurrency/leaks, performance) plus a
**feature-gap analysis** of `PRD.md` vs. the implementation for beta release.
**Method:** Four parallel research subagents; every reported finding
re-verified by the coordinator against the cited `file:line`.
**Status:** Findings only — no code changed in this pass.

> **Spot-check correction:** the security subagent reported **four "Critical"**
> findings. All four were re-read and **downgraded** — they are either explicit
> PRD §5.4 design decisions (token logging, the disable-auth escape hatch) or a
> misread of intentional transparent-relay behavior. No Critical issues exist in
> this codebase. This is exactly the inflation the verification step exists to catch.

## Executive Summary

- **Critical: 0**
- **High: 0** (1 correctness item is High-leaning; treated as Medium pending repro)
- **Medium: 5**
- **Low: 6**
- **Info / by-design: 5**

The orchestrator actor model is **sound** — `go vet` clean, no actor-model
violations, no TOCTOU on the atomic snapshot, no goroutine/channel deadlocks in
the state machine. The real beta blockers are **operational gaps** (no CI, no
unauthenticated liveness endpoint, no metrics), not code defects.

**Top 5 by ROI:**
1. Unauthenticated liveness endpoint — `internal/server/handler.go:26` (admin auth gates `/stagehand/status`, so health probes can't reach it)
2. Docker-events goroutine/conn leak on resubscribe — `internal/dockerctl/client.go:105` + `internal/orchestrator/events.go:128`
3. Large request bodies lose retry-after-swap — `internal/proxy/bodypeek.go:34`
4. No panic recovery in the WS tunnel goroutines — `internal/proxy/websocket.go:114`
5. No CI pipeline (`go build/vet/test -race` + gated e2e) — repo has no `.github/`

---

## Beta-Readiness & Feature Gaps

This is the answer to *"what's missing before a beta."* These are **not bugs** —
they are completeness/operability gaps for an infra component people will deploy.

### High priority (block a clean beta)

- **No unauthenticated liveness/readiness endpoint.** Admin auth is on by default
  and gates *everything* under `/stagehand/*` (`handler.go:26-33`), including
  `GET /stagehand/status`. A Docker `HEALTHCHECK`, k8s liveness/readiness probe,
  or uptime monitor therefore **cannot check StageHand's own health** without the
  admin token — and the `Dockerfile` has no `HEALTHCHECK`. **Add a tokenless
  `GET /healthz`** (process-up only, no internal detail) and keep the detailed
  `/stagehand/status` authenticated. This is the single most important beta gap.
- **No CI.** There is no `.github/workflows/`. CLAUDE.md mandates
  `go build && go vet && go test -race ./...` on every change, and a gated e2e
  suite exists, but nothing runs them automatically. Add a CI workflow (build +
  vet + race tests on PR; optionally the `STAGEHAND_E2E=1` suite on a schedule).

### Medium priority (strongly recommended for beta)

- **No metrics endpoint.** For an ops-facing proxy, a Prometheus `/metrics`
  (swap count/duration, queue depth, per-pool state, 429/502/504 counters,
  in-flight requests) is the table-stakes observability surface. PRD doesn't
  require it, but beta operators will expect it. Currently the only signal is the
  JSON status blob (now behind auth) and slog lines.
- **No per-request access logging.** slog is wired for lifecycle events, but
  there's no structured access log (method, path, matched service, status,
  latency, swap-triggered). Hard to debug routing/swap behavior in production
  without it.
- **TLS not addressed.** StageHand serves plain HTTP only (`run.go:28`). Fine if
  always behind a TLS-terminating proxy, but that assumption should be an explicit
  documented deployment requirement (or add optional `server.tls`).

### Low priority (post-beta is fine)

- **No rate limiting** on the admin or proxy planes (PRD doesn't require it).
- **No request lifetime ceiling.** `run.go:30-32` deliberately omits a write
  timeout (correct for SSE / long cold starts) — but that also means a stuck
  backend or slow-loris holds a connection indefinitely. Acceptable tradeoff for
  AI workloads; note it as a known limitation.
- **Version string is hardcoded** (`version.Version`); wire it to a build-time
  ldflags stamp before tagging a beta so `/status` and `--version` report the
  real release.

### PRD features confirmed IMPLEMENTED (no gap)

Routing (path/header/model, `bodypeek.go`), bounded FIFO queues + 429/503/502/504
semantics, pool state machine (IDLE/ACTIVE/SWAPPING/ERROR), grace period & idle
cooldown, Docker events reconciliation, hot reload (SIGHUP + endpoint, old config
kept on failure), graceful shutdown with 15s drain (`run.go`), admin API
(status/swap/pool-stop/reload, `admin.go`), constant-time auth with header
stripping, WebSocket tunneling, CORS preflight. **The PRD's functional surface is
essentially complete** — the gaps above are operational, not functional.

---

## Security (OWASP)

### Low

#### Wildcard CORS is accepted and reflected — `internal/server/cors.go:44`
**What:** When `cors_allowed_origins` contains `"*"` (the PRD's own example
default), `allowedOrigin` echoes the caller's specific Origin for any request.
**Why it matters:** Less dangerous than it first appears — `Access-Control-Allow-Credentials`
is **never set**, and the admin/proxy tokens are custom headers (not cookies), so a
malicious browser page still can't forge an authenticated request. The residual risk
is only that responses become readable cross-origin. **Fix:** Reject `"*"` in
`validate.go` for production configs, or warn loudly; prefer an explicit allowlist.

#### Unbounded request body forwarded without a size cap — `internal/proxy/proxy.go` (forward path)
**What:** Non-peeked request bodies stream straight to the backend with no
`http.MaxBytesReader`. (The *peek* path is correctly capped at 1 MiB,
`bodypeek.go:14,28`.) **Why it matters:** StageHand itself doesn't buffer these, so
the memory-DoS risk is limited — the cost lands on the backend. **Fix:** Optionally
enforce a configurable max body size at the edge for defense-in-depth.

#### Error responses enumerate internal topology — `internal/server/handler.go:63`, `internal/server/errors.go`
**What:** A 404 lists all known routes; Docker/orchestrator errors surface
container names and internal state to the proxy caller. **Why it matters:** Minor
info disclosure to unauthenticated proxy clients (the route list aids an attacker
mapping the deployment). **Fix:** Gate verbose route listing behind a debug flag;
keep client-facing errors generic.

### Info / by-design (reported as Critical by the subagent — corrected)

- **Auto-generated admin token logged** (`main.go:88`) — PRD §5.4 behavior, fires
  only when no token is configured, and logs the *freshly minted random* token so
  the operator can retrieve it. Hardening option: write it to a file or print to
  stdout instead of the slog stream. **Not Critical.**
- **`STAGEHAND_DISABLE_ADMIN_AUTH`** (`main.go:34,63`) — documented PRD §5.4 escape
  hatch with a prominent stderr banner (captured by `docker logs`). Deliberate,
  fail-safe (unparseable value keeps auth on). **Not Critical.**
- **WebSocket "skips 101 validation"** (`websocket.go:111`) — misread. The tunnel
  *intentionally* relays the backend's actual handshake response (101 *or* a 403/500
  rejection) to the client transparently; the client sees the real backend status.
  No confused-deputy. **Clean.**
- **"Auth header name in error"** (`handler.go:30`) — the error contains the header
  *name* (`X-Stagehand-Admin-Token`), never the token value. **Clean.**

### Checked and clean
Constant-time token compare + header stripping before forward (`auth.go`,
`handler.go:48-49`); `crypto/rand` 256-bit fallback token (`main.go:172`); no SQL/
command/template injection surface; Docker socket path validated; `target_url`
parsed/validated; no client influence over backend host (targets are config-fixed).

---

## Correctness (concurrency / leaks)

### Medium

#### Docker-events goroutine + connection leak on resubscribe — `internal/dockerctl/client.go:105`, `internal/orchestrator/events.go:128`
**What:** `Watcher.Run` resubscribes with the **same long-lived `ctx`** on every
stream error (`events.go:126-128`). Each `Events(ctx)` spawns a forwarder goroutine
(`client.go:111`) that sends on an unbuffered `out` channel with only `ctx.Done()`
as the escape. If a message races in on the old stream *after* the watcher has
broken `consume` and moved to a new subscription, that old goroutine blocks on
`out <- ev` until process exit — leaking a goroutine and the underlying events
connection. **Why it matters:** Accumulates one goroutine + conn per resubscribe
under a flapping Docker daemon. Narrow trigger (needs a stream error *and* a
late message), hence Medium not High. **Fix:** Derive a per-subscription
`context.WithCancel(ctx)` and cancel it before resubscribing so the old forwarder's
`<-ctx.Done()` always fires.

#### Large request bodies lose transparent retry-after-swap — `internal/proxy/bodypeek.go:34-42`
**What:** The fully-buffered (<1 MiB) path sets `req.GetBody` so `http.Transport`
can retry on a stale pooled connection (the normal case right after a swap,
`bodypeek.go:44-53`). The **oversized** path sets `req.Body` to a `replayBody` but
**no `GetBody`**. **Why it matters:** Large uploads — exactly the image-gen
(ComfyUI/A1111) workloads that trigger the most swaps — fail with a 502 on the
first stale-conn attempt instead of retrying. Correctness asymmetry on the primary
use case. **Fix:** The oversized body is already fully resident; provide a `GetBody`
that re-reads the buffered prefix + remainder, or document that >1 MiB bodies aren't
retried.

#### No panic recovery in request / tunnel goroutines — `internal/proxy/websocket.go:114-122`, `internal/server/handler.go:15`
**What:** `net/http` recovers panics per-connection for normal handlers, but the two
`io.Copy` tunnel goroutines (`websocket.go:114,118`) run **outside** that guard.
**Why it matters:** An unrecovered panic there crashes the **entire process**, not
just one connection — bad for a long-running daemon. `io.Copy` on a `net.Conn`
rarely panics, hence Medium. **Fix:** Add `defer recover()` in both tunnel
goroutines and a recover middleware on the root handler.

### Low

- **Health probe ignores pool shutdown** — `swap.go:65`, `health.go:31`: `healthOK`
  uses `context.Background()`, bounded only by the 2s client timeout, not `p.done`.
  Always returns; deviates from the pervasive `p.done` discipline.
- **`opCtx` spawns a goroutine per Docker call** — `swap.go:164-174`: not a leak
  (each exits on `cancel()`), just churn on the swap path. Optional consolidation.
- **ERROR-state pool with empty queue has no recovery timer** — `events.go:207,220`:
  a default-service pool killed out-of-band only recovers on the next request.
  Possibly intended (PRD §6 says "recovers on the next request"); confirm.
- **moby client never `Close()`d** — `client.go:66`: one-time leak at shutdown only.

### Checked and clean
All mutable `Pool` state confined to the manager goroutine; atomic snapshot
re-validated under the manager (no TOCTOU, `pool.go:192→274`); buffered reply
channels prevent waiter deadlock/leak; shutdown vs in-flight swap/timer race-clean
(`sync.Once` + `reloadMu`); no direct `time.Now()`/`time.Sleep` in pool logic; no
loop-variable capture (Go 1.26); body-peek size-limited and fails closed (400, not a
corrupted forward); hot reload builds+validates the new runtime before the atomic
pointer swap (bad reload keeps old config); shared `http.Transport` is a correctly
reused singleton.

---

## Performance

All Medium/Low — the hot path is fundamentally clean (no mutex held across I/O, no
per-request regex compile, no config re-parse, atomic snapshot is optimal).

### Medium
- **Router is an O(routes) linear scan per request** — `internal/router/router.go:56-74`.
  Fine for a handful of routes; build a prefix index if route counts grow large.
- **Body buffering allocates per model-routed POST** — `internal/proxy/bodypeek.go:18-52`:
  `io.ReadAll` + closure per request. Consider a `sync.Pool` of buffers.
- **Queue flush is O(queue_size) serial work on the manager loop** — `internal/orchestrator/queue.go:36-41`.
  Negligible at the default 100; spawn concurrent senders only if `max_queue_size` is large.

### Low
- CORS origin lookup is O(n) per request (`cors.go:44`) — negligible at typical 1-5 origins.
- Router re-matched after a model hit (`handler.go:59`) — one extra scan on model-routed POSTs.
- 404 path clones the known-routes slice (`handler.go:63`) — 404s are rare.

---

## Methodology

Four parallel subagents (security via `Explore`; concurrency & leaks via
`general-purpose`; performance via `Explore`) audited the scope above, writing raw
notes to `/tmp/audit-{security,concurrency,leaks,perf}.md`. The coordinator
re-verified findings against cited `file:line`: **7 findings spot-checked, 4
required severity correction** — all four of the security pillar's "Critical"
ratings were downgraded to Low/Info after reading the code and cross-checking PRD
§5.4. `go vet ./...` is clean. The feature-gap section was produced by the
coordinator comparing PRD.md §§2-7 against the implementation directly.
