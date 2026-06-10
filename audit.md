# Codebase Audit & Production-Readiness Review — 2026-06-08

**Scope:** Full `internal/` + `cmd/` (~7,700 LOC Go). Three audit pillars
(security/OWASP, correctness/concurrency/leaks, performance) plus a
**production-readiness gap analysis** of `PRD.md`/operability vs. the
implementation.
**Method:** Four parallel research subagents against the current tree; five
findings re-verified by the coordinator against the cited `file:line`
(0 required correction).
**Status:** Findings only — no code changed in this pass.

> **This supersedes the 2026-06-03 beta-readiness review.** Every one of that
> review's Top-5 blockers has since been fixed and the fixes were re-verified
> here (see below). What remains is one transient-correctness bug, a handful of
> micro-optimizations, and **operational hardening for production** (HA story,
> TLS posture, metrics, release engineering, runbook).

## Executive Summary

- **Critical: 0**
- **High (code): 0**
- **Medium (code): 1** (M1 **fixed 2026-06-10**; M2 perf remains)
- **Low / Info (code): 6**
- **Production-readiness gaps: 3 High · 5 Medium · ~8 Low** (operational, not code defects)

The orchestrator actor model remains **sound** — `go vet` clean, no
actor-model violations, no TOCTOU on the atomic snapshot, no goroutine/channel
deadlocks. The one new code finding (M1) was a transient mutual-exclusion
violation under a narrow admin-stop-during-swap race; it self-healed and is now
**fixed** (superseded swap workers are cancelled before they can start a target).
The real
distance to **production** (vs. beta) is operational: there is no
multi-replica/HA story, no metrics endpoint, plaintext-only serving with an
undocumented TLS-termination requirement, and no release/runbook engineering.

**Top 5 by ROI:**
1. ~~AdminStop/swap-abort during SWAPPING can leave a container running while the pool reports IDLE (M1)~~ — **FIXED 2026-06-10** (`internal/orchestrator/pool.go`, `swap.go`; regression test `swap_supersede_test.go`)
2. Document + guard the single-instance assumption (two replicas on one Docker socket race swaps) — **DONE** (`docs/deployment.md`, README, PRD §1) (P-H1)
3. Document the TLS-termination deployment requirement — **DONE** (`docs/deployment.md` reverse-proxy guides) (P-H3)
4. Add a Prometheus `/metrics` endpoint (swap count/duration, queue depth, pool state, error rates) — `absent` (P-M1)
5. `sync.Pool` the per-request body-peek buffer — `internal/proxy/bodypeek.go:52` (M2)

---

## Prior (2026-06-03) Top-5 blockers — all VERIFIED FIXED

| Prior blocker | Status | Evidence (re-verified) |
|---|---|---|
| Unauthenticated liveness endpoint | **DONE** | `handler.go:30-33` short-circuits `/stagehand/healthz` before the admin gate; `healthz_test.go` |
| Docker-events goroutine/conn leak on resubscribe | **DONE** | `events.go:131,152` per-subscription `context.WithCancel` + `cancel()` before resubscribe; `events_leak_test.go` |
| Large request bodies lose retry-after-swap | **DONE (as designed)** | `bodypeek.go`: capped bodies are fully buffered with `GetBody`; oversized path documented (see L2) |
| No panic recovery in WS tunnel / request goroutines | **DONE** | `server.go` `recoverPanics` (re-panics `ErrAbortHandler`, logs+500, nested in access log); `websocket.go` recovers both relay goroutines; `recover_test.go` |
| No CI | **DONE (minimal)** | `.github/workflows/ci.yml` — build + vet + `test -race` on push/PR (hardening gaps: see P-M2) |

Also closed since 2026-06-03: CORS `"*"` warning (`config/validate.go`), per-request
access logging (`access_log.go`), request body-size cap `max_request_bytes`
(`handler.go`, `bodypeek.go`), Docker `HEALTHCHECK` + `-healthcheck` flag
(`Dockerfile`, `main.go`), version ldflags stamping (`Dockerfile`, `version.go`),
clock injection into the events backoff (`events.go`).

---

## Correctness (concurrency / leaks)

### Medium

#### M1 — AdminStop / swap-abort during SWAPPING can leave a container running while the pool reports IDLE — `internal/orchestrator/pool.go:484`, `internal/orchestrator/swap.go:30,53,55` — **FIXED 2026-06-10**
**What:** `handleAdminStop` in the `StateSwapping` case does `p.opSeq++` (pool.go:484)
to orphan the in-flight swap, then `startStopAll()`. But `opSeq++` only orphans the
worker's *terminal report* (the manager drops stale `swapMsg`s) — it does **not**
cancel the running `doSwap` goroutine, which takes `op` (swap.go:30) but never
re-checks it. The orphaned worker can still proceed past its sweep loop to
`p.docker.Start(target)` (swap.go:55) **after** `doStopAll` has already swept every
member. Worse, `doSwap` pre-registered `p.ops.expect(target,"start")` (swap.go:53),
so when the reconciler sees the resulting `start` event it treats it as an expected
self-op and does **not** force-stop it. The same shape is reachable via the
external-event swap-abort path (`events.go` swap-abort).
**Why it matters:** Net result is a container left running while the pool reports
`IDLE` — a transient violation of the pool's mutual-exclusion (VRAM) invariant, the
single most load-bearing guarantee in the system. An operator who issues "stop the
pool" sees IDLE but the GPU is still occupied. The state machine does **not** wedge
(the stale message is dropped) and it **self-heals** on the next swap (which sweeps
all siblings first), hence Medium not High. Trigger is narrow: an admin-stop (or
out-of-band swap-abort) landing in the 10–30s SWAPPING window, with the orphaned
worker reaching `docker.Start` after `doStopAll`'s sweep.
**Fix:** Tie worker cancellation to `opSeq`, not just shutdown — pass a
per-swap `context.Context` derived from the op, cancel it when `opSeq` advances, and
have `doSwap` check cancellation immediately before `docker.Start`. Alternatively,
have `doStopAll` run after confirming no orphaned `doSwap` is mid-flight, or
re-sweep on the `poolStopped` report.
**Resolution (FIXED, commit "cancel superseded swap worker to preserve mutual
exclusion"):** A manager-owned `swapCancel context.CancelFunc` now sits beside
`opSeq` (pool.go). `supersedeWorker()` cancels the in-flight worker *before*
bumping `opSeq`; `startSwap`/`startStopAll` install a fresh per-worker context and
`handleSwap` clears it on a terminal report. `doSwap` re-checks `ctx.Err()`
immediately before `ops.expect(start)` + `docker.Start` (so a superseded worker
never registers the expectation or starts the target), and `doStopAll` re-checks
before each stop (closing the symmetric reverse race where a cold-stop clobbers a
superseding swap's freshly started target). A deterministic regression test
(`swap_supersede_test.go`, parks the worker mid-sweep via a new
`FakeClient.SetBeforeStop` hook) fails against the pre-fix code and passes after.
Residual is Info-level: a cancel landing between the check and the daemon applying
the start is a narrow window that self-heals on the next swap's sweep.

### Low / Info

- **L1 — Health probe ignores `p.done`** — `swap.go:65`, `health.go:31`: `healthOK`
  uses `context.Background()`, bounded only by the 2s client timeout, not `p.done`;
  at most one trailing 2s GET on shutdown. Deviates from the pervasive `p.done`
  discipline.
- **L2 — Oversized body not retryable after a swap when uncapped** — `bodypeek.go:58-69`:
  the >limit branch sets `req.Body = replayBody{}` with **no `GetBody`**, so a stale
  pooled connection right after a swap yields a 502 instead of a transparent retry.
  Only reachable with the default `max_request_bytes=0` (uncapped) and a >1 MiB body;
  the code comment documents it. Setting a cap eliminates it. Low.
- **L3 — Token compare is constant-time only across equal lengths** — `auth.go:24`:
  `subtle.ConstantTimeCompare` returns 0 immediately on a length mismatch, so the
  function's "constant-time" doc-comment is technically inaccurate. Negligible for
  fixed-length 256-bit tokens; either pre-hash both sides or soften the comment.
- **Info — Grace-zero swap-out-from-under just-flushed waiters** — `pool.go` admit
  path: a by-design hint-race that only manifests with `grace=0`.
- **Info — `AfterFunc` timers not `Stop()`d** — `pool.go`: not a leak; neutralized by
  the epoch guard, and cooldown self-reschedules.
- **Info — moby client never `Close()`d** — `dockerctl/client.go`: one-time, at
  process shutdown only.

### Checked and clean
All three 2026-06-03 fixes re-verified correct (events leak, body retry, panic
recovery). All mutable `Pool` state confined to the manager goroutine; atomic
snapshot re-validated under the manager (no TOCTOU); buffered reply channels prevent
waiter deadlock/leak; shutdown vs in-flight swap/timer/reload race-clean
(`sync.Once` + `reloadMu`); no direct `time.Now()`/`time.Sleep` in pool logic; no
loop-variable capture (Go 1.26); hot reload builds+validates the new runtime before
the atomic pointer swap; shared `http.Transport` is a correctly reused singleton; no
process-crashing panics on the request or tunnel paths.

---

## Security (OWASP)

No Critical / High / Medium issues. The security pillar found the new code
(auth.go, access_log.go, cors.go, handler.go, bodypeek.go, proxy.go, admin.go,
websocket.go, main.go, config/validate.go) clean against the OWASP Top 10, with no
regressions from the recent changes.

### Low / Info

- **CORS `"*"` reflection** — `cors.go`: when `cors_allowed_origins: ["*"]`, the
  caller's Origin is echoed. Now **warned** at config load (`validate.go`).
  `Access-Control-Allow-Credentials` is never set and tokens are custom headers (not
  cookies), so no authenticated-request forgery; residual risk is only cross-origin
  readability. Prefer an explicit allowlist in production.
- **Error responses enumerate internal topology** — `handler.go`, `errors.go`: a 404
  lists known routes; orchestrator/Docker errors surface container names to the proxy
  caller. Minor info disclosure to unauthenticated proxy clients. Gate verbose route
  listing behind a debug flag; keep client-facing errors generic.
- **Info — admin auth default-open escape hatch** — `main.go`:
  `STAGEHAND_DISABLE_ADMIN_AUTH` is a documented PRD §5.4 env-only hatch, checked at
  startup with a prominent stderr banner, fail-safe (unparseable keeps auth on).
- **Info — auto-generated admin token logged** — `main.go`: PRD §5.4 behavior, fires
  only when no token is configured, logs the freshly minted random token so the
  operator can retrieve it. Hardening option: write to a file / stdout instead of the
  slog stream.

### Checked and clean
Constant-time token compare + header stripping before forward (`auth.go`,
`handler.go:57-58`); `crypto/rand` 256-bit fallback token (`main.go`); no SQL /
command / template injection surface; backend targets are config-fixed (no
client-controlled SSRF; `websocket.go` `dialTarget` takes only a pre-configured
`*url.URL`); body peek bounded with int64-overflow guard (`bodypeek.go:49`); body-cap
enforced via immediate 413 on declared Content-Length + `MaxBytesReader` for chunked;
healthz leaks only the version string; hot-reload endpoint authenticated and
validates containers before applying.

---

## Performance

Hot path is fundamentally clean (shared transport singleton, lock-free atomic
config snapshot, no mutex held across I/O, no per-request regex compile or config
re-parse). All findings are tune-if-profiling-shows-it.

### Medium

- **M2 — Per-request body buffering allocates on the peek path** — `bodypeek.go:52`:
  `io.ReadAll(io.LimitReader(...))` allocates a `[]byte` proportional to body size on
  every model-routed POST. At high concurrency with large bodies this dominates GC
  cost on the peek path. Fix: a `sync.Pool` of buffers.

### Low

- Double route-match on model-routed POSTs — `handler.go` re-matches after the model
  peek (~100–200ns, 2× the O(routes) scan).
- O(n) CORS origin lookup per request — `cors.go` `slices.Contains`; pre-compute a
  `map[string]struct{}` at config load. Negligible at typical 1–5 origins.
- Per-request `statusRecorder` allocation — `access_log.go`; pool only if profiling
  flags it.
- Per-request reply-channel allocation in `Admit` — `pool.go`; optional.
- O(n) queue removal on cancellation — `queue.go`; fine at default depth 100, watch
  if `max_queue_size` is set large.
- Router is an O(routes) linear scan — `router.go`; build a prefix index only if
  route counts grow large.

---

## Production-Readiness Gaps

These are **not code defects** — they are operability/deployment gaps that stand
between the (functionally complete) implementation and a safe production rollout.

### High (block a safe production deploy)

- **P-H1 — Multi-replica / HA is unsafe and undocumented** — **DONE 2026-06-10.**
  Single-instance-only is now documented as a hard requirement in
  `docs/deployment.md`, `README.md` (Deployment), and `PRD.md` §1, with the
  supervisor-restart-not-replicas guidance. (A Docker-label lease / advisory lock for
  active-passive failover remains a possible longer-term enhancement, out of current
  scope.)
- **P-H2 — No operator runbook for stuck swaps / ERROR-state pools** — **DONE
  2026-06-10.** `docs/runbook.md` is a symptom→diagnosis→action runbook keyed off the
  `/status` states (stuck `SWAPPING`, `ERROR` recovery, "GPU busy but IDLE", the
  4xx/5xx codes, daemon outages, reload gotchas, shutdown), linked from README.
- **P-H3 — Plaintext-only serving; TLS-termination requirement undocumented** —
  **DONE 2026-06-10.** `docs/deployment.md` states the TLS-terminating-proxy
  requirement and provides working nginx/Caddy/Traefik/cloudflared examples; README
  Deployment carries the one-liner. (Optional native `server.tls` remains out of
  scope per user direction.)

### Medium (strongly recommended for production)

- **P-M1 — No Prometheus `/metrics`** — `absent`. The only signals are the authed
  status JSON and slog lines. For an ops-facing proxy, scrapeable series (swap
  count/duration, queue depth, per-pool state, 429/502/503/504 counters, in-flight
  requests) are table stakes for alerting.
- **P-M2 — CI lacks lint, vuln, and e2e coverage** — **PARTIAL 2026-06-10.**
  `govulncheck` (pinned `v1.3.0`) now runs after vet on every push/PR (`ci.yml`).
  `golangci-lint`/`staticcheck`/`gosec` and the gated e2e-on-a-schedule remain
  unaddressed.
- **P-M3 — No release / image-publishing pipeline** — **DONE (disabled) 2026-06-10.**
  `.github/workflows/release.yml` publishes a multi-arch (amd64+arm64) image to
  `ghcr.io/kingpin/stagehand` on `v*` tags, reusing the `Dockerfile` `ARG VERSION`
  ldflags stamping. Shipped guarded `if: false` — enabled when the repo goes public.
  SBOM / build provenance remain a follow-up.
- **P-M4 — Security-posture docs missing** — **DONE 2026-06-10.** `docs/deployment.md`
  documents the token model, the TLS-termination requirement, the
  `STAGEHAND_DISABLE_ADMIN_AUTH` hatch, and secret-store guidance. (The root +
  `docker.sock` trust model could be expanded further.)
- **P-M5 — Docker-daemon-flap runtime behavior undocumented** — **DONE 2026-06-10.**
  `docs/runbook.md` documents both the startup hard-fail and the runtime events-watcher
  backoff/resubscribe behavior and pool behavior while the daemon is down.

### Low (post-launch is fine)

- Access log omits matched-service and swap-triggered fields (`access_log.go`).
- No distributed tracing.
- `Dockerfile` runs with broad privileges (root + `docker.sock`) with no
  resource-limit guidance.
- Non-release builds report `1.0.0-dev` (`version.go`); CI doesn't stamp.
- e2e suite is a single test.
- No proactive ERROR-state recovery timer — a default-service pool killed out-of-band
  recovers only on the next request (`events.go`; likely by-design per PRD §6).
- No request-lifetime ceiling / slow-loris bound (`run.go`; deliberate tradeoff for
  long SSE / cold starts — note as a known limitation).
- No rate limiting on the admin or proxy planes (PRD doesn't require it).

### PRD functional surface — confirmed COMPLETE
Routing (path/header/model, `bodypeek.go`), bounded FIFO queues + 429/503/502/504
semantics, pool state machine (IDLE/ACTIVE/SWAPPING/ERROR), grace period & idle
cooldown, Docker-events reconciliation, hot reload (SIGHUP + endpoint, old config
kept on failure), graceful shutdown with 15s drain (`run.go`), admin API
(status/swap/pool-stop/reload), constant-time auth with header stripping, WebSocket
tunneling, CORS preflight, request body cap, healthz probe, access logging. **The
PRD's functional surface is essentially complete** — the gaps above are operational,
not functional.

---

## Methodology

Four parallel subagents audited the current tree (2026-06-08): security via
`Explore`, correctness/concurrency/leaks via `general-purpose`, performance via
`Explore`, and a production-readiness gap analysis via `general-purpose`. Raw notes
in `/tmp/audit-{security,correctness,perf,readiness}.md`. The coordinator
re-verified **5 findings** against cited `file:line` — the M1 mutual-exclusion race
(`pool.go:484` + `swap.go:30,53,55`), the events-leak fix (`events.go:129-152`), the
oversized-body path (`bodypeek.go:58-69`), the token compare (`auth.go:24`), and the
healthz short-circuit (`handler.go:30-33`) — with **0 corrections required**. All
five prior Top-5 blockers were verified fixed against their commits. `go vet ./...`
is clean.
