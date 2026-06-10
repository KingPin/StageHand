# StageHand Operator Runbook

Symptom → diagnosis → action. This assumes you can reach the admin API. All
admin calls require the `X-Stagehand-Admin-Token` header unless
`STAGEHAND_DISABLE_ADMIN_AUTH=true` is set.

First stop for almost everything is the status endpoint:

```sh
curl -s -H "X-Stagehand-Admin-Token: $TOKEN" \
  https://ai.example.com/stagehand/status | jq
```

```json
{
  "status": "healthy",
  "version": "1.2.3",
  "vram_pools": {
    "gpu_0_vram": {
      "state": "ACTIVE",
      "active_service": "comfyui",
      "seconds_until_cooldown": 240,
      "queued_requests_count": 0
    }
  },
  "always_on_services": { "embeddings-cpu": "healthy" }
}
```

Pool `state` is one of `IDLE` (cold, nothing running), `ACTIVE` (one service
healthy and serving), `SWAPPING` (a stop/start/health transition in flight), or
`ERROR` (external state inconsistent — e.g. the active container died
out-of-band). Admin verbs:

| Action | Call |
|---|---|
| Pre-warm / force a swap | `POST /stagehand/swap/{service}` |
| Force a pool cold (stop everything) | `POST /stagehand/pool/{pool}/stop` |
| Hot-reload config | `POST /stagehand/reload` (or `SIGHUP`) |
| Liveness (no token) | `GET /stagehand/healthz` |

---

## Pool stuck in `SWAPPING`

**Symptom.** `state: "SWAPPING"` persists, requests to that pool queue up
(`queued_requests_count` climbing), clients wait.

**Diagnose.**
1. A swap legitimately takes up to the target's `startup_timeout_seconds` (the
   example config uses 180–240s). Give it that long before treating it as stuck.
2. Check the target container directly: `docker logs <container>` and
   `docker ps`. A model that's slow to load (large weights, cold disk cache) is
   the usual cause.
3. If the container is up but its `health_path` never returns 2xx, the health
   probe will never pass and the swap will end in a `504` startup timeout, then
   the pool returns to `IDLE`.

**Act.**
- Wait out a legitimately slow load, or raise that service's
  `startup_timeout_seconds` and `POST /stagehand/reload`.
- Fix an unhealthy backend (wrong `health_path`, port, or a crashing model),
  then force a clean retry: `POST /stagehand/swap/{service}`.
- To abandon the swap entirely and free the GPU: `POST /stagehand/pool/{pool}/stop`.

---

## Pool in `ERROR`

**Symptom.** `state: "ERROR"`. StageHand observed the active container die
out-of-band (someone ran `docker stop`, an OOM kill, a crash, or a `restart`
policy bouncing it).

**Diagnose.** `docker events` / `docker ps -a` and the container's logs around
the death. Check for OOM (`dmesg`, exit code 137) or a conflicting external
`docker stop`.

**Act.** `ERROR` is self-healing: the next request to the pool triggers a fresh
swap and recovery. To recover immediately without waiting for traffic,
`POST /stagehand/swap/{service}` to force the desired service active. If a human
or another tool keeps stopping the container, that's the root cause to fix —
StageHand expects to be the only thing driving these containers' lifecycle.

---

## "GPU is busy but status says `IDLE`"

**Symptom.** `nvidia-smi` shows a container holding VRAM, but `/stagehand/status`
reports the pool `IDLE` (or active on a *different* service). New traffic that
triggers a swap may then OOM because the "idle" GPU isn't actually free.

**Cause.** This was the **M1 swap-worker race** (audit M1), now **fixed**: an
admin stop or external swap-abort issued mid-swap could orphan the swap worker,
which then started its target *after* the stop swept the pool — leaving a
container running while the manager believed the pool was cold.

**Act.**
1. Confirm you're on a build that includes the M1 fix (commit "cancel superseded
   swap worker to preserve mutual exclusion" or later; check `GET /stagehand/healthz`
   `version`).
2. To recover now: `POST /stagehand/swap/{service}` re-sweeps every pool sibling
   (stopping any stray running container) before starting the target, restoring
   mutual exclusion. `POST /stagehand/pool/{pool}/stop` likewise sweeps the pool
   cold.
3. If you can still reproduce this on a fixed build, capture
   `/stagehand/status` + `docker ps` + StageHand logs and file it — the
   invariant should hold.

---

## Clients getting error status codes

| Status | Meaning | Action |
|---|---|---|
| `401` | Missing/invalid admin or proxy token | Send the right header (`X-Stagehand-Admin-Token` for `/stagehand/*`, `X-Stagehand-Token` for proxy traffic if `proxy_token` is set). Confirm the token matches config; remember it's hot-reloadable and may have changed on a `SIGHUP`. The auto-generated token is logged once at boot. |
| `413` | Request body exceeded `max_request_bytes` | Raise `max_request_bytes` above your largest legitimate request, or have the client shrink the payload. |
| `429` | Service queue full (`max_queue_size`) | Backend can't keep up or swaps are thrashing. Raise `max_queue_size`, raise `grace_period_seconds` to reduce swap churn, or scale the workload down. `Retry-After: 5` is set. |
| `502` | Docker stop/start failed | Check the Docker daemon and the container (`docker logs`). Also the one-off swap-retry case below. |
| `503` | StageHand is shutting down, or the pool was force-stopped / its config changed on reload | Expected during shutdown and `pool/{pool}/stop`. If unexpected, check whether a reload removed/changed the pool. |
| `504` | Service didn't become healthy within `startup_timeout_seconds` | Backend too slow or unhealthy — see "Pool stuck in SWAPPING". Raise the timeout or fix the health endpoint. |

### One-off `502` right after a swap

A single `502` on the *first* request after a container swap, for a request with
a body larger than 1 MiB, is the known body-replay caveat: an oversized,
unbuffered body can't be replayed onto the freshly-swapped backend when the
first attempt hits a stale pooled connection. **Fix:** set `max_request_bytes`
above your largest request so all bodies are buffered and replayable (see
[`deployment.md`](deployment.md#request-body-size--the-swap-retry-caveat)).
Clients should retry idempotent requests.

---

## Docker daemon down or flapping

**At startup:** StageHand **hard-fails by design** — it pings Docker and verifies
every configured container exists before serving. A bad socket path, a stopped
daemon, or a missing container exits the process with a loud error. Fix the
daemon/socket/config and restart.

**At runtime:** the Docker events watcher backs off and resubscribes if the
event stream drops, so a daemon blip doesn't permanently blind StageHand to
container lifecycle changes. While the daemon is down, swaps will fail with
`502`; they recover once it's back.

---

## Admin API unreachable / `401` on every call

- Confirm the token: it comes from `server.auth.admin_token`, or the
  random one logged at boot (`generated a random admin token`) if you didn't set
  one. A `SIGHUP` reload can change it.
- If you intended auth to be off, confirm `STAGEHAND_DISABLE_ADMIN_AUTH=true` is
  actually set (a non-boolean value is ignored and auth stays **on**, with a
  warning logged). The disable banner prints at the top of the console on every
  startup when it's active.
- `/stagehand/healthz` needs **no** token — use it to confirm the process is up
  before debugging auth.

---

## Hot reload didn't take effect

- A reload (`POST /stagehand/reload` or `SIGHUP`) that fails validation is
  **rejected and the old config is kept** — check the logs for the validation
  error. The control plane stays up on the previous good config.
- **Listener address (`host`/`port`) and `docker_socket_path` changes require a
  full restart** — they're not hot-reloadable.
- Pools whose config is unchanged keep their queues and active container across
  a reload; changed or removed pools flush their queues with `503`.

---

## Graceful shutdown expectations

On `SIGINT`/`SIGTERM` StageHand:

1. stops accepting new connections,
2. releases queued requests with a clean `503`,
3. closes tunneled WebSocket connections,
4. drains in-flight responses, bounded to **15s**, then forces close,
5. **leaves running containers running** — models are expensive to reload.

So after a StageHand restart you may find a backend already `ACTIVE`; that's
expected. Note that StageHand stops containers via the Docker API, which Docker
honors even under `restart: always` / `unless-stopped`.
