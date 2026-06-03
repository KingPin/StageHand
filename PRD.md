# Production PRD: StageHand (Dynamic VRAM Multiplexer & Reverse Proxy)

This document is the complete, self-contained Product Requirement Document (PRD) and
Technical Specification for **StageHand**, a configuration-driven VRAM Multiplexer and
Reverse Proxy written in Go.

> **Revision 2** — amends the original draft with decisions made during design review:
> official Docker SDK (API version negotiated, config pin dropped), shared Docker
> network topology, JSON body `model`-field routing, bounded request queues,
> per-service startup timeouts, a concrete pool state enum, a Docker events watcher,
> an admin API, and hot config reload.

## 1. Executive Summary & Core Concept

When running multiple open-source AI engines (ComfyUI, Automatic1111, Llama.cpp,
Ollama, Whisper, vLLM, TTS, etc.) on consumer hardware with limited GPU memory
(e.g., 8GB–16GB VRAM), running them concurrently causes Out-Of-Memory (OOM) crashes.

StageHand is a lightweight, high-performance, containerized asynchronous reverse
proxy that acts as a **VRAM Multiplexer**. By reading a declarative configuration
file (`config.yaml`), StageHand maps incoming request routes to specific containers,
groups those containers into mutually exclusive **VRAM pools**, automatically
stops/starts them on-demand via the host's Docker socket, and queues incoming
traffic during transitions so that no client requests are lost.

### Deployment Topology

StageHand runs as a container joined to a **shared user-defined Docker network**
with all backend containers. Service `target_url`s therefore use container names
as hosts (e.g., `http://comfyui-stable:8188`). The host's Docker socket is mounted
into the StageHand container for lifecycle control.

## 2. Declarative Configuration Spec (`config.yaml`)

StageHand parses `config.yaml` on startup (and on hot reload, §7). Users may declare
arbitrary numbers of LLMs, speech-to-text engines, image-generation interfaces, and
embedding servers.

```yaml
# Global Configuration
server:
  host: "0.0.0.0"
  port: 8080
  docker_socket_path: "/var/run/docker.sock"
  cors_allowed_origins: ["*"]     # Allowed origins for web-UI integrations
  max_queue_size: 100             # Default per-service queue bound (override per service)

# Mutual Exclusion Pools
vram_pools:
  gpu_0_vram:
    grace_period_seconds: 60      # Min run-time of any container before it may be swapped out
    cooldown_seconds: 300         # Idle time before stopping active containers (0 disables)
    default_service: null         # null = spin down ALL containers when idle (Cold Pool)

# Declared Services (AI Backends)
services:
  comfyui:
    container_name: "comfyui-stable"
    target_url: "http://comfyui-stable:8188"   # Container-name host (shared network)
    health_path: "/"
    startup_timeout_seconds: 180               # Per-service cold-start budget (default 180)
    vram_pool: "gpu_0_vram"                    # Belongs to the dynamic VRAM-bound GPU pool

  llama-moe:
    container_name: "llama-cpp-moe"
    target_url: "http://llama-cpp-moe:8081"
    health_path: "/health"
    startup_timeout_seconds: 240
    max_queue_size: 50                         # Optional per-service queue override
    vram_pool: "gpu_0_vram"                    # Mutually exclusive with comfyui

  embeddings-cpu:
    container_name: "llama-embeddings"
    target_url: "http://llama-embeddings:8082"
    health_path: "/health"
    vram_pool: null                            # Always-On CPU Service (no VRAM limitation)

  reranker-cpu:
    container_name: "llama-reranker"
    target_url: "http://llama-reranker:8083"
    health_path: "/health"
    vram_pool: null                            # Always-On CPU Service

  whisper-stt:
    container_name: "faster-whisper"
    target_url: "http://faster-whisper:8084"
    health_path: "/v1/models"
    vram_pool: null                            # Always-On CPU Service

# HTTP Path, Header, and Model Routing Rules (ordered, first match wins)
routes:
  - path_prefix: "/v1/embeddings"
    service: "embeddings-cpu"

  - path_prefix: "/v1/rerank"
    service: "reranker-cpu"

  - path_prefix: "/v1/audio"
    service: "whisper-stt"

  # Body-based model routing: maps the JSON body's "model" field to a service.
  # "service" is the fallback when no model entry matches.
  - path_prefix: "/v1/chat/completions"
    models:
      "qwen-moe": "llama-moe"
      "flux": "comfyui"
    service: "llama-moe"

  # Optional header-matching is still supported
  - path_prefix: "/v1/images"
    service: "comfyui"
    headers:
      X-Use-Model: "comfy"

  # WebSocket routing rules
  - path_prefix: "/ws"
    service: "comfyui"
```

### 2.1 Routing Semantics

- Routes are evaluated **in declared order; first match wins**.
- A route with a `headers` map matches only when every listed header matches; on
  mismatch the request **falls through** to later routes.
- A route with a `models` map (for `POST` requests with a JSON content type) causes
  StageHand to peek the request body (buffered, capped at 1 MiB) and extract the
  top-level `"model"` field. A matching entry selects that service; otherwise the
  route's `service` is the fallback. The buffered body is replayed to the backend
  byte-for-byte. Only the *request* body is ever buffered — never the response.
  This makes StageHand a drop-in OpenAI-compatible endpoint for clients (OpenWebUI,
  LibreChat, `openai` SDKs) that select backends via the `model` field.
- If no route matches, StageHand returns `404` with a JSON error listing the known
  routes.

### 2.2 Validation

Configuration is validated at load: all route services and model targets must
exist; pool references must exist; a pool's `default_service` must belong to that
pool; `target_url` must be an absolute http(s) URL; duplicate `container_name`s
are rejected; timeouts and queue sizes must be positive. At boot, StageHand pings
the Docker daemon and verifies every configured `container_name` exists on the
host — failing loudly with the list of missing containers otherwise.

## 3. Dynamic State Machine & VRAM Pool Orchestration

StageHand maintains an internal, thread-safe, in-memory state machine representing
the status of all configured services and VRAM pools.

### 3.1 Pool States

Each pool is in exactly one state:

| State | Meaning |
|---|---|
| `IDLE` | No container in the pool is running (0MB VRAM occupied — "Cold Pool") |
| `ACTIVE` | Exactly one service is running and healthy |
| `SWAPPING` | A stop/start/health-check transition is in flight |
| `ERROR` | The last Docker operation failed or external state is inconsistent; recovers on the next request or reconciliation |

### 3.2 VRAM Pool State Rules

For each defined pool in `vram_pools`:

- **Mutex Lock:** Only ONE service associated with a given `vram_pool` may have a
  Docker state of `running` at any moment.
- **Swap sequence:** If a request is routed to a target service in pool X and that
  service is currently stopped:
  1. The pool transitions to `SWAPPING`.
  2. **Fast Startup Path:** if the pool is `IDLE`, skip the stop steps entirely and
     proceed immediately to the start sequence.
  3. Otherwise stop the active service gracefully (10s timeout before Docker
     hard-kills) and wait for confirmation of `exited`.
  4. Start the target service.
  5. Poll the target's configured `health_path` (or TCP port check) until a
     `200 OK` is returned, bounded by the service's `startup_timeout_seconds`.
  6. Transition to `ACTIVE` and flush held requests in FIFO order.

### 3.3 Thread-Safe Queueing & Connection Buffering

To prevent losing client requests during a container transition (10–30 seconds):

- **Connection Holding:** incoming requests enter a **bounded FIFO queue**
  (per-service; `max_queue_size`, default 100) and the client TCP socket is kept
  alive. When the queue is full, StageHand responds `429 Too Many Requests` with
  a `Retry-After` header and a JSON error body.
- **Startup budget:** each service's `startup_timeout_seconds` (default 180) bounds
  the cold start; queued clients receive `504 Gateway Timeout` if it is exceeded.
  Server timeouts must accommodate the largest configured startup budget.
- **Transition Blocking:** while a transition is in progress, subsequent requests
  for any service in that pool are held in their service's queue.
- **Client cancellation:** a client that disconnects while queued is removed from
  the queue cleanly.

### 3.4 Dynamic Cooldown & Idle Handlers (the "Cold Pool" state)

If `cooldown_seconds > 0`:

- An internal countdown timer monitors activity on the pool. Every request to any
  service in the pool resets it. The timer never fires mid-swap.
- When the timer expires:
  - **Option A (`default_service` set):** if the running container is not the
    default service, transition to the default service.
  - **Option B (`default_service: null`):** stop the active service and transition
    the pool to `IDLE` (0MB VRAM occupied).

### 3.5 Anti-Thrashing (Grace Period)

If service A started less than `grace_period_seconds` ago and a request for
service B arrives, the request is queued and the swap is deferred until A's grace
period elapses — preventing GPU thrash.

## 4. API Classification & Request Proxying

The reverse proxy performs robust HTTP forwarding, supporting token streaming and
WebSockets.

```
       Incoming HTTP/WS Unified Proxy Request (Port 8080)
                             |
         +-------------------+-------------------+
         |                                       |
    [ Match Path/Headers/Model ]        [ Match WebSockets ]
         |                                       |
   Find matching route.                   Handle standard upgrade
   Determine target service.              and tunnel frames to
         |                                the running backend.
   Check VRAM Pool state.
         |
   +-----+-----+
   |           |
[ Healthy ]  [ Stopped/Transitioning/Idle ]
   v           v
Forward      Queue request -> Trigger VRAM swap -> Flush queue on ready
```

### 4.1 Chunked Streaming Protocol

AI completions (`/v1/chat/completions`) use Server-Sent Events (SSE):

- **No Buffering:** StageHand must not read the full response body into memory.
- **Chunk Propagation:** raw bytes stream iteratively from the backend to the
  client as they arrive.
- **Header Copying:** `Content-Type: text/event-stream`, `Cache-Control: no-cache`,
  and `Transfer-Encoding: chunked` are preserved and forwarded unmodified.

### 4.2 Transparent WebSocket Forwarding

Web interfaces like ComfyUI and Automatic1111 use WebSockets for real-time
progress, queues, and execution status:

- **Handshake Upgrade:** when an incoming connection carries
  `Upgrade: websocket`, StageHand negotiates a raw tunnel.
- **Duplex Pipeline:** an active TCP link to the target's port pipes packets
  bi-directionally between the client socket and the backend container.
- WebSocket connects to a pooled service participate in pool orchestration like
  any HTTP request (they can trigger and wait on a swap).
- **Known limitation — tunnels do not pin containers:** an established tunnel
  does not prevent its backend from being swapped out later (grace-period swap,
  cooldown stop, or admin action). When that happens the tunnel drops without a
  WebSocket close frame; clients are expected to reconnect (ComfyUI and A1111
  frontends do this automatically), which re-enters pool orchestration and can
  swap the service back in.

## 5. System Health, Observability, & CORS

StageHand reserves the `/stagehand/*` namespace for native API calls.

### 5.1 Self-Health API (`GET /stagehand/status`)

```json
{
  "status": "healthy",
  "version": "1.0.0",
  "vram_pools": {
    "gpu_0_vram": {
      "state": "ACTIVE",
      "active_service": "llama-moe",
      "seconds_until_cooldown": 245,
      "queued_requests_count": 0
    }
  },
  "always_on_services": {
    "embeddings-cpu": "healthy",
    "reranker-cpu": "healthy"
  }
}
```

`state` is one of `IDLE`, `ACTIVE`, `SWAPPING`, `ERROR` (§3.1).

### 5.2 Admin API

- `POST /stagehand/swap/{service}` — manually pre-warm/force a swap to the given
  service (bypasses the grace period; chains safely behind an in-flight swap).
- `POST /stagehand/pool/{pool}/stop` — force a pool to `IDLE`, flushing its queues
  with `503`.
- `POST /stagehand/reload` — hot config reload (§7).

### 5.3 Seamless CORS Preflight Handling

Browser frontends run on separate origins (e.g., `http://localhost:3000`):

- On `OPTIONS`, respond `200 OK` immediately with:
  - `Access-Control-Allow-Origin: <matched origin from cors_allowed_origins>`
  - `Access-Control-Allow-Headers: <echo of Access-Control-Request-Headers>`
  - `Access-Control-Allow-Methods: GET, POST, OPTIONS, PUT, DELETE`

### 5.4 Authentication

The admin API (§5.1, §5.2) and the proxy share one listener, so the control
plane must be authenticated. Tokens come from `server.auth` and are compared in
constant time; both checks run after CORS preflight, so `OPTIONS` is never
gated.

- **Admin auth — on by default.** Every `/stagehand/*` request must carry
  `server.auth.admin_token` in the `X-Stagehand-Admin-Token` header; otherwise
  `401`. If `admin_token` is omitted, StageHand generates a random token at boot
  and logs it once. The escape hatch is the `STAGEHAND_DISABLE_ADMIN_AUTH`
  environment variable: when truthy it disables admin auth entirely, and a
  warning banner is printed at the top of the console on **every** startup. The
  env switch is read once at boot and is not affected by hot reload.
- **Proxy auth — optional.** When `server.auth.proxy_token` is set, every
  non-admin request must carry it in the `X-Stagehand-Token` header; otherwise
  `401`. This header is stripped before forwarding so it never reaches a
  backend. `Authorization` is left untouched for pass-through to backends.

`admin_token`/`proxy_token` are hot-reloadable (§7); the disable switch and the
auto-generated fallback token are fixed for the process lifetime.

## 6. Docker Lifecycle Guardrails & Edge Cases

- **Docker SDK:** StageHand uses the official Docker Go SDK with automatic API
  version negotiation (no version pin in config).
- **Docker Events Watcher:** StageHand subscribes to the Docker events stream.
  Externally-initiated container changes (a human running `docker stop`/`start`)
  reconcile the state machine: an active container dying externally moves the pool
  to `ERROR` and flushes its queue with `502`; an unauthorized container starting
  in a pool is stopped to preserve mutual exclusion. Self-initiated operations are
  distinguished from external ones internally and do not trigger reconciliation.
- **Conflict with Docker Restart Policies:** when StageHand stops a container via
  the API, Docker honors the explicit stop and will not auto-restart it (including
  under `restart: always` / `unless-stopped`).
- **Graceful stops:** stop commands always use a graceful timeout (10s) before
  Docker hard-kills, preventing database/model file corruption.
- **Docker Failure Recovery:** if a stop/start fails (or the container is missing),
  StageHand flushes that service's queue with `502 Bad Gateway` and a JSON payload
  detailing the Docker error, then restores the pool to `IDLE`.
- **Graceful Shutdown:** on `SIGINT`/`SIGTERM`, StageHand stops accepting new
  connections, releases queued requests with a clean `503 Service Unavailable`,
  closes tunneled WebSocket connections, and exits. Running containers are left
  as-is (models are expensive to reload).

## 7. Hot Configuration Reload

`SIGHUP` or `POST /stagehand/reload` re-reads and validates `config.yaml`:

- The new route table applies to **new requests only**; in-flight requests finish
  against their resolved targets.
- Services added to a pool become available immediately; a service removed while
  active keeps serving until cooled down or swapped out, but is never chosen as a
  new target; queues for removed services are flushed with `503`.
- Validation failures (or transient Docker errors during revalidation) keep the
  old configuration — a bad reload never drops traffic.
- Changing `server.host`, `server.port`, or `docker_socket_path` requires a
  restart (documented limitation).
