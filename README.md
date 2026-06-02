# StageHand

**Dynamic VRAM multiplexer & reverse proxy for AI backends on a single GPU.**

Run ComfyUI, llama.cpp, Whisper, vLLM, and friends behind one endpoint on a
consumer GPU (8–16GB). StageHand groups GPU-hungry containers into mutually
exclusive **VRAM pools** — only one runs at a time. When a request targets a
stopped service, StageHand stops the active container, starts the target via
the Docker API, waits for health, and releases the queued requests. No client
request is lost during the 10–30s swap.

```
client ──> stagehand :8080 ──┬─> [gpu pool] comfyui      (swapped on demand)
                             │             llama-cpp-moe (swapped on demand)
                             ├─> embeddings-cpu          (always-on)
                             └─> whisper-stt             (always-on)
```

## Features

- **VRAM pools** — at most one container per pool runs; swaps are automatic,
  self-healing, and protected by an anti-thrashing grace period
- **Request queueing** — bounded FIFO per service; clients held (not dropped)
  through cold starts, `429` + `Retry-After` on overflow
- **OpenAI-style model routing** — routes on the JSON body's `"model"` field,
  so OpenWebUI/LibreChat/`openai` SDK clients work unmodified
- **True streaming** — SSE chunks forwarded unbuffered; transparent WebSocket
  tunneling for ComfyUI/A1111 progress sockets
- **Cooldown / cold pool** — idle pools swap to a default service or spin
  fully down to 0MB VRAM
- **Docker events reconciliation** — out-of-band `docker stop/start` is
  detected; intruders are stopped, dead actives are recovered
- **Ops surface** — `/stagehand/status`, manual pre-warm, force-stop, hot
  config reload (SIGHUP or HTTP), graceful shutdown

## Quick start

1. Create a shared network and attach your AI backend containers to it
   (StageHand reaches them by container name):

   ```sh
   docker network create ai-net
   # example backends (yours will differ)
   docker run -d --name llama-cpp-moe --network ai-net --gpus all ... 
   docker run -d --name comfyui-stable --network ai-net --gpus all ...
   ```

2. Write `config.yaml` (see [`config.example.yaml`](config.example.yaml) for
   the full annotated version):

   ```yaml
   server:
     port: 8080
   vram_pools:
     gpu_0_vram:
       grace_period_seconds: 60
       cooldown_seconds: 300
       default_service: null          # cold pool: stop everything when idle
   services:
     llama-moe:
       container_name: "llama-cpp-moe"
       target_url: "http://llama-cpp-moe:8081"
       health_path: "/health"
       vram_pool: "gpu_0_vram"
     comfyui:
       container_name: "comfyui-stable"
       target_url: "http://comfyui-stable:8188"
       health_path: "/"
       vram_pool: "gpu_0_vram"
   routes:
     - path_prefix: "/v1/chat/completions"
       models:
         "qwen-moe": "llama-moe"
         "flux": "comfyui"
       service: "llama-moe"           # fallback when no model matches
     - path_prefix: "/ws"
       service: "comfyui"
   ```

3. Run StageHand on the same network with the Docker socket mounted:

   ```sh
   docker build -t stagehand .
   docker run -d --name stagehand --network ai-net \
     -p 8080:8080 \
     -v /var/run/docker.sock:/var/run/docker.sock \
     -v $PWD/config.yaml:/etc/stagehand/config.yaml:ro \
     stagehand
   ```

4. Point your clients at `http://host:8080/v1/...`. Selecting a model in an
   OpenAI-compatible UI swaps the right container in automatically.

## Configuration reference

### `server`

| Key | Default | Meaning |
|---|---|---|
| `host` / `port` | `0.0.0.0` / `8080` | Listen address |
| `docker_socket_path` | `/var/run/docker.sock` | Docker daemon socket |
| `cors_allowed_origins` | — | `["*"]` or explicit origins; preflights echo requested headers |
| `max_queue_size` | `100` | Default per-service queue bound |

### `vram_pools.<name>`

| Key | Default | Meaning |
|---|---|---|
| `grace_period_seconds` | `0` | Min runtime before a container may be swapped out (anti-thrash) |
| `cooldown_seconds` | `0` | Idle time before cooldown action (`0` disables) |
| `default_service` | `null` | Swap here on cooldown; `null` = stop everything (cold pool) |

### `services.<name>`

| Key | Default | Meaning |
|---|---|---|
| `container_name` | required | Docker container StageHand manages |
| `target_url` | required | `http://<container-name>:<port>` on the shared network |
| `health_path` | `/` | Polled until `200` after start |
| `startup_timeout_seconds` | `180` | Cold-start budget; queued clients get `504` past it |
| `max_queue_size` | server default | Per-service queue override |
| `vram_pool` | `null` | Pool membership; `null` = always-on (no orchestration) |

### `routes` (ordered, first match wins)

| Key | Meaning |
|---|---|
| `path_prefix` | Prefix match against the request path |
| `headers` | All listed headers must match, else the route falls through |
| `models` | Maps the JSON body `"model"` value → service (`POST` + JSON only) |
| `service` | Target, or fallback when `models` has no entry |

Unmatched requests get `404` with the known-route list in the body.

## API

| Endpoint | Method | Purpose |
|---|---|---|
| `/stagehand/status` | GET | Pool states, active service, cooldown countdown, queue depths, always-on health |
| `/stagehand/swap/{service}` | POST | Pre-warm/force a swap (bypasses grace; chains behind an in-flight swap) |
| `/stagehand/pool/{pool}/stop` | POST | Force a pool cold; queued requests get `503` |
| `/stagehand/reload` | POST | Hot config reload (also `SIGHUP`); a bad config is rejected and the old one kept |

Everything else proxies per your routes — including SSE streams and
WebSocket upgrades.

## Behavior notes

- **Boot validation**: StageHand pings Docker and verifies every configured
  container exists, exiting loudly otherwise.
- **Failure handling**: Docker errors flush the affected queue with `502` +
  the Docker error detail; health-check timeouts flush `504`; the pool
  returns to `IDLE` and recovers on the next request.
- **Hot reload**: pools whose config is unchanged keep their queues and
  active container; changed/removed pools flush queues with `503`. Listener
  address and socket path changes require a restart.
- **Shutdown** (`SIGINT`/`SIGTERM`): queued requests get a clean `503`,
  WebSocket tunnels close, in-flight responses drain (15s bound), and
  containers are left running — models are expensive to reload.
- **Restart policies**: StageHand stops containers via the Docker API, which
  Docker honors even under `restart: always`/`unless-stopped`.
- **WebSocket tunnels don't pin containers**: a live tunnel does not prevent
  its backend from being swapped out by another model's traffic (post-grace),
  a cooldown, or an admin action — the tunnel drops and the client should
  reconnect (ComfyUI/A1111 frontends do so automatically). Size
  `grace_period_seconds` to your interactive sessions if this matters.

## Development

```sh
go build ./... && go vet ./... && go test -race ./...   # local toolchain
./scripts/go test -race ./...                           # or via Docker, no Go needed
```

The test suite runs entirely against an in-memory Docker fake. An optional
real-Docker e2e suite is gated behind `STAGEHAND_E2E=1` (see `e2e`).

## License

Apache License 2.0 — see [LICENSE](LICENSE).
