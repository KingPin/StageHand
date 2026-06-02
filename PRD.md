Production PRD: StageHand (Dynamic VRAM Multiplexer & Reverse Proxy)This document serves as the complete, self-contained Product Requirement Document (PRD) and Technical Specification for StageHand, a configuration-driven VRAM Multiplexer and Reverse Proxy written in Go.1. Executive Summary & Core ConceptWhen running multiple open-source AI engines (ComfyUI, Automatic1111, Llama.cpp, Ollama, Whisper, vLLM, TTS, etc.) on consumer hardware with limited GPU memory (e.g., 8GB–16GB VRAM), running them concurrently causes Out-Of-Memory (OOM) crashes.StageHand is a lightweight, high-performance, containerized asynchronous reverse proxy that acts as a VRAM Multiplexer. By reading a declarative configuration file (config.yaml), StageHand maps incoming request routes to specific containers, groups those containers into mutually exclusive VRAM pools, automatically stops/starts them on-demand via the host's Docker socket, and queues incoming traffic during transitions so that no client requests are lost.2. Declarative Configuration Spec (config.yaml)StageHand parses a config.yaml file on startup. This allows users to add arbitrary numbers of LLMs, speech-to-text engines, image-generation interfaces, and embedding servers.# Global Configuration
server:
  host: "0.0.0.0"
  port: 8080
  docker_socket_path: "/var/run/docker.sock"
  docker_api_version: "v1.40"
  cors_allowed_origins: ["*"]    # Allowed origins for web-UI integrations

# Mutual Exclusion Pools
vram_pools:
  gpu_0_vram:
    grace_period_seconds: 60      # Min run-time of any container in this pool before allowed to swap
    cooldown_seconds: 300         # Idle time before stopping active containers
    default_service: null         # Set to null to spin down ALL containers when idle (Cold Pool)

# Declared Services (AI Backends)
services:
  comfyui:
    container_name: "comfyui-stable"
    target_url: "http://localhost:8188"
    health_path: "/"
    vram_pool: "gpu_0_vram"       # Belongs to the dynamic VRAM-bound GPU pool

  llama-moe:
    container_name: "llama-cpp-moe"
    target_url: "http://localhost:8081"
    health_path: "/health"
    vram_pool: "gpu_0_vram"       # Mutually exclusive with comfyui

  embeddings-cpu:
    container_name: "llama-embeddings"
    target_url: "http://localhost:8082"
    health_path: "/health"
    vram_pool: null               # Always-On CPU Service (no VRAM limitation)

  reranker-cpu:
    container_name: "llama-reranker"
    target_url: "http://localhost:8083"
    health_path: "/health"
    vram_pool: null               # Always-On CPU Service

  whisper-stt:
    container_name: "faster-whisper"
    target_url: "http://localhost:8084"
    health_path: "/v1/models"
    vram_pool: null               # Always-On CPU Service

# HTTP Path and Header Routing Rules
routes:
  - path_prefix: "/v1/embeddings"
    service: "embeddings-cpu"

  - path_prefix: "/v1/rerank"
    service: "reranker-cpu"

  - path_prefix: "/v1/audio"
    service: "whisper-stt"

  # Dynamic routing with optional header-matching
  - path_prefix: "/v1/chat/completions"
    service: "llama-moe"
    headers:
      X-Use-Model: "moe"

  - path_prefix: "/v1/images"
    service: "comfyui"

  # WebSocket routing rules
  - path_prefix: "/ws"
    service: "comfyui"
3. Dynamic State Machine & VRAM Pool OrchestrationStageHand maintains an internal, thread-safe, in-memory state tracking machine representing the status of all configured services and VRAM pools.3.1 VRAM Pool State RulesFor each defined pool in vram_pools:Mutex Lock: Only ONE service declared in services associated with a specific vram_pool can have a Docker state of running at any given moment.Explicit Stop: If a request is routed to a target service in pool X, and that service is currently stopped:Acquire an asynchronous exclusive Mutex lock for pool X.Identify which other service in pool X is currently running.Conditional Shutdown (Fast Startup Path): If no other service is active (i.e., the pool is in STATE_IDLE), skip the stop steps entirely and proceed immediately to the start sequence.If another service is active:Stop that active service via a POST /containers/{id}/stop call to the Docker socket.Wait for confirmation of stop (status exited).Start the target service via a POST /containers/{id}/start call.Run an asynchronous loop polling the target's configured health_path (or TCP port check) until a 200 OK is returned.Release the Mutex lock and flush any held requests.3.2 Thread-Safe Queueing & Connection BufferingTo prevent losing client requests during a container transition (which can take 10–30 seconds):Connection Holding: StageHand accepts incoming requests, puts them into a memory-bound queue (FIFO) associated with the target service, and keeps the client TCP socket alive.Timeout Buffer: StageHand must configure an HTTP gateway timeout of at least 180 seconds to allow for extremely slow cold-starts.Transition Blocking: If a transition is already in progress, any subsequent incoming requests for either the starting service or any other service in that pool are automatically held in the queue.3.3 Dynamic Cooldown & Idle Handlers (The "Cold Pool" State)If cooldown_seconds is greater than 0:An internal countdown timer monitors activity on the dynamic VRAM pool. Every incoming request to any service in that pool resets this timer.When the timer hits 0:Option A (default_service is set): If the currently running container is not the default service, trigger a transition to shut it down and start the default service.Option B (default_service: null): Send a stop signal to the active service. Transition the pool's state to STATE_IDLE where 0MB of VRAM is occupied by this pool.4. API Classification & Request ProxyingThe reverse proxy performs robust HTTP forwarding, supporting modern AI requirements like token streaming and WebSockets.       Incoming HTTP/WS Unified Proxy Request (Port 8080)
                             |
         +-------------------+-------------------+
         |                                       |
    [ Match Path/Headers ]              [ Match WebSockets ]
         |                                       |
   Find matching route.                   Handle standard upgrade
   Determine target service.             and tunnel frames to 
         |                                the running ComfyUI/etc.
   Check VRAM Pool state.
         |
   +-----+-----+
   |           |
[ Healthy ]  [ Stopped/Transitioning/Idle ]
   v           v
Forward      Queue request -> Trigger VRAM swap -> Flush queue on ready
4.1 Chunked Streaming ProtocolAI completions (/v1/chat/completions) utilize Server-Sent Events (SSE) to stream responses.No Buffering: StageHand must not read the full response body into memory.Chunk Propagation: StageHand must stream raw bytes iteratively directly from the target service backend to the client connection as they arrive.HTTP Header Copying: Headers such as Content-Type: text/event-stream, Cache-Control: no-cache, and Transfer-Encoding: chunked must be preserved and forwarded unmodified.4.2 Transparent WebSocket ForwardingWeb interfaces like ComfyUI and Automatic1111 use WebSockets for real-time progress bars, queues, and execution statuses.Handshake Upgrade: When an incoming connection contains an Upgrade: websocket header, StageHand catches this and negotiates a raw WebSocket connection.Duplex Pipeline: Secure an active TCP link to the target service's local port and pipe message packets bi-directionally between the client socket and the backend container.5. System Health, Observability, & CORSTo make StageHand production-ready and open-source friendly, the proxy must expose internal telemetry and handle browser-security standards.5.1 Self-Health API (/stagehand/status)StageHand reserves the /stagehand/* namespace for native API calls. It must expose a GET /stagehand/status endpoint that returns a JSON schema showing the state of the orchestrator:{
  "status": "healthy",
  "version": "1.0.0",
  "vram_pools": {
    "gpu_0_vram": {
      "current_active_service": "llama-moe",
      "state": "STATE_TEXT",
      "seconds_until_cooldown": 245,
      "queued_requests_count": 0
    }
  },
  "always_on_services": {
    "embeddings-cpu": "healthy",
    "reranker-cpu": "healthy"
  }
}
5.2 Seamless CORS Preflight HandlingMany frontends run in the browser on separate origins (e.g., http://localhost:3000 or http://127.0.0.1:5173).Preflight Requests: When an incoming HTTP request is an OPTIONS request, StageHand must immediately respond with status 200 OK, applying the configured cors_allowed_origins headers:Access-Control-Allow-Origin: <origin>Access-Control-Allow-Headers: *Access-Control-Allow-Methods: GET, POST, OPTIONS, PUT, DELETE6. Docker Lifecycle Guardrails & Edge CasesAnti-Thrashing (Grace Period): If service A was spun up 10 seconds ago, and a request for service B comes in, the system must wait until A has run for at least grace_period_seconds before stopping it to prevent thrashing your GPU.Conflict with Docker Restart Policies:On a typical host, containers might be configured with restart: always or restart: unless-stopped.When StageHand stops a container via the Docker socket, Docker honors the explicit API stop command and will not auto-restart it.StageHand should always send the stop command gracefully (timeout of 10s) before executing a hard kill to prevent database/model file corruption.Docker Failure Recovery: If a docker stop or docker start REST command fails (or the target container is missing on the host), StageHand must release its queue, return an HTTP 502 Bad Gateway error with a JSON payload detailing the Docker API error, and restore the VRAM pool to its default idle state.Graceful Shutdown: Upon receiving a terminate signal (SIGINT or SIGTERM), StageHand must release any held connections with a clean HTTP 503 Service Unavailable message before exiting.
