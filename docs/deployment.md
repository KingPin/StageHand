# Deploying StageHand

StageHand serves plain HTTP and controls containers through a single Docker
socket. This guide covers how to run it safely in production: as a single
instance, behind a TLS-terminating reverse proxy, with the right streaming and
WebSocket settings, and with its tokens kept secret.

See also: [`runbook.md`](runbook.md) for operational symptom → action steps.

---

## Single instance by design

**Run exactly one StageHand process per Docker host.** StageHand is not a
clustered or horizontally-scalable service:

- It holds all pool state (which container is active, queued requests, grace and
  cooldown timers) **in memory** — nothing is replicated or persisted.
- It enforces GPU mutual exclusion by driving one Docker socket directly. Two
  StageHand instances against the same Docker host would race each other's swaps
  and break the "at most one container per VRAM pool" invariant.

There is no HA/clustering mode and none is planned — the problem it solves
(time-slicing one GPU) is inherently single-host. For availability, rely on the
supervisor restarting the single instance, **not** on running replicas:

- **Docker / Compose**: `restart: unless-stopped` on the StageHand service.
- **systemd**: `Restart=always`.
- **Kubernetes**: a `Deployment` with `replicas: 1` (or a `DaemonSet` pinned to
  the GPU node). Do **not** scale the replica count up.

A restart is cheap: StageHand re-reads config, re-validates that every container
exists, and leaves already-running backends as-is.

---

## Run behind a TLS-terminating reverse proxy

StageHand listens on plain HTTP (default `:8080`) and has **no built-in TLS**.
Its admin and proxy tokens travel in cleartext headers, so never expose the
listener directly to an untrusted network. Put a TLS-terminating reverse proxy
in front of it and bind StageHand to localhost (or a private Docker network).

Two behaviors every proxy in front of StageHand must respect:

1. **Long waits are normal.** A request that triggers a cold-start swap is
   **held open with no response bytes** until the target container is healthy —
   typically 10–30s, but up to your service's `startup_timeout_seconds` (the
   example config uses 180–240s). After that, AI backends stream for minutes.
   The proxy's read/response timeouts must be raised well past your longest
   cold start **plus** generation time, or it will cut requests off mid-swap or
   mid-stream.
2. **Streaming must not be buffered.** StageHand proxies SSE and chunked
   responses unbuffered (`FlushInterval -1`). A reverse proxy that buffers
   responses will defeat token-by-token streaming.

Point external health checks at the unauthenticated `GET /stagehand/healthz`.

### nginx

```nginx
# In the http{} block: maps the Upgrade header for WebSocket support.
map $http_upgrade $connection_upgrade {
    default upgrade;
    ''      close;
}

upstream stagehand {
    server 127.0.0.1:8080;
}

server {
    listen 443 ssl;
    http2 on;
    server_name ai.example.com;

    ssl_certificate     /etc/letsencrypt/live/ai.example.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/ai.example.com/privkey.pem;

    # AI backends stream tokens and may think for minutes; a cold-start swap
    # holds the request open with no bytes for up to startup_timeout_seconds.
    # Disable response buffering and raise timeouts past your longest
    # cold start + generation, or streams and swaps get cut off.
    proxy_buffering off;
    proxy_read_timeout 3600s;
    proxy_send_timeout 3600s;

    # Match (or exceed) server.max_request_bytes — base64 images, long contexts.
    client_max_body_size 100m;

    location / {
        proxy_pass http://stagehand;
        proxy_http_version 1.1;
        proxy_set_header Host              $host;
        proxy_set_header X-Real-IP         $remote_addr;
        proxy_set_header X-Forwarded-For   $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;

        # WebSocket upgrade (e.g. ComfyUI /ws).
        proxy_set_header Upgrade    $http_upgrade;
        proxy_set_header Connection $connection_upgrade;
    }

    # Optional: cheap unauthenticated liveness for your external monitor.
    location = /stagehand/healthz {
        proxy_pass http://stagehand;
    }
}
```

### Caddy

Caddy provisions TLS automatically and streams responses without buffering, so
the defaults are close to right. WebSocket upgrades are handled transparently.

```caddy
ai.example.com {
    reverse_proxy 127.0.0.1:8080 {
        # Flush each write immediately so SSE/chunked streaming isn't buffered.
        flush_interval -1
    }

    # Match (or exceed) server.max_request_bytes.
    request_body {
        max_size 100MB
    }
}
```

Caddy has no response read timeout by default, so long cold starts and
generations are not cut off. If you set global timeouts, make sure
`read_timeout`/`write_timeout` on the HTTP server are `0` (off) or very large.

### Traefik

WebSockets are tunneled transparently and responses stream without buffering by
default. Docker label example (Traefik v2/v3):

```yaml
labels:
  - "traefik.enable=true"
  - "traefik.http.routers.stagehand.rule=Host(`ai.example.com`)"
  - "traefik.http.routers.stagehand.entrypoints=websecure"
  - "traefik.http.routers.stagehand.tls.certresolver=le"
  - "traefik.http.services.stagehand.loadbalancer.server.port=8080"
```

Watch the entrypoint's `transport.respondingTimeouts.idleTimeout` (default
`180s`): a swap that takes longer than the idle timeout to produce the first
byte can be dropped. Raise it past your longest `startup_timeout_seconds`:

```yaml
# static config
entryPoints:
  websecure:
    address: ":443"
    transport:
      respondingTimeouts:
        readTimeout: 0      # no limit on slow request reads
        idleTimeout: 600s   # > your longest cold start
```

### cloudflared tunnel

A tunnel needs no inbound ports and terminates TLS at Cloudflare's edge.

```yaml
# ~/.cloudflared/config.yml
tunnel: <TUNNEL-ID>
credentials-file: /etc/cloudflared/<TUNNEL-ID>.json

ingress:
  - hostname: ai.example.com
    service: http://127.0.0.1:8080
    originRequest:
      connectTimeout: 30s
      # cloudflared streams responses by default; no response timeout here.
  - service: http_status:404
```

**Two Cloudflare-edge caveats that bite AI workloads:**

- **100-second response-header timeout (HTTP 524).** Cloudflare's edge expects
  the origin to start responding within ~100s. Because StageHand holds a request
  with **no response bytes** during a cold-start swap, a swap longer than ~100s
  (large models with a high `startup_timeout_seconds`) will surface as a `524`
  to the client even though StageHand is working correctly. Mitigate by
  pre-warming with `POST /stagehand/swap/{service}` (see the runbook) so the
  model is hot before user traffic arrives, or keep cold starts under the limit.
- **Request body size limit.** Cloudflare caps upload size by plan (100 MB on
  Free/Pro). Large base64 image payloads can exceed it before they ever reach
  StageHand.

---

## Security posture

- **Token model.** StageHand uses two StageHand-specific request headers,
  compared in constant time — **not** cookies:
  - `X-Stagehand-Admin-Token` gates the `/stagehand/*` control plane (status,
    swap, pool stop, reload). On by default.
  - `X-Stagehand-Token` (opt-in via `server.auth.proxy_token`) gates all
    non-admin proxy traffic. StageHand strips both headers before forwarding, so
    neither leaks to a backend. The standard `Authorization` header is passed
    through untouched for backends that need it.
- **Keep the admin token in a secret store.** Set `server.auth.admin_token` from
  a secret manager / environment-injected file, not committed config. If you
  omit it, StageHand mints a random token at boot and logs it **once** — fine for
  a quick start, awkward for automation. The token is hot-reloadable via `SIGHUP`.
- **The disable hatch is env-only and loud.** `STAGEHAND_DISABLE_ADMIN_AUTH=true`
  turns the admin gate off entirely and prints a warning banner on every
  startup. Only use it behind a trusted network boundary (e.g. a sidecar bound to
  localhost). It cannot be toggled by a config reload.
- **Terminate TLS upstream.** Bind StageHand to `127.0.0.1` or a private Docker
  network so tokens never traverse a network in cleartext.

---

## Request body size & the swap-retry caveat

`server.max_request_bytes` defaults to `0` (uncapped). **Set it to a positive
value in production** — above your largest legitimate request (base64 image
payloads, long chat contexts).

The reason is not just abuse protection. Right after a container swap, the first
request often lands on a **stale pooled keep-alive connection** to the
just-replaced backend. StageHand's transport transparently retries such a
request against the fresh backend — **but only if the body is replayable**. A
body is made replayable by buffering it, and StageHand only buffers bodies when
a cap is set (it buffers up to the cap). With the default `0`:

- Bodies up to the 1 MiB model-peek window are still buffered and replay fine.
- Bodies **larger than 1 MiB are not buffered, so they are not retryable** — a
  request that hits a stale connection in the moments after a swap can see a
  one-off `502`. The client must retry.

Setting `max_request_bytes` above your largest real request closes this gap:
every accepted body becomes replayable. See PRD §4.3 for the full mechanics.
