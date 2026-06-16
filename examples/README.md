# Example configs

Each `.yaml` here is a **complete, valid StageHand config** that teaches one
distinct routing/pooling pattern. Copy the one closest to your setup, rename it
`config.yaml`, and adjust the container names / URLs / model names.

For the exhaustive field reference (every key, type, and default), see
[`../config.example.yaml`](../config.example.yaml) and the
[Configuration reference](../README.md#configuration-reference) section of the
root README. This page focuses on *which shape solves which problem*.

## Which one?

| File | Use it when… |
|------|--------------|
| [`01-single-gpu.yaml`](01-single-gpu.yaml) | You have one GPU backend and just want on-demand start/stop. |
| [`02-cpu-gpu-by-model.yaml`](02-cpu-gpu-by-model.yaml) | One endpoint, small model on CPU + big model on GPU, picked by the request's `model` field. |
| [`03-warm-vs-cold-pool.yaml`](03-warm-vs-cold-pool.yaml) | Two GPU backends sharing one card; choosing warm (keep one hot) vs cold (free the GPU when idle). |
| [`04-header-gated.yaml`](04-header-gated.yaml) | Same path, different backend based on a request header (e.g. tenant/priority). |
| [`05-full-openai-stack.yaml`](05-full-openai-stack.yaml) | A production-shaped OpenAI-compatible stack: embeddings + rerank + chat + image-gen + websockets + auth. |

## Running an example

Mount the chosen file at `/etc/stagehand/config.yaml` (see the root README's
quick-start for the full `docker run` line), or point the binary at it directly:

```sh
stagehand -config examples/02-cpu-gpu-by-model.yaml
```

The container names in `target_url` assume your backends are on the same
user-defined Docker network as StageHand — replace them with your own.

---

## The patterns

### 01 — Minimal single-GPU service
One service in one pool behind a catch-all `/` route. First request starts the
container and waits for health; after `cooldown_seconds` of idle it stops again.
`default_service: null` makes it a **cold pool** — nothing runs while idle.

### 02 — Split CPU vs GPU by model name
The headline pattern. A route with a `models:` map tells StageHand to peek the
JSON body's `"model"` field and route to the mapped service. `service:` is the
**fallback** when the model isn't in the map or the body can't be read — keep it
the cheap/always-on backend so an unknown model never triggers a GPU swap. Here
`"small"` → an always-on CPU model, `"big"` → a GPU-pool model.

### 03 — Warm vs cold pool
Two GPU services share `gpu_0`, so they're **mutually exclusive**: a request for
the inactive one triggers a swap (stop active → start target → wait for health →
release queued requests). `grace_period_seconds` prevents thrashing by keeping a
freshly-started container up for a minimum time. On idle, a **warm** pool
(`default_service: "llama"`) swaps back to its default; a **cold** pool
(`default_service: null`) stops everything.

### 04 — Header-gated routing
Two routes share a path; the first is gated by `headers:`. Headers only *gate* —
if they don't all match, the route **falls through** to the next one. So the
gated route must be declared **before** the un-gated fallback, since matching is
first-match-wins.

### 05 — Full OpenAI-compatible stack
Combines the above: always-on CPU services for `/v1/embeddings` and `/v1/rerank`,
a GPU pool shared by a chat model and an image generator, model-based routing on
`/v1/chat/completions`, a transparent `/ws` websocket tunnel, and a catch-all.
It also enables `server.auth` (admin + proxy tokens) and `max_request_bytes`.

---

## Routing recap

- Routes are evaluated **in order; first match wins** (not longest-prefix).
- A route matches when the request path has the `path_prefix`.
- `headers:` must *all* match (exact value) or the route falls through.
- `models:` maps the body `"model"` field to a service; `service:` is the
  fallback. Routes without `models:` ignore the body.
- No route matches → `404` with the known routes listed.

## Pool recap

- All services in a `vram_pool` are **mutually exclusive** (one runs at a time);
  `vram_pool: null` is an always-on service that bypasses orchestration.
- `grace_period_seconds` — anti-thrash minimum run-time before a swap-out.
- `cooldown_seconds` — idle time before acting (`0` disables it).
- `default_service` — on cooldown, swap to this service (**warm**) or, when
  `null`, stop everything (**cold**).
