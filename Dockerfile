# StageHand — VRAM multiplexer & reverse proxy
# Build:  docker build -t stagehand .
# Run:    see README.md (needs the docker socket + a shared network)

FROM golang:1.26 AS build
WORKDIR /src

# Cache module downloads independently of source changes.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath \
    -ldflags="-s -w -X github.com/KingPin/StageHand/internal/version.Version=${VERSION}" \
    -o /out/stagehand ./cmd/stagehand

# Static binary + CA certs; root is required to use the mounted docker
# socket on most hosts.
FROM gcr.io/distroless/static-debian12
COPY --from=build /out/stagehand /stagehand
EXPOSE 8080
ENTRYPOINT ["/stagehand", "-config", "/etc/stagehand/config.yaml"]
