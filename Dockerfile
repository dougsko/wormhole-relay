# syntax=docker/dockerfile:1
#
# wormhole-relay (transit relay), packaged for podman (or docker). Build with:
#
#   podman build --format docker -t wormhole-relay .
#
# (--format docker keeps the HEALTHCHECK; podman's default OCI format drops it)
#
# Run with:
#
#   podman run -d --name wormhole-relay \
#       -p 4001:4001 -v wormhole-relay-data:/data \
#       wormhole-relay [extra flags...]
#
# Extra flags replace the default CMD entirely. To serve WebSocket clients
# instead of (in addition to) raw TCP, pass --websocket and optionally
# --websocket-url:
#
#   podman run -d --name wormhole-relay \
#       -p 4001:4001 -p 4002:4002 -v wormhole-relay-data:/data \
#       wormhole-relay --port tcp:4001:interface=:: \
#       --websocket tcp:4002:interface=:: \
#       --websocket-url ws://localhost:4002/ \
#       --usage-db /data/relay-usage.sqlite

FROM docker.io/library/golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" \
    -o /out/wormhole-relay ./cmd/relay-server

FROM docker.io/library/alpine:3.21
RUN adduser -D -u 10001 wormhole \
    && mkdir -p /data \
    && chown wormhole:wormhole /data
COPY --from=build /out/wormhole-relay /usr/local/bin/wormhole-relay
USER wormhole
VOLUME ["/data"]
EXPOSE 4001
# the relay speaks raw TCP, so probe the port with busybox nc instead of HTTP
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD nc -z 127.0.0.1 4001 || exit 1
ENTRYPOINT ["/usr/local/bin/wormhole-relay"]
# default listens on TCP/4001 and records usage to /data (the Python plugin
# only enables the usage DB when --usage-db is passed)
CMD ["--port", "tcp:4001:interface=::", "--usage-db", "/data/relay-usage.sqlite"]
