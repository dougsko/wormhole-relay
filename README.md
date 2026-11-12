# Wormhole Transit Relay (Go)

Public home: <https://github.com/dougsko/wormhole-relay>

A Go reimplementation of the
[Magic Wormhole transit relay](https://github.com/magic-wormhole/magic-wormhole-transit-relay)
server, which the Magic Wormhole protocol uses to establish direct TCP/WebSocket
connections between two peers through a third-party relay.

Clients connect, send a handshake containing a shared token, and the relay
pairs up connections that present the same token (with different "side"
tokens, when provided) and glues the two sockets together.

## Protocol

A client connects and sends one of the following handshake lines:

```
please relay <64-hex-char-token>\n            # legacy
please relay <64-hex-char-token> for side <16-char-side>\n
```

If no matching token is pending, the client waits (up to 30 seconds in the
reference implementation) and receives `ok\n` once its partner connects.

If the token line is malformed the server responds with `bad handshake\n` and
closes the connection. If data is received before the partner has been found,
the server responds with `impatient\n` and closes the connection.

Once relaying, bytes are forwarded verbatim between the two sockets.

## Usage

```sh
git clone https://github.com/dougsko/wormhole-relay.git
cd wormhole-relay
go build ./cmd/relay-server
./relay-server [options]
```

Or install the binary directly with Go:

```sh
go install github.com/dougsko/wormhole-relay/cmd/relay-server@latest
```

Options — the names, short flags, and defaults match the Python
`twist transitrelay` plugin (twisted `usage.Options`), and the endpoints use
the same `tcp:PORT[:interface=ADDR]` syntax as twisted's endpoint strings
(bare ports and `host:port` are also accepted):

| Flag | Default | Python equivalent | Description |
|------|---------|-------------------|-------------|
| `-p`, `--port` | `tcp:4001:interface=::` | `-p`, `--port` | endpoint to listen on |
| `-w`, `--websocket` | (disabled) | `-w`, `--websocket` | endpoint to listen for WebSocket connections |
| `-u`, `--websocket-url` | derived | `-u`, `--websocket-url` | WebSocket URL (derived from endpoint if not provided) |
| `--blur-usage` | `0` | `--blur-usage` | blur timestamps and data sizes in logs (`0` = off) |
| `--log-fd` | (none) | `--log-fd` | write JSON usage logs to this file descriptor (a number, or a path) |
| `--usage-db` | (none) | `--usage-db` | record usage data (SQLite) |

Notes on parity with the Python plugin:

- `--port` and `--websocket` take the same twisted endpoint strings the
  Python plugin accepts (`tcp:PORT`, `tcp:PORT:interface=ADDR`), not just a
  bare port; the defaults are identical.
- `--log-fd` matches the Python flag's intent (the Python version is strictly
  a file descriptor number; we also accept a file path, a superset).
- `--websocket-url` is accepted for compatibility and derived from the
  `--websocket` endpoint when not given (`ws://localhost:PORT/`), exactly like
  the Python plugin. It is not otherwise used (the Python factory also only
  uses it for that derivation).
- Usage logging: `--log-fd` writes one JSON object per finished connection
  (`started`, `total_time`, `waiting_time`, `total_bytes`, `mood`), and
  `--usage-db` writes the same events into a SQLite database in the reference
  v1 schema (`db-schemas/usage-v1.sql`, copied verbatim). The `waiting_time`
  column is `NULL` for connections that were never paired, and moods include
  `happy`, `lonely`, `redundant`, `impatient`, `errory`, `jilted`, and `empty`
  (a connection that closed without ever sending a handshake) — the same set
  the Python version records. In addition, a `current` row (`rebooted`,
  `updated`, `connected`, `waiting`, `incomplete_bytes`) is written at startup
  and every 5 minutes, mirroring the reference's `TimerService`.

## Client-side environment variables

Setting a server flag is not enough to make a `wormhole` client talk to it —
the *client* chooses the servers. The Python `wormhole` CLI (and library)
reads these environment variables, which are the same for the Go and Python
servers, since they speak the same protocols:

- `WORMHOLE_RELAY_URL` (`--relay-url`) — mailbox/rendezvous URL, e.g. `ws://localhost:4000/v1`
- `WORMHOLE_TRANSIT_HELPER` (`--transit-helper`) — `tcp:HOST:PORT` of a transit relay, e.g. `tcp:localhost:4001`

Neither Python server, nor these Go servers, read any environment variables
themselves (twisted `usage.Options` doesn't, and neither do these ports).

Example, using both Go servers:

```sh
mailbox-server --port 4500 --channel-db /tmp/mb.sqlite &
relay-server --port 4001 --usage-db /tmp/relay-usage.sqlite &
export WORMHOLE_RELAY_URL=ws://localhost:4500/v1
export WORMHOLE_TRANSIT_HELPER=tcp:localhost:4001
wormhole send --no-listen FILE            # add --no-listen on both sides to force the relay path
wormhole receive --no-listen --accept-file --output-file OUT CODE
```

## Running tests

```sh
go test ./...
```

## Project layout

```
cmd/relay-server/   Main binary
internal/relay/     Transit relay state machine, connection handling, usage tracking
```

## Running in a container (podman)

```sh
podman build --format docker -t wormhole-relay .
podman run -d --name wormhole-relay \
    -p 4001:4001 -v wormhole-relay-data:/data \
    wormhole-relay
```

- `--format docker` keeps the `HEALTHCHECK` (podman's default OCI image
  format drops it; the build still succeeds without it, you just lose the
  health probe). The probe uses busybox `nc -z` because the transit relay
  speaks raw TCP, not HTTP.
- The image is alpine-based and runs as non-root uid 10001. `/data` is a
  volume holding `relay-usage.sqlite` (the default `--usage-db`).
- Extra flags replace the default CMD entirely. To serve WebSocket clients
  as well as raw TCP:

  ```sh
  podman run -d --name wormhole-relay \
      -p 4001:4001 -p 4002:4002 -v wormhole-relay-data:/data \
      wormhole-relay --port tcp:4001:interface=:: \
      --websocket tcp:4002:interface=:: \
      --websocket-url ws://localhost:4002/ \
      --usage-db /data/relay-usage.sqlite
  ```

- `.dockerignore` keeps `.git`, local databases (`*.sqlite`), and editor
  noise out of the build context.
- Verified end-to-end: the unmodified Python client (`magic-wormhole`
  0.24.0) completes a file transfer against the containerized relay and
  mailbox (via `--no-listen` on both sides to force the relay path); the
  usage DB inside `/data` records the expected `happy` row.

## License

MIT — see [LICENSE](LICENSE). This is a Go port of the MIT-licensed Python
[magic-wormhole-transit-relay](https://github.com/magic-wormhole/magic-wormhole-transit-relay);
the usage schema under `internal/relay/db-schemas/` is derived verbatim from the reference.
