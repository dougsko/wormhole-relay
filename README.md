# Wormhole Transit Relay (Go)

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
go build ./cmd/relay-server
./relay-server [options]
```

Options:

| Flag | Default | Description |
|------|---------|-------------|
| `--port` | `4001` | TCP port to listen on |
| `--websocket` | (disabled) | WebSocket port to listen on (e.g. `4002`) |
| `--websocket-url` | derived | WebSocket URL to advertise |
| `--blur-usage` | `0` | Blur timestamps and sizes in usage logs |
| `--log-fd` | (none) | Write JSON usage logs to a file descriptor |
| `--usage-db` | (none) | SQLite database for usage tracking (planned) |

## Running tests

```sh
go test ./...
```

## Project layout

```
cmd/relay-server/   Main binary
internal/relay/     Transit relay state machine, connection handling, usage tracking
```
