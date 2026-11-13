# E2E interoperability tests for the Go transit relay

End-to-end tests that push **real transfers through the Go relay**:

| Test | Sender | Receiver | Payload |
|------|--------|----------|---------|
| `TestFileGoToPython` | wormhole-william (Go) | magic-wormhole (Python) | file |
| `TestDirectoryGoToPython` | wormhole-william (Go) | magic-wormhole (Python) | directory |
| `TestFilePythonToGo` | magic-wormhole (Python) | wormhole-william (Go) | file |
| `TestDirectoryPythonToGo` | magic-wormhole (Python) | wormhole-william (Go) | directory |
| `TestFilePythonToPython` | magic-wormhole (Python) | magic-wormhole (Python) | file (control) |
| `TestFileGoToGo` | wormhole-william (Go) | wormhole-william (Go) | file (control) |

Text messages ride the mailbox (not the transit relay), so there is no text
case here; the mailbox repository's e2e suite covers those.

Every transfer is **forced through the Go relay**: the Python client runs
with `--no-listen` and the Go library with `NoListen: true`, so neither side
advertises a direct-connection candidate and the relay is the only transit
path. In addition, each test asserts the relay's usage database
(`--usage-db`) contains a `happy` pairing with at least the payload's byte
count — proof the bytes actually traversed the relay. Received payloads are
verified by SHA-256; directory trees are compared recursively.

Topology per test (everything on 127.0.0.1):

```
wormhole-mailbox (Go)  <-- rendezvous -->  clients
wormhole-relay   (Go)  <-- transit    -->  clients
```

Each test boots its own `mailbox-server` and `relay-server` (fresh temporary
databases). No traffic leaves 127.0.0.1 and the public relay/transit servers
are never contacted.

## Requirements

- Go (see `../go.mod` for the version)
- Python with [`magic-wormhole`](https://pypi.org/project/magic-wormhole/)
  installed, e.g.:

  ```sh
  pip install magic-wormhole
  ```

  (The `magic-wormhole-transit-relay` package is *not* needed — this test
  exercises the Go relay instead.)

- checkouts of **wormhole-mailbox** and **wormhole-william** as siblings of
  this repository:

  ```
  bin/
    wormhole-relay/     # this repository
    wormhole-mailbox/   # github.com/dougsko/wormhole-mailbox
    wormhole-william/   # github.com/dougsko/wormhole-william
  ```

  Otherwise, set `WORMHOLE_E2E_MAILBOX` to the mailbox checkout and/or edit
  the `replace` directive in `e2e/go.mod` for wormhole-william.

## Running

```sh
cd e2e
go test -tags=e2e -v .
```

The `e2e` build tag keeps the default `go test ./...` fast and free of the
Python requirement.

## Environment variables

| Variable | Meaning |
|----------|---------|
| `WORMHOLE_E2E_PYTHON` | path to the Python `wormhole` entry script (default: `/tmp/wormhole-venv/bin/wormhole`, then `wormhole` from `PATH`) |
| `WORMHOLE_E2E_MAILBOX` | path to the wormhole-mailbox checkout (default: sibling `../../wormhole-mailbox`) |
| `WORMHOLE_E2E_REQUIRE=1` | fail instead of skip when a requirement is missing (used by CI) |

## CI

The `e2e` job in `.github/workflows/ci.yaml` checks out all three
repositories as siblings, installs `magic-wormhole`, and runs the suite with
`WORMHOLE_E2E_REQUIRE=1`.
