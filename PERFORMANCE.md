# Performance tooling and baseline numbers

This document describes how to benchmark and stress the relay, and records
the baseline measurements taken before the performance fixes from the code
review land. It exists so every perf change can be validated with numbers
instead of intuition.

## Tooling

### 1. Microbenchmarks — `internal/relay/bench_test.go`

Run with:

```sh
go test ./internal/relay -run XXX -bench . -benchmem -benchtime 1s
```

| Benchmark | What it measures |
|---|---|
| `BenchmarkRelayThroughput` | Sustained bulk transfer through a real loopback relayed pair (8 MiB per iteration, MB/s reported) |
| `BenchmarkRelaySmallMessages` | 1 KiB ping-pong latency (Nagle/delayed-ACK territory; also trips on per-message overhead) |
| `BenchmarkPairingRate/{serial,parallel}` | `PendingRequests` register/unregister cost, single-threaded and parallel |
| `BenchmarkUsageRecord/{no_backends,sqlite_memory,no_backends_blurred}` | Per-connection usage recording; isolates the SQLite INSERT cost |
| `BenchmarkUpdateStats/active_{100,1000,10000}` | Periodic stats sweep with N active connections |
| `BenchmarkHandshake/{regex_old,regex_new}` | Handshake regex parse cost |

Notes:
- Networking benchmarks use a real loopback listener and real client
  connections, so they exercise the full read → state machine → buddy-write
  path. Loopback numbers are dominated by kernel buffers; treat them as
  **relative** before/after numbers, not capacity.
- Use `benchstat` (`go install golang.org/x/perf/cmd/benchstat`) when
  comparing revisions; don't eyeball single runs.

### 2. Load generator — `cmd/relay-stress`

Speaks the wire protocol directly, so it works unchanged against this Go
relay, the Python reference implementation, or a production relay.

```sh
go build -o relay-stress ./cmd/relay-stress

# bulk throughput: N concurrent pairs, X bytes through each
relay-stress -addr 127.0.0.1:4001 -mode throughput -pairs 8 -bytes 16777216

# pairing churn: complete connect/handshake/close cycles as fast as possible
relay-stress -addr 127.0.0.1:4001 -mode churn -pairs 16 -duration 10s

# stuck receiver: one pair, receiver never reads; does the relay notice?
relay-stress -addr 127.0.0.1:4001 -mode stall -stall-timeout 15s
```

`stall` is the deterministic test for the backpressure finding: with no
write deadlines/buffering in the relay, the sender blocks after a few MB
(kernel buffers) and the relay never closes the pipe. After a write-deadline
fix, the relay is expected to close the stuck pipe, which the tool reports.

Exit code is non-zero on any pair error. Progress/diagnostics go to stderr,
machine-greppable `key=value` summaries to stdout.

### 3. A/B comparison against the Python reference

The reference (`magic-wormhole-transit-relay`) checkout sits next to this
repository and runs from source — no install needed:

```sh
cd ../magic-wormhole-transit-relay-original
PYTHONPATH=src /tmp/wormhole-venv/bin/twistd -n transitrelay --port tcp:4012
# (venv with twisted + autobahn; override the path as needed)
```

`twistd` forks by default CWD-pidfile: pass `--pidfile /tmp/py.pid
--logfile /tmp/py.log` when running repeated instances, or stale
`twistd.pid` files will make subsequent launches refuse to start (this
silently invalidated one early measurement run).

Then point `relay-stress` at both ports. Keep configurations matched
(same `--usage-db` on/off, same machine, interleaved runs).

## Baseline numbers (2026-11-16, before perf fixes)

Machine: 4 CPUs, loopback only, Go 1.26.8, Linux. Go relay built from this
tree; Python relay = twisted 26.10.0 / autobahn 26.11.1 on CPython 3.14.4,
epollreactor. Absolute numbers are machine-specific; the ratios are the
story.

### Microbenchmarks

```
BenchmarkRelayThroughput         522 MB/s   (256 KiB allocs ≈ 1/64 KiB relayed;
                                             ~8.4 MB B/op per 8 MiB transfer
                                             — the per-read defensive copy)
BenchmarkRelaySmallMessages      19.0 MB/s  (53.9 µs per 1 KiB ping-pong)
BenchmarkPairingRate/serial      967 ns/op  5 allocs
BenchmarkPairingRate/parallel    1.03 µs/op 5 allocs
BenchmarkUsageRecord/no_backends        169 ns/op
BenchmarkUsageRecord/sqlite_memory    43.5 µs/op   (≈250× the no-backend cost)
BenchmarkUpdateStats/active_100        3.8 µs/op
BenchmarkUpdateStats/active_1000      43.2 µs/op
BenchmarkUpdateStats/active_10000     407 µs/op    (~40 ns per active conn)
BenchmarkHandshake/regex_old     1.97 µs/op
BenchmarkHandshake/regex_new     2.77 µs/op
```

### Throughput (mode=throughput, 8 pairs × 16 MiB)

| Relay | Aggregate | Pair p50 |
|---|---|---|
| Go (this repo) | **669–682 MB/s** | ~175 ms |
| Python reference | 308–318 MB/s | ~405 ms |

≈2.1× the reference.

### Churn (mode=churn, 16 workers, 5 s)

| Relay | No `--usage-db` | With `--usage-db` |
|---|---|---|
| Go (this repo) | **4570–4790 pairs/s** | 3300–3390 pairs/s (−~30%) |
| Python reference | 1020–1180 pairs/s | 730–1130 pairs/s |

≈4.2–4.5× the reference without the usage DB; the synchronous per-connection
SQLite INSERT costs this relay ~30% of churn capacity — the single clearest
target for the async/batched usage-writer fix.

### Stall (mode=stall) — both relays behave the same today

```
go      sender blocked after 2.50 s with ~5.4 MB written; relay did not close the pipe
python  sender blocked after 2.50 s with ~3.9 MB written; relay did not close the pipe
```

Both lack backpressure handling / write deadlines: the sender jams once
kernel buffers fill and nothing ever recovers. This is expected pre-fix
behavior on both sides; re-run this after adding write deadlines/pumps to
verify the stuck pipe gets closed.

## Reproducing the A/B runs

```sh
go build -o /tmp/wormhole-relay-bin ./cmd/relay-server
go build -o /tmp/relay-stress ./cmd/relay-stress

/tmp/wormhole-relay-bin --port tcp:4011 &                                 # Go
( cd ../magic-wormhole-transit-relay-original && \
  PYTHONPATH=src /tmp/wormhole-venv/bin/twistd -n --pidfile /tmp/py.pid \
  --logfile /tmp/py.log transitrelay --port tcp:4012 ) &                  # Python

/tmp/relay-stress -addr 127.0.0.1:4011 -mode throughput -pairs 8 -bytes 16777216
/tmp/relay-stress -addr 127.0.0.1:4012 -mode throughput -pairs 8 -bytes 16777216
/tmp/relay-stress -addr 127.0.0.1:4011 -mode churn -pairs 16 -duration 5s
/tmp/relay-stress -addr 127.0.0.1:4012 -mode churn -pairs 16 -duration 5s
/tmp/relay-stress -addr 127.0.0.1:4011 -mode stall
/tmp/relay-stress -addr 127.0.0.1:4012 -mode stall
```

Verify the usage path end-to-end by adding `--usage-db /tmp/go.sqlite`
(Go) / `--usage-db /tmp/py.sqlite` (Python) and inspecting the `usage`
table (`select result, count(*) from usage group by result` — expect
`happy` rows from the churn runs).

## Fix → benchmark mapping

| Fix | Proof it worked |
|---|---|
| Drop per-read defensive copy | `BenchmarkRelayThroughput` MB/s and B/op improve; `-benchmem` allocs drop ~135/op |
| Write pump / write deadlines | `mode=stall` reports the relay closing the stuck pipe; throughput unchanged or better |
| WS write serialization + `recover` | WS stress passes without "concurrent write" panics |
| Async/batched usage DB writer | `BenchmarkUsageRecord` and `mode=churn` with `--usage-db`: −30% churn penalty shrinks |
| Accept-loop backoff | fd-exhaustion soak: flat CPU, bounded logs |
| `TCP_NODELAY` | `BenchmarkRelaySmallMessages` ns/op improves |
| Handshake buffer cap / WS read limit / server timeouts | flood/slowloris tests (future); memory bounded |
| `recordUsage` race fix | `go test -race` + WS/TCP mixed-pair test |

## Caveats

- 4-CPU box; both servers share it. Numbers are noisy (±5%); interleave
  runs and use `benchstat` for benchmarks.
- Loopback only — kernel buffers, Nagle and epoll behavior differ from a
  real network. No WS transit benchmark yet (gorilla framing not exercised
  by `relay-stress`); add one if the WS path gets optimized.
- The Python reference is included for context, not as a hard target; it is
  not expected to match Go on raw throughput.
