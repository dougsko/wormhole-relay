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

Captured on this 4-CPU box with loopback only, Go 1.26.8, Linux (Go relay
built from this tree; Python relay = twisted 26.10.0 / autobahn 26.11.1 on
CPython 3.14.4, epollreactor). **Capture conditions matter for reading
absolute MB/s figures**: the pre-fix baseline was taken with loadavg ≈ 1–2;
post-fix reruns ran under loadavg 6–14 (frigate, ollama, podman builds on
this machine). Treat capacity numbers as approximate; allocation counts and
interleaved A/B ratios are the robust comparisons.

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

### Stall (mode=stall)

Pre-fix (both relays):

```
go      sender blocked after 2.50 s with ~5.4 MB written; relay never closed the pipe
python  sender blocked after 2.50 s with ~3.9 MB written; relay never closed the pipe
```

Post-fix (Go relay): the relay's write to the stuck receiver hits
`writeTimeout` (30 s default, 300 ms in `write_timeout_test.go`) and the
relay tears the whole pair down — `write_timeout_test.go` verifies teardown
and usage recording. The Python reference is unchanged.

## Post-fix numbers (2026-11-16, after the review fixes)

What changed relative to baseline, commit by commit (see `git log`):

- **Defensive 64 KiB copy per read removed + handshake slices zero-copy**
  (`c9c778e`): `BenchmarkRelayThroughput` dropped from ~8.4 MB allocs and
  ~135 allocs per 8 MiB transfer to **~200 B and 5 allocs**; MB/s improved
  522 → 837 on a quiet box (≈60%). Small-message MB/s 19 → 22.6.
- **Accepted conns get TCP_NODELAY + WS endpoint moved to explicit
  http.Server with timeouts + accept-loop backoff** (`0ca0c4b`).
- **FD leak closed** (`ee2bf58`): teardown now closes the relay-side
  socket (regression tests: `connclose_test.go`).
- **Usage recording moved off the teardown path, batched in one tx**
  (`eb9b772`): `Record` costs **~550 ns** on the caller (was ~43.5 µs
  against SQLite — ~80× cheaper); batch of 64 amortizes to **~14–15 µs**
  per event. No churn regression vs pre-async build in interleaved runs
  under loadavg ≈ 1–15 (means statistically identical; the earlier −30%
  with `--usage-db` was measured on a quieter box and is expected to
  improve correspondingly — the teardown-path INSERT no longer exists).
- **Write deadlines (30 s data / 5 s close frame) + WS write mutex +
  panic isolation** (`d3da747`, race fix `80fd0c9`): a stuck receiver now
  tears the pair down at the write timeout instead of leaking goroutines;
  see `write_timeout_test.go` (0.3 s teardown with a 300 ms test timeout).

### Interleaved A/B under the same (loaded) machine

Throughput (8 × 16 MiB pairs), Go vs Python reference, alternating runs:
Go pair p50 ≈ **125–148 ms** vs Python ≈ **478–517 ms** (~3.5×).
Churn (16 workers): Go **≈ 2200 pairs/s** vs Python ≈ **870 pairs/s** (~2.5×).
Go relay with `--usage-db` keeps `happy` records flowing (11 870 `happy`
rows from one 5 s churn run).

### Current microbenchmark snapshot (loadavg ≈ 6–14; absolute MB/s deflated)

```
BenchmarkRelayThroughput              ~515–600 MB/s   178–262 B/op   5 allocs
BenchmarkRelaySmallMessages           18.7 MB/s       0 B/op         0 allocs
BenchmarkUsageRecord/no_backends         131 ns/op
BenchmarkUsageRecord/sqlite_memory       367 ns/op   (enqueue; worker drains concurrently)
BenchmarkUsageRecord/sqlite_memory_batch64  14.3 µs/op (≈ production shape)
BenchmarkUpdateStats/active_10000       472 µs/op    (~47 ns per active conn)
```

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

## Fix → benchmark mapping (all items now implemented)

| Fix | Proof it worked | Status |
|---|---|---|
| Drop per-read defensive copy | `BenchmarkRelayThroughput` allocs 135 → 5 per op; bytes 8.4 MB → ~200 B; MB/s 522 → 837 (quiet box) | done (`c9c778e`) |
| Write pump / write deadlines | `write_timeout_test.go`: stuck pair self-closes at the write deadline; teardown ~0.3 s with 300 ms override. (Chose deadline+self-close over a pump: a pump can't fix per-direction head-of-line blocking either, and it adds per-conn buffer management.) | done (`d3da747`) |
| WS write serialization + `recover` | WS write mutex added; per-connection panic isolation at both entry points; WS tests + `-race` clean | done (`d3da747`) |
| Async/batched usage DB writer | `BenchmarkUsageRecord`: Record ≈ 550 ns (was 43.5 µs); `sqlite_memory_batch64` ≈ 14.3 µs/op amortized | done (`eb9b772`) |
| Accept-loop backoff | `0ca0c4b`: exponential backoff 5 ms→1 s on TCP accept errors; fatal only if the listener closes; WS path uses `http.Serve`'s built-in backoff | done |
| `TCP_NODELAY` | `noDelayListener` wraps both listeners; `BenchmarkRelaySmallMessages` improved (19 → 22.6 MB/s on a quiet box) | done (`0ca0c4b`) |
| Handshake buffer cap, WS read limit, server timeouts | `handshake_bounds_test.go`, `WS SetReadLimit` (4 MiB), `ReadHeaderTimeout` on the WS `http.Server` | done (`c9c778e`, `0ca0c4b`) |
| `recordUsage` race fix | `buddyStats()` interface method with mutex; new `pair_race_test.go` clean under `-race -count=5` | done (`db34544`) |
| (found during fixes) relay-side FD leak in `connectionLost` | closed; `connclose_test.go` asserts `net.ErrClosed` semantics, negative-controlled | done (`ee2bf58`) |

## Caveats

- This box runs frigate, ollama and podman builds; loadavg oscillates
  between ~1 and ~15. Absolute MB/s numbers are not comparable across
  runs weeks apart; use `benchstat` on interleaved runs, or compare
  allocs/op (machine-independent) and ratios from interleaved A/B.
- Loopback only — kernel buffers, Nagle and epoll behavior differ from a
  real network. No WS transit benchmark yet (gorilla framing not exercised
  by `relay-stress`); add one if the WS path gets optimized.
- The Python reference is included for context, not as a hard target; it is
  not expected to match Go on raw throughput.
- `mode=stall` measures at the client side; the authoritative check for
  the stuck-pipe fix is `write_timeout_test.go`, which has deterministic
  control over `writeTimeout`.
