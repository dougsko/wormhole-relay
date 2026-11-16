// relay-stress is a load generator for transit relays. It speaks the wire
// protocol directly (handshake + byte pipe), so it can exercise any
// implementation — the Go relay in this repository, the Python reference
// (magic-wormhole-transit-relay), or a production relay.
//
// Modes:
//
//	throughput  open -pairs concurrent pairs, push -bytes through each,
//	            report aggregate MB/s and per-pair latency percentiles
//	churn       connect/handshake/close as fast as possible for -duration
//	            seconds, report completed pairs/sec (pairing-churn load)
//	stall       one pair; the receiver never reads; observe whether the
//	            relay applies backpressure / closes the stuck pipe
//	            (behavior changed by write-deadline fixes)
//
// Example:
//
//	relay-stress -addr 127.0.0.1:4001 -mode throughput -pairs 8 -bytes 16777216
//
// Exit code is non-zero if any pair errors out.
package main

import (
	"crypto/rand"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"sort"
	"sync"
	"time"
)

var (
	addr        = flag.String("addr", "127.0.0.1:4001", "relay address (host:port)")
	mode        = flag.String("mode", "throughput", "throughput | churn | stall")
	pairs       = flag.Int("pairs", 8, "concurrent pairs (throughput, churn)")
	bytesPer    = flag.Int64("bytes", 16<<20, "bytes to relay per pair (throughput)")
	chunk       = flag.Int("chunk", 64<<10, "write chunk size (throughput)")
	duration    = flag.Duration("duration", 10*time.Second, "run duration (churn)")
	stallWait   = flag.Duration("stall-timeout", 15*time.Second, "how long to watch the stalled pipe (stall)")
	dialTimeout = flag.Duration("dial-timeout", 5*time.Second, "TCP dial/handshake timeout")
)

func randomToken() string {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		panic(err)
	}
	return fmt.Sprintf("%x", buf)
}

// handshake dials both ends of a pair, performs the token handshake on each,
// and waits for "ok\n". Returns the two connections.
func handshake(addr, token string) (net.Conn, net.Conn, error) {
	dialer := net.Dialer{Timeout: *dialTimeout}

	c1, err := dialer.Dial("tcp", addr)
	if err != nil {
		return nil, nil, fmt.Errorf("dial side 1: %w", err)
	}
	c2, err := dialer.Dial("tcp", addr)
	if err != nil {
		c1.Close()
		return nil, nil, fmt.Errorf("dial side 2: %w", err)
	}
	line := "please relay " + token + "\n"
	for _, c := range []net.Conn{c1, c2} {
		c.SetDeadline(time.Now().Add(*dialTimeout))
		if _, err := io.WriteString(c, line); err != nil {
			c1.Close()
			c2.Close()
			return nil, nil, fmt.Errorf("handshake write: %w", err)
		}
	}
	for _, c := range []net.Conn{c1, c2} {
		ok := make([]byte, 3)
		if _, err := io.ReadFull(c, ok); err != nil {
			c1.Close()
			c2.Close()
			return nil, nil, fmt.Errorf("reading ok: %w", err)
		}
		if string(ok) != "ok\n" {
			c1.Close()
			c2.Close()
			return nil, nil, fmt.Errorf("unexpected handshake response %q", ok)
		}
		c.SetDeadline(time.Time{})
	}
	return c1, c2, nil
}

func isTimeout(err error) bool {
	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		return true
	}
	return false
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(p * float64(len(sorted)-1))
	return sorted[idx]
}

func runThroughput() int {
	payload := make([]byte, *chunk)
	if _, err := rand.Read(payload); err != nil {
		panic(err)
	}
	sink := make([]byte, 128<<10)

	var mu sync.Mutex
	var durations []time.Duration
	var totalBytes int64
	var errCount int

	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < *pairs; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			pairStart := time.Now()
			c1, c2, err := handshake(*addr, randomToken())
			if err != nil {
				mu.Lock()
				errCount++
				mu.Unlock()
				fmt.Fprintf(os.Stderr, "pair %d: %v\n", id, err)
				return
			}
			defer c1.Close()
			defer c2.Close()

			var pairWG sync.WaitGroup
			pairWG.Add(2)
			var readErr, writeErr error
			go func() {
				defer pairWG.Done()
				remaining := *bytesPer
				for remaining > 0 {
					n := int64(len(payload))
					if remaining < n {
						n = remaining
					}
					c1.SetWriteDeadline(time.Now().Add(120 * time.Second))
					wn, err := c1.Write(payload[:n])
					if err != nil {
						writeErr = err
						return
					}
					remaining -= int64(wn)
				}
			}()
			go func() {
				defer pairWG.Done()
				remaining := *bytesPer
				for remaining > 0 {
					c2.SetReadDeadline(time.Now().Add(120 * time.Second))
					n, err := c2.Read(sink)
					if err != nil {
						readErr = err
						return
					}
					remaining -= int64(n)
				}
			}()
			pairWG.Wait()

			mu.Lock()
			switch {
			case writeErr != nil:
				errCount++
				fmt.Fprintf(os.Stderr, "pair %d write: %v\n", id, writeErr)
			case readErr != nil:
				errCount++
				fmt.Fprintf(os.Stderr, "pair %d read: %v\n", id, readErr)
			default:
				durations = append(durations, time.Since(pairStart))
				totalBytes += *bytesPer
			}
			mu.Unlock()
		}(i)
	}
	wg.Wait()
	wall := time.Since(start)

	sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })

	fmt.Printf("mode=throughput addr=%s pairs=%d bytes_per_pair=%d wall=%s\n", *addr, *pairs, *bytesPer, wall.Round(time.Millisecond))
	fmt.Printf("aggregate: %.1f MB/s (%d bytes in %s), errors=%d\n",
		float64(totalBytes)/wall.Seconds()/(1<<20), totalBytes, wall.Round(time.Millisecond), errCount)
	if len(durations) > 0 {
		fmt.Printf("pair latency: min=%s p50=%s p99=%s max=%s\n",
			durations[0].Round(time.Millisecond),
			percentile(durations, 0.50).Round(time.Millisecond),
			percentile(durations, 0.99).Round(time.Millisecond),
			durations[len(durations)-1].Round(time.Millisecond))
	}
	if errCount > 0 {
		return 1
	}
	return 0
}

func runChurn() int {
	deadline := time.Now().Add(*duration)
	var mu sync.Mutex
	var completed, errCount int64

	var wg sync.WaitGroup
	for i := 0; i < *pairs; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(deadline) {
				c1, c2, err := handshake(*addr, randomToken())
				if err != nil {
					mu.Lock()
					errCount++
					mu.Unlock()
					continue
				}
				c1.Close()
				c2.Close()
				mu.Lock()
				completed++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	rate := float64(completed) / duration.Seconds()
	fmt.Printf("mode=churn addr=%s workers=%d duration=%s\n", *addr, *pairs, *duration)
	fmt.Printf("completed pairs=%d rate=%.1f pairs/s errors=%d\n", completed, rate, errCount)
	if errCount > 0 {
		return 1
	}
	return 0
}

func runStall() int {
	c1, c2, err := handshake(*addr, randomToken())
	if err != nil {
		fmt.Fprintf(os.Stderr, "handshake: %v\n", err)
		return 1
	}
	defer c1.Close()
	defer c2.Close()
	// c2 deliberately never reads: this is the stuck-receiver scenario.

	payload := make([]byte, 64<<10)
	rand.Read(payload)

	start := time.Now()
	var written int64
	blockedAt := time.Duration(-1)
	closedByRelay := false
	for time.Since(start) < *stallWait {
		c1.SetWriteDeadline(time.Now().Add(500 * time.Millisecond))
		n, err := c1.Write(payload)
		written += int64(n)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				// Write-side stalled: are we connected to a dead-but-open
				// pipe, or did the relay close on us? A real remote close
				// yields EOF/ECONNRESET; our own read deadline expiring
				// means the pipe is still open.
				c1.SetReadDeadline(time.Now().Add(2 * time.Second))
				one := make([]byte, 1)
				_, rerr := c1.Read(one)
				blockedAt = time.Since(start)
				if rerr != nil {
					if isTimeout(rerr) {
						break // pipe still open; relay did not close it
					}
					closedByRelay = true
					fmt.Printf("mode=stall addr=%s\n", *addr)
					fmt.Printf("relay closed the stuck pipe after ~%d bytes (blocked after %s, read err: %v)\n", written, blockedAt.Round(time.Millisecond), rerr)
					return 0
				}
				break
			}
			// hard error: relay closed or reset the connection
			fmt.Printf("mode=stall addr=%s\n", *addr)
			fmt.Printf("relay closed/reset the pipe after ~%d bytes (write err: %v, after %s)\n", written, err, time.Since(start).Round(time.Millisecond))
			return 0
		}
	}

	if blockedAt < 0 {
		blockedAt = time.Since(start)
	}
	fmt.Printf("mode=stall addr=%s\n", *addr)
	if closedByRelay {
		// unreachable today; kept for clarity
		return 0
	}
	fmt.Printf("sender still blocked after %s with ~%d bytes written; relay did not close the pipe within %s\n",
		blockedAt.Round(time.Millisecond), written, *stallWait)
	fmt.Printf("(this is the expected outcome for a relay without write deadlines / backpressure handling)\n")
	return 0
}

func main() {
	flag.Parse()
	var code int
	switch *mode {
	case "throughput":
		code = runThroughput()
	case "churn":
		code = runChurn()
	case "stall":
		code = runStall()
	default:
		fmt.Fprintf(os.Stderr, "unknown mode %q\n", *mode)
		code = 2
	}
	os.Exit(code)
}
