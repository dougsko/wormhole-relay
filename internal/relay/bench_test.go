package relay

// Benchmarks for the relay hot paths. Run with:
//
//	go test ./internal/relay -bench=. -benchmem -run=XXX
//
// The networking benchmarks use a real loopback TCP listener and a real
// relayed pair of client connections, so they measure the full path
// (read -> state machine -> buddy write) rather than isolated pieces.
// Loopback numbers are dominated by kernel buffers; use them for relative
// before/after comparison, not as absolute capacity figures.

import (
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// discardUsageRecorder drops all usage events.
type discardUsageRecorder struct{}

func (discardUsageRecorder) Record(started, buddyStarted time.Time, mood Mood, bytesSent, buddyBytes int) {
}

func (discardUsageRecorder) UpdateStats(rebooted, updated time.Time, connected, waiting, incompleteBytes int) {
}

// benchPendingClient is a no-op PendingClient for registry benchmarks. It
// carries an id so distinct instances are not the same (zero-size-struct)
// pointer in map keys.
type benchPendingClient struct{ id uint64 }

func (benchPendingClient) partnerConnectionLost()         {}
func (benchPendingClient) gotPartner(other PendingClient) {}
func (benchPendingClient) buddyStats() (time.Time, int)   { return time.Time{}, 0 }

// setupRelayBenchServer starts a TransitServer on a loopback listener and
// returns its address. Callers dial pairs of clients and handshake them.
func setupRelayBenchServer(b *testing.B) (string, func()) {
	b.Helper()
	server := NewTransitServer(discardUsageRecorder{}, func() time.Time { return time.Now() })
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go HandleTCPConnection(conn, server)
		}
	}()
	return listener.Addr().String(), func() { listener.Close() }
}

const benchToken = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

// dialRelayPair dials two clients against addr, performs the handshake on
// both, waits for "ok\n" on each, and returns the connected pair.
func dialRelayPair(b *testing.B, addr string) (net.Conn, net.Conn) {
	b.Helper()
	c1, err := net.Dial("tcp", addr)
	if err != nil {
		b.Fatal(err)
	}
	c2, err := net.Dial("tcp", addr)
	if err != nil {
		c1.Close()
		b.Fatal(err)
	}
	for _, c := range []net.Conn{c1, c2} {
		if _, err := io.WriteString(c, "please relay "+benchToken+"\n"); err != nil {
			b.Fatal(err)
		}
	}
	ok := make([]byte, 3)
	for _, c := range []net.Conn{c1, c2} {
		c.SetReadDeadline(time.Now().Add(5 * time.Second))
		if _, err := io.ReadFull(c, ok); err != nil {
			b.Fatalf("waiting for ok: %v", err)
		}
		if string(ok) != "ok\n" {
			b.Fatalf("expected ok\\n, got %q", ok)
		}
		c.SetReadDeadline(time.Time{})
	}
	return c1, c2
}

// BenchmarkRelayThroughput measures sustained bulk transfer through a relayed
// pair (one direction). b.SetBytes reports MB/s.
func BenchmarkRelayThroughput(b *testing.B) {
	addr, cleanup := setupRelayBenchServer(b)
	defer cleanup()

	c1, c2 := dialRelayPair(b, addr)
	defer c1.Close()
	defer c2.Close()

	const transferSize = 8 << 20 // 8 MiB per iteration
	payload := make([]byte, 256<<10)
	sink := make([]byte, 256<<10)

	b.SetBytes(transferSize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		wg.Add(2)
		var readErr, writeErr error
		go func() {
			defer wg.Done()
			remaining := int64(transferSize)
			for remaining > 0 {
				n := int64(len(payload))
				if remaining < n {
					n = remaining
				}
				wn, err := c1.Write(payload[:n])
				if err != nil {
					writeErr = err
					return
				}
				remaining -= int64(wn)
			}
		}()
		go func() {
			defer wg.Done()
			remaining := int64(transferSize)
			for remaining > 0 {
				n, err := c2.Read(sink)
				if err != nil {
					readErr = err
					return
				}
				remaining -= int64(n)
			}
		}()
		wg.Wait()
		if writeErr != nil {
			b.Fatal(writeErr)
		}
		if readErr != nil {
			b.Fatal(readErr)
		}
	}
}

// BenchmarkRelaySmallMessages measures ping-pong latency of 1 KiB messages
// (sensitive to Nagle/delayed-ACK and per-message overhead).
func BenchmarkRelaySmallMessages(b *testing.B) {
	addr, cleanup := setupRelayBenchServer(b)
	defer cleanup()

	c1, c2 := dialRelayPair(b, addr)
	defer c1.Close()
	defer c2.Close()

	msg := make([]byte, 1024)
	sink := make([]byte, 1024)
	b.SetBytes(int64(len(msg)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := c1.Write(msg); err != nil {
			b.Fatal(err)
		}
		if _, err := io.ReadFull(c2, sink); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkPairingRate measures the cost of registering and unregistering
// pending requests (the churn-critical registry path).
func BenchmarkPairingRate(b *testing.B) {
	b.Run("serial", func(b *testing.B) {
		active := NewActiveConnections()
		pr := NewPendingRequests(active)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			token := fmt.Sprintf("%064x", i)
			pr.Register(token, "", &benchPendingClient{id: uint64(i)})
			pr.Unregister(token, "", &benchPendingClient{id: uint64(i)})
		}
	})
	b.Run("parallel", func(b *testing.B) {
		active := NewActiveConnections()
		pr := NewPendingRequests(active)
		var counter atomic.Uint64
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				n := counter.Add(1)
				token := fmt.Sprintf("%064x", n)
				pr.Register(token, "", &benchPendingClient{id: n})
				pr.Unregister(token, "", &benchPendingClient{id: n})
			}
		})
	})
}

// BenchmarkUsageRecord measures per-connection usage recording with different
// backend configurations, isolating the SQLite INSERT cost from the rest of
// the record path.
func BenchmarkUsageRecord(b *testing.B) {
	started := time.Now()
	b.Run("no_backends", func(b *testing.B) {
		tracker, err := CreateUsageTracker(0, "", "")
		if err != nil {
			b.Fatal(err)
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			tracker.Record(started, started, MoodHappy, 12345, 54321)
		}
	})
	b.Run("sqlite_memory", func(b *testing.B) {
		tracker, err := CreateUsageTracker(0, "", ":memory:")
		if err != nil {
			b.Fatal(err)
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			tracker.Record(started, started, MoodHappy, 12345, 54321)
		}
	})
	b.Run("no_backends_blurred", func(b *testing.B) {
		tracker, err := CreateUsageTracker(3600, "", "")
		if err != nil {
			b.Fatal(err)
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			tracker.Record(started, started, MoodHappy, 12345, 54321)
		}
	})
}

// BenchmarkUpdateStats measures the cost of the periodic stats sweep with a
// realistic number of active connections.
func BenchmarkUpdateStats(b *testing.B) {
	for _, numActive := range []int{100, 1000, 10000} {
		b.Run(fmt.Sprintf("active_%d", numActive), func(b *testing.B) {
			server := NewTransitServer(discardUsageRecorder{}, func() time.Time { return time.Now() })
			// distinct pointer identities; a shared value would dedup in the map
			for i := 0; i < numActive; i++ {
				server.activeConnections.Register(&benchPendingClient{id: uint64(i * 2)}, &benchPendingClient{id: uint64(i*2 + 1)})
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				server.UpdateStats()
			}
		})
	}
}

// BenchmarkHandshake registers pending requests the way HandleTCPConnection
// does once the first handshake line is parsed (regex + registry).
func BenchmarkHandshake(b *testing.B) {
	b.Run("regex_old", func(b *testing.B) {
		line := []byte("please relay " + strings.Repeat("a", 64))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = oldRelayRe.FindSubmatch(line)
		}
	})
	b.Run("regex_new", func(b *testing.B) {
		line := []byte("please relay " + strings.Repeat("a", 64) + " for side " + strings.Repeat("b", 16))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = newRelayRe.FindSubmatch(line)
		}
	})
}
