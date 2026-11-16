package relay

// Write-path behavior: a stuck receiver must eventually cause the relay
// to tear down the pair (via writeTimeout), not leak the goroutines.

import (
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestWriteTimeoutTearsDownStuckPair(t *testing.T) {
	oldTimeout := writeTimeout
	writeTimeout = 300 * time.Millisecond
	defer func() { writeTimeout = oldTimeout }()

	usage := &testUsageRecorder{}
	server := NewTransitServer(usage, func() time.Time { return time.Now() })

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go HandleTCPConnection(conn, server)
		}
	}()

	token := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	c1, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c1.Close()
	c2, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	// c2 never reads: this is the stuck receiver.
	for _, c := range []net.Conn{c1, c2} {
		if _, err := io.WriteString(c, "please relay "+token+"\n"); err != nil {
			t.Fatal(err)
		}
	}
	for _, c := range []net.Conn{c1, c2} {
		ok := make([]byte, 3)
		_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
		if _, err := io.ReadFull(c, ok); err != nil {
			t.Fatal(err)
		}
		_ = c.SetReadDeadline(time.Time{})
	}

	payload := make([]byte, 64<<10)
	var writeErr atomic.Value
	var writerWG sync.WaitGroup
	writerWG.Add(1)
	go func() {
		defer writerWG.Done()
		for {
			_, err := c1.Write(payload)
			if err != nil {
				writeErr.Store(err)
				return
			}
		}
	}()

	// once the relay's buffered write to c2 exceeds writeTimeout, the
	// relay closes both conns and c1's write fails; that's the signal
	finished := make(chan struct{})
	go func() {
		writerWG.Wait()
		close(finished)
	}()

	select {
	case <-finished:
		if err := writeErr.Load(); err == nil {
			t.Fatal("expected the writer to observe a write error after relay teardown")
		}
	case <-time.After(15 * time.Second):
		t.Fatal("relay did not tear down the stuck pair within 15s")
	}

	// teardown recorded at least one usage event
	deadline := time.Now().Add(2 * time.Second)
	for len(usage.snapshot()) == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if len(usage.snapshot()) == 0 {
		t.Fatal("expected usage records after stuck-pair teardown")
	}
}
