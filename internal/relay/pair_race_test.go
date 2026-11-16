package relay

// Stress the relay pair and the teardown path concurrently; most
// valuable under -race.

import (
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

func TestConcurrentPairExchangeAndTeardown(t *testing.T) {
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

	token := fmt.Sprintf("%064x", 42)
	c1, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	c2, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	// both handshakes first, then both "ok\n" responses
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

	payload := make([]byte, 16<<10)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	reader := func(c net.Conn) {
		defer wg.Done()
		buf := make([]byte, 64<<10)
		for {
			select {
			case <-stop:
				return
			default:
			}
			_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
			if _, err := c.Read(buf); err != nil {
				return
			}
		}
	}
	wg.Add(2)
	go reader(c1)
	go reader(c2)

	// both sides write continuously until told to stop
	for _, c := range []net.Conn{c1, c2} {
		go func(c net.Conn) {
			for {
				select {
				case <-stop:
					return
				default:
				}
				_ = c.SetWriteDeadline(time.Now().Add(5 * time.Second))
				if _, err := c.Write(payload); err != nil {
					return
				}
			}
		}(c)
	}

	time.Sleep(150 * time.Millisecond)
	// teardown from both ends at once: this is where the recordUsage
	// buddy-field race used to bite
	_ = c1.Close()
	_ = c2.Close()
	close(stop)
	wg.Wait()

	deadline := time.Now().Add(2 * time.Second)
	for len(usage.snapshot()) == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if len(usage.snapshot()) == 0 {
		t.Fatal("expected at least one usage record after teardown")
	}
}
