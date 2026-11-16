package relay

// Tests for the pre-handshake resource bounds: overlong handshakes are
// rejected and idle pre-handshake connections are reaped by a read
// deadline.

import (
	"bufio"
	"net"
	"testing"
	"time"
)

func TestHandshakeTooLong(t *testing.T) {
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

	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	// send more than maxHandshakeBytes with no newline
	_, err = conn.Write(make([]byte, maxHandshakeBytes+1))
	if err != nil {
		t.Fatal(err)
	}

	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	response, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		t.Fatalf("expected bad handshake response: %v", err)
	}
	if response != "bad handshake\n" {
		t.Fatalf("expected %q, got %q", "bad handshake\n", response)
	}
}

func TestPreHandshakeReadTimeout(t *testing.T) {
	oldTimeout := preHandshakeReadTimeout
	preHandshakeReadTimeout = 100 * time.Millisecond
	defer func() { preHandshakeReadTimeout = oldTimeout }()

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

	// connect, send nothing, expect the relay to close the connection
	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, err = conn.Read(make([]byte, 1))
	if err == nil {
		t.Fatal("expected the relay to close an idle pre-handshake connection")
	}
	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		t.Fatal("relay did not close the idle connection within the test window")
	}

	// and it is recorded as an empty-mood usage event
	deadline := time.Now().Add(time.Second)
	for len(usage.snapshot()) == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	events := usage.snapshot()
	if len(events) != 1 {
		t.Fatalf("expected 1 usage record, got %d", len(events))
	}
	if events[0].mood != MoodEmpty {
		t.Fatalf("expected mood=empty, got %q", events[0].mood)
	}
}
