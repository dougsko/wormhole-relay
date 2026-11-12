package relay

import (
	"bytes"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// TestNewTransitServer ensures the server is created properly
func TestNewTransitServer(t *testing.T) {
	usage := &testUsageRecorder{}
	server := NewTransitServer(usage, func() time.Time { return time.Now() })
	if server == nil {
		t.Fatal("expected server")
	}
	if server.activeConnections == nil {
		t.Fatal("expected active connections")
	}
	if server.pendingRequests == nil {
		t.Fatal("expected pending requests")
	}
}

// TestPendingRequestsRegister ensures token registration works
func TestPendingRequestsRegister(t *testing.T) {
	active := NewActiveConnections()
	pr := NewPendingRequests(active)

	usage := &testUsageRecorder{}
	server := NewTransitServer(usage, func() time.Time { return time.Now() })
	tc := &TransitConnection{server: server}
	// First side should return true
	if !pr.Register("token1234567890abcdef", "", tc) {
		t.Fatal("expected first side to return true")
	}
	// Second side should return false
	tc2 := &TransitConnection{server: server}
	if pr.Register("token1234567890abcdef", "", tc2) {
		t.Fatal("expected second side to return false")
	}
}

// TestPendingRequestsMatchingSides ensures side matching logic
func TestPendingRequestsMatchingSides(t *testing.T) {
	usage := &testUsageRecorder{}
	server := NewTransitServer(usage, func() time.Time { return time.Now() })
	active := NewActiveConnections()
	pr := NewPendingRequests(active)

	tc1 := &TransitConnection{server: server}
	tc2 := &TransitConnection{server: server}
	tc3 := &TransitConnection{server: server}

	// Register with side A
	if !pr.Register("token", "sideA", tc1) {
		t.Fatal("first side should return true")
	}
	// Register with side B - should match
	if pr.Register("token", "sideB", tc2) {
		t.Fatal("matching side should return false (matched)")
	}
	// Side A should be gone now
	if pr.Count() != 0 {
		t.Fatal("matched token should be removed")
	}
	_ = tc3
}

// TestPendingRequestsRedundantSides ensures redundant sides are dropped
func TestPendingRequestsRedundantSides(t *testing.T) {
	usage := &testUsageRecorder{}
	server := NewTransitServer(usage, func() time.Time { return time.Now() })
	active := NewActiveConnections()
	pr := NewPendingRequests(active)

	tc1 := &TransitConnection{server: server}
	tc2 := &TransitConnection{server: server}

	// Register side A twice (two TCP connections from the same side)
	pr.Register("token", "sideA", tc1)
	pr.Register("token", "sideA", tc2) // redundant

	// Match from the other side
	tc3 := &TransitConnection{server: server}
	pr.Register("token", "sideB", tc3)

	// Only one pair should be matched; count should be 0
	if pr.Count() != 0 {
		t.Fatalf("expected 0 pending after match, got %d", pr.Count())
	}
	if active.Count() != 2 {
		t.Fatalf("expected 2 active connections, got %d", active.Count())
	}
}

// TestTransitConnectionBadToken ensures bad handshake handling
func TestTransitConnectionBadToken(t *testing.T) {
	usage := &testUsageRecorder{}
	server := NewTransitServer(usage, func() time.Time { return time.Now() })

	// Simulate a bad line
	tc := &TransitConnection{
		server: server,
		state:  StateWaitRelay,
	}
	tc.handleLine([]byte("garbage"))

	if tc.mood != MoodErrory {
		t.Fatalf("expected mood errory, got %s", tc.mood)
	}
	if tc.state != StateDone {
		t.Fatalf("expected state done, got %d", tc.state)
	}
}

// TestTransitConnectionPleaseRelay ensures handshake handling
func TestTransitConnectionPleaseRelay(t *testing.T) {
	usage := &testUsageRecorder{}
	server := NewTransitServer(usage, func() time.Time { return time.Now() })

	tc := &TransitConnection{
		server: server,
		state:  StateWaitRelay,
	}
	token := "0000000000000000000000000000000000000000000000000000000000000000"
	tc.handleLine([]byte("please relay " + token))

	if tc.token != token {
		t.Fatalf("expected token set")
	}
	if tc.state != StateWaitPartner {
		t.Fatalf("expected wait partner state, got %d", tc.state)
	}
}

// TestTransitConnectionPleaseRelayForSide ensures handshake with side works
func TestTransitConnectionPleaseRelayForSide(t *testing.T) {
	usage := &testUsageRecorder{}
	server := NewTransitServer(usage, func() time.Time { return time.Now() })

	tc := &TransitConnection{
		server: server,
		state:  StateWaitRelay,
	}
	token := "0000000000000000000000000000000000000000000000000000000000000000"
	side := "0123456789abcdef"
	tc.handleLine([]byte("please relay " + token + " for side " + side))

	if tc.token != token {
		t.Fatalf("expected token set")
	}
	if tc.side != PendingSide(side) {
		t.Fatalf("expected side %s, got %s", side, tc.side)
	}
	if tc.state != StateWaitPartner {
		t.Fatalf("expected wait partner state, got %d", tc.state)
	}
}

// TestActiveConnections ensures active connection registration works
func TestActiveConnections(t *testing.T) {
	tc1 := &TransitConnection{}
	tc2 := &TransitConnection{}
	ac := NewActiveConnections()

	ac.Register(tc1, tc2)
	if ac.Count() != 2 {
		t.Fatalf("expected 2 active connections, got %d", ac.Count())
	}
	ac.Unregister(tc1)
	if ac.Count() != 1 {
		t.Fatalf("expected 1 active connection, got %d", ac.Count())
	}
}

// TestUpdateStats ensures stats update works
func TestUpdateStats(t *testing.T) {
	usage := &testUsageRecorder{}
	server := NewTransitServer(usage, func() time.Time { return time.Now() })
	server.UpdateStats()
}

func TestRelayRegexes(t *testing.T) {
	// Legacy form
	if !oldRelayRe.MatchString("please relay " + string(bytes.Repeat([]byte("a"), 64))) {
		t.Fatal("old regex should match 64 word chars")
	}
	if oldRelayRe.MatchString("please relay short") {
		t.Fatal("old regex should not match short token")
	}
	// New form
	valid := "please relay " + string(bytes.Repeat([]byte("b"), 64)) + " for side " + string(bytes.Repeat([]byte("c"), 16))
	if !newRelayRe.MatchString(valid) {
		t.Fatal("new regex should match")
	}
	if newRelayRe.MatchString("please relay " + string(bytes.Repeat([]byte("b"), 64)) + " for side " + string(bytes.Repeat([]byte("c"), 15))) {
		t.Fatal("new regex should not match 15-char side")
	}
}

// testUsageEvent captures a recorded usage event for test assertions
type testUsageEvent struct {
	mood       Mood
	bytesSent  int
	buddyBytes int
}

// testUsageRecorder is a simple usage recorder for tests
type testUsageRecorder struct {
	mu     sync.Mutex
	events []testUsageEvent
}

func (r *testUsageRecorder) Record(started, buddyStarted time.Time, mood Mood, bytesSent, buddyBytes int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, testUsageEvent{mood: mood, bytesSent: bytesSent, buddyBytes: buddyBytes})
}

func (r *testUsageRecorder) UpdateStats(rebooted, updated time.Time, connected, waiting, incompleteBytes int) {
}

func (r *testUsageRecorder) snapshot() []testUsageEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]testUsageEvent{}, r.events...)
}

// TestTCPRelayEndToEnd tests the full relay flow with real TCP sockets
func TestTCPRelayEndToEnd(t *testing.T) {
	usage := &testUsageRecorder{}
	server := NewTransitServer(usage, func() time.Time { return time.Now() })

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	addr := listener.Addr().String()

	// Accept exactly 2 connections in background
	acceptDone := make(chan struct{}, 2)
	go func() {
		for i := 0; i < 2; i++ {
			conn, err := listener.Accept()
			if err == nil {
				acceptDone <- struct{}{}
				go func(c net.Conn) {
					tc := NewTransitConnection(c, server)
					defer tc.connectionLost()
					tc.Handle()
				}(conn)
			}
		}
	}()

	// Dial two clients
	token := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

	client1, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	client2, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}

	<-acceptDone
	<-acceptDone

	// Client 1 handshake
	client1.Write([]byte("please relay " + token + "\n"))
	// Client 2 handshake
	client2.Write([]byte("please relay " + token + "\n"))

	// Both should receive "ok\n"
	buf := make([]byte, 3)
	client1.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, err = client1.Read(buf)
	if err != nil {
		t.Fatalf("client1 read ok: %v", err)
	}
	if string(buf) != "ok\n" {
		t.Fatalf("expected ok\\n, got %q", buf)
	}

	client2.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, err = client2.Read(buf)
	if err != nil {
		t.Fatalf("client2 read ok: %v", err)
	}
	if string(buf) != "ok\n" {
		t.Fatalf("expected ok\\n, got %q", buf)
	}

	// Send data between them
	msg := []byte("hello from client1\n")
	client1.Write(msg)

	buf2 := make([]byte, len(msg))
	client2.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, err = client2.Read(buf2)
	if err != nil {
		t.Fatalf("client2 read msg: %v", err)
	}
	if string(buf2) != string(msg) {
		t.Fatalf("expected %q, got %q", msg, buf2)
	}

	client1.Close()
	client2.Close()
}

// TestTCPImpatient tests that a client which sends data after its handshake
// but before a partner appears receives "impatient\n" and is disconnected.
func TestTCPImpatient(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	addr := listener.Addr().String()

	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		tc := NewTransitConnection(conn, NewTransitServer(&testUsageRecorder{}, func() time.Time { return time.Now() }))
		tc.Handle()
	}()

	client, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	// Send handshake followed by extra data in the same write, simulating
	// a client that does not wait for the handshake to be acknowledged.
	token := strings.Repeat("e", 64)
	handshake := "please relay " + token + "\n"
	_, err = client.Write([]byte(handshake + "extra bytes before partner\n"))
	if err != nil {
		t.Fatal(err)
	}

	client.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 64)
	n, err := client.Read(buf)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(buf[:n]) != "impatient\n" {
		t.Fatalf("expected %q, got %q", "impatient\n", buf[:n])
	}
}

// TestWebSocketImmediateData tests that a WebSocket client which sends
// data messages before its partner appears receives "impatient\n".
func TestWebSocketImmediateData(t *testing.T) {
	usage := &testUsageRecorder{}
	server := NewTransitServer(usage, func() time.Time { return time.Now() })

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	addr := listener.Addr().String()

	upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		HandleWebSocket(conn, server)
	})
	go func() { _ = http.Serve(listener, mux) }()

	token := strings.Repeat("f", 64)
	side := strings.Repeat("3", 16)
	url := "ws://" + addr + "/"

	ws, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ws.Close()

	// Send handshake as first message, data as second message
	if err := ws.WriteMessage(websocket.BinaryMessage,
		[]byte("please relay "+token+" for side "+side)); err != nil {
		t.Fatal(err)
	}
	if err := ws.WriteMessage(websocket.BinaryMessage,
		[]byte("too early")); err != nil {
		t.Fatal(err)
	}

	ws.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, msg, err := ws.ReadMessage()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(msg) != "impatient\n" {
		t.Fatalf("expected impatient\\n, got %q", msg)
	}
}

// TestJiltedUsageRecordSuppressed mirrors the Python reference test
// test_one_happy_one_jilted: when a paired connection ends, exactly one
// usage record is emitted (the "happy" one) - the side whose partner
// went happy is not recorded.
func TestJiltedUsageRecordSuppressed(t *testing.T) {
	for _, disconnectFirst := range []string{"first", "second"} {
		t.Run(disconnectFirst, func(t *testing.T) {
			usage := &testUsageRecorder{}
			server := NewTransitServer(usage, func() time.Time { return time.Now() })

			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			addr := listener.Addr().String()

			go func() {
				for i := 0; i < 2; i++ {
					conn, err := listener.Accept()
					if err != nil {
						return
					}
					go HandleTCPConnection(conn, server)
				}
			}()

			token := strings.Repeat("b", 64)
			side1 := strings.Repeat("1", 16)
			side2 := strings.Repeat("2", 16)

			client1, err := net.Dial("tcp", addr)
			if err != nil {
				t.Fatal(err)
			}
			defer client1.Close()
			client2, err := net.Dial("tcp", addr)
			if err != nil {
				t.Fatal(err)
			}
			defer client2.Close()

			if _, err := client1.Write([]byte("please relay " + token + " for side " + side1 + "\n")); err != nil {
				t.Fatal(err)
			}
			if _, err := client2.Write([]byte("please relay " + token + " for side " + side2 + "\n")); err != nil {
				t.Fatal(err)
			}

			readOK := func(c net.Conn) {
				c.SetReadDeadline(time.Now().Add(2 * time.Second))
				buf := make([]byte, 3)
				if _, err := io.ReadFull(c, buf); err != nil {
					t.Fatalf("read ok: %v", err)
				}
				if string(buf) != "ok\n" {
					t.Fatalf("expected ok\\n, got %q", buf)
				}
			}
			readOK(client1)
			readOK(client2)

			// client1 sends 13 bytes, client2 sends 7 bytes
			if _, err := client1.Write(bytes.Repeat([]byte("a"), 13)); err != nil {
				t.Fatal(err)
			}
			if _, err := client2.Write(bytes.Repeat([]byte("b"), 7)); err != nil {
				t.Fatal(err)
			}
			drain := func(c net.Conn, n int) {
				c.SetReadDeadline(time.Now().Add(2 * time.Second))
				if _, err := io.ReadFull(c, make([]byte, n)); err != nil {
					t.Fatalf("drain %d bytes: %v", n, err)
				}
			}
			drain(client2, 13)
			drain(client1, 7)

			if disconnectFirst == "first" {
				client1.Close()
			} else {
				client2.Close()
			}

			// Allow both connection_lost paths (the one disconnecting and the
			// partner it takes down) to run.
			deadline := time.Now().Add(2 * time.Second)
			for time.Now().Before(deadline) {
				if len(usage.snapshot()) >= 1 {
					time.Sleep(300 * time.Millisecond)
					break
				}
				time.Sleep(5 * time.Millisecond)
			}

			events := usage.snapshot()
			if len(events) != 1 {
				t.Fatalf("expected exactly 1 usage event, got %d: %+v", len(events), events)
			}
			if events[0].mood != MoodHappy {
				t.Fatalf("expected mood happy, got %s", events[0].mood)
			}
			if events[0].bytesSent+events[0].buddyBytes != 20 {
				t.Fatalf("expected 20 total bytes, got %+v", events[0])
			}
		})
	}
}

// TestBareConnectionRecordsEmptyMood verifies that a client that connects
// and disconnects without ever sending a handshake records a usage event
// with mood "empty" (as the Python reference does), not "errory".
func TestBareConnectionRecordsEmptyMood(t *testing.T) {
	usage := &testUsageRecorder{}
	server := NewTransitServer(usage, func() time.Time { return time.Now() })

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	addr := listener.Addr().String()

	go func() {
		conn, err := listener.Accept()
		if err == nil {
			go HandleTCPConnection(conn, server)
		}
	}()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	// connect and immediately disconnect without sending anything
	_ = conn.Close()

	deadline := time.Now().Add(2 * time.Second)
	for len(usage.snapshot()) == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	events := usage.snapshot()
	if len(events) != 1 {
		t.Fatalf("expected exactly one usage record, got %d", len(events))
	}
	if events[0].mood != MoodEmpty {
		t.Fatalf("expected mood empty, got %s", events[0].mood)
	}
}
