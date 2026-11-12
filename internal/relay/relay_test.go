package relay

import (
	"bytes"
	"net"
	"net/http"
	"strings"
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

// testUsageRecorder is a simple usage recorder for tests
type testUsageRecorder struct{}

func (r *testUsageRecorder) Record(started, buddyStarted time.Time, mood Mood, bytesSent, buddyBytes int) {
}

func (r *testUsageRecorder) UpdateStats(rebooted, updated time.Time, connected, waiting, incompleteBytes int) {
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
