package relay

import (
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// TestWebSocketEndToEnd tests the full WebSocket relay flow
func TestWebSocketEndToEnd(t *testing.T) {
	usage := &testUsageRecorder{}
	server := NewTransitServer(usage, func() time.Time { return time.Now() })

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	addr := listener.Addr().String()

	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		HandleWebSocket(conn, server)
	})
	serverDone := make(chan struct{})
	go func() {
		_ = http.Serve(listener, mux)
		close(serverDone)
	}()
	defer func() {
		listener.Close()
		<-serverDone
	}()

	token := strings.Repeat("c", 64)
	side1 := strings.Repeat("1", 16)
	side2 := strings.Repeat("2", 16)

	url := "ws://" + addr + "/"

	ws1, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatal(err)
	}
	ws2, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ws1.Close()
	defer ws2.Close()

	// Handshake both sides
	if err := ws1.WriteMessage(websocket.BinaryMessage, []byte("please relay "+token+" for side "+side1)); err != nil {
		t.Fatal(err)
	}
	if err := ws2.WriteMessage(websocket.BinaryMessage, []byte("please relay "+token+" for side "+side2)); err != nil {
		t.Fatal(err)
	}

	// Both should receive ok\n
	_, msg1, err := ws1.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	if string(msg1) != "ok\n" {
		t.Fatalf("expected ok\\n, got %q", msg1)
	}

	_, msg2, err := ws2.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	if string(msg2) != "ok\n" {
		t.Fatalf("expected ok\\n, got %q", msg2)
	}

	// Send data between them
	if err := ws1.WriteMessage(websocket.BinaryMessage, []byte("hello ws")); err != nil {
		t.Fatal(err)
	}
	ws2.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, msg3, err := ws2.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	if string(msg3) != "hello ws" {
		t.Fatalf("expected %q, got %q", "hello ws", msg3)
	}
}

// TestWebSocketBadHandshake tests that bad WebSocket handshakes are rejected
func TestWebSocketBadHandshake(t *testing.T) {
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
	serverDone := make(chan struct{})
	go func() {
		_ = http.Serve(listener, mux)
		close(serverDone)
	}()
	defer func() {
		listener.Close()
		<-serverDone
	}()

	url := "ws://" + addr + "/"
	ws, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ws.Close()

	if err := ws.WriteMessage(websocket.BinaryMessage, []byte("nonsense")); err != nil {
		t.Fatal(err)
	}

	_, msg, err := ws.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	if string(msg) != "bad handshake" {
		t.Fatalf("expected bad handshake, got %q", msg)
	}
}
