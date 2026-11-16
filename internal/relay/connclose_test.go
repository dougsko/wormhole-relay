package relay

// Regression tests for the connection-teardown path: when the peer goes
// away, the relay must close its own socket, not just the buddy's.

import (
	"errors"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestTCPConnectionLostClosesOwnConnection(t *testing.T) {
	usage := &testUsageRecorder{}
	server := NewTransitServer(usage, func() time.Time { return time.Now() })

	// real TCP loopback: net.Pipe has no half-close semantics, so it
	// cannot detect whether the relay closed its own side
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	acceptErr := make(chan error, 1)
	relaySideCh := make(chan net.Conn, 1)
	go func() {
		c, err := listener.Accept()
		if err != nil {
			acceptErr <- err
			return
		}
		relaySideCh <- c
	}()

	client, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	// client connects, never sends a handshake, goes away
	_ = client.Close()

	var relaySide net.Conn
	select {
	case relaySide = <-relaySideCh:
	case err := <-acceptErr:
		t.Fatal(err)
	case <-time.After(2 * time.Second):
		t.Fatal("relay never accepted the connection")
	}
	done := make(chan struct{})
	go func() {
		tc := NewTransitConnection(relaySide, server)
		tc.Handle()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("connection handler did not return after peer close")
	}

	// the relay must have closed its own side: reading a locally closed
	// socket yields net.ErrClosed, while a merely peer-closed socket
	// yields io.EOF — that's the discriminator
	_ = relaySide.SetReadDeadline(time.Now().Add(time.Second))
	_, rerr := relaySide.Read(make([]byte, 1))
	if !errors.Is(rerr, net.ErrClosed) {
		t.Fatalf("relay side was not closed by the relay (got %v); own connection leaked", rerr)
	}
}

func TestWebSocketConnectionLostClosesOwnConnection(t *testing.T) {
	usage := &testUsageRecorder{}
	server := NewTransitServer(usage, func() time.Time { return time.Now() })

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	captured := make(chan *websocket.Conn, 1)
	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		captured <- conn
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

	url := "ws://" + listener.Addr().String() + "/"
	ws, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = ws.Close()

	var serverConn *websocket.Conn
	select {
	case serverConn = <-captured:
	case <-time.After(2 * time.Second):
		t.Fatal("server never saw the websocket connection")
	}

	// wait for the connectionLost path to run (it records usage)
	deadline := time.Now().Add(2 * time.Second)
	for len(usage.snapshot()) == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := len(usage.snapshot()); got != 1 {
		t.Fatalf("expected 1 usage record, got %d", got)
	}

	// connectionLost ran, which now closes the relay-side websocket too;
	// reading the underlying (hijacked) conn must report the local close
	// (net.ErrClosed), not merely the peer's FIN (io.EOF)
	underlying, ok := serverConn.UnderlyingConn().(net.Conn)
	if !ok {
		t.Fatal("underlying conn is not a net.Conn")
	}
	_ = underlying.SetReadDeadline(time.Now().Add(time.Second))
	_, rerr := underlying.Read(make([]byte, 1))
	if !errors.Is(rerr, net.ErrClosed) {
		t.Fatalf("server side was not closed by the relay (got %v); own connection leaked", rerr)
	}
}
