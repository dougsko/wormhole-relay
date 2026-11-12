package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/dougsko/wormhole-relay/internal/relay"

	"github.com/gorilla/websocket"
)

const (
	DEFAULT_PORT = "tcp:4001:interface=::"
)

type Config struct {
	Port         string
	WebSocket    string
	WebSocketURL string
	BlurUsage    int
	LogFD        string
	UsageDB      string
}

// listenEndpoint is a resolved listen address and its port.
type listenEndpoint struct {
	addr string
	port int
}

// parseEndpoint parses a twisted-style endpoint string into a net.Listen
// address. Supported forms (matching the Python twist transitrelay plugin,
// whose --port/--websocket take "tcp:PORT[:interface=ADDR]"):
//
//	tcp:PORT
//	tcp:PORT:interface=ADDR
//	PORT
//	HOST:PORT / [HOST]:PORT
func parseEndpoint(ep string) (listenEndpoint, error) {
	var out listenEndpoint
	ep = strings.TrimSpace(ep)
	if !strings.HasPrefix(ep, "tcp:") {
		// bare port or host:port
		if port, err := strconv.Atoi(ep); err == nil {
			out.port = port
			out.addr = fmt.Sprintf(":%d", port)
			return out, nil
		}
		host, portStr, err := net.SplitHostPort(ep)
		if err != nil {
			return out, fmt.Errorf("unrecognized endpoint %q (want tcp:PORT[:interface=ADDR] or host:port)", ep)
		}
		port, err := strconv.Atoi(portStr)
		if err != nil {
			return out, fmt.Errorf("unrecognized endpoint %q: bad port %q", ep, portStr)
		}
		out.port = port
		out.addr = net.JoinHostPort(host, portStr)
		return out, nil
	}
	rest := strings.TrimPrefix(ep, "tcp:")
	parts := strings.Split(rest, ":")
	if len(parts) == 0 || parts[0] == "" {
		return out, fmt.Errorf("unrecognized endpoint %q", ep)
	}
	port, err := strconv.Atoi(parts[0])
	if err != nil {
		return out, fmt.Errorf("unrecognized endpoint %q: bad port %q", ep, parts[0])
	}
	iface := ""
	for _, p := range parts[1:] {
		if strings.HasPrefix(p, "interface=") {
			iface = strings.TrimPrefix(p, "interface=")
		}
	}
	out.port = port
	if iface == "" {
		out.addr = fmt.Sprintf(":%d", port)
		return out, nil
	}
	if strings.Contains(iface, ":") && !strings.HasPrefix(iface, "[") {
		out.addr = fmt.Sprintf("[%s]:%d", iface, port)
		return out, nil
	}
	out.addr = net.JoinHostPort(strings.Trim(iface, "[]"), strconv.Itoa(port))
	return out, nil
}

func main() {
	var config Config
	// option names and short flags match the Python "twist transitrelay" plugin
	flag.StringVar(&config.Port, "port", DEFAULT_PORT, "endpoint to listen on")
	flag.StringVar(&config.Port, "p", DEFAULT_PORT, "endpoint to listen on (shorthand)")
	flag.StringVar(&config.WebSocket, "websocket", "", "endpoint to listen for WebSocket connections")
	flag.StringVar(&config.WebSocket, "w", "", "endpoint to listen for WebSocket connections (shorthand)")
	flag.StringVar(&config.WebSocketURL, "websocket-url", "", "WebSocket URL (derived from endpoint if not provided)")
	flag.StringVar(&config.WebSocketURL, "u", "", "WebSocket URL (derived from endpoint if not provided) (shorthand)")
	flag.IntVar(&config.BlurUsage, "blur-usage", 0, "blur timestamps and data sizes in logs")
	flag.StringVar(&config.LogFD, "log-fd", "", "write JSON usage logs to this file descriptor")
	flag.StringVar(&config.UsageDB, "usage-db", "", "record usage data (SQLite)")
	flag.Parse()

	// Create usage tracker
	usage, err := relay.CreateUsageTracker(config.BlurUsage, config.LogFD, config.UsageDB)
	if err != nil {
		log.Fatalf("Failed to create usage tracker: %v", err)
	}

	// Create transit server
	transit := relay.NewTransitServer(
		usage,
		func() time.Time { return time.Now() },
	)

	// Increase rlimits if possible
	IncreaseRLimits()

	// Setup TCP listener
	tcpEndpoint, err := parseEndpoint(config.Port)
	if err != nil {
		log.Fatalf("bad --port: %v", err)
	}
	listener, err := net.Listen("tcp", tcpEndpoint.addr)
	if err != nil {
		log.Fatalf("Failed to listen on TCP %s: %v", tcpEndpoint.addr, err)
	}

	// Setup WebSocket listener if configured
	var wsListener net.Listener
	if config.WebSocket != "" {
		wsEndpoint, err := parseEndpoint(config.WebSocket)
		if err != nil {
			log.Fatalf("bad --websocket: %v", err)
		}
		wsListener, err = net.Listen("tcp", wsEndpoint.addr)
		if err != nil {
			log.Fatalf("Failed to listen on WebSocket %s: %v", wsEndpoint.addr, err)
		}
		// derive the WebSocket URL like the Python plugin does
		if config.WebSocketURL == "" {
			config.WebSocketURL = fmt.Sprintf("ws://localhost:%d/", wsEndpoint.port)
			fmt.Printf("Using WebSocket URL %q\n", config.WebSocketURL)
		}
	}

	log.Printf("Transit relay server listening on TCP %s", listener.Addr())
	if wsListener != nil {
		log.Printf("Transit relay server listening on WebSocket %s", wsListener.Addr())
	}

	// Start WebSocket handler
	if wsListener != nil {
		upgrader := websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool { return true },
		}
		http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			conn, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				log.Printf("WebSocket upgrade failed: %v", err)
				return
			}
			relay.HandleWebSocket(conn, transit)
		})
		go func() {
			if err := http.Serve(wsListener, nil); err != nil && err != http.ErrServerClosed {
				log.Fatalf("WebSocket server failed: %v", err)
			}
		}()
	}

	// Update stats immediately at startup, then every 5 minutes; mirrors the
	// reference's TimerService(5*60, transit.update_stats), which twisted
	// runs immediately when the service starts.
	go func() {
		transit.UpdateStats()
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()
		for range ticker.C {
			transit.UpdateStats()
		}
	}()

	// Accept TCP connections
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("TCP accept error: %v", err)
			continue
		}
		go relay.HandleTCPConnection(conn, transit)
	}
}

func IncreaseRLimits() {
	// Try to increase RLIMIT_NOFILE so the server can handle many connections.
	// Linux defaults are often (1024, 1048576); macOS reports a huge hard
	// limit but caps the soft limit at 10240.
	const RLIMIT_NOFILE = 7

	var rl syscall.Rlimit
	if err := syscall.Getrlimit(RLIMIT_NOFILE, &rl); err != nil {
		log.Printf("warning: getrlimit(RLIMIT_NOFILE) failed: %v", err)
		return
	}
	soft, hard := rl.Cur, rl.Max
	if soft >= 10000 {
		log.Printf("RLIMIT_NOFILE soft=%d already >= 10000, leaving it", soft)
		return
	}
	for _, newLimit := range []uint64{hard, 10000, 3200, 1024} {
		if err := syscall.Setrlimit(RLIMIT_NOFILE, &syscall.Rlimit{Cur: newLimit, Max: hard}); err == nil {
			log.Printf("RLIMIT_NOFILE raised from %d to %d (hard=%d)", soft, newLimit, hard)
			return
		}
	}
	log.Printf("warning: unable to raise RLIMIT_NOFILE, leaving it at %d", soft)
}
