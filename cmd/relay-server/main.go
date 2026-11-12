package main

import (
	"flag"
	"log"
	"net"
	"net/http"
	"syscall"
	"time"

	"wormhole-relay/internal/relay"

	"github.com/gorilla/websocket"
)

const (
	DEFAULT_PORT    = "4001"
	DEFAULT_WS_PORT = "4002"
)

type Config struct {
	Port         string
	WebSocket    string
	WebSocketURL string
	BlurUsage    int
	LogFD        string
	UsageDB      string
}

func main() {
	var config Config
	flag.StringVar(&config.Port, "port", DEFAULT_PORT, "TCP port to listen on")
	flag.StringVar(&config.WebSocket, "websocket", "", "WebSocket port to listen on (e.g. 4002)")
	flag.StringVar(&config.WebSocketURL, "websocket-url", "", "WebSocket URL (derived from endpoint if not provided)")
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
	listener, err := net.Listen("tcp", ":"+config.Port)
	if err != nil {
		log.Fatalf("Failed to listen on TCP %s: %v", config.Port, err)
	}

	// Setup WebSocket listener if configured
	var wsListener net.Listener
	if config.WebSocket != "" {
		wsListener, err = net.Listen("tcp", ":"+config.WebSocket)
		if err != nil {
			log.Fatalf("Failed to listen on WebSocket %s: %v", config.WebSocket, err)
		}
	}

	log.Printf("Transit relay server listening on TCP :%s", config.Port)
	if config.WebSocket != "" {
		log.Printf("Transit relay server listening on WebSocket :%s", config.WebSocket)
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
		go http.Serve(wsListener, nil)
	}

	// Update stats every 5 minutes
	go func() {
		ticker := time.NewTicker(5 * time.Minute)
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
