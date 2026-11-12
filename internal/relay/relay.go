package relay

import (
	"bytes"
	"io"
	"net"
	"regexp"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// TransitServerState represents the state of a single transit connection
type TransitServerState int

const (
	StateListening TransitServerState = iota
	StateWaitRelay
	StateWaitPartner
	StateRelaying
	StateDone
)

// Mood represents the connection result mood
type Mood string

const (
	MoodHappy      Mood = "happy"
	MoodLonely     Mood = "lonely"
	MoodRedundant  Mood = "redundant"
	MoodImpatient  Mood = "impatient"
	MoodErrory     Mood = "errory"
	MoodJilted     Mood = "jilted"
)

var (
	// old: "please relay {64}\n"
	oldRelayRe = regexp.MustCompile(`^please relay (\w{64})$`)
	// new: "please relay {64} for side {16}\n"
	newRelayRe = regexp.MustCompile(`^please relay (\w{64}) for side (\w{16})$`)
)

// PendingSide represents a side token (can be empty for legacy)
type PendingSide string

// PendingClient is the common interface used by pending request tracking
type PendingClient interface {
	// partnerConnectionLost is called when a redundant connection is dropped
	partnerConnectionLost()
	// gotPartner is called when a matching partner is found
	gotPartner(other PendingClient)
}

// ActiveConnections tracks active (paired) connections
type ActiveConnections struct {
	mu          sync.Mutex
	connections map[PendingClient]bool
}

// NewActiveConnections creates a new ActiveConnections
func NewActiveConnections() *ActiveConnections {
	return &ActiveConnections{
		connections: make(map[PendingClient]bool),
	}
}

func (ac *ActiveConnections) Register(side0, side1 PendingClient) {
	ac.mu.Lock()
	defer ac.mu.Unlock()
	ac.connections[side0] = true
	ac.connections[side1] = true
}

func (ac *ActiveConnections) Unregister(side PendingClient) {
	ac.mu.Lock()
	defer ac.mu.Unlock()
	delete(ac.connections, side)
}

func (ac *ActiveConnections) Count() int {
	ac.mu.Lock()
	defer ac.mu.Unlock()
	return len(ac.connections)
}

// PendingRequests tracks outstanding (non-active) requests
type PendingRequests struct {
	mu       sync.Mutex
	requests map[string]map[PendingSide]PendingClient
	active   *ActiveConnections
}

// NewPendingRequests creates a new PendingRequests
func NewPendingRequests(active *ActiveConnections) *PendingRequests {
	return &PendingRequests{
		requests: make(map[string]map[PendingSide]PendingClient),
		active:   active,
	}
}

func (pr *PendingRequests) SetActive(active *ActiveConnections) {
	pr.mu.Lock()
	defer pr.mu.Unlock()
	pr.active = active
}

func (pr *PendingRequests) Unregister(token string, side PendingSide, tc PendingClient) {
	pr.mu.Lock()
	defer pr.mu.Unlock()
	if sides, ok := pr.requests[token]; ok {
		delete(sides, side)
		if len(sides) == 0 {
			delete(pr.requests, token)
		}
	}
	if pr.active != nil {
		pr.active.Unregister(tc)
	}
}

func (pr *PendingRequests) Register(token string, side PendingSide, tc PendingClient) bool {
	pr.mu.Lock()
	defer pr.mu.Unlock()

	sides := pr.requests[token]
	if sides == nil {
		sides = make(map[PendingSide]PendingClient)
		pr.requests[token] = sides
	}

	// Check for matching side
	for existingSide, existingTC := range sides {
		if existingSide == "" || side == "" || existingSide != side {
			// Found a match!
			delete(sides, existingSide)
			// Drop any other sides (redundant)
			for _, leftoverTC := range sides {
				leftoverTC.partnerConnectionLost()
			}
			delete(pr.requests, token)

			// Glue the two ends together
			if pr.active != nil {
				pr.active.Register(tc, existingTC)
			}
			tc.gotPartner(existingTC)
			existingTC.gotPartner(tc)
			return false
		}
	}

	sides[side] = tc
	return true
}

func (pr *PendingRequests) Count() int {
	pr.mu.Lock()
	defer pr.mu.Unlock()
	count := 0
	for _, sides := range pr.requests {
		count += len(sides)
	}
	return count
}

// TransitServer manages pairs of simultaneous connections
type TransitServer struct {
	activeConnections *ActiveConnections
	pendingRequests   *PendingRequests
	usage             UsageRecorder
	getTimestamp      func() time.Time
	rebooted          time.Time
	mu                sync.Mutex
}

// UsageRecorder interface for recording usage statistics
type UsageRecorder interface {
	Record(started, buddyStarted time.Time, mood Mood, bytesSent, buddyBytes int)
	UpdateStats(rebooted, updated time.Time, connected, waiting, incompleteBytes int)
}

// NewTransitServer creates a new transit server
func NewTransitServer(usage UsageRecorder, getTimestamp func() time.Time) *TransitServer {
	server := &TransitServer{
		activeConnections: NewActiveConnections(),
		usage:             usage,
		getTimestamp:      getTimestamp,
		rebooted:          getTimestamp(),
	}
	server.pendingRequests = NewPendingRequests(server.activeConnections)
	return server
}

// UpdateStats updates global statistics
func (ts *TransitServer) UpdateStats() {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	connected := ts.activeConnections.Count()
	waiting := ts.pendingRequests.Count()

	// Calculate incomplete bytes
	incompleteBytes := 0
	ts.activeConnections.mu.Lock()
	for tc := range ts.activeConnections.connections {
		switch conn := tc.(type) {
		case *TransitConnection:
			conn.mu.Lock()
			incompleteBytes += conn.totalSent
			conn.mu.Unlock()
		case *WebSocketTransitConnection:
			conn.mu.Lock()
			incompleteBytes += conn.totalSent
			conn.mu.Unlock()
		}
	}
	ts.activeConnections.mu.Unlock()

	ts.usage.UpdateStats(
		ts.rebooted,
		ts.getTimestamp(),
		connected,
		waiting,
		incompleteBytes,
	)
}

// TransitConnection represents a single TCP transit connection
type TransitConnection struct {
	conn      net.Conn
	server    *TransitServer
	state     TransitServerState
	token     string
	side      PendingSide
	first     bool
	mood      Mood
	buddy     PendingClient
	totalSent int
	started   time.Time
	rawMode   bool
	mu        sync.Mutex
}

// NewTransitConnection creates a new transit connection
func NewTransitConnection(conn net.Conn, server *TransitServer) *TransitConnection {
	return &TransitConnection{
		conn:    conn,
		server:  server,
		state:   StateWaitRelay,
		started: server.getTimestamp(),
	}
}

func (tc *TransitConnection) send(data []byte) {
	tc.mu.Lock()
	conn := tc.conn
	tc.mu.Unlock()
	if conn != nil {
		conn.Write(data)
	}
}

func (tc *TransitConnection) close() {
	tc.mu.Lock()
	conn := tc.conn
	tc.conn = nil
	tc.mu.Unlock()
	if conn != nil {
		conn.Close()
	}
}

// Handle runs the connection state machine
func (tc *TransitConnection) Handle() {
	defer tc.connectionLost()
	buf := make([]byte, 65536)
	var handshakeBuf []byte

	for {
		tc.mu.Lock()
		stateDone := tc.state == StateDone
		conn := tc.conn
		tc.mu.Unlock()
		if stateDone || conn == nil {
			return
		}
		n, err := conn.Read(buf)
		if err != nil {
			if err == io.EOF {
				tc.connectionLost()
			}
			break
		}
		if n == 0 {
			continue
		}
		chunk := append([]byte{}, buf[:n]...)

		// Re-check raw mode now that we have data, because
		// gotPartner may have flipped it since last iteration.
		tc.mu.Lock()
		raw := tc.rawMode
		tc.mu.Unlock()

		if raw {
			tc.handleData(chunk)
			continue
		}

		// Pre-handshake: accumulate bytes until we see a newline
		// (or until rawMode flips).
		handshakeBuf = append(handshakeBuf, chunk...)
		for {
			idx := bytes.IndexByte(handshakeBuf, '\n')
			if idx == -1 {
				break
			}
			line := append([]byte{}, handshakeBuf[:idx]...)
			rest := append([]byte{}, handshakeBuf[idx+1:]...)
			handshakeBuf = rest

			tc.handleLine(bytes.TrimSpace(line))

			// Check if handleLine put us in Done state or raw mode
			tc.mu.Lock()
			stateDone := tc.state == StateDone
			raw = tc.rawMode
			rcBuffer := append([]byte{}, handshakeBuf...)
			handshakeBuf = nil
			tc.mu.Unlock()

			if stateDone {
				// flush remaining raw data as bytes if we switched to raw,
				// otherwise drop it
				if raw && len(rcBuffer) > 0 {
					tc.handleData(rcBuffer)
				}
				return
			}
			if raw && len(rcBuffer) > 0 {
				tc.handleData(rcBuffer)
			}
		}
	}
}

func (tc *TransitConnection) handleLine(line []byte) {
	// old: "please relay {64}\n"
	if matches := oldRelayRe.FindSubmatch(line); matches != nil {
		token := string(matches[1])
		tc.pleaseRelay(token)
		return
	}
	// new: "please relay {64} for side {16}\n"
	if matches := newRelayRe.FindSubmatch(line); matches != nil {
		token := string(matches[1])
		side := PendingSide(matches[2])
		tc.pleaseRelayForSide(token, side)
		return
	}
	tc.badToken()
}

func (tc *TransitConnection) pleaseRelay(token string) {
	tc.mu.Lock()
	tc.token = token
	tc.side = ""
	tc.mood = MoodLonely
	// Valid handshake -> switch to raw byte reading immediately, like
	// Python's lineReceived() calling setRawMode() before the partner
	// is found. This is what makes "got_bytes" -> "impatient" work.
	tc.rawMode = true
	tc.mu.Unlock()

	first := tc.server.pendingRequests.Register(token, "", tc)
	tc.first = first

	tc.mu.Lock()
	if first {
		// If the partner matched during Register, Register will have
		// called gotPartner() which sets StateRelaying; otherwise wait.
		if tc.state != StateRelaying {
			tc.state = StateWaitPartner
		}
	}
	// else we stay in StateRelaying (partner already set it)
	tc.mu.Unlock()
}

func (tc *TransitConnection) pleaseRelayForSide(token string, side PendingSide) {
	tc.mu.Lock()
	tc.token = token
	tc.side = side
	tc.mood = MoodLonely
	// Valid handshake -> switch to raw byte reading immediately.
	tc.rawMode = true
	tc.mu.Unlock()

	first := tc.server.pendingRequests.Register(token, side, tc)
	tc.first = first

	tc.mu.Lock()
	if first {
		if tc.state != StateRelaying {
			tc.state = StateWaitPartner
		}
	}
	// else we stay in StateRelaying (partner already set it)
	tc.mu.Unlock()
}

func (tc *TransitConnection) badToken() {
	tc.mu.Lock()
	tc.mood = MoodErrory
	tc.state = StateDone
	tc.mu.Unlock()

	tc.send([]byte("bad handshake\n"))
	tc.close()
	tc.recordUsage()
}

func (tc *TransitConnection) gotPartner(other PendingClient) {
	tc.mu.Lock()
	tc.buddy = other
	tc.mood = MoodHappy
	tc.state = StateRelaying
	tc.rawMode = true
	tc.mu.Unlock()

	tc.send([]byte("ok\n"))
}

func (tc *TransitConnection) partnerConnectionLost() {
	tc.mu.Lock()
	wasWaiting := tc.state == StateWaitPartner
	if wasWaiting {
		tc.mood = MoodRedundant
		tc.state = StateDone
	}
	tc.mu.Unlock()
	if !wasWaiting {
		return
	}
	tc.close()
	tc.recordUsage()
}

func (tc *TransitConnection) handleData(data []byte) {
	tc.mu.Lock()
	if tc.state != StateRelaying {
		token, side := tc.token, tc.side
		tc.mood = MoodImpatient
		tc.state = StateDone
		tc.mu.Unlock()
		tc.send([]byte("impatient\n"))
		tc.close()
		tc.server.pendingRequests.Unregister(token, side, tc)
		tc.recordUsage()
		return
	}
	buddy := tc.buddy
	if buddy != nil {
		tc.totalSent += len(data)
	}
	tc.mu.Unlock()

	if buddy == nil {
		return
	}
	if other, ok := buddy.(*TransitConnection); ok {
		other.send(data)
	} else if wsConn, ok := buddy.(*WebSocketTransitConnection); ok {
		wsConn.send(data)
	}
}

func (tc *TransitConnection) connectionLost() {
	tc.mu.Lock()
	if tc.state == StateDone {
		tc.mu.Unlock()
		return
	}

	switch tc.state {
	case StateListening, StateWaitRelay:
		tc.mood = MoodErrory
		tc.state = StateDone
		tc.mu.Unlock()
		tc.recordUsage()

	case StateWaitPartner:
		tc.mood = MoodLonely
		token, side := tc.token, tc.side
		tc.state = StateDone
		tc.mu.Unlock()
		tc.server.pendingRequests.Unregister(token, side, tc)
		tc.recordUsage()

	case StateRelaying:
		if tc.first {
			tc.mood = MoodHappy
		} else {
			tc.mood = MoodJilted
		}
		buddy := tc.buddy
		token, side := tc.token, tc.side
		tc.state = StateDone
		tc.mu.Unlock()
		if buddy != nil {
			if other, ok := buddy.(*TransitConnection); ok {
				other.close()
			} else if wsConn, ok := buddy.(*WebSocketTransitConnection); ok {
				wsConn.close()
			}
		}
		tc.server.pendingRequests.Unregister(token, side, tc)
		tc.recordUsage()

	default:
		tc.state = StateDone
		tc.mu.Unlock()
	}
}

func (tc *TransitConnection) recordUsage() {
	buddyStarted := time.Time{}
	buddyBytes := 0
	if tc.buddy != nil {
		switch other := tc.buddy.(type) {
		case *TransitConnection:
			buddyStarted = other.started
			buddyBytes = other.totalSent
		case *WebSocketTransitConnection:
			buddyStarted = other.started
			buddyBytes = other.totalSent
		}
	}

	tc.server.usage.Record(
		tc.started,
		buddyStarted,
		tc.mood,
		tc.totalSent,
		buddyBytes,
	)
}

// WebSocketTransitConnection handles WebSocket connections
type WebSocketTransitConnection struct {
	conn     *websocket.Conn
	server   *TransitServer
	state    TransitServerState
	token    string
	side     PendingSide
	first    bool
	mood     Mood
	buddy    PendingClient
	totalSent int
	started   time.Time
	firstMsg  bool
	mu        sync.Mutex
}

// NewWebSocketTransitConnection creates a new WebSocket transit connection
func NewWebSocketTransitConnection(conn *websocket.Conn, server *TransitServer) *WebSocketTransitConnection {
	return &WebSocketTransitConnection{
		conn:     conn,
		server:   server,
		state:    StateWaitRelay,
		started:  server.getTimestamp(),
		firstMsg: true,
	}
}

func (wsc *WebSocketTransitConnection) send(data []byte) {
	wsc.mu.Lock()
	conn := wsc.conn
	wsc.mu.Unlock()
	if conn != nil {
		conn.WriteMessage(websocket.BinaryMessage, data)
	}
}

func (wsc *WebSocketTransitConnection) close() {
	wsc.mu.Lock()
	conn := wsc.conn
	wsc.conn = nil
	wsc.mu.Unlock()
	if conn != nil {
		conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
		conn.Close()
	}
}

// Handle runs the WebSocket connection state machine
func (wsc *WebSocketTransitConnection) Handle() {
	defer wsc.connectionLost()
	for {
		wsc.mu.Lock()
		if wsc.state == StateDone {
			wsc.mu.Unlock()
			break
		}
		conn := wsc.conn
		wsc.mu.Unlock()

		if conn == nil {
			return
		}
		_, data, err := conn.ReadMessage()
		if err != nil {
			wsc.connectionLost()
			break
		}
		wsc.handleDataOrHandshake(data)
	}
}

func (wsc *WebSocketTransitConnection) handleDataOrHandshake(data []byte) {
	wsc.mu.Lock()
	isFirst := wsc.firstMsg
	wsc.firstMsg = false
	wsc.mu.Unlock()

	if isFirst {
		wsc.handleHandshake(data)
		return
	}
	wsc.handleData(data)
}

func (wsc *WebSocketTransitConnection) handleHandshake(data []byte) {
	// old: "please relay {64}"
	if matches := oldRelayRe.FindSubmatch(data); matches != nil {
		token := string(matches[1])
		wsc.pleaseRelay(token)
		return
	}
	// new: "please relay {64} for side {16}"
	if matches := newRelayRe.FindSubmatch(data); matches != nil {
		token := string(matches[1])
		side := PendingSide(matches[2])
		wsc.pleaseRelayForSide(token, side)
		return
	}
	wsc.badToken()
}

func (wsc *WebSocketTransitConnection) pleaseRelay(token string) {
	wsc.mu.Lock()
	wsc.token = token
	wsc.side = ""
	wsc.mood = MoodLonely
	wsc.mu.Unlock()

	first := wsc.server.pendingRequests.Register(token, "", wsc)
	wsc.first = first

	wsc.mu.Lock()
	if first {
		wsc.state = StateWaitPartner
	}
	wsc.mu.Unlock()
}

func (wsc *WebSocketTransitConnection) pleaseRelayForSide(token string, side PendingSide) {
	wsc.mu.Lock()
	wsc.token = token
	wsc.side = side
	wsc.mood = MoodLonely
	wsc.mu.Unlock()

	first := wsc.server.pendingRequests.Register(token, side, wsc)
	wsc.first = first

	wsc.mu.Lock()
	if first {
		wsc.state = StateWaitPartner
	}
	wsc.mu.Unlock()
}

func (wsc *WebSocketTransitConnection) badToken() {
	wsc.mu.Lock()
	wsc.mood = MoodErrory
	wsc.state = StateDone
	wsc.mu.Unlock()

	wsc.send([]byte("bad handshake"))
	wsc.close()
	wsc.recordUsage()
}

func (wsc *WebSocketTransitConnection) gotPartner(other PendingClient) {
	wsc.mu.Lock()
	wsc.buddy = other
	wsc.mood = MoodHappy
	wsc.state = StateRelaying
	wsc.mu.Unlock()

	wsc.send([]byte("ok\n"))
}

func (wsc *WebSocketTransitConnection) partnerConnectionLost() {
	wsc.mu.Lock()
	wasWaiting := wsc.state == StateWaitPartner
	if wasWaiting {
		wsc.mood = MoodRedundant
		wsc.state = StateDone
	}
	wsc.mu.Unlock()
	if !wasWaiting {
		return
	}
	wsc.close()
	wsc.recordUsage()
}

func (wsc *WebSocketTransitConnection) handleData(data []byte) {
	wsc.mu.Lock()
	if wsc.state != StateRelaying {
		token, side := wsc.token, wsc.side
		wsc.mood = MoodImpatient
		wsc.state = StateDone
		wsc.mu.Unlock()
		wsc.send([]byte("impatient\n"))
		wsc.close()
		wsc.server.pendingRequests.Unregister(token, side, wsc)
		wsc.recordUsage()
		return
	}

	buddy := wsc.buddy
	if buddy != nil {
		wsc.totalSent += len(data)
	}
	wsc.mu.Unlock()

	if buddy == nil {
		return
	}
	if other, ok := buddy.(*TransitConnection); ok {
		other.send(data)
	} else if other, ok := buddy.(*WebSocketTransitConnection); ok {
		other.send(data)
	}
}

func (wsc *WebSocketTransitConnection) connectionLost() {
	wsc.mu.Lock()
	defer wsc.mu.Unlock()

	if wsc.state == StateDone {
		return
	}

	switch wsc.state {
	case StateWaitRelay:
		wsc.mood = MoodErrory
	case StateWaitPartner:
		wsc.mood = MoodLonely
		wsc.server.pendingRequests.Unregister(wsc.token, wsc.side, wsc)
	case StateRelaying:
		if wsc.first {
			wsc.mood = MoodHappy
		} else {
			wsc.mood = MoodJilted
		}
		if wsc.buddy != nil {
			if other, ok := wsc.buddy.(*TransitConnection); ok {
				other.close()
			} else if other, ok := wsc.buddy.(*WebSocketTransitConnection); ok {
				other.close()
			}
		}
		wsc.server.pendingRequests.Unregister(wsc.token, wsc.side, wsc)
	default:
		wsc.mood = MoodErrory
	}

	wsc.recordUsage()
	wsc.state = StateDone
}

func (wsc *WebSocketTransitConnection) recordUsage() {
	buddyStarted := time.Time{}
	buddyBytes := 0
	if wsc.buddy != nil {
		switch other := wsc.buddy.(type) {
		case *TransitConnection:
			buddyStarted = other.started
			buddyBytes = other.totalSent
		case *WebSocketTransitConnection:
			buddyStarted = other.started
			buddyBytes = other.totalSent
		}
	}

	wsc.server.usage.Record(
		wsc.started,
		buddyStarted,
		wsc.mood,
		wsc.totalSent,
		buddyBytes,
	)
}

// HandleTCPConnection handles a TCP connection
func HandleTCPConnection(conn net.Conn, transit *TransitServer) {
	tc := NewTransitConnection(conn, transit)
	tc.Handle()
}

// HandleWebSocket handles a WebSocket connection
func HandleWebSocket(conn *websocket.Conn, transit *TransitServer) {
	wsc := NewWebSocketTransitConnection(conn, transit)
	wsc.Handle()
}
