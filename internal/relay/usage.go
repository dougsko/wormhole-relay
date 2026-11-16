package relay

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// UsageEvent is one completed-connection usage record. WaitingTime is nil
// when the connection was never paired (lonely); the database records NULL,
// matching the Python reference.
type UsageEvent struct {
	Started     time.Time
	TotalTime   float64
	WaitingTime *float64
	TotalBytes  int
	Mood        Mood
}

// queue bounds for the asynchronous usage pipeline. A full queue drops
// events (usage is telemetry, not correctness) and counts the drops.
const (
	usageQueueCap      = 1024
	usageBatchSize     = 64
	usageFlushInterval = 100 * time.Millisecond
	usageDropLogEvery  = 1024
)

// UsageWriter interface for recording usage statistics.
type UsageWriter interface {
	RecordUsage(ev UsageEvent)
	UpdateStats(rebooted, updated time.Time, connected, waiting, incompleteBytes int)
}

// BatchUsageWriter is implemented by writers that can persist a batch of
// events in one transaction. Falls back to per-record RecordUsage.
type BatchUsageWriter interface {
	RecordUsageBatch(events []UsageEvent)
}

type queueMsg struct {
	ev    *UsageEvent   // set for a record
	flush chan struct{} // set for a flush request; closed when drained
	stop  bool          // set for shutdown
}

// UsageTracker tracks usage statistics of connections. Writers run on a
// single background goroutine fed by a bounded queue, so the connection
// teardown path never blocks on SQLite or file I/O. Batches are written in
// one transaction where the backend supports it.
type UsageTracker struct {
	backends  map[string]UsageWriter
	blurUsage int
	logFile   *os.File

	queue      chan queueMsg
	workerDone chan struct{}
	dropped    uint64
}

// CreateUsageTracker creates a new UsageTracker
func CreateUsageTracker(blurUsage int, logFile string, usageDB string) (*UsageTracker, error) {
	tracker := &UsageTracker{
		backends:  map[string]UsageWriter{},
		blurUsage: blurUsage,
	}
	if blurUsage > 0 {
		fmt.Printf("blurring access times to %d seconds\n", blurUsage)
	} else {
		fmt.Printf("not blurring access times\n")
	}
	if logFile != "" {
		// logFile can be a path or a file descriptor number
		var file *os.File
		var err error
		if fd, convErr := strconv.Atoi(logFile); convErr == nil {
			file = os.NewFile(uintptr(fd), "log")
		} else {
			file, err = os.OpenFile(logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		}
		if err != nil {
			return nil, fmt.Errorf("failed to open log file %q: %v", logFile, err)
		}
		if file != nil {
			tracker.logFile = file
			tracker.backends["log_file"] = NewLogFileUsageWriter(file)
		}
	}
	if usageDB != "" {
		db, err := OpenUsageDB(usageDB)
		if err != nil {
			return nil, fmt.Errorf("failed to open usage db %q: %v", usageDB, err)
		}
		tracker.backends["database"] = &DatabaseUsageWriter{db: db}
	}
	if len(tracker.backends) > 0 {
		tracker.queue = make(chan queueMsg, usageQueueCap)
		tracker.workerDone = make(chan struct{})
		go tracker.worker()
	}
	return tracker, nil
}

func (ut *UsageTracker) worker() {
	defer close(ut.workerDone)
	ticker := time.NewTicker(usageFlushInterval)
	defer ticker.Stop()
	var pending []UsageEvent
	flush := func() {
		if len(pending) == 0 {
			return
		}
		ut.dispatch(pending)
		pending = pending[:0]
	}
	for {
		select {
		case msg := <-ut.queue:
			switch {
			case msg.ev != nil:
				pending = append(pending, *msg.ev)
				if len(pending) >= usageBatchSize {
					flush()
				}
			case msg.flush != nil:
				flush()
				close(msg.flush)
			case msg.stop:
				flush()
				return
			}
		case <-ticker.C:
			flush()
		}
	}
}

// dispatch fans a batch of events out to the backends.
func (ut *UsageTracker) dispatch(events []UsageEvent) {
	for _, backend := range ut.backends {
		if batcher, ok := backend.(BatchUsageWriter); ok {
			batcher.RecordUsageBatch(events)
			continue
		}
		for _, ev := range events {
			backend.RecordUsage(ev)
		}
	}
}

// Flush blocks until every record enqueued so far has been written to the
// backends. Intended for tests and shutdown.
func (ut *UsageTracker) Flush() {
	if ut.queue == nil {
		return
	}
	ack := make(chan struct{})
	ut.queue <- queueMsg{flush: ack}
	<-ack
}

// Close flushes pending records and stops the background writer.
func (ut *UsageTracker) Close() error {
	if ut.queue == nil {
		return nil
	}
	ut.queue <- queueMsg{stop: true}
	<-ut.workerDone
	var err error
	for _, backend := range ut.backends {
		if closer, ok := backend.(interface{ Close() error }); ok {
			if cerr := closer.Close(); cerr != nil {
				err = cerr
			}
		}
	}
	return err
}

// Record records a usage event (computes everything up front, then hands
// off to the background writer; never touches the backends inline)
func (ut *UsageTracker) Record(started, buddyStarted time.Time, mood Mood, bytesSent, buddyBytes int) {
	finished := time.Now()
	var totalTime, waitingTime float64
	var hasWaitingTime bool
	var totalBytes int

	if !buddyStarted.IsZero() {
		earliest := minTime([]time.Time{started, buddyStarted})
		latest := maxTime([]time.Time{started, buddyStarted})
		totalTime = finished.Sub(earliest).Seconds()
		waitingTime = latest.Sub(earliest).Seconds()
		hasWaitingTime = true
		totalBytes = bytesSent + buddyBytes
	} else {
		totalTime = finished.Sub(started).Seconds()
		totalBytes = bytesSent
		// note that "bytesSent" should always be 0 here, but we're
		// recording what the state-machine remembered anyway
	}

	// Blur if configured
	if ut.blurUsage > 0 {
		started = blurStartTime(started, ut.blurUsage)
		totalBytes = blurSize(totalBytes)
	}

	var waiting *float64
	if hasWaitingTime {
		waiting = &waitingTime
	}
	if len(ut.backends) == 0 {
		return
	}
	ev := UsageEvent{
		Started:     started,
		TotalTime:   totalTime,
		WaitingTime: waiting,
		TotalBytes:  totalBytes,
		Mood:        mood,
	}
	select {
	case ut.queue <- queueMsg{ev: &ev}:
	default:
		dropped := atomic.AddUint64(&ut.dropped, 1)
		if dropped%usageDropLogEvery == 1 {
			log.Printf("warning: usage queue full; dropped %d usage events so far", dropped)
		}
	}
}

func minTime(t []time.Time) time.Time {
	if t[0].Before(t[1]) {
		return t[0]
	}
	return t[1]
}

func maxTime(t []time.Time) time.Time {
	if t[0].After(t[1]) {
		return t[0]
	}
	return t[1]
}

func blurStartTime(t time.Time, coarseness int) time.Time {
	ts := t.Unix()
	blurred := (ts / int64(coarseness)) * int64(coarseness)
	return time.Unix(blurred, 0)
}

func roundTo(size, coarseness int64) int64 {
	return coarseness * (1 + (size-1)/coarseness)
}

func blurSize(size int) int {
	if size == 0 {
		return 0
	}
	if size < int(1e6) {
		return int(roundTo(int64(size), int64(10e3)))
	}
	if size < int(1e9) {
		return int(roundTo(int64(size), int64(1e6)))
	}
	return int(roundTo(int64(size), int64(100e6)))
}

// UpdateStats updates global statistics; the stats row is tiny and
// infrequent (every 5 minutes), so it stays on the synchronous path.
func (ut *UsageTracker) UpdateStats(rebooted, updated time.Time, connected, waiting, incompleteBytes int) {
	for _, backend := range ut.backends {
		backend.UpdateStats(rebooted, updated, connected, waiting, incompleteBytes)
	}
}

// LogFileUsageWriter writes usage records to a file, one JSON object per line
type LogFileUsageWriter struct {
	file *os.File
	mu   sync.Mutex
	w    *bufio.Writer
}

// NewLogFileUsageWriter creates a LogFileUsageWriter with a persistent
// buffer; writes from many goroutines are serialized.
func NewLogFileUsageWriter(file *os.File) *LogFileUsageWriter {
	return &LogFileUsageWriter{file: file, w: bufio.NewWriter(file)}
}

func (lw *LogFileUsageWriter) RecordUsage(ev UsageEvent) {
	data := map[string]interface{}{
		"started":      float64(ev.Started.UnixNano()) / 1e9,
		"total_time":   ev.TotalTime,
		"waiting_time": ev.WaitingTime,
		"total_bytes":  ev.TotalBytes,
		"mood":         string(ev.Mood),
	}
	line, err := json.Marshal(data)
	if err != nil {
		log.Printf("warning: unable to marshal usage record: %v", err)
		return
	}
	lw.mu.Lock()
	defer lw.mu.Unlock()
	lw.w.Write(line)
	lw.w.WriteByte('\n')
	// flush per record so log consumers see events promptly
	_ = lw.w.Flush()
}

func (lw *LogFileUsageWriter) UpdateStats(rebooted, updated time.Time, connected, waiting, incompleteBytes int) {
	// not implemented for log files (the Python reference only updates the
	// database backend as well)
}

// DatabaseUsageWriter writes usage records to an SQLite database. It is the
// equivalent of DatabaseUsageRecorder in the Python reference; the schema is
// copied verbatim from db-schemas/v1.sql in magic-wormhole-transit-relay.
type DatabaseUsageWriter struct {
	db *sql.DB
}

func (dw *DatabaseUsageWriter) RecordUsage(ev UsageEvent) {
	if _, err := dw.db.Exec("INSERT INTO `usage`"+
		" (`started`, `total_time`, `waiting_time`, `total_bytes`, `result`)"+
		" VALUES (?,?,?,?,?)",
		float64(ev.Started.UnixNano())/1e9, ev.TotalTime, ev.WaitingTime, ev.TotalBytes, string(ev.Mood)); err != nil {
		log.Printf("warning: unable to record usage: %v", err)
		return
	}
}

// RecordUsageBatch writes a batch of events in a single transaction; this is
// the hot path under churn and cuts per-record commit overhead.
func (dw *DatabaseUsageWriter) RecordUsageBatch(events []UsageEvent) {
	tx, err := dw.db.Begin()
	if err != nil {
		log.Printf("warning: unable to begin usage batch: %v", err)
		return
	}
	stmt, err := tx.Prepare("INSERT INTO `usage`" +
		" (`started`, `total_time`, `waiting_time`, `total_bytes`, `result`)" +
		" VALUES (?,?,?,?,?)")
	if err != nil {
		_ = tx.Rollback()
		log.Printf("warning: unable to prepare usage insert: %v", err)
		return
	}
	for _, ev := range events {
		if _, err := stmt.Exec(float64(ev.Started.UnixNano())/1e9, ev.TotalTime, ev.WaitingTime, ev.TotalBytes, string(ev.Mood)); err != nil {
			log.Printf("warning: unable to record usage (batch): %v", err)
		}
	}
	_ = stmt.Close()
	if err := tx.Commit(); err != nil {
		log.Printf("warning: unable to commit usage batch: %v", err)
	}
}

func (dw *DatabaseUsageWriter) UpdateStats(rebooted, updated time.Time, connected, waiting, incompleteBytes int) {
	if _, err := dw.db.Exec("DELETE FROM `current`"); err != nil {
		fmt.Printf("warning: unable to clear current usage stats: %v\n", err)
		return
	}
	if _, err := dw.db.Exec("INSERT INTO `current`"+
		" (`rebooted`, `updated`, `connected`, `waiting`, `incomplete_bytes`)"+
		" VALUES (?,?,?,?,?)",
		rebooted.Unix(), updated.Unix(), connected, waiting, incompleteBytes); err != nil {
		fmt.Printf("warning: unable to record current usage stats: %v\n", err)
	}
}

// Close closes the underlying database, if any.
func (dw *DatabaseUsageWriter) Close() error {
	if dw.db != nil {
		return dw.db.Close()
	}
	return nil
}

// Force UsageTracker to satisfy UsageRecorder
var _ UsageRecorder = (*UsageTracker)(nil)
