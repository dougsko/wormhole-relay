package relay

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"
)

// UsageTracker tracks usage statistics of connections
type UsageTracker struct {
	backends  map[string]UsageWriter
	blurUsage int
	logFile   *os.File
}

// UsageWriter interface for recording usage statistics. waitingTime is nil
// when the connection was never paired (lonely); the database records NULL,
// matching the Python reference.
type UsageWriter interface {
	RecordUsage(started time.Time, totalTime float64, waitingTime *float64, totalBytes int, mood Mood)
	UpdateStats(rebooted, updated time.Time, connected, waiting, incompleteBytes int)
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
			tracker.backends["log_file"] = &LogFileUsageWriter{file: file}
		}
	}
	if usageDB != "" {
		db, err := OpenUsageDB(usageDB)
		if err != nil {
			return nil, fmt.Errorf("failed to open usage db %q: %v", usageDB, err)
		}
		tracker.backends["database"] = &DatabaseUsageWriter{db: db}
	}
	return tracker, nil
}

// Record records a usage event
func (ut *UsageTracker) Record(started, buddyStarted time.Time, mood Mood, bytesSent, buddyBytes int) {
	finished := time.Now()
	var totalTime, waitingTime float64
	var hasWaitingTime bool
	var totalBytes int

	if !buddyStarted.IsZero() {
		starts := []time.Time{started, buddyStarted}
		earliest := minTime(starts)
		latest := maxTime(starts)
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
	ut.notifyBackends(started, totalTime, waiting, totalBytes, mood)
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

func (ut *UsageTracker) notifyBackends(started time.Time, totalTime float64, waitingTime *float64, totalBytes int, mood Mood) {
	for _, backend := range ut.backends {
		backend.RecordUsage(started, totalTime, waitingTime, totalBytes, mood)
	}
}

// UpdateStats updates global statistics
func (ut *UsageTracker) UpdateStats(rebooted, updated time.Time, connected, waiting, incompleteBytes int) {
	for _, backend := range ut.backends {
		backend.UpdateStats(rebooted, updated, connected, waiting, incompleteBytes)
	}
}

// LogFileUsageWriter writes usage records to a file, one JSON object per line
type LogFileUsageWriter struct {
	file *os.File
}

func (lw *LogFileUsageWriter) RecordUsage(started time.Time, totalTime float64, waitingTime *float64, totalBytes int, mood Mood) {
	data := map[string]interface{}{
		"started":      float64(started.UnixNano()) / 1e9,
		"total_time":   totalTime,
		"waiting_time": waitingTime,
		"total_bytes":  totalBytes,
		"mood":         string(mood),
	}
	writer := bufio.NewWriter(lw.file)
	jsonData, _ := json.Marshal(data)
	writer.Write(jsonData)
	writer.WriteByte('\n')
	writer.Flush()
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

func (dw *DatabaseUsageWriter) RecordUsage(started time.Time, totalTime float64, waitingTime *float64, totalBytes int, mood Mood) {
	if _, err := dw.db.Exec("INSERT INTO `usage`"+
		" (`started`, `total_time`, `waiting_time`, `total_bytes`, `result`)"+
		" VALUES (?,?,?,?,?)",
		float64(started.UnixNano())/1e9, totalTime, waitingTime, totalBytes, string(mood)); err != nil {
		fmt.Printf("warning: unable to record usage: %v\n", err)
		return
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
