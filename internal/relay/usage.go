package relay

import (
	"bufio"
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

// UsageWriter interface for recording usage statistics
type UsageWriter interface {
	RecordUsage(started, buddyStarted time.Time, totalTime, waitingTime float64, totalBytes int, mood Mood)
	UpdateStats(rebooted, updated time.Time, connected, waiting, incompleteBytes int)
}

// CreateUsageTracker creates a new UsageTracker
func CreateUsageTracker(blurUsage int, logFile string, usageDB string) (*UsageTracker, error) {
	tracker := &UsageTracker{
		backends:  map[string]UsageWriter{},
		blurUsage: blurUsage,
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
		tracker.backends["database"] = &DatabaseUsageWriter{path: usageDB}
	}
	return tracker, nil
}

// Record records a usage event
func (ut *UsageTracker) Record(started, buddyStarted time.Time, mood Mood, bytesSent, buddyBytes int) {
	finished := time.Now()
	var totalTime, waitingTime float64
	var totalBytes int

	if !buddyStarted.IsZero() {
		starts := []time.Time{started, buddyStarted}
		earliest := minTime(starts)
		latest := maxTime(starts)
		totalTime = finished.Sub(earliest).Seconds()
		waitingTime = latest.Sub(earliest).Seconds()
		totalBytes = bytesSent + buddyBytes
	} else {
		totalTime = finished.Sub(started).Seconds()
		waitingTime = 0.0
		totalBytes = bytesSent
	}

	// Blur if configured
	if ut.blurUsage > 0 {
		started = blurStartTime(started, ut.blurUsage)
		totalBytes = blurSize(totalBytes)
	}

	ut.notifyBackends(map[string]interface{}{
		"started":       started,
		"total_time":    totalTime,
		"waiting_time":  waitingTime,
		"total_bytes":   totalBytes,
		"mood":          string(mood),
	})
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

func (ut *UsageTracker) notifyBackends(data map[string]interface{}) {
	for _, backend := range ut.backends {
		if writer, ok := backend.(UsageWriter); ok {
			writer.RecordUsage(
				data["started"].(time.Time),
				time.Time{},
				data["total_time"].(float64),
				data["waiting_time"].(float64),
				data["total_bytes"].(int),
				Mood(data["mood"].(string)),
			)
		}
	}
}

// UpdateStats updates general statistics
func (ut *UsageTracker) UpdateStats(rebooted, updated time.Time, connected, waiting, incompleteBytes int) {
	for _, backend := range ut.backends {
		if writer, ok := backend.(UsageWriter); ok {
			writer.UpdateStats(rebooted, updated, connected, waiting, incompleteBytes)
		}
	}
}

// LogFileUsageWriter writes usage records to a file
type LogFileUsageWriter struct {
	file *os.File
}

func (lw *LogFileUsageWriter) RecordUsage(started, buddyStarted time.Time, totalTime, waitingTime float64, totalBytes int, mood Mood) {
	data := map[string]interface{}{
		"started":      started.Unix(),
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
	// Not implemented for log files
}

// DatabaseUsageWriter writes usage records to an SQLite database
type DatabaseUsageWriter struct {
	path string
}

func (dw *DatabaseUsageWriter) RecordUsage(started, buddyStarted time.Time, totalTime, waitingTime float64, totalBytes int, mood Mood) {
	// TODO: implement SQLite-backed usage recording
}

func (dw *DatabaseUsageWriter) UpdateStats(rebooted, updated time.Time, connected, waiting, incompleteBytes int) {
	// TODO: implement SQLite-backed current-stats update
}

// Force UsageTracker to satisfy UsageRecorder
var _ UsageRecorder = (*UsageTracker)(nil)