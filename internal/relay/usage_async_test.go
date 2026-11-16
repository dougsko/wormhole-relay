package relay

import (
	"testing"
	"time"
)

// Closing the tracker without an explicit Flush must still persist all
// records, because the stop message is processed in queue order behind
// every queued record.
func TestUsageTrackerCloseFlushesPending(t *testing.T) {
	usageFile := t.TempDir() + "/relay-usage-close.sqlite"
	tracker, err := CreateUsageTracker(0, "", usageFile)
	if err != nil {
		t.Fatal(err)
	}
	started := time.Unix(1794506000, 0)
	const n = 150 // more than two worker batches (64)
	for i := 0; i < n; i++ {
		tracker.Record(started, started, MoodHappy, 100, 200)
	}
	if err := tracker.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := OpenUsageDB(usageFile)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM `usage`").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != n {
		t.Fatalf("expected %d usage rows after Close, got %d", n, count)
	}
}
