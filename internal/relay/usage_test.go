package relay

import (
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestUsageDBCreateRecordAndStats(t *testing.T) {
	usageFile := filepath.Join(t.TempDir(), "relay-usage.sqlite")

	tracker, err := CreateUsageTracker(0, "", usageFile)
	if err != nil {
		t.Fatal(err)
	}

	started := time.Unix(1794506000, 0)
	buddyStarted := started.Add(3 * time.Second)
	tracker.Record(started, buddyStarted, MoodHappy, 100, 200)

	db, err := OpenUsageDB(usageFile)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var gotStarted, totalTime, waitingTime float64
	var totalBytes int64
	var result string
	if err := db.QueryRow("SELECT `started`, `total_time`, `waiting_time`, `total_bytes`, `result` FROM `usage`").
		Scan(&gotStarted, &totalTime, &waitingTime, &totalBytes, &result); err != nil {
		t.Fatal(err)
	}
	// started is stored as float epoch seconds (like the Python reference)
	if math.Abs(gotStarted-float64(started.Unix())) > 1 {
		t.Fatalf("started off by more than a second: %v vs %v", gotStarted, started.Unix())
	}
	if totalBytes != 300 {
		t.Fatalf("expected total_bytes=300, got %d", totalBytes)
	}
	if result != "happy" {
		t.Fatalf("expected result=happy, got %q", result)
	}
	if waitingTime < 2 || waitingTime > 4 {
		t.Fatalf("expected waiting_time around 3, got %v", waitingTime)
	}
	if totalTime <= 0 {
		t.Fatalf("expected positive total_time, got %v", totalTime)
	}

	// lonely: no buddy, waiting_time must be NULL
	tracker.Record(started, time.Time{}, MoodLonely, 0, 0)
	var waiting interface{}
	if err := db.QueryRow("SELECT `waiting_time` FROM `usage` WHERE `result`='lonely'").Scan(&waiting); err != nil {
		t.Fatal(err)
	}
	if waiting != nil {
		t.Fatalf("expected waiting_time NULL for lonely, got %v", waiting)
	}

	// stats snapshot: one row, updated in place on subsequent calls
	rebooted := started
	tracker.UpdateStats(rebooted, started.Add(time.Minute), 3, 2, 42)
	var gotRebooted, gotUpdated, connected, waitingN, incomplete int64
	if err := db.QueryRow("SELECT `rebooted`, `updated`, `connected`, `waiting`, `incomplete_bytes` FROM `current`").
		Scan(&gotRebooted, &gotUpdated, &connected, &waitingN, &incomplete); err != nil {
		t.Fatal(err)
	}
	if gotRebooted != rebooted.Unix() || connected != 3 || waitingN != 2 || incomplete != 42 {
		t.Fatalf("wrong current row: rebooted=%d connected=%d waiting=%d incomplete=%d",
			gotRebooted, connected, waitingN, incomplete)
	}
	tracker.UpdateStats(rebooted, started.Add(2*time.Minute), 1, 0, 0)
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM `current`").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("expected exactly one current row, got %d", count)
	}

	// version table says v1
	var version int
	if err := db.QueryRow("SELECT `version` FROM `version`").Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != 1 {
		t.Fatalf("expected version 1, got %d", version)
	}
}

func TestUsageDBBlur(t *testing.T) {
	usageFile := filepath.Join(t.TempDir(), "relay-usage-blur.sqlite")
	tracker, err := CreateUsageTracker(100, "", usageFile)
	if err != nil {
		t.Fatal(err)
	}
	started := time.Unix(1794506123, 0) // not a multiple of 100
	tracker.Record(started, time.Time{}, MoodLonely, 0, 0)

	db, err := OpenUsageDB(usageFile)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var gotStarted float64
	if err := db.QueryRow("SELECT `started` FROM `usage`").Scan(&gotStarted); err != nil {
		t.Fatal(err)
	}
	want := float64(started.Unix() / 100 * 100)
	if gotStarted != want {
		t.Fatalf("expected blurred started=%v, got %v", want, gotStarted)
	}
}

func TestUsageDBInMemory(t *testing.T) {
	db, err := OpenUsageDB(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var version int
	if err := db.QueryRow("SELECT `version` FROM `version`").Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != 1 {
		t.Fatalf("expected version 1, got %d", version)
	}
}

func TestUsageDBInvalidFile(t *testing.T) {
	badFile := filepath.Join(t.TempDir(), "not-a-db.sqlite")
	if err := writeGarbageFile(badFile); err != nil {
		t.Fatal(err)
	}
	// a garbage file is opened, then rejected by the version check
	if _, err := OpenUsageDB(badFile); err == nil {
		t.Fatal("expected error opening garbage db file")
	}
}

func writeGarbageFile(path string) error {
	return os.WriteFile(path, []byte("this is not sqlite"), 0644)
}
