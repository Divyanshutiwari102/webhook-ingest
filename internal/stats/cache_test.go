package stats_test

import (
	"sync"
	"testing"

	"github.com/convin/webhook-ingest/internal/stats"
)

func TestCacheRecordAccumulates(t *testing.T) {
	c := stats.NewCache()

	c.Record("acc_1", 30)
	c.Record("acc_1", 12)
	c.Record("acc_2", 5)

	got := c.Get("acc_1")
	if got.CallCount != 2 || got.TotalDurationSec != 42 {
		t.Fatalf("acc_1: got %+v, want CallCount=2 TotalDurationSec=42", got)
	}

	other := c.Get("acc_2")
	if other.CallCount != 1 || other.TotalDurationSec != 5 {
		t.Fatalf("acc_2: got %+v, want CallCount=1 TotalDurationSec=5", other)
	}
}

func TestCacheGetUnknownAccountIsZero(t *testing.T) {
	c := stats.NewCache()
	if got := c.Get("nobody"); got.CallCount != 0 || got.TotalDurationSec != 0 {
		t.Fatalf("got %+v, want zero value", got)
	}
}

// TestCacheRecordConcurrent runs many goroutines calling Record on the same
// account to ensure the mutex protects against data races.
func TestCacheRecordConcurrent(t *testing.T) {
	c := stats.NewCache()
	const (
		numGoroutines = 100
		callsPerGR    = 100
		durationSec   = 10
	)
	var wg sync.WaitGroup
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < callsPerGR; j++ {
				c.Record("account_x", durationSec)
			}
		}()
	}
	wg.Wait()
	got := c.Get("account_x")
	expectedCount := int64(numGoroutines * callsPerGR)
	expectedDuration := expectedCount * int64(durationSec)
	if got.CallCount != expectedCount {
		t.Fatalf("expected CallCount %d, got %d", expectedCount, got.CallCount)
	}
	if got.TotalDurationSec != expectedDuration {
		t.Fatalf("expected TotalDurationSec %d, got %d", expectedDuration, got.TotalDurationSec)
	}
}
