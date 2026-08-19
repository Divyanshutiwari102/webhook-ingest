package ingest_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/convin/webhook-ingest/internal/ingest"
	"github.com/convin/webhook-ingest/internal/stats"
	"github.com/convin/webhook-ingest/internal/store"
	"github.com/convin/webhook-ingest/internal/testutil"
)

// eventJSON builds a well-formed call-completion payload.
func eventJSON(eventID, callID, accountID string) string {
	return fmt.Sprintf(`{
	  "event_id":      %q,
	  "call_id":       %q,
	  "account_id":    %q,
	  "status":        "completed",
	  "duration_sec":  143,
	  "recording_url": "https://recordings.example.com/%s.wav",
	  "occurred_at":   "2026-08-13T09:12:00Z"
	}`, eventID, callID, accountID, callID)
}

func post(t *testing.T, url, body string) *http.Response {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

func TestWebhookStoresEventAndCall(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)
	if resp := post(t, srv.URL+"/webhooks/calls", body); resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d, want 200", resp.StatusCode)
	}

	exists, err := st.EventExists(ctx, eventID)
	if err != nil {
		t.Fatalf("EventExists: %v", err)
	}
	if !exists {
		t.Fatal("expected the event to be stored")
	}

	var gotAccount string
	row := st.Pool().QueryRow(ctx, `SELECT account_id FROM calls WHERE call_id = $1`, callID)
	if err := row.Scan(&gotAccount); err != nil {
		t.Fatalf("expected a call record for %s: %v", callID, err)
	}
	if gotAccount != accountID {
		t.Fatalf("call belongs to %q, want %q", gotAccount, accountID)
	}
}

func TestDuplicateDeliveryIsIgnored(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)
	for i := 0; i < 3; i++ {
		if resp := post(t, srv.URL+"/webhooks/calls", body); resp.StatusCode != http.StatusOK {
			t.Fatalf("delivery %d: got %d, want 200", i, resp.StatusCode)
		}
	}

	var n int
	row := st.Pool().QueryRow(ctx, `SELECT count(*) FROM events WHERE event_id = $1`, eventID)
	if err := row.Scan(&n); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if n != 1 {
		t.Fatalf("stored %d copies of %s, want 1", n, eventID)
	}
}

// TestConcurrentDuplicateDelivery ensures that concurrent webhook deliveries with the
// same event_id result in exactly one stored event and correct account stats.
func TestConcurrentDuplicateDelivery(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)

	// Send 10 concurrent requests.
	const numRequests = 10
	var wg sync.WaitGroup
	wg.Add(numRequests)
	for i := 0; i < numRequests; i++ {
		go func() {
			defer wg.Done()
			if resp := post(t, srv.URL+"/webhooks/calls", body); resp.StatusCode != http.StatusOK {
				t.Errorf("request got %d, want 200", resp.StatusCode)
			}
		}()
	}
	wg.Wait()

	// Check that only one event was stored.
	var n int
	row := st.Pool().QueryRow(ctx, `SELECT count(*) FROM events WHERE event_id = $1`, eventID)
	if err := row.Scan(&n); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if n != 1 {
		t.Fatalf("stored %d copies of %s, want 1", n, eventID)
	}

	// Check that the call record exists.
	var gotAccount string
	row = st.Pool().QueryRow(ctx, `SELECT account_id FROM calls WHERE call_id = $1`, callID)
	if err := row.Scan(&gotAccount); err != nil {
		t.Fatalf("expected a call record for %s: %v", callID, err)
	}
	if gotAccount != accountID {
		t.Fatalf("call belongs to %q, want %q", gotAccount, accountID)
	}

	// Check that account stats show exactly one call.
	got, err := st.AccountStats(ctx, accountID)
	if err != nil {
		t.Fatalf("AccountStats: %v", err)
	}
	if got.CallCount != 1 {
		t.Fatalf("expected call count 1, got %d", got.CallCount)
	}
	// The total duration should match the event's duration.
	if got.TotalDurationSec != int64(143) { // from eventJSON
		t.Fatalf("expected total duration 143, got %d", got.TotalDurationSec)
	}
}

// TestRecordingProcessedAfterRequestEnds verifies that recording processing completes
// even after the HTTP request context is cancelled (i.e., the client disconnects).
func TestRecordingProcessedAfterRequestEnds(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)
	// This event has a recording URL, so recording processing will be triggered.
	if resp := post(t, srv.URL+"/webhooks/calls", body); resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d, want 200", resp.StatusCode)
	}

	// Poll the database until the recording is marked processed or timeout.
	const timeout = 5 * time.Second
	deadline := time.Now().Add(timeout)
	var processed bool
	for time.Now().Before(deadline) {
		row := st.Pool().QueryRow(ctx, `SELECT recording_processed FROM calls WHERE call_id = $1`, callID)
		err := row.Scan(&processed)
		if err != nil {
			t.Fatalf("scan: %v", err)
		}
		if processed {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !processed {
		t.Fatalf("recording not processed after %v", timeout)
	}
}

// TestServiceWaitTimeout tests the WaitTimeout method of the Service.
// It verifies that WaitTimeout returns nil when the work finishes before the timeout,
// and returns an error when the timeout is exceeded.
func TestServiceWaitTimeout(t *testing.T) {
	st := testutil.NewStore(t)
	// Create a service with a discarded logger.
	svc := ingest.New(st, stats.NewCache(), nil, slog.New(slog.NewTextHandler(io.Discard, nil)))

	// Create an event with a recording URL to trigger a recording goroutine.
	eventID, callID, accountID := testutil.IDs(t, st)
	event := store.Event{
		EventID:      eventID,
		CallID:       callID,
		AccountID:    accountID,
		Status:       "completed",
		DurationSec:  10,
		RecordingURL: "https://example.com/a.wav",
		OccurredAt:   time.Now(),
		Payload:      []byte(`{}`),
	}

	// Start the ingest process, which will spawn a recording goroutine.
	if err := svc.Ingest(context.Background(), ingest.Event{
		EventID:      event.EventID,
		CallID:       event.CallID,
		AccountID:    event.AccountID,
		Status:       event.Status,
		DurationSec:  event.DurationSec,
		RecordingURL: event.RecordingURL,
		OccurredAt:   event.OccurredAt,
	}); err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	// First, test that WaitTimeout returns nil when given a duration longer than the work takes.
	// The recording work takes 50ms (recordingWork constant). We'll give it 200ms.
	var waitErr error
	if waitErr = svc.WaitTimeout(200 * time.Millisecond); waitErr != nil {
		t.Fatalf("WaitTimeout should have succeeded but got error: %v", waitErr)
	}

	// Now, test that WaitTimeout returns an error when given a duration shorter than the work.
	// We need to spawn another recording goroutine.
	event2 := store.Event{
		EventID:      eventID + "2",
		CallID:       callID + "2",
		AccountID:    accountID,
		Status:       "completed",
		DurationSec:  10,
		RecordingURL: "https://example.com/b.wav",
		OccurredAt:   time.Now(),
		Payload:      []byte(`{}`),
	}
	if err := svc.Ingest(context.Background(), ingest.Event{
		EventID:      event2.EventID,
		CallID:       event2.CallID,
		AccountID:    event2.AccountID,
		Status:       event2.Status,
		DurationSec:  event2.DurationSec,
		RecordingURL: event2.RecordingURL,
		OccurredAt:   event2.OccurredAt,
	}); err != nil {
		t.Fatalf("Ingest failed for second event: %v", err)
	}
	// Give it only 10ms, which is less than the 50ms work.
	if waitErr = svc.WaitTimeout(10 * time.Millisecond); waitErr == nil {
		t.Fatalf("WaitTimeout should have timed out but returned nil")
	}
	// Ensure the error is a timeout error.
	if !strings.Contains(waitErr.Error(), "timeout waiting for background recording processing") {
		t.Fatalf("WaitTimeout error does not indicate timeout: %v", waitErr)
	}

	// Finally, wait for the goroutine to finish with a longer timeout to clean up.
	if waitErr = svc.WaitTimeout(200 * time.Millisecond); waitErr != nil {
		t.Fatalf("WaitTimeout should have succeeded after work completed but got error: %v", waitErr)
	}
}
