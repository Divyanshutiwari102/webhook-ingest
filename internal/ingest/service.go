// Package ingest accepts call-completion webhooks and processes them.
package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/convin/webhook-ingest/internal/stats"
	"github.com/convin/webhook-ingest/internal/store"
)

// recordingWork stands in for downloading and transcoding a recording.
const recordingWork = 50 * time.Millisecond

// Service ingests webhook deliveries.
type Service struct {
	store *store.Store
	cache *stats.Cache
	rdb   *redis.Client
	log   *slog.Logger
	wg    sync.WaitGroup
}

// New builds a Service.
func New(s *store.Store, c *stats.Cache, rdb *redis.Client, log *slog.Logger) *Service {
	return &Service{store: s, cache: c, rdb: rdb, log: log}
}

// Stats returns the cached totals for an account.
func (s *Service) Stats(accountID string) stats.AccountStats {
	return s.cache.Get(accountID)
}

// Ingest stores a delivery and kicks off processing. Processing runs
// asynchronously so the provider gets a fast acknowledgement.
func (s *Service) Ingest(ctx context.Context, evt Event) error {
	payload, err := json.Marshal(evt)
	if err != nil {
		return err
	}

	rec := store.Event{
		EventID:      evt.EventID,
		CallID:       evt.CallID,
		AccountID:    evt.AccountID,
		Status:       evt.Status,
		DurationSec:  evt.DurationSec,
		RecordingURL: evt.RecordingURL,
		OccurredAt:   evt.OccurredAt,
		Payload:      payload,
	}

	// Begin a transaction.
	tx, err := s.store.Pool().Begin(ctx)
	if err != nil {
		return err
	}
	// Make sure to rollback if there's an error.
	defer func() { _ = tx.Rollback(ctx) }()

	// Insert the event, skip if duplicate.
	inserted, err := s.store.InsertEvent(ctx, tx, rec)
	if err != nil {
		return err
	}
	if !inserted {
		s.log.Info("duplicate delivery ignored", "event_id", rec.EventID)
		return nil // The deferred rollback will roll back the transaction.
	}

	// Upsert the call record.
	if err := s.store.UpsertCall(ctx, tx, rec); err != nil {
		return err
	}

	// Increment account stats.
	if err := s.store.IncrementAccountStats(ctx, tx, rec.AccountID, rec.DurationSec); err != nil {
		return err
	}

	// Commit the transaction.
	if err := tx.Commit(ctx); err != nil {
		return err
	}

	// Update the cache after successful transaction.
	s.cache.Record(rec.AccountID, rec.DurationSec)

	// Recordings are slow to fetch, so that part does not block the provider.
	if rec.RecordingURL != "" {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if err := s.processRecording(ctx, rec); err != nil {
				s.log.Error("failed to process recording", "event_id", rec.EventID, "err", err)
			}
		}()
	}

	return nil
}

// processRecording downloads and transcodes the call recording, then marks
// the call as done.
func (s *Service) processRecording(ctx context.Context, rec store.Event) error {
	time.Sleep(recordingWork)
	return s.store.MarkRecordingProcessed(ctx, rec.CallID)
}

// Wait blocks until all recording processing goroutines have completed.
func (s *Service) Wait() {
	s.wg.Wait()
}

// WaitTimeout waits for the wait group to finish or for the timeout to expire.
// It returns an error if the timeout occurs.
func (s *Service) WaitTimeout(d time.Duration) error {
	c := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(c)
	}()
	select {
	case <-c:
		return nil
	case <-time.After(d):
		return fmt.Errorf("timeout waiting for background recording processing after %v", d)
	}
}
