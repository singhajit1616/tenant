package outbox

import (
	"context"
	"log/slog"
	"time"

	"controlplane/internal/store"
)

type Publisher interface {
	PublishOutbound(ctx context.Context, payload []byte) error
}

type Relay struct {
	store *store.Store
	pub   Publisher
	log   *slog.Logger
	every time.Duration
	batch int
}

// New creates an outbox relay that checks for work every 200 milliseconds after success
// and claims up to 20 records per batch.
func New(store *store.Store, pub Publisher, log *slog.Logger) *Relay {
	return &Relay{
		store: store,
		pub:   pub,
		log:   log,
		every: 200 * time.Millisecond,
		batch: 20,
	}
}

// Run flushes immediately, waits 200 milliseconds after success, retries failures with
// delays capped at 16 seconds, and stops when cancelled.
func (r *Relay) Run(ctx context.Context) {
	backoff := time.Second

	for ctx.Err() == nil {
		delay := r.every
		if err := r.flush(ctx); err != nil && ctx.Err() == nil {
			r.log.Warn("outbox flush failed", "err", err)
			delay = backoff
			backoff = min(backoff*2, 16*time.Second)
		} else {
			backoff = time.Second
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
	}
}

// flush claims committed unpublished records, publishes each with a five-second timeout,
// and marks the batch published only if every publication succeeds.
func (r *Relay) flush(ctx context.Context) error {
	return r.store.WithOutboxClaim(ctx, r.batch, func(rows []store.OutboxRow) error {
		for _, row := range rows {
			attempt, cancel := context.WithTimeout(ctx, 5*time.Second)
			err := r.pub.PublishOutbound(attempt, row.Payload)
			cancel()
			if err != nil {
				return err
			}
		}
		return nil
	})
}
