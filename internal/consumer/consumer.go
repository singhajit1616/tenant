package consumer

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"controlplane/internal/domain"
	"controlplane/internal/store"
)

type Consumer struct {
	store *store.Store
	log   *slog.Logger
}

// New creates a progress consumer backed by the store and logger.
func New(store *store.Store, log *slog.Logger) *Consumer {
	return &Consumer{store: store, log: log}
}

// Handle parses and validates progress, logs and skips poison messages, and applies valid
// updates within a ten-second database timeout; transient failures are returned for retry.
func (c *Consumer) Handle(data []byte) error {
	var u domain.WorkerUpdate
	if err := json.Unmarshal(data, &u); err != nil {
		c.log.Error("poison progress message, skipping", "err", err, "payload", string(data))
		return nil
	}
	if err := u.Validate(); err != nil {
		c.log.Error("invalid progress message, skipping", "err", err, "payload", string(data))
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := c.store.ApplyWorkerUpdate(ctx, u, domain.Now()); err != nil {
		c.log.Warn("apply worker update failed", "err", err, "update_id", u.UpdateID, "task_id", u.TaskID)
		return err
	}
	c.log.Info("applied worker update", "update_id", u.UpdateID, "task_id", u.TaskID, "status", u.Status)
	return nil
}
