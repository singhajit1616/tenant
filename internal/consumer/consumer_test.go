package consumer

import (
	"io"
	"log/slog"
	"testing"
)

// TestPoisonMessagesAreAcknowledgedWithoutDatabase verifies that malformed or invalid
// progress returns success for acknowledgement without accessing the database.
func TestPoisonMessagesAreAcknowledgedWithoutDatabase(t *testing.T) {
	c := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	for _, payload := range []string{
		"not JSON", "null", "{}",
		`{"update_id":"x","task_id":"invalid","status":"done"}`,
		`{"update_id":"x","task_id":"00000000-0000-0000-0000-000000000001","status":"accepted"}`,
	} {
		if err := c.Handle([]byte(payload)); err != nil {
			t.Fatalf("poison %q: %v", payload, err)
		}
	}
}
