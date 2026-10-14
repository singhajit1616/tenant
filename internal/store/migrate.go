package store

import (
	"context"
	"embed"
	"fmt"
)

//go:embed schema.sql
var schemaFS embed.FS

// Migrate executes the embedded schema to create any missing tables and indexes, returning
// errors when loading or applying it fails.
func Migrate(ctx context.Context, s *Store) error {
	sql, err := schemaFS.ReadFile("schema.sql")
	if err != nil {
		return fmt.Errorf("read schema: %w", err)
	}
	if _, err := s.pool.Exec(ctx, string(sql)); err != nil {
		return fmt.Errorf("apply schema: %w", err)
	}
	return nil
}
