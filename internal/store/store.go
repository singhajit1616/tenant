package store

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"controlplane/internal/domain"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct {
	pool *pgxpool.Pool
}

// New wraps an existing PostgreSQL connection pool in the store.
func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// Pool exposes the underlying PostgreSQL connection pool.
func (s *Store) Pool() *pgxpool.Pool {
	return s.pool
}

// Ping checks database connectivity using the supplied context.
func (s *Store) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

type ListParams struct {
	Limit    int
	Offset   int
	TenantID string
	Status   string
}

// CreateTenant inserts the tenant, task, and outbound event atomically and translates
// uniqueness violations into a tenant-already-exists error.
func (s *Store) CreateTenant(ctx context.Context, tenant *domain.Tenant, task *domain.Task) error {
	return s.withTx(ctx, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `
			INSERT INTO tenants (id, slug, name, status, version, created_at, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7)`,
			tenant.ID, tenant.Slug, tenant.Name, tenant.Status, tenant.Version,
			tenant.CreatedAt.Time(), tenant.UpdatedAt.Time(),
		)
		if err != nil {
			return mapUniqueSlug(err)
		}
		return insertTaskAndOutbox(ctx, tx, task)
	})
}

// GetTenant loads a tenant by ID and returns a tenant-not-found error when no row exists.
func (s *Store) GetTenant(ctx context.Context, id string) (*domain.Tenant, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT id, slug, name, status, version, created_at, updated_at
		FROM tenants WHERE id = $1`, id)
	tenant, err := scanTenant(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, domain.NewError(domain.CodeTenantNotFound, "tenant not found")
	}
	return tenant, err
}

// ListTenants reads a page of tenants ordered newest first, including destroyed tenants,
// and returns the total tenant count.
func (s *Store) ListTenants(ctx context.Context, p ListParams) ([]domain.Tenant, int, error) {
	var total int
	if err := s.pool.QueryRow(ctx, `SELECT COUNT(*) FROM tenants`).Scan(&total); err != nil {
		return nil, 0, err
	}
	rows, err := s.pool.Query(ctx, `
		SELECT id, slug, name, status, version, created_at, updated_at
		FROM tenants
		ORDER BY created_at DESC, id DESC
		LIMIT $1 OFFSET $2`, p.Limit, p.Offset)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	items := make([]domain.Tenant, 0)
	for rows.Next() {
		t, err := scanTenant(rows)
		if err != nil {
			return nil, 0, err
		}
		items = append(items, *t)
	}
	return items, total, rows.Err()
}

// PatchTenant locks the tenant, checks its version and active state, applies an optional
// name, increments its version, and atomically creates an update task and outbox record.
func (s *Store) PatchTenant(ctx context.Context, id string, name *string, version int64, now domain.Time, taskID string) (*domain.Tenant, *domain.Task, error) {
	var tenant *domain.Tenant
	var task *domain.Task
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		t, err := getTenantForUpdate(ctx, tx, id)
		if err != nil {
			return err
		}
		if t.Version != version {
			return domain.NewError(domain.CodeTenantVersionConflict, "tenant version conflict")
		}
		if !domain.CanUpdate(t.Status) {
			return domain.NewError(domain.CodeTenantUpdateNotAllowed, "tenant can only be updated from active")
		}
		if name != nil {
			if err := domain.ValidateName(*name); err != nil {
				return err
			}
			t.Name = *name
		}
		t.Status = domain.TenantUpdating
		t.Version++
		t.UpdatedAt = now
		if err := updateTenant(ctx, tx, t); err != nil {
			return err
		}
		task = domain.NewTask(taskID, domain.TaskUpdate, t.ID, now)
		if err := insertTaskAndOutbox(ctx, tx, task); err != nil {
			return err
		}
		tenant = t
		return nil
	})
	return tenant, task, err
}

// DeleteTenant locks an active or failed tenant, marks it destroying, increments its
// version, and atomically creates a destroy task and outbox record.
func (s *Store) DeleteTenant(ctx context.Context, id string, now domain.Time, taskID string) (*domain.Tenant, *domain.Task, error) {
	var tenant *domain.Tenant
	var task *domain.Task
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		t, err := getTenantForUpdate(ctx, tx, id)
		if err != nil {
			return err
		}
		if !domain.CanDelete(t.Status) {
			return domain.NewError(domain.CodeTenantUpdateNotAllowed, "tenant can only be deleted from active or failed")
		}
		t.Status = domain.TenantDestroying
		t.Version++
		t.UpdatedAt = now
		if err := updateTenant(ctx, tx, t); err != nil {
			return err
		}
		task = domain.NewTask(taskID, domain.TaskDestroy, t.ID, now)
		if err := insertTaskAndOutbox(ctx, tx, task); err != nil {
			return err
		}
		tenant = t
		return nil
	})
	return tenant, task, err
}

// GetTask loads a task by ID and returns a task-not-found error when no row exists.
func (s *Store) GetTask(ctx context.Context, id string) (*domain.Task, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT id, type, tenant_id, status, created_at, updated_at
		FROM tasks WHERE id = $1`, id)
	task, err := scanTask(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, domain.NewError(domain.CodeTaskNotFound, "task not found")
	}
	return task, err
}

// ListTasks returns a page and count of tasks matching optional tenant and status filters,
// ordered newest first.
func (s *Store) ListTasks(ctx context.Context, p ListParams) ([]domain.Task, int, error) {
	const filter = ` WHERE ($1 = '' OR tenant_id = NULLIF($1, '')::uuid)
		AND ($2 = '' OR status = $2)`
	var total int
	if err := s.pool.QueryRow(ctx, "SELECT COUNT(*) FROM tasks"+filter, p.TenantID, p.Status).Scan(&total); err != nil {
		return nil, 0, err
	}
	rows, err := s.pool.Query(ctx, `
		SELECT id, type, tenant_id, status, created_at, updated_at
		FROM tasks`+filter+`
		ORDER BY created_at DESC, id DESC
		LIMIT $3 OFFSET $4`, p.TenantID, p.Status, p.Limit, p.Offset)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	items := make([]domain.Task, 0)
	for rows.Next() {
		task, err := scanTask(rows)
		if err != nil {
			return nil, 0, err
		}
		items = append(items, *task)
	}
	return items, total, rows.Err()
}

// ApplyWorkerUpdate atomically claims the update ID, applies valid forward task progress,
// and increments the tenant version; terminal outcomes also change tenant status while
// duplicate, stale, and unknown-task updates make no resource changes.
func (s *Store) ApplyWorkerUpdate(ctx context.Context, u domain.WorkerUpdate, now domain.Time) error {
	if err := u.Validate(); err != nil {
		return err
	}
	return s.withTx(ctx, func(tx pgx.Tx) error {
		// Claim the idempotency key before applying anything. Concurrent deliveries
		// of the same key wait for the winning transaction, even across tasks.
		claim, err := tx.Exec(ctx, `INSERT INTO processed_updates (update_id, task_id, processed_at)
			VALUES ($1, $2, $3) ON CONFLICT (update_id) DO NOTHING`, u.UpdateID, u.TaskID, now.Time())
		if err != nil {
			return err
		}
		if claim.RowsAffected() == 0 {
			return nil
		}

		task, err := getTaskForUpdate(ctx, tx, u.TaskID)
		if err != nil {
			if de, ok := domain.AsError(err); ok && de.Code == domain.CodeTaskNotFound {
				return nil
			}
			return err
		}

		next, apply := domain.ApplyTaskStatus(task.Status, u.Status)
		if !apply {
			return nil
		}
		if _, err := tx.Exec(ctx, `UPDATE tasks SET status = $1, updated_at = $2 WHERE id = $3`,
			next, now.Time(), task.ID); err != nil {
			return err
		}
		tenant, err := getTenantForUpdate(ctx, tx, task.TenantID)
		if err != nil {
			return err
		}
		tenant.Version++
		tenant.UpdatedAt = now
		if newStatus, ok := domain.TenantStatusAfterTask(task.Type, next); ok {
			tenant.Status = newStatus
		}
		return updateTenant(ctx, tx, tenant)
	})
}

type OutboxRow struct {
	ID      string
	Payload []byte
}

// WithOutboxClaim locks a batch of committed unpublished events while skipping rows
// claimed elsewhere, invokes the publisher callback, and commits published timestamps only
// if the callback succeeds.
func (s *Store) WithOutboxClaim(ctx context.Context, limit int, fn func([]OutboxRow) error) error {
	return s.withTx(ctx, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			SELECT id, payload
			FROM outbox
			WHERE published_at IS NULL
			ORDER BY created_at
			FOR UPDATE SKIP LOCKED
			LIMIT $1`, limit)
		if err != nil {
			return err
		}
		var items []OutboxRow
		for rows.Next() {
			var r OutboxRow
			if err := rows.Scan(&r.ID, &r.Payload); err != nil {
				rows.Close()
				return err
			}
			items = append(items, r)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		if len(items) == 0 {
			return nil
		}
		if err := fn(items); err != nil {
			return err
		}
		for _, item := range items {
			if _, err := tx.Exec(ctx, `UPDATE outbox SET published_at = NOW() WHERE id = $1`, item.ID); err != nil {
				return err
			}
		}
		return nil
	})
}

// withTx runs a callback in a database transaction, commits on success, and rolls back
// uncommitted changes on errors or exit.
func (s *Store) withTx(ctx context.Context, fn func(pgx.Tx) error) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// insertTaskAndOutbox inserts a task and its serialized event into the caller's
// transaction without publishing to the broker.
func insertTaskAndOutbox(ctx context.Context, tx pgx.Tx, task *domain.Task) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO tasks (id, type, tenant_id, status, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		task.ID, task.Type, task.TenantID, task.Status, task.CreatedAt.Time(), task.UpdatedAt.Time(),
	)
	if err != nil {
		return err
	}
	payload, err := json.Marshal(task.Event())
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO outbox (id, payload, created_at)
		VALUES ($1, $2, $3)`, task.ID, payload, task.CreatedAt.Time())
	return err
}

// getTenantForUpdate loads and locks a tenant row for the transaction, preventing
// concurrent mutation until commit or rollback.
func getTenantForUpdate(ctx context.Context, tx pgx.Tx, id string) (*domain.Tenant, error) {
	row := tx.QueryRow(ctx, `
		SELECT id, slug, name, status, version, created_at, updated_at
		FROM tenants WHERE id = $1 FOR UPDATE`, id)
	tenant, err := scanTenant(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, domain.NewError(domain.CodeTenantNotFound, "tenant not found")
	}
	return tenant, err
}

// getTaskForUpdate loads and locks a task row for the transaction so competing progress
// updates cannot change it simultaneously.
func getTaskForUpdate(ctx context.Context, tx pgx.Tx, id string) (*domain.Task, error) {
	row := tx.QueryRow(ctx, `
		SELECT id, type, tenant_id, status, created_at, updated_at
		FROM tasks WHERE id = $1 FOR UPDATE`, id)
	task, err := scanTask(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, domain.NewError(domain.CodeTaskNotFound, "task not found")
	}
	return task, err
}

// updateTenant persists the tenant's mutable fields inside an existing transaction; the
// caller supplies the new version and timestamps.
func updateTenant(ctx context.Context, tx pgx.Tx, t *domain.Tenant) error {
	_, err := tx.Exec(ctx, `
		UPDATE tenants
		SET name = $1, status = $2, version = $3, updated_at = $4
		WHERE id = $5`,
		t.Name, t.Status, t.Version, t.UpdatedAt.Time(), t.ID)
	return err
}

type scanner interface {
	Scan(dest ...any) error
}

// scanTenant converts a database row into a Tenant and normalizes its timestamps to UTC
// milliseconds.
func scanTenant(row scanner) (*domain.Tenant, error) {
	var t domain.Tenant
	var created, updated time.Time
	if err := row.Scan(&t.ID, &t.Slug, &t.Name, &t.Status, &t.Version, &created, &updated); err != nil {
		return nil, err
	}
	t.CreatedAt = domain.NewTime(created)
	t.UpdatedAt = domain.NewTime(updated)
	return &t, nil
}

// scanTask converts a database row into a Task and normalizes its timestamps to UTC
// milliseconds.
func scanTask(row scanner) (*domain.Task, error) {
	var t domain.Task
	var created, updated time.Time
	if err := row.Scan(&t.ID, &t.Type, &t.TenantID, &t.Status, &created, &updated); err != nil {
		return nil, err
	}
	t.CreatedAt = domain.NewTime(created)
	t.UpdatedAt = domain.NewTime(updated)
	return &t, nil
}

// mapUniqueSlug maps PostgreSQL uniqueness violations to tenant_already_exists and returns
// other errors unchanged.
func mapUniqueSlug(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return domain.NewError(domain.CodeTenantAlreadyExists, "tenant already exists")
	}
	return err
}
