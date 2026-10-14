package store_test

import (
	"errors"
	"sync"
	"testing"

	"controlplane/internal/domain"
	"controlplane/internal/store"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TestListTaskFiltersAndPagination checks that tenant and status filters affect
// both the returned page and total, including combined filters and empty results.
func TestListTaskFiltersAndPagination(t *testing.T) {
	st, ctx := openStore(t)
	tenant, deploy := createProvisioning(t, st, ctx, "filter-one")
	_, _ = createProvisioning(t, st, ctx, "filter-two")
	activate(t, st, ctx, deploy)
	active, err := st.GetTenant(ctx, tenant.ID)
	if err != nil {
		t.Fatal(err)
	}
	_, update, err := st.PatchTenant(ctx, tenant.ID, nil, active.Version, domain.Now(), uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	if err := st.ApplyWorkerUpdate(ctx, domain.WorkerUpdate{UpdateID: uuid.NewString(), TaskID: update.ID, Status: domain.TaskInProgress}, domain.Now()); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name         string
		params       store.ListParams
		total, count int
	}{
		{"all", store.ListParams{Limit: 20}, 3, 3},
		{"tenant", store.ListParams{TenantID: tenant.ID, Limit: 20}, 2, 2},
		{"accepted", store.ListParams{Status: "accepted", Limit: 20}, 1, 1},
		{"done", store.ListParams{Status: "done", Limit: 20}, 1, 1},
		{"combined", store.ListParams{TenantID: tenant.ID, Status: "in_progress", Limit: 20}, 1, 1},
		{"no match", store.ListParams{TenantID: tenant.ID, Status: "accepted", Limit: 20}, 0, 0},
		{"unknown tenant", store.ListParams{TenantID: uuid.NewString(), Limit: 20}, 0, 0},
		{"page", store.ListParams{Limit: 1, Offset: 1}, 3, 1},
		{"past last page", store.ListParams{Limit: 1, Offset: 3}, 3, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			items, total, err := st.ListTasks(ctx, tc.params)
			if err != nil {
				t.Fatal(err)
			}
			if total != tc.total || len(items) != tc.count {
				t.Fatalf("total=%d items=%d want %d/%d", total, len(items), tc.total, tc.count)
			}
			for _, item := range items {
				if tc.params.TenantID != "" && item.TenantID != tc.params.TenantID {
					t.Fatalf("wrong tenant: %+v", item)
				}
				if tc.params.Status != "" && string(item.Status) != tc.params.Status {
					t.Fatalf("wrong status: %+v", item)
				}
			}
		})
	}
}

// TestLifecycleAndFailureOutcomes checks tenant and task lifecycle outcomes, version
// increments, duplicate progress, and permanently reserved slugs for successful and failed
// operations.
func TestLifecycleAndFailureOutcomes(t *testing.T) {
	for _, failType := range []domain.TaskType{"", domain.TaskDeploy, domain.TaskUpdate, domain.TaskDestroy} {
		t.Run(string(failType), func(t *testing.T) {
			st, ctx := openStore(t)
			tenant, task := createProvisioning(t, st, ctx, "lifecycle")
			version := int64(1)
			finish := func(task *domain.Task) *domain.Tenant {
				t.Helper()
				for _, status := range []domain.TaskStatus{domain.TaskInProgress, domain.TaskDone} {
					if task.Type == failType && status == domain.TaskDone {
						status = domain.TaskFailed
					}
					u := domain.WorkerUpdate{UpdateID: uuid.NewString(), TaskID: task.ID, Status: status}
					if err := st.ApplyWorkerUpdate(ctx, u, domain.Now()); err != nil {
						t.Fatal(err)
					}
					version++
					// Duplicate delivery must preserve the entire tenant and task.
					if err := st.ApplyWorkerUpdate(ctx, u, domain.Now()); err != nil {
						t.Fatal(err)
					}
				}
				got, err := st.GetTenant(ctx, tenant.ID)
				if err != nil {
					t.Fatal(err)
				}
				if got.Version != version {
					t.Fatalf("version=%d want %d", got.Version, version)
				}
				return got
			}
			got := finish(task)
			if failType != domain.TaskDeploy {
				if got.Status != domain.TenantActive {
					t.Fatalf("deploy: %s", got.Status)
				}
				name := "Changed"
				_, task, err := st.PatchTenant(ctx, tenant.ID, &name, got.Version, domain.Now(), uuid.NewString())
				if err != nil {
					t.Fatal(err)
				}
				version++
				got = finish(task)
			}
			if failType == domain.TaskDeploy || failType == domain.TaskUpdate {
				if got.Status != domain.TenantFailed {
					t.Fatalf("failure: %s", got.Status)
				}
			}
			_, task, err := st.DeleteTenant(ctx, tenant.ID, domain.Now(), uuid.NewString())
			if err != nil {
				t.Fatal(err)
			}
			version++
			got = finish(task)
			want := domain.TenantDestroyed
			if failType == domain.TaskDestroy {
				want = domain.TenantFailed
			}
			if got.Status != want {
				t.Fatalf("destroy: %s want %s", got.Status, want)
			}
			// Recreating a slug must fail even after successful destruction.
			other, err := domain.NewTenant(uuid.NewString(), tenant.Slug, "Other", domain.Now())
			if err != nil {
				t.Fatal(err)
			}
			err = st.CreateTenant(ctx, other, domain.NewTask(uuid.NewString(), domain.TaskDeploy, other.ID, domain.Now()))
			de, ok := domain.AsError(err)
			if !ok || de.Code != domain.CodeTenantAlreadyExists {
				t.Fatalf("slug reuse: %v", err)
			}
		})
	}
}

// TestPersistenceAcrossConnectionRestart verifies that closing and reopening the
// connection pool preserves tenant and task records.
func TestPersistenceAcrossConnectionRestart(t *testing.T) {
	st, ctx := openStore(t)
	tenant, task := createProvisioning(t, st, ctx, "persistent")
	cfg := st.Pool().Config().Copy()
	st.Pool().Close()
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	restarted := store.New(pool)
	got, err := restarted.GetTenant(ctx, tenant.ID)
	if err != nil {
		t.Fatal(err)
	}
	if *got != *tenant {
		t.Fatalf("tenant changed across restart: %+v", got)
	}
	gotTask, err := restarted.GetTask(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if *gotTask != *task {
		t.Fatalf("task changed across restart: %+v", gotTask)
	}
}

// TestTransactionRollbackLeavesNoEvent forces task insertion to fail and verifies that
// tenant, task, and outbox writes are all rolled back.
func TestTransactionRollbackLeavesNoEvent(t *testing.T) {
	st, ctx := openStore(t)
	now := domain.Now()
	tenant, err := domain.NewTenant(uuid.NewString(), "rollback", "Rollback", now)
	if err != nil {
		t.Fatal(err)
	}
	// Invalid UUID fails after the tenant INSERT, forcing a transaction rollback.
	err = st.CreateTenant(ctx, tenant, domain.NewTask("invalid", domain.TaskDeploy, tenant.ID, now))
	if err == nil {
		t.Fatal("expected failed task insert")
	}
	for _, table := range []string{"tenants", "tasks", "outbox"} {
		var count int
		if err := st.Pool().QueryRow(ctx, "SELECT COUNT(*) FROM "+table).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("rollback left %s rows=%d", table, count)
		}
	}
}

// TestOutboxPublishFailureRetried checks that failed publication leaves an event pending
// and successful publication prevents it from being claimed again.
func TestOutboxPublishFailureRetried(t *testing.T) {
	st, ctx := openStore(t)
	_, task := createProvisioning(t, st, ctx, "publish-retry")
	failed := errors.New("broker unavailable")
	if err := st.WithOutboxClaim(ctx, 10, func([]store.OutboxRow) error { return failed }); !errors.Is(err, failed) {
		t.Fatal(err)
	}
	called := 0
	for attempt := 0; attempt < 2; attempt++ {
		if err := st.WithOutboxClaim(ctx, 10, func(rows []store.OutboxRow) error {
			called++
			if len(rows) != 1 || rows[0].ID != task.ID {
				t.Fatalf("retry rows: %+v", rows)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	if called != 1 {
		t.Fatalf("published callback ran %d times after success", called)
	}
	var published int
	if err := st.Pool().QueryRow(ctx, "SELECT COUNT(*) FROM outbox WHERE published_at IS NOT NULL").Scan(&published); err != nil {
		t.Fatal(err)
	}
	if published != 1 {
		t.Fatalf("published=%d", published)
	}
}

// TestConcurrentIdempotencyKeyAcrossTasks verifies that concurrent messages sharing an
// update ID can mutate only one task and tenant.
func TestConcurrentIdempotencyKeyAcrossTasks(t *testing.T) {
	st, ctx := openStore(t)
	a, ta := createProvisioning(t, st, ctx, "idem-one")
	b, tb := createProvisioning(t, st, ctx, "idem-two")
	var wg sync.WaitGroup
	errCh := make(chan error, 2)
	for _, task := range []*domain.Task{ta, tb} {
		wg.Add(1)
		go func(task *domain.Task) {
			defer wg.Done()
			errCh <- st.ApplyWorkerUpdate(ctx, domain.WorkerUpdate{UpdateID: "shared-key", TaskID: task.ID, Status: domain.TaskDone}, domain.Now())
		}(task)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
	var active int
	for _, tenant := range []*domain.Tenant{a, b} {
		got, err := st.GetTenant(ctx, tenant.ID)
		if err != nil {
			t.Fatal(err)
		}
		if got.Status == domain.TenantActive {
			active++
		}
	}
	if active != 1 {
		t.Fatalf("same key applied %d times", active)
	}
}
