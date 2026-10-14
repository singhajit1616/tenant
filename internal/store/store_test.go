package store_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"controlplane/internal/domain"
	"controlplane/internal/store"
	"controlplane/internal/testutil"

	"github.com/google/uuid"
)

// openStore delegates to the shared helper to create an isolated database store and
// context for a test.
func openStore(t *testing.T) (*store.Store, context.Context) {
	return testutil.OpenStore(t)
}

// createProvisioning creates a provisioning tenant with an accepted deploy task and outbox
// record in the test database.
func createProvisioning(t *testing.T, st *store.Store, ctx context.Context, slug string) (*domain.Tenant, *domain.Task) {
	t.Helper()
	now := domain.Now()
	tenant, err := domain.NewTenant(uuid.NewString(), slug, "Acme", now)
	if err != nil {
		t.Fatal(err)
	}
	task := domain.NewTask(uuid.NewString(), domain.TaskDeploy, tenant.ID, now)
	if err := st.CreateTenant(ctx, tenant, task); err != nil {
		t.Fatal(err)
	}
	return tenant, task
}

// TestCreateTenantDuplicateSlug checks that the store rejects an existing slug with
// tenant_already_exists.
func TestCreateTenantDuplicateSlug(t *testing.T) {
	st, ctx := openStore(t)
	_, _ = createProvisioning(t, st, ctx, "dup-slug")
	now := domain.Now()
	tenant, err := domain.NewTenant(uuid.NewString(), "dup-slug", "Other", now)
	if err != nil {
		t.Fatal(err)
	}
	task := domain.NewTask(uuid.NewString(), domain.TaskDeploy, tenant.ID, now)
	err = st.CreateTenant(ctx, tenant, task)
	de, ok := domain.AsError(err)
	if !ok || de.Code != domain.CodeTenantAlreadyExists {
		t.Fatalf("got %v, want tenant_already_exists", err)
	}
}

// TestConcurrentCreateSameSlug verifies that 16 concurrent creates produce exactly one
// success and 15 duplicate-slug errors.
func TestConcurrentCreateSameSlug(t *testing.T) {
	st, ctx := openStore(t)
	const n = 16
	errCh := make(chan error, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			now := domain.Now()
			tenant, err := domain.NewTenant(uuid.NewString(), "race-slug", "Acme", now)
			if err != nil {
				errCh <- err
				return
			}
			task := domain.NewTask(uuid.NewString(), domain.TaskDeploy, tenant.ID, now)
			errCh <- st.CreateTenant(ctx, tenant, task)
		}()
	}
	wg.Wait()
	close(errCh)
	var ok, exists int
	for err := range errCh {
		if err == nil {
			ok++
			continue
		}
		de, isDom := domain.AsError(err)
		if isDom && de.Code == domain.CodeTenantAlreadyExists {
			exists++
			continue
		}
		t.Fatalf("unexpected error: %v", err)
	}
	if ok != 1 || exists != n-1 {
		t.Fatalf("success=%d already_exists=%d want 1/%d", ok, exists, n-1)
	}
}

// activate applies a terminal-first deploy success to make a test tenant active.
func activate(t *testing.T, st *store.Store, ctx context.Context, task *domain.Task) {
	t.Helper()
	err := st.ApplyWorkerUpdate(ctx, domain.WorkerUpdate{
		UpdateID: uuid.NewString(),
		TaskID:   task.ID,
		Status:   domain.TaskDone,
	}, domain.Now())
	if err != nil {
		t.Fatal(err)
	}
}

// TestWorkerUpdateIdempotentAndOrderTolerant applies duplicate and stale progress after
// completion and verifies that task and tenant statuses do not regress.
func TestWorkerUpdateIdempotentAndOrderTolerant(t *testing.T) {
	st, ctx := openStore(t)
	tenant, task := createProvisioning(t, st, ctx, "msg-tenant")

	doneID := "update-done-1"
	if err := st.ApplyWorkerUpdate(ctx, domain.WorkerUpdate{
		UpdateID: doneID, TaskID: task.ID, Status: domain.TaskDone,
	}, domain.Now()); err != nil {
		t.Fatal(err)
	}
	if err := st.ApplyWorkerUpdate(ctx, domain.WorkerUpdate{
		UpdateID: doneID, TaskID: task.ID, Status: domain.TaskDone,
	}, domain.Now()); err != nil {
		t.Fatal(err)
	}
	if err := st.ApplyWorkerUpdate(ctx, domain.WorkerUpdate{
		UpdateID: "stale-in-progress", TaskID: task.ID, Status: domain.TaskInProgress,
	}, domain.Now()); err != nil {
		t.Fatal(err)
	}

	gotTask, err := st.GetTask(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if gotTask.Status != domain.TaskDone {
		t.Fatalf("task status %s, want done", gotTask.Status)
	}
	gotTenant, err := st.GetTenant(ctx, tenant.ID)
	if err != nil {
		t.Fatal(err)
	}
	if gotTenant.Status != domain.TenantActive {
		t.Fatalf("tenant status %s, want active", gotTenant.Status)
	}
}

// TestConcurrentPatchSameVersion verifies one successful mutation and one new task/outbox
// record when concurrent PATCH requests use the same version.
func TestConcurrentPatchSameVersion(t *testing.T) {
	st, ctx := openStore(t)
	tenant, task := createProvisioning(t, st, ctx, "patch-race")
	activate(t, st, ctx, task)
	got, err := st.GetTenant(ctx, tenant.ID)
	if err != nil {
		t.Fatal(err)
	}

	const n = 12
	errCh := make(chan error, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			name := fmt.Sprintf("name-%d", i)
			_, _, err := st.PatchTenant(ctx, got.ID, &name, got.Version, domain.Now(), uuid.NewString())
			errCh <- err
		}()
	}
	wg.Wait()
	close(errCh)

	var success, conflict int
	for err := range errCh {
		if err == nil {
			success++
			continue
		}
		de, ok := domain.AsError(err)
		if ok && de.Code == domain.CodeTenantVersionConflict {
			conflict++
			continue
		}
		t.Fatalf("unexpected error: %v", err)
	}
	if success != 1 || conflict != n-1 {
		t.Fatalf("success=%d conflict=%d want 1/%d", success, conflict, n-1)
	}

	after, err := st.GetTenant(ctx, tenant.ID)
	if err != nil {
		t.Fatal(err)
	}
	if after.Status != domain.TenantUpdating || after.Version != got.Version+1 {
		t.Fatalf("after patch: status=%s version=%d", after.Status, after.Version)
	}
	for _, table := range []string{"tasks", "outbox"} {
		var count int
		if err := st.Pool().QueryRow(ctx, "SELECT COUNT(*) FROM "+table).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 2 {
			t.Fatalf("%s rows=%d, want deploy + exactly one update", table, count)
		}
	}
}

// TestStateGuards verifies that updating or deleting a provisioning tenant is rejected.
func TestStateGuards(t *testing.T) {
	st, ctx := openStore(t)
	tenant, _ := createProvisioning(t, st, ctx, "guard-tenant")
	name := "nope"
	_, _, err := st.PatchTenant(ctx, tenant.ID, &name, 1, domain.Now(), uuid.NewString())
	de, ok := domain.AsError(err)
	if !ok || de.Code != domain.CodeTenantUpdateNotAllowed {
		t.Fatalf("patch while provisioning: %v", err)
	}
	_, _, err = st.DeleteTenant(ctx, tenant.ID, domain.Now(), uuid.NewString())
	de, ok = domain.AsError(err)
	if !ok || de.Code != domain.CodeTenantUpdateNotAllowed {
		t.Fatalf("delete while provisioning: %v", err)
	}
}

// TestOutboxWrittenWithTenant verifies that tenant creation writes one pending outbox
// event and that the event can be claimed.
func TestOutboxWrittenWithTenant(t *testing.T) {
	st, ctx := openStore(t)
	_, task := createProvisioning(t, st, ctx, "outbox-tenant")
	var n int
	if err := st.Pool().QueryRow(ctx, `SELECT COUNT(*) FROM outbox WHERE id = $1 AND published_at IS NULL`, task.ID).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("outbox rows=%d want 1", n)
	}
	called := false
	if err := st.WithOutboxClaim(ctx, 10, func(rows []store.OutboxRow) error {
		called = true
		if len(rows) != 1 {
			t.Fatalf("claimed %d", len(rows))
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("expected outbox claim")
	}
}
