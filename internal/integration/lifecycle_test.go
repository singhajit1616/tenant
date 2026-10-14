package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"controlplane/internal/api"
	"controlplane/internal/broker"
	"controlplane/internal/consumer"
	"controlplane/internal/domain"
	"controlplane/internal/outbox"
	"controlplane/internal/store"
	"controlplane/internal/testutil"
	"github.com/google/uuid"
)

// This exercises the real HTTP handler, DB, relay, RabbitMQ, worker process,
// and progress consumer. The Compose quality command requires it to run.
func TestEndToEnd(t *testing.T) {
	url, binary := os.Getenv("RABBITMQ_URL"), os.Getenv("WORKER_BINARY")
	if url == "" || binary == "" {
		if os.Getenv("REQUIRE_INTEGRATION") == "1" {
			t.Fatal("RABBITMQ_URL and WORKER_BINARY required")
		}
		t.Skip("run Compose quality command for full process integration")
	}
	proxy := newBrokerProxy(t, url)
	url = proxy.URL()
	st, ctx := testutil.OpenStore(t)
	ctx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)
	bus, err := broker.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(bus.Close)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	cons := consumer.New(st, log)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = bus.ConsumeProgress(ctx, cons.Handle)
	}()
	relayDone := make(chan struct{})
	go func() { defer close(relayDone); outbox.New(st, bus, log).Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("consumer did not stop")
		}
		select {
		case <-relayDone:
		case <-time.After(5 * time.Second):
			t.Error("relay did not stop")
		}
	})
	h := api.New(st).Router()
	stopWorker := startWorker(t, binary, url, "0")
	request := func(method, path string, body any, status int) *httptest.ResponseRecorder {
		t.Helper()
		data, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(method, path, bytes.NewReader(data)))
		if rec.Code != status {
			t.Fatalf("%s %s: %d %s", method, path, rec.Code, rec.Body.String())
		}
		return rec
	}
	create := func(slug string) domain.Tenant {
		t.Helper()
		rec := request("POST", "/v1/tenants", map[string]string{"slug": slug, "name": "Integration"}, 201)
		var tenant domain.Tenant
		if err := json.Unmarshal(rec.Body.Bytes(), &tenant); err != nil {
			t.Fatal(err)
		}
		return tenant
	}

	tenant := create("e2e-" + uuid.NewString()[:8])
	got := waitTenant(t, ctx, st, tenant.ID, domain.TenantActive)
	if got.Version != 3 {
		t.Fatalf("deploy version=%d want 3", got.Version)
	}
	request("PATCH", "/v1/tenants/"+tenant.ID, map[string]any{"name": "Updated", "version": got.Version}, 200)
	got = waitTenant(t, ctx, st, tenant.ID, domain.TenantActive)
	if got.Name != "Updated" || got.Version != 6 {
		t.Fatalf("update: %+v", got)
	}
	request("DELETE", "/v1/tenants/"+tenant.ID, nil, 202)
	got = waitTenant(t, ctx, st, tenant.ID, domain.TenantDestroyed)
	if got.Version != 9 {
		t.Fatalf("destroy version=%d want 9", got.Version)
	}
	rec := request("GET", "/v1/tenants/"+tenant.ID+"/tasks", nil, 200)
	var page struct {
		Items []domain.Task `json:"items"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &page); err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 3 {
		t.Fatalf("tasks=%d want 3", len(page.Items))
	}
	for _, task := range page.Items {
		if task.Status != domain.TaskDone {
			t.Fatalf("task not done: %+v", task)
		}
		request("GET", "/v1/tasks/"+task.ID, nil, 200)
	}
	stopWorker()
	stopFailWorker := startWorker(t, binary, url, "1")
	failed := create("fail-" + uuid.NewString()[:8])
	waitTenant(t, ctx, st, failed.ID, domain.TenantFailed)
	request("DELETE", "/v1/tenants/"+failed.ID, nil, 202)
	waitTenant(t, ctx, st, failed.ID, domain.TenantFailed)
	stopFailWorker()

	// Deliver terminal first, then the same message twice, stale progress,
	// and a conflicting terminal outcome. All must preserve the first outcome.
	manual := create("manual-" + uuid.NewString()[:8])
	tasks, _, err := st.ListTasks(ctx, store.ListParams{TenantID: manual.ID, Limit: 10})
	if err != nil || len(tasks) != 1 {
		t.Fatalf("manual task: %+v %v", tasks, err)
	}
	// RabbitMQ delivers duplicates; MessageId is metadata, so database
	// idempotency must handle repeated logical updates.
	updates := []domain.WorkerUpdate{
		{UpdateID: "manual-done-" + manual.ID, TaskID: tasks[0].ID, Status: domain.TaskDone},
		{UpdateID: "manual-done-" + manual.ID, TaskID: tasks[0].ID, Status: domain.TaskDone},
		{UpdateID: "manual-stale-" + manual.ID, TaskID: tasks[0].ID, Status: domain.TaskInProgress},
		{UpdateID: "manual-conflict-" + manual.ID, TaskID: tasks[0].ID, Status: domain.TaskFailed},
	}
	for _, payload := range [][]byte{[]byte("invalid json"), []byte(`{"task_id":"bad","status":"done","update_id":"poison"}`)} {
		if err := bus.Publish(ctx, broker.QueueProgress, "poison", payload); err != nil {
			t.Fatal(err)
		}
	}
	for _, u := range updates {
		payload, err := json.Marshal(u)
		if err != nil {
			t.Fatal(err)
		}
		if err := bus.Publish(ctx, broker.QueueProgress, u.UpdateID, payload); err != nil {
			t.Fatal(err)
		}
	}
	waitTenant(t, ctx, st, manual.ID, domain.TenantActive)
	deadline := time.Now().Add(10 * time.Second)
	for {
		var count int
		err := st.Pool().QueryRow(ctx, "SELECT COUNT(*) FROM processed_updates WHERE task_id=$1", tasks[0].ID).Scan(&count)
		if err != nil {
			t.Fatal(err)
		}
		if count == 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("processed updates=%d want 3", count)
		}
		time.Sleep(20 * time.Millisecond)
	}
	got, err = st.GetTenant(ctx, manual.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != domain.TenantActive || got.Version != 2 {
		t.Fatalf("duplicates regressed or mutated tenant: %+v", got)
	}

	// Disconnect actual RabbitMQ clients. The API still commits its outbox while
	// delivery is unavailable, then the same process recovers automatically.
	proxy.pause()
	outage := create("outage-" + uuid.NewString()[:8])
	var pending int
	if err := st.Pool().QueryRow(ctx, `SELECT COUNT(*) FROM outbox o JOIN tasks t ON t.id=o.id
		WHERE t.tenant_id=$1 AND o.published_at IS NULL`, outage.ID).Scan(&pending); err != nil {
		t.Fatal(err)
	}
	if pending != 1 {
		t.Fatalf("outage pending outbox=%d want 1", pending)
	}
	proxy.resume()
	stopRecoveredWorker := startWorker(t, binary, url, "0")
	waitTenant(t, ctx, st, outage.ID, domain.TenantActive)
	stopRecoveredWorker()

	request("GET", "/v1/tenants/"+tenant.ID, nil, http.StatusOK)
}

// waitTenant polls the store until the tenant reaches the expected status, failing on
// database errors or a 15-second timeout.
func waitTenant(t *testing.T, ctx context.Context, st *store.Store, id string, status domain.TenantStatus) *domain.Tenant {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		got, err := st.GetTenant(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if got.Status == status {
			return got
		}
		if time.Now().After(deadline) {
			t.Fatalf("tenant %s stuck at %s, want %s", id, got.Status, status)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// startWorker launches the actual worker executable with short delays and the requested
// fail rate and returns an idempotent stop function registered for test cleanup.
func startWorker(t *testing.T, binary, url, rate string) func() {
	t.Helper()
	logFile, err := os.Create(filepath.Join(t.TempDir(), "worker.log"))
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(binary, "--fail-rate="+rate, "--min-delay-ms=20", "--max-delay-ms=40")
	cmd.Env = append(os.Environ(), "RABBITMQ_URL="+url)
	cmd.Stdout, cmd.Stderr = logFile, logFile
	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		t.Fatal(err)
	}
	stopped := false
	stop := func() {
		if stopped {
			return
		}
		stopped = true
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		_ = logFile.Close()
	}
	t.Cleanup(stop)
	return stop
}
