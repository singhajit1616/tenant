package api_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"controlplane/internal/api"
	"controlplane/internal/testutil"
)

// apiServer creates an HTTP handler backed by an isolated migrated test schema.
func apiServer(t *testing.T) http.Handler {
	t.Helper()
	st, _ := testutil.OpenStore(t)
	return api.New(st).Router()
}

// TestCreateAndGetTenant creates a tenant over HTTP and confirms it can be retrieved in
// provisioning state.
func TestCreateAndGetTenant(t *testing.T) {
	h := apiServer(t)
	body := []byte(`{"slug":"api-acme","name":"Acme"}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/tenants", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create: %d %s", rec.Code, rec.Body.String())
	}
	var tenant map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &tenant); err != nil {
		t.Fatal(err)
	}
	if tenant["status"] != "provisioning" || tenant["slug"] != "api-acme" {
		t.Fatalf("tenant: %v", tenant)
	}
	id, _ := tenant["id"].(string)
	req = httptest.NewRequest(http.MethodGet, "/v1/tenants/"+id, nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("get: %d %s", rec.Code, rec.Body.String())
	}
}

// TestDuplicateSlugHTTP verifies that a second create with the same slug returns HTTP 409
// and tenant_already_exists.
func TestDuplicateSlugHTTP(t *testing.T) {
	h := apiServer(t)
	body := []byte(`{"slug":"http-dup","name":"A"}`)
	for i := 0; i < 2; i++ {
		req := httptest.NewRequest(http.MethodPost, "/v1/tenants", bytes.NewReader(body))
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if i == 0 && rec.Code != http.StatusCreated {
			t.Fatalf("first create: %d %s", rec.Code, rec.Body.String())
		}
		if i == 1 {
			if rec.Code != http.StatusConflict {
				t.Fatalf("second create: %d %s", rec.Code, rec.Body.String())
			}
			var env struct {
				Error struct {
					Code string `json:"code"`
				} `json:"error"`
			}
			_ = json.Unmarshal(rec.Body.Bytes(), &env)
			if env.Error.Code != "tenant_already_exists" {
				t.Fatalf("error body: %s", rec.Body.String())
			}
		}
	}
}

// TestInvalidSlug verifies that an invalid tenant slug returns HTTP 400.
func TestInvalidSlug(t *testing.T) {
	h := apiServer(t)
	req := httptest.NewRequest(http.MethodPost, "/v1/tenants", bytes.NewReader([]byte(`{"slug":"NO","name":"A"}`)))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("got %d %s", rec.Code, rec.Body.String())
	}
}

// TestTaskNotFound verifies that an unknown task UUID returns HTTP 404.
func TestTaskNotFound(t *testing.T) {
	h := apiServer(t)
	req := httptest.NewRequest(http.MethodGet, "/v1/tasks/00000000-0000-0000-0000-000000000001", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("got %d %s", rec.Code, rec.Body.String())
	}
}
