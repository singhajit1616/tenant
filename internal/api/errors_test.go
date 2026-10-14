package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestStructuredErrorsWithoutDatabase checks JSON routing and request-validation errors
// without needing a database connection.
func TestStructuredErrorsWithoutDatabase(t *testing.T) {
	h := New(nil).Router()
	for _, tc := range []struct {
		method, path, body, code string
		status                   int
	}{
		{"GET", "/missing", "", "route_not_found", 404},
		{"POST", "/v1/tasks", "", "method_not_allowed", 405},
		{"POST", "/v1/tasks/", "", "method_not_allowed", 405},
		{"GET", "/v1/tasks?tenant_id=bad", "", "invalid_request", 400},
		{"GET", "/v1/tasks?status=bad", "", "invalid_request", 400},
		{"GET", "/v1/tenants/bad", "", "invalid_request", 400},
		{"POST", "/v1/tenants", `{"slug":"abc","name":"A"} {}`, "invalid_request", 400},
		{"POST", "/v1/tenants", `{"slug":"abc","name":"A","extra":1}`, "invalid_request", 400},
		{"POST", "/v1/tenants/", `{"slug":"abc","name":"A","extra":1}`, "invalid_request", 400},
		{"PATCH", "/v1/tenants/00000000-0000-0000-0000-000000000001", `{"version":0}`, "invalid_request", 400},
	} {
		t.Run(tc.method+tc.path+tc.body, func(t *testing.T) {
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body)))
			assertError(t, rec, tc.status, tc.code)
		})
	}
}

// TestPanicReturnsJSON verifies that panic recovery produces the standard HTTP 500 JSON
// error.
func TestPanicReturnsJSON(t *testing.T) {
	rec := httptest.NewRecorder()
	recoverJSON(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { panic("test") })).ServeHTTP(rec, httptest.NewRequest("GET", "/", nil))
	assertError(t, rec, 500, "internal_error")
}

// assertError checks a response's HTTP status, JSON content type, and structured error
// code.
func assertError(t *testing.T, rec *httptest.ResponseRecorder, status int, code string) {
	t.Helper()
	var body errorBody
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if rec.Code != status || body.Error.Code != code || rec.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("got %d %s, want %d/%s", rec.Code, rec.Body.String(), status, code)
	}
}
