package api

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"controlplane/internal/domain"
	"controlplane/internal/store"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/google/uuid"
)

type Server struct {
	store       *store.Store
	brokerReady func() bool
}

// WithBrokerReadiness attaches a broker connection check used by the readiness endpoint
// and returns the server.
func (s *Server) WithBrokerReadiness(check func() bool) *Server {
	s.brokerReady = check
	return s
}

// New creates an API server backed by the store.
func New(s *store.Store) *Server {
	return &Server{store: s}
}

// Router registers tenant, task, and health endpoints with request middleware, body
// limits, and structured routing errors.
func (s *Server) Router() http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(recoverJSON)
	r.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
			next.ServeHTTP(w, r)
		})
	})
	r.NotFound(func(w http.ResponseWriter, _ *http.Request) {
		writeError(w, http.StatusNotFound, domain.NewError("route_not_found", "route not found"))
	})
	r.MethodNotAllowed(func(w http.ResponseWriter, _ *http.Request) {
		writeError(w, http.StatusMethodNotAllowed, domain.NewError("method_not_allowed", "method not allowed"))
	})
	r.Get("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	r.Get("/readyz", s.ready)

	r.Post("/v1/tenants", s.createTenant)
	r.Get("/v1/tenants", s.listTenants)
	r.Get("/v1/tenants/{id}", s.getTenant)
	r.Patch("/v1/tenants/{id}", s.patchTenant)
	r.Delete("/v1/tenants/{id}", s.deleteTenant)
	r.Get("/v1/tenants/{id}/tasks", s.listTenantTasks)
	r.Get("/v1/tasks", s.listTasks)
	r.Get("/v1/tasks/{id}", s.getTask)
	// Preserve the collection URLs with a trailing slash too.
	r.Post("/v1/tenants/", s.createTenant)
	r.Get("/v1/tenants/", s.listTenants)
	r.Get("/v1/tasks/", s.listTasks)
	return r
}

// ready returns HTTP 200 when the database and configured broker check are healthy, or a
// structured HTTP 503 error otherwise.
func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	if s.brokerReady != nil && !s.brokerReady() {
		writeError(w, http.StatusServiceUnavailable, domain.NewError("not_ready", "broker unavailable"))
		return
	}
	if err := s.store.Ping(r.Context()); err != nil {
		writeError(w, http.StatusServiceUnavailable, domain.NewError("not_ready", "database unavailable"))
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

type createTenantRequest struct {
	Slug string `json:"slug"`
	Name string `json:"name"`
}

// createTenant validates the request, creates a provisioning tenant with an accepted
// deploy task and outbox record, and returns HTTP 201 after commit.
func (s *Server) createTenant(w http.ResponseWriter, r *http.Request) {
	var req createTenantRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, domain.NewError(domain.CodeInvalidRequest, err.Error()))
		return
	}
	now := domain.Now()
	tenant, err := domain.NewTenant(uuid.NewString(), req.Slug, req.Name, now)
	if err != nil {
		writeDomainError(w, err)
		return
	}
	task := domain.NewTask(uuid.NewString(), domain.TaskDeploy, tenant.ID, now)
	if err := s.store.CreateTenant(r.Context(), tenant, task); err != nil {
		writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, tenant)
}

// listTenants returns a page of tenants with pagination settings and the total count.
func (s *Server) listTenants(w http.ResponseWriter, r *http.Request) {
	limit, offset := parsePage(r)
	items, total, err := s.store.ListTenants(r.Context(), store.ListParams{Limit: limit, Offset: offset})
	if err != nil {
		writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, pageResponse{Items: items, Limit: limit, Offset: offset, Total: total})
}

// getTenant validates the path UUID and returns the tenant or a structured error if it
// cannot be found.
func (s *Server) getTenant(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if err := validateID(id); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	tenant, err := s.store.GetTenant(r.Context(), id)
	if err != nil {
		writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, tenant)
}

type patchTenantRequest struct {
	Name    *string `json:"name"`
	Version *int64  `json:"version"`
}

// patchTenant requires a positive version, trims an optional name, asks the store to start
// an update, and returns the updated tenant or a conflict error.
func (s *Server) patchTenant(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if err := validateID(id); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	var req patchTenantRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, domain.NewError(domain.CodeInvalidRequest, err.Error()))
		return
	}
	if req.Version == nil || *req.Version < 1 {
		writeError(w, http.StatusBadRequest, domain.NewError(domain.CodeInvalidRequest, "positive version is required"))
		return
	}
	if req.Name != nil {
		*req.Name = strings.TrimSpace(*req.Name)
	}
	tenant, _, err := s.store.PatchTenant(r.Context(), id, req.Name, *req.Version, domain.Now(), uuid.NewString())
	if err != nil {
		writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, tenant)
}

// deleteTenant asks the store to begin asynchronous destruction and returns HTTP 202 with
// the tenant rather than removing its database record.
func (s *Server) deleteTenant(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if err := validateID(id); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	tenant, _, err := s.store.DeleteTenant(r.Context(), id, domain.Now(), uuid.NewString())
	if err != nil {
		writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, tenant)
}

// listTenantTasks verifies that the tenant exists before returning its filtered and
// paginated tasks.
func (s *Server) listTenantTasks(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if err := validateID(id); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if _, err := s.store.GetTenant(r.Context(), id); err != nil {
		writeDomainError(w, err)
		return
	}
	s.listTasksFiltered(w, r, id)
}

// listTasks reads the optional tenant_id query filter and delegates to the shared task-
// list handler.
func (s *Server) listTasks(w http.ResponseWriter, r *http.Request) {
	s.listTasksFiltered(w, r, strings.TrimSpace(r.URL.Query().Get("tenant_id")))
}

// listTasksFiltered validates tenant and status filters and returns a page of matching
// tasks with the total count.
func (s *Server) listTasksFiltered(w http.ResponseWriter, r *http.Request, tenantID string) {
	if tenantID != "" {
		if err := validateID(tenantID); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
	}
	limit, offset := parsePage(r)
	status := strings.TrimSpace(r.URL.Query().Get("status"))
	if status != "" && !domain.TaskStatus(status).Valid() {
		writeError(w, http.StatusBadRequest, domain.NewError(domain.CodeInvalidRequest, "invalid task status"))
		return
	}
	items, total, err := s.store.ListTasks(r.Context(), store.ListParams{
		Limit:    limit,
		Offset:   offset,
		TenantID: tenantID,
		Status:   status,
	})
	if err != nil {
		writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, pageResponse{Items: items, Limit: limit, Offset: offset, Total: total})
}

// getTask validates the path UUID and returns the task or a structured task-not-found
// error.
func (s *Server) getTask(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if err := validateID(id); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	task, err := s.store.GetTask(r.Context(), id)
	if err != nil {
		writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, task)
}

type pageResponse struct {
	Items  any `json:"items"`
	Limit  int `json:"limit"`
	Offset int `json:"offset"`
	Total  int `json:"total"`
}

type errorBody struct {
	Error errorPayload `json:"error"`
}

type errorPayload struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// decodeJSON decodes one JSON value into the destination, rejecting malformed input,
// unknown fields, and extra trailing values.
func decodeJSON(r *http.Request, dst any) error {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return errors.New("malformed JSON body")
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		return errors.New("body must contain exactly one JSON object")
	}
	return nil
}

// recoverJSON wraps a handler to log recovered panics and respond with a structured HTTP
// 500 error.
func recoverJSON(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if value := recover(); value != nil {
				slog.Error("request panic", "panic", value)
				writeError(w, http.StatusInternalServerError, domain.NewError("internal_error", "internal error"))
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// parsePage reads limit and offset, uses defaults for invalid values, and caps the page
// size at 100.
func parsePage(r *http.Request) (limit, offset int) {
	limit, err := strconv.Atoi(r.URL.Query().Get("limit"))
	if err != nil || limit <= 0 {
		limit = 20
	}
	limit = min(limit, 100)
	offset, err = strconv.Atoi(r.URL.Query().Get("offset"))
	if err != nil || offset < 0 {
		offset = 0
	}
	return limit, offset
}

// validateID checks that an identifier can be parsed as a UUID and returns an invalid-
// request error otherwise.
func validateID(id string) *domain.Error {
	if _, err := uuid.Parse(id); err != nil {
		return domain.NewError(domain.CodeInvalidRequest, "id must be a UUID")
	}
	return nil
}

// writeJSON sets the JSON content type, writes the requested HTTP status, and encodes the
// response body.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// writeError sends an error code and message inside the API's standard JSON error
// envelope.
func writeError(w http.ResponseWriter, status int, err *domain.Error) {
	writeJSON(w, status, errorBody{Error: errorPayload{Code: err.Code, Message: err.Message}})
}

// writeDomainError maps domain errors to HTTP statuses and hides unexpected internal
// errors behind a generic HTTP 500 response.
func writeDomainError(w http.ResponseWriter, err error) {
	de, ok := domain.AsError(err)
	if !ok {
		writeError(w, http.StatusInternalServerError, domain.NewError("internal_error", "internal error"))
		return
	}
	status := http.StatusBadRequest
	switch de.Code {
	case domain.CodeTenantNotFound, domain.CodeTaskNotFound:
		status = http.StatusNotFound
	case domain.CodeTenantAlreadyExists, domain.CodeTenantUpdateNotAllowed, domain.CodeTenantVersionConflict:
		status = http.StatusConflict
	}
	writeError(w, status, de)
}
