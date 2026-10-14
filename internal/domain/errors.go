package domain

import "fmt"

const (
	CodeInvalidRequest         = "invalid_request"
	CodeTenantNotFound         = "tenant_not_found"
	CodeTenantAlreadyExists    = "tenant_already_exists"
	CodeTenantUpdateNotAllowed = "tenant_update_not_allowed"
	CodeTenantVersionConflict  = "tenant_version_conflict"
	CodeTaskNotFound           = "task_not_found"
)

type Error struct {
	Code    string
	Message string
}

// Error formats a domain error as its code and message, returning an empty string for a
// nil receiver.
func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

// NewError creates a domain error with a machine-readable code and a human-readable
// message.
func NewError(code, message string) *Error {
	return &Error{Code: code, Message: message}
}

// AsError checks whether the supplied error is directly a domain Error; it does not unwrap
// wrapped errors.
func AsError(err error) (*Error, bool) {
	de, ok := err.(*Error)
	return de, ok
}
