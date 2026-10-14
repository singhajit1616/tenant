package domain

import (
	"regexp"
	"strings"
)

var slugRE = regexp.MustCompile(`^[a-z][-a-z0-9]{1,26}[a-z0-9]$`)

type TenantStatus string

const (
	TenantProvisioning TenantStatus = "provisioning"
	TenantActive       TenantStatus = "active"
	TenantUpdating     TenantStatus = "updating"
	TenantDestroying   TenantStatus = "destroying"
	TenantDestroyed    TenantStatus = "destroyed"
	TenantFailed       TenantStatus = "failed"
)

type Tenant struct {
	ID        string       `json:"id"`
	Slug      string       `json:"slug"`
	Name      string       `json:"name"`
	Status    TenantStatus `json:"status"`
	Version   int64        `json:"version"`
	CreatedAt Time         `json:"created_at"`
	UpdatedAt Time         `json:"updated_at"`
}

// ValidateSlug enforces the tenant slug's allowed characters, start and end rules, and
// 3-28 character length.
func ValidateSlug(slug string) error {
	if !slugRE.MatchString(slug) {
		return NewError(CodeInvalidRequest, "slug must be 3-28 characters and match ^[a-z][-a-z0-9]{1,26}[a-z0-9]$")
	}
	return nil
}

// ValidateName rejects names that are empty or contain only whitespace.
func ValidateName(name string) error {
	if strings.TrimSpace(name) == "" {
		return NewError(CodeInvalidRequest, "name must be non-empty")
	}
	return nil
}

// CanUpdate allows tenant updates only when the tenant is active.
func CanUpdate(status TenantStatus) bool {
	return status == TenantActive
}

// CanDelete allows destruction only when the tenant is active or failed.
func CanDelete(status TenantStatus) bool {
	return status == TenantActive || status == TenantFailed
}

// TenantStatusAfterTask determines the tenant status for a terminal task outcome and
// reports false when there is no applicable status change.
func TenantStatusAfterTask(taskType TaskType, taskStatus TaskStatus) (TenantStatus, bool) {
	if taskStatus == TaskFailed {
		return TenantFailed, true
	}
	if taskStatus != TaskDone {
		return "", false
	}
	switch taskType {
	case TaskDeploy, TaskUpdate:
		return TenantActive, true
	case TaskDestroy:
		return TenantDestroyed, true
	default:
		return "", false
	}
}

// NewTenant validates the slug and name and constructs a provisioning tenant at version
// one with the supplied ID and timestamps.
func NewTenant(id, slug, name string, now Time) (*Tenant, error) {
	if err := ValidateSlug(slug); err != nil {
		return nil, err
	}
	if err := ValidateName(name); err != nil {
		return nil, err
	}
	return &Tenant{
		ID:        id,
		Slug:      slug,
		Name:      strings.TrimSpace(name),
		Status:    TenantProvisioning,
		Version:   1,
		CreatedAt: now,
		UpdatedAt: now,
	}, nil
}
