package domain

import "github.com/google/uuid"

type TaskType string

const (
	TaskDeploy  TaskType = "deploy"
	TaskUpdate  TaskType = "update"
	TaskDestroy TaskType = "destroy"
)

type TaskStatus string

const (
	TaskAccepted   TaskStatus = "accepted"
	TaskInProgress TaskStatus = "in_progress"
	TaskDone       TaskStatus = "done"
	TaskFailed     TaskStatus = "failed"
)

type Task struct {
	ID        string     `json:"id"`
	Type      TaskType   `json:"type"`
	TenantID  string     `json:"tenant_id"`
	Status    TaskStatus `json:"status"`
	CreatedAt Time       `json:"created_at"`
	UpdatedAt Time       `json:"updated_at"`
}

// NewTask constructs an accepted task with the supplied ID, operation type, tenant
// reference, and timestamps.
func NewTask(id string, typ TaskType, tenantID string, now Time) *Task {
	return &Task{
		ID:        id,
		Type:      typ,
		TenantID:  tenantID,
		Status:    TaskAccepted,
		CreatedAt: now,
		UpdatedAt: now,
	}
}

// Terminal reports whether a task is done or failed and therefore cannot transition again.
func (s TaskStatus) Terminal() bool {
	return s == TaskDone || s == TaskFailed
}

// Valid reports whether a value is one of the four supported task statuses.
func (s TaskStatus) Valid() bool {
	switch s {
	case TaskAccepted, TaskInProgress, TaskDone, TaskFailed:
		return true
	default:
		return false
	}
}

// ApplyTaskStatus returns the next status and whether the task row should change.
// Duplicate and stale updates are no-ops (applied=false) and never regress state.
func ApplyTaskStatus(current, incoming TaskStatus) (next TaskStatus, apply bool) {
	if !incoming.Valid() || incoming == TaskAccepted {
		return current, false
	}
	if current == incoming {
		return current, false
	}
	if current.Terminal() {
		return current, false
	}
	switch incoming {
	case TaskInProgress:
		if current == TaskAccepted {
			return incoming, true
		}
	case TaskDone, TaskFailed:
		if current == TaskAccepted || current == TaskInProgress {
			return incoming, true
		}
	}
	return current, false
}

// TaskEvent is the outbound broker payload. The task is the event.
type TaskEvent struct {
	ID       string     `json:"id"`
	Type     TaskType   `json:"type"`
	TenantID string     `json:"tenant_id"`
	Status   TaskStatus `json:"status"`
}

// Event copies the task's ID, type, tenant ID, and current status into its outbound
// message payload.
func (t Task) Event() TaskEvent {
	return TaskEvent{
		ID:       t.ID,
		Type:     t.Type,
		TenantID: t.TenantID,
		Status:   t.Status,
	}
}

// WorkerUpdate is the inbound progress message from the worker simulator.
type WorkerUpdate struct {
	UpdateID string     `json:"update_id"`
	TaskID   string     `json:"task_id"`
	Status   TaskStatus `json:"status"`
}

// Validate requires a worker update ID, a valid task UUID, and an in_progress, done, or
// failed status.
func (u WorkerUpdate) Validate() error {
	if u.UpdateID == "" {
		return NewError(CodeInvalidRequest, "update_id is required")
	}
	if u.TaskID == "" {
		return NewError(CodeInvalidRequest, "task_id is required")
	}
	if _, err := uuid.Parse(u.TaskID); err != nil {
		return NewError(CodeInvalidRequest, "task_id must be a UUID")
	}
	if u.Status != TaskInProgress && u.Status != TaskDone && u.Status != TaskFailed {
		return NewError(CodeInvalidRequest, "status must be in_progress, done, or failed")
	}
	return nil
}
