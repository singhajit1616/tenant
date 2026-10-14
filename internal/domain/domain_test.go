package domain

import (
	"encoding/json"
	"testing"
	"time"
)

// TestTimestampContract checks UTC conversion, millisecond precision, the Z suffix, and
// timestamp JSON round trips.
func TestTimestampContract(t *testing.T) {
	stamp := NewTime(time.Date(2026, 10, 9, 12, 30, 45, 123456789, time.FixedZone("IST", 19800)))
	data, err := json.Marshal(stamp)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != `"2026-10-09T07:00:45.123Z"` {
		t.Fatalf("timestamp: %s", data)
	}
	var roundTrip Time
	if err := json.Unmarshal(data, &roundTrip); err != nil {
		t.Fatal(err)
	}
	if !roundTrip.Equal(stamp) {
		t.Fatal("timestamp round trip changed")
	}
}

// TestValidateSlug checks representative valid and invalid tenant slugs.
func TestValidateSlug(t *testing.T) {
	t.Parallel()
	ok := []string{"abc", "acme-prod", "a-b", "tenant01"}
	for _, s := range ok {
		if err := ValidateSlug(s); err != nil {
			t.Errorf("ValidateSlug(%q) unexpected error: %v", s, err)
		}
	}
	bad := []string{"", "ab", "A-bc", "-abc", "abc-", "1abc", "ab_c", "a..b"}
	for _, s := range bad {
		if err := ValidateSlug(s); err == nil {
			t.Errorf("ValidateSlug(%q) want error", s)
		}
	}
}

// repeat builds a repeated string used to construct slug length boundary cases.
func repeat(s string, n int) string {
	out := ""
	for i := 0; i < n; i++ {
		out += s
	}
	return out
}

// TestSlugLengthBounds verifies the 3-character minimum and 28-character maximum for
// slugs.
func TestSlugLengthBounds(t *testing.T) {
	t.Parallel()
	three := "ab1"
	if err := ValidateSlug(three); err != nil {
		t.Fatalf("3-char slug should pass: %v", err)
	}
	// 28 chars: a + 26 of [a-z0-9-] + trailing [a-z0-9]
	s28 := "a" + repeat("b", 26) + "c"
	if len(s28) != 28 {
		t.Fatalf("setup: got len %d", len(s28))
	}
	if err := ValidateSlug(s28); err != nil {
		t.Fatalf("28-char slug should pass: %v", err)
	}
	s29 := "a" + repeat("b", 27) + "c"
	if err := ValidateSlug(s29); err == nil {
		t.Fatal("29-char slug should fail")
	}
}

// TestCanUpdateDelete checks which tenant states allow update and destroy operations.
func TestCanUpdateDelete(t *testing.T) {
	t.Parallel()
	if !CanUpdate(TenantActive) {
		t.Fatal("active should allow update")
	}
	for _, s := range []TenantStatus{TenantProvisioning, TenantUpdating, TenantDestroying, TenantDestroyed, TenantFailed} {
		if CanUpdate(s) {
			t.Errorf("status %s should not allow update", s)
		}
	}
	if !CanDelete(TenantActive) || !CanDelete(TenantFailed) {
		t.Fatal("active and failed should allow delete")
	}
	for _, s := range []TenantStatus{TenantProvisioning, TenantUpdating, TenantDestroying, TenantDestroyed} {
		if CanDelete(s) {
			t.Errorf("status %s should not allow delete", s)
		}
	}
}

// TestApplyTaskStatus checks forward transitions and verifies that repeated, backward, and
// conflicting terminal updates do not change task state.
func TestApplyTaskStatus(t *testing.T) {
	t.Parallel()
	cases := []struct {
		current, incoming TaskStatus
		wantNext          TaskStatus
		wantApply         bool
	}{
		{TaskAccepted, TaskInProgress, TaskInProgress, true},
		{TaskAccepted, TaskDone, TaskDone, true},
		{TaskAccepted, TaskFailed, TaskFailed, true},
		{TaskInProgress, TaskDone, TaskDone, true},
		{TaskInProgress, TaskFailed, TaskFailed, true},
		{TaskInProgress, TaskInProgress, TaskInProgress, false},
		{TaskDone, TaskInProgress, TaskDone, false},
		{TaskDone, TaskFailed, TaskDone, false},
		{TaskFailed, TaskDone, TaskFailed, false},
		{TaskDone, TaskDone, TaskDone, false},
		{TaskAccepted, TaskAccepted, TaskAccepted, false},
		{TaskInProgress, TaskAccepted, TaskInProgress, false},
	}
	for _, tc := range cases {
		next, apply := ApplyTaskStatus(tc.current, tc.incoming)
		if next != tc.wantNext || apply != tc.wantApply {
			t.Errorf("ApplyTaskStatus(%s,%s)=(%s,%v) want (%s,%v)",
				tc.current, tc.incoming, next, apply, tc.wantNext, tc.wantApply)
		}
	}
}

// TestTenantStatusAfterTask checks tenant outcomes for each task type and confirms that
// nonterminal progress produces no status change.
func TestTenantStatusAfterTask(t *testing.T) {
	t.Parallel()
	assert := func(typ TaskType, st TaskStatus, want TenantStatus, ok bool) {
		t.Helper()
		got, gotOK := TenantStatusAfterTask(typ, st)
		if got != want || gotOK != ok {
			t.Errorf("%s/%s -> (%s,%v) want (%s,%v)", typ, st, got, gotOK, want, ok)
		}
	}
	assert(TaskDeploy, TaskDone, TenantActive, true)
	assert(TaskUpdate, TaskDone, TenantActive, true)
	assert(TaskDestroy, TaskDone, TenantDestroyed, true)
	assert(TaskDeploy, TaskFailed, TenantFailed, true)
	assert(TaskUpdate, TaskFailed, TenantFailed, true)
	assert(TaskDestroy, TaskFailed, TenantFailed, true)
	assert(TaskDeploy, TaskInProgress, "", false)
}

// TestNewTenant checks the initial provisioning state, version one, and trimmed tenant
// name.
func TestNewTenant(t *testing.T) {
	t.Parallel()
	now := Now()
	tenant, err := NewTenant("t1", "acme-prod", " Acme ", now)
	if err != nil {
		t.Fatal(err)
	}
	if tenant.Status != TenantProvisioning || tenant.Version != 1 || tenant.Name != "Acme" {
		t.Fatalf("tenant: %+v", tenant)
	}
}
