package main

import (
	"testing"

	"github.com/google/uuid"
	"github.com/multica-ai/multica/server/internal/events"
	"github.com/multica-ai/multica/server/internal/service"
	"github.com/multica-ai/multica/server/internal/testutil"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/protocol"
)

type workspaceIdleFixture struct {
	fx          *testutil.Fixture
	bus         *events.Bus
	workspaceID string
	ownerID     string
	runtimeID   string
	agentID     string
}

func newWorkspaceIdleFixture(t *testing.T) *workspaceIdleFixture {
	t.Helper()
	seed := uuid.NewString()
	fx := testutil.New(testPool, "", "")
	ownerID := fx.User(t, "Idle Owner", "idle-owner-"+seed+"@multica.test")
	workspaceID := fx.Workspace(t, "Idle Test", "idle-test-"+seed)
	fx.WorkspaceID = workspaceID
	fx.UserID = ownerID
	fx.Cleanup(t, `DELETE FROM workspace_idle_state WHERE workspace_id = $1`, workspaceID)
	fx.Cleanup(t, `DELETE FROM inbox_item WHERE workspace_id = $1`, workspaceID)
	fx.Member(t, workspaceID, ownerID, "owner")
	runtimeID := fx.Runtime(t, "Idle Test Runtime")
	agentID := fx.Agent(t, "Idle Test Agent", runtimeID)

	bus := events.New()
	taskSvc := service.NewTaskService(db.New(testPool), testPool, nil, bus)
	registerWorkspaceIdleListeners(bus, testPool, taskSvc)
	return &workspaceIdleFixture{
		fx: fx, bus: bus, workspaceID: workspaceID, ownerID: ownerID,
		runtimeID: runtimeID, agentID: agentID,
	}
}

func (f *workspaceIdleFixture) issue(t *testing.T, assigneeType, assigneeID, status string) string {
	t.Helper()
	return f.fx.Issue(t, "Idle workflow issue", testutil.Cols{
		"workspace_id":  f.workspaceID,
		"creator_type":  "member",
		"creator_id":    f.ownerID,
		"assignee_type": assigneeType,
		"assignee_id":   assigneeID,
		"status":        status,
	})
}

func (f *workspaceIdleFixture) task(t *testing.T, issueID, status string, extra testutil.Cols) string {
	t.Helper()
	cols := testutil.Cols{
		"runtime_id":          f.runtimeID,
		"issue_id":            issueID,
		"status":              status,
		"originator_user_id":  f.ownerID,
		"accountable_user_id": f.ownerID,
		"originator_source":   "direct_human",
	}
	if status == "running" {
		cols["started_at"] = testutil.Raw("now()")
	}
	for key, value := range extra {
		cols[key] = value
	}
	return f.fx.Task(t, f.agentID, cols)
}

func (f *workspaceIdleFixture) finishTask(t *testing.T, taskID, eventType string, retryPending bool) {
	t.Helper()
	status := "completed"
	if eventType == protocol.EventTaskFailed {
		status = "failed"
	} else if eventType == protocol.EventTaskCancelled {
		status = "cancelled"
	}
	f.fx.Exec(t, `UPDATE agent_task_queue SET status = $2, completed_at = now() WHERE id = $1`, taskID, status)
	f.bus.Publish(events.Event{
		Type:        eventType,
		WorkspaceID: f.workspaceID,
		TaskID:      taskID,
		Payload: map[string]any{
			"task_id":       taskID,
			"issue_id":      f.taskIssueID(t, taskID),
			"retry_pending": retryPending,
		},
	})
}

func (f *workspaceIdleFixture) taskIssueID(t *testing.T, taskID string) string {
	t.Helper()
	var issueID string
	f.fx.QueryRow(t, `SELECT issue_id FROM agent_task_queue WHERE id = $1`, taskID).Scan(&issueID)
	return issueID
}

func (f *workspaceIdleFixture) reconciliationTasks(t *testing.T, issueID string) int {
	t.Helper()
	return f.fx.Count(t, `
		SELECT count(*) FROM agent_task_queue
		WHERE issue_id = $1 AND trigger_evidence_kind = 'workflow_reconcile'
	`, issueID)
}

func (f *workspaceIdleFixture) inboxCount(t *testing.T) int {
	t.Helper()
	return f.fx.Count(t, `
		SELECT count(*) FROM inbox_item
		WHERE workspace_id = $1 AND recipient_id = $2 AND type = 'workspace_idle'
	`, f.workspaceID, f.ownerID)
}

func TestWorkflowReconcileAutomaticallyContinuesStalledAgentIssue(t *testing.T) {
	f := newWorkspaceIdleFixture(t)
	issueID := f.issue(t, "agent", f.agentID, "in_progress")
	taskID := f.task(t, issueID, "running", nil)

	f.finishTask(t, taskID, protocol.EventTaskCompleted, false)

	if got := f.reconciliationTasks(t, issueID); got != 1 {
		t.Fatalf("workflow reconciliation tasks = %d, want 1", got)
	}
	if got := f.inboxCount(t); got != 0 {
		t.Fatalf("attention inbox rows = %d, want 0 while automatic continuation is queued", got)
	}
	var originatorID, accountableID, evidenceRef, handoffNote string
	f.fx.QueryRow(t, `
		SELECT originator_user_id, accountable_user_id, trigger_evidence_ref_id, handoff_note
		FROM agent_task_queue
		WHERE issue_id = $1 AND trigger_evidence_kind = 'workflow_reconcile'
	`, issueID).Scan(&originatorID, &accountableID, &evidenceRef, &handoffNote)
	if originatorID != f.ownerID || accountableID != f.ownerID {
		t.Fatalf("reconciliation attribution = originator %s accountable %s, want %s", originatorID, accountableID, f.ownerID)
	}
	if evidenceRef != taskID {
		t.Fatalf("reconciliation evidence = %s, want source task %s", evidenceRef, taskID)
	}
	if handoffNote == "" {
		t.Fatal("reconciliation task has no convergence instruction")
	}
}

func TestWorkspaceIdleReconcilesIssueThatStoppedBeforeTheLastTask(t *testing.T) {
	f := newWorkspaceIdleFixture(t)
	stalledIssueID := f.issue(t, "agent", f.agentID, "in_progress")
	stalledTaskID := f.task(t, stalledIssueID, "running", nil)
	gateIssueID := f.issue(t, "member", f.ownerID, "in_progress")
	gateTaskID := f.task(t, gateIssueID, "running", nil)

	f.finishTask(t, stalledTaskID, protocol.EventTaskCompleted, false)
	if got := f.reconciliationTasks(t, stalledIssueID); got != 0 {
		t.Fatalf("workflow reconciliation tasks while workspace busy = %d, want 0", got)
	}

	f.finishTask(t, gateTaskID, protocol.EventTaskCompleted, false)
	if got := f.reconciliationTasks(t, stalledIssueID); got != 1 {
		t.Fatalf("workflow reconciliation tasks after workspace became idle = %d, want 1", got)
	}
	if got := f.reconciliationTasks(t, gateIssueID); got != 0 {
		t.Fatalf("member-owned gate issue reconciliation tasks = %d, want 0", got)
	}
	if got := f.inboxCount(t); got != 0 {
		t.Fatalf("attention inbox rows = %d, want 0 while automatic continuation is queued", got)
	}
}

func TestWorkflowReconcileTreatsDeferredTaskAsPlannedWork(t *testing.T) {
	f := newWorkspaceIdleFixture(t)
	issueID := f.issue(t, "agent", f.agentID, "in_progress")
	sourceID := f.task(t, issueID, "running", nil)
	f.task(t, issueID, "deferred", testutil.Cols{"fire_at": testutil.Raw("now() + interval '1 minute'")})

	f.finishTask(t, sourceID, protocol.EventTaskCompleted, false)

	if got := f.reconciliationTasks(t, issueID); got != 0 {
		t.Fatalf("workflow reconciliation tasks = %d, want 0 with deferred work", got)
	}
	if got := f.inboxCount(t); got != 0 {
		t.Fatalf("attention inbox rows = %d, want 0 with deferred work", got)
	}
}

func TestWorkflowReconcileRecognisesCustomInProgressStatus(t *testing.T) {
	f := newWorkspaceIdleFixture(t)
	f.fx.Insert(t, "issue_status", testutil.Cols{
		"workspace_id": f.workspaceID,
		"key":          "actively_building",
		"name":         "Actively Building",
		"description":  "Custom in-progress state",
		"category":     "in_progress",
		"color":        "#f59e0b",
		"position":     1,
	})
	issueID := f.issue(t, "agent", f.agentID, "actively_building")
	taskID := f.task(t, issueID, "running", nil)

	f.finishTask(t, taskID, protocol.EventTaskCompleted, false)

	if got := f.reconciliationTasks(t, issueID); got != 1 {
		t.Fatalf("workflow reconciliation tasks = %d, want 1 for custom in_progress", got)
	}
}

func TestWorkflowReconcileDoesNotRunHumanOwnedIssue(t *testing.T) {
	f := newWorkspaceIdleFixture(t)
	issueID := f.issue(t, "member", f.ownerID, "in_progress")
	taskID := f.task(t, issueID, "running", nil)

	f.finishTask(t, taskID, protocol.EventTaskCompleted, false)

	if got := f.reconciliationTasks(t, issueID); got != 0 {
		t.Fatalf("workflow reconciliation tasks = %d, want 0 for member-owned issue", got)
	}
	if got := f.inboxCount(t); got != 0 {
		t.Fatalf("attention inbox rows = %d, want 0 for member-owned issue", got)
	}
}

func TestWorkflowReconcileNotifiesOnlyAfterAutomaticContinuationStalls(t *testing.T) {
	f := newWorkspaceIdleFixture(t)
	issueID := f.issue(t, "agent", f.agentID, "in_progress")
	sourceID := f.task(t, issueID, "running", nil)
	f.finishTask(t, sourceID, protocol.EventTaskCompleted, false)

	var reconcileID string
	f.fx.QueryRow(t, `
		SELECT id FROM agent_task_queue
		WHERE issue_id = $1 AND trigger_evidence_kind = 'workflow_reconcile'
	`, issueID).Scan(&reconcileID)
	f.fx.Exec(t, `UPDATE agent_task_queue SET status = 'running', started_at = now() WHERE id = $1`, reconcileID)
	f.finishTask(t, reconcileID, protocol.EventTaskCompleted, false)

	if got := f.reconciliationTasks(t, issueID); got != 1 {
		t.Fatalf("workflow reconciliation tasks = %d, want exactly 1", got)
	}
	if got := f.inboxCount(t); got != 1 {
		t.Fatalf("attention inbox rows = %d, want 1 after reconciliation stalls", got)
	}
	var targetIssueID, body string
	f.fx.QueryRow(t, `
		SELECT issue_id, body FROM inbox_item
		WHERE workspace_id = $1 AND recipient_id = $2 AND type = 'workspace_idle'
	`, f.workspaceID, f.ownerID).Scan(&targetIssueID, &body)
	if targetIssueID != issueID || body == "" {
		t.Fatalf("attention target/body = %s/%q, want issue %s with guidance", targetIssueID, body, issueID)
	}

	// Replaying either terminal event cannot create another continuation or
	// another attention row.
	f.bus.Publish(events.Event{Type: protocol.EventTaskCompleted, WorkspaceID: f.workspaceID, TaskID: sourceID})
	f.bus.Publish(events.Event{Type: protocol.EventTaskCompleted, WorkspaceID: f.workspaceID, TaskID: reconcileID})
	if got := f.reconciliationTasks(t, issueID); got != 1 {
		t.Fatalf("workflow reconciliation tasks after replay = %d, want 1", got)
	}
	if got := f.inboxCount(t); got != 1 {
		t.Fatalf("attention rows after replay = %d, want idempotent 1", got)
	}
}

func TestWorkflowReconcileSkipsRetryPendingFailure(t *testing.T) {
	f := newWorkspaceIdleFixture(t)
	issueID := f.issue(t, "agent", f.agentID, "in_progress")
	taskID := f.task(t, issueID, "running", nil)

	f.finishTask(t, taskID, protocol.EventTaskFailed, true)

	if got := f.reconciliationTasks(t, issueID); got != 0 {
		t.Fatalf("workflow reconciliation tasks = %d, want 0 while retry is pending", got)
	}
	if got := f.inboxCount(t); got != 0 {
		t.Fatalf("attention inbox rows = %d, want 0 while retry is pending", got)
	}
}

func TestWorkflowReconcileDoesNotRestartDeliberatelyCancelledTask(t *testing.T) {
	f := newWorkspaceIdleFixture(t)
	issueID := f.issue(t, "agent", f.agentID, "in_progress")
	taskID := f.task(t, issueID, "running", nil)

	f.finishTask(t, taskID, protocol.EventTaskCancelled, false)

	if got := f.reconciliationTasks(t, issueID); got != 0 {
		t.Fatalf("workflow reconciliation tasks = %d, want 0 after deliberate cancellation", got)
	}
	if got := f.inboxCount(t); got != 0 {
		t.Fatalf("attention inbox rows = %d, want 0 after deliberate cancellation", got)
	}
}

func TestWorkflowReconcileDoesNothingForSettledIssue(t *testing.T) {
	f := newWorkspaceIdleFixture(t)
	issueID := f.issue(t, "agent", f.agentID, "in_review")
	taskID := f.task(t, issueID, "running", nil)

	f.finishTask(t, taskID, protocol.EventTaskCompleted, false)

	if got := f.reconciliationTasks(t, issueID); got != 0 {
		t.Fatalf("workflow reconciliation tasks = %d, want 0 for in_review issue", got)
	}
	if got := f.inboxCount(t); got != 0 {
		t.Fatalf("attention inbox rows = %d, want 0 for in_review issue", got)
	}
}
