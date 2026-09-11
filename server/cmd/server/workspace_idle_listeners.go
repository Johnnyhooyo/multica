package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/multica-ai/multica/server/internal/events"
	"github.com/multica-ai/multica/server/internal/service"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/dbid"
	"github.com/multica-ai/multica/server/pkg/protocol"
)

// Keep the existing wire type so installed clients continue to render these
// rows. Its semantics are now narrower: it is issue-scoped and means automatic
// workflow reconciliation was exhausted, never merely that a workspace is idle.
const workspaceIdleNotificationType = "workspace_idle"

var workspaceTerminalTaskEvents = []string{
	protocol.EventTaskCompleted,
	protocol.EventTaskFailed,
	protocol.EventTaskCancelled,
}

// registerWorkspaceIdleListeners starts a convergence pass when a workspace
// crosses from busy to idle. Every agent-owned in-progress issue without
// durable follow-up work gets one automatic continuation. A human is
// interrupted only when that continuation could not be started or could not
// move the issue forward.
func registerWorkspaceIdleListeners(bus *events.Bus, pool *pgxpool.Pool, taskSvc *service.TaskService) {
	for _, eventType := range workspaceTerminalTaskEvents {
		bus.Subscribe(eventType, func(e events.Event) {
			if e.WorkspaceID == "" || taskRetryPending(e) {
				return
			}
			reconcileIdleWorkspace(context.Background(), bus, pool, taskSvc, e.WorkspaceID)
		})
	}
}

func reconcileIdleWorkspace(
	ctx context.Context,
	bus *events.Bus,
	pool *pgxpool.Pool,
	taskSvc *service.TaskService,
	workspaceID string,
) {
	workspaceUUID := parseUUID(workspaceID)
	queries := db.New(pool)
	hasActive, err := queries.HasActiveTasksInWorkspace(ctx, workspaceUUID)
	if err != nil {
		slog.Error("workspace reconciliation: active task check failed", "workspace_id", workspaceID, "error", err)
		return
	}
	if hasActive {
		return
	}

	sourceTaskIDs, err := queries.ListIdleWorkspaceWorkflowReconcileSourceTaskIDs(ctx, workspaceUUID)
	if err != nil {
		slog.Error("workspace reconciliation: list stalled issues failed", "workspace_id", workspaceID, "error", err)
		return
	}
	for _, sourceTaskID := range sourceTaskIDs {
		result, reconcileErr := taskSvc.ReconcileStalledWorkflow(ctx, sourceTaskID)
		if reconcileErr != nil {
			slog.Error("workflow reconcile failed",
				"workspace_id", workspaceID,
				"task_id", util.UUIDToString(sourceTaskID),
				"error", reconcileErr,
			)
			continue
		}
		if result.Outcome == service.WorkflowReconcileAttention {
			notifyWorkflowAttention(ctx, queries, bus, workspaceID, result)
		}
	}
}

func notifyWorkflowAttention(ctx context.Context, queries *db.Queries, bus *events.Bus, workspaceID string, result service.WorkflowReconcileResult) {
	issueID := util.UUIDToString(result.Issue.ID)
	if issueID == "" {
		return
	}
	details, _ := json.Marshal(map[string]string{
		"source_task_id": util.UUIDToString(result.SourceTask.ID),
		"reason":         result.AttentionNote,
	})
	recipientID := result.SourceTask.AccountableUserID
	if !recipientID.Valid {
		recipientID = result.SourceTask.OriginatorUserID
	}
	if !recipientID.Valid && result.Issue.CreatorType == "member" {
		recipientID = result.Issue.CreatorID
	}
	if !recipientID.Valid {
		return
	}
	recipient := util.UUIDToString(recipientID)
	prefs := loadUserPrefs(ctx, queries, workspaceID, []string{recipient})
	if p, ok := prefs[recipient]; ok && isNotifMuted(p, workspaceIdleNotificationType) {
		return
	}
	item, err := queries.CreateWorkflowAttentionInboxItem(ctx, db.CreateWorkflowAttentionInboxItemParams{
		ID:          dbid.NewV7(),
		WorkspaceID: parseUUID(workspaceID),
		RecipientID: recipientID,
		IssueID:     result.Issue.ID,
		Title:       result.Issue.Title,
		Body:        pgtype.Text{String: result.AttentionNote, Valid: result.AttentionNote != ""},
		Details:     details,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return
	}
	if err != nil {
		slog.Error("workflow attention notification creation failed", "issue_id", issueID, "recipient_id", recipient, "error", err)
		return
	}
	resp := inboxItemToResponse(item)
	resp["issue_status"] = result.Issue.Status
	bus.Publish(events.Event{
		Type:        protocol.EventInboxNew,
		WorkspaceID: workspaceID,
		ActorType:   "system",
		Payload:     map[string]any{"item": resp},
	})
}

func taskRetryPending(e events.Event) bool {
	if e.Type != protocol.EventTaskFailed {
		return false
	}
	payload, ok := e.Payload.(map[string]any)
	if !ok {
		return false
	}
	retryPending, _ := payload["retry_pending"].(bool)
	return retryPending
}
