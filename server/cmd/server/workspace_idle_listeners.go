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

// registerWorkspaceIdleListeners turns completed, failed, and server-refused
// issue-task events into a convergence pass. The historical workspace-wide
// idle notification is gone: an empty workspace is not actionable. A human is
// interrupted only when the issue has no durable work and one automatic
// reconciliation pass could not be started or could not move the workflow
// forward.
func registerWorkspaceIdleListeners(bus *events.Bus, pool *pgxpool.Pool, taskSvc *service.TaskService) {
	queries := db.New(pool)
	for _, eventType := range workspaceTerminalTaskEvents {
		bus.Subscribe(eventType, func(e events.Event) {
			if e.WorkspaceID == "" || taskRetryPending(e) {
				return
			}
			taskID := taskEventTaskID(e)
			if !taskID.Valid {
				return
			}
			result, err := taskSvc.ReconcileStalledWorkflow(context.Background(), taskID)
			if err != nil {
				slog.Error("workflow reconcile failed",
					"workspace_id", e.WorkspaceID,
					"task_id", util.UUIDToString(taskID),
					"error", err,
				)
				return
			}
			if result.Outcome != service.WorkflowReconcileAttention {
				return
			}
			notifyWorkflowAttention(context.Background(), queries, bus, e.WorkspaceID, result)
		})
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

func taskEventTaskID(e events.Event) pgtype.UUID {
	if e.TaskID != "" {
		if id, err := util.ParseUUID(e.TaskID); err == nil {
			return id
		}
	}
	payload, ok := e.Payload.(map[string]any)
	if !ok {
		return pgtype.UUID{}
	}
	taskID, _ := payload["task_id"].(string)
	id, _ := util.ParseUUID(taskID)
	return id
}
