-- name: MarkWorkspaceWorkflowBusy :exec
-- The first active task after an idle notification starts a new generation.
-- Further task events in the same busy period leave that generation unchanged.
INSERT INTO workspace_idle_state (
    workspace_id,
    busy_generation,
    idle_notified_generation,
    updated_at
) VALUES ($1, 1, 0, now())
ON CONFLICT (workspace_id) DO UPDATE
SET busy_generation = CASE
        WHEN workspace_idle_state.idle_notified_generation = workspace_idle_state.busy_generation
        THEN workspace_idle_state.busy_generation + 1
        ELSE workspace_idle_state.busy_generation
    END,
    updated_at = now();

-- name: LockWorkspaceForIdleNotification :one
-- Task inserts take FOR KEY SHARE on this row through lock_task_owner_rows.
-- Holding FOR UPDATE until inbox rows commit makes the final active-task check
-- and the busy -> idle edge atomic with respect to the next task creation.
SELECT id FROM workspace
WHERE id = $1
FOR UPDATE;

-- name: HasActiveTasksInWorkspace :one
SELECT EXISTS (
    SELECT 1
    FROM agent_task_queue task
    JOIN agent a ON a.id = task.agent_id
    WHERE a.workspace_id = $1
      AND task.status IN ('queued', 'dispatched', 'running', 'waiting_local_directory')
) AS has_active;

-- name: ClaimWorkspaceIdleNotification :one
UPDATE workspace_idle_state
SET idle_notified_generation = busy_generation,
    updated_at = now()
WHERE workspace_id = $1
  AND idle_notified_generation < busy_generation
RETURNING busy_generation;

-- name: ListIdleWorkspaceWorkflowReconcileSourceTaskIDs :many
-- When the workspace crosses from busy to idle, reconcile every agent-owned
-- in-progress issue that has no durable follow-up work. The latest terminal
-- task is the attribution and idempotency source used by the issue-level
-- reconciliation path.
SELECT latest.id
FROM issue i
JOIN LATERAL (
    SELECT task.id
    FROM agent_task_queue task
    WHERE task.issue_id = i.id
      AND task.status IN ('completed', 'failed', 'cancelled')
    ORDER BY task.completed_at DESC NULLS LAST, task.created_at DESC, task.id DESC
    LIMIT 1
) latest ON true
WHERE i.workspace_id = $1
  AND issue_effective_status(i.workspace_id, i.status) = 'in_progress'
  AND i.assignee_type IN ('agent', 'squad')
  AND i.assignee_id IS NOT NULL
  AND NOT EXISTS (
      SELECT 1
      FROM agent_task_queue active
      WHERE active.issue_id = i.id
        AND active.status IN ('queued', 'dispatched', 'running', 'waiting_local_directory', 'deferred')
  )
ORDER BY i.created_at, i.id;

-- name: EnsureWorkspaceWorkflowBusy :exec
-- A terminal event can be the first lifecycle signal observed after a rolling
-- deployment or process restart. Seed only a missing row; unlike Mark*, this
-- never advances a generation for a duplicate terminal event.
INSERT INTO workspace_idle_state (
    workspace_id,
    busy_generation,
    idle_notified_generation,
    updated_at
) VALUES ($1, 1, 0, now())
ON CONFLICT (workspace_id) DO NOTHING;

-- name: CountWorkflowInProgressIssues :one
SELECT count(*)
FROM issue
WHERE workspace_id = $1
  AND issue_effective_status(workspace_id, status) = 'in_progress';

-- name: ListWorkflowIdleRecipientUserIDs :many
WITH in_progress_issues AS MATERIALIZED (
    SELECT id, creator_type, creator_id
    FROM issue
    WHERE workspace_id = $1
      AND issue_effective_status(workspace_id, status) = 'in_progress'
), recipient_candidates AS (
    SELECT creator_id AS user_id
    FROM in_progress_issues
    WHERE creator_type = 'member'

    UNION ALL

    SELECT subscriber.user_id
    FROM in_progress_issues issue
    JOIN issue_subscriber subscriber ON subscriber.issue_id = issue.id
    WHERE subscriber.user_type = 'member'
      AND subscriber.reason = 'delegated'
      AND subscriber.unsubscribed_at IS NULL
)
SELECT DISTINCT candidate.user_id
FROM recipient_candidates candidate
JOIN member m ON m.workspace_id = $1 AND m.user_id = candidate.user_id
ORDER BY candidate.user_id;
