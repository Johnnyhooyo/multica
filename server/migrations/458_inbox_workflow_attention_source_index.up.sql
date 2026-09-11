CREATE UNIQUE INDEX CONCURRENTLY idx_inbox_workflow_attention_source_uidx
ON inbox_item (recipient_id, issue_id, (details->>'source_task_id'))
WHERE type = 'workspace_idle' AND issue_id IS NOT NULL AND details ? 'source_task_id';
