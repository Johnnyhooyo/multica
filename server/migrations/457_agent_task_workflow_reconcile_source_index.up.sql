CREATE UNIQUE INDEX CONCURRENTLY idx_agent_task_workflow_reconcile_source_uidx
ON agent_task_queue (issue_id, trigger_evidence_ref_id)
WHERE trigger_evidence_kind = 'workflow_reconcile';
