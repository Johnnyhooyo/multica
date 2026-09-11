package service

import (
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

func TestIsWorkflowReconcileTrigger(t *testing.T) {
	tests := []struct {
		name string
		task db.AgentTaskQueue
		want bool
	}{
		{name: "completed", task: db.AgentTaskQueue{Status: "completed"}, want: true},
		{name: "failed", task: db.AgentTaskQueue{Status: "failed"}, want: true},
		{name: "server refused", task: db.AgentTaskQueue{
			Status:        "cancelled",
			FailureReason: pgtype.Text{String: "runtime_incompatible", Valid: true},
		}, want: true},
		{name: "deliberate cancellation", task: db.AgentTaskQueue{Status: "cancelled"}, want: false},
		{name: "still running", task: db.AgentTaskQueue{Status: "running"}, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isWorkflowReconcileTrigger(tt.task); got != tt.want {
				t.Fatalf("isWorkflowReconcileTrigger() = %v, want %v", got, tt.want)
			}
		})
	}
}
