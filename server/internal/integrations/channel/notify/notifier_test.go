package notify

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/events"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/protocol"
)

const (
	testWorkspace = "11111111-1111-1111-1111-111111111111"
	testRecipient = "22222222-2222-2222-2222-222222222222"
	testIssue     = "33333333-3333-3333-3333-333333333333"
	testInboxItem = "44444444-4444-4444-4444-444444444444"
	testInstall   = "55555555-5555-5555-5555-555555555555"
)

func mustUUID(t *testing.T, s string) pgtype.UUID {
	t.Helper()
	u, err := util.ParseUUID(s)
	if err != nil {
		t.Fatalf("ParseUUID(%q): %v", s, err)
	}
	return u
}

type fakeQueries struct {
	binding    db.ChannelUserBinding
	bindingErr error
	workspace  db.Workspace
	statusKey  string // what Effective should report for a custom status
	created    []db.CreateChannelPushMessageParams
	createErr  error
}

func (f *fakeQueries) FindChannelBindingForMember(context.Context, db.FindChannelBindingForMemberParams) (db.ChannelUserBinding, error) {
	if f.bindingErr != nil {
		return db.ChannelUserBinding{}, f.bindingErr
	}
	return f.binding, nil
}

func (f *fakeQueries) GetWorkspace(context.Context, pgtype.UUID) (db.Workspace, error) {
	return f.workspace, nil
}

func (f *fakeQueries) GetIssueStatusEntryByKey(_ context.Context, arg db.GetIssueStatusEntryByKeyParams) (db.IssueStatus, error) {
	if f.statusKey == "" || arg.Key != f.statusKey {
		return db.IssueStatus{}, pgx.ErrNoRows
	}
	return db.IssueStatus{Key: arg.Key, Category: "in_review"}, nil
}

func (f *fakeQueries) CreateChannelPushMessage(_ context.Context, arg db.CreateChannelPushMessageParams) (db.ChannelPushMessage, error) {
	if f.createErr != nil {
		return db.ChannelPushMessage{}, f.createErr
	}
	f.created = append(f.created, arg)
	return db.ChannelPushMessage{}, nil
}

type fakeAdapter struct {
	result DeliverResult
	err    error
	calls  int
	lastTo db.ChannelUserBinding
	lastTx string
}

func (a *fakeAdapter) DeliverDM(_ context.Context, _ PushRef, binding db.ChannelUserBinding, text string) (DeliverResult, error) {
	a.calls++
	a.lastTo = binding
	a.lastTx = text
	return a.result, a.err
}

func newTestNotifier(t *testing.T, q *fakeQueries, a *fakeAdapter) *Notifier {
	t.Helper()
	q.binding.InstallationID = mustUUID(t, testInstall)
	q.binding.ChannelType = "lark"
	if q.binding.ChannelUserID == "" {
		q.binding.ChannelUserID = "ou_target"
	}
	n := New(q, slog.Default())
	n.Register(map[string]DMDeliverer{"lark": a})
	return n
}

func inReviewEvent() events.Event {
	return events.Event{
		Type:        protocol.EventInboxNew,
		WorkspaceID: testWorkspace,
		Payload: map[string]any{"item": map[string]any{
			"id":             testInboxItem,
			"workspace_id":   testWorkspace,
			"recipient_type": "member",
			"recipient_id":   testRecipient,
			"type":           "status_changed",
			"severity":       "info",
			"issue_id":       testIssue,
			"issue_status":   "in_review",
			"title":          "Ship the thing",
		}},
	}
}

func TestNotifierDeliversAndRecordsAnInReviewPush(t *testing.T) {
	q := &fakeQueries{workspace: db.Workspace{Slug: "acme"}}
	a := &fakeAdapter{result: DeliverResult{State: StateDelivered, MessageID: "om_123"}}
	newTestNotifier(t, q, a).HandleInboxNew(inReviewEvent())

	if a.calls != 1 {
		t.Fatalf("adapter calls = %d, want 1", a.calls)
	}
	if a.lastTo.ChannelUserID != "ou_target" {
		t.Errorf("delivered to %q, want ou_target", a.lastTo.ChannelUserID)
	}
	if a.lastTx == "" {
		t.Error("delivered empty text")
	}
	if len(q.created) != 1 {
		t.Fatalf("ledger rows = %d, want 1", len(q.created))
	}
	got := q.created[0]
	if got.ChannelMessageID != "om_123" {
		t.Errorf("ledger ChannelMessageID = %q, want om_123", got.ChannelMessageID)
	}
	if util.UUIDToString(got.IssueID) != testIssue {
		t.Errorf("ledger IssueID = %q, want %q", util.UUIDToString(got.IssueID), testIssue)
	}
	if util.UUIDToString(got.RecipientUserID) != testRecipient {
		t.Errorf("ledger RecipientUserID = %q, want %q", util.UUIDToString(got.RecipientUserID), testRecipient)
	}
}

// The ledger is an index of replyable pushes. A push nobody can reply to must
// not create a row: it would be a permanent miss that only grows the table.
func TestNotifierDoesNotRecordWhenTheresNothingToReplyTo(t *testing.T) {
	tests := []struct {
		name   string
		result DeliverResult
	}{
		{"handed off to another replica", DeliverResult{State: StateHandedOff}},
		{"delivered without a platform message id", DeliverResult{State: StateDelivered}},
		{"adapter does not support DMs", DeliverResult{State: StateUnsupported}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := &fakeQueries{workspace: db.Workspace{Slug: "acme"}}
			a := &fakeAdapter{result: tt.result}
			newTestNotifier(t, q, a).HandleInboxNew(inReviewEvent())
			if len(q.created) != 0 {
				t.Errorf("ledger rows = %d, want 0", len(q.created))
			}
		})
	}
}

// A pushed-but-unreplyable notification type must reach the user and still
// leave no ledger row, even though the adapter handed back a message id.
func TestNotifierDoesNotRecordUnreplyableTypes(t *testing.T) {
	q := &fakeQueries{workspace: db.Workspace{Slug: "acme"}}
	a := &fakeAdapter{result: DeliverResult{State: StateDelivered, MessageID: "om_qc"}}
	e := inReviewEvent()
	item := e.Payload.(map[string]any)["item"].(map[string]any)
	item["type"] = "quick_create_failed"
	delete(item, "issue_id")
	delete(item, "issue_status")

	newTestNotifier(t, q, a).HandleInboxNew(e)

	if a.calls != 1 {
		t.Errorf("adapter calls = %d, want 1 (quick_create_failed still pushes)", a.calls)
	}
	if len(q.created) != 0 {
		t.Errorf("ledger rows = %d, want 0 (no issue to reply into)", len(q.created))
	}
}

func TestNotifierSkipsWhatTheWhitelistRejects(t *testing.T) {
	tests := []struct {
		name       string
		notifType  string
		issueState string
	}{
		{"a done transition", "status_changed", "done"},
		{"an in_progress transition", "status_changed", "in_progress"},
		{"a new comment", "new_comment", ""},
		{"a mention", "mentioned", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := &fakeQueries{workspace: db.Workspace{Slug: "acme"}}
			a := &fakeAdapter{result: DeliverResult{State: StateDelivered, MessageID: "om_x"}}
			e := inReviewEvent()
			item := e.Payload.(map[string]any)["item"].(map[string]any)
			item["type"] = tt.notifType
			item["issue_status"] = tt.issueState

			newTestNotifier(t, q, a).HandleInboxNew(e)

			if a.calls != 0 {
				t.Errorf("adapter calls = %d, want 0", a.calls)
			}
		})
	}
}

// A workspace's custom status inherits its category's behaviour. Judging the
// literal key would silently exclude every workspace that renamed in_review.
func TestNotifierNormalisesACustomStatusToItsCategory(t *testing.T) {
	q := &fakeQueries{workspace: db.Workspace{Slug: "acme"}, statusKey: "awaiting_signoff"}
	a := &fakeAdapter{result: DeliverResult{State: StateDelivered, MessageID: "om_custom"}}
	e := inReviewEvent()
	e.Payload.(map[string]any)["item"].(map[string]any)["issue_status"] = "awaiting_signoff"

	newTestNotifier(t, q, a).HandleInboxNew(e)

	if a.calls != 1 {
		t.Errorf("adapter calls = %d, want 1 for a custom status in the in_review category", a.calls)
	}
}

func TestNotifierIgnoresAgentRecipients(t *testing.T) {
	q := &fakeQueries{workspace: db.Workspace{Slug: "acme"}}
	a := &fakeAdapter{result: DeliverResult{State: StateDelivered, MessageID: "om_x"}}
	e := inReviewEvent()
	e.Payload.(map[string]any)["item"].(map[string]any)["recipient_type"] = "agent"

	newTestNotifier(t, q, a).HandleInboxNew(e)

	if a.calls != 0 {
		t.Errorf("adapter calls = %d, want 0", a.calls)
	}
}

// An unbound member is not an error. They keep seeing the notification in the
// in-app inbox, which is the degradation WeCom's existing path already set.
func TestNotifierIsANoOpForAnUnboundMember(t *testing.T) {
	q := &fakeQueries{workspace: db.Workspace{Slug: "acme"}, bindingErr: pgx.ErrNoRows}
	a := &fakeAdapter{result: DeliverResult{State: StateDelivered, MessageID: "om_x"}}
	newTestNotifier(t, q, a).HandleInboxNew(inReviewEvent())

	if a.calls != 0 {
		t.Errorf("adapter calls = %d, want 0", a.calls)
	}
	if len(q.created) != 0 {
		t.Errorf("ledger rows = %d, want 0", len(q.created))
	}
}

func TestNotifierSurvivesAnAdapterError(t *testing.T) {
	q := &fakeQueries{workspace: db.Workspace{Slug: "acme"}}
	a := &fakeAdapter{err: errors.New("platform refused the message")}
	newTestNotifier(t, q, a).HandleInboxNew(inReviewEvent())

	if len(q.created) != 0 {
		t.Errorf("ledger rows = %d, want 0 after a failed send", len(q.created))
	}
}

// The binding names the platform; a workspace bound to a channel we have no
// adapter for must not panic on the map miss.
func TestNotifierIgnoresAChannelWithNoAdapter(t *testing.T) {
	q := &fakeQueries{workspace: db.Workspace{Slug: "acme"}}
	a := &fakeAdapter{result: DeliverResult{State: StateDelivered, MessageID: "om_x"}}
	n := newTestNotifier(t, q, a)
	q.binding.ChannelType = "dingtalk"

	n.HandleInboxNew(inReviewEvent())

	if a.calls != 0 {
		t.Errorf("adapter calls = %d, want 0", a.calls)
	}
}

func TestNotifierIgnoresMalformedPayloads(t *testing.T) {
	tests := []struct {
		name    string
		payload any
	}{
		{"not a map", "nonsense"},
		{"no item key", map[string]any{}},
		{"item is not a map", map[string]any{"item": 42}},
		{"no recipient", map[string]any{"item": map[string]any{
			"workspace_id": testWorkspace, "recipient_type": "member", "type": "task_failed",
		}}},
		{"no workspace", map[string]any{"item": map[string]any{
			"recipient_id": testRecipient, "recipient_type": "member", "type": "task_failed",
		}}},
		{"unparseable recipient", map[string]any{"item": map[string]any{
			"workspace_id": testWorkspace, "recipient_id": "not-a-uuid",
			"recipient_type": "member", "type": "task_failed",
		}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := &fakeQueries{workspace: db.Workspace{Slug: "acme"}}
			a := &fakeAdapter{result: DeliverResult{State: StateDelivered, MessageID: "om_x"}}
			// Must not panic.
			newTestNotifier(t, q, a).HandleInboxNew(events.Event{
				Type: protocol.EventInboxNew, WorkspaceID: testWorkspace, Payload: tt.payload,
			})
			if a.calls != 0 {
				t.Errorf("adapter calls = %d, want 0", a.calls)
			}
		})
	}
}
