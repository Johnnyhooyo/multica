package lark

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/integrations/channel/engine"
	"github.com/multica-ai/multica/server/internal/integrations/channel/notify"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

type fakeCardActionStore struct {
	installation Installation
	binding      UserBinding
	inboxItem    db.InboxItem
	workspace    db.Workspace
}

func (s *fakeCardActionStore) GetLarkInstallationByAppID(context.Context, string) (Installation, error) {
	return s.installation, nil
}

func (s *fakeCardActionStore) GetLarkUserBindingByOpenID(context.Context, GetUserBindingByOpenIDParams) (UserBinding, error) {
	return s.binding, nil
}

func (s *fakeCardActionStore) GetInboxItemInWorkspace(context.Context, db.GetInboxItemInWorkspaceParams) (db.InboxItem, error) {
	return s.inboxItem, nil
}

func (s *fakeCardActionStore) GetWorkspace(context.Context, pgtype.UUID) (db.Workspace, error) {
	return s.workspace, nil
}

type fakeReviewApprover struct {
	push       db.ChannelPushMessage
	lookupOK   bool
	result     engine.PushReplyResult
	approvedBy pgtype.UUID
	calls      int
}

func (a *fakeReviewApprover) LookupPush(context.Context, pgtype.UUID, string) (db.ChannelPushMessage, bool, error) {
	return a.push, a.lookupOK, nil
}

func (a *fakeReviewApprover) ApprovePushReview(_ context.Context, _ db.ChannelPushMessage, sender pgtype.UUID) (engine.PushReplyResult, error) {
	a.calls++
	a.approvedBy = sender
	return a.result, nil
}

func TestDecodeCardAction(t *testing.T) {
	payload := []byte(`{
		"schema":"2.0",
		"header":{"event_id":"evt_1","event_type":"card.action.trigger","app_id":"cli_app"},
		"event":{
			"operator":{"open_id":"ou_member"},
			"action":{"tag":"button","value":{"action":"approve_review"}},
			"context":{"open_message_id":"om_push","open_chat_id":"oc_dm"}
		}
	}`)
	action, ok, err := NewLarkJSONFrameDecoder().DecodeCardAction(payload, Installation{})
	if err != nil || !ok {
		t.Fatalf("DecodeCardAction: ok=%v err=%v", ok, err)
	}
	if action.EventID != "evt_1" || action.AppID != "cli_app" || action.OperatorOpenID != "ou_member" ||
		action.OpenMessageID != "om_push" || action.OpenChatID != "oc_dm" || action.Action != reviewApprovalAction {
		t.Fatalf("action = %+v", action)
	}
}

func TestReviewCardActionApprovesAndOnlyRemovesApprovalButton(t *testing.T) {
	t.Setenv("MULTICA_APP_URL", "https://app.example.com")
	installationID := uuidFromString(t, "11111111-1111-4111-8111-111111111111")
	workspaceID := uuidFromString(t, "22222222-2222-4222-8222-222222222222")
	userID := uuidFromString(t, "33333333-3333-4333-8333-333333333333")
	inboxItemID := uuidFromString(t, "44444444-4444-4444-8444-444444444444")
	issueID := uuidFromString(t, "55555555-5555-4555-8555-555555555555")
	store := &fakeCardActionStore{
		installation: Installation{ID: installationID, WorkspaceID: workspaceID, AppID: "cli_app", Status: string(InstallationActive)},
		binding:      UserBinding{InstallationID: installationID, WorkspaceID: workspaceID, MulticaUserID: userID, ChannelUserID: "ou_member"},
		inboxItem: db.InboxItem{
			ID: inboxItemID, WorkspaceID: workspaceID, RecipientType: "member", RecipientID: userID,
			Type: "status_changed", IssueID: issueID, Title: "Ship *this*",
			Body:    pgtype.Text{String: "正文 **重点**\n- 核对结果", Valid: true},
			Details: []byte(`{"comment_id":"66666666-6666-4666-8666-666666666666"}`),
		},
		workspace: db.Workspace{ID: workspaceID, Slug: "acme"},
	}
	approver := &fakeReviewApprover{
		lookupOK: true,
		push: db.ChannelPushMessage{
			InstallationID:  installationID,
			WorkspaceID:     workspaceID,
			RecipientUserID: userID,
			IssueID:         issueID,
			InboxItemID:     inboxItemID,
			ChannelType:     channelTypeFeishu,
		},
		result: engine.PushReplyResult{
			Posted:          true,
			Message:         "审核已通过，任务已流转为 done。",
			IssueTitle:      "Ship it",
			ReviewFinalized: true,
			ApprovalApplied: true,
		},
	}
	h := NewReviewCardActionHandler(store, approver)
	response, err := h.HandleCardAction(context.Background(), Installation{AppID: "cli_app"}, CardAction{
		AppID: "cli_app", OperatorOpenID: "ou_member", OpenMessageID: "om_push", Action: reviewApprovalAction,
	})
	if err != nil {
		t.Fatalf("HandleCardAction: %v", err)
	}
	if approver.calls != 1 || approver.approvedBy != userID {
		t.Fatalf("approver calls=%d sender=%v", approver.calls, approver.approvedBy)
	}
	if response.Toast == nil || response.Toast.Type != "success" || response.Card == nil {
		t.Fatalf("response = %+v", response)
	}
	wantRef := notify.RebuildReviewPushRef(store.inboxItem, store.workspace.Slug)
	wantCard := cardWithoutApprovalButton(t, issuePushCardData(wantRef.Card, wantRef.WebURL, wantRef.DesktopURL))
	if !reflect.DeepEqual(response.Card.Data, wantCard) {
		t.Fatalf("updated card differs beyond the approval button:\n got: %#v\nwant: %#v", response.Card.Data, wantCard)
	}
	raw, err := json.Marshal(response.Card.Data)
	if err != nil {
		t.Fatalf("marshal card: %v", err)
	}
	card := string(raw)
	for _, want := range []string{"[待你审核] Ship *this*", "正文 **重点**", "在 Multica 中查看", "https://app.example.com/acme/issues/"} {
		if !strings.Contains(card, want) {
			t.Errorf("updated card %s does not contain %q", card, want)
		}
	}
	if strings.Contains(card, "approve_review") {
		t.Errorf("updated card retained an approval button: %s", card)
	}
}

func cardWithoutApprovalButton(t *testing.T, card map[string]any) map[string]any {
	t.Helper()
	raw, err := json.Marshal(card)
	if err != nil {
		t.Fatalf("marshal original card: %v", err)
	}
	var clone map[string]any
	if err := json.Unmarshal(raw, &clone); err != nil {
		t.Fatalf("clone original card: %v", err)
	}
	body := clone["body"].(map[string]any)
	elements := body["elements"].([]any)
	kept := make([]any, 0, len(elements)-1)
	for _, rawElement := range elements {
		element := rawElement.(map[string]any)
		if element["element_id"] == "approve_review" {
			continue
		}
		kept = append(kept, element)
	}
	body["elements"] = kept
	return clone
}

func TestReviewCardActionKeepsCardWhenReviewIsStale(t *testing.T) {
	t.Setenv("MULTICA_APP_URL", "https://app.example.com")
	installationID := uuidFromString(t, "11111111-1111-4111-8111-111111111111")
	workspaceID := uuidFromString(t, "22222222-2222-4222-8222-222222222222")
	userID := uuidFromString(t, "33333333-3333-4333-8333-333333333333")
	inboxItemID := uuidFromString(t, "44444444-4444-4444-8444-444444444444")
	issueID := uuidFromString(t, "55555555-5555-4555-8555-555555555555")
	store := &fakeCardActionStore{
		installation: Installation{ID: installationID, WorkspaceID: workspaceID, AppID: "cli_app", Status: string(InstallationActive)},
		binding:      UserBinding{MulticaUserID: userID},
		inboxItem: db.InboxItem{
			ID: inboxItemID, WorkspaceID: workspaceID, RecipientType: "member", RecipientID: userID,
			Type: "status_changed", IssueID: issueID, Title: "Stale review",
		},
		workspace: db.Workspace{ID: workspaceID, Slug: "acme"},
	}
	approver := &fakeReviewApprover{
		lookupOK: true,
		push: db.ChannelPushMessage{
			WorkspaceID: workspaceID, RecipientUserID: userID, IssueID: issueID,
			InboxItemID: inboxItemID, ChannelType: channelTypeFeishu,
		},
		result: engine.PushReplyResult{Message: "任务已不在待审核状态，请打开 Multica 查看。"},
	}
	response, err := NewReviewCardActionHandler(store, approver).HandleCardAction(
		context.Background(), Installation{AppID: "cli_app"}, CardAction{
			AppID: "cli_app", OperatorOpenID: "ou_member", OpenMessageID: "om_push", Action: reviewApprovalAction,
		})
	if err != nil {
		t.Fatalf("HandleCardAction: %v", err)
	}
	if response.Toast == nil || response.Toast.Type != "error" || response.Card != nil {
		t.Fatalf("response = %+v, want error toast without card replacement", response)
	}
}
