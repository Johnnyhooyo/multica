package lark

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/integrations/channel/engine"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

type fakeCardActionStore struct {
	installation Installation
	binding      UserBinding
}

func (s *fakeCardActionStore) GetLarkInstallationByAppID(context.Context, string) (Installation, error) {
	return s.installation, nil
}

func (s *fakeCardActionStore) GetLarkUserBindingByOpenID(context.Context, GetUserBindingByOpenIDParams) (UserBinding, error) {
	return s.binding, nil
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

func TestReviewCardActionApprovesAndReturnsButtonFreeCard(t *testing.T) {
	installationID := uuidFromString(t, "11111111-1111-4111-8111-111111111111")
	workspaceID := uuidFromString(t, "22222222-2222-4222-8222-222222222222")
	userID := uuidFromString(t, "33333333-3333-4333-8333-333333333333")
	store := &fakeCardActionStore{
		installation: Installation{ID: installationID, WorkspaceID: workspaceID, AppID: "cli_app", Status: string(InstallationActive)},
		binding:      UserBinding{InstallationID: installationID, WorkspaceID: workspaceID, MulticaUserID: userID, ChannelUserID: "ou_member"},
	}
	approver := &fakeReviewApprover{
		lookupOK: true,
		push: db.ChannelPushMessage{
			InstallationID: installationID,
			WorkspaceID:    workspaceID,
			ChannelType:    channelTypeFeishu,
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
	raw, err := json.Marshal(response.Card.Data)
	if err != nil {
		t.Fatalf("marshal card: %v", err)
	}
	card := string(raw)
	if !strings.Contains(card, "[审核已通过] Ship it") {
		t.Errorf("updated card = %s", card)
	}
	if strings.Contains(card, "approve_review") || strings.Contains(card, `"tag":"button"`) {
		t.Errorf("updated card retained an approval button: %s", card)
	}
}

func TestReviewCardActionKeepsCardWhenReviewIsStale(t *testing.T) {
	installationID := uuidFromString(t, "11111111-1111-4111-8111-111111111111")
	workspaceID := uuidFromString(t, "22222222-2222-4222-8222-222222222222")
	store := &fakeCardActionStore{
		installation: Installation{ID: installationID, WorkspaceID: workspaceID, AppID: "cli_app", Status: string(InstallationActive)},
		binding:      UserBinding{MulticaUserID: uuidFromString(t, "33333333-3333-4333-8333-333333333333")},
	}
	approver := &fakeReviewApprover{
		lookupOK: true,
		push:     db.ChannelPushMessage{WorkspaceID: workspaceID, ChannelType: channelTypeFeishu},
		result:   engine.PushReplyResult{Message: "任务已不在待审核状态，请打开 Multica 查看。"},
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
