package lark

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/multica-ai/multica/server/internal/integrations/channel/engine"
	"github.com/multica-ai/multica/server/internal/util"
)

const reviewApprovalAction = "approve_review"

// CardAction is the small, verified slice of card.action.trigger consumed by
// the review card flow. The message id and operator identity come from Lark's
// signed callback envelope; the action value only selects the operation.
type CardAction struct {
	EventID        string
	AppID          string
	OperatorOpenID string
	OpenMessageID  string
	OpenChatID     string
	Action         string
}

// CardActionHandler processes one synchronous Lark card callback and returns
// the payload Lark should apply to the card. Expected product denials are
// encoded as Toast-only responses; errors are reserved for infrastructure
// faults so the connector can NACK and let Lark retry.
type CardActionHandler interface {
	HandleCardAction(ctx context.Context, inst Installation, action CardAction) (CardActionResponse, error)
}

type CardActionResponse struct {
	Toast *CardActionToast `json:"toast,omitempty"`
	Card  *CardActionCard  `json:"card,omitempty"`
}

type CardActionToast struct {
	Type    string `json:"type"`
	Content string `json:"content"`
}

type CardActionCard struct {
	Type string         `json:"type"`
	Data map[string]any `json:"data"`
}

type cardActionStore interface {
	GetLarkInstallationByAppID(ctx context.Context, appID string) (Installation, error)
	GetLarkUserBindingByOpenID(ctx context.Context, arg GetUserBindingByOpenIDParams) (UserBinding, error)
}

type reviewCardActionHandler struct {
	store    cardActionStore
	approver engine.PushReviewApprover
}

func NewReviewCardActionHandler(store cardActionStore, approver engine.PushReviewApprover) CardActionHandler {
	return &reviewCardActionHandler{store: store, approver: approver}
}

func (h *reviewCardActionHandler) HandleCardAction(ctx context.Context, inst Installation, action CardAction) (CardActionResponse, error) {
	if action.Action != reviewApprovalAction || action.OpenMessageID == "" || action.OperatorOpenID == "" {
		return cardActionError("无法识别这个卡片操作，请打开 Multica 处理。"), nil
	}
	if action.AppID == "" || (inst.AppID != "" && action.AppID != inst.AppID) {
		return cardActionError("该卡片不属于当前应用。"), nil
	}
	installation, err := h.store.GetLarkInstallationByAppID(ctx, action.AppID)
	if errors.Is(err, pgx.ErrNoRows) {
		return cardActionError("该飞书连接已失效。"), nil
	}
	if err != nil {
		return CardActionResponse{}, fmt.Errorf("resolve card action installation: %w", err)
	}
	if InstallationStatus(installation.Status) != InstallationActive {
		return cardActionError("该飞书连接已失效。"), nil
	}

	push, ok, err := h.approver.LookupPush(ctx, installation.ID, action.OpenMessageID)
	if err != nil {
		return CardActionResponse{}, fmt.Errorf("lookup review card push: %w", err)
	}
	if !ok || push.ChannelType != channelTypeFeishu {
		return cardActionError("这张卡片已无法审核，请打开 Multica 处理。"), nil
	}
	if push.WorkspaceID != installation.WorkspaceID {
		return CardActionResponse{}, fmt.Errorf(
			"review card push workspace %s does not match installation workspace %s",
			util.UUIDToString(push.WorkspaceID), util.UUIDToString(installation.WorkspaceID))
	}

	binding, err := h.store.GetLarkUserBindingByOpenID(ctx, GetUserBindingByOpenIDParams{
		InstallationID: installation.ID,
		ChannelUserID:  action.OperatorOpenID,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return cardActionError("你没有权限执行该操作。"), nil
	}
	if err != nil {
		return CardActionResponse{}, fmt.Errorf("resolve card action operator: %w", err)
	}

	result, err := h.approver.ApprovePushReview(ctx, push, binding.MulticaUserID)
	if err != nil {
		return CardActionResponse{}, fmt.Errorf("approve review card: %w", err)
	}
	if !result.ReviewFinalized {
		return cardActionError(result.Message), nil
	}

	statusLabel := "审核已通过"
	if !result.ApprovalApplied {
		statusLabel = "任务已完成"
	}
	return CardActionResponse{
		Toast: &CardActionToast{Type: "success", Content: result.Message},
		Card: &CardActionCard{
			Type: "raw",
			Data: completedReviewCard(statusLabel, result.IssueTitle, result.Message),
		},
	}, nil
}

func cardActionError(message string) CardActionResponse {
	if message == "" {
		message = "审核操作未完成，请打开 Multica 查看。"
	}
	return CardActionResponse{Toast: &CardActionToast{Type: "error", Content: message}}
}

// completedReviewCard intentionally contains no callback button. Returning it
// synchronously is what removes the approval affordance from the message the
// member clicked, while keeping an explicit audit state visible in the chat.
func completedReviewCard(statusLabel, issueTitle, message string) map[string]any {
	title := "[" + statusLabel + "]"
	if issueTitle != "" {
		title += " " + issueTitle
	}
	return map[string]any{
		"schema": "2.0",
		"config": map[string]any{
			"summary":      map[string]any{"content": title},
			"update_multi": true,
		},
		"header": map[string]any{
			"template": "green",
			"title": map[string]any{
				"tag":     "plain_text",
				"content": title,
			},
		},
		"body": map[string]any{
			"elements": []any{
				map[string]any{
					"tag":     "markdown",
					"content": escapeCardMarkdown(message),
				},
			},
		},
	}
}

// DecodeCardAction parses the callback envelope carried by a long-connection
// frame. It returns ok=false for every other event type so the normal message
// decoder remains the authority for im.message.receive_v1.
func (d *LarkJSONFrameDecoder) DecodeCardAction(payload []byte, _ Installation) (CardAction, bool, error) {
	if len(payload) == 0 {
		return CardAction{}, false, nil
	}
	var env larkEventEnvelope
	if err := json.Unmarshal(payload, &env); err != nil {
		return CardAction{}, false, fmt.Errorf("card action envelope: %w", err)
	}
	if env.Header.EventType != "card.action.trigger" {
		return CardAction{}, false, nil
	}
	if env.Event == nil {
		return CardAction{}, false, errors.New("card.action.trigger with empty event payload")
	}
	var event struct {
		Operator struct {
			OpenID string `json:"open_id"`
		} `json:"operator"`
		Action struct {
			Value struct {
				Action string `json:"action"`
			} `json:"value"`
		} `json:"action"`
		Context struct {
			OpenMessageID string `json:"open_message_id"`
			OpenChatID    string `json:"open_chat_id"`
		} `json:"context"`
	}
	if err := json.Unmarshal(env.Event, &event); err != nil {
		return CardAction{}, false, fmt.Errorf("card action event: %w", err)
	}
	return CardAction{
		EventID:        env.Header.EventID,
		AppID:          env.Header.AppID,
		OperatorOpenID: event.Operator.OpenID,
		OpenMessageID:  event.Context.OpenMessageID,
		OpenChatID:     event.Context.OpenChatID,
		Action:         event.Action.Value.Action,
	}, true, nil
}
