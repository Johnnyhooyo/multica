package lark

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strings"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/integrations/channel/notify"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// CredentialsFunc resolves an installation's API credentials.
type CredentialsFunc func(installationID pgtype.UUID) (InstallationCredentials, error)

type dmDeliverer struct {
	client APIClient
	creds  CredentialsFunc
	logger *slog.Logger
}

var _ notify.DMDeliverer = (*dmDeliverer)(nil)
var _ notify.TopicReplyDeliverer = (*dmDeliverer)(nil)

// NewDMDeliverer adapts the Lark client to the shared push layer.
//
// Lark is the reference implementation of a replyable push: it hands back a
// per-message id on send and reports a per-message ReplyCtx on inbound, so a
// reply to a push can be traced to the issue it was about. WeCom does
// neither.
func NewDMDeliverer(client APIClient, creds CredentialsFunc, logger *slog.Logger) notify.DMDeliverer {
	if logger == nil {
		logger = slog.Default()
	}
	return &dmDeliverer{client: client, creds: creds, logger: logger}
}

func (d *dmDeliverer) DeliverDM(ctx context.Context, ref notify.PushRef, binding db.ChannelUserBinding, text string) (notify.DeliverResult, error) {
	if text == "" || binding.ChannelUserID == "" {
		return notify.DeliverResult{}, nil
	}
	creds, err := d.creds(binding.InstallationID)
	if err != nil {
		return notify.DeliverResult{}, err
	}
	params := SendDirectParams{
		InstallationID: creds,
		OpenID:         OpenID(binding.ChannelUserID),
	}
	plainText := notify.PlainHead(text)
	if ref.WebURL != "" && ref.DesktopURL != "" && ref.Card.Title != "" {
		cardJSON, cardErr := issuePushCard(ref.Card, ref.WebURL, ref.DesktopURL)
		if cardErr != nil {
			return notify.DeliverResult{}, cardErr
		}
		params.CardJSON = cardJSON
	} else {
		params.Text = plainText
	}
	messageID, err := d.client.SendDirectMessage(ctx, params)
	if err != nil {
		return notify.DeliverResult{}, err
	}
	if messageID == "" {
		// Without an id the push is unaddressable, so a reply to it would
		// fall through to the ordinary chat path and confuse the user.
		// Surfacing this as a failure keeps the metric honest.
		return notify.DeliverResult{}, errors.New("lark: send returned no message_id")
	}
	if ref.StartTopic {
		if _, topicErr := d.client.SendTextMessage(ctx, SendTextParams{
			InstallationID: creds,
			Text:           "请在本话题中回复处理意见。",
			ReplyTarget:    ReplyTarget{MessageID: messageID, InThread: true},
		}); topicErr != nil {
			// The root push is already visible and its message id is required for
			// inbound attribution. Treat topic creation as a graceful degradation
			// rather than returning an error that would leave the root untracked.
			d.logger.Warn("lark: create push topic", "message_id", messageID, "err", topicErr)
		}
	}
	return notify.DeliverResult{State: notify.StateDelivered, MessageID: messageID}, nil
}

func (d *dmDeliverer) DeliverTopicReply(ctx context.Context, installationID pgtype.UUID, rootMessageID, text string) (notify.DeliverResult, error) {
	if rootMessageID == "" || strings.TrimSpace(text) == "" {
		return notify.DeliverResult{}, nil
	}
	creds, err := d.creds(installationID)
	if err != nil {
		return notify.DeliverResult{}, err
	}
	messageID, err := d.client.SendTextMessage(ctx, SendTextParams{
		InstallationID: creds,
		Text:           text,
		ReplyTarget:    ReplyTarget{MessageID: rootMessageID, InThread: true},
	})
	if err != nil {
		return notify.DeliverResult{}, err
	}
	return notify.DeliverResult{State: notify.StateDelivered, MessageID: messageID}, nil
}

func issuePushCard(content notify.PushCardContent, webURL, desktopURL string) (string, error) {
	defaultURL := webURL
	if defaultURL == "" {
		defaultURL = desktopURL
	}
	pcURL := desktopBridgeURL(webURL, desktopURL)
	if pcURL == "" {
		pcURL = defaultURL
	}
	elements := make([]any, 0, 3)
	if content.Body != "" {
		elements = append(elements, map[string]any{
			"tag":     "markdown",
			"content": sanitizeIssueCardMarkdown(content.Body),
		})
	}
	if content.ReplyHint != "" {
		elements = append(elements, map[string]any{
			"tag":     "markdown",
			"content": "**处理方式**\n" + escapeCardMarkdown(content.ReplyHint),
		})
	}
	if content.CanApprove {
		elements = append(elements, map[string]any{
			"tag":        "button",
			"element_id": "approve_review",
			"type":       "primary_filled",
			"text": map[string]any{
				"tag":     "plain_text",
				"content": "审核通过",
			},
			"behaviors": []any{
				map[string]any{
					"type": "callback",
					"value": map[string]any{
						"action": reviewApprovalAction,
					},
				},
			},
		})
	}
	elements = append(elements, map[string]any{
		"tag":  "button",
		"type": "primary",
		"text": map[string]any{
			"tag":     "plain_text",
			"content": "在 Multica 中查看",
		},
		"behaviors": []any{
			map[string]any{
				"type":        "open_url",
				"default_url": defaultURL,
				"pc_url":      pcURL,
			},
		},
	})
	doc := map[string]any{
		"schema": "2.0",
		"config": map[string]any{
			"summary":      map[string]any{"content": content.Title},
			"update_multi": true,
		},
		"header": map[string]any{
			"template": "grey",
			"title": map[string]any{
				"tag":     "plain_text",
				"content": content.Title,
			},
		},
		"body": map[string]any{
			"elements": elements,
		},
	}
	raw, err := json.Marshal(doc)
	if err != nil {
		return "", fmt.Errorf("lark: encode issue push card: %w", err)
	}
	return string(raw), nil
}

// desktopBridgeURL keeps Lark's card URL on HTTP(S). Lark Desktop ignores
// arbitrary custom schemes in open_url behaviors, so the browser handoff page
// performs the multica:// launch from a normal user-visible web origin and
// retains the issue URL as a fallback.
func desktopBridgeURL(webURL, desktopURL string) string {
	if webURL == "" || desktopURL == "" {
		return ""
	}
	bridge, err := url.Parse(webURL)
	if err != nil || (bridge.Scheme != "https" && bridge.Scheme != "http") || bridge.Host == "" {
		return ""
	}
	target, err := url.Parse(desktopURL)
	if err != nil || target.Scheme != "multica" || target.Host != "issue" {
		return ""
	}
	issueID := strings.TrimPrefix(target.Path, "/")
	workspaceIDs := target.Query()["workspace"]
	commentIDs := target.Query()["comment"]
	if issueID == "" || strings.Contains(issueID, "/") || len(workspaceIDs) != 1 || len(commentIDs) > 1 {
		return ""
	}
	query := url.Values{
		"fallback":  {webURL},
		"issue":     {issueID},
		"workspace": {workspaceIDs[0]},
	}
	if len(commentIDs) == 1 {
		query.Set("comment", commentIDs[0])
	}
	bridge.Path = "/desktop/open"
	bridge.RawPath = ""
	bridge.RawQuery = query.Encode()
	bridge.Fragment = ""
	return bridge.String()
}

// sanitizeIssueCardMarkdown preserves useful formatting while preventing
// member-authored content from creating a hidden link or a Lark-native tag
// under the bot's identity. Bare URLs remain visible and can still be
// auto-linked by the client; bold, lists, quotes, tables, and code survive.
// Card structure, title, action guidance, and the trusted button live in
// separate JSON fields, so body Markdown can never inject another element.
func sanitizeIssueCardMarkdown(text string) string {
	replacer := strings.NewReplacer(
		"\\", "\\\\",
		"](", "] (",
		"]:", "] :",
		"<", "\\<",
	)
	return replacer.Replace(text)
}

// escapeCardMarkdown renders trusted plain text inside one markdown element.
func escapeCardMarkdown(text string) string {
	replacer := strings.NewReplacer(
		"\\", "\\\\",
		"`", "\\`",
		"*", "\\*",
		"_", "\\_",
		"~", "\\~",
		"#", "\\#",
		"[", "\\[",
		"]", "\\]",
		"(", "\\(",
		")", "\\)",
		"<", "\\<",
		">", "\\>",
	)
	return replacer.Replace(text)
}

// AcceptsReplies is true for Lark: SendDirectMessage returns a real
// message_id on send, and Lark's inbound delivery reports a per-message
// reply context, so a reply to a push can be attributed back to the issue
// it was about. Contrast WeCom (internal/integrations/wecom/notify_dm.go),
// which returns false — its send ack carries no message id and its inbound
// callback carries no reply context, so neither half of the loop exists
// there.
func (d *dmDeliverer) AcceptsReplies() bool { return true }
