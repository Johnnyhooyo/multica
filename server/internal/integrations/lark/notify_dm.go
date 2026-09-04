package lark

import (
	"context"
	"errors"
	"log/slog"

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

func (d *dmDeliverer) DeliverDM(ctx context.Context, _ notify.PushRef, binding db.ChannelUserBinding, text string) (notify.DeliverResult, error) {
	if text == "" || binding.ChannelUserID == "" {
		return notify.DeliverResult{}, nil
	}
	creds, err := d.creds(binding.InstallationID)
	if err != nil {
		return notify.DeliverResult{}, err
	}
	messageID, err := d.client.SendDirectMessage(ctx, SendDirectParams{
		InstallationID: creds,
		OpenID:         OpenID(binding.ChannelUserID),
		// PlainHead, because SendDirectMessage posts msg_type=text and Lark
		// renders no markdown there — the title's "**" would reach the
		// recipient as two literal asterisks on every push. A markdown card
		// would render it, but cards address a chat_id, and this path
		// deliberately opens a fresh 1:1 by open_id.
		Text: notify.PlainHead(text),
	})
	if err != nil {
		return notify.DeliverResult{}, err
	}
	if messageID == "" {
		// Without an id the push is unaddressable, so a reply to it would
		// fall through to the ordinary chat path and confuse the user.
		// Surfacing this as a failure keeps the metric honest.
		return notify.DeliverResult{}, errors.New("lark: send returned no message_id")
	}
	return notify.DeliverResult{State: notify.StateDelivered, MessageID: messageID}, nil
}

// AcceptsReplies is true for Lark: SendDirectMessage returns a real
// message_id on send, and Lark's inbound delivery reports a per-message
// reply context, so a reply to a push can be attributed back to the issue
// it was about. Contrast WeCom (internal/integrations/wecom/notify_dm.go),
// which returns false — its send ack carries no message id and its inbound
// callback carries no reply context, so neither half of the loop exists
// there.
func (d *dmDeliverer) AcceptsReplies() bool { return true }
