package wecom

import (
	"context"

	"github.com/multica-ai/multica/server/internal/integrations/channel/notify"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// DeliverDM sends an inbox push to a bound member over the aibot socket.
//
// This is the body that used to live in tryDeliverInbox. The subscription,
// the whitelist and the binding lookup moved up into the notify package; what
// stays here is the part that is genuinely WeCom's — addressing a single chat
// by the bot-scoped T-* userid, and routing across replicas when this one
// does not hold the WS lease.
//
// The result never carries a MessageID. sendTextCtx reads the ack only for
// its error code and discards the body, and WeCom's inbound callback carries
// no reply context, so a WeCom push cannot be replied to no matter what we
// record. Reporting an empty id keeps that honest rather than writing a
// ledger row no inbound message will ever match.
func (o *Outbound) DeliverDM(ctx context.Context, ref notify.PushRef, binding db.ChannelUserBinding, text string) (notify.DeliverResult, error) {
	if text == "" {
		return notify.DeliverResult{}, nil
	}
	var sender *wsSender
	if o.senders != nil {
		sender = o.senders.get(binding.InstallationID)
	}
	if sender == nil {
		if o.relay.publish(relayFrame{
			Kind:           relayKindInbox,
			InstallationID: util.UUIDToString(binding.InstallationID),
			ChatID:         binding.ChannelUserID,
			ChatType:       chatTypeSingleInt,
			Content:        text,
		}, relayInboxEventID(ref.InboxItemID, ref.RecipientUserID)) {
			o.logger.DebugContext(ctx, "wecom outbound: routed an inbox push to the replica holding the socket",
				"installation_id", uuidStringPub(binding.InstallationID))
			return notify.DeliverResult{State: notify.StateHandedOff}, nil
		}
		o.logger.WarnContext(ctx, "wecom outbound: inbox push not delivered and not routable",
			"installation_id", uuidStringPub(binding.InstallationID))
		return notify.DeliverResult{}, nil
	}
	if err := sender.sendTextCtx(ctx, binding.ChannelUserID, chatTypeSingleInt, text); err != nil {
		return notify.DeliverResult{}, err
	}
	o.logger.DebugContext(ctx, "wecom outbound: inbox delivered via bot",
		"installation_id", uuidStringPub(binding.InstallationID))
	return notify.DeliverResult{State: notify.StateDelivered}, nil
}
