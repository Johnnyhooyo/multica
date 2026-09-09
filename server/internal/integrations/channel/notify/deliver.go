package notify

import (
	"context"

	"github.com/jackc/pgx/v5/pgtype"

	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// DeliverState is the outcome of asking an adapter to send a direct message.
type DeliverState int

const (
	// StateUnsupported means the adapter does not do direct messages. The
	// zero value, so an adapter that returns an empty result is treated as
	// having done nothing rather than as having succeeded.
	StateUnsupported DeliverState = iota

	// StateDelivered means the platform accepted the message. MessageID is
	// the platform's identifier for it when the platform gives one out.
	//
	// An empty MessageID is legal and means "sent, but not addressable": the
	// push happened, and no reply to it can be attributed back. WeCom is
	// exactly this case — sendTextCtx discards the send ack and the inbound
	// callback carries no reply context at all.
	StateDelivered

	// StateHandedOff means this replica could not send but passed the
	// message to the replica that can. Whether it goes out, and what message
	// id it gets, are both unknowable from here.
	//
	// This exists for WeCom: it has no outbound REST path, every write goes
	// over the aibot WebSocket, and the WS lease means exactly one replica
	// holds it — while EventInboxNew fires on whichever replica the load
	// balancer picked. The hand-off must be asynchronous (a synchronous
	// round-trip would let one unhealthy bot stall an entire shard), so no
	// receipt comes back.
	//
	// It is its own state because counting it as delivered makes the metrics
	// lie, and counting it as failure would trip a fallback that should not
	// fire.
	StateHandedOff
)

// DeliverResult is what an adapter reports back.
type DeliverResult struct {
	State DeliverState
	// MessageID is the platform message identifier, when the platform
	// returned one. Only a non-empty value makes a push replyable.
	MessageID string
}

// PushCardContent keeps trusted notification structure separate from the
// platform-neutral text fallback. Card-capable adapters can render the title,
// body, and action hint as distinct elements without parsing a string that
// mixes system copy with member-authored Markdown.
type PushCardContent struct {
	Title     string
	Body      string
	ReplyHint string
}

// PushRef identifies the inbox row a push came from.
//
// It exists because WeCom's cross-replica relay claims work under
// relayInboxEventID(inboxItemID, recipientUserID). Once the subscription
// moved up into this package the adapter stopped being able to look those
// ids up, so they are passed down. Adapters that do not relay ignore it.
type PushRef struct {
	InboxItemID     string
	RecipientUserID string
	// Card carries the semantic fields used by card-capable adapters. Plain-text
	// adapters continue to use the separate text argument to DeliverDM.
	Card PushCardContent
	// WebURL is the ordinary HTTP(S) fallback for opening the notification's
	// resource. DesktopURL is the installed-app target for platforms that can
	// choose a PC-specific URL (Lark cards do). Empty values mean the inbox
	// item has no issue resource to open.
	WebURL     string
	DesktopURL string
	// StartTopic asks a capable adapter to make this push the root of a
	// dedicated discussion topic. It is set only for replyable issue pushes;
	// informational notifications stay as ordinary messages.
	StartTopic bool
}

// DMDeliverer sends one direct message to a bound member.
//
// This is deliberately not channel.Channel.Send. That seam does not exist on
// every platform — wecomChannel.Send returns ErrSendNotSupported by design —
// so a shared layer built on it would be dead on arrival for WeCom. The
// binding is passed whole so the adapter, not this package, decides how to
// address a single chat; the shared layer never guesses single-vs-group.
type DMDeliverer interface {
	DeliverDM(ctx context.Context, ref PushRef, binding db.ChannelUserBinding, text string) (DeliverResult, error)

	// AcceptsReplies reports whether a reply to a push on this platform can
	// ever reach us. It answers for the platform, not for one push: a
	// platform that can be replied to still produces individual pushes that
	// cannot be (an empty MessageID, a relayed hand-off), and the ledger row
	// is what gates those.
	//
	// The rendered text promises the recipient they can reply, so it needs
	// this answer before delivery rather than after. WeCom returns false —
	// it can push, but its send ack carries no message id and its inbound
	// callback carries no reply context, so a reply lands in an ordinary
	// conversation and is discarded. Promising one there would tell every
	// WeCom user to do something that silently does nothing.
	AcceptsReplies() bool
}

// TopicReplyDeliverer is the optional capability required to return an agent
// comment to the exact topic whose push reply caused that run. It is separate
// from DMDeliverer because platforms such as WeCom can push but cannot address
// a replyable topic.
type TopicReplyDeliverer interface {
	DeliverTopicReply(ctx context.Context, installationID pgtype.UUID, rootMessageID, text string) (DeliverResult, error)
}
