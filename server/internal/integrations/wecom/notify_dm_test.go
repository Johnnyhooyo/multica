package wecom

import (
	"context"
	"log/slog"
	"testing"

	"github.com/multica-ai/multica/server/internal/integrations/channel/notify"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// testPushRef is a non-empty PushRef. The wecom tests never assert on it
// beyond feeding the relay key, so any stable pair of ids will do.
var testPushRef = notify.PushRef{InboxItemID: "item-1", RecipientUserID: "user-1"}

// newTestRelay builds a relay whose publish() always reaches a fanout target,
// so DeliverDM's off-lease handoff branch has somewhere to route to.
func newTestRelay(t *testing.T) *RelayOutbound {
	t.Helper()
	return NewRelayOutbound(&fanoutRelay{}, nil, RelayConfig{Shards: 1}, slog.Default())
}

// WeCom sends over the aibot socket, which reports no platform message id.
// Delivered-with-no-id is the honest answer: the push happened and nobody can
// reply to it.
func TestDeliverDMReportsDeliveredWithoutAMessageID(t *testing.T) {
	q := &fakeOutboundQueries{}
	o, instID, conn := newOutboundWithConn(t, q)
	binding := db.ChannelUserBinding{InstallationID: instID, ChannelUserID: "T_USER_1"}

	res, err := o.DeliverDM(context.Background(), testPushRef, binding, "**[in_review] Ship it**")
	if err != nil {
		t.Fatalf("DeliverDM: %v", err)
	}
	if res.State != notify.StateDelivered {
		t.Errorf("State = %v, want StateDelivered", res.State)
	}
	if res.MessageID != "" {
		t.Errorf("MessageID = %q, want empty: wecom send acks carry no id", res.MessageID)
	}

	body := conn.sendBody(t, 0)
	if body["chatid"] != "T_USER_1" {
		t.Errorf("chatid = %v, want T_USER_1", body["chatid"])
	}
	// The binding's channel_user_id is the bot-scoped T-* userid, which WeCom
	// treats as the chatid of a single chat. Never guess group.
	if body["chat_type"] != float64(chatTypeSingleInt) {
		t.Errorf("chat_type = %v, want %d", body["chat_type"], chatTypeSingleInt)
	}
}

// EventInboxNew fires on whichever replica the load balancer picked; the WS
// lease lives on exactly one. Handing the frame to that replica is neither a
// delivery nor a failure.
func TestDeliverDMHandsOffWhenThisReplicaHasNoSocket(t *testing.T) {
	q := &fakeOutboundQueries{}
	o := NewOutbound(q, newSendersRegistry(), slog.Default(), WithRelay(newTestRelay(t)))
	binding := db.ChannelUserBinding{InstallationID: mustTestUUID(t), ChannelUserID: "T_USER_1"}

	res, err := o.DeliverDM(context.Background(), testPushRef, binding, "text")
	if err != nil {
		t.Fatalf("DeliverDM: %v", err)
	}
	if res.State != notify.StateHandedOff {
		t.Errorf("State = %v, want StateHandedOff", res.State)
	}
}
