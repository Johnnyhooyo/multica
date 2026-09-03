package lark

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/integrations/channel/notify"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

type recordingDMClient struct {
	APIClient // embedded so only the method under test needs implementing
	lastOpen  OpenID
	lastText  string
	messageID string
	err       error
}

func (c *recordingDMClient) SendDirectMessage(_ context.Context, p SendDirectParams) (string, error) {
	c.lastOpen, c.lastText = p.OpenID, p.Text
	return c.messageID, c.err
}

// testDMCreds stands in for the real credentials lookup NewDMDeliverer takes
// a CredentialsFunc for. Named distinctly from http_client_test.go's
// testCreds() (a different, argument-less helper already in this package).
func testDMCreds(pgtype.UUID) (InstallationCredentials, error) {
	return InstallationCredentials{AppID: "cli_test"}, nil
}

// The message id is what makes the push replyable. Losing it here silently
// turns the whole reply half off for Lark.
func TestDeliverDMReturnsTheLarkMessageID(t *testing.T) {
	c := &recordingDMClient{messageID: "om_abc123"}
	d := NewDMDeliverer(c, testDMCreds, slog.Default())

	res, err := d.DeliverDM(context.Background(), notify.PushRef{},
		db.ChannelUserBinding{ChannelUserID: "ou_recipient"}, "**[in_review] Ship it**")
	if err != nil {
		t.Fatalf("DeliverDM: %v", err)
	}
	if res.State != notify.StateDelivered {
		t.Errorf("State = %v, want StateDelivered", res.State)
	}
	if res.MessageID != "om_abc123" {
		t.Errorf("MessageID = %q, want om_abc123", res.MessageID)
	}
	// A member binding's channel_user_id is an open_id, not a chat_id.
	if c.lastOpen != OpenID("ou_recipient") {
		t.Errorf("OpenID = %q, want ou_recipient", c.lastOpen)
	}
	if c.lastText == "" {
		t.Error("sent empty text")
	}
}

func TestDeliverDMPropagatesAnAPIError(t *testing.T) {
	c := &recordingDMClient{err: errors.New("lark refused")}
	d := NewDMDeliverer(c, testDMCreds, slog.Default())

	res, err := d.DeliverDM(context.Background(), notify.PushRef{},
		db.ChannelUserBinding{ChannelUserID: "ou_recipient"}, "text")
	if err == nil {
		t.Fatal("DeliverDM error = nil, want the API error")
	}
	if res.State != notify.StateUnsupported {
		t.Errorf("State = %v, want the zero state on failure", res.State)
	}
}

// A delivered-but-idless result would be recorded as replyable by nothing and
// silently drop the reply path; treat a missing id as a failed push.
func TestDeliverDMTreatsAMissingMessageIDAsAFailure(t *testing.T) {
	c := &recordingDMClient{messageID: ""}
	d := NewDMDeliverer(c, testDMCreds, slog.Default())

	_, err := d.DeliverDM(context.Background(), notify.PushRef{},
		db.ChannelUserBinding{ChannelUserID: "ou_recipient"}, "text")
	if err == nil {
		t.Fatal("DeliverDM error = nil, want an error when Lark returns no message_id")
	}
}

// AcceptsReplies is the flag that turns the reply half of the feature on for
// Lark. If it ever returned false, pushes would still be delivered but
// silently stop being replyable, and no other test here would notice —
// DeliverDM itself does not consult it.
func TestLarkDMDelivererAcceptsReplies(t *testing.T) {
	d := NewDMDeliverer(&recordingDMClient{}, testDMCreds, slog.Default())
	if !d.AcceptsReplies() {
		t.Error("AcceptsReplies() = false, want true: Lark send returns a message id and inbound carries a per-message reply context")
	}
}
