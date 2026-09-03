package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/integrations/channel"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// fakePushReplies is the PushReplyPoster test double. LookupPush is keyed by
// the platform message id the caller passes, mirroring the real
// (installationID, channelMessageID) pair without asserting on installationID
// itself — the router harness only ever runs one installation.
type fakePushReplies struct {
	byMessageID map[string]db.ChannelPushMessage
	posted      []string
	result      PushReplyResult
	lookupErr   error
}

func (f *fakePushReplies) LookupPush(_ context.Context, _ pgtype.UUID, id string) (db.ChannelPushMessage, bool, error) {
	if f.lookupErr != nil {
		return db.ChannelPushMessage{}, false, f.lookupErr
	}
	row, ok := f.byMessageID[id]
	return row, ok, nil
}

func (f *fakePushReplies) PostPushReplyComment(_ context.Context, _ db.ChannelPushMessage, _ pgtype.UUID, content string) (PushReplyResult, error) {
	f.posted = append(f.posted, content)
	return f.result, nil
}

// newHarnessWithPushReplies rebuilds the router with a PushReplies poster
// wired in, reusing every other fake exactly as newHarness configured them.
// Mirrors the existing pattern of overriding h.router with a custom
// RouterConfig (see TestRouter_MediaResolverTimeoutAppendsOriginalMessage).
func newHarnessWithPushReplies(t *testing.T, f PushReplyPoster) *harness {
	t.Helper()
	h := newHarness(t)
	h.router = NewRouter(h.issues, h.tasks, h.reader, RouterConfig{
		Logger: discardLogger(), Lifecycle: h.lifecycle, PushReplies: f,
	})
	h.router.Register(channel.TypeFeishu, ResolverSet{
		Installation: h.inst,
		Identity:     h.ident,
		Dedup:        h.dedup,
		Session:      h.binder,
		Audit:        h.audit,
		Replier:      h.replier,
		Typing:       h.typing,
		Media:        h.media,
		OriginType:   "lark_chat",
	})
	return h
}

// lastResult waits for the detached replier to receive a Result and returns
// the most recent one. The replier is always invoked off the ACK path (see
// Router.scheduleReply), so every push-reply assertion needs this instead of
// reading Handle's return value.
func lastResult(t *testing.T, h *harness) Result {
	t.Helper()
	var res Result
	if !waitFor(time.Second, func() bool {
		calls := h.replier.calls()
		if len(calls) == 0 {
			return false
		}
		res = calls[len(calls)-1]
		return true
	}) {
		t.Fatal("replier never received a Result")
	}
	return res
}

// The core routing claim: a reply to a push leaves the chat pipeline.
func TestPushReplyDoesNotTouchTheChatPipeline(t *testing.T) {
	f := &fakePushReplies{
		byMessageID: map[string]db.ChannelPushMessage{"om_push_1": {}},
		result:      PushReplyResult{Posted: true, Message: "已记录"},
	}
	h := newHarnessWithPushReplies(t, f)

	msg := p2pMessage(t)
	msg.Text = "确认审核"
	msg.ReplyTo = &channel.ReplyCtx{MessageID: "om_push_1"}

	if err := h.router.Handle(context.Background(), msg); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	res := lastResult(t, h)

	if res.Outcome != OutcomePushReply {
		t.Fatalf("Outcome = %q, want %q", res.Outcome, OutcomePushReply)
	}
	if res.PushReplyText != "已记录" {
		t.Errorf("PushReplyText = %q", res.PushReplyText)
	}
	if len(f.posted) != 1 || f.posted[0] != "确认审核" {
		t.Errorf("posted = %v, want one entry with the reply text", f.posted)
	}
	// The two things the spec forbids on this path.
	if h.binder.ensureCalls != 0 || h.binder.startCalls != 0 {
		t.Errorf("session resolution ran: ensure=%d start=%d", h.binder.ensureCalls, h.binder.startCalls)
	}
	if h.issues.called {
		t.Error("issue creation ran; /issue must not be parsed on this path")
	}
}

// A "/issue Foo" typed as a reply to a push is a decision, not a command.
func TestPushReplyDoesNotParseIssueCommand(t *testing.T) {
	f := &fakePushReplies{
		byMessageID: map[string]db.ChannelPushMessage{"om_push_1": {}},
		result:      PushReplyResult{Posted: true, Message: "已记录"},
	}
	h := newHarnessWithPushReplies(t, f)

	msg := p2pMessage(t)
	msg.Text = "/issue 顺便再建一个"
	msg.ReplyTo = &channel.ReplyCtx{MessageID: "om_push_1"}

	if err := h.router.Handle(context.Background(), msg); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	res := lastResult(t, h)

	if res.Outcome != OutcomePushReply {
		t.Fatalf("Outcome = %q, want %q", res.Outcome, OutcomePushReply)
	}
	if h.issues.called {
		t.Error("an /issue command was parsed out of a push reply")
	}
}

// Slack reports only a thread-level id, so RootID is the fallback key.
func TestPushReplyFallsBackToRootID(t *testing.T) {
	f := &fakePushReplies{
		byMessageID: map[string]db.ChannelPushMessage{"root_1": {}},
		result:      PushReplyResult{Posted: true, Message: "已记录"},
	}
	h := newHarnessWithPushReplies(t, f)

	msg := p2pMessage(t)
	msg.Text = "确认审核"
	msg.ReplyTo = &channel.ReplyCtx{MessageID: "not_a_push", RootID: "root_1"}

	if err := h.router.Handle(context.Background(), msg); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if res := lastResult(t, h); res.Outcome != OutcomePushReply {
		t.Fatalf("Outcome = %q, want the RootID fallback to hit", res.Outcome)
	}
}

// A reply to an ordinary agent message must behave exactly as before.
func TestReplyToANonPushTakesTheChatPath(t *testing.T) {
	f := &fakePushReplies{byMessageID: map[string]db.ChannelPushMessage{}}
	h := newHarnessWithPushReplies(t, f)

	msg := p2pMessage(t)
	msg.Text = "再补充一句"
	msg.ReplyTo = &channel.ReplyCtx{MessageID: "om_ordinary"}

	if err := h.router.Handle(context.Background(), msg); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	res := lastResult(t, h)
	if res.Outcome == OutcomePushReply || res.Outcome == OutcomePushReplyDenied {
		t.Fatalf("Outcome = %q; a non-push reply must stay on the chat path", res.Outcome)
	}
	if h.binder.ensureCalls == 0 {
		t.Error("chat path did not run")
	}
}

// No ReplyTo at all: the lookup must not even be attempted.
func TestNoReplyToSkipsTheLookup(t *testing.T) {
	f := &fakePushReplies{lookupErr: errors.New("LookupPush must not be called")}
	h := newHarnessWithPushReplies(t, f)

	msg := p2pMessage(t)
	if err := h.router.Handle(context.Background(), msg); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if res := lastResult(t, h); res.Outcome == OutcomePushReply {
		t.Fatal("a message with no ReplyTo entered the push path")
	}
}

// A denial still reaches the user; silence would look like the bot is broken.
func TestPushReplyDenialRepliesAndWritesNothing(t *testing.T) {
	f := &fakePushReplies{
		byMessageID: map[string]db.ChannelPushMessage{"om_push_1": {}},
		result:      PushReplyResult{Posted: false, Message: "你没有权限回复这条推送。"},
	}
	h := newHarnessWithPushReplies(t, f)

	msg := p2pMessage(t)
	msg.Text = "确认审核"
	msg.ReplyTo = &channel.ReplyCtx{MessageID: "om_push_1"}

	if err := h.router.Handle(context.Background(), msg); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	res := lastResult(t, h)
	if res.Outcome != OutcomePushReplyDenied {
		t.Fatalf("Outcome = %q, want %q", res.Outcome, OutcomePushReplyDenied)
	}
	if res.PushReplyText == "" {
		t.Error("denial carried no text; the user would see silence")
	}
	if h.binder.ensureCalls != 0 {
		t.Error("a denied reply still ran the chat path")
	}
}

// The feature must be inert until Task 8 wires it: newHarness never sets
// RouterConfig.PushReplies, so it defaults to nil.
func TestNilPushRepliesLeavesTheOldPathUntouched(t *testing.T) {
	h := newHarness(t)

	msg := p2pMessage(t)
	msg.Text = "确认审核"
	msg.ReplyTo = &channel.ReplyCtx{MessageID: "om_push_1"}

	if err := h.router.Handle(context.Background(), msg); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if res := lastResult(t, h); res.Outcome == OutcomePushReply {
		t.Fatal("push path ran with no poster configured")
	}
}
