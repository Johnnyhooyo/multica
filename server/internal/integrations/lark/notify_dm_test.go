package lark

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/url"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/integrations/channel/notify"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

type recordingDMClient struct {
	APIClient // embedded so only the method under test needs implementing
	lastOpen  OpenID
	lastText  string
	lastCard  string
	textSends []SendTextParams
	cardSends []SendMarkdownCardParams
	messageID string
	err       error
	topicErr  error
}

func (c *recordingDMClient) SendDirectMessage(_ context.Context, p SendDirectParams) (string, error) {
	c.lastOpen, c.lastText, c.lastCard = p.OpenID, p.Text, p.CardJSON
	return c.messageID, c.err
}

func (c *recordingDMClient) SendTextMessage(_ context.Context, p SendTextParams) (string, error) {
	c.textSends = append(c.textSends, p)
	return "om_topic", c.topicErr
}

func (c *recordingDMClient) SendMarkdownCard(_ context.Context, p SendMarkdownCardParams) (string, error) {
	c.cardSends = append(c.cardSends, p)
	return "om_topic", c.topicErr
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

// SendDirectMessage posts msg_type=text, which Lark renders verbatim. The
// shared renderer wraps the title in "**" because WeCom's aibot does render
// markdown, so the adapter has to undo it here or every Lark push opens with
// two literal asterisks — the first thing a recipient sees.
func TestDeliverDMSendsNoLiteralMarkdownToLark(t *testing.T) {
	c := &recordingDMClient{messageID: "om_abc123"}
	d := NewDMDeliverer(c, testDMCreds, slog.Default())

	if _, err := d.DeliverDM(context.Background(), notify.PushRef{},
		db.ChannelUserBinding{ChannelUserID: "ou_recipient"},
		"**[状态变更] Ship it**\nhttps://app.example.com/acme/issues/x"); err != nil {
		t.Fatalf("DeliverDM: %v", err)
	}
	if strings.Contains(c.lastText, "**") {
		t.Errorf("sent %q; Lark shows these asterisks to the user", c.lastText)
	}
	if !strings.HasPrefix(c.lastText, "[状态变更] Ship it") {
		t.Errorf("sent %q, want the title intact without its emphasis", c.lastText)
	}
	// The rest of the push has to survive: the deep link is the recipient's
	// route into the app.
	if !strings.Contains(c.lastText, "https://app.example.com/acme/issues/x") {
		t.Errorf("sent %q, want the deep link preserved", c.lastText)
	}
}

func TestDeliverDMSendsIssueCardAndStartsDedicatedTopic(t *testing.T) {
	c := &recordingDMClient{messageID: "om_root"}
	d := NewDMDeliverer(c, testDMCreds, slog.Default())
	ref := notify.PushRef{
		WebURL:     "https://app.example.com/acme/issues/11111111-2222-3333-4444-555555555555#comment-99999999-8888-7777-6666-555555555555",
		DesktopURL: "multica://issue/11111111-2222-3333-4444-555555555555?workspace=aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee&comment=99999999-8888-7777-6666-555555555555",
		StartTopic: true,
		Card: notify.PushCardContent{
			Title:      "[待你审核] Ship *this*",
			Body:       "正文 **重点**\n- 核对结果\n[伪装链接](https://evil.example)\n<at id=all>所有人</at>",
			ReplyHint:  "审核通过可回复「审核通过」；需要修改请直接说明。",
			CanApprove: true,
		},
	}

	res, err := d.DeliverDM(context.Background(), ref,
		db.ChannelUserBinding{ChannelUserID: "ou_recipient"},
		"**[待你审核] Ship *this***\n正文 **重点**")
	if err != nil {
		t.Fatalf("DeliverDM: %v", err)
	}
	if res.MessageID != "om_root" || res.State != notify.StateDelivered {
		t.Fatalf("result = %+v", res)
	}
	if c.lastText != "" {
		t.Errorf("text = %q, want card-only send", c.lastText)
	}
	var card struct {
		Schema string `json:"schema"`
		Config struct {
			Summary struct {
				Content string `json:"content"`
			} `json:"summary"`
		} `json:"config"`
		Header struct {
			Title struct {
				Tag     string `json:"tag"`
				Content string `json:"content"`
			} `json:"title"`
		} `json:"header"`
		Body struct {
			Elements []map[string]any `json:"elements"`
		} `json:"body"`
	}
	if err := json.Unmarshal([]byte(c.lastCard), &card); err != nil {
		t.Fatalf("card json: %v", err)
	}
	if card.Schema != "2.0" || len(card.Body.Elements) != 4 {
		t.Fatalf("card = %+v", card)
	}
	if card.Header.Title.Tag != "plain_text" || card.Header.Title.Content != ref.Card.Title {
		t.Errorf("card header = %+v", card.Header.Title)
	}
	if card.Config.Summary.Content != ref.Card.Title {
		t.Errorf("card summary = %q", card.Config.Summary.Content)
	}
	body, _ := card.Body.Elements[0]["content"].(string)
	for _, want := range []string{"正文 **重点**", "- 核对结果", "[伪装链接] (https://evil.example)", "\\<at id=all>所有人\\</at>"} {
		if !strings.Contains(body, want) {
			t.Errorf("safe markdown body %q does not contain %q", body, want)
		}
	}
	if strings.Contains(body, "](") || strings.Contains(body, "\n<at") {
		t.Errorf("unsafe markdown survived in card body: %q", body)
	}
	hint, _ := card.Body.Elements[1]["content"].(string)
	if !strings.HasPrefix(hint, "**处理方式**\n") || !strings.Contains(hint, "审核通过") {
		t.Errorf("card reply hint = %q", hint)
	}
	approve := card.Body.Elements[2]
	if approve["element_id"] != "approve_review" || approve["type"] != "primary_filled" {
		t.Fatalf("approve button = %#v", approve)
	}
	approveBehaviors, _ := approve["behaviors"].([]any)
	if len(approveBehaviors) != 1 {
		t.Fatalf("approve behaviors = %#v", approve["behaviors"])
	}
	callback := approveBehaviors[0].(map[string]any)
	value := callback["value"].(map[string]any)
	if callback["type"] != "callback" || value["action"] != reviewApprovalAction {
		t.Errorf("approve callback = %#v", callback)
	}
	button := card.Body.Elements[3]
	behaviors, _ := button["behaviors"].([]any)
	if len(behaviors) != 1 {
		t.Fatalf("button behaviors = %#v", button["behaviors"])
	}
	openURL := behaviors[0].(map[string]any)
	if openURL["default_url"] != ref.WebURL {
		t.Errorf("open_url behavior = %#v", openURL)
	}
	pcURL, err := url.Parse(openURL["pc_url"].(string))
	if err != nil {
		t.Fatalf("pc_url: %v", err)
	}
	if pcURL.Scheme != "https" || pcURL.Host != "app.example.com" || pcURL.Path != "/desktop/open" {
		t.Errorf("pc_url = %q, want HTTPS desktop bridge", pcURL)
	}
	if pcURL.Query().Get("issue") != "11111111-2222-3333-4444-555555555555" ||
		pcURL.Query().Get("workspace") != "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee" ||
		pcURL.Query().Get("comment") != "99999999-8888-7777-6666-555555555555" ||
		pcURL.Query().Get("fallback") != ref.WebURL {
		t.Errorf("pc_url query = %#v", pcURL.Query())
	}
	if len(c.textSends) != 1 {
		t.Fatalf("topic sends = %d, want 1", len(c.textSends))
	}
	topic := c.textSends[0]
	if topic.ChatID != "" || topic.ReplyTarget.MessageID != "om_root" || !topic.ReplyTarget.InThread {
		t.Errorf("topic send = %+v", topic)
	}
}

func TestIssuePushCardOmitsApprovalButtonWhenNotReviewable(t *testing.T) {
	cardJSON, err := issuePushCard(notify.PushCardContent{
		Title: "[任务受阻] Ship it",
		Body:  "请补充信息后继续。",
	}, "https://app.example.com/acme/issues/11111111-2222-3333-4444-555555555555", "")
	if err != nil {
		t.Fatalf("issuePushCard: %v", err)
	}

	var card struct {
		Body struct {
			Elements []map[string]any `json:"elements"`
		} `json:"body"`
	}
	if err := json.Unmarshal([]byte(cardJSON), &card); err != nil {
		t.Fatalf("card json: %v", err)
	}
	for _, element := range card.Body.Elements {
		if element["element_id"] == "approve_review" {
			t.Fatalf("non-review card contains approval button: %#v", element)
		}
		behaviors, _ := element["behaviors"].([]any)
		for _, behavior := range behaviors {
			callback, _ := behavior.(map[string]any)
			if callback["type"] == "callback" {
				t.Fatalf("non-review card contains callback behavior: %#v", callback)
			}
		}
	}
}

func TestSanitizeIssueCardMarkdownKeepsFormattingAndBreaksImpersonation(t *testing.T) {
	in := "## 结果\n- **通过**\n- `code`\n[重置密码](https://evil.example)\n[重置密码]: https://evil.example\n<at id=all>所有人</at>\n\\<at id=all>"
	got := sanitizeIssueCardMarkdown(in)
	for _, want := range []string{"## 结果", "- **通过**", "- `code`"} {
		if !strings.Contains(got, want) {
			t.Errorf("sanitizeIssueCardMarkdown() = %q, lost %q", got, want)
		}
	}
	for _, unsafe := range []string{"](", "]:", "\n<at"} {
		if strings.Contains(got, unsafe) {
			t.Errorf("sanitizeIssueCardMarkdown() = %q, retained %q", got, unsafe)
		}
	}
}

func TestDesktopBridgeURLRejectsNonWebFallback(t *testing.T) {
	if got := desktopBridgeURL("javascript:alert(1)", "multica://issue/123"); got != "" {
		t.Fatalf("desktopBridgeURL = %q, want empty", got)
	}
}

func TestDeliverDMKeepsDeliveredRootWhenTopicCreationFails(t *testing.T) {
	c := &recordingDMClient{
		messageID: "om_root",
		topicErr:  errors.New("topic unavailable"),
	}
	d := NewDMDeliverer(c, testDMCreds, slog.Default())

	res, err := d.DeliverDM(context.Background(), notify.PushRef{
		DesktopURL: "multica://issue/issue-id?workspace=workspace-id",
		StartTopic: true,
	}, db.ChannelUserBinding{ChannelUserID: "ou_recipient"}, "text")
	if err != nil {
		t.Fatalf("DeliverDM: %v", err)
	}
	if res.State != notify.StateDelivered || res.MessageID != "om_root" {
		t.Errorf("result = %+v, want delivered root", res)
	}
	if c.lastCard != "" || c.lastText != "text" {
		t.Errorf("push without a web fallback = card %q, text %q; want plain text", c.lastCard, c.lastText)
	}
}

func TestDeliverTopicReplySendsCardToOriginalPushRoot(t *testing.T) {
	c := &recordingDMClient{}
	d := NewDMDeliverer(c, testDMCreds, slog.Default())
	capable := d.(notify.TopicReplyDeliverer)
	content := "## 处理结果\n\n- 已完成修改\n- 已通过测试"
	res, err := capable.DeliverTopicReply(context.Background(), pgtype.UUID{}, "om_root", content)
	if err != nil {
		t.Fatalf("DeliverTopicReply: %v", err)
	}
	if res.State != notify.StateDelivered || res.MessageID != "om_topic" {
		t.Fatalf("result = %+v", res)
	}
	if len(c.textSends) != 0 {
		t.Fatalf("text sends = %d, want 0", len(c.textSends))
	}
	if len(c.cardSends) != 1 {
		t.Fatalf("card sends = %d, want 1", len(c.cardSends))
	}
	got := c.cardSends[0]
	if got.ChatID != "" || got.Markdown != content || got.ReplyTarget.MessageID != "om_root" || !got.ReplyTarget.InThread {
		t.Fatalf("topic reply params = %+v", got)
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
