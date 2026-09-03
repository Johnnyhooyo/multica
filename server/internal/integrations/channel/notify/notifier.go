package notify

import (
	"context"
	"errors"
	"log/slog"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/events"
	"github.com/multica-ai/multica/server/internal/issuestatus"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/protocol"
)

// deliverTimeout bounds one push. The bus dispatches handlers synchronously,
// so an unbounded send here would stall whichever request published the
// notification.
const deliverTimeout = 5 * time.Second

// Queries is what the notifier needs from the database. *db.Queries
// satisfies it.
type Queries interface {
	issuestatus.EntryReader
	FindChannelBindingForMember(ctx context.Context, arg db.FindChannelBindingForMemberParams) (db.ChannelUserBinding, error)
	GetWorkspace(ctx context.Context, id pgtype.UUID) (db.Workspace, error)
	CreateChannelPushMessage(ctx context.Context, arg db.CreateChannelPushMessageParams) (db.ChannelPushMessage, error)
}

// Notifier forwards whitelisted inbox notifications to bound IM channels.
type Notifier struct {
	q        Queries
	logger   *slog.Logger
	adapters map[string]DMDeliverer
}

func New(q Queries, logger *slog.Logger) *Notifier {
	if logger == nil {
		logger = slog.Default()
	}
	return &Notifier{q: q, logger: logger, adapters: map[string]DMDeliverer{}}
}

// Register installs the per-channel_type adapters. Channels with no adapter
// are silently skipped.
func (n *Notifier) Register(adapters map[string]DMDeliverer) {
	for k, v := range adapters {
		n.adapters[k] = v
	}
}

func (n *Notifier) Subscribe(bus *events.Bus) {
	bus.Subscribe(protocol.EventInboxNew, n.HandleInboxNew)
}

// HandleInboxNew is the inbox:new subscriber.
//
// Every miss is a no-op, never an error: a non-member recipient, a type the
// whitelist rejects, an unbound member, a channel with no adapter, a send
// that failed. In all of them the member still sees the notification in the
// in-app inbox, which is the degradation WeCom's original path established.
//
// Muting needs no handling here. notifTypeToGroup + isNotifMuted run before
// the inbox row is created, so a muted notification produces no row, no
// EventInboxNew, and therefore no push.
func (n *Notifier) HandleInboxNew(e events.Event) {
	payload, ok := e.Payload.(map[string]any)
	if !ok {
		return
	}
	item, ok := payload["item"].(map[string]any)
	if !ok {
		return
	}
	// Only member recipients. Agents have no IM identity to DM.
	if rt, _ := item["recipient_type"].(string); rt != "member" {
		return
	}
	recipientID, ok := parseItemUUID(item, "recipient_id")
	if !ok {
		return
	}
	workspaceID, ok := parseItemUUID(item, "workspace_id")
	if !ok {
		return
	}
	inboxItemID, _ := parseItemUUID(item, "id")

	ctx, cancel := context.WithTimeout(context.Background(), deliverTimeout)
	defer cancel()

	notifType, _ := item["type"].(string)
	rawStatus, _ := item["issue_status"].(string)
	effective := rawStatus
	if rawStatus != "" {
		effective = issuestatus.Effective(ctx, n.q, workspaceID, rawStatus)
	}
	decision := Decide(notifType, effective)
	if !decision.Push {
		return
	}

	binding, ok := n.findBinding(ctx, workspaceID, recipientID)
	if !ok {
		return
	}
	adapter, ok := n.adapters[binding.ChannelType]
	if !ok {
		return
	}

	slug := ""
	if ws, wsErr := n.q.GetWorkspace(ctx, workspaceID); wsErr == nil {
		slug = ws.Slug
	}
	// A push is only offered as replyable when the notification type allows a
	// reply AND the platform can carry one back. WeCom fails the second half,
	// so its pushes render without the reply hint and rely on the deep link.
	replyable := decision.Replyable && adapter.AcceptsReplies()
	text := renderPush(item, util.UUIDToString(workspaceID), slug, replyable)
	if text == "" {
		return
	}

	ref := PushRef{
		InboxItemID:     util.UUIDToString(inboxItemID),
		RecipientUserID: util.UUIDToString(recipientID),
	}
	res, err := adapter.DeliverDM(ctx, ref, binding, text)
	if err != nil {
		n.logger.WarnContext(ctx, "notify: push failed",
			"error", err, "channel_type", binding.ChannelType,
			"workspace_id", util.UUIDToString(workspaceID))
		return
	}

	// Only a delivered push with a real platform message id can be replied
	// to. StateHandedOff has no id by construction (the relay is one-way),
	// and some platforms deliver without returning one at all.
	if !replyable || res.State != StateDelivered || res.MessageID == "" {
		return
	}
	issueID, _ := parseItemUUID(item, "issue_id")
	if _, err := n.q.CreateChannelPushMessage(ctx, db.CreateChannelPushMessageParams{
		InstallationID:   binding.InstallationID,
		ChannelType:      binding.ChannelType,
		ChannelMessageID: res.MessageID,
		WorkspaceID:      workspaceID,
		RecipientUserID:  recipientID,
		IssueID:          issueID,
		InboxItemID:      inboxItemID,
	}); err != nil && !errors.Is(err, pgx.ErrNoRows) {
		// ErrNoRows is the ON CONFLICT DO NOTHING path: already recorded.
		n.logger.WarnContext(ctx, "notify: recording the push for reply attribution failed",
			"error", err, "channel_type", binding.ChannelType)
	}
}

// findBinding picks which channel to DM and returns the binding it found.
// FindChannelBindingForMember takes one channel_type, so the notifier tries
// the registered adapters in a fixed order and uses the first bound one.
// Only one message is ever sent.
func (n *Notifier) findBinding(ctx context.Context, workspaceID, recipientID pgtype.UUID) (db.ChannelUserBinding, bool) {
	for _, ct := range n.channelOrder() {
		binding, err := n.q.FindChannelBindingForMember(ctx, db.FindChannelBindingForMemberParams{
			WorkspaceID: workspaceID, MulticaUserID: recipientID, ChannelType: ct,
		})
		if err == nil {
			return binding, true
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			n.logger.WarnContext(ctx, "notify: member binding lookup failed",
				"error", err, "workspace_id", util.UUIDToString(workspaceID))
		}
	}
	return db.ChannelUserBinding{}, false
}

// channelOrder returns the registered adapter keys in a deterministic order,
// so two replicas make the same choice for a member bound to two platforms.
func (n *Notifier) channelOrder() []string {
	order := make([]string, 0, len(n.adapters))
	for ct := range n.adapters {
		order = append(order, ct)
	}
	sort.Strings(order)
	return order
}

// itemString reads a string field from an inbox_item map. Nullable columns
// reach the bus as *string, because inboxItemToResponse builds them with
// util.UUIDToPtr / util.TextToPtr (cmd/server/notification_listeners.go:1026),
// so both shapes have to be accepted — asserting only string silently drops
// every nullable field.
func itemString(item map[string]any, key string) string {
	switch v := item[key].(type) {
	case string:
		return v
	case *string:
		if v != nil {
			return *v
		}
	}
	return ""
}

func parseItemUUID(item map[string]any, key string) (pgtype.UUID, bool) {
	s := itemString(item, key)
	if s == "" {
		return pgtype.UUID{}, false
	}
	u, err := util.ParseUUID(s)
	if err != nil || !u.Valid {
		return pgtype.UUID{}, false
	}
	return u, true
}

// pushAppURL resolves the frontend URL for building the deep link. Precedence
// is WECOM_APP_URL, then MULTICA_APP_URL, then FRONTEND_ORIGIN — WeCom's
// per-tenant override stays first because it was the only push channel before
// this package existed and its deployments already set it. Only HTTPS values
// are accepted; a non-HTTPS override is silently dropped so a misconfigured
// env cannot leak an http:// URL into a user chat.
func pushAppURL() string {
	for _, name := range []string{"WECOM_APP_URL", "MULTICA_APP_URL", "FRONTEND_ORIGIN"} {
		v := strings.TrimSpace(os.Getenv(name))
		if v == "" {
			continue
		}
		if !strings.HasPrefix(v, "https://") {
			continue
		}
		return strings.TrimRight(v, "/")
	}
	return ""
}

// pushItemIssueID extracts issue_id when present. Chat-only notifications
// (quick_create_failed, quick_create_unconfirmed) have no issue_id.
func pushItemIssueID(item map[string]any) string {
	return itemString(item, "issue_id")
}

// pushItemBody extracts the body/description string from an inbox_item map.
// Body may arrive as *string (nil-able JSON field), string, or missing.
func pushItemBody(item map[string]any) string {
	return itemString(item, "body")
}

// pushLink builds the deep link: <app base>/<slug or workspace uuid>/issues/<issue_id>
// when there is an issue to link to, or .../<slug or workspace uuid>/inbox
// otherwise. Returns "" when no app URL is configured — the caller then
// drops the whole link segment rather than sending a broken one.
func pushLink(item map[string]any, workspaceID, slug string) string {
	appURL := pushAppURL()
	if appURL == "" {
		return ""
	}
	seg := slug
	if seg == "" {
		seg = workspaceID
	}
	var b strings.Builder
	b.WriteString(appURL)
	b.WriteString("/")
	b.WriteString(url.PathEscape(seg))
	if issueID := pushItemIssueID(item); issueID != "" {
		b.WriteString("/issues/")
		b.WriteString(url.PathEscape(issueID))
	} else {
		b.WriteString("/inbox")
	}
	return b.String()
}

// pushTypeLabel is the Chinese display label used in the pushed message's
// preamble.
var pushTypeLabels = map[string]string{
	"status_changed":           "状态变更",
	"task_failed":              "task 失败",
	"quick_create_failed":      "快速创建失败",
	"quick_create_unconfirmed": "快速创建待确认",
}

func pushTypeLabel(t string) string {
	if label, ok := pushTypeLabels[t]; ok {
		return label
	}
	return "新消息"
}

// replyHint is appended to a replyable push so the recipient knows they can
// act on it without leaving IM.
const replyHint = "直接回复本条消息即可处理。"

// FitPush trims a rendered push to at most maxRunes runes without dropping the
// parts the recipient acts on. It is the counterpart to renderPush: the body is
// the only unbounded segment, so the body is what gets cut.
//
// A plain tail cut would take the deep link and the reply hint instead, which
// is worst exactly where it hurts most — a platform whose pushes are not
// replyable has the deep link as the recipient's only route to the
// notification, so a long body would leave them a message they can neither
// reply to nor click through.
//
// Adapters call this after any per-platform escaping, never before: escaping
// grows the text, and a budget measured before it is not a budget.
//
// It lives beside renderPush because it reads that function's layout back. The
// tail is read off renderPush's grammar, which permits at most one bare-link
// line followed by at most one reply hint, at the very end — never more. A
// looser match that just kept taking trailing link-shaped lines would walk into
// the body: a member whose comment ends in a bare URL would have that line
// preserved whole in front of the real deep link, and at adversarial length it
// would crowd the real link out of the budget, which is the failure this
// function exists to prevent.
func FitPush(text string, maxRunes int) string {
	if maxRunes <= 0 {
		return ""
	}
	if utf8.RuneCountInString(text) <= maxRunes {
		return text
	}
	lines := strings.Split(text, "\n")
	head, rest := lines[0], lines[1:]
	var tail []string
	if len(rest) > 0 && rest[len(rest)-1] == replyHint {
		tail = append(tail, rest[len(rest)-1])
		rest = rest[:len(rest)-1]
	}
	if len(rest) > 0 && isBarePushLink(rest[len(rest)-1]) {
		tail = append([]string{rest[len(rest)-1]}, tail...)
		rest = rest[:len(rest)-1]
	}
	tailText := ""
	for _, l := range tail {
		tailText += "\n" + l
	}
	tailRunes := utf8.RuneCountInString(tailText)

	// One rune of ellipsis marks every cut, so the recipient can tell a
	// truncated push from a short one.
	const ellipsis = "…"

	// The question is whether the tail fits at all — alongside one rune of title
	// and the cut marker — not whether it fits beside the whole title. An
	// over-long title is the fallback's job below, which cuts the title and keeps
	// the tail; measuring against the untruncated head here would throw away a
	// link that had room, which is the failure this function exists to prevent.
	//
	// The +2 is exactly what the fallback needs: it cuts the head to
	// maxRunes-tailRunes-1, so one rune of title survives iff tailRunes+2 fits.
	// One rune is not much of a title — renderPush's head opens with "**[", so
	// the first survivor is punctuation — but a budget that tight is smaller than
	// the deep link plus two, which no configured app URL comes near. Below it
	// there is no push worth shaping, and the title is the half that at least
	// says a notification happened.
	if tailRunes+2 > maxRunes {
		return capRunes(truncateRunes(head, maxRunes-1)+ellipsis, maxRunes)
	}

	if body := strings.Join(rest, "\n"); body != "" {
		room := maxRunes - utf8.RuneCountInString(head) - tailRunes - 2 // "\n" + ellipsis
		if room > 0 {
			return capRunes(head+"\n"+truncateRunes(body, room)+ellipsis+tailText, maxRunes)
		}
	}
	// No room for any body means the title alone is over budget.
	return capRunes(truncateRunes(head, maxRunes-tailRunes-1)+ellipsis+tailText, maxRunes)
}

// capRunes is the unconditional guarantee FitPush's callers need: whatever the
// reasoning above produced, what comes back fits.
//
// No branch above currently needs it — each one lands at or under the budget on
// its own arithmetic. It stays because that arithmetic is subtle enough to have
// been wrong three times in review, and the failure it backstops is silent: a
// platform that refuses an over-long frame whole while acking the send loses the
// push with no error anywhere to notice it by.
func capRunes(s string, maxRunes int) string {
	if utf8.RuneCountInString(s) <= maxRunes {
		return s
	}
	return truncateRunes(s, maxRunes)
}

func isBarePushLink(line string) bool {
	return strings.HasPrefix(line, "https://") && !strings.ContainsAny(line, " \t")
}

// truncateRunes trims s to at most maxRunes runes. Rune-based rather than
// byte-based so a cut never splits a Chinese character.
//
// It only ever drops a suffix. That is what lets an adapter run it after a
// markdown-escaping pass without undoing it: dropping characters cannot put a
// "]" back beside a "(" or a ":".
func truncateRunes(s string, maxRunes int) string {
	if maxRunes <= 0 {
		return ""
	}
	if utf8.RuneCountInString(s) <= maxRunes {
		return s
	}
	i := 0
	for pos := range s {
		if i == maxRunes {
			return s[:pos]
		}
		i++
	}
	return s
}

// renderPush builds the pushed message from an inbox_item payload. It is
// platform-neutral: per-platform escaping and length budgets belong to the
// adapter, which applies them to this output (see FitPush).
//
// Format:
//
//	**[{label}] {title}**
//	{body}
//	{deep link}
//	{reply hint, when replyable}
//
// Returns "" when both title and type are empty — nothing worth sending.
func renderPush(item map[string]any, workspaceID, slug string, replyable bool) string {
	title, _ := item["title"].(string)
	typeStr, _ := item["type"].(string)
	if title == "" && typeStr == "" {
		return ""
	}
	body := pushItemBody(item)
	link := pushLink(item, workspaceID, slug)

	var b strings.Builder
	b.WriteString("**[")
	b.WriteString(pushTypeLabel(typeStr))
	b.WriteString("] ")
	b.WriteString(title)
	b.WriteString("**")
	if body != "" {
		b.WriteString("\n")
		b.WriteString(body)
	}
	if link != "" {
		b.WriteString("\n")
		b.WriteString(link)
	}
	if replyable {
		b.WriteString("\n")
		b.WriteString(replyHint)
	}
	return b.String()
}
