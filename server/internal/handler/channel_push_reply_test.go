package handler

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/testutil"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// The happy path: the person the push was addressed to replies, and the reply
// becomes a member-authored comment on the issue the push was about.
func TestPostPushReplyCommentCreatesAMemberComment(t *testing.T) {
	ctx := context.Background()
	wsID := dbfx.Workspace(t, "Push Reply", "push-reply-"+uuid.NewString())
	userID := dbfx.User(t, "Push Reply User", "push-reply-"+uuid.NewString()+"@multica.ai")
	dbfx.Member(t, wsID, userID, "member")
	issueID := dbfx.Issue(t, "Push reply issue", testutil.Cols{
		"workspace_id": wsID, "status": "in_review",
	})

	push := db.ChannelPushMessage{
		WorkspaceID:     parseUUID(wsID),
		RecipientUserID: parseUUID(userID),
		IssueID:         parseUUID(issueID),
	}

	res, err := testHandler.PostPushReplyComment(ctx, push, parseUUID(userID), "确认审核")
	if err != nil {
		t.Fatalf("PostPushReplyComment: %v", err)
	}
	if !res.Posted {
		t.Fatalf("Posted = false, message = %q", res.Message)
	}

	comments := listPushReplyTestComments(t, issueID, wsID)
	if len(comments) != 1 {
		t.Fatalf("got %d comments, want 1", len(comments))
	}
	if comments[0].AuthorType != "member" {
		t.Errorf("AuthorType = %q, want member", comments[0].AuthorType)
	}
	// The comment must be attributed to the human who replied — an agent
	// woken by it inherits this identity as the originator for its own
	// invocation checks.
	if uuidToString(comments[0].AuthorID) != userID {
		t.Errorf("AuthorID = %v, want the replying user", comments[0].AuthorID)
	}
	if comments[0].Content != "确认审核" {
		t.Errorf("Content = %q", comments[0].Content)
	}
	// "comment", never a machine type: this is a human speaking.
	if comments[0].Type != "comment" {
		t.Errorf("Type = %q, want comment", comments[0].Type)
	}
}

// Someone else in the same workspace replying to a push addressed to another
// person must not be able to speak as them.
func TestPostPushReplyCommentRejectsAnotherUser(t *testing.T) {
	ctx := context.Background()
	wsID := dbfx.Workspace(t, "Push Reply Other User", "push-reply-other-"+uuid.NewString())
	ownerID := dbfx.User(t, "Push Reply Owner", "push-reply-owner-"+uuid.NewString()+"@multica.ai")
	otherID := dbfx.User(t, "Push Reply Bystander", "push-reply-other-"+uuid.NewString()+"@multica.ai")
	dbfx.Member(t, wsID, ownerID, "member")
	dbfx.Member(t, wsID, otherID, "member")
	issueID := dbfx.Issue(t, "Push reply issue (other user)", testutil.Cols{"workspace_id": wsID})

	push := db.ChannelPushMessage{
		WorkspaceID:     parseUUID(wsID),
		RecipientUserID: parseUUID(ownerID),
		IssueID:         parseUUID(issueID),
	}

	res, err := testHandler.PostPushReplyComment(ctx, push, parseUUID(otherID), "确认审核")
	if err != nil {
		t.Fatalf("PostPushReplyComment: %v", err)
	}
	if res.Posted {
		t.Fatal("Posted = true; a non-recipient must not be able to reply")
	}
	if res.Message == "" {
		t.Error("denial carried no message; the user would see silence")
	}
	assertNoPushReplyComments(t, issueID, wsID)
}

// A ledger row outlives membership. Removal from the workspace must revoke
// the reply path even though the row still points at the issue.
func TestPostPushReplyCommentRejectsANonMember(t *testing.T) {
	ctx := context.Background()
	wsID := dbfx.Workspace(t, "Push Reply Non Member", "push-reply-nonmember-"+uuid.NewString())
	userID := dbfx.User(t, "Push Reply Departed User", "push-reply-nonmember-"+uuid.NewString()+"@multica.ai")
	issueID := dbfx.Issue(t, "Push reply issue (non member)", testutil.Cols{"workspace_id": wsID})
	// deliberately no dbfx.Member

	push := db.ChannelPushMessage{
		WorkspaceID:     parseUUID(wsID),
		RecipientUserID: parseUUID(userID),
		IssueID:         parseUUID(issueID),
	}

	res, _ := testHandler.PostPushReplyComment(ctx, push, parseUUID(userID), "确认审核")
	if res.Posted {
		t.Fatal("Posted = true for a user who is no longer a member")
	}
	assertNoPushReplyComments(t, issueID, wsID)
}

// quick_create_failed pushes have no issue, so there is nowhere to inject.
// The user gets told rather than ignored.
func TestPostPushReplyCommentWithoutAnIssueExplainsItself(t *testing.T) {
	ctx := context.Background()
	wsID := dbfx.Workspace(t, "Push Reply No Issue", "push-reply-noissue-"+uuid.NewString())
	userID := dbfx.User(t, "Push Reply No Issue User", "push-reply-noissue-"+uuid.NewString()+"@multica.ai")
	dbfx.Member(t, wsID, userID, "member")

	push := db.ChannelPushMessage{
		WorkspaceID:     parseUUID(wsID),
		RecipientUserID: parseUUID(userID),
		IssueID:         pgtype.UUID{}, // NULL
	}

	res, err := testHandler.PostPushReplyComment(ctx, push, parseUUID(userID), "确认审核")
	if err != nil {
		t.Fatalf("PostPushReplyComment: %v", err)
	}
	if res.Posted {
		t.Fatal("Posted = true with no issue to post to")
	}
	if res.Message == "" {
		t.Error("no explanation for an unreplyable push")
	}
}

// An issue deleted between push and reply must not 500 the inbound pipeline.
func TestPostPushReplyCommentOnAMissingIssue(t *testing.T) {
	ctx := context.Background()
	wsID := dbfx.Workspace(t, "Push Reply Missing Issue", "push-reply-missing-"+uuid.NewString())
	userID := dbfx.User(t, "Push Reply Missing Issue User", "push-reply-missing-"+uuid.NewString()+"@multica.ai")
	dbfx.Member(t, wsID, userID, "member")

	push := db.ChannelPushMessage{
		WorkspaceID:     parseUUID(wsID),
		RecipientUserID: parseUUID(userID),
		IssueID:         parseUUID("00000000-0000-7000-8000-000000000001"),
	}

	res, err := testHandler.PostPushReplyComment(ctx, push, parseUUID(userID), "确认审核")
	if err != nil {
		t.Fatalf("PostPushReplyComment returned err %v; want a denial result", err)
	}
	if res.Posted {
		t.Fatal("Posted = true for a missing issue")
	}
}

// Empty content (an IM sticker or an image-only reply) is not a decision.
func TestPostPushReplyCommentRejectsEmptyContent(t *testing.T) {
	ctx := context.Background()
	wsID := dbfx.Workspace(t, "Push Reply Empty Content", "push-reply-empty-"+uuid.NewString())
	userID := dbfx.User(t, "Push Reply Empty Content User", "push-reply-empty-"+uuid.NewString()+"@multica.ai")
	dbfx.Member(t, wsID, userID, "member")
	issueID := dbfx.Issue(t, "Push reply issue (empty content)", testutil.Cols{"workspace_id": wsID})

	push := db.ChannelPushMessage{
		WorkspaceID:     parseUUID(wsID),
		RecipientUserID: parseUUID(userID),
		IssueID:         parseUUID(issueID),
	}

	res, _ := testHandler.PostPushReplyComment(ctx, push, parseUUID(userID), "   ")
	if res.Posted {
		t.Fatal("Posted = true for whitespace-only content")
	}
	assertNoPushReplyComments(t, issueID, wsID)
}

func listPushReplyTestComments(t *testing.T, issueID, workspaceID string) []db.Comment {
	t.Helper()
	comments, err := testHandler.Queries.ListCommentsForIssue(context.Background(), db.ListCommentsForIssueParams{
		IssueID:     parseUUID(issueID),
		WorkspaceID: parseUUID(workspaceID),
		Limit:       10,
	})
	if err != nil {
		t.Fatalf("ListCommentsForIssue: %v", err)
	}
	return comments
}

func assertNoPushReplyComments(t *testing.T, issueID, workspaceID string) {
	t.Helper()
	comments := listPushReplyTestComments(t, issueID, workspaceID)
	if len(comments) != 0 {
		t.Fatalf("got %d comments, want none — a denied reply must not write", len(comments))
	}
}
