package handler

import (
	"context"
	"errors"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/integrations/channel/engine"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/dbid"
	"github.com/multica-ai/multica/server/pkg/protocol"
)

// PostPushReplyComment turns a reply-to-a-push into an issue comment.
//
// It is the whole reply half of the IM decision loop. Nothing here advances a
// status: the comment wakes the issue's agent through the ordinary comment
// trigger, and the agent decides what the reply meant and does its own
// follow-on work. That indirection is the point — approval may close a final
// review, advance the next dependency-gated stage, or authorize other work.
// A hard-coded status write cannot distinguish those outcomes.
//
// Modeled on TaskService.createAgentComment, the other non-HTTP comment path.
func (h *Handler) PostPushReplyComment(
	ctx context.Context,
	push db.ChannelPushMessage,
	senderUserID pgtype.UUID,
	content string,
) (engine.PushReplyResult, error) {
	content, denial, ok := engine.PushReplyPrecondition(
		uuidToString(push.RecipientUserID), uuidToString(senderUserID), content)
	if !ok {
		return denial, nil
	}

	// A ledger row outlives membership; re-check rather than trusting it. Only
	// "no such member" is a denial — a connection reset is a fault the sender
	// cannot act on, and answering it with "你没有权限" would tell a legitimate
	// member they had lost access and drop their reply with nothing to retry.
	if _, err := h.Queries.GetMemberByUserAndWorkspace(ctx, db.GetMemberByUserAndWorkspaceParams{
		UserID:      senderUserID,
		WorkspaceID: push.WorkspaceID,
	}); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return engine.PushReplyResult{Message: engine.PushReplyDenied}, nil
		}
		return engine.PushReplyResult{}, err
	}

	// quick_create pushes carry no issue: there is no thread to inject into.
	if !push.IssueID.Valid {
		return engine.PushReplyResult{Message: "这条推送不能直接回复决策，请打开 Multica 处理。"}, nil
	}

	issue, err := h.Queries.GetIssueInWorkspace(ctx, db.GetIssueInWorkspaceParams{
		ID:          push.IssueID,
		WorkspaceID: push.WorkspaceID,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return engine.PushReplyResult{Message: "关联的 issue 已不存在。"}, nil
		}
		return engine.PushReplyResult{}, err
	}

	created, err := h.Queries.CreateComment(ctx, db.CreateCommentParams{
		ID:          dbid.NewV7(),
		IssueID:     issue.ID,
		WorkspaceID: issue.WorkspaceID,
		AuthorType:  "member",
		AuthorID:    senderUserID,
		Content:     content,
		Type:        "comment",
	})
	if err != nil {
		return engine.PushReplyResult{}, err
	}
	comment := created.Comment()

	actorID := uuidToString(senderUserID)
	resp := commentToResponse(comment, nil, nil)
	resp.IssueRevision = created.IssueRevision
	h.publish(protocol.EventCommentCreated, uuidToString(issue.WorkspaceID), "member", actorID, map[string]any{
		"comment":             resp,
		"issue_title":         issue.Title,
		"issue_assignee_type": textToPtr(issue.AssigneeType),
		"issue_assignee_id":   uuidToPtr(issue.AssigneeID),
		"issue_status":        issue.Status,
		"issue_revision":      created.IssueRevision,
	})

	// The wake. originatorUserID is the replying human, so the agent this
	// starts inherits exactly that person's invocation authority — the same
	// value CreateComment passes for a member author
	// (invokeOriginatorFromRequest returns actorID unchanged for "member").
	//
	// Two recipients each approving produces two comments; the second is
	// folded into the pending task by mergeCommentIntoPendingTask rather than
	// starting a second run. Do not add a second guard here.
	h.triggerTasksForComment(ctx, issue, comment, nil, "member", actorID, actorID, nil)

	slog.Info("push reply posted as comment",
		"issue_id", uuidToString(issue.ID),
		"comment_id", uuidToString(comment.ID),
		"channel_type", push.ChannelType,
	)
	return engine.PushReplyResult{Posted: true, Message: "已记录审核意见，Multica 会结合任务上下文继续处理。"}, nil
}

// LookupPush finds the push a reply is answering. A miss is not an error:
// nearly every inbound reply is ordinary chat, and the router uses the bool
// to fall through to that path.
func (h *Handler) LookupPush(ctx context.Context, installationID pgtype.UUID, channelMessageID string) (db.ChannelPushMessage, bool, error) {
	if channelMessageID == "" {
		return db.ChannelPushMessage{}, false, nil
	}
	row, err := h.Queries.FindChannelPushMessage(ctx, db.FindChannelPushMessageParams{
		InstallationID:   installationID,
		ChannelMessageID: channelMessageID,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return db.ChannelPushMessage{}, false, nil
	}
	if err != nil {
		return db.ChannelPushMessage{}, false, err
	}
	return row, true, nil
}

// Compile-time assertion: a future signature drift on either side fails the
// build rather than silently disabling the feature.
var _ engine.PushReplyPoster = (*Handler)(nil)
