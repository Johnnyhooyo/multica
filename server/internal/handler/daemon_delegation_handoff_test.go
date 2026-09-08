package handler

import (
	"testing"

	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/protocol"
)

func TestSplitDelegationHandoffInputKeepsMemberAndSystemContentSeparate(t *testing.T) {
	messages := []db.ChatMessage{
		{Role: "user", Content: "first result", MessageKind: protocol.ChatMessageKindDelegationHandoff},
		{Role: "user", Content: "我的答案", MessageKind: protocol.ChatMessageKindMessage},
		{Role: "user", Content: "second result", MessageKind: protocol.ChatMessageKindDelegationHandoff},
	}

	visible, handoff := splitDelegationHandoffInput(messages, "first result")
	if len(visible) != 1 || visible[0].Content != "我的答案" {
		t.Fatalf("visible input = %#v, want only the member message", visible)
	}
	if handoff != "first result\n\nsecond result" {
		t.Fatalf("handoff = %q", handoff)
	}
}
