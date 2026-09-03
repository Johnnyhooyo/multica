package notify

import (
	"strings"
	"testing"
	"unicode/utf8"
)

// FitPush and renderPush are two halves of one format: this reads the real
// renderer's output back and asserts the parts the recipient acts on survive a
// body long enough to blow any platform's budget. A plain tail cut passes a
// total-length assertion and fails this one.
func TestFitPushKeepsWhatTheRecipientActsOn(t *testing.T) {
	t.Setenv("MULTICA_APP_URL", "https://app.example.com")
	issueID := "33333333-3333-3333-3333-333333333333"
	item := map[string]any{
		"type":     "status_changed",
		"title":    "Ship the thing",
		"issue_id": &issueID,
		"body":     strings.Repeat("蒜", 5000),
	}
	for _, replyable := range []bool{true, false} {
		text := renderPush(item, testWorkspace, "acme", replyable)
		link := pushLink(item, testWorkspace, "acme")
		if link == "" {
			t.Fatal("pushLink returned empty; the app URL fixture is not taking effect")
		}

		const max = 4000
		got := FitPush(text, max)
		if n := utf8.RuneCountInString(got); n > max {
			t.Errorf("replyable=%v: %d runes, want at most %d", replyable, n, max)
		}
		if !strings.Contains(got, "Ship the thing") {
			t.Errorf("replyable=%v: dropped the title: %q", replyable, first(got))
		}
		if !strings.Contains(got, link) {
			t.Errorf("replyable=%v: dropped the deep link", replyable)
		}
		if strings.Contains(got, replyHint) != replyable {
			t.Errorf("replyable=%v: reply hint presence = %v", replyable, !replyable)
		}
		if !strings.Contains(got, "…") {
			t.Errorf("replyable=%v: a truncated push carries no ellipsis", replyable)
		}
	}
}

// A push already inside the budget must come back byte-identical — no stray
// ellipsis, no reflowed tail.
func TestFitPushLeavesAShortPushAlone(t *testing.T) {
	text := "**[状态变更] Ship it**\nbody\nhttps://app.example.com/acme/issues/x\n" + replyHint
	if got := FitPush(text, 4000); got != text {
		t.Errorf("FitPush rewrote a push that already fits:\n got %q\nwant %q", got, text)
	}
}

// The degenerate case: a title alone over budget. There is no body to spend, so
// the title is what gets cut, and the guarantee still has to hold.
func TestFitPushCapsATitleThatAloneExceedsTheBudget(t *testing.T) {
	text := "**[状态变更] " + strings.Repeat("蒜", 500) + "**\nhttps://app.example.com/a/issues/x"
	got := FitPush(text, 100)
	if n := utf8.RuneCountInString(got); n > 100 {
		t.Errorf("%d runes, want at most 100", n)
	}
	if got == "" {
		t.Error("FitPush returned nothing for an over-long title")
	}
}

func TestFitPushRejectsANonPositiveBudget(t *testing.T) {
	if got := FitPush("anything", 0); got != "" {
		t.Errorf("FitPush(_, 0) = %q, want empty", got)
	}
}

func TestTruncateRunes(t *testing.T) {
	cases := []struct {
		in     string
		max    int
		expect string
	}{
		{"abc", 0, ""},
		{"abc", 3, "abc"},
		{"abc", 2, "ab"},
		{"你好世界", 2, "你好"},
		{"你好世界", 4, "你好世界"},
		{"你好世界", 5, "你好世界"},
	}
	for _, tc := range cases {
		if got := truncateRunes(tc.in, tc.max); got != tc.expect {
			t.Errorf("truncateRunes(%q,%d)=%q; want %q", tc.in, tc.max, got, tc.expect)
		}
	}
}

func first(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}
