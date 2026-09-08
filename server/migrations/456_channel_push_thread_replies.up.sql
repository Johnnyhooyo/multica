-- Keep the causal edge from a member's reply to the push topic and the
-- agent comments already relayed back into that topic. Arrays are bounded by
-- the seven-day channel_push_message retention window.
ALTER TABLE channel_push_message
    ADD COLUMN reply_comment_ids UUID[] NOT NULL DEFAULT '{}',
    ADD COLUMN relayed_agent_comment_ids UUID[] NOT NULL DEFAULT '{}';
