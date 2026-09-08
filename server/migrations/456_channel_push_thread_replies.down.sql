ALTER TABLE channel_push_message
    DROP COLUMN IF EXISTS relayed_agent_comment_ids,
    DROP COLUMN IF EXISTS reply_comment_ids;
