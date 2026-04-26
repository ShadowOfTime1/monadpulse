-- MonadPulse — Governance schema v2
-- Adds per-post edit history (Discourse revisions). Lets the timeline render
-- real edit events with proper timestamps + diffs instead of "5h ago" mass
-- labels driven by initial scrape time.
-- Apply: psql "$DATABASE_URL" -f scripts/migrate_governance_v2.sql

BEGIN;

CREATE TABLE IF NOT EXISTS mip_post_revisions (
    id                  SERIAL      PRIMARY KEY,
    post_id             INT         NOT NULL REFERENCES mip_posts(id) ON DELETE CASCADE,
    topic_id            INT         NOT NULL REFERENCES mip_topics(id) ON DELETE CASCADE,
    revision_number     INT         NOT NULL,           -- Discourse current_revision (>= 2)
    editor_username     TEXT,                            -- acting_user_name (who actually edited)
    author_username     TEXT,                            -- display_username (who owns the post)
    edited_at           TIMESTAMPTZ NOT NULL,            -- revision created_at — REAL forum time
    edit_reason         TEXT,
    body_changes_inline TEXT,                            -- raw inline-diff HTML from Discourse
    summary             TEXT,                            -- compact text summary of the change
    fetched_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (post_id, revision_number)
);
CREATE INDEX IF NOT EXISTS idx_mip_revisions_topic_time
    ON mip_post_revisions(topic_id, edited_at);
CREATE INDEX IF NOT EXISTS idx_mip_revisions_post
    ON mip_post_revisions(post_id, revision_number);

COMMIT;
