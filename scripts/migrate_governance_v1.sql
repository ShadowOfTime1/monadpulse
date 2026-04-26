-- MonadPulse — Governance schema v1
-- Tables for tracking MIPs (Monad Improvement Proposals) from forum.monad.xyz
-- Apply: psql "$DATABASE_URL" -f scripts/migrate_governance_v1.sql

BEGIN;

-- One row per Discourse topic in the MIPs category. Topic = the proposal itself.
CREATE TABLE IF NOT EXISTS mip_topics (
    id              INT         PRIMARY KEY,         -- forum topic_id (e.g. 416 for MIP-9)
    mip_number      INT,                             -- parsed from title; NULL for non-numbered topics
    slug            TEXT        NOT NULL,
    title           TEXT        NOT NULL,
    category        TEXT,                            -- 'Core Protocol' / 'Contracts' / 'Informational' / 'Meta'
    status          TEXT        NOT NULL DEFAULT 'Draft',
    author_username TEXT,
    author_id       INT,
    forum_created_at  TIMESTAMPTZ NOT NULL,          -- when topic was created on forum
    forum_updated_at  TIMESTAMPTZ NOT NULL,          -- forum's last_posted_at / bumped_at
    views           INT         DEFAULT 0,
    reply_count     INT         DEFAULT 0,
    tags            TEXT[]      DEFAULT '{}',        -- forum tags (used for status detection)
    archetype       TEXT,                            -- 'regular' / 'private_message' / etc.
    pinned          BOOLEAN     DEFAULT FALSE,
    closed          BOOLEAN     DEFAULT FALSE,
    last_scraped_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_mip_topics_status   ON mip_topics(status);
CREATE INDEX IF NOT EXISTS idx_mip_topics_updated  ON mip_topics(forum_updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_mip_topics_mipnum   ON mip_topics(mip_number);

-- One row per post in any tracked topic. post_number=1 is the OP (proposal body).
-- Edits update the row; we keep a hash of cooked_html to detect changes cheaply.
CREATE TABLE IF NOT EXISTS mip_posts (
    id                INT         PRIMARY KEY,       -- forum post_id
    topic_id          INT         NOT NULL REFERENCES mip_topics(id) ON DELETE CASCADE,
    post_number       INT         NOT NULL,          -- 1 = OP, 2..N = replies
    username          TEXT,
    cooked_html       TEXT,                          -- rendered HTML body (forum's "cooked")
    raw_markdown      TEXT,                          -- markdown source (for diffs)
    raw_hash          TEXT,                          -- sha256(raw_markdown), for fast change detection
    forum_created_at  TIMESTAMPTZ NOT NULL,
    forum_updated_at  TIMESTAMPTZ NOT NULL,
    version           INT         DEFAULT 1,         -- forum's edit version counter
    reply_to_post_number INT,                        -- threading: which post this replies to
    last_scraped_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_mip_posts_topic    ON mip_posts(topic_id, post_number);

-- Append-only log of detected changes. Powers the timeline section + Telegram alerts.
CREATE TABLE IF NOT EXISTS mip_changes (
    id              SERIAL      PRIMARY KEY,
    topic_id        INT         NOT NULL REFERENCES mip_topics(id) ON DELETE CASCADE,
    post_id         INT         REFERENCES mip_posts(id) ON DELETE SET NULL,
    change_type     TEXT        NOT NULL,            -- 'topic_created'|'op_edited'|'reply_added'|'status_changed'|'tag_changed'
    old_value       TEXT,                            -- prev status / prev hash / prev tag-list
    new_value       TEXT,
    detail          JSONB,                           -- diff, post snippet, etc.
    detected_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    notified_at     TIMESTAMPTZ                      -- set when Telegram alert fired
);
CREATE INDEX IF NOT EXISTS idx_mip_changes_topic    ON mip_changes(topic_id, detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_mip_changes_pending  ON mip_changes(notified_at) WHERE notified_at IS NULL;

-- LLM-generated plain-language summary + per-stakeholder impact analysis.
-- One row per topic; regenerated only when source_hash (OP markdown hash) changes.
CREATE TABLE IF NOT EXISTS mip_summaries (
    topic_id          INT         PRIMARY KEY REFERENCES mip_topics(id) ON DELETE CASCADE,
    summary           TEXT        NOT NULL,          -- 2-3 paragraphs plain English
    validator_impact  JSONB       NOT NULL,          -- array of bullet strings
    delegator_impact  JSONB       NOT NULL,
    builder_impact    JSONB       NOT NULL,
    source_hash       TEXT        NOT NULL,          -- sha256 of OP markdown that produced this
    model             TEXT        NOT NULL,          -- 'claude-opus-4-7-1m' etc.
    generated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMIT;
