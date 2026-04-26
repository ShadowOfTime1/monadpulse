-- MonadPulse — Governance schema v3
-- Adds provenance + freshness tracking to mip_summaries.
-- context_validator_ids — which val_ids were in the LLM's grounding context
-- when this summary was generated. Lets us later filter MIPs that affect a
-- specific operator (e.g. "MIPs that named me by rank in the impact bullets").
-- Apply: psql "$DATABASE_URL" -f scripts/migrate_governance_v3.sql

BEGIN;

ALTER TABLE mip_summaries
    ADD COLUMN IF NOT EXISTS context_validator_ids INT[] DEFAULT '{}';

COMMIT;
