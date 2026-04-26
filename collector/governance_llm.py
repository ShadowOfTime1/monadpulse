"""MonadPulse — Governance LLM analyser.

Generates plain-language summary + per-stakeholder impact analysis for one
MIP via the Anthropic Messages API. Results cached in mip_summaries; only
regenerates when the OP markdown's hash changes.

Graceful degradation: if ANTHROPIC_API_KEY is not set, this module is a
no-op — collector keeps running, governance pages render without the
AI section. The API endpoint and frontend already handle a missing
summary cleanly.

On-chain context injection: for MIPs whose impact depends on chain state
(e.g. MIP-9 expanding the active set — we want the summary to know
"there are currently 200 validators and the proposal would lift the cap
to 300"), MIP_CONTEXT_HOOKS maps mip_number → async callable returning
a dict that's appended to the prompt.
"""
from __future__ import annotations

import hashlib
import json as _json
import logging
import os
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable

import httpx

log = logging.getLogger("monadpulse.governance_llm")

ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages"
MODEL = "claude-opus-4-5"  # fast, capable; falls back gracefully if Anthropic deprecates
ANTHROPIC_VERSION = "2023-06-01"
MAX_OP_CHARS = 18000  # safe upper bound for a single MIP body
SYSTEM_PROMPT = """You explain Monad blockchain governance proposals (MIPs) to non-technical stakeholders.

You output JSON ONLY, no other text, with these exact keys:
- summary: 2-3 paragraphs of plain English. Explain what the MIP actually does and why. No jargon unless absolutely necessary; if you must use a term, define it inline.
- validator_impact: array of 3-5 short bullet strings, each in second person ("you"), explaining concrete effects on validators (block rewards, operations, competitive position, active-set status, commission strategy).
- delegator_impact: array of 3-5 short bullet strings, second person, explaining effects on delegators (rewards, where to delegate, risk).
- builder_impact: array of 3-5 short bullet strings, second person, explaining effects on smart-contract developers (breaking changes, gas, migration).
- referenced_validator_ids: array of integers — every val_id from the on-chain context that you actually quoted by name in any of the impact bullets. Empty array if you didn't name any specific validator. This is for provenance only; it does not appear in the rendered text.

CRITICAL DATA INTEGRITY RULE — read carefully, this overrides any other instinct:
The on-chain context contains two arrays of validator records: `validators_by_rank` (the active set) and `edge_validators_outside_active_set`. Each record has the form `{"rank": R, "name": N, "id": I, "stake_mon": S}`. These records are the ONLY source of truth.

The context also exposes `stake_basis: "execution"`. Ranks in the records are computed by sorting validators on execution stake (descending). Active-set membership is technically determined by consensus stake on Monad, but our snapshot only carries execution stake — close enough for current-testnet stakes which cluster tightly.

When mentioning a validator by name you MUST use the EXACT triple (name, rank, stake_mon) from the data. You MUST NOT:
- Modify a validator's rank — never write a different rank than the one in the record.
- Round, estimate, or recalculate rank.
- Cite a rank for a validator that is not in the provided arrays. If you want to mention an operator at rank 75 but no record at rank 75 has a name, say "the validator at rank 75 by execution stake" without naming.
- Use stake_mon values that differ from the record (you may format e.g. 25034117 → "25M" but the source value must come from the record).
- Use any validator name from prior training data — only names appearing verbatim in the records.
- Place a val_id in `referenced_validator_ids` if it is not present as `id` in either array. The post-process strips IDs that fail this check; using only what's in the data avoids your output being silently corrected.

Citation format depends on whether the validator is inside or outside the active set:
- Validators from `validators_by_rank` (in the active set): write "Name (rank R by execution stake, S MON staked)". Example for `{"rank": 4, "name": "Validation Cloud", "stake_mon": 11000267}`: "Validation Cloud (rank 4 by execution stake, 11M MON staked)".
- Validators from `edge_validators_outside_active_set` (outside the active set): write "Name (rank R outside active set, S MON staked)". Don't claim they are in the active set when they aren't. Example for `{"rank": 205, "name": "shadowoftime", "stake_mon": 9000000}`: "shadowoftime (rank 205 outside active set, 9M MON staked)".

Other rules for impact bullets (the validator_impact / delegator_impact / builder_impact arrays):
- When discussing impact, name actual validators across the spectrum: at minimum reference (a) the top of the set (top 5 by stake), (b) typical mid-pack operators sampled across ranks 25-175, and (c) edge cases near the proposal's affected boundary. NEVER write a bullet that talks abstractly about "validators" if specific named records in the context support the point — name them.
- If a bullet truly cannot reference specific entities (because no relevant named record exists for the point you want to make), prefix it with: "Without specific validator data: ".
- If you don't have specific data to support a numeric claim, say so explicitly ("the proposal's effect on the cost of capital is not quantifiable from current chain data") rather than inventing numbers, ranks, or names.
- Be specific. When the on-chain context provides a useful number — current active-set size, stake at a specific rank, total delegator count — quote it from the data.

For the builder_impact section specifically:
- MonadPulse does NOT have data on which deployed contracts read protocol-level state, on contract storage patterns, or on contract gas profiling.
- If a MIP could plausibly affect builders, you MUST include at least one bullet that explicitly states what we do NOT know, prefixed with: "MonadPulse data limitation: ". Example: "MonadPulse data limitation: we cannot tell you which deployed mainnet contracts read validator set membership — you must audit your own contracts for this."
- If a stakeholder is genuinely unaffected by a particular MIP, say so with a single bullet ("This MIP does not directly affect delegators.") instead of padding with generic statements.

Style:
- Do not editorialize about whether the MIP is good or bad. Stay neutral.
- Keep each bullet under 320 characters."""


def _hash_text(s: str | None) -> str:
    return hashlib.sha256((s or "").encode("utf-8")).hexdigest()


# ─── On-chain context builders ─────────────────────────────────────────
# A generic "validator landscape" context is injected for every MIP so the
# LLM can ground impact bullets in real names + numbers. Topic-specific
# context hooks (active-set boundary, commission outliers, etc.) layer on
# top based on heuristics about the proposal's text.

CONTEXT_NETWORK = "mainnet"  # the network whose state we feed to the LLM
# Why mainnet: MIPs are protocol-level governance affecting both networks
# once activated, but mainnet is where real economic stakes are. The
# testnet plateau (197 validators all at 9-25M MON) doesn't show realistic
# active-set pressure or whale concentration, while mainnet (203 validators,
# 1.09B MON at rank 1, ~25M MON threshold for active set, 3 already outside)
# gives concrete impact bullets. See backlog for dual-network context if
# testnet-specific MIPs ever appear.


def _load_validator_directory() -> dict[str, dict]:
    """Return {auth_lower: {val_id, name, shared_auth}} from the directory
    plus override file.

    `shared_auth=True` flags entries where multiple val_ids in the directory
    bind to the same auth address — currently true for the Category Labs
    cluster on testnet (val_ids 8/9/10/12 all under auth 0xfa034…). Such
    entries are excluded from the LLM context and the canonical map until
    the data layer captures per-val_id stakes (validator_stake_history PK
    today is (auth, epoch), so 4 val_ids collapse to 1 stake row and we
    can't honestly attribute per-val_id rank). See backlog: PK migration."""
    raw_entries: list[tuple[str, dict]] = []
    for fname in (
        f"/opt/monadpulse/validator_directory_{CONTEXT_NETWORK}.json",
        f"/opt/monadpulse/validator_directory_override_{CONTEXT_NETWORK}.json",
    ):
        path = Path(fname)
        if not path.exists():
            continue
        try:
            rows = _json.loads(path.read_text())
        except Exception as e:
            log.warning("validator directory load failed (%s): %s", fname, e)
            continue
        for r in rows:
            auth = (r.get("auth") or "").lower()
            if auth:
                raw_entries.append((auth, r))

    # Count how many val_ids each auth is bound to. Anything > 1 = shared.
    auth_count: dict[str, int] = {}
    for auth, _ in raw_entries:
        auth_count[auth] = auth_count.get(auth, 0) + 1

    out: dict[str, dict] = {}
    for auth, r in raw_entries:
        out[auth] = {
            "val_id": r.get("val_id"),
            "name":   r.get("name") or f"val#{r.get('val_id')}",
            "shared_auth": auth_count[auth] > 1,
        }
    return out


ACTIVE_SET_SIZE_CURRENT = 200       # current consensus cap on testnet
EDGE_BUFFER_RANKS = 30              # how far past the cap we send to the LLM


async def _validator_landscape(pool) -> dict:
    """Build an immutable, rank-keyed factual snapshot for the LLM. Two
    arrays — `validators_by_rank` (ranks 1..ACTIVE_SET_SIZE_CURRENT) and
    `edge_validators_outside_active_set` (ranks above the cap) — each entry
    carries an explicit `rank` field. The LLM is forbidden from recomputing
    rank from array index; this is the source of truth.

    Source: MAX(epoch) snapshot of validator_stake_history.
    Tie-break: val_id ASC (matches /api/validators/directory iteration order
    used by external consumers — ensures rank parity with side-panel checks).
    For entries without a directory match (no val_id), fall back to auth ASC.
    """
    directory = _load_validator_directory()

    async with pool.acquire() as conn:
        current_epoch = await conn.fetchval(
            "SELECT MAX(epoch) FROM validator_stake_history WHERE network = $1",
            CONTEXT_NETWORK,
        )
        stake_rows = await conn.fetch(
            """
            SELECT validator_id, total_stake, self_stake, delegator_count
            FROM validator_stake_history
            WHERE network = $1 AND epoch = $2
            """,
            CONTEXT_NETWORK, current_epoch,
        )
        deleg_row = await conn.fetchrow(
            """
            SELECT
                COUNT(DISTINCT delegator) AS unique_delegators,
                COUNT(*)                   AS total_delegate_events
            FROM stake_events
            WHERE network = $1 AND event_type = 'delegate'
            """,
            CONTEXT_NETWORK,
        )

    # Enrich each row with its directory val_id, then sort in Python so we
    # can tie-break on val_id (which lives only in the directory JSON, not
    # in the DB). Sentinel for missing val_id sorts to end of any plateau.
    # Drop shared-auth rows — until the data layer captures per-val_id
    # stake (PK migration, see backlog), we can't attribute rank honestly
    # for clusters where one auth holds N val_ids.
    INF = float("inf")
    enriched: list[dict] = []
    skipped_shared = 0
    for r in stake_rows:
        auth = (r["validator_id"] or "").lower()
        meta = directory.get(auth) or {}
        if meta.get("shared_auth"):
            skipped_shared += 1
            continue
        enriched.append({
            "auth":            auth,
            "total_stake":     int(r["total_stake"]),
            "self_stake":      int(r["self_stake"]),
            "delegator_count": int(r["delegator_count"] or 0),
            "val_id":          meta.get("val_id"),
            "name":            meta.get("name"),
        })
    if skipped_shared:
        log.info("governance landscape: skipped %d shared-auth row(s)", skipped_shared)
    enriched.sort(key=lambda x: (
        -x["total_stake"],
        x["val_id"] if x["val_id"] is not None else INF,
        x["auth"],
    ))

    ranked: list[dict] = []
    for idx, r in enumerate(enriched, start=1):
        ranked.append({
            "rank":            idx,
            "val_id":          r["val_id"],
            "name":            r["name"],
            "auth":            r["auth"],
            "stake_mon":       r["total_stake"] // 10**18,
            "self_stake_mon":  r["self_stake"]  // 10**18,
            "delegator_count": r["delegator_count"],
        })

    if not ranked:
        return {"note": "no on-chain context available — stake history empty"}

    total = len(ranked)

    def _expose(v: dict) -> dict:
        # Public shape sent to the LLM — explicit rank, sorted upstream.
        return {
            "rank":      v["rank"],
            "name":      v["name"],          # may be null
            "id":        v["val_id"],         # may be null
            "stake_mon": v["stake_mon"],
        }

    in_active_set = [_expose(v) for v in ranked if v["rank"] <= ACTIVE_SET_SIZE_CURRENT]
    edge_set = [
        _expose(v) for v in ranked
        if ACTIVE_SET_SIZE_CURRENT < v["rank"] <= ACTIVE_SET_SIZE_CURRENT + EDGE_BUFFER_RANKS
    ]

    median_idx = total // 2

    return {
        "network":                CONTEXT_NETWORK,
        "snapshot_taken_at":      datetime.now(timezone.utc).isoformat(),
        "snapshot_epoch":         current_epoch,
        "stake_basis":            "execution",   # see project backlog item #1
        "validator_count_total":  total,
        "active_set_size_current": ACTIVE_SET_SIZE_CURRENT,
        "median_stake_mon":       ranked[median_idx]["stake_mon"],
        "average_stake_mon":      int(sum(v["stake_mon"] for v in ranked) / total),
        # Two arrays, immutable, sorted, every entry carries its real rank.
        "validators_by_rank":                  in_active_set,
        "edge_validators_outside_active_set":  edge_set,
        "delegator_stats": {
            "unique_delegators_seen": int(deleg_row["unique_delegators"] or 0),
            "total_delegate_events":  int(deleg_row["total_delegate_events"] or 0),
        },
    }


def _topic_mentions(text: str, terms: list[str]) -> bool:
    t = (text or "").lower()
    return any(term in t for term in terms)


async def _commission_outliers(pool) -> dict:
    """Top-5 highest + bottom-5 lowest commissions among validators with
    nonzero stake. Only included for MIPs whose body discusses commission
    or rewards (cheap heuristic — keeps the prompt shorter for unrelated
    proposals). Source is the staking precompile via the on-chain query —
    we don't currently mirror commission into Postgres for every snapshot,
    so this falls back to validator_directory metadata when present."""
    # Pull commissions from a recent block via the validators API helper —
    # but to avoid pulling RPC into the LLM path, we take what's already
    # stamped into epoch_validators.commission. If empty (testnet currently
    # has no rows there), return a note.
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT validator_id, commission, stake
            FROM epoch_validators
            WHERE network = $1
              AND epoch_number = (SELECT MAX(epoch_number) FROM epoch_validators WHERE network = $1)
              AND stake > 0
            """,
            CONTEXT_NETWORK,
        )
    if not rows:
        return {"note": "commission outlier data not available on this network"}
    sorted_rows = sorted(rows, key=lambda r: int(r["commission"] or 0))
    lowest = [{"validator_id": r["validator_id"], "commission_bps": int(r["commission"])} for r in sorted_rows[:5]]
    highest = [{"validator_id": r["validator_id"], "commission_bps": int(r["commission"])} for r in sorted_rows[-5:]]
    return {"lowest_commission": lowest, "highest_commission": highest}


async def _build_onchain_context(pool, topic: dict, op_md: str) -> dict:
    """Compose generic landscape + topic-specific extras based on keywords
    in the proposal body."""
    landscape = await _validator_landscape(pool)
    extras: dict = {}
    body_blob = (op_md or "") + " " + (topic.get("title") or "")
    if _topic_mentions(body_blob, ["commission", "reward rate", "fee schedule", "validator reward", "operator fee"]):
        try:
            extras["commission_outliers"] = await _commission_outliers(pool)
        except Exception as e:
            log.warning("commission outlier ctx failed: %s", e)
    return {"landscape": landscape, **extras}


async def _build_canonical_validator_map(pool) -> dict:
    """Return {name_lower: {rank, stake_mon, val_id, name, in_active_set}}
    for every named validator in the CURRENT EPOCH only.

    Source semantics — read this carefully before changing:
      - Source: MAX(epoch) snapshot of validator_stake_history. Never mix
        with older epochs.
      - rank = position after sorting by (-stake, val_id ASC, auth ASC).
        val_id ASC tie-break matches the side-panel iteration order over
        /api/validators/directory; auth ASC is the final fallback for
        entries without a directory match.
      - stake_basis = "execution". The total_stake column is misnamed; the
        collector writes execution_stake from the staking precompile to it.
        Active-set membership is technically by consensus_stake on Monad —
        not yet stored. See backlog.
      - in_active_set = rank <= ACTIVE_SET_SIZE_CURRENT. Used by the
        corrector to phrase outside-active-set citations correctly.
    """
    directory = _load_validator_directory()
    async with pool.acquire() as conn:
        current_epoch = await conn.fetchval(
            "SELECT MAX(epoch) FROM validator_stake_history WHERE network = $1",
            CONTEXT_NETWORK,
        )
        rows = await conn.fetch(
            """
            SELECT validator_id, total_stake
            FROM validator_stake_history
            WHERE network = $1 AND epoch = $2
            """,
            CONTEXT_NETWORK, current_epoch,
        )
    INF = float("inf")
    enriched = []
    skipped_shared = 0
    for r in rows:
        auth = (r["validator_id"] or "").lower()
        meta = directory.get(auth) or {}
        if meta.get("shared_auth"):
            skipped_shared += 1
            continue
        enriched.append({
            "auth":        auth,
            "total_stake": int(r["total_stake"]),
            "val_id":      meta.get("val_id"),
            "name":        meta.get("name"),
        })
    if skipped_shared:
        log.info("governance canonical: skipped %d shared-auth row(s)", skipped_shared)
    enriched.sort(key=lambda x: (
        -x["total_stake"],
        x["val_id"] if x["val_id"] is not None else INF,
        x["auth"],
    ))
    canonical: dict[str, dict] = {}
    for idx, r in enumerate(enriched, start=1):
        if not r["name"]:
            continue
        canonical[r["name"].lower()] = {
            "name":          r["name"],
            "rank":          idx,
            "stake_mon":     r["total_stake"] // 10**18,
            "stake_basis":   "execution",
            "val_id":        r["val_id"],
            "epoch":         current_epoch,
            "in_active_set": idx <= ACTIVE_SET_SIZE_CURRENT,
        }
    return canonical


def _format_stake_mon(stake_mon: int) -> str:
    """Compact stake formatter used by the corrector. 25034117 → '25M'."""
    if stake_mon >= 1_000_000:
        return f"{stake_mon // 1_000_000}M"
    if stake_mon >= 1_000:
        return f"{stake_mon // 1_000}K"
    return str(stake_mon)


def _correct_bullet_text(bullet: str, canonical: dict) -> tuple[str, dict]:
    """Find every "Name (rank N[, X MON staked])" in `bullet` where Name is
    a known directory entry. Force rank + stake to canonical values.

    Strategy:
      - For each canonical name (longest first), build a regex that grabs
        the parenthetical immediately after the name.
      - Accept both "rank N" and "rank N by execution stake" (the new
        explicit-basis phrasing we want LLM output to use).
      - If cited rank or stake differs from canonical, rewrite. Always
        emit "rank N by execution stake, X MON staked" so the basis is
        explicit even if LLM emitted the legacy form.
      - Each rewrite is logged so we have a paper trail when a side panel
        check disagrees with our output.

    Returns (corrected_bullet, stats)."""
    stats = {"corrected": 0, "preserved": 0}
    if not canonical or not bullet:
        return bullet, stats

    text = bullet
    # Process longest names first so "Monad Foundation - lsn-tyo-006" wins
    # over the prefix "Monad Foundation".
    for nm_low in sorted(canonical.keys(), key=len, reverse=True):
        canon = canonical[nm_low]
        canon_name = canon["name"]
        canon_rank = canon["rank"]
        canon_stake_str = _format_stake_mon(canon["stake_mon"])
        in_active = canon.get("in_active_set", True)
        rank_phrase = (
            f"rank {canon_rank} by execution stake"
            if in_active
            else f"rank {canon_rank} outside active set"
        )
        # Match: name + "(rank N[ by execution stake | outside active set][, X MON staked])"
        # Tolerates legacy "rank N" plus both new phrasings the LLM might emit.
        name_re = re.escape(canon_name)
        pattern = re.compile(
            rf"(?<![\w/])({name_re})\s*\(\s*rank\s+(\d+)"
            rf"(?:\s+by\s+execution\s+stake|\s+outside\s+active\s+set)?"
            rf"\s*(?:,\s*([^)]*?MON[^)]*?))?\s*\)",
            re.IGNORECASE,
        )

        def _do(m, _rank_phrase=rank_phrase, _canon_rank=canon_rank,
                _canon_stake_str=canon_stake_str, _in_active=in_active):
            matched_name = m.group(1)
            cited_rank = int(m.group(2))
            cited_stake_text = m.group(3)
            stake_present = cited_stake_text is not None
            if stake_present:
                replacement = (
                    f"{matched_name} ({_rank_phrase}, "
                    f"{_canon_stake_str} MON staked)"
                )
            else:
                replacement = f"{matched_name} ({_rank_phrase})"
            if cited_rank != _canon_rank:
                stats["corrected"] += 1
                log.info(
                    "governance correction: %r — cited rank %d → canonical %d "
                    "(stake %s, in_active_set=%s)",
                    matched_name, cited_rank, _canon_rank,
                    _canon_stake_str, _in_active,
                )
            else:
                stats["preserved"] += 1
            return replacement

        text = pattern.sub(_do, text)
    return text, stats


def _extract_context_validator_ids(ctx: dict) -> list[int]:
    """Collect all val_ids that appear anywhere in the on-chain context, so
    we can persist them as provenance — even if the LLM didn't cite them in
    bullet text, they were present at generation time."""
    ids: set[int] = set()
    landscape = ctx.get("landscape") or {}
    for key in ("validators_by_rank", "edge_validators_outside_active_set"):
        for v in landscape.get(key, []) or []:
            # New schema uses `id`; tolerate legacy `val_id` for safety.
            vid = v.get("id") if "id" in v else v.get("val_id")
            if isinstance(vid, int):
                ids.add(vid)
    return sorted(ids)


# ─── Anthropic API ─────────────────────────────────────────────────────


async def _call_anthropic(api_key: str, system: str, user: str) -> dict | None:
    """Call Messages API. Returns parsed JSON dict from the model's reply,
    or None on transport / parse failure."""
    payload = {
        "model": MODEL,
        "max_tokens": 2000,
        "system": system,
        "messages": [{"role": "user", "content": user}],
    }
    headers = {
        "x-api-key": api_key,
        "anthropic-version": ANTHROPIC_VERSION,
        "content-type": "application/json",
    }
    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            r = await client.post(ANTHROPIC_API_URL, headers=headers, json=payload)
        if r.status_code != 200:
            log.warning("anthropic %d: %s", r.status_code, r.text[:300])
            return None
        body = r.json()
    except Exception as e:
        log.warning("anthropic call failed: %s", e)
        return None

    # Extract text from the first content block
    blocks = body.get("content") or []
    text = ""
    for b in blocks:
        if b.get("type") == "text":
            text += b.get("text", "")
    if not text:
        return None

    # Strip code-fence wrappers (the model sometimes returns ```json ... ```)
    text = text.strip()
    if text.startswith("```"):
        # Drop first line and last fence
        lines = text.splitlines()
        if lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip().startswith("```"):
            lines = lines[:-1]
        text = "\n".join(lines)

    try:
        return _json.loads(text)
    except Exception as e:
        log.warning("LLM did not return valid JSON: %s; raw=%s", e, text[:300])
        return None


# ─── Public entry ──────────────────────────────────────────────────────


async def regenerate_summary_if_needed(pool, topic_id: int) -> str | None:
    """If OP markdown hash changed (or no summary exists), call Anthropic and
    upsert mip_summaries. Returns 'generated' / 'cached' / 'skipped' / None
    for status reporting."""
    api_key = os.environ.get("ANTHROPIC_API_KEY") or ""
    if not api_key.strip():
        return "skipped"  # no key — graceful no-op

    async with pool.acquire() as conn:
        topic = await conn.fetchrow(
            "SELECT id, mip_number, title, status, category FROM mip_topics WHERE id = $1",
            topic_id,
        )
        if not topic:
            return None
        op = await conn.fetchrow(
            "SELECT raw_markdown, raw_hash FROM mip_posts WHERE topic_id = $1 AND post_number = 1",
            topic_id,
        )
        if not op or not op["raw_markdown"]:
            return None
        existing = await conn.fetchrow(
            "SELECT source_hash, generated_at FROM mip_summaries WHERE topic_id = $1",
            topic_id,
        )

    op_hash = op["raw_hash"] or _hash_text(op["raw_markdown"])
    # Cache invalidation: regen if (a) source markdown changed OR (b) summary
    # is older than 24h — stake distribution drifts, so impact bullets that
    # reference real ranks should be refreshed even when the proposal text
    # hasn't moved.
    if existing and existing["source_hash"] == op_hash:
        gen_at = existing["generated_at"]
        if gen_at and gen_at.tzinfo is None:
            gen_at = gen_at.replace(tzinfo=timezone.utc)
        if gen_at and (datetime.now(timezone.utc) - gen_at) < timedelta(hours=24):
            return "cached"  # nothing changed since last run, still fresh

    # Build the on-chain context — generic validator landscape + any
    # topic-specific extras the body mentions (commission, etc.).
    try:
        onchain_ctx = await _build_onchain_context(
            pool,
            {"title": topic["title"], "mip_number": topic["mip_number"]},
            op["raw_markdown"] or "",
        )
    except Exception as e:
        log.warning("onchain context build failed for MIP-%s: %s", topic["mip_number"], e)
        onchain_ctx = {}

    # Trim very long OPs so we stay safely within token limits
    op_md = op["raw_markdown"]
    if len(op_md) > MAX_OP_CHARS:
        op_md = op_md[:MAX_OP_CHARS] + "\n\n[truncated]"

    user_prompt = (
        f"Title: {topic['title']}\n"
        f"MIP number: {topic['mip_number'] if topic['mip_number'] is not None else 'unnumbered draft'}\n"
        f"Status: {topic['status']}\n"
        f"Category: {topic['category'] or 'unknown'}\n"
        f"\n--- Proposal body (markdown from forum.monad.xyz) ---\n"
        f"{op_md}\n"
        f"\n--- On-chain context ---\n"
        f"{_json.dumps(onchain_ctx, indent=2) if onchain_ctx else 'none'}\n"
    )

    parsed = await _call_anthropic(api_key, SYSTEM_PROMPT, user_prompt)
    if not parsed:
        return None

    # Validate shape — accept partial output gracefully but require summary
    summary_text = (parsed.get("summary") or "").strip()
    if not summary_text:
        log.warning("LLM returned empty summary for topic %s", topic_id)
        return None
    val_imp = parsed.get("validator_impact") or []
    del_imp = parsed.get("delegator_impact") or []
    bld_imp = parsed.get("builder_impact") or []

    # Post-generation safety net: silent-correct any rank/stake the LLM cited
    # for a real directory validator. Catches cases where the model ignored the
    # "do not modify rank" rule and emitted "Backpack (rank 100, 11M MON)" when
    # Backpack is actually at rank 111. Names not in canonical pass through; the
    # frontend won't link them either way.
    canonical = await _build_canonical_validator_map(pool)
    correction_stats = {"corrected": 0, "preserved": 0}
    def _correct_list(items: list[str]) -> list[str]:
        out: list[str] = []
        for b in items:
            if not isinstance(b, str):
                out.append(b)
                continue
            new_b, st = _correct_bullet_text(b, canonical)
            correction_stats["corrected"] += st["corrected"]
            correction_stats["preserved"] += st["preserved"]
            out.append(new_b)
        return out
    val_imp = _correct_list(val_imp)
    del_imp = _correct_list(del_imp)
    bld_imp = _correct_list(bld_imp)
    if correction_stats["corrected"]:
        log.warning(
            "topic %s: corrected %d hallucinated rank/stake reference(s) (preserved %d)",
            topic_id, correction_stats["corrected"], correction_stats["preserved"],
        )

    # Provenance: every val_id that appeared in the LLM's grounding context
    # (so we can later filter MIPs that name a specific operator). The model
    # also returns `referenced_validator_ids` for the val_ids it actually
    # cited. We DROP cited val_ids that aren't in our on-chain context — the
    # LLM occasionally hallucinates val_ids from training data (e.g. mainnet
    # operators on a testnet analysis); those would create broken validator
    # links on the frontend.
    ctx_ids = _extract_context_validator_ids(onchain_ctx)
    ctx_id_set = set(ctx_ids)
    cited_ids: list[int] = []
    dropped_cited: list[int] = []
    for v in (parsed.get("referenced_validator_ids") or []):
        try:
            x = int(v)
        except Exception:
            continue
        if x in ctx_id_set:
            cited_ids.append(x)
        else:
            dropped_cited.append(x)
    if dropped_cited:
        log.warning(
            "topic %s: LLM cited val_ids not in on-chain context — dropped %s",
            topic_id, dropped_cited,
        )
    provenance_ids = sorted(set(ctx_ids) | set(cited_ids))

    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO mip_summaries (
                topic_id, summary, validator_impact, delegator_impact, builder_impact,
                source_hash, model, generated_at, context_validator_ids
            ) VALUES ($1, $2, $3::jsonb, $4::jsonb, $5::jsonb, $6, $7, NOW(), $8::int[])
            ON CONFLICT (topic_id) DO UPDATE SET
                summary               = EXCLUDED.summary,
                validator_impact      = EXCLUDED.validator_impact,
                delegator_impact      = EXCLUDED.delegator_impact,
                builder_impact        = EXCLUDED.builder_impact,
                source_hash           = EXCLUDED.source_hash,
                model                 = EXCLUDED.model,
                generated_at          = NOW(),
                context_validator_ids = EXCLUDED.context_validator_ids
            """,
            topic_id,
            summary_text,
            _json.dumps(val_imp),
            _json.dumps(del_imp),
            _json.dumps(bld_imp),
            op_hash,
            MODEL,
            provenance_ids,
        )
    log.info("governance: summary generated for topic %s (MIP-%s)",
             topic_id, topic["mip_number"])
    return "generated"


async def regenerate_summaries_for_all(pool) -> dict:
    """Walk all topics; regenerate where OP hash changed or summary is >24h old.
    Called from collector loop after every successful scrape pass.

    Rollout safety: when GOVERNANCE_LLM_ALLOWED_TOPIC_IDS is set (comma-list of
    forum topic IDs), only those topics are eligible for regeneration. Used to
    confine prompt/context changes to a single canary topic until the operator
    confirms the new output is good and removes the env var. With the var
    unset (or empty), regeneration is unrestricted (the steady-state default).
    """
    if not (os.environ.get("ANTHROPIC_API_KEY") or "").strip():
        return {"skipped": "no_api_key"}

    allowed_csv = (os.environ.get("GOVERNANCE_LLM_ALLOWED_TOPIC_IDS") or "").strip()
    allowed_ids: set[int] | None = None
    if allowed_csv:
        try:
            allowed_ids = {int(x.strip()) for x in allowed_csv.split(",") if x.strip().isdigit()}
        except Exception:
            allowed_ids = None
        if allowed_ids:
            log.info("governance_llm: rollout lock active — only topics %s eligible", sorted(allowed_ids))

    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT id FROM mip_topics ORDER BY id")
    counts = {"generated": 0, "cached": 0, "failed": 0, "skipped": 0, "locked_out": 0}
    for r in rows:
        if allowed_ids is not None and r["id"] not in allowed_ids:
            counts["locked_out"] += 1
            continue
        result = await regenerate_summary_if_needed(pool, r["id"])
        if result in counts:
            counts[result] += 1
        elif result is None:
            counts["failed"] += 1
    return counts
