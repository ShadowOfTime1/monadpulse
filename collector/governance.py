"""MonadPulse — Governance scraper.

Pulls Monad Improvement Proposals (MIPs) from forum.monad.xyz via the
Discourse JSON API. Detects new topics, edits to existing posts, status
changes, and replies. Stores everything in mip_topics / mip_posts and
appends to mip_changes for the timeline + Telegram alerts.

Flow (called from collector/main.py every 30 min, testnet collector only):
    scrape_governance_full(pool)
        -> scrape_mip_index(pool)         # category listing
            for each topic:
                -> scrape_mip_topic(pool, topic_id)
                    -> upsert posts, detect edits/replies, append changes
        -> emit pending Telegram alerts (caller flushes)

Polite scraping: User-Agent identifies us, 15s timeout, ≥0.3s delay between
requests, no concurrent fetches. Forum.monad.xyz is small (≤20 topics) so
this stays well under any reasonable rate limit.
"""
from __future__ import annotations

import asyncio
import hashlib
import json as _json
import logging
import re
from datetime import datetime, timezone
from typing import Any

import httpx

log = logging.getLogger("monadpulse.governance")

FORUM_BASE = "https://forum.monad.xyz"
USER_AGENT = "MonadPulse Governance Tracker (https://monadpulse.xyz)"
MIPS_CATEGORY_ID = 8  # /c/mips/ on forum.monad.xyz
REQUEST_DELAY_SEC = 0.4  # politeness gap between forum requests

# Status detection — best-effort. The forum doesn't have an explicit status
# field, so we look at (in priority order):
#   1. tags on the topic (e.g. tag "soft-consensus" / "approved" / "draft")
#   2. explicit "Status: X" line in the OP markdown
#   3. fallback to 'Draft'
KNOWN_STATUSES = [
    ("activated",       "Activated"),
    ("approved",        "Approved"),
    ("soft-consensus",  "Soft Consensus"),
    ("soft_consensus",  "Soft Consensus"),
    ("discussion",      "Discussion"),
    ("review",          "Discussion"),
    ("draft",           "Draft"),
    ("withdrawn",       "Withdrawn"),
    ("rejected",        "Rejected"),
    ("stagnant",        "Stagnant"),
]
STATUS_TAG_MAP = dict(KNOWN_STATUSES)
STATUS_LABEL_MAP = {label.lower(): label for _, label in KNOWN_STATUSES}

# Status line regex: "Status: Soft Consensus", "**Status**: draft", etc.
STATUS_LINE_RE = re.compile(
    r"(?im)^\s*\*{0,2}\s*status\s*\*{0,2}\s*[:\-]\s*\*{0,2}\s*([A-Za-z][\w\s\-]*?)\s*\*{0,2}\s*$"
)

# MIP number from title: "MIP-9: ..." / "MIP 11 - ..." / case-insensitive
MIP_NUMBER_RE = re.compile(r"^\s*MIP[\s\-_]*(\d+)\b", re.IGNORECASE)


# ─── HTTP layer ────────────────────────────────────────────────────────

_client: httpx.AsyncClient | None = None


async def _get_client() -> httpx.AsyncClient:
    global _client
    if _client is None or _client.is_closed:
        _client = httpx.AsyncClient(
            timeout=15.0,
            headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
            follow_redirects=True,
        )
    return _client


async def _fetch_json(path: str) -> dict | None:
    """GET {FORUM_BASE}{path}, return parsed JSON or None on error."""
    url = FORUM_BASE + path
    client = await _get_client()
    try:
        await asyncio.sleep(REQUEST_DELAY_SEC)
        r = await client.get(url)
        if r.status_code != 200:
            log.warning("forum %s -> HTTP %d", path, r.status_code)
            return None
        return r.json()
    except Exception as e:
        log.warning("forum %s -> %s", path, e)
        return None


# ─── Helpers ───────────────────────────────────────────────────────────


def _parse_mip_number(title: str) -> int | None:
    m = MIP_NUMBER_RE.search(title or "")
    return int(m.group(1)) if m else None


def _normalize_tags(raw: Any) -> list[str]:
    """Discourse can return tags as list[str] OR list[{name|tag, ...}] depending
    on endpoint and topic. Normalize to a clean list of lowercase strings."""
    if not raw:
        return []
    out = []
    for item in raw:
        if isinstance(item, str):
            out.append(item)
        elif isinstance(item, dict):
            # try common field names
            name = item.get("name") or item.get("tag") or item.get("id") or item.get("slug")
            if name:
                out.append(str(name))
    return [t for t in out if t]


_ACTIVATED_MIPS_CACHE: dict | None = None


def _load_activated_mips() -> dict:
    """Load the hard-fork → activated-MIPs map from config. The map provides
    the canonical source for MIP activation status — the forum's tag/Status
    field lags or never updates after a fork. Cached after first read."""
    global _ACTIVATED_MIPS_CACHE
    if _ACTIVATED_MIPS_CACHE is not None:
        return _ACTIVATED_MIPS_CACHE
    from pathlib import Path
    path = Path("/opt/monadpulse/config/activated_mips.json")
    if not path.exists():
        _ACTIVATED_MIPS_CACHE = {}
        return _ACTIVATED_MIPS_CACHE
    try:
        raw = _json.loads(path.read_text())
    except Exception as e:
        log.warning("activated_mips load failed: %s", e)
        _ACTIVATED_MIPS_CACHE = {}
        return _ACTIVATED_MIPS_CACHE
    # Drop comment keys
    out = {k: v for k, v in raw.items() if not k.startswith("_")}
    _ACTIVATED_MIPS_CACHE = out
    return out


def _activation_for_mip(mip_number: int | None) -> dict | None:
    """Return {fork, testnet_ts, mainnet_ts, source_url} if mip_number was
    activated in any known fork; else None."""
    if mip_number is None:
        return None
    label = f"MIP-{mip_number}"
    for fork, meta in _load_activated_mips().items():
        if label in (meta.get("mips") or []):
            return {
                "fork": fork,
                "testnet_activation_ts": meta.get("testnet_activation_ts"),
                "mainnet_activation_ts": meta.get("mainnet_activation_ts"),
                "source_url": meta.get("source_url"),
            }
    return None


def _detect_status(tags: list[str], op_markdown: str) -> str:
    # 1. tag-based (highest priority — explicit signal from author/Foundation)
    if tags:
        for t in tags:
            t_low = (t or "").lower()
            if t_low in STATUS_TAG_MAP:
                return STATUS_TAG_MAP[t_low]
    # 2. "Status: X" line in OP markdown
    if op_markdown:
        m = STATUS_LINE_RE.search(op_markdown)
        if m:
            raw = m.group(1).strip().lower()
            if raw in STATUS_LABEL_MAP:
                return STATUS_LABEL_MAP[raw]
            # Try matching "soft consensus" -> "soft-consensus" tag form
            normalized = re.sub(r"\s+", "-", raw)
            if normalized in STATUS_TAG_MAP:
                return STATUS_TAG_MAP[normalized]
    return "Draft"


def _classify_category(tags: list[str], title: str) -> str | None:
    """MIP-1 (the meta proposal) defines three categories. We mirror them.
    Fall back to None when we can't tell."""
    title_low = (title or "").lower()
    tag_blob = " ".join(t.lower() for t in (tags or []))
    if "informational" in tag_blob or "informational" in title_low or "meta" in tag_blob:
        return "Informational"
    if "contracts" in tag_blob or "predeploy" in tag_blob or "contract" in title_low:
        return "Contracts"
    if "core" in tag_blob or "protocol" in tag_blob:
        return "Core Protocol"
    return None


def _parse_iso(s: str | None) -> datetime | None:
    if not s:
        return None
    try:
        # Discourse uses "2026-04-21T17:10:53.000Z"
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def _hash_text(s: str | None) -> str:
    return hashlib.sha256((s or "").encode("utf-8")).hexdigest()


def _is_mip_topic(t: dict) -> bool:
    """Filter out the pinned 'About the MIPs category' meta-topic."""
    title = (t.get("title") or "").strip()
    if title.lower().startswith("about the"):
        return False
    if t.get("pinned") and "about" in title.lower():
        return False
    # Keep all other topics in the category — including pre-MIP-numbered
    # proposals (e.g. "Proposal: Cryptographic ..." that haven't received a
    # MIP-N tag yet).
    return True


# ─── Database upserts ──────────────────────────────────────────────────


async def _upsert_topic(conn, topic: dict, op_markdown: str) -> tuple[bool, str | None, str | None]:
    """Insert or update topic row. Returns (is_new, old_status, old_tags_csv).
    Tags are stored as a real text[] column in PG.
    """
    tags = _normalize_tags(topic.get("tags"))
    status = _detect_status(tags, op_markdown)
    mip_num = _parse_mip_number(topic.get("title", ""))
    category = _classify_category(tags, topic.get("title", ""))
    # Hard-fork override: if this MIP shipped in a known fork, force
    # status to 'Activated' regardless of forum tag — the forum's
    # status field rarely catches up after activation.
    if _activation_for_mip(mip_num) is not None:
        status = "Activated"
    poster = (topic.get("posters") or [{}])
    # 'posters' lists user IDs; first entry with description containing "Original Poster" is the author.
    # The full topic JSON gives us details_created_by; we use that when present.
    details = topic.get("details") or {}
    created_by = details.get("created_by") or {}
    author_username = created_by.get("username") or topic.get("last_poster_username")
    author_id = created_by.get("id")

    forum_created = _parse_iso(topic.get("created_at")) or datetime.now(timezone.utc)
    forum_updated = (
        _parse_iso(topic.get("last_posted_at"))
        or _parse_iso(topic.get("bumped_at"))
        or forum_created
    )

    existing = await conn.fetchrow(
        "SELECT status, tags, mip_number FROM mip_topics WHERE id = $1",
        topic["id"],
    )
    is_new = existing is None
    old_status = existing["status"] if existing else None
    old_tags = list(existing["tags"]) if existing and existing["tags"] is not None else []
    old_tags_csv = ",".join(old_tags) if old_tags else None

    await conn.execute(
        """
        INSERT INTO mip_topics (
            id, mip_number, slug, title, category, status,
            author_username, author_id,
            forum_created_at, forum_updated_at,
            views, reply_count, tags, archetype, pinned, closed, last_scraped_at
        ) VALUES (
            $1, $2, $3, $4, $5, $6,
            $7, $8,
            $9, $10,
            $11, $12, $13, $14, $15, $16, NOW()
        )
        ON CONFLICT (id) DO UPDATE SET
            mip_number       = EXCLUDED.mip_number,
            slug             = EXCLUDED.slug,
            title            = EXCLUDED.title,
            category         = EXCLUDED.category,
            status           = EXCLUDED.status,
            author_username  = COALESCE(EXCLUDED.author_username, mip_topics.author_username),
            author_id        = COALESCE(EXCLUDED.author_id, mip_topics.author_id),
            forum_updated_at = EXCLUDED.forum_updated_at,
            views            = EXCLUDED.views,
            reply_count      = EXCLUDED.reply_count,
            tags             = EXCLUDED.tags,
            archetype        = EXCLUDED.archetype,
            pinned           = EXCLUDED.pinned,
            closed           = EXCLUDED.closed,
            last_scraped_at  = NOW()
        """,
        topic["id"],
        mip_num,
        topic.get("slug") or "",
        topic.get("title") or "",
        category,
        status,
        author_username,
        author_id,
        forum_created,
        forum_updated,
        int(topic.get("views") or 0),
        # Discourse exposes two counters: `posts_count` (all visible posts
        # incl. OP) and `reply_count` (top-level direct replies to OP — does
        # not include nested or whisper/staff posts). The forum listing UI
        # shows `posts_count - 1`, so we mirror that. Hidden/deleted posts
        # are already excluded from posts_count.
        int(max(0, (topic.get("posts_count") or 1) - 1)),
        tags,
        topic.get("archetype"),
        bool(topic.get("pinned")),
        bool(topic.get("closed")),
    )
    return is_new, old_status, old_tags_csv


async def _upsert_post(conn, topic_id: int, post: dict) -> dict:
    """Insert or update a single post. Returns dict with change info."""
    raw = post.get("raw") or ""
    cooked = post.get("cooked") or ""
    new_hash = _hash_text(raw)
    forum_created = _parse_iso(post.get("created_at")) or datetime.now(timezone.utc)
    forum_updated = _parse_iso(post.get("updated_at")) or forum_created
    version = int(post.get("version") or 1)

    existing = await conn.fetchrow(
        "SELECT raw_hash, version, raw_markdown FROM mip_posts WHERE id = $1",
        post["id"],
    )
    is_new = existing is None
    is_edited = (not is_new) and (existing["raw_hash"] != new_hash or existing["version"] != version)
    old_raw_markdown = existing["raw_markdown"] if existing else None

    await conn.execute(
        """
        INSERT INTO mip_posts (
            id, topic_id, post_number, username,
            cooked_html, raw_markdown, raw_hash,
            forum_created_at, forum_updated_at, version, reply_to_post_number,
            last_scraped_at
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, NOW()
        )
        ON CONFLICT (id) DO UPDATE SET
            cooked_html       = EXCLUDED.cooked_html,
            raw_markdown      = EXCLUDED.raw_markdown,
            raw_hash          = EXCLUDED.raw_hash,
            forum_updated_at  = EXCLUDED.forum_updated_at,
            version           = EXCLUDED.version,
            reply_to_post_number = EXCLUDED.reply_to_post_number,
            last_scraped_at   = NOW()
        """,
        post["id"],
        topic_id,
        int(post.get("post_number") or 1),
        post.get("username"),
        cooked,
        raw,
        new_hash,
        forum_created,
        forum_updated,
        version,
        post.get("reply_to_post_number"),
    )
    return {
        "is_new": is_new,
        "is_edited": is_edited,
        "post_number": int(post.get("post_number") or 1),
        "old_hash": existing["raw_hash"] if existing else None,
        "new_hash": new_hash,
        "old_raw_markdown": old_raw_markdown,
        "new_raw_markdown": raw,
    }


async def _record_change(
    conn, topic_id: int, change_type: str,
    post_id: int | None = None,
    old_value: str | None = None,
    new_value: str | None = None,
    detail: dict | None = None,
) -> None:
    await conn.execute(
        """
        INSERT INTO mip_changes (topic_id, post_id, change_type, old_value, new_value, detail)
        VALUES ($1, $2, $3, $4, $5, $6)
        """,
        topic_id, post_id, change_type, old_value, new_value,
        _json.dumps(detail) if detail else None,
    )


_INS_RE  = re.compile(r"<ins[^>]*>(.*?)</ins>", re.DOTALL)
_DEL_RE  = re.compile(r"<del[^>]*>(.*?)</del>", re.DOTALL)
_TAG_RE  = re.compile(r"<[^>]+>")
_WS_RE   = re.compile(r"\s+")


def _strip_tags(html: str) -> str:
    """Strip HTML tags and collapse whitespace."""
    return _WS_RE.sub(" ", _TAG_RE.sub("", html or "")).strip()


def _summarize_revision(inline_html: str | None, max_chars: int = 240) -> str:
    """Produce a one-line text summary of a Discourse inline-diff HTML.

    Discourse wraps each token in its own <del>/<ins>:
      "<del>As </del><del>the </del><del>Monad </del>..."
    Each token may include a trailing space. Concatenating the spans with
    no separator naturally reconstructs the original prose flow, including
    contractions ("network's") and punctuation. For numeric / parameter
    edits where the same short token (e.g. "250") repeats in title,
    parameter table, and prose, the no-separator join produces "250250250"
    — we detect that case and re-tokenize + dedupe.
    """
    if not inline_html:
        return ""

    del_spans = _DEL_RE.findall(inline_html)
    ins_spans = _INS_RE.findall(inline_html)

    def _clean(s: str) -> str:
        s = _TAG_RE.sub("", s or "")
        s = _WS_RE.sub(" ", s).strip()
        return s

    def _is_prose(spans: list[str]) -> bool:
        """Discourse word-tokenizes prose with each token carrying its
        trailing space ('As ', 'the ', 'Monad '), but it tokenizes numeric
        cell content tightly ('250', '50'). If the majority of spans end
        with whitespace, we're looking at prose — concatenate with no
        separator to reproduce the sentence with proper spacing. Otherwise
        we have a numeric/parameter edit and retokenize + dedupe."""
        cands = [s for s in spans if _clean(s)]
        if len(cands) < 3:
            return True  # too few — concat is fine either way
        spaced = sum(1 for s in cands if s and s[-1].isspace())
        return spaced >= len(cands) // 2

    def _retoken(spans: list[str]) -> str:
        """For numeric/parameter edits: dedupe word-level, preserve order."""
        words = [_clean(x) for x in spans if _clean(x)]
        seen: set[str] = set()
        out: list[str] = []
        for w in words:
            if w not in seen:
                seen.add(w)
                out.append(w)
        return " ".join(out)

    if _is_prose(del_spans):
        del_text = _clean("".join(del_spans))
    else:
        del_text = _retoken(del_spans)

    if _is_prose(ins_spans):
        ins_text = _clean("".join(ins_spans))
    else:
        ins_text = _retoken(ins_spans)

    if not del_text and not ins_text:
        return ""

    if del_text and ins_text:
        d = del_text[:160]
        i = ins_text[:160]
        summary = f'"{d}" → "{i}"'
    elif ins_text:
        summary = "added: " + ins_text[:220]
    else:
        summary = "removed: " + del_text[:220]

    if len(summary) > max_chars:
        summary = summary[:max_chars - 1] + "…"
    return summary


async def _fetch_post_revisions(conn, topic_id: int, post_id: int, version: int) -> int:
    """For a post with version > 1, fetch every Discourse revision (2..version)
    that we don't already have in mip_post_revisions, and persist it.
    Returns # of new revisions stored."""
    if version is None or version < 2:
        return 0
    existing = await conn.fetch(
        "SELECT revision_number FROM mip_post_revisions WHERE post_id = $1",
        post_id,
    )
    have = {r["revision_number"] for r in existing}
    stored = 0
    for rev in range(2, version + 1):
        if rev in have:
            continue
        data = await _fetch_json(f"/posts/{post_id}/revisions/{rev}.json")
        if not data:
            continue
        body_changes = data.get("body_changes") or {}
        inline_html = body_changes.get("inline") or ""
        summary = _summarize_revision(inline_html)
        edited_at = _parse_iso(data.get("created_at")) or datetime.now(timezone.utc)
        editor = data.get("acting_user_name") or data.get("display_username") or data.get("username")
        author = data.get("display_username") or data.get("username")
        await conn.execute(
            """
            INSERT INTO mip_post_revisions (
                post_id, topic_id, revision_number,
                editor_username, author_username,
                edited_at, edit_reason,
                body_changes_inline, summary
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (post_id, revision_number) DO NOTHING
            """,
            post_id, topic_id, rev,
            editor, author,
            edited_at, data.get("edit_reason"),
            inline_html, summary,
        )
        stored += 1
    return stored


def _build_diff_summary(old_md: str | None, new_md: str | None, max_lines: int = 12) -> dict:
    """Compute a compact diff summary between two markdown bodies.
    Returns dict with line-count delta, char-count delta, and a small unified-diff
    snippet for the timeline / Telegram alert. Avoid storing full diff for huge
    rewrites — the snippet caps at `max_lines` lines."""
    import difflib
    old_md = old_md or ""
    new_md = new_md or ""
    old_lines = old_md.splitlines()
    new_lines = new_md.splitlines()
    diff = list(difflib.unified_diff(
        old_lines, new_lines,
        fromfile="before", tofile="after",
        lineterm="", n=2,
    ))
    # Drop the file headers and ANY hunk noise — keep substantive +/- lines
    snippet_lines = []
    for ln in diff[2:]:  # skip --- / +++
        if ln.startswith("@@"):
            continue
        if ln.startswith("+") or ln.startswith("-") or ln.startswith(" "):
            snippet_lines.append(ln)
        if len(snippet_lines) >= max_lines:
            snippet_lines.append("...")
            break
    added   = sum(1 for ln in diff if ln.startswith("+") and not ln.startswith("+++"))
    removed = sum(1 for ln in diff if ln.startswith("-") and not ln.startswith("---"))
    return {
        "lines_added":   added,
        "lines_removed": removed,
        "chars_before":  len(old_md),
        "chars_after":   len(new_md),
        "snippet":       "\n".join(snippet_lines)[:1200],
    }


# ─── Top-level scrapers ────────────────────────────────────────────────


async def scrape_mip_topic(pool, topic_id: int) -> tuple[bool, int]:
    """Fetch one topic with all posts and persist.
    Returns (success, # changes detected)."""
    # Discourse rejects ?print=true on this forum; ?include_raw=1 alone is fine
    # and pulls the markdown source we need for diff/hash detection.
    data = await _fetch_json(f"/t/{topic_id}.json?include_raw=1")
    if not data:
        return False, 0
    posts = (data.get("post_stream") or {}).get("posts") or []
    if not posts:
        return False, 0

    # OP is post_number == 1
    op = next((p for p in posts if int(p.get("post_number") or 0) == 1), posts[0])
    op_markdown = op.get("raw") or ""

    changes = 0
    async with pool.acquire() as conn:
        async with conn.transaction():
            is_new_topic, old_status, old_tags_csv = await _upsert_topic(conn, data, op_markdown)
            if is_new_topic:
                await _record_change(
                    conn, topic_id, "topic_created",
                    new_value=data.get("title"),
                    detail={"author": data.get("details", {}).get("created_by", {}).get("username")},
                )
                changes += 1
            else:
                # status change?
                tags = _normalize_tags(data.get("tags"))
                new_status = _detect_status(tags, op_markdown)
                if old_status and old_status != new_status:
                    await _record_change(
                        conn, topic_id, "status_changed",
                        old_value=old_status, new_value=new_status,
                    )
                    changes += 1
                # tag change (informational)
                new_tags_csv = ",".join(tags) if tags else None
                if (old_tags_csv or "") != (new_tags_csv or ""):
                    await _record_change(
                        conn, topic_id, "tag_changed",
                        old_value=old_tags_csv, new_value=new_tags_csv,
                    )

            for post in posts:
                info = await _upsert_post(conn, topic_id, post)
                if info["is_new"] and info["post_number"] > 1:
                    await _record_change(
                        conn, topic_id, "reply_added",
                        post_id=post["id"],
                        new_value=post.get("username"),
                        detail={"post_number": info["post_number"]},
                    )
                    changes += 1
                elif info["is_edited"]:
                    ct = "op_edited" if info["post_number"] == 1 else "reply_edited"
                    diff = _build_diff_summary(
                        info.get("old_raw_markdown"),
                        info.get("new_raw_markdown"),
                    )
                    await _record_change(
                        conn, topic_id, ct,
                        post_id=post["id"],
                        old_value=info["old_hash"],
                        new_value=info["new_hash"],
                        detail={"post_number": info["post_number"], **diff},
                    )
                    changes += 1

    # Pass 2 — fetch missing revision history for posts with version > 1.
    # Done outside the topic transaction because each revision is a separate
    # HTTP call and we don't want to hold DB locks during forum I/O.
    revisions_added = 0
    for post in posts:
        version = int(post.get("version") or 1)
        if version < 2:
            continue
        async with pool.acquire() as conn:
            try:
                revisions_added += await _fetch_post_revisions(
                    conn, topic_id, int(post["id"]), version,
                )
            except Exception as e:
                log.warning("revision fetch failed for post %s: %s", post.get("id"), e)
    if revisions_added:
        log.info("governance: topic %s — stored %d revisions", topic_id, revisions_added)
    return True, changes


async def scrape_mip_index(pool) -> dict:
    """Fetch the MIPs category listing, scrape every topic. Returns stats."""
    cat_data = await _fetch_json(f"/c/mips/{MIPS_CATEGORY_ID}.json")
    if not cat_data:
        return {"topics_seen": 0, "topics_succeeded": 0, "topics_failed": 0, "changes": 0}
    topics = (cat_data.get("topic_list") or {}).get("topics") or []
    relevant = [t for t in topics if _is_mip_topic(t)]
    total_changes = 0
    succeeded = 0
    failed = 0
    for t in relevant:
        ok, changes = await scrape_mip_topic(pool, t["id"])
        if ok:
            succeeded += 1
        else:
            failed += 1
        total_changes += changes
    return {
        "topics_seen": len(topics),
        "topics_succeeded": succeeded,
        "topics_failed": failed,
        "changes": total_changes,
    }


async def scrape_governance_full(pool) -> dict:
    """Entry point called from collector main loop."""
    log.info("governance: starting scrape")
    try:
        stats = await scrape_mip_index(pool)
        log.info(
            "governance: %d topics seen, %d ok, %d failed, %d changes",
            stats["topics_seen"], stats["topics_succeeded"],
            stats["topics_failed"], stats["changes"],
        )
        # After persisting raw forum data, regenerate LLM summaries for any
        # topic whose OP markdown hash drifted. No-op if ANTHROPIC_API_KEY
        # is unset — collector keeps running.
        try:
            from collector.governance_llm import regenerate_summaries_for_all
            llm_stats = await regenerate_summaries_for_all(pool)
            log.info("governance: LLM summaries — %s", llm_stats)
            stats["llm"] = llm_stats
        except Exception as e:
            log.warning("governance: LLM stage failed: %s", e)
        # Dispatch any pending Telegram alerts (topic_created / status /
        # op_edited / reply_added). seed_initial_run silences the backfill
        # the very first time the dispatcher sees a populated table.
        try:
            from collector.governance_alerts import dispatch_pending_alerts
            tg_stats = await dispatch_pending_alerts(pool)
            log.info("governance: telegram — %s", tg_stats)
            stats["telegram"] = tg_stats
        except Exception as e:
            log.warning("governance: telegram dispatch failed: %s", e)
        return stats
    finally:
        global _client
        if _client and not _client.is_closed:
            await _client.aclose()
            _client = None
