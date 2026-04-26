"""MonadPulse — Governance API routes.

Endpoints:
    GET /governance/list                  — paginated list of MIPs with metadata
    GET /governance/mip/{topic_id}        — single MIP with all posts + summary + change log
    GET /governance/changes               — flat change log (powers timeline / Telegram)
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request

router = APIRouter()


def _load_validator_directory_for_link_map(network: str = "mainnet") -> dict[str, dict]:
    """Return {name_lower: {val_id, display_name}} for fast lookup when
    building the impact-analysis validator link map. Loads upstream + override."""
    out: dict[str, dict] = {}
    for fname in (
        f"/opt/monadpulse/validator_directory_{network}.json",
        f"/opt/monadpulse/validator_directory_override_{network}.json",
    ):
        path = Path(fname)
        if not path.exists():
            continue
        try:
            rows = json.loads(path.read_text())
        except Exception:
            continue
        for r in rows:
            name = (r.get("name") or "").strip()
            vid = r.get("val_id")
            if name and isinstance(vid, int):
                out[name.lower()] = {"val_id": vid, "name": name}
    return out


# ─── Edit classification ──────────────────────────────────────────────
# Substantive  — changes the actual proposal: numbers in proposal text,
#                parameter values, key verbs ("increase/decrease/from/to"),
#                or content in the first ~3 paragraphs of the post.
# Cosmetic     — link rename, typo, formatting only. Diff length < 20 chars
#                of meaningful change.
# Returns one of: 'substantive', 'cosmetic'.
_NUM_RE       = re.compile(r"\b\d{2,}\b")
_GOV_KEYWORDS = ("increase", "decrease", "raise", "lower", "from", "to",
                 "active set", "valset", "commission", "reward", "stake",
                 "epoch", "validator", "set size")


def _classify_edit(summary: str | None, body_changes_inline: str | None,
                   post_number: int | None) -> str:
    s = (summary or "").strip().lower()
    body = (body_changes_inline or "").lower()
    if not s:
        return "cosmetic"
    # Numbers changed in the diff body — substantive almost always.
    if _NUM_RE.search(s) and ("→" in summary or "added:" in s or "removed:" in s):
        # But pure URL/path numerics ("MIP9") aren't proposal changes
        url_only = (("located here" in s) or ("github.com" in s) or
                    ("forum.monad.xyz" in s) or ("mips.monad.xyz" in s))
        if not url_only:
            return "substantive"
    # Governance keywords near the diff (parameters, set sizes, commission).
    if any(k in s for k in _GOV_KEYWORDS):
        return "substantive"
    # Long deletion of prose from the OP — likely substantive narrative change.
    if post_number == 1 and s.startswith("removed:") and len(s) > 80:
        return "substantive"
    return "cosmetic"


def _row_to_topic(r) -> dict:
    # Activation info — derived from the hard-fork → MIPs map, not stored
    # in DB. Lets the frontend render an "Activated in MONAD_NINE · DD MMM YYYY"
    # badge without a schema change.
    activation = None
    try:
        from collector.governance import _activation_for_mip
        activation = _activation_for_mip(r["mip_number"])
    except Exception:
        pass
    return {
        "topic_id": r["id"],
        "mip_number": r["mip_number"],
        "slug": r["slug"],
        "title": r["title"],
        "category": r["category"],
        "status": r["status"],
        "author_username": r["author_username"],
        "forum_created_at": r["forum_created_at"].isoformat() if r["forum_created_at"] else None,
        "forum_updated_at": r["forum_updated_at"].isoformat() if r["forum_updated_at"] else None,
        "views": r["views"],
        "reply_count": r["reply_count"],
        "tags": list(r["tags"]) if r["tags"] is not None else [],
        "forum_url": f"https://forum.monad.xyz/t/{r['slug']}/{r['id']}",
        "activation_info": activation,
    }


@router.get("/list")
async def governance_list(
    request: Request,
    status: str | None = Query(None),
    category: str | None = Query(None),
    sort: str = Query("updated"),  # 'updated' | 'created' | 'activity' | 'mip'
):
    """List MIPs with metadata. Supports filter by status/category and sort.
    No pagination yet — set is small (<50 topics expected for MVP)."""
    sort_clauses = {
        "updated":  "forum_updated_at DESC",
        "created":  "forum_created_at DESC",
        "activity": "reply_count DESC, views DESC",
        "mip":      "mip_number ASC NULLS LAST, forum_created_at ASC",
    }
    order_by = sort_clauses.get(sort, sort_clauses["updated"])
    where = ["1=1"]
    args: list[Any] = []
    if status:
        args.append(status)
        where.append(f"status = ${len(args)}")
    if category:
        args.append(category)
        where.append(f"category = ${len(args)}")

    sql = f"""
        SELECT id, mip_number, slug, title, category, status,
               author_username, forum_created_at, forum_updated_at,
               views, reply_count, tags
        FROM mip_topics
        WHERE {' AND '.join(where)}
        ORDER BY {order_by}
    """

    pool = request.app.state.pool
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, *args)
    return [_row_to_topic(r) for r in rows]


@router.get("/mip/{topic_id}")
async def governance_mip_detail(request: Request, topic_id: int):
    """Full MIP detail: topic metadata, all posts (OP + replies), summary,
    and change history. Frontend renders the whole page from this one call."""
    pool = request.app.state.pool
    async with pool.acquire() as conn:
        topic_row = await conn.fetchrow(
            """
            SELECT id, mip_number, slug, title, category, status,
                   author_username, author_id,
                   forum_created_at, forum_updated_at,
                   views, reply_count, tags, archetype, pinned, closed
            FROM mip_topics WHERE id = $1
            """,
            topic_id,
        )
        if not topic_row:
            raise HTTPException(status_code=404, detail=f"MIP topic {topic_id} not found")

        post_rows = await conn.fetch(
            """
            SELECT id, post_number, username, cooked_html, raw_markdown,
                   forum_created_at, forum_updated_at, version, reply_to_post_number
            FROM mip_posts
            WHERE topic_id = $1
            ORDER BY post_number ASC
            """,
            topic_id,
        )

        summary_row = await conn.fetchrow(
            """
            SELECT summary, validator_impact, delegator_impact, builder_impact,
                   model, generated_at
            FROM mip_summaries WHERE topic_id = $1
            """,
            topic_id,
        )

        change_rows = await conn.fetch(
            """
            SELECT id, post_id, change_type, old_value, new_value, detail, detected_at
            FROM mip_changes
            WHERE topic_id = $1
            ORDER BY detected_at DESC
            LIMIT 100
            """,
            topic_id,
        )

        revision_rows = await conn.fetch(
            """
            SELECT r.id, r.post_id, r.revision_number, r.editor_username, r.author_username,
                   r.edited_at, r.edit_reason, r.summary, r.body_changes_inline,
                   p.post_number
            FROM mip_post_revisions r
            JOIN mip_posts p ON p.id = r.post_id
            WHERE r.topic_id = $1
            ORDER BY r.edited_at ASC
            """,
            topic_id,
        )

    posts = [
        {
            "id": p["id"],
            "post_number": p["post_number"],
            "username": p["username"],
            "cooked_html": p["cooked_html"],
            "raw_markdown": p["raw_markdown"],
            "forum_created_at": p["forum_created_at"].isoformat() if p["forum_created_at"] else None,
            "forum_updated_at": p["forum_updated_at"].isoformat() if p["forum_updated_at"] else None,
            "version": p["version"],
            "reply_to_post_number": p["reply_to_post_number"],
            "edited": (p["forum_updated_at"] - p["forum_created_at"]).total_seconds() > 60
                      if p["forum_created_at"] and p["forum_updated_at"] else False,
        }
        for p in post_rows
    ]

    summary = None
    if summary_row:
        # JSONB columns come back as Python objects already in asyncpg, but
        # legacy rows may store them as strings — handle both.
        def _coerce(v):
            if isinstance(v, str):
                try:
                    return json.loads(v)
                except Exception:
                    return []
            return v or []
        val_imp = _coerce(summary_row["validator_impact"])
        del_imp = _coerce(summary_row["delegator_impact"])
        bld_imp = _coerce(summary_row["builder_impact"])
        # Read-time rank/stake correction. Stake distribution drifts between
        # epochs, so the rank cited at LLM-gen time may not match what the
        # current /api/validators view shows. Re-running the corrector here
        # pegs every "Name (rank N, X MON staked)" reference to the current
        # epoch's snapshot.
        #
        # Failure policy: log + traceback, then return uncorrected LLM output.
        # The previous silent try/except masked the canonical-source bug for
        # an entire iteration — never again.
        import logging as _logging
        api_log = _logging.getLogger("monadpulse.governance_api")
        try:
            from collector.governance_llm import (
                _build_canonical_validator_map,
                _correct_bullet_text,
            )
            canon = await _build_canonical_validator_map(pool)
            agg_stats = {"corrected": 0, "preserved": 0}
            def _live_correct(items):
                out = []
                for b in items:
                    if isinstance(b, str):
                        nb, st = _correct_bullet_text(b, canon)
                        agg_stats["corrected"] += st.get("corrected", 0)
                        agg_stats["preserved"] += st.get("preserved", 0)
                        out.append(nb)
                    else:
                        out.append(b)
                return out
            val_imp = _live_correct(val_imp)
            del_imp = _live_correct(del_imp)
            bld_imp = _live_correct(bld_imp)
            if agg_stats["corrected"] or agg_stats["preserved"]:
                api_log.info(
                    "live_correct(topic=%s): canonical_size=%d corrected=%d preserved=%d",
                    topic_id, len(canon),
                    agg_stats["corrected"], agg_stats["preserved"],
                )
        except Exception as e:
            api_log.error(
                "live_correct failed for topic %s: %s", topic_id, e, exc_info=True
            )
            # Fall through with the LLM's uncorrected bullets — explicit so
            # a future review sees that the path failed rather than silently
            # accepting whatever was in DB.
        summary = {
            "summary": summary_row["summary"],
            "validator_impact": val_imp,
            "delegator_impact": del_imp,
            "builder_impact": bld_imp,
            "model": summary_row["model"],
            "generated_at": summary_row["generated_at"].isoformat() if summary_row["generated_at"] else None,
        }

    changes = [
        {
            "id": c["id"],
            "post_id": c["post_id"],
            "change_type": c["change_type"],
            "old_value": c["old_value"],
            "new_value": c["new_value"],
            "detail": (json.loads(c["detail"]) if isinstance(c["detail"], str) else c["detail"]) if c["detail"] else None,
            "detected_at": c["detected_at"].isoformat(),
        }
        for c in change_rows
    ]

    # Build a real-time-ordered unified timeline that the frontend renders
    # directly. Sources, in priority order:
    #   1. topic_created — synthesized from topic.forum_created_at + OP author
    #   2. reply_added   — one per post_number > 1 (real post.forum_created_at)
    #   3. post_edited   — one per row in mip_post_revisions (real edited_at)
    # Ordered ASC by `at` so the page reads chronologically top-to-bottom.
    timeline: list[dict] = []

    op_post = next((p for p in posts if p["post_number"] == 1), None)
    if topic_row["forum_created_at"]:
        timeline.append({
            "kind": "created",
            "at": topic_row["forum_created_at"].isoformat(),
            "post_number": 1,
            "actor": (op_post["username"] if op_post else topic_row["author_username"]),
            "title": topic_row["title"],
        })

    for p in posts:
        if p["post_number"] == 1:
            continue
        # short preview from raw markdown — strip leading whitespace, trim quote prefix
        raw = (p["raw_markdown"] or "").strip()
        # drop blockquote-style replies: lines starting with '>' before content
        preview_src_lines = [ln for ln in raw.splitlines() if ln.strip() and not ln.strip().startswith(">")]
        preview_src = " ".join(preview_src_lines).strip()
        preview = preview_src[:140] + ("…" if len(preview_src) > 140 else "")
        timeline.append({
            "kind": "reply",
            "at": p["forum_created_at"],
            "post_number": p["post_number"],
            "actor": p["username"],
            "preview": preview,
        })

    for r in revision_rows:
        severity = _classify_edit(r["summary"], r["body_changes_inline"], r["post_number"])
        timeline.append({
            "kind": "edited",
            "severity": severity,  # 'substantive' or 'cosmetic'
            "at": r["edited_at"].isoformat() if r["edited_at"] else None,
            "post_number": r["post_number"],
            "actor": r["editor_username"] or r["author_username"],
            "editor_username": r["editor_username"],
            "author_username": r["author_username"],
            "revision_number": r["revision_number"],
            "summary": r["summary"],
            "edit_reason": r["edit_reason"],
        })

    # Normalize 'at' to ISO strings (some came in as datetime above)
    for ev in timeline:
        if hasattr(ev["at"], "isoformat"):
            ev["at"] = ev["at"].isoformat()
    timeline.sort(key=lambda e: e["at"] or "")

    # Validator link map — only for names that meaningfully appear in any
    # impact bullet. Names short or numeric (e.g. "1" or "12") are excluded
    # to avoid spurious matches.
    validator_link_map: dict[str, dict] = {}
    if summary:
        # Must match collector/governance_llm.py CONTEXT_NETWORK so the link
        # map reflects the same on-chain state the LLM was grounded against.
        directory = _load_validator_directory_for_link_map("mainnet")
        bullet_blob = " ".join(
            (summary.get("validator_impact") or [])
            + (summary.get("delegator_impact") or [])
            + (summary.get("builder_impact") or [])
        )
        bullet_blob_lower = bullet_blob.lower()
        for name_low, meta in directory.items():
            # Skip too-short or too-generic names that would over-match
            if len(name_low) < 4:
                continue
            if name_low.isdigit():
                continue
            # Match using the same boundary semantics the frontend linker uses.
            # The previous \b…\b form silently failed for names ending in
            # punctuation ("CDEF,Inc.") because \b after \. requires a word
            # char to follow, which never happens before a normal sentence
            # space. (^|[^\w/]) and (?=$|[^\w/]) work for any character class.
            pattern = r"(^|[^\w/])" + re.escape(name_low) + r"(?=$|[^\w/])"
            if re.search(pattern, bullet_blob_lower):
                # val_id namespaces are independent across testnet/mainnet —
                # /validator.html defaults to testnet when ?network is absent,
                # so an MIP grounded against mainnet must point to mainnet.
                # Keep this in sync with collector/governance_llm.py:CONTEXT_NETWORK.
                validator_link_map[meta["name"]] = {
                    "val_id": meta["val_id"],
                    "url": f"/validator.html?id={meta['val_id']}&network=mainnet",
                }

    return {
        "topic": _row_to_topic(topic_row),
        "posts": posts,
        "summary": summary,
        "changes": changes,
        "timeline": timeline,
        "validator_link_map": validator_link_map,
    }


@router.get("/changes")
async def governance_changes(
    request: Request,
    since: str | None = Query(None, description="ISO-8601 timestamp; only changes after this"),
    limit: int = Query(50, le=200),
):
    """Flat recent-change log across all MIPs. Used by /governance index for
    activity sidebar and (optionally) for Telegram replay."""
    pool = request.app.state.pool
    sql = """
        SELECT c.id, c.topic_id, c.post_id, c.change_type, c.old_value, c.new_value,
               c.detail, c.detected_at,
               t.mip_number, t.title, t.slug, t.status
        FROM mip_changes c
        JOIN mip_topics t ON t.id = c.topic_id
    """
    args: list[Any] = []
    if since:
        from datetime import datetime
        try:
            ts = datetime.fromisoformat(since.replace("Z", "+00:00"))
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid 'since' timestamp")
        args.append(ts)
        sql += f" WHERE c.detected_at > ${len(args)}"
    args.append(limit)
    sql += f" ORDER BY c.detected_at DESC LIMIT ${len(args)}"

    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, *args)
    return [
        {
            "id": r["id"],
            "topic_id": r["topic_id"],
            "post_id": r["post_id"],
            "change_type": r["change_type"],
            "old_value": r["old_value"],
            "new_value": r["new_value"],
            "detail": (json.loads(r["detail"]) if isinstance(r["detail"], str) else r["detail"]) if r["detail"] else None,
            "detected_at": r["detected_at"].isoformat(),
            "mip_number": r["mip_number"],
            "title": r["title"],
            "slug": r["slug"],
            "status": r["status"],
        }
        for r in rows
    ]
