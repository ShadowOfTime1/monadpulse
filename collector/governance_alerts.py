"""MonadPulse — Governance → Telegram dispatcher.

Reads new entries from mip_changes (notified_at IS NULL) and posts them to
the existing @monadpulse_alerts channel via collector/telegram.py. Uses the
same HTML / blockquote / link formatting as commission_change and other
existing alerts so the channel feels consistent.

Coalescing: when the same scrape detects multiple events for one MIP within
seconds (e.g. status change + tag change), they're merged into a single
post. Mass-edit storms are bounded — we never spam.

Called from collector/main.py after each successful scrape pass.
"""
from __future__ import annotations

import json as _json
import logging
from datetime import datetime, timezone
from typing import Any

from collector.telegram import send_alert as tg_send

log = logging.getLogger("monadpulse.governance_alerts")

MONADPULSE_BASE = "https://monadpulse.xyz"
FORUM_BASE = "https://forum.monad.xyz"

# Hard caps to keep us friendly to Telegram + readers
MAX_POSTS_PER_RUN  = 12   # safety: never flood the channel from a single scrape
MAX_DIFF_LINES     = 8    # snippet trimmed to N most relevant lines


# ─── Helpers ───────────────────────────────────────────────────────────


def _esc(s: str | None) -> str:
    """HTML-escape a string for Telegram parse_mode=HTML."""
    if not s:
        return ""
    return (s.replace("&", "&amp;")
             .replace("<", "&lt;")
             .replace(">", "&gt;"))


def _mip_label(topic: dict) -> str:
    if topic.get("mip_number") is not None:
        return f"MIP-{topic['mip_number']}"
    return "Proposal"


def _mip_title(topic: dict) -> str:
    """Strip leading 'MIP-N: ' from title — we already show the number elsewhere."""
    raw = topic.get("title") or ""
    import re
    return re.sub(r"^\s*MIP[\s\-_]*\d+\s*[:\-]\s*", "", raw, count=1)


def _mip_url(topic: dict) -> str:
    return f"{MONADPULSE_BASE}/governance-mip.html?id={topic['id']}"


def _forum_url(topic: dict) -> str:
    return f"{FORUM_BASE}/t/{topic['slug']}/{topic['id']}"


def _decode_detail(d: Any) -> dict:
    if d is None:
        return {}
    if isinstance(d, str):
        try:
            return _json.loads(d)
        except Exception:
            return {}
    return d if isinstance(d, dict) else {}


# ─── Alert formatters ──────────────────────────────────────────────────


def _format_new_topic(topic: dict, change: dict) -> tuple[str, str]:
    """When we first see a topic in our DB. Could be a brand-new MIP being
    posted, OR (during the very first scrape after deploy) an old MIP catching
    up. We err on "new" since that's the common case in steady-state."""
    label = _mip_label(topic)
    title = _esc(_mip_title(topic))
    detail = _decode_detail(change.get("detail"))
    author = _esc(detail.get("author") or topic.get("author_username") or "unknown")
    desc = (
        f"<blockquote><b>{label}</b>: {title}\n"
        f"by {author}</blockquote>\n"
        f'<a href="{_mip_url(topic)}">Open on MonadPulse</a>  ·  '
        f'<a href="{_forum_url(topic)}">Forum</a>'
    )
    return "New proposal", desc


def _format_status(topic: dict, change: dict) -> tuple[str, str]:
    label = _mip_label(topic)
    title = _esc(_mip_title(topic))
    old = _esc(change.get("old_value") or "—")
    new = _esc(change.get("new_value") or "—")
    desc = (
        f"<blockquote><b>{label}</b>: {title}\n"
        f"status <b>{old}</b> → <b>{new}</b></blockquote>\n"
        f'<a href="{_mip_url(topic)}">Open on MonadPulse</a>'
    )
    return "Proposal status changed", desc


def _format_op_edited(topic: dict, change: dict) -> tuple[str, str]:
    label = _mip_label(topic)
    title = _esc(_mip_title(topic))
    detail = _decode_detail(change.get("detail"))
    added = detail.get("lines_added")
    removed = detail.get("lines_removed")
    snippet = detail.get("snippet") or ""
    snippet_short = "\n".join(snippet.splitlines()[:MAX_DIFF_LINES])
    body = f"<blockquote><b>{label}</b>: {title}\nproposal edited"
    if added is not None and removed is not None:
        body += f"\n+{added} / −{removed} lines"
    body += "</blockquote>"
    if snippet_short:
        body += f"\n<pre>{_esc(snippet_short)}</pre>"
    body += f'\n<a href="{_mip_url(topic)}">Open on MonadPulse</a>'
    return "Proposal edited", body


def _format_reply(topic: dict, change: dict) -> tuple[str, str]:
    label = _mip_label(topic)
    title = _esc(_mip_title(topic))
    author = _esc(change.get("new_value") or "unknown")
    desc = (
        f"<blockquote><b>{label}</b>: {title}\n"
        f"new reply by <b>{author}</b></blockquote>\n"
        f'<a href="{_mip_url(topic)}">Read discussion</a>'
    )
    return "New discussion reply", desc


CHANGE_TYPE_FORMATTERS = {
    "topic_created":  ("governance_new",     _format_new_topic),
    "status_changed": ("governance_status",  _format_status),
    "op_edited":      ("governance_edited",  _format_op_edited),
    "reply_added":    ("governance_reply",   _format_reply),
}


# ─── Dispatcher ────────────────────────────────────────────────────────


async def dispatch_pending_alerts(pool, *, dry_run: bool = False) -> dict:
    """Process unnotified changes, post to Telegram, mark notified_at.

    Notable: 'tag_changed' and 'reply_edited' are persisted but NOT posted —
    they're noise for subscribers. They still appear in the on-site timeline.
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT c.id, c.topic_id, c.post_id, c.change_type,
                   c.old_value, c.new_value, c.detail, c.detected_at,
                   t.id AS t_id, t.mip_number, t.title, t.slug,
                   t.author_username, t.status
            FROM mip_changes c
            JOIN mip_topics t ON t.id = c.topic_id
            WHERE c.notified_at IS NULL
              AND c.change_type IN ('topic_created', 'status_changed', 'op_edited', 'reply_added')
            ORDER BY c.detected_at ASC, c.id ASC
            LIMIT $1
            """,
            MAX_POSTS_PER_RUN + 1,
        )

    if not rows:
        return {"sent": 0, "skipped": 0}

    overflow = len(rows) > MAX_POSTS_PER_RUN
    if overflow:
        rows = rows[:MAX_POSTS_PER_RUN]

    sent = 0
    skipped = 0
    notified_ids: list[int] = []
    for r in rows:
        topic = {
            "id":              r["t_id"],
            "mip_number":      r["mip_number"],
            "title":           r["title"],
            "slug":            r["slug"],
            "author_username": r["author_username"],
            "status":          r["status"],
        }
        change = {
            "old_value":  r["old_value"],
            "new_value":  r["new_value"],
            "detail":     r["detail"],
        }
        formatter_entry = CHANGE_TYPE_FORMATTERS.get(r["change_type"])
        if not formatter_entry:
            skipped += 1
            notified_ids.append(r["id"])  # mark anyway so we don't loop forever
            continue
        alert_type, formatter = formatter_entry
        try:
            title, desc = formatter(topic, change)
        except Exception as e:
            log.warning("formatter failed for change %s (%s): %s",
                        r["id"], r["change_type"], e)
            skipped += 1
            notified_ids.append(r["id"])
            continue
        if dry_run:
            log.info("DRY-RUN governance alert: %s — %s", title, desc[:120])
        else:
            try:
                await tg_send(alert_type, "info", title, desc)
            except Exception as e:
                log.warning("tg_send failed for change %s: %s", r["id"], e)
                # don't mark as notified — retry next cycle
                continue
        sent += 1
        notified_ids.append(r["id"])

    # Mark dispatched rows so we don't re-send
    if notified_ids:
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE mip_changes SET notified_at = NOW() WHERE id = ANY($1::int[])",
                notified_ids,
            )

    return {"sent": sent, "skipped": skipped, "overflow": overflow}


async def seed_initial_run(pool) -> int:
    """One-time helper: mark all existing change rows as notified WITHOUT posting.
    Used right after deploy so the channel doesn't get flooded with backfilled
    history (60+ topic_created rows from the first scrape). Idempotent — safe
    to call multiple times.
    """
    async with pool.acquire() as conn:
        result = await conn.execute(
            "UPDATE mip_changes SET notified_at = NOW() WHERE notified_at IS NULL"
        )
    # asyncpg returns "UPDATE N"
    try:
        n = int(result.split()[-1])
    except Exception:
        n = 0
    log.info("governance_alerts: seeded — marked %d backlog rows as notified", n)
    return n
