"""MonadPulse — Telegram alert sender."""

import os
import logging
import httpx

log = logging.getLogger("monadpulse.telegram")

BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
CHANNEL_ID = os.environ.get("TELEGRAM_CHANNEL_ID", "")
API_URL = f"https://api.telegram.org/bot{BOT_TOKEN}"

SEVERITY_EMOJI = {
    "info": "\u2139\ufe0f",      # ℹ️
    "warning": "\u26a0\ufe0f",   # ⚠️
    "critical": "\U0001f534",    # 🔴
}

TYPE_EMOJI = {
    "slow_block": "\U0001f422",       # 🐢
    "new_epoch": "\U0001f504",        # 🔄
    "validator_joined": "\u2705",     # ✅
    "validator_left": "\u274c",       # ❌
    "tps_spike": "\u26a1",           # ⚡
    "missed_blocks": "\U0001f6a8",   # 🚨
    "new_version": "\U0001f4e6",     # 📦
    "large_delegation": "\U0001f40b", # 🐋
    # governance — extended for MIP tracking
    "governance_new":      "\U0001f4dc",  # 📜
    "governance_status":   "\U0001f504",  # 🔄
    "governance_edited":   "\u270f\ufe0f",# ✏
    "governance_reply":    "\U0001f4ac",  # 💬
}

NET_EMOJI = {
    "testnet": "\U0001f9ea",  # 🧪
    "mainnet": "\U0001f310",  # 🌐
}


async def send_alert(alert_type: str, severity: str, title: str, description: str = None, data: dict = None):
    """Send an alert to the Telegram channel."""
    if not BOT_TOKEN or not CHANNEL_ID:
        return

    network = os.environ.get("MONADPULSE_NETWORK", "testnet")
    net_emoji = NET_EMOJI.get(network, "")
    net_label = network.upper()

    emoji = TYPE_EMOJI.get(alert_type, SEVERITY_EMOJI.get(severity, "\u2139\ufe0f"))
    # Hashtags: keep only the most useful ones (network + alert_type).
    # Severity #info is noise (it's always info for most alerts) — show the
    # severity tag only when it conveys new information (warning / critical).
    tags = [f"#{network}", f"#{alert_type}"]
    if severity and severity not in ("info",):
        tags.append(f"#{severity}")
    tags_line = " ".join(tags)

    text = f"{emoji} {net_emoji} <b>[{net_label}]</b> {title}"
    if description:
        text += f"\n{description}"
    text += f"\n\n{tags_line}"
    # Generic footer link: only add if description didn't already include one
    # (commission_change / others now include a validator-specific link in the
    # description, so the generic alerts-page link below would be redundant).
    if not description or "monadpulse.xyz" not in (description or ""):
        text += f"\n<a href=\"https://monadpulse.xyz/alerts.html\">View on MonadPulse</a>"

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(
                f"{API_URL}/sendMessage",
                data={
                    "chat_id": CHANNEL_ID,
                    "text": text,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": "true",
                },
            )
            if resp.status_code != 200:
                log.warning(f"Telegram send failed: {resp.text}")
    except Exception as e:
        log.warning(f"Telegram error: {e}")
