#!/usr/bin/env python3
"""Daily MonadPulse digest — once-per-day Telegram post at 06:00 UTC (09:00 MSK).

Summarises the last 24h network-wide:
  • top 5 block producers
  • top 5 whale stake events
  • network health (null proposer rate, avg block time, gas burned)
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, "/opt/monadpulse")

import asyncio
import asyncpg

from api.routes.names import _load as load_names  # re-use name loader

DB_URL = os.environ.get("DATABASE_URL") or open("/opt/monadpulse/.env").read().split("DATABASE_URL=")[1].split()[0]
NOTIFY_CMD = "/opt/monad/scripts/notify.sh"
MON = 10 ** 18


def send(msg: str) -> None:
    subprocess.run([NOTIFY_CMD, msg], check=False)


async def build(network: str) -> str:
    pool = await asyncpg.create_pool(DB_URL, min_size=1, max_size=2)
    try:
        async with pool.acquire() as conn:
            # Top 5 producers
            top = await conn.fetch("""
                SELECT proposer_address AS addr, COUNT(*) AS blocks
                FROM blocks
                WHERE network = $1
                  AND timestamp > NOW() - INTERVAL '24 hours'
                  AND proposer_address != '0x0000000000000000000000000000000000000000'
                GROUP BY proposer_address
                ORDER BY blocks DESC
                LIMIT 5
            """, network)

            # Network health
            stats = await conn.fetchrow("""
                SELECT COUNT(*) AS total_blocks,
                       COUNT(*) FILTER (WHERE proposer_address = '0x0000000000000000000000000000000000000000') AS null_blocks,
                       COALESCE(SUM(tx_count), 0) AS total_tx,
                       COALESCE(SUM(gas_used::NUMERIC * base_fee::NUMERIC / 1e18), 0)::NUMERIC AS burned_mon,
                       COUNT(DISTINCT proposer_address) FILTER (
                           WHERE proposer_address != '0x0000000000000000000000000000000000000000'
                       ) AS unique_prop
                FROM blocks
                WHERE network = $1 AND timestamp > NOW() - INTERVAL '24 hours'
            """, network)

            # Top 5 whale stake events
            whales = await conn.fetch("""
                SELECT event_type, validator_id, delegator, amount::NUMERIC AS amt, block_number
                FROM stake_events
                WHERE network = $1
                  AND timestamp > NOW() - INTERVAL '24 hours'
                  AND event_type IN ('delegate', 'undelegate')
                ORDER BY amount DESC
                LIMIT 5
            """, network)
    finally:
        await pool.close()

    names = load_names(network)

    def short(addr: str) -> str:
        return f"{addr[:10]}…{addr[-4:]}"

    def nm(addr: str) -> str:
        return names.get(addr.lower()) or short(addr)

    lines = [f"📡 MonadPulse daily digest — {network}"]

    # Health
    if stats and stats["total_blocks"]:
        null_pct = (stats["null_blocks"] / stats["total_blocks"]) * 100
        lines.append(
            f"🩺 Health: {stats['total_blocks']:,} blocks · {int(stats['total_tx']):,} tx · "
            f"{stats['unique_prop']} unique proposers · null {null_pct:.1f}%"
        )
        lines.append(f"🔥 Burned ~{float(stats['burned_mon']):,.2f} MON (base fee × gas)")

    # Top producers
    if top:
        lines.append("")
        lines.append("🏆 Top block producers (24h):")
        for i, r in enumerate(top, 1):
            lines.append(f"  {i}. {nm(r['addr'])} — {r['blocks']:,} blocks")

    # Whale events
    whales_big = [w for w in whales if float(w["amt"]) / MON >= 100_000]
    if whales_big:
        lines.append("")
        lines.append("🐋 Largest stake moves (24h):")
        for w in whales_big:
            amt_mon = float(w["amt"]) / MON
            arrow = "+" if w["event_type"] == "delegate" else "−"
            lines.append(
                f"  {arrow}{amt_mon:,.0f} MON → val #{w['validator_id']} by {short(w['delegator'])}"
            )

    return "\n".join(lines)


async def main() -> int:
    for net in ("testnet", "mainnet"):
        try:
            msg = await build(net)
            send(msg)
        except Exception as e:
            send(f"⚠ daily-digest [{net}] error: {e}")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
