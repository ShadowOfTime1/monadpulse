#!/usr/bin/env python3
"""MonadPulse Data Collector — live + backfill block ingestion."""

import asyncio
import logging
import os
import signal
import sys
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

from collector.rpc import MonadRPC
from collector.telegram import send_alert as tg_send
from collector.db import (
    get_pool, close_pool, insert_block, get_last_block_number,
    insert_alert, upsert_collector_state, get_collector_state,
    insert_stake_event,
)
from collector.stake import decode_log as decode_stake_log

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("monadpulse.collector")

RATE_LIMIT = 50  # max concurrent RPC calls for local node
BACKFILL_BLOCKS_LOCAL = 100_000
BACKFILL_BLOCKS_REMOTE = 1_000
ANOMALY_BLOCK_TIME_MS = 1000  # alert if block_time > 1s

# Network from env or arg
NETWORK = os.environ.get("MONADPULSE_NETWORK", "testnet")

shutdown_event = asyncio.Event()


def handle_signal(*_):
    log.info("Shutdown signal received")
    shutdown_event.set()


async def process_batch(rpc: MonadRPC, pool, start: int, end: int) -> int:
    """Fetch and store a batch of blocks. Returns count inserted."""
    tasks = [rpc.get_block(n) for n in range(start, end + 1)]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    blocks = []
    for r in results:
        if isinstance(r, Exception):
            log.warning(f"Block fetch error: {r}")
            continue
        if r is not None:
            blocks.append(r)

    if not blocks:
        return 0

    blocks.sort(key=lambda b: b["block_number"])

    # Compute block_time_ms (diff between consecutive timestamps)
    prev_block = None
    async with pool.acquire() as conn:
        # Get previous block timestamp for first block_time calc
        if blocks:
            row = await conn.fetchrow(
                "SELECT timestamp FROM blocks WHERE block_number = $1",
                blocks[0]["block_number"] - 1,
            )
            if row:
                prev_ts = row["timestamp"].timestamp()
            else:
                prev_ts = None

        for block in blocks:
            if prev_ts is not None:
                block["block_time_ms"] = int((block["timestamp"] - prev_ts) * 1000)
            else:
                block["block_time_ms"] = None
            prev_ts = block["timestamp"]

            await insert_block(conn, block, NETWORK)

            # Anomaly detection: slow block (>5s only, save to DB; Telegram only for >10s)
            if block["block_time_ms"] and block["block_time_ms"] > 5000:
                alert_title = f"Slow block #{block['block_number']}: {block['block_time_ms']}ms"
                alert_desc = f"Proposer: {block['proposer_address']} [{NETWORK}]"
                await insert_alert(
                    conn,
                    alert_type="slow_block",
                    severity="warning",
                    title=alert_title,
                    description=alert_desc,
                    data_json={"block_number": block["block_number"], "block_time_ms": block["block_time_ms"]},
                    network=NETWORK,
                )
                if block["block_time_ms"] > 10000:
                    await tg_send("slow_block", "warning", alert_title, alert_desc)

    return len(blocks)


async def aggregate_hourly(pool):
    """Aggregate hourly gas stats from blocks table — pure SQL upsert."""
    async with pool.acquire() as conn:
        result = await conn.execute(
            "INSERT INTO hourly_gas_stats (hour_timestamp, avg_gas, total_gas, tx_count, avg_base_fee, burned_mon, network) "
            "SELECT date_trunc('hour', timestamp), "
            "COALESCE(AVG(gas_used), 0)::NUMERIC, COALESCE(SUM(gas_used), 0)::NUMERIC, "
            "COALESCE(SUM(tx_count), 0)::INT, COALESCE(AVG(base_fee), 0)::NUMERIC, "
            "COALESCE(SUM(gas_used::NUMERIC * base_fee::NUMERIC / 1e18), 0)::NUMERIC, $1 "
            "FROM blocks WHERE timestamp > NOW() - INTERVAL '2 hours' AND network = $1 "
            "GROUP BY date_trunc('hour', timestamp) "
            "ON CONFLICT (hour_timestamp, network) DO UPDATE SET "
            "avg_gas = EXCLUDED.avg_gas, total_gas = EXCLUDED.total_gas, "
            "tx_count = EXCLUDED.tx_count, avg_base_fee = EXCLUDED.avg_base_fee, "
            "burned_mon = EXCLUDED.burned_mon",
            NETWORK,
        )
        log.info(f"Hourly aggregation [{NETWORK}]: {result}")


async def track_epoch(rpc: MonadRPC, pool):
    """Track current epoch + post a detailed summary of the PREVIOUS epoch."""
    epoch_num = await rpc.get_epoch()
    if epoch_num is None:
        return

    async with pool.acquire() as conn:
        existing = await conn.fetchrow(
            "SELECT epoch_number FROM epochs WHERE epoch_number = $1 AND network = $2", epoch_num, NETWORK
        )
        if existing:
            return

        boundary = epoch_num * 50_000
        prev_lo = max(0, boundary - 50_000)

        # Summary of the PREVIOUS (just-ended) epoch
        summary = await conn.fetchrow("""
            SELECT COUNT(*) AS blocks,
                   COALESCE(SUM(tx_count), 0) AS txs,
                   COALESCE(AVG(block_time_ms), 0)::INT AS avg_bt,
                   COUNT(DISTINCT proposer_address) FILTER (
                       WHERE proposer_address != '0x0000000000000000000000000000000000000000'
                   ) AS val_count,
                   COUNT(*) FILTER (
                       WHERE proposer_address = '0x0000000000000000000000000000000000000000'
                   ) AS null_blocks,
                   COALESCE(AVG(base_fee), 0)::BIGINT AS avg_base_fee,
                   MIN(timestamp) AS started,
                   MAX(timestamp) AS ended
            FROM blocks
            WHERE block_number >= $1 AND block_number < $2 AND network = $3
        """, prev_lo, boundary, NETWORK)

        val_count = summary["val_count"] if summary else 0

        await conn.execute(
            "INSERT INTO epochs (epoch_number, boundary_block, timestamp, validator_count, network) "
            "VALUES ($1, $2, NOW(), $3, $4) ON CONFLICT DO NOTHING",
            epoch_num, boundary, val_count or 0, NETWORK,
        )
        log.info(f"Epoch {epoch_num} recorded, boundary={boundary}, validators={val_count}")

        # Epoch summary (previous epoch just ended at this boundary)
        if summary and summary["blocks"]:
            blocks_n = summary["blocks"]
            txs_n = int(summary["txs"])
            avg_bt = int(summary["avg_bt"] or 0)
            null_n = int(summary["null_blocks"] or 0)
            null_pct = (null_n * 100 / blocks_n) if blocks_n else 0
            base_fee_gwei = int(summary["avg_base_fee"] or 0) // 10**9
            duration_s = int((summary["ended"] - summary["started"]).total_seconds()) if summary["started"] and summary["ended"] else 0
            dur_h, rem = divmod(duration_s, 3600)
            dur_m = rem // 60
            tps = txs_n / duration_s if duration_s > 0 else 0
            digest_title = f"Epoch {epoch_num - 1} summary"
            digest_desc = (
                f"⏱ {dur_h}h {dur_m}m · {blocks_n:,} blocks · avg {avg_bt}ms\n"
                f"💳 {txs_n:,} transactions · {tps:.0f} TPS\n"
                f"👥 {val_count} unique proposers · {null_n:,} null blocks ({null_pct:.1f}%)\n"
                f"⛽ median base fee ~{base_fee_gwei} gwei"
            )
            await insert_alert(
                conn, alert_type="epoch_summary", severity="info",
                title=digest_title, description=digest_desc,
                data_json={
                    "prev_epoch": epoch_num - 1, "blocks": blocks_n, "txs": txs_n,
                    "avg_block_time_ms": avg_bt, "validators": val_count,
                    "null_blocks": null_n, "base_fee_gwei": base_fee_gwei,
                    "duration_sec": duration_s,
                },
                network=NETWORK,
            )
            await tg_send("epoch_summary", "info", digest_title, digest_desc)

        # Short "epoch changed" ping (keeps existing behavior)
        alert_title = f"New epoch {epoch_num} started at block {boundary}"
        alert_desc = f"Active validators: {val_count}"
        await insert_alert(
            conn, alert_type="new_epoch", severity="info",
            title=alert_title, description=alert_desc,
            data_json={"epoch": epoch_num, "boundary_block": boundary, "validator_count": val_count},
            network=NETWORK,
        )
        await tg_send("new_epoch", "info", alert_title, alert_desc)

        # Detect newly-registered validators between previous and current epoch
        await detect_new_validators(rpc, pool, epoch_num)


_SEEN_VAL_IDS: set[int] | None = None


async def detect_new_validators(rpc: MonadRPC, pool, epoch_num: int):
    """Compare current execution valset to the last snapshot, alert on new ids.
    First run just seeds the cache silently."""
    global _SEEN_VAL_IDS
    try:
        # Fetch current execution valset via staking precompile.
        # We bypass the sdk to keep main.py slim; use eth_call directly.
        selector = "7cb074df"  # get_execution_valset(uint64)
        current: set[int] = set()
        start = 0
        while True:
            calldata = "0x" + selector + start.to_bytes(32, "big").hex()
            result = await rpc._call("eth_call", [
                {"to": "0x0000000000000000000000000000000000001000", "data": calldata},
                "latest",
            ])
            if not result or len(result) < 130:
                break
            raw = bytes.fromhex(result[2:])
            done = raw[31] == 1
            next_idx = int.from_bytes(raw[56:64], "big")
            # Decode uint64[] at offset 0x60
            arr_len = int.from_bytes(raw[0x60 + 24:0x60 + 32], "big")
            ids = [int.from_bytes(raw[0x80 + i * 32 + 24:0x80 + i * 32 + 32], "big") for i in range(arr_len)]
            current.update(ids)
            if done:
                break
            start = next_idx
    except Exception as e:
        log.warning(f"new-validator detect: valset fetch err {e}")
        return

    if _SEEN_VAL_IDS is None:
        _SEEN_VAL_IDS = current
        log.info(f"new-validator detect: seeded with {len(current)} ids")
        return

    new_ids = current - _SEEN_VAL_IDS
    _SEEN_VAL_IDS = current
    for vid in sorted(new_ids):
        title = f"New validator #{vid} registered"
        desc = f"Joined at epoch {epoch_num}. Active validators now: {len(current)}."
        async with pool.acquire() as conn:
            await insert_alert(
                conn, alert_type="new_validator", severity="info",
                title=title, description=desc,
                data_json={"validator_id": vid, "epoch": epoch_num}, network=NETWORK,
            )
        await tg_send("new_validator", "info", title, desc)


async def compute_health_scores(pool):
    """Compute validator health scores from block production data."""
    async with pool.acquire() as conn:
        validators = await conn.fetch("""
            SELECT
                proposer_address,
                COUNT(*) AS total_blocks,
                AVG(block_time_ms) AS avg_bt,
                MIN(timestamp) AS first_seen,
                MAX(timestamp) AS last_seen
            FROM blocks
            WHERE timestamp > NOW() - INTERVAL '7 days' AND network = $1
              AND proposer_address != '0x0000000000000000000000000000000000000000'
            GROUP BY proposer_address
            HAVING COUNT(*) >= 5
        """, NETWORK)

        if not validators:
            return

        max_blocks = max(v["total_blocks"] for v in validators)
        now_ts = datetime.now(timezone.utc)

        for v in validators:
            # Uptime: proportion of blocks vs top producer (40%)
            uptime_score = min((v["total_blocks"] / max(max_blocks, 1)) * 100, 100) * 0.4

            # Block time quality: closer to 400ms is better (20%)
            avg_bt = float(v["avg_bt"] or 400)
            bt_quality = max(0, 100 - abs(avg_bt - 400) / 4) * 0.2

            # Age: longer = better, max at 30 days (10%)
            age_days = (now_ts - v["first_seen"].replace(tzinfo=timezone.utc)).days if v["first_seen"] else 0
            age_score = min(age_days / 30, 1) * 100 * 0.1

            # Simplified: no upgrade/stake data yet, give defaults (30%)
            upgrade_score = 75 * 0.15  # assume decent
            stake_score = 50 * 0.15  # neutral

            total = uptime_score + bt_quality + age_score + upgrade_score + stake_score

            await conn.execute("""
                INSERT INTO health_scores
                    (validator_id, timestamp, total_score, uptime_score, miss_score, upgrade_score, stake_score, age_score, network)
                VALUES ($1, NOW(), $2, $3, $4, $5, $6, $7, $8)
            """,
                v["proposer_address"],
                round(total, 1),
                round(uptime_score / 0.4, 1),
                round(bt_quality / 0.2, 1),
                round(upgrade_score / 0.15, 1),
                round(stake_score / 0.15, 1),
                round(age_score / 0.1, 1),
                NETWORK,
            )

        log.info(f"Health scores computed for {len(validators)} validators")


STAKE_LOGS_CHUNK = 500 if os.environ.get("MONADPULSE_NETWORK", "testnet") != "mainnet" else 100
STAKE_BACKFILL_BLOCKS = 200_000 if os.environ.get("MONADPULSE_NETWORK", "testnet") != "mainnet" else 5_000


WHALE_STAKE_MON = 1_000_000  # absolute floor (≥ 1M MON to even consider)
STAKE_CRITICAL_PCT = 15      # alert if action ≥ 15% of validator's stake
ACTIVE_VALIDATOR_STAKE = 10_000_000 * 10 ** 18  # Monad active-set threshold
GET_VALIDATOR_SELECTOR = "2b6d639a"
PRECOMPILE = "0x0000000000000000000000000000000000001000"


async def _current_val_stake(rpc: MonadRPC, val_id: int) -> int | None:
    """Read execution_stake for val_id via staking precompile (returns wei)."""
    try:
        calldata = "0x" + GET_VALIDATOR_SELECTOR + int(val_id).to_bytes(32, "big").hex()
        result = await rpc._call("eth_call", [
            {"to": PRECOMPILE, "data": calldata}, "latest",
        ])
        if not result or len(result) < 130:
            return None
        # Fields layout: address (32) + flags (32) + execution_stake (32) + ...
        # Skip first 64 bytes (addr + flags), next 32 = execution_stake
        execution_stake_hex = result[2 + 64 * 2: 2 + 96 * 2]
        return int(execution_stake_hex, 16)
    except Exception:
        return None


def _lookup_val_name(val_id: int) -> str | None:
    """Look up a human-readable validator name from validator_directory file."""
    try:
        import json as _json
        p = Path(f"/opt/monadpulse/validator_directory_{NETWORK}.json")
        if not p.exists():
            return None
        for e in _json.loads(p.read_text()):
            if e.get("val_id") == val_id:
                return e.get("name")
    except Exception:
        pass
    return None


async def _maybe_alert_stake_event(conn, ev: dict, rpc: MonadRPC | None = None) -> None:
    """Anti-spam filter for stake events. Only alerts when truly informative:
      • commission change — always
      • delegate/undelegate — only if ≥1M MON AND (≥15% of validator stake OR
        drops validator below ACTIVE_VALIDATOR_STAKE threshold)"""
    et = ev["event_type"]
    amount = int(ev["amount"])

    if et == "commission_changed":
        new_pct = amount / (10 ** 16)
        title = f"Validator #{ev['validator_id']} commission → {new_pct:.2f}%"
        await insert_alert(
            conn, alert_type="commission_change", severity="info",
            title=title, description=None,
            data_json={"validator_id": ev["validator_id"], "new_rate_wei": amount},
            network=NETWORK,
        )
        await tg_send("commission_change", "info", title)
        return

    if et not in ("delegate", "undelegate"):
        return

    amount_mon = amount / 10 ** 18
    if amount_mon < WHALE_STAKE_MON:
        return  # under absolute floor

    val_id = int(ev["validator_id"])
    stake_wei = await _current_val_stake(rpc, val_id) if rpc else None
    stake_mon = stake_wei / 10 ** 18 if stake_wei else None
    pct = (amount_mon / stake_mon * 100) if stake_mon else 0

    # Critical: undelegation knocks validator below active threshold
    drops_below_active = False
    if et == "undelegate" and stake_wei is not None:
        post_stake_wei = stake_wei - amount
        if stake_wei >= ACTIVE_VALIDATOR_STAKE and post_stake_wei < ACTIVE_VALIDATOR_STAKE:
            drops_below_active = True

    # Significance filter — noise reduction
    if not drops_below_active and pct < STAKE_CRITICAL_PCT:
        return  # routine rebalance, skip

    # Build alert
    name = _lookup_val_name(val_id) or f"#{val_id}"
    severity = "critical" if drops_below_active else "info"
    if drops_below_active:
        emoji = "🚨 Validator exit risk"
    elif et == "delegate":
        emoji = "🐋 Large delegation"
    else:
        emoji = "🦈 Large undelegation"

    title = f"{emoji}: {amount_mon:,.0f} MON ({pct:.0f}% of stake)"
    desc_lines = [
        f"Validator: {name} (id {val_id})",
        f"Delegator: {ev['delegator']}",
    ]
    if stake_mon is not None:
        post_mon = stake_mon + (amount_mon if et == "delegate" else -amount_mon)
        desc_lines.append(f"Stake: {stake_mon:,.0f} → {post_mon:,.0f} MON")
    if drops_below_active:
        desc_lines.append(
            f"⚠ Falls below ACTIVE_VALIDATOR_STAKE ({ACTIVE_VALIDATOR_STAKE // 10**18:,} MON) "
            "— validator will exit active set next epoch."
        )
    desc_lines.append(f"block {ev['block_number']}")
    desc = "\n".join(desc_lines)

    await insert_alert(
        conn, alert_type="whale_stake", severity=severity,
        title=title, description=desc,
        data_json={
            "event_type": et, "validator_id": val_id,
            "delegator": ev["delegator"], "amount_mon": amount_mon,
            "stake_mon": stake_mon, "pct": pct,
            "drops_below_active": drops_below_active,
            "block": ev["block_number"],
        },
        network=NETWORK,
    )
    await tg_send("whale_stake", severity, title, desc)


async def ingest_stake_events(rpc: MonadRPC, pool):
    """Poll staking precompile logs and persist decoded events."""
    async with pool.acquire() as conn:
        cursor_str = await get_collector_state(conn, "last_stake_block", NETWORK)
        chain_head = await rpc.get_block_number()
        if cursor_str is None:
            cursor = max(0, chain_head - STAKE_BACKFILL_BLOCKS)
            log.info(f"Stake ingest [{NETWORK}]: first run, backfilling from {cursor}")
        else:
            cursor = int(cursor_str) + 1

    if cursor > chain_head:
        return

    inserted_total = 0
    current = cursor
    while current <= chain_head:
        end = min(current + STAKE_LOGS_CHUNK - 1, chain_head)
        try:
            raw = await rpc.get_stake_logs(current, end)
        except Exception as e:
            log.warning(f"Stake log fetch {current}-{end} failed: {e}")
            break

        decoded = [d for d in (decode_stake_log(l) for l in raw) if d]
        if decoded:
            async with pool.acquire() as conn:
                for ev in decoded:
                    await insert_stake_event(conn, ev, NETWORK)
                    await _maybe_alert_stake_event(conn, ev, rpc)
            inserted_total += len(decoded)

        async with pool.acquire() as conn:
            await upsert_collector_state(conn, "last_stake_block", str(end), NETWORK)
        current = end + 1

    if inserted_total:
        log.info(f"Stake ingest [{NETWORK}]: +{inserted_total} events up to {chain_head}")


async def detect_offline_validators(pool):
    """Flag validators that are in execution valset but produced 0 blocks in
    the last 24h. One alert per validator per 24h (dedup via alerts table)."""
    global _SEEN_VAL_IDS
    if _SEEN_VAL_IDS is None:
        return  # no seed yet

    async with pool.acquire() as conn:
        # Blocks produced per auth_address in last 24h
        # Note: on testnet miner != auth; we check via directory file if present.
        # Fallback: count via proposer_address == auth (works for mainnet + some testnet cases)
        active_directory_path = Path(f"/opt/monadpulse/validator_directory_{NETWORK}.json")
        if not active_directory_path.exists():
            return
        import json as _json
        directory = _json.loads(active_directory_path.read_text())
        auth_to_vid = {e["auth"]: e["val_id"] for e in directory if e.get("auth")}
        vid_to_name = {e["val_id"]: e.get("name") for e in directory}

        rows = await conn.fetch("""
            SELECT proposer_address, COUNT(*) AS blk
            FROM blocks
            WHERE network = $1
              AND timestamp > NOW() - INTERVAL '24 hours'
              AND proposer_address != '0x0000000000000000000000000000000000000000'
            GROUP BY proposer_address
        """, NETWORK)
        active_auths = {r["proposer_address"] for r in rows if r["blk"] > 0}

        for vid in _SEEN_VAL_IDS:
            # Find auth for this vid
            auth = next((a for a, v in auth_to_vid.items() if v == vid), None)
            if not auth or auth in active_auths:
                continue
            # Dedup — already alerted within 24h?
            already = await conn.fetchval("""
                SELECT 1 FROM alerts
                WHERE alert_type = 'validator_offline' AND network = $1
                  AND data_json->>'validator_id' = $2
                  AND timestamp > NOW() - INTERVAL '24 hours'
                LIMIT 1
            """, NETWORK, str(vid))
            if already:
                continue
            name = vid_to_name.get(vid) or f"#{vid}"
            title = f"⚠ Validator {name} offline — 0 blocks in 24h"
            desc = f"val_id={vid}, auth={auth[:10]}…{auth[-4:]}"
            await insert_alert(
                conn, alert_type="validator_offline", severity="warning",
                title=title, description=desc,
                data_json={"validator_id": str(vid), "auth": auth},
                network=NETWORK,
            )
            await tg_send("validator_offline", "warning", title, desc)


async def detect_tps_spike(pool):
    """Detect TPS spike — alert if >3x 24h average, max 1 alert per 6 hours."""
    async with pool.acquire() as conn:
        # Check cooldown — skip if alerted in last 6 hours
        last_alert = await conn.fetchval(
            "SELECT MAX(timestamp) FROM alerts WHERE alert_type='tps_spike' AND network=$1",
            NETWORK,
        )
        if last_alert:
            from datetime import timezone
            age = (datetime.now(timezone.utc) - last_alert.replace(tzinfo=timezone.utc)).total_seconds()
            if age < 21600:  # 6 hours
                return

        row = await conn.fetchrow("""
            WITH avg_24h AS (
                SELECT COALESCE(AVG(tx_count), 0) AS avg_tx
                FROM blocks WHERE timestamp > NOW() - INTERVAL '24 hours' AND network = $1
            ),
            last_5min AS (
                SELECT COALESCE(AVG(tx_count), 0) AS recent_tx
                FROM blocks WHERE timestamp > NOW() - INTERVAL '5 minutes' AND network = $1
            )
            SELECT avg_24h.avg_tx, last_5min.recent_tx
            FROM avg_24h, last_5min
        """, NETWORK)
        if row and row["avg_tx"] > 0 and row["recent_tx"] > row["avg_tx"] * 3:
            alert_title = f"TPS spike [{NETWORK}]: {row['recent_tx']:.0f} tx/block (avg: {row['avg_tx']:.0f})"
            await insert_alert(conn, "tps_spike", "warning", alert_title, network=NETWORK)
            await tg_send("tps_spike", "warning", alert_title)
            log.info(alert_title)


_last_known_release = None

async def check_new_release(pool):
    """Check GitHub for new Monad releases and alert."""
    global _last_known_release
    import httpx
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(
                "https://api.github.com/repos/category-labs/monad/releases?per_page=1",
                headers={"Accept": "application/vnd.github+json"},
            )
            releases = r.json()
        if not releases or not isinstance(releases, list):
            return
        latest = releases[0]
        tag = latest.get("tag_name", "")
        if not tag:
            return
        if _last_known_release is None:
            _last_known_release = tag
            return
        if tag != _last_known_release:
            _last_known_release = tag
            alert_title = f"New Monad release: {tag}"
            alert_desc = f"{latest.get('name', '')} — Update within 48h for VDP compliance"
            async with pool.acquire() as conn:
                await insert_alert(conn, "new_version", "critical", alert_title, alert_desc, network=NETWORK)
            await tg_send("new_version", "critical", alert_title, alert_desc)
            log.info(f"New release detected: {tag}")
    except Exception as e:
        log.warning(f"Release check error: {e}")


async def run():
    if NETWORK == "mainnet":
        rpc_url = os.environ.get("MAINNET_RPC", "https://rpc.monad.xyz")
        rate = 5
        batch_size = 10
        backfill = BACKFILL_BLOCKS_REMOTE
    else:
        rpc_url = os.environ.get("TESTNET_RPC", "http://localhost:8080")
        rate = RATE_LIMIT
        batch_size = 100
        backfill = BACKFILL_BLOCKS_LOCAL
    rpc = MonadRPC(rpc_url, rate_limit=rate)
    pool = await get_pool()

    log.info(f"Collector started [{NETWORK}], RPC: {rpc_url}")

    try:
        chain_head = await rpc.get_block_number()
        log.info(f"Chain head: {chain_head}")

        async with pool.acquire() as conn:
            db_head = await get_last_block_number(conn, NETWORK)

        if db_head is None:
            # First run — backfill
            start = max(0, chain_head - backfill)
            log.info(f"First run, backfilling from {start} to {chain_head}")
        else:
            start = db_head + 1
            log.info(f"Resuming from block {start}")

        # Backfill phase
        current = start
        while current <= chain_head and not shutdown_event.is_set():
            batch_end = min(current + batch_size - 1, chain_head)
            count = await process_batch(rpc, pool, current, batch_end)
            if count > 0:
                progress = ((current - start) / max(chain_head - start, 1)) * 100
                log.info(f"Backfill: {current}-{batch_end} ({count} blocks, {progress:.1f}%)")
                async with pool.acquire() as conn:
                    await upsert_collector_state(conn, "last_block", str(batch_end), NETWORK)
            current = batch_end + 1

        if shutdown_event.is_set():
            return

        log.info("Backfill complete, running initial aggregations...")
        await aggregate_hourly(pool)
        await track_epoch(rpc, pool)
        await compute_health_scores(pool)
        log.info("Entering live mode")

        # Live mode
        last_aggregate = asyncio.get_event_loop().time()
        last_epoch_check = 0
        last_health_calc = 0
        last_tps_check = 0
        last_stake_ingest = 0
        last_offline_check = 0
        while not shutdown_event.is_set():
            try:
                chain_head = await rpc.get_block_number()
                async with pool.acquire() as conn:
                    db_head = await get_last_block_number(conn, NETWORK)

                if db_head is None:
                    db_head = chain_head - 1

                if chain_head > db_head:
                    # Detect epoch boundary crossing: any block in [db_head+1..chain_head]
                    # that is a multiple of 50000 → trigger immediate epoch refresh.
                    epoch_boundary_crossed = False
                    for bn in range(db_head + 1, chain_head + 1):
                        if bn % 50_000 == 0:
                            epoch_boundary_crossed = True
                            break

                    for batch_start in range(db_head + 1, chain_head + 1, batch_size):
                        if shutdown_event.is_set():
                            break
                        batch_end = min(batch_start + batch_size - 1, chain_head)
                        count = await process_batch(rpc, pool, batch_start, batch_end)
                        if count > 0:
                            log.info(f"Live: +{count} blocks up to {batch_end}")
                            async with pool.acquire() as conn:
                                await upsert_collector_state(conn, "last_block", str(batch_end), NETWORK)

                    # Refresh epoch state immediately on boundary crossing
                    if epoch_boundary_crossed:
                        log.info(f"Epoch boundary crossed — refreshing epoch state [{NETWORK}]")
                        await track_epoch(rpc, pool)
                        last_epoch_check = asyncio.get_event_loop().time()
                        # Rebuild validator name + delegation graph maps
                        # (fire-and-forget). Only testnet collector triggers —
                        # both scripts handle both networks.
                        if NETWORK == "testnet":
                            import subprocess
                            for script in (
                                "/opt/monadpulse/scripts/rebuild_validator_names.py",
                                "/opt/monadpulse/scripts/rebuild_delegation_graph.py",
                            ):
                                subprocess.Popen(
                                    [script],
                                    stdout=subprocess.DEVNULL,
                                    stderr=subprocess.DEVNULL,
                                    start_new_session=True,
                                )
                            log.info("Triggered validator-names + delegation-graph rebuilds (background)")

                now = asyncio.get_event_loop().time()

                # Hourly aggregation
                if now - last_aggregate > 3600:
                    await aggregate_hourly(pool)
                    last_aggregate = now

                # Epoch tracking — fallback every 60s (boundary detection is primary)
                if now - last_epoch_check > 60:
                    await track_epoch(rpc, pool)
                    last_epoch_check = now

                # Health scores — every hour
                if now - last_health_calc > 3600:
                    await compute_health_scores(pool)
                    if NETWORK == "testnet":  # only check once, not from both collectors
                        await check_new_release(pool)
                    last_health_calc = now

                # TPS spike detection — every 5 minutes
                if now - last_tps_check > 300:
                    await detect_tps_spike(pool)
                    last_tps_check = now

                # Offline-validator check — daily (24h), testnet only to avoid dupes
                if NETWORK == "testnet" and now - last_offline_check > 86400:
                    try:
                        await detect_offline_validators(pool)
                    except Exception as e:
                        log.warning(f"Offline detect error: {e}")
                    last_offline_check = now

                # Stake event ingestion — every 60 seconds
                if now - last_stake_ingest > 60:
                    try:
                        await ingest_stake_events(rpc, pool)
                    except Exception as e:
                        log.warning(f"Stake ingest error: {e}")
                    last_stake_ingest = now

            except Exception as e:
                log.error(f"Live loop error: {e}")

            await asyncio.sleep(1)

    except Exception as e:
        log.error(f"Collector fatal error: {e}", exc_info=True)
    finally:
        await rpc.close()
        await close_pool()
        log.info("Collector stopped")


def main():
    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)
    asyncio.run(run())


if __name__ == "__main__":
    main()
