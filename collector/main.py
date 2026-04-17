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
    """Track current epoch and record in DB."""
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
        # Get validator count from blocks in this epoch range
        val_count = await conn.fetchval("""
            SELECT COUNT(DISTINCT proposer_address)
            FROM blocks WHERE block_number >= $1 AND block_number < $2 AND network = $3
        """, max(0, boundary - 50_000), boundary, NETWORK)

        await conn.execute(
            "INSERT INTO epochs (epoch_number, boundary_block, timestamp, validator_count, network) "
            "VALUES ($1, $2, NOW(), $3, $4) ON CONFLICT DO NOTHING",
            epoch_num, boundary, val_count or 0, NETWORK,
        )
        log.info(f"Epoch {epoch_num} recorded, boundary={boundary}, validators={val_count}")

        # Alert on new epoch
        alert_title = f"New epoch {epoch_num} started at block {boundary}"
        alert_desc = f"Active validators: {val_count}"
        await insert_alert(
            conn,
            alert_type="new_epoch",
            severity="info",
            title=alert_title,
            description=alert_desc,
            data_json={"epoch": epoch_num, "boundary_block": boundary, "validator_count": val_count},
            network=NETWORK,
        )
        await tg_send("new_epoch", "info", alert_title, alert_desc)


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
            inserted_total += len(decoded)

        async with pool.acquire() as conn:
            await upsert_collector_state(conn, "last_stake_block", str(end), NETWORK)
        current = end + 1

    if inserted_total:
        log.info(f"Stake ingest [{NETWORK}]: +{inserted_total} events up to {chain_head}")


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
