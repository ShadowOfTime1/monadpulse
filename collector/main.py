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


# ─── Upgrade score (local node only; others use a neutral placeholder) ──
# For the validator whose auth == MONADPULSE_LOCAL_AUTH we can compare the
# live web3_clientVersion with the latest GitHub release and produce a real
# upgrade score. For every other validator we can't see their client
# version without their own endpoint — leave the placeholder (75).
import time as _time_mod

LOCAL_AUTH = (os.environ.get("MONADPULSE_LOCAL_AUTH") or "").lower()
_LOCAL_VERSION_CACHE: dict = {"version": None, "fetched_at": 0}


async def _get_local_version(rpc) -> str | None:
    if _LOCAL_VERSION_CACHE["version"] and (_time_mod.time() - _LOCAL_VERSION_CACHE["fetched_at"]) < 300:
        return _LOCAL_VERSION_CACHE["version"]
    try:
        v = await rpc.get_client_version()
        # e.g. "Monad/0.14.1" or raw "0.14.1"
        version = v.split("/")[-1].strip()
        _LOCAL_VERSION_CACHE["version"] = version
        _LOCAL_VERSION_CACHE["fetched_at"] = _time_mod.time()
        return version
    except Exception as e:
        log.warning(f"local version fetch err: {e}")
        return None


def _upgrade_pct(current: str | None, latest: str | None) -> float:
    """Compare semver-ish strings. 100 = at or above latest; 75 = one minor
    behind; 50 = two minor behind; 25 = major behind; 75 default on parse
    failure so we stay neutral when we can't tell."""
    if not current or not latest:
        return 75.0
    def parse(v):
        v = v.lstrip("v").split("-")[0]  # drop pre-release suffix
        return tuple(int(p) for p in v.split(".") if p.isdigit())
    try:
        c = parse(current); l = parse(latest)
        if not c or not l:
            return 75.0
        if c >= l:
            return 100.0
        if len(c) >= 2 and len(l) >= 2:
            if c[0] < l[0]:
                return 25.0
            diff = l[1] - c[1]
            if diff <= 0:
                return 100.0
            if diff == 1:
                return 75.0
            return 50.0
        return 75.0
    except Exception:
        return 75.0


# ─── First-active lookup (Reward-events scan) ────────────────────────────
# compute_health_scores wants a TRUE first-active timestamp per validator to
# compute the Age component honestly. Our blocks table only sees data from
# when the collector started, and on testnet all blocks with block.miner=0x0
# lose their attribution until backfill_null_proposers resolves them — so
# MIN(timestamp) from blocks under-reports validator age. Instead we scan
# eth_getLogs for the staking-precompile Reward event (topic[1] = val_id)
# and take the earliest block number emitted. Cached to disk so we pay the
# scan cost once per validator.

FIRST_ACTIVE_CACHE_PATH = Path(f"/opt/monadpulse/first_active_{os.environ.get('MONADPULSE_NETWORK', 'testnet')}.json")
REWARD_EVENT_SIG = "0x3a420a01486b6b28d6ae89c51f5c3bde3e0e74eecbb646a0c481ccba3aae3754"
STAKING_PRECOMPILE = "0x0000000000000000000000000000000000001000"
_FIRST_ACTIVE_MEM: dict[int, dict] = {}
_FIRST_ACTIVE_LOADED = False
_FIRST_ACTIVE_FILL_RUNNING = False  # guard so duplicate compute cycles don't stack scans


def _load_first_active_cache() -> None:
    global _FIRST_ACTIVE_MEM, _FIRST_ACTIVE_LOADED
    if _FIRST_ACTIVE_LOADED:
        return
    if FIRST_ACTIVE_CACHE_PATH.exists():
        try:
            import json as _json
            _FIRST_ACTIVE_MEM = {
                int(k): v for k, v in _json.loads(FIRST_ACTIVE_CACHE_PATH.read_text()).items()
            }
        except Exception as e:
            log.warning(f"first-active cache load err: {e}")
    _FIRST_ACTIVE_LOADED = True


def _save_first_active_cache() -> None:
    import json as _json
    try:
        FIRST_ACTIVE_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp = FIRST_ACTIVE_CACHE_PATH.with_suffix(".json.tmp")
        tmp.write_text(_json.dumps({str(k): v for k, v in _FIRST_ACTIVE_MEM.items()}))
        tmp.replace(FIRST_ACTIVE_CACHE_PATH)
    except Exception as e:
        log.warning(f"first-active cache save err: {e}")


async def _scan_first_reward_event(val_id: int, rpc=None) -> dict | None:
    """Reward-event scan. Returns {'block': N, 'timestamp': T} or None.
    Chunked backward scan; stops after 5 empty chunks past the last hit.
    Chunk sizes match the RPC's eth_getLogs block-range cap — bigger values
    get rejected with 'block range too large' and produce a false-negative."""
    import httpx as _httpx
    chunk = 100 if NETWORK == "mainnet" else 1000
    EMPTY_STREAK = 5
    HARD_CAP = 2_000_000
    val_topic = "0x" + format(val_id, "064x")
    url = os.environ.get("MONADPULSE_RPC_URL") or os.environ.get("RPC_URL") or "http://localhost:8080"

    async with _httpx.AsyncClient(timeout=10) as client:
        r = await client.post(url, json={
            "jsonrpc": "2.0", "method": "eth_blockNumber", "params": [], "id": 1,
        })
        latest = int(r.json().get("result", "0x0"), 16)
        if latest <= 0:
            return None

        # Forward scan from (latest − HARD_CAP). First chunk with a Reward
        # event for val_id gives us the earliest visible block — short-
        # circuit immediately. For active validators this terminates after
        # a handful of chunks; for never-active ones it scans the whole
        # HARD_CAP window (~2000 calls testnet) and returns None.
        start = max(0, latest - HARD_CAP)
        earliest = None
        cur_lo = start
        while cur_lo <= latest:
            cur_hi = min(latest, cur_lo + chunk - 1)
            try:
                rr = await client.post(url, json={
                    "jsonrpc": "2.0", "method": "eth_getLogs",
                    "params": [{
                        "address": STAKING_PRECOMPILE,
                        "fromBlock": hex(cur_lo), "toBlock": hex(cur_hi),
                        "topics": [REWARD_EVENT_SIG, val_topic],
                    }], "id": 1,
                })
                logs = (rr.json() or {}).get("result") or []
            except Exception:
                logs = []

            if logs:
                earliest = min(int(lg["blockNumber"], 16) for lg in logs)
                break

            cur_lo = cur_hi + 1

        if earliest is None:
            return None

        r = await client.post(url, json={
            "jsonrpc": "2.0", "method": "eth_getBlockByNumber",
            "params": [hex(earliest), False], "id": 1,
        })
        ts = int(r.json()["result"]["timestamp"], 16)
    return {"block": earliest, "timestamp": ts}


async def fill_first_active_cache(val_ids: list[int], rpc, max_new: int = 10) -> None:
    """Progressive enhancement — on each compute cycle, resolve up to max_new
    new validators that don't have first_active cached yet. Expensive
    (one Reward-event scan per call) so bounded per cycle and guarded so
    overlapping compute cycles don't stack parallel scans."""
    global _FIRST_ACTIVE_FILL_RUNNING
    if _FIRST_ACTIVE_FILL_RUNNING:
        return
    _FIRST_ACTIVE_FILL_RUNNING = True
    try:
        _load_first_active_cache()
        unresolved = [vid for vid in val_ids if vid not in _FIRST_ACTIVE_MEM][:max_new]
        log.info(f"first-active fill start: {len(unresolved)} to resolve")
        if not unresolved:
            return
        any_changed = False
        for vid in unresolved:
            try:
                res = await asyncio.wait_for(_scan_first_reward_event(vid, rpc), timeout=120)
                if res:
                    _FIRST_ACTIVE_MEM[vid] = res
                    log.info(f"first-active resolved vid={vid}: block {res['block']}, ts {res['timestamp']}")
                else:
                    _FIRST_ACTIVE_MEM[vid] = {"block": None, "timestamp": None}
                any_changed = True
            except asyncio.TimeoutError:
                log.warning(f"first-active scan timeout vid={vid}")
            except Exception as e:
                log.warning(f"first-active scan err vid={vid}: {e!r}")
        if any_changed:
            _save_first_active_cache()
            log.info(f"first-active cache filled {len(unresolved)} validators "
                     f"(total cached: {len(_FIRST_ACTIVE_MEM)})")
    finally:
        _FIRST_ACTIVE_FILL_RUNNING = False


async def compute_health_scores(pool, rpc=None):
    """Compute validator health scores from block production data.

    Uptime formula (post-Matthias-feedback 2026-04-20):
        expected = (network_blocks_since_first_active / active_validators_count)
        uptime  = min(100, actual_blocks / expected * 100)

    The old formula ("blocks vs top-1 producer") unfairly penalized newer
    validators — they physically couldn't accumulate as many blocks as a
    day-1 validator in a rolling 7d window. Now we normalize by time-alive
    so a validator active 2 days, producing at expected rate, reads as
    ~100% instead of ~28%.

    Cross-miner clustering: on mainnet (and some testnet cases) one
    validator rotates across several ephemeral block.miner addresses.
    Each was previously scored as a separate "validator", always with a
    fresh first_seen and low block count. We cluster by canonical name
    from validator_names_{network}.json so Backpack's three miner rotations
    contribute to one health score keyed on its auth address."""
    import json as _json
    names_path = Path(f"/opt/monadpulse/validator_names_{NETWORK}.json")
    directory_path = Path(f"/opt/monadpulse/validator_directory_{NETWORK}.json")
    names_map: dict = {}
    auth_by_name: dict = {}
    if names_path.exists():
        try:
            names_map = {k.lower(): v for k, v in _json.loads(names_path.read_text()).items()}
        except Exception:
            pass
    if directory_path.exists():
        try:
            for e in _json.loads(directory_path.read_text()):
                if e.get("name") and e.get("auth"):
                    auth = e["auth"].lower()
                    auth_by_name[e["name"]] = auth
                    # Also let the auth address itself resolve to the canonical
                    # name — otherwise a row with proposer_address=<auth> (e.g.
                    # after null-miner backfill rewrote it to auth) wouldn't
                    # cluster with the rows under real miner addresses.
                    names_map.setdefault(auth, e["name"])
        except Exception:
            pass

    # Retire rows keyed on miner addrs that now cluster into an auth entry.
    # Without this the API's "latest per validator_id" query keeps returning
    # one row per miner alongside the canonical auth-keyed row, re-splitting
    # the validator in the UI after it was just clustered.
    redundant_miner_ids = set()
    for addr, name in names_map.items():
        canonical = auth_by_name.get(name)
        if canonical and canonical != addr:
            redundant_miner_ids.add(addr)

    async with pool.acquire() as conn:
        if redundant_miner_ids:
            await conn.execute(
                "DELETE FROM health_scores WHERE network = $1 AND validator_id = ANY($2::text[])",
                NETWORK, list(redundant_miner_ids),
            )

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

        # Network-wide baseline for the same 7d window — used to compute
        # per-validator expected block counts.
        net_row = await conn.fetchrow("""
            SELECT
                COUNT(*) AS total,
                MIN(timestamp) AS first_ts
            FROM blocks
            WHERE timestamp > NOW() - INTERVAL '7 days' AND network = $1
              AND proposer_address != '0x0000000000000000000000000000000000000000'
        """, NETWORK)

        if not validators or not net_row or not net_row["total"]:
            return

        # Cluster proposers into validators by name (auth + all miner rotations).
        # Each cluster aggregates blocks, earliest first_seen, averaged block time.
        # Unnamed proposers (no match in names_map) form singleton clusters —
        # they still benefit from the new time-normalized formula.
        clusters: dict[str, dict] = {}
        for v in validators:
            addr = v["proposer_address"].lower()
            name = names_map.get(addr)
            canonical_addr = auth_by_name.get(name) if name else None
            key = canonical_addr or name or addr
            c = clusters.setdefault(key, {
                "validator_id": canonical_addr or addr,  # prefer auth for display
                "total_blocks": 0,
                "weighted_bt_sum": 0.0,
                "first_seen": None,
                "last_seen": None,
                "name": name,
            })
            c["total_blocks"] += int(v["total_blocks"])
            if v["avg_bt"] is not None:
                c["weighted_bt_sum"] += float(v["avg_bt"]) * int(v["total_blocks"])
            if v["first_seen"] is not None:
                c["first_seen"] = v["first_seen"] if c["first_seen"] is None else min(c["first_seen"], v["first_seen"])
            if v["last_seen"] is not None:
                c["last_seen"] = v["last_seen"] if c["last_seen"] is None else max(c["last_seen"], v["last_seen"])

        now_ts = datetime.now(timezone.utc)
        network_total_7d = int(net_row["total"])
        active_validators = len(clusters)

        # Build addr→val_id reverse map from directory, then attach val_id to
        # each cluster (when resolvable) so we can look up the TRUE first-active
        # via the Reward-events cache — far more accurate than MIN(timestamp)
        # from our own collector window.
        addr_to_vid: dict[str, int] = {}
        try:
            directory = _json.loads(directory_path.read_text()) if directory_path.exists() else []
            for e in directory:
                if e.get("auth") and e.get("val_id") is not None:
                    addr_to_vid[e["auth"].lower()] = int(e["val_id"])
        except Exception:
            pass

        _load_first_active_cache()
        for c in clusters.values():
            vid = addr_to_vid.get((c["validator_id"] or "").lower())
            c["val_id"] = vid
            cached = _FIRST_ACTIVE_MEM.get(vid) if vid is not None else None
            if cached and cached.get("timestamp"):
                c["first_active_ts"] = cached["timestamp"]
            else:
                c["first_active_ts"] = None
        # "Slot size" = average expected blocks per validator per unit time.
        # Per-unit-time rate = network_blocks / 7 days.
        seconds_in_7d = 7 * 24 * 3600
        network_rate = network_total_7d / seconds_in_7d  # blocks per second network-wide
        per_validator_rate = network_rate / max(active_validators, 1)

        for c in clusters.values():
            first_seen = c["first_seen"]
            if first_seen is None:
                continue
            first_seen_aware = first_seen if first_seen.tzinfo else first_seen.replace(tzinfo=timezone.utc)
            # Uptime calc still uses the 7d-window first_seen: we're asking
            # "did you propose at the expected rate for as long as you were
            # visible to us?", not "ever". Age uses the true first-active
            # below, which goes back to the first Reward event on-chain.
            seconds_alive_uptime = max(1, (now_ts - first_seen_aware).total_seconds())
            effective_seconds = min(seconds_alive_uptime, seconds_in_7d)
            expected = per_validator_rate * effective_seconds

            # Grace period: first 24h show 100% uptime. Not enough data yet to
            # distinguish a real outage from statistical noise in small samples.
            hours_alive = seconds_alive_uptime / 3600
            if hours_alive < 24:
                uptime_pct = 100.0
            elif expected <= 0:
                uptime_pct = 0.0
            else:
                uptime_pct = min(100.0, c["total_blocks"] / expected * 100.0)

            # Block time quality: closer to 400ms is better (20%)
            avg_bt = (c["weighted_bt_sum"] / c["total_blocks"]) if c["total_blocks"] else 400.0
            bt_quality_pct = max(0.0, 100.0 - abs(avg_bt - 400) / 4)

            # Age: use the TRUE first-active timestamp from the Reward-events
            # cache when available. Otherwise fall back to our 7d-window
            # first_seen (undercounts but monotonic — improves as cache fills).
            first_active_ts = c.get("first_active_ts")
            if first_active_ts:
                age_seconds = max(1, now_ts.timestamp() - first_active_ts)
            else:
                age_seconds = seconds_alive_uptime
            age_days = age_seconds / 86400
            age_pct = min(1.0, age_days / 30) * 100.0

            # Upgrade — real score for our own node (where we can call
            # web3_clientVersion); placeholder 75 for everyone else.
            upgrade_pct = 75.0
            cluster_addr = (c["validator_id"] or "").lower()
            if LOCAL_AUTH and cluster_addr == LOCAL_AUTH and rpc is not None:
                # Make sure we have a "latest" to compare against — if the
                # periodic release-check hasn't run yet (first compute cycle
                # after boot), fetch it on-demand.
                if not _last_known_release:
                    await check_new_release(pool)
                local_ver = await _get_local_version(rpc)
                upgrade_pct = _upgrade_pct(local_ver, _last_known_release)
                log.info(f"local upgrade: version={local_ver} latest={_last_known_release} pct={upgrade_pct}")

            # Stake stability — compare last 3 snapshots in validator_stake_history.
            # Decline from earliest → latest is penalised; flat or growing stake
            # reads as fully stable. Neutral 75 until we have ≥2 snapshots (takes
            # a few epochs to accumulate on first deploy).
            stake_pct = 75.0
            try:
                hist = await conn.fetch(
                    "SELECT epoch, total_stake FROM validator_stake_history "
                    "WHERE validator_id = $1 AND network = $2 "
                    "ORDER BY epoch DESC LIMIT 3",
                    cluster_addr, NETWORK,
                )
                if len(hist) >= 2:
                    stakes = [int(r["total_stake"]) for r in hist]
                    latest = stakes[0]
                    oldest = stakes[-1]
                    if latest >= oldest:
                        stake_pct = 100.0
                    else:
                        decline = (oldest - latest) / max(oldest, 1) * 100
                        if decline < 5:
                            stake_pct = 85.0
                        elif decline < 15:
                            stake_pct = 60.0
                        else:
                            stake_pct = 30.0
            except Exception:
                pass

            total = (uptime_pct * 0.4 + bt_quality_pct * 0.2
                     + upgrade_pct * 0.15 + stake_pct * 0.15 + age_pct * 0.1)

            await conn.execute("""
                INSERT INTO health_scores
                    (validator_id, timestamp, total_score, uptime_score, miss_score, upgrade_score, stake_score, age_score, network)
                VALUES ($1, NOW(), $2, $3, $4, $5, $6, $7, $8)
            """,
                c["validator_id"],
                round(total, 1),
                round(uptime_pct, 1),
                round(bt_quality_pct, 1),
                round(upgrade_pct, 1),
                round(stake_pct, 1),
                round(age_pct, 1),
                NETWORK,
            )

        log.info(f"Health scores computed for {len(clusters)} validators (from {len(validators)} proposer rows)")

    # Progressive first-active cache fill. Fire-and-forget — blocks too
    # long to run inline in the health-compute cycle (worst case ~30s per
    # validator at the 300k HARD_CAP). We schedule it on the event loop
    # and move on; the cache benefits from it on the NEXT compute cycle.
    vids_with_val_id = [c["val_id"] for c in clusters.values() if c.get("val_id") is not None]
    log.info(f"first-active: {len(vids_with_val_id)} clusters with val_id, cache size {len(_FIRST_ACTIVE_MEM)}")
    if vids_with_val_id:
        asyncio.create_task(fill_first_active_cache(vids_with_val_id, None, max_new=3))


STAKE_LOGS_CHUNK = 500 if os.environ.get("MONADPULSE_NETWORK", "testnet") != "mainnet" else 100
STAKE_BACKFILL_BLOCKS = 200_000 if os.environ.get("MONADPULSE_NETWORK", "testnet") != "mainnet" else 5_000


WHALE_STAKE_MON = 1_000_000  # absolute floor (≥ 1M MON to even consider)
STAKE_CRITICAL_PCT = 15      # alert if action ≥ 15% of validator's stake
ACTIVE_VALIDATOR_STAKE = 10_000_000 * 10 ** 18  # Monad active-set threshold
GET_VALIDATOR_SELECTOR = "2b6d639a"
PRECOMPILE = "0x0000000000000000000000000000000000001000"


async def snapshot_stakes(rpc: MonadRPC, pool) -> None:
    """Snapshot current execution stakes for every directory validator.
    Writes into validator_stake_history, one row per (validator_id, epoch).
    Later epochs overwrite the previous row for the same epoch via
    ON CONFLICT — so the last value within an epoch wins. Across epochs we
    accumulate a history that compute_health_scores can diff for stability."""
    epoch = await rpc.get_epoch()
    if epoch is None:
        return
    import json as _json
    directory_path = Path(f"/opt/monadpulse/validator_directory_{NETWORK}.json")
    if not directory_path.exists():
        return
    try:
        directory = _json.loads(directory_path.read_text())
    except Exception:
        return
    written = 0
    async with pool.acquire() as conn:
        for e in directory:
            vid = e.get("val_id")
            auth = (e.get("auth") or "").lower()
            if vid is None or not auth:
                continue
            stake = await _current_val_stake(rpc, vid)
            if stake is None or stake <= 0:
                continue
            try:
                await conn.execute("""
                    INSERT INTO validator_stake_history
                        (validator_id, epoch, total_stake, self_stake, delegator_count, network)
                    VALUES ($1, $2, $3, 0, 0, $4)
                    ON CONFLICT (validator_id, epoch) DO UPDATE SET total_stake = EXCLUDED.total_stake
                """, auth, int(epoch), stake, NETWORK)
                written += 1
            except Exception as e:
                log.warning(f"stake-snapshot write err val_id={vid}: {e}")
    log.info(f"stake snapshot: wrote {written} validators at epoch {epoch}")


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
    the last 24h. One alert per validator per 24h (dedup via alerts table).

    The tricky bit: on testnet + mainnet block.miner rotates across several
    ephemeral addresses per validator and never equals auth_address. So we
    can't just check "is auth in active proposers". Resolution chain:
    1. Gather every address the names map attributes to this validator's
       canonical name (auth + all known miner addresses).
    2. If the names map only knows auth, probe get_proposer_val_id() at
       ~200 unique recent proposers and harvest real miner addrs.
    3. Count blocks_24h matching ANY of those. Only alert if that's zero.
    """
    global _SEEN_VAL_IDS
    if _SEEN_VAL_IDS is None:
        return  # no seed yet

    import json as _json
    active_directory_path = Path(f"/opt/monadpulse/validator_directory_{NETWORK}.json")
    names_map_path = Path(f"/opt/monadpulse/validator_names_{NETWORK}.json")
    if not active_directory_path.exists():
        return
    directory = _json.loads(active_directory_path.read_text())
    auth_to_vid = {e["auth"]: e["val_id"] for e in directory if e.get("auth")}
    vid_to_name = {e["val_id"]: e.get("name") for e in directory}
    vid_to_auth = {e["val_id"]: e.get("auth") for e in directory if e.get("auth")}

    names_map: dict = {}
    if names_map_path.exists():
        try:
            names_map = _json.loads(names_map_path.read_text())
        except Exception:
            pass

    # Lazy RPC discovery to learn miner addrs for validators not in the names map
    async def discover_miners(conn, vid: int) -> list[str]:
        import httpx as _httpx
        rows = await conn.fetch("""
            SELECT proposer_address, MAX(block_number) AS bn
            FROM blocks
            WHERE network = $1 AND timestamp > NOW() - INTERVAL '6 hours'
              AND proposer_address != '0x0000000000000000000000000000000000000000'
            GROUP BY proposer_address
        """, NETWORK)
        if not rows:
            return []
        url = os.environ.get("MONADPULSE_RPC_URL") or os.environ.get("RPC_URL") \
              or "http://localhost:8080"
        miners: list[str] = []
        sem = asyncio.Semaphore(20)

        async def probe(client, addr, bn):
            async with sem:
                try:
                    r = await client.post(url, json={
                        "jsonrpc": "2.0", "method": "eth_call",
                        "params": [{"to": "0x0000000000000000000000000000000000001000",
                                    "data": "0xfbacb0be"}, hex(bn)],
                        "id": 1,
                    })
                    raw = (r.json() or {}).get("result", "0x0")
                    if raw and raw != "0x" and int(raw, 16) == vid:
                        miners.append(addr.lower())
                except Exception:
                    pass

        async with _httpx.AsyncClient(timeout=15) as client:
            await asyncio.gather(*(probe(client, r["proposer_address"], int(r["bn"])) for r in rows))
        return miners

    async with pool.acquire() as conn:
        for vid in _SEEN_VAL_IDS:
            auth = vid_to_auth.get(vid)
            if not auth:
                continue
            name = vid_to_name.get(vid) or f"#{vid}"

            # Step 1: candidate addrs from names map + auth
            candidates = {auth.lower()}
            for addr, nm in names_map.items():
                if nm == name:
                    candidates.add(addr.lower())

            # Step 2: count blocks for ANY candidate
            count = await conn.fetchval("""
                SELECT COUNT(*) FROM blocks
                WHERE network = $1 AND proposer_address = ANY($2::text[])
                  AND timestamp > NOW() - INTERVAL '24 hours'
            """, NETWORK, list(candidates))

            # Step 3: if zero and we only had auth, try RPC discovery
            if count == 0 and len(candidates) == 1:
                try:
                    discovered = await discover_miners(conn, vid)
                except Exception as e:
                    log.warning(f"offline-detect discover err vid={vid}: {e}")
                    discovered = []
                if discovered:
                    for m in discovered:
                        candidates.add(m)
                    count = await conn.fetchval("""
                        SELECT COUNT(*) FROM blocks
                        WHERE network = $1 AND proposer_address = ANY($2::text[])
                          AND timestamp > NOW() - INTERVAL '24 hours'
                    """, NETWORK, list(candidates))

            if count > 0:
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
        await compute_health_scores(pool, rpc)
        log.info("Entering live mode")

        # Live mode
        last_aggregate = asyncio.get_event_loop().time()
        last_epoch_check = 0
        last_health_calc = 0
        last_tps_check = 0
        last_stake_ingest = 0
        last_offline_check = 0
        last_stake_snapshot = 0
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

                # Stake snapshot — every 30 min. Writes every validator's
                # current execution_stake to validator_stake_history so the
                # health-score stake-stability component has historical data.
                if now - last_stake_snapshot > 1800:
                    try:
                        await snapshot_stakes(rpc, pool)
                    except Exception as e:
                        log.warning(f"stake snapshot err: {e}")
                    last_stake_snapshot = now

                # Health scores — every hour
                if now - last_health_calc > 3600:
                    await compute_health_scores(pool, rpc)
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
