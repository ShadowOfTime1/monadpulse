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
                bn = block["block_number"]
                dt_ms = block["block_time_ms"]
                alert_title = f"Slow block #{bn}: {dt_ms}ms"
                alert_desc_db = f"Proposer: {block['proposer_address']} [{NETWORK}]"
                await insert_alert(
                    conn,
                    alert_type="slow_block",
                    severity="warning",
                    title=alert_title,
                    description=alert_desc_db,
                    data_json={"block_number": bn, "block_time_ms": dt_ms},
                    network=NETWORK,
                )
                if dt_ms > 10000:
                    net_qs = f"&network={NETWORK}" if NETWORK != "testnet" else ""
                    proposer = block['proposer_address']
                    tg_desc = (
                        f"<blockquote>block <b>#{bn}</b>\n"
                        f"took <b>{dt_ms:,} ms</b>  (target 400)</blockquote>\n"
                        f'proposer <a href="https://monadpulse.xyz/validator.html?addr={proposer}{net_qs}"><code>{proposer[:10]}…</code></a>'
                    )
                    await tg_send("slow_block", "warning", "Slow block", tg_desc)

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
            digest_desc_db = (
                f"⏱ {dur_h}h {dur_m}m · {blocks_n:,} blocks · avg {avg_bt}ms\n"
                f"💳 {txs_n:,} transactions · {tps:.0f} TPS\n"
                f"👥 {val_count} unique proposers · {null_n:,} null blocks ({null_pct:.1f}%)\n"
                f"⛽ median base fee ~{base_fee_gwei} gwei"
            )
            await insert_alert(
                conn, alert_type="epoch_summary", severity="info",
                title=digest_title, description=digest_desc_db,
                data_json={
                    "prev_epoch": epoch_num - 1, "blocks": blocks_n, "txs": txs_n,
                    "avg_block_time_ms": avg_bt, "validators": val_count,
                    "null_blocks": null_n, "base_fee_gwei": base_fee_gwei,
                    "duration_sec": duration_s,
                },
                network=NETWORK,
            )
            tg_epoch_desc = (
                f"<blockquote><b>Epoch {epoch_num - 1}</b>  ·  {dur_h}h {dur_m}m\n"
                f"<b>{blocks_n:,}</b> blocks  ·  avg <b>{avg_bt} ms</b>\n"
                f"<b>{txs_n:,}</b> txs  ·  <b>{tps:.0f} TPS</b>\n"
                f"<b>{val_count}</b> unique proposers  ·  {null_pct:.1f}% null\n"
                f"base fee ~<b>{base_fee_gwei} gwei</b></blockquote>"
            )
            await tg_send("epoch_summary", "info", "Epoch summary", tg_epoch_desc)

        # Short "epoch changed" ping (keeps existing behavior)
        alert_title = f"New epoch {epoch_num} started at block {boundary}"
        alert_desc_db = f"Active validators: {val_count}"
        await insert_alert(
            conn, alert_type="new_epoch", severity="info",
            title=alert_title, description=alert_desc_db,
            data_json={"epoch": epoch_num, "boundary_block": boundary, "validator_count": val_count},
            network=NETWORK,
        )
        tg_new_epoch_desc = (
            f"<blockquote><b>Epoch {epoch_num}</b>  started\n"
            f"at block <b>#{boundary:,}</b>\n"
            f"active validators: <b>{val_count}</b></blockquote>"
        )
        await tg_send("new_epoch", "info", "New epoch", tg_new_epoch_desc)

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
    if not new_ids:
        return

    # Keep per-validator rows in DB (UI shows each as separate event) but
    # collapse the Telegram blast into a single message — during VDP batches
    # Foundation activates several validators at once and previously this
    # emitted 4–10 identical messages in ~1 second.
    sorted_ids = sorted(new_ids)
    async with pool.acquire() as conn:
        for vid in sorted_ids:
            title = f"Validator #{vid} joined active set"
            desc = f"Entered at epoch {epoch_num}. Active set size now: {len(current)}."
            await insert_alert(
                conn, alert_type="new_validator", severity="info",
                title=title, description=desc,
                data_json={"validator_id": vid, "epoch": epoch_num}, network=NETWORK,
            )

    net_qs_nv = f"&network={NETWORK}" if NETWORK != "testnet" else ""
    def _vref(vid: int) -> str:
        nm = _lookup_val_name(vid)
        label = nm if nm else f"Validator {vid}"
        return f'<a href="https://monadpulse.xyz/validator.html?id={vid}{net_qs_nv}">{label}</a>'

    if len(sorted_ids) == 1:
        vid = sorted_ids[0]
        tg_title = "Validator joined active set"
        tg_desc = (
            f"<blockquote>{_vref(vid)}\n"
            f"entered at epoch <b>{epoch_num}</b>\n"
            f"active set size now: <b>{len(current)}</b></blockquote>"
        )
    else:
        tg_title = f"{len(sorted_ids)} validators joined active set"
        shown = sorted_ids[:12]
        lines = "\n".join(f"• {_vref(v)}" for v in shown)
        if len(sorted_ids) > len(shown):
            lines += f"\n<i>… and {len(sorted_ids) - len(shown)} more</i>"
        tg_desc = (
            f"<blockquote>epoch <b>{epoch_num}</b>  ·  active set now <b>{len(current)}</b></blockquote>\n"
            + lines
        )
    await tg_send("new_validator", "info", tg_title, tg_desc)


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

        # Detect validators currently in Foundation's stake-rotation (confirmed
        # by Jackson 2026-04-23: a script rotates VDP delegation across all
        # delegated validators until the active-set is expanded via MIP9).
        # Criterion: Foundation (0xf235ab9b...) undelegated ≥1.9M from this
        # val_id in the last 48h. These validators are forced out of active-set
        # by policy — they physically can't propose blocks. We must not apply
        # the recency penalty to their uptime score, otherwise our dashboard
        # would penalise every VDP-enrolled validator during their "out" phase
        # of the rotation cycle.
        rotation_rows = await conn.fetch("""
            SELECT DISTINCT validator_id
            FROM stake_events
            WHERE network = $1
              AND delegator ILIKE '0xf235ab9b%'
              AND event_type = 'undelegate'
              AND amount::numeric >= 1900000::numeric * 1000000000000000000::numeric
              AND timestamp > NOW() - INTERVAL '48 hours'
        """, NETWORK)
        rotation_vids: set[int] = set()
        for r in rotation_rows:
            try:
                rotation_vids.add(int(r["validator_id"]))
            except (TypeError, ValueError):
                pass

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

        # Fallback addr→val_id: directory misses validators whose
        # validator-info PR is pending (e.g. shadowoftime val 267 while the
        # upstream PR is unmerged). Recover their val_id from the earliest
        # self-delegate in stake_events (delegator != Foundation at
        # addValidator time is always the operator themselves).
        missing_addrs = [(c["validator_id"] or "").lower() for c in clusters.values()
                         if (c["validator_id"] or "").lower() not in addr_to_vid]
        if missing_addrs:
            fb_rows = await conn.fetch("""
                SELECT DISTINCT ON (delegator)
                    delegator AS auth, validator_id::int AS vid
                FROM stake_events
                WHERE network = $1
                  AND event_type = 'delegate'
                  AND delegator NOT ILIKE '0xf235ab9b%'
                  AND lower(delegator) = ANY($2::text[])
                ORDER BY delegator, block_number ASC
            """, NETWORK, missing_addrs)
            for r in fb_rows:
                addr_to_vid.setdefault(r["auth"].lower(), int(r["vid"]))

        for c in clusters.values():
            vid = addr_to_vid.get((c["validator_id"] or "").lower())
            c["val_id"] = vid
            cached = _FIRST_ACTIVE_MEM.get(vid) if vid is not None else None
            if cached and cached.get("timestamp"):
                c["first_active_ts"] = cached["timestamp"]
            else:
                c["first_active_ts"] = None
        seconds_in_7d = 7 * 24 * 3600
        network_rate = network_total_7d / seconds_in_7d  # blocks per second network-wide

        # Stake-weighted baseline. Monad proposer selection is stake-weighted
        # (not round-robin), so expected blocks = stake_share × network_blocks.
        # The old uniform baseline (1/N for all) unfairly flagged low-stake
        # validators at ~20% uptime on mainnet where max/median stake ratio
        # is ~30×. We read current stakes from the latest snapshot in
        # validator_stake_history; if empty (fresh install), fall back to
        # uniform per-validator rate.
        stake_rows = await conn.fetch(
            """
            SELECT validator_id, total_stake FROM validator_stake_history
            WHERE network = $1 AND epoch = (
                SELECT MAX(epoch) FROM validator_stake_history WHERE network = $1
            )
            """,
            NETWORK,
        )
        stakes_by_auth = {r["validator_id"].lower(): int(r["total_stake"]) for r in stake_rows}
        total_active_stake = sum(stakes_by_auth.values())
        per_validator_rate_uniform = network_rate / max(active_validators, 1)

        def expected_rate_for(cluster_addr: str) -> float:
            """Blocks-per-second this validator is expected to produce."""
            if total_active_stake > 0:
                stake = stakes_by_auth.get(cluster_addr.lower(), 0)
                if stake > 0:
                    share = stake / total_active_stake
                    return network_rate * share
            return per_validator_rate_uniform

        for c in clusters.values():
            first_seen = c["first_seen"]
            if first_seen is None:
                continue
            first_seen_aware = first_seen if first_seen.tzinfo else first_seen.replace(tzinfo=timezone.utc)
            # Uptime: "did this validator produce its stake-weighted share of
            # blocks in the window we've observed them?" 7d-window first_seen
            # is intentional — we can't claim "you missed blocks on day 2"
            # if we only saw you starting day 5.
            seconds_alive_uptime = max(1, (now_ts - first_seen_aware).total_seconds())
            effective_seconds = min(seconds_alive_uptime, seconds_in_7d)
            expected = expected_rate_for(c["validator_id"]) * effective_seconds

            # Grace period: first 24h show 100% uptime. Not enough data yet to
            # distinguish a real outage from statistical noise in small samples.
            hours_alive = seconds_alive_uptime / 3600
            if hours_alive < 24:
                uptime_pct = 100.0
            elif expected <= 0:
                uptime_pct = 0.0
            else:
                uptime_pct = min(100.0, c["total_blocks"] / expected * 100.0)

            # Recency penalty: 7-day window smooths over recent outages. A
            # validator that stopped signing 6 hours ago but was active before
            # would still read ~100% (6h/168h = 3.5%). Worse: if stake was
            # also reduced (e.g. Foundation rebalance), the lower `expected`
            # offsets the missed blocks and uptime stays capped at 100%.
            #
            # Fix: if time since last block is many times the expected
            # inter-block gap, linearly scale uptime toward 0. Kicks in at
            # ~10× expected gap (obvious outage) and bottoms out at ~60×.
            #
            # Exception: validators currently in Foundation's rotation script
            # (confirmed 2026-04-23 by Jackson) are forced out of active-set
            # by design. Applying recency penalty to them would mislabel a
            # deliberate policy action as operator-level outage.
            in_rotation = c.get("val_id") in rotation_vids
            last_seen = c.get("last_seen")
            if last_seen and expected > 0 and not in_rotation:
                last_seen_aware = last_seen if last_seen.tzinfo else last_seen.replace(tzinfo=timezone.utc)
                minutes_silent = (now_ts - last_seen_aware).total_seconds() / 60.0
                rate_per_sec = expected_rate_for(c["validator_id"])
                if rate_per_sec > 0:
                    expected_gap_min = 1.0 / rate_per_sec / 60.0
                    silence_ratio = minutes_silent / expected_gap_min
                    if silence_ratio > 10:
                        recency_factor = max(0.0, 1.0 - (silence_ratio - 10) / 50.0)
                        uptime_pct = min(uptime_pct, 100.0 * recency_factor)

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

            # Upgrade — default 100 for any active validator. Reasoning:
            # they're producing blocks at the expected rate (uptime 100),
            # which means their client version is still compatible with
            # consensus. Outdated nodes fall out of consensus within days
            # and their uptime drops — so the "upgrade health" signal is
            # already captured by uptime. Hard-coding a 75 placeholder for
            # everyone but the local node created an unfair bias where our
            # own health score systematically beat every peer by ~3.75 pts
            # even when they were on the same version as us.
            #
            # Only penalise when we can *prove* a validator is behind:
            # that's possible only for the local node (we can query its
            # web3_clientVersion). If the local node is behind the latest
            # GitHub release, we penalise OURSELVES accordingly.
            upgrade_pct = 100.0
            cluster_addr = (c["validator_id"] or "").lower()
            if LOCAL_AUTH and cluster_addr == LOCAL_AUTH and rpc is not None:
                if not _last_known_release:
                    await check_new_release(pool)
                local_ver = await _get_local_version(rpc)
                proven_pct = _upgrade_pct(local_ver, _last_known_release)
                if proven_pct < 100:
                    upgrade_pct = proven_pct
                    log.info(f"local upgrade penalty: version={local_ver} latest={_last_known_release} pct={proven_pct}")

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

        # Cleanup stale entries. Any validator_id that appears in
        # health_scores but is NOT one of the current clusters' canonical
        # IDs is a leftover: old miner address from before clustering, or
        # a one-hit proposer whose collector window included them once and
        # never again. The API returns LATEST per validator_id, so leaving
        # them in the table surfaces them in the UI forever as "bad
        # validators with 0.8% uptime". Delete them.
        current_ids = {(c["validator_id"] or "").lower() for c in clusters.values() if c.get("validator_id")}
        if current_ids:
            deleted = await conn.execute(
                "DELETE FROM health_scores WHERE network = $1 "
                "AND LOWER(validator_id) <> ALL($2::text[])",
                NETWORK, list(current_ids),
            )
            log.info(f"stale health_scores cleanup: {deleted}")

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


async def _get_val_commission_at_block(rpc: MonadRPC, val_id: int, block_num: int) -> int | None:
    """Read execution_commission (wei-scale, 1e18 = 100%) at a specific block.
    Used to recover the OLD commission when our DB doesn't have the prior
    commission_changed event (stake-events backfill cutoff). Requires node
    with historical state available at that block — Monad full nodes keep
    this since they replay everything from a forkpoint."""
    if rpc is None or block_num < 0:
        return None
    try:
        calldata = "0x" + GET_VALIDATOR_SELECTOR + int(val_id).to_bytes(32, "big").hex()
        result = await rpc._call("eth_call", [
            {"to": PRECOMPILE, "data": calldata},
            hex(block_num),
        ])
        if not result or len(result) < 130:
            return None
        # Layout: [0]auth(32) [1]flags(32) [2]exec_stake(32) [3]rewards_per_token(32)
        #         [4]execution_commission(32) ...
        # execution_commission is at offset 4*32 = 128 bytes
        commission_hex = result[2 + 128 * 2: 2 + 160 * 2]
        return int(commission_hex, 16)
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
        vid = int(ev["validator_id"])
        name = _lookup_val_name(vid)
        who = name if name else f"Validator {vid}"
        # Find the PREVIOUS commission for this validator. Two-step lookup:
        #   1. DB: most recent commission_changed event before this one —
        #      fast, no RPC, works when we've indexed the full history.
        #   2. RPC fallback: read validator's state at block_number - 1 from
        #      the staking precompile — needed on mainnet where our stake-
        #      events backfill cutoff may predate the validator's own
        #      creation (e.g. VALIDEXIS set commission before we started
        #      indexing mainnet).
        # If both miss → treat as initial (validator was created with this
        # commission baked in at addValidator, no prior state to compare).
        old_wei = None
        prev_row = await conn.fetchrow("""
            SELECT amount FROM stake_events
            WHERE network = $1 AND validator_id = $2 AND event_type = 'commission_changed'
              AND block_number < $3
            ORDER BY block_number DESC
            LIMIT 1
        """, NETWORK, str(vid), ev["block_number"])
        if prev_row:
            old_wei = int(prev_row["amount"])
        elif rpc is not None:
            try:
                old_wei = await _get_val_commission_at_block(rpc, vid, int(ev["block_number"]) - 1)
            except Exception:
                old_wei = None

        if old_wei is not None:
            old_pct = old_wei / (10 ** 16)
            change_line = f"commission <b>{old_pct:.2f}%</b> → <b>{new_pct:.2f}%</b>"
            db_change = f"{old_pct:.2f}% → {new_pct:.2f}%"
        else:
            change_line = f"commission set to <b>{new_pct:.2f}%</b>  <i>(initial)</i>"
            db_change = f"initial → {new_pct:.2f}%"

        net_qs = f"&network={NETWORK}" if NETWORK != "testnet" else ""
        val_url = f"https://monadpulse.xyz/validator.html?id={vid}{net_qs}"
        title = "Commission change"
        desc = (
            f"<blockquote><b>{who}</b>\n"
            f"{change_line}</blockquote>\n"
            f'<a href="{val_url}">Open on MonadPulse</a>'
        )
        # DB record keeps a descriptive title for alerts.html feed
        db_title = f"{who} commission {db_change}"
        await insert_alert(
            conn, alert_type="commission_change", severity="info",
            title=db_title, description=desc,
            data_json={
                "validator_id": vid, "new_rate_wei": amount, "new_pct": new_pct,
                "old_pct": (old_wei / (10 ** 16)) if old_wei is not None else None,
            },
            network=NETWORK,
        )
        await tg_send("commission_change", "info", title, desc)
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
    resolved_name = _lookup_val_name(val_id)
    # One visible label per validator — name if we have it, "Validator N"
    # otherwise. Never show "#N (id N)" — it's the same number twice.
    val_label = resolved_name if resolved_name else f"Validator {val_id}"
    severity = "critical" if drops_below_active else "info"
    if et == "delegate":
        emoji = "🐋"
        action = "Large delegation"
    else:
        emoji = "🦈"
        action = "Large undelegation"
    if drops_below_active:
        emoji = "🚨"
        action = "Validator exit risk"

    net_qs = f"&network={NETWORK}" if NETWORK != "testnet" else ""
    val_url = f"https://monadpulse.xyz/validator.html?id={val_id}{net_qs}"

    title = f"{action}"
    sign = "+" if et == "delegate" else "−"
    quote_lines = [
        f"<b>{val_label}</b>",
        f"{sign}<b>{amount_mon:,.0f} MON</b>  ·  {pct:.0f}% of stake",
    ]
    if stake_mon is not None:
        post_mon = stake_mon + (amount_mon if et == "delegate" else -amount_mon)
        quote_lines.append(f"stake {stake_mon:,.0f} → {post_mon:,.0f} MON")
    desc_parts = [f"<blockquote>" + "\n".join(quote_lines) + "</blockquote>"]
    if drops_below_active:
        desc_parts.append(
            f"⚠ <b>Falls below {ACTIVE_VALIDATOR_STAKE // 10**18:,} MON active threshold</b> — "
            "validator exits active set next epoch."
        )
    desc_parts.append(f'<a href="{val_url}">Open on MonadPulse</a>')
    desc = "\n".join(desc_parts)
    # For DB (shown on /alerts.html feed)
    db_title = f"{emoji} {action}: {sign}{amount_mon:,.0f} MON ({pct:.0f}% stake) · {val_label}"

    await insert_alert(
        conn, alert_type="whale_stake", severity=severity,
        title=db_title, description=desc,
        data_json={
            "event_type": et, "validator_id": val_id,
            "delegator": ev["delegator"], "amount_mon": amount_mon,
            "stake_mon": stake_mon, "pct": pct,
            "drops_below_active": drops_below_active,
            "block": ev["block_number"],
        },
        network=NETWORK,
    )
    # Return alert payload for batched Telegram flush at end of ingest cycle
    # — callers who don't care can discard it (e.g. single-event paths).
    return {
        "severity": severity, "title": title, "desc": desc,
        "emoji": emoji, "amount_mon": amount_mon, "pct": pct,
        "name": resolved_name, "val_id": val_id, "event_type": et,
        "drops_below_active": drops_below_active,
        "block": ev["block_number"],
        "val_url": val_url,
    }


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
    whale_batch: list[dict] = []
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
                    payload = await _maybe_alert_stake_event(conn, ev, rpc)
                    if payload:
                        whale_batch.append(payload)
            inserted_total += len(decoded)

        async with pool.acquire() as conn:
            await upsert_collector_state(conn, "last_stake_block", str(end), NETWORK)
        current = end + 1

    # Flush whale_stake alerts as a single Telegram message. During VDP-batch
    # epochs Foundation fires 10–20 delegations/undelegations within seconds,
    # which previously spammed the channel with identical-looking alerts.
    if len(whale_batch) == 1:
        a = whale_batch[0]
        await tg_send("whale_stake", a["severity"], a["title"], a["desc"])
    elif whale_batch:
        # Aggregate: sort by severity (critical first), then amount desc.
        whale_batch.sort(key=lambda x: (0 if x["severity"] == "critical" else 1,
                                        -x["amount_mon"]))
        total_delegated = sum(x["amount_mon"] for x in whale_batch if x["event_type"] == "delegate")
        total_undelegated = sum(x["amount_mon"] for x in whale_batch if x["event_type"] == "undelegate")
        n_del = sum(1 for x in whale_batch if x["event_type"] == "delegate")
        n_und = sum(1 for x in whale_batch if x["event_type"] == "undelegate")
        has_critical = any(x["severity"] == "critical" for x in whale_batch)
        summary_sev = "critical" if has_critical else "info"

        quote_parts = []
        if n_del:
            quote_parts.append(f"🐋 {n_del}× delegations  <b>+{total_delegated:,.0f} MON</b>")
        if n_und:
            quote_parts.append(f"🦈 {n_und}× undelegations  <b>−{total_undelegated:,.0f} MON</b>")
        summary_title = "Whale stake batch"

        # Each validator on its own line, name-only when we have it, "Validator N"
        # fallback otherwise. One link per line → the validator's page. No #id
        # duplication inside the label itself. Show ALL events — Telegram caps
        # messages at 4096 chars; ~60 chars/line fits 60+ events comfortably,
        # beyond that we split into continuation messages.
        detail_lines = []
        for a in whale_batch:
            sign = "+" if a["event_type"] == "delegate" else "−"
            warn = "  🚨" if a.get("drops_below_active") else ""
            label = a["name"] if a.get("name") else f"Validator {a['val_id']}"
            detail_lines.append(
                f'• {sign}<b>{a["amount_mon"]:,.0f} MON</b>  '
                f'<a href="{a["val_url"]}">{label}</a>{warn}'
            )

        body = (
            f"<blockquote>" + "\n".join(quote_parts) + "</blockquote>\n"
            + "\n".join(detail_lines)
        )
        await tg_send("whale_stake", summary_sev, summary_title, body)

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
        # Pre-fetch the set of proposer addresses that have produced any
        # block in the last 30 days. Validators not in this set are
        # ghosts (registered in the execution valset but never started
        # producing — abandoned/test registrations) and are silently
        # dropped from the offline detection. Saves one count query per
        # ghost per cycle and removes the entire class of false alerts.
        producer_rows = await conn.fetch("""
            SELECT DISTINCT proposer_address
            FROM blocks
            WHERE network = $1
              AND proposer_address != '0x0000000000000000000000000000000000000000'
              AND timestamp > NOW() - INTERVAL '30 days'
        """, NETWORK)
        active_producers: set[str] = {r["proposer_address"].lower() for r in producer_rows}

        # Foundation rotation set — intentionally-idle validators, also dropped.
        rotation_rows = await conn.fetch("""
            SELECT DISTINCT validator_id
            FROM stake_events
            WHERE network = $1
              AND delegator ILIKE '0xf235ab9b%'
              AND event_type = 'undelegate'
              AND amount::numeric >= 1900000::numeric * 1000000000000000000::numeric
              AND timestamp > NOW() - INTERVAL '48 hours'
        """, NETWORK)
        rotation_vids: set[int] = set()
        for r in rotation_rows:
            try:
                rotation_vids.add(int(r["validator_id"]))
            except (TypeError, ValueError):
                pass

        for vid in _SEEN_VAL_IDS:
            auth = vid_to_auth.get(vid)
            if not auth:
                continue
            # Skip validators in Foundation rotation — they're intentionally
            # offline by design, alerting on them is pure noise.
            if vid in rotation_vids:
                continue
            name = vid_to_name.get(vid) or f"#{vid}"

            # Step 1: candidate addrs from names map + auth
            candidates = {auth.lower()}
            for addr, nm in names_map.items():
                if nm == name:
                    candidates.add(addr.lower())

            # Ghost filter: if NONE of this validator's candidate addresses
            # produced any block in the last 30 days, skip immediately.
            # No reason to scan blocks/probe miners for someone who has
            # never been seen as a proposer.
            if not (candidates & active_producers):
                continue

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

            # (Ghost filter happens at top of loop via active_producers set —
            # if we got this far, this validator IS a recent producer that
            # has now gone silent. Real signal.)

            # Dedup window is 7 days, not 24 h. The detector runs every 24 h,
            # so a stably-offline validator (decommissioned, long-term outage,
            # operator MIA) would otherwise generate one alert every day for
            # weeks. Once is enough; if they come back online and go offline
            # again, the gap will exceed 7 days and a fresh alert fires.
            already = await conn.fetchval("""
                SELECT 1 FROM alerts
                WHERE alert_type = 'validator_offline' AND network = $1
                  AND data_json->>'validator_id' = $2
                  AND timestamp > NOW() - INTERVAL '7 days'
                LIMIT 1
            """, NETWORK, str(vid))
            if already:
                continue
            db_title = f"⚠ Validator {name} offline — 0 blocks in 24h"
            db_desc = f"val_id={vid}, auth={auth[:10]}…{auth[-4:]}"
            await insert_alert(
                conn, alert_type="validator_offline", severity="warning",
                title=db_title, description=db_desc,
                data_json={"validator_id": str(vid), "auth": auth},
                network=NETWORK,
            )
            net_qs_off = f"&network={NETWORK}" if NETWORK != "testnet" else ""
            label = name if name and name != f"#{vid}" else f"Validator {vid}"
            tg_desc = (
                f"<blockquote><b>{label}</b>\n"
                f"no blocks in last <b>24h</b>\n"
                f"possible outage or VDP rotation</blockquote>\n"
                f'<a href="https://monadpulse.xyz/validator.html?id={vid}{net_qs_off}">Open on MonadPulse</a>'
            )
            await tg_send("validator_offline", "warning", "Validator offline", tg_desc)


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
            recent = float(row["recent_tx"])
            avg = float(row["avg_tx"])
            mult = recent / avg if avg > 0 else 0
            db_title = f"TPS spike [{NETWORK}]: {recent:.0f} tx/block (avg: {avg:.0f})"
            await insert_alert(conn, "tps_spike", "warning", db_title, network=NETWORK)
            tg_desc = (
                f"<blockquote>last 5 min: <b>{recent:.0f}</b> tx/block\n"
                f"24h avg: <b>{avg:.0f}</b> tx/block\n"
                f"spike: <b>{mult:.1f}×</b> normal</blockquote>"
            )
            await tg_send("tps_spike", "warning", "TPS spike", tg_desc)
            log.info(db_title)


_last_known_release = None  # execution-repo tag, used by health-score upgrade penalty

# Repos we track. First tuple element is the GitHub slug, second is the label
# shown in the alert. Execution comes first so _last_known_release gets seeded
# from it (preserving the existing upgrade-penalty semantics).
_RELEASE_REPOS = [
    ("category-labs/monad", "execution"),
    ("category-labs/monad-bft", "consensus"),
]
_RELEASE_STATE_PATH = Path("/opt/monadpulse/state_release_tracker.json")

def _load_release_state() -> dict:
    import json as _json
    if _RELEASE_STATE_PATH.exists():
        try:
            return _json.loads(_RELEASE_STATE_PATH.read_text())
        except Exception:
            return {}
    return {}

def _save_release_state(state: dict) -> None:
    import json as _json
    try:
        _RELEASE_STATE_PATH.write_text(_json.dumps(state, indent=2))
    except Exception as e:
        log.warning(f"release state save failed: {e}")


async def check_new_release(pool):
    """
    Poll GitHub for new releases in every tracked Monad repo. State is
    persisted to disk so a collector restart doesn't silently re-seed and
    miss notifications. First ever run (no state file) seeds silently.
    """
    global _last_known_release
    import httpx
    state = _load_release_state()
    updated = False
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            for slug, label in _RELEASE_REPOS:
                try:
                    # per_page=5 + sort-by-published_at: GitHub's default order is
                    # by created_at descending, which can put a re-created older
                    # release ahead of the real latest. Publication date is what
                    # matters for "new" detection.
                    r = await client.get(
                        f"https://api.github.com/repos/{slug}/releases?per_page=5",
                        headers={"Accept": "application/vnd.github+json"},
                    )
                    releases = r.json()
                except Exception as e:
                    log.warning(f"release fetch {slug}: {e}")
                    continue
                if not releases or not isinstance(releases, list):
                    continue
                published = [x for x in releases if isinstance(x, dict) and x.get("published_at")]
                if not published:
                    continue
                latest = max(published, key=lambda x: x.get("published_at", ""))
                tag = latest.get("tag_name") or ""
                if not tag:
                    continue
                prev = state.get(slug)
                if prev is None:
                    # Never seen this repo before — seed silently
                    state[slug] = tag
                    updated = True
                elif tag != prev:
                    state[slug] = tag
                    updated = True
                    db_title = f"New Monad {label} release: {tag}"
                    db_desc = f"{latest.get('name', tag)} — Update within 48h for VDP compliance ({slug})"
                    async with pool.acquire() as conn:
                        await insert_alert(conn, "new_version", "critical", db_title, db_desc, network=NETWORK)
                    release_url = latest.get("html_url") or f"https://github.com/{slug}/releases/tag/{tag}"
                    tg_desc = (
                        f"<blockquote>Monad <b>{label}</b>\n"
                        f"new release <b>{tag}</b>\n"
                        f"previous: {prev}\n"
                        f"⚠ upgrade within <b>48h</b> for VDP compliance</blockquote>\n"
                        f'<a href="{release_url}">Release notes on GitHub</a>'
                    )
                    await tg_send("new_version", "critical", "New Monad release", tg_desc)
                    log.info(f"New release detected: {slug} {prev} -> {tag}")
                # Keep in-memory execution-repo tag for upgrade-penalty logic
                if slug == "category-labs/monad":
                    _last_known_release = tag
    finally:
        if updated:
            _save_release_state(state)


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
        last_retention = 0
        last_governance_scan = 0
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

                # Governance scrape — every 30 min, testnet collector only.
                # Forum content is global (network-agnostic), so one collector
                # handles it. Same dup-avoidance pattern as check_new_release.
                if NETWORK == "testnet" and now - last_governance_scan > 1800:
                    try:
                        from collector.governance import scrape_governance_full
                        await scrape_governance_full(pool)
                    except Exception as e:
                        log.warning(f"Governance scrape error: {e}")
                    last_governance_scan = now

                # Retention — prune old rows from append-only tables every 6h.
                # Keeps the DB bounded without losing useful recent history.
                # Only runs on testnet collector to avoid double-delete.
                if NETWORK == "testnet" and now - last_retention > 21600:
                    try:
                        async with pool.acquire() as conn:
                            # Blocks: keep 90 days of raw per-block rows. API
                            # queries cap at 30 days (INTERVALS dict in
                            # blocks.py), so 90 gives a safe buffer + a month
                            # of debug/backfill room. At ~80 MB/day this caps
                            # the table at ~7 GB instead of unbounded.
                            # Hourly gas stats (hourly_gas_stats) already hold
                            # long-term aggregates.
                            b = await conn.execute(
                                "DELETE FROM blocks WHERE timestamp < NOW() - INTERVAL '90 days'"
                            )
                            # Hourly health snapshots: keep 30 days → ~324k max
                            # rows (450 validators × 24h × 30 days × 2 nets)
                            h = await conn.execute(
                                "DELETE FROM health_scores WHERE timestamp < NOW() - INTERVAL '30 days'"
                            )
                            # Alerts: keep 90 days
                            a = await conn.execute(
                                "DELETE FROM alerts WHERE timestamp < NOW() - INTERVAL '90 days'"
                            )
                            # Stake snapshots: keep last 3000 epochs (~60 days
                            # on testnet). Used by health-score stake
                            # stability check (3-snapshot window).
                            s = await conn.execute(
                                "DELETE FROM validator_stake_history WHERE epoch < "
                                "(SELECT COALESCE(MAX(epoch), 0) FROM validator_stake_history) - 3000"
                            )
                        log.info(f"retention: blocks={b} health={h} alerts={a} stake_hist={s}")
                    except Exception as e:
                        log.warning(f"retention error: {e}")
                    last_retention = now

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
