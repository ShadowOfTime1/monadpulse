from fastapi import APIRouter, Request, Query

router = APIRouter()


@router.get("/summary")
async def summary(request: Request, network: str = Query("testnet")):
    pool = request.app.state.pool
    async with pool.acquire() as conn:
        latest = await conn.fetchrow("""
            SELECT block_number, timestamp, tx_count, gas_used, base_fee, block_time_ms, proposer_address
            FROM blocks WHERE network = $1 ORDER BY block_number DESC LIMIT 1
        """, network)
        stats_24h = await conn.fetchrow("""
            SELECT
                COUNT(*) AS block_count,
                COALESCE(SUM(tx_count), 0) AS total_tx,
                COALESCE(AVG(tx_count), 0) AS avg_tps_per_block,
                COALESCE(AVG(block_time_ms), 0) AS avg_block_time_ms,
                COUNT(DISTINCT proposer_address) FILTER (
                    WHERE proposer_address != '0x0000000000000000000000000000000000000000'
                ) AS active_validators
            FROM blocks
            WHERE network = $1 AND timestamp > NOW() - INTERVAL '24 hours'
        """, network)
        epoch = await conn.fetchrow("""
            SELECT epoch_number, boundary_block, validator_count, timestamp
            FROM epochs WHERE network = $1 ORDER BY epoch_number DESC LIMIT 1
        """, network)
        # Recent epoch durations — used to estimate progress through the
        # CURRENT epoch since durations vary on Monad (no fixed block count
        # per epoch).
        recent_epochs = await conn.fetch("""
            SELECT epoch_number, timestamp
            FROM epochs WHERE network = $1
            ORDER BY epoch_number DESC LIMIT 10
        """, network)

    result = {
        "latest_block": None,
        "stats_24h": None,
        "epoch": None,
    }

    if latest:
        result["latest_block"] = {
            "number": latest["block_number"],
            "timestamp": latest["timestamp"].isoformat() if latest["timestamp"] else None,
            "tx_count": latest["tx_count"],
            "gas_used": latest["gas_used"],
            "block_time_ms": latest["block_time_ms"],
            "proposer": latest["proposer_address"],
        }

    if stats_24h:
        block_count = stats_24h["block_count"]
        # Throughput-derived block time (ms per block over 24h window).
        # More informative than AVG(block_time_ms) because RPC timestamps
        # are 1s-granular, forcing raw diffs to be 0 or 1000 only.
        bt_effective = (86_400_000 / block_count) if block_count > 0 else 0
        tps = float(stats_24h["total_tx"]) / 86400 if block_count > 0 else 0
        result["stats_24h"] = {
            "block_count": block_count,
            "total_tx": stats_24h["total_tx"],
            "tps": round(tps, 2),
            "avg_block_time_ms": round(bt_effective, 1),
            "active_validators": stats_24h["active_validators"],
        }

    if epoch:
        result["epoch"] = {
            "number": epoch["epoch_number"],
            "boundary_block": epoch["boundary_block"],
            "validator_count": epoch["validator_count"],
        }

    # Compute epoch progress using the actual recorded epoch transition
    # timestamp + a median of recent epoch durations. Block-number
    # arithmetic doesn't work — epoch length on Monad is not a fixed block
    # count.
    if latest and epoch:
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc)
        bn = latest["block_number"]
        current_epoch = epoch["epoch_number"]
        epoch_started = epoch["timestamp"]
        if epoch_started and epoch_started.tzinfo is None:
            epoch_started = epoch_started.replace(tzinfo=timezone.utc)
        elapsed_s = int((now - epoch_started).total_seconds()) if epoch_started else 0

        # Median duration over the last few transitions (resilient to one
        # weirdly long/short epoch). Need ≥2 rows ordered DESC to compute gaps.
        ts_list = [r["timestamp"] for r in recent_epochs if r["timestamp"]]
        for i, t in enumerate(ts_list):
            if t.tzinfo is None:
                ts_list[i] = t.replace(tzinfo=timezone.utc)
        gaps_s = [int((ts_list[i] - ts_list[i + 1]).total_seconds())
                  for i in range(len(ts_list) - 1)]
        gaps_s = [g for g in gaps_s if g > 0]
        if gaps_s:
            gaps_sorted = sorted(gaps_s)
            median_duration_s = gaps_sorted[len(gaps_sorted) // 2]
        else:
            median_duration_s = 0

        progress_pct = round(elapsed_s / median_duration_s * 100, 1) if median_duration_s else 0
        # Cap visible progress at 100 — if elapsed > median, show 100% with
        # a flag rather than over-extending.
        progress_pct_capped = min(progress_pct, 100.0)
        eta_seconds = max(median_duration_s - elapsed_s, 0) if median_duration_s else 0

        # Approximate "blocks since boundary" using throughput-derived block
        # time. This is a UI hint, not authoritative — the duration is what
        # actually matters for the progress bar.
        avg_bt = result["stats_24h"]["avg_block_time_ms"] if result.get("stats_24h") else 400
        if not avg_bt:
            avg_bt = 400
        progress_blocks = int(elapsed_s * 1000 / avg_bt) if avg_bt > 0 else 0
        remaining_blocks = int(eta_seconds * 1000 / avg_bt) if avg_bt > 0 else 0

        result["epoch_progress"] = {
            "current_epoch": current_epoch,
            "progress_pct": progress_pct_capped,
            "elapsed_seconds": elapsed_s,
            "median_duration_seconds": median_duration_s,
            "progress_blocks": progress_blocks,
            "remaining_blocks": remaining_blocks,
            "eta_seconds": eta_seconds,
        }

    return result
