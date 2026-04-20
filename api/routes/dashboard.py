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
            SELECT epoch_number, boundary_block, validator_count
            FROM epochs WHERE network = $1 ORDER BY epoch_number DESC LIMIT 1
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

    # Compute epoch progress from latest block
    if latest:
        bn = latest["block_number"]
        # Prefer the collector-recorded epoch number (via staking precompile
        # getEpoch()) over block-number arithmetic. The two can disagree for
        # a few seconds around a boundary — epoch.number is authoritative.
        current_epoch = epoch["epoch_number"] if epoch else bn // 50000
        progress_blocks = bn % 50000
        progress_pct = round(progress_blocks / 50000 * 100, 1)
        remaining = 50000 - progress_blocks
        # Reuse throughput-derived bt if available
        avg_bt = result["stats_24h"]["avg_block_time_ms"] if result.get("stats_24h") else 400
        if not avg_bt:
            avg_bt = 400
        eta_seconds = int(remaining * avg_bt / 1000) if avg_bt > 0 else 0
        result["epoch_progress"] = {
            "current_epoch": current_epoch,
            "progress_pct": progress_pct,
            "progress_blocks": progress_blocks,
            "remaining_blocks": remaining,
            "eta_seconds": eta_seconds,
        }

    return result
