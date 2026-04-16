from datetime import datetime, timedelta, timezone
from fastapi import APIRouter, Request, Query

router = APIRouter()

INTERVALS = {"24h": timedelta(hours=24), "7d": timedelta(days=7), "30d": timedelta(days=30)}


def hour_bounds(delta: timedelta):
    """Return (start, end) as full-hour boundaries. Excludes current partial hour."""
    now = datetime.now(timezone.utc)
    end = now.replace(minute=0, second=0, microsecond=0)  # start of current hour
    start = (now - delta).replace(minute=0, second=0, microsecond=0)
    return start, end


@router.get("/recent")
async def recent_blocks(request: Request, limit: int = Query(50, le=500), network: str = Query("testnet")):
    pool = request.app.state.pool
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT block_number, timestamp, proposer_address, tx_count, gas_used, base_fee, block_time_ms "
            "FROM blocks WHERE network = $1 ORDER BY block_number DESC LIMIT $2",
            network, limit,
        )
    return [
        {
            "number": r["block_number"],
            "timestamp": r["timestamp"].isoformat(),
            "proposer": r["proposer_address"],
            "tx_count": r["tx_count"],
            "gas_used": r["gas_used"],
            "base_fee": r["base_fee"],
            "block_time_ms": r["block_time_ms"],
        }
        for r in rows
    ]


@router.get("/proposer-stats")
async def proposer_stats(request: Request, period: str = Query("24h"), network: str = Query("testnet")):
    delta = INTERVALS.get(period, timedelta(hours=24))
    start, end = hour_bounds(delta)
    pool = request.app.state.pool
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT proposer_address, COUNT(*) AS blocks_proposed, "
            "AVG(block_time_ms)::INT AS avg_block_time_ms, SUM(tx_count) AS total_tx "
            "FROM blocks WHERE network = $1 AND timestamp >= $2 AND timestamp < $3 "
            "GROUP BY proposer_address ORDER BY blocks_proposed DESC",
            network, start, end,
        )
    return [
        {
            "proposer": r["proposer_address"],
            "blocks_proposed": r["blocks_proposed"],
            "avg_block_time_ms": r["avg_block_time_ms"],
            "total_tx": r["total_tx"],
        }
        for r in rows
    ]


@router.get("/timeline")
async def block_timeline(request: Request, hours: int = Query(24, le=168), network: str = Query("testnet")):
    start, end = hour_bounds(timedelta(hours=hours))
    pool = request.app.state.pool
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT date_trunc('hour', timestamp) AS hour, COUNT(*) AS block_count, "
            "AVG(block_time_ms)::INT AS avg_block_time, SUM(tx_count) AS total_tx, "
            "AVG(gas_used)::BIGINT AS avg_gas, SUM(gas_used)::NUMERIC AS total_gas "
            "FROM blocks WHERE network = $1 AND timestamp >= $2 AND timestamp < $3 "
            "GROUP BY hour ORDER BY hour",
            network, start, end,
        )
    result = [
        {
            "hour": r["hour"].isoformat(),
            "block_count": r["block_count"],
            "avg_block_time": r["avg_block_time"],
            "total_tx": r["total_tx"],
            "avg_gas": r["avg_gas"],
            "total_gas": float(r["total_gas"]) if r["total_gas"] else 0,
        }
        for r in rows
    ]
    # Trim incomplete first hour (< 70% of median)
    if len(result) > 2:
        counts = sorted(r["block_count"] for r in result)
        median = counts[len(counts) // 2]
        if median > 0 and result[0]["block_count"] < median * 0.7:
            result = result[1:]
    return result
