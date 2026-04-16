from datetime import datetime, timedelta, timezone
from fastapi import APIRouter, Request, Query

router = APIRouter()


def hour_bounds(delta: timedelta):
    now = datetime.now(timezone.utc)
    end = now.replace(minute=0, second=0, microsecond=0)
    start = (now - delta).replace(minute=0, second=0, microsecond=0)
    return start, end


@router.get("/hourly")
async def hourly_gas(request: Request, hours: int = Query(24, le=168), network: str = Query("testnet")):
    start, end = hour_bounds(timedelta(hours=hours))
    pool = request.app.state.pool
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT hour_timestamp, avg_gas, total_gas, tx_count, avg_base_fee, burned_mon "
            "FROM hourly_gas_stats WHERE network = $1 "
            "AND hour_timestamp >= $2 AND hour_timestamp < $3 "
            "ORDER BY hour_timestamp",
            network, start, end,
        )
        if not rows:
            rows = await conn.fetch(
                "SELECT date_trunc('hour', timestamp) AS hour_timestamp, "
                "AVG(gas_used)::BIGINT AS avg_gas, SUM(gas_used)::BIGINT AS total_gas, "
                "SUM(tx_count)::INT AS tx_count, AVG(base_fee)::BIGINT AS avg_base_fee, "
                "SUM(gas_used::NUMERIC * base_fee::NUMERIC / 1e18)::NUMERIC AS burned_mon "
                "FROM blocks WHERE network = $1 AND timestamp >= $2 AND timestamp < $3 "
                "GROUP BY date_trunc('hour', timestamp) ORDER BY hour_timestamp",
                network, start, end,
            )
    result = [
        {
            "hour": r["hour_timestamp"].isoformat(),
            "avg_gas": r["avg_gas"],
            "total_gas": r["total_gas"],
            "tx_count": r["tx_count"],
            "avg_base_fee": r["avg_base_fee"],
            "burned_mon": float(r["burned_mon"]),
        }
        for r in rows
    ]
    # Trim incomplete first hour
    if len(result) > 2:
        counts = sorted(r["tx_count"] for r in result)
        median = counts[len(counts) // 2]
        if median > 0 and result[0]["tx_count"] < median * 0.7:
            result = result[1:]
    return result


@router.get("/heatmap")
async def gas_heatmap(request: Request, network: str = Query("testnet")):
    start, end = hour_bounds(timedelta(days=7))
    pool = request.app.state.pool
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT EXTRACT(DOW FROM timestamp)::INT AS dow, "
            "EXTRACT(HOUR FROM timestamp)::INT AS hour, "
            "AVG(gas_used)::BIGINT AS avg_gas, AVG(tx_count)::INT AS avg_tx "
            "FROM blocks WHERE network = $1 AND timestamp >= $2 AND timestamp < $3 "
            "GROUP BY dow, hour ORDER BY dow, hour",
            network, start, end,
        )
    return [
        {"dow": r["dow"], "hour": r["hour"], "avg_gas": r["avg_gas"], "avg_tx": r["avg_tx"]}
        for r in rows
    ]


@router.get("/top-contracts")
async def top_contracts(request: Request, limit: int = Query(20, le=100), network: str = Query("testnet")):
    pool = request.app.state.pool
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT contract_address, total_gas_used, tx_count, first_seen "
            "FROM top_contracts WHERE network = $1 ORDER BY total_gas_used DESC LIMIT $2",
            network, limit,
        )
    return [
        {
            "address": r["contract_address"],
            "total_gas": r["total_gas_used"],
            "tx_count": r["tx_count"],
            "first_seen": r["first_seen"].isoformat() if r["first_seen"] else None,
        }
        for r in rows
    ]
