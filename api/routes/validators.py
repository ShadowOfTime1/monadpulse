from datetime import datetime, timedelta, timezone
from fastapi import APIRouter, Request, Query

router = APIRouter()


@router.get("/list")
async def validator_list(request: Request, period: str = Query("24h"), network: str = Query("testnet")):
    delta = {"24h": timedelta(hours=24), "7d": timedelta(days=7), "30d": timedelta(days=30)}.get(period, timedelta(hours=24))
    now = datetime.now(timezone.utc)
    start = (now - delta).replace(minute=0, second=0, microsecond=0)
    pool = request.app.state.pool
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT proposer_address AS validator, COUNT(*) AS blocks_proposed, "
            "AVG(block_time_ms)::INT AS avg_block_time_ms, SUM(tx_count) AS total_tx, "
            "MIN(timestamp) AS first_seen, MAX(timestamp) AS last_seen "
            "FROM blocks WHERE network = $1 AND timestamp >= $2 "
            "AND proposer_address != '0x0000000000000000000000000000000000000000' "
            "GROUP BY proposer_address ORDER BY blocks_proposed DESC",
            network, start,
        )
    return [
        {
            "address": r["validator"],
            "blocks_proposed": r["blocks_proposed"],
            "avg_block_time_ms": r["avg_block_time_ms"],
            "total_tx": r["total_tx"],
            "first_seen": r["first_seen"].isoformat(),
            "last_seen": r["last_seen"].isoformat(),
        }
        for r in rows
    ]


@router.get("/{address}")
async def validator_detail(request: Request, address: str, network: str = Query("testnet")):
    address = address.lower()
    pool = request.app.state.pool
    async with pool.acquire() as conn:
        stats = await conn.fetchrow("""
            SELECT
                COUNT(*) AS total_blocks,
                AVG(block_time_ms)::INT AS avg_block_time_ms,
                SUM(tx_count) AS total_tx,
                MIN(timestamp) AS first_seen,
                MAX(timestamp) AS last_seen
            FROM blocks
            WHERE network = $1 AND proposer_address = $2
        """, network, address)
        recent = await conn.fetch(
            "SELECT block_number, timestamp, tx_count, gas_used, block_time_ms "
            "FROM blocks WHERE network = $1 AND proposer_address = $2 ORDER BY block_number DESC LIMIT 20",
            network, address,
        )
        geo = await conn.fetchrow(
            "SELECT name, country, city, lat, lon, provider FROM validator_geo WHERE validator_id = $1 AND network = $2",
            address, network,
        )
    result = {
        "address": address,
        "stats": None,
        "recent_blocks": [],
        "geo": None,
    }
    if stats and stats["total_blocks"]:
        result["stats"] = {
            "total_blocks": stats["total_blocks"],
            "avg_block_time_ms": stats["avg_block_time_ms"],
            "total_tx": stats["total_tx"],
            "first_seen": stats["first_seen"].isoformat(),
            "last_seen": stats["last_seen"].isoformat(),
        }
    result["recent_blocks"] = [
        {
            "number": r["block_number"],
            "timestamp": r["timestamp"].isoformat(),
            "tx_count": r["tx_count"],
            "gas_used": r["gas_used"],
            "block_time_ms": r["block_time_ms"],
        }
        for r in recent
    ]
    if geo:
        result["geo"] = dict(geo)
    return result
