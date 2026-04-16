from datetime import datetime, timedelta, timezone
from fastapi import APIRouter, Request, Query

router = APIRouter()


@router.get("/scores")
async def health_scores(request: Request, limit: int = Query(50, le=500), network: str = Query("testnet")):
    pool = request.app.state.pool
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT * FROM (
                SELECT DISTINCT ON (validator_id)
                    validator_id, timestamp, total_score,
                    uptime_score, miss_score, upgrade_score, stake_score, age_score
                FROM health_scores
                WHERE network = $1
                ORDER BY validator_id, timestamp DESC
            ) sub
            ORDER BY total_score DESC
            LIMIT $2
        """, network, limit)
    return [
        {
            "validator_id": r["validator_id"],
            "timestamp": r["timestamp"].isoformat(),
            "total_score": float(r["total_score"]),
            "uptime_score": float(r["uptime_score"]),
            "miss_score": float(r["miss_score"]),
            "upgrade_score": float(r["upgrade_score"]),
            "stake_score": float(r["stake_score"]),
            "age_score": float(r["age_score"]),
        }
        for r in rows
    ]


@router.get("/scores/{validator_id}/history")
async def health_history(request: Request, validator_id: str, days: int = Query(7, le=30), network: str = Query("testnet")):
    pool = request.app.state.pool
    async with pool.acquire() as conn:
        since = datetime.now(timezone.utc) - timedelta(days=days)
        rows = await conn.fetch(
            "SELECT timestamp, total_score, uptime_score, miss_score, upgrade_score, stake_score, age_score "
            "FROM health_scores WHERE network = $1 AND validator_id = $2 AND timestamp > $3 "
            "ORDER BY timestamp",
            network, validator_id.lower(), since,
        )
    return [
        {
            "timestamp": r["timestamp"].isoformat(),
            "total_score": float(r["total_score"]),
            "components": {
                "uptime": float(r["uptime_score"]),
                "miss": float(r["miss_score"]),
                "upgrade": float(r["upgrade_score"]),
                "stake": float(r["stake_score"]),
                "age": float(r["age_score"]),
            },
        }
        for r in rows
    ]
