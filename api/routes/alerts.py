from fastapi import APIRouter, Request, Query

router = APIRouter()


@router.get("/recent")
async def recent_alerts(request: Request, limit: int = Query(50, le=200), alert_type: str = None, network: str = Query("testnet")):
    pool = request.app.state.pool
    async with pool.acquire() as conn:
        if alert_type:
            rows = await conn.fetch(
                "SELECT id, timestamp, alert_type, severity, title, description, data_json "
                "FROM alerts WHERE network = $1 AND alert_type = $2 ORDER BY timestamp DESC LIMIT $3",
                network, alert_type, limit,
            )
        else:
            rows = await conn.fetch(
                "SELECT id, timestamp, alert_type, severity, title, description, data_json "
                "FROM alerts WHERE network = $1 ORDER BY timestamp DESC LIMIT $2",
                network, limit,
            )
    return [
        {
            "id": r["id"],
            "timestamp": r["timestamp"].isoformat(),
            "type": r["alert_type"],
            "severity": r["severity"],
            "title": r["title"],
            "description": r["description"],
            "data": r["data_json"],
        }
        for r in rows
    ]


@router.get("/stats")
async def alert_stats(request: Request, network: str = Query("testnet")):
    pool = request.app.state.pool
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT alert_type, severity, COUNT(*) AS count
            FROM alerts
            WHERE network = $1 AND timestamp > NOW() - INTERVAL '24 hours'
            GROUP BY alert_type, severity
            ORDER BY count DESC
        """, network)
    return [
        {"type": r["alert_type"], "severity": r["severity"], "count": r["count"]}
        for r in rows
    ]
