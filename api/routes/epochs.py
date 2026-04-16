from fastapi import APIRouter, Request, Query

router = APIRouter()


@router.get("/list")
async def epoch_list(request: Request, limit: int = Query(20, le=100), network: str = Query("testnet")):
    pool = request.app.state.pool
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT epoch_number, boundary_block, timestamp, validator_count "
            "FROM epochs WHERE network = $1 ORDER BY epoch_number DESC LIMIT $2",
            network, limit,
        )
    return [
        {
            "epoch": r["epoch_number"],
            "boundary_block": r["boundary_block"],
            "timestamp": r["timestamp"].isoformat(),
            "validator_count": r["validator_count"],
        }
        for r in rows
    ]


@router.get("/{epoch_number}/validators")
async def epoch_validators(request: Request, epoch_number: int, network: str = Query("testnet")):
    pool = request.app.state.pool
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT validator_id, stake, commission, status "
            "FROM epoch_validators WHERE epoch_number = $1 AND network = $2 ORDER BY stake DESC",
            epoch_number, network,
        )
    return [
        {
            "validator_id": r["validator_id"],
            "stake": float(r["stake"]),
            "commission": r["commission"],
            "status": r["status"],
        }
        for r in rows
    ]
