from datetime import datetime, timedelta, timezone
from fastapi import APIRouter, Request, Query

router = APIRouter()


@router.get("/top-earners")
async def top_earners(request: Request, hours: int = Query(24, le=168), network: str = Query("testnet")):
    """Top validators by block production (proxy for rewards earned)."""
    now = datetime.now(timezone.utc)
    start = (now - timedelta(hours=hours)).replace(minute=0, second=0, microsecond=0)
    pool = request.app.state.pool
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT proposer_address, COUNT(*) AS blocks, SUM(tx_count) AS txns, "
            "SUM(gas_used)::NUMERIC AS total_gas "
            "FROM blocks WHERE network = $1 AND timestamp >= $2 "
            "GROUP BY proposer_address ORDER BY blocks DESC LIMIT 30",
            network, start,
        )
    # Estimate rewards: each block ~ base_reward (placeholder until real data)
    return [
        {
            "validator": r["proposer_address"],
            "blocks": r["blocks"],
            "txns": r["txns"],
            "total_gas": float(r["total_gas"]),
            "est_rewards_mon": round(r["blocks"] * 25.0, 1),  # ~25 MON per block from logs
        }
        for r in rows
    ]


@router.get("/flow")
async def stake_flow(request: Request, network: str = Query("testnet")):
    """Validator stake changes between epochs (from epoch_validators if available)."""
    pool = request.app.state.pool
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT validator_id, epoch, total_stake, self_stake, delegator_count "
            "FROM validator_stake_history WHERE validator_id != '' "
            "ORDER BY epoch DESC, total_stake DESC LIMIT 100"
        )
    if not rows:
        return {"message": "Stake flow data will appear when epoch validator snapshots accumulate"}
    return [
        {
            "validator": r["validator_id"],
            "epoch": r["epoch"],
            "total_stake": float(r["total_stake"]),
            "self_stake": float(r["self_stake"]),
            "delegators": r["delegator_count"],
        }
        for r in rows
    ]


@router.get("/events")
async def stake_events(request: Request, limit: int = Query(50, le=200), network: str = Query("testnet")):
    """Recent stake events (delegate/undelegate)."""
    pool = request.app.state.pool
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT block_number, timestamp, event_type, validator_id, delegator, amount "
            "FROM stake_events WHERE network = $1 ORDER BY timestamp DESC LIMIT $2",
            network, limit,
        )
    if not rows:
        return {"message": "No delegation events detected yet. Monitoring staking precompile events."}
    return [
        {
            "block": r["block_number"],
            "timestamp": r["timestamp"].isoformat(),
            "type": r["event_type"],
            "validator": r["validator_id"],
            "delegator": r["delegator"],
            "amount": float(r["amount"]),
        }
        for r in rows
    ]
