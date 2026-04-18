import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
from eth_abi import decode, encode
from fastapi import APIRouter, HTTPException, Query, Request

router = APIRouter()

_DIR_CACHE: dict[str, tuple[float, list]] = {}


def _load_directory(network: str) -> list[dict]:
    """Return cached validator directory (refreshes on file mtime change)."""
    path = Path(f"/opt/monadpulse/validator_directory_{network}.json")
    if not path.exists():
        return []
    mtime = path.stat().st_mtime
    cached = _DIR_CACHE.get(network)
    if cached and cached[0] == mtime:
        return cached[1]
    try:
        data = json.loads(path.read_text())
    except Exception:
        return []
    _DIR_CACHE[network] = (mtime, data)
    return data

STAKING_PRECOMPILE = "0x0000000000000000000000000000000000001000"
GET_VALIDATOR_SELECTOR = "2b6d639a"
GET_VALIDATOR_ABI = [
    "address", "uint256", "uint256", "uint256", "uint256", "uint256",
    "uint256", "uint256", "uint256", "uint256", "bytes", "bytes",
]
RPC_URLS = {
    "testnet": os.environ.get("TESTNET_RPC", "http://localhost:8080"),
    "mainnet": os.environ.get("MAINNET_RPC", "https://rpc.monad.xyz"),
}


async def _get_validator_onchain(val_id: int, network: str) -> tuple | None:
    """Call staking precompile get_validator(val_id) via eth_call."""
    calldata = "0x" + GET_VALIDATOR_SELECTOR + encode(["uint64"], [val_id]).hex()
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.post(RPC_URLS.get(network, RPC_URLS["testnet"]), json={
            "jsonrpc": "2.0", "method": "eth_call",
            "params": [{"to": STAKING_PRECOMPILE, "data": calldata}, "latest"],
            "id": 1,
        })
        data = r.json()
    if "error" in data or not data.get("result"):
        return None
    return decode(GET_VALIDATOR_ABI, bytes.fromhex(data["result"][2:]))


@router.get("/search")
async def validator_search(q: str = Query(..., min_length=1), network: str = Query("testnet"), limit: int = Query(10, le=50)):
    """Search validator directory by name substring, val_id, auth, or SECP pubkey.
    Backed by validator_directory_{network}.json (rebuilt per epoch)."""
    directory = _load_directory(network)
    q_low = q.lower().strip()
    q_is_id = q_low.lstrip("#").isdigit()
    q_id = int(q_low.lstrip("#")) if q_is_id else None
    q_hex = q_low[2:] if q_low.startswith("0x") else q_low

    matches = []
    for e in directory:
        hit = False
        if q_is_id and e["val_id"] == q_id:
            hit = True
        elif e.get("name") and q_low in e["name"].lower():
            hit = True
        elif q_hex and len(q_hex) >= 4 and (q_hex in e["auth"].lower() or q_hex in (e.get("secp") or "").lower()):
            hit = True
        if hit:
            matches.append(e)
            if len(matches) >= limit:
                break
    return {"query": q, "network": network, "matches": matches}


@router.get("/by-id/{val_id}")
async def validator_by_id(val_id: int, network: str = Query("testnet")):
    """On-chain validator state from staking precompile. Works for any validator
    including operators whose block.miner is ephemeral / not indexed in blocks table."""
    v = await _get_validator_onchain(val_id, network)
    if v is None:
        raise HTTPException(status_code=404, detail=f"validator {val_id} not found on {network}")
    auth_raw = v[0]
    auth = auth_raw if isinstance(auth_raw, str) else "0x" + auth_raw.hex()
    secp = v[10].hex() if hasattr(v[10], "hex") else bytes(v[10]).hex()
    bls = v[11].hex() if hasattr(v[11], "hex") else bytes(v[11]).hex()
    # Empty registration check: secp all zeros
    if secp == "00" * (len(secp) // 2):
        raise HTTPException(status_code=404, detail=f"validator {val_id} not registered on {network}")
    return {
        "validator_id": val_id,
        "network": network,
        "auth_address": auth.lower(),
        "flags": int(v[1]),
        "execution_stake": int(v[2]),
        "rewards_per_token": int(v[3]),
        "execution_commission": int(v[4]),
        "unclaimed_rewards": int(v[5]),
        "consensus_stake": int(v[6]),
        "consensus_commission": int(v[7]),
        "snapshot_stake": int(v[8]),
        "snapshot_commission": int(v[9]),
        "secp_pubkey": secp,
        "bls_pubkey": bls,
    }


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
