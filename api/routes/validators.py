import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
from eth_abi import decode, encode
from fastapi import APIRouter, HTTPException, Query, Request

router = APIRouter()

_DIR_CACHE: dict[str, tuple[float, list]] = {}
_FIRST_ACTIVE_CACHE: dict[tuple[str, int], tuple[int, int]] = {}
# (network, val_id) -> (block_number, timestamp)

REWARD_EVENT_SIG = "0x3a420a01486b6b28d6ae89c51f5c3bde3e0e74eecbb646a0c481ccba3aae3754"


async def _has_reward_in_range(network: str, val_id: int, lo: int, hi: int) -> list[dict]:
    """eth_getLogs for Reward event with topic[1]=val_id in range [lo,hi]."""
    val_topic = "0x" + format(val_id, "064x")
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.post(RPC_URLS.get(network, RPC_URLS["testnet"]), json={
            "jsonrpc": "2.0", "method": "eth_getLogs",
            "params": [{
                "address": STAKING_PRECOMPILE,
                "fromBlock": hex(lo), "toBlock": hex(hi),
                "topics": [REWARD_EVENT_SIG, val_topic],
            }],
            "id": 1,
        })
        data = r.json()
    return data.get("result") or []


async def _find_first_active_block(network: str, val_id: int) -> tuple[int, int] | None:
    """Earliest block with Reward event for val_id. Single-pass backward scan in
    chunks. Stops after EMPTY_STREAK empty chunks past the last event.
    Returns (block_number, timestamp) or None. Cached indefinitely."""
    key = (network, val_id)
    if key in _FIRST_ACTIVE_CACHE:
        return _FIRST_ACTIVE_CACHE[key]

    chunk = 100 if network == "mainnet" else 1000
    EMPTY_STREAK = 5   # 5 empty chunks × chunk blocks = stopping condition
    HARD_CAP = 2_000_000  # don't scan more than this many blocks

    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.post(RPC_URLS.get(network, RPC_URLS["testnet"]), json={
            "jsonrpc": "2.0", "method": "eth_blockNumber", "params": [], "id": 1,
        })
        latest = int(r.json()["result"], 16)

    earliest = None
    empty_streak = 0
    cur_hi = latest
    total_scanned = 0

    while cur_hi > 0 and total_scanned < HARD_CAP:
        cur_lo = max(0, cur_hi - chunk + 1)
        try:
            logs = await _has_reward_in_range(network, val_id, cur_lo, cur_hi)
        except Exception:
            logs = []

        if logs:
            empty_streak = 0
            for log in logs:
                bn = int(log["blockNumber"], 16)
                if earliest is None or bn < earliest:
                    earliest = bn
        else:
            if earliest is not None:
                empty_streak += 1
                if empty_streak >= EMPTY_STREAK:
                    break
            # else: keep scanning, validator may just be further back

        total_scanned += (cur_hi - cur_lo + 1)
        cur_hi = cur_lo - 1

    if earliest is None:
        return None

    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.post(RPC_URLS.get(network, RPC_URLS["testnet"]), json={
            "jsonrpc": "2.0", "method": "eth_getBlockByNumber",
            "params": [hex(earliest), False], "id": 1,
        })
        ts = int(r.json()["result"]["timestamp"], 16)

    _FIRST_ACTIVE_CACHE[key] = (earliest, ts)
    return earliest, ts


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
GET_DELEGATOR_SELECTOR = "573c1ce0"
GET_VALIDATOR_ABI = [
    "address", "uint256", "uint256", "uint256", "uint256", "uint256",
    "uint256", "uint256", "uint256", "uint256", "bytes", "bytes",
]
GET_DELEGATOR_ABI = ["uint256", "uint256", "uint256", "uint256", "uint256", "uint64", "uint64"]
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


async def _get_delegator_onchain(val_id: int, delegator: str, network: str) -> tuple | None:
    """Call staking precompile get_delegator(val_id, address) via eth_call."""
    delegator_bytes = bytes.fromhex(delegator[2:]) if delegator.startswith("0x") else bytes.fromhex(delegator)
    calldata = "0x" + GET_DELEGATOR_SELECTOR + encode(["uint64", "address"], [val_id, "0x" + delegator_bytes.hex()]).hex()
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.post(RPC_URLS.get(network, RPC_URLS["testnet"]), json={
            "jsonrpc": "2.0", "method": "eth_call",
            "params": [{"to": STAKING_PRECOMPILE, "data": calldata}, "latest"],
            "id": 1,
        })
        data = r.json()
    if "error" in data or not data.get("result"):
        return None
    return decode(GET_DELEGATOR_ABI, bytes.fromhex(data["result"][2:]))


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
    # Query auth's own delegator slot to surface operator-claimable rewards
    # (the contract only lets a delegator claim their own share — see plan
    # curious-hopping-quasar.md).
    auth_lower = auth.lower()
    operator_claimable = None
    operator_active_stake = None
    try:
        d = await _get_delegator_onchain(val_id, auth_lower, network)
        if d is not None:
            operator_active_stake = int(d[0])
            operator_claimable = int(d[2])
    except Exception:
        pass

    return {
        "validator_id": val_id,
        "network": network,
        "auth_address": auth_lower,
        "flags": int(v[1]),
        "execution_stake": int(v[2]),
        "rewards_per_token": int(v[3]),
        "execution_commission": int(v[4]),
        "unclaimed_rewards": int(v[5]),          # pool total (all delegators)
        "consensus_stake": int(v[6]),
        "consensus_commission": int(v[7]),
        "snapshot_stake": int(v[8]),
        "snapshot_commission": int(v[9]),
        "secp_pubkey": secp,
        "bls_pubkey": bls,
        # Auth-scoped accounting (may be null if call failed or auth is not a delegator)
        "operator_active_stake": operator_active_stake,
        "operator_claimable_rewards": operator_claimable,
    }


@router.get("/by-id/{val_id}/first-active")
async def validator_first_active(val_id: int, network: str = Query("testnet")):
    """Earliest block where val_id received a Reward event (≈ first active block).
    Cached indefinitely after first lookup (may take 1-5s on cold cache)."""
    result = await _find_first_active_block(network, val_id)
    if result is None:
        return {"validator_id": val_id, "network": network, "first_active_block": None, "first_active_timestamp": None}
    block, ts = result
    return {
        "validator_id": val_id,
        "network": network,
        "first_active_block": block,
        "first_active_timestamp": ts,
    }


@router.get("/geo")
async def validator_geo(network: str = Query("testnet")):
    """Return the manually-verified geography entries for validators whose
    location we've confirmed from their public website / social profiles.
    Frontend map reads from this instead of a hardcoded JS blob so the list
    can be updated by editing one file + git commit, and so the source is
    visible to anyone inspecting /api/."""
    path = Path(f"/opt/monadpulse/validator_geo_{network}.json")
    if not path.exists():
        return {"network": network, "validators": [], "source": "manually verified"}
    try:
        data = json.loads(path.read_text())
    except Exception:
        return {"network": network, "validators": [], "source": "manually verified"}
    data["source"] = "manually verified from operator public profiles"
    return data


GET_PROPOSER_VAL_ID_SELECTOR = "fbacb0be"  # get_proposer_val_id() — reads proposer of the given historical block
_MINER_DISCOVERY_CACHE: dict[tuple[str, int], tuple[float, list[str]]] = {}
MINER_DISCOVERY_TTL = 600  # 10 min


async def _discover_miners_via_rpc(val_id: int, network: str, pool) -> list[str]:
    """Fallback when the names map has no miners attributed to this val_id.
    For each unique proposer_address that produced blocks recently, probe
    get_proposer_val_id() at its most recent block — O(distinct proposers)
    RPC calls ≈ 200. Cached 10 min per (network, val_id)."""
    import time as _t
    import asyncio
    key = (network, val_id)
    cached = _MINER_DISCOVERY_CACHE.get(key)
    if cached and (_t.time() - cached[0]) < MINER_DISCOVERY_TTL:
        return cached[1]

    # Collect distinct (miner, recent block_number) from the last 6h of blocks.
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT proposer_address, MAX(block_number) AS bn
            FROM blocks
            WHERE network = $1
              AND timestamp > NOW() - INTERVAL '6 hours'
              AND proposer_address != '0x0000000000000000000000000000000000000000'
            GROUP BY proposer_address
            """,
            network,
        )

    url = RPC_URLS.get(network, RPC_URLS["testnet"])
    miners: set[str] = set()

    async def probe(client: httpx.AsyncClient, miner: str, bn: int) -> None:
        try:
            r = await client.post(url, json={
                "jsonrpc": "2.0", "method": "eth_call",
                "params": [{"to": STAKING_PRECOMPILE, "data": "0x" + GET_PROPOSER_VAL_ID_SELECTOR}, hex(bn)],
                "id": 1,
            })
            data = r.json()
            vid = int(data.get("result", "0x0"), 16) if data.get("result") else 0
            if vid == val_id:
                miners.add(miner.lower())
        except Exception:
            pass

    async with httpx.AsyncClient(timeout=15) as client:
        # Parallel probes, bounded concurrency.
        sem = asyncio.Semaphore(20)
        async def bounded(m, bn):
            async with sem:
                await probe(client, m, bn)
        await asyncio.gather(*(bounded(r["proposer_address"], int(r["bn"])) for r in rows))

    discovered = sorted(miners)
    _MINER_DISCOVERY_CACHE[key] = (_t.time(), discovered)
    return discovered


def _candidate_addrs_for_valid(val_id: int, auth: str, network: str) -> list[str]:
    """Return the set of addresses that can produce blocks for this validator.
    On testnet block.miner == auth. On mainnet the miner rotates across several
    ephemeral addrs, all attributed to the same canonical name in the names map.
    We seed with auth and add every miner the map attributes to the same name."""
    addrs = {auth.lower()}
    try:
        # Lazy-import to avoid circular deps with names.py
        from api.routes.names import _load as _load_names
    except Exception:
        return list(addrs)
    nmap = _load_names(network) or {}
    # Find the canonical name for this val_id via auth (testnet) or miners
    # (mainnet we don't know yet) — just walk the map and cluster by name.
    target_name = nmap.get(auth.lower())
    if not target_name:
        # auth may not be in the miner-indexed names map on mainnet; fall back
        # to the directory JSON which maps val_id → name directly.
        try:
            dir_path = Path(f"/opt/monadpulse/validator_directory_{network}.json")
            if dir_path.exists():
                for row in json.loads(dir_path.read_text()):
                    if int(row.get("val_id", -1)) == val_id:
                        target_name = row.get("name")
                        break
        except Exception:
            pass
    if target_name:
        for addr, nm in nmap.items():
            if nm == target_name:
                addrs.add(addr.lower())
    return list(addrs)


@router.get("/by-id/{val_id}/signing-uptime")
async def validator_signing_uptime(val_id: int, request: Request, network: str = Query("testnet")):
    """Proposing-share uptime over rolling 1h / 8h / 24h windows.

    Baseline is self-calibrated per validator: we take the validator's own
    24h-average share of total network blocks as the "expected" rate, and
    compare the 1h / 8h counts to that projection. This removes the
    stake-weighting problem (Monad proposer selection is stake-weighted —
    uniform average makes Backpack look 21× over-quota and small operators
    look permanently under-quota). For the 24h window itself, baseline is
    still validator's 24h share (so pct≈100% by definition unless there
    was a clear drop within the window).

    This is a proxy via proposing rate, not true BFT signing (RPC doesn't
    expose individual signer sets per block). Good enough to spot a node
    outage: if a validator stops proposing, their 1h count falls to 0
    while 24h share stays nonzero, and pct_1h crashes toward 0."""
    v = await _get_validator_onchain(val_id, network)
    if v is None:
        raise HTTPException(status_code=404, detail=f"validator {val_id} not found on {network}")
    auth_raw = v[0]
    auth = auth_raw if isinstance(auth_raw, str) else "0x" + auth_raw.hex()
    addrs = _candidate_addrs_for_valid(val_id, auth.lower(), network)

    pool = request.app.state.pool
    # If the only candidate is auth (name map knew nothing about this validator),
    # try RPC discovery — probes get_proposer_val_id() at historical blocks and
    # harvests real miner addresses. Necessary for validators missing from the
    # upstream validator-info repo (e.g. shadowoftime on testnet).
    if len(addrs) == 1:
        discovered = await _discover_miners_via_rpc(val_id, network, pool)
        if discovered:
            for a in discovered:
                if a not in addrs:
                    addrs.append(a)

    now = datetime.now(timezone.utc)
    # Single query that captures the three windows in one pass.
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT
              COUNT(*) FILTER (WHERE timestamp >= $2 AND proposer_address != '0x0000000000000000000000000000000000000000') AS total_1h,
              COUNT(*) FILTER (WHERE timestamp >= $2 AND proposer_address = ANY($5::text[])) AS actual_1h,
              COUNT(*) FILTER (WHERE timestamp >= $3 AND proposer_address != '0x0000000000000000000000000000000000000000') AS total_8h,
              COUNT(*) FILTER (WHERE timestamp >= $3 AND proposer_address = ANY($5::text[])) AS actual_8h,
              COUNT(*) FILTER (WHERE timestamp >= $4 AND proposer_address != '0x0000000000000000000000000000000000000000') AS total_24h,
              COUNT(*) FILTER (WHERE timestamp >= $4 AND proposer_address = ANY($5::text[])) AS actual_24h,
              COUNT(DISTINCT proposer_address) FILTER (WHERE timestamp >= $4 AND proposer_address != '0x0000000000000000000000000000000000000000') AS active_24h
            FROM blocks
            WHERE network = $1 AND timestamp >= $4
            """,
            network,
            now - timedelta(hours=1),
            now - timedelta(hours=8),
            now - timedelta(hours=24),
            addrs,
        )
    total_24h = int(row["total_24h"] or 0)
    actual_24h = int(row["actual_24h"] or 0)
    # Self-calibrated baseline: validator's 24h share of all proposals
    share_24h = (actual_24h / total_24h) if total_24h > 0 else 0

    def window(total: int, actual: int, hours: int) -> dict:
        expected = total * share_24h if share_24h > 0 else 0
        pct = min(100.0, actual / expected * 100) if expected > 0 else None
        return {
            "actual": actual,
            "expected": round(expected, 2),
            "pct": round(pct, 1) if pct is not None else None,
            "total_blocks": total,
            "hours": hours,
        }

    out = {
        "1h":  window(int(row["total_1h"] or 0),  int(row["actual_1h"] or 0),  1),
        "8h":  window(int(row["total_8h"] or 0),  int(row["actual_8h"] or 0),  8),
        "24h": window(total_24h, actual_24h, 24),
    }
    return {
        "validator_id": val_id,
        "network": network,
        "auth": auth.lower(),
        "candidate_addrs": addrs,
        "windows": out,
        "baseline": "self-24h-share",
        "share_24h": round(share_24h * 100, 3),
        "active_validators_24h": int(row["active_24h"] or 0),
        "note": "proxy via proposing rate; baseline is validator's own 24h share — 1h pct will crash toward 0 on outage",
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
