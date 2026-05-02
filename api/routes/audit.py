"""Network audit endpoint — live decentralization & rotation metrics.

One catch-all endpoint /audit/network?network=testnet|mainnet returns:
- geographic concentration (top countries, HHI)
- hosting concentration (top ISPs, top ASNs, datacenter share)
- subnet sharing (/24 + /16 collisions, all-anonymous clusters)
- shared-auth stake clusters (Category Labs reveal)
- Foundation rotation pattern (testnet only — daily ratchet, time-of-day)
- VDP rotation pool performance distribution (bimodal histogram + named zero-block list)

Cached 5 min — these aggregates are heavy to compute and don't change minute-to-minute.
"""
from __future__ import annotations

import ipaddress
import json
import time
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fastapi import APIRouter, Query, Request

router = APIRouter()

CACHE_TTL = 300
_CACHE: dict[tuple, tuple[float, dict]] = {}


def _load_geo(network: str) -> list[dict]:
    p = Path(f"/opt/monadpulse/validator_geo_full_{network}.json")
    if not p.exists():
        return []
    try:
        d = json.loads(p.read_text())
        return d.get("validators", d) if isinstance(d, dict) else d
    except Exception:
        return []


def _load_directory(network: str) -> list[dict]:
    p = Path(f"/opt/monadpulse/validator_directory_{network}.json")
    if not p.exists():
        return []
    try:
        return json.loads(p.read_text())
    except Exception:
        return []


def _hhi(counts: dict) -> int:
    """Herfindahl-Hirschman Index — sum of squared % shares × 100. >2500 = concentrated."""
    total = sum(counts.values())
    if not total:
        return 0
    return int(sum((n / total * 100) ** 2 for n in counts.values()))


def _aggregate_geo(network: str) -> dict:
    vs = _load_geo(network)
    if not vs:
        return {"total": 0}

    countries = Counter(v.get("country") for v in vs if v.get("country"))
    isps = Counter(v.get("isp") for v in vs if v.get("isp"))
    asns = Counter(v.get("asn") for v in vs if v.get("asn"))

    n = len(vs)
    dc = sum(1 for v in vs if v.get("datacenter"))

    # Subnet analysis
    n24 = defaultdict(list)
    n16 = Counter()
    for v in vs:
        ip = v.get("ip")
        if not ip:
            continue
        try:
            if ipaddress.ip_address(ip).version != 4:
                continue
        except Exception:
            continue
        parts = ip.split(".")
        n24[".".join(parts[:3]) + ".0/24"].append(v)
        n16[".".join(parts[:2]) + ".0.0/16"] += 1

    shared24 = sorted(
        [(k, vs_) for k, vs_ in n24.items() if len(vs_) >= 2],
        key=lambda x: -len(x[1]),
    )
    anon_clusters = []
    co_located = []
    for subnet, members in shared24:
        anonymous = [m for m in members if not m.get("name") or (m.get("name") or "").startswith("val#") or "Unknown" in (m.get("name") or "")]
        entry = {
            "subnet": subnet,
            "count": len(members),
            "isp": members[0].get("isp"),
            "city": members[0].get("city"),
            "country": members[0].get("country"),
            "anonymous": len(anonymous),
            "members": [
                {"name": m.get("name") or f"val#{m.get('val_id')}", "val_id": m.get("val_id"), "ip": m.get("ip")}
                for m in members[:8]
            ],
        }
        co_located.append(entry)
        if len(anonymous) == len(members) and len(members) >= 3:
            anon_clusters.append(entry)

    return {
        "total_with_geo": n,
        "datacenter_count": dc,
        "datacenter_pct": round(dc / n * 100, 1),
        "country_top": [{"country": c, "count": k, "pct": round(k / n * 100, 1)} for c, k in countries.most_common(10)],
        "country_total": len(countries),
        "country_hhi": _hhi(countries),
        "isp_top": [{"isp": i, "count": k, "pct": round(k / n * 100, 1)} for i, k in isps.most_common(10)],
        "isp_total": len(isps),
        "asn_top": [{"asn": a, "count": k, "pct": round(k / n * 100, 1)} for a, k in asns.most_common(8)],
        "asn_total": len(asns),
        "asn_hhi": _hhi(asns),
        "shared24_count": len(shared24),
        "shared24_validators": sum(len(vs_) for _, vs_ in shared24),
        "shared24_top": co_located[:10],
        "anonymous_clusters": anon_clusters[:8],
        "shared16_top": [{"subnet": s, "count": n} for s, n in n16.most_common(6) if n >= 5],
    }


async def _aggregate_stake_clusters(pool, network: str) -> dict:
    """Find auth addresses with multiple val_ids (Category Labs cluster pattern).
    Combined stake under one wallet = effective concentration."""
    directory = _load_directory(network)
    auth_to_vids = defaultdict(list)
    for e in directory:
        auth = (e.get("auth") or "").lower()
        if auth and e.get("val_id") is not None:
            auth_to_vids[auth].append({"val_id": int(e["val_id"]), "name": e.get("name")})

    shared = [(a, vs) for a, vs in auth_to_vids.items() if len(vs) > 1]
    out_clusters = []

    if shared:
        async with pool.acquire() as conn:
            current_epoch = await conn.fetchval(
                "SELECT MAX(epoch) FROM validator_stake_history WHERE network = $1",
                network,
            )
            for auth, vids in shared:
                rows = await conn.fetch(
                    "SELECT val_id, total_stake FROM validator_stake_history "
                    "WHERE network = $1 AND epoch = $2 AND val_id = ANY($3::int[])",
                    network, current_epoch, [v["val_id"] for v in vids],
                )
                stake_by_vid = {int(r["val_id"]): int(r["total_stake"]) for r in rows}
                total = sum(stake_by_vid.values())
                out_clusters.append({
                    "auth": auth,
                    "val_count": len(vids),
                    "total_stake_mon": total // 10**18,
                    "members": [
                        {"val_id": v["val_id"], "name": v["name"], "stake_mon": stake_by_vid.get(v["val_id"], 0) // 10**18}
                        for v in sorted(vids, key=lambda x: x["val_id"])
                    ],
                })
    out_clusters.sort(key=lambda x: -x["total_stake_mon"])
    return {"clusters": out_clusters}


async def _aggregate_rotation(pool, network: str) -> dict:
    """Foundation rotation pattern — meaningful only on testnet (mainnet has no VDP rotation)."""
    if network != "testnet":
        return {"network": network, "applicable": False}
    async with pool.acquire() as conn:
        daily = await conn.fetch(
            """
            SELECT date_trunc('day', timestamp) AS d,
                   COUNT(*) FILTER (WHERE event_type='undelegate') AS out_count,
                   COUNT(*) FILTER (WHERE event_type='delegate') AS in_count,
                   COUNT(DISTINCT validator_id) AS distinct_vals
            FROM stake_events
            WHERE network = $1
              AND delegator ILIKE '0xf235ab9b%'
              AND amount::numeric >= 1900000::numeric * 1000000000000000000::numeric
              AND timestamp > NOW() - INTERVAL '14 days'
            GROUP BY d
            ORDER BY d ASC
            """,
            network,
        )
        hourly = await conn.fetch(
            """
            SELECT EXTRACT(hour FROM timestamp)::int AS h, COUNT(*) AS n
            FROM stake_events
            WHERE network = $1
              AND delegator ILIKE '0xf235ab9b%'
              AND event_type IN ('delegate','undelegate')
              AND amount::numeric >= 1900000::numeric * 1000000000000000000::numeric
              AND timestamp > NOW() - INTERVAL '14 days'
            GROUP BY h ORDER BY h
            """,
            network,
        )
        pool_size_row = await conn.fetchrow(
            """
            SELECT COUNT(DISTINCT validator_id) AS pool_size
            FROM stake_events
            WHERE network = $1
              AND delegator ILIKE '0xf235ab9b%'
              AND event_type='delegate'
              AND amount::numeric >= 1900000::numeric * 1000000000000000000::numeric
              AND timestamp > NOW() - INTERVAL '30 days'
            """,
            network,
        )

    return {
        "network": network,
        "applicable": True,
        "rotation_pool_size": int(pool_size_row["pool_size"] or 0),
        "daily": [
            {"date": r["d"].date().isoformat(), "out": int(r["out_count"]), "in": int(r["in_count"]), "distinct_vals": int(r["distinct_vals"])}
            for r in daily
        ],
        "hourly_distribution": [{"hour": int(r["h"]), "events": int(r["n"])} for r in hourly],
    }


async def _aggregate_performance(pool, network: str) -> dict:
    """VDP rotation pool: blocks-produced distribution + named zero-block list (testnet only)."""
    if network != "testnet":
        return {"network": network, "applicable": False}
    directory = _load_directory(network)
    vid_to_name = {int(e["val_id"]): e.get("name") for e in directory if e.get("val_id") is not None}
    auth_to_vid = {(e.get("auth") or "").lower(): int(e["val_id"]) for e in directory if e.get("val_id") is not None and e.get("auth")}
    async with pool.acquire() as conn:
        rotation_rows = await conn.fetch(
            """
            SELECT DISTINCT validator_id::int AS val_id
            FROM stake_events
            WHERE network = $1
              AND delegator ILIKE '0xf235ab9b%'
              AND event_type IN ('delegate','undelegate')
              AND amount::numeric >= 1900000::numeric * 1000000000000000000::numeric
              AND timestamp > NOW() - INTERVAL '14 days'
            """,
            network,
        )
        rotation_vids = [int(r["val_id"]) for r in rotation_rows]
        proposed_rows = await conn.fetch(
            """
            SELECT lower(proposer_address) AS auth, COUNT(*) AS blocks_7d
            FROM blocks WHERE network = $1 AND timestamp > NOW() - INTERVAL '7 days'
            GROUP BY lower(proposer_address)
            """,
            network,
        )
        blocks_by_auth = {r["auth"]: int(r["blocks_7d"]) for r in proposed_rows}
    # Reverse: val_id → auth from directory, then look up blocks
    vid_to_auth = {v: k for k, v in auth_to_vid.items()}
    items = []
    for vid in rotation_vids:
        auth = vid_to_auth.get(vid)
        items.append({
            "val_id": vid,
            "name": vid_to_name.get(vid),
            "blocks_7d": blocks_by_auth.get(auth, 0),
        })
    items.sort(key=lambda x: (-x["blocks_7d"], x["val_id"]))

    buckets = {"0": 0, "1-999": 0, "1000-2999": 0, "3000-4999": 0, "5000+": 0}
    for it in items:
        b = it["blocks_7d"]
        if b == 0:
            buckets["0"] += 1
        elif b < 1000:
            buckets["1-999"] += 1
        elif b < 3000:
            buckets["1000-2999"] += 1
        elif b < 5000:
            buckets["3000-4999"] += 1
        else:
            buckets["5000+"] += 1

    zero_named = [it for it in items if it["blocks_7d"] == 0 and it["name"]]
    top_performers = items[:10]

    return {
        "network": network,
        "applicable": True,
        "pool_size": len(items),
        "buckets": buckets,
        "zero_blocks_named": zero_named[:30],
        "top_performers": top_performers,
    }


@router.get("/network")
async def network_audit(request: Request, network: str = Query("testnet")):
    cache_key = ("net", network)
    now = time.time()
    cached = _CACHE.get(cache_key)
    if cached and now - cached[0] < CACHE_TTL:
        return cached[1]

    pool = request.app.state.pool
    geo = _aggregate_geo(network)
    stake_clusters = await _aggregate_stake_clusters(pool, network)
    rotation = await _aggregate_rotation(pool, network)
    performance = await _aggregate_performance(pool, network)

    result = {
        "network": network,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "geo": geo,
        "stake_clusters": stake_clusters,
        "rotation": rotation,
        "performance": performance,
    }
    _CACHE[cache_key] = (now, result)
    return result
