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


def _normalize_isp(name: str) -> str:
    """Strip corporate suffixes so 'Limestone Networks Inc.' and 'Limestone Networks'
    don't fragment into separate buckets. ASN-grouping is the proper fix for the
    final HHI; this normalization is the fallback for the visible top-N table."""
    if not name:
        return name
    s = name.strip()
    for suffix in (", Inc.", " Inc.", ", LLC", " LLC", ", Ltd.", " Ltd.", " Limited",
                   " GmbH", " S.A.", " SAS", " sp. z o.o.", " UAB", " s.r.o.", " AG",
                   " Co.", " B.V.", " Pty Ltd", " AB", " Oy"):
        if s.endswith(suffix):
            s = s[: -len(suffix)].rstrip(",").rstrip()
    return s


def _aggregate_geo(network: str) -> dict:
    vs = _load_geo(network)
    if not vs:
        return {"total": 0}

    # Split: registered validators (have val_id) vs unidentified P2P peers.
    # Hosting/geo claims should be made about the validator set, not the
    # full peer crawl which includes RPC nodes, sentries, etc.
    registered = [v for v in vs if v.get("val_id") is not None]
    peers_only = len(vs) - len(registered)

    # Build ASN→canonical-ISP map and merge by ASN where ASN is known.
    # Where ASN is missing, fall back to suffix-stripped ISP name.
    asn_to_isp_canonical: dict[str, str] = {}
    for v in registered:
        asn, isp = v.get("asn"), v.get("isp")
        if asn and isp and asn not in asn_to_isp_canonical:
            asn_to_isp_canonical[asn] = _normalize_isp(isp)

    def canonical_isp(v):
        asn = v.get("asn")
        if asn and asn in asn_to_isp_canonical:
            return asn_to_isp_canonical[asn]
        return _normalize_isp(v.get("isp")) if v.get("isp") else None

    countries = Counter(v.get("country") for v in registered if v.get("country"))
    isps = Counter(canonical_isp(v) for v in registered if canonical_isp(v))
    asns = Counter(v.get("asn") for v in registered if v.get("asn"))

    n = len(registered)
    dc = sum(1 for v in registered if v.get("datacenter"))

    # Subnet analysis (over registered validators only)
    n24 = defaultdict(list)
    n16 = Counter()
    for v in registered:
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
            "isp": canonical_isp(members[0]),  # use normalized ISP name (consistent with bar chart)
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

    # For the "checked N" denominator on the anonymous-clusters empty-state.
    # We checked every /24 subnet that holds ≥2 registered validators (i.e.
    # the candidate set for a cluster). Single-validator subnets and the
    # `peers_only` peer crawl are NOT in scope — the anon-cluster check is
    # specifically about co-located groups.
    total_subnets_inspected = len(shared24)
    total_distinct_subnets = len(n24)

    return {
        "total_with_geo": n,  # registered validators with geo data
        "registered_count": n,
        "peers_only_count": peers_only,
        "subnets_inspected_for_anon": total_subnets_inspected,
        "distinct_subnets_total": total_distinct_subnets,
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


async def _enrich_geo_with_stake_hhi(pool, network: str, geo: dict) -> dict:
    """Compute STAKE-WEIGHTED country and ASN HHI on top of the count-based numbers.
    Stake-weighted is the metric that actually matters for consensus security:
    one whale validator at 100M MON in country X moves the dial more than ten
    small validators in country Y. Without this, count-HHI under-reports real
    concentration when stake is uneven."""
    vs = _load_geo(network)
    registered = [v for v in vs if v.get("val_id") is not None]
    if not registered:
        return geo
    val_ids = [int(v["val_id"]) for v in registered]
    async with pool.acquire() as conn:
        current_epoch = await conn.fetchval(
            "SELECT MAX(epoch) FROM validator_stake_history WHERE network = $1", network,
        )
        if current_epoch is None:
            return geo
        # Stake-weighted HHI must reflect VOTING stake. A validator rotated
        # out of the active set has consensus_stake=0 (their MON doesn't vote
        # this epoch); we must exclude them, otherwise stake-HHI counts MON
        # that has no consensus weight. Pre-migration rows where the column
        # is NULL fall back to total_stake (transitional, expected to phase
        # out as old epochs roll off).
        rows = await conn.fetch(
            """
            SELECT val_id,
                   COALESCE(consensus_stake, total_stake)::numeric AS stake
            FROM validator_stake_history
            WHERE network = $1 AND epoch = $2 AND val_id = ANY($3::int[])
              AND (consensus_stake IS NULL OR consensus_stake > 0)
            """,
            network, current_epoch, val_ids,
        )
    stake_by_vid = {int(r["val_id"]): int(r["stake"]) for r in rows}

    # Same canonical-ISP map as in _aggregate_geo
    asn_to_isp_canonical: dict[str, str] = {}
    for v in registered:
        asn, isp = v.get("asn"), v.get("isp")
        if asn and isp and asn not in asn_to_isp_canonical:
            asn_to_isp_canonical[asn] = _normalize_isp(isp)

    def canonical_isp(v):
        asn = v.get("asn")
        if asn and asn in asn_to_isp_canonical:
            return asn_to_isp_canonical[asn]
        return _normalize_isp(v.get("isp")) if v.get("isp") else None

    weighted_countries: dict[str, int] = {}
    weighted_asns: dict[str, int] = {}
    for v in registered:
        stake = stake_by_vid.get(int(v["val_id"]), 0)
        if stake <= 0:
            continue
        c = v.get("country")
        if c:
            weighted_countries[c] = weighted_countries.get(c, 0) + stake
        a = v.get("asn")
        if a:
            weighted_asns[a] = weighted_asns.get(a, 0) + stake

    geo["stake_country_hhi"] = _hhi(weighted_countries)
    geo["stake_asn_hhi"] = _hhi(weighted_asns)
    geo["stake_weighted_top_country"] = (
        sorted(weighted_countries.items(), key=lambda x: -x[1])[:3]
        if weighted_countries else []
    )
    return geo


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


FOUNDATION_ADDR = "0xf235ab9b2f80a9569079c0d62aab91024f4dd61e"
ROTATION_AMOUNT_MIN = 1_900_000 * 10**18   # ≥1.9M MON: rotation move
ROTATION_AMOUNT_MAX = 2_500_000 * 10**18   # ≤2.5M MON: still rotation, not 10.9M onboarding


async def _aggregate_rotation(pool, network: str) -> dict:
    """Foundation rotation pattern — meaningful only on testnet (mainnet has no VDP rotation).

    Rotation moves are ~2M MON. Onboarding events (initial 10.9M Foundation
    delegate to a new validator) used to slip through the prior ≥1.9M filter
    and inflated the daily counts on onboarding-batch days. Capping at 2.5M
    excludes onboardings while keeping every real rotation move."""
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
              AND lower(delegator) = $2
              AND amount::numeric BETWEEN $3 AND $4
              AND timestamp > NOW() - INTERVAL '14 days'
            GROUP BY d
            ORDER BY d ASC
            """,
            network, FOUNDATION_ADDR, str(ROTATION_AMOUNT_MIN), str(ROTATION_AMOUNT_MAX),
        )
        hourly = await conn.fetch(
            """
            SELECT EXTRACT(hour FROM timestamp)::int AS h, COUNT(*) AS n
            FROM stake_events
            WHERE network = $1
              AND lower(delegator) = $2
              AND event_type IN ('delegate','undelegate')
              AND amount::numeric BETWEEN $3 AND $4
              AND timestamp > NOW() - INTERVAL '14 days'
            GROUP BY h ORDER BY h
            """,
            network, FOUNDATION_ADDR, str(ROTATION_AMOUNT_MIN), str(ROTATION_AMOUNT_MAX),
        )
        pool_size_row = await conn.fetchrow(
            """
            SELECT COUNT(DISTINCT validator_id) AS pool_size
            FROM stake_events
            WHERE network = $1
              AND lower(delegator) = $2
              AND event_type='delegate'
              AND amount::numeric BETWEEN $3 AND $4
              AND timestamp > NOW() - INTERVAL '30 days'
            """,
            network, FOUNDATION_ADDR, str(ROTATION_AMOUNT_MIN), str(ROTATION_AMOUNT_MAX),
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
    """VDP rotation pool: blocks-produced distribution + named zero-block list (testnet only).

    Zero-block flag requires ≥14 days in the rotation pool (i.e. first
    Foundation delegate to that val_id was at least 14d ago). Without this
    gate, a validator who joined rotation 3 days ago and hasn't been picked
    yet would be falsely flagged as 'stuck'."""
    if network != "testnet":
        return {"network": network, "applicable": False}
    directory = _load_directory(network)
    # Build val_id → auth direct (avoids the lossy reverse-from-auth-dict
    # collapse for shared-auth clusters like Category Labs val 8/9/10/12).
    vid_to_name: dict[int, str] = {}
    vid_to_auth: dict[int, str] = {}
    for e in directory:
        if e.get("val_id") is None:
            continue
        vid = int(e["val_id"])
        vid_to_name[vid] = e.get("name")
        if e.get("auth"):
            vid_to_auth[vid] = e["auth"].lower()

    async with pool.acquire() as conn:
        rotation_rows = await conn.fetch(
            """
            SELECT validator_id::int AS val_id,
                   MIN(timestamp) FILTER (WHERE event_type='delegate') AS first_delegate
            FROM stake_events
            WHERE network = $1
              AND lower(delegator) = $2
              AND event_type IN ('delegate','undelegate')
              AND amount::numeric BETWEEN $3 AND $4
              AND timestamp > NOW() - INTERVAL '30 days'
            GROUP BY val_id
            HAVING MAX(timestamp) > NOW() - INTERVAL '14 days'
            """,
            network, FOUNDATION_ADDR, str(ROTATION_AMOUNT_MIN), str(ROTATION_AMOUNT_MAX),
        )
        # Tenure = days since the FIRST Foundation delegate of any amount.
        # This catches the initial 10.9M onboarding event AND every subsequent
        # rotation move. MIN over all delegates is monotonic — re-delegation
        # after a full undelegate cycle does NOT reset the clock (the original
        # earliest timestamp is preserved).
        first_delegate_rows = await conn.fetch(
            """
            SELECT validator_id::int AS val_id, MIN(timestamp) AS first_delegate
            FROM stake_events
            WHERE network = $1
              AND lower(delegator) = $2
              AND event_type = 'delegate'
            GROUP BY val_id
            """,
            network, FOUNDATION_ADDR,
        )
        first_delegate_by_vid = {int(r["val_id"]): r["first_delegate"] for r in first_delegate_rows}
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

    now = datetime.now(timezone.utc)
    items = []
    for vid in rotation_vids:
        auth = vid_to_auth.get(vid)
        first_delegate = first_delegate_by_vid.get(vid)
        days_in_pool = None
        if first_delegate:
            if first_delegate.tzinfo is None:
                first_delegate = first_delegate.replace(tzinfo=timezone.utc)
            days_in_pool = int((now - first_delegate).total_seconds() / 86400)
        items.append({
            "val_id": vid,
            "name": vid_to_name.get(vid),
            "blocks_7d": blocks_by_auth.get(auth, 0),
            "days_in_pool": days_in_pool,
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

    # Zero-block list requires both a name AND ≥14 days in the pool —
    # so we never label a recent enrollee as "stuck".
    zero_named = [it for it in items if it["blocks_7d"] == 0 and it["name"] and (it.get("days_in_pool") or 0) >= 14]
    zero_recent = [it for it in items if it["blocks_7d"] == 0 and it["name"] and (it.get("days_in_pool") or 0) < 14]
    top_performers = items[:10]

    return {
        "network": network,
        "applicable": True,
        "pool_size": len(items),
        "buckets": buckets,
        "zero_blocks_named": zero_named[:30],   # ≥14d in pool — defensible "stuck"
        "zero_blocks_recent_count": len(zero_recent),  # joined recently — separate
        "top_performers": top_performers,
        "tenure_threshold_days": 14,
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
    geo = await _enrich_geo_with_stake_hhi(pool, network, geo)
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
