"""Delegation graph analytics — clusters, Sankey edges, cartel signals.

Data source: /opt/monadpulse/delegation_graph_{network}.json, rebuilt per
epoch boundary by scripts/rebuild_delegation_graph.py.
"""
from __future__ import annotations

import json
from pathlib import Path

from fastapi import APIRouter, HTTPException, Query

router = APIRouter()

_GRAPH_CACHE: dict[str, tuple[float, list]] = {}


def _load_graph(network: str) -> list[dict]:
    path = Path(f"/opt/monadpulse/delegation_graph_{network}.json")
    if not path.exists():
        return []
    mtime = path.stat().st_mtime
    cached = _GRAPH_CACHE.get(network)
    if cached and cached[0] == mtime:
        return cached[1]
    data = json.loads(path.read_text())
    _GRAPH_CACHE[network] = (mtime, data)
    return data


def _load_names(network: str) -> dict:
    p = Path(f"/opt/monadpulse/validator_directory_{network}.json")
    if not p.exists():
        return {}
    directory = json.loads(p.read_text())
    return {e["val_id"]: e.get("name") for e in directory if e.get("val_id")}


@router.get("/delegator-clusters")
async def delegator_clusters(
    network: str = Query("testnet"),
    min_validators: int = Query(2, ge=1, description="Only return delegators staking to ≥ this many validators"),
    limit: int = Query(50, le=200),
):
    """Top delegators ranked by the number of distinct validators they stake to.
    High values (>10) are a cartel-like signal (Foundation-style redistribution,
    single operator running multiple validators, or coordinated group)."""
    edges = _load_graph(network)
    if not edges:
        raise HTTPException(404, "graph_not_ready")

    names = _load_names(network)
    agg: dict[str, dict] = {}
    for e in edges:
        d = e["delegator"]
        agg.setdefault(d, {"delegator": d, "validator_count": 0, "total_stake_wei": 0, "validators": []})
        agg[d]["validator_count"] += 1
        agg[d]["total_stake_wei"] += int(e["active_stake_wei"])
        agg[d]["validators"].append({
            "val_id": e["val_id"],
            "name": names.get(e["val_id"]),
            "stake_wei": int(e["active_stake_wei"]),
        })

    results = [
        a for a in agg.values() if a["validator_count"] >= min_validators
    ]
    results.sort(key=lambda x: (-x["validator_count"], -x["total_stake_wei"]))
    results = results[:limit]
    for r in results:
        r["total_stake_mon"] = r["total_stake_wei"] / 10 ** 18
        r["validators"].sort(key=lambda v: -v["stake_wei"])
    return {"network": network, "total_delegators": len(agg), "returned": len(results), "clusters": results}


@router.get("/delegation-graph")
async def delegation_graph(
    network: str = Query("testnet"),
    top_delegators: int = Query(30, le=100),
    top_validators: int = Query(30, le=100),
):
    """Return nodes + edges for a delegator→validator Sankey flow, limited to
    top-N of each side by total stake. Keeps the viz readable."""
    edges = _load_graph(network)
    if not edges:
        raise HTTPException(404, "graph_not_ready")
    names = _load_names(network)

    # Total stake per delegator and per validator
    by_deleg: dict[str, int] = {}
    by_val: dict[int, int] = {}
    for e in edges:
        by_deleg[e["delegator"]] = by_deleg.get(e["delegator"], 0) + int(e["active_stake_wei"])
        by_val[e["val_id"]] = by_val.get(e["val_id"], 0) + int(e["active_stake_wei"])

    top_dels = {d for d, _ in sorted(by_deleg.items(), key=lambda x: -x[1])[:top_delegators]}
    top_vals = {v for v, _ in sorted(by_val.items(), key=lambda x: -x[1])[:top_validators]}

    delegator_nodes = [
        {
            "id": f"d:{d}",
            "label": d,
            "type": "delegator",
            "total_stake_wei": by_deleg[d],
            "total_stake_mon": by_deleg[d] / 10 ** 18,
        }
        for d in sorted(top_dels, key=lambda x: -by_deleg[x])
    ]
    validator_nodes = [
        {
            "id": f"v:{v}",
            "label": names.get(v) or f"val #{v}",
            "val_id": v,
            "type": "validator",
            "total_stake_wei": by_val[v],
            "total_stake_mon": by_val[v] / 10 ** 18,
        }
        for v in sorted(top_vals, key=lambda x: -by_val[x])
    ]

    filtered_edges = [
        {
            "source": f"d:{e['delegator']}",
            "target": f"v:{e['val_id']}",
            "value_wei": int(e["active_stake_wei"]),
            "value_mon": int(e["active_stake_wei"]) / 10 ** 18,
        }
        for e in edges
        if e["delegator"] in top_dels and e["val_id"] in top_vals
    ]

    return {
        "network": network,
        "total_delegators": len(by_deleg),
        "total_validators": len(by_val),
        "nodes": {"delegators": delegator_nodes, "validators": validator_nodes},
        "edges": filtered_edges,
    }
