import time
import os
import httpx
from fastapi import APIRouter

router = APIRouter()

_version_cache = {"version": None, "fetched": 0}
_releases_cache = {"data": None, "fetched": 0}


@router.get("/current")
async def current_version():
    now = time.time()
    if _version_cache["version"] and now - _version_cache["fetched"] < 300:
        return {"version": _version_cache["version"]}
    try:
        rpc = os.environ.get("TESTNET_RPC", "http://localhost:8080")
        async with httpx.AsyncClient(timeout=5) as client:
            r = await client.post(rpc, json={
                "jsonrpc": "2.0", "method": "web3_clientVersion", "params": [], "id": 1
            })
            ver = r.json().get("result", "unknown")
            _version_cache["version"] = ver
            _version_cache["fetched"] = now
            return {"version": ver}
    except Exception:
        return {"version": _version_cache["version"] or "unknown"}


@router.get("/releases")
async def github_releases():
    now = time.time()
    if _releases_cache["data"] and now - _releases_cache["fetched"] < 900:
        return _releases_cache["data"]
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(
                "https://api.github.com/repos/category-labs/monad/releases?per_page=10",
                headers={"Accept": "application/vnd.github+json"},
            )
            releases = r.json()
        result = []
        for rel in sorted(releases, key=lambda r: r.get("published_at", ""), reverse=True):
            if not isinstance(rel, dict):
                continue
            result.append({
                "tag": rel.get("tag_name", ""),
                "name": rel.get("name", ""),
                "date": rel.get("published_at", ""),
                "prerelease": rel.get("prerelease", False),
                "url": rel.get("html_url", ""),
                "body": (rel.get("body", "") or "")[:500],
            })
        _releases_cache["data"] = result
        _releases_cache["fetched"] = now
        return result
    except Exception:
        return _releases_cache["data"] or []


@router.get("/status")
async def upgrade_status():
    ver_data = await current_version()
    rel_data = await github_releases()

    current = ver_data.get("version", "unknown")
    current_ver = current.split("/")[-1] if "/" in current else current

    latest = None
    latest_stable = None
    for r in rel_data:
        if not latest:
            latest = r
        if not r.get("prerelease") and not latest_stable:
            latest_stable = r
        if latest and latest_stable:
            break

    result = {
        "current_version": current_ver,
        "latest_release": latest_stable or latest,
        "all_releases": rel_data[:5],
        "up_to_date": False,
        "hours_since_release": None,
    }

    if latest_stable:
        tag = latest_stable["tag"].lstrip("v")
        result["up_to_date"] = current_ver == tag
        from datetime import datetime, timezone
        try:
            rel_time = datetime.fromisoformat(latest_stable["date"].replace("Z", "+00:00"))
            hours = (datetime.now(timezone.utc) - rel_time).total_seconds() / 3600
            result["hours_since_release"] = round(hours, 1)
        except Exception:
            pass

    return result
