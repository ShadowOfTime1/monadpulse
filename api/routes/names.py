import json
from pathlib import Path
from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

router = APIRouter()

_cache = {}
_cache_mtime = {}

LEGACY = Path("/opt/monadpulse/validator_names.json")
PER_NET = {
    "testnet": Path("/opt/monadpulse/validator_names_testnet.json"),
    "mainnet": Path("/opt/monadpulse/validator_names_mainnet.json"),
}


def _load(network: str) -> dict:
    global _cache, _cache_mtime
    files = [PER_NET.get(network), LEGACY]
    merged: dict = {}
    for f in files:
        if not f or not f.exists():
            continue
        key = str(f)
        mtime = f.stat().st_mtime
        if _cache_mtime.get(key) != mtime:
            _cache[key] = json.loads(f.read_text())
            _cache_mtime[key] = mtime
        # Per-network file takes precedence; legacy fills gaps
        for addr, name in _cache[key].items():
            merged.setdefault(addr.lower(), name)
    return merged


@router.get("/map")
async def name_map(network: str = Query("testnet")):
    return JSONResponse(_load(network), headers={"Cache-Control": "public, max-age=3600"})
