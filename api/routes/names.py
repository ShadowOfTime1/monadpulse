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
DIRECTORY = {
    "testnet": Path("/opt/monadpulse/validator_directory_testnet.json"),
    "mainnet": Path("/opt/monadpulse/validator_directory_mainnet.json"),
}


def _load(network: str) -> dict:
    """
    Merge miner→name (names_map) and auth→name (directory) into one lookup table.
    names_map is populated from block proposer observations — covers active
    validators. directory is the upstream validator-info registry — covers any
    validator with a PR, including ones that haven't proposed yet (PENDING,
    freshly added, or currently offline). Both keys lowercase.
    """
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
    # Add auth addresses from the directory (for PENDING / not-yet-proposed
    # validators whose miner address hasn't been observed and so isn't in
    # names_map). Only entries with a non-empty name.
    dir_path = DIRECTORY.get(network)
    if dir_path and dir_path.exists():
        key = str(dir_path)
        mtime = dir_path.stat().st_mtime
        if _cache_mtime.get(key) != mtime:
            _cache[key] = json.loads(dir_path.read_text())
            _cache_mtime[key] = mtime
        for entry in _cache[key]:
            auth = (entry.get("auth") or "").lower()
            name = entry.get("name")
            if auth and name:
                merged.setdefault(auth, name)
    return merged


@router.get("/map")
async def name_map(network: str = Query("testnet")):
    return JSONResponse(_load(network), headers={"Cache-Control": "public, max-age=3600"})
