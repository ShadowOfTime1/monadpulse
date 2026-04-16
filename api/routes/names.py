import json
from pathlib import Path
from fastapi import APIRouter
from fastapi.responses import JSONResponse

router = APIRouter()

NAMES_FILE = Path("/opt/monadpulse/validator_names.json")
_cache = {}
_cache_mtime = 0


def _load():
    global _cache, _cache_mtime
    if not NAMES_FILE.exists():
        return _cache
    mtime = NAMES_FILE.stat().st_mtime
    if mtime != _cache_mtime:
        _cache = json.loads(NAMES_FILE.read_text())
        _cache_mtime = mtime
    return _cache


@router.get("/map")
async def name_map():
    return JSONResponse(_load(), headers={"Cache-Control": "public, max-age=3600"})
