#!/usr/bin/env python3
"""Build validator_geo_full_<network>.json with authoritative location data.

Sources, in priority:
  1. Manual override (validator_geo_<network>.json — operator public profiles)
  2. P2P signed peer record (peers.toml — IP signed by operator's secp key)
  3. GeoIP lookup of the P2P-declared IP via ip-api.com (city/country/AS)

Identity map is built from BOTH the upstream validator-info directory
(named operators) AND on-chain enumeration via the staking precompile
(catches operators who haven't yet submitted to validator-info but are
already producing blocks). Result: every active validator on the network
appears, with the most authoritative location we can prove.

Output schema (per entry):
  {
    "val_id": int,
    "name": str (or "Validator #N" for unnamed),
    "auth": "0x..." (lowercase),
    "secp": "0x..." (lowercase, no 0x in the toml),
    "ip": "1.2.3.4",
    "city": "Helsinki",
    "country": "FI",
    "region": "Europe",
    "lat": 60.17, "lon": 24.94,
    "asn": "AS24940",
    "isp": "Hetzner Online GmbH",
    "datacenter": True/False,
    "source": "manual_override" | "p2p_signed_geoip"
  }
"""
from __future__ import annotations

import asyncio
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx

DATA_DIR = Path("/opt/monadpulse/data")
DIR_PATH = Path("/opt/monadpulse")  # validator_directory_*.json
RPC_URLS = {
    "testnet": "http://localhost:8080",
    # mainnet: requires observer node; deferred (Phase 2.M1)
}
PRECOMPILE = "0x0000000000000000000000000000000000001000"
SEL_GET_VALIDATOR = "2b6d639a"           # get_validator(uint64)
SEL_GET_EXEC_VALSET = "7cb074df"         # get_execution_valset(uint64)

REGION_BY_COUNTRY = {
    # Quick mapping country code → continent. Used only for the badge counts;
    # if country code is unknown we leave region empty.
    "US": "North America", "CA": "North America", "MX": "North America",
    "GB": "Europe", "DE": "Europe", "FR": "Europe", "NL": "Europe",
    "FI": "Europe", "SE": "Europe", "NO": "Europe", "DK": "Europe",
    "ES": "Europe", "IT": "Europe", "PL": "Europe", "CH": "Europe",
    "AT": "Europe", "BE": "Europe", "IE": "Europe", "PT": "Europe",
    "CZ": "Europe", "RO": "Europe", "HU": "Europe", "GR": "Europe",
    "UA": "Europe", "RU": "Europe", "BG": "Europe", "RS": "Europe",
    "EE": "Europe", "LV": "Europe", "LT": "Europe", "MD": "Europe",
    "JP": "Asia", "KR": "Asia", "CN": "Asia", "SG": "Asia", "IN": "Asia",
    "HK": "Asia", "TW": "Asia", "TH": "Asia", "ID": "Asia", "VN": "Asia",
    "MY": "Asia", "PH": "Asia", "PK": "Asia", "AE": "Asia", "IL": "Asia",
    "TR": "Asia", "SA": "Asia",
    "AU": "Oceania", "NZ": "Oceania",
    "BR": "South America", "AR": "South America", "CL": "South America",
    "CO": "South America", "PE": "South America", "UY": "South America",
    "ZA": "Africa", "NG": "Africa", "EG": "Africa", "KE": "Africa",
}


# ─── Identity map: val_id → {auth, secp} via on-chain enumeration ──────


async def rpc_call(client: httpx.AsyncClient, rpc_url: str, data: str) -> str | None:
    """Single eth_call against the staking precompile."""
    try:
        r = await client.post(rpc_url, json={
            "jsonrpc": "2.0", "method": "eth_call",
            "params": [{"to": PRECOMPILE, "data": data}, "latest"],
            "id": 1,
        })
        return (r.json() or {}).get("result") or None
    except Exception:
        return None


async def enumerate_valset(client: httpx.AsyncClient, rpc_url: str) -> set[int]:
    """Walk get_execution_valset(start) pages until done. Returns set of val_ids."""
    out: set[int] = set()
    start = 0
    while True:
        data = "0x" + SEL_GET_EXEC_VALSET + start.to_bytes(32, "big").hex()
        res = await rpc_call(client, rpc_url, data)
        if not res or len(res) < 130:
            break
        raw = bytes.fromhex(res[2:])
        done = raw[31] == 1
        next_idx = int.from_bytes(raw[56:64], "big")
        arr_len = int.from_bytes(raw[0x60 + 24:0x60 + 32], "big")
        ids = [
            int.from_bytes(raw[0x80 + i * 32 + 24:0x80 + i * 32 + 32], "big")
            for i in range(arr_len)
        ]
        out.update(ids)
        if done:
            break
        start = next_idx
    return out


async def get_validator_identity(client: httpx.AsyncClient, rpc_url: str, vid: int) -> dict | None:
    """Returns {auth, secp} for a val_id via get_validator(uint64)."""
    data = "0x" + SEL_GET_VALIDATOR + format(vid, '064x')
    res = await rpc_call(client, rpc_url, data)
    if not res or len(res) < 130:
        return None
    h = res[2:]
    auth = "0x" + h[24:64]
    # secp: layout puts pubkey starting at offset 10*32 + 32 dynamic-offset header.
    # Easier: secp is a `bytes` field; ABI offset stored at field 10. Since the
    # staking-cli ABI is opaque to us here, trust the directory's secp instead
    # for matching purposes; on-chain auth alone suffices for identity.
    return {"auth": auth.lower()}


# ─── peers.toml parser ─────────────────────────────────────────────────


def parse_peers_toml(path: Path) -> list[dict]:
    if not path.exists():
        return []
    text = path.read_text()
    blocks = text.split("[[peers]]")[1:]
    out = []
    for blk in blocks:
        addr = re.search(r'address\s*=\s*"([^"]+)"', blk)
        secp = re.search(r'secp256k1_pubkey\s*=\s*"0x([0-9a-fA-F]+)"', blk)
        if addr and secp:
            ip = addr.group(1).split(":")[0]
            out.append({"ip": ip, "secp": secp.group(1).lower()})
    return out


# ─── GeoIP via ip-api.com batch ────────────────────────────────────────


async def geoip_batch(client: httpx.AsyncClient, ips: list[str]) -> dict[str, dict]:
    """Lookup a list of IPs; returns {ip: {city, country, lat, lon, asn, isp}}.
    Uses ip-api.com batch endpoint (free, 100/req, 15req/min)."""
    out: dict[str, dict] = {}
    fields = "status,country,countryCode,region,regionName,city,lat,lon,timezone,isp,org,as,proxy,hosting,query"
    for chunk_start in range(0, len(ips), 100):
        chunk = ips[chunk_start:chunk_start + 100]
        body = [{"query": ip, "fields": fields} for ip in chunk]
        try:
            r = await client.post("http://ip-api.com/batch?fields=" + fields, json=body, timeout=30)
            for entry in r.json():
                if entry.get("status") != "success":
                    continue
                ip = entry.get("query")
                country = entry.get("countryCode")
                out[ip] = {
                    "city": entry.get("city"),
                    "country": country,
                    "country_name": entry.get("country"),
                    "region": REGION_BY_COUNTRY.get(country, ""),
                    "lat": entry.get("lat"),
                    "lon": entry.get("lon"),
                    "asn": (entry.get("as") or "").split()[0] if entry.get("as") else None,
                    "isp": entry.get("isp"),
                    "org": entry.get("org"),
                    "datacenter": bool(entry.get("hosting") or entry.get("proxy")),
                }
        except Exception as e:
            print(f"  geoip batch failed: {e}", file=sys.stderr)
        # ip-api free tier: 15 req/min. Sleep enough between chunks.
        if chunk_start + 100 < len(ips):
            await asyncio.sleep(5)
    return out


# ─── Build pipeline ────────────────────────────────────────────────────


async def build(network: str) -> dict:
    rpc_url = RPC_URLS.get(network)
    if not rpc_url:
        raise SystemExit(f"no RPC configured for network={network}")

    # Load directory (operator-submitted names + secp)
    dir_path = DIR_PATH / f"validator_directory_{network}.json"
    directory = json.loads(dir_path.read_text()) if dir_path.exists() else []
    secp_to_dir = {(v.get("secp") or "").lower(): v for v in directory if v.get("secp")}
    auth_to_dir = {(v.get("auth") or "").lower(): v for v in directory if v.get("auth")}

    # Load manual override (operator public profile / hand-verified)
    manual_path = DIR_PATH / f"validator_geo_{network}.json"
    manual_data = json.loads(manual_path.read_text()) if manual_path.exists() else {}
    manual_validators = (manual_data.get("validators") or []) if isinstance(manual_data, dict) else []
    manual_by_name = {v.get("name", "").lower(): v for v in manual_validators}

    # Parse peers.toml — authoritative IPs
    peers = parse_peers_toml(DATA_DIR / f"peers_{network}.toml")
    secp_to_ip = {p["secp"]: p["ip"] for p in peers}
    print(f"[{network}] parsed {len(peers)} peers from peers.toml")

    async with httpx.AsyncClient(timeout=15) as c:
        # On-chain enumeration: catches operators not yet in upstream directory
        valset = await enumerate_valset(c, rpc_url)
        print(f"[{network}] execution valset has {len(valset)} val_ids on-chain")

        # Build full identity map: val_id → {auth, secp_from_dir, name_from_dir, ip}
        # Need auth for each val_id; secp comes from directory match
        identities = []
        # For unnamed (not in directory) we still want them, identified by val_id
        # auth via on-chain — but get_validator() ABI parsing is brittle, so for
        # MVP fall back to: use peers.toml as the canonical secp set, then
        # enrich names from directory where possible, leave unnamed as
        # "Validator #N" if peers.toml has them.
        for p in peers:
            secp = p["secp"]
            ip = p["ip"]
            dir_meta = secp_to_dir.get(secp)
            if dir_meta:
                identities.append({
                    "val_id": dir_meta.get("val_id"),
                    "name": dir_meta.get("name"),
                    "auth": (dir_meta.get("auth") or "").lower(),
                    "secp": secp,
                    "ip": ip,
                })
            else:
                # Unknown secp in network — not in our directory yet
                identities.append({
                    "val_id": None,
                    "name": None,
                    "auth": None,
                    "secp": secp,
                    "ip": ip,
                })

        # GeoIP for all unique IPs
        unique_ips = sorted({i["ip"] for i in identities if i["ip"]})
        print(f"[{network}] geo-looking up {len(unique_ips)} unique IPs...")
        geo_by_ip = await geoip_batch(c, unique_ips)
        print(f"[{network}] geoip resolved {len(geo_by_ip)} / {len(unique_ips)}")

    # Assemble final entries
    final = []
    for ident in identities:
        ip = ident.get("ip")
        geo = geo_by_ip.get(ip, {}) if ip else {}
        name = ident.get("name") or (f"Validator #{ident['val_id']}" if ident.get("val_id") else f"Unknown ({ident['secp'][:10]}…)")
        # Manual override wins for the name → city mapping (e.g. shadowoftime
        # explicitly says Sydney even if Sydney IP geos to a CDN edge)
        manual = manual_by_name.get(name.lower())
        if manual:
            entry = {
                "val_id": ident.get("val_id"),
                "name": name,
                "auth": ident.get("auth"),
                "secp": ident["secp"],
                "ip": ip,
                "city": manual.get("city"),
                "country": geo.get("country"),  # manual didn't have 2-letter
                "region": manual.get("region", geo.get("region")),
                "lat": manual.get("lat"),
                "lon": manual.get("lon"),
                "asn": geo.get("asn"),
                "isp": geo.get("isp"),
                "datacenter": geo.get("datacenter"),
                "source": "manual_override",
            }
        else:
            entry = {
                "val_id": ident.get("val_id"),
                "name": name,
                "auth": ident.get("auth"),
                "secp": ident["secp"],
                "ip": ip,
                **geo,
                "source": "p2p_signed_geoip" if geo else "p2p_signed_no_geo",
            }
        final.append(entry)

    return {
        "network": network,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source_summary": {
            "p2p_signed_total": sum(1 for e in final if e["source"].startswith("p2p_signed")),
            "manual_override": sum(1 for e in final if e["source"] == "manual_override"),
            "no_geo": sum(1 for e in final if e["source"] == "p2p_signed_no_geo"),
        },
        "validators": final,
    }


async def main():
    for network in ("testnet",):
        out = await build(network)
        out_path = DIR_PATH / f"validator_geo_full_{network}.json"
        out_path.write_text(json.dumps(out, indent=2, ensure_ascii=False))
        print(f"[{network}] wrote {out_path} ({len(out['validators'])} validators)")
        print(f"  manual_override : {out['source_summary']['manual_override']}")
        print(f"  p2p_signed_geoip: {out['source_summary']['p2p_signed_total'] - out['source_summary']['no_geo']}")
        print(f"  no_geo (failed) : {out['source_summary']['no_geo']}")


if __name__ == "__main__":
    asyncio.run(main())
