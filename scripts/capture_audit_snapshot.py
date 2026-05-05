#!/usr/bin/env python3
"""
capture_audit_snapshot.py — pull current concentration metrics from
/audit/network and write one row per network into audit_snapshots.

Run daily via systemd timer. Idempotent for the same captured_at second
(primary key conflict → skip). The endpoint already caches 5 min, so we
just hit it for both networks back-to-back.
"""
from __future__ import annotations

import asyncio
import os
import sys
from datetime import datetime, timezone

import asyncpg
import requests

API_BASE = os.environ.get("MONADPULSE_API", "http://127.0.0.1:8890")
DB_URL = os.environ.get("DATABASE_URL") or open("/opt/monadpulse/.env").read().split("DATABASE_URL=")[1].split()[0]


def fetch(network: str) -> dict:
    r = requests.get(f"{API_BASE}/audit/network", params={"network": network}, timeout=60)
    r.raise_for_status()
    return r.json()


async def insert(conn, network: str, payload: dict) -> bool:
    g = payload.get("geo") or {}
    perf = payload.get("performance") or {}
    rot = payload.get("rotation") or {}
    active = rot.get("rotation_pool_size") or perf.get("pool_size") or g.get("registered_count")

    result = await conn.fetchval(
        """
        INSERT INTO audit_snapshots (
            network, captured_at,
            total_validators, active_validators,
            country_count, isp_count, asn_count,
            country_hhi, asn_hhi, stake_country_hhi, stake_asn_hhi,
            datacenter_pct, datacenter_count, distinct_subnets_total
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
        ON CONFLICT (network, captured_at) DO NOTHING
        RETURNING captured_at
        """,
        network,
        datetime.now(timezone.utc),
        g.get("total_with_geo"),
        active,
        g.get("country_total"),
        g.get("isp_total"),
        g.get("asn_total"),
        g.get("country_hhi"),
        g.get("asn_hhi"),
        g.get("stake_country_hhi"),
        g.get("stake_asn_hhi"),
        g.get("datacenter_pct"),
        g.get("datacenter_count"),
        g.get("distinct_subnets_total"),
    )
    return result is not None


async def main() -> int:
    pool = await asyncpg.create_pool(DB_URL, min_size=1, max_size=2)
    try:
        async with pool.acquire() as conn:
            for net in ("testnet", "mainnet"):
                try:
                    data = fetch(net)
                    ok = await insert(conn, net, data)
                    g = data.get("geo") or {}
                    print(f"{net}: {'inserted' if ok else 'skipped (dup ts)'} country_hhi={g.get('country_hhi')} stake_country_hhi={g.get('stake_country_hhi')}")
                except Exception as e:
                    print(f"{net}: FAILED — {e}", file=sys.stderr)
                    return 1
    finally:
        await pool.close()
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
