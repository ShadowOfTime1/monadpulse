#!/usr/bin/env python3
"""Resolve null-miner blocks in the blocks table via staking precompile
get_proposer_val_id() and update proposer_address to the validator's auth.

Why: on Monad testnet block.miner is sometimes 0x0…0 (proposer-recovery
fail at the execution layer). The collector writes those straight through,
so aggregate queries keyed on proposer_address silently lose the owning
validator — Sybil / signing-uptime calculations end up with share_24h=0 for
real validators (e.g. shadowoftime val_id=267).

This job pulls null-proposer rows from the last 24h in descending order,
asks get_proposer_val_id(block_number) for each, looks the val_id up in
validator_directory_{network}.json to get the auth address, and UPDATEs
the row.

Safe to run every few minutes as a systemd timer."""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

import asyncpg
import httpx

RPC_URLS = {
    "testnet": os.environ.get("TESTNET_RPC", "http://localhost:8080"),
    "mainnet": os.environ.get("MAINNET_RPC", "https://rpc.monad.xyz"),
}
PRECOMPILE = "0x0000000000000000000000000000000000001000"
GET_PROPOSER_VAL_ID = "0xfbacb0be"  # selector
NULL_ADDR = "0x" + "0" * 40
DIR_FILE = "/opt/monadpulse/validator_directory_{network}.json"
DB_URL = os.environ.get("MONADPULSE_DB", "postgresql:///monadpulse")


def load_val_to_auth(network: str) -> dict[int, str]:
    path = Path(DIR_FILE.format(network=network))
    if not path.exists():
        print(f"no directory file: {path}", file=sys.stderr)
        return {}
    rows = json.loads(path.read_text())
    out: dict[int, str] = {}
    for r in rows:
        vid = r.get("val_id")
        auth = (r.get("auth") or "").lower()
        if vid is not None and auth:
            out[int(vid)] = auth
    return out


async def resolve_blocks(rpc_url: str, blocks: list[int]) -> dict[int, int]:
    """Return {block_number: val_id} for each block the RPC resolved."""
    out: dict[int, int] = {}
    sem = asyncio.Semaphore(20)

    async def one(client: httpx.AsyncClient, bn: int) -> None:
        async with sem:
            try:
                r = await client.post(rpc_url, json={
                    "jsonrpc": "2.0", "method": "eth_call",
                    "params": [
                        {"to": PRECOMPILE, "data": GET_PROPOSER_VAL_ID},
                        hex(bn),
                    ],
                    "id": 1,
                })
                data = r.json()
                raw = data.get("result", "0x0")
                if raw and raw != "0x":
                    vid = int(raw, 16)
                    if vid > 0:
                        out[bn] = vid
            except Exception as e:
                print(f"rpc err block {bn}: {e}", file=sys.stderr)

    async with httpx.AsyncClient(timeout=15) as client:
        await asyncio.gather(*(one(client, bn) for bn in blocks))
    return out


async def main_async(args) -> int:
    val_to_auth = load_val_to_auth(args.network)
    if not val_to_auth:
        print("empty val→auth map; cannot resolve", file=sys.stderr)
        return 1

    conn = await asyncpg.connect(DB_URL)
    try:
        rows = await conn.fetch(
            """
            SELECT block_number FROM blocks
            WHERE network = $1
              AND proposer_address = $2
              AND timestamp > NOW() - INTERVAL '24 hours'
            ORDER BY block_number DESC
            LIMIT $3
            """,
            args.network, NULL_ADDR, args.limit,
        )
        blocks = [int(r["block_number"]) for r in rows]
        print(f"[{args.network}] {len(blocks)} null-proposer blocks to check")
        if not blocks:
            return 0

        resolved = await resolve_blocks(RPC_URLS[args.network], blocks)
        print(f"[{args.network}] RPC resolved {len(resolved)} val_ids")

        updates: list[tuple[str, int, str]] = []
        for bn, vid in resolved.items():
            auth = val_to_auth.get(vid)
            if auth:
                updates.append((auth, bn, args.network))

        print(f"[{args.network}] {len(updates)} will be updated")
        if args.dry_run or not updates:
            return 0

        await conn.executemany(
            "UPDATE blocks SET proposer_address = $1 "
            "WHERE block_number = $2 AND network = $3",
            updates,
        )
        print(f"[{args.network}] committed {len(updates)} updates")
    finally:
        await conn.close()
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--network", choices=["testnet", "mainnet"], required=True)
    ap.add_argument("--limit", type=int, default=500)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    return asyncio.run(main_async(args))


if __name__ == "__main__":
    sys.exit(main())
