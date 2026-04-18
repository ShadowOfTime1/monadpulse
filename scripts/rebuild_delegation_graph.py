#!/usr/bin/env python3
"""Rebuild delegation graph (delegator → validator → active_stake) per network.

Walks `get_delegators(val_id, cursor)` on the staking precompile for every
registered validator, then fetches each (val_id, delegator) slot's
active_stake via `get_delegator`. Emits:
  • /opt/monadpulse/delegation_graph_{testnet,mainnet}.json
      [{val_id, delegator, active_stake_wei}, …]

Intended cadence: per epoch boundary (triggered by collector) + weekly
systemd timer as backup. Each run takes ~1–2 min on testnet (local RPC),
~5–10 min on mainnet (public RPC).
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

sys.path.insert(0, "/home/shadowoftime/staking-sdk-cli/src")
sys.path.insert(0, "/home/shadowoftime/staking-sdk-cli/cli-venv/lib/python3.12/site-packages")

from eth_abi.abi import decode
from web3 import Web3

from staking_sdk_py.callGetters import call_getter

PRE = "0x0000000000000000000000000000000000001000"
NETWORKS = {
    "testnet": "http://localhost:8080",
    "mainnet": "https://rpc.monad.xyz",
}
OUT_DIR = Path("/opt/monadpulse")
ZERO = "0x0000000000000000000000000000000000000000"


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def collect_val_ids(w3: Web3) -> list[int]:
    ids: set[int] = set()
    start = 0
    while True:
        done, next_idx, vals = call_getter(w3, "get_execution_valset", PRE, start)
        ids.update(int(v) for v in vals)
        if done:
            break
        start = next_idx
    return sorted(ids)


def get_delegators_for_val(w3: Web3, val_id: int) -> list[str]:
    """Return list of delegator addresses for a validator (lower-cased)."""
    out: list[str] = []
    cursor = ZERO
    tries = 0
    while True:
        try:
            done, next_addr, addrs = call_getter(w3, "get_delegators", PRE, val_id, cursor)
        except Exception as e:
            log(f"  val {val_id} get_delegators err: {e}")
            break
        for a in addrs:
            addr_lower = a.lower() if isinstance(a, str) else ("0x" + a.hex().lower())
            if addr_lower != ZERO and addr_lower not in out:
                out.append(addr_lower)
        if done:
            break
        cursor = next_addr if isinstance(next_addr, str) else ("0x" + next_addr.hex())
        tries += 1
        if tries > 500:
            break
    return out


def get_active_stake(w3: Web3, val_id: int, delegator: str) -> int:
    """Return active_stake wei for (val_id, delegator)."""
    try:
        d = call_getter(w3, "get_delegator", PRE, val_id, delegator)
        return int(d[0])
    except Exception:
        return 0


def build(network: str) -> None:
    log(f"=== {network} ===")
    w3 = Web3(Web3.HTTPProvider(NETWORKS[network]))
    ids = collect_val_ids(w3)
    log(f"  valset size: {len(ids)}")

    edges: list[dict] = []
    processed = 0
    for vid in ids:
        delegators = get_delegators_for_val(w3, vid)
        for deleg in delegators:
            stake = get_active_stake(w3, vid, deleg)
            if stake > 0:
                edges.append({
                    "val_id": vid,
                    "delegator": deleg,
                    "active_stake_wei": stake,
                })
        processed += 1
        if processed % 50 == 0:
            log(f"  {processed}/{len(ids)} validators, {len(edges)} edges so far")

    out = OUT_DIR / f"delegation_graph_{network}.json"
    out.write_text(json.dumps(edges, indent=2))
    log(f"  {len(edges)} edges → {out}")


def main() -> int:
    for net in ("testnet", "mainnet"):
        try:
            build(net)
        except Exception as e:
            log(f"ERROR {net}: {e}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
