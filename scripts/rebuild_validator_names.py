#!/usr/bin/env python3
"""Rebuild validator_names_{testnet,mainnet}.json from on-chain data.

Pipeline:
1. Query staking precompile get_execution_valset → all val_ids
2. For each val_id → get_validator → (auth_address, secp_pubkey)
3. testnet: auth_address == block.miner directly
   mainnet: block.miner != auth; resolve via historical get_proposer_val_id
4. Match secp_pubkey against monad-developers/validator-info repo JSONs
5. Write {block.miner_address.lower(): "name"} per network

Meant to be run periodically (systemd timer ~weekly) to pick up new
validators and name updates from the validator-info repo.
"""
from __future__ import annotations

import concurrent.futures
import json
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

# Make staking-sdk importable
sys.path.insert(0, "/home/shadowoftime/staking-sdk-cli/src")
sys.path.insert(0, "/home/shadowoftime/staking-sdk-cli/cli-venv/lib/python3.12/site-packages")

from eth_abi.abi import decode
from web3 import Web3

from staking_sdk_py.callGetters import call_getter
from staking_sdk_py.generateCalldata import get_proposer_val_id

PRE = "0x0000000000000000000000000000000000001000"

NETWORKS = {
    "testnet": "http://localhost:8080",
    "mainnet": "https://rpc.monad.xyz",
}
OUT_DIR = Path("/opt/monadpulse")


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


def fetch_val_to_secp(w3: Web3, ids: list[int]) -> dict[int, tuple[str, str]]:
    """Returns {val_id: (auth_address_lower, secp_hex_lower)}."""
    out: dict[int, tuple[str, str]] = {}
    for vid in ids:
        try:
            v = call_getter(w3, "get_validator", PRE, vid)
            auth_raw = v[0]
            auth = auth_raw.lower() if isinstance(auth_raw, str) else "0x" + auth_raw.hex().lower()
            if auth == "0x" + "00" * 20:
                continue
            secp_raw = v[10]
            secp = (secp_raw.hex() if hasattr(secp_raw, "hex") else bytes(secp_raw).hex()).lower()
            out[vid] = (auth, secp)
        except Exception:
            pass
    return out


def fetch_validator_info(network: str, val_to_secp: dict[int, tuple[str, str]]) -> dict[int, str]:
    """Returns {val_id: display_name}. Matches SECP against validator-info repo."""
    req = urllib.request.Request(
        f"https://api.github.com/repos/monad-developers/validator-info/contents/{network}?per_page=1000",
        headers={"Accept": "application/vnd.github+json"},
    )
    listing = json.loads(urllib.request.urlopen(req, timeout=30).read())
    secp_files: dict[str, str] = {}
    for item in listing:
        fname = item.get("name", "")
        if fname.endswith(".json"):
            secp_files[fname[:-5].lower()] = item["download_url"]

    def fetch(url: str) -> str | None:
        try:
            return json.loads(urllib.request.urlopen(url, timeout=15).read()).get("name")
        except Exception:
            return None

    out: dict[int, str] = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=30) as ex:
        futs = {
            ex.submit(fetch, secp_files[secp]): vid
            for vid, (_auth, secp) in val_to_secp.items()
            if secp in secp_files
        }
        for fut in concurrent.futures.as_completed(futs):
            name = fut.result()
            if name:
                out[futs[fut]] = name
    return out


def resolve_miners_mainnet(w3: Web3) -> dict[str, int]:
    """For each unique mainnet block.miner, resolve its val_id via historical
    get_proposer_val_id."""
    q = (
        "SELECT proposer_address, MAX(block_number) FROM blocks "
        "WHERE network='mainnet' "
        "AND proposer_address != '0x0000000000000000000000000000000000000000' "
        "GROUP BY proposer_address"
    )
    res = subprocess.run(
        ["sudo", "-u", "postgres", "psql", "-d", "monadpulse", "-t", "-A", "-F", "|", "-c", q],
        capture_output=True, text=True, check=True,
    )
    out: dict[str, int] = {}
    data = get_proposer_val_id()
    for line in res.stdout.strip().splitlines():
        if "|" not in line:
            continue
        addr, blk = line.strip().split("|")
        try:
            raw = w3.eth.call(
                {"to": Web3.to_checksum_address(PRE), "data": data},
                block_identifier=int(blk),
            )
            vid = decode(["uint64"], raw)[0]
            out[addr.lower()] = int(vid)
        except Exception:
            pass
    return out


def build(network: str) -> None:
    log(f"=== {network} ===")
    w3 = Web3(Web3.HTTPProvider(NETWORKS[network]))

    ids = collect_val_ids(w3)
    log(f"  valset size: {len(ids)}")

    val_to_secp = fetch_val_to_secp(w3, ids)
    log(f"  val→secp: {len(val_to_secp)}")

    val_to_name = fetch_validator_info(network, val_to_secp)
    log(f"  val→name: {len(val_to_name)}")

    if network == "testnet":
        # auth_address == block.miner on testnet
        addr_to_name = {
            auth: val_to_name[vid]
            for vid, (auth, _secp) in val_to_secp.items()
            if vid in val_to_name
        }
    else:
        # On mainnet, resolve miner→val_id via historical get_proposer_val_id
        miner_to_vid = resolve_miners_mainnet(w3)
        log(f"  miner→val_id: {len(miner_to_vid)}")
        addr_to_name = {
            miner: val_to_name[vid]
            for miner, vid in miner_to_vid.items()
            if vid in val_to_name
        }
    # Merge local overrides. upstream monad-developers/validator-info can't
    # cover every operator (PRs take time to land, privacy preferences, etc).
    # A manually-maintained override file lets us surface names immediately.
    #
    # Override format: { "auth_address_lowercase": "Display Name", ... }
    #
    # We resolve overrides both into the address→name map (what the UI reads)
    # and into the directory's val_to_name map (what search matches).
    override_path = OUT_DIR / f"validator_names_override_{network}.json"
    overrides: dict = {}
    if override_path.exists():
        try:
            overrides = {k.lower(): v for k, v in json.loads(override_path.read_text()).items()}
            log(f"  overrides loaded: {len(overrides)} from {override_path.name}")
        except Exception as e:
            log(f"  override read err: {e}")
    # address→name map: simple dict merge (override wins)
    for addr, name in overrides.items():
        addr_to_name[addr] = name
    # val_to_name: look up by auth — so search-by-name finds the val_id too
    auth_to_vid = {auth.lower(): vid for vid, (auth, _secp) in val_to_secp.items()}
    for addr, name in overrides.items():
        vid = auth_to_vid.get(addr)
        if vid is not None:
            val_to_name[vid] = name

    log(f"  addr→name: {len(addr_to_name)}")

    out_path = OUT_DIR / f"validator_names_{network}.json"
    out_path.write_text(json.dumps(addr_to_name, indent=2, ensure_ascii=False))
    log(f"  → {out_path}")

    # Also emit a directory for search (name / val_id / auth lookup)
    directory = [
        {
            "val_id": vid,
            "name": val_to_name.get(vid),
            "auth": auth,
            "secp": secp,
        }
        for vid, (auth, secp) in val_to_secp.items()
    ]
    directory.sort(key=lambda e: e["val_id"])
    dir_path = OUT_DIR / f"validator_directory_{network}.json"
    dir_path.write_text(json.dumps(directory, indent=2, ensure_ascii=False))
    log(f"  → {dir_path}")


def main() -> int:
    for net in ("testnet", "mainnet"):
        try:
            build(net)
        except Exception as e:
            log(f"ERROR {net}: {e}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
