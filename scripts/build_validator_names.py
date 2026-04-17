#!/usr/bin/env python3
"""Build {eoa_address: name} mapping per network from monad-developers/validator-info.

The repo stores one JSON per validator keyed by SECP compressed pubkey.
We derive the EOA (proposer_address in blocks) from each SECP pubkey.
"""
import json
import sys
from pathlib import Path

import httpx
from eth_keys import keys
from eth_utils import keccak, to_checksum_address


REPO_API = "https://api.github.com/repos/monad-developers/validator-info/contents"
REPO_RAW = "https://raw.githubusercontent.com/monad-developers/validator-info/main"
OUT_DIR = Path("/opt/monadpulse")


def secp_compressed_to_eoa(secp_hex: str) -> str:
    """33-byte compressed secp256k1 pubkey (hex) → 0x EOA."""
    raw = bytes.fromhex(secp_hex)
    if len(raw) != 33:
        raise ValueError(f"Expected 33 bytes, got {len(raw)}")
    # Decompress using eth_keys (works for secp256k1)
    pub = keys.PublicKey.from_compressed_bytes(raw)
    return pub.to_checksum_address().lower()


def fetch_network(network: str, client: httpx.Client) -> dict[str, str]:
    """Return {eoa_lower: name} for all validators in network."""
    listing = client.get(f"{REPO_API}/{network}", timeout=30).json()
    if not isinstance(listing, list):
        print(f"  ERROR fetching listing: {listing}", file=sys.stderr)
        return {}
    result = {}
    for item in listing:
        name = item["name"]
        if not name.endswith(".json"):
            continue
        # Fetch raw JSON
        try:
            raw = client.get(f"{REPO_RAW}/{network}/{name}", timeout=30).json()
        except Exception as e:
            print(f"  skip {name}: {e}", file=sys.stderr)
            continue
        secp = raw.get("secp")
        display_name = raw.get("name")
        if not secp or not display_name:
            continue
        try:
            eoa = secp_compressed_to_eoa(secp)
        except Exception as e:
            print(f"  skip {name} (bad secp): {e}", file=sys.stderr)
            continue
        result[eoa] = display_name
    return result


def main():
    with httpx.Client() as client:
        for net in ("testnet", "mainnet"):
            print(f"Fetching {net}...")
            mapping = fetch_network(net, client)
            out = OUT_DIR / f"validator_names_{net}.json"
            out.write_text(json.dumps(mapping, indent=2, ensure_ascii=False))
            print(f"  {net}: {len(mapping)} names → {out}")


if __name__ == "__main__":
    main()
