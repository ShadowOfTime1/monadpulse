"""Decode staking precompile events (0x...1000) on Monad.

Event signatures verified by reverse keccak match against live testnet logs.
"""

STAKE_PRECOMPILE = "0x0000000000000000000000000000000000001000"

STAKE_EVENTS = {
    # Delegate(uint64 valId, address delegator, uint256 amount, uint64 epoch)
    "0xe4d4df1e1827dd28252fd5c3cd7ebccd3da6e0aa31f74c828f3c8542af49d840": "delegate",
    # Undelegate(uint64 valId, address delegator, uint8 withdrawalId, uint256 amount, uint64 epoch)
    "0x3e53c8b91747e1b72a44894db10f2a45fa632b161fdcdd3a17bd6be5482bac62": "undelegate",
    # ClaimRewards(uint64 valId, address delegator, uint256 amount, uint64 epoch)
    "0xcb607e6b63c89c95f6ae24ece9fe0e38a7971aa5ed956254f1df47490921727b": "claim_rewards",
    # Withdraw(uint64 valId, address delegator, uint8 withdrawalId, uint256 amount, uint64 epoch)
    "0x63030e4238e1146c63f38f4ac81b2b23c8be28882e68b03f0887e50d0e9bb18f": "withdraw",
    # CommissionChanged(uint64 valId, uint256 oldRate, uint256 newRate)
    "0xd1698d3454c5b5384b70aaae33f1704af7c7e055f0c75503ba3146dc28995920": "commission_changed",
}


def _topic_to_int(h: str) -> int:
    return int(h, 16)


def _topic_to_address(h: str) -> str:
    return "0x" + h[-40:].lower()


def decode_log(log: dict) -> dict | None:
    """Decode a precompile log. Returns normalized dict or None if unknown."""
    topics = log.get("topics", [])
    if not topics:
        return None
    event_type = STAKE_EVENTS.get(topics[0].lower())
    if not event_type:
        return None

    data = log.get("data", "0x")[2:]

    try:
        if event_type in ("delegate", "claim_rewards"):
            validator_id = _topic_to_int(topics[1])
            delegator = _topic_to_address(topics[2])
            amount = int(data[0:64], 16)
        elif event_type in ("undelegate", "withdraw"):
            validator_id = _topic_to_int(topics[1])
            delegator = _topic_to_address(topics[2])
            amount = int(data[64:128], 16)
        elif event_type == "commission_changed":
            validator_id = _topic_to_int(topics[1])
            delegator = ""
            amount = int(data[64:128], 16)
        else:
            return None
    except Exception:
        return None

    return {
        "event_type": event_type,
        "validator_id": str(validator_id),
        "delegator": delegator,
        "amount": amount,
        "tx_hash": log.get("transactionHash"),
        "log_index": int(log.get("logIndex", "0x0"), 16),
        "block_number": int(log["blockNumber"], 16),
        "block_timestamp": int(log["blockTimestamp"], 16) if log.get("blockTimestamp") else None,
    }
