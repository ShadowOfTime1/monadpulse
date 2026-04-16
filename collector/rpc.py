import asyncio
import httpx
import logging

log = logging.getLogger("monadpulse.rpc")


class MonadRPC:
    def __init__(self, url: str, rate_limit: float = 50):
        self.url = url
        self._semaphore = asyncio.Semaphore(rate_limit)
        self._client = httpx.AsyncClient(timeout=30)
        self._id = 0

    async def _call(self, method: str, params: list = None) -> dict:
        async with self._semaphore:
            self._id += 1
            payload = {
                "jsonrpc": "2.0",
                "method": method,
                "params": params or [],
                "id": self._id,
            }
            for attempt in range(3):
                try:
                    resp = await self._client.post(self.url, json=payload)
                    data = resp.json()
                    if "error" in data:
                        raise Exception(f"RPC error: {data['error']}")
                    return data.get("result")
                except (httpx.ReadTimeout, httpx.ConnectTimeout, httpx.ConnectError) as e:
                    if attempt < 2:
                        await asyncio.sleep(1)
                        continue
                    raise

    async def get_block_number(self) -> int:
        result = await self._call("eth_blockNumber")
        return int(result, 16)

    async def get_block(self, number: int) -> dict | None:
        hex_num = hex(number)
        result = await self._call("eth_getBlockByNumber", [hex_num, False])
        if result is None:
            return None
        return {
            "block_number": int(result["number"], 16),
            "timestamp": int(result["timestamp"], 16),
            "proposer_address": result["miner"].lower(),
            "tx_count": len(result.get("transactions", [])),
            "gas_used": int(result["gasUsed"], 16),
            "base_fee": int(result.get("baseFeePerGas", "0x0"), 16),
            "hash": result["hash"],
        }

    async def get_client_version(self) -> str:
        return await self._call("web3_clientVersion")

    async def get_epoch(self) -> int | None:
        """Call staking precompile getEpoch() — 0x757991a8 is the selector."""
        try:
            result = await self._call("eth_call", [{
                "to": "0x0000000000000000000000000000000000001000",
                "data": "0x757991a8",
            }, "latest"])
            # ABI-encoded: first 32 bytes (64 hex chars after 0x) = epoch number
            return int(result[2:66], 16)
        except Exception as e:
            log.warning(f"getEpoch failed: {e}")
            return None

    async def close(self):
        await self._client.aclose()
