import asyncpg
import os

_pool: asyncpg.Pool | None = None


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(
            os.environ["DATABASE_URL"],
            min_size=2,
            max_size=10,
        )
    return _pool


async def close_pool():
    global _pool
    if _pool:
        await _pool.close()
        _pool = None


async def insert_block(conn, block: dict, network: str = "testnet"):
    await conn.execute(
        """
        INSERT INTO blocks (block_number, timestamp, proposer_address, tx_count, gas_used, base_fee, block_time_ms, network)
        VALUES ($1, to_timestamp($2), $3, $4, $5, $6, $7, $8)
        ON CONFLICT (block_number, network) DO NOTHING
        """,
        block["block_number"],
        block["timestamp"],
        block["proposer_address"],
        block["tx_count"],
        block["gas_used"],
        block["base_fee"],
        block["block_time_ms"],
        network,
    )


async def get_last_block_number(conn, network: str = "testnet") -> int | None:
    row = await conn.fetchrow("SELECT MAX(block_number) AS num FROM blocks WHERE network = $1", network)
    return row["num"] if row and row["num"] is not None else None


async def insert_alert(conn, alert_type: str, severity: str, title: str, description: str = None, data_json: dict = None, network: str = "testnet"):
    import json
    await conn.execute(
        """
        INSERT INTO alerts (timestamp, alert_type, severity, title, description, data_json, network)
        VALUES (NOW(), $1, $2, $3, $4, $5, $6)
        """,
        alert_type,
        severity,
        title,
        description,
        json.dumps(data_json) if data_json else None,
        network,
    )


async def upsert_collector_state(conn, key: str, value: str, network: str = "testnet"):
    await conn.execute(
        """
        INSERT INTO collector_state (key, value, updated_at, network)
        VALUES ($1, $2, NOW(), $3)
        ON CONFLICT (key, network) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
        """,
        key, value, network,
    )


async def get_collector_state(conn, key: str, network: str = "testnet") -> str | None:
    row = await conn.fetchrow("SELECT value FROM collector_state WHERE key = $1 AND network = $2", key, network)
    return row["value"] if row else None
