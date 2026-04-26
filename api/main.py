#!/usr/bin/env python3
"""MonadPulse API — FastAPI backend."""

import os
from contextlib import asynccontextmanager
from pathlib import Path

import asyncpg
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

from api.routes import dashboard, blocks, epochs, gas, alerts, validators, health, names, upgrades, stakeflow, analytics, governance

pool: asyncpg.Pool | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global pool
    pool = await asyncpg.create_pool(os.environ["DATABASE_URL"], min_size=2, max_size=10)
    app.state.pool = pool
    yield
    await pool.close()


app = FastAPI(title="MonadPulse API", version="0.1.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://monadpulse.xyz", "https://www.monadpulse.xyz"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

app.include_router(dashboard.router, prefix="/dashboard", tags=["dashboard"])
app.include_router(blocks.router, prefix="/blocks", tags=["blocks"])
app.include_router(epochs.router, prefix="/epochs", tags=["epochs"])
app.include_router(gas.router, prefix="/gas", tags=["gas"])
app.include_router(alerts.router, prefix="/alerts", tags=["alerts"])
app.include_router(validators.router, prefix="/validators", tags=["validators"])
app.include_router(health.router, prefix="/health", tags=["health"])
app.include_router(names.router, prefix="/names", tags=["names"])
app.include_router(upgrades.router, prefix="/upgrades", tags=["upgrades"])
app.include_router(stakeflow.router, prefix="/stake", tags=["stake"])


@app.get("/ping")
async def ping():
    return {"status": "ok"}

app.include_router(analytics.router, prefix="/analytics", tags=["analytics"])
app.include_router(governance.router, prefix="/governance", tags=["governance"])
