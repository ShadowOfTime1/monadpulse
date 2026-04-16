# MonadPulse

**Deep network intelligence for the Monad ecosystem.**

Live at [monadpulse.xyz](https://monadpulse.xyz) | Alerts on [Telegram](https://t.me/monadpulse_alerts)

---

MonadPulse is an independent analytics platform for the Monad network. It shows what no standard block explorer does: validator performance patterns, health scores, block production anomalies, gas trends, and real-time alerts.

Built by [shadowoftime](https://shadowoftime1.github.io) — independent Monad validator, Sydney, Australia.

## Features

**Dashboard** — real-time network overview with live block ticker, epoch progress, upgrade tracker, and key metrics (TPS, block time, validator count).

**Block Production** — 48h charts (blocks/hour, block time, TPS, gas usage vs limit), searchable block table with pagination, proposer filter dropdown, click-to-detail panel.

**Validator Health Score** — composite 0-100 rating for every validator, recalculated hourly:
| Weight | Factor | Description |
|--------|--------|-------------|
| 40% | Uptime | Block production rate relative to top producer |
| 20% | Block Quality | Consistency of block timing (~400ms target) |
| 15% | Upgrade Speed | How quickly validator updates to new releases |
| 15% | Stake Stability | Delegation retention (when data available) |
| 10% | Age | Time active on network (max 30 days) |

**Network Map** — geographic distribution of validators on a dark Leaflet.js map with city clusters, click-for-details, and block time histogram.

**Stake Flow** — top earners by estimated rewards, delegation event monitoring via staking precompile logs.

**Gas & Fee Analytics** — hourly gas trends, base fee tracking, transaction heatmap by day/hour, top contracts.

**Alerts** — real-time event detection with Telegram notifications:
- Slow blocks (>5s, Telegram for >10s)
- New epochs
- TPS spikes (>3x average, 6h cooldown)
- New client releases (critical for VDP — 48h update window)

**Dual Network** — testnet (local node RPC) and mainnet (public RPC) with one-click switch.

## Architecture

```
Monad Node RPC (:8080)
        │
        ▼
 Data Collector (Python asyncio, systemd)
   │  live mode: real-time block ingestion
   │  backfill: catch up on missed blocks
   │  epoch tracking, health scores, alerts
        │
        ▼
 PostgreSQL 16 (all tables have network column)
        │
        ▼
 FastAPI API (:8890)
   │  /dashboard, /blocks, /validators, /health
   │  /epochs, /gas, /alerts, /stake, /upgrades, /names
        │
        ▼
 Nginx (:443, HTTPS, rate-limited)
        │
        ▼
 Frontend (vanilla HTML/JS, Chart.js)
   │  8 pages + About
   │  Monad brand colors, dark theme
   │  mobile responsive
        │
        ▼
 monadpulse.xyz
```

## Tech Stack

- **Collector:** Python 3.12, asyncio, httpx, asyncpg
- **API:** FastAPI, uvicorn
- **Database:** PostgreSQL 16
- **Frontend:** HTML, CSS, vanilla JS, Chart.js, Leaflet.js
- **Infra:** Nginx, Let's Encrypt, systemd, iptables
- **Monitoring:** Custom healthcheck (8 checks, 5min cron), Telegram alerts
- **Server:** OVH Advance-4, AMD EPYC 4585PX, 64GB RAM, 4x NVMe, Sydney AU

## Security

- CSP, HSTS, X-Frame-Options, X-Content-Type-Options headers
- Rate limiting (30 req/s at nginx)
- Parameterized SQL queries (no f-string injection)
- XSS escaping on all dynamic content
- CORS restricted to monadpulse.xyz
- .env with 600 permissions, not in git
- API docs disabled in production
- IPv4 + IPv6 firewall hardened
- Services run as unprivileged user with memory/CPU limits

## Data Sources

| Source | Method | Frequency |
|--------|--------|-----------|
| Block data | Local Monad RPC `eth_getBlockByNumber` | Every block |
| Epoch data | Staking precompile `getEpoch()` | Every 5 min |
| Validator names | github.com/monad-developers/validator-info | On startup |
| Client versions | github.com/category-labs/monad releases | Every hour |
| Mainnet data | Public RPC (rpc.monad.xyz) | Rate-limited |
| Staking events | Precompile logs via `eth_getLogs` | Per block |

## Setup

```bash
# Clone
git clone https://github.com/ShadowOfTime1/monadpulse.git
cd monadpulse

# Python environment
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Configure
cp .env.example .env
# Edit .env: DATABASE_URL, TESTNET_RPC, MAINNET_RPC, TELEGRAM_BOT_TOKEN, TELEGRAM_CHANNEL_ID

# Database
sudo -u postgres createuser monadpulse
sudo -u postgres createdb monadpulse -O monadpulse
# Run schema from docs/schema.sql

# Start
python -m collector.main  # data collector
uvicorn api.main:app --host 127.0.0.1 --port 8890  # API
```

## License

MIT

## Links

- Live: [monadpulse.xyz](https://monadpulse.xyz)
- Alerts: [t.me/monadpulse_alerts](https://t.me/monadpulse_alerts)
- Validator: [shadowoftime1.github.io](https://shadowoftime1.github.io)
- Monad: [monad.xyz](https://monad.xyz)
