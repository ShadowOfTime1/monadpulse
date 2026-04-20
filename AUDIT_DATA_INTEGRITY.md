# Data Integrity Audit — MonadPulse

Date: 2026-04-20
Scope: all data-producing code paths, static files, scheduled jobs, and
user-visible claims. Goal: verify that everything MonadPulse shows
publicly is either (a) sourced live from the chain / our DB, (b) sourced
from a canonical upstream repo, or (c) clearly labelled as a manual
annotation with an explicit refresh plan.

## Findings and fixes

### 🟥 Misleading geographic simulation — FIXED

**Found**: `/var/www/monadpulse/js/map.js` carried a `CITY_POOL` +
`REGION_TARGETS` block that algorithmically distributed unverified
validators across the map. For a network with ~200 validators and ~30
whose location we'd verified, the map was padded with 170 algorithmic
points — `REGION_TARGETS: {NA: 32%, EU: 38%, Asia: 18%, ...}`. Points
were labelled "estimated (unverified location)" in the popup but
**visually indistinguishable** from real ones at a glance.

**Fix**: Removed `CITY_POOL` + `REGION_TARGETS` entirely. Map now plots
only validators whose geography we've verified (the
`KNOWN_VALIDATORS` list). Legend was restructured to show the honest
breakdown: `N with verified location / K without public geo data`.
About page now carries a "Validator location" data-source entry
explicitly stating manual verification is the source of truth.

### 🟥 Stale static file exposed publicly — FIXED

**Found**: `/var/www/monadpulse/data/names.json` — 30 KB file, 474
entries, **last modified 4 days ago**. Missing validators added since
(including `shadowoftime`). No code referenced it, but nginx served
it at `https://monadpulse.xyz/data/names.json` with HTTP 200.
Anybody curl'ing that URL got a stale, incomplete name map.

**Fix**: Deleted. `/api/names/map?network=...` is now the single
source of truth (663 testnet, 672 mainnet, rebuilt weekly + on every
epoch boundary).

### 🟨 About page claimed wrong refresh cadence — FIXED

**Found**: about.html said "Epoch data checked every 5 minutes". Real
interval is **60 seconds**, plus immediate detection on every block
ingested at an epoch boundary.

**Fix**: Corrected to "polled every 60 seconds as fallback; epoch
boundaries also detected immediately on block ingest". Also expanded:
- Block data now distinguishes testnet (local RPC) vs mainnet (public RPC)
- Staking events entry spells out all five event types + 60s interval
- Added "Delegation graph" entry documenting weekly + boundary rebuild
- Added "Validator location" entry making the manual verification
  source of truth explicit

### 🟨 `auto-rewards.service` exit code 1 despite successful compound

**Found**: systemd shows the service as `failed` with status=1. The
logs show the compound transaction succeeded on-chain (`Tx status: 1`,
hash `0xb209...`). The script's exit 1 is triggered by the status
check itself because the staking CLI's multiline output prints the
tx hash across two lines, and the bash regex `0x[0-9a-f]{64}` matches
0 hashes — the script then goes into the else branch even though
everything on-chain was fine.

**Impact**: Cosmetic only. Compound actions work, Telegram alerts
still fire (the script's notify call is before the exit). Just a
false-positive systemd "failed" state that looks scary in
`systemctl --failed`.

**Fix not applied in this audit** — tracked for a later housekeeping
pass on `/opt/monad/scripts/auto-rewards.sh`. Needs the regex to
tolerate whitespace/newlines inside the tx hash match, or switch to
parsing the cli's structured output.

### 🟩 No other hardcoded validator data exposed to users

Scanned `/var/www/monadpulse/` and `/opt/monadpulse/` for hardcoded
Ethereum addresses. All hits are either:
- The null-address sentinel (0x0...0) — intentional
- Site metadata (MonadPulse author footer, Telegram links)
- `KNOWN_VALIDATORS` in map.js — manually maintained geography (now
  explicitly documented on About)

### 🟩 All scheduled jobs are running

`systemctl list-timers`:

| timer | next run | interval |
|---|---|---|
| `validator-watch.timer` | every minute | monitors shadowoftime state |
| `monadpulse-backfill-nulls.timer` | every 5 min | null-miner resolution |
| `monadpulse-backup.timer` | 03:03 UTC daily | pg_dump |
| `monadpulse-vacuum.timer` | 04:17 UTC daily | VACUUM ANALYZE |
| `auto-rewards.timer` | 06:00 UTC daily | compound / claim |
| `monadpulse-digest.timer` | 06:00 UTC daily | Telegram daily digest |
| `validator-names-rebuild.timer` | weekly Monday 00:00 | upstream name sync |
| `monadpulse-graph-rebuild.timer` | weekly Monday 00:00 | graph + clusters |
| `certbot.timer` | ~13h | SSL renewal |
| `signing-uptime-alert.timer` | every 10 min | drop alert |

In-process periodic tasks in the collector main loop (verified by
grep on `last_*_check > N`):
- Block ingestion — continuous (batched every ~1s live)
- Hourly aggregation (`hourly_gas_stats`) — 3600s
- Epoch check — 60s + immediate boundary trigger
- Health-score compute — 3600s
- TPS spike detection — 300s
- Stake snapshot (`validator_stake_history`) — 1800s
- Stake events ingest — 60s
- Offline validator detection — 86400s (testnet only)
- Retention (blocks/health/alerts/stake_history) — 21600s (testnet only)

### 🟩 API endpoint freshness verified

Probed every major endpoint for current-timestamp data (see section D
of the full audit log). All return fresh data with expected lag:
dashboard (seconds), blocks (seconds), health-scores (up to an hour
per compute cycle), stake events (minute), alerts (event-driven),
gas aggregates (hour granularity by design).

### 🟩 On-chain vs UI spot-check

- `shadowoftime` on-chain: commission 20%, execution stake 100k MON,
  VDP delegation 10.9M MON. UI shows correct values on /validator.html.
- Backpack mainnet (val_id=97) on-chain: commission 0%, execution
  stake 1.6B MON. UI shows correct values.
- CMS Holdings (val_id=92): commission 4% — UI displays 4.00% after
  the wei→percent fix.
- Health score stake-weighted baseline: top mainnet validators at
  100% uptime, a single laggard at 30%+ — matches on-chain rate of
  proposer selection given their respective stakes.

## What remains manual

These data points are manually maintained. Each is documented on the
About page:

1. **Validator geography** — 30 testnet / 16 mainnet entries in
   `KNOWN_VALIDATORS` (map.js). Source: each operator's public site
   / social profiles. Needs quarterly review as the valset grows.
2. **Local name overrides** — `validator_names_override_{network}.json`
   for operators not yet in `monad-developers/validator-info`. Drops
   out automatically once upstream PRs merge.

Neither is displayed as "chain data". Both are clearly attributed as
"manually verified" or "local override" on the About page.

## Ready for public promotion

**YES** — after the two 🟥 fixes shipped today (algorithmic map padding
removed, stale static names.json purged) and the 🟨 About corrections,
I don't see any remaining path by which MonadPulse would surface
incorrect or misleading data to a reviewer.

Next audit pass recommended: after the Monad mainnet VDP program goes
live and the validator set expands past ~200, re-check the
`KNOWN_VALIDATORS` geography coverage and the manual override list.
