# Changelog

All notable changes to MonadPulse are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to semantic-ish versioning.

## [Unreleased]

### Added
- `CHANGELOG.md` tracking project history
- README badges (live, license, Telegram, last commit)
- PNG version of `og:image` (1200×630) for Facebook / LinkedIn / WhatsApp
  social preview compatibility
- `og:image:width`, `og:image:height`, `og:image:type` meta tags on all
  pages

### Changed
- `og:image` / `twitter:image` now point to `/og-image.png` — universal
  social preview. SVG source retained as `og-image.svg` for future edits.

## [0.4.0] — 2026-04-18

### Added
- **On-chain validator name pipeline** — `scripts/rebuild_validator_names.py`
  queries the staking precompile for every registered validator, joins
  with the `monad-developers/validator-info` repo by SECP pubkey, and
  generates per-network `validator_names_{testnet,mainnet}.json` maps
  (testnet auth→name direct, mainnet via historical `get_proposer_val_id`).
- Weekly `validator-names-rebuild.timer` **plus** in-collector trigger on
  every epoch boundary crossing — names stay fresh with ≤5.5h latency
  instead of 7 days.
- `/api/validators/by-id/{N}` — queries staking precompile directly
  (handles validators whose `block.miner` is ephemeral on testnet).
- `/api/validators/by-id/{N}/first-active` — backward scan for the
  earliest Reward event emitted for a given validator; cached after
  first lookup.
- `/api/validators/search?q=` — directory-backed search by name
  substring / val_id / auth or SECP hex. Results surface as
  "On-chain directory matches" on `/validators.html` above the table
  for validators that aren't in block data.
- `/validator.html?id=N` — hero card rendered from staking precompile
  state: Execution / Consensus / Snapshot stake, commission,
  unclaimed pool, SECP + BLS pubkeys.
- **VDP Uptime** metric (% of 4-week target) on `/validator.html`
  and in hourly Telegram summary. Color-coded: pink <50%, amber
  50–99%, green ≥100%.

### Changed
- Dashboard `Validators` metric now reflects **current-epoch valset**
  (from staking precompile snapshot) instead of 24h rolling distinct
  proposers. Updates at each epoch boundary.
- Collector detects `block_number % 50_000 == 0` crossing → immediate
  `track_epoch` call (latency ~400ms). Fallback poll lowered 300s → 60s.
- `Validator count` label: "Validators (this epoch)" with tooltip.

### Fixed
- Null proposer (`0x0000…0000`) no longer appears in Dashboard
  "Top Validators — Health" widget (regression from 0.3.0 — stale
  `health_scores` rows deleted + collector filter reloaded after
  systemd restart).

## [0.3.0] — 2026-04-17

### Added
- SEO meta tags on all 9 pages: `description`, `og:*`, `twitter:card`,
  with `og-image.svg` 1200×630.
- `og:image` → `/og-image.svg`, `twitter:card` → `summary_large_image`.
- Warning banner on `/validator.html?addr=0x0000…0000` explaining
  proposer-recovery-fail semantics.
- Human-readable "null proposer (recovery fail)" label in Blocks
  proposer filter dropdown.
- Block detail panel on `/blocks.html` — click on block number
  reveals inline panel with timestamp, gas used, base fee, proposer,
  and an "open on SocialScan" link.
- Activity Heatmap: diagonal-stripe pattern + "Data populating"
  notice for incomplete weeks (< 7 days of collected data).
- `[testnet]` / `[mainnet]` prefix backfilled on **188** historical
  alert titles.
- GitHub repo settings populated: description, 8 topics, homepage URL,
  MIT LICENSE recognised.
- `web/` directory with full frontend added to the repository
  (previously only backend was versioned).
- **Staking events Whale Watch** — collector decodes 5 staking
  precompile events (Delegate, Undelegate, ClaimRewards, Withdraw,
  CommissionChanged), API returns them on `/api/stake/events`, UI
  groups adjacent identical events on `/stake.html`.
- `validator.html?addr=…` detail page: Health breakdown (5 weighted
  components), Recent Blocks, geo line.

### Changed
- `avg_block_time_ms` on dashboard derived from throughput
  (`86_400_000 / block_count_24h`) instead of raw
  `AVG(block_time_ms)` to escape the RPC 1-second timestamp
  granularity artifact (raw values are 0 or 1000ms only).
- Gas Base Fee chart values divided by `1e9` → displayed in gwei
  with a corresponding chart title.
- Map region-balance coloring rebuilt: underrepresented (green
  `#10b981`), balanced (purple), oversaturated (coral). Category
  indicator in each marker popup and in the legend.
- 13 validator locations corrected from public-source research
  (Chorus One → Zug, Everstake → Kyiv, Luganodes → Lugano,
  Blockdaemon/Allnodes → LA, StakingCabin → Dubai, Validation Cloud
  → Zug, CertHum → NY, Nodes.Guru → Buenos Aires, Needlecast →
  London, InfStones → Palo Alto, OshVanK → Istanbul, Stakecraft →
  Chișinău, snoopfear → Warsaw).
- `CITY_POOL` for anonymous distribution expanded to 14 verified
  hub cities (Zug, Lugano, Kyiv, Dubai, Chișinău, Buenos Aires,
  Munich, Zurich, Helsinki, Madrid, Lisbon, Taipei, Bangalore,
  Palo Alto).
- `validator_geo` table schema: added `network` column +
  compound primary key `(validator_id, network)` to prevent
  cross-network geo leak.
- Map `Show all N validators` label → `Show all N known identities`
  — clarifies the N vs the smaller on-map active set.
- Nginx: `Cache-Control: no-cache, must-revalidate` for HTML/JS/CSS,
  `max-age=30` for `/api/` JSON.

### Fixed
- Null proposer address filtered from all aggregate endpoints
  (`/validators/list`, `/stake/top-earners`, `/blocks/proposer-stats`,
  `dashboard.active_validators`, collector `compute_health_scores`).
- Live Blocks ticker cleared on network switch (DOM + queue reset).
- `Block Time` column on `/validator.html` recent blocks replaced
  with `Gas %` (raw `block_time_ms` is 0 or 1000 only — useless per-row).
- `chart-wrap` `min-height 220px` (260px mobile) — Gas Usage chart
  no longer squashed on narrow viewports.

### Security
- All standard security headers verified: HSTS, CSP, X-Frame-Options,
  X-Content-Type-Options, Referrer-Policy, Permissions-Policy.

## [0.2.0] — 2026-04-17

### Added
- **Full frontend** committed to the repo under `web/` — 9 pages
  (Dashboard, Blocks, Validators, Map, Stake, Gas, Alerts, About,
  Validator detail).
- Dual-network support (Testnet + Mainnet) with `sessionStorage`
  persistence.
- Validator Health Score — composite 0-100 with 5 weighted components
  (Uptime, Block Time, Upgrade, Stake, Age).
- Real-time block streaming with requestAnimationFrame-driven ticker.
- Telegram alerts channel ([@monadpulse_alerts](https://t.me/monadpulse_alerts))
  with per-type coloring.
- Leaflet.js geographic map with regional clustering.
- Validators search, sorting, period filter (24h / 7d / 30d),
  pagination 20 per page.
- Stake Flow page with top earners chart and recent staking events
  feed.
- Gas analytics (hourly trends, base fee, activity heatmap).
- Alerts feed with type filter + Load-more pagination.
- MIT LICENSE file added to the repo.

## [0.1.0] — 2026-04-16

### Added
- Initial commit: Python collector service (live block ingestion, epoch
  tracking, health score computation, TPS spike detection, release
  monitoring) + FastAPI backend with 12 route modules + PostgreSQL
  schema.
- `healthcheck.sh` with cron integration.
- README describing the platform, modules, and stack.

[Unreleased]: https://github.com/ShadowOfTime1/monadpulse/compare/v0.4.0...HEAD
[0.4.0]: https://github.com/ShadowOfTime1/monadpulse/releases/tag/v0.4.0
[0.3.0]: https://github.com/ShadowOfTime1/monadpulse/releases/tag/v0.3.0
[0.2.0]: https://github.com/ShadowOfTime1/monadpulse/releases/tag/v0.2.0
[0.1.0]: https://github.com/ShadowOfTime1/monadpulse/releases/tag/v0.1.0
