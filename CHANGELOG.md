# Changelog

All notable changes to MonadPulse are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to semantic-ish versioning.

## [Unreleased]

### Changed (CRITICAL — data integrity)
- **Health Score** now shows the raw API `total_score` everywhere:
  Dashboard, Validators page, Validator detail, About page. Previously
  Dashboard and Validators each ran their own min/max normalization over
  the current population, so the same validator got three different
  numbers (e.g. Backpack mainnet: API 78.8 vs Dashboard 100 vs Validators
  100). About page now documents that 100 is the theoretical maximum —
  real validators top out in the upper-70s.
- **Stake "Share %"** now computed against the true network block count
  for the selected period (via `/blocks/proposer-stats`), not against the
  sum of the top-30 earners shown in the table. Previously misleading:
  Backpack mainnet appeared as 28.88% of the network, actually 11.2%.
  Column renamed `Share` → `Share of network`.

### Added
- `CHANGELOG.md` tracking project history
- README badges (live, license, Telegram, last commit)
- PNG version of `og:image` (1200×630) for Facebook / LinkedIn / WhatsApp
  social preview compatibility
- `og:image:width`, `og:image:height`, `og:image:type` meta tags on all
  pages
- Clusters page: "Show inactive (0 MON)" toggle — inactive delegators
  hidden by default for a meaningful default view
- Validator page: `?addr=` URLs now auto-resolve to `?id=N` via the
  on-chain directory search and `history.replaceState`, so a single
  unified view is always served regardless of entry-point.

### Changed
- `og:image` / `twitter:image` now point to `/og-image.png` — universal
  social preview. SVG source retained as `og-image.svg` for future edits.
- Dashboard metrics switched from flex-wrap to explicit 6-column CSS grid;
  6 items now fit on a single row at 1440px and scale to 3 cols (tablet)
  and 1/2 cols (mobile). Labels `nowrap` + ellipsis to survive longer
  captions like "Validators (this epoch)".
- Dashboard `.metric-val` reduced 32 → 26px (22px ≤1400px) with
  `font-variant-numeric: tabular-nums` — long values like "26,388,658"
  no longer get ellipsis-truncated on 1440px viewports.
- Dashboard: metric labels compacted (9px + letter-spacing 0.02em) to
  prevent truncation of "Validators (this epoch)" at 1440px.
- `/validator.html` unified: `?id=N` and `?addr=0x…` now converge on a
  single renderer that shows Health Score + rank, block stats (when
  available), on-chain stake/commission/VDP-uptime row, pool-unclaimed
  vs operator-claimable split, health breakdown bars, consensus keys,
  and recent blocks — end of the previous "two half-pages" split.

### Fixed
- **Graph (root cause fix)**: labels no longer pile up in a phantom
  column above the diagram. Top-N delegators whose outgoing edges all
  fell outside top-N validators were being placed in the RIGHT column
  by d3-sankey (orphans with no outgoing flow in the filtered graph),
  but the label renderer anchored them by `n.type`, producing a
  misplaced x≈976 column. Their rects had height=0 so all labels
  collapsed to the same y and then got 16px-stacked by collision
  resolution. Fix classifies label side by rendered x position,
  hides labels for rects < 6px tall, and surfaces an explicit
  "+N delegators without top-N target" / "+N validators without
  top-N source" summary along the bottom of each column.
- Graph: tooltip now flips horizontally when it would overflow the
  viewport right edge and clamps to the viewport vertically; uses
  `position: fixed` so page scroll can't desync it from the cursor.
- Network toggle: switching Testnet ↔ Mainnet on /graph.html and
  /clusters.html now re-renders the page — previously reloadPage()
  had no case for either, so the old network's data lingered.
- Network toggle on /graph.html: follow-up fix. The first attempt
  added a /graph.html branch to reloadPage() that called `render()`,
  but that name didn't resolve reliably to the inline graph function
  from app.js scope. Renamed inline function to `renderGraph` and
  exposed as `window.renderGraph` (same for `window.loadClusters`).
- URL ?network= query now honored on page load (any page). Deep
  links like /graph.html?network=mainnet now land on the right
  network and persist the choice to sessionStorage.
- **Graph page Apply button** — restyled as a distinct primary button
  instead of near-invisible flat text. Added Enter-to-apply on both
  top-N inputs (blur-apply too, only if the value actually changed).
- **Graph page viewport fit** — Sankey height now scales with the
  number of rows (32px per node, clamped [420, 3200]), and the
  container has `max-height: calc(100vh - 260px)` with
  `overflow-y: auto` so large graphs scroll inside the container
  rather than pushing the "How to read" block off-screen.
- **Graph page labels regression** — restored row height coefficient
  (26 → 32) so low-stake delegator nodes stay above V9's 6px
  label-hiding threshold. rowH 26 pushed many small-stake nodes
  under the threshold and their labels vanished; V9 was tuned
  against 32.
- **Validator page `?id=`** — now loads the canonical name from
  `/validators/search?q=<valId>` in parallel with the other fetches
  so identified validators show their real name (e.g. "Backpack")
  instead of "Validator #N". The old `valName(auth)` lookup missed
  on mainnet because `block.miner ≠ auth` there — names map is
  indexed on miner. first-active endpoint failure is already
  non-fatal (falls back to "—" in VDP Uptime without breaking
  the page).
- **Validator page `?addr=`** — resolved a false "not registered in
  validator set" banner for real validators whose block-miner addr
  differs from their registered auth (Backpack mainnet). Two-step
  resolver: search by addr, then if empty use the names map to get
  a name and search by name instead. URL is normalized to `?id=N`
  (network query preserved) so both entry points converge.
- Validator page: Block Stats empty-state label no longer says
  "n/a (testnet)" regardless of active network; replaced with a
  neutral em-dash plus a network-agnostic tooltip.
- **Graph click navigation** — clicks on validator nodes now include
  `&network=` so shared links and network context stay consistent
  when opening the validator detail page.
- **Graph mobile viewport** — at ≤768px the container now scrolls
  horizontally and the SVG is pinned at `min-width: 900px` so labels
  stay readable; node labels locked to 12px on mobile. Previously
  the 1200px viewBox squeezed down ~3× on 390px screens, rendering
  labels at ~4px.
- **Network toggle URL sync** — clicking Testnet/Mainnet now rewrites
  the URL's `?network=` query via `history.replaceState`, so
  share-links / refresh / browser history reflect the active network
  instead of the stale one from the original page load. Works on
  every page with a network toggle (fixed in shared `initNetSwitch`).
- **Clusters "Details" button** — truncated to "Deta" because it
  inherited the circular `.page-btn` (32×32 fixed). Overridden with
  a scoped pill-shape class that allows the full word.
- **Validator page commission** — corrected wei→percent formatting.
  Previously showed nonsense values (e.g. `400000000000000.00 %`) for
  any validator with a nonzero commission. The staking contract
  returns commission as a wei-scale ratio (1e18 = 100%), not
  basis-points-times-100; dividing by `1e16` gives the correct
  percent. Hidden until now because testnet validators and Backpack
  all have commission = 0.
- **Validator page Health Score + Block Stats on mainnet** —
  `/health/scores` and `/validators/list` are indexed on
  `block.miner`, but mainnet rotates several ephemeral miner
  addresses per validator while `/validators/by-id` returns the
  registered auth. The strict auth-equality lookup missed on
  mainnet, leaving Backpack and siblings with blank Health Score
  and "—" Block Stats. Now builds a candidate-address set (auth +
  every miner addr the names map attributes to the same canonical
  name) and joins health/list/blocks on any of them. Block totals
  sum across all candidate miners for truthful figures (blocks +
  txns); avg_block_time + last_seen come from the most recent
  entry. Testnet unchanged.
- Dashboard: Top Validators widget names now always resolved on first
  render, including after a network switch. Previously `_loadDashboard`
  fired its health-score fetch before `loadNames()` completed on the
  switch path, so the widget briefly showed raw addresses. Fix: await
  `loadNames()` in both `reloadPage()` and inside `_loadDashboard`
  Promise.all itself.
- Stake bar chart (Top Validators by Estimated Rewards): X-axis labels
  no longer collapse to a single visible name — `autoSkip: false` plus
  45° rotation shows every validator in the top-15. Aspect ratio
  shortened to give the rotated labels room.
- Clusters: FOUNDATION badge now requires BOTH `validator_count >= 100`
  AND `total_stake_mon >= 10,000,000`. Previously count-only, which
  mis-labelled sybil / airdrop patterns (e.g. 169 validators × 1 MON
  each) as "foundation-scale redistribution". New `dust` category for
  high-count / low-stake addresses, added to the legend.
- Stake page: events feed now explicitly clears when switching to a
  network with no recent events, instead of leaving the previous
  network's events on screen.
- Gas page: Base Fee chart now carries an auto-annotation when the 48h
  range is flat (max–min < 0.5 gwei) explaining that dynamic base fee
  is not yet active on the selected network.
- Map: "Show all N known identities" button rephrased to clarify how
  many of the listed validators are actually plotted on the map vs how
  many are named but have no geo data.
- Clusters: removed horizontal scrollbar at 1440px — table now uses
  `table-layout: fixed` with explicit column widths and the wrapper is
  `overflow-x: hidden`. Previously content was 1193px in a 1185px wrapper,
  producing an 8px scrollbar that visually implied broken layout.
- Graph: Sankey labels no longer overlap when one flow dominates (e.g.
  Foundation 2.1B MON vs 14 delegators with micro-stakes). Added a
  label-collision pass that spreads labels by `fontSize + 4` minimum gap
  on each side, plus leader-lines connecting rects to shifted labels.
  `nodePadding` raised 8 → 14. On dense renders (>60×60) labels under
  0.2–0.5% share are hidden.
- Map: validator markers in dense regions (Europe, US) now cluster via
  the Leaflet.markercluster plugin. Cluster bubbles preserve the
  under/balanced/oversaturated color palette by dominant-category
  weighting across children; shadowoftime's teal highlight propagates
  into any cluster that contains it. Clustering disables at zoom ≥5.
- Graph: empty state now clears the stale SVG when no data — previously
  an old render stayed visible next to the "no data" banner, creating a
  contradictory UX.
- Graph: user-facing error messages no longer expose the backend
  rebuild-script filename (neutral `graph_not_ready` / "Graph data is
  not available yet" surfaces instead).
- Graph: Top delegators / Top validators inputs now `min=2 max=100` with
  client-side clamping; label "(2–100)" next to controls.
- Clusters: "Show inactive" toggle now uses a ≥1 MON active threshold
  (matching the table's rounded display). Sub-1-MON dust stakes that
  rendered as "0 MON" are no longer visible by default, fixing the
  checkbox/table-rows desync.

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
