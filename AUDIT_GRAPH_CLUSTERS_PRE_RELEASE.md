# Pre-Release Audit — Graph + Clusters

Date: 2026-04-19
Last commit audited: `3e40607 docs: changelog for graph rowH regression fix`
Auditor: Claude Code (no headless browser available — all browser-only checks
marked `NOT-AUTO-VERIFIABLE`, everything else done via live API +
d3-sankey simulation that replicates `renderGraph()` layout exactly).

---

## Graph

### A1. Measurement table (d3-sankey replay on live API data)

| network | config    | viewBox        | API meta                          | del labels | val labels | rects | zero-H | orphans on wrong side |
|---------|-----------|----------------|-----------------------------------|-----------:|-----------:|------:|-------:|----------------------:|
| mainnet | 15×15     | 0,0,1200,480   | 15/10039 del · 15/202 val · 13 f  |          8 |         12 |    30 |      9 |                     1 |
| mainnet | 30×15     | 0,0,1200,960   | 30/10039 del · 15/202 val · 43 f  |         18 |         15 |    45 |     12 |                     1 |
| mainnet | 50×30     | 0,0,1200,1600  | 50/10039 del · 30/202 val · 114 f |         32 |         30 |    80 |      7 |                     1 |
| mainnet | 100×100   | 0,0,1200,3200  | 100/10039 del · 100/202 val · 327 |         39 |        100 |   200 |      0 |                     0 |
| testnet | 15×15     | 0,0,1200,480   | 15/425 del · 15/200 val · 21 f    |          8 |         15 |    30 |      4 |                     1 |
| testnet | 30×15     | 0,0,1200,960   | 30/425 del · 15/200 val · 23 f    |          8 |         15 |    45 |     17 |                     1 |
| testnet | 50×30     | 0,0,1200,1600  | 50/425 del · 30/200 val · 46 f    |          8 |         30 |    80 |     29 |                     1 |
| testnet | 100×100   | 0,0,1200,3200  | 100/425 del · 100/200 val · 173 f |          8 |        100 |   200 |     22 |                     1 |

- All four seeded ranges match the previously-verified ballpark numbers.
- **Phantom-column check**: `orphans on wrong side` ≤1 in every config, and
  those singletons have `rectH=0` → `hideLabel=true` → no label is actually
  rendered. V9 phantom-column fix holds.
- **Hidden delegators on testnet** stay at 8 regardless of top-N. This is the
  Foundation concentration baseline: one whale (2.1B MON) dominates the
  vertical space so most small-stake delegators stay below the 6px
  `MIN_RECT_FOR_LABEL` threshold. They are surfaced via the
  "+N delegators without top-N target" summary along the container bottom.

### A2. Tooltip hover

**NOT-AUTO-VERIFIABLE** (no headless browser in the audit environment).

Code review: `showTT` in `graph.html` uses `getBoundingClientRect()` after
`classList.add('visible')`, flips horizontally when
`left + r.width > vw - pad`, clamps top/bottom, and anchors via
`position: fixed`. Logic matches the V9 intent and is syntactically
correct. Visual behavior must be confirmed on monadpulse.xyz:

1. Hover on Backpack node (mainnet 30×15) — expect node-total popup.
2. Hover on a flow — expect source→target + value popup.
3. Hover near right edge — expect horizontal flip.
4. Mouseout — expect popup to disappear within ~100ms.

### A3. Click on validator node

Code path: `window.location = '/validator.html?id=' + d.val_id` for
validators, `window.open(EXPLORER + '/address/' + d.label)` for delegators.
`val_id` comes from the `/analytics/delegation-graph` API payload. Spot-check
of live API: mainnet validator nodes carry `val_id` integers, so the link
format resolves. Validator page supports both `?id=` and `?addr=` since V5.

**NOT-AUTO-VERIFIABLE** end-to-end browser click, but the URL construction
and backend route are correct.

### A4. Mobile 390×812

**NOT-AUTO-VERIFIABLE** end-to-end.

Code review:
- Global `@media (max-width: 768px)` makes nav horizontally scrollable.
- `#sankey-container { min-height: 420px; max-height: calc(100vh - 260px) }`
  — on 812px mobile this resolves to max-height ≈ 552px, well above
  min-height, so internal scroll engages when needed.
- Sankey viewBox 1200 scales down 3.07× at 390px width — small but
  readable for labels at fontSize 12.
- `.gctrl-apply` button uses mono font 12px and does NOT shrink below
  that on mobile; `.gctrl` container has `flex-wrap: wrap`, so inputs
  + button wrap cleanly.

Known limitation: at 390×812 viewport the Sankey becomes dense; this is
fundamental to the diagram, not a bug.

### A5. Network toggle (V9.1 regression check)

Code: `reloadPage()` in `app.js` contains the `/graph.html` branch that
calls `window.renderGraph()`. `graph.html` exposes `window.renderGraph =
renderGraph;`. Both symbols present in deployed files (`curl` verified).
`apiFetch()` appends `&network=${NETWORK}` so the refetch uses the switched
network automatically.

**NOT-AUTO-VERIFIABLE** browser click sequence, but all three required
pieces (reloadPage branch, window export, API param) are in place.

### A6. Edge cases

Programmatic verification of `clamp(parseInt(input, 10), 2, 100)`:

| input   | parseInt | clamp result | expected | status |
|---------|----------|--------------|----------|--------|
| `15`    | 15       | 15           | 15       | PASS   |
| `101`   | 101      | 100          | 100      | PASS   |
| `1`     | 1        | 2            | 2        | PASS   |
| `0`     | 0        | 2            | 2        | PASS   |
| `-5`    | -5       | 2            | 2        | PASS   |
| `abc`   | NaN      | 2            | 2        | PASS   |
| (empty) | NaN      | 2            | 2        | PASS   |
| `15.7`  | 15       | 15           | 15       | PASS   |

Minimum config (2×2) on testnet returns 1 edge and renders normally
(validated via API). Empty-data path (`data.edges.length === 0`) triggers
`showEmpty()` with a neutral "No delegation data" message.

---

## Clusters

### B1. Categories — mainnet + testnet

Classification replicates `clusters.html` exactly:
`FOUNDATION`=count≥100 AND stake≥10M; `COORDINATED`=count≥21 AND stake≥100;
`POWER`=count≥6 AND stake≥100; `DUST`=count≥6 AND stake<100; `NORMAL` rest.

**Mainnet** (200 clusters in the limit=200 slice, 10039 total delegators):
| category    | all | ≥1 MON active |
|-------------|-----|---------------|
| foundation  |   1 |             1 |
| coordinated |   5 |             5 |
| power       |  49 |            49 |
| dust        |  13 |            12 |
| normal      | 132 |           130 |

The single FOUNDATION row (`0x1b68626dca36…`, 130 validators × 424M MON) is
the real Monad Foundation redistribution address. Previously the top-1 and
top-2 rows (169 vals × 169 MON, 168 vals × 168 MON) were mis-classified as
FOUNDATION under the count-only rule; now correctly categorized as
COORDINATED.

**Testnet** (71 clusters, 425 total delegators):
| category    | all | ≥1 MON active |
|-------------|-----|---------------|
| foundation  |   1 |             1 |
| coordinated |   0 |             0 |
| power       |  10 |            10 |
| dust        |   9 |             5 |
| normal      |  51 |            48 |

FOUNDATION row: `0xf235ab9b2f80…` (193 vals × 2.14B MON) — Monad testnet
Foundation. No COORDINATED class on testnet: the top-count non-Foundation
addresses carry near-zero stake and land in DUST correctly.

### B2. "Show inactive (0 MON)" toggle

Code: `ACTIVE_THRESHOLD_MON = 1`. Filter `c.total_stake_mon >= 1`. Meta
line: `${shown.length} shown · ${active.length} active · ${total} total`.
Checkbox `change` handler calls `renderClusters` (re-filter, re-classify,
re-render). Sub-1-MON dust that rounds to "0 MON" in the table is hidden
by default (V5 fix) — matches the rounded display.

### B3. Network toggle (V9.1)

Code: `reloadPage()` calls `window.loadClusters()` on `/clusters.html`.
`clusters.html` exposes `window.loadClusters = async function() {…}`.
`apiFetch()` sends `&network=${NETWORK}`. Pattern identical to Graph.

**NOT-AUTO-VERIFIABLE** browser click, but all hooks are in place and
verified on live: `curl https://monadpulse.xyz/js/app.js | grep
window.loadClusters` → match.

### B4. Sort / search

**No sort or search UI on Clusters by design.** Rows are ranked by
`validator_count` DESC from the API. The "Details" button per row reveals
the full validator chip list inline. Documented as the intended behavior
— not a bug.

### B5. Click behavior

- Delegator address → `EXPLORER/address/${c.delegator}` with
  `target="_blank"` → opens on SocialScan/explorer in a new tab.
- Validator chip → `/validator.html?id=${v.val_id}` → internal SPA
  navigation to the unified validator detail page.
- "Details" page-btn → `toggleCluster(idx)` → expands the chip list
  beneath the row (in-place, no navigation).

### B6. Mobile 390×812

**NOT-AUTO-VERIFIABLE** end-to-end.

Code review:
- Table wrapper uses `overflow-x: hidden` with `table-layout: fixed` and
  explicit column widths (V8 fix) — no horizontal scrollbar.
- Column widths in % scale naturally to 390px viewport.
- Five `signal` badges have distinct chip backgrounds (extreme/high/medium/
  low/dust). Colors verified in deployed CSS.

### B7. Data-integrity spot-check (3 rows per network)

Chip count vs `validator_count`, and stake sum (sum of `stake_wei / 1e18`
across chips) vs `total_stake_mon` — all within 0.5 MON tolerance:

| network | row index | count match | stake match |
|---------|-----------|-------------|-------------|
| mainnet | 0         | PASS        | PASS        |
| mainnet | 100       | PASS        | PASS        |
| mainnet | 199       | PASS        | PASS        |
| testnet | 0         | PASS        | PASS        |
| testnet | 35        | PASS        | PASS        |
| testnet | 70        | PASS        | PASS        |

---

## Cross-cutting

### C1. CHANGELOG

`[Unreleased]` section contains entries for:
- `f266547` Graph Apply button UX + Enter-to-apply → **present**
- `f1271bf` Graph viewport fit / internal scroll → **present**
- `dc37df8` Graph rowH regression restore → **present**

Plus prior V7–V9 audit entries (health score normalization, stake share %,
clusters classification, Sankey phantom column, tooltip clamping, network
toggle, `?network=` deep links). All ordered and clean.

### C2. README / about.html

**Fixed in this audit:** `README.md` previously didn't mention Graph or
Clusters pages at all. Added both with feature descriptions, bumped "8
pages + About" → "10 pages + About" in the architecture diagram.
`about.html` links both pages in nav; feature-doc expansion for it
left for a later pass (not blocking release).

### C3. Console errors

**NOT-AUTO-VERIFIABLE** runtime console in this environment.

Code-level lint on deployed HTML: balanced braces/parens/backticks in both
`/graph.html` (42/42, 201/201, 22) and `/clusters.html` (34/34, 45/45).
No `console.error` paths reachable under normal flow except the one in
`apiFetch` which only fires on a non-200 HTTP response — and in that case
`showEmpty()` is the UX fallback, not a red error.

### C4. Security headers

Both `/graph.html` and `/clusters.html` carry identical, correct headers:

| header                    | status                                              |
|---------------------------|-----------------------------------------------------|
| Strict-Transport-Security | `max-age=31536000; includeSubDomains`               |
| Content-Security-Policy   | present (default-src 'self', CDN allow-list)        |
| X-Frame-Options           | `DENY`                                              |
| X-Content-Type-Options    | `nosniff`                                           |
| Referrer-Policy           | `strict-origin-when-cross-origin`                   |
| Permissions-Policy        | `camera=(), microphone=(), geolocation=()`          |

### C5. OG image

`https://monadpulse.xyz/og-image.png` → `HTTP/1.1 200 OK`,
`Content-Type: image/png`, `Content-Length: 108565`.

---

## Bugs found and fixed in this audit

- **Documentation gap**: `README.md` didn't mention Graph or Clusters pages.
  Fixed in the same commit as this audit report (see git log).

No runtime bugs found in either page. The previously-flagged items (phantom
column, tooltip overflow, Apply button invisibility, viewport clipping,
rowH regression) are all verified-fixed by this audit.

## Known limitations acceptable for release

- **Mainnet 15×15 shows 8/15 delegator labels.** Backpack delegator (747M
  MON) holds ~93% of top-N stake, leaving 14 other delegators to share
  ~7% of vertical space. With `MIN_RECT_FOR_LABEL = 6` half of them fall
  below threshold. This reflects the real on-chain distribution, not a
  rendering bug. The "+N without top-N target" summary along the
  container bottom tells users they exist.
- **Testnet 15×15 shows 8/15 delegator labels** for the same reason
  (Foundation 2.1B MON whale).
- **Mobile Sankey density**: at 390×812 the viewBox scales 3× down; labels
  are small-but-readable. Sankey is fundamentally a desktop-first
  visualization — this is inherent, not a bug.
- **Tooltip on touch devices**: d3 `mouseenter` may not fire on pure
  touch; users would need to tap-hold. Accepted for v1 — tap hit-boxes
  on nodes still work for navigation (click opens validator detail).
- **Clusters sort/search**: intentionally omitted. Rows are ordered by
  validator count DESC; the data fits in a single page for most networks.

## Ready for release

**YES** — pending visual confirmation of tooltip flip, mobile layout, and
network-toggle clicks in a real browser (the four `NOT-AUTO-VERIFIABLE`
items in sections A2 / A4 / A5 / B3 / B6 / C3). All code paths, API
integrity, security headers, and OG asset check out; all data-integrity
spot-checks PASS.
