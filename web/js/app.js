/* MonadPulse — Main JS */

const API = '/api';
let NETWORK = sessionStorage.getItem('mp_net') || 'testnet';

// Chart.js — Monad palette (only if Chart.js is loaded)
if (typeof Chart !== 'undefined') {
  Chart.defaults.color = '#6B6580';
  Chart.defaults.borderColor = 'rgba(110,84,255,0.07)';
  Chart.defaults.font.family = "'Roboto Mono', monospace";
  Chart.defaults.font.size = 11;
}

/* ═══ Helpers ═══ */
const EXPLORER = 'https://monad.socialscan.io';

function esc(s) {
  if (!s) return '';
  const d = document.createElement('div');
  d.textContent = s;
  return d.innerHTML;
}
let _nameMap = {};
let _namesLoaded = false;

async function loadNames() {
  if (_namesLoaded) return;
  try {
    const r = await fetch(API + '/names/map?network=' + NETWORK);
    if (r.ok) _nameMap = await r.json();
  } catch (e) {}
  // Also load the directory so we can resolve names by validator ID
  // (stake events, and validators not in names_map yet).
  try {
    const dr = await fetch(API + '/validators/directory?network=' + NETWORK);
    if (dr.ok) {
      const dd = await dr.json();
      _directoryById = {};
      _directoryByAuth = {};
      (dd.validators || []).forEach(e => {
        if (e.val_id != null) _directoryById[e.val_id] = e;
        if (e.auth) _directoryByAuth[e.auth.toLowerCase()] = e;
      });
    }
  } catch (e) {}
  _namesLoaded = true;
}

let _directoryById = {};
let _directoryByAuth = {};

function valName(addr) {
  if (!addr) return null;
  const a = addr.toLowerCase();
  if (_nameMap[a]) return _nameMap[a];
  // Fallback via directory: auth addresses aren't in names_map on mainnet
  // because names_map is indexed on block.miner, but directory gives us
  // auth→name directly for anyone the upstream repo knows about.
  const dir = _directoryByAuth[a];
  return dir?.name || null;
}

function valNameById(valId) {
  if (valId == null) return null;
  const e = _directoryById[valId];
  return e?.name || null;
}

function blockLink(num) {
  if (num == null) return '—';
  return `<a href="${EXPLORER}/block/${num}" target="_blank" class="addr">${fmtNum(num)}</a>`;
}

function addrLink(addr) {
  if (!addr) return '—';
  const name = valName(addr);
  const display = name
    ? `<span class="val-name">${esc(name)}</span>`
    : shortAddr(addr);
  return `<a href="${EXPLORER}/address/${addr}" target="_blank" class="addr" title="${addr}">${display}</a>`;
}

function validatorLink(addr) {
  if (!addr) return '—';
  const name = valName(addr);
  const display = name
    ? `<span class="val-name">${esc(name)}</span>`
    : shortAddr(addr);
  return `<a href="/validator.html?addr=${addr}" class="addr" title="${addr}">${display}</a>`;
}

function shortAddr(addr) {
  if (!addr) return '—';
  return addr.slice(0, 8) + '...' + addr.slice(-6);
}

function fmtNum(n) {
  if (n == null) return '—';
  return Number(n).toLocaleString('en-US');
}

function fmtTime(iso) {
  if (!iso) return '—';
  return new Date(iso).toLocaleTimeString('en-US', {hour:'2-digit',minute:'2-digit',second:'2-digit',hour12:false});
}

function fmtHour(iso) {
  if (!iso) return '';
  return new Date(iso).toLocaleTimeString('en-US', {hour:'2-digit',minute:'2-digit',hour12:false});
}

function timeAgo(iso) {
  if (!iso) return '—';
  const s = Math.floor((Date.now() - new Date(iso).getTime()) / 1000);
  if (s < 60) return s + 's ago';
  if (s < 3600) return Math.floor(s/60) + 'm ago';
  return Math.floor(s/3600) + 'h ago';
}

async function apiFetch(path) {
  try {
    const sep = path.includes('?') ? '&' : '?';
    const r = await fetch(API + path + sep + 'network=' + NETWORK);
    if (!r.ok) throw new Error(r.status);
    return await r.json();
  } catch (e) {
    console.error('API error:', path, e);
    return null;
  }
}

/* ═══ Animated count-up for metrics ═══ */
function animateValue(el, target, duration = 800) {
  if (!el) return;
  const text = String(target);
  // If not a number, just set it
  if (isNaN(parseFloat(text.replace(/,/g, '')))) {
    el.textContent = text;
    return;
  }
  const suffix = text.replace(/[\d,.\s]/g, '');
  const num = parseFloat(text.replace(/[^0-9.]/g, ''));
  const start = parseFloat(el.textContent.replace(/[^0-9.]/g, '')) || 0;
  if (start === num) { el.textContent = text; return; }
  const startTime = performance.now();
  function step(now) {
    const progress = Math.min((now - startTime) / duration, 1);
    const eased = 1 - Math.pow(1 - progress, 3); // easeOutCubic
    const current = start + (num - start) * eased;
    if (num > 100) {
      el.textContent = Math.round(current).toLocaleString('en-US') + suffix;
    } else {
      el.textContent = current.toFixed(num % 1 ? 1 : 0) + suffix;
    }
    if (progress < 1) requestAnimationFrame(step);
  }
  requestAnimationFrame(step);
}

/* ═══ Live block ticker ═══ */
const _tickerQueue = [];
let _tickerBusy = false;

// Independent fast-poll just for the dashboard's live ticker + Latest Block
// metric. The full loadDashboard runs every 30 s for charts/tables/metrics
// (those don't change visibly faster), but at ~400 ms per block that 30 s
// gap means ~70 blocks scroll by between dashboard ticks — users staring
// at the page perceive that as "blocks lost". This refreshes the ticker
// every 2 s, reuses queueTickerBlock so dedup vs _lastBlockNum keeps
// behaviour identical, and only updates two DOM nodes.
async function refreshLiveTicker() {
  if (document.hidden) return;
  try {
    const recent = await apiFetch('/blocks/recent?limit=10');
    if (!recent || !recent.length) return;
    if (_lastBlockNum > 0) {
      recent.filter(b => b.number > _lastBlockNum)
            .sort((a, b) => a.number - b.number)
            .forEach(b => queueTickerBlock(b));
    }
    if (recent[0].number > _lastBlockNum) _lastBlockNum = recent[0].number;
    const blockEl = document.getElementById('m-block');
    if (blockEl && blockEl.textContent !== fmtNum(recent[0].number)) {
      animateValue(blockEl, fmtNum(recent[0].number));
    }
  } catch (_) { /* transient API hiccup — next tick retries */ }
}

function queueTickerBlock(block) {
  if (_tickerQueue.length > 20) _tickerQueue.splice(0, _tickerQueue.length - 10);
  _tickerQueue.push(block);
  if (!_tickerBusy) drainTicker();
}

function drainTicker() {
  const ticker = document.getElementById('block-ticker');
  if (!ticker || _tickerQueue.length === 0) { _tickerBusy = false; return; }
  _tickerBusy = true;

  const block = _tickerQueue.shift();
  const item = document.createElement('div');
  item.className = 'ticker-item';
  item.innerHTML = `
    <span class="ticker-block"><a href="${EXPLORER}/block/${block.number}" target="_blank">#${fmtNum(block.number)}</a></span>
    <span class="ticker-proposer">${validatorLink(block.proposer)}</span>
    <span class="ticker-tx">${block.tx_count} tx</span>
    <span class="ticker-gas">${fmtNum(block.gas_used)} gas</span>
    <span class="ticker-time">${fmtTime(block.timestamp)}</span>
  `;
  ticker.prepend(item);
  // Trigger reflow then animate
  void item.offsetHeight;
  requestAnimationFrame(() => item.classList.add('visible'));

  // Remove overflow items immediately
  while (ticker.children.length > 8) {
    ticker.lastChild.remove();
  }

  // Next block after delay — stagger them visually
  setTimeout(drainTicker, 600);
}

/* ═══ Chart gradient helpers ═══ */
const _gradients = {};

function barGradient(ctx, chartArea) {
  if (!chartArea) return 'rgba(110,84,255,0.4)';
  const key = 'bar_' + chartArea.bottom;
  if (_gradients[key]) return _gradients[key];
  const g = ctx.createLinearGradient(0, chartArea.bottom, 0, chartArea.top);
  g.addColorStop(0, 'rgba(110,84,255,0.25)');
  g.addColorStop(1, 'rgba(133,230,255,0.5)');
  _gradients[key] = g;
  return g;
}

function barHoverGradient(ctx, chartArea) {
  if (!chartArea) return 'rgba(133,230,255,0.7)';
  const key = 'barH_' + chartArea.bottom;
  if (_gradients[key]) return _gradients[key];
  const g = ctx.createLinearGradient(0, chartArea.bottom, 0, chartArea.top);
  g.addColorStop(0, 'rgba(110,84,255,0.5)');
  g.addColorStop(1, 'rgba(133,230,255,0.8)');
  _gradients[key] = g;
  return g;
}

/* ═══ Charts ═══ */
const _charts = {};

function makeChart(canvasId, type, labels, datasets, opts = {}) {
  const ctx = document.getElementById(canvasId);
  if (!ctx) return null;
  if (_charts[canvasId]) {
    const chart = _charts[canvasId];
    chart.data.labels = labels;
    // Only update data arrays, keep existing dataset objects to avoid leaks
    datasets.forEach((ds, i) => {
      if (chart.data.datasets[i]) {
        chart.data.datasets[i].data = ds.data;
      } else {
        chart.data.datasets[i] = ds;
      }
    });
    chart.update('none');
    return chart;
  }
  _charts[canvasId] = new Chart(ctx, {
    type,
    data: { labels, datasets },
    options: {
      responsive: true,
      maintainAspectRatio: true,
      aspectRatio: 2,
      animation: { duration: 800, easing: 'easeOutQuart' },
      interaction: { intersect: false, mode: 'index' },
      plugins: {
        legend: { display: false },
        tooltip: {
          enabled: true,
          backgroundColor: 'rgba(14,9,28,0.95)',
          borderColor: 'rgba(110,84,255,0.3)',
          borderWidth: 1,
          titleFont: { family: "'Roboto Mono', monospace", size: 11, weight: '600' },
          bodyFont: { family: "'Roboto Mono', monospace", size: 12 },
          titleColor: '#DDD7FE',
          bodyColor: '#EEEDF5',
          padding: { top: 10, bottom: 10, left: 14, right: 14 },
          cornerRadius: 8,
          displayColors: true,
          boxWidth: 8,
          boxHeight: 8,
          boxPadding: 4,
          caretSize: 6,
          caretPadding: 8,
        },
      },
      scales: {
        x: {
          grid: { display: false },
          ticks: { maxRotation: 0, maxTicksLimit: 12, color: '#6B6580' },
        },
        y: {
          grid: { color: 'rgba(110,84,255,0.06)', lineWidth: 0.5 },
          ticks: { color: '#6B6580' },
          beginAtZero: true,
        },
      },
      ...opts,
    },
  });
  return _charts[canvasId];
}

/* ═══ Validator search ═══ */
let _allValidators = [];

let _searchDebounce = null;

function initSearch() {
  const input = document.getElementById('validator-search');
  if (!input) return;
  input.addEventListener('input', () => {
    const q = input.value.toLowerCase().trim();
    if (!q) {
      renderValidators(_allValidators);
      renderDirectoryMatches([], '');
      return;
    }
    // Client-side filter of blocks-indexed validators
    const filtered = _allValidators.filter(v =>
      v.address.toLowerCase().includes(q) ||
      (valName(v.address) || '').toLowerCase().includes(q)
    );
    renderValidators(filtered);

    // Debounced directory search (name / val_id / auth / secp)
    clearTimeout(_searchDebounce);
    _searchDebounce = setTimeout(async () => {
      try {
        const r = await apiFetch('/validators/search?q=' + encodeURIComponent(q) + '&limit=10');
        if (!r || !r.matches) return;
        // Filter out ones already in the blocks-based list
        const knownAuths = new Set(_allValidators.map(v => v.address.toLowerCase()));
        const extra = r.matches.filter(m => !knownAuths.has(m.auth));
        renderDirectoryMatches(extra, q);
      } catch (e) {}
    }, 250);
  });
}

function renderDirectoryMatches(matches, query) {
  let box = document.getElementById('directory-matches');
  if (!box) {
    const table = document.querySelector('#validators-table');
    if (!table) return;
    box = document.createElement('div');
    box.id = 'directory-matches';
    box.style.cssText = 'margin:0 0 16px;padding:12px 16px;background:rgba(110,84,255,0.04);border:1px solid rgba(110,84,255,0.15);border-radius:10px;font-family:var(--mono);font-size:12px;display:none';
    table.parentElement.insertBefore(box, table);
  }
  if (!matches.length) { box.style.display = 'none'; return; }
  box.style.display = 'block';
  box.innerHTML = `<div style="color:var(--text-dim);margin-bottom:8px;font-size:10px;letter-spacing:1px;text-transform:uppercase">On-chain directory matches (not in block data)</div>` +
    matches.map(m => {
      const label = m.name ? esc(m.name) : `Validator #${m.val_id}`;
      return `<div style="padding:6px 0;border-top:1px solid rgba(110,84,255,0.08)">
        <a href="/validator.html?id=${m.val_id}" style="color:#a78bfa;text-decoration:none;font-weight:600">${label}</a>
        <span style="color:var(--text-dim);margin-left:8px">id ${m.val_id} · auth ${shortAddr(m.auth)}</span>
      </div>`;
    }).join('');
}

function scoreColor(score) {
  if (score >= 80) return '#4ade80';
  if (score >= 50) return '#FFAE45';
  return '#FF8EE4';
}

function scoreBar(score) {
  if (score == null) return '<span style="color:var(--text-dim)">—</span>';
  const color = scoreColor(score);
  return `<div style="display:flex;align-items:center;gap:8px">
    <div style="flex:1;height:4px;background:rgba(110,84,255,0.1);border-radius:2px;min-width:50px;max-width:80px">
      <div style="width:${score}%;height:100%;background:${color};border-radius:2px;box-shadow:0 0 6px ${color}40"></div>
    </div>
    <span style="color:${color};font-weight:600;font-size:12px">${score.toFixed(1)}</span>
  </div>`;
}

const VALS_PER_PAGE = 20;
let _valsPage = 0;
let _valsFiltered = [];

function renderValidators(data) {
  _valsFiltered = data;
  _valsPage = 0;
  renderValidatorsPage();
}

function renderValidatorsPage() {
  const data = _valsFiltered;
  const tbody = document.querySelector('#validators-table tbody');
  if (!tbody) return;
  if (!data.length) {
    tbody.innerHTML = '<tr><td colspan="8" style="text-align:center;color:var(--text-dim);padding:32px">No validators found</td></tr>';
    const pag = document.getElementById('validators-pagination');
    if (pag) pag.innerHTML = '';
    return;
  }

  const totalPages = Math.ceil(data.length / VALS_PER_PAGE);
  const start = _valsPage * VALS_PER_PAGE;
  const slice = data.slice(start, start + VALS_PER_PAGE);

  tbody.innerHTML = slice.map((v, i) => {
    const rank = start + i + 1;
    // Detect anomalies
    const flags = [];
    if (v.blocks_proposed < 50) flags.push('low-blocks');
    if (v.avg_block_time_ms != null && v.avg_block_time_ms > 450) flags.push('high-bt');
    if (v.avg_block_time_ms != null && v.avg_block_time_ms < 300 && v.avg_block_time_ms > 0) flags.push('low-bt');
    const hasAnomaly = flags.length > 0;

    const anomalyIcon = hasAnomaly ? '<span class="anomaly-icon" title="' + flags.join(', ') + '">&#9888;</span> ' : '';

    // Rotation badge — Foundation's stake rotation periodically pulls
    // delegation from VDP validators so every
    // operator gets time in the active-set. Validators flagged here are
    // *intentionally* out — blocks_proposed will fall to 0 until they cycle
    // back in. Not an operator-level outage.
    const rotationBadge = v.rotation_status === 'rotating'
      ? '<span class="rotation-badge" title="In Foundation stake rotation — temporarily out of active-set by design, not an outage">&#x1F504;</span> '
      : '';

    const btVal = v.avg_block_time_ms != null ? v.avg_block_time_ms + ' ms' : '—';
    const btClass = v.avg_block_time_ms != null && (v.avg_block_time_ms > 450 || (v.avg_block_time_ms < 300 && v.avg_block_time_ms > 0)) ? ' style="color:var(--pink,#FF8EE4)"' : '';
    const blocksClass = v.blocks_proposed < 50 ? ' style="color:var(--orange,#FFAE45)"' : '';

    // Uptime column — raw uptime_score from health_components (0-100, new
    // formula = actual vs expected blocks since first-active). Color-coded
    // so outages stand out at a glance.
    //
    // Rotation validators get a distinct cyan tint + "idle" tag instead of
    // green: the score reflects their *pre-rotation* performance (which is
    // still 100% — they were producing well before Foundation pulled stake),
    // but they're not signing *right now*. Showing full green 100% would
    // imply "live and healthy", which is misleading during forced idle.
    const up = v.health_components && v.health_components.uptime;
    const rotating = v.rotation_status === 'rotating';
    const upStr = up == null ? '—' : up.toFixed(1) + '%';
    // VDP 4-week uptime target is 98% — green only when above.
    const upCol = up == null ? 'var(--text-dim)'
      : rotating ? '#85E6FF'                          // cyan — idle by policy
      : up >= 98 ? '#4ade80' : up >= 50 ? '#FFAE45' : '#FF8EE4';
    const upSuffix = rotating
      ? '<span style="color:var(--text-dim);font-size:10px;margin-left:4px;font-weight:400" title="Score reflects performance before Foundation stake rotation. Currently not signing.">idle</span>'
      : '';
    const upCell = `<span style="color:${upCol};font-variant-numeric:tabular-nums;font-weight:600">${upStr}</span>${upSuffix}`;

    return `<tr class="fade-row${hasAnomaly ? ' anomaly-row' : ''}${v.rotation_status === 'rotating' ? ' rotating-row' : ''}" style="animation-delay:${Math.min(i * 20, 300)}ms">
      <td><span class="rank">${rank}</span></td>
      <td>${anomalyIcon}${rotationBadge}${validatorLink(v.address)}</td>
      <td>${scoreBar(v.health_score)}</td>
      <td>${upCell}</td>
      <td${blocksClass}>${fmtNum(v.blocks_proposed)}</td>
      <td${btClass}>${btVal}</td>
      <td>${fmtNum(v.total_tx)}</td>
      <td>${timeAgo(v.last_seen)}</td>
    </tr>`;
  }).join('');

  // Pagination
  const pag = document.getElementById('validators-pagination');
  if (!pag) return;
  if (totalPages <= 1) { pag.innerHTML = ''; return; }

  let html = '';
  for (let p = 0; p < totalPages; p++) {
    const active = p === _valsPage ? ' active' : '';
    html += `<button class="page-btn${active}" onclick="_valsPage=${p};renderValidatorsPage();this.closest('.data-section').querySelector('table').scrollIntoView({behavior:'smooth',block:'start'})">${p + 1}</button>`;
  }
  html += `<span style="color:var(--text-dim);font-family:var(--mono);font-size:11px;margin-left:8px">${start+1}–${Math.min(start+VALS_PER_PAGE, data.length)} of ${data.length}</span>`;
  pag.innerHTML = html;
}

/* ═══ Dashboard ═══ */
let _lastBlockNum = 0;
let _dashLoading = false;

async function loadDashboard() {
  if (_dashLoading) return;
  _dashLoading = true;
  try { await _loadDashboard(); } finally { _dashLoading = false; }
}

async function _loadDashboard() {
  // Ensure names are resolved BEFORE rendering. On network switch reloadPage()
  // clears _namesLoaded and schedules loadDashboard directly (without going
  // through the router which calls loadNames first), so the Top Validators
  // widget used to paint with raw addresses. Await it alongside the data fetches.
  const [_, summary, timeline, healthScores, recent, upgradeStatus] = await Promise.all([
    loadNames(),
    apiFetch('/dashboard/summary'),
    apiFetch('/blocks/timeline?hours=24'),
    apiFetch('/health/scores?limit=5'),
    apiFetch('/blocks/recent?limit=8'),
    apiFetch('/upgrades/status'),
  ]);

  if (!summary) {
    // If /dashboard/summary fails on the initial load but other endpoints
    // succeeded, the header stays as the "—" placeholder until the next
    // 30 s poll. Retry once after 2 s so a transient hiccup doesn't leave
    // the user staring at dashes.
    console.warn('Dashboard summary missing; scheduling retry in 2s');
    setTimeout(loadDashboard, 2000);
  }
  if (summary) {
    const lb = summary.latest_block;
    const s = summary.stats_24h;
    if (lb) animateValue(document.getElementById('m-block'), fmtNum(lb.number));
    if (s) {
      animateValue(document.getElementById('m-tps'), String(s.tps));
      animateValue(document.getElementById('m-blocktime'), s.avg_block_time_ms + ' ms');
      // Prefer current-epoch valset count (updates at each epoch boundary);
      // fall back to 24h distinct proposers if epoch data not yet recorded.
      const valCount = summary.epoch?.validator_count || s.active_validators;
      animateValue(document.getElementById('m-validators'), String(valCount));
      // Inject epoch number into the label and tooltip so the user can
      // distinguish "active in epoch N" from "registered in directory".
      const epochN = summary.epoch_progress?.current_epoch;
      const valLblEl = document.getElementById('m-validators-lbl');
      const valCardEl = document.getElementById('m-validators-card');
      if (valLblEl) {
        valLblEl.textContent = epochN ? `Active validators · epoch ${epochN}` : 'Active validators';
      }
      if (valCardEl && epochN) {
        valCardEl.title = `Validators currently producing blocks in epoch ${epochN}. Total registered validators (including idle / Foundation rotation) — see Validators page.`;
      }
      animateValue(document.getElementById('m-blocks24'), fmtNum(s.block_count));
      animateValue(document.getElementById('m-tx24'), fmtNum(s.total_tx));
    }
    // Epoch progress
    const ep = summary.epoch_progress;
    if (ep) {
      const numEl = document.getElementById('epoch-num');
      const fillEl = document.getElementById('epoch-fill');
      const pctEl = document.getElementById('epoch-pct');
      const etaEl = document.getElementById('epoch-eta');
      if (numEl) numEl.textContent = ep.current_epoch;
      if (fillEl) fillEl.style.width = ep.progress_pct + '%';
      if (pctEl) pctEl.textContent = ep.progress_pct + '%';
      if (etaEl) {
        const h = Math.floor(ep.eta_seconds / 3600);
        const m = Math.floor((ep.eta_seconds % 3600) / 60);
        etaEl.textContent = h > 0 ? `~${h}h ${m}m to next` : `~${m}m to next`;
      }
    }
  }

  if (timeline && timeline.length) {
    const labels = timeline.map(t => fmtHour(t.hour));
    makeChart('chart-blocks', 'bar', labels, [{
      label: 'Blocks',
      data: timeline.map(t => t.block_count),
      backgroundColor: function(context) {
        return barGradient(context.chart.ctx, context.chart.chartArea);
      },
      hoverBackgroundColor: function(context) {
        return barHoverGradient(context.chart.ctx, context.chart.chartArea);
      },
      borderColor: 'rgba(133,230,255,0.4)',
      borderWidth: 1,
      borderRadius: 6,
      borderSkipped: false,
    }]);
    makeChart('chart-tx', 'line', labels, [{
      label: 'Transactions',
      data: timeline.map(t => t.total_tx),
      borderColor: '#85E6FF',
      backgroundColor: 'rgba(133,230,255,0.06)',
      fill: true,
      tension: 0.4,
      pointRadius: 0,
      pointHoverRadius: 6,
      pointHoverBackgroundColor: '#85E6FF',
      pointHoverBorderColor: '#fff',
      pointHoverBorderWidth: 2,
      borderWidth: 2,
    }]);
  }

  if (recent && recent.length) {
    // Feed ticker with new blocks — staggered one by one
    if (_lastBlockNum > 0) {
      const newBlocks = recent.filter(b => b.number > _lastBlockNum);
      // Queue them oldest-first so they appear in order
      newBlocks.sort((a, b) => a.number - b.number).forEach(b => queueTickerBlock(b));
    } else {
      // First load — show last 6 immediately staggered
      recent.slice(0, 6).reverse().forEach(b => queueTickerBlock(b));
    }
    if (recent[0]) _lastBlockNum = recent[0].number;

    // Recent Blocks table — compact, 5 rows
    const tbody = document.querySelector('#recent-blocks tbody');
    if (tbody) {
      tbody.innerHTML = recent.slice(0, 5).map((b, i) => `
        <tr class="fade-row" style="animation-delay:${i * 30}ms">
          <td>${blockLink(b.number)}</td>
          <td>${validatorLink(b.proposer)}</td>
          <td>${b.tx_count}</td>
        </tr>
      `).join('');
    }
  }

  if (healthScores && healthScores.length) {
    // Show raw total_score (0–100 composite from the API) — no UI-side
    // normalization. Dashboard, Validators page and API must all agree.
    const tbody = document.querySelector('#top-proposers tbody');
    if (tbody) {
      tbody.innerHTML = healthScores.slice(0, 5).map((h, i) => {
        const score = h.total_score;
        const color = scoreColor(score);
        return `<tr class="fade-row" style="animation-delay:${i * 30}ms">
          <td><span class="rank">${i + 1}</span></td>
          <td>${validatorLink(h.validator_id)}</td>
          <td><span style="color:${color};font-weight:600">${score.toFixed(1)}</span></td>
        </tr>`;
      }).join('');
    }
  }

  // Upgrade tracker
  const strip = document.getElementById('upgrade-strip');
  if (strip && upgradeStatus) {
    const u = upgradeStatus;
    const hrs = u.hours_since_release;
    let dotClass = 'ok';
    let statusText = 'Up to date';
    if (!u.up_to_date) {
      if (hrs != null && hrs < 48) { dotClass = 'warn'; statusText = `Update available (${Math.round(hrs)}h ago)`; }
      else if (hrs != null) { dotClass = 'crit'; statusText = `Update overdue (${Math.round(hrs)}h ago)`; }
      else { dotClass = 'warn'; statusText = 'Update available'; }
    }

    const releases = (u.all_releases || []).map(r => {
      const isCurrent = r.tag.replace('v','') === u.current_version;
      return `<a href="${r.url}" target="_blank" class="upgrade-tag ${isCurrent ? 'current' : ''}">${r.tag}${r.prerelease ? ' rc' : ''}</a>`;
    }).join('');

    strip.innerHTML = `
      <div class="upgrade-ver">
        <span class="upgrade-dot ${dotClass}"></span>
        <span class="upgrade-current">v${u.current_version}</span>
        <span class="upgrade-latest">${statusText}</span>
      </div>
      <div class="upgrade-releases">${releases}</div>
    `;
  }
}

/* ═══ Blocks page ═══ */
let _blocksAllData = [];

function initBlocksControls() {
  // Search by block number
  const searchInput = document.getElementById('block-search');
  if (searchInput) {
    searchInput.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') {
        const num = parseInt(searchInput.value.replace(/,/g, ''));
        if (num > 0) {
          // Find in loaded data or open explorer
          const found = _blocksAllData.find(b => b.number === num);
          if (found) {
            showBlockDetail(found);
            window._blocksPage = Math.floor(_blocksAllData.indexOf(found) / BLOCKS_PER_PAGE);
            renderBlocksPage();
          } else {
            window.open(`${EXPLORER}/block/${num}`, '_blank');
          }
        }
      }
    });
  }

  // Custom proposer dropdown
  initProposerDropdown();
}

let _proposerList = [];

const NULL_ADDR = '0x0000000000000000000000000000000000000000';

function populateProposerFilter(data) {
  const list = document.getElementById('proposer-list');
  if (!list) return;
  const counts = {};
  data.forEach(b => { counts[b.proposer] = (counts[b.proposer] || 0) + 1; });
  _proposerList = Object.entries(counts).sort((a, b) => b[1] - a[1]).map(([addr, cnt]) => ({
    addr,
    name: valName(addr),
    display: addr === NULL_ADDR
      ? 'null proposer (recovery fail)'
      : (valName(addr) || shortAddr(addr)),
    count: cnt,
  }));
  renderProposerList(_proposerList);
}

function renderProposerList(items) {
  const list = document.getElementById('proposer-list');
  if (!list) return;
  list.innerHTML = `<div class="proposer-item" data-addr="">All proposers <span class="pi-count">${_blocksAllData.length}</span></div>` +
    items.map(p => `<div class="proposer-item" data-addr="${p.addr}">
      ${esc(p.display)} <span class="pi-count">${p.count}</span>
    </div>`).join('');
}

function initProposerDropdown() {
  const toggle = document.getElementById('proposer-toggle');
  const panel = document.getElementById('proposer-panel');
  const search = document.getElementById('proposer-search');
  const list = document.getElementById('proposer-list');
  if (!toggle || !panel) return;

  // Toggle open/close
  toggle.addEventListener('click', (e) => {
    e.stopPropagation();
    panel.classList.toggle('open');
    if (panel.classList.contains('open') && search) {
      search.value = '';
      renderProposerList(_proposerList);
      setTimeout(() => search.focus(), 50);
    }
  });

  // Close on outside click
  document.addEventListener('click', (e) => {
    if (!e.target.closest('.proposer-dropdown')) panel.classList.remove('open');
  });

  // Search within dropdown
  if (search) {
    search.addEventListener('input', () => {
      const q = search.value.toLowerCase().trim();
      if (!q) { renderProposerList(_proposerList); return; }
      const filtered = _proposerList.filter(p =>
        (p.name || '').toLowerCase().includes(q) || p.addr.toLowerCase().includes(q)
      );
      renderProposerList(filtered);
    });
  }

  // Click on item
  list.addEventListener('click', (e) => {
    const item = e.target.closest('.proposer-item');
    if (!item) return;
    const addr = item.dataset.addr;

    // Update button text
    if (!addr) {
      toggle.innerHTML = 'All proposers <span class="dd-arrow">&#9662;</span>';
      window._blocksData = _blocksAllData;
    } else {
      const p = _proposerList.find(x => x.addr === addr);
      const label = p ? esc(p.display) : shortAddr(addr);
      toggle.innerHTML = `${label} <span class="dd-arrow">&#9662;</span>`;
      window._blocksData = _blocksAllData.filter(b => b.proposer === addr);
    }

    panel.classList.remove('open');
    window._blocksPage = 0;
    renderBlocksPage();
  });
}

function showBlockDetail(block) {
  const panel = document.getElementById('block-detail');
  const content = document.getElementById('block-detail-content');
  if (!panel || !content) return;

  const name = valName(block.proposer);
  const proposerDisplay = name ? `${esc(name)} <span style="color:var(--text-dim)">${shortAddr(block.proposer)}</span>` : block.proposer;

  content.innerHTML = `
    <div style="display:grid;grid-template-columns:120px 1fr;gap:6px 16px;line-height:2">
      <span style="color:var(--text-dim)">Block</span>
      <span><a href="${EXPLORER}/block/${block.number}" target="_blank" style="color:var(--purple-light,#DDD7FE);font-weight:600">#${fmtNum(block.number)}</a></span>
      <span style="color:var(--text-dim)">Timestamp</span>
      <span>${new Date(block.timestamp).toUTCString()}</span>
      <span style="color:var(--text-dim)">Proposer</span>
      <span>${proposerDisplay}</span>
      <span style="color:var(--text-dim)">Transactions</span>
      <span>${block.tx_count}</span>
      <span style="color:var(--text-dim)">Gas Used</span>
      <span>${fmtNum(block.gas_used)}</span>
      <span style="color:var(--text-dim)">Base Fee</span>
      <span>${block.base_fee != null ? (block.base_fee / 1e9) + ' gwei' : '—'}</span>
      <span style="color:var(--text-dim)">Explorer</span>
      <span><a href="${EXPLORER}/block/${block.number}" target="_blank" style="color:var(--cyan,#85E6FF)">View on SocialScan →</a></span>
    </div>
  `;
  panel.style.display = 'block';
  panel.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
}

async function loadBlocks() {
  const [recent, timeline] = await Promise.all([
    apiFetch('/blocks/recent?limit=100'),
    apiFetch('/blocks/timeline?hours=48'),
  ]);

  if (timeline && timeline.length) {
    const labels = timeline.map(t => fmtHour(t.hour));
    makeChart('chart-blocks-48h', 'bar', labels, [{
      label: 'Blocks per hour',
      data: timeline.map(t => t.block_count),
      backgroundColor: function(context) {
        return barGradient(context.chart.ctx, context.chart.chartArea);
      },
      hoverBackgroundColor: function(context) {
        return barHoverGradient(context.chart.ctx, context.chart.chartArea);
      },
      borderColor: 'rgba(133,230,255,0.4)',
      borderWidth: 1,
      borderRadius: 6,
      borderSkipped: false,
    }]);
    const btData = timeline.map(t => t.avg_block_time);
    const btAvg = btData.reduce((a, b) => a + b, 0) / btData.length;
    const btStd = Math.sqrt(btData.reduce((s, v) => s + (v - btAvg) ** 2, 0) / btData.length);
    // Tight Y axis: avg +/- 3*std, minimum 10ms padding
    const spread = Math.max(btStd * 3, 10);
    const btMin = Math.round(btAvg - spread);
    const btMax = Math.round(btAvg + spread);

    // Average line as second dataset
    const avgLine = btData.map(() => Math.round(btAvg));

    makeChart('chart-blocktime', 'line', labels, [
      {
        label: 'Avg Block Time (ms)',
        data: btData,
        borderColor: '#FFAE45',
        backgroundColor: 'transparent',
        fill: false,
        tension: 0.4,
        pointRadius: 2,
        pointBackgroundColor: '#FFAE45',
        pointHoverRadius: 6,
        pointHoverBackgroundColor: '#FFAE45',
        pointHoverBorderColor: '#fff',
        pointHoverBorderWidth: 2,
        borderWidth: 2.5,
      },
      {
        label: 'Mean (' + Math.round(btAvg) + ' ms)',
        data: avgLine,
        borderColor: 'rgba(133,230,255,0.3)',
        borderWidth: 1,
        borderDash: [6, 4],
        pointRadius: 0,
        pointHoverRadius: 0,
        fill: false,
        tension: 0,
      },
    ], {
      plugins: { legend: { display: true, labels: { boxWidth: 12, padding: 16, color: '#6B6580', font: { size: 10, family: "'Roboto Mono', monospace" } } } },
      scales: {
        x: { grid: { display: false }, ticks: { maxRotation: 0, maxTicksLimit: 12 } },
        y: {
          grid: { color: 'rgba(110,84,255,0.06)', lineWidth: 0.5 },
          min: btMin, max: btMax,
          ticks: { callback: v => v + ' ms', color: '#6B6580', stepSize: Math.max(1, Math.round(spread / 4)) },
        },
      },
    });

    // "Now" point from most recent blocks
    const tpsData = timeline.map(t => {
      return t.block_count > 0 ? Math.round(t.total_tx / 3600 * 10) / 10 : 0;
    });
    const nowBlocks = await apiFetch('/blocks/recent?limit=10');
    const nowLabels = [...labels];
    const GAS_LIMIT_M = 200;
    const gasUsed = timeline.map(t => t.avg_gas ? Math.round(t.avg_gas / 1e6) : 0);
    const tpsWithNow = [...tpsData];

    if (nowBlocks && nowBlocks.length > 1) {
      const avgGasNow = nowBlocks.reduce((s, b) => s + b.gas_used, 0) / nowBlocks.length;
      const avgTxNow = nowBlocks.reduce((s, b) => s + b.tx_count, 0) / nowBlocks.length;
      gasUsed.push(Math.round(avgGasNow / 1e6));
      tpsWithNow.push(Math.round(avgTxNow / 0.4 * 10) / 10);
      nowLabels.push('Now');
    }

    // TPS chart
    const tpsPointRadius = tpsWithNow.map((_, i) => i === tpsWithNow.length - 1 && nowLabels[i] === 'Now' ? 6 : 2);
    const tpsPointColor = tpsWithNow.map((_, i) => i === tpsWithNow.length - 1 && nowLabels[i] === 'Now' ? '#FF8EE4' : '#85E6FF');

    makeChart('chart-tps', 'line', nowLabels, [{
      label: 'TPS',
      data: tpsWithNow,
      borderColor: '#85E6FF',
      backgroundColor: 'rgba(133,230,255,0.06)',
      fill: true,
      tension: 0.4,
      pointRadius: tpsPointRadius,
      pointBackgroundColor: tpsPointColor,
      pointHoverRadius: 8,
      pointHoverBackgroundColor: '#85E6FF',
      pointHoverBorderColor: '#fff',
      pointHoverBorderWidth: 2,
      borderWidth: 2,
    }]);

    const gasLimitLine = nowLabels.map(() => GAS_LIMIT_M);

    // Highlight "Now" point — last point larger and different color
    const gasPointRadius = gasUsed.map((_, i) => i === gasUsed.length - 1 && nowLabels[i] === 'Now' ? 6 : 2);
    const gasPointColor = gasUsed.map((_, i) => i === gasUsed.length - 1 && nowLabels[i] === 'Now' ? '#FF8EE4' : '#6E54FF');

    makeChart('chart-gas-usage', 'line', nowLabels, [
      {
        label: 'Avg Gas/Block (M)',
        data: gasUsed,
        borderColor: '#6E54FF',
        backgroundColor: 'rgba(110,84,255,0.06)',
        fill: true,
        tension: 0.4,
        pointRadius: gasPointRadius,
        pointBackgroundColor: gasPointColor,
        pointHoverRadius: 8,
        pointHoverBorderColor: '#fff',
        pointHoverBorderWidth: 2,
        borderWidth: 2,
      },
      {
        label: 'Gas Limit (200M)',
        data: gasLimitLine,
        borderColor: 'rgba(255,142,228,0.4)',
        borderWidth: 1.5,
        borderDash: [6, 4],
        pointRadius: 0,
        pointHoverRadius: 0,
        fill: false,
        tension: 0,
      },
    ], {
      plugins: { legend: { display: true, labels: { boxWidth: 12, padding: 16, color: '#6B6580', font: { size: 10, family: "'Roboto Mono', monospace" } } } },
      scales: {
        x: { grid: { display: false }, ticks: { maxRotation: 0, maxTicksLimit: 12 } },
        y: { grid: { color: 'rgba(110,84,255,0.06)', lineWidth: 0.5 }, beginAtZero: true,
          ticks: { callback: v => v + 'M', color: '#6B6580' } },
      },
    });
  }

  if (recent && recent.length) {
    // Meta labels: base fee + real avg block time
    const meta = document.getElementById('blocks-meta');
    if (meta && recent.length > 1) {
      // Base fee
      const fees = new Set(recent.map(b => b.base_fee).filter(f => f != null));
      let feeText = '';
      if (fees.size === 1) {
        feeText = `Base Fee: ${[...fees][0] / 1e9} gwei`;
      } else if (fees.size > 1) {
        feeText = `Base Fee: ${Math.min(...fees) / 1e9}–${Math.max(...fees) / 1e9} gwei`;
      }

      // Real block time from timestamps
      const first = new Date(recent[recent.length - 1].timestamp).getTime();
      const last = new Date(recent[0].timestamp).getTime();
      const spanMs = last - first;
      const avgBt = spanMs > 0 ? (spanMs / (recent.length - 1)).toFixed(0) : '—';

      meta.innerHTML = [
        feeText ? `<span>${feeText}</span>` : '',
        `<span>Avg Block Time: ${avgBt} ms (computed from ${recent.length} blocks)</span>`,
      ].filter(Boolean).join('<span style="color:var(--purple,#6E54FF);opacity:0.3">|</span>');
    }

    _blocksAllData = recent;
    window._blocksData = recent;
    window._blocksPage = 0;
    populateProposerFilter(recent);
    renderBlocksPage();
  }
}

const BLOCKS_PER_PAGE = 20;

function renderBlocksPage() {
  const data = window._blocksData;
  if (!data) return;
  const page = window._blocksPage || 0;
  const totalPages = Math.ceil(data.length / BLOCKS_PER_PAGE);
  const start = page * BLOCKS_PER_PAGE;
  const slice = data.slice(start, start + BLOCKS_PER_PAGE);

  const tbody = document.querySelector('#blocks-table tbody');
  tbody.innerHTML = slice.map((b, i) => `
    <tr class="fade-row block-clickable" style="animation-delay:${Math.min(i * 15, 300)}ms;cursor:pointer" data-idx="${start + i}">
      <td><span class="addr" style="color:var(--purple-light,#DDD7FE);font-weight:600">#${fmtNum(b.number)}</span></td>
      <td>${fmtTime(b.timestamp)}</td>
      <td>${validatorLink(b.proposer)}</td>
      <td>${b.tx_count}</td>
      <td>${fmtNum(b.gas_used)}</td>
    </tr>
  `).join('');

  // Row click → detail (but not if clicking a link)
  tbody.querySelectorAll('.block-clickable').forEach(tr => {
    tr.addEventListener('click', (e) => {
      if (e.target.closest('a')) return;
      const idx = parseInt(tr.dataset.idx);
      const block = window._blocksData[idx];
      if (block) showBlockDetail(block);
    });
  });

  // Pagination controls
  const pag = document.getElementById('blocks-pagination');
  if (!pag) return;
  if (totalPages <= 1) { pag.innerHTML = ''; return; }

  let html = '';
  for (let p = 0; p < totalPages; p++) {
    const active = p === page ? ' active' : '';
    html += `<button class="page-btn${active}" onclick="window._blocksPage=${p};renderBlocksPage();this.closest('.data-section').querySelector('table').scrollIntoView({behavior:'smooth',block:'start'})">${p + 1}</button>`;
  }
  html += `<span style="color:var(--text-dim,#6B6580);font-family:var(--mono,'Roboto Mono',monospace);font-size:11px;margin-left:8px">${start+1}–${Math.min(start+BLOCKS_PER_PAGE, data.length)} of ${data.length}</span>`;
  pag.innerHTML = html;
}

/* ═══ Validators page ═══ */
let _sortBy = 'health'; // 'health' | 'uptime' | 'blocks'

async function loadValidators(period = '24h') {
  const [data, healthData] = await Promise.all([
    apiFetch('/validators/list?period=' + period),
    apiFetch('/health/scores?limit=500'),
  ]);
  if (!data) return;

  // Merge health scores into validator data. /health/scores is keyed on
  // auth_address (canonical, after cross-miner clustering), while
  // /validators/list still returns per-miner rows with address=miner_addr.
  // Backend now exposes an auth_address field on each list row so we can
  // do the join directly — fall back to address if the backend version is
  // older or the validator isn't in the directory.
  const scoreMap = {};
  if (healthData) {
    healthData.forEach(h => { scoreMap[(h.validator_id || '').toLowerCase()] = h; });
  }
  data.forEach(v => {
    const key = (v.auth_address || v.address || '').toLowerCase();
    const h = scoreMap[key];
    if (h) {
      v.health_score_raw = h.total_score;
      v.health_components = {
        uptime: h.uptime_score,
        miss: h.miss_score,
        upgrade: h.upgrade_score,
        stake: h.stake_score,
        age: h.age_score,
      };
    } else {
      v.health_score_raw = null;
    }
  });

  // Use the raw composite score from the API (0-100 theoretical max). Do NOT
  // min/max normalize against the current population — that would produce a
  // different number on every page and break the invariant that API ==
  // Dashboard == Validators for a given validator.
  data.forEach(v => {
    v.health_score = v.health_score_raw != null
      ? Math.round(v.health_score_raw * 10) / 10
      : null;
  });

  // Sort by health score by default
  if (_sortBy === 'health') {
    data.sort((a, b) => (b.health_score || 0) - (a.health_score || 0));
  }

  _allValidators = data;
  renderValidators(data);

  // If addr param — scroll and highlight
  const params = new URLSearchParams(window.location.search);
  const addr = params.get('addr');
  if (addr) {
    const input = document.getElementById('validator-search');
    if (input) {
      input.value = addr;
      input.dispatchEvent(new Event('input'));
    }
  }
}

/* ═══ Gas page ═══ */
async function loadGas() {
  const [hourly, heatmap] = await Promise.all([
    apiFetch('/gas/hourly?hours=48'),
    apiFetch('/gas/heatmap'),
  ]);

  if (hourly && hourly.length) {
    const labels = hourly.map(h => fmtHour(h.hour));
    makeChart('chart-gas', 'line', labels, [{
      label: 'Avg Gas Used',
      data: hourly.map(h => h.avg_gas),
      borderColor: '#6E54FF',
      backgroundColor: 'rgba(110,84,255,0.08)',
      fill: true, tension: 0.4, pointRadius: 0, borderWidth: 2,
    }]);
    const baseFeeValues = hourly.map(h => h.avg_base_fee / 1e9);
    makeChart('chart-basefee', 'line', labels, [{
      label: 'Avg Base Fee (gwei)',
      data: baseFeeValues,
      borderColor: '#FFAE45',
      backgroundColor: 'rgba(255,174,69,0.08)',
      fill: true, tension: 0.4, pointRadius: 0, borderWidth: 2,
    }]);
    // Base fee is currently pinned on Monad (dynamic pricing not yet
    // activated). If 48h of data is a flat line, explain it in-place rather
    // than leaving viewers to wonder why the chart is a straight line.
    const note = document.getElementById('basefee-note');
    if (note && baseFeeValues.length) {
      const min = Math.min(...baseFeeValues);
      const max = Math.max(...baseFeeValues);
      const isPinned = (max - min) < 0.5;
      if (isPinned) {
        note.style.display = 'block';
        note.innerHTML = `Dynamic base fee is not yet active on Monad ${esc(NETWORK)} — value is pinned at ${min.toFixed(0)} gwei.`;
      } else {
        note.style.display = 'none';
      }
    }
    makeChart('chart-txcount', 'bar', labels, [{
      label: 'Transactions',
      data: hourly.map(h => h.tx_count),
      backgroundColor: 'rgba(133,230,255,0.35)',
      hoverBackgroundColor: 'rgba(133,230,255,0.6)',
      borderColor: '#85E6FF',
      borderWidth: 1, borderRadius: 6, borderSkipped: false,
    }]);
  }

  if (heatmap && heatmap.length) renderHeatmap(heatmap);
}

function renderHeatmap(data) {
  const container = document.getElementById('heatmap');
  if (!container) return;
  const days = ['Sun','Mon','Tue','Wed','Thu','Fri','Sat'];
  const maxTx = Math.max(...data.map(d => d.avg_tx || 0), 1);
  const daysWithData = new Set(data.map(d => d.dow)).size;
  let html = '';
  if (daysWithData < 7) {
    const missing = 7 - daysWithData;
    html += `<div style="font-family:var(--mono);font-size:11px;color:var(--text-dim);margin-bottom:10px;letter-spacing:0.3px">
      <span style="color:#FFAE45">◇</span> Data populating — ${daysWithData}/7 days collected, full week view available after ${missing} more day${missing>1?'s':''} of collection
    </div>`;
  }
  html += '<div class="heatmap-grid"><div></div>';
  for (let h = 0; h < 24; h++) html += `<div class="heatmap-cell" style="color:var(--text-dim)">${h}</div>`;
  for (let d = 0; d < 7; d++) {
    html += `<div class="heatmap-label">${days[d]}</div>`;
    for (let h = 0; h < 24; h++) {
      const cell = data.find(x => x.dow === d && x.hour === h);
      if (cell) {
        const val = cell.avg_tx || 0;
        const intensity = Math.min(val / maxTx, 1);
        html += `<div class="heatmap-cell" style="background:rgba(110,84,255,${(0.05 + intensity * 0.55).toFixed(2)})" title="${days[d]} ${h}:00 — ${val} avg tx">${val||''}</div>`;
      } else {
        html += `<div class="heatmap-cell heatmap-empty" title="${days[d]} ${h}:00 — no data"></div>`;
      }
    }
  }
  html += '</div>';
  container.innerHTML = html;
}

/* ═══ Alerts page ═══ */
const ALERTS_PAGE_SIZE = 10;
let _alertsAll = [];
let _alertsFilter = '';
let _alertsVisible = ALERTS_PAGE_SIZE;
let _lastAlertId = 0;

function linkifyAddresses(escapedText) {
  return escapedText.replace(/0x[0-9a-fA-F]{40}/g, (addr) =>
    `<a href="/validator.html?addr=${addr.toLowerCase()}" class="addr" title="${addr}">${shortAddr(addr)}</a>`
  );
}

// Allowed tag whitelist for alert descriptions (the source is our own
// Telegram dispatcher which formats with these tags). Anything outside
// the whitelist gets escaped — no DOMPurify needed for a 6-tag set.
// `<a>` keeps `href` only and only when the URL is http/https or root-relative.
const _ALERT_ALLOWED_TAGS = new Set(['b','i','code','blockquote','br','a','strong','em']);

function sanitizeAlertHtml(raw) {
  if (!raw) return '';
  // First fully escape, then unescape the small set of tags we trust.
  // Per-tag regex is safer than a full HTML parser at this scope.
  let out = esc(raw);
  // Unescape simple paired/empty tags
  out = out.replace(
    /&lt;(\/?)(b|i|code|blockquote|br|strong|em)\s*\/?&gt;/gi,
    (_m, slash, tag) => `<${slash}${tag.toLowerCase()}>`
  );
  // Anchors: allow href only, validate URL scheme
  out = out.replace(
    /&lt;a\s+href=(?:&quot;|"|&#039;|')([^"'&]+?)(?:&quot;|"|&#039;|')\s*&gt;/gi,
    (_m, href) => {
      if (!/^(https?:\/\/|\/)/i.test(href)) return '';
      return `<a href="${href}" target="_blank" rel="noopener">`;
    }
  );
  out = out.replace(/&lt;\/a&gt;/gi, '</a>');
  return out;
}

function renderAlertItem(a) {
  const title = linkifyAddresses(esc(a.title));
  // Description may contain HTML formatting (blockquote/b/a) from the Telegram
  // dispatcher — sanitize+whitelist instead of plain-escape so the formatting
  // renders. linkifyAddresses runs after to wrap raw 0x addresses too.
  const desc = a.description ? linkifyAddresses(sanitizeAlertHtml(a.description)) : '';
  return `<div class="alert-item type-${a.type}" data-id="${a.id}">
    <div class="alert-sev ${a.severity}"></div>
    <div class="alert-content">
      <div class="alert-title">${title}</div>
      <div class="alert-time">${fmtTime(a.timestamp)} — ${timeAgo(a.timestamp)}</div>
      ${desc ? `<div class="alert-desc">${desc}</div>` : ''}
      <span class="alert-type-badge">${a.type}</span>
    </div>
  </div>`;
}

function renderAlertsList(animateNew = false) {
  const container = document.getElementById('alerts-list');
  const more = document.getElementById('alerts-loadmore-wrap');
  if (!container) return;

  const filtered = _alertsFilter
    ? _alertsAll.filter(a => a.type === _alertsFilter)
    : _alertsAll;

  if (!filtered.length) {
    container.innerHTML = `<div class="empty-state">No ${_alertsFilter ? _alertsFilter + ' ' : ''}alerts in recent history.</div>`;
    if (more) more.style.display = 'none';
    return;
  }

  const slice = filtered.slice(0, _alertsVisible);
  const prevTopId = animateNew ? _lastAlertId : null;
  container.innerHTML = slice.map(renderAlertItem).join('');

  if (animateNew && prevTopId) {
    slice.forEach(a => {
      if (a.id > prevTopId) {
        const el = container.querySelector(`.alert-item[data-id="${a.id}"]`);
        if (el) {
          el.style.opacity = '0';
          el.style.transform = 'translateY(-16px)';
          requestAnimationFrame(() => {
            el.style.transition = 'all 0.4s ease-out';
            el.style.opacity = '1';
            el.style.transform = 'translateY(0)';
          });
        }
      }
    });
  }

  if (more) {
    const remaining = filtered.length - _alertsVisible;
    if (remaining > 0) {
      more.style.display = 'block';
      const btn = document.getElementById('alerts-loadmore');
      if (btn) btn.textContent = `Load more (${remaining} remaining)`;
    } else {
      more.style.display = 'none';
    }
  }
}

async function loadAlerts() {
  const data = await apiFetch('/alerts/recent?limit=200');
  if (!data) return;
  const isFirstLoad = _alertsAll.length === 0;
  _alertsAll = data;
  renderAlertsList(!isFirstLoad);
  if (data.length) _lastAlertId = data[0].id;
  renderAlertsSummary();
}

function renderAlertsSummary() {
  const sum = document.getElementById('alerts-summary');
  if (!sum) return;
  const TYPE_COLOR = {
    new_epoch: '#00d4ff', tps_spike: '#f59e0b', slow_block: '#ef4444', new_version: '#FF8EE4',
  };
  const LABEL = {
    slow_block: 'slow blocks', tps_spike: 'TPS spikes', new_epoch: 'new epochs', new_version: 'version alerts',
  };
  if (!_alertsAll.length) {
    sum.innerHTML = `<span class="lbl">Recent</span><span style="font-style:italic">no alerts</span>`;
    return;
  }
  const byType = {};
  _alertsAll.forEach(a => { byType[a.type] = (byType[a.type] || 0) + 1; });
  const parts = Object.entries(byType)
    .sort((a, b) => b[1] - a[1])
    .map(([t, c]) => `<span class="chip" style="color:${TYPE_COLOR[t] || '#DDD7FE'}">${c}</span> ${LABEL[t] || t}`);
  sum.innerHTML = `<span class="lbl">Recent (${_alertsAll.length})</span>${parts.join('<span class="sep">·</span>')}`;
}

function initAlertsControls() {
  document.querySelectorAll('#alerts-filter .period-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      document.querySelectorAll('#alerts-filter .period-btn').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      _alertsFilter = btn.dataset.type || '';
      _alertsVisible = ALERTS_PAGE_SIZE;
      renderAlertsList(false);
    });
  });
  const btn = document.getElementById('alerts-loadmore');
  if (btn) {
    btn.addEventListener('click', () => {
      _alertsVisible += ALERTS_PAGE_SIZE;
      renderAlertsList(false);
    });
  }
}

/* ═══ Network switch ═══ */
async function reloadPage() {
  // Destroy all charts
  Object.keys(_charts).forEach(k => { _charts[k].destroy(); delete _charts[k]; });
  _lastBlockNum = 0;
  _allValidators = [];
  // Clear Live Blocks ticker (stale blocks from previous network)
  _tickerQueue.length = 0;
  _tickerBusy = false;
  const tickerEl = document.getElementById('block-ticker');
  if (tickerEl) tickerEl.innerHTML = '';
  // Force reload of names for the new network and wait — otherwise page-level
  // loaders that don't call loadNames themselves (loadValidators, loadBlocks,
  // loadGas, loadAlerts) will render with an empty _nameMap and show raw
  // addresses until the next refresh cycle.
  _namesLoaded = false;
  _nameMap = {};
  await loadNames();

  const path = _normalizePath(window.location.pathname);
  if (path === '/' || path === '/index') loadDashboard();
  else if (path === '/blocks') loadBlocks();
  else if (path === '/validators') loadValidators();
  else if (path === '/map') { if (typeof buildMap === 'function') buildMap(); }
  else if (path === '/gas') loadGas();
  else if (path === '/alerts') { _alertsAll = []; loadAlerts(); }
  else if (path === '/stake') { if (typeof loadStake === 'function') loadStake(); }
  else if (path === '/validator') { if (typeof loadValidator === 'function') loadValidator(); }
  else if (path === '/governance') loadGovernance();
  else if (path === '/governance-mip') loadGovernanceMip();
  // Pages whose loader is declared inline in the HTML — expose via window.*
  // so reloadPage can reach them from app.js scope reliably.
  else if (path === '/graph') { if (typeof window.renderGraph === 'function') window.renderGraph(); }
  else if (path === '/clusters') { if (typeof window.loadClusters === 'function') window.loadClusters(); }
}

// Normalize pathname for the page dispatcher. Both /blocks and /blocks.html
// resolve to '/blocks' so clean URLs (served by the nginx try_files chain)
// fire the same loader as the .html form. Without this, /blocks rendered
// the dashboard's empty state because no branch matched.
function _normalizePath(p) {
  return (p || '/').replace(/\.html$/i, '');
}

function initNetSwitch() {
  document.querySelectorAll('.net-btn').forEach(btn => {
    // Sync initial active state with restored NETWORK
    btn.classList.toggle('active', btn.dataset.net === NETWORK);
    btn.addEventListener('click', () => {
      const net = btn.dataset.net;
      if (net === NETWORK) return;
      NETWORK = net;
      sessionStorage.setItem('mp_net', net);
      document.querySelectorAll('.net-btn').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      // Keep the URL in sync with the toggle. Share-links / refresh /
      // browser history now reflect the active network instead of the
      // stale ?network= (or missing one) from the original page load.
      const url = new URL(location.href);
      url.searchParams.set('network', net);
      history.replaceState(null, '', url.pathname + url.search + url.hash);
      // Update live dot text
      const dot = document.querySelector('.live-dot');
      if (dot) dot.textContent = `Live — syncing with Monad ${net}`;
      reloadPage();
    });
  });
  // Sync live dot on page load too
  const dot = document.querySelector('.live-dot');
  if (dot && NETWORK !== 'testnet') dot.textContent = `Live — syncing with Monad ${NETWORK}`;
}

/* ═══ Router ═══ */
async function init() {
  // ?network=testnet|mainnet in the URL overrides sessionStorage. Lets users
  // share deep links like /graph.html?network=mainnet and land on the right
  // data. Persist it so in-page toggles still behave consistently afterwards.
  const qnet = new URLSearchParams(location.search).get('network');
  if (qnet === 'testnet' || qnet === 'mainnet') {
    NETWORK = qnet;
    sessionStorage.setItem('mp_net', qnet);
  }

  await loadNames();
  // Normalize so /blocks and /blocks.html both dispatch to loadBlocks().
  const path = _normalizePath(window.location.pathname);

  initNetSwitch();

  if (path === '/' || path === '/index') {
    loadDashboard();
    setInterval(loadDashboard, 30000);
    // Live ticker / Latest Block refresh — far more frequent than full
    // dashboard so users don't see ~70 blocks "skip" between 30 s ticks.
    setInterval(refreshLiveTicker, 2000);
  } else if (path === '/blocks') {
    loadBlocks();
    initBlocksControls();
  } else if (path === '/validators') {
    loadValidators();
    initSearch();
    document.querySelectorAll('.period-btn').forEach(btn => {
      btn.addEventListener('click', () => {
        document.querySelectorAll('.period-btn').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        loadValidators(btn.dataset.period);
      });
    });
    document.querySelectorAll('.sort-btn').forEach(btn => {
      btn.addEventListener('click', () => {
        document.querySelectorAll('.sort-btn').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        _sortBy = btn.dataset.sort;
        const sorted = [..._allValidators];
        if (_sortBy === 'health') {
          sorted.sort((a, b) => (b.health_score || 0) - (a.health_score || 0));
        } else if (_sortBy === 'uptime') {
          const u = v => (v.health_components && v.health_components.uptime) ?? -1;
          sorted.sort((a, b) => u(b) - u(a));
        } else {
          sorted.sort((a, b) => b.blocks_proposed - a.blocks_proposed);
        }
        renderValidators(sorted);
      });
    });
  } else if (path === '/gas') {
    loadGas();
  } else if (path === '/alerts') {
    loadAlerts();
    initAlertsControls();
    setInterval(loadAlerts, 15000);
  } else if (path === '/governance') {
    initGovernanceControls();
    loadGovernance();
    // Refresh listing every 60s — captures new MIPs/edits between scraper runs.
    setInterval(loadGovernance, 60000);
  } else if (path === '/governance-mip') {
    if (typeof window.renderGovernanceMip === 'function') window.renderGovernanceMip();
    // Refresh detail every 60s — picks up new replies + edits + summary updates.
    setInterval(() => {
      if (typeof window.renderGovernanceMip === 'function') window.renderGovernanceMip();
    }, 60000);
  }

  // Nav highlight — match by normalized path so clean URLs highlight too.
  document.querySelectorAll('nav a').forEach(a => {
    const href = _normalizePath(a.getAttribute('href') || '');
    a.classList.toggle('active', href === path || (path === '/' && href === '/'));
  });
}

/* ═══ Governance ═══════════════════════════════════════════════════ */
// Governance is global (forum.monad.xyz is one source for both networks),
// so we don't pass network=... — the governance API is network-agnostic.

let _govStatusFilter = '';
let _govSort = 'updated';
let _govAll = [];

async function _govFetch(path) {
  try {
    const r = await fetch(API + path);
    if (!r.ok) throw new Error(r.status);
    return await r.json();
  } catch (e) {
    console.error('Governance fetch error:', path, e);
    return null;
  }
}

function initGovernanceControls() {
  document.querySelectorAll('#gov-status-filter .period-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      document.querySelectorAll('#gov-status-filter .period-btn').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      _govStatusFilter = btn.dataset.status || '';
      renderGovernance();
    });
  });
  document.querySelectorAll('.sort-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      document.querySelectorAll('.sort-btn').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      _govSort = btn.dataset.sort || 'updated';
      loadGovernance();
    });
  });
}

function _statusPill(s) {
  return `<span class="status-pill" data-pill="${esc(s)}">${esc(s)}</span>`;
}

function _activationBadge(ai) {
  if (!ai || !ai.fork) return '';
  // Prefer mainnet activation date for the user-facing label; fall back to
  // testnet if mainnet hasn't fired yet (forks usually ship to testnet first).
  const ts = ai.mainnet_activation_ts || ai.testnet_activation_ts;
  if (!ts) {
    return `<span class="status-pill" data-pill="Activated" style="margin-left:6px">${esc(ai.fork)}</span>`;
  }
  const d = new Date(ts * 1000);
  const months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
  const label = `${esc(ai.fork)} · ${d.getUTCDate()} ${months[d.getUTCMonth()]} ${d.getUTCFullYear()}`;
  const tip = `Activated in ${ai.fork} on ${ai.mainnet_activation_ts ? 'mainnet' : 'testnet'} (${d.toISOString().slice(0,10)})`
            + (ai.source_url ? ` — see ${ai.source_url}` : '');
  return `<span class="status-pill activation-badge" data-pill="Activated" title="${esc(tip)}" style="margin-left:6px;background:rgba(120,200,255,0.12);color:var(--cyan,#78c8ff);border-color:rgba(120,200,255,0.4)">${label}</span>`;
}
window._activationBadge = _activationBadge;

function _govMipLink(m, label) {
  const href = `/governance-mip.html?id=${m.topic_id}`;
  return `<a href="${href}" class="addr">${label}</a>`;
}

async function loadGovernance() {
  const data = await _govFetch(`/governance/list?sort=${encodeURIComponent(_govSort)}`);
  if (!data) return;
  _govAll = data;
  renderGovernance();
}

function renderGovernance() {
  const tbody = document.querySelector('#governance-table tbody');
  const meta = document.getElementById('gov-meta');
  if (!tbody) return;
  const data = _govStatusFilter
    ? _govAll.filter(m => m.status === _govStatusFilter)
    : _govAll;
  if (meta) {
    meta.textContent = `${data.length} of ${_govAll.length} proposal${_govAll.length === 1 ? '' : 's'}`;
  }
  if (!data.length) {
    tbody.innerHTML = '<tr><td colspan="8" style="text-align:center;color:var(--text-dim);padding:32px">No proposals match this filter</td></tr>';
    return;
  }
  tbody.innerHTML = data.map((m, i) => {
    const num = m.mip_number != null
      ? `<span style="color:var(--purple-light);font-weight:600">MIP-${m.mip_number}</span>`
      : '<span style="color:var(--text-dim);font-style:italic">draft</span>';
    const lastEdited = m.forum_updated_at ? timeAgo(m.forum_updated_at) : '—';
    return `<tr class="fade-row" style="animation-delay:${Math.min(i * 30, 400)}ms">
      <td>${num}</td>
      <td>${_govMipLink(m, esc(m.title))}</td>
      <td>${_statusPill(m.status)}${_activationBadge(m.activation_info)}</td>
      <td style="color:var(--text-mid);font-family:var(--mono);font-size:11px">${esc(m.category || '—')}</td>
      <td style="color:var(--text-mid);font-family:var(--mono);font-size:11px">${esc(m.author_username || '—')}</td>
      <td style="font-variant-numeric:tabular-nums">${fmtNum(m.reply_count)}</td>
      <td style="font-variant-numeric:tabular-nums">${fmtNum(m.views)}</td>
      <td style="color:var(--text-dim);font-family:var(--mono);font-size:11px">${lastEdited}</td>
    </tr>`;
  }).join('');
}

async function loadGovernanceMip() {
  // Implemented in Step 4 — placeholder so router doesn't error
  if (typeof window.renderGovernanceMip === 'function') window.renderGovernanceMip();
}

document.addEventListener('DOMContentLoaded', init);
