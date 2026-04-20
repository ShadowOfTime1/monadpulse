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
  _namesLoaded = true;
}

function valName(addr) {
  if (!addr) return null;
  return _nameMap[addr.toLowerCase()] || null;
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

    const btVal = v.avg_block_time_ms != null ? v.avg_block_time_ms + ' ms' : '—';
    const btClass = v.avg_block_time_ms != null && (v.avg_block_time_ms > 450 || (v.avg_block_time_ms < 300 && v.avg_block_time_ms > 0)) ? ' style="color:var(--pink,#FF8EE4)"' : '';
    const blocksClass = v.blocks_proposed < 50 ? ' style="color:var(--orange,#FFAE45)"' : '';

    // Uptime column — raw uptime_score from health_components (0-100, new
    // formula = actual vs expected blocks since first-active). Color-coded
    // so outages stand out at a glance.
    const up = v.health_components && v.health_components.uptime;
    const upStr = up == null ? '—' : up.toFixed(1) + '%';
    // VDP 4-week uptime target is 98% — green only when above.
    const upCol = up == null ? 'var(--text-dim)'
      : up >= 98 ? '#4ade80' : up >= 50 ? '#FFAE45' : '#FF8EE4';
    const upCell = `<span style="color:${upCol};font-variant-numeric:tabular-nums;font-weight:600">${upStr}</span>`;

    return `<tr class="fade-row${hasAnomaly ? ' anomaly-row' : ''}" style="animation-delay:${Math.min(i * 20, 300)}ms">
      <td><span class="rank">${rank}</span></td>
      <td>${anomalyIcon}${validatorLink(v.address)}</td>
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

  // Merge health scores into validator data
  const scoreMap = {};
  if (healthData) {
    healthData.forEach(h => { scoreMap[h.validator_id] = h; });
  }
  data.forEach(v => {
    const h = scoreMap[v.address];
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

  if (data.length > 0) {
    // Chart: top 15 by health score
    const top = data.filter(v => v.health_score != null).slice(0, 15);
    makeChart('chart-validators', 'bar', top.map(v => shortAddr(v.address)), [{
      label: 'Health Score',
      data: top.map(v => v.health_score),
      backgroundColor: top.map(v => scoreColor(v.health_score) + '50'),
      hoverBackgroundColor: top.map(v => scoreColor(v.health_score) + '90'),
      borderColor: top.map(v => scoreColor(v.health_score)),
      borderWidth: 1,
      borderRadius: 6,
      borderSkipped: false,
    }]);
  }

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

function renderAlertItem(a) {
  const title = linkifyAddresses(esc(a.title));
  const desc = a.description ? linkifyAddresses(esc(a.description)) : '';
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

  const path = window.location.pathname;
  if (path === '/' || path === '/index.html') loadDashboard();
  else if (path === '/blocks.html') loadBlocks();
  else if (path === '/validators.html') loadValidators();
  else if (path === '/map.html') { if (typeof buildMap === 'function') buildMap(); }
  else if (path === '/gas.html') loadGas();
  else if (path === '/alerts.html') { _alertsAll = []; loadAlerts(); }
  else if (path === '/stake.html') { if (typeof loadStake === 'function') loadStake(); }
  else if (path === '/validator.html') { if (typeof loadValidator === 'function') loadValidator(); }
  // Pages whose loader is declared inline in the HTML — expose via window.*
  // so reloadPage can reach them from app.js scope reliably.
  else if (path === '/graph.html') { if (typeof window.renderGraph === 'function') window.renderGraph(); }
  else if (path === '/clusters.html') { if (typeof window.loadClusters === 'function') window.loadClusters(); }
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
  const path = window.location.pathname;

  initNetSwitch();

  if (path === '/' || path === '/index.html') {
    loadDashboard();
    setInterval(loadDashboard, 30000);
  } else if (path === '/blocks.html') {
    loadBlocks();
    initBlocksControls();
  } else if (path === '/validators.html') {
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
  } else if (path === '/gas.html') {
    loadGas();
  } else if (path === '/alerts.html') {
    loadAlerts();
    initAlertsControls();
    setInterval(loadAlerts, 15000);
  }

  // Nav highlight
  document.querySelectorAll('nav a').forEach(a => {
    a.classList.toggle('active', a.getAttribute('href') === path || (path === '/' && a.getAttribute('href') === '/'));
  });
}

document.addEventListener('DOMContentLoaded', init);
