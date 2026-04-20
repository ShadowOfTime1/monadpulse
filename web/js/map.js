/* MonadPulse — Network Map with city clusters, network-aware */

const KNOWN_VALIDATORS = {
  testnet: [
    { name: 'shadowoftime', city: 'Sydney', lat: -33.87, lon: 151.21, region: 'Oceania' },
    { name: 'GalaxyDigital', city: 'New York', lat: 40.71, lon: -74.01, region: 'North America' },
    { name: 'Figment', city: 'Toronto', lat: 43.65, lon: -79.38, region: 'North America' },
    { name: 'Coinbase Cloud', city: 'San Francisco', lat: 37.77, lon: -122.42, region: 'North America' },
    { name: 'Chorus One', city: 'Zug', lat: 47.17, lon: 8.52, region: 'Europe' },
    { name: 'P2P.org', city: 'Amsterdam', lat: 52.37, lon: 4.90, region: 'Europe' },
    { name: 'Everstake', city: 'Kyiv', lat: 50.45, lon: 30.52, region: 'Europe' },
    { name: 'HashKey Cloud', city: 'Hong Kong', lat: 22.32, lon: 114.17, region: 'Asia' },
    { name: 'Luganodes', city: 'Lugano', lat: 46.00, lon: 8.95, region: 'Europe' },
    { name: 'Blockdaemon', city: 'Los Angeles', lat: 34.05, lon: -118.24, region: 'North America' },
    { name: 'Staking Facilities', city: 'Munich', lat: 48.14, lon: 11.58, region: 'Europe' },
    { name: 'Kiln', city: 'Paris', lat: 48.86, lon: 2.35, region: 'Europe' },
    { name: 'InfStones', city: 'Palo Alto', lat: 37.44, lon: -122.14, region: 'North America' },
    { name: 'Allnodes', city: 'Los Angeles', lat: 34.05, lon: -118.24, region: 'North America' },
    { name: 'StakingCabin', city: 'Dubai', lat: 25.20, lon: 55.27, region: 'Asia' },
    { name: 'Stakely', city: 'Madrid', lat: 40.42, lon: -3.70, region: 'Europe' },
    { name: 'DSRV', city: 'Seoul', lat: 37.57, lon: 126.98, region: 'Asia' },
    { name: 'Nansen', city: 'Singapore', lat: 1.29, lon: 103.85, region: 'Asia' },
    { name: 'Validation Cloud', city: 'Zug', lat: 47.17, lon: 8.52, region: 'Europe' },
    { name: 'CertHum', city: 'New York', lat: 40.71, lon: -74.01, region: 'North America' },
    { name: 'DeSpread', city: 'Seoul', lat: 37.55, lon: 127.00, region: 'Asia' },
    { name: 'Nodes.Guru', city: 'Buenos Aires', lat: -34.60, lon: -58.38, region: 'South America' },
    { name: 'OnNode', city: 'Tokyo', lat: 35.68, lon: 139.69, region: 'Asia' },
    { name: 'Needlecast', city: 'London', lat: 51.51, lon: -0.13, region: 'Europe' },
    { name: 'Stakecraft', city: 'Chișinău', lat: 47.00, lon: 28.86, region: 'Europe' },
    { name: 'JETSTAKE', city: 'Moscow', lat: 55.76, lon: 37.62, region: 'Europe' },
    { name: 'snoopfear|PON', city: 'Warsaw', lat: 52.23, lon: 21.01, region: 'Europe' },
    { name: 'GO2Pro', city: 'Moscow', lat: 55.76, lon: 37.62, region: 'Europe' },
    { name: 'MMS', city: 'Saint Petersburg', lat: 59.93, lon: 30.32, region: 'Europe' },
    { name: 'OshVanK', city: 'Istanbul', lat: 41.01, lon: 28.98, region: 'Europe' },
  ],
  mainnet: [
    { name: 'GalaxyDigital', city: 'New York', lat: 40.71, lon: -74.01, region: 'North America' },
    { name: 'Figment', city: 'Toronto', lat: 43.65, lon: -79.38, region: 'North America' },
    { name: 'Coinbase Cloud', city: 'San Francisco', lat: 37.77, lon: -122.42, region: 'North America' },
    { name: 'Chorus One', city: 'Zug', lat: 47.17, lon: 8.52, region: 'Europe' },
    { name: 'Kiln', city: 'Paris', lat: 48.86, lon: 2.35, region: 'Europe' },
    { name: 'Blockdaemon', city: 'Los Angeles', lat: 34.05, lon: -118.24, region: 'North America' },
    { name: 'Everstake', city: 'Kyiv', lat: 50.45, lon: 30.52, region: 'Europe' },
    { name: 'P2P.org', city: 'Amsterdam', lat: 52.37, lon: 4.90, region: 'Europe' },
    { name: 'Luganodes', city: 'Lugano', lat: 46.00, lon: 8.95, region: 'Europe' },
    { name: 'DSRV', city: 'Seoul', lat: 37.57, lon: 126.98, region: 'Asia' },
    { name: 'Validation Cloud', city: 'Zug', lat: 47.17, lon: 8.52, region: 'Europe' },
    { name: 'Staking Facilities', city: 'Munich', lat: 48.14, lon: 11.58, region: 'Europe' },
    { name: 'InfStones', city: 'Palo Alto', lat: 37.44, lon: -122.14, region: 'North America' },
    { name: 'HashKey Cloud', city: 'Hong Kong', lat: 22.32, lon: 114.17, region: 'Asia' },
    { name: 'Allnodes', city: 'Los Angeles', lat: 34.05, lon: -118.24, region: 'North America' },
    { name: 'Stakely', city: 'Madrid', lat: 40.42, lon: -3.70, region: 'Europe' },
  ],
};

// NOTE (2026-04-20): CITY_POOL + REGION_TARGETS removed. They were used to
// algorithmically distribute unverified validators across the map to make it
// look fuller. That's misleading data. Now we only plot validators whose
// location we've verified (KNOWN_VALIDATORS above), and the legend
// explicitly shows how many remain without public geo.

const UNDER_THRESHOLD = 0.05;
const OVER_THRESHOLD = 0.30;

const REGION_COLOR = {
  under: '#10b981',
  normal: '#6E54FF',
  over: '#FF7849',
};

const CATEGORY_LABEL = {
  under: 'underrepresented',
  normal: 'balanced',
  over: 'oversaturated',
};

let _map = null;
let _markersLayer = null;
let _legendControl = null;

async function buildMap() {
  // Real validator count from API — used only for the legend total.
  const summary = await apiFetch('/dashboard/summary');
  const totalValidators = summary?.epoch?.validator_count || summary?.stats_24h?.active_validators || 0;
  // Manually-verified geography now comes from /api/validators/geo
  // (backed by validator_geo_{network}.json in the repo). Previously this
  // list was hardcoded in JS; moving it behind an endpoint makes the
  // source visible and editable without a frontend deploy.
  const geoResp = await apiFetch('/validators/geo');
  const known = (geoResp && geoResp.validators) || KNOWN_VALIDATORS[NETWORK] || [];

  // Clear previous markers
  if (_markersLayer) _markersLayer.clearLayers();
  if (_legendControl) _map.removeControl(_legendControl);

  // Build clusters from VERIFIED validators only. We used to pad the map
  // with algorithmically-distributed anonymous points (CITY_POOL +
  // REGION_TARGETS), flagged "estimated", but that looked like real data
  // at a glance. Honest approach: only plot validators whose geo we've
  // verified; show the unknown count explicitly in the legend.
  const clusters = {};
  known.forEach(v => {
    if (!clusters[v.city]) clusters[v.city] = { lat: v.lat, lon: v.lon, region: v.region, city: v.city, validators: [] };
    clusters[v.city].validators.push(v.name);
  });

  // Region shares for category coloring
  const regionCounts = {};
  Object.values(clusters).forEach(c => {
    regionCounts[c.region] = (regionCounts[c.region] || 0) + c.validators.length;
  });
  const totalClusterCount = Object.values(regionCounts).reduce((a, b) => a + b, 0) || 1;
  const regionCategory = (region) => {
    const share = (regionCounts[region] || 0) / totalClusterCount;
    if (share < UNDER_THRESHOLD) return 'under';
    if (share > OVER_THRESHOLD) return 'over';
    return 'normal';
  };

  const maxCount = Math.max(...Object.values(clusters).map(c => c.validators.length));

  Object.values(clusters).forEach(cluster => {
    const count = cluster.validators.length;
    const named = cluster.validators.filter(v => v);
    const anonymous = count - named.length;
    const minSize = 16, maxSize = 44;
    const size = Math.round(minSize + (count / maxCount) * (maxSize - minSize));
    const hasShadow = named.includes('shadowoftime');
    const category = regionCategory(cluster.region);
    const color = hasShadow ? '#14b8a6' : REGION_COLOR[category];

    const icon = L.divIcon({
      className: '',
      html: `<div style="
        width:${size}px;height:${size}px;border-radius:50%;
        background:radial-gradient(circle at 35% 35%, ${color}cc, ${color}50);
        box-shadow:0 0 ${size}px ${color}40, 0 0 ${size/2}px ${color}20;
        border:2px solid rgba(255,255,255,0.25);
        display:flex;align-items:center;justify-content:center;
        font-family:'Roboto Mono',monospace;font-size:${size > 28 ? 12 : 10}px;
        font-weight:700;color:#fff;cursor:pointer;
        animation:pulse 3s ease-in-out infinite;
      ">${count}</div>`,
      iconSize: [size, size],
      iconAnchor: [size/2, size/2],
    });

    let popup = `<div class="val-popup">`;
    popup += `<div style="font-size:13px;font-weight:700;color:#DDD7FE;margin-bottom:6px">${esc(cluster.city)}</div>`;
    const catColor = REGION_COLOR[category];
    const catLabel = CATEGORY_LABEL[category];
    popup += `<div style="color:#6B6580;font-size:11px;margin-bottom:4px">${esc(cluster.region)} — ${count} validator${count > 1 ? 's' : ''}</div>`;
    popup += `<div style="color:${catColor};font-size:10px;margin-bottom:8px;letter-spacing:0.5px;text-transform:uppercase"><span style="display:inline-block;width:6px;height:6px;border-radius:50%;background:${catColor};box-shadow:0 0 6px ${catColor}80;margin-right:5px;vertical-align:middle"></span>${catLabel}</div>`;
    named.forEach(n => {
      const c = n === 'shadowoftime' ? '#14b8a6' : '#6E54FF';
      popup += `<div style="padding:3px 0;color:#EEEDF5;font-size:11px"><span style="color:${c};margin-right:4px">&#x25CF;</span>${esc(n)}</div>`;
    });
    popup += `</div>`;

    const marker = L.marker([cluster.lat, cluster.lon], { icon });
    // Attach metadata so the markercluster iconCreateFunction can aggregate
    // counts and pick the dominant category/color for the cluster bubble.
    marker._mp = { count, category, hasShadow, region: cluster.region };
    marker.bindPopup(popup, { className: 'dark-popup', maxWidth: 240 });
    _markersLayer.addLayer(marker);
  });

  _legendControl = L.control({ position: 'bottomleft' });
  _legendControl.onAdd = function() {
    const div = L.DomUtil.create('div', 'map-legend');
    const netLabel = NETWORK.charAt(0).toUpperCase() + NETWORK.slice(1);

    const categoryRow = (cat) => {
      const col = REGION_COLOR[cat];
      return `<div style="display:flex;align-items:center;gap:6px;padding:1px 0;font-size:10px">
        <span style="width:8px;height:8px;border-radius:50%;background:${col};box-shadow:0 0 6px ${col}80;flex-shrink:0"></span>
        <span style="color:#9994AE;text-transform:uppercase;letter-spacing:0.5px">${CATEGORY_LABEL[cat]}</span>
      </div>`;
    };

    const regionRow = ([r, c]) => {
      const cat = regionCategory(r);
      const col = REGION_COLOR[cat];
      const share = Math.round((c / totalClusterCount) * 100);
      return `<div style="display:flex;align-items:center;gap:6px;padding:2px 0">
        <span style="width:6px;height:6px;border-radius:50%;background:${col};box-shadow:0 0 4px ${col}80;flex-shrink:0"></span>
        <span style="color:#9994AE">${esc(r)}</span>
        <span style="color:#6B6580;font-size:10px">${share}%</span>
        <span style="color:#EEEDF5;font-weight:600;margin-left:auto">${c}</span>
      </div>`;
    };

    const orderedRegions = Object.entries(regionCounts).sort((a, b) => b[1] - a[1]);

    const plotted = known.length;
    const unknownGeo = Math.max(0, totalValidators - plotted);
    div.innerHTML =
      `<div style="color:#DDD7FE;font-weight:600;margin-bottom:4px;font-size:12px">${netLabel} — ${totalValidators} active validators</div>` +
      `<div style="color:#6B6580;font-size:10px;margin-bottom:8px;line-height:1.5">` +
        `${plotted} with verified location<br>` +
        `${unknownGeo} without public geo data` +
      `</div>` +
      `<div style="border-top:1px solid rgba(110,84,255,0.15);padding-top:6px;margin-bottom:8px">` +
        categoryRow('under') + categoryRow('normal') + categoryRow('over') +
      `</div>` +
      `<div style="border-top:1px solid rgba(110,84,255,0.15);padding-top:6px">` +
        orderedRegions.map(regionRow).join('') +
      `</div>`;
    return div;
  };
  _legendControl.addTo(_map);

  // Build full validator list: known (with location) + all from API (by address)
  buildValidatorList(known);
}

let _mapTableExpanded = false;
let _mapTableData = [];
let _mapFullList = [];

async function buildValidatorList(knownLocations) {
  // Fetch all active validators from API
  const apiValidators = await apiFetch('/validators/list?period=24h');
  if (!apiValidators) { renderMapTable(knownLocations); return; }

  // Build set of known names for dedup
  const knownNames = new Set(knownLocations.map(v => v.name.toLowerCase()));

  // Named validators with location — first priority
  const withLocation = knownLocations.map(v => ({
    name: v.name,
    city: v.city,
    region: v.region,
    lat: v.lat,
    lon: v.lon,
    hasLocation: true,
  }));
  withLocation.sort((a, b) => a.name.localeCompare(b.name));

  // API validators — add name from nameMap, mark those without known location.
  // Dedup by canonical name because /validators/list is per-miner-address
  // and one validator can rotate through several miner addrs on mainnet —
  // we don't want to count Backpack (3 miner rows) as 3 validators.
  const rest = [];
  const seenNames = new Set();
  apiValidators.forEach(v => {
    const name = valName(v.address);
    // Skip if already in known locations
    if (name && knownNames.has(name.toLowerCase())) return;
    // Skip duplicate miner rotations of the same named validator
    if (name) {
      const key = name.toLowerCase();
      if (seenNames.has(key)) return;
      seenNames.add(key);
    }

    rest.push({
      name: name || v.address,
      city: '—',
      region: '—',
      lat: null,
      lon: null,
      hasLocation: false,
      isAddress: !name,
    });
  });

  // Sort: named first alphabetically, then addresses numerically
  const named = rest.filter(v => !v.isAddress).sort((a, b) => a.name.localeCompare(b.name));
  const addresses = rest.filter(v => v.isAddress).sort((a, b) => a.name.localeCompare(b.name));

  const full = [...withLocation, ...named, ...addresses];
  _mapFullList = full;
  renderMapTable(full);
}

function renderMapTable(validators) {
  _mapTableData = validators;
  _mapTableExpanded = false;
  _renderMapRows();
}

function _renderMapRows() {
  const tbody = document.querySelector('#map-validators-table tbody');
  const btnWrap = document.getElementById('map-show-more-wrap');
  if (!tbody) return;

  const data = _mapTableData;
  if (!data.length) {
    tbody.innerHTML = '<tr><td colspan="3" style="text-align:center;color:var(--text-dim);padding:24px">No known locations</td></tr>';
    if (btnWrap) btnWrap.style.display = 'none';
    return;
  }

  const visible = _mapTableExpanded ? data : data.slice(0, 5);

  tbody.onclick = function(e) {
    const row = e.target.closest('.map-row');
    if (!row || !_map) return;
    _map.flyTo([parseFloat(row.dataset.lat), parseFloat(row.dataset.lon)], 6, { duration: 1 });
    document.getElementById('map-container')?.scrollIntoView({ behavior: 'smooth', block: 'center' });
  };

  tbody.innerHTML = visible.map((v, i) => {
    const isShadow = v.name === 'shadowoftime';
    const hasLoc = v.hasLocation;
    const isAddr = v.isAddress;

    let dotColor = '#6E54FF';
    if (isShadow) dotColor = '#14b8a6';
    else if (!hasLoc) dotColor = '#3d2e99';

    const nameStyle = isShadow ? 'color:#14b8a6;font-weight:600'
      : isAddr ? 'color:var(--text-dim);font-size:11px' : 'color:var(--text)';

    const displayName = isAddr ? shortAddr(v.name) : esc(v.name);
    const clickable = hasLoc ? `class="fade-row map-row" style="animation-delay:${Math.min(i * 15, 300)}ms;cursor:pointer" data-lat="${v.lat}" data-lon="${v.lon}"` : `class="fade-row" style="animation-delay:${Math.min(i * 15, 300)}ms"`;

    return `<tr ${clickable}>
      <td><span style="color:${dotColor};margin-right:6px;font-size:8px">&#x25CF;</span><span style="${nameStyle}">${displayName}</span></td>
      <td style="color:${hasLoc ? 'var(--text)' : 'var(--text-dim)'}">${esc(v.city)}</td>
      <td style="color:var(--text-dim)">${esc(v.region)}</td>
    </tr>`;
  }).join('');

  // Show/hide button
  if (btnWrap) {
    if (data.length <= 5) {
      btnWrap.style.display = 'none';
    } else {
      btnWrap.style.display = 'block';
      const btn = btnWrap.querySelector('.map-show-btn');
      if (btn) {
        if (_mapTableExpanded) {
          btn.textContent = 'Show less';
        } else {
          // Clarify how many of the listed validators are actually plotted
          // on the map above — the rest are known names without geo data.
          const withGeo = data.filter(v => v.hasLocation).length;
          const noGeo = data.length - withGeo;
          btn.textContent = noGeo > 0
            ? `Show all ${data.length} validators (${withGeo} on map · ${noGeo} without geo)`
            : `Show all ${data.length} validators`;
        }
      }
    }
  }
}

function initMapSearch() {
  const input = document.getElementById('map-search');
  if (!input) return;
  input.addEventListener('input', () => {
    const q = input.value.toLowerCase().trim();
    if (!q) { _mapTableData = _mapFullList; _mapTableExpanded = false; _renderMapRows(); return; }
    const src = _mapFullList;
    const filtered = src.filter(v =>
      v.name.toLowerCase().includes(q) ||
      v.city.toLowerCase().includes(q) ||
      v.region.toLowerCase().includes(q)
    );
    _mapTableData = filtered;
    _mapTableExpanded = true;
    _renderMapRows();
  });
}

// Global toggle for show more/less button
window._toggleMapTable = function() {
  _mapTableExpanded = !_mapTableExpanded;
  _renderMapRows();
};

function initMap() {
  const container = document.getElementById('map-container');
  if (!container) return;

  _map = L.map('map-container', {
    center: [20, 10], zoom: 2, minZoom: 2, maxZoom: 8,
    zoomControl: true, attributionControl: false,
  });

  L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
    subdomains: 'abcd', maxZoom: 19,
  }).addTo(_map);

  // If the markercluster plugin failed to load, fall back to a plain layerGroup
  // so the map still works (just without clustering).
  _markersLayer = (typeof L.markerClusterGroup === 'function') ? L.markerClusterGroup({
    maxClusterRadius: 45,
    spiderfyOnMaxZoom: true,
    showCoverageOnHover: false,
    zoomToBoundsOnClick: true,
    disableClusteringAtZoom: 5,
    iconCreateFunction: function(cluster) {
      const children = cluster.getAllChildMarkers();
      let sum = 0, hasShadow = false;
      const catWeights = { under: 0, normal: 0, over: 0 };
      children.forEach(m => {
        const meta = m._mp;
        if (!meta) return;
        sum += meta.count;
        if (meta.hasShadow) hasShadow = true;
        catWeights[meta.category] = (catWeights[meta.category] || 0) + meta.count;
      });
      const dominant = Object.entries(catWeights).sort((a, b) => b[1] - a[1])[0][0] || 'normal';
      const color = hasShadow ? '#14b8a6' : REGION_COLOR[dominant] || '#6E54FF';
      const size = Math.round(Math.min(60, 28 + Math.log2(sum + 1) * 4));
      const fontSize = size > 40 ? 13 : 11;
      return L.divIcon({
        className: 'mp-cluster-wrap',
        html: `<div class="mp-cluster" style="
          background:radial-gradient(circle at 35% 35%, ${color}cc, ${color}50);
          box-shadow:0 0 ${size}px ${color}40, 0 0 ${size/2}px ${color}20;
          font-size:${fontSize}px;
        ">${sum}</div>`,
        iconSize: [size, size],
      });
    },
  }) : L.layerGroup();
  _markersLayer.addTo(_map);
  buildMap();
  initMapSearch();

  // Listen for network switch
  document.querySelectorAll('.net-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      setTimeout(buildMap, 100); // rebuild after NETWORK changes in app.js
    });
  });
}

document.addEventListener('DOMContentLoaded', () => {
  initMap();
});
