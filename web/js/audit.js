// Network Audit page renderer.
//
// Hits /api/audit/network?network=... and lays out the report in sections.
// Network switch wires through the existing app.js NETWORK state.

(async function init() {
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", boot);
  } else {
    boot();
  }
})();

async function boot() {
  await loadAndRender();
  // Re-render when network switches.
  document.querySelectorAll(".net-btn").forEach((btn) => {
    btn.addEventListener("click", () => setTimeout(loadAndRender, 0));
  });
}

async function loadAndRender() {
  const root = document.getElementById("audit-content");
  if (!root) return;
  root.innerHTML = '<div class="audit-loading">Loading audit data…</div>';
  const data = await apiFetch("/audit/network");
  if (!data) {
    root.innerHTML = '<div class="audit-loading" style="color:var(--pink)">Failed to load audit data.</div>';
    return;
  }
  const html = [
    renderHero(data),
    renderGeo(data.geo),
    renderHosting(data.geo),
    renderSubnets(data.geo),
    renderAnonClusters(data.geo),
    renderStakeClusters(data.stake_clusters),
    renderRotation(data.rotation),
    renderPerformance(data.performance),
    renderFooter(data),
  ].join("");
  root.innerHTML = html;
}

function renderHero(d) {
  const g = d.geo || {};
  const HHI = g.country_hhi || 0;
  const hhiLabel = HHI < 1500 ? "competitive" : HHI < 2500 ? "moderate" : "concentrated";
  const hhiColor = HHI < 1500 ? "#4ade80" : HHI < 2500 ? "#FFAE45" : "#FF8EE4";
  return `
  <section class="audit-section">
    <div class="audit-grid">
      <div class="audit-stat">
        <div class="audit-stat-label">Validators with geo</div>
        <div class="audit-stat-value">${g.total_with_geo || 0}</div>
        <div class="audit-stat-sub">via peers.toml + GeoIP</div>
      </div>
      <div class="audit-stat">
        <div class="audit-stat-label">Country HHI</div>
        <div class="audit-stat-value" style="color:${hhiColor}">${HHI}</div>
        <div class="audit-stat-sub">${hhiLabel} · ${g.country_total || 0} countries</div>
      </div>
      <div class="audit-stat">
        <div class="audit-stat-label">ASN HHI</div>
        <div class="audit-stat-value">${g.asn_hhi || 0}</div>
        <div class="audit-stat-sub">${g.asn_total || 0} unique ASNs</div>
      </div>
      <div class="audit-stat">
        <div class="audit-stat-label">Datacenter share</div>
        <div class="audit-stat-value">${g.datacenter_pct || 0}%</div>
        <div class="audit-stat-sub">${g.datacenter_count || 0} of ${g.total_with_geo || 0}</div>
      </div>
      <div class="audit-stat">
        <div class="audit-stat-label">/24 subnet collisions</div>
        <div class="audit-stat-value">${g.shared24_count || 0}</div>
        <div class="audit-stat-sub">${g.shared24_validators || 0} validators co-located</div>
      </div>
      <div class="audit-stat">
        <div class="audit-stat-label">Anonymous clusters</div>
        <div class="audit-stat-value" style="color:${(g.anonymous_clusters||[]).length ? '#FF8EE4' : 'var(--text)'}">${(g.anonymous_clusters || []).length}</div>
        <div class="audit-stat-sub">subnets where every member is unidentified</div>
      </div>
    </div>
  </section>
  `;
}

function renderGeo(g) {
  if (!g || !g.country_top) return "";
  const max = g.country_top[0]?.pct || 1;
  const rows = g.country_top.map((c) => {
    const w = (c.pct / max) * 100;
    return `<div class="bar-row">
      <div class="bar-label">${esc(c.country)}</div>
      <div class="bar-track"><div class="bar-fill" style="width:${w}%"></div></div>
      <div class="bar-value">${c.count} (${c.pct}%)</div>
    </div>`;
  }).join("");
  return `
  <section class="data-section audit-section">
    <div class="section-title">Geographic distribution</div>
    <div style="margin-top:10px">${rows}</div>
  </section>
  `;
}

function renderHosting(g) {
  if (!g) return "";
  const isps = g.isp_top || [];
  const asns = g.asn_top || [];
  const max_isp = isps[0]?.pct || 1;
  const ispRows = isps.map((i) => {
    const w = (i.pct / max_isp) * 100;
    return `<div class="bar-row">
      <div class="bar-label">${esc(truncate(i.isp, 30))}</div>
      <div class="bar-track"><div class="bar-fill" style="width:${w}%"></div></div>
      <div class="bar-value">${i.count} (${i.pct}%)</div>
    </div>`;
  }).join("");
  const asnPills = asns.map((a) => `<span class="pill"><strong>${esc(a.asn)}</strong> ${a.count} (${a.pct}%)</span>`).join("");
  return `
  <section class="data-section audit-section">
    <div class="section-title">Hosting concentration</div>
    <div style="margin:10px 0 16px">${ispRows}</div>
    <div style="font-size:11px;color:var(--text-dim);text-transform:uppercase;letter-spacing:1px;margin-bottom:6px">Top ASNs</div>
    <div class="pill-row">${asnPills}</div>
  </section>
  `;
}

function renderSubnets(g) {
  if (!g || !g.shared24_top) return "";
  const top = g.shared24_top.slice(0, 8);
  if (!top.length) return "";
  const rows = top.map((s) => {
    const flag = s.anonymous === s.count ? '<span style="color:#FF8EE4">⚠ all anonymous</span>' : (s.anonymous > 0 ? `<span style="color:#FFAE45">${s.anonymous}/${s.count} anon</span>` : "");
    return `<tr>
      <td><strong style="color:var(--text);font-family:var(--mono)">${esc(s.subnet)}</strong></td>
      <td>${s.count} validators</td>
      <td>${esc(truncate(s.isp || '?', 24))}</td>
      <td>${esc(s.city || '?')}, ${esc(s.country || '?')}</td>
      <td>${flag}</td>
    </tr>`;
  }).join("");
  const hint16 = (g.shared16_top || []).slice(0, 5).map((s) => `<span class="pill"><strong>${esc(s.subnet)}</strong> ${s.count}</span>`).join("");
  return `
  <section class="data-section audit-section">
    <div class="section-title">Co-located validators (/24 subnets)</div>
    <div style="overflow-x:auto;margin-top:10px">
      <table class="audit-table">
        <thead><tr><th>Subnet</th><th>Members</th><th>ISP</th><th>Location</th><th>Note</th></tr></thead>
        <tbody>${rows}</tbody>
      </table>
    </div>
    ${hint16 ? `<div style="margin-top:14px;font-size:11px;color:var(--text-dim);text-transform:uppercase;letter-spacing:1px">/16 hotspots</div><div class="pill-row">${hint16}</div>` : ""}
  </section>
  `;
}

function renderAnonClusters(g) {
  const clusters = g?.anonymous_clusters || [];
  if (!clusters.length) return "";
  const cards = clusters.map((c) => `
    <div class="cluster-card" style="border-color:rgba(255,142,228,0.25);background:rgba(255,142,228,0.04)">
      <div class="cluster-head">
        <span class="cluster-subnet">${esc(c.subnet)} → ${c.count} anonymous</span>
        <span class="cluster-meta">${esc(truncate(c.isp || '?', 30))} · ${esc(c.city || '?')}, ${esc(c.country || '?')}</span>
      </div>
      <div class="cluster-members">${c.members.map((m) => esc(m.ip)).join(" · ")}</div>
    </div>
  `).join("");
  return `
  <section class="data-section audit-section">
    <div class="section-title">Anonymous-cluster signals</div>
    <div class="audit-flag">
      <div class="audit-flag-title">⚠ Possible single-operator multi-validator</div>
      A subnet where every member is unidentified (no entry in upstream <code>monad-developers/validator-info</code>) is suggestive of one operator running multiple validators behind anonymity, or a shared "validator-as-a-service" provider. Not a definitive sybil signal — but worth scrutiny.
    </div>
    ${cards}
  </section>
  `;
}

function renderStakeClusters(s) {
  const clusters = s?.clusters || [];
  if (!clusters.length) return "";
  const cards = clusters.map((c) => `
    <div class="cluster-card">
      <div class="cluster-head">
        <span class="cluster-subnet">${esc(shortAddr ? shortAddr(c.auth) : c.auth)} controls ${c.val_count} validators</span>
        <span class="cluster-meta">total ${fmtNum ? fmtNum(c.total_stake_mon) : c.total_stake_mon} MON</span>
      </div>
      <div class="cluster-members">${c.members.map((m) => `val#${m.val_id} ${m.name ? '(' + esc(m.name) + ')' : ''} — ${fmtNum ? fmtNum(m.stake_mon) : m.stake_mon} MON`).join(" · ")}</div>
    </div>
  `).join("");
  return `
  <section class="data-section audit-section">
    <div class="section-title">Hidden stake concentration (shared-auth wallets)</div>
    <div style="font-size:12px;color:var(--text-mid);margin-bottom:10px">When one auth address registers multiple val_ids, on-chain stake is split per val_id but effectively under one wallet's control. Most explorers display the sum once or hide these clusters entirely.</div>
    ${cards}
  </section>
  `;
}

function renderRotation(r) {
  if (!r || !r.applicable) return "";
  const daily = r.daily || [];
  if (!daily.length) return "";
  const maxDay = Math.max(...daily.map((d) => Math.max(d.in, d.out)));
  const dailyBars = daily.map((d) => {
    const wIn = (d.in / maxDay) * 100;
    const wOut = (d.out / maxDay) * 100;
    return `<div style="display:flex;align-items:center;gap:8px;font-size:11px;font-family:var(--mono);padding:2px 0">
      <span style="min-width:90px;color:var(--text-dim)">${d.date}</span>
      <span style="min-width:130px"><span style="color:#4ade80">+${d.in}</span> / <span style="color:#FF8EE4">−${d.out}</span></span>
      <span style="flex:1;display:flex;align-items:center;height:10px;gap:1px">
        <span style="background:#4ade80;height:100%;width:${wIn / 2}%;border-radius:2px 0 0 2px"></span>
        <span style="background:#FF8EE4;height:100%;width:${wOut / 2}%;border-radius:0 2px 2px 0"></span>
      </span>
      <span style="color:var(--text-dim);min-width:60px;text-align:right">${d.distinct_vals} vids</span>
    </div>`;
  }).join("");

  const hourly = r.hourly_distribution || [];
  const maxHour = Math.max(1, ...hourly.map((h) => h.events));
  const hourCells = Array.from({ length: 24 }, (_, h) => {
    const ev = hourly.find((x) => x.hour === h)?.events || 0;
    const intensity = ev / maxHour;
    const bg = intensity > 0 ? `rgba(110,84,255,${0.15 + intensity * 0.8})` : "rgba(110,84,255,0.04)";
    return `<div style="flex:1;text-align:center;padding:6px 0;background:${bg};border-radius:3px;font-size:9px;color:${intensity > 0.5 ? '#fff' : 'var(--text-dim)'};font-family:var(--mono);min-width:18px" title="${h}:00 UTC — ${ev} events">${h}</div>`;
  }).join("");

  return `
  <section class="data-section audit-section">
    <div class="section-title">Foundation VDP rotation pattern (testnet)</div>
    <div style="font-size:12px;color:var(--text-mid);margin-bottom:14px">
      Detected pattern: ~32 in / 32 out per day from rotation pool of <strong>${r.rotation_pool_size}</strong> validators (≥1.9M MON Foundation undelegate threshold).
    </div>
    <div style="font-size:11px;color:var(--text-dim);text-transform:uppercase;letter-spacing:1px;margin:14px 0 8px">Daily in/out — last 14 days</div>
    ${dailyBars}
    <div style="font-size:11px;color:var(--text-dim);text-transform:uppercase;letter-spacing:1px;margin:24px 0 8px">Time-of-day distribution (UTC)</div>
    <div style="display:flex;gap:2px;align-items:stretch">${hourCells}</div>
    <div style="font-size:10px;color:var(--text-dim);margin-top:8px">Each cell is one UTC hour. Darker = more rotation events fired in that hour over the 14d window.</div>
  </section>
  `;
}

function renderPerformance(p) {
  if (!p || !p.applicable) return "";
  const b = p.buckets || {};
  const total = Object.values(b).reduce((a, v) => a + v, 0) || 1;
  const segments = [
    { label: "0 blocks", value: b["0"] || 0, color: "#FF8EE4" },
    { label: "1-999", value: b["1-999"] || 0, color: "#FFAE45" },
    { label: "1000-2999", value: b["1000-2999"] || 0, color: "#85E6FF" },
    { label: "3000-4999", value: b["3000-4999"] || 0, color: "#a78bfa" },
    { label: "5000+", value: b["5000+"] || 0, color: "#4ade80" },
  ];
  const segs = segments.map((s) => {
    const w = (s.value / total) * 100;
    if (w < 0.5) return "";
    return `<div class="histogram-segment" style="background:${s.color};width:${w}%" title="${s.label}: ${s.value}">${s.value}</div>`;
  }).join("");
  const legend = segments.map((s) => `<span><span class="histogram-legend-dot" style="background:${s.color}"></span>${s.label}</span>`).join("");

  const zeroList = (p.zero_blocks_named || []).slice(0, 24);
  const zeroPills = zeroList.map((it) => `<span class="pill" title="val_id ${it.val_id}">${esc(it.name)}</span>`).join("");

  const top = (p.top_performers || []).slice(0, 8);
  const topRows = top.map((it) => `<tr>
    <td><strong style="color:var(--text)">${esc(it.name || `val#${it.val_id}`)}</strong></td>
    <td style="color:var(--text-dim)">${it.val_id}</td>
    <td style="text-align:right;color:#4ade80;font-weight:600">${fmtNum ? fmtNum(it.blocks_7d) : it.blocks_7d}</td>
  </tr>`).join("");

  return `
  <section class="data-section audit-section">
    <div class="section-title">VDP rotation pool — 7-day block production</div>
    <div style="font-size:12px;color:var(--text-mid);margin-bottom:10px">
      Pool size <strong>${p.pool_size}</strong>. Distribution is <strong>bimodal</strong>: validators are either in the active set producing thousands of blocks, or completely idle. The middle is empty.
    </div>
    <div class="histogram-bar">${segs}</div>
    <div class="histogram-legend">${legend}</div>

    ${zeroList.length ? `
      <div style="font-size:11px;color:var(--text-dim);text-transform:uppercase;letter-spacing:1px;margin:24px 0 8px">Named operators stuck at 0 blocks (last 7d)</div>
      <div class="audit-flag">
        <div class="audit-flag-title">⚠ Possible rotation-fairness issue</div>
        ${zeroList.length} VDP-enrolled named operators produced zero blocks in the last 7 days. They've been in the rotation pool for weeks but the algorithm consistently keeps them out of the active set. Either they are queued behind others by design or there's an ordering bias worth checking.
      </div>
      <div class="pill-row" style="margin-top:10px">${zeroPills}</div>
    ` : ""}

    ${topRows ? `
      <div style="font-size:11px;color:var(--text-dim);text-transform:uppercase;letter-spacing:1px;margin:24px 0 8px">Top 8 performers</div>
      <div style="overflow-x:auto"><table class="audit-table"><thead><tr><th>Validator</th><th>val_id</th><th style="text-align:right">Blocks (7d)</th></tr></thead><tbody>${topRows}</tbody></table></div>
    ` : ""}
  </section>
  `;
}

function renderFooter(d) {
  return `
  <section class="audit-section" style="text-align:center;padding-top:20px;border-top:1px solid rgba(110,84,255,0.08);font-size:11px;color:var(--text-dim);font-family:var(--mono)">
    Generated ${new Date(d.generated_at).toLocaleString()} · cached 5 min · sources: peers.toml (P2P-signed), staking precompile, ip-api.com
  </section>
  `;
}

function truncate(s, n) {
  if (!s) return "";
  return s.length > n ? s.slice(0, n - 1) + "…" : s;
}
