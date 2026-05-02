// Network Audit page renderer.
//
// Hits /api/audit/network?network=... and lays out the report.
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
  // Order: verdict → performance (the lead story) → hero stats →
  // geo → hosting → subnets → anon clusters → stake clusters →
  // rotation → methodology footer.
  const html = [
    renderVerdict(data),
    renderPerformance(data.performance),
    renderHero(data),
    renderGeo(data.geo),
    renderHosting(data.geo),
    renderSubnets(data.geo),
    renderAnonClusters(data.geo),
    renderStakeClusters(data.stake_clusters),
    renderRotation(data.rotation),
    renderMethodology(data),
  ].join("");
  root.innerHTML = html;
}

// Hero verdict — plain-English summary with traffic-light tone.
// Tone now considers BOTH HHI and datacenter share so verdict can't be green
// when 60%+ of validators sit in commercial datacenters.
function renderVerdict(d) {
  const g = d.geo || {};
  const p = d.performance || {};
  const network = d.network || "testnet";
  const HHI = Math.max(g.country_hhi || 0, g.stake_country_hhi || 0);
  const dcPct = g.datacenter_pct || 0;

  let tone;
  if (HHI < 1500 && dcPct < 70) tone = { color: "#4ade80", label: "Healthy spread" };
  else if (HHI < 2500 && dcPct < 85) tone = { color: "#FFAE45", label: "Moderate concentration" };
  else tone = { color: "#FF8EE4", label: "High concentration" };

  const stuck = (p.zero_blocks_named || []).length;
  const stuckSentence = p.applicable && stuck > 0
    ? `<strong>${stuck} delegation-program operators</strong> have been in the rotation pool for 14+ days yet produced zero blocks in the last 7 — could be queue order or an algorithm bias worth raising.`
    : "";
  const subnets = g.shared24_count || 0;
  const subnetSentence = subnets
    ? `<strong>${g.shared24_validators}</strong> validators share data-center racks with at least one peer (${subnets} co-located /24 subnets).`
    : "";

  return `
  <section class="audit-section verdict-band">
    <div class="verdict-tone" style="border-color:${tone.color}33;background:${tone.color}0a">
      <div class="verdict-label" style="color:${tone.color}">${tone.label}</div>
      <p class="verdict-text">
        Monad <strong>${network === "mainnet" ? "mainnet" : "testnet"}</strong> spans <strong>${g.registered_count || 0}</strong> registered validators across <strong>${g.country_total || 0}</strong> countries and <strong>${g.asn_total || 0}</strong> network operators. <strong>${dcPct}%</strong> of nodes sit in commercial datacenters.
      </p>
      ${subnetSentence || stuckSentence ? `<p class="verdict-text" style="margin-top:8px">${subnetSentence}${subnetSentence && stuckSentence ? " " : ""}${stuckSentence}</p>` : ""}
    </div>
  </section>
  `;
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
        <div class="audit-stat-label" title="Validators with an on-chain val_id (full nodes / RPC peers excluded)">Registered validators</div>
        <div class="audit-stat-value">${g.registered_count || 0}</div>
        <div class="audit-stat-sub">+ ${g.peers_only_count || 0} unregistered P2P peers</div>
      </div>
      <div class="audit-stat">
        <div class="audit-stat-label" title="Herfindahl-Hirschman Index. 0–1500: healthy spread. 2500+: concentrated.">Country diversity (HHI)</div>
        <div class="audit-stat-value" style="color:${hhiColor}">${HHI}</div>
        <div class="audit-stat-sub">by validator count · ${g.country_total || 0} countries</div>
        ${g.stake_country_hhi ? `<div class="audit-stat-sub" title="Same index but each validator is weighted by current stake. Heavier validators count more, which surfaces real consensus-security concentration.">stake-weighted: <strong style="color:${g.stake_country_hhi > HHI ? '#FFAE45' : '#4ade80'}">${g.stake_country_hhi}</strong></div>` : ""}
      </div>
      <div class="audit-stat">
        <div class="audit-stat-label" title="Same diversity index but over unique Autonomous System Numbers (network operators / hosting providers)">ASN diversity (HHI)</div>
        <div class="audit-stat-value">${g.asn_hhi || 0}</div>
        <div class="audit-stat-sub">by validator count · ${g.asn_total || 0} unique ASNs</div>
        ${g.stake_asn_hhi ? `<div class="audit-stat-sub" title="Same index but weighted by current stake — surfaces hidden hosting concentration when one provider hosts the heaviest validators.">stake-weighted: <strong style="color:${g.stake_asn_hhi > (g.asn_hhi || 0) ? '#FFAE45' : '#4ade80'}">${g.stake_asn_hhi}</strong></div>` : ""}
      </div>
      <div class="audit-stat">
        <div class="audit-stat-label" title="Servers hosted in commercial datacenters vs other (residential, business ISP)">Datacenter share</div>
        <div class="audit-stat-value">${g.datacenter_pct || 0}%</div>
        <div class="audit-stat-sub">${g.datacenter_count || 0} of ${g.registered_count || 0}</div>
      </div>
      <div class="audit-stat">
        <div class="audit-stat-label" title="/24 subnets (≈256 IPs, typically one rack) holding two or more validators">Co-located /24 subnets</div>
        <div class="audit-stat-value">${g.shared24_count || 0}</div>
        <div class="audit-stat-sub">${g.shared24_validators || 0} validators sharing racks</div>
      </div>
      <div class="audit-stat">
        <div class="audit-stat-label" title="/24 subnets where every member has no public entry in monad-developers/validator-info">Anonymous clusters</div>
        <div class="audit-stat-value" style="color:${(g.anonymous_clusters||[]).length ? '#FF8EE4' : 'var(--text)'}">${(g.anonymous_clusters || []).length}</div>
        <div class="audit-stat-sub">all members unidentified</div>
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
    <div style="font-size:12px;color:var(--text-mid);margin-bottom:6px">Top-10 countries by registered-validator count.</div>
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
    <div style="font-size:12px;color:var(--text-mid);margin-bottom:6px">Where validator servers physically live. ISP names are merged by ASN, so "Limestone Networks" and "Limestone Networks, Inc." count as one provider.</div>
    <div style="margin:10px 0 16px">${ispRows}</div>
    <div style="font-size:11px;color:var(--text-dim);text-transform:uppercase;letter-spacing:1px;margin-bottom:6px">Top ASNs (autonomous systems)</div>
    <div class="pill-row">${asnPills}</div>
  </section>
  `;
}

function renderSubnets(g) {
  if (!g || !g.shared24_top) return "";
  const top = g.shared24_top.slice(0, 8);
  if (!top.length) return "";
  const rows = top.map((s) => {
    // Anon flag is now ALWAYS in the subnet column (mobile-safe)
    const anonDot = s.anonymous === s.count
      ? '<span style="color:#FF8EE4;margin-right:6px" title="all members anonymous">●</span>'
      : (s.anonymous > 0 ? `<span style="color:#FFAE45;margin-right:6px" title="${s.anonymous} of ${s.count} anonymous">◐</span>` : '');
    return `<tr>
      <td>${anonDot}<strong style="color:var(--text);font-family:var(--mono)">${esc(s.subnet)}</strong></td>
      <td>${s.count} validators</td>
      <td>${esc(truncate(s.isp || '?', 24))}</td>
      <td>${esc(s.city || '?')}, ${esc(s.country || '?')}</td>
    </tr>`;
  }).join("");
  const hint16 = (g.shared16_top || []).slice(0, 5).map((s) => `<span class="pill"><strong>${esc(s.subnet)}</strong> ${s.count}</span>`).join("");
  return `
  <section class="data-section audit-section">
    <div class="section-title">Co-located validators (/24 subnets)</div>
    <div style="font-size:12px;color:var(--text-mid);margin-bottom:10px">/24 = a subnet of ≈256 IPs, typically one rack or one provider segment. Multiple validators in the same /24 share hardware-failure risk.</div>
    <div style="overflow-x:auto;margin-top:10px">
      <table class="audit-table">
        <thead><tr><th>Subnet</th><th>Members</th><th>ISP</th><th>Location</th></tr></thead>
        <tbody>${rows}</tbody>
      </table>
    </div>
    ${hint16 ? `<div style="margin-top:14px;font-size:11px;color:var(--text-dim);text-transform:uppercase;letter-spacing:1px">/16 hotspots (≈65k addresses)</div><div class="pill-row">${hint16}</div>` : ""}
    <div style="font-size:10px;color:var(--text-dim);margin-top:10px;line-height:1.5">● = every member of the subnet is unidentified · ◐ = some members anonymous</div>
  </section>
  `;
}

function renderAnonClusters(g) {
  const clusters = g?.anonymous_clusters || [];
  if (!clusters.length) {
    // Don't silently disappear when zero — show the threshold so a reader
    // can tell "we checked and found none" from "we hid the section".
    return `
    <section class="data-section audit-section">
      <div class="section-title">Unidentified clusters</div>
      <div class="audit-empty">
        Checked all ${g?.shared24_count || 0} co-located /24 subnets across <strong>${g?.registered_count || 0}</strong> registered validators. No subnet matched the threshold (≥3 members, all without an entry in <code>monad-developers/validator-info</code>).
      </div>
    </section>`;
  }
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
    <div class="section-title">Unidentified clusters</div>
    <div class="audit-flag" style="background:rgba(110,84,255,0.05);border-color:rgba(110,84,255,0.18)">
      <div class="audit-flag-title" style="color:var(--purple-light)">ℹ What this means</div>
      Subnets where none of the members has an entry in <code>monad-developers/validator-info</code>. Plausible explanations: (1) one operator running multiple validators under anonymity, (2) Monad Foundation / Category Labs internal infrastructure (backup nodes, test rigs), (3) operators who simply haven't filed a PR to the public registry. <strong>This is not proof of a Sybil pattern</strong> — just a signal worth public clarification.
    </div>
    ${cards}
  </section>
  `;
}

function renderStakeClusters(s) {
  const clusters = s?.clusters || [];
  if (!clusters.length) {
    return `
    <section class="data-section audit-section">
      <div class="section-title">Operators with multiple validators under one wallet</div>
      <div class="audit-empty">
        No auth wallet on this network is registered for more than one val_id — every validator has its own dedicated authorization wallet.
      </div>
    </section>`;
  }
  const cards = clusters.map((c) => `
    <div class="cluster-card">
      <div class="cluster-head">
        <span class="cluster-subnet">${esc(shortAddr ? shortAddr(c.auth) : c.auth)} — ${c.val_count} validators</span>
        <span class="cluster-meta">total ${fmtNum ? fmtNum(c.total_stake_mon) : c.total_stake_mon} MON</span>
      </div>
      <div class="cluster-members">${c.members.map((m) => `val#${m.val_id} ${m.name ? '(' + esc(m.name) + ')' : ''} — ${fmtNum ? fmtNum(m.stake_mon) : m.stake_mon} MON`).join(" · ")}</div>
    </div>
  `).join("");
  return `
  <section class="data-section audit-section">
    <div class="section-title">Operators with multiple validators under one wallet</div>
    <div style="font-size:12px;color:var(--text-mid);margin-bottom:10px">The same auth wallet is registered for multiple val_ids. The information is public on-chain, but most explorers don't aggregate it. Category Labs, for example, is the core Monad team and their joint registration is documented in validator-info — this is not hidden Sybil behavior.</div>
    ${cards}
  </section>
  `;
}

function renderRotation(r) {
  if (!r || !r.applicable) {
    if (r && r.applicable === false) {
      return `
      <section class="data-section audit-section">
        <div class="section-title">Delegation rotation cadence (VDP)</div>
        <div class="audit-empty">
          <strong>Mainnet</strong>: Foundation rotation does not apply on mainnet — validator selection follows different rules. This section is shown for testnet only.
        </div>
      </section>`;
    }
    return "";
  }
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
      <span style="color:var(--text-dim);min-width:60px;text-align:right">${d.distinct_vals} val_ids</span>
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
    <div class="section-title">Rhythm of delegation rotation (testnet)</div>
    <div style="font-size:12px;color:var(--text-mid);margin-bottom:14px">
      Monad Foundation periodically moves delegations (~2M MON each) between validators in the queue. Reserve list (pool): <strong>${r.rotation_pool_size}</strong> validators. All events are public on-chain — this is just a visualization, not reverse-engineering.
      <span style="color:#4ade80">+ entered active set</span> · <span style="color:#FF8EE4">− exited</span>
    </div>
    <div style="font-size:11px;color:var(--text-dim);text-transform:uppercase;letter-spacing:1px;margin:14px 0 8px">Daily ins/outs over the last 14 days</div>
    ${dailyBars}
    <div style="font-size:11px;color:var(--text-dim);text-transform:uppercase;letter-spacing:1px;margin:24px 0 8px">UTC hour distribution</div>
    <div style="display:flex;gap:2px;align-items:stretch">${hourCells}</div>
    <div style="font-size:10px;color:var(--text-dim);margin-top:8px">Each cell = 1 UTC hour. Darker = more rotation events fired in that hour over the 14-day window.</div>
  </section>
  `;
}

function renderPerformance(p) {
  if (!p || !p.applicable) {
    if (p && p.applicable === false) {
      return `
      <section class="data-section audit-section">
        <div class="section-title">Block production within the rotation pool</div>
        <div class="audit-empty">
          <strong>Mainnet</strong>: the VDP rotation pool exists only on testnet. This section is hidden on mainnet.
        </div>
      </section>`;
    }
    return "";
  }
  const b = p.buckets || {};
  const total = Object.values(b).reduce((a, v) => a + v, 0) || 1;
  // Severity gradient — pink (idle) → orange → washed-purple (middle) → cyan → green (active)
  const segments = [
    { label: "0 blocks", value: b["0"] || 0, color: "#FF8EE4", text: "#08050F" },
    { label: "1-999", value: b["1-999"] || 0, color: "#FFAE45", text: "#08050F" },
    { label: "1000-2999", value: b["1000-2999"] || 0, color: "#c4b5fd", text: "#08050F" },
    { label: "3000-4999", value: b["3000-4999"] || 0, color: "#85E6FF", text: "#08050F" },
    { label: "5000+", value: b["5000+"] || 0, color: "#4ade80", text: "#08050F" },
  ];
  const segs = segments.map((s) => {
    const w = (s.value / total) * 100;
    if (s.value === 0) {
      // Hairline marker so the empty middle is visible, not silently missing
      return `<div class="histogram-segment" style="background:${s.color};width:2px;opacity:0.4" title="${s.label}: empty"></div>`;
    }
    // Non-empty buckets get a minimum visible width so a 0.4% sliver
    // (e.g. the lone "1000-2999" datapoint that proves bimodality) is
    // still distinguishable from the empty hairline next to it.
    const minPct = 100 * 12 / 800; // ~12px on a typical 800px container = ~1.5%
    const effectiveW = Math.max(w, minPct);
    if (w < 4) {
      return `<div class="histogram-segment" style="background:${s.color};flex:0 0 ${effectiveW}%;color:${s.text}" title="${s.label}: ${s.value}"></div>`;
    }
    return `<div class="histogram-segment" style="background:${s.color};flex:0 0 ${w}%;color:${s.text}" title="${s.label}: ${s.value}">${s.value}</div>`;
  }).join("");
  const legend = segments.map((s) => `<span><span class="histogram-legend-dot" style="background:${s.color}"></span>${s.label}</span>`).join("");

  const zeroList = (p.zero_blocks_named || []).slice(0, 24);
  const recentCount = p.zero_blocks_recent_count || 0;
  const tenureDays = p.tenure_threshold_days || 14;

  const zeroPills = zeroList.map((it) => {
    const days = it.days_in_pool ? ` · ${it.days_in_pool}d in pool` : "";
    return `<span class="pill pill-warn" title="val_id ${it.val_id}${days}">${esc(it.name)}</span>`;
  }).join("");

  const top = (p.top_performers || []).slice(0, 8);
  const topRows = top.map((it) => `<tr>
    <td><strong style="color:var(--text)">${esc(it.name || `val#${it.val_id}`)}</strong></td>
    <td style="color:var(--text-dim)">${it.val_id}</td>
    <td style="text-align:right;color:#4ade80;font-weight:600">${fmtNum ? fmtNum(it.blocks_7d) : it.blocks_7d}</td>
  </tr>`).join("");

  return `
  <section class="data-section audit-section">
    <div class="section-title">Block production within the rotation pool (last 7 days)</div>
    <div style="font-size:12px;color:var(--text-mid);margin-bottom:10px">
      Pool size <strong>${p.pool_size}</strong> validators. Distribution is <strong>bimodal</strong>: validators are either in the active set producing thousands of blocks, or completely idle. The middle is nearly empty.
    </div>
    <div class="histogram-bar">${segs}</div>
    <div class="histogram-legend">${legend}</div>
    <div style="font-size:10px;color:var(--text-dim);margin-top:6px">← idle · active →</div>

    ${zeroList.length ? `
      <div style="font-size:11px;color:var(--text-dim);text-transform:uppercase;letter-spacing:1px;margin:24px 0 8px">Operators stuck at zero blocks (≥${tenureDays} days in pool, last 7d)</div>
      <div class="audit-flag">
        <div class="audit-flag-title">⚠ Possible queue-fairness question</div>
        <strong>${zeroList.length}</strong> delegation-program operators have been in the rotation pool for over two weeks but produced zero blocks in the last 7 days. Plausible explanations: (a) the queue is long and their slot hasn't come up, (b) Foundation uses internal performance gates before promotion, (c) bias in the rotation algorithm. The list is not a verdict on operator quality — it's an observation worth raising with the Foundation.
        ${recentCount ? `<div style="margin-top:8px;font-size:11px;color:var(--text-dim)">Additionally: ${recentCount} more operators are also at zero, but joined the pool less than ${tenureDays} days ago — not flagged as "stuck".</div>` : ""}
        <div style="margin-top:10px;padding-top:10px;border-top:1px solid rgba(255,142,228,0.15);font-size:11px;color:var(--text-dim);line-height:1.6">
          <strong style="color:var(--text-mid)">Conflict-of-interest disclosure:</strong> the author of this page operates val_id 267 (<em>shadowoftime</em>) and is in the same rotation pool. The author's val_id appears in the same statistics; names below are shown without editorial selection.
        </div>
      </div>
      <details class="zero-blocks-details">
        <summary style="cursor:pointer;color:var(--text-mid);font-family:var(--mono);font-size:12px;padding:8px 0">
          Show ${zeroList.length} named operator${zeroList.length === 1 ? '' : 's'} ▾
        </summary>
        <div class="pill-row" style="margin-top:10px">${zeroPills}</div>
      </details>
    ` : ""}

    ${topRows ? `
      <div style="font-size:11px;color:var(--text-dim);text-transform:uppercase;letter-spacing:1px;margin:24px 0 8px">Top-8 by block count</div>
      <div style="overflow-x:auto"><table class="audit-table"><thead><tr><th>Validator</th><th>val_id</th><th style="text-align:right">Blocks (7d)</th></tr></thead><tbody>${topRows}</tbody></table></div>
    ` : ""}
  </section>
  `;
}

function renderMethodology(d) {
  return `
  <section class="audit-section" style="padding-top:20px;border-top:1px solid rgba(110,84,255,0.08);font-size:12px;color:var(--text-mid);font-family:var(--mono);line-height:1.7">
    <div style="font-size:11px;color:var(--text-dim);text-transform:uppercase;letter-spacing:1px;margin-bottom:8px">Methodology &amp; limitations</div>
    <div><strong>Sources:</strong> peers.toml (P2P-signed peer registry from MonadPulse's own testnet validator and mainnet observer), staking precompile (on-chain stake events), <a href="https://ip-api.com" target="_blank" style="color:var(--purple-light)">ip-api.com</a> for GeoIP.</div>
    <div><strong>Accuracy:</strong> ip-api.com is ~85% country-accurate, ~55% city-accurate; the "datacenter" flag is a heuristic, not a strict criterion. ISP names are merged by ASN where possible, but variants of the same operator may still split.</div>
    <div><strong>Freshness:</strong> the GeoIP base is rebuilt hourly; on-chain metrics (stake, rotation, blocks) update in real time from local RPC; this page caches 5 minutes.</div>
    <div><strong>Windows:</strong> blocks — 7 days; rotation — 14 days; pool tenure — measured from the first Foundation delegate to the val_id (full history).</div>
    <div><strong>Conflict-of-interest disclosure:</strong> built by an independent validator (val_id 267, "shadowoftime"). The author is also in the rotation pool and appears in the same statistics. Names of competitor operators in the "stuck" list are shown without editorial choice and verified for ≥14d pool tenure before publication.</div>
    <div style="margin-top:8px;font-size:10px;color:var(--text-dim)">Generated ${new Date(d.generated_at).toLocaleString()}. Open source: <a href="https://github.com/ShadowOfTime1/monadpulse" target="_blank" style="color:var(--purple-light)">github.com/ShadowOfTime1/monadpulse</a></div>
  </section>
  `;
}

function truncate(s, n) {
  if (!s) return "";
  return s.length > n ? s.slice(0, n - 1) + "…" : s;
}
