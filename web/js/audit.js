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

// Hero verdict — one-sentence plain-English summary with traffic-light tone.
function renderVerdict(d) {
  const g = d.geo || {};
  const p = d.performance || {};
  const r = d.rotation || {};
  const network = d.network || "testnet";
  const HHI = g.country_hhi || 0;
  const tone = HHI < 1500 ? { color: "#4ade80", label: "Спокойная" }
              : HHI < 2500 ? { color: "#FFAE45", label: "Умеренная" }
              : { color: "#FF8EE4", label: "Высокая" };

  const stuck = (p.zero_blocks_named || []).length;
  const stuckSentence = p.applicable && stuck > 0
    ? ` <strong>${stuck} операторов</strong> зарегистрированы в программе делегирования больше двух недель и не предложили ни одного блока за прошедшие 7 дней — это может быть очередь или баг алгоритма.`
    : "";
  const subnets = g.shared24_count || 0;
  const subnetSentence = subnets ? ` <strong>${g.shared24_validators}</strong> валидаторов делят дата-центровые стойки с соседями (${subnets} «общих» подсетей /24).` : "";

  return `
  <section class="audit-section verdict-band">
    <div class="verdict-tone" style="border-color:${tone.color}33;background:${tone.color}0a">
      <div class="verdict-label" style="color:${tone.color}">${tone.label} концентрация</div>
      <div class="verdict-text">
        Сеть <strong>${network === "mainnet" ? "mainnet" : "testnet"}</strong> Monad — <strong>${g.registered_count || 0}</strong> валидаторов в <strong>${g.country_total || 0}</strong> странах через <strong>${g.asn_total || 0}</strong> провайдеров.${subnetSentence}${stuckSentence}
      </div>
    </div>
  </section>
  `;
}

function renderHero(d) {
  const g = d.geo || {};
  const HHI = g.country_hhi || 0;
  const hhiLabel = HHI < 1500 ? "конкурентная" : HHI < 2500 ? "умеренная" : "концентрированная";
  const hhiColor = HHI < 1500 ? "#4ade80" : HHI < 2500 ? "#FFAE45" : "#FF8EE4";
  return `
  <section class="audit-section">
    <div class="audit-grid">
      <div class="audit-stat">
        <div class="audit-stat-label" title="Валидаторы с прописанным val_id, зарегистрированные on-chain">Зарегистр. валидаторов</div>
        <div class="audit-stat-value">${g.registered_count || 0}</div>
        <div class="audit-stat-sub">+ ${g.peers_only_count || 0} незарегистр. P2P-узлов</div>
      </div>
      <div class="audit-stat">
        <div class="audit-stat-label" title="Индекс Херфиндаля-Хиршмана: 0–1500 — здоровое распределение, 2500+ — высокая концентрация">Раскид по странам</div>
        <div class="audit-stat-value" style="color:${hhiColor}">${HHI}</div>
        <div class="audit-stat-sub">${hhiLabel} · ${g.country_total || 0} стран</div>
      </div>
      <div class="audit-stat">
        <div class="audit-stat-label" title="Тот же индекс по уникальным сетевым операторам (ASN)">Раскид по провайдерам</div>
        <div class="audit-stat-value">${g.asn_hhi || 0}</div>
        <div class="audit-stat-sub">${g.asn_total || 0} уникальных ASN</div>
      </div>
      <div class="audit-stat">
        <div class="audit-stat-label" title="Серверы в коммерческих дата-центрах vs остальное">Дата-центры</div>
        <div class="audit-stat-value">${g.datacenter_pct || 0}%</div>
        <div class="audit-stat-sub">${g.datacenter_count || 0} из ${g.registered_count || 0}</div>
      </div>
      <div class="audit-stat">
        <div class="audit-stat-label" title="Подсети /24 (≈256 IP) с двумя или более валидаторами">Общие подсети /24</div>
        <div class="audit-stat-value">${g.shared24_count || 0}</div>
        <div class="audit-stat-sub">${g.shared24_validators || 0} валидаторов в общих стойках</div>
      </div>
      <div class="audit-stat">
        <div class="audit-stat-label" title="Подсети /24, где ни один валидатор не идентифицирован в monad-developers/validator-info">Анонимные кластеры</div>
        <div class="audit-stat-value" style="color:${(g.anonymous_clusters||[]).length ? '#FF8EE4' : 'var(--text)'}">${(g.anonymous_clusters || []).length}</div>
        <div class="audit-stat-sub">все участники без публичного имени</div>
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
    <div class="section-title">География</div>
    <div style="font-size:12px;color:var(--text-mid);margin-bottom:6px">Top-10 стран по числу зарегистрированных валидаторов.</div>
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
    <div class="section-title">Концентрация хостинга</div>
    <div style="font-size:12px;color:var(--text-mid);margin-bottom:6px">Где физически живут серверы валидаторов. Имена ISP объединены по ASN, чтобы «Limestone Networks» и «Limestone Networks, Inc.» считались одним провайдером.</div>
    <div style="margin:10px 0 16px">${ispRows}</div>
    <div style="font-size:11px;color:var(--text-dim);text-transform:uppercase;letter-spacing:1px;margin-bottom:6px">Top ASN (автономных систем)</div>
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
      ? '<span style="color:#FF8EE4;margin-right:6px" title="все участники анонимны">●</span>'
      : (s.anonymous > 0 ? `<span style="color:#FFAE45;margin-right:6px" title="${s.anonymous} из ${s.count} анонимны">◐</span>` : '');
    return `<tr>
      <td>${anonDot}<strong style="color:var(--text);font-family:var(--mono)">${esc(s.subnet)}</strong></td>
      <td>${s.count} валидаторов</td>
      <td>${esc(truncate(s.isp || '?', 24))}</td>
      <td>${esc(s.city || '?')}, ${esc(s.country || '?')}</td>
    </tr>`;
  }).join("");
  const hint16 = (g.shared16_top || []).slice(0, 5).map((s) => `<span class="pill"><strong>${esc(s.subnet)}</strong> ${s.count}</span>`).join("");
  return `
  <section class="data-section audit-section">
    <div class="section-title">Совместное размещение валидаторов (/24 подсети)</div>
    <div style="font-size:12px;color:var(--text-mid);margin-bottom:10px">/24 — подсеть из ≈256 IP-адресов, обычно одна стойка или один провайдер. Несколько валидаторов в одной подсети = общий риск отказа железа.</div>
    <div style="overflow-x:auto;margin-top:10px">
      <table class="audit-table">
        <thead><tr><th>Подсеть</th><th>Кол-во</th><th>Провайдер</th><th>Локация</th></tr></thead>
        <tbody>${rows}</tbody>
      </table>
    </div>
    ${hint16 ? `<div style="margin-top:14px;font-size:11px;color:var(--text-dim);text-transform:uppercase;letter-spacing:1px">/16 «горячие точки» (≈65k адресов)</div><div class="pill-row">${hint16}</div>` : ""}
    <div style="font-size:10px;color:var(--text-dim);margin-top:10px;line-height:1.5">● = все участники подсети без публичного имени · ◐ = часть анонимна</div>
  </section>
  `;
}

function renderAnonClusters(g) {
  const clusters = g?.anonymous_clusters || [];
  if (!clusters.length) return "";
  const cards = clusters.map((c) => `
    <div class="cluster-card" style="border-color:rgba(255,142,228,0.25);background:rgba(255,142,228,0.04)">
      <div class="cluster-head">
        <span class="cluster-subnet">${esc(c.subnet)} → ${c.count} анонимных</span>
        <span class="cluster-meta">${esc(truncate(c.isp || '?', 30))} · ${esc(c.city || '?')}, ${esc(c.country || '?')}</span>
      </div>
      <div class="cluster-members">${c.members.map((m) => esc(m.ip)).join(" · ")}</div>
    </div>
  `).join("");
  return `
  <section class="data-section audit-section">
    <div class="section-title">Кластеры без идентификации</div>
    <div class="audit-flag" style="background:rgba(110,84,255,0.05);border-color:rgba(110,84,255,0.18)">
      <div class="audit-flag-title" style="color:var(--purple-light)">ℹ Что это значит</div>
      Подсеть, где ни у одного из участников нет записи в <code>monad-developers/validator-info</code>. Возможные объяснения: (1) единый оператор с несколькими валидаторами под анонимностью, (2) внутренняя инфраструктура Monad Foundation / Category Labs (резервные ноды, тестовые стенды), (3) операторы, которые просто не подавали PR в публичный реестр. <strong>Это не доказательство sybil-атаки</strong> — это сигнал, который стоит прокомментировать публично.
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
        <span class="cluster-subnet">${esc(shortAddr ? shortAddr(c.auth) : c.auth)} — ${c.val_count} валидаторов</span>
        <span class="cluster-meta">всего ${fmtNum ? fmtNum(c.total_stake_mon) : c.total_stake_mon} MON</span>
      </div>
      <div class="cluster-members">${c.members.map((m) => `val#${m.val_id} ${m.name ? '(' + esc(m.name) + ')' : ''} — ${fmtNum ? fmtNum(m.stake_mon) : m.stake_mon} MON`).join(" · ")}</div>
    </div>
  `).join("");
  return `
  <section class="data-section audit-section">
    <div class="section-title">Операторы с несколькими валидаторами под одним кошельком</div>
    <div style="font-size:12px;color:var(--text-mid);margin-bottom:10px">Один и тот же auth-кошелёк зарегистрирован для нескольких val_id. Информация публичная (читается с blockchain), но большинство explorer'ов её не агрегируют. Например, Category Labs — это ядро команды Monad, и их совместная регистрация задокументирована в реестре validator-info; это не скрытая sybil-схема.</div>
    ${cards}
  </section>
  `;
}

function renderRotation(r) {
  if (!r || !r.applicable) {
    if (r && r.applicable === false) {
      return `
      <section class="data-section audit-section">
        <div class="section-title">Ротация делегирования (VDP)</div>
        <div class="audit-empty">
          <strong>Mainnet</strong>: программы ротации Foundation на этой сети нет — выбор валидаторов работает по другим правилам. Этот раздел показывается только для testnet.
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
      <span style="color:var(--text-dim);min-width:60px;text-align:right">${d.distinct_vals} val_id</span>
    </div>`;
  }).join("");

  const hourly = r.hourly_distribution || [];
  const maxHour = Math.max(1, ...hourly.map((h) => h.events));
  const hourCells = Array.from({ length: 24 }, (_, h) => {
    const ev = hourly.find((x) => x.hour === h)?.events || 0;
    const intensity = ev / maxHour;
    const bg = intensity > 0 ? `rgba(110,84,255,${0.15 + intensity * 0.8})` : "rgba(110,84,255,0.04)";
    return `<div style="flex:1;text-align:center;padding:6px 0;background:${bg};border-radius:3px;font-size:9px;color:${intensity > 0.5 ? '#fff' : 'var(--text-dim)'};font-family:var(--mono);min-width:18px" title="${h}:00 UTC — ${ev} событий">${h}</div>`;
  }).join("");

  return `
  <section class="data-section audit-section">
    <div class="section-title">Ритм ротации делегирования (testnet)</div>
    <div style="font-size:12px;color:var(--text-mid);margin-bottom:14px">
      Monad Foundation периодически переводит делегации (~2M MON каждая) между валидаторами в очереди. Резервный список (пул): <strong>${r.rotation_pool_size}</strong> валидаторов. Все события публичные, читаются с blockchain — это просто визуализация on-chain активности, не «расшифровка» внутренней логики.
      <span style="color:#4ade80">+ зашёл в активный набор</span> · <span style="color:#FF8EE4">− вышел</span>
    </div>
    <div style="font-size:11px;color:var(--text-dim);text-transform:uppercase;letter-spacing:1px;margin:14px 0 8px">Заходы/выходы за последние 14 дней</div>
    ${dailyBars}
    <div style="font-size:11px;color:var(--text-dim);text-transform:uppercase;letter-spacing:1px;margin:24px 0 8px">Распределение по часам UTC</div>
    <div style="display:flex;gap:2px;align-items:stretch">${hourCells}</div>
    <div style="font-size:10px;color:var(--text-dim);margin-top:8px">Каждая ячейка = 1 час UTC. Темнее = больше событий ротации в этот час за 14 дней.</div>
  </section>
  `;
}

function renderPerformance(p) {
  if (!p || !p.applicable) {
    if (p && p.applicable === false) {
      return `
      <section class="data-section audit-section">
        <div class="section-title">Производство блоков в пуле ротации</div>
        <div class="audit-empty">
          <strong>Mainnet</strong>: пул VDP-ротации существует только на testnet. На mainnet раздел не отображается.
        </div>
      </section>`;
    }
    return "";
  }
  const b = p.buckets || {};
  const total = Object.values(b).reduce((a, v) => a + v, 0) || 1;
  // Severity gradient — pink (idle) → orange → washed-purple (middle) → cyan → green (active)
  const segments = [
    { label: "0 блоков", value: b["0"] || 0, color: "#FF8EE4", text: "#08050F" },
    { label: "1-999", value: b["1-999"] || 0, color: "#FFAE45", text: "#08050F" },
    { label: "1000-2999", value: b["1000-2999"] || 0, color: "#c4b5fd", text: "#08050F" },
    { label: "3000-4999", value: b["3000-4999"] || 0, color: "#85E6FF", text: "#08050F" },
    { label: "5000+", value: b["5000+"] || 0, color: "#4ade80", text: "#08050F" },
  ];
  const segs = segments.map((s) => {
    const w = (s.value / total) * 100;
    if (s.value === 0) {
      // Hairline marker so the empty middle is visible, not silently missing
      return `<div class="histogram-segment" style="background:${s.color};width:2px;opacity:0.4" title="${s.label}: пусто"></div>`;
    }
    if (w < 1.5) {
      return `<div class="histogram-segment" style="background:${s.color};width:${w}%;color:${s.text}" title="${s.label}: ${s.value}"></div>`;
    }
    return `<div class="histogram-segment" style="background:${s.color};width:${w}%;color:${s.text}" title="${s.label}: ${s.value}">${s.value}</div>`;
  }).join("");
  const legend = segments.map((s) => `<span><span class="histogram-legend-dot" style="background:${s.color}"></span>${s.label}</span>`).join("");

  const zeroList = (p.zero_blocks_named || []).slice(0, 24);
  const recentCount = p.zero_blocks_recent_count || 0;
  const tenureDays = p.tenure_threshold_days || 14;

  const zeroPills = zeroList.map((it) => {
    const days = it.days_in_pool ? ` · ${it.days_in_pool}д в пуле` : "";
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
    <div class="section-title">Производство блоков в пуле ротации (за 7 дней)</div>
    <div style="font-size:12px;color:var(--text-mid);margin-bottom:10px">
      Размер пула <strong>${p.pool_size}</strong> валидаторов. Распределение <strong>бимодальное</strong>: либо в активном наборе и ты делаешь тысячи блоков, либо вообще никаких. Середина почти пустая.
    </div>
    <div class="histogram-bar">${segs}</div>
    <div class="histogram-legend">${legend}</div>
    <div style="font-size:10px;color:var(--text-dim);margin-top:6px">← простой · активные →</div>

    ${zeroList.length ? `
      <div style="font-size:11px;color:var(--text-dim);text-transform:uppercase;letter-spacing:1px;margin:24px 0 8px">Именованные операторы с нулём блоков (≥${tenureDays} дней в пуле, последние 7 дней)</div>
      <div class="audit-flag">
        <div class="audit-flag-title">⚠ Возможный вопрос к справедливости очереди</div>
        ${zeroList.length} VDP-операторов больше двух недель находятся в пуле ротации, но ни одного блока за прошедшую неделю не выпустили. Возможные объяснения: (а) очередь длинная и до них пока не дошло, (б) Foundation использует внутренние критерии оценки до промоушена, (в) баг в алгоритме ротации. Список не утверждает, что это недостаток операторов — это наблюдаемый паттерн, который стоит обсудить с Foundation.
        ${recentCount ? `<div style="margin-top:8px;font-size:11px;color:var(--text-dim)">Дополнительно: ещё ${recentCount} операторов также с нулём блоков, но они вошли в пул менее ${tenureDays} дней назад — их пока не считаем «застрявшими».</div>` : ""}
      </div>
      <div class="pill-row" style="margin-top:10px">${zeroPills}</div>
    ` : ""}

    ${topRows ? `
      <div style="font-size:11px;color:var(--text-dim);text-transform:uppercase;letter-spacing:1px;margin:24px 0 8px">Top-8 по числу блоков</div>
      <div style="overflow-x:auto"><table class="audit-table"><thead><tr><th>Валидатор</th><th>val_id</th><th style="text-align:right">Блоков (7д)</th></tr></thead><tbody>${topRows}</tbody></table></div>
    ` : ""}
  </section>
  `;
}

function renderMethodology(d) {
  return `
  <section class="audit-section" style="padding-top:20px;border-top:1px solid rgba(110,84,255,0.08);font-size:12px;color:var(--text-mid);font-family:var(--mono);line-height:1.7">
    <div style="font-size:11px;color:var(--text-dim);text-transform:uppercase;letter-spacing:1px;margin-bottom:8px">Методология и ограничения</div>
    <div><strong>Источники данных:</strong> peers.toml (P2P-подписанный реестр пиров от обоих узлов MonadPulse), staking precompile (on-chain stake events), <a href="https://ip-api.com" target="_blank" style="color:var(--purple-light)">ip-api.com</a> для GeoIP.</div>
    <div><strong>Точность:</strong> ip-api.com даёт ~85% точности по странам, ~55% по городам; флаг «дата-центр» — эвристика, не строгий критерий. Названия ISP объединены по ASN, но варианты у одного оператора всё равно возможны.</div>
    <div><strong>Свежесть:</strong> геолокационная база перестраивается раз в час; on-chain метрики (ставки, ротация, блоки) обновляются в реальном времени с локальных RPC; страница кэшируется 5 минут.</div>
    <div><strong>Окна:</strong> блоки — 7 дней; ротация — 14 дней; продолжительность в пуле — с момента первого делегирования от Foundation (полная история).</div>
    <div><strong>Раскрытие интересов:</strong> страница построена независимым валидатором (val_id 267, «shadowoftime»). Я тоже в пуле ротации и попадаю в общую статистику. Имена конкурентов в списке «застрявших» приведены без редакторских правок и проверены на возраст в пуле перед публикацией.</div>
    <div style="margin-top:8px;font-size:10px;color:var(--text-dim)">Сгенерировано ${new Date(d.generated_at).toLocaleString("ru-RU")}. Открытый исходный код: <a href="https://github.com/ShadowOfTime1/monadpulse" target="_blank" style="color:var(--purple-light)">github.com/ShadowOfTime1/monadpulse</a></div>
  </section>
  `;
}

function truncate(s, n) {
  if (!s) return "";
  return s.length > n ? s.slice(0, n - 1) + "…" : s;
}
