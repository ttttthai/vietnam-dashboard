// ── FINSYS renderer ──
// "Hệ thống tài chính Việt Nam": system-wide view (SBV, money supply, deposits/credit, flows, why rates stay high, safety ratios).
// Individual banks live in the collapsed appendix below. Data: FINSYS (SBV statistics, IMF, World Bank, securities research).
const fsE = () => typeof APP_LANG !== 'undefined' && APP_LANG === 'en';
const fsT = (vi, en) => fsE() ? en : vi;
const fsN = (v, d = 1) => v == null || !isFinite(v) ? '—' : v.toLocaleString(APP_LOC, { minimumFractionDigits: d, maximumFractionDigits: d });
const fsTr = bn => bn == null ? '—' : bn >= 1e6 ? fsN(bn / 1e6, 2) + fsT(' triệu tỷ', ' qn') : fsN(bn / 1e3, 0) + fsT(' nghìn tỷ', ' tn');   // input: billion VND
const fsMon = k => { const [y, mo] = String(k).split('-'); return mo ? `T${+mo}/${y.slice(2)}` : k; };   // 2026-07 → T7/26
const fsLast = arr => { for (let i = (arr || []).length - 1; i >= 0; i--) if (arr[i] != null) return { v: arr[i], i }; return { v: null, i: -1 }; };
const fsRatioLast = k => { const a = (FINSYS.ratios[k] || []); return a.length ? a[a.length - 1] : null; };
const fsFlow = prefix => { const k = Object.keys(FINSYS.flows).find(x => x.startsWith(prefix)); const a = k ? FINSYS.flows[k] : []; return a.length ? a[a.length - 1] : null; };
const fsSrc = u => u ? ` <a class="fs-src" href="${ciE(u)}" target="_blank" rel="noopener">${fsT('nguồn', 'source')}</a>` : '';
const fsTile = (l, v, sub, u) => `<div class="kpi-tile"><div class="kpi-label">${l}</div><div class="kpi-val">${v}</div>${sub ? `<div class="kpi-sub">${sub}${fsSrc(u)}</div>` : (u ? `<div class="kpi-sub">${fsSrc(u)}</div>` : '')}</div>`;

function renderFinSys() {
  const root = document.getElementById('fs-root'); if (!root || typeof FINSYS === 'undefined') return;
  const F = FINSYS, M = F.monthly, A = F.annual;
  const cr = fsLast(M.credit_level), dp = fsLast(M.deposits_level), m2 = fsLast(M.m2_level);
  const crY = fsLast(M.credit_ytd), crYoY = fsLast(M.credit_yoy);
  const ldr = fsRatioLast('ldr_sbv_tt22_pct'), st = fsRatioLast('short_term_funds_for_mlt_loans_pct'), npl = fsRatioLast('npl_onbalance_sbv_pct'), nplB = fsRatioLast('npl_incl_vamc_and_potential_pct');
  const roe = fsRatioLast('roe_sbv_pct'), cg = fsRatioLast('credit_to_gdp_pct'), nim = fsLast((F.why.series.nim || {}).values), cof = fsLast((F.why.series.cof || {}).values);
  const ldrSimple = fsLast((F.why.series.ldr || {}).values), re = fsFlow('Real estate credit, total'), reSh = fsFlow('Real estate credit share');
  const lend = fsLast((F.why.series.avg_lending_rate_sbv || {}).values), dep12 = fsLast((F.why.series.deposit12m_private_banks_mbs || {}).values);
  const kbnn = fsFlow('State Treasury (KBNN) deposits at banks'), bonds = fsFlow('Corporate bonds outstanding'), margin = fsFlow('Stock-market lending');
  const fxRes = (typeof ECON_OFFICIAL !== 'undefined' && ECON_OFFICIAL.fx_reserves_busd) ? ECON_OFFICIAL.fx_reserves_busd[ECON_OFFICIAL.fx_reserves_busd.length - 1] : null;
  const lbl = i => M.months[i];
  const crLast = F.monthly.months[crY.i];

  let h = `<div class="fs-nav" role="navigation">${[['fs-ov', fsT('Tổng quan', 'Overview')], ['fs-sbv', fsT('NHNN', 'SBV')], ['fs-money', fsT('Cung tiền & tín dụng', 'Money & credit')], ['fs-flow', fsT('Dòng tiền đi đâu', 'Where money flows')], ['fs-why', fsT('Vì sao lãi suất khó giảm', 'Why rates stay high')], ['fs-safe', fsT('An toàn hệ thống', 'System soundness')], ['fs-appendix', fsT('Phụ lục: 17 ngân hàng', 'Appendix: 17 banks')]].map(([id, t]) => `<button type="button" data-fs-go="${id}">${t}</button>`).join('')}</div>`;

  // ── Overview ──
  h += `<div class="econ-card full-width" id="fs-ov"><div class="econ-card-header">${fsT('Hệ thống tài chính Việt Nam — tổng quan', 'Vietnam financial system — overview')} <span class="pn-label" style="font-family:var(--font-ui)">· ${fsT('số liệu NHNN, IMF, WB; cập nhật', 'SBV, IMF, WB data; as of')} ${pgFmt(F.asof)}</span></div><div class="econ-card-body"><div class="kpi-grid fs-kpis">`;
  h += fsTile(fsT('Tổng phương tiện thanh toán (M2)', 'Money supply (M2)'), fsTr(m2.v), `${lbl(m2.i)} · ${fsT('so với đầu năm', 'YTD')} ${fsN(fsLast(M.m2_ytd).v, 2)}%`);
  h += fsTile(fsT('Tổng huy động (tiền gửi KH)', 'Customer deposits'), fsTr(dp.v), `${lbl(dp.i)} · YTD ${fsN(fsLast(M.deposits_ytd).v, 2)}%`);
  h += fsTile(fsT('Dư nợ tín dụng nền kinh tế', 'Credit to the economy'), fsTr(cr.v), `${lbl(cr.i)}`);
  h += fsTile(fsT('Tăng trưởng tín dụng', 'Credit growth'), `${fsN(crY.v, 2)}% YTD`, `${crLast}${crYoY.v != null ? ` · ${fsN(crYoY.v, 2)}% ${fsT('svck', 'YoY')} (${lbl(crYoY.i)})` : ''} · ${fsT('mục tiêu 2026', '2026 target')} ~15%`);
  h += fsTile(fsT('Tín dụng / GDP', 'Credit / GDP'), cg ? fsN(cg.v, 0) + '%' : '—', cg ? `${fsMon(cg.d)}` : '', cg && cg.u);
  h += fsTile(fsT('LDR (TT 22, NHNN)', 'LDR (Circ. 22, SBV)'), ldr ? fsN(ldr.v, 1) + '%' : '—', `${ldr ? fsMon(ldr.d) : ''} · ${fsT('trần 85% → 95% từ 1/12/2026; LDR đơn giản ~', 'cap 85% → 95% from 1/12/2026; simple LDR ~')}${fsN(ldrSimple.v, 0)}%`, ldr && ldr.u);
  h += fsTile(fsT('Nợ xấu nội bảng', 'On-balance NPL'), npl ? fsN(npl.v, 2) + '%' : '—', `${npl ? fsMon(npl.d) : ''}${nplB ? ` · ${fsT('gồm VAMC & tiềm ẩn', 'incl. VAMC & potential')} ${fsN(nplB.v, 2)}%` : ''}`, npl && npl.u);
  h += fsTile(fsT('Chi phí vốn · NIM', 'Cost of funds · NIM'), `${fsN(cof.v, 2)}% · ${fsN(nim.v, 2)}%`, fsT('COF Q1/26 (Q2/26: 4,79%) · NIM 2025, NH niêm yết', 'COF Q1-26 (Q2-26: 4.79%) · NIM 2025, listed banks'));
  h += fsTile(fsT('Lãi suất cho vay BQ', 'Average lending rate'), String(lend.v ?? '—') + '%', fsT('T8/26, NHTM Nhà nước & cổ phần (mới + cũ)', 'Aug-26, state & JS banks (new + outstanding)'));
  h += fsTile(fsT('Huy động 12T BQ NH tư nhân', 'Avg 12M deposit, private banks'), fsN(dep12.v, 2) + '%', 'MBS · T8/26');
  h += fsTile(fsT('Tín dụng bất động sản', 'Real-estate credit'), re ? fsTr(re.v) : '—', `${re ? fsMon(re.d) : ''}${reSh ? ` · ${fsN(reSh.v, 1)}% ${fsT('tổng dư nợ', 'of credit')}` : ''}`, re && re.u);
  h += fsTile(fsT('Dự trữ ngoại hối', 'FX reserves'), fxRes ? fsN(fxRes, 1) + fsT(' tỷ USD', ' bn USD') : '—', fsT('cuối 2025 (IMF); ~87,6 tỷ USD 18/6/2026', 'end-2025 (IMF); ~87.6 bn USD on 18/6/2026'));
  h += `</div><ul class="fs-bullets">${(fsE() ? F.why.summary_en : F.why.summary_vi || []).map(x => `<li>${ciE(x)}</li>`).join('')}</ul></div></div>`;

  // ── SBV balance sheet & income ──
  h += `<div class="econ-card full-width" id="fs-sbv"><div class="econ-card-header">${fsT('Ngân hàng Nhà nước — bảng cân đối & kết quả hoạt động', 'State Bank of Vietnam — balance sheet & income')}</div><div class="econ-card-body" id="fs-sbv-body"></div></div>`;

  // ── Money & credit ──
  h += `<div class="econ-card full-width" id="fs-money"><div class="econ-card-header">${fsT('Cung tiền, huy động & tín dụng', 'Money supply, deposits & credit')}</div><div class="econ-card-body">
    <div class="fs-grid2">
      <div><div class="iv-sec">${fsT('Quy mô cuối năm (nghìn tỷ đồng)', 'Year-end levels (trillion VND)')}</div><div id="fs-ch-levels" class="fs-ch"></div></div>
      <div><div class="iv-sec">${fsT('Tăng trưởng hằng năm (%)', 'Annual growth (%)')}</div><div id="fs-ch-growth" class="fs-ch"></div></div>
      <div><div class="iv-sec">${fsT('Theo tháng: dư nợ vs huy động (nghìn tỷ) — khoảng chênh = vốn ngân hàng phải tìm thêm', 'Monthly: credit vs deposits (trillion) — the gap banks must fund elsewhere')}</div><div id="fs-ch-gap" class="fs-ch"></div></div>
      <div><div class="iv-sec">${fsT('Tín dụng & M2 so với GDP (%)', 'Credit & M2 to GDP (%)')}</div><div id="fs-ch-depth" class="fs-ch"></div></div>
    </div>
    <div class="iv-src">${fsT('Nguồn: NHNN (thống kê tiền tệ, T1/2025–T7/2026; từ T10/2025 NHNN đổi phương pháp M2/huy động nên không so sánh trực tiếp với trước đó), IMF FSI (huy động, dư nợ theo năm), World Bank (M2/GDP, tín dụng tư nhân/GDP đến 2022), tín dụng/GDP 2025 ~146%. NHNN không công bố M0, M1 thành chuỗi — chỉ công bố tỷ lệ tiền mặt/M2.', 'Sources: SBV monetary statistics (Jan-2025–Jul-2026; SBV changed the M2/deposit method from Oct-2025, so not directly comparable with earlier months), IMF FSI (annual deposits, loans), World Bank (M2/GDP, private credit/GDP to 2022), credit/GDP 2025 ~146%. SBV does not publish M0/M1 series — only the cash/M2 ratio.')}</div>
  </div></div>`;

  // ── Where the money flows ──
  h += `<div class="econ-card full-width" id="fs-flow"><div class="econ-card-header">${fsT('Dòng tiền đi đâu', 'Where the money flows')}</div><div class="econ-card-body">
    <div id="fs-sankey" class="fs-sankey"></div>
    <div class="fs-grid2">
      <div><div class="iv-sec">${fsT('Tăng trưởng tín dụng theo ngành (% so với đầu năm)', 'Credit growth by sector (% YTD)')}</div><div id="fs-ch-secgrowth" class="fs-ch"></div></div>
      <div><div class="iv-sec">${fsT('Các kênh dẫn vốn khác', 'Other funding channels')}</div><div class="kpi-grid" id="fs-flow-tiles"></div></div>
    </div>
    <div class="iv-src">${fsT('Nguồn: NHNN — dư nợ theo ngành kinh tế (T7/2026); tín dụng BĐS: NHNN/Bộ Xây dựng; trái phiếu DN: VBMA/HNX; cho vay ký quỹ: UBCKNN; tiền gửi KBNN: báo cáo KBNN/NHNN. "Hoạt động dịch vụ khác" gồm phần lớn tín dụng tiêu dùng, mua nhà và kinh doanh BĐS.', 'Sources: SBV — credit by economic sector (Jul-2026); real-estate credit: SBV/Ministry of Construction; corporate bonds: VBMA/HNX; margin: SSC; State Treasury deposits: Treasury/SBV. "Other services" contains most consumer, home-purchase and real-estate business credit.')}</div>
  </div></div>`;

  // ── Why rates stay high ──
  h += `<div class="econ-card full-width" id="fs-why"><div class="econ-card-header">${fsT('Vì sao ngân hàng chưa thể hạ lãi suất', 'Why banks cannot cut lending rates')}</div><div class="econ-card-body">
    <div class="fs-grid2">
      <div><div class="iv-sec">${fsT('Lãi suất huy động, cho vay, OMO và lạm phát (%)', 'Deposit, lending, OMO rates and inflation (%)')}</div><div id="fs-ch-rates" class="fs-ch"></div></div>
      <div><div class="iv-sec">${fsT('Áp lực tỷ giá: USD/VND trung tâm & thị trường, DXY, lãi suất Fed', 'FX pressure: USD/VND central & market, DXY, Fed rate')}</div><div id="fs-ch-fx" class="fs-ch"></div></div>
    </div>
    <div class="iv-sec">${fsT('Các nguyên nhân (xếp theo mức độ tác động)', 'Drivers (ranked by impact)')}</div>
    <div class="fs-drivers">${F.why.drivers.map(d => `<details class="fs-drv"><summary><span class="fs-rank">${d.rank}</span><span class="fs-dir ${d.direction === 'up_pressure' ? 'up' : 'lim'}">${d.direction === 'up_pressure' ? fsT('đẩy lãi suất lên', 'pushes rates up') : fsT('cản trở việc hạ', 'blocks cuts')}</span> <b>${ciE(fsE() ? d.title_en : d.title_vi)}</b></summary>
      <p>${ciE(fsE() ? d.explain_en : d.explain_vi)}</p>
      ${(d.metrics || []).length ? `<ul>${d.metrics.map(m => `<li>${ciE(m.name)}: <b>${typeof m.value === 'number' ? fsN(m.value, 2) : ciE(String(m.value))}</b> ${ciE(m.unit || '')}${m.date ? ` <span class="fs-dt">(${ciE(m.date)})</span>` : ''}${fsSrc(m.url)}</li>`).join('')}</ul>` : ''}
      ${(d.quotes || []).map(q => `<blockquote>“${ciE(q.text_vi)}” — <b>${ciE(q.who)}</b>${q.date ? `, ${ciE(q.date)}` : ''}${fsSrc(q.url)}</blockquote>`).join('')}
    </details>`).join('')}</div>
  </div></div>`;

  // ── Soundness ratios ──
  h += `<div class="econ-card full-width" id="fs-safe"><div class="econ-card-header">${fsT('Chỉ số an toàn & hiệu quả toàn hệ thống', 'System soundness & profitability')}</div><div class="econ-card-body">
    <div class="fs-grid2">
      <div><div class="iv-sec">${fsT('Nợ xấu & bao phủ nợ xấu (%)', 'NPLs & coverage (%)')}</div><div id="fs-ch-npl" class="fs-ch"></div></div>
      <div><div class="iv-sec">${fsT('Thanh khoản: LDR & vốn ngắn hạn cho vay trung dài hạn (%)', 'Liquidity: LDR & short-term funds in medium/long-term loans (%)')}</div><div id="fs-ch-liq" class="fs-ch"></div></div>
      <div><div class="iv-sec">${fsT('Vốn: CAR (%)', 'Capital: CAR (%)')}</div><div id="fs-ch-car" class="fs-ch"></div></div>
      <div><div class="iv-sec">${fsT('Sinh lời: ROA & ROE (%)', 'Profitability: ROA & ROE (%)')}</div><div id="fs-ch-roe" class="fs-ch"></div></div>
    </div>
    <div class="iv-src">${fsT('Nguồn: NHNN (thống kê một số chỉ tiêu cơ bản; nợ xấu theo quý), IMF Financial Soundness Indicators (năm). ROA/ROE của NHNN là lũy kế trong năm.', 'Sources: SBV (basic indicators; quarterly NPL), IMF Financial Soundness Indicators (annual). SBV ROA/ROE are year-to-date cumulative.')}</div>
  </div></div>`;
  root.innerHTML = h;
  fsDrawAll();
}

// small SVG line/bar helper on a shared x-axis (labels), several series; dashed = secondary
function fsLines(elId, labels, series, o = {}) {
  _drawSeries(elId, { labels, cur: labels.length - 1, h: o.h || 200, yMin: o.yMin, yMax: o.yMax, fmt: o.fmt || (v => fsN(v, o.dp ?? 1) + (o.unit ?? '%')), axisFmt: o.axisFmt, series: series.map(s => ({ ...s, data: s.data.map(v => v == null || !isFinite(v) ? null : v) })), bars: o.bars, ref: o.ref });
}
function fsDrawAll() {
  const F = FINSYS, A = F.annual, M = F.monthly, yrs = A.years.map(String);
  // levels (trillion VND)
  const tn = a => (a || []).map(v => v == null ? null : v / 1e3);
  fsLines('fs-ch-levels', yrs, [
    { label: 'M2', data: tn(A.m2), color: '#4a3aa7' },
    { label: fsT('Huy động (IMF)', 'Deposits (IMF)'), data: tn(A.deposits), color: '#2a78d6' },
    { label: fsT('Dư nợ (IMF)', 'Loans (IMF)'), data: tn(A.loans_imf_fsi), color: '#e34948' },
  ], { unit: '', dp: 0, axisFmt: v => fsN(v / 1e3, 0) + 'k', yMin: 0 });
  fsLines('fs-ch-growth', yrs, [
    { label: fsT('Tín dụng', 'Credit'), data: A.credit_growth, color: '#e34948' },
    { label: fsT('Huy động', 'Deposits'), data: A.deposit_growth, color: '#2a78d6' },
    { label: 'M2', data: A.m2_growth, color: '#4a3aa7', dash: 1 },
  ], { yMin: 0, dp: 1 });
  // monthly gap
  const mk = M.months.map((m, i) => ({ m, c: M.credit_level[i], d: M.deposits_level[i] })).filter(x => x.c != null && x.d != null);
  fsLines('fs-ch-gap', mk.map(x => x.m), [
    { label: fsT('Dư nợ tín dụng', 'Credit'), data: mk.map(x => x.c / 1e3), color: '#e34948' },
    { label: fsT('Tiền gửi khách hàng', 'Customer deposits'), data: mk.map(x => x.d / 1e3), color: '#2a78d6' },
  ], { unit: '', dp: 0, axisFmt: v => fsN(v / 1e3, 1) + 'k', bars: { label: fsT('Chênh lệch', 'Gap'), color: '#eda100', data: mk.map(x => (x.c - x.d) / 1e3) }, yMin: 0 });
  // depth
  const cg = F.ratios.credit_to_gdp_pct || [];
  const yrs2 = yrs.slice();
  fsLines('fs-ch-depth', yrs2, [
    { label: fsT('Tín dụng tư nhân/GDP (WB)', 'Private credit/GDP (WB)'), data: A.private_credit_to_gdp_pct_wb || [], color: '#e34948' },
    { label: 'M2/GDP (WB)', data: A.m2_to_gdp_pct_wb || [], color: '#4a3aa7' },
    { label: fsT('Tín dụng/GDP (cuối 2025)', 'Credit/GDP (end-2025)'), data: yrs2.map(y => { const r = cg.find(x => String(x.d).startsWith(y)); return r ? r.v : null; }), color: '#eb6834' },
  ], { dp: 0, yMin: 50 });
  // sector growth: 2025 full-year vs 2026 YTD
  const g25 = (fsFlow('Credit growth by sector, 2025') || {}).v || {}, g26 = (fsFlow('Credit growth by sector, 2026') || {}).v || {};
  const SEC = [['agriculture_forestry_fisheries', fsT('Nông, lâm, thủy sản', 'Agriculture')], ['industry', fsT('Công nghiệp', 'Industry')], ['construction', fsT('Xây dựng', 'Construction')], ['trade', fsT('Thương mại', 'Trade')], ['transport_telecom', fsT('Vận tải, viễn thông', 'Transport, telecom')], ['other_services', fsT('Dịch vụ khác (gồm tiêu dùng, BĐS)', 'Other services (incl. consumer, real estate)')], ['total', fsT('Toàn hệ thống', 'Total')]];
  document.getElementById('fs-ch-secgrowth').innerHTML = `<table class="iv-table"><thead><tr><th>${fsT('Ngành', 'Sector')}</th><th class="num">2025</th><th class="num">2026 YTD (T7)</th><th style="width:40%"></th></tr></thead><tbody>${SEC.map(([k, n]) => { const a = g25[k], b = g26[k], mx = 32;
    return `<tr${k === 'total' ? ' style="font-weight:700"' : ''}><td>${n}</td><td class="num">${fsN(a, 1)}%</td><td class="num">${fsN(b, 1)}%</td><td><div class="fs-bar"><i style="width:${Math.min(100, (a || 0) / mx * 100)}%;background:#2a78d6"></i></div><div class="fs-bar"><i style="width:${Math.min(100, (b || 0) / mx * 100)}%;background:#eb6834"></i></div></td></tr>`; }).join('')}</tbody></table><div class="iv-src"><span style="color:#2a78d6">■</span> 2025 · <span style="color:#eb6834">■</span> 2026 YTD</div>`;
  // other channels
  const t = (l, f, sub) => { const x = fsFlow(f); return x ? fsTile(l, typeof x.v === 'number' ? (x.unit === '%' ? fsN(x.v, 1) + '%' : fsTr(x.v)) : ciE(String(x.v)), `${fsMon(x.d)}${sub ? ' · ' + sub : ''}`, x.u) : ''; };
  document.getElementById('fs-flow-tiles').innerHTML =
    t(fsT('Tín dụng BĐS (tổng)', 'Real-estate credit (total)'), 'Real estate credit, total') + t(fsT('Kinh doanh BĐS (Bộ XD)', 'Real-estate business (MoC)'), 'Real-estate BUSINESS credit (MoC') +
    t(fsT('Cho vay mua nhà', 'Home-purchase loans'), 'Home-purchase loans') + t(fsT('Trái phiếu DN lưu hành', 'Corporate bonds outstanding'), 'Corporate bonds outstanding') +
    t(fsT('Cho vay ký quỹ CTCK', 'Securities margin lending'), 'Stock-market lending') + t(fsT('Tiền gửi KBNN tại NHTM', 'State Treasury deposits at banks'), 'State Treasury (KBNN) deposits at banks') +
    t(fsT('Tín dụng DNNVV', 'SME credit'), 'Credit to SMEs') + t(fsT('Tín dụng xanh', 'Green credit'), 'Green credit outstanding');
  fsSankey();
  // why: rates vs CPI (same 24-month window as RATES24 / CPI)
  if (typeof RATES24 !== 'undefined') {
    const real = RATES24.pvt12.map((v, i) => v == null ? null : v - CPI_YOY_24[i]);
    fsLines('fs-ch-rates', CPI_MONTHS_24, [
      { label: fsT('Huy động 12T NH tư nhân (MBS)', '12M deposit, private banks (MBS)'), data: RATES24.pvt12, color: '#eb6834' },
      { label: fsT('Cho vay mới BQ (NHNN)', 'Avg new loan (SBV)'), data: RATES24.lend, color: '#e34948' },
      { label: 'OMO', data: RATES24.omo, color: '#4a3aa7', dash: 1 },
      { label: 'CPI YoY', data: CPI_YOY_24, color: '#008300' },
      { label: fsT('LS thực 12T', 'Real 12M rate'), data: real, color: '#2a78d6', dash: 1 },
    ], { yMin: 0, yMax: 10, dp: 2 });
  }
  const fx = F.why.series.usdvnd_monthly || {}, dxy = F.why.series.dxy_month_end || {}, fed = F.why.series.fed_funds_upper || {};
  if (fx.months) {
    const L = fx.months.map(fsMon), idx = (arr) => { const b = arr.find(v => v != null); return arr.map(v => v == null || !b ? null : v / b * 100); };
    const dx = fx.months.map(m => { const i = (dxy.months || []).indexOf(m); return i >= 0 ? dxy.values[i] : null; });
    fsLines('fs-ch-fx', L, [
      { label: fsT('USD/VND trung tâm (chỉ số)', 'USD/VND central (index)'), data: idx(fx.central), color: '#e34948' },
      { label: fsT('USD/VND bán VCB (chỉ số)', 'USD/VND VCB sell (index)'), data: idx(fx.vcb_sell), color: '#eb6834' },
      { label: fsT('DXY (chỉ số)', 'DXY (index)'), data: idx(dx), color: '#2a78d6', dash: 1 },
    ], { dp: 1, unit: '', axisFmt: v => fsN(v, 0) });
  }
  // soundness
  const ser = k => F.ratios[k] || [];
  const yearly = (k, yrsL) => yrsL.map(y => { const r = ser(k).filter(x => String(x.d).startsWith(y)); return r.length ? r[r.length - 1].v : null; });
  const Y = ['2015', '2016', '2017', '2018', '2019', '2020', '2021', '2022', '2023', '2024', '2025', '2026'];
  fsLines('fs-ch-npl', Y, [
    { label: fsT('Nợ xấu (IMF FSI)', 'NPL (IMF FSI)'), data: yearly('npl_imf_fsi_pct', Y), color: '#e34948' },
    { label: fsT('Nợ xấu nội bảng (NHNN)', 'On-balance NPL (SBV)'), data: yearly('npl_onbalance_sbv_pct', Y), color: '#eb6834' },
    { label: fsT('Gồm VAMC & tiềm ẩn', 'Incl. VAMC & potential'), data: yearly('npl_incl_vamc_and_potential_pct', Y), color: '#4a3aa7', dash: 1 },
  ], { yMin: 0, dp: 2 });
  fsLines('fs-ch-liq', Y, [
    { label: 'LDR (TT22)', data: yearly('ldr_sbv_tt22_pct', Y), color: '#2a78d6' },
    { label: fsT('Vốn NH cho vay TDH', 'ST funds in M/LT loans'), data: yearly('short_term_funds_for_mlt_loans_pct', Y), color: '#eda100' },
  ], { yMin: 0, dp: 1, ref: { v: 85, label: fsT('trần LDR 85%', 'LDR cap 85%'), color: '#e34948' } });
  fsLines('fs-ch-car', Y, [
    { label: 'CAR (IMF FSI)', data: yearly('car_imf_fsi_pct', Y), color: '#1baf7a' },
    { label: fsT('CAR NH áp dụng Basel II (NHNN)', 'CAR Basel-II banks (SBV)'), data: yearly('car_basel2_banks_tt41_pct', Y), color: '#2a78d6', dash: 1 },
  ], { yMin: 0, dp: 1, ref: { v: 8, label: fsT('tối thiểu 8%', 'minimum 8%'), color: '#e34948' } });
  fsLines('fs-ch-roe', Y, [
    { label: 'ROE (IMF)', data: yearly('roe_imf_fsi_pct', Y), color: '#4a3aa7' },
    { label: 'ROE (NHNN)', data: yearly('roe_sbv_pct', Y), color: '#2a78d6', dash: 1 },
    { label: 'ROA (IMF)', data: yearly('roa_imf_fsi_pct', Y), color: '#1baf7a' },
  ], { yMin: 0, dp: 2 });
  fsRenderSbv();
}

// Sankey-style flow: deposit sources → banking system → credit by sector (latest month, trillion VND)
function fsSankey() {
  const el = document.getElementById('fs-sankey'); if (!el) return;
  const F = FINSYS, S = F.sectors, li = S.periods.length - 1, per = fsMon(S.periods[li]);
  const L = k => (S.levels[k] || [])[li];
  const res = fsFlow('Deposits of residents'), org = fsFlow('Deposits of economic organisations');
  const total = L('total');
  const srcs = [[fsT('Tiền gửi dân cư', 'Household deposits'), res ? res.v : 0, '#2a78d6'], [fsT('Tiền gửi tổ chức kinh tế', 'Corporate deposits'), org ? org.v : 0, '#5b9be6']];
  const other = total - srcs.reduce((t, x) => t + x[1], 0);
  srcs.push([fsT('Vốn khác (vốn chủ, GTCG, liên NH, KBNN…)', 'Other funding (equity, paper, interbank, Treasury…)'), Math.max(0, other), '#94a3b8']);
  const uses = [['other_services', fsT('Dịch vụ khác (tiêu dùng, BĐS…)', 'Other services (consumer, real estate…)'), '#e34948'], ['trade', fsT('Thương mại', 'Trade'), '#eb6834'], ['industry', fsT('Công nghiệp', 'Industry'), '#4a3aa7'], ['construction', fsT('Xây dựng', 'Construction'), '#eda100'], ['agriculture_forestry_fisheries', fsT('Nông, lâm, thủy sản', 'Agriculture'), '#008300'], ['transport_telecom', fsT('Vận tải, viễn thông', 'Transport, telecom'), '#1baf7a']].map(([k, n, c]) => [n, L(k) || 0, c]);
  const W = 1000, H = 300, nodeW = 14, gap = 8, x0 = 240, x1 = 493, x2 = 760;
  const sumS = srcs.reduce((t, x) => t + x[1], 0), sumU = uses.reduce((t, x) => t + x[1], 0), tot = Math.max(sumS, sumU);
  const sc = (H - 30 - gap * 5) / tot;
  let g = '';
  // left nodes
  let y = 10; const sPos = srcs.map(([n, v, c]) => { const h = v * sc, p = { n, v, c, y, h }; y += h + gap; return p; });
  y = 10; const uPos = uses.map(([n, v, c]) => { const h = v * sc, p = { n, v, c, y, h }; y += h + gap; return p; });
  const midH = tot * sc, midY = 10 + ((H - 30) - midH) / 2;
  let my = midY;
  sPos.forEach(p => { g += `<path d="M${x0 + nodeW},${p.y} C${(x0 + x1) / 2},${p.y} ${(x0 + x1) / 2},${my} ${x1},${my} L${x1},${my + p.h} C${(x0 + x1) / 2},${my + p.h} ${(x0 + x1) / 2},${p.y + p.h} ${x0 + nodeW},${p.y + p.h} Z" fill="${p.c}" opacity="0.28"/>`; my += p.h; });
  my = midY;
  uPos.forEach(p => { g += `<path d="M${x1 + nodeW},${my} C${(x1 + x2) / 2},${my} ${(x1 + x2) / 2},${p.y} ${x2},${p.y} L${x2},${p.y + p.h} C${(x1 + x2) / 2},${p.y + p.h} ${(x1 + x2) / 2},${my + p.h} ${x1 + nodeW},${my + p.h} Z" fill="${p.c}" opacity="0.28"/>`; my += p.h; });
  sPos.forEach(p => { g += `<rect x="${x0}" y="${p.y}" width="${nodeW}" height="${Math.max(1, p.h)}" fill="${p.c}"/><text x="${x0 - 6}" y="${p.y + p.h / 2 - 2}" text-anchor="end" font-size="11" font-weight="600" fill="#0f172a">${p.n}</text><text x="${x0 - 6}" y="${p.y + p.h / 2 + 11}" text-anchor="end" font-size="10.5" fill="#475569">${fsTr(p.v)} · ${fsN(p.v / sumS * 100, 0)}%</text>`; });
  g += `<rect x="${x1}" y="${midY}" width="${nodeW}" height="${midH}" fill="#0f172a"/><text x="${x1 + nodeW / 2}" y="${midY - 4}" text-anchor="middle" font-size="11" font-weight="700" fill="#0f172a">${fsT('Hệ thống ngân hàng', 'Banking system')}</text><text x="${x1 + nodeW / 2}" y="${midY + midH + 13}" text-anchor="middle" font-size="10.5" fill="#475569">${fsT('dư nợ', 'credit')} ${fsTr(total)}</text>`;
  uPos.forEach(p => { g += `<rect x="${x2}" y="${p.y}" width="${nodeW}" height="${Math.max(1, p.h)}" fill="${p.c}"/><text x="${x2 + nodeW + 6}" y="${p.y + p.h / 2 - 2}" font-size="11" font-weight="600" fill="#0f172a">${p.n}</text><text x="${x2 + nodeW + 6}" y="${p.y + p.h / 2 + 11}" font-size="10.5" fill="#475569">${fsTr(p.v)} · ${fsN(p.v / sumU * 100, 1)}%</text>`; });
  el.innerHTML = `<div class="iv-sec">${fsT('Nguồn vốn → ngân hàng → dư nợ theo ngành', 'Funding → banks → credit by sector')} · ${per}</div><svg viewBox="0 0 ${W} ${H}" width="100%" role="img" aria-label="${fsT('Dòng tiền qua hệ thống ngân hàng', 'Money flows through the banking system')}">${g}</svg>`;
}

// SBV balance sheet & income statement (filled when FINSYS.sbv is available)
function fsRenderSbv() {
  const el = document.getElementById('fs-sbv-body'); if (!el) return;
  const B = FINSYS.sbv;
  if (!B || !B.balance_sheet) { el.innerHTML = `<div class="iv-empty">${fsT('Đang tổng hợp số liệu bảng cân đối NHNN (IMF Central Bank Survey, báo cáo thường niên NHNN)…', 'Compiling SBV balance-sheet data (IMF Central Bank Survey, SBV annual reports)…')}</div>`; return; }
  el.innerHTML = fsSbvHtml(B);
  fsSbvDraw(B);
}
// SBV section renderers — replaced when the SBV balance-sheet data is integrated
function fsSbvHtml(B) { return ''; }
function fsSbvDraw(B) {}
document.addEventListener('click', e => {
  const b = e.target.closest && e.target.closest('[data-fs-go]'); if (!b) return;
  const t = document.getElementById(b.dataset.fsGo); if (!t) return;
  if (t.tagName === 'DETAILS') t.open = true;
  t.scrollIntoView({ behavior: 'smooth', block: 'start' });
});
// ── /FINSYS renderer ──
