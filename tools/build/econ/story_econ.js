// ── STORY renderer (Economy) ──
// Data-journalism layout: each chapter = kicker + headline takeaway + one large annotated visual (+ small multiples).
// Text lives on the charts as annotations; scroll reveals animate marks in (respects prefers-reduced-motion).
// Palette: validated categorical order (#2a78d6 blue, #eb6834 orange, #1baf7a aqua, #eda100 yellow, #e87ba4 magenta,
// #008300 green, #4a3aa7 violet, #e34948 red) on paper #faf8f3; light slots always carry direct labels.
const ST = { blue: '#2a78d6', orange: '#eb6834', aqua: '#1baf7a', yellow: '#eda100', magenta: '#e87ba4', green: '#008300', violet: '#4a3aa7', red: '#e34948', ink: '#16181d', ink2: '#5b6170', ink3: '#8a8f99', rule: '#dedbd2', paper: '#faf8f3', zero: '#efede7' };
const stT = (vi, en) => (typeof APP_LANG !== 'undefined' && APP_LANG === 'en') ? en : vi;
const stN = (v, d = 1) => v == null || !isFinite(v) ? '—' : v.toLocaleString(APP_LOC, { minimumFractionDigits: d, maximumFractionDigits: d });
const stLin = (d0, d1, r0, r1) => v => r0 + (v - d0) / ((d1 - d0) || 1) * (r1 - r0);
const stTip = (e, html) => dotTipShow(e, html);
let stBopYear = null, stBopTimer = null, stBudYear = null;

// scroll reveal: chapters fade/slide in; marks with .st-grow scale from baseline, .st-draw lines draw on
function stObserve(root) {
  const els = root.querySelectorAll('.st-chap');
  if (!('IntersectionObserver' in window) || matchMedia('(prefers-reduced-motion: reduce)').matches) { els.forEach(x => x.classList.add('in')); return; }
  const io = new IntersectionObserver(es => es.forEach(en => { if (en.isIntersecting) { en.target.classList.add('in'); io.unobserve(en.target); } }), { threshold: 0.12 });
  els.forEach(x => io.observe(x));
}
const stChap = (id, kicker, title, dek, body) => `<section class="st-chap" id="${id}"><div class="st-kicker">${kicker}</div><h2 class="st-h">${title}</h2>${dek ? `<p class="st-dek">${dek}</p>` : ''}${body}</section>`;
const stSrc = t => `<div class="st-src">${t}</div>`;

function renderEconStory() {
  const root = document.getElementById('ec-root'); if (!root || typeof ECONFLOW === 'undefined') return;
  const G = ECONFLOW.gdp, gy = G.years.length - 1, yr = G.years[gy];
  const pct = k => { const a = (G.pct_gdp || {})[k]; return Array.isArray(a) ? a[gy] : a; };
  const g9 = ((G.growth_real_pct || {})['9M_2026_yoy'] || {});
  const grw = (ECON_OFFICIAL.gdp_growth || []), gdpUsd = G.GDP[gy] * 1e9 / (FX_USD[15] || 25450) / 1e9;
  let h = '';
  // ── 0. Hero ──
  h += `<section class="st-chap st-hero" id="st-hero"><div class="st-kicker">${stT('Kinh tế Việt Nam · ', 'Vietnam\'s economy · ')}${yr}</div>
    <div class="st-hero-grid"><div>
      <div class="st-big">${stN(G.GDP[gy] / 1e6, 2)}<span>${stT(' triệu tỷ đồng', ' quadrillion VND')}</span></div>
      <p class="st-dek">${stT(`≈ ${stN(gdpUsd, 0)} tỷ USD — tăng <b>${stN(grw[grw.length - 1], 2)}%</b> năm ${yr}, cao nhất từ 2022. Chín tháng đầu 2026 còn nhanh hơn: <b>+${stN(g9.GDP, 2)}%</b>.`, `≈ ${stN(gdpUsd, 0)} bn USD — up <b>${stN(grw[grw.length - 1], 2)}%</b> in ${yr}, the fastest since 2022. The first nine months of 2026 ran even faster: <b>+${stN(g9.GDP, 2)}%</b>.`)}</p>
    </div><div id="st-growth" class="st-viz st-viz-s"></div></div></section>`;
  // ── 1. GDP formula ──
  h += stChap('st-formula', stT('Chương 1 · Công thức GDP', 'Chapter 1 · The GDP formula'),
    stT('Hơn một nửa nền kinh tế là chi tiêu của hộ gia đình', 'Over half of the economy is household spending'),
    stT('GDP = C + I + G + (X − M). Mỗi khối dưới đây có độ rộng đúng bằng tỷ trọng của nó trong GDP.', 'GDP = C + I + G + (X − M). Each block below is drawn to its exact share of GDP.'),
    `<div id="st-eq" class="st-viz"></div><div class="st-sub">${stT('Hai "gã khổng lồ": xuất khẩu và nhập khẩu, so với cả GDP', 'Two giants: exports and imports, against all of GDP')}</div><div id="st-giants" class="st-viz"></div>
     <div class="st-sub">${stT('Cơ cấu thay đổi thế nào, 2015–2025 (% GDP)', 'How the mix has shifted, 2015–2025 (% of GDP)')}</div><div id="st-mix" class="st-viz"></div>
     ${stSrc(stT('Nguồn: NSO — GDP theo mục đích sử dụng, giá hiện hành (2024 sơ bộ, 2025 ước tính); X, M: World Bank.', 'Source: NSO — GDP by expenditure, current prices (2024 preliminary, 2025 estimate); X, M: World Bank.'))}`);
  // ── 2. Balance of payments ──
  h += stChap('st-bop', stT('Chương 2 · Tiền chảy qua biên giới', 'Chapter 2 · Money across the border'),
    stT('Việt Nam bán ra thế giới nhiều hơn mua vào — nhưng phần lớn thặng dư lại chảy ngược ra ngoài', 'Vietnam sells the world more than it buys — yet most of the surplus flows back out'),
    stT('Mỗi dòng có độ dày bằng giá trị (tỷ USD). Bên trái: tiền vào; bên phải: tiền ra; phần còn lại đổ vào dự trữ ngoại hối.', 'Each band is as thick as its value (bn USD). Left: money in; right: money out; what is left lands in FX reserves.'),
    `<div class="st-ctrl" id="st-bop-ctrl"></div><div id="st-sankey" class="st-viz"></div>
     <div class="st-sub">${stT('Bốn dòng quyết định, 2015–2025 (tỷ USD; xanh = vào, đỏ = ra)', 'Four flows that decide it, 2015–2025 (bn USD; blue = in, red = out)')}</div><div id="st-bop-mult" class="st-mult"></div>
     <div id="st-bop-facts" class="st-facts"></div>
     ${stSrc(stT('Nguồn: IMF Balance of Payments (BPM6) theo số NHNN; du lịch: NSO; kiều hối: NHNN/World Bank; lao động: DOLAB. Lợi nhuận FDI chuyển về nước và lãi vay được công bố gộp.', 'Source: IMF Balance of Payments (BPM6) from SBV data; tourism: NSO; remittances: SBV/World Bank; labour: DOLAB. FDI profit repatriation and interest are published combined.'))}`);
  // ── 3. Budget ──
  h += stChap('st-budget', stT('Chương 3 · Ngân sách nhà nước', 'Chapter 3 · The State budget'),
    stT('Cứ 100 đồng Nhà nước thu về, đến từ đâu và đi về đâu?', 'Of every 100 dong the State collects — where does it come from, and where does it go?'),
    stT('Mỗi ô vuông là 1% tổng thu (trái) hoặc tổng chi (phải).', 'Each square is 1% of total revenue (left) or total spending (right).'),
    `<div class="st-ctrl" id="st-bud-ctrl"></div><div class="st-waffles"><div id="st-waf-in"></div><div id="st-waf-out"></div></div>
     <div class="st-sub">${stT('Thu, chi và bội chi, 2018–2026 (nghìn tỷ đồng)', 'Revenue, spending and deficit, 2018–2026 (trillion VND)')}</div><div id="st-bud-line" class="st-viz"></div>
     <div id="st-bud-deep"></div>
     ${stSrc(stT('Nguồn: Nghị quyết Quốc hội & quyết toán NSNN (Bộ Tài chính); 2018–2024 quyết toán, 2025 ước tính, 2026 dự toán. Bộ Tài chính không tách thuế GTGT, TNDN, TTĐB trong thu nội địa.', 'Source: National Assembly resolutions & MoF budget final accounts; 2018–2024 final, 2025 estimate, 2026 plan. MoF does not split VAT, CIT and excise within domestic revenue.'))}`);
  // ── 4. Investment ──
  h += stChap('st-inv', stT('Chương 4 · Ai đang đầu tư?', 'Chapter 4 · Who is investing?'),
    stT('Khu vực tư nhân bỏ vốn nhiều nhất — vốn Nhà nước tăng nhanh trở lại', 'The private sector invests the most — state money is picking up again'),
    stT('Vốn đầu tư thực hiện toàn xã hội theo nguồn (nghìn tỷ đồng).', 'Realised investment by owner (trillion VND).'),
    `<div id="st-inv-area" class="st-viz"></div>${stSrc(stT('Nguồn: NSO V04.01; 9T/2026 tăng: Nhà nước +17,3%, ngoài nhà nước +14,0%, FDI +14,5%.', 'Source: NSO V04.01; 9M-2026 growth: state +17.3%, non-state +14.0%, FDI +14.5%.'))}`);
  // ── 5. Inflation ──
  h += stChap('st-cpi', stT('Chương 5 · Lạm phát', 'Chapter 5 · Inflation'),
    stT('Giá cả nóng lên từ tháng 3/2026 — giao thông và nhà ở dẫn đầu', 'Prices heated up from March 2026 — transport and housing led'),
    stT('Mỗi ô là mức tăng giá so với cùng kỳ năm trước của một nhóm hàng trong một tháng. Đỏ = tăng, xanh = giảm.', 'Each cell is a group\'s year-on-year price change in one month. Red = rising, blue = falling.'),
    `<div id="st-heat" class="st-viz"></div><div class="st-sub">${stT('CPI chung và dự báo đến T12/2027 (vùng tô = khoảng thấp–cao)', 'Headline CPI and projection to Dec-2027 (band = low–high range)')}</div><div id="st-fan" class="st-viz"></div>
     ${stSrc(stT('Nguồn: NSO (Biểu 1 – Cả nước); dự báo: ước tính của dashboard từ dự báo các tổ chức và yếu tố giá.', 'Source: NSO (Biểu 1 – Cả nước); projection: dashboard estimate from institutional forecasts and price drivers.'))}`);
  root.innerHTML = h;
  stGrowth(); stEquation(); stGiants(); stMix(); stBopControls(); stSankey(); stBopMult(); stBopFacts(); stBudControls(); stWaffles(); stBudLine(); stInvArea(); stHeat(); stFan();
  if (typeof stBudgetDeep === 'function') stBudgetDeep();
  stObserve(root);
}

// ── chart helpers ──
const stSvg = (W, H, inner, label) => `<svg viewBox="0 0 ${W} ${H}" width="100%" role="img" aria-label="${ciE(label || '')}">${inner}</svg>`;
const stAnn = (x, y, tx, ty, text, anchor = 'start') => `<g class="st-ann"><path d="M${x},${y} L${tx},${ty}"/><circle cx="${x}" cy="${y}" r="2.5"/><text x="${tx + (anchor === 'end' ? -4 : 4)}" y="${ty + 4}" text-anchor="${anchor}">${text}</text></g>`;
function stAxisY(sy, ticks, x0, x1, fmt) { return ticks.map(t => `<line class="st-grid" x1="${x0}" x2="${x1}" y1="${sy(t)}" y2="${sy(t)}"/><text class="st-tick" x="${x0 - 6}" y="${sy(t) + 4}" text-anchor="end">${fmt(t)}</text>`).join(''); }
const stNice = (lo, hi, n = 4) => { const span = hi - lo || 1, step0 = span / n, mag = Math.pow(10, Math.floor(Math.log10(step0))), step = [1, 2, 2.5, 5, 10].map(m => m * mag).find(s => s >= step0) || step0; const a = Math.floor(lo / step) * step, out = []; for (let v = a; v <= hi + 1e-9; v += step) out.push(+v.toFixed(10)); return out; };

// 0 — growth bars 2011–2025 + 9M-2026
function stGrowth() {
  const el = document.getElementById('st-growth'), g = ECON_OFFICIAL.gdp_growth || [], yrs = g.map((_, i) => 2010 + i).slice(1), vals = g.slice(1);
  const g9 = ((ECONFLOW.gdp.growth_real_pct || {})['9M_2026_yoy'] || {}).GDP; yrs.push('9T/26'); vals.push(g9);
  const W = 520, H = 200, ML = 30, MR = 8, MT = 26, MB = 22, sx = i => ML + i * (W - ML - MR) / vals.length, bw = (W - ML - MR) / vals.length - 4, sy = stLin(0, 10, H - MB, MT);
  let s = stAxisY(sy, [0, 5, 10], ML, W - MR, v => v + '%');
  vals.forEach((v, i) => { const hi = i >= vals.length - 2, c = hi ? ST.blue : '#b9c7da';
    s += `<rect class="st-grow" style="--d:${i * 30}ms" x="${sx(i) + 2}" y="${sy(v)}" width="${bw}" height="${sy(0) - sy(v)}" rx="2" fill="${c}"><title>${yrs[i]}: ${stN(v, 2)}%</title></rect>`;
    if (i % 3 === 0 || hi) s += `<text class="st-tick" x="${sx(i) + 2 + bw / 2}" y="${H - 6}" text-anchor="middle">${String(yrs[i]).replace(/^20/, '')}</text>`; });
  const i20 = yrs.indexOf(2021), last = vals.length - 1;
  s += stAnn(sx(i20) + 2 + bw / 2, sy(vals[i20]) - 2, sx(i20) - 30, sy(9.2), `COVID 2021: ${stN(vals[i20], 2)}%`, 'end');
  s += `<text class="st-lab" x="${sx(last) + bw}" y="${sy(vals[last]) - 6}" text-anchor="end">${stN(vals[last], 2)}%</text>`;
  el.innerHTML = stSvg(W, H, s, stT('Tăng trưởng GDP 2011–2026', 'GDP growth 2011–2026'));
}

// 1a — the equation as one proportional bar
function stEquation() {
  const el = document.getElementById('st-eq'), G = ECONFLOW.gdp, gy = G.years.length - 1, pct = k => { const a = G.pct_gdp[k]; return Array.isArray(a) ? a[gy] : a; };
  const parts = [['C', stT('Hộ gia đình tiêu dùng', 'Household consumption'), pct('C'), ST.blue, G.C[gy]], ['I', stT('Đầu tư', 'Investment'), pct('I'), ST.orange, G.I[gy]], ['G', stT('Nhà nước chi tiêu', 'Government'), pct('G'), ST.violet, G.G[gy]], ['X−M', stT('Xuất khẩu ròng', 'Net exports'), pct('net_exports'), ST.aqua, G.net_exports[gy]], ['ε', stT('sai số', 'discrepancy'), pct('discrepancy'), ST.ink3, G.discrepancy[gy]]];
  const W = 1000, H = 158, x0 = 10, x1 = W - 10, tot = parts.reduce((t, p) => t + p[2], 0), sc = (x1 - x0) / tot;
  let x = x0, s = '';
  parts.forEach(([sym, name, p, c, v], i) => { const w = p * sc;
    s += `<g class="st-grow-x" style="--d:${i * 120}ms"><rect x="${x}" y="40" width="${Math.max(1, w - 2)}" height="64" fill="${c}" rx="3"/>`;
    if (w > 70) s += `<text x="${x + 10}" y="72" class="st-eq-sym">${sym}</text><text x="${x + 10}" y="94" class="st-eq-val">${stN(p, 1)}%</text>`;
    s += `</g>`;
    if (w > 70) s += `<text class="st-lab" x="${x + 10}" y="28">${name}</text><text class="st-tick" x="${x + 10}" y="124">${stN(v / 1e3, 0)} ${stT('nghìn tỷ', 'tn')}</text>`;
    else s += stAnn(x + w / 2, 104, x + w / 2 - 40, i === parts.length - 1 ? 150 : 130, `${sym} ${name} ${stN(p, 1)}%`, 'end');
    x += w; });
  el.innerHTML = stSvg(W, H, s, 'GDP = C + I + G + (X − M)');
}

// 1b — exports and imports against GDP
function stGiants() {
  const el = document.getElementById('st-giants'), G = ECONFLOW.gdp, gy = G.years.length - 1, pct = k => { const a = G.pct_gdp[k]; return Array.isArray(a) ? a[gy] : a; };
  const W = 1000, H = 150, x0 = 140, x1 = W - 60, sx = stLin(0, 100, x0, x1), rows = [[stT('GDP', 'GDP'), 100, ST.ink], [stT('Xuất khẩu (X)', 'Exports (X)'), pct('X'), ST.blue], [stT('Nhập khẩu (M)', 'Imports (M)'), pct('M'), ST.red]];
  let s = '';
  rows.forEach(([n, v, c], i) => { const y = 14 + i * 36;
    s += `<text class="st-lab" x="${x0 - 10}" y="${y + 17}" text-anchor="end">${n}</text><rect class="st-grow-x" style="--d:${i * 150}ms" x="${x0}" y="${y}" width="${sx(v) - x0}" height="24" rx="3" fill="${c}" opacity="${i ? 0.9 : 0.18}"/><text class="st-val" x="${sx(v) + 6}" y="${y + 17}">${stN(v, 0)}%</text>`; });
  const xm = sx(pct('M')), xx = sx(pct('X'));
  s += `<rect x="${xm}" y="50" width="${xx - xm}" height="60" fill="${ST.aqua}" opacity=".35"/>` + stAnn((xm + xx) / 2, 112, (xm + xx) / 2 - 120, 136, stT(`Chênh lệch chỉ ${stN(pct('net_exports'), 1)}% GDP — đó là "X − M" trong công thức`, `The gap is only ${stN(pct('net_exports'), 1)}% of GDP — that is the "X − M" in the formula`), 'end');
  el.innerHTML = stSvg(W, H, s, stT('Xuất nhập khẩu so với GDP', 'Exports and imports vs GDP'));
}

// 1c — mix over time (lines, direct labels)
function stMix() {
  const el = document.getElementById('st-mix'), G = ECONFLOW.gdp, yrs = G.years, P = k => G[k].map((v, i) => v == null ? null : v / G.GDP[i] * 100);
  const S = [['C', stT('Hộ gia đình', 'Households'), ST.blue], ['I', stT('Đầu tư', 'Investment'), ST.orange], ['G', stT('Nhà nước', 'Government'), ST.violet], ['net_exports', 'X − M', ST.aqua]];
  const W = 1000, H = 240, ML = 40, MR = 120, MT = 14, MB = 24, sx = stLin(yrs[0], yrs[yrs.length - 1], ML, W - MR), sy = stLin(-5, 70, H - MB, MT);
  let s = stAxisY(sy, [0, 20, 40, 60], ML, W - MR, v => v + '%');
  yrs.forEach((y, i) => { if (i % 2 === 0 || i === yrs.length - 1) s += `<text class="st-tick" x="${sx(y)}" y="${H - 6}" text-anchor="middle">${y}</text>`; });
  const ends = [];
  S.forEach(([k, n, c]) => { const d = P(k), pts = d.map((v, i) => v == null ? null : `${sx(yrs[i])},${sy(v)}`).filter(Boolean).join(' L');
    s += `<path class="st-draw" d="M${pts}" fill="none" stroke="${c}" stroke-width="2.5" stroke-linejoin="round"/>`;
    ends.push({ y: sy(d[d.length - 1]), t: `${n} ${stN(d[d.length - 1], 1)}%` }); });
  ends.sort((a, b) => a.y - b.y).forEach((e, i, a) => { if (i && e.y - a[i - 1].y < 14) e.y = a[i - 1].y + 14; s += `<text class="st-lab" x="${W - MR + 8}" y="${e.y + 4}">${e.t}</text>`; });
  const i21 = yrs.indexOf(2021); if (i21 >= 0) s += stAnn(sx(2021), sy(P('net_exports')[i21]), sx(2021) + 30, sy(18), stT('2021: nhập khẩu tăng vọt, xuất khẩu ròng gần 0', '2021: imports surge, net exports near zero'));
  el.innerHTML = stSvg(W, H, s, stT('Cơ cấu GDP theo thời gian', 'GDP mix over time'));
}

// 2 — balance of payments sankey
function stBopItems(yi) {
  const S = ECONFLOW.bop.s, v = k => (S[k] || [])[yi] || 0;
  const cdA = v('currency_deposits_assets'), cdL = v('currency_deposits_liabilities'), loans = v('loans_net'), oth = v('other_inv_net') - (loans + cdL - cdA), pf = v('portfolio_net'), eo = v('errors_omissions'), res = v('reserves_change');
  const IN = [[stT('Xuất khẩu hàng hoá', 'Goods exports'), v('goods_x'), ST.blue], [stT('Xuất khẩu dịch vụ (du lịch, vận tải…)', 'Services exports (tourism, transport…)'), v('services_x'), ST.aqua], [stT('FDI vào Việt Nam', 'FDI into Vietnam'), v('fdi_in'), ST.violet], [stT('Kiều hối & chuyển giao nhận về', 'Remittances & transfers in'), v('secondary_income_in'), ST.green], [stT('Thu nhập nhận về', 'Income received'), v('primary_income_in'), ST.yellow], [stT('Tiền gửi của người nước ngoài', 'Non-resident deposits'), cdL, ST.magenta]];
  const OUT = [[stT('Nhập khẩu hàng hoá', 'Goods imports'), v('goods_m'), ST.red], [stT('Nhập khẩu dịch vụ (vận tải, du lịch ra nước ngoài…)', 'Services imports (freight, travel abroad…)'), v('services_m'), ST.orange], [stT('Lợi nhuận FDI & lãi vay chuyển ra', 'FDI profits & interest paid out'), v('primary_income_out'), ST.violet], [stT('Ngoại tệ, tiền gửi chuyển ra/giữ ngoài', 'FX cash & deposits moved out'), cdA, ST.magenta], [stT('Chuyển giao trả ra', 'Transfers paid out'), v('secondary_income_out'), ST.yellow], [stT('Đầu tư ra nước ngoài', 'Outward investment'), v('fdi_out'), ST.aqua]];
  const flip = (val, a, b, c) => { if (Math.abs(val) < 0.05) return; (val >= 0 ? IN : OUT).push([val >= 0 ? a : b, Math.abs(val), c]); };
  flip(pf, stT('Vốn gián tiếp vào', 'Portfolio inflows'), stT('Khối ngoại rút vốn', 'Foreign portfolio exit'), ST.blue);
  flip(loans, stT('Vay nước ngoài ròng', 'Net foreign borrowing'), stT('Trả nợ nước ngoài ròng', 'Net debt repayment'), ST.green);
  flip(oth, stT('Đầu tư khác vào', 'Other investment in'), stT('Đầu tư khác ra', 'Other investment out'), ST.ink3);
  flip(eo, stT('Lỗi & sai sót (vào)', 'Errors & omissions (in)'), stT('Lỗi & sai sót: thất thoát chưa ghi nhận', 'Errors & omissions: unrecorded outflow'), '#9aa0aa');
  if (res >= 0) OUT.push([stT('→ Tăng dự trữ ngoại hối', '→ Added to FX reserves'), res, ST.ink]); else IN.push([stT('← Rút dự trữ ngoại hối', '← Drawn from FX reserves'), -res, ST.ink]);
  return { IN: IN.filter(x => x[1] > 0.05).sort((a, b) => b[1] - a[1]), OUT: OUT.filter(x => x[1] > 0.05).sort((a, b) => b[1] - a[1]), res, ca: v('current_account') };
}
function stBopControls() {
  const ys = ECONFLOW.bop.years, cur = stBopYear ?? ys[ys.length - 1];
  document.getElementById('st-bop-ctrl').innerHTML = `<button type="button" class="st-play" data-st-play="bop" aria-label="${stT('Chạy qua các năm', 'Play through years')}">${stBopTimer ? '❚❚' : '▶'}</button>` + ys.map(y => `<button type="button" data-st-bop="${y}" class="${y === cur ? 'on' : ''}">${y}</button>`).join('');
}
function stSankey() {
  const el = document.getElementById('st-sankey'), ys = ECONFLOW.bop.years, yr = stBopYear ?? ys[ys.length - 1], yi = ys.indexOf(yr), B = stBopItems(yi);
  const sIn = B.IN.reduce((t, x) => t + x[1], 0), sOut = B.OUT.reduce((t, x) => t + x[1], 0), tot = Math.max(sIn, sOut);
  const W = 1000, H = 470, gap = 6, xL = 300, xC = 488, xR = 690, nw = 12, avail = H - 30 - gap * Math.max(B.IN.length, B.OUT.length), k = avail / tot;
  let s = '', y = 20;
  const lay = (arr, x) => { let yy = 20; return arr.map(([n, v, c]) => { const h = Math.max(1.5, v * k), o = { n, v, c, y: yy, h, x }; yy += h + gap; return o; }); };
  const L = lay(B.IN), R = lay(B.OUT), cH = tot * k, cY = 20 + ((H - 40) - cH) / 2;
  let cy = cY;
  L.forEach((p, i) => { s += `<path class="st-band" style="--d:${i * 70}ms" d="M${xL + nw},${p.y} C${(xL + xC) / 2},${p.y} ${(xL + xC) / 2},${cy} ${xC},${cy} L${xC},${cy + p.h} C${(xL + xC) / 2},${cy + p.h} ${(xL + xC) / 2},${p.y + p.h} ${xL + nw},${p.y + p.h} Z" fill="${p.c}" opacity=".32" data-st-k="${encodeURIComponent(p.n + '|' + p.v)}"/>`; cy += p.h; });
  cy = cY;
  R.forEach((p, i) => { s += `<path class="st-band" style="--d:${300 + i * 70}ms" d="M${xC + nw},${cy} C${(xC + xR) / 2},${cy} ${(xC + xR) / 2},${p.y} ${xR},${p.y} L${xR},${p.y + p.h} C${(xC + xR) / 2},${p.y + p.h} ${(xC + xR) / 2},${cy + p.h} ${xC + nw},${cy + p.h} Z" fill="${p.c}" opacity=".32" data-st-k="${encodeURIComponent(p.n + '|' + p.v)}"/>`; cy += p.h; });
  const lab = (p, side) => { const tx = side === 'L' ? p.x - 8 : p.x + nw + 8, a = side === 'L' ? 'end' : 'start', cyy = p.y + p.h / 2;
    return `<rect x="${p.x}" y="${p.y}" width="${nw}" height="${p.h}" fill="${p.c}"/>` + (p.h > 9 || p.v > 4 ? `<text class="st-lab" x="${tx}" y="${cyy + (p.h > 22 ? -2 : 4)}" text-anchor="${a}">${p.n}</text>${p.h > 22 ? `<text class="st-val" x="${tx}" y="${cyy + 13}" text-anchor="${a}">${stN(p.v, 1)}</text>` : ''}` : ''); };
  let ly = -1e9; L.forEach(p => { if (p.y + p.h / 2 - ly < 13 && p.h <= 9) return; s += lab(p, 'L'); ly = p.y + p.h / 2; });
  ly = -1e9; R.forEach(p => { if (p.y + p.h / 2 - ly < 13 && p.h <= 9) return; s += lab(p, 'R'); ly = p.y + p.h / 2; });
  s += `<rect x="${xC}" y="${cY}" width="${nw}" height="${cH}" fill="${ST.ink}"/><text class="st-lab" x="${xC + nw / 2}" y="${cY - 8}" text-anchor="middle">${stT('Việt Nam', 'Vietnam')} ${yr}</text>`;
  s += `<text class="st-tick" x="${xL}" y="12" text-anchor="end">${stT('TIỀN VÀO', 'MONEY IN')} · ${stN(sIn, 0)}</text><text class="st-tick" x="${xR + nw}" y="12">${stT('TIỀN RA', 'MONEY OUT')} · ${stN(sOut, 0)} ${stT('tỷ USD', 'bn USD')}</text>`;
  el.innerHTML = stSvg(W, H, s, stT('Dòng tiền vào ra Việt Nam', 'Money flows in and out of Vietnam'));
}
function stBopMult() {
  const S = ECONFLOW.bop.s, ys = ECONFLOW.bop.years, cur = stBopYear ?? ys[ys.length - 1];
  const M = [['current_account', stT('Cán cân vãng lai', 'Current account')], ['fdi_net', stT('FDI ròng', 'Net FDI')], ['errors_omissions', stT('Lỗi & sai sót (thất thoát)', 'Errors & omissions (leakage)')], ['reserves_change', stT('Thay đổi dự trữ', 'Reserves change')]];
  document.getElementById('st-bop-mult').innerHTML = M.map(([k, n]) => { const d = S[k] || [], mx = Math.max(...d.map(v => Math.abs(v || 0))) || 1, W = 240, H = 110, z = 60, sh = 46 / mx, bw = W / d.length;
    const bars = d.map((v, i) => v == null ? '' : `<rect x="${i * bw + 1}" y="${v >= 0 ? z - v * sh : z}" width="${bw - 2}" height="${Math.max(1, Math.abs(v) * sh)}" rx="1.5" fill="${v >= 0 ? ST.blue : ST.red}" opacity="${ys[i] === cur ? 1 : 0.45}"><title>${ys[i]}: ${stN(v, 1)}</title></rect>`).join('');
    const ci = ys.indexOf(cur);
    return `<div class="st-mini"><div class="st-mini-h">${n}</div><div class="st-mini-v">${stN(d[ci], 1)}</div><svg viewBox="0 0 ${W} ${H}" width="100%"><line x1="0" x2="${W}" y1="${z}" y2="${z}" stroke="${ST.rule}"/>${bars}<text class="st-tick" x="0" y="${H - 4}">${ys[0]}</text><text class="st-tick" x="${W}" y="${H - 4}" text-anchor="end">${ys[ys.length - 1]}</text></svg></div>`; }).join('');
}
function stBopFacts() {
  const T = ECONFLOW.tour, R = ECONFLOW.remit, L = ECONFLOW.labour, ti = T.years.length - 1;
  const spark = (arr, c) => { const a = (arr || []).map(v => typeof v === 'number' ? v : null), mx = Math.max(...a.filter(v => v != null)), W = 160, H = 36; let p = ''; a.forEach((v, i) => { if (v == null) return; p += `${p ? 'L' : 'M'}${(i / (a.length - 1) * W).toFixed(1)},${(H - 2 - v / mx * (H - 6)).toFixed(1)}`; }); return `<svg viewBox="0 0 ${W} ${H}" width="100%" height="36"><path d="${p}" fill="none" stroke="${c}" stroke-width="2"/></svg>`; };
  const remit = R.national_wb_knomad_busd.map((v, i) => v ?? R.national_sbv_busd[i] ?? R.national_wb_projection_busd[i]);
  document.getElementById('st-bop-facts').innerHTML = [
    [stT('Kiều hối', 'Remittances'), `${stN(remit.filter(v => v != null).slice(-1)[0], 1)} ${stT('tỷ USD', 'bn USD')}`, stT('2015 → 2025 (NHNN/WB)', '2015 → 2025 (SBV/WB)'), spark(remit, ST.green)],
    [stT('Lao động đi làm ở nước ngoài', 'Workers sent abroad'), stN(L.workers_sent.filter(v => v != null).slice(-1)[0], 0), stT('người, 2025 · gửi về ~6,5–7 tỷ USD/năm', 'in 2025 · send home ~6.5–7 bn USD a year'), spark(L.workers_sent, ST.violet)],
    [stT('Khách quốc tế 9T/2026', 'Foreign visitors 9M-2026'), `${stN(T.intl_arrivals_thousand[ti] / 1e3, 1)} ${stT('triệu', 'mn')}`, stT(`người Việt đi nước ngoài ${stN(T.vietnamese_outbound_departures_thousand[ti] / 1e3, 1)} triệu`, `Vietnamese outbound ${stN(T.vietnamese_outbound_departures_thousand[ti] / 1e3, 1)} mn`), spark(T.intl_arrivals_thousand.slice(0, -1), ST.aqua)],
  ].map(([h4, v, sub, sp]) => `<div class="st-fact"><div class="st-mini-h">${h4}</div><div class="st-mini-v">${v}</div>${sp}<div class="st-fact-s">${sub}</div></div>`).join('');
}

// 3 — budget waffles + line
function stBudgetParts(yi) {
  const B = ECONFLOW.budget, R = B.revenue, X = B.expenditure, g = (o, k) => (o[k] || [])[yi] || 0;
  const known = ['pit', 'land_use_fees', 'env_protection', 'lottery', 'fees_charges'].reduce((t, k) => t + g(R, k), 0);
  const IN = [[stT('Thuế khác trong nước (GTGT, TNDN…)', 'Other domestic taxes (VAT, CIT…)'), g(R, 'domestic') - known, ST.blue], [stT('Tiền sử dụng đất', 'Land-use fees'), g(R, 'land_use_fees'), ST.orange], [stT('Xuất nhập khẩu', 'Import-export'), g(R, 'import_export'), ST.aqua], [stT('Thuế TNCN', 'Personal income tax'), g(R, 'pit'), ST.violet], [stT('Phí, lệ phí, xổ số, BVMT', 'Fees, lottery, env. tax'), g(R, 'fees_charges') + g(R, 'lottery') + g(R, 'env_protection'), ST.yellow], [stT('Dầu thô', 'Crude oil'), g(R, 'crude_oil'), ST.ink3], [stT('Viện trợ', 'Grants'), g(R, 'grants'), ST.green]];
  const knownX = ['development_investment', 'recurrent', 'interest'].reduce((t, k) => t + g(X, k), 0);
  const OUT = [[stT('Chi thường xuyên (lương, an sinh, y tế, giáo dục…)', 'Recurrent (salaries, social, health, education…)'), g(X, 'recurrent'), ST.violet], [stT('Đầu tư phát triển', 'Development investment'), g(X, 'development_investment'), ST.orange], [stT('Trả lãi vay', 'Interest'), g(X, 'interest'), ST.red], [stT('Khác (dự phòng, viện trợ, chi từ tăng thu…)', 'Other (reserve, aid, overrun-funded…)'), Math.max(0, g(X, 'total') - knownX), ST.ink3]];
  return { IN: IN.filter(x => x[1] > 0), OUT: OUT.filter(x => x[1] > 0), rev: g(R, 'total'), exp: g(X, 'total'), def: (B.deficit || [])[yi], basis: (B.basis || [])[yi] };
}
function stBudControls() {
  const ys = ECONFLOW.budget.years, cur = stBudYear ?? 2025;
  document.getElementById('st-bud-ctrl').innerHTML = ys.map(y => `<button type="button" data-st-bud="${y}" class="${y === cur ? 'on' : ''}">${y}</button>`).join('');
}
function stWaffle(el, parts, total, title, color0) {
  const cnt = parts.map(p => p[1] / total * 100), fl = cnt.map(Math.floor); let rem = 100 - fl.reduce((t, v) => t + v, 0);
  cnt.map((v, i) => [v - fl[i], i]).sort((a, b) => b[0] - a[0]).forEach(([, i]) => { if (rem > 0) { fl[i]++; rem--; } });
  const cells = []; fl.forEach((n, i) => { for (let k = 0; k < n; k++) cells.push(i); });
  const C = 22, G2 = 3, W = 10 * (C + G2), H = 10 * (C + G2);
  let s = cells.map((pi, k) => { const r = Math.floor(k / 10), c = k % 10; return `<rect class="st-cell" style="--d:${k * 6}ms" x="${c * (C + G2)}" y="${r * (C + G2)}" width="${C}" height="${C}" rx="3" fill="${parts[pi][2]}" data-st-k="${encodeURIComponent(parts[pi][0] + '|' + stN(parts[pi][1] / 1e3, 0) + ' ' + stT('nghìn tỷ', 'tn VND') + ' · ' + stN(cnt[pi], 1) + '%')}"/>`; }).join('');
  el.innerHTML = `<div class="st-waf-h">${title}</div><div class="st-waf-row"><svg viewBox="0 0 ${W} ${H}" width="${W}" style="max-width:100%">${s}</svg><ul class="st-leg">${parts.map((p, i) => `<li><i style="background:${p[2]}"></i><b>${fl[i]}</b> ${p[0]}</li>`).join('')}</ul></div>`;
}
function stWaffles() {
  const ys = ECONFLOW.budget.years, yi = ys.indexOf(stBudYear ?? 2025), P = stBudgetParts(yi), bl = { final: stT('quyết toán', 'final'), estimate: stT('ước tính', 'estimate'), plan: stT('dự toán', 'plan') }[P.basis] || '';
  stWaffle(document.getElementById('st-waf-in'), P.IN, P.rev, `${stT('Thu', 'Revenue')} ${stN(P.rev / 1e3, 0)} ${stT('nghìn tỷ', 'tn VND')} <span>(${bl})</span>`);
  stWaffle(document.getElementById('st-waf-out'), P.OUT, P.exp, `${stT('Chi', 'Spending')} ${stN(P.exp / 1e3, 0)} ${stT('nghìn tỷ', 'tn VND')} · ${stT('bội chi', 'deficit')} ${stN((P.def || 0) / 1e3, 0)}`);
}
function stBudLine() {
  const el = document.getElementById('st-bud-line'), B = ECONFLOW.budget, ys = B.years, t = a => (a || []).map(v => v == null ? null : v / 1e3);
  const rev = t(B.revenue.total), exp = t(B.expenditure.total), def = t(B.deficit), dev = t(B.expenditure.development_investment);
  const W = 1000, H = 260, ML = 50, MR = 150, MT = 14, MB = 26, sx = stLin(ys[0], ys[ys.length - 1], ML, W - MR), sy = stLin(0, 3500, H - MB, MT);
  let s = stAxisY(sy, [0, 1000, 2000, 3000], ML, W - MR, v => stN(v, 0));
  ys.forEach((y, i) => { s += `<text class="st-tick" x="${sx(y)}" y="${H - 6}" text-anchor="middle">${y}</text>`; if (def[i] != null) s += `<rect class="st-grow" style="--d:${i * 50}ms" x="${sx(y) - 9}" y="${sy(def[i])}" width="18" height="${sy(0) - sy(def[i])}" rx="2" fill="${ST.yellow}"><title>${y}: ${stT('bội chi', 'deficit')} ${stN(def[i], 0)}</title></rect>`; });
  const line = (d, c, w, dash) => `<path class="st-draw" d="M${d.map((v, i) => v == null ? null : `${sx(ys[i])},${sy(v)}`).filter(Boolean).join(' L')}" fill="none" stroke="${c}" stroke-width="${w}" ${dash ? 'stroke-dasharray="5,4"' : ''}/>`;
  s += line(rev, ST.aqua, 2.5) + line(exp, ST.red, 2.5) + line(dev, ST.orange, 2, 1);
  const L = ys.length - 1, lab = (d, n, dy = 0) => `<text class="st-lab" x="${W - MR + 8}" y="${sy(d[L]) + 4 + dy}">${n} ${stN(d[L], 0)}</text>`;
  s += lab(rev, stT('Thu', 'Revenue'), 0) + lab(exp, stT('Chi', 'Spending'), -10) + lab(dev, stT('Đầu tư PT', 'Dev. invest.'), 0) + `<text class="st-lab" x="${W - MR + 8}" y="${sy(def[L]) + 4}">${stT('Bội chi', 'Deficit')} ${stN(def[L], 0)}</text>`;
  const i25 = ys.indexOf(2025); if (i25 >= 0) s += stAnn(sx(2025), sy(rev[i25]), sx(2025) - 60, sy(3300), stT('2025: thu vượt mạnh nhờ tiền đất', '2025: revenue surged on land fees'), 'end');
  el.innerHTML = stSvg(W, H, s, stT('Thu chi ngân sách', 'Budget revenue and spending'));
}

// 4 — investment by owner (stacked area)
function stInvArea() {
  const el = document.getElementById('st-inv-area'), I = ECONFLOW.inv, ys = I.years.filter(y => typeof y === 'number'), n = ys.length;
  const S = [['nonstate', stT('Ngoài nhà nước', 'Non-state'), ST.blue], ['state', stT('Nhà nước', 'State'), ST.red], ['fdi', 'FDI', ST.yellow]];
  const val = (k, i) => (I[k] || [])[i] / 1e3 || 0, tot = i => S.reduce((t, s) => t + val(s[0], i), 0), mx = Math.max(...ys.map((_, i) => tot(i)));
  const W = 1000, H = 260, ML = 50, MR = 150, MT = 14, MB = 26, sx = stLin(ys[0], ys[n - 1], ML, W - MR), sy = stLin(0, mx * 1.08, H - MB, MT);
  let s = stAxisY(sy, stNice(0, mx, 4).filter(v => v <= mx * 1.08), ML, W - MR, v => stN(v, 0)), base = ys.map(() => 0);
  S.forEach(([k, nm, c], si) => { const top = ys.map((_, i) => base[i] + val(k, i));
    const d = `M${ys.map((y, i) => `${sx(y)},${sy(top[i])}`).join(' L')} L${ys.map((y, i) => `${sx(y)},${sy(base[i])}`).reverse().join(' L')} Z`;
    s += `<path class="st-fade" style="--d:${si * 150}ms" d="${d}" fill="${c}" opacity=".85" stroke="${ST.paper}" stroke-width="1.5"/>`;
    const mid = (top[n - 1] + base[n - 1]) / 2; s += `<text class="st-lab" x="${W - MR + 8}" y="${sy(mid) + 4}">${nm} ${stN(val(k, n - 1) / tot(n - 1) * 100, 0)}%</text>`;
    base = top; });
  ys.forEach((y, i) => { if (i % 2 === 0 || i === n - 1) s += `<text class="st-tick" x="${sx(y)}" y="${H - 6}" text-anchor="middle">${y}</text>`; });
  el.innerHTML = stSvg(W, H, s, stT('Vốn đầu tư theo nguồn', 'Investment by owner'));
}

// 5 — CPI heat strip + fan chart
function stHeat() {
  const el = document.getElementById('st-heat'), M = CPI_MONTHS_24, trg = x => (APP_LANG === 'en' && window.I18N ? window.I18N.tr(x) || x : x), rows = CPI_BASKET.map(g => [trg(g.name), g.yoy]).concat([[stT('CPI chung', 'Headline CPI'), CPI_YOY_24]]);
  const W = 1000, LW = 230, CW = (W - LW - 50) / M.length, RH = 20, H = rows.length * (RH + 2) + 30;
  const col = v => { if (v == null) return '#f3f2ee'; const a = Math.min(1, Math.abs(v) / 8); const mix = (c1, c2, t) => '#' + [0, 2, 4].map(k => Math.round(parseInt(c1.slice(k + 1, k + 3), 16) * (1 - t) + parseInt(c2.slice(k + 1, k + 3), 16) * t).toString(16).padStart(2, '0')).join(''); return v >= 0 ? mix('#efede7', '#c2362f', a) : mix('#efede7', '#1c5cab', a); };
  let s = '';
  rows.forEach(([n, a], r) => { const y = r * (RH + 2), last = r === rows.length - 1;
    s += `<text class="st-lab${last ? ' st-b' : ''}" x="${LW - 8}" y="${y + 14}" text-anchor="end">${ciE(n)}</text>`;
    a.forEach((v, i) => { s += `<rect class="st-cell" style="--d:${(r + i) * 8}ms" x="${LW + i * CW}" y="${y}" width="${CW - 1.5}" height="${RH}" rx="2" fill="${col(v)}" data-st-k="${encodeURIComponent(n + ' · ' + M[i] + '|' + (v == null ? '—' : stN(v, 2) + '%'))}"/>`; });
    const lv = a[a.length - 1]; s += `<text class="st-val" x="${LW + M.length * CW + 6}" y="${y + 14}">${lv == null ? '—' : stN(lv, 1) + '%'}</text>`; });
  M.forEach((m, i) => { if (i % 3 === 0 || i === M.length - 1) s += `<text class="st-tick" x="${LW + i * CW + CW / 2}" y="${H - 8}" text-anchor="middle">${m}</text>`; });
  el.innerHTML = stSvg(W, H, s, stT('Bản đồ nhiệt lạm phát theo nhóm', 'Inflation heat map by group'));
}
function stFan() {
  const el = document.getElementById('st-fan'); if (typeof cpiForecast !== 'function') return;
  const F = cpiForecast(), A = CPI_YOY_24, L = CPI_MONTHS_24.concat(F.central.labels), n = L.length, na = A.length;
  const W = 1000, H = 250, ML = 40, MR = 110, MT = 16, MB = 26, sx = i => ML + i / (n - 1) * (W - ML - MR), sy = stLin(0, 8, H - MB, MT);
  let s = stAxisY(sy, [0, 2, 4, 6, 8], ML, W - MR, v => v + '%');
  s += `<rect x="${sx(na - 1)}" y="${MT}" width="${W - MR - sx(na - 1)}" height="${H - MT - MB}" fill="#f1efe8"/>`;
  s += `<line x1="${ML}" x2="${W - MR}" y1="${sy(4.5)}" y2="${sy(4.5)}" stroke="${ST.green}" stroke-dasharray="4,3"/><text class="st-tick" x="${ML + 4}" y="${sy(4.5) - 5}">${stT('mục tiêu 2026 ~4,5%', '2026 target ~4.5%')}</text>`;
  const lo = [A[na - 1], ...F.low.yoy], hi = [A[na - 1], ...F.high.yoy], ce = [A[na - 1], ...F.central.yoy];
  s += `<path class="st-fade" d="M${hi.map((v, i) => `${sx(na - 1 + i)},${sy(v)}`).join(' L')} L${lo.map((v, i) => `${sx(na - 1 + i)},${sy(v)}`).reverse().join(' L')} Z" fill="${ST.orange}" opacity=".18"/>`;
  s += `<path class="st-draw" d="M${A.map((v, i) => `${sx(i)},${sy(v)}`).join(' L')}" fill="none" stroke="${ST.ink}" stroke-width="2.5"/>`;
  s += `<path class="st-draw" d="M${ce.map((v, i) => `${sx(na - 1 + i)},${sy(v)}`).join(' L')}" fill="none" stroke="${ST.orange}" stroke-width="2.5" stroke-dasharray="6,4"/>`;
  L.forEach((m, i) => { if (i % 4 === 0 || i === n - 1) s += `<text class="st-tick" x="${sx(i)}" y="${H - 6}" text-anchor="middle">${m}</text>`; });
  s += `<text class="st-lab" x="${W - MR + 8}" y="${sy(ce[ce.length - 1]) + 4}">${stT('Cơ sở', 'Base')} ${stN(ce[ce.length - 1], 1)}%</text>`;
  const im = A.indexOf(Math.max(...A)); s += stAnn(sx(im), sy(A[im]), sx(im) - 50, sy(7.4), `${CPI_MONTHS_24[im]}: ${stN(A[im], 2)}%`, 'end');
  s += stAnn(sx(na + 3), sy(F.central.yoy[3]), sx(na + 3) + 20, sy(7.2), stT('T1/27: hết giảm thuế GTGT', 'Jan-27: VAT cut expires'));
  el.innerHTML = stSvg(W, H, s, stT('CPI và dự báo', 'CPI and projection'));
}

// interactions
document.addEventListener('click', e => {
  const b = e.target.closest && e.target.closest('[data-st-bop]');
  if (b) { stBopYear = +b.dataset.stBop; stBopControls(); stSankey(); stBopMult(); return; }
  const p = e.target.closest && e.target.closest('[data-st-play="bop"]');
  if (p) { if (stBopTimer) { clearInterval(stBopTimer); stBopTimer = null; stBopControls(); return; }
    const ys = ECONFLOW.bop.years; let i = 0; stBopTimer = setInterval(() => { stBopYear = ys[i++]; stBopControls(); stSankey(); stBopMult(); if (i >= ys.length) { clearInterval(stBopTimer); stBopTimer = null; stBopControls(); } }, 1100); stBopControls(); return; }
  const d = e.target.closest && e.target.closest('[data-st-bud]');
  if (d) { stBudYear = +d.dataset.stBud; stBudControls(); stWaffles(); if (typeof stBudgetDeep === 'function') stBudgetDeep(); }
});
document.addEventListener('mouseover', e => { const k = e.target.closest && e.target.closest('[data-st-k]'); if (!k) return; const [a, b] = decodeURIComponent(k.dataset.stK).split('|'); stTip(e, `<div class="dt-t">${ciE(a)}</div><div class="dt-s">${ciE(isNaN(+b) ? b : stN(+b, 1) + stT(' tỷ USD', ' bn USD'))}</div>`); });
document.addEventListener('mousemove', e => { if (e.target.closest && e.target.closest('[data-st-k]')) dotTipMove(e); });
document.addEventListener('mouseout', e => { if (e.target.closest && e.target.closest('[data-st-k]')) dotTipHide(); });
// ── /STORY renderer ──
