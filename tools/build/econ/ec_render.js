// ── ECON renderer ──
// Economy tab header: GDP identity (C + I + G + X − M), Vietnam's "revenues and costs" with the rest of the world (balance of
// payments) and the State budget ledger. Data: ECONFLOW (NSO PxWeb, IMF BOP, World Bank, MoF, DOLAB; research 05/10/2026).
const ecT = (vi, en) => (typeof APP_LANG !== 'undefined' && APP_LANG === 'en') ? en : vi;
const ecN = (v, d = 1) => v == null || !isFinite(v) ? '—' : v.toLocaleString(APP_LOC, { minimumFractionDigits: d, maximumFractionDigits: d });
const ecTn = bn => bn == null ? '—' : fsN(bn / 1e3, 0) + ecT(' nghìn tỷ', ' tn VND');   // input billion VND
let ecBopYear = null, ecBudYear = null;

function renderEconTop() {
  const root = document.getElementById('ec-root'); if (!root || typeof ECONFLOW === 'undefined') return;
  const G = ECONFLOW.gdp, gy = G.years.length - 1, yr = G.years[gy], pct = G.pct_gdp || {};
  const gr = (G.growth_real_pct || {})['9M_2026_yoy'] || {};
  const term = (sym, vi, en, k, sub, cls) => `<div class="ec-term ${cls || ''}"><div class="ec-sym">${sym}</div><div class="ec-tname">${ecT(vi, en)}</div><div class="ec-tval">${ecTn(G[k][gy])}</div><div class="ec-tpct">${ecN(pct[k], 1)}% GDP</div>${sub ? `<div class="ec-tsub">${sub}</div>` : ''}</div>`;
  let h = `<div class="econ-card full-width" id="ec-formula"><div class="econ-card-header">${ecT('Công thức GDP — GDP được tạo thành từ đâu', 'The GDP formula — where GDP comes from')} <span class="pn-label" style="font-family:var(--font-ui)">· ${yr} (${ecT('NSO ước tính', 'NSO estimate')})</span></div><div class="econ-card-body">
    <div class="ec-formula">
      <div class="ec-term ec-gdp"><div class="ec-sym">GDP</div><div class="ec-tname">${ecT('Tổng sản phẩm trong nước', 'Gross domestic product')}</div><div class="ec-tval">${ecTn(G.GDP[gy])}</div><div class="ec-tpct">${ecT('tăng trưởng 9T/2026', '9M-2026 growth')} ${ecN(gr.GDP, 2)}%</div></div>
      <div class="ec-op">=</div>${term('C', 'Tiêu dùng hộ gia đình', 'Household consumption', 'C')}
      <div class="ec-op">+</div>${term('I', 'Đầu tư (tích luỹ tài sản)', 'Investment (capital formation)', 'I', `${ecT('9T/2026', '9M-26')} +${ecN(gr.gross_capital_formation, 1)}%`)}
      <div class="ec-op">+</div>${term('G', 'Chi tiêu của Nhà nước', 'Government consumption', 'G')}
      <div class="ec-op">+</div><div class="ec-term ec-nx"><div class="ec-sym">(X − M)</div><div class="ec-tname">${ecT('Xuất khẩu ròng', 'Net exports')}</div><div class="ec-tval">${ecTn(G.net_exports[gy])}</div><div class="ec-tpct">${ecN(pct.net_exports, 1)}% GDP</div><div class="ec-tsub">X ${ecN(pct.X, 0)}% · M ${ecN(pct.M, 0)}% GDP</div></div>
      <div class="ec-op">+</div>${term('ε', 'Sai số thống kê', 'Statistical discrepancy', 'discrepancy', '', 'ec-small')}
    </div>
    <div class="fs-grid2">
      <div><div class="iv-sec">${ecT('Cơ cấu GDP theo mục đích sử dụng (% GDP)', 'GDP by expenditure (% of GDP)')}</div><div id="ec-ch-mix" class="fs-ch"></div></div>
      <div><div class="iv-sec">${ecT('Đầu tư thực hiện toàn xã hội theo nguồn (nghìn tỷ đồng)', 'Realised investment by owner (trillion VND)')}</div><div id="ec-ch-inv" class="fs-ch"></div></div>
    </div>
    <div class="iv-src">${ecT(`Nguồn: NSO, GDP theo giá hiện hành phân theo mục đích sử dụng (PxWeb V03.08; 2024 sơ bộ, 2025 ước tính); X, M: World Bank (theo NSO). Tăng trưởng thực 9T/2026: tiêu dùng cuối cùng +${ecN(gr.final_consumption, 2)}%, tích luỹ tài sản +${ecN(gr.gross_capital_formation, 2)}%, xuất khẩu +${ecN(gr.exports_gs, 2)}%, nhập khẩu +${ecN(gr.imports_gs, 2)}%. Vốn đầu tư thực hiện (V04.01) khác khái niệm với tích luỹ tài sản (I).`, `Sources: NSO, GDP at current prices by expenditure (PxWeb V03.08; 2024 preliminary, 2025 estimate); X, M: World Bank (from NSO). Real growth 9M-2026: final consumption +${ecN(gr.final_consumption, 2)}%, capital formation +${ecN(gr.gross_capital_formation, 2)}%, exports +${ecN(gr.exports_gs, 2)}%, imports +${ecN(gr.imports_gs, 2)}%. Realised investment (V04.01) is a different concept from capital formation (I).`)}</div>
  </div></div>`;
  h += `<div class="econ-card full-width" id="ec-bop"><div class="econ-card-header">${ecT('Thu – chi của Việt Nam với thế giới (cán cân thanh toán)', "Vietnam's revenues & costs with the rest of the world (balance of payments)")}</div><div class="econ-card-body" id="ec-bop-body"></div></div>`;
  h += `<div class="econ-card full-width" id="ec-bud"><div class="econ-card-header">${ecT('Thu – chi ngân sách nhà nước', 'State budget revenue & spending')}</div><div class="econ-card-body" id="ec-bud-body"></div></div>`;
  root.innerHTML = h;
  // charts
  const yrs = G.years.map(String), P = k => G[k].map((v, i) => v == null || !G.GDP[i] ? null : v / G.GDP[i] * 100);
  fsLines('ec-ch-mix', yrs, [
    { label: ecT('C: tiêu dùng hộ', 'C: households'), data: P('C'), color: '#2a78d6' },
    { label: ecT('I: đầu tư', 'I: investment'), data: P('I'), color: '#eb6834' },
    { label: ecT('G: Nhà nước', 'G: government'), data: P('G'), color: '#4a3aa7' },
    { label: 'X − M', data: P('net_exports'), color: '#1baf7a' },
  ], { yMin: -5, dp: 1 });
  const I = ECONFLOW.inv, iy = I.years.map(String), num = a => (a || []).map(v => typeof v === 'number' ? v / 1e3 : null);
  fsLines('ec-ch-inv', iy, [
    { label: ecT('Nhà nước', 'State'), data: num(I.state), color: '#e34948' },
    { label: ecT('Ngoài nhà nước', 'Non-state'), data: num(I.nonstate), color: '#2a78d6' },
    { label: 'FDI', data: num(I.fdi), color: '#eda100' },
  ], { unit: '', dp: 0, yMin: 0, axisFmt: v => fsN(v, 0) });
  ecRenderBop(); ecRenderBudget();
}

// Ledger 1 — inflows vs outflows with the rest of the world (bn USD), balancing item = change in reserves
function ecRenderBop() {
  const el = document.getElementById('ec-bop-body'), B = ECONFLOW.bop, S = B.s, ys = B.years;
  const yi = ecBopYear == null ? ys.length - 1 : ys.indexOf(ecBopYear), yr = ys[yi], v = k => (S[k] || [])[yi];
  const cdA = v('currency_deposits_assets') || 0, cdL = v('currency_deposits_liabilities') || 0, loans = v('loans_net') || 0, oth = (v('other_inv_net') || 0) - (loans + cdL - cdA);
  const pf = v('portfolio_net') || 0, eo = v('errors_omissions') || 0;
  const IN = [], OUT = [];
  const add = (side, vi, en, val, sub) => { if (val == null || Math.abs(val) < 0.05) return; (side === 'in' ? IN : OUT).push({ n: ecT(vi, en), v: val, sub }); };
  add('in', 'Xuất khẩu hàng hoá', 'Goods exports', v('goods_x'));
  add('in', 'Xuất khẩu dịch vụ', 'Services exports', v('services_x'), `${ecT('du lịch', 'tourism')} ${ecN(v('travel_x'), 1)} · ${ecT('vận tải', 'transport')} ${ecN(v('transport_x'), 1)}`);
  add('in', 'Kiều hối & chuyển giao nhận về', 'Remittances & transfers received', v('secondary_income_in'), ecT('kiều hối, viện trợ, thu nhập người lao động ở nước ngoài gửi về', 'remittances, grants, money sent home by workers abroad'));
  add('in', 'Thu nhập nhận về (lãi, cổ tức, tiền lương)', 'Income received (interest, dividends, wages)', v('primary_income_in'));
  add('in', 'FDI vào Việt Nam', 'FDI into Vietnam', v('fdi_in'));
  add(pf >= 0 ? 'in' : 'out', pf >= 0 ? 'Vốn gián tiếp vào (ròng)' : 'Khối ngoại rút vốn gián tiếp (ròng)', pf >= 0 ? 'Portfolio inflows (net)' : 'Portfolio outflows (net)', Math.abs(pf));
  add(loans >= 0 ? 'in' : 'out', loans >= 0 ? 'Vay nước ngoài (ròng)' : 'Trả nợ nước ngoài (ròng)', loans >= 0 ? 'Foreign borrowing (net)' : 'Net foreign debt repayment', Math.abs(loans));
  add('in', 'Tiền gửi của người nước ngoài', 'Non-resident deposits', cdL);
  add('out', 'Nhập khẩu hàng hoá', 'Goods imports', v('goods_m'));
  add('out', 'Nhập khẩu dịch vụ', 'Services imports', v('services_m'), `${ecT('du lịch, du học, khám chữa bệnh ở nước ngoài', 'travel, study, medical abroad')} ${ecN(v('travel_m'), 1)} · ${ecT('vận tải, logistics', 'transport, logistics')} ${ecN(v('transport_m'), 1)}`);
  add('out', 'Thu nhập trả ra (lợi nhuận FDI, lãi vay)', 'Income paid abroad (FDI profits, interest)', v('primary_income_out'), ecT('chủ yếu lợi nhuận, cổ tức doanh nghiệp FDI chuyển về nước', 'mostly FDI profits & dividends repatriated'));
  add('out', 'Chuyển giao trả ra', 'Transfers paid abroad', v('secondary_income_out'));
  add('out', 'Đầu tư ra nước ngoài', 'Outward investment', v('fdi_out'));
  add('out', 'Ngoại tệ, tiền gửi chuyển ra/giữ ngoài hệ thống', 'FX cash & deposits moved abroad / held outside', cdA, ecT('người dân, DN tích trữ ngoại tệ, gửi tiền ra nước ngoài', 'households and firms hoarding FX or depositing abroad'));
  add(oth >= 0 ? 'in' : 'out', 'Đầu tư khác (tín dụng thương mại…)', 'Other investment (trade credit…)', Math.abs(oth));
  add(eo >= 0 ? 'in' : 'out', eo >= 0 ? 'Lỗi & sai sót (dòng vào chưa ghi nhận)' : 'Lỗi & sai sót (dòng ra chưa ghi nhận)', eo >= 0 ? 'Errors & omissions (unrecorded inflow)' : 'Errors & omissions (unrecorded outflow)', Math.abs(eo), eo < 0 ? ecT('vàng nhập lậu, ngoại tệ thất thoát, sai lệch thống kê', 'smuggled gold, unrecorded FX outflows, statistical gaps') : '');
  [IN, OUT].forEach(a => a.sort((x, y) => y.v - x.v));
  const sIn = IN.reduce((t, x) => t + x.v, 0), sOut = OUT.reduce((t, x) => t + x.v, 0), mx = Math.max(...IN.map(x => x.v), ...OUT.map(x => x.v));
  const col = (a, c) => a.map(x => `<div class="ec-li"><div class="ec-li-n">${x.n}${x.sub ? `<span>${x.sub}</span>` : ''}</div><div class="ec-li-b"><i style="width:${(x.v / mx * 100).toFixed(1)}%;background:${c}"></i></div><div class="ec-li-v">${ecN(x.v, 1)}</div></div>`).join('');
  const T = ECONFLOW.tour, R = ECONFLOW.remit, L = ECONFLOW.labour, ti = T.years.indexOf(yr), ri = R.years.indexOf(yr), li = L.years.indexOf(yr);
  const rem = ri >= 0 ? (R.national_sbv_busd[ri] ?? R.national_wb_knomad_busd[ri] ?? R.national_wb_projection_busd[ri]) : null;
  let h = `<div class="ec-yrs">${ys.map(y => `<button type="button" data-ec-bop="${y}" class="${y === yr ? 'on' : ''}">${y}</button>`).join('')}</div>
    <div class="ec-ledger">
      <div><div class="ec-lh in">${ecT('Tiền vào (tỷ USD)', 'Money in (bn USD)')} <b>${ecN(sIn, 1)}</b></div>${col(IN, '#1baf7a')}</div>
      <div><div class="ec-lh out">${ecT('Tiền ra (tỷ USD)', 'Money out (bn USD)')} <b>${ecN(sOut, 1)}</b></div>${col(OUT, '#e34948')}</div>
    </div>
    <div class="ec-net">${ecT('Chênh lệch vào − ra', 'Net in − out')} = <b>${ecN(sIn - sOut, 1)}</b> ${ecT('tỷ USD', 'bn USD')} ≈ ${ecT('dự trữ ngoại hối thay đổi', 'change in FX reserves')} <b>${(v('reserves_change') >= 0 ? '+' : '') + ecN(v('reserves_change'), 1)}</b> · ${ecT('cán cân vãng lai', 'current account')} <b>${ecN(v('current_account'), 1)}</b></div>
    <div class="kpi-grid fs-kpis">
      ${fsTile(ecT('Kiều hối', 'Remittances'), rem != null ? ecN(rem, 1) + ecT(' tỷ USD', ' bn USD') : '—', R.national_sbv_busd[ri] != null ? ecT('NHNN', 'SBV') : R.national_wb_knomad_busd[ri] != null ? 'World Bank/KNOMAD' : ecT('WB dự báo', 'WB projection'))}
      ${fsTile(ecT('Lao động đi làm việc ở nước ngoài', 'Workers sent abroad'), li >= 0 && L.workers_sent[li] != null ? ecN(L.workers_sent[li], 0) + ecT(' người', '') : '—', ecT('DOLAB · ước gửi về 6,5–7 tỷ USD/năm (10/2025)', 'DOLAB · est. 6.5–7 bn USD sent home a year (Oct-2025)'))}
      ${fsTile(ecT('Khách quốc tế', 'International arrivals'), ti >= 0 && T.intl_arrivals_thousand[ti] != null ? ecN(T.intl_arrivals_thousand[ti] / 1e3, 1) + ecT(' triệu lượt', ' mn') : '—', `${ecT('thu', 'receipts')} ${ecN(ti >= 0 ? T.travel_receipts_busd[ti] : null, 1)} · ${ecT('chi du lịch ra nước ngoài', 'spent abroad')} ${ecN(ti >= 0 ? T.travel_payments_busd[ti] : null, 1)} ${ecT('tỷ USD', 'bn USD')}`)}
      ${fsTile(ecT('9 tháng 2026', '9M 2026'), ecN(T.intl_arrivals_thousand[T.years.length - 1] / 1e3, 1) + ecT(' triệu khách', ' mn arrivals'), `${ecT('người Việt đi nước ngoài', 'Vietnamese outbound')} ${ecN(T.vietnamese_outbound_departures_thousand[T.years.length - 1] / 1e3, 1)} ${ecT('triệu', 'mn')}`)}
    </div>
    <div class="iv-sec">${ecT('Diễn biến 2015–2025 (tỷ USD)', '2015–2025 trend (bn USD)')}</div><div id="ec-ch-bop" class="fs-ch"></div>
    <div class="iv-src">${ecT('Nguồn: IMF Balance of Payments (BPM6) theo số NHNN; du lịch: NSO V09.15, V10.04; kiều hối: NHNN/World Bank KNOMAD; lao động: Cục Quản lý lao động ngoài nước. Việt Nam không công bố tách lợi nhuận FDI chuyển về nước và lãi vay trong "thu nhập trả ra", cũng như kiều hối riêng trong "chuyển giao nhận về". Vay nợ chỉ có số ròng.', 'Sources: IMF Balance of Payments (BPM6) from SBV data; tourism: NSO V09.15, V10.04; remittances: SBV/World Bank KNOMAD; labour export: DOLAB. Vietnam does not split FDI profit repatriation from interest in "income paid", nor remittances within "transfers received". Loans are net only.')}</div>`;
  el.innerHTML = h;
  fsLines('ec-ch-bop', ys.map(String), [
    { label: ecT('Cán cân vãng lai', 'Current account'), data: S.current_account, color: '#2a78d6' },
    { label: ecT('FDI ròng', 'Net FDI'), data: S.fdi_net, color: '#eda100' },
    { label: ecT('Lợi nhuận/lãi trả ra', 'Income paid abroad'), data: S.primary_income_out.map(x => x == null ? null : -x), color: '#e34948', dash: 1 },
    { label: ecT('Lỗi & sai sót', 'Errors & omissions'), data: S.errors_omissions, color: '#94a3b8' },
    { label: ecT('Thay đổi dự trữ', 'Reserves change'), data: S.reserves_change, color: '#1baf7a' },
  ], { unit: '', dp: 1, axisFmt: v => fsN(v, 0) });
}

// Ledger 2 — State budget (billion VND)
function ecRenderBudget() {
  const el = document.getElementById('ec-bud-body'), B = ECONFLOW.budget, ys = B.years;
  const yi = ecBudYear == null ? ys.indexOf(2025) : ys.indexOf(ecBudYear), yr = ys[yi], R = B.revenue, X = B.expenditure, g = (o, k) => (o[k] || [])[yi];
  const known = ['pit', 'land_use_fees', 'env_protection', 'lottery', 'fees_charges'].reduce((t, k) => t + (g(R, k) || 0), 0);
  const IN = [[ecT('Thuế khác từ hoạt động trong nước (GTGT, TNDN, TTĐB…)', 'Other domestic taxes (VAT, CIT, excise…)'), (g(R, 'domestic') || 0) - known], [ecT('Tiền sử dụng đất', 'Land-use fees'), g(R, 'land_use_fees')], [ecT('Thuế thu nhập cá nhân', 'Personal income tax'), g(R, 'pit')], [ecT('Thu cân đối từ xuất nhập khẩu', 'Import-export revenue'), g(R, 'import_export')], [ecT('Phí, lệ phí', 'Fees & charges'), g(R, 'fees_charges')], [ecT('Dầu thô', 'Crude oil'), g(R, 'crude_oil')], [ecT('Xổ số', 'Lottery'), g(R, 'lottery')], [ecT('Thuế bảo vệ môi trường', 'Environmental protection tax'), g(R, 'env_protection')], [ecT('Viện trợ', 'Grants'), g(R, 'grants')]].filter(x => x[1] > 0);
  const knownX = ['development_investment', 'recurrent', 'interest', 'aid', 'reserve_fund'].reduce((t, k) => t + (g(X, k) || 0), 0);
  const OUT = [[ecT('Chi thường xuyên (lương, an sinh, y tế, giáo dục…)', 'Recurrent (salaries, social, health, education…)'), g(X, 'recurrent')], [ecT('Chi đầu tư phát triển', 'Development investment'), g(X, 'development_investment')], [ecT('Trả lãi vay', 'Interest payments'), g(X, 'interest')], [ecT('Viện trợ', 'Aid'), g(X, 'aid')], [ecT('Dự phòng & khác', 'Reserve & other'), (g(X, 'reserve_fund') || 0) + Math.max(0, (g(X, 'total') || 0) - knownX)]].filter(x => x[1] > 0);
  const mx = Math.max(...IN.map(x => x[1]), ...OUT.map(x => x[1]));
  const col = (a, c) => a.sort((p, q) => q[1] - p[1]).map(([n, v]) => `<div class="ec-li"><div class="ec-li-n">${n}</div><div class="ec-li-b"><i style="width:${(v / mx * 100).toFixed(1)}%;background:${c}"></i></div><div class="ec-li-v">${fsN(v / 1e3, 0)}</div></div>`).join('');
  const basis = (B.basis || [])[yi], bl = { final: ecT('quyết toán', 'final accounts'), estimate: ecT('ước tính', 'estimate'), plan: ecT('dự toán', 'budget plan') }[basis] || basis;
  el.innerHTML = `<div class="ec-yrs">${ys.map(y => `<button type="button" data-ec-bud="${y}" class="${y === yr ? 'on' : ''}">${y}</button>`).join('')} <span class="pol-ft">${yr}: ${bl}</span></div>
    <div class="ec-ledger">
      <div><div class="ec-lh in">${ecT('Thu (nghìn tỷ đồng)', 'Revenue (trillion VND)')} <b>${fsN((g(R, 'total') || 0) / 1e3, 0)}</b></div>${col(IN, '#1baf7a')}</div>
      <div><div class="ec-lh out">${ecT('Chi (nghìn tỷ đồng)', 'Spending (trillion VND)')} <b>${fsN((g(X, 'total') || 0) / 1e3, 0)}</b></div>${col(OUT, '#e34948')}</div>
    </div>
    <div class="ec-net">${ecT('Bội chi chính thức', 'Official deficit')} <b>${fsN((g(B, 'deficit') || 0) / 1e3, 0)} ${ecT('nghìn tỷ', 'tn VND')}</b>${(B.deficit_pct_gdp || [])[yi] != null ? ` (${ecN(B.deficit_pct_gdp[yi], 1)}% GDP)` : ''}</div>
    <div class="iv-sec">${ecT('Thu, chi và bội chi 2018–2026 (nghìn tỷ đồng)', 'Revenue, spending and deficit 2018–2026 (trillion VND)')}</div><div id="ec-ch-bud" class="fs-ch"></div>
    <div class="iv-src">${ecT('Nguồn: Nghị quyết Quốc hội & quyết toán NSNN của Bộ Tài chính (2018–2024 quyết toán, 2025 ước tính, 2026 dự toán), NSO. Số thu chi không gồm chuyển nguồn; bội chi là số chính thức (không bằng thu − chi). Chi 2025 gồm chi từ nguồn tăng thu nên không so sánh trực tiếp. Bộ Tài chính không công bố tách thuế GTGT, TNDN, TTĐB trong thu nội địa — gộp vào "thuế khác".', 'Sources: National Assembly resolutions & MoF budget final accounts (2018–2024 final, 2025 estimate, 2026 plan), NSO. Totals exclude carry-overs; the deficit is the official figure (not revenue − spending). 2025 spending includes spending from revenue overruns, so not directly comparable. MoF does not split VAT, CIT and excise within domestic revenue — grouped as "other taxes".')}</div>`;
  const yl = ys.map(String), t = a => (a || []).map(v => v == null ? null : v / 1e3);
  fsLines('ec-ch-bud', yl, [
    { label: ecT('Thu', 'Revenue'), data: t(R.total), color: '#1baf7a' },
    { label: ecT('Chi', 'Spending'), data: t(X.total), color: '#e34948' },
    { label: ecT('Chi đầu tư phát triển', 'Development investment'), data: t(X.development_investment), color: '#eb6834', dash: 1 },
  ], { unit: '', dp: 0, yMin: 0, axisFmt: v => fsN(v, 0), bars: { label: ecT('Bội chi', 'Deficit'), color: '#eda100', data: t(B.deficit) } });
}
document.addEventListener('click', e => {
  const b = e.target.closest && e.target.closest('[data-ec-bop]'); if (b) { ecBopYear = +b.dataset.ecBop; ecRenderBop(); return; }
  const d = e.target.closest && e.target.closest('[data-ec-bud]'); if (d) { ecBudYear = +d.dataset.ecBud; ecRenderBudget(); }
});
// ── /ECON renderer ──
