// SBV section: Vietnam does not publish the SBV's own balance sheet / income statement → show what is published:
// IMF monetary-survey items (NFA, net claims on government, currency outside banks), FX reserves, OMO operations, other operations, budget contribution.
function fsSbvHtml(B) {
  const S = B.balance_sheet, inc = B.income_statement, ops = B.operations || [];
  const pick = it => ops.filter(o => o.item === it && o.value != null);
  const last = it => { const a = pick(it); return a.length ? a[a.length - 1] : null; };
  const omoO = last('omo_outstanding'), kb = last('state_treasury_deposits'), fx = S.monthly_supplement.fx_reserves_total_busd;
  const fxL = fsLast(fx.values);
  const ev = ops.filter(o => ['fx_sales', 'fx_swap', 'sbv_bills_issued', 'rrr', 'state_treasury_deposits', 'special_loans', 'refinancing_rate', 'omo_rate', 'fx_purchases'].includes(o.item) && /^\d{4}-\d{2}/.test(o.date)).sort((a, b) => b.date.localeCompare(a.date)).slice(0, 14);
  const NAME = { fx_sales: fsT('Bán ngoại tệ', 'FX sales'), fx_swap: fsT('Hoán đổi ngoại tệ', 'FX swap'), sbv_bills_issued: fsT('Phát hành tín phiếu', 'SBV bills issued'), rrr: fsT('Dự trữ bắt buộc', 'Reserve requirement'), state_treasury_deposits: fsT('Tiền gửi KBNN', 'Treasury deposits'), special_loans: fsT('Cho vay đặc biệt', 'Special loans'), refinancing_rate: fsT('LS tái cấp vốn', 'Refinancing rate'), omo_rate: fsT('LS OMO', 'OMO rate'), fx_purchases: fsT('Mua ngoại tệ', 'FX purchases') };
  const comb = (inc.combined_line || inc.mof_combined_line || null);
  const bc = inc.budget_contribution || [], bcL = fsLast(bc);
  let h = `<div class="iv-src" style="margin:0 0 8px">${fsT('Việt Nam không công bố bảng cân đối và báo cáo thu chi riêng của NHNN (báo cáo thường niên NHNN không có báo cáo tài chính; IMF không có Central Bank Survey của Việt Nam). Dưới đây là các khoản mục được công bố: tài sản ngoại ròng, tín dụng ròng cho Chính phủ, tiền mặt ngoài hệ thống ngân hàng (IMF Article IV), dự trữ ngoại hối, nghiệp vụ thị trường mở và các nghiệp vụ khác.', "Vietnam does not publish the SBV's own balance sheet or income statement (SBV annual reports contain no financial statements; the IMF has no Central Bank Survey for Vietnam). Shown below are the published items: net foreign assets, net claims on government, currency outside banks (IMF Article IV), FX reserves, open-market operations and other operations.")}</div>`;
  h += '<div class="kpi-grid fs-kpis">';
  h += fsTile(fsT('Dự trữ ngoại hối (IMF)', 'FX reserves (IMF)'), fsN(fxL.v, 1) + fsT(' tỷ USD', ' bn USD'), `${fsMon(fx.periods[fxL.i].replace('M', ''))} · ${fsT('NHNN công bố ~87,6 tỷ USD (18/6/2026)', 'SBV stated ~87.6 bn USD (18/6/2026)')}`);
  const nfaL = fsLast(S.series.nfa); h += fsTile(fsT('Tài sản ngoại ròng NHNN', 'SBV net foreign assets'), fsN(nfaL.v, 0) + fsT(' nghìn tỷ', ' tn VND'), `${S.periods[nfaL.i]} · IMF Article IV`);
  const cuL = fsLast(S.series.currency_outside_banks); h += fsTile(fsT('Tiền mặt ngoài ngân hàng (M0)', 'Currency outside banks (M0)'), fsN(cuL.v, 0) + fsT(' nghìn tỷ', ' tn VND'), `${S.periods[cuL.i]} · IMF; ${fsT('ước T7/26', 'est. Jul-26')} ${fsN(fsLast(S.monthly_supplement.currency_in_circulation_derived.values).v, 0)}`);
  if (omoO) h += fsTile(fsT('Dư nợ OMO (bơm qua cầm cố GTCG)', 'OMO outstanding (repo injections)'), fsTr(omoO.v), `${pgFmt(omoO.date)} · VBMA · ${fsT('lãi suất 4,5%', 'rate 4.5%')}`, omoO.url);
  if (kb) h += fsTile(fsT('Tiền gửi KBNN tại Big 4', 'Treasury deposits at Big 4'), fsTr(kb.v), pgFmt(kb.date), kb.url);
  if (bcL.v != null) h += fsTile(fsT('Chênh lệch thu chi NHNN nộp NSNN', 'SBV surplus paid to budget'), fsN(bcL.v, 1) + fsT(' nghìn tỷ', ' tn VND'), `${inc.years[bcL.i]} · ${fsT('quyết toán; dự toán 18,2 — vượt do lãi đầu tư dự trữ ở nước ngoài', 'settlement; budget 18.2 — beat on foreign reserve investment income')}`);
  h += '</div><div class="fs-grid2">';
  h += `<div><div class="iv-sec">${fsT('Khoản mục NHNN theo IMF (nghìn tỷ đồng, cuối năm)', 'SBV items per IMF (trillion VND, year-end)')}</div><div id="fs-ch-sbv" class="fs-ch"></div></div>`;
  h += `<div><div class="iv-sec">${fsT('Dự trữ ngoại hối theo tháng (tỷ USD)', 'Monthly FX reserves (bn USD)')}</div><div id="fs-ch-res" class="fs-ch"></div></div>`;
  h += `<div><div class="iv-sec">${fsT('Thị trường mở: bơm/hút ròng theo tuần & dư nợ OMO (nghìn tỷ)', 'Open market: weekly net injection & OMO outstanding (trillion)')}</div><div id="fs-ch-omo" class="fs-ch"></div></div>`;
  h += `<div><div class="iv-sec">${fsT('Các nghiệp vụ & quyết định gần đây', 'Recent operations & decisions')}</div><div class="iv-tablewrap" style="max-height:220px;overflow:auto"><table class="iv-table"><tbody>${ev.map(o => `<tr><td style="white-space:nowrap">${pgFmt(o.date.slice(0, 10).length === 7 ? o.date + '-01' : o.date.slice(0, 10))}</td><td><b>${NAME[o.item] || o.item}</b>${o.value != null ? ` · ${fsN(o.value, o.unit === '%' ? 1 : 1)} ${ciE(o.unit || '')}` : ''}<div style="font-size:11px;color:var(--text3);white-space:normal">${ciE(o.note || '')}${fsSrc(o.url)}</div></td></tr>`).join('')}</tbody></table></div></div>`;
  h += '</div>';
  h += `<div class="iv-src">${fsT('Nguồn: IMF Article IV (CR 19/235, 21/42, 23/338, 24/306, 25/283 — dùng số mới nhất cho mỗi năm); IMF International Liquidity (dự trữ theo tháng, gồm vàng); VBMA báo cáo tuần (OMO, tín phiếu); Bộ Tài chính quyết toán NSNN 2024. Tiền mặt T7/26 = M2 × tỷ lệ tiền mặt/M2 của NHNN (ước tính).', 'Sources: IMF Article IV (CR 19/235, 21/42, 23/338, 24/306, 25/283 — latest vintage per year); IMF International Liquidity (monthly reserves incl. gold); VBMA weekly reports (OMO, bills); MoF 2024 budget settlement. Jul-26 currency = M2 × SBV cash/M2 ratio (estimate).')}</div>`;
  return h;
}
function fsSbvDraw(B) {
  const S = B.balance_sheet;
  fsLines('fs-ch-sbv', S.periods.slice(0, S.series.nfa.length), [
    { label: fsT('Tài sản ngoại ròng', 'Net foreign assets'), data: S.series.nfa, color: '#2a78d6' },
    { label: fsT('Tiền mặt ngoài NH', 'Currency outside banks'), data: S.series.currency_outside_banks, color: '#eb6834' },
    { label: fsT('Tín dụng ròng cho CP', 'Net claims on govt'), data: S.series.net_claims_govt_sbv, color: '#4a3aa7', dash: 1 },
  ], { unit: '', dp: 0, axisFmt: v => fsN(v, 0) });
  const R = S.monthly_supplement, per = R.fx_reserves_total_busd.periods, from = per.findIndex(p => p >= '2019-M01');
  const L = per.slice(from).map(p => { const [y, m] = p.split('-M'); return `T${+m}/${y.slice(2)}`; });
  fsLines('fs-ch-res', L, [
    { label: fsT('Tổng (gồm vàng)', 'Total (incl. gold)'), data: R.fx_reserves_total_busd.values.slice(from), color: '#1baf7a' },
    { label: fsT('Vàng', 'Gold'), data: R.gold_reserves_busd.values.slice(from), color: '#eda100' },
  ], { unit: '', dp: 1, yMin: 0 });
  const ops = B.operations || [], net = ops.filter(o => o.item === 'omo_net_injection' && o.value != null), out = ops.filter(o => o.item === 'omo_outstanding' && o.value != null);
  const wk = net.map(o => o.date.split('..').pop());
  const outBy = d => { const x = out.find(o => o.date === d); return x ? x.value / 1e3 : null; };
  fsLines('fs-ch-omo', wk.map(d => { const [y, m, dd] = d.split('-'); return `${+dd}/${+m}/${y.slice(2)}`; }), [
    { label: fsT('Dư nợ OMO', 'OMO outstanding'), data: wk.map(outBy), color: '#4a3aa7' },
  ], { unit: '', dp: 0, bars: { label: fsT('Bơm (+) / hút (−) ròng trong tuần', 'Net weekly injection (+) / drain (−)'), color: '#2a78d6', data: net.map(o => o.value / 1e3) }, axisFmt: v => fsN(v, 0) });
}
