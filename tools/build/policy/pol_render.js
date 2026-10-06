// ── POLICY renderer ──
// "Chính sách": registry of monetary (SBV) and fiscal instruments — current setting, every change since 2023
// (date, legal document, from → to, easing/tightening), timeline of moves and latest news. Data: POLICY (research 05/10/2026).
const polE = () => typeof APP_LANG !== 'undefined' && APP_LANG === 'en';
const polT = (vi, en) => polE() ? en : vi;
const polF = (o, k) => (polE() && o[k + '_en']) || o[k + '_vi'] || '';
const POL_GROUPS = {
  rates: ['Lãi suất điều hành & trần lãi suất', 'Policy rates & rate caps'], fx: ['Tỷ giá, ngoại hối & vàng', 'Exchange rate, FX & gold'],
  prudential: ['Giới hạn an toàn (LDR, vốn, rủi ro)', 'Prudential limits (LDR, capital, risk)'], credit: ['Tăng trưởng & phân bổ tín dụng', 'Credit growth & allocation'],
  targeted: ['Gói tín dụng ưu đãi', 'Targeted credit programmes'], restructuring: ['Tái cơ cấu & pháp lý ngân hàng', 'Bank restructuring & law'], other: ['Khác (bảo hiểm tiền gửi, thanh toán)', 'Other (deposit insurance, payments)'],
  tax: ['Thuế', 'Taxes'], fees: ['Phí, lệ phí & tiền thuê đất', 'Fees, charges & land rent'], spending: ['Đầu tư công', 'Public investment'], budget: ['Ngân sách & bội chi', 'Budget & deficit'],
  debt: ['Nợ công & trái phiếu Chính phủ', 'Public debt & government bonds'], social: ['Tiền lương & an sinh', 'Wages & social'], stimulus: ['Hỗ trợ & kích thích', 'Support & stimulus'], trade: ['Thương mại & thuế quan', 'Trade & tariffs'],
};
const POL_DIR = { easing: ['nới lỏng', 'easing', '#1baf7a'], tightening: ['thắt chặt', 'tightening', '#e34948'], neutral: ['trung tính', 'neutral', '#94a3b8'] };
const polDir = d => { const x = POL_DIR[d] || POL_DIR.neutral; return `<span class="pol-dir" style="--c:${x[2]}">${polT(x[0], x[1])}</span>`; };
const polLastChange = ins => { const h = (ins.history || []).filter(x => x.date <= '9999'); return h.length ? h[h.length - 1] : null; };

function renderPolicy() {
  const root = document.getElementById('pol-root'); if (!root || typeof POLICY === 'undefined') return;
  const P = POLICY, all = [...P.mon.instruments.map(i => ({ ...i, side: 'mon' })), ...P.fis.instruments.map(i => ({ ...i, side: 'fis' }))];
  const since = new Date(); since.setFullYear(since.getFullYear() - 1); const sinceS = since.toISOString().slice(0, 10);
  const cnt = (side, dir) => all.filter(i => i.side === side).flatMap(i => i.history || []).filter(h => h.date >= sinceS && h.direction === dir).length;
  let h = `<div class="pol-nav2">${[['pol-stance', polT('Định hướng', 'Stance')], ['pol-timeline', polT('Dòng thời gian', 'Timeline')], ['pol-mon', polT('Chính sách tiền tệ', 'Monetary policy')], ['pol-fis', polT('Chính sách tài khoá', 'Fiscal policy')], ['pol-news', polT('Tin mới', 'Latest news')], ['pol-appendix', polT('Phụ lục số liệu', 'Data appendix')]].map(([id, t]) => `<button type="button" data-pol-go="${id}">${t}</button>`).join('')}</div>`;
  // stance
  h += `<div class="econ-card full-width" id="pol-stance"><div class="econ-card-header">${polT('Định hướng chính sách hiện nay', 'Current policy stance')} <span class="pn-label" style="font-family:var(--font-ui)">· ${polT('cập nhật', 'as of')} ${pgFmt(P.mon.as_of)}</span></div><div class="econ-card-body"><div class="pol-stance">
    ${[['mon', polT('Tiền tệ (NHNN)', 'Monetary (SBV)'), P.mon], ['fis', polT('Tài khoá (Chính phủ, Quốc hội, Bộ Tài chính)', 'Fiscal (Government, National Assembly, MoF)'), P.fis]].map(([s, t, D]) => `<div class="pol-st-card">
      <h4>${t}</h4><p>${ciE(polF(D, 'stance'))}</p>
      <div class="pol-st-cnt"><span style="color:#1baf7a">▲ ${cnt(s, 'easing')} ${polT('lần nới lỏng', 'easing moves')}</span> · <span style="color:#e34948">▼ ${cnt(s, 'tightening')} ${polT('lần thắt chặt', 'tightening moves')}</span> · ${cnt(s, 'neutral')} ${polT('trung tính', 'neutral')} <span style="color:var(--text3)">(${polT('12 tháng qua', 'last 12 months')})</span></div>
      <div class="pol-st-n">${D.instruments.length} ${polT('công cụ theo dõi', 'instruments tracked')}</div></div>`).join('')}
  </div></div></div>`;
  // timeline
  h += `<div class="econ-card full-width" id="pol-timeline"><div class="econ-card-header">${polT('Dòng thời gian các lần điều chỉnh chính sách (2023–2026)', 'Timeline of policy moves (2023–2026)')}</div><div class="econ-card-body"><div id="pol-tl"></div>
    <div class="pg-legend">${Object.values(POL_DIR).map(x => `<span><svg width="12" height="12"><circle cx="6" cy="6" r="5" fill="${x[2]}"/></svg> ${polT(x[0], x[1])}</span>`).join('')}<span>· ${polT('Rê chuột để xem chi tiết, bấm để mở công cụ', 'Hover for detail, click to open the instrument')}</span></div></div></div>`;
  // tables
  const table = (side, D, title, id) => {
    const groups = [...new Set(D.instruments.map(i => i.group))];
    return `<div class="econ-card full-width" id="${id}"><div class="econ-card-header">${title}</div><div class="econ-card-body"><div class="iv-tablewrap"><table class="iv-table pol-table"><thead><tr><th>${polT('Công cụ', 'Instrument')}</th><th>${polT('Hiện hành', 'Current setting')}</th><th>${polT('Thay đổi gần nhất', 'Last change')}</th><th>${polT('Hướng', 'Direction')}</th></tr></thead><tbody>
      ${groups.map(g => `<tr class="pol-grp"><td colspan="4">${polT(...(POL_GROUPS[g] || [g, g]))}</td></tr>` + D.instruments.filter(i => i.group === g).map(i => { const lc = polLastChange(i);
        return `<tr class="pol-row" data-pol-ins="${side}:${i.id}"><td><b>${ciE(polF(i, 'name'))}</b>${i.expires ? `<div class="pol-exp">${polT('hết hạn', 'expires')} ${pgFmt(i.expires)}</div>` : ''}</td><td style="white-space:normal">${ciE(polF(i, 'current'))}</td>
          <td style="white-space:normal">${lc ? `<b>${pgFmt(lc.date)}</b>${lc.doc ? ` · ${ciE(lc.doc)}` : ''}<div class="pol-ft">${ciE(lc.from || '')}${lc.to ? ` → ${ciE(lc.to)}` : ''}</div>` : '—'}</td><td>${lc ? polDir(lc.direction) : ''}<div class="pol-ft">${(i.history || []).length} ${polT('lần', 'moves')} ▸</div></td></tr>
          <tr class="pol-detail" id="pol-d-${side}-${i.id}" hidden><td colspan="4"></td></tr>`; }).join('')).join('')}
    </tbody></table></div></div></div>`; };
  h += table('mon', P.mon, polT('Chính sách tiền tệ — các công cụ của NHNN', 'Monetary policy — SBV instruments'), 'pol-mon');
  h += table('fis', P.fis, polT('Chính sách tài khoá — thuế, chi tiêu, ngân sách', 'Fiscal policy — taxes, spending, budget'), 'pol-fis');
  // news
  const news = [...P.mon.news.map(n => ({ ...n, side: 'mon' })), ...P.fis.news.map(n => ({ ...n, side: 'fis' }))].sort((a, b) => b.date.localeCompare(a.date));
  const insName = (side, id) => { const i = P[side].instruments.find(x => x.id === id); return i ? polF(i, 'name') : id; };
  h += `<div class="econ-card full-width" id="pol-news"><div class="econ-card-header">${polT('Tin chính sách mới nhất', 'Latest policy news')}</div><div class="econ-card-body"><ul class="pg-news">${news.map(n => `<li><time>${pgFmt(n.date)}</time><span class="pol-side ${n.side}">${n.side === 'mon' ? polT('Tiền tệ', 'Monetary') : polT('Tài khoá', 'Fiscal')}</span> <a href="${ciE(n.url)}" target="_blank" rel="noopener">${ciE(polF(n, 'title'))}</a><div>${(n.instrument_ids || []).map(id => `<span class="pg-tag" data-pol-open="${n.side}:${id}">${ciE(insName(n.side, id))}</span>`).join('')}</div></li>`).join('')}</ul></div></div>`;
  root.innerHTML = h;
  polTimeline(all);
}

function polDetail(key) {
  const [side, id] = key.split(':'), ins = POLICY[side].instruments.find(x => x.id === id), row = document.getElementById(`pol-d-${side}-${id}`);
  if (!ins || !row) return;
  if (!row.hidden) { row.hidden = true; return; }
  const hist = (ins.history || []).slice().reverse();
  row.querySelector('td').innerHTML = `<div class="pol-det">${ins.why_vi ? `<p><b>${polT('Mục đích', 'Purpose')}:</b> ${ciE(polF(ins, 'why'))}</p>` : ''}
    ${ins.series && ins.series.values && ins.series.values.filter(v => v != null).length > 1 ? `<div class="pol-step" id="pol-s-${side}-${id}"></div>` : ''}
    <ul>${hist.map(x => `<li><b>${pgFmt(x.date)}</b> ${polDir(x.direction)} ${x.doc ? `<span class="pol-doc">${ciE(x.doc)}</span>` : ''} ${ciE(x.from || '')}${x.to ? ` → <b>${ciE(x.to)}</b>` : ''}<div class="pol-ft">${ciE(polF(x, 'note'))}${x.url ? ` <a class="fs-src" href="${ciE(x.url)}" target="_blank" rel="noopener">${polT('nguồn', 'source')}</a>` : ''}</div></li>`).join('')}</ul>
    ${ins.notes_vi ? `<div class="iv-src">${ciE(polF(ins, 'notes'))}</div>` : ''}</div>`;
  row.hidden = false;
  const s = ins.series, el = document.getElementById(`pol-s-${side}-${id}`);
  if (s && el) {   // step chart of the setting over time
    const pts = s.dates.map((d, i) => [d, s.values[i]]).filter(p => p[1] != null), W = 600, H = 120, ML = 40, MR = 12, MT = 12, MB = 20;
    const t0 = +new Date(pts[0][0]), t1 = Math.max(+new Date(pts[pts.length - 1][0]), Date.now()), vs = pts.map(p => p[1]), lo = Math.min(...vs), hi = Math.max(...vs), pad = (hi - lo) * 0.15 || Math.abs(hi) * 0.1 || 1;
    const sx = d => ML + (+new Date(d) - t0) / (t1 - t0 || 1) * (W - ML - MR), sy = v => MT + (1 - (v - (lo - pad)) / ((hi + pad) - (lo - pad))) * (H - MT - MB);
    let p = `M${sx(pts[0][0])},${sy(pts[0][1])}`; pts.slice(1).forEach(([d, v]) => { p += ` H${sx(d)} V${sy(v)}`; }); p += ` H${sx(new Date(t1).toISOString())}`;
    el.innerHTML = `<svg viewBox="0 0 ${W} ${H}" width="100%"><path d="${p}" fill="none" stroke="#2a78d6" stroke-width="2"/>${pts.map(([d, v]) => `<circle cx="${sx(d)}" cy="${sy(v)}" r="3" fill="#2a78d6"><title>${pgFmt(d)}: ${v}</title></circle>`).join('')}
      <text x="${ML - 4}" y="${sy(hi) + 3}" text-anchor="end" font-size="10" fill="#64748b">${hi.toLocaleString(APP_LOC)}</text><text x="${ML - 4}" y="${sy(lo) + 3}" text-anchor="end" font-size="10" fill="#64748b">${lo.toLocaleString(APP_LOC)}</text>
      <text x="${ML}" y="${H - 4}" font-size="10" fill="#64748b">${pgFmt(pts[0][0])}</text><text x="${W - MR}" y="${H - 4}" text-anchor="end" font-size="10" fill="#64748b">${polT('hôm nay', 'today')}</text>
      ${s.label_vi || ins.series_label_vi ? `<text x="${ML + 4}" y="${MT + 8}" font-size="10" fill="#475569">${ciE(polE() ? (s.label_en || ins.series_label_en || '') : (s.label_vi || ins.series_label_vi || ''))}</text>` : ''}</svg>`;
  }
}

// timeline: one row per instrument group, monetary lanes on top, fiscal below; dot per change coloured by direction
function polTimeline(all) {
  const el = document.getElementById('pol-tl'); if (!el) return;
  const rows = [];
  ['mon', 'fis'].forEach(side => { const gs = [...new Set(POLICY[side].instruments.map(i => i.group))]; gs.forEach(g => rows.push({ side, g })); });
  const W = 1100, LW = 230, RH = 26, MT = 22, H = MT + rows.length * RH + 30;
  const t0 = +new Date('2023-01-01'), t1 = +new Date('2027-01-31'), sx = t => LW + (t - t0) / (t1 - t0) * (W - LW - 16);
  let g = '';
  rows.forEach((r, k) => { const y = MT + k * RH;
    g += `<rect x="0" y="${y}" width="${W}" height="${RH}" fill="${k % 2 ? '#f8fafc' : '#fff'}"/>`;
    if (k === 0 || rows[k - 1].side !== r.side) g += `<line x1="0" y1="${y}" x2="${W}" y2="${y}" stroke="#94a3b8"/><text x="6" y="${y - 6}" font-size="11" font-weight="700" fill="#0f172a">${r.side === 'mon' ? polT('TIỀN TỆ', 'MONETARY') : polT('TÀI KHOÁ', 'FISCAL')}</text>`;
    g += `<text x="8" y="${y + 17}" font-size="11" fill="#334155">${polT(...(POL_GROUPS[r.g] || [r.g, r.g]))}</text>`; });
  for (let d = new Date(2023, 0, 1); +d <= t1; d.setMonth(d.getMonth() + 3)) { const x = sx(+d), jan = d.getMonth() === 0;
    g += `<line x1="${x}" y1="${MT}" x2="${x}" y2="${MT + rows.length * RH}" stroke="${jan ? '#cbd2db' : '#eef1f5'}"/><text x="${x}" y="${H - 10}" text-anchor="middle" font-size="10" fill="${jan ? '#334155' : '#64748b'}" font-weight="${jan ? 700 : 400}">T${d.getMonth() + 1}/${String(d.getFullYear()).slice(2)}</text>`; }
  const tx = sx(Date.now()); g += `<line x1="${tx}" y1="${MT - 6}" x2="${tx}" y2="${MT + rows.length * RH}" stroke="#b42318" stroke-width="1.5"/><text x="${tx - 4}" y="${MT - 9}" text-anchor="end" font-size="10" font-weight="700" fill="#b42318">${polT('Hôm nay', 'Today')} ${pgFmtD(pgToday())}</text>`;
  all.forEach(ins => { const k = rows.findIndex(r => r.side === ins.side && r.g === ins.group); if (k < 0) return; const y = MT + k * RH + RH / 2;
    const seen = {};
    (ins.history || []).forEach(hh => { const t = +new Date(hh.date); if (!(t >= t0)) return; let x = sx(t); const key = Math.round(x / 6); seen[key] = (seen[key] || 0) + 1; const dy = (seen[key] - 1) * 5 * (seen[key] % 2 ? 1 : -1);
      const c = (POL_DIR[hh.direction] || POL_DIR.neutral)[2];
      g += `<circle class="pol-dot" data-pol-open="${ins.side}:${ins.id}" cx="${x}" cy="${y + dy}" r="5" fill="${c}" stroke="#fff" stroke-width="1.5"><title>${pgFmt(hh.date)} · ${ciE(polF(ins, 'name'))}${hh.doc ? ' · ' + ciE(hh.doc) : ''}\n${ciE(hh.from || '')}${hh.to ? ' → ' + ciE(hh.to) : ''}\n${ciE(polF(hh, 'note'))}</title></circle>`; }); });
  el.innerHTML = `<svg viewBox="0 0 ${W} ${H}" width="100%" role="img" aria-label="${polT('Dòng thời gian điều chỉnh chính sách', 'Policy moves timeline')}">${g}</svg>`;
}
document.addEventListener('click', e => {
  const go = e.target.closest && e.target.closest('[data-pol-go]');
  if (go) { const t = document.getElementById(go.dataset.polGo); if (t) { if (t.tagName === 'DETAILS') t.open = true; t.scrollIntoView({ behavior: 'smooth', block: 'start' }); } return; }
  const op = e.target.closest && e.target.closest('[data-pol-open]');
  if (op) { const [side, id] = op.dataset.polOpen.split(':'); const row = document.getElementById(`pol-d-${side}-${id}`); if (row && row.hidden) polDetail(op.dataset.polOpen); const tr = document.querySelector(`[data-pol-ins="${op.dataset.polOpen}"]`); if (tr) tr.scrollIntoView({ behavior: 'smooth', block: 'center' }); return; }
  const r = e.target.closest && e.target.closest('.pol-row'); if (r && !e.target.closest('a')) polDetail(r.dataset.polIns);
});
// ── /POLICY renderer ──
