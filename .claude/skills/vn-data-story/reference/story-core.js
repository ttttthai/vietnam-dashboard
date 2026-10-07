// Extracted from vietnam_dashboard.html: story core helpers (stT … stTables). Depends on APP_LANG/APP_LOC, dotTipShow/dotTipMove/dotTipHide, ciE (HTML escape).
const stT = (vi, en) => (typeof APP_LANG !== 'undefined' && APP_LANG === 'en') ? en : vi;
const stN = (v, d = 1) => v == null || !isFinite(v) ? '—' : v.toLocaleString(APP_LOC, { minimumFractionDigits: d, maximumFractionDigits: d });
const stLin = (d0, d1, r0, r1) => v => r0 + (v - d0) / ((d1 - d0) || 1) * (r1 - r0);
const stTip = (e, html) => dotTipShow(e, html);
let stBopYear = null, stBopTimer = null, stBudYear = null, stBopGross = false, stDD = null, stDDYear = null, stDDTax = false, stDDAlt = false;   // stDD* = budget drill-down panel state

// scroll reveal: chapters fade/slide in; marks with .st-grow scale from baseline, .st-draw lines draw on
function stObserve(root) {
  const els = root.querySelectorAll('.st-chap');
  if (!('IntersectionObserver' in window) || matchMedia('(prefers-reduced-motion: reduce)').matches) { els.forEach(x => x.classList.add('in')); return; }
  const io = new IntersectionObserver(es => es.forEach(en => { if (en.isIntersecting) { en.target.classList.add('in'); const big = en.target.querySelector('.st-big'); if (big) stCountUp(big); io.unobserve(en.target); } }), { threshold: 0.12 });
  els.forEach(x => io.observe(x));
}
// hero count-up: once, on reveal, the big number counts from 0 to its rendered value in step with the hero chart's bar sweep
// (≈1.1s), then the original text is put back exactly. Reduced motion or no observer: the static number is all there is.
function stCountUp(el, ms = 1100) {
  if (el._stCU || matchMedia('(prefers-reduced-motion: reduce)').matches) return;
  const tn = [...el.childNodes].find(n => n.nodeType === 3 && /\d/.test(n.textContent)); if (!tn) return;
  const txt = tn.textContent, m = txt.match(/\d[\d.,]*/); if (!m) return;
  const dec = (APP_LOC || '').startsWith('en') ? '.' : ',', grp = dec === '.' ? ',' : '.', raw = m[0].replace(/[.,]$/, '');
  const v = parseFloat(raw.split(grp).join('').replace(dec, '.')), d = (raw.split(dec)[1] || '').length; if (!isFinite(v) || v === 0) return;
  el._stCU = 1; el.classList.add('st-counting');
  const pre = txt.slice(0, m.index), post = txt.slice(m.index + raw.length), t0 = performance.now();
  const step = now => { const k = Math.max(0, Math.min(1, (now - t0) / ms)), e = 1 - Math.pow(1 - k, 3);
    if (k < 1) { tn.textContent = pre + (v * e).toLocaleString(APP_LOC, { minimumFractionDigits: d, maximumFractionDigits: d }) + post; requestAnimationFrame(step); }
    else { tn.textContent = txt; el.classList.remove('st-counting'); } };
  requestAnimationFrame(step);
}
const stChap = (id, kicker, title, dek, body) => `<section class="st-chap" id="${id}"><div class="st-kicker">${kicker}</div><h2 class="st-h">${title}</h2>${dek ? `<p class="st-dek">${dek}</p>` : ''}${body}</section>`;
const stSrc = t => `<div class="st-src">${t}</div>`;

const stSvg = (W, H, inner, label) => `<svg viewBox="0 0 ${W} ${H}" width="100%" role="img" aria-label="${ciE(label || '')}">${inner}</svg>`;
// width-aware drawing (chart audit Top-10 #4): a chart measures its container (stCW), draws in real pixels so 11–12px
// labels stay 11–12px on a phone, and registers its draw function (stRW); one resize listener redraws the charts whose
// width changed (sub-tab switches dispatch a resize too) and rebuilds the table twins of their story root.
const stCW = (el, max = 1000, min = 300) => { const w = Math.round((el && (el.clientWidth || (el.parentNode && el.parentNode.clientWidth))) || max); if (el) el._w = w; return Math.max(min, Math.min(max, w)); };
const stRW = (el, fn) => { if (el) { el._rd = fn; el.classList.add('st-rw'); } };
let stRWT = null;
window.addEventListener('resize', () => { clearTimeout(stRWT); stRWT = setTimeout(() => { const roots = new Set();
  document.querySelectorAll('.st-rw').forEach(el => { if (!el.isConnected || !el._rd || !el.clientWidth || Math.abs(el.clientWidth - (el._w || 0)) <= 16) return;
    try { el._rd(); } catch (e) { console.warn('redraw', e); } const r = el.closest('.st-root'); if (r) roots.add(r); });
  roots.forEach(r => { try { stTables(r); } catch (e) {} }); }, 160); });
// annotation: primary (ink, bold) locates the headline's evidence; supporting (muted, smaller) is context only
const stAnn = (x, y, tx, ty, text, anchor = 'start', tier = 'pri') => `<g class="st-ann st-ann-${tier}">${Math.hypot(tx - x, ty - y) > 6 ? `<path d="M${x},${y} L${tx},${ty}"/>` : ''}<circle cx="${x}" cy="${y}" r="2.5"/><text x="${tx + (anchor === 'end' ? -4 : anchor === 'middle' ? 0 : 4)}" y="${ty + 4}" text-anchor="${anchor}">${text}</text></g>`;
function stAxisY(sy, ticks, x0, x1, fmt) { return ticks.map(t => `<line class="st-grid" x1="${x0}" x2="${x1}" y1="${sy(t)}" y2="${sy(t)}"/><text class="st-tick" x="${x0 - 6}" y="${sy(t) + 4}" text-anchor="end">${fmt(t)}</text>`).join(''); }
const stNice = (lo, hi, n = 4) => { const span = hi - lo || 1, step0 = span / n, mag = Math.pow(10, Math.floor(Math.log10(step0))), step = [1, 2, 2.5, 5, 10].map(m => m * mag).find(s => s >= step0) || step0; const a = Math.floor(lo / step) * step, out = []; for (let v = a; v <= hi + 1e-9; v += step) out.push(+v.toFixed(10)); return out; };
// ── STORY core: mark, legend, hover, crosshair, table-view helpers (dataviz skill: thin marks, 4px rounded data-end,
// 2px surface gap/ring, >=24px hit targets, legend for 2+ series, value-first tooltip, table twin for every chart) ──
const ST_SURF = '#faf8f3', ST_MUTE = '#c9ccd2', ST_BAR = 24;
// rounded rectangle with per-corner radius; only the data-end corners are rounded, the baseline stays square
function stRR(x, y, w, h, r, c) { w = Math.max(0, w); h = Math.max(0, h); r = Math.min(r, w / 2, h / 2); const [tl, tr, br, bl] = c.map(k => k ? r : 0);
  return `M${x + tl},${y} H${x + w - tr} ${tr ? `Q${x + w},${y} ${x + w},${y + tr}` : ''} V${y + h - br} ${br ? `Q${x + w},${y + h} ${x + w - br},${y + h}` : ''} H${x + bl} ${bl ? `Q${x},${y + h} ${x},${y + h - bl}` : ''} V${y + tl} ${tl ? `Q${x},${y} ${x + tl},${y}` : ''} Z`; }
const stKey = (label, value) => `data-st-k="${encodeURIComponent(String(label) + '|' + String(value))}"`;
// vertical bar centred in its slot, width capped at 24px, grows from yBase to yVal; round=false for an interior stacked segment
function stBarV(cx, slot, yVal, yBase, fill, key, o = {}) { const w = Math.min(ST_BAR, Math.max(2, slot - 4)), up = yVal <= yBase, y = Math.min(yVal, yBase), h = Math.abs(yBase - yVal);
  const r = o.round === false ? 0 : 4, d = stRR(cx - w / 2, y, w, h, r, up ? [1, 1, 0, 0] : [0, 0, 1, 1]);
  return `<path class="st-bar ${o.cls ?? 'st-grow'}" style="--d:${o.d || 0}ms;${up ? '' : 'transform-origin:top;'}${o.style || ''}" d="${d}" fill="${fill}"${o.op != null ? ` opacity="${o.op}"` : ''} ${key ? stKey(...key) : ''} ${o.attr || ''}/>`; }
// horizontal bar, thickness capped at 24px, grows from xBase to xVal
function stBarH(cy, slot, xVal, xBase, fill, key, o = {}) { const h = Math.min(ST_BAR, Math.max(2, slot - 4)), right = xVal >= xBase, x = Math.min(xVal, xBase), w = Math.abs(xVal - xBase);
  const r = o.round === false ? 0 : 4, d = stRR(x, cy - h / 2, w, h, r, right ? [0, 1, 1, 0] : [1, 0, 0, 1]);
  return `<path class="st-bar ${o.cls ?? 'st-grow-x'}" style="--d:${o.d || 0}ms;${right ? '' : 'transform-origin:right;'}${o.style || ''}" d="${d}" fill="${fill}"${o.op != null ? ` opacity="${o.op}"` : ''} ${key ? stKey(...key) : ''} ${o.attr || ''}/>`; }
// stacked column: segs = [{v, c, key}] bottom→top (or right for horizontal); 2px surface gap between segments, only the outer end rounded
function stStackV(cx, slot, sy, segs, o = {}) { let base = 0, s = ''; const vis = segs.filter(g => g.v > 0), last = vis[vis.length - 1];
  vis.forEach((g, i) => { const y0 = sy(base), y1 = sy(base + g.v), u = y1 <= y0 ? 1 : -1; s += stBarV(cx, slot, y1 + (g === last ? 0 : u), y0 - (i ? u : 0), g.c, g.key, { round: g === last, d: (o.d || 0) + i * 40, attr: g.attr }); base += g.v; }); return s; }   // works up (sy decreasing) or down
function stStackH(cy, slot, sx, segs, o = {}) { let base = 0, s = ''; const vis = segs.filter(g => g.v > 0), last = vis[vis.length - 1];
  vis.forEach((g, i) => { const x0 = sx(base), x1 = sx(base + g.v), gap = i ? 1 : 0; s += stBarH(cy, slot, x1 - (g === last ? 0 : 1), x0 + gap, g.c, g.key, { round: g === last, d: (o.d || 0) + i * 40, attr: g.attr }); base += g.v; }); return s; }
// dot / marker: r >= 4, 2px surface ring, invisible 24px hit area that carries the tooltip
function stDot(cx, cy, r, fill, key, o = {}) { r = Math.max(4, r);
  return `<g class="st-pt ${o.cls ?? 'st-cell'}" style="--d:${o.d || 0}ms" ${key ? stKey(...key) : ''} ${o.attr || ''}><circle cx="${cx}" cy="${cy}" r="${Math.max(12, r + 2)}" fill="transparent"/><circle cx="${cx}" cy="${cy}" r="${r}" fill="${o.hollow ? ST_SURF : fill}" stroke="${o.hollow ? fill : ST_SURF}" stroke-width="2"${o.dash ? ' stroke-dasharray="3,2"' : ''}/></g>`; }
// 2px line through points [[x,y],…]; nulls break the line (a gap stays a gap)
function stLine(pts, c, o = {}) { let d = '', pen = false; pts.forEach(p => { if (!p || p[1] == null || !isFinite(p[1])) { pen = false; return; } d += `${pen ? 'L' : 'M'}${p[0].toFixed(1)},${p[1].toFixed(1)} `; pen = true; });
  return `<path class="${o.cls ?? 'st-draw'}" d="${d}" fill="none" stroke="${c}" stroke-width="${o.w || 2}" stroke-linejoin="round" stroke-linecap="round"${o.dash ? ` stroke-dasharray="${o.dash}"` : ''}${o.op != null ? ` opacity="${o.op}"` : ''} ${o.attr || ''}/>`; }
// legend (always above the chart, same place on every chart): items [{n, c, k:'rect'|'line'|'dash'|'dot'|'ring'}]
const stLegend = items => `<div class="st-legend">${items.map(i => `<span><i class="k-${i.k || 'rect'}" style="--c:${i.c}"></i>${i.n}</span>`).join('')}</div>`;
// explainer note under a chart: line 1 = the finding with its anchor, line 2 = one caveat / contrast / consequence
const stNote = (lead, follow) => `<p class="st-note">${lead}${follow ? `<span>${follow}</span>` : ''}</p>`;
// tooltip: value leads, label follows; built with textContent (labels are data)
function stTipKV(e, label, value, rows) {
  let tip = document.getElementById('dot-tip'); if (!tip) { tip = document.createElement('div'); tip.id = 'dot-tip'; tip.setAttribute('role', 'tooltip'); document.body.appendChild(tip); }
  tip.textContent = ''; const add = (cls, txt, c) => { const d = document.createElement('div'); d.className = cls; if (c) { const k = document.createElement('i'); k.className = 'dt-key'; k.style.background = c; d.appendChild(k); } d.appendChild(document.createTextNode(txt)); tip.appendChild(d); return d; };
  if (rows) { add('dt-l', label); rows.forEach(r => { const d = add('dt-row', '', r.c); const b = document.createElement('b'); b.textContent = r.v; d.appendChild(b); d.appendChild(document.createTextNode(' ' + r.n)); }); }
  else { add('dt-v', value); add('dt-l', label); }
  tip.style.display = 'block'; dotTipMove(e);
}
// crosshair for line charts: a hairline snaps to the nearest x and one tooltip lists every series at that x
// cfg = {xs:[px], labels:[…], top, bottom, series:[{n, c, v:[…], f}]}; uses d3.pointer/bisectCenter when d3 is loaded
function stCross(el, cfg) {
  const svg = el.querySelector('svg'); if (!svg) return; el._stRows = { head: [''].concat(cfg.series.map(s => s.n)), rows: cfg.labels.map((l, i) => [l].concat(cfg.series.map(s => s.v[i] == null ? '—' : s.f(s.v[i])))) };
  const NS = 'http://www.w3.org/2000/svg', g = document.createElementNS(NS, 'g'), ln = document.createElementNS(NS, 'line'), hit = document.createElementNS(NS, 'rect');
  const x0 = Math.min(...cfg.xs), x1 = Math.max(...cfg.xs);
  ln.setAttribute('y1', cfg.top); ln.setAttribute('y2', cfg.bottom); ln.setAttribute('class', 'st-xh-line'); ln.style.display = 'none';
  hit.setAttribute('x', x0 - 12); hit.setAttribute('y', cfg.top); hit.setAttribute('width', x1 - x0 + 24); hit.setAttribute('height', cfg.bottom - cfg.top); hit.setAttribute('fill', 'transparent'); hit.style.cursor = 'crosshair';
  g.appendChild(ln); g.appendChild(hit); svg.appendChild(g);
  const order = cfg.xs.map((x, i) => [x, i]).sort((a, b) => a[0] - b[0]), xsS = order.map(o => o[0]);
  const near = mx => { const k = window.d3 && d3.bisectCenter ? d3.bisectCenter(xsS, mx) : xsS.reduce((b, x, j) => Math.abs(x - mx) < Math.abs(xsS[b] - mx) ? j : b, 0); return order[Math.max(0, Math.min(order.length - 1, k))][1]; };
  const move = ev => { const mx = window.d3 && d3.pointer ? d3.pointer(ev, svg)[0] : (() => { const p = svg.createSVGPoint(); p.x = ev.clientX; p.y = ev.clientY; return p.matrixTransform(svg.getScreenCTM().inverse()).x; })();
    const i = near(mx); ln.setAttribute('x1', cfg.xs[i]); ln.setAttribute('x2', cfg.xs[i]); ln.style.display = '';
    stTipKV(ev, String(cfg.labels[i]), '', cfg.series.filter(s => s.v[i] != null).map(s => ({ n: s.n, c: s.c, v: s.f(s.v[i]) }))); };
  hit.addEventListener('pointermove', move); hit.addEventListener('pointerenter', move);
  hit.addEventListener('pointerleave', () => { ln.style.display = 'none'; dotTipHide(); });
}
// table view: every chart gets a collapsed data table built from its marks (or its crosshair rows) - the WCAG twin
function stTables(root) {
  root.querySelectorAll('.st-viz, .st-mult, .st-waffles').forEach(el => {
    if (el.closest('.st-mult') && el !== el.closest('.st-mult')) return;
    const nx = el.nextElementSibling; if (nx && nx.classList.contains('st-tbl')) nx.remove();
    let head, rows = [];
    if (el._stRows) ({ head, rows } = el._stRows);
    else { const seen = new Set(); el.querySelectorAll('[data-st-k]').forEach(m => { const k = m.dataset.stK; if (seen.has(k)) return; seen.add(k); const p = decodeURIComponent(k).split('|'); rows.push([p[0], p.slice(1).join(' · ')]); }); head = [stT('Mục', 'Item'), stT('Giá trị', 'Value')]; }
    if (!rows.length) return;
    const det = document.createElement('details'); det.className = 'st-tbl'; const sm = document.createElement('summary'); sm.textContent = `${stT('Bảng số liệu', 'Data table')} · ${rows.length} ${stT('dòng', 'rows')}`; det.appendChild(sm);
    const w = document.createElement('div'); w.className = 'st-tbl-w'; const t = document.createElement('table'), tr0 = document.createElement('tr');
    head.forEach(h => { const th = document.createElement('th'); th.textContent = h; tr0.appendChild(th); }); const th0 = document.createElement('thead'); th0.appendChild(tr0); t.appendChild(th0);
    const tb = document.createElement('tbody'); rows.forEach(r => { const tr = document.createElement('tr'); r.forEach((c, j) => { const td = document.createElement(j ? 'td' : 'th'); td.textContent = c; tr.appendChild(td); }); tb.appendChild(tr); }); t.appendChild(tb);
    w.appendChild(t); det.appendChild(w); el.after(det);
  });
}
