"""vn_deck — PowerPoint decks in the Vietnam Dashboard's editorial data-story style (python-pptx >= 0.6.21 + stdlib).

One finding per slide: red uppercase kicker, serif headline that states the finding, the chart, a two-line note
(finding with its anchor, then one caveat) and a hairline footer with source + period and the slide number.
Typography and colour are the dashboard's: Newsreader (headlines) and IBM Plex Sans (everything else), ink #16181D,
kicker red #C2362F, cream #FAF8F3, the categorical palette in its fixed order, grey #C9CCD2 for context.

    import sys; sys.path.insert(0, '<skill>/scripts')
    from vn_deck import Deck
    d = Deck(lang='vi')                       # one language per deck: 'vi' (2.650,1) or 'en' (2,650.1)
    d.cover('BÁO CÁO KINH TẾ', 'Tăng trưởng 9 tháng 9,01%', 'Cập nhật 7/10/2026')
    d.chart('CHƯƠNG 1 · TĂNG TRƯỞNG', 'GDP tăng 8,02% năm 2025',
            {'type': 'bar', 'categories': ['2023', '2024', '2025'],
             'series': [{'name': 'GDP', 'values': [4.98, 7.04, 8.02]}], 'dec': 2, 'unit': '%', 'highlight': 2},
            note=('GDP tăng 8,02% năm 2025.', 'Số 2025 là ước tính.'), source='Nguồn: NSO. Số năm.')
    d.save('deck.pptx')                       # embeds the bundled fonts (embed_fonts=True by default)

Deck(lang='vi', safe_fonts=False, embed_fonts=True, editable=False, brand='Vietnam Dashboard', template=None)
  safe_fonts=True  -> Georgia / Arial everywhere (no embedding needed)
  editable=True    -> chart() uses native, data-editable PowerPoint charts for the types that support it
                      (line, area, bar, barh, diverging, stacked, stacked_h, stacked100_h, scatter); everything else
                      is always shape-drawn. A spec can override with 'editable': True/False.

SLIDE METHODS
  cover(kicker, title, subtitle='', date='', chart=None, note='')        cover, cream, optional motif chart
  section(number, title, dek='', items=())                               chapter divider with contents list
  hero(kicker, headline, big, unit='', dek='', counts=(), chart=None, chart_title='', source='')
                                                                         headline number + counters + mini chart
  kpis(kicker, headline, tiles, note=None, source='')                    2-4 KPI tiles with delta arrows
  chart(kicker, headline, spec, note=None, source='', dek='', panel=None, chart_title='')
                                                                         one chart; panel={...} -> side panel layout
  two_charts(kicker, headline, left, right, titles=('', ''), note=None, source='')
  table(kicker, headline, header, rows, source='', col_widths=None, number_cols=(), note=None)
  watch(kicker, headline, items, source='')                              3 dated "what to watch" items
  quote(kicker, quote, who, role='', note=None, source='')               analyst note / callout
  sources(kicker, headline, items, method=(), source='')                 sources & methodology
  save(path)

CHART SPEC TYPES (spec['type']; all shape-drawn with one scale per chart, grouped as one group shape per chart)
  line         categories, series[{name, values, color?, muted?, dashed?, dash_from?, end_label?}], dec, unit,
               y_min/y_max, zero, forecast_from, forecast_label, band{low, high, name, color}, refs[{value, label}],
               annotations[{series, at, text, tier:'pri'|'sup', dx, dy}], markers:'last'|'all'|None
  fan          categories, actual, base, low, high, names{actual, base, band}, dec, unit (line + 80% band)
  area         categories, series (stacked areas; end labels give the share of the last total)
  bar          categories, series (1 = single, 2+ = grouped), highlight, basis[...'estimate'|'plan'], labels
  barh         categories, series[0], highlight, (ranked: sort it yourself; first row on top)
  diverging    categories, values (blue > 0, red < 0, around a zero line)
  stacked      categories, series (rounded top segment only, 2px surface gaps), totals, basis
  stacked_h    categories, series (horizontal stacked)
  stacked100_h categories, series (each row = 100%; parts of one whole)
  waffle       parts[{name, value}] (1 cell = 1%, largest-remainder rounding)
  waterfall    steps[{name, value, total?}] (start -> contributions -> end)
  tornado      base, rows[{name, low, high}], labels=(low, high)
  dumbbell     rows[{name, a, b}], labels=(a, b)
  slope        periods=(a, b), series[{name, values:[a, b], hi?}]
  bump         periods, series[{name, values}] (ranked inside; 1 = largest), highlight[names]
  heatmap      rows, cols, values[[...]], scale:'diverging'|'sequential', pos_color, show_values
  scatter      points[{name, x, y, label?, hi?}], x_title, y_title, x_dec, y_dec, trend
  multiples    panels[{title, categories, values, kind:'col'|'line', dec, unit}], cols
  spark_table  years, rows[{name, sub, values, dec, unit}], headers
  pyramid      bands, left{name, values}, right{name, values}, compare{name, left, right}, unit
  tilemap      tiles[{name, short?, col, row, value, panel?}], panels[titles], breaks, dec, unit
  flow         columns[[{id, name, color?}]], links[(src, dst, value)], dec, unit (Sankey-style)
  levers       columns[{title, items[{id, name, sub}]}], links[(a, b)], highlight[ids]
  timeline     events[{date, title, text, future?}], today
  bullet       rows[{name, sub, value, target, max, ranges, dec, unit, target_label}]
  tiles        tiles[{label, value, unit, delta, delta_label, tone, sub, spark}]

Numbers: Deck.num(v, d) formats in the deck language (VI 2.650,1 / EN 2,650.1; minus is U+2212). Gaps are None,
never 0. Text is always ink or grey, never a series colour. Bars <= 0.34 in thick with a rounded data end only.
"""
import io
import math
import os
import struct

from pptx import Presentation
from pptx.chart.data import CategoryChartData, XyChartData
from pptx.dml.color import RGBColor
from pptx.enum.chart import XL_CHART_TYPE, XL_LEGEND_POSITION, XL_TICK_LABEL_POSITION, XL_MARKER_STYLE
from pptx.enum.dml import MSO_LINE_DASH_STYLE
from pptx.enum.shapes import MSO_CONNECTOR, MSO_SHAPE
from pptx.enum.text import MSO_ANCHOR, MSO_AUTO_SIZE, PP_ALIGN
from pptx.opc.constants import RELATIONSHIP_TYPE as RT
from pptx.opc.package import Part
from pptx.opc.packuri import PackURI
from pptx.oxml.ns import qn
from pptx.oxml.xmlchemy import OxmlElement
from pptx.util import Pt

# ── tokens (same values as the dashboard's story CSS) ──
INK, INK2, INK3, SUP = '16181D', '5B6170', '8A8F99', '6B7080'
RED, PAPER, SURF, MUTE = 'C2362F', 'FFFFFF', 'FAF8F3', 'C9CCD2'
RULE, HAIR, BASE, ZEBRA, PANEL, ZONE = 'DEDBD2', 'E1E0D9', 'C3C2B7', 'F6F5F1', 'F3F1EA', 'F1EFE8'
BLUE, ORANGE, AQUA, YELLOW, MAGENTA, GREEN, VIOLET, REDS = ('2A78D6', 'EB6834', '1BAF7A', 'EDA100', 'E87BA4', '008300',
                                                             '4A3AA7', 'E34948')
PALETTE = [BLUE, ORANGE, AQUA, YELLOW, MAGENTA, GREEN, VIOLET, REDS]   # fixed order; colour follows the entity
NEUTRAL = 'E6E4DE'                                                     # midpoint of blue <-> red polarity scales

SW, SH = 13.333, 7.5                 # slide, inches (16:9)
MX = 0.65                            # side margin
FOOT_Y = SH - 0.46                   # footer hairline
BAR_MAX = 0.34                       # max bar thickness (≈ 24px on the page)
BAR_R = 0.055                        # data-end radius (≈ 4px)
GAP = 0.022                          # surface gap between stacked segments (≈ 2px)
EMU = 914400

FONTS_DIR = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'fonts'))
# role -> (family, bold, file used for metrics). Families are the fonts' legacy (nameID 1) names, so each weight
# maps to a real face in PowerPoint, LibreOffice and the embedded font list.
ROLES = {
    'head': ('Newsreader', True, 'Newsreader-700'), 'head_semi': ('Newsreader SemiBold', False, 'Newsreader-600'),
    'head_med': ('Newsreader Medium', False, 'Newsreader-500'), 'head_reg': ('Newsreader', False, 'Newsreader-400'),
    'ui': ('IBM Plex Sans', False, 'IBMPlexSans-400'), 'ui_med': ('IBM Plex Sans Medium', False, 'IBMPlexSans-500'),
    'ui_semi': ('IBM Plex Sans SemiBold', False, 'IBMPlexSans-600'), 'ui_bold': ('IBM Plex Sans', True, 'IBMPlexSans-700'),
}
SAFE = {'head': ('Georgia', True), 'head_semi': ('Georgia', True), 'head_med': ('Georgia', False),
        'head_reg': ('Georgia', False), 'ui': ('Arial', False), 'ui_med': ('Arial', False),
        'ui_semi': ('Arial', True), 'ui_bold': ('Arial', True)}
EMBED = {'Newsreader': {'regular': 'Newsreader-400', 'bold': 'Newsreader-700'},
         'Newsreader Medium': {'regular': 'Newsreader-500'}, 'Newsreader SemiBold': {'regular': 'Newsreader-600'},
         'IBM Plex Sans': {'regular': 'IBMPlexSans-400', 'bold': 'IBMPlexSans-700'},
         'IBM Plex Sans Medium': {'regular': 'IBMPlexSans-500'}, 'IBM Plex Sans SemiBold': {'regular': 'IBMPlexSans-600'}}
TXT = {'vi': {'forecast': 'Dự báo', 'band80': 'Khoảng 80%', 'trend': 'Xu hướng', 'base': 'Cơ sở', 'today': 'Hôm nay',
              'target': 'mục tiêu', 'of_target': 'mục tiêu', 'in_chapter': 'Trong chương này', 'method': 'Phương pháp',
              'estimate': 'ước tính', 'plan': 'dự toán', 'own_scale': 'mỗi ô một thang đo'},
       'en': {'forecast': 'Forecast', 'band80': '80% band', 'trend': 'Trend', 'base': 'Base', 'today': 'Today',
              'target': 'target', 'of_target': 'of target', 'in_chapter': 'In this chapter', 'method': 'Method',
              'estimate': 'estimate', 'plan': 'plan', 'own_scale': 'each panel has its own scale'}}


def E(v):
    return int(round(v * EMU))


def rgb(h):
    return RGBColor.from_string(h)


def mix(a, b, t):
    """Blend hex a -> b by t (0..1)."""
    pa, pb = [int(a[i:i + 2], 16) for i in (0, 2, 4)], [int(b[i:i + 2], 16) for i in (0, 2, 4)]
    return ''.join('%02X' % int(round(x + (y - x) * max(0, min(1, t)))) for x, y in zip(pa, pb))


def lum(h):
    c = [int(h[i:i + 2], 16) / 255 for i in (0, 2, 4)]
    c = [x / 12.92 if x <= 0.03928 else ((x + 0.055) / 1.055) ** 2.4 for x in c]
    return 0.2126 * c[0] + 0.7152 * c[1] + 0.0722 * c[2]


def on(fill):
    """Text colour for a label sitting on `fill` (ink on light fills, white on dark)."""
    return INK if lum(fill) > 0.33 else 'FFFFFF'


def nice(lo, hi, n=4, zero=False):
    """Nice axis (lo, hi, ticks, step): the page's stNice, trying n and n+1 intervals and keeping the tighter one."""
    a = _nice(lo, hi, n, zero)
    b = _nice(lo, hi, n + 1, zero)
    return b if (b[1] - b[0]) < (a[1] - a[0]) - 1e-9 else a


def _nice(lo, hi, n=4, zero=False):
    if zero:
        lo, hi = min(lo, 0), max(hi, 0)
    if hi == lo:
        hi = lo + 1
    step0 = (hi - lo) / n
    mag = 10 ** math.floor(math.log10(step0))
    step = next(m * mag for m in (1, 2, 2.5, 5, 10) if m * mag >= step0 - 1e-12)
    a, b = math.floor(lo / step + 1e-9) * step, math.ceil(hi / step - 1e-9) * step
    ticks, t = [], a
    while t <= b + step * 1e-6:
        ticks.append(round(t, 10))
        t += step
    return a, b, ticks, step


def step_dec(step):
    return 0 if step >= 1 and abs(step - round(step)) < 1e-9 else (1 if abs(step * 10 - round(step * 10)) < 1e-9 else 2)


def spread(items, gap, lo, hi):
    """items: list of [y, ...] (mutable). Push label centres apart by >= gap inside [lo, hi]."""
    items.sort(key=lambda it: it[0])
    for i in range(1, len(items)):
        if items[i][0] < items[i - 1][0] + gap:
            items[i][0] = items[i - 1][0] + gap
    if items and items[-1][0] > hi:
        items[-1][0] = hi
        for i in range(len(items) - 2, -1, -1):
            if items[i][0] > items[i + 1][0] - gap:
                items[i][0] = items[i + 1][0] - gap
    if items and items[0][0] < lo:
        items[0][0] = lo
    return items


def largest_remainder(vals, total=100):
    s = sum(vals) or 1
    raw = [v / s * total for v in vals]
    out = [int(math.floor(r)) for r in raw]
    for i in sorted(range(len(raw)), key=lambda i: raw[i] - out[i], reverse=True)[:total - sum(out)]:
        out[i] += 1
    return out


# ── font files: metrics (text measuring) and EOT wrapping for embedding — stdlib only ──
class _TTF:
    def __init__(self, path):
        self.data = d = open(path, 'rb').read()
        n = struct.unpack('>H', d[4:6])[0]
        self.t = {d[12 + 16 * i:16 + 16 * i].decode('latin-1'): struct.unpack('>II', d[20 + 16 * i:28 + 16 * i])
                  for i in range(n)}
        ho = self.t['head'][0]
        self.upem = struct.unpack('>H', d[ho + 18:ho + 20])[0]
        nh = struct.unpack('>H', d[self.t['hhea'][0] + 34:self.t['hhea'][0] + 36])[0]
        mo = self.t['hmtx'][0]
        self.adv = [struct.unpack('>H', d[mo + 4 * i:mo + 4 * i + 2])[0] for i in range(nh)]
        self.cmap = self._cmap()
        self._w = {}

    def _cmap(self):
        d, co = self.data, self.t['cmap'][0]
        n = struct.unpack('>H', d[co + 2:co + 4])[0]
        subs = {}
        for i in range(n):
            p, e, off = struct.unpack('>HHI', d[co + 4 + 8 * i:co + 12 + 8 * i])
            subs[(p, e)] = co + off
        m = {}
        if (3, 10) in subs:
            o = subs[(3, 10)]
            ng = struct.unpack('>I', d[o + 12:o + 16])[0]
            for g in range(ng):
                s, e, sg = struct.unpack('>III', d[o + 16 + 12 * g:o + 28 + 12 * g])
                for c in range(s, e + 1):
                    m[c] = sg + c - s
            return m
        o = subs.get((3, 1)) or subs.get((0, 3))
        seg = struct.unpack('>H', d[o + 6:o + 8])[0] // 2
        ends = struct.unpack('>%dH' % seg, d[o + 14:o + 14 + 2 * seg])
        starts = struct.unpack('>%dH' % seg, d[o + 16 + 2 * seg:o + 16 + 4 * seg])
        deltas = struct.unpack('>%dh' % seg, d[o + 16 + 4 * seg:o + 16 + 6 * seg])
        ro = o + 16 + 6 * seg
        ranges = struct.unpack('>%dH' % seg, d[ro:ro + 2 * seg])
        for i in range(seg):
            for c in range(starts[i], ends[i] + 1):
                if c == 0xFFFF:
                    continue
                if ranges[i] == 0:
                    g = (c + deltas[i]) & 0xFFFF
                else:
                    a = ro + 2 * i + ranges[i] + 2 * (c - starts[i])
                    g = struct.unpack('>H', d[a:a + 2])[0]
                    g = (g + deltas[i]) & 0xFFFF if g else 0
                m[c] = g
        return m

    def width(self, s, size):
        """Advance width of s at size pt, in inches."""
        tot = 0
        for ch in s:
            w = self._w.get(ch)
            if w is None:
                g = self.cmap.get(ord(ch), 0)
                w = self._w[ch] = self.adv[min(g, len(self.adv) - 1)]
            tot += w
        return tot / self.upem * size / 72

    def names(self):
        d, no = self.data, self.t['name'][0]
        cnt, so = struct.unpack('>HH', d[no + 2:no + 6])
        out = {}
        for i in range(cnt):
            p, e, l, nid, ln, off = struct.unpack('>6H', d[no + 6 + 12 * i:no + 18 + 12 * i])
            if p == 3 and e == 1 and l == 0x409 and nid in (1, 2, 4, 5):
                out[nid] = d[no + so + off:no + so + off + ln]          # UTF-16BE bytes
        return out

    def eot(self):
        """Uncompressed, unencrypted EOT (version 0x00020001): the format PowerPoint stores in /ppt/fonts/*.fntdata."""
        d = self.data
        oo, ho = self.t['OS/2'][0], self.t['head'][0]
        weight, = struct.unpack('>H', d[oo + 4:oo + 6])
        fstype, = struct.unpack('>H', d[oo + 8:oo + 10])
        panose = d[oo + 32:oo + 42]
        ur = struct.unpack('>4I', d[oo + 42:oo + 58])
        fssel, = struct.unpack('>H', d[oo + 62:oo + 64])
        cpr = struct.unpack('>2I', d[oo + 78:oo + 86]) if self.t['OS/2'][1] >= 86 else (1, 0)
        csa, = struct.unpack('>I', d[ho + 8:ho + 12])
        nm = self.names()

        def name(i):
            b = nm.get(i, b'')
            le = b''.join(b[k + 1:k + 2] + b[k:k + 1] for k in range(0, len(b), 2))   # UTF-16BE -> LE
            return struct.pack('<H', len(le)) + le
        body = (panose + struct.pack('<BBIHH', 1, fssel & 1, weight, fstype, 0x504C) + struct.pack('<4I', *ur)
                + struct.pack('<2I', *cpr) + struct.pack('<I', csa) + struct.pack('<4I', 0, 0, 0, 0)
                + struct.pack('<H', 0) + name(1) + struct.pack('<H', 0) + name(2) + struct.pack('<H', 0) + name(5)
                + struct.pack('<H', 0) + name(4) + struct.pack('<H', 0) + struct.pack('<H', 0))
        size = 16 + len(body) + len(d)
        return struct.pack('<IIII', size, len(d), 0x00020001, 0) + body + d


_TTF_CACHE = {}


def _ttf(key):
    if key not in _TTF_CACHE:
        p = os.path.join(FONTS_DIR, key + '.ttf')
        _TTF_CACHE[key] = _TTF(p) if os.path.exists(p) else None
    return _TTF_CACHE[key]


# ── low-level shape styling ──
def _clean(sh):
    st = sh._element.find(qn('p:style'))
    if st is not None:
        sh._element.remove(st)
    return sh


def _fill(sh, color, alpha=None):
    if color is None:
        sh.fill.background()
        return
    sh.fill.solid()
    sh.fill.fore_color.rgb = rgb(color)
    if alpha is not None and alpha < 1:
        clr = sh.fill._xPr.find(qn('a:solidFill'))[0]
        a = OxmlElement('a:alpha')
        a.set('val', str(int(alpha * 100000)))
        clr.append(a)


DASH = {'dash': MSO_LINE_DASH_STYLE.DASH, 'dot': MSO_LINE_DASH_STYLE.ROUND_DOT,
        'sdash': MSO_LINE_DASH_STYLE.SQUARE_DOT, 'long': MSO_LINE_DASH_STYLE.LONG_DASH}


def _line(sh, color, w=0.75, dash=None, cap='rnd', tail=None):
    if color is None:
        sh.line.fill.background()
        return
    sh.line.color.rgb = rgb(color)
    sh.line.width = Pt(w)
    if dash:
        sh.line.dash_style = DASH[dash]
    ln = sh.line._get_or_add_ln()
    ln.set('cap', cap)
    for tag in ('a:round', 'a:bevel', 'a:miter', 'a:headEnd', 'a:tailEnd'):
        for el in ln.findall(qn(tag)):
            ln.remove(el)
    ln.append(OxmlElement('a:round'))
    if tail:
        te = OxmlElement('a:tailEnd')
        te.set('type', tail)
        te.set('w', 'med')
        te.set('len', 'med')
        ln.append(te)


class _Cv:
    """A chart canvas: one group shape; all coordinates in inches (slide space)."""

    def __init__(self, deck, slide, x, y, w, h, name='Chart', bg=PAPER):
        self.d, self.slide = deck, slide
        self.grp = slide.shapes.add_group_shape()
        self.grp.name = name
        self.s = self.grp.shapes
        self.x, self.y, self.w, self.h, self.bg = x, y, w, h, bg

    @property
    def x1(self):
        return self.x + self.w

    @property
    def y1(self):
        return self.y + self.h

    def rect(self, x, y, w, h, fill=None, line=None, lw=0.75, dash=None, alpha=None, kind=MSO_SHAPE.RECTANGLE,
             adj=None, rot=0):
        sh = _clean(self.s.add_shape(kind, E(x), E(y), max(E(w), 1), max(E(h), 1)))
        if adj is not None:
            for i, a in enumerate(adj):
                sh.adjustments[i] = a
        if rot:
            sh.rotation = rot
        _fill(sh, fill, alpha)
        _line(sh, line, lw, dash)
        return sh

    def rrect(self, x, y, w, h, fill=None, r=0.04, **kw):
        return self.rect(x, y, w, h, fill, kind=MSO_SHAPE.ROUNDED_RECTANGLE, adj=[min(0.5, r / max(min(w, h), 1e-6))],
                         **kw)

    def bar(self, x0, y0, x1, y1, fill, end='t', r=BAR_R, **kw):
        """Bar spanning the box, only the data end ('t','b','l','r') rounded; the baseline end stays square."""
        x0, x1 = sorted((x0, x1))
        y0, y1 = sorted((y0, y1))
        w, h = x1 - x0, y1 - y0
        if w <= 0.002 or h <= 0.002:
            return None
        thick, length = (w, h) if end in ('t', 'b') else (h, w)
        rr = min(r, thick / 2, length)
        if end is None or rr < 0.006:
            return self.rect(x0, y0, w, h, fill, **kw)
        cx, cy = x0 + w / 2, y0 + h / 2
        bw, bh = thick, length
        rot = {'t': 0, 'b': 180, 'r': 90, 'l': 270}[end]
        return self.rect(cx - bw / 2, cy - bh / 2, bw, bh, fill, kind=MSO_SHAPE.ROUND_2_SAME_RECTANGLE,
                         adj=[min(0.5, rr / min(bw, bh)), 0.0], rot=rot, **kw)

    def poly(self, pts, color=None, w=2.25, dash=None, fill=None, alpha=None, close=False):
        pts = [(E(x), E(y)) for x, y in pts]
        if len(pts) < 2:
            return None
        ff = self.s.build_freeform(pts[0][0], pts[0][1], scale=1.0)
        ff.add_line_segments(pts[1:], close=close)
        sh = _clean(ff.convert_to_shape())
        _fill(sh, fill, alpha)
        _line(sh, color, w, dash)
        return sh

    def seg(self, x1, y1, x2, y2, color=HAIR, w=0.75, dash=None, tail=None, cap='rnd'):
        c = _clean(self.s.add_connector(MSO_CONNECTOR.STRAIGHT, E(x1), E(y1), E(x2), E(y2)))
        _line(c, color, w, dash, cap=cap, tail=tail)
        return c

    def dot(self, cx, cy, r, fill, hollow=False, ring=None, ring_w=1.5):
        if hollow:
            return self.rect(cx - r, cy - r, 2 * r, 2 * r, self.bg, fill, 1.75, kind=MSO_SHAPE.OVAL)
        return self.rect(cx - r, cy - r, 2 * r, 2 * r, fill, ring or self.bg, ring_w, kind=MSO_SHAPE.OVAL)

    def tri(self, cx, cy, s, fill, down=False):
        return self.rect(cx - s / 2, cy - s * 0.43, s, s * 0.86, fill, kind=MSO_SHAPE.ISOSCELES_TRIANGLE,
                         rot=180 if down else 0)

    def text(self, x, y, w, h, paras, **kw):
        return self.d._tx(self.s, x, y, w, h, paras, **kw)

    def label(self, x, y, s, size=10, role='ui', color=INK3, ha='l', va='m', caps=False, spacing=None):
        """Single-line label anchored at (x, y); ha l/r/c, va t/m/b. Returns (left, top, width, height)."""
        if s is None or s == '':
            return (x, y, 0, 0)
        tw = self.d.tw(s.upper() if caps else s, size, role, spacing) + 0.03
        th = size * 1.3 / 72
        lx = x if ha == 'l' else (x - tw if ha == 'r' else x - tw / 2)
        ty = y - th / 2 if va == 'm' else (y if va == 't' else y - th)
        self.d._tx(self.s, lx, ty, tw, th, s, size=size, role=role, color=color, align=ha, anchor='m', wrap=False,
                   caps=caps, spacing=spacing)
        return (lx, ty, tw, th)

    def runs(self, x, y, runs, ha='l', va='m', size=10):
        """Single line of mixed runs [(text, {size, role, color})] anchored like label()."""
        tw = sum(self.d.tw(t, o.get('size', size), o.get('role', 'ui')) for t, o in runs) + 0.04
        mx = max(o.get('size', size) for _, o in runs)
        th = mx * 1.3 / 72
        lx = x if ha == 'l' else (x - tw if ha == 'r' else x - tw / 2)
        ty = y - th / 2 if va == 'm' else (y if va == 't' else y - th)
        self.d._tx(self.s, lx, ty, tw, th, [runs], size=size, align=ha, anchor='m', wrap=False)
        return (lx, ty, tw, th)


class _XY:
    def __init__(self, x0, x1, y0, y1, lo, hi):
        self.x0, self.x1, self.y0, self.y1, self.lo, self.hi = x0, x1, y0, y1, lo, hi

    def sy(self, v):
        return self.y1 - (v - self.lo) / ((self.hi - self.lo) or 1) * (self.y1 - self.y0)


class _HX:
    def __init__(self, x0, x1, lo, hi):
        self.x0, self.x1, self.lo, self.hi = x0, x1, lo, hi

    def sx(self, v):
        return self.x0 + (v - self.lo) / ((self.hi - self.lo) or 1) * (self.x1 - self.x0)


class Deck:
    def __init__(self, lang='vi', safe_fonts=False, embed_fonts=True, editable=False, brand='Vietnam Dashboard',
                 template=None):
        self.lang = lang if lang in TXT else 'vi'
        self.T = TXT[self.lang]
        self.safe = safe_fonts
        self.embed = embed_fonts and not safe_fonts
        self.editable = editable
        self.brand = brand
        self.prs = Presentation(template) if template else Presentation()
        self.prs.slide_width, self.prs.slide_height = E(SW), E(SH)
        self.n = 0
        self._used = set()
        self._numbers = []
        self.lang_tag = 'vi-VN' if self.lang == 'vi' else 'en-GB'

    # ── fonts, measuring, numbers ──
    def font(self, role):
        if self.safe:
            return SAFE[role]
        fam, bold, _ = ROLES[role]
        return fam, bold

    def tw(self, s, size, role='ui', spacing=None):
        f = _ttf(ROLES[role][2])
        w = f.width(s, size) if f else len(s) * 0.53 * size / 72
        if self.safe:
            w *= 1.06
        if spacing:
            w += len(s) * spacing / 72
        return w

    @staticmethod
    def lh(size, line=1.0, n=1):
        """Height (in) of n lines: PowerPoint and LibreOffice both pitch lines at 1.2 x size x spacing."""
        return n * size * 1.2 * (line or 1.0) / 72

    def nlines(self, text, size, role, width):
        sp = self.tw(' ', size, role)
        lines, cur = 1, 0.0
        for word in text.split(' '):
            ww = self.tw(word, size, role)
            if cur and cur + sp + ww > width:
                lines, cur = lines + 1, ww
            else:
                cur += (sp if cur else 0) + ww
        return lines

    def num(self, v, d=1, sign=False):
        if v is None:
            return '—'
        s = f'{abs(v):,.{d}f}'
        if self.lang == 'vi':
            s = s.replace(',', '\x00').replace('.', ',').replace('\x00', '.')
        neg = v < 0 and round(abs(v), d) != 0
        return ('−' if neg else ('+' if sign and v > 0 and round(v, d) != 0 else '')) + s

    def _fmt(self, sp, dec=None, unit=None):
        d = sp.get('dec', 1) if dec is None else dec
        u = sp.get('unit', '') if unit is None else unit
        return lambda v, sign=False: self.num(v, d, sign) + (u if v is not None else '')

    # ── text ──
    def _tx(self, shapes, x, y, w, h, paras, size=12, color=INK, role='ui', align='l', anchor='t', wrap=True,
            line=None, caps=False, spacing=None, after=0):
        tb = shapes.add_textbox(E(x), E(y), max(E(w), 1), max(E(h), 1))
        tf = tb.text_frame
        tf.word_wrap = wrap
        tf.auto_size = MSO_AUTO_SIZE.NONE
        tf.margin_left = tf.margin_right = tf.margin_top = tf.margin_bottom = 0
        tf.vertical_anchor = {'t': MSO_ANCHOR.TOP, 'm': MSO_ANCHOR.MIDDLE, 'b': MSO_ANCHOR.BOTTOM}[anchor]
        if isinstance(paras, (str, dict)):
            paras = [paras]
        for i, p in enumerate(paras):
            para = tf.paragraphs[0] if i == 0 else tf.add_paragraph()
            po = {}
            if isinstance(p, dict):
                po, p = p, p.get('runs', p.get('text', ''))
            runs = p if isinstance(p, list) else [p]
            para.alignment = {'l': PP_ALIGN.LEFT, 'r': PP_ALIGN.RIGHT, 'c': PP_ALIGN.CENTER}[po.get('align', align)]
            if po.get('line', line):
                para.line_spacing = po.get('line', line)
            if po.get('after', after):
                para.space_after = Pt(po.get('after', after))
            if po.get('before'):
                para.space_before = Pt(po['before'])
            for r in runs:
                t, o = (r if isinstance(r, tuple) else (r, {}))
                o = dict({'size': po.get('size', size), 'color': po.get('color', color), 'role': po.get('role', role),
                          'caps': po.get('caps', caps), 'spacing': po.get('spacing', spacing)}, **o)
                self._run(para, t, **o)
        return tb

    def _run(self, para, t, size=12, color=INK, role='ui', caps=False, spacing=None):
        r = para.add_run()
        r.text = t.upper() if caps else t
        fam, bold = self.font(role)
        f = r.font
        f.size, f.name, f.bold, f.italic = Pt(size), fam, bold, False
        f.color.rgb = rgb(color)
        rPr = r._r.get_or_add_rPr()
        rPr.set('lang', self.lang_tag)
        if spacing:
            rPr.set('spc', str(int(spacing * 100)))
        self._used.add(fam)
        return r

    # ── slide chrome ──
    def _slide(self, bg=PAPER):
        s = self.prs.slides.add_slide(self.prs.slide_layouts[6])
        s.background.fill.solid()
        s.background.fill.fore_color.rgb = rgb(bg)
        self.n += 1
        s._bg = bg
        return s

    def _head(self, s, kicker, headline, dek='', width=None):
        """Heavy rule, kicker, serif headline (auto-sized to <= 2 lines), optional dek. Returns content top (in)."""
        width = width or (SW - 2 * MX - 0.6)
        r = s.shapes.add_connector(MSO_CONNECTOR.STRAIGHT, E(MX), E(0.42), E(SW - MX), E(0.42))
        _clean(r)
        _line(r, INK, 3.5, cap='flat')
        self._tx(s.shapes, MX, 0.58, 10, 0.25, kicker, 10.5, RED, 'ui_semi', caps=True, spacing=1.2)
        size = 28
        while size > 21 and self.nlines(headline, size, 'head', width) > 2:
            size -= 1
        nl = self.nlines(headline, size, 'head', width)
        hh = self.lh(size, 1.0, nl)
        self._tx(s.shapes, MX, 0.86, width, hh + 0.08, headline, size, INK, 'head', line=1.0)
        y = 0.86 + hh + 0.12
        if dek:
            nd = self.nlines(dek, 13, 'ui', width)
            self._tx(s.shapes, MX, y, width, self.lh(13, 1.15, nd), dek, 13, INK2, 'ui', line=1.15)
            y += self.lh(13, 1.15, nd) + 0.06
        return y + 0.14

    def _foot(self, s, source):
        c = s.shapes.add_connector(MSO_CONNECTOR.STRAIGHT, E(MX), E(FOOT_Y), E(SW - MX), E(FOOT_Y))
        _clean(c)
        _line(c, RULE, 0.75, cap='flat')
        if source:
            size = 9
            while size > 7.5 and self.nlines(source, size, 'ui', SW - 2 * MX - 2.2) > 1:
                size -= 0.5
            two = self.nlines(source, size, 'ui', SW - 2 * MX - 2.2) > 1
            self._tx(s.shapes, MX, FOOT_Y + 0.07, SW - 2 * MX - 2.2, 0.36, source, size, INK3, 'ui',
                     line=1.0 if two else None)
        tb = self._tx(s.shapes, SW - MX - 2.0, FOOT_Y + 0.07, 2.0, 0.2,
                      [[(self.brand + '   ', {'color': INK3, 'role': 'ui'}), (str(self.n), {'color': INK, 'role': 'ui_semi'})]],
                      9, INK3, align='r')
        self._numbers.append(tb)

    def _note(self, s, x, y, w, note, size=12.5):
        if not note:
            return 0
        lead, follow = (note if isinstance(note, (list, tuple)) else (note, None))
        paras = [{'text': lead, 'size': size, 'color': INK, 'role': 'ui', 'line': 1.12, 'after': 2}]
        if follow:
            paras.append({'text': follow, 'size': size - 0.5, 'color': INK2, 'role': 'ui', 'line': 1.12})
        h = self._note_h(note, w, size)
        self._tx(s.shapes, x, y, w, h, paras)
        return h

    def _note_h(self, note, w, size=12.5):
        if not note:
            return 0
        lead, follow = (note if isinstance(note, (list, tuple)) else (note, None))
        h = self.lh(size, 1.12, self.nlines(lead, size, 'ui', w)) + 2 / 72
        if follow:
            h += self.lh(size - 0.5, 1.12, self.nlines(follow, size - 0.5, 'ui', w))
        return h + 0.04

    def _panel(self, s, x, y, w, h, fill=SURF):
        p = _clean(s.shapes.add_shape(MSO_SHAPE.RECTANGLE, E(x), E(y), E(w), E(h)))
        _fill(p, fill)
        _line(p, None)
        return p

    # ── legend (above every chart with 2+ series) ──
    def _legend(self, cv, items, x=None, y=None, maxw=None, size=10.5):
        x = cv.x if x is None else x
        y = cv.y if y is None else y
        maxw = maxw or cv.w
        cx, cy, rowh = x, y + 0.12, 0.27
        for name, color, kind in items:
            kw = 0.26 if kind in ('line', 'dash', 'band', 'hollow_line') else 0.14
            need = kw + 0.08 + self.tw(name, size, 'ui_med') + 0.3
            if cx > x and cx + need > x + maxw:
                cx, cy = x, cy + rowh
            if kind == 'line':
                cv.seg(cx, cy, cx + kw, cy, color, 2.25)
            elif kind == 'dash':
                cv.seg(cx, cy, cx + kw, cy, color, 2.25, dash='dash', cap='flat')
            elif kind == 'band':
                cv.rect(cx, cy - 0.07, kw, 0.14, color, alpha=0.25)
            elif kind == 'dot':
                cv.dot(cx + 0.07, cy, 0.06, color)
            elif kind == 'ring':
                cv.dot(cx + 0.07, cy, 0.055, color, hollow=True)
            elif kind == 'hollow':
                cv.rrect(cx, cy - 0.07, 0.14, 0.14, mix(color, 'FFFFFF', 0.8), r=0.025, line=color, lw=1, dash='sdash')
            elif kind == 'outline':
                cv.rect(cx, cy - 0.07, 0.14, 0.14, None, line=color, lw=1.25, dash='sdash')
            elif kind == 'tick':
                cv.seg(cx + 0.07, cy - 0.09, cx + 0.07, cy + 0.09, color, 2.5, cap='flat')
            else:
                cv.rrect(cx, cy - 0.07, 0.14, 0.14, color, r=0.025)
            cv.label(cx + kw + 0.08, cy, name, size, 'ui_med', INK)
            cx += need
        return cy - y + 0.2

    # ── axes ──
    def _vaxis(self, cv, sp, vals, top, right_pad=0.0, bottom_pad=0.34, zero=True, grid=True, left_pad=None):
        vals = [v for v in vals if v is not None]
        lo, hi = min(vals), max(vals)
        if sp.get('y_min') is not None:
            lo = sp['y_min']
        if sp.get('y_max') is not None:
            hi = sp['y_max']
        lo, hi, ticks, step = nice(lo, hi, sp.get('ticks', 4), zero)
        if sp.get('y_min') is not None:
            lo = sp['y_min']
            ticks = [t for t in ticks if t >= lo - 1e-9]
        dt = sp.get('tick_dec', step_dec(step))
        labels = [self.num(t, dt) for t in ticks]
        lw = (max(self.tw(l, 10, 'ui') for l in labels) + 0.12) if left_pad is None else left_pad
        if sp.get('y_title'):
            cv.label(cv.x, top + 0.02, sp['y_title'], 9.5, 'ui', INK3, va='t')
            top += 0.3
        xy = _XY(cv.x + lw, cv.x1 - right_pad, top + 0.06, cv.y1 - bottom_pad, lo, hi)
        for t, l in zip(ticks, labels):
            yy = xy.sy(t)
            if grid and abs(t) > 1e-12:
                cv.seg(xy.x0, yy, xy.x1, yy, HAIR, 0.75, cap='flat')
            cv.label(xy.x0 - 0.1, yy, l, 10, 'ui', INK3, ha='r')
        xy.zero = (lo <= 0 <= hi)
        return xy

    def _baseline(self, cv, xy):
        if xy.lo <= 0 <= xy.hi:
            yy = xy.sy(0)
            cv.seg(xy.x0, yy, xy.x1, yy, BASE, 1, cap='flat')

    def _xlabels(self, cv, xs, cats, y, size=10, color=INK2, role='ui', always_last=True):
        n = len(cats)
        if not n:
            return
        widths = [self.tw(str(c), size, role) for c in cats]
        gap = (xs[1] - xs[0]) if n > 1 else 10
        step = 1
        while step < n and max(widths) + 0.1 > gap * step:
            step += 1
        idx = list(range(0, n, step))
        if always_last and idx[-1] != n - 1:
            if (xs[n - 1] - xs[idx[-1]]) < (widths[n - 1] + widths[idx[-1]]) / 2 + 0.1:
                idx.pop()
            idx.append(n - 1)
        for i in idx:
            cv.label(xs[i], y, str(cats[i]), size, role, color, ha='c', va='t')

    def _ann(self, cv, px, py, text, tier='pri', dx=0.3, dy=-0.4, w=None):
        pri = tier == 'pri'
        size, role, color = (10.5, 'ui_semi', INK) if pri else (9.5, 'ui', SUP)
        tx, ty = px + dx, py + dy
        if math.hypot(dx, dy) > 0.1:
            # leader stops short of the text
            k = 1 - 0.05 / max(math.hypot(dx, dy), 0.05)
            cv.seg(px, py, px + dx * k, py + dy * k, INK2 if pri else INK3, 0.75)
        cv.dot(px, py, 0.035, INK if pri else INK3, ring=cv.bg, ring_w=0.75)
        lines = text.split('\n')
        tw = w or max(self.tw(l, size, role) for l in lines) + 0.05
        th = len(lines) * size * 1.25 / 72
        ha = 'l' if dx >= 0 else 'r'
        lx = tx + 0.04 if ha == 'l' else tx - 0.04 - tw
        top = ty - th / 2 if abs(dy) < 0.05 else (ty - th if dy < 0 else ty)
        self._tx(cv.s, lx, top, tw, th, lines, size, color, role, align=ha, line=1.05, anchor='m')

    # ── chart dispatcher ──
    NATIVE = ('line', 'area', 'bar', 'barh', 'diverging', 'stacked', 'stacked_h', 'stacked100_h', 'scatter')

    def _chart(self, s, x, y, w, h, spec, bg=None):
        t = spec['type']
        if spec.get('editable', self.editable) and t in self.NATIVE:
            return self._native(s, x, y, w, h, spec)
        cv = _Cv(self, s, x, y, w, h, 'Chart · ' + t, bg or getattr(s, '_bg', PAPER))
        getattr(self, '_c_' + t)(cv, spec)
        return cv.grp

    # ── line / fan / area ──
    def _c_fan(self, cv, sp):
        n_act = len(sp['actual'])
        cats = sp['categories']
        act = list(sp['actual']) + [None] * (len(cats) - n_act)
        base = [None] * (n_act - 1) + [sp['actual'][-1]] + list(sp['base'])
        low = [None] * (n_act - 1) + [sp['actual'][-1]] + list(sp['low'])
        high = [None] * (n_act - 1) + [sp['actual'][-1]] + list(sp['high'])
        nm = sp.get('names', {})
        ln = dict(sp, type='line', series=[
            {'name': nm.get('actual', ''), 'values': act, 'color': sp.get('color', INK), 'end_label': False},
            {'name': nm.get('base', self.T['forecast']), 'values': base, 'color': sp.get('color2', BLUE), 'dashed': True}],
            band={'low': low, 'high': high, 'name': nm.get('band', self.T['band80']), 'color': sp.get('color2', BLUE)},
            forecast_from=sp.get('forecast_from', n_act))
        self._c_line(cv, ln)

    def _c_line(self, cv, sp):
        cats, S = sp['categories'], sp['series']
        n = len(cats)
        f = self._fmt(sp)
        cols = [MUTE if s.get('muted') else s.get('color', PALETTE[i % len(PALETTE)]) for i, s in enumerate(S)]
        top = cv.y
        leg = [(s['name'], c, 'dash' if s.get('dashed') else 'line') for s, c in zip(S, cols) if s.get('name')]
        if sp.get('band'):
            leg.append((sp['band'].get('name', self.T['band80']), sp['band'].get('color', BLUE), 'band'))
        if len(leg) > 1 and sp.get('legend', True):
            top += self._legend(cv, leg)
        allv = [v for s in S for v in s['values'] if v is not None]
        if sp.get('band'):
            allv += [v for v in sp['band']['low'] + sp['band']['high'] if v is not None]
        allv += [r['value'] for r in sp.get('refs', [])]
        ends = []
        for i, s in enumerate(S):
            v = s['values']
            if s.get('end_label', True) and v and v[-1] is not None:
                ends.append((i, s.get('label', s['name']), v[-1]))
        rp = max([self.tw(f'{nm} {f(v)}' if nm else f(v), 10, 'ui_semi') for _, nm, v in ends] + [0]) + 0.22
        xy = self._vaxis(cv, sp, allv, top, right_pad=rp, zero=sp.get('zero', False))
        inset = 0.12
        xs = [xy.x0 + inset + i * (xy.x1 - xy.x0 - 2 * inset) / max(n - 1, 1) for i in range(n)]
        sy = xy.sy
        ff = sp.get('forecast_from')
        if ff is not None and 0 < ff < n:
            fx = (xs[ff - 1] + xs[ff]) / 2
            cv.rect(fx, xy.y0, xy.x1 - fx + rp - 0.1, xy.y1 - xy.y0, ZONE)
            cv.label(fx + 0.08, xy.y0 + 0.04, sp.get('forecast_label', self.T['forecast']), 9, 'ui_semi', INK2,
                     va='t', caps=True, spacing=0.8)
        for r in sp.get('refs', []):
            yy = sy(r['value'])
            cv.seg(xy.x0, yy, xy.x1, yy, INK2, 1, dash='dash', cap='flat')
            if r.get('label'):
                cv.label(xy.x0 + 0.06, yy - 0.12, r['label'], 9.5, 'ui', INK2)
        if xy.zero and sp.get('zero_line', True):
            self._baseline(cv, xy)
        b = sp.get('band')
        if b:
            idx = [i for i in range(n) if b['low'][i] is not None and b['high'][i] is not None]
            pts = [(xs[i], sy(b['high'][i])) for i in idx] + [(xs[i], sy(b['low'][i])) for i in reversed(idx)]
            cv.poly(pts, None, fill=b.get('color', BLUE), alpha=0.2, close=True)
        self._xlabels(cv, xs, cats, xy.y1 + 0.08)
        order = sorted(range(len(S)), key=lambda i: 0 if S[i].get('muted') else 1)
        for i in order:
            s, c = S[i], cols[i]
            w = s.get('width', 1.75 if s.get('muted') else 2.25)
            v = s['values']
            df = s.get('dash_from')
            runs, cur = [], []
            for k in range(n):
                if v[k] is None:
                    if cur:
                        runs.append(cur)
                    cur = []
                else:
                    cur.append(k)
            if cur:
                runs.append(cur)
            for rr in runs:
                if df is None:
                    if len(rr) > 1:
                        cv.poly([(xs[k], sy(v[k])) for k in rr], c, w, 'dash' if s.get('dashed') else None)
                else:
                    a = [k for k in rr if k < df]
                    bb = [k for k in rr if k >= df - 1]
                    if len(a) > 1:
                        cv.poly([(xs[k], sy(v[k])) for k in a], c, w)
                    if len(bb) > 1:
                        cv.poly([(xs[k], sy(v[k])) for k in bb], c, w, 'dash')
                if len(rr) == 1:
                    cv.dot(xs[rr[0]], sy(v[rr[0]]), 0.045, c)
            mk = s.get('markers', sp.get('markers', 'last'))
            if mk == 'all':
                for k in range(n):
                    if v[k] is not None:
                        cv.dot(xs[k], sy(v[k]), 0.045, c, hollow=s.get('dashed') and (df is None or k >= df))
            elif mk == 'last' and not s.get('muted'):
                last = max([k for k in range(n) if v[k] is not None], default=None)
                if last is not None:
                    cv.dot(xs[last], sy(v[last]), 0.055, c, hollow=bool(s.get('dashed')))
        # direct end labels (ink text, never series colour), spread so they don't collide
        items = [[sy(v), i, nm, v] for i, nm, v in ends]
        spread(items, 0.21, xy.y0, xy.y1)
        for yy, i, nm, v in items:
            ly = sy(S[i]['values'][-1])
            if abs(yy - ly) > 0.03:
                cv.seg(xs[-1] + 0.08, ly, xs[-1] + 0.14, yy, INK3, 0.5)
            muted = S[i].get('muted')
            runs = ([(nm + ' ', {'role': 'ui_med', 'color': INK2 if muted else INK})] if nm else []) + \
                   [(f(v), {'role': 'ui_semi', 'color': INK2 if muted else INK})]
            cv.runs(xs[-1] + 0.16, yy, runs, size=10)
        for a in sp.get('annotations', []):
            s = S[a.get('series', 0)]
            k = a['at']
            val = a.get('value', s['values'][k] if s['values'][k] is not None else None)
            if val is None:
                continue
            self._ann(cv, xs[k], sy(val), a['text'], a.get('tier', 'pri'), a.get('dx', 0.3), a.get('dy', -0.45))

    def _c_area(self, cv, sp):
        cats, S = sp['categories'], sp['series']
        n = len(cats)
        f = self._fmt(sp)
        cols = [MUTE if s.get('muted') else s.get('color', PALETTE[i % len(PALETTE)]) for i, s in enumerate(S)]
        top = cv.y + self._legend(cv, [(s['name'], c, 'rect') for s, c in zip(S, cols)])
        tot = [sum((s['values'][k] or 0) for s in S) for k in range(n)]
        last_tot = tot[-1]
        labels = [f"{s.get('label', s['name'])} {self.num((s['values'][-1] or 0) / last_tot * 100, 0)}%" for s in S]
        rp = max(self.tw(l, 10, 'ui_semi') for l in labels) + 0.25
        xy = self._vaxis(cv, sp, tot + [0], top, right_pad=rp, zero=True)
        xs = [xy.x0 + i * (xy.x1 - xy.x0) / max(n - 1, 1) for i in range(n)]
        cum = [0.0] * n
        mids = []
        for i, s in enumerate(S):
            lo = cum[:]
            cum = [cum[k] + (s['values'][k] or 0) for k in range(n)]
            pts = [(xs[k], xy.sy(cum[k])) for k in range(n)] + [(xs[k], xy.sy(lo[k])) for k in reversed(range(n))]
            cv.poly(pts, None, fill=cols[i], close=True, alpha=sp.get('alpha', 0.92))
            cv.poly([(xs[k], xy.sy(cum[k])) for k in range(n)], cv.bg, 1.25)       # 2px surface gap between layers
            mids.append([xy.sy((lo[-1] + cum[-1]) / 2), i])
        self._baseline(cv, xy)
        self._xlabels(cv, xs, cats, xy.y1 + 0.08)
        spread(mids, 0.22, xy.y0, xy.y1)
        for yy, i in mids:
            cv.runs(xs[-1] + 0.12, yy, [(S[i].get('label', S[i]['name']) + ' ', {'role': 'ui_med'}),
                                         (self.num((S[i]['values'][-1] or 0) / last_tot * 100, 0) + '%',
                                          {'role': 'ui_semi'})], size=10)
        for a in sp.get('annotations', []):
            k = a['at']
            val = a.get('value', tot[k])
            self._ann(cv, xs[k], xy.sy(val), a['text'], a.get('tier', 'pri'), a.get('dx', 0.3), a.get('dy', -0.4))

    # ── columns / bars ──
    def _c_bar(self, cv, sp):
        cats, S = sp['categories'], sp['series']
        n, m = len(cats), len(S)
        f = self._fmt(sp)
        hi = sp.get('highlight')
        hi = set(hi if isinstance(hi, (list, tuple)) else ([] if hi is None else [hi]))
        basis = sp.get('basis') or [None] * n
        cols = [s.get('color', PALETTE[i % len(PALETTE)]) for i, s in enumerate(S)]
        top = cv.y
        leg = [(s['name'], c, 'rect') for s, c in zip(S, cols)] if m > 1 else []
        if any(b in ('estimate', 'plan') for b in basis):
            leg.append((sp.get('basis_label', self.T['estimate'] + ' / ' + self.T['plan']), cols[0] if m > 1 or not hi
                        else cols[0], 'hollow'))
        if leg:
            top += self._legend(cv, leg)
        allv = [v for s in S for v in s['values'] if v is not None]
        xy = self._vaxis(cv, sp, allv + [0], top + 0.2, zero=True, grid=sp.get('grid', True))
        slot = (xy.x1 - xy.x0) / n
        grp = slot * 0.72
        th = min(BAR_MAX if m == 1 else BAR_MAX * 0.8, (grp - (m - 1) * 0.03) / m)
        xs = [xy.x0 + (k + 0.5) * slot for k in range(n)]
        lab = sp.get('labels', 'auto')
        for j, s in enumerate(S):
            for k in range(n):
                v = s['values'][k]
                if v is None:
                    continue
                cx = xs[k] + (j - (m - 1) / 2) * (th + 0.03)
                c = cols[j] if (m > 1 or not hi or k in hi) else MUTE
                y0, y1 = xy.sy(0), xy.sy(v)
                hollow = basis[k] in ('estimate', 'plan')
                kw = dict(line=c if c != MUTE else INK3, lw=1, dash='sdash') if hollow else {}
                cv.bar(cx - th / 2, y1, cx + th / 2, y0, mix(c, 'FFFFFF', 0.8) if hollow else c,
                       't' if v >= 0 else 'b', **kw)
                show = lab is True or lab == 'all' or (lab == 'auto' and (slot > self.tw(f(v), 9.5, 'ui_semi') + 0.08 or
                                                                           k in hi or k == n - 1)) or (lab == 'hi' and k in hi)
                if show and lab is not False and lab != 'none':
                    strong = k in hi
                    cv.label(cx, y1 - 0.05 if v >= 0 else y1 + 0.05, f(v), 9.5 if not strong else 10.5,
                             'ui_bold' if strong else 'ui_semi', INK if strong else INK2, ha='c',
                             va='b' if v >= 0 else 't')
        self._baseline(cv, xy)
        self._xlabels(cv, xs, cats, xy.y1 + 0.08)
        for a in sp.get('annotations', []):
            k = a['at']
            v = S[a.get('series', 0)]['values'][k]
            self._ann(cv, xs[k], xy.sy(v) - 0.25, a['text'], a.get('tier', 'pri'), a.get('dx', 0.3), a.get('dy', -0.3))

    def _rows(self, cv, cats, top, size=11, role='ui_med', maxw=3.4, rowmax=0.46):
        lw = min(maxw, max(self.tw(str(c), size, role) for c in cats) + 0.18)
        rowh = min(rowmax, (cv.y1 - top) / len(cats))
        ys = [top + (i + 0.5) * rowh for i in range(len(cats))]
        return lw, rowh, ys

    def _c_barh(self, cv, sp, polarity=False):
        cats = sp['categories']
        vals = sp['values'] if 'values' in sp else sp['series'][0]['values']
        color = sp.get('color', (sp.get('series') or [{}])[0].get('color', BLUE))
        f = self._fmt(sp)
        hi = sp.get('highlight')
        hi = set(hi if isinstance(hi, (list, tuple)) else ([] if hi is None else [hi]))
        top = cv.y
        if polarity:
            top += self._legend(cv, [(sp.get('pos_label', '> 0'), sp.get('pos_color', BLUE), 'rect'),
                                     (sp.get('neg_label', '< 0'), sp.get('neg_color', REDS), 'rect')])
        top += 0.05
        lw, rowh, ys = self._rows(cv, cats, top + (0.25 if sp.get('axis') else 0), sp.get('label_size', 11))
        vv = [v for v in vals if v is not None]
        lo, hi_v = min(vv + [0]), max(vv + [0])
        valw = max(self.tw(f(v, polarity), 10, 'ui_semi') for v in vv) + 0.12
        x0 = cv.x + lw + (valw if lo < 0 else 0.05)
        x1 = cv.x1 - valw
        if sp.get('axis'):
            a, b, ticks, st = nice(lo, hi_v, 4, True)
            hx = _HX(x0, x1, a, b)
            for t in ticks:
                cv.seg(hx.sx(t), top + 0.22, hx.sx(t), ys[-1] + rowh / 2, HAIR, 0.75, cap='flat')
                cv.label(hx.sx(t), top + 0.1, self.num(t, step_dec(st)), 9.5, 'ui', INK3, ha='c')
        else:
            hx = _HX(x0, x1, lo, hi_v)
        th = min(BAR_MAX * 0.85, rowh * 0.62)
        for i, (c, v) in enumerate(zip(cats, vals)):
            y = ys[i]
            cv.label(cv.x + lw - 0.14, y, str(c), sp.get('label_size', 11), 'ui_med', INK, ha='r')
            if v is None:
                cv.label(hx.sx(0) + 0.08, y, '—', 10, 'ui', INK3)
                continue
            if polarity:
                col = sp.get('pos_color', BLUE) if v >= 0 else sp.get('neg_color', REDS)
                if sp.get('mute_below') is not None and abs(v) < sp['mute_below']:
                    col = MUTE
            else:
                col = color if (not hi or i in hi) else MUTE
            xa, xb = hx.sx(0), hx.sx(v)
            cv.bar(xa, y - th / 2, xb, y + th / 2, col, 'r' if v >= 0 else 'l')
            strong = i in hi
            cv.label(xb + (0.07 if v >= 0 else -0.07), y, f(v, polarity), 10, 'ui_bold' if strong else 'ui_semi',
                     INK if strong or polarity else INK2, ha='l' if v >= 0 else 'r')
        zx = hx.sx(0)
        cv.seg(zx, ys[0] - rowh / 2, zx, ys[-1] + rowh / 2, BASE if not polarity else INK2, 1, cap='flat')

    def _c_diverging(self, cv, sp):
        self._c_barh(cv, sp, polarity=True)

    def _c_stacked(self, cv, sp, horizontal=False, pct=False):
        cats, S = sp['categories'], sp['series']
        n = len(cats)
        f = self._fmt(sp)
        cols = [MUTE if s.get('muted') else s.get('color', PALETTE[i % len(PALETTE)]) for i, s in enumerate(S)]
        basis = sp.get('basis') or [None] * n
        leg = [(s['name'], c, 'rect') for s, c in zip(S, cols)]
        if any(b in ('estimate', 'plan') for b in basis):
            leg.append((sp.get('basis_label', self.T['estimate'] + ' / ' + self.T['plan']), INK3, 'hollow'))
        top = cv.y + self._legend(cv, leg)
        tots = [sum((s['values'][k] or 0) for s in S) for k in range(n)]
        if not horizontal:
            xy = self._vaxis(cv, sp, tots + [0], top + 0.22, zero=True)
            slot = (xy.x1 - xy.x0) / n
            th = min(BAR_MAX, slot * 0.6)
            for k in range(n):
                cx = xy.x0 + (k + 0.5) * slot
                segs = [(S[j]['values'][k], cols[j]) for j in range(len(S)) if (S[j]['values'][k] or 0) > 0]
                base = 0.0
                hollow = basis[k] in ('estimate', 'plan')
                for q, (v, c) in enumerate(segs):
                    last = q == len(segs) - 1
                    y0, y1 = xy.sy(base) - (GAP / 2 if q else 0), xy.sy(base + v) + (0 if last else GAP / 2)
                    kw = dict(line=c, lw=1, dash='sdash') if hollow else {}
                    cv.bar(cx - th / 2, y1, cx + th / 2, y0, mix(c, 'FFFFFF', 0.72) if hollow else c,
                           't' if last else None, **kw)
                    base += v
                if sp.get('totals', True):
                    cv.label(cx, xy.sy(tots[k]) - 0.05, f(tots[k]), 9.5, 'ui_semi', INK2, ha='c', va='b')
            self._baseline(cv, xy)
            self._xlabels(cv, [xy.x0 + (k + 0.5) * slot for k in range(n)], cats, xy.y1 + 0.08)
            return
        # horizontal (stacked_h / stacked100_h)
        lw, rowh, ys = self._rows(cv, cats, top + (0.05 if pct else 0.3), 11, rowmax=0.5)
        tw_ = max(self.tw(f(t), 10, 'ui_semi') for t in tots) + 0.15 if not pct else 0.1
        x0, x1 = cv.x + lw, cv.x1 - tw_
        mx = 100 if pct else nice(0, max(tots), 4)[1]
        hx = _HX(x0, x1, 0, mx)
        if not pct:
            a, b, ticks, st = nice(0, max(tots), 4)
            for t in ticks:
                cv.seg(hx.sx(t), top + 0.25, hx.sx(t), ys[-1] + rowh / 2, HAIR, 0.75, cap='flat')
                cv.label(hx.sx(t), top + 0.12, self.num(t, step_dec(st)), 9.5, 'ui', INK3, ha='c')
        th = min(BAR_MAX, rowh * 0.64)
        for k in range(n):
            y = ys[k]
            cv.label(cv.x + lw - 0.14, y, str(cats[k]), 11, 'ui_med', INK, ha='r')
            segs = [(S[j]['values'][k], cols[j], j) for j in range(len(S)) if (S[j]['values'][k] or 0) > 0]
            tot = sum(v for v, _, _ in segs) or 1
            base = 0.0
            for q, (v, c, j) in enumerate(segs):
                last = q == len(segs) - 1
                vv = v / tot * 100 if pct else v
                xa = hx.sx(base) + (GAP / 2 if q else 0)
                xb = hx.sx(base + vv) - (0 if last else GAP / 2)
                cv.bar(xa, y - th / 2, xb, y + th / 2, c, 'r' if last else None)
                if pct:
                    txt = self.num(vv, sp.get('dec', 0)) + '%'
                    if xb - xa > self.tw(txt, 9.5, 'ui_semi') + 0.1:
                        cv.label((xa + xb) / 2, y, txt, 9.5, 'ui_semi', on(c), ha='c')
                base += vv
            if not pct:
                cv.label(hx.sx(tots[k]) + 0.07, y, f(tots[k]), 10, 'ui_semi', INK2)

    def _c_stacked_h(self, cv, sp):
        self._c_stacked(cv, sp, horizontal=True)

    def _c_stacked100_h(self, cv, sp):
        self._c_stacked(cv, sp, horizontal=True, pct=True)

    # ── parts of a whole ──
    def _c_waffle(self, cv, sp):
        parts = sp['parts']
        cells = largest_remainder([p['value'] for p in parts], 100)
        cols = [MUTE if p.get('muted') else p.get('color', PALETTE[i % len(PALETTE)]) for i, p in enumerate(parts)]
        side = min(cv.h, cv.w * 0.5)
        g = side / 10
        gap = g * 0.14
        seq = [c for c, k in zip(cols, cells) for _ in range(k)]
        for k, c in enumerate(seq):
            r_, c_ = divmod(k, 10)
            cv.rrect(cv.x + c_ * g, cv.y + r_ * g, g - gap, g - gap, c, r=(g - gap) * 0.18)
        lx, ly = cv.x + side + 0.45, cv.y + 0.05
        fv = sp.get('fmt')
        for i, p in enumerate(parts):
            cv.rrect(lx, ly + 0.08, 0.16, 0.16, cols[i], r=0.03)
            lab = lambda q: fv(q['value']) if fv else self.num(q['value'], q.get('dec', sp.get('dec', 0))) + sp.get('unit', '%')
            cv.label(lx + 0.28, ly + 0.16, lab(p), 20, 'head', INK)
            vw = max(self.tw(lab(q), 20, 'head') for q in parts)
            nl = self.nlines(p['name'], 11, 'ui', cv.x1 - (lx + 0.4 + vw))
            self._tx(cv.s, lx + 0.4 + vw, ly + 0.06, cv.x1 - (lx + 0.4 + vw), 0.2 * nl + 0.05, p['name'], 11, INK2,
                     'ui', line=1.05)
            if p.get('sub'):
                cv.label(lx + 0.4 + vw, ly + 0.08 + 0.2 * nl + 0.06, p['sub'], 9, 'ui', INK3, va='t')
            ly += max(0.62, 0.2 * nl + 0.32)
        if sp.get('caption'):
            cv.label(lx, cv.y + side - 0.1, sp['caption'], 9.5, 'ui', INK3)

    # ── waterfall, tornado ──
    def _c_waterfall(self, cv, sp):
        steps = sp['steps']
        f = self._fmt(sp)
        run, bars = 0.0, []
        for st in steps:
            if st.get('total'):
                v = st['value'] if st.get('value') is not None else run
                bars.append((0, v, 'total', st))
                run = v
            else:
                bars.append((run, run + st['value'], 'delta', st))
                run += st['value']
        leg = [(sp.get('total_label', 'Tổng' if self.lang == 'vi' else 'Total'), INK, 'rect'),
               (sp.get('pos_label', 'Tăng' if self.lang == 'vi' else 'Adds'), BLUE, 'rect'),
               (sp.get('neg_label', 'Giảm' if self.lang == 'vi' else 'Subtracts'), REDS, 'rect')]
        if any(st.get('basis') in ('estimate', 'plan') for st in steps):
            leg.append((sp.get('basis_label', self.T['estimate'] + ' / ' + self.T['plan']), INK, 'hollow'))
        top = cv.y + self._legend(cv, leg)
        vals = [b for a, b, _, _ in bars] + [a for a, b, _, _ in bars]
        xy = self._vaxis(cv, sp, vals, top + 0.15, zero=True, bottom_pad=0.55)
        n = len(bars)
        slot = (xy.x1 - xy.x0) / n
        th = min(BAR_MAX * 1.25, slot * 0.58)
        prev = None
        for k, (a, b, kind, st) in enumerate(bars):
            cx = xy.x0 + (k + 0.5) * slot
            col = st.get('color', INK if kind == 'total' else (BLUE if b >= a else REDS))
            up = b >= a
            hollow = st.get('basis') in ('estimate', 'plan')
            kw = dict(line=col, lw=1, dash='sdash') if hollow else {}
            ya_, yb_ = [min(max(xy.sy(v), xy.y0), xy.y1) for v in (a, b)]      # never draw outside the plot
            cv.bar(cx - th / 2, yb_, cx + th / 2, ya_, mix(col, 'FFFFFF', 0.78) if hollow else col, 't' if up else 'b',
                   **kw)
            if prev is not None:
                cv.seg(prev, xy.sy(a if kind == 'delta' else b), cx - th / 2, xy.sy(a if kind == 'delta' else b), INK3,
                       0.75, dash='sdash', cap='flat')
            prev = cx + th / 2
            val = b if kind == 'total' else b - a
            cv.label(cx, xy.sy(max(a, b)) - 0.05, f(val, kind == 'delta'), 10 if kind == 'total' else 9.5,
                     'ui_bold' if kind == 'total' else 'ui_semi', INK if kind == 'total' else INK2, ha='c', va='b')
            self._tx(cv.s, cx - slot / 2 + 0.03, xy.y1 + 0.08, slot - 0.06, 0.45, st['name'], 9.5,
                     INK if kind == 'total' else INK2, 'ui_med' if kind == 'total' else 'ui', align='c', line=1.0)
        self._baseline(cv, xy)

    def _c_tornado(self, cv, sp):
        rows, base = sp['rows'], sp['base']
        f = self._fmt(sp)
        la, lb = sp.get('labels', ('Thấp', 'Cao'))
        ca, cb = sp.get('colors', (BLUE, ORANGE))
        top = cv.y + self._legend(cv, [(la, ca, 'rect'), (lb, cb, 'rect')])
        vals = [r['low'] for r in rows] + [r['high'] for r in rows] + [base]
        a, b, ticks, st = nice(min(vals), max(vals), 5)
        lw, rowh, ys = self._rows(cv, [r['name'] for r in rows], top + 0.36, 10.5, maxw=3.6, rowmax=0.4)
        rowh = min(rowh, (cv.y1 - top - 0.36 - 0.3) / len(rows))
        ys = [top + 0.36 + (i + 0.5) * rowh for i in range(len(rows))]
        vw = max(self.tw(f(v), 9.5, 'ui_semi') for v in vals) + 0.12
        hx = _HX(cv.x + lw + vw, cv.x1 - vw, a, b)
        yb = ys[-1] + rowh / 2
        for t in ticks:
            cv.seg(hx.sx(t), top + 0.3, hx.sx(t), yb, HAIR, 0.75, cap='flat')
            cv.label(hx.sx(t), yb + 0.08, self.num(t, step_dec(st)) + sp.get('unit', ''), 9.5, 'ui', INK3, ha='c', va='t')
        th = min(BAR_MAX * 0.8, rowh * 0.6)
        bx = hx.sx(base)
        for i, r in enumerate(rows):
            y = ys[i]
            cv.label(cv.x + lw - 0.14, y, r['name'], 10.5, 'ui_med', INK, ha='r')
            for v, c in ((r['low'], ca), (r['high'], cb)):
                xv = hx.sx(v)
                if abs(xv - bx) < 0.004:
                    continue
                cv.bar(bx, y - th / 2, xv, y + th / 2, c, 'r' if xv > bx else 'l')
                cv.label(xv + (0.06 if xv > bx else -0.06), y, f(v), 9.5, 'ui_semi', INK2, ha='l' if xv > bx else 'r')
        cv.seg(bx, top + 0.3, bx, yb + 0.02, INK, 1.25, cap='flat')
        cv.label(bx, top + 0.04, f"{sp.get('base_label', self.T['base'])} {f(base)}", 10, 'ui_semi', INK, ha='c',
                 va='t')

    # ── comparisons: dumbbell, slope, bump ──
    def _c_dumbbell(self, cv, sp):
        rows = sp['rows']
        f = self._fmt(sp)
        la, lb = sp.get('labels', ('A', 'B'))
        ca, cb = sp.get('colors', (INK3, BLUE))
        top = cv.y + self._legend(cv, [(la, ca, 'ring'), (lb, cb, 'dot')])
        vals = [r[k] for r in rows for k in ('a', 'b') if r.get(k) is not None]
        a, b, ticks, st = nice(min(vals), max(vals), 5, zero=sp.get('zero', False))
        lw, rowh, ys = self._rows(cv, [r['name'] for r in rows], top + 0.42, 11, rowmax=0.42)
        vw = max(self.tw(f(v), 9.5, 'ui_semi') for v in vals) + 0.16
        hx = _HX(cv.x + lw + vw, cv.x1 - vw, a, b)
        for t in ticks:
            cv.seg(hx.sx(t), top + 0.3, hx.sx(t), ys[-1] + rowh / 2, HAIR, 0.75, cap='flat')
            cv.label(hx.sx(t), top + 0.15, self.num(t, step_dec(st)) + sp.get('unit', ''), 9.5, 'ui', INK3, ha='c')
        for i, r in enumerate(rows):
            y = ys[i]
            cv.label(cv.x + lw - 0.14, y, r['name'], 11, 'ui_med', INK, ha='r')
            xa = hx.sx(r['a']) if r.get('a') is not None else None
            xb = hx.sx(r['b']) if r.get('b') is not None else None
            if xa is not None and xb is not None:
                cv.seg(xa, y, xb, y, MUTE, 3, cap='flat')
            if xa is not None:
                cv.dot(xa, y, 0.06, ca, hollow=True)
            if xb is not None:
                cv.dot(xb, y, 0.07, cb)
            if xa is not None and xb is not None:
                right_b = xb >= xa
                cv.label(xb + (0.11 if right_b else -0.11), y, f(r['b']), 9.5, 'ui_semi', INK, ha='l' if right_b else 'r')
                cv.label(xa + (-0.11 if right_b else 0.11), y, f(r['a']), 9.5, 'ui', INK3, ha='r' if right_b else 'l')
            elif xb is not None or xa is not None:
                xx = xb if xb is not None else xa
                cv.label(xx + 0.11, y, f(r['b'] if xb is not None else r['a']), 9.5, 'ui_semi', INK)
                cv.label(xx + 0.11 + self.tw(f(r['b'] if xb is not None else r['a']), 9.5, 'ui_semi') + 0.08, y,
                         sp.get('missing', 'chưa công bố' if self.lang == 'vi' else 'not published'), 9, 'ui', INK3)

    def _c_slope(self, cv, sp):
        S, pa = sp['series'], sp['periods']
        f = self._fmt(sp)
        vals = [v for s in S for v in s['values']]
        lab_l = [f"{s['name']}  {f(s['values'][0])}" for s in S]
        lab_r = [f"{f(s['values'][1])}  {s['name']}" for s in S]
        wl = max(self.tw(t, 10.5, 'ui_semi') for t in lab_l) + 0.2
        wr = max(self.tw(t, 10.5, 'ui_semi') for t in lab_r) + 0.2
        span = min(cv.w - wl - wr, 5.5)
        xa = cv.x + wl + (cv.w - wl - wr - span) / 2
        xb = xa + span
        top = cv.y + 0.45
        lo, hi = min(vals), max(vals)
        pad = (hi - lo) * 0.04
        xy = _XY(xa, xb, top + 0.1, cv.y1 - 0.15, lo - pad, hi + pad)
        cv.label(xa, cv.y + 0.08, pa[0], 11, 'ui_semi', INK, ha='c', va='t')
        cv.label(xb, cv.y + 0.08, pa[1], 11, 'ui_semi', INK, ha='c', va='t')
        cv.seg(xa, top, xa, cv.y1 - 0.05, BASE, 1, cap='flat')
        cv.seg(xb, top, xb, cv.y1 - 0.05, BASE, 1, cap='flat')
        order = sorted(range(len(S)), key=lambda i: 1 if S[i].get('hi') else 0)
        ci = 0
        cols = {}
        for i in range(len(S)):
            if S[i].get('hi'):
                cols[i] = S[i].get('color', PALETTE[ci % len(PALETTE)])
                ci += 1
            else:
                cols[i] = MUTE
        for i in order:
            s = S[i]
            c = cols[i]
            y0, y1 = xy.sy(s['values'][0]), xy.sy(s['values'][1])
            cv.poly([(xa, y0), (xb, y1)], c, 2.25 if s.get('hi') else 1.75)
            cv.dot(xa, y0, 0.05, c)
            cv.dot(xb, y1, 0.05, c)
        L = spread([[xy.sy(s['values'][0]), i] for i, s in enumerate(S)], 0.21, xy.y0, xy.y1)
        R = spread([[xy.sy(s['values'][1]), i] for i, s in enumerate(S)], 0.21, xy.y0, xy.y1)
        for yy, i in L:
            s = S[i]
            strong = bool(s.get('hi'))
            cv.runs(xa - 0.12, yy, [(s['name'] + '  ', {'role': 'ui_med', 'color': INK if strong else INK2}),
                                    (f(s['values'][0]), {'role': 'ui_semi' if strong else 'ui', 'color': INK if strong else INK2})],
                    ha='r', size=10.5)
        for yy, i in R:
            s = S[i]
            strong = bool(s.get('hi'))
            cv.runs(xb + 0.12, yy, [(f(s['values'][1]) + '  ', {'role': 'ui_bold' if strong else 'ui', 'color': INK if strong else INK2}),
                                    (s['name'], {'role': 'ui_med', 'color': INK if strong else INK2})], size=10.5)

    def _c_bump(self, cv, sp):
        P, S = sp['periods'], sp['series']
        k, m = len(S), len(P)
        f = self._fmt(sp)
        hl = sp.get('highlight', [])
        ranks = []
        for j in range(m):
            order = sorted(range(k), key=lambda i: -(S[i]['values'][j] if S[i]['values'][j] is not None else -1e9))
            r = [0] * k
            for pos, i in enumerate(order):
                r[i] = pos + 1
            ranks.append(r)
        cols = {}
        ci = 0
        for i, s in enumerate(S):
            if s['name'] in hl:
                cols[i] = s.get('color', PALETTE[ci % len(PALETTE)])
                ci += 1
            else:
                cols[i] = MUTE
        top = cv.y
        if hl:
            top += self._legend(cv, [(S[i]['name'], cols[i], 'line') for i in cols if cols[i] != MUTE] +
                                [(sp.get('others', 'Ngành khác' if self.lang == 'vi' else 'Others'), MUTE, 'line')])
        wl = max(self.tw(s['name'], 10, 'ui_med') for s in S) + 0.55
        wr = max(self.tw(f"{s['name']} {f(s['values'][-1])}", 10, 'ui_med') for s in S) + 0.55
        x0, x1 = cv.x + wl, cv.x1 - wr
        y0 = top + 0.45
        rowh = (cv.y1 - y0) / k
        xs = [x0 + j * (x1 - x0) / (m - 1) for j in range(m)]
        ry = lambda r: y0 + (r - 0.5) * rowh
        for j, p in enumerate(P):
            cv.label(xs[j], top + 0.08, str(p), 10, 'ui_semi', INK2, ha='c', va='t')
        for i in sorted(range(k), key=lambda i: 1 if cols[i] != MUTE else 0):
            c = cols[i]
            pts = [(xs[j], ry(ranks[j][i])) for j in range(m)]
            cv.poly(pts, c, 2.5 if c != MUTE else 1.5)
            for (x, y) in pts:
                cv.dot(x, y, 0.05 if c != MUTE else 0.04, c)
        for i, s in enumerate(S):
            strong = cols[i] != MUTE
            cv.runs(x0 - 0.12, ry(ranks[0][i]), [(s['name'] + ' ', {'role': 'ui_med', 'color': INK if strong else INK2}),
                                                  (str(ranks[0][i]), {'role': 'ui_semi', 'color': INK3})], ha='r', size=10)
            cv.runs(x1 + 0.12, ry(ranks[-1][i]), [(f'{ranks[-1][i]}. ', {'role': 'ui_semi', 'color': INK3}),
                                                   (s['name'] + ' ', {'role': 'ui_semi' if strong else 'ui_med', 'color': INK if strong else INK2}),
                                                   (f(s['values'][-1]), {'role': 'ui', 'color': INK2})], size=10)

    # ── heatmap, scatter ──
    def _heat_color(self, sp, v, vmax):
        if v is None:
            return 'F3F2EE'
        if sp.get('scale', 'diverging') == 'diverging':
            c = sp.get('center', 0)
            t = (v - c) / (vmax or 1)
            return mix(NEUTRAL, sp.get('pos_color', REDS) if t > 0 else sp.get('neg_color', BLUE), min(1, abs(t)) ** 0.8)
        lo = sp.get('vmin', 0)
        t = (v - lo) / ((vmax - lo) or 1)
        return mix('EEF2F7', sp.get('color', BLUE), max(0, min(1, t)) * 0.95 + 0.05)

    def _c_heatmap(self, cv, sp):
        R, C, V = sp['rows'], sp['cols'], sp['values']
        dec = sp.get('dec', 1)
        allv = [v for r in V for v in r if v is not None]
        if sp.get('scale', 'diverging') == 'diverging':
            vmax = sp.get('vmax') or max(abs(v - sp.get('center', 0)) for v in allv)
        else:
            vmax = sp.get('vmax') or max(allv)
        # legend: 5 swatches
        lx, ly = cv.x, cv.y + 0.1
        cv.label(lx, ly + 0.02, sp.get('legend_title', ''), 9.5, 'ui_semi', INK, va='m')
        lx += self.tw(sp.get('legend_title', ''), 9.5, 'ui_semi') + 0.15
        if sp.get('scale', 'diverging') == 'diverging':
            c0 = sp.get('center', 0)
            stops = [c0 - vmax, c0 - vmax / 2, c0, c0 + vmax / 2, c0 + vmax]
        else:
            lo = sp.get('vmin', 0)
            stops = [lo + (vmax - lo) * t for t in (0, .25, .5, .75, 1)]
        for v in stops:
            cv.rrect(lx, ly - 0.06, 0.26, 0.16, self._heat_color(sp, v, vmax), r=0.03)
            t = self.num(v, dec, sign=sp.get('scale', 'diverging') == 'diverging')
            cv.label(lx + 0.31, ly + 0.02, t, 9, 'ui', INK2)
            lx += 0.31 + self.tw(t, 9, 'ui') + 0.18
        top = cv.y + 0.42
        lw = max(self.tw(r, 10, 'ui_med') for r in R) + 0.2
        cw = (cv.w - lw) / len(C)
        ch = min(0.42, (cv.y1 - top - 0.3) / len(R))
        gx = 0.025
        for j, c in enumerate(C):
            cv.label(cv.x + lw + (j + 0.5) * cw, top + 0.14, str(c), 9, 'ui', INK2, ha='c')
        y = top + 0.3
        for i, r in enumerate(R):
            cv.label(cv.x + lw - 0.12, y + ch / 2, r, 10, 'ui_med', INK, ha='r')
            for j in range(len(C)):
                v = V[i][j]
                col = self._heat_color(sp, v, vmax)
                cv.rrect(cv.x + lw + j * cw + gx / 2, y + gx / 2, cw - gx, ch - gx, col, r=0.03)
                if sp.get('show_values', True) and v is not None:
                    t = self.num(v, dec)
                    if self.tw(t, 8.5, 'ui') + 0.06 < cw:
                        cv.label(cv.x + lw + (j + 0.5) * cw, y + ch / 2, t, 8.5, 'ui_med', on(col), ha='c')
            y += ch

    def _c_scatter(self, cv, sp):
        P = sp['points']
        fx, fy = (lambda v: self.num(v, sp.get('x_dec', 0))), (lambda v: self.num(v, sp.get('y_dec', 1)))
        hl = [p for p in P if p.get('hi')]
        top = cv.y
        if hl or sp.get('trend'):
            leg = [(sp.get('hi_label', ''), sp.get('color', BLUE), 'dot')] if hl and sp.get('hi_label') else []
            leg += [(sp.get('other_label', ''), MUTE, 'dot')] if sp.get('other_label') else []
            if sp.get('trend'):
                leg.append((self.T['trend'], INK2, 'dash'))
            if leg:
                top += self._legend(cv, leg)
        xa, xb, xt, xst = nice(min(p['x'] for p in P), max(p['x'] for p in P), 5, zero=sp.get('x_zero', False))
        ya, yb, yt, yst = nice(min(p['y'] for p in P), max(p['y'] for p in P), 4, zero=sp.get('y_zero', False))
        lw = max(self.tw(self.num(t, step_dec(yst)), 10, 'ui') for t in yt) + 0.14
        if sp.get('y_title'):
            cv.label(cv.x, top + 0.02, sp['y_title'], 9.5, 'ui', INK3, va='t')
            top += 0.3
        x0, x1, y0, y1 = cv.x + lw, cv.x1 - 0.3, top + 0.08, cv.y1 - 0.55
        sx = lambda v: x0 + (v - xa) / (xb - xa) * (x1 - x0)
        sy = lambda v: y1 - (v - ya) / (yb - ya) * (y1 - y0)
        for t in yt:
            cv.seg(x0, sy(t), x1, sy(t), HAIR if t != 0 else BASE, 0.75, cap='flat')
            cv.label(x0 - 0.1, sy(t), self.num(t, step_dec(yst)), 10, 'ui', INK3, ha='r')
        for t in xt:
            cv.seg(sx(t), y0, sx(t), y1, HAIR, 0.75, cap='flat')
            cv.label(sx(t), y1 + 0.08, self.num(t, step_dec(xst)), 10, 'ui', INK3, ha='c', va='t')
        if sp.get('x_title'):
            cv.label(x1, y1 + 0.34, sp['x_title'], 9.5, 'ui', INK3, ha='r', va='t')
        if sp.get('trend') and len(P) > 2:
            n = len(P)
            mx_, my_ = sum(p['x'] for p in P) / n, sum(p['y'] for p in P) / n
            sxx = sum((p['x'] - mx_) ** 2 for p in P)
            sxy = sum((p['x'] - mx_) * (p['y'] - my_) for p in P)
            syy = sum((p['y'] - my_) ** 2 for p in P)
            b = sxy / sxx
            r = sxy / math.sqrt(sxx * syy)
            self.last_r = r
            xl, xr = min(p['x'] for p in P), max(p['x'] for p in P)
            cv.seg(sx(xl), sy(my_ + b * (xl - mx_)), sx(xr), sy(my_ + b * (xr - mx_)), INK2, 1.25, dash='dash', cap='flat')
            cv.label(sx(xr) - 0.02, sy(my_ + b * (xr - mx_)) - 0.17, f"r = {self.num(r, 2)}", 9.5, 'ui_semi', INK2,
                     ha='r')
        boxes = []
        for p in sorted(P, key=lambda p: 1 if p.get('hi') else 0):
            c = sp.get('color', BLUE) if p.get('hi') or not hl else MUTE
            cv.dot(sx(p['x']), sy(p['y']), 0.065 if p.get('hi') else 0.055, c)
        for p in sorted(P, key=lambda p: 0 if p.get('hi') else 1):
            if not p.get('label'):
                continue
            px, py = sx(p['x']), sy(p['y'])
            role = 'ui_semi' if p.get('hi') else 'ui'
            t = p.get('text', p['name'])
            tw, th = self.tw(t, 9.5, role) + 0.03, 0.17
            for dx, dy, ha in ((0.09, 0, 'l'), (-0.09, 0, 'r'), (0, -0.16, 'c'), (0, 0.16, 'c'), (0.09, -0.15, 'l'),
                               (0.09, 0.15, 'l'), (-0.09, -0.15, 'r'), (-0.09, 0.15, 'r')):
                lx = px + dx if ha == 'l' else (px + dx - tw if ha == 'r' else px - tw / 2)
                b = (lx, py + dy - th / 2, lx + tw, py + dy + th / 2)
                if b[2] > cv.x1 or b[0] < x0:
                    continue
                if not any(b[0] < o[2] and b[2] > o[0] and b[1] < o[3] and b[3] > o[1] for o in boxes):
                    break
            boxes.append(b)
            cv.label(b[0], (b[1] + b[3]) / 2, t, 9.5, role, INK if p.get('hi') else INK2)

    # ── small multiples, sparkline table ──
    def _mini(self, cv, x, y, w, h, cats, vals, kind='col', polarity=True, color=BLUE, dec=1, unit='', axis=True):
        vv = [v for v in vals if v is not None]
        lo, hi = (min(vv + [0]), max(vv + [0])) if kind == 'col' else (min(vv), max(vv))   # bars start at zero
        if hi == lo:
            hi = lo + 1
        sy = lambda v: y + h - (v - lo) / (hi - lo) * h
        n = len(vals)
        if axis:
            for t in (hi, lo):
                if t != 0:
                    cv.seg(x, sy(t), x + w, sy(t), HAIR, 0.5, cap='flat')
                    cv.label(x + w + 0.04, sy(t), self.num(t, dec), 8, 'ui', INK3)
        if kind == 'col':
            slot = w / n
            th = min(BAR_MAX * 0.6, slot * 0.66)
            for k, v in enumerate(vals):
                if v is None:
                    continue
                cx = x + (k + 0.5) * slot
                c = (BLUE if v >= 0 else REDS) if polarity else color
                cv.bar(cx - th / 2, sy(v), cx + th / 2, sy(0), c, 't' if v >= 0 else 'b', r=0.03)
        else:
            xs = [x + k * w / max(n - 1, 1) for k in range(n)]
            pts = [(xs[k], sy(v)) for k, v in enumerate(vals) if v is not None]
            cv.poly(pts, color, 1.75)
            cv.dot(pts[-1][0], pts[-1][1], 0.045, color)
        if lo <= 0 <= hi:
            cv.seg(x, sy(0), x + w, sy(0), BASE, 0.75, cap='flat')

    def _c_multiples(self, cv, sp):
        P = sp['panels']
        cols = sp.get('cols', 4)
        rows = math.ceil(len(P) / cols)
        gx, gy = 0.4, 0.3
        top = cv.y
        if sp.get('legend', True) and sp.get('polarity', True):
            top += self._legend(cv, [(sp.get('pos_label', 'Dương' if self.lang == 'vi' else 'Positive'), BLUE, 'rect'),
                                     (sp.get('neg_label', 'Âm' if self.lang == 'vi' else 'Negative'), REDS, 'rect')])
        cw = (cv.w - gx * (cols - 1)) / cols
        ch = (cv.y1 - top - gy * (rows - 1)) / rows
        for i, p in enumerate(P):
            r, c = divmod(i, cols)
            x, y = cv.x + c * (cw + gx), top + r * (ch + gy)
            cv.seg(x, y, x + cw, y, INK, 1.25, cap='flat')
            cv.label(x, y + 0.06, p['title'], 10, 'ui_semi', INK2, va='t')
            last = [v for v in p['values'] if v is not None][-1]
            dec = p.get('dec', sp.get('dec', 1))
            cv.runs(x, y + 0.48, [(self.num(last, dec), {'role': 'ui_semi', 'size': 18, 'color': INK}),
                                  (' ' + p.get('unit', sp.get('unit', '')), {'role': 'ui', 'size': 10, 'color': INK2}),
                                  ('  ' + str(p['categories'][-1]), {'role': 'ui', 'size': 9, 'color': INK3})])
            my, mh = y + 0.78, ch - 0.78 - 0.28
            self._mini(cv, x, my, cw - 0.45, mh, p['categories'], p['values'], p.get('kind', 'col'),
                       sp.get('polarity', True), p.get('color', BLUE), dec)
            cv.label(x, my + mh + 0.06, str(p['categories'][0]), 8.5, 'ui', INK3, va='t')
            cv.label(x + cw - 0.45, my + mh + 0.06, str(p['categories'][-1]), 8.5, 'ui', INK3, ha='r', va='t')

    def _c_spark_table(self, cv, sp):
        R = sp['rows']
        H = sp.get('headers', ('', '', '', ''))
        yrs = sp.get('years', [])
        wname, wsp, wlast, wchg = cv.w * 0.34, cv.w * 0.34, cv.w * 0.14, cv.w * 0.18
        xs = [cv.x, cv.x + wname, cv.x + wname + wsp, cv.x + wname + wsp + wlast]
        hy = cv.y + 0.12
        cv.label(xs[0], hy, H[0], 9, 'ui_semi', INK2, caps=True, spacing=0.6)
        cv.label(xs[1] + 0.15, hy, H[1], 9, 'ui_semi', INK2, caps=True, spacing=0.6)
        cv.label(xs[2] + wlast - 0.1, hy, H[2], 9, 'ui_semi', INK2, ha='r', caps=True, spacing=0.6)
        cv.label(xs[3] + wchg - 0.05, hy, H[3], 9, 'ui_semi', INK2, ha='r', caps=True, spacing=0.6)
        cv.seg(cv.x, cv.y + 0.3, cv.x1, cv.y + 0.3, INK, 1, cap='flat')
        rh = min(0.6, (cv.y1 - cv.y - 0.35) / len(R))
        y = cv.y + 0.33
        for i, r in enumerate(R):
            if i % 2 == 1:
                cv.rect(cv.x, y, cv.w, rh, ZEBRA)
            dec, unit = r.get('dec', 1), r.get('unit', '')
            cv.label(xs[0] + 0.08, y + rh * 0.36, r['name'], 11, 'ui_med', INK)
            if r.get('sub'):
                cv.label(xs[0] + 0.08, y + rh * 0.72, r['sub'], 8.5, 'ui', INK3)
            v = r['values']
            vv = [q for q in v if q is not None]
            lo, hi = min(vv), max(vv)
            sx0, sx1, sy0, sy1 = xs[1] + 0.15, xs[1] + wsp - 0.6, y + 0.1, y + rh - 0.1
            syf = lambda q: sy1 - (q - lo) / ((hi - lo) or 1) * (sy1 - sy0)
            n = len(v)
            pts = [(sx0 + k * (sx1 - sx0) / (n - 1), syf(q)) for k, q in enumerate(v) if q is not None]
            if lo < 0 < hi:
                cv.seg(sx0, syf(0), sx1, syf(0), BASE, 0.5, cap='flat')
            cv.poly(pts, INK2, 1.5)
            kmin = min(range(n), key=lambda k: v[k] if v[k] is not None else 1e9)
            cv.dot(sx0 + kmin * (sx1 - sx0) / (n - 1), syf(v[kmin]), 0.035, MUTE, ring=cv.bg, ring_w=0.5)
            cv.dot(pts[-1][0], pts[-1][1], 0.05, BLUE)
            cv.label(xs[2] + wlast - 0.1, y + rh / 2, self.num(v[-1], dec) + unit, 12, 'ui_semi', INK, ha='r')
            first = next(q for q in v if q is not None)
            ch = v[-1] - first
            ctext = self.num(ch, dec, sign=True) + r.get('chg_unit', ' đ.%' if self.lang == 'vi' else ' pp')
            tx = xs[3] + wchg - 0.05
            cv.label(tx, y + rh / 2, ctext, 10.5, 'ui', INK2, ha='r')
            if abs(ch) >= 10 ** -dec / 2:
                cv.tri(tx - self.tw(ctext, 10.5, 'ui') - 0.15, y + rh / 2, 0.11, r.get('up_color', BLUE) if ch > 0
                       else r.get('down_color', REDS), down=ch < 0)
            y += rh
        cv.seg(cv.x, y, cv.x1, y, RULE, 0.75, cap='flat')

    # ── population pyramid ──
    def _c_pyramid(self, cv, sp):
        B, L, R = sp['bands'], sp['left'], sp['right']
        cmp_ = sp.get('compare')
        f = self._fmt(sp)
        cl, cr = sp.get('colors', (BLUE, ORANGE))
        leg = [(L['name'], cl, 'rect'), (R['name'], cr, 'rect')]
        if cmp_:
            leg.append((cmp_['name'], INK, 'outline'))
        top = cv.y + self._legend(cv, leg)
        allv = L['values'] + R['values'] + ((cmp_['left'] + cmp_['right']) if cmp_ else [])
        a, b, ticks, st = nice(0, max(allv), 3)
        mid = cv.x + cv.w / 2
        cw = 0.62
        half = cv.w / 2 - cw / 2 - 0.45
        n = len(B)
        rowh = (cv.y1 - top - 0.35) / n
        th = min(0.22, rowh * 0.74)
        sl = lambda v: mid - cw / 2 - v / b * half
        sr = lambda v: mid + cw / 2 + v / b * half
        for t in ticks:
            for xx in (sl(t), sr(t)):
                cv.seg(xx, top + 0.05, xx, top + n * rowh + 0.05, HAIR, 0.5, cap='flat')
                cv.label(xx, top + n * rowh + 0.1, self.num(t, step_dec(st)) + sp.get('unit', ''), 9, 'ui', INK3,
                         ha='c', va='t')
        for i in range(n):
            k = n - 1 - i                          # oldest band at the top
            y = top + 0.05 + (i + 0.5) * rowh
            cv.label(mid, y, B[k], 9, 'ui_med', INK2, ha='c')
            cv.bar(sl(L['values'][k]), y - th / 2, sl(0), y + th / 2, cl, 'l', r=0.035)
            cv.bar(sr(0), y - th / 2, sr(R['values'][k]), y + th / 2, cr, 'r', r=0.035)
            if cmp_:
                cv.rect(sl(cmp_['left'][k]), y - th / 2, sl(0) - sl(cmp_['left'][k]), th, None, INK, 1, dash='sdash')
                cv.rect(sr(0), y - th / 2, sr(cmp_['right'][k]) - sr(0), th, None, INK, 1, dash='sdash')
        for a_ in sp.get('annotations', []):
            k = B.index(a_['band'])
            i = n - 1 - k
            y = top + 0.05 + (i + 0.5) * rowh
            side = a_.get('side', 'r')
            v = (R if side == 'r' else L)['values'][k] if not a_.get('compare') else cmp_['right' if side == 'r' else 'left'][k]
            px = sr(v) if side == 'r' else sl(v)
            self._ann(cv, px, y, a_['text'], a_.get('tier', 'pri'), a_.get('dx', 0.35 if side == 'r' else -0.35),
                      a_.get('dy', -0.25))

    # ── tile map ──
    def _c_tilemap(self, cv, sp):
        T = sp['tiles']
        dec = sp.get('dec', 1)
        unit = sp.get('unit', '')
        vals = [t['value'] for t in T if t.get('value') is not None]
        br = sp.get('breaks') or [min(vals) + (max(vals) - min(vals)) * q for q in (0.2, 0.4, 0.6, 0.8)]
        ramp = [mix('EEF2F7', sp.get('color', BLUE), t) for t in (0.12, 0.32, 0.55, 0.78, 1.0)]

        def col(v):
            if v is None:
                return 'F3F2EE'
            return ramp[sum(1 for b in br if v >= b)]
        # legend
        lx, ly = cv.x, cv.y + 0.1
        if sp.get('legend_title'):
            cv.label(lx, ly + 0.02, sp['legend_title'], 9.5, 'ui_semi', INK)
            lx += self.tw(sp['legend_title'], 9.5, 'ui_semi') + 0.15
        edges = [None] + list(br) + [None]
        for i, c in enumerate(ramp):
            a, b = edges[i], edges[i + 1]
            t = (f'< {self.num(b, dec)}' if a is None else (f'≥ {self.num(a, dec)}' if b is None else
                                                             f'{self.num(a, dec)}–{self.num(b, dec)}')) + unit
            cv.rrect(lx, ly - 0.06, 0.26, 0.16, c, r=0.03)
            cv.label(lx + 0.31, ly + 0.02, t, 9, 'ui', INK2)
            lx += 0.31 + self.tw(t, 9, 'ui') + 0.2
        if any(t.get('value') is None for t in T):
            cv.rrect(lx, ly - 0.06, 0.26, 0.16, 'F3F2EE', r=0.03, line=RULE, lw=0.5)
            cv.label(lx + 0.31, ly + 0.02, sp.get('missing', 'chưa công bố' if self.lang == 'vi' else 'not published'), 9,
                     'ui', INK2)
        panels = sp.get('panels') or ['']
        top = cv.y + 0.45
        pw = (cv.w - 0.4 * (len(panels) - 1)) / len(panels)
        for pi, title in enumerate(panels):
            tiles = [t for t in T if t.get('panel', 0) == pi]
            if not tiles:
                continue
            r0, r1 = min(t['row'] for t in tiles), max(t['row'] for t in tiles)
            c0, c1 = min(t['col'] for t in tiles), max(t['col'] for t in tiles)
            nr, nc = r1 - r0 + 1, c1 - c0 + 1
            px = cv.x + pi * (pw + 0.4)
            ty = top + (0.3 if title else 0)
            if title:
                cv.label(px, top + 0.04, title, 10, 'ui_semi', INK2, va='t', caps=True, spacing=0.6)
            ts = min((cv.y1 - ty) / nr, pw / nc)
            tw_, th_ = min(ts * 1.45, pw / nc), ts
            for t in tiles:
                x = px + (t['col'] - c0) * tw_
                y = ty + (t['row'] - r0) * th_
                c = col(t.get('value'))
                cv.rrect(x + 0.02, y + 0.02, tw_ - 0.04, th_ - 0.04, c, r=0.05)
                tc = on(c) if t.get('value') is not None else INK2
                cv.label(x + 0.07, y + 0.1, t.get('short', t['name']), 7.5, 'ui_med', tc, va='t')
                cv.label(x + 0.07, y + th_ - 0.08, self.num(t['value'], dec) if t.get('value') is not None else '—', 9.5,
                         'ui_semi', tc, va='b')

    # ── flow (Sankey-style) ──
    def _c_flow(self, cv, sp):
        cols, links = sp['columns'], sp['links']
        f = self._fmt(sp)
        node = {}
        for ci, col in enumerate(cols):
            for nd in col:
                node[nd['id']] = dict(nd, col=ci, inv=0.0, outv=0.0)
        for s_, d_, v in links:
            node[s_]['outv'] += v
            node[d_]['inv'] += v
        for nd in node.values():
            nd['v'] = max(nd['inv'], nd['outv'])
        gap = 0.16
        top = cv.y + 0.1
        avail = cv.h - 0.2
        k = min((avail - gap * (len(c) - 1)) / sum(node[n['id']]['v'] for n in c) for c in cols)
        lab_w = [max(self.tw(n['name'], 10, 'ui_med') for n in c) + 0.1 for c in cols]
        val_w = [max(self.tw(f(node[n['id']]['v']), 10, 'ui_semi') for n in c) + 0.1 for c in cols]
        nw = 0.13
        left_pad = max(lab_w[0], val_w[0]) + 0.15
        right_pad = max(lab_w[-1], val_w[-1]) + 0.15
        span = cv.w - left_pad - right_pad - nw
        cx = [cv.x + left_pad + i * span / (len(cols) - 1) for i in range(len(cols))]
        for ci, col in enumerate(cols):
            tot = sum(node[n['id']]['v'] for n in col) * k + gap * (len(col) - 1)
            y = top + (avail - tot) / 2
            for n in col:
                nd = node[n['id']]
                nd['y'] = y
                nd['h'] = nd['v'] * k
                nd['oy'] = nd['iy'] = y
                y += nd['h'] + gap
        for s_, d_, v in links:
            a, b = node[s_], node[d_]
            h = v * k
            xa, xb = cx[a['col']] + nw, cx[b['col']]
            ya, yb = a['oy'], b['iy']
            a['oy'] += h
            b['iy'] += h
            N = 24
            curve = lambda y0, y1: [(xa + (xb - xa) * t, y0 + (y1 - y0) * (3 * t * t - 2 * t * t * t))
                                    for t in [i / N for i in range(N + 1)]]
            topc, botc = curve(ya, yb), curve(ya + h, yb + h)
            c = sp.get('link_color', {}).get((s_, d_)) or (a.get('color') if a['col'] == 0 or not b.get('color')
                                                             else b.get('color')) or INK3
            cv.poly(topc + botc[::-1], None, fill=c, alpha=0.3, close=True)
        for nd in node.values():
            c = nd.get('color', INK)
            cv.rect(cx[nd['col']], nd['y'], nw, max(nd['h'], 0.01), c)
            ci = nd['col']
            ym = nd['y'] + nd['h'] / 2
            if ci == 0:
                if nd['h'] > 0.3:                      # name above value
                    cv.label(cx[ci] - 0.1, ym - 0.1, nd['name'], 10, 'ui_med', INK, ha='r')
                    cv.label(cx[ci] - 0.1, ym + 0.1, f(nd['v']), 10, 'ui_semi', INK2, ha='r')
                else:                                  # thin node: "value  name" on one line
                    cv.runs(cx[ci] - 0.1, ym, [(f(nd['v']) + '  ', {'role': 'ui_semi', 'color': INK2}),
                                               (nd['name'], {'role': 'ui_med'})], ha='r', size=10)
            elif ci == len(cols) - 1:
                cv.label(cx[ci] + nw + 0.1, ym - (0.1 if nd['h'] > 0.3 else 0), nd['name'], 10, 'ui_med', INK)
                if nd['h'] > 0.3:
                    cv.label(cx[ci] + nw + 0.1, ym + 0.1, f(nd['v']), 10, 'ui_semi', INK2)
                else:
                    cv.label(cx[ci] + nw + 0.1 + self.tw(nd['name'], 10, 'ui_med') + 0.1, ym, f(nd['v']), 10, 'ui_semi',
                             INK2)
            else:
                cv.label(cx[ci] + nw / 2, nd['y'] - 0.06, nd['name'], 10, 'ui_semi', INK, ha='c', va='b')
                cv.label(cx[ci] + nw / 2, nd['y'] + nd['h'] + 0.06, f(nd['v']), 10, 'ui_semi', INK2, ha='c', va='t')

    # ── lever / causal diagram ──
    def _c_levers(self, cv, sp):
        C, links = sp['columns'], sp['links']
        hl = set(sp.get('highlight', []))
        nc = len(C)
        gapx = 0.75
        bw = (cv.w - gapx * (nc - 1)) / nc
        pos = {}
        for ci, col in enumerate(C):
            x = cv.x + ci * (bw + gapx)
            cv.label(x, cv.y + 0.02, col['title'], 9.5, 'ui_semi', RED, va='t', caps=True, spacing=1)
            n = len(col['items'])
            top = cv.y + 0.4
            vg = sp.get('vgap', 0.3)
            bh = min(0.9, (cv.y1 - top - vg * (n - 1)) / n)
            tot = n * bh + (n - 1) * vg
            y = top + (cv.y1 - top - tot) / 2
            for it in col['items']:
                pos[it['id']] = (x, y, bw, bh, it)
                y += bh + vg
        col_of = {it['id']: ci for ci, col in enumerate(C) for it in col['items']}
        for a, b in links:
            xa, ya, wa, ha, _ = pos[a]
            xb, yb, wb, hb, _ = pos[b]
            strong = (a in hl and b in hl) if hl else True
            kw = dict(color=INK if strong else MUTE, w=1.25 if strong else 1, tail='triangle')
            if col_of[a] == col_of[b]:                   # same column: vertical arrow, bottom -> top
                cv.seg(xa + wa / 2, ya + ha, xb + wb / 2, yb - 0.02, **kw)
            else:
                cv.seg(xa + wa, ya + ha / 2, xb - 0.02, yb + hb / 2, **kw)
        for key, (x, y, w, h, it) in pos.items():
            strong = (key in hl) if hl else True
            cv.rrect(x, y, w, h, SURF if strong else 'FFFFFF', r=0.06, line=INK if (hl and strong) else MUTE,
                     lw=1.25 if (hl and strong) else 0.75, dash=None if not it.get('dashed') else 'sdash')
            paras = [{'text': it['name'], 'size': 11, 'role': 'ui_semi', 'color': INK if strong else INK2, 'line': 1.0}]
            if it.get('sub'):
                paras.append({'text': it['sub'], 'size': 9.5, 'role': 'ui', 'color': INK2, 'line': 1.0, 'before': 2})
            self._tx(cv.s, x + 0.12, y + 0.06, w - 0.24, h - 0.12, paras, anchor='m')

    # ── timeline ──
    def _c_timeline(self, cv, sp):
        Ev = sp['events']
        n = len(Ev)
        ay = cv.y + cv.h * sp.get('axis_at', 0.5)
        x0, x1 = cv.x + 0.2, cv.x1 - 0.2
        xs = [x0 + (i + 0.5) * (x1 - x0) / n for i in range(n)]
        today = sp.get('today')
        fut = [i for i, e in enumerate(Ev) if e.get('future')]
        split = (xs[fut[0] - 1] + xs[fut[0]]) / 2 if fut and fut[0] > 0 else (x0 if fut else x1)
        cv.seg(x0, ay, split, ay, INK, 2, cap='flat')
        if fut:
            cv.seg(split, ay, x1, ay, INK2, 2, dash='dash', cap='flat')
            if today:
                cv.seg(split, ay - 0.2, split, ay + 0.2, RED, 1.5, cap='flat')
                cv.label(split + 0.06, ay - 0.13, today, 8.5, 'ui_semi', RED)
        colw = (x1 - x0) / n - 0.12
        for i, e in enumerate(Ev):
            up = i % 2 == 0
            x = xs[i]
            future = e.get('future')
            cv.seg(x, ay, x, ay + (-0.32 if up else 0.32), INK3, 0.75, cap='flat')
            cv.dot(x, ay, 0.075, INK if not future else INK2, hollow=bool(future))
            nl_t = self.nlines(e['title'], 11, 'ui_semi', colw)
            nl_x = self.nlines(e.get('text', ''), 9.5, 'ui', colw) if e.get('text') else 0
            bh = self.lh(10) + 2 / 72 + self.lh(11, 1.0, nl_t) + self.lh(9.5, 1.0, nl_x) + 0.08
            ty = ay - 0.36 - bh if up else ay + 0.36
            paras = [{'text': e['date'], 'size': 10, 'role': 'ui_semi', 'color': RED if not future else INK2,
                      'after': 2}, {'text': e['title'], 'size': 11, 'role': 'ui_semi', 'color': INK, 'line': 1.0}]
            if e.get('text'):
                paras.append({'text': e['text'], 'size': 9.5, 'role': 'ui', 'color': INK2, 'line': 1.0, 'before': 2})
            self._tx(cv.s, x - colw / 2, ty, colw, bh, paras, align='c', anchor='b' if up else 't')

    # ── bullet chart ──
    def _c_bullet(self, cv, sp):
        R = sp['rows']
        top = cv.y + self._legend(cv, [(sp.get('actual_label', 'Thực hiện' if self.lang == 'vi' else 'Actual'),
                                        sp.get('color', INK), 'rect'),
                                       (sp.get('target_label', 'Mục tiêu' if self.lang == 'vi' else 'Target'), RED, 'tick'),
                                       (sp.get('range_label', 'Vùng tham chiếu' if self.lang == 'vi' else 'Reference bands'),
                                        'E4E2DB', 'rect')])
        lw = max(max(self.tw(r['name'], 11.5, 'ui_semi'), self.tw(r.get('sub', ''), 9, 'ui')) for r in R) + 0.3
        rw = max(self.tw(self.num(r['value'], r.get('dec', 1)) + r.get('unit', ''), 12, 'ui_bold') +
                 self.tw('  ' + r.get('value_text', ''), 9.5, 'ui') for r in R) + 0.3
        rowh = min(0.85, (cv.y1 - top - 0.1) / len(R))
        x0, x1 = cv.x + lw, cv.x1 - max(rw, 2.0)
        for i, r in enumerate(R):
            y = top + 0.1 + (i + 0.5) * rowh
            cv.label(cv.x, y - 0.09, r['name'], 11.5, 'ui_semi', INK)
            if r.get('sub'):
                cv.label(cv.x, y + 0.12, r['sub'], 9, 'ui', INK3)
            mx = r['max']
            hx = _HX(x0, x1, 0, mx)
            bh = min(0.34, rowh * 0.5)
            rs = [0] + list(r.get('ranges', [])) + [mx]
            shades = ['EEECE6', 'E4E2DB', 'D9D7CF', 'CFCDC4']
            for q in range(len(rs) - 1):
                cv.rect(hx.sx(rs[q]), y - bh / 2, hx.sx(rs[q + 1]) - hx.sx(rs[q]), bh, shades[q % len(shades)])
            th = bh * 0.38
            cv.bar(hx.sx(0), y - th / 2, hx.sx(r['value']), y + th / 2, r.get('color', sp.get('color', INK)), 'r', r=0.03)
            tx = hx.sx(r['target'])
            cv.seg(tx, y - bh / 2 - 0.05, tx, y + bh / 2 + 0.05, RED, 2.5, cap='flat')
            for t in (0, mx):
                cv.label(hx.sx(t), y + bh / 2 + 0.04, self.num(t, r.get('tick_dec', 0)) + r.get('unit', ''), 8, 'ui', INK3,
                         ha='c' if t else 'l', va='t')
            cv.runs(x1 + 0.2, y, [(self.num(r['value'], r.get('dec', 1)) + r.get('unit', ''), {'role': 'ui_bold', 'size': 12}),
                                  ('  ' + r.get('value_text', ''), {'role': 'ui', 'size': 9.5, 'color': INK2})])

    # ── KPI tiles ──
    def _c_tiles(self, cv, sp):
        t = sp['tiles']
        n = len(t)
        gap = 0.35
        tw_ = (cv.w - gap * (n - 1)) / n
        th_ = min(cv.h, 3.3)
        y = cv.y + (cv.h - th_) / 2
        for i, k in enumerate(t):
            x = cv.x + i * (tw_ + gap)
            cv.rect(x, y, tw_, th_, SURF)
            cv.seg(x, y, x + tw_, y, INK, 2.5, cap='flat')
            px = x + 0.22
            iw = tw_ - 0.44
            cv.label(px, y + 0.3, k['label'], 9.5, 'ui_semi', INK2, caps=True, spacing=0.8)
            vs = 44 if n <= 3 else 38
            while vs > 26 and self.tw(k['value'], vs, 'ui_bold') + self.tw(' ' + k.get('unit', ''), 15, 'ui_semi') > iw:
                vs -= 2
            cv.runs(px, y + 0.88, [(k['value'], {'role': 'ui_bold', 'size': vs}),
                                   (' ' + k.get('unit', ''), {'role': 'ui_semi', 'size': 15, 'color': INK2})])
            yy = y + 1.45
            if k.get('delta') is not None:
                d = k['delta']
                tone = k.get('tone') or (BLUE if d > 0 else (REDS if d < 0 else INK3))
                if abs(d) > 1e-12:
                    cv.tri(px + 0.07, yy, 0.13, tone, down=d < 0)
                txt = k.get('delta_text') or self.num(d, k.get('delta_dec', 1), sign=True) + k.get('delta_unit', '')
                cv.runs(px + 0.2, yy, [(txt, {'role': 'ui_semi', 'size': 12, 'color': INK}),
                                       ('  ' + k.get('delta_label', ''), {'role': 'ui', 'size': 10, 'color': INK2})])
                yy += 0.32
            if k.get('spark'):
                sv = k['spark']
                self._mini(cv, px, yy + 0.08, iw - 0.5, 0.55, list(range(len(sv))), sv, k.get('spark_kind', 'line'),
                           False, k.get('spark_color', BLUE), k.get('dec', 1), axis=False)
                yy += 0.75
            if k.get('sub'):
                nl = self.nlines(k['sub'], 9.5, 'ui', iw)
                self._tx(cv.s, px, y + th_ - 0.18 - nl * 0.17, iw, nl * 0.17 + 0.04, k['sub'], 9.5, INK3, 'ui', line=1.0)

    # ── native, editable charts (editable=True) ──
    def _native(self, s, x, y, w, h, sp):
        t = sp['type']
        if t == 'scatter':
            cd = XyChartData()
            se = cd.add_series(sp.get('name', 'Points'))
            for p in sp['points']:
                se.add_data_point(p['x'], p['y'])
            gf = s.shapes.add_chart(XL_CHART_TYPE.XY_SCATTER, E(x), E(y), E(w), E(h), cd)
            ch = gf.chart
            self._native_style(ch, sp, multi=False)
            ser = ch.plots[0].series[0]
            ser.marker.style = XL_MARKER_STYLE.CIRCLE
            ser.marker.size = 7
            ser.marker.format.fill.solid()
            ser.marker.format.fill.fore_color.rgb = rgb(sp.get('color', BLUE))
            ser.marker.format.line.color.rgb = rgb(PAPER)
            ser.format.line.fill.background()
            return gf
        cats = sp['categories']
        S = sp['series'] if 'series' in sp else [{'name': sp.get('name', ''), 'values': sp['values']}]
        kind = {'line': XL_CHART_TYPE.LINE, 'area': XL_CHART_TYPE.AREA_STACKED, 'bar': XL_CHART_TYPE.COLUMN_CLUSTERED,
                'barh': XL_CHART_TYPE.BAR_CLUSTERED, 'diverging': XL_CHART_TYPE.BAR_CLUSTERED,
                'stacked': XL_CHART_TYPE.COLUMN_STACKED, 'stacked_h': XL_CHART_TYPE.BAR_STACKED,
                'stacked100_h': XL_CHART_TYPE.BAR_STACKED_100}[t]
        nf = sp.get('number_format', '#,##0' + ('.' + '0' * sp.get('dec', 1) if sp.get('dec', 1) else ''))
        cd = CategoryChartData()
        cd.categories = [str(c) for c in cats]
        for se in S:
            cd.add_series(se['name'], se['values'], nf)
        gf = s.shapes.add_chart(kind, E(x), E(y), E(w), E(h), cd)
        ch = gf.chart
        multi = len(S) > 1
        self._native_style(ch, sp, multi)
        plot = ch.plots[0]
        if t in ('bar', 'barh', 'diverging', 'stacked', 'stacked_h', 'stacked100_h'):
            plot.gap_width = sp.get('gap', 70 if t in ('bar', 'stacked') else 45)
            if t.startswith('stacked'):
                plot.overlap = 100
        if t in ('barh', 'diverging', 'stacked_h', 'stacked100_h'):
            ch.category_axis.reverse_order = True
        hi = sp.get('highlight')
        hi = set(hi if isinstance(hi, (list, tuple)) else ([] if hi is None else [hi]))
        for i, (se, ps) in enumerate(zip(S, plot.series)):
            col = MUTE if se.get('muted') else se.get('color', PALETTE[i % len(PALETTE)])
            if t == 'line':
                ln = ps.format.line
                ln.color.rgb = rgb(col)
                ln.width = Pt(se.get('width', 2.25))
                if se.get('dashed'):
                    ln.dash_style = MSO_LINE_DASH_STYLE.DASH
                ps.smooth = False
                ps.marker.style = XL_MARKER_STYLE.NONE
            else:
                ps.format.fill.solid()
                ps.format.fill.fore_color.rgb = rgb(col)
                ps.format.line.color.rgb = rgb(PAPER)
                ps.format.line.width = Pt(1.5 if t.startswith('stacked') or t == 'area' else 0)
                if t == 'diverging':
                    ps.invert_if_negative = False
                    for j, v in enumerate(se['values']):
                        pt = ps.points[j]
                        pt.format.fill.solid()
                        pt.format.fill.fore_color.rgb = rgb(BLUE if (v or 0) >= 0 else REDS)
                elif hi and not multi:
                    for j in range(len(cats)):
                        pt = ps.points[j]
                        pt.format.fill.solid()
                        pt.format.fill.fore_color.rgb = rgb(col if j in hi else MUTE)
            if sp.get('labels', t in ('bar', 'barh', 'diverging')) and t not in ('line', 'area'):
                ps.data_labels.show_value = True
                ps.data_labels.font.size = Pt(10)
                ps.data_labels.font.name = self.font('ui_semi')[0]
                ps.data_labels.font.color.rgb = rgb(INK2 if not t.startswith('stacked') else PAPER)
                ps.data_labels.number_format = nf
                ps.data_labels.number_format_is_linked = False
        return gf

    def _native_style(self, ch, sp, multi):
        fam = self.font('ui')[0]
        self._used.add(fam)
        ch.font.size = Pt(10.5)
        ch.font.name = fam
        ch.font.color.rgb = rgb(INK2)
        ch.has_title = False
        ch.has_legend = multi
        if multi:
            ch.legend.position = XL_LEGEND_POSITION.TOP
            ch.legend.include_in_layout = False
            ch.legend.font.size = Pt(10.5)
            ch.legend.font.color.rgb = rgb(INK)
        va, ca = ch.value_axis, ch.category_axis
        va.has_major_gridlines = True
        va.major_gridlines.format.line.color.rgb = rgb(HAIR)
        va.major_gridlines.format.line.width = Pt(0.75)
        va.format.line.fill.background()
        va.tick_labels.font.size = Pt(10)
        va.tick_labels.font.color.rgb = rgb(INK3)
        if sp.get('y_min') is not None:
            va.minimum_scale = sp['y_min']
        if sp.get('y_max') is not None:
            va.maximum_scale = sp['y_max']
        ca.format.line.color.rgb = rgb(BASE)
        ca.format.line.width = Pt(1)
        ca.has_major_gridlines = False
        ca.tick_labels.font.size = Pt(10)
        ca.tick_labels.font.color.rgb = rgb(INK2)
        try:
            ca.tick_label_position = XL_TICK_LABEL_POSITION.LOW
        except Exception:
            pass

    # ── slide types ──
    def cover(self, kicker, title, subtitle='', date='', chart=None, note=''):
        s = self._slide(SURF)
        r = _clean(s.shapes.add_connector(MSO_CONNECTOR.STRAIGHT, E(MX), E(0.62), E(SW - MX), E(0.62)))
        _line(r, INK, 5, cap='flat')
        self._tx(s.shapes, MX, 0.82, 8, 0.3, self.brand, 11, INK, 'ui_semi', caps=True, spacing=1.6)
        self._tx(s.shapes, SW - MX - 4, 0.82, 4, 0.3, date, 11, INK2, 'ui', align='r')
        tw = 7.3 if chart else SW - 2 * MX
        self._tx(s.shapes, MX, 2.05, tw, 0.35, kicker, 12, RED, 'ui_semi', caps=True, spacing=1.6)
        size = 48
        while size > 34 and self.nlines(title, size, 'head', tw) > 3:
            size -= 2
        nl = self.nlines(title, size, 'head', tw)
        self._tx(s.shapes, MX, 2.5, tw, self.lh(size, 0.98, nl) + 0.1, title, size, INK, 'head', line=0.98)
        y = 2.5 + self.lh(size, 0.98, nl) + 0.32
        if subtitle:
            ns = self.nlines(subtitle, 17, 'ui', tw)
            self._tx(s.shapes, MX, y, tw, self.lh(17, 1.2, ns), subtitle, 17, INK2, 'ui', line=1.2)
        if chart:
            self._chart(s, 8.55, 2.1, SW - MX - 8.55, 3.7, chart, bg=SURF)
        r = _clean(s.shapes.add_connector(MSO_CONNECTOR.STRAIGHT, E(MX), E(SH - 0.95), E(SW - MX), E(SH - 0.95)))
        _line(r, INK, 0.75, cap='flat')
        if note:
            self._tx(s.shapes, MX, SH - 0.82, SW - 2 * MX, 0.4, note, 10, INK2, 'ui')
        return s

    def section(self, number, title, dek='', items=()):
        s = self._slide(SURF)
        r = _clean(s.shapes.add_connector(MSO_CONNECTOR.STRAIGHT, E(MX), E(0.62), E(SW - MX), E(0.62)))
        _line(r, INK, 3.5, cap='flat')
        num = f'{number:02d}' if isinstance(number, int) else str(number)
        self._tx(s.shapes, MX, 1.6, 4, 1.6, num, 120, RED, 'head', line=0.9)
        self._tx(s.shapes, MX, 3.45, 7.2, 1.6, title, 38, INK, 'head', line=1.0)
        nl = self.nlines(title, 38, 'head', 7.2)
        if dek:
            self._tx(s.shapes, MX, 3.45 + self.lh(38, 1.0, nl) + 0.15, 7.0, 1.2, dek, 15, INK2, 'ui', line=1.25)
        if items:
            x = 8.7
            self._tx(s.shapes, x, 1.75, SW - MX - x, 0.3, self.T['in_chapter'], 10, INK2, 'ui_semi', caps=True,
                     spacing=1.2)
            y = 2.15
            for it in items:
                c = _clean(s.shapes.add_connector(MSO_CONNECTOR.STRAIGHT, E(x), E(y), E(SW - MX), E(y)))
                _line(c, RULE, 0.75, cap='flat')
                nl2 = self.nlines(it, 12.5, 'ui', SW - MX - x)
                self._tx(s.shapes, x, y + 0.1, SW - MX - x, self.lh(12.5, 1.1, nl2) + 0.05, it, 12.5, INK, 'ui', line=1.1)
                y += self.lh(12.5, 1.1, nl2) + 0.3
        self._foot(s, '')
        return s

    def hero(self, kicker, headline, big, unit='', dek='', counts=(), chart=None, chart_title='', source='', note=None):
        s = self._slide()
        top = self._head(s, kicker, headline)
        lw = 6.0
        size = 96
        while size > 60 and self.tw(big, size, 'ui_bold') + self.tw(' ' + unit, 24, 'ui_semi') > lw:
            size -= 4
        self._tx(s.shapes, MX, top + 0.05, lw, size / 72 * 1.05, [[(big, {'role': 'ui_bold', 'size': size}),
                                                                   (' ' + unit, {'role': 'ui_semi', 'size': 24, 'color': INK2})]],
                 line=0.9)
        y = top + 0.15 + size / 72 * 1.05
        if dek:
            nd = self.nlines(dek, 14, 'ui', lw - 0.3)
            self._tx(s.shapes, MX, y, lw - 0.3, self.lh(14, 1.3, nd), dek, 14, INK2, 'ui', line=1.3)
            y += self.lh(14, 1.3, nd) + 0.3
        if counts:
            cv = _Cv(self, s, MX, y, lw, 1.1, 'Counters')
            n = len(counts)
            cw = lw / n
            mxv = max(abs(c[2]) for c in counts if len(c) > 2) if any(len(c) > 2 for c in counts) else None
            for i, c in enumerate(counts):
                x = MX + i * cw
                cv.seg(x, y, x + cw - 0.2, y, INK, 1.5, cap='flat')
                cv.label(x, y + 0.08, c[0], 9, 'ui_semi', INK2, va='t', caps=True, spacing=0.6)
                cv.label(x, y + 0.5, c[1], 22, 'ui_semi', INK)
                if len(c) > 2 and mxv:
                    cv.rect(x, y + 0.8, cw - 0.3, 0.07, 'EEECE6')
                    cv.bar(x, y + 0.8, x + (cw - 0.3) * c[2] / mxv, y + 0.87, BLUE if i == n - 1 else MUTE, 'r', r=0.035)
        if chart:
            px = MX + lw + 0.35
            pw = SW - MX - px
            ph = FOOT_Y - 0.3 - top
            self._panel(s, px, top, pw, ph)
            if chart_title:
                self._tx(s.shapes, px + 0.3, top + 0.22, pw - 0.6, 0.3, chart_title, 11, INK, 'ui_semi')
            self._chart(s, px + 0.3, top + 0.6, pw - 0.6, ph - 0.85, chart, bg=SURF)
        if note:
            self._note(s, MX, FOOT_Y - 0.25 - self._note_h(note, lw), lw, note)
        self._foot(s, source)
        return s

    def kpis(self, kicker, headline, tiles, note=None, source=''):
        return self.chart(kicker, headline, {'type': 'tiles', 'tiles': tiles}, note=note, source=source)

    def chart(self, kicker, headline, spec, note=None, source='', dek='', panel=None, chart_title=''):
        s = self._slide()
        top = self._head(s, kicker, headline, dek)
        if panel:
            pw = 3.55
            cw = SW - 2 * MX - pw - 0.4
            if chart_title:
                self._tx(s.shapes, MX, top, cw, 0.3, chart_title, 11, INK, 'ui_semi')
                top += 0.36
            self._chart(s, MX, top, cw, FOOT_Y - 0.3 - top, spec)
            px = SW - MX - pw
            self._panel(s, px, top - (0.36 if chart_title else 0), pw, FOOT_Y - 0.3 - top + (0.36 if chart_title else 0))
            self._side(s, px + 0.28, top - (0.36 if chart_title else 0) + 0.3, pw - 0.56, panel, note)
        else:
            nh = self._note_h(note, SW - 2 * MX - 1.5)
            bottom = FOOT_Y - 0.22 - (nh + 0.22 if note else 0)
            if chart_title:
                self._tx(s.shapes, MX, top, SW - 2 * MX, 0.3, chart_title, 11, INK, 'ui_semi')
                top += 0.36
            self._chart(s, MX, top, SW - 2 * MX, bottom - top, spec)
            if note:
                self._note(s, MX, FOOT_Y - 0.2 - nh, SW - 2 * MX - 1.5, note)
        self._foot(s, source)
        return s

    def _side(self, s, x, y, w, panel, note):
        if panel.get('label'):
            self._tx(s.shapes, x, y, w, 0.25, panel['label'], 9.5, INK2, 'ui_semi', caps=True, spacing=0.8)
            y += 0.32
        if panel.get('value'):
            size = 40
            while size > 26 and self.tw(panel['value'], size, 'ui_bold') + self.tw(' ' + panel.get('unit', ''), 14,
                                                                                     'ui_semi') > w:
                size -= 2
            self._tx(s.shapes, x, y, w, size / 72 * 1.1, [[(panel['value'], {'role': 'ui_bold', 'size': size}),
                                                           (' ' + panel.get('unit', ''), {'role': 'ui_semi', 'size': 14,
                                                                                          'color': INK2})]], line=0.9)
            y += size / 72 * 1.1 + 0.1
        if panel.get('sub'):
            n = self.nlines(panel['sub'], 10, 'ui', w)
            self._tx(s.shapes, x, y, w, self.lh(10, 1.1, n), panel['sub'], 10, INK3, 'ui', line=1.1)
            y += self.lh(10, 1.1, n) + 0.15
        c = _clean(s.shapes.add_connector(MSO_CONNECTOR.STRAIGHT, E(x), E(y + 0.05), E(x + w), E(y + 0.05)))
        _line(c, RULE, 0.75, cap='flat')
        y += 0.2
        if note:
            lead, follow = (note if isinstance(note, (list, tuple)) else (note, None))
            n1 = self.nlines(lead, 12.5, 'ui', w)
            self._tx(s.shapes, x, y, w, self.lh(12.5, 1.15, n1), lead, 12.5, INK, 'ui', line=1.15)
            y += self.lh(12.5, 1.15, n1) + 0.12
            if follow:
                n2 = self.nlines(follow, 11.5, 'ui', w)
                self._tx(s.shapes, x, y, w, self.lh(11.5, 1.15, n2), follow, 11.5, INK2, 'ui', line=1.15)
                y += self.lh(11.5, 1.15, n2) + 0.1
        for b in panel.get('bullets', []):
            n = self.nlines(b, 10.5, 'ui', w - 0.2)
            cv = _Cv(self, s, x, y, w, 0.1, 'Bullet', SURF)
            cv.rect(x, y + 0.07, 0.07, 0.07, INK)
            self._tx(s.shapes, x + 0.2, y, w - 0.2, self.lh(10.5, 1.1, n), b, 10.5, INK, 'ui', line=1.1)
            y += self.lh(10.5, 1.1, n) + 0.1

    def two_charts(self, kicker, headline, left, right, titles=('', ''), note=None, source='', dek=''):
        s = self._slide()
        top = self._head(s, kicker, headline, dek)
        gap = 0.6
        cw = (SW - 2 * MX - gap) / 2
        nh = self._note_h(note, SW - 2 * MX - 1.5)
        bottom = FOOT_Y - 0.22 - (nh + 0.22 if note else 0)
        for i, (sp, tt) in enumerate(zip((left, right), titles)):
            x = MX + i * (cw + gap)
            t2 = top
            if tt:
                self._tx(s.shapes, x, top, cw, 0.3, tt, 11.5, INK, 'ui_semi')
                t2 += 0.4
            self._chart(s, x, t2, cw, bottom - t2, sp)
        if note:
            self._note(s, MX, FOOT_Y - 0.2 - nh, SW - 2 * MX - 1.5, note)
        self._foot(s, source)
        return s

    def table(self, kicker, headline, header, rows, source='', col_widths=None, number_cols=(), note=None):
        s = self._slide()
        top = self._head(s, kicker, headline)
        n_r, n_c = len(rows) + 1, len(header)
        nh = self._note_h(note, SW - 2 * MX - 1.5)
        avail = FOOT_Y - 0.3 - top - (nh + 0.25 if note else 0)
        rh = min(0.36, avail / n_r)
        gf = s.shapes.add_table(n_r, n_c, E(MX), E(top), E(SW - 2 * MX), E(rh * n_r))
        tbl = gf.table
        tblPr = gf._element.graphic.graphicData.tbl.tblPr
        sid = tblPr.find(qn('a:tableStyleId'))
        if sid is None:
            sid = OxmlElement('a:tableStyleId')
            tblPr.append(sid)
        sid.text = '{2D5ABB26-0587-4C30-8999-92F81FD0307C}'          # "No Style, No Grid"
        tbl.first_row = True
        tbl.horz_banding = False
        if col_widths:
            for j, wv in enumerate(col_widths):
                tbl.columns[j].width = E(wv)
        for i in range(n_r):
            tbl.rows[i].height = E(rh)
            for j in range(n_c):
                c = tbl.cell(i, j)
                v = header[j] if i == 0 else rows[i - 1][j]
                if isinstance(v, (int, float)) and not isinstance(v, bool) and i:
                    v = self.num(v, 1) if isinstance(v, float) else str(v)
                txt = '—' if v is None else str(v)
                c.fill.solid()
                c.fill.fore_color.rgb = rgb(SURF if i == 0 else (PAPER if i % 2 else ZEBRA))
                c.margin_left = c.margin_right = E(0.1)
                c.margin_top = c.margin_bottom = E(0.02)
                c.vertical_anchor = MSO_ANCHOR.MIDDLE
                tf = c.text_frame
                tf.text = ''
                p = tf.paragraphs[0]
                p.alignment = PP_ALIGN.RIGHT if j in number_cols else PP_ALIGN.LEFT
                self._run(p, txt, size=10.5 if i else 9.5, color=(INK3 if v is None else INK) if i else INK2,
                          role='ui_semi' if i == 0 else ('ui_med' if j == 0 else 'ui'), caps=i == 0)
                tcPr = c._tc.get_or_add_tcPr()
                for tag in ('a:lnL', 'a:lnR', 'a:lnT', 'a:lnB'):
                    for el in tcPr.findall(qn(tag)):
                        tcPr.remove(el)
                for tag, show, col, wpt in (('a:lnL', False, None, 0), ('a:lnR', False, None, 0),
                                            ('a:lnT', i == 0, INK, 1.25), ('a:lnB', True, INK if i == 0 else HAIR,
                                                                           1 if i == 0 else 0.5)):
                    ln = OxmlElement(tag)
                    if show:
                        ln.set('w', str(int(wpt * 12700)))
                        sf = OxmlElement('a:solidFill')
                        clr = OxmlElement('a:srgbClr')
                        clr.set('val', col)
                        sf.append(clr)
                        ln.append(sf)
                    else:
                        ln.set('w', '0')
                        ln.append(OxmlElement('a:noFill'))
                    tcPr.insert(['a:lnL', 'a:lnR', 'a:lnT', 'a:lnB'].index(tag), ln)
        if note:
            self._note(s, MX, FOOT_Y - 0.2 - nh, SW - 2 * MX - 1.5, note)
        self._foot(s, source)
        return s

    def watch(self, kicker, headline, items, source='', note=None):
        s = self._slide()
        top = self._head(s, kicker, headline)
        n = len(items)
        gap = 0.4
        cw = (SW - 2 * MX - gap * (n - 1)) / n
        y0 = top + 0.25
        for i, it in enumerate(items):
            d, t, x = it[:3]
            cx = MX + i * (cw + gap)
            cv = _Cv(self, s, cx, y0, cw, 3.5, 'Watch item')
            cv.rect(cx, y0, cw, FOOT_Y - 0.4 - y0, SURF)
            cv.seg(cx, y0, cx + cw, y0, INK, 2.5, cap='flat')
            cv.label(cx + 0.25, y0 + 0.35, d, 11.5, 'ui_semi', RED, spacing=0.4)
            nt = self.nlines(t, 17, 'ui_semi', cw - 0.5)
            self._tx(s.shapes, cx + 0.25, y0 + 0.62, cw - 0.5, self.lh(17, 1.08, nt), t, 17, INK, 'ui_semi', line=1.08)
            self._tx(s.shapes, cx + 0.25, y0 + 0.62 + self.lh(17, 1.08, nt) + 0.2, cw - 0.5, 2.0, x, 12, INK2, 'ui',
                     line=1.3)
            if len(it) > 3 and it[3]:                    # countdown block at the foot of the card
                yb = FOOT_Y - 0.4 - 1.05
                cv.seg(cx + 0.25, yb, cx + cw - 0.25, yb, RULE, 0.75, cap='flat')
                big, small = (it[3] if isinstance(it[3], (list, tuple)) else (it[3], ''))
                cv.runs(cx + 0.25, yb + 0.48, [(big, {'role': 'ui_bold', 'size': 30}),
                                               ('  ' + small, {'role': 'ui', 'size': 11, 'color': INK2})])
        self._foot(s, source)
        return s

    def quote(self, kicker, quote, who, role='', note=None, source='', facts=()):
        s = self._slide()
        r = _clean(s.shapes.add_connector(MSO_CONNECTOR.STRAIGHT, E(MX), E(0.42), E(SW - MX), E(0.42)))
        _line(r, INK, 3.5, cap='flat')
        self._tx(s.shapes, MX, 0.58, 10, 0.25, kicker, 10.5, RED, 'ui_semi', caps=True, spacing=1.2)
        qw = 8.3 if facts else SW - 2 * MX - 1.0
        self._panel(s, MX, 1.05, qw + 0.9, FOOT_Y - 0.35 - 1.05)
        bar = _clean(s.shapes.add_shape(MSO_SHAPE.RECTANGLE, E(MX), E(1.05), E(0.06), E(FOOT_Y - 0.35 - 1.05)))
        _fill(bar, INK)
        _line(bar, None)
        self._tx(s.shapes, MX + 0.4, 1.0, 1.2, 1.2, '“', 96, RED, 'head', line=0.8)
        size = 23
        while size > 15 and self.lh(size, 1.22, self.nlines(quote, size, 'head_med', qw - 0.2)) > 3.3:
            size -= 1
        nl = self.nlines(quote, size, 'head_med', qw - 0.2)
        self._tx(s.shapes, MX + 0.5, 1.95, qw - 0.1, self.lh(size, 1.22, nl) + 0.1, quote, size, INK, 'head_med', line=1.22)
        y = 1.95 + self.lh(size, 1.22, nl) + 0.3
        self._tx(s.shapes, MX + 0.5, y, qw, 0.5, [[('— ' + who, {'role': 'ui_semi', 'size': 12, 'color': INK})],
                                                  [(role, {'role': 'ui', 'size': 10.5, 'color': INK2})]], line=1.2)
        if facts:
            x = MX + qw + 1.3
            w = SW - MX - x
            y = 1.25
            for t, v, sub in facts:
                c = _clean(s.shapes.add_connector(MSO_CONNECTOR.STRAIGHT, E(x), E(y), E(x), E(y + 1.0)))
                _line(c, INK, 2.5, cap='flat')
                self._tx(s.shapes, x + 0.2, y, w - 0.2, 0.25, t, 9.5, INK2, 'ui_semi', caps=True, spacing=0.6)
                self._tx(s.shapes, x + 0.2, y + 0.26, w - 0.2, 0.5, v, 24, INK, 'ui_bold', line=0.9)
                self._tx(s.shapes, x + 0.2, y + 0.72, w - 0.2, 0.3, sub, 9.5, INK3, 'ui')
                y += 1.35
        if note:
            self._note(s, MX + 0.5, FOOT_Y - 0.6 - self._note_h(note, qw), qw, note, size=11.5)
        self._foot(s, source)
        return s

    def sources(self, kicker, headline, items, method=(), source=''):
        """items: [(publisher, what, period, url)]; method: list of short lines."""
        s = self._slide()
        top = self._head(s, kicker, headline)
        mw = 4.1 if method else 0
        lw = SW - 2 * MX - (mw + 0.5 if method else 0)
        colw = (lw - 0.4) / 2
        half = math.ceil(len(items) / 2)
        for ci in range(2):
            y = top + 0.05
            x = MX + ci * (colw + 0.4)
            for k, it in enumerate(items[ci * half:(ci + 1) * half]):
                pub, what, period, url = (list(it) + ['', '', '', ''])[:4]
                c = _clean(s.shapes.add_connector(MSO_CONNECTOR.STRAIGHT, E(x), E(y), E(x + colw), E(y)))
                _line(c, RULE, 0.75, cap='flat')
                nw = self.nlines(what, 10, 'ui', colw - 0.05)
                paras = [[(f'{ci * half + k + 1:>2}  ', {'role': 'ui_semi', 'color': RED, 'size': 10}),
                          (pub, {'role': 'ui_semi', 'size': 11, 'color': INK})],
                         {'text': what, 'size': 10, 'color': INK2, 'role': 'ui', 'line': 1.05}]
                meta = ' · '.join(x for x in (period, url) if x)
                if meta:
                    paras.append({'text': meta, 'size': 8.5, 'color': INK3, 'role': 'ui', 'before': 1})
                h = self.lh(11) + self.lh(10, 1.05, nw) + (self.lh(8.5) + 0.02 if meta else 0) + 0.04
                self._tx(s.shapes, x, y + 0.07, colw, h, paras)
                y += h + 0.14
        if method:
            px = SW - MX - mw
            self._panel(s, px, top, mw, FOOT_Y - 0.3 - top)
            self._tx(s.shapes, px + 0.25, top + 0.22, mw - 0.5, 0.25, self.T['method'], 9.5, INK2, 'ui_semi', caps=True,
                     spacing=0.8)
            y = top + 0.58
            for m in method:
                n = self.nlines(m, 10.5, 'ui', mw - 0.7)
                b = _clean(s.shapes.add_shape(MSO_SHAPE.RECTANGLE, E(px + 0.25), E(y + 0.07), E(0.07), E(0.07)))
                _fill(b, INK)
                _line(b, None)
                self._tx(s.shapes, px + 0.45, y, mw - 0.7, n * 0.2, m, 10.5, INK, 'ui', line=1.1)
                y += n * 0.2 + 0.14
        self._foot(s, source)
        return s

    # ── save + font embedding ──
    def _embed_fonts(self):
        """Add the bundled TTFs as EOT .fntdata parts + <p:embeddedFontLst>, the way PowerPoint stores them."""
        fams = [f for f in EMBED if f in self._used]
        if not fams:
            return 0
        prs_part = self.prs.part
        pres = prs_part._element
        lst = pres.find(qn('p:embeddedFontLst'))
        if lst is not None:
            pres.remove(lst)
        lst = OxmlElement('p:embeddedFontLst')
        k = 0
        for fam in fams:
            ef = OxmlElement('p:embeddedFont')
            fe = OxmlElement('p:font')
            fe.set('typeface', fam)
            first = _ttf(EMBED[fam]['regular'])
            if first is None:
                continue
            oo = first.t['OS/2'][0]
            fe.set('panose', first.data[oo + 32:oo + 42].hex().upper())
            fe.set('pitchFamily', '18' if fam.startswith('Newsreader') else '34')
            fe.set('charset', '0')
            ef.append(fe)
            for slot in ('regular', 'bold'):
                key = EMBED[fam].get(slot)
                if not key or _ttf(key) is None:
                    continue
                k += 1
                part = Part(PackURI(f'/ppt/fonts/font{k}.fntdata'), 'application/x-fontdata', self.prs.part.package,
                            _ttf(key).eot())
                rid = prs_part.relate_to(part, RT.FONT)
                el = OxmlElement('p:' + slot)
                el.set(qn('r:id'), rid)
                ef.append(el)
            lst.append(ef)
        # schema order: ... sldSz, notesSz, smartTags?, embeddedFontLst, custShowLst?, ..., defaultTextStyle, ...
        anchor = pres.find(qn('p:notesSz'))
        anchor.addnext(lst)
        pres.set('embedTrueTypeFonts', '1')
        return k

    def save(self, path):
        self.prs.core_properties.author = self.brand
        self.prs.core_properties.language = self.lang_tag
        self.embedded = self._embed_fonts() if self.embed else 0
        self.prs.save(path)
        return path
