"""vn_deck — PowerPoint decks in the Vietnam Dashboard's editorial data-story style (python-pptx >= 0.6.21, xlsxwriter).

One finding per slide: tracked red kicker, Newsreader SemiBold headline that states the finding (balanced, <= 2
lines), the chart, a two-line note aligned to the chart's left edge (finding in ink, caveat in grey) and a hairline
footer with source + period and the slide number. 16:9, 12-column grid (0.6 in margins, 0.2 in gutters); kicker,
headline and chart top sit at the same positions on every slide. Colour: ink #16181D, kicker #C2362F, cream
#FAF8F3, the categorical palette in fixed order, grey #C9CCD2 for context, hairline grid #E1E0D9.

    import sys; sys.path.insert(0, '<skill>/scripts')
    from vn_deck import Deck
    d = Deck(lang='vi')                       # editable=True by default: native PowerPoint charts
    d.cover('BÁO CÁO KINH TẾ', 'Tăng trưởng 9 tháng 9,01%', 'Cập nhật 7/10/2026')
    d.chart('CHƯƠNG 1 · TĂNG TRƯỞNG', 'GDP tăng 8,02% năm 2025',
            {'type': 'bar', 'categories': ['2023', '2024', '2025'],
             'series': [{'name': 'GDP', 'values': [4.98, 7.04, 8.02]}], 'dec': 2, 'unit': '%', 'highlight': 2},
            note=('GDP tăng 8,02% năm 2025.', 'Số 2025 là ước tính.'), source='Nguồn: NSO. Số năm.', method='Cột')
    d.save('deck.pptx')                       # embeds the bundled fonts (embed_fonts=True by default)

Deck(lang='vi', safe_fonts=False, embed_fonts=True, editable=True, brand='Vietnam Dashboard', template=None)
  editable=True  (default) every chart is a native PowerPoint chart with an embedded workbook ("Edit Data" works);
                 forms with no chart type are native tables (heatmap, calendar, waffle) or editable grouped shapes
                 (tilemap, flow, levers, timeline, treemap) whose source numbers go into the slide notes.
  editable=False shape-drawn charts with rounded data ends (native charts cannot round bar ends) for the types
                 that have a drawn version: line fan area bar barh diverging stacked stacked_h stacked100_h waffle
                 waterfall tornado dumbbell slope bump heatmap scatter multiples spark_table pyramid bullet.
                 Other types stay native. A spec can override with 'editable': True/False.
  safe_fonts=True -> Georgia / Arial everywhere (no embedding).

NATIVE CHART ENGINE (_XChart, _Book): python-pptx only creates the chart and workbook parts; vn_deck writes its own
workbook (xlsxwriter; helper columns are Excel formulas, so editing the input column recalculates waterfall bases,
fan bands, funnel padding, box-plot quartiles, histogram counts, bump ranks, totals) and its own chart XML in schema
order: several chart groups per plot area, secondary axes, per-point formats, error bars, trend lines, label
positions and offsets, fonts in txPr (Plex 10 pt, #5B6170), hairline grid, no value-axis line, ink baseline, no
borders. Legends are small inline keys drawn above the plot (shapes), so helper series never show. VI decks tag
number formats with the Vietnamese locale ([$-42A]); PowerPoint still formats numbers in the viewer's system locale.

SLIDE METHODS (method= registers the slide in the catalogue; notes= adds speaker notes)
  cover(kicker, title, subtitle='', date='', chart=None, note='')
  catalog(kicker, headline, dek='')                       index slide, filled at save(): method -> slide -> build
  section(number, title, dek='', items=())                chapter divider (sets the catalogue chapter)
  hero(kicker, headline, big, unit='', dek='', counts=(), chart=None, chart_title='', source='', note=None, method)
  kpis(kicker, headline, tiles, note=None, source='', method)
  chart(kicker, headline, spec, note=None, source='', dek='', panel=None, chart_title='', method=None, notes=None)
  two_charts(kicker, headline, left, right, titles=('', ''), note=None, source='', method)
  table(kicker, headline, header, rows, source='', col_widths=None, number_cols=(), note=None, method)  native table
  watch(kicker, headline, items, source='', method)       3 dated "what to watch" items
  quote(kicker, quote, who, role='', note=None, source='', facts=(), method)
  sources(kicker, headline, items, method=(), source='', method_name=None)
  save(path)

CHART SPEC TYPES (spec['type'])          built as
  line         categories, series[{name, values, color, muted, dashed, dash_from, end_label, label, markers}],
               dec, unit, y_min/y_max, zero, forecast_from + forecast_label (zone = full-height column series),
               refs[{value, label}] (constant dashed series), annotations[{series, at, text, tier, dx, dy}]
                                                          line chart (+ column zone)
  fan          categories, actual, base, low, high, names   line + stacked area (invisible lower bound + band)
  step         series[{name, points[(x, y)], color, until}], x_min, x_max, x_step, x_label   XY scatter, doubled pts
  area / area100   categories, series                     area chart, standard / stacked / percentStacked
  bar          categories, series (1 = single, 2+ = grouped), highlight, basis, labels, y_title   clustered column
  barh         categories, values or series (2+ = grouped)            clustered bar, reversed categories
  stacked / stacked100 / stacked_h / stacked100_h   categories, series, totals, basis
                                                          stacked bars + invisible clustered twin carrying totals
  histogram    data (raw), edges, bin_labels, highlight   column, gap 4, COUNTIFS formulas
  diverging    categories, values, pos_label/neg_label    two bar series (+/−), overlap 100
  lollipop     categories, values, highlight              marker-only line + minus error bars as sticks
  dot          categories, series, refs                   marker-only line chart
  dumbbell     rows[{name, a, b}], labels                 marker-only lines + custom error bar connector
  slope        periods (2), series[{name, values, hi}]    2-category line chart, labels both ends
  bump         periods, series[{name, values}], highlight  line chart of RANK() formulas, reversed axis
  combo        categories, bar{name, values, dec}, line{name, values, dec, unit}   column + line on right axis,
               zero lines aligned; only for two different units
  waterfall    steps[{name, value, total, basis}]         stacked column: hidden base + up + down + total
  tornado      base, rows[{name, low, high}], labels      clustered bar of (level − base), overlap 100
  pyramid      bands, left, right, compare                clustered bar, left stored negative, outline twin
  funnel       stages[{name, value}]                      stacked bar centred by hidden padding
  bullet       rows[{name, value, target, max, ranges, unit, dec, value_text, color}]
                                                          one small chart per row: stacked bands + error-bar bar
                                                          + dash-marker target
  box          groups[{name, values}]                     stacked column + error-bar whiskers (QUARTILE.INC)
  scatter      points[{name, x, y, label, hi, text, group}], trend, x_title, y_title   XY scatter + trendline
  bubble       points[... size], groups, size_title       bubble chart
  donut / pie  parts[{name, value, color}], center        doughnut / pie
  gauge        value, max, target, caption                half doughnut (hidden lower half)
  radar        axes, series                               radar chart
  marimekko    columns[{name, width, values}], series     100% stacked column of zero-gap slices
  multiples    panels[{title, categories, values, kind}], cols   grid of small native charts
  spark_table  rows[{name, sub, values, dec, unit}], headers     native table + native sparklines
  tiles        tiles[{label, value, unit, delta, spark, spark_kind, spark_cats, sub}]   shapes + native sparks
  heatmap / calendar   rows, cols, values, scale, vmax/vmin, legend_title   native table with cell fills
  waffle       parts[{name, value}]                       native 10 × 10 table with cell fills
  treemap      items[{name, value, group}], groups        squarified shapes; numbers in the notes
  tilemap      tiles[{name, short, col, row, value, panel}], panels, breaks   shapes; numbers in the notes
  flow         columns[[{id, name, color}]], links[(src, dst, value)]          Sankey shapes; numbers in notes
  levers       columns[{title, items}], links, highlight  shapes + connectors; notes
  timeline     events[{date, title, text, future}], today  shapes; notes

Numbers: Deck.num(v, d) formats in the deck language (VI 2.650,1 / EN 2,650.1; minus is U+2212). Gaps are None,
never 0. Text is ink or grey, never a series colour. Check a finished deck with scripts/check_deck.py.
"""
import io
import re
import math
import os
import struct

from pptx import Presentation
from pptx.chart.data import CategoryChartData
from pptx.dml.color import RGBColor
from pptx.enum.chart import XL_CHART_TYPE
from pptx.enum.dml import MSO_LINE_DASH_STYLE
from pptx.enum.shapes import MSO_CONNECTOR, MSO_SHAPE
from pptx.enum.text import MSO_ANCHOR, MSO_AUTO_SIZE, PP_ALIGN
from pptx.opc.constants import RELATIONSHIP_TYPE as RT
from pptx.opc.package import Part
from pptx.opc.packuri import PackURI
from pptx.oxml.ns import qn
from pptx.oxml.xmlchemy import OxmlElement
from pptx.util import Pt
from pptx.oxml import parse_xml
from xml.sax.saxutils import escape as _xml_escape

# ── tokens (same values as the dashboard's story CSS) ──
INK, INK2, INK3, SUP = '16181D', '5B6170', '8A8F99', '6B7080'
RED, PAPER, SURF, MUTE = 'C2362F', 'FFFFFF', 'FAF8F3', 'C9CCD2'
RULE, HAIR, BASE, ZEBRA, PANEL, ZONE = 'DEDBD2', 'E1E0D9', 'C3C2B7', 'F6F5F1', 'F3F1EA', 'F1EFE8'
BLUE, ORANGE, AQUA, YELLOW, MAGENTA, GREEN, VIOLET, REDS = ('2A78D6', 'EB6834', '1BAF7A', 'EDA100', 'E87BA4', '008300',
                                                             '4A3AA7', 'E34948')
PALETTE = [BLUE, ORANGE, AQUA, YELLOW, MAGENTA, GREEN, VIOLET, REDS]   # fixed order; colour follows the entity
NEUTRAL = 'E6E4DE'                                                     # midpoint of blue <-> red polarity scales

SW, SH = 13.333, 7.5                 # slide, inches (16:9)
MX = 0.6                             # side margin
GUT = 0.2                            # gutter of the 12-column grid
COLW = (SW - 2 * MX - 11 * GUT) / 12  # one grid column (≈ 0.83 in)
RULE_Y, KICK_Y, HEAD_Y = 0.42, 0.56, 0.84   # hairline, kicker, headline: identical on every slide
CT = 1.92                            # chart top on every chart slide
FOOT_Y = 7.02                        # footer hairline
BAR_MAX = 0.34                       # max bar thickness (≈ 24px on the page)
BAR_R = 0.055                        # data-end radius (≈ 4px)
GAP = 0.022                          # surface gap between stacked segments (≈ 2px)
EMU = 914400


def gx(i):
    """Left edge of grid column i (0..11)."""
    return MX + i * (COLW + GUT)


def gw(n):
    """Width of n grid columns including the gutters between them."""
    return n * COLW + (n - 1) * GUT


HEAD_W = gw(10)                      # headline measure (balanced inside it)
DEK_W = gw(7)                        # dek measure (≈ 75 characters at 13.5 pt)
NOTE_W = gw(7)                       # note measure, aligned to the chart's left edge

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


# ── native chart engine ──────────────────────────────────────────────────────────────────────────────────────────
# Every native chart is written by this engine, not by python-pptx's chart writer: python-pptx only creates the chart
# part and its embedded-workbook part; the engine then writes (1) its own workbook (xlsxwriter; helper columns can be
# Excel formulas, so "Edit Data" recalculates waterfall bases, fan bands, funnel padding, box-plot quartiles, ...) and
# (2) its own chart XML in strict schema order (several chart groups per plot area, secondary axes, per-point
# formats, error bars, trend lines, hi-low lines, label positions, legend entries hidden for helper series). Each
# series points at its workbook column, so the chart is a real, data-editable PowerPoint chart.
C_NS = ('xmlns:c="http://schemas.openxmlformats.org/drawingml/2006/chart" '
        'xmlns:a="http://schemas.openxmlformats.org/drawingml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"')


def _xe(s):
    return _xml_escape(str(s), {'"': '&quot;'})


def _colname(j):
    s, j = '', j + 1
    while j:
        j, r = divmod(j - 1, 26)
        s = chr(65 + r) + s
    return s


class _Book:
    """The embedded workbook. Column j holds a header in row 1 and values from row 2; a cell may be a formula."""

    def __init__(self):
        self.cols = []

    def add(self, header, values, fmt=None, formulas=None):
        self.cols.append((header, list(values), fmt, formulas))
        return len(self.cols) - 1

    def ref(self, j, n=None):
        c = _colname(j)
        n = len(self.cols[j][1]) if n is None else n
        return f'Sheet1!${c}$2:${c}${n + 1}'

    def href(self, j):
        return f'Sheet1!${_colname(j)}$1'

    def cell(self, j, i):
        """A1-style address of data row i (0-based) in column j, for formulas."""
        return f'{_colname(j)}{i + 2}'

    def blob(self):
        import xlsxwriter
        bio = io.BytesIO()
        wb = xlsxwriter.Workbook(bio, {'in_memory': True, 'use_future_functions': True})
        ws = wb.add_worksheet('Sheet1')
        hf = wb.add_format({'bold': True})
        fmts = {}
        for j, (h, vals, fmt, forms) in enumerate(self.cols):
            if h is not None:
                ws.write_string(0, j, str(h), hf)
            nf = None
            if fmt:
                nf = fmts.setdefault(fmt, wb.add_format({'num_format': fmt}))
            for i, v in enumerate(vals):
                f = forms[i] if forms else None
                if f:
                    ws.write_formula(i + 1, j, f, nf, '' if v is None else v)
                elif v is None:
                    continue
                elif isinstance(v, str):
                    ws.write_string(i + 1, j, v)
                else:
                    ws.write_number(i + 1, j, float(v), nf)
            ws.set_column(j, j, 22 if j == 0 else 13)
        wb.close()
        return bio.getvalue()


def _x_fill(color, alpha=None):
    if color is None:
        return '<a:noFill/>'
    a = f'<a:alpha val="{int(alpha * 100000)}"/>' if alpha is not None and alpha < 1 else ''
    return f'<a:solidFill><a:srgbClr val="{color}">{a}</a:srgbClr></a:solidFill>'


def _x_ln(color, w=0.75, dash=None, cap='rnd'):
    if color is None:
        return '<a:ln><a:noFill/></a:ln>'
    d = f'<a:prstDash val="{dash}"/>' if dash else ''
    return f'<a:ln w="{int(round(w * 12700))}" cap="{cap}">{_x_fill(color)}{d}<a:round/></a:ln>'


def _x_sppr(fill=False, line=False, lw=0.75, dash=None, alpha=None, cap='rnd'):
    """fill/line: hex colour, None (= no fill / no line) or False (= leave out, inherit)."""
    out = ''
    if fill is not False:
        out += _x_fill(fill, alpha)
    if line is not False:
        out += _x_ln(line, lw, dash, cap)
    return f'<c:spPr>{out}</c:spPr>'


def _x_rpr(tag, size, color, fam, bold=False, lang='vi-VN'):
    return (f'<a:{tag} lang="{lang}" sz="{int(round(size * 100))}" b="{int(bool(bold))}" i="0" baseline="0">'
            f'{_x_fill(color)}<a:latin typeface="{_xe(fam)}"/><a:ea typeface="{_xe(fam)}"/>'
            f'<a:cs typeface="{_xe(fam)}"/></a:{tag}>')


def _x_txpr(size, color, fam, rot=None, lang='vi-VN', bold=False, wrap=True):
    r = f' rot="{int(rot * 60000)}" vert="horz"' if rot is not None else ''
    r += '' if wrap else ' wrap="none"'
    d = _x_rpr('defRPr', size, color, fam, bold, lang).replace(f' lang="{lang}"', '')
    return (f'<c:txPr><a:bodyPr{r} spcFirstLastPara="1" vertOverflow="ellipsis" wrap="square" anchor="ctr" '
            f'anchorCtr="1"/><a:lstStyle/><a:p><a:pPr>{d}</a:pPr><a:endParaRPr lang="{lang}"/></a:p></c:txPr>').replace(
        ' wrap="none" spcFirstLastPara="1" vertOverflow="ellipsis" wrap="square"', ' spcFirstLastPara="1" vertOverflow="overflow" wrap="none"')


def _x_strcache(vals):
    pts = ''.join(f'<c:pt idx="{i}"><c:v>{_xe(v)}</c:v></c:pt>' for i, v in enumerate(vals) if v is not None)
    return f'<c:strCache><c:ptCount val="{len(vals)}"/>{pts}</c:strCache>'


def _x_numcache(vals, fmt='General'):
    pts = ''.join(f'<c:pt idx="{i}"><c:v>{repr(float(v))}</c:v></c:pt>' for i, v in enumerate(vals) if v is not None)
    return f'<c:numCache><c:formatCode>{_xe(fmt)}</c:formatCode><c:ptCount val="{len(vals)}"/>{pts}</c:numCache>'


def _x_ref(tag, ref, vals, fmt='General'):
    """<c:cat>/<c:val>/<c:xVal>/<c:yVal>/<c:bubbleSize>/<c:plus>/<c:minus> pointing at a workbook range."""
    if vals and all(v is None or isinstance(v, (int, float)) for v in vals) and tag not in ('cat_str',):
        body = f'<c:numRef><c:f>{ref}</c:f>{_x_numcache(vals, fmt)}</c:numRef>'
    else:
        body = f'<c:strRef><c:f>{ref}</c:f>{_x_strcache(["" if v is None else str(v) for v in vals])}</c:strRef>'
    tag = 'cat' if tag == 'cat_str' else tag
    return f'<c:{tag}>{body}</c:{tag}>'


DLBL_FLAGS = ('showLegendKey', 'showVal', 'showCatName', 'showSerName', 'showPercent', 'showBubbleSize')


def _x_flags(show):
    m = {'key': 'showLegendKey', 'val': 'showVal', 'cat': 'showCatName', 'ser': 'showSerName', 'pct': 'showPercent',
         'size': 'showBubbleSize'}
    on = {m[k] for k in show}
    return ''.join(f'<c:{k} val="{int(k in on)}"/>' for k in DLBL_FLAGS)


class _XChart:
    """Collects series/groups/axes and writes a complete <c:chartSpace> in schema order."""

    def __init__(self, deck, book):
        self.d, self.book = deck, book
        self.groups, self.axes = [], []
        self.legend = None
        self.plot = None
        self._idx = 0
        self.fam = deck.font('ui')[0]
        self.fam_semi = deck.font('ui_semi')[0]
        self.fam_med = deck.font('ui_med')[0]
        deck._used.update({self.fam, self.fam_semi, self.fam_med})
        self.lang = deck.lang_tag

    # text helpers
    def tx(self, size=10, color=INK2, semi=False, med=False, rot=None, bold=False, wrap=True):
        fam = self.fam_semi if semi else (self.fam_med if med else self.fam)
        return _x_txpr(size, color, fam, rot, self.lang, bold, wrap)

    def rich(self, text, size=10, color=INK, semi=True):
        fam = self.fam_semi if semi else self.fam
        paras = ''.join(f'<a:p><a:pPr>{_x_rpr("defRPr", size, color, fam, False, self.lang)}</a:pPr><a:r>'
                        f'{_x_rpr("rPr", size, color, fam, False, self.lang)}<a:t>{_xe(t)}</a:t></a:r></a:p>'
                        for t in str(text).split('\n'))
        return f'<c:tx><c:rich><a:bodyPr wrap="none" anchor="ctr"/><a:lstStyle/>{paras}</c:rich></c:tx>'

    # data labels
    def dlbls(self, d, kind, n):
        """d = {show: ('val',), pos, fmt, size, color, semi, only: [idx], text: {idx: str}, sep, hide: [idx]}."""
        if not d:
            return ''
        show = d.get('show', ('val',))
        pos = d.get('pos')
        nf = f'<c:numFmt formatCode="{_xe(d["fmt"])}" sourceLinked="0"/>' if d.get('fmt') else ''
        txp = self.tx(d.get('size', 10), d.get('color', INK2), semi=d.get('semi', True), wrap=False)
        sp = '<c:spPr><a:noFill/><a:ln><a:noFill/></a:ln></c:spPr>'
        posx = f'<c:dLblPos val="{pos}"/>' if pos else ''
        sep = f'<c:separator>{_xe(d.get("sep", " "))}</c:separator>' if 'ser' in show or 'cat' in show else ''
        texts = d.get('text', {})
        only = d.get('only')
        out = ''
        offs = d.get('off', {})
        idxs = sorted(set(only or []) | set(texts) | ({k for k, v in offs.items() if v != (0, 0)} if only is None
                                                       and not texts else set()))
        for i in idxs:
            if i is None or i < 0 or i >= n:
                continue
            ppos = d.get('pos_at', {}).get(i, pos)
            ox, oy = offs.get(i, (0, 0))
            lay = (f'<c:layout><c:manualLayout><c:x val="{ox:.5f}"/><c:y val="{oy:.5f}"/></c:manualLayout></c:layout>'
                   if abs(ox) > 1e-4 or abs(oy) > 1e-4 else '')
            pp = f'<c:dLblPos val="{ppos}"/>' if ppos else ''
            col = d.get('color_at', {}).get(i)
            t = (self.rich(texts[i], d.get('size', 10), col or d.get('color', INK2), d.get('semi', True))
                 if i in texts else '')
            ptx = self.tx(d.get('size', 10), col or d.get('color', INK2), semi=d.get('semi', True), wrap=False)
            out += f'<c:dLbl><c:idx val="{i}"/>{lay}{t}{nf}{sp}{ptx}{pp}{_x_flags(show)}{sep}</c:dLbl>'
        for i in d.get('hide', []):
            if i not in idxs:
                out += f'<c:dLbl><c:idx val="{i}"/><c:delete val="1"/></c:dLbl>'
        if only is not None or (texts and d.get('only_text', True)):
            return f'<c:dLbls>{out}{nf}{sp}{txp}{_x_flags(())}</c:dLbls>'
        return f'<c:dLbls>{out}{nf}{sp}{txp}{posx}{_x_flags(show)}{sep}</c:dLbls>'

    # series
    def ser(self, name, col=None, vals=None, cat=None, x=None, size=None, fill=False, line=False, lw=2.0, dash=None,
            alpha=None, marker=None, dpts=None, labels=None, err=None, trend=None, smooth=False, legend=True,
            explosion=None, fmt='General', cap='rnd'):
        """col: workbook column of the values (or y for scatter); cat: (col, values); x / size: (col, values).
        marker = None | {'symbol', 'size', 'fill', 'line', 'lw'}; dpts = {idx: {fill, line, lw, dash, marker}}."""
        i = self._idx
        self._idx += 1
        return dict(i=i, name=name, col=col, vals=vals, cat=cat, x=x, size=size, fill=fill, line=line, lw=lw, dash=dash,
                    alpha=alpha, marker=marker, dpts=dpts or {}, labels=labels, err=err or [], trend=trend,
                    smooth=smooth, legend=legend, explosion=explosion, fmt=fmt, cap=cap)

    def _marker(self, m):
        if not m:
            return '<c:marker><c:symbol val="none"/></c:marker>'
        sp = _x_sppr(m.get('fill', INK), m.get('line', PAPER), m.get('lw', 0.75))
        return f'<c:marker><c:symbol val="{m.get("symbol", "circle")}"/><c:size val="{int(m.get("size", 5))}"/>{sp}</c:marker>'

    def _ser_xml(self, kind, s):
        b = self.book
        s = dict(s, i=s.get('_n', s['i']))
        n = len(s['vals'] if s['vals'] is not None else (s['x'][1] if s['x'] else []))
        out = f'<c:idx val="{s["i"]}"/><c:order val="{s["i"]}"/>'
        if s['col'] is not None:
            out += (f'<c:tx><c:strRef><c:f>{b.href(s["col"])}</c:f>{_x_strcache([s["name"]])}</c:strRef></c:tx>')
        else:
            out += f'<c:tx><c:v>{_xe(s["name"])}</c:v></c:tx>'
        if kind in ('line', 'scatter', 'radar'):
            out += _x_sppr(False, s['line'], s['lw'], s['dash'], cap=s['cap'])
        elif kind in ('pie',):
            out += _x_sppr(s['fill'], s['line'], s['lw'], s['dash'], s['alpha'])
        else:
            out += _x_sppr(s['fill'], s['line'], s['lw'], s['dash'], s['alpha'], cap='flat')
        if kind in ('bar', 'bubble'):
            out += '<c:invertIfNegative val="0"/>'
        if kind in ('line', 'scatter', 'radar'):
            out += self._marker(s['marker'])
        if kind == 'pie' and s['explosion'] is not None:
            out += f'<c:explosion val="{s["explosion"]}"/>'
        for k in sorted(s['dpts']):
            p = s['dpts'][k]
            dp = f'<c:dPt><c:idx val="{k}"/>'
            if kind in ('bar', 'bubble'):
                dp += '<c:invertIfNegative val="0"/>'
            if kind in ('line', 'scatter', 'radar') and 'marker' in p:
                dp += self._marker(p['marker'])
            if kind == 'bubble':
                dp += '<c:bubble3D val="0"/>'
            if kind in ('line', 'scatter', 'radar'):
                if 'line' in p or 'dash' in p:
                    dp += _x_sppr(False, p.get('line', s['line']), p.get('lw', s['lw']), p.get('dash', s['dash']))
            else:
                dp += _x_sppr(p.get('fill', s['fill']), p.get('line', s['line'] if s['line'] is not False else None),
                              p.get('lw', s['lw'] if s['line'] else 0.75), p.get('dash'), p.get('alpha', s['alpha']))
            out += dp + '</c:dPt>'
        if s['labels']:
            out += self.dlbls(s['labels'], kind, n)
        if s['trend'] and kind in ('scatter', 'line', 'bar', 'bubble', 'area'):
            t = s['trend']
            out += (f'<c:trendline>{_x_sppr(False, t.get("line", INK2), t.get("lw", 1.0), t.get("dash", "dash"))}'
                    f'<c:trendlineType val="linear"/><c:dispRSqr val="0"/><c:dispEq val="0"/></c:trendline>')
        for e in s['err'][: (2 if kind in ('scatter', 'bubble', 'area') else 1)]:
            out += self._err(kind, e)
        if kind in ('scatter', 'bubble'):
            out += _x_ref('xVal', b.ref(s['x'][0]), s['x'][1])
            out += _x_ref('yVal', b.ref(s['col']), s['vals'], s['fmt'])
            if kind == 'bubble':
                out += _x_ref('bubbleSize', b.ref(s['size'][0]), s['size'][1])
                out += '<c:bubble3D val="0"/>'
        else:
            if s['cat'] is not None:
                cvals = s['cat'][1]
                numeric = all(isinstance(v, (int, float)) for v in cvals)
                out += _x_ref('cat' if numeric else 'cat_str', b.ref(s['cat'][0], len(cvals)), cvals)
            out += _x_ref('val', b.ref(s['col'], len(s['vals'])), s['vals'], s['fmt'])
        if kind in ('line', 'scatter'):
            out += f'<c:smooth val="{int(bool(s["smooth"]))}"/>'
        return f'<c:ser>{out}</c:ser>'

    def _err(self, kind, e):
        """e = {dir: 'x'|'y', type: 'plus'|'minus'|'both', plus: (col, vals), minus: (col, vals), fixed: v, line, lw}."""
        out = '<c:errBars>'
        if kind in ('scatter', 'bubble'):
            out += f'<c:errDir val="{e.get("dir", "y")}"/>'
        out += f'<c:errBarType val="{e.get("type", "both")}"/>'
        if e.get('fixed') is not None:
            out += f'<c:errValType val="fixedVal"/><c:noEndCap val="{int(e.get("nocap", True))}"/>'
            out += f'<c:val val="{e["fixed"]}"/>'
        else:
            out += f'<c:errValType val="cust"/><c:noEndCap val="{int(e.get("nocap", True))}"/>'
            for side in ('plus', 'minus'):
                if e.get(side):
                    c, v = e[side]
                    out += _x_ref(side, self.book.ref(c, len(v)), v)
        out += _x_sppr(False, e.get('line', INK), e.get('lw', 1.0), e.get('dash'), cap='flat')
        return out + '</c:errBars>'

    # groups
    def group(self, kind, series, axes=(1, 2), **o):
        self.groups.append(dict(kind=kind, series=series, axes=axes, o=o))

    def _group_xml(self, g):
        k, S, o, ax = g['kind'], g['series'], g['o'], g['axes']
        sers = ''.join(self._ser_xml(k, s) for s in S)
        axx = ''.join(f'<c:axId val="{a}"/>' for a in ax)
        if k == 'bar':
            ov = o.get('overlap', 100 if o.get('grouping', 'clustered') != 'clustered' else 0)
            return (f'<c:barChart><c:barDir val="{o.get("dir", "col")}"/><c:grouping val="{o.get("grouping", "clustered")}"/>'
                    f'<c:varyColors val="0"/>{sers}<c:gapWidth val="{int(max(0, min(500, o.get("gap", 80))))}"/>'
                    f'<c:overlap val="{int(ov)}"/>{axx}</c:barChart>')
        if k == 'line':
            hl = ''
            if o.get('hilo'):
                h = o['hilo']
                hl = f'<c:hiLowLines>{_x_sppr(False, h.get("line", MUTE), h.get("lw", 2.5), cap="rnd")}</c:hiLowLines>'
            return (f'<c:lineChart><c:grouping val="{o.get("grouping", "standard")}"/><c:varyColors val="0"/>{sers}'
                    f'{hl}<c:marker val="1"/>{axx}</c:lineChart>')
        if k == 'area':
            return (f'<c:areaChart><c:grouping val="{o.get("grouping", "standard")}"/><c:varyColors val="0"/>{sers}'
                    f'{axx}</c:areaChart>')
        if k == 'scatter':
            return (f'<c:scatterChart><c:scatterStyle val="lineMarker"/><c:varyColors val="0"/>{sers}{axx}'
                    f'</c:scatterChart>')
        if k == 'bubble':
            return (f'<c:bubbleChart><c:varyColors val="0"/>{sers}<c:bubbleScale val="{int(o.get("scale", 100))}"/>'
                    f'<c:showNegBubbles val="0"/><c:sizeRepresents val="area"/>{axx}</c:bubbleChart>')
        if k == 'doughnut':
            return (f'<c:doughnutChart><c:varyColors val="1"/>{sers}<c:firstSliceAng val="{int(o.get("first", 0))}"/>'
                    f'<c:holeSize val="{int(o.get("hole", 62))}"/></c:doughnutChart>')
        if k == 'pie':
            return (f'<c:pieChart><c:varyColors val="1"/>{sers}<c:firstSliceAng val="{int(o.get("first", 0))}"/>'
                    f'</c:pieChart>')
        if k == 'radar':
            return (f'<c:radarChart><c:radarStyle val="marker"/><c:varyColors val="0"/>{sers}{axx}</c:radarChart>')
        raise ValueError(k)

    # axes
    def cat_ax(self, aid, cross, pos='b', line=INK, lw=0.75, delete=False, reverse=False, lbl='nextTo', size=10,
               color=INK2, crosses='autoZero', skip=None, grid=False, date=False, rot=None, numfmt=None):
        self.axes.append(('date' if date else 'cat', dict(aid=aid, cross=cross, pos=pos, line=line, lw=lw, delete=delete,
                                                          reverse=reverse, lbl=lbl, size=size, color=color,
                                                          crosses=crosses, skip=skip, grid=grid, rot=rot,
                                                          numfmt=numfmt)))

    def val_ax(self, aid, cross, lo=None, hi=None, step=None, fmt='General', pos='l', grid=True, delete=False,
               reverse=False, crosses='autoZero', between='between', lbl='nextTo', size=10, color=INK2, line=None,
               lw=0.75):
        self.axes.append(('val', dict(aid=aid, cross=cross, lo=lo, hi=hi, step=step, fmt=fmt, pos=pos, grid=grid,
                                      delete=delete, reverse=reverse, crosses=crosses, between=between, lbl=lbl,
                                      size=size, color=color, line=line, lw=lw)))

    def _ax_xml(self, kind, a):
        sc = f'<c:orientation val="{"maxMin" if a["reverse"] else "minMax"}"/>'
        if kind == 'val':
            if a['hi'] is not None:
                sc += f'<c:max val="{a["hi"]}"/>'
            if a['lo'] is not None:
                sc += f'<c:min val="{a["lo"]}"/>'
        head = (f'<c:axId val="{a["aid"]}"/><c:scaling>{sc}</c:scaling><c:delete val="{int(a["delete"])}"/>'
                f'<c:axPos val="{a["pos"]}"/>')
        if a.get('grid'):
            head += f'<c:majorGridlines>{_x_sppr(False, HAIR, 0.5, cap="flat")}</c:majorGridlines>'
        cr = a['crosses']
        crx = (f'<c:crossesAt val="{cr}"/>' if isinstance(cr, (int, float)) and not isinstance(cr, bool)
               else f'<c:crosses val="{cr}"/>')
        txp = self.tx(a['size'], a['color'], rot=a.get('rot'))
        sp = _x_sppr(None, a['line'], a['lw'], cap='flat')
        if kind == 'val':
            nf = f'<c:numFmt formatCode="{_xe(a["fmt"])}" sourceLinked="0"/>'
            mu = f'<c:majorUnit val="{a["step"]}"/>' if a['step'] else ''
            return (f'<c:valAx>{head}{nf}<c:majorTickMark val="none"/><c:minorTickMark val="none"/>'
                    f'<c:tickLblPos val="{a["lbl"]}"/>{sp}{txp}<c:crossAx val="{a["cross"]}"/>{crx}'
                    f'<c:crossBetween val="{a["between"]}"/>{mu}</c:valAx>')
        nf = f'<c:numFmt formatCode="{_xe(a["numfmt"])}" sourceLinked="0"/>' if a.get('numfmt') else ''
        if kind == 'date':
            return (f'<c:dateAx>{head}{nf}<c:majorTickMark val="none"/><c:minorTickMark val="none"/>'
                    f'<c:tickLblPos val="{a["lbl"]}"/>{sp}{txp}<c:crossAx val="{a["cross"]}"/>{crx}<c:auto val="0"/>'
                    f'<c:lblOffset val="100"/><c:baseTimeUnit val="days"/></c:dateAx>')
        skip = f'<c:tickLblSkip val="{a["skip"]}"/>' if a.get('skip') else ''
        return (f'<c:catAx>{head}{nf}<c:majorTickMark val="none"/><c:minorTickMark val="none"/>'
                f'<c:tickLblPos val="{a["lbl"]}"/>{sp}{txp}<c:crossAx val="{a["cross"]}"/>{crx}<c:auto val="1"/>'
                f'<c:lblAlgn val="ctr"/><c:lblOffset val="60"/>{skip}<c:noMultiLvlLbl val="0"/></c:catAx>')

    def set_legend(self, x, y, w, h, hide=(), size=10, vary_hide=()):
        self.legend = dict(x=x, y=y, w=w, h=h, hide=list(hide), size=size)

    def xml(self, rid, frame, plot=None, target='inner'):
        """frame = (x, y, w, h) of the graphic frame; plot = (x, y, w, h) of the inner plot area, both in inches."""
        fx, fy, fw, fh = frame
        lay = '<c:layout/>'
        if plot:
            px, py, pw, ph = plot
            lay = (f'<c:layout><c:manualLayout><c:layoutTarget val="{target}"/><c:xMode val="edge"/><c:yMode val="edge"/>'
                   f'<c:x val="{(px - fx) / fw:.5f}"/><c:y val="{(py - fy) / fh:.5f}"/>'
                   f'<c:w val="{pw / fw:.5f}"/><c:h val="{ph / fh:.5f}"/></c:manualLayout></c:layout>')
        k = 0
        for g in self.groups:                      # idx/order = appearance order, so every app agrees on indices
            for q in g['series']:
                q['_n'] = k
                k += 1
        groups = ''.join(self._group_xml(g) for g in self.groups)
        axes = ''.join(self._ax_xml(k, a) for k, a in self.axes)
        leg = ''
        if self.legend:
            L = self.legend
            ents = ''.join(f'<c:legendEntry><c:idx val="{i}"/><c:delete val="1"/></c:legendEntry>' for i in L['hide'])
            leg = (f'<c:legend><c:legendPos val="t"/>{ents}<c:layout><c:manualLayout><c:xMode val="edge"/>'
                   f'<c:yMode val="edge"/><c:x val="{max(0, (L["x"] - fx) / fw):.5f}"/>'
                   f'<c:y val="{max(0, (L["y"] - fy) / fh):.5f}"/><c:w val="{min(1, L["w"] / fw):.5f}"/>'
                   f'<c:h val="{min(1, L["h"] / fh):.5f}"/></c:manualLayout></c:layout><c:overlay val="0"/>'
                   f'<c:spPr><a:noFill/><a:ln><a:noFill/></a:ln></c:spPr>'
                   f'{self.tx(L["size"], INK, med=True)}</c:legend>')
        out = (f'<c:chartSpace {C_NS}><c:date1904 val="0"/><c:lang val="{self.lang}"/><c:roundedCorners val="0"/>'
                f'<c:chart><c:autoTitleDeleted val="1"/><c:plotArea>{lay}{groups}{axes}'
                f'<c:spPr><a:noFill/><a:ln><a:noFill/></a:ln></c:spPr></c:plotArea>{leg}'
                f'<c:plotVisOnly val="1"/><c:dispBlanksAs val="gap"/></c:chart>'
                f'<c:spPr><a:noFill/><a:ln><a:noFill/></a:ln></c:spPr>{self.tx(10, INK2)}'
                f'<c:externalData r:id="{rid}"><c:autoUpdate val="0"/></c:externalData></c:chartSpace>')
        if self.lang.startswith('vi'):
            # locale tag: apps that honour it (LibreOffice) show 2.650,1 / 19,07%; PowerPoint uses the system locale
            out = re.sub(r'formatCode="(?!General)([^"\[]+)"', r'formatCode="[$-42A]\1"', out)
            out = re.sub(r'<c:formatCode>(?!General)([^<\[]+)</c:formatCode>', r'<c:formatCode>[$-42A]\1</c:formatCode>', out)
        return out

    def place(self, shapes, frame, plot=None, name='Chart', target='inner'):
        """Create the graphic frame + chart part (python-pptx), then swap in our workbook and our chart XML."""
        x, y, w, h = frame
        cd = CategoryChartData()
        cd.categories = ['a']
        cd.add_series('s', [1])
        gf = shapes.add_chart(XL_CHART_TYPE.COLUMN_CLUSTERED, E(x), E(y), E(w), E(h), cd)
        gf.name = name
        part = gf.chart_part
        part.chart_workbook.update_from_xlsx_blob(self.book.blob())
        rid = part._element.find(qn('c:externalData')).get(qn('r:id'))
        part._element = parse_xml(self.xml(rid, frame, plot, target).encode('utf-8'))
        return gf


def _nf(dec=1, unit='', sign=False):
    """Excel number format: thousands separator, `dec` decimals, literal unit, U+2212 minus. Excel/PowerPoint render
    the separators in the viewer's locale (VI shows 2.650,1, EN shows 2,650.1)."""
    b = '#,##0' + ('.' + '0' * dec if dec else '')
    u = f'"{unit}"' if unit else ''
    if sign:
        return f'+{b}{u};"−"{b}{u};{b}{u}'
    return f'{b}{u};"−"{b}{u}'


def squarify(values, x, y, w, h):
    """Squarified treemap (Bruls et al.): values sorted descending -> list of (x, y, w, h) in the same order."""
    vals = [max(v, 0) for v in values]
    tot = sum(vals) or 1
    area = [v / tot * w * h for v in vals]
    rects, i = [], 0

    def worst(row, side):
        s = sum(row)
        return max(max(side * side * r / (s * s), (s * s) / (side * side * r)) for r in row) if s else 1e9
    while i < len(area):
        side = min(w, h)
        row = [area[i]]
        j = i + 1
        while j < len(area) and worst(row + [area[j]], side) <= worst(row, side):
            row.append(area[j])
            j += 1
        s = sum(row)
        if w >= h:                                   # lay the row as a column on the left
            cw = s / h if h else 0
            yy = y
            for r in row:
                rh = r / cw if cw else 0
                rects.append((x, yy, cw, rh))
                yy += rh
            x, w = x + cw, w - cw
        else:                                        # lay the row along the top
            rh = s / w if w else 0
            xx = x
            for r in row:
                rw = r / rh if rh else 0
                rects.append((xx, y, rw, rh))
                xx += rw
            y, h = y + rh, h - rh
        i = j
    return rects


def _quart(vals, q):
    """Inclusive quartile (Excel QUARTILE.INC)."""
    v = sorted(vals)
    pos = (len(v) - 1) * q
    lo = int(math.floor(pos))
    hi = min(lo + 1, len(v) - 1)
    return v[lo] + (v[hi] - v[lo]) * (pos - lo)


class _Native:
    """Native, data-editable chart builders. Each `_n_<type>(s, x, y, w, h, sp)` writes one real PowerPoint chart (or
    a native table, or a grid of native charts) into the box (x, y, w, h) on slide s."""

    # ── geometry helpers ──
    def _yscale(self, vals, sp, zero=False, n=4):
        vv = [v for v in vals if v is not None]
        lo, hi = (min(vv), max(vv)) if vv else (0, 1)
        if sp.get('y_min') is not None:
            lo = sp['y_min']
        if sp.get('y_max') is not None:
            hi = sp['y_max']
        a, b, ticks, st = nice(lo, hi, sp.get('ticks', n), zero)
        if sp.get('y_min') is not None:
            a = sp['y_min']
        if sp.get('y_max') is not None:
            b = sp['y_max']
        dt = sp.get('tick_dec', step_dec(st))
        k = math.floor(a / st + 1e-9)
        tl = [t for t in (st * i for i in range(k, k + 200)) if a - 1e-9 <= t <= b + 1e-9]
        lw = max(self.tw(self.num(t, dt) + sp.get('tick_unit', ''), 10, 'ui') for t in tl) + 0.14
        return a, b, st, dt, lw

    def _legend_w(self, items):
        """items: [(name, kind)] -> (width, rows) of a native legend laid out left to right."""
        return sum(self.tw(nm, 10, 'ui_med') + (0.46 if k == 'line' else 0.24) + 0.26 for nm, k in items) + 0.1

    def _put_legend(self, X, x, y, w, items, hide=()):
        """Legend above the plot as small inline keys (shapes), left-aligned like the page's .st-legend. items =
        [(name, colour, kind)]; kind: rect | line | dash | band | dot | hollow | outline | dashkey. Helper series never
        appear because only the listed items are drawn."""
        cv = _Cv(self, self._cur, x, y, w, 0.3, 'Legend')
        return self._legend(cv, items, x, y, w, size=10) + 0.04

    def _skip(self, cats, pw, size=10):
        n = len(cats)
        if not n:
            return None
        widest = max(self.tw(str(c), size, 'ui') for c in cats) + 0.12
        step = 1
        while step < n and widest > pw / n * step:
            step += 1
        return step if step > 1 else None

    def _xs(self, n, px, pw, mid=False):
        """Category centres: mid=True for 'midCat' (points on the edges), else 'between' (slot centres)."""
        if mid:
            return [px + (k * pw / (n - 1) if n > 1 else pw / 2) for k in range(n)]
        return [px + (k + 0.5) * pw / n for k in range(n)]

    def _gap(self, slot, m=1, max_t=BAR_MAX, frac=0.62):
        t = min(max_t, slot * frac / m)
        return max(0, min(500, (slot - m * t) / t * 100)), t

    def _ann_n(self, s, pts, frame):
        """Annotation overlay for native charts: pts = [(px, py, text, tier, dx, dy)] in slide inches."""
        if not pts:
            return
        cv = _Cv(self, s, *frame, name='Chart annotations')
        for px, py, text, tier, dx, dy in pts:
            self._ann(cv, px, py, text, tier, dx, dy)

    def _end_offsets(self, items, gap, lo, hi, fh):
        """items: [(key, y_in)] -> {key: dy as a fraction of the frame height} so end labels never collide."""
        L = spread([[yy, k, yy] for k, yy in items], gap, lo, hi)
        return {k: (yy - y0) / fh for yy, k, y0 in L}

    # ── line, multi-line highlight, fan ──
    def _n_line(self, s, x, y, w, h, sp):
        cats, S = [str(c) for c in sp['categories']], sp['series']
        n, dec, unit = len(cats), sp.get('dec', 1), sp.get('unit', '')
        cols = [MUTE if q.get('muted') else q.get('color', PALETTE[i % len(PALETTE)]) for i, q in enumerate(S)]
        band, ff, refs = sp.get('band'), sp.get('forecast_from'), sp.get('refs', [])
        allv = [v for q in S for v in q['values'] if v is not None] + [r['value'] for r in refs]
        if band:
            allv += [v for v in band['low'] + band['high'] if v is not None]
        lo, hi, st, dt, lw = self._yscale(allv, sp, zero=sp.get('zero', False))
        b = _Book()
        cc = b.add(None, cats)
        X = _XChart(self, b)
        cat = (cc, cats)
        leg = [(q['name'], c, 'dash' if q.get('dashed') else 'line') for q, c in zip(S, cols) if q.get('name')] + \
            ([(band.get('name', self.T['band80']), band.get('color', BLUE), 'band')] if band else [])
        top = y
        if len(leg) > 1 and sp.get('legend', True):
            top += self._put_legend(X, x, y, w, leg)
        if sp.get('y_title'):
            top += 0.28
        ends = []
        for i, q in enumerate(S):
            v = q['values']
            last = max([k for k in range(n) if v[k] is not None], default=None)
            if q.get('end_label', True) and last is not None:
                lab = q.get('label', q['name'])
                ends.append((i, last, (lab + ' ' if lab else '') + self.num(v[last], dec) + unit))
        rp = max([self.tw(t, 10, 'ui_semi') for _, _, t in ends] + [0]) + 0.25
        px, py = x + lw, top + 0.08
        pw, ph = w - lw - rp, y + h - 0.36 - py
        mid = not (ff is not None and 0 < ff < n)
        xs = self._xs(n, px, pw, mid)
        sy = lambda v: py + ph - (v - lo) / ((hi - lo) or 1) * ph
        groups_bar, groups_area, sers = [], [], []
        hidden = []
        if not mid:                                     # forecast zone: full-height columns behind the lines
            zc = b.add(sp.get('forecast_label', self.T['forecast']), [hi if k >= ff else None for k in range(n)])
            z = X.ser(sp.get('forecast_label', self.T['forecast']), zc, b.cols[zc][1], cat, fill=ZONE, line=None)
            groups_bar.append(z)
            hidden.append(z['i'])
        if band:
            lc = b.add(band.get('low_name', 'Cận dưới' if self.lang == 'vi' else 'Lower bound'), band['low'])
            hc = len(b.cols) + 1
            forms = [None if (l_ is None or h_ is None) else f'={b.cell(hc, k)}-{b.cell(lc, k)}'
                     for k, (l_, h_) in enumerate(zip(band['low'], band['high']))]
            bc = b.add(band.get('name', self.T['band80']),
                       [None if (l_ is None or h_ is None) else h_ - l_ for l_, h_ in zip(band['low'], band['high'])],
                       formulas=forms)
            b.add(band.get('high_name', 'Cận trên' if self.lang == 'vi' else 'Upper bound'), band['high'])
            l_ = X.ser(b.cols[lc][0], lc, b.cols[lc][1], cat, fill=None, line=None)
            u_ = X.ser(band.get('name', self.T['band80']), bc, b.cols[bc][1], cat,
                       fill=band.get('color', BLUE), alpha=0.22, line=None)
            groups_area += [l_, u_]
            hidden.append(l_['i'])
        offs = self._end_offsets([(i, sy(S[i]['values'][k])) for i, k, _ in ends], 0.22, py - 0.05, py + ph + 0.05,
                                 h)
        for i, q in enumerate(S):
            c = cols[i]
            v = q['values']
            col = b.add(q['name'], v, fmt=_nf(dec))
            dp = {}
            df = q.get('dash_from')
            if df is not None:
                for k in range(df, n):
                    dp[k] = {'dash': 'dash'}
            mk = q.get('markers', sp.get('markers', 'last'))
            msize = 6
            last = max([k for k in range(n) if v[k] is not None], default=None)
            hollow = bool(q.get('dashed'))
            mkr = {'symbol': 'circle', 'size': msize, 'fill': PAPER if hollow else c, 'line': c if hollow else PAPER,
                   'lw': 1.25 if hollow else 0.75}
            marker = mkr if mk == 'all' else None
            if mk == 'last' and not q.get('muted') and last is not None:
                dp.setdefault(last, {})['marker'] = mkr
            lab = None
            e = [t for t in ends if t[0] == i]
            if e:
                _, k, t = e[0]
                lab = {'show': ('ser', 'val') if q.get('label', q['name']) else ('val',), 'only': [k],
                       'pos': 'r', 'fmt': _nf(dec, unit), 'color': INK2 if q.get('muted') else INK, 'size': 10,
                       'off': {k: (0, offs.get(i, 0))}}
                if q.get('label') is not None and q.get('label') != q['name']:
                    lab['text'] = {k: t}
            sers.append(X.ser(q['name'], col, v, cat, line=c, lw=q.get('width', 1.75 if q.get('muted') else 2.0),
                              dash='dash' if q.get('dashed') else None, marker=marker, dpts=dp, labels=lab))
        for r in refs:
            rc = b.add(r.get('label', 'Ref'), [r['value']] * n)
            rs = X.ser(r.get('label', 'Ref'), rc, b.cols[rc][1], cat, line=INK2, lw=1.0, dash='dash',
                       labels={'text': {0: r['label']}, 'pos': 'r', 'color': INK2, 'semi': False, 'size': 9.5,
                               'off': {0: (0, -0.14 / h)}}
                       if r.get('label') else None)
            sers.append(rs)
            hidden.append(rs['i'])
        if X.legend:
            X.legend['hide'] = hidden
        if groups_bar:
            X.group('bar', groups_bar, gap=0)
        if groups_area:
            X.group('area', groups_area, grouping='stacked')
        sers.sort(key=lambda q: 0 if q['line'] in (MUTE, INK2) else 1)    # muted and reference lines behind
        X.group('line', sers)
        X.cat_ax(1, 2, line=INK if lo <= 0 <= hi or sp.get('zero') else BASE, skip=self._skip(cats, pw))
        X.val_ax(2, 1, lo, hi, st, _nf(dt) if not sp.get('tick_unit') else _nf(dt, sp['tick_unit']),
                 between='midCat' if mid else 'between')
        gf = X.place(s.shapes, (x, y, w, h), (px, py, pw, ph), 'Chart · line')
        cv = _Cv(self, s, x, y, w, 0.3, 'Chart labels')
        if sp.get('y_title'):
            cv.label(x, top - 0.2, sp['y_title'], 9.5, 'ui', INK2)
        if not mid:
            cv.label(xs[ff] - pw / n / 2 + 0.08, py + 0.12, sp.get('forecast_label', self.T['forecast']), 9, 'ui_semi',
                     INK2, caps=True, spacing=0.8)
        pts = []
        for a in sp.get('annotations', []):
            q = S[a.get('series', 0)]
            k = a['at']
            v = a.get('value', q['values'][k])
            if v is not None:
                pts.append((xs[k], sy(v), a['text'], a.get('tier', 'pri'), a.get('dx', 0.3), a.get('dy', -0.45)))
        self._ann_n(s, pts, (x, y, w, h))
        return gf

    def _n_fan(self, s, x, y, w, h, sp):
        n_act = len(sp['actual'])
        cats = sp['categories']
        a = sp['actual']
        act = list(a) + [None] * (len(cats) - n_act)
        base = [None] * (n_act - 1) + [a[-1]] + list(sp['base'])
        low = [None] * (n_act - 1) + [a[-1]] + list(sp['low'])
        high = [None] * (n_act - 1) + [a[-1]] + list(sp['high'])
        nm = sp.get('names', {})
        ln = dict(sp, type='line', series=[
            {'name': nm.get('actual', ''), 'values': act, 'color': sp.get('color', INK), 'end_label': False},
            {'name': nm.get('base', self.T['forecast']), 'values': base, 'color': sp.get('color2', BLUE),
             'dashed': True, 'label': ''}],
            band={'low': low, 'high': high, 'name': nm.get('band', self.T['band80']), 'color': sp.get('color2', BLUE)},
            forecast_from=sp.get('forecast_from', n_act))
        return self._n_line(s, x, y, w, h, ln)

    # ── step line (scatter with duplicated points) ──
    def _n_step(self, s, x, y, w, h, sp):
        """XY scatter with straight lines; every change point is written twice (old level, new level) so the line
        steps. All series share one X column (column B; column A holds the date labels — XY data must not start in
        column A, which some apps read as categories)."""
        S = sp['series']
        dec, unit = sp.get('dec', 2), sp.get('unit', '')
        xa, xb = sp['x_min'], sp['x_max']
        P = [sorted(q['points']) for q in S]
        x0 = min(p[0][0] for p in P)
        cur = [p[0][1] if abs(p[0][0] - x0) < 1e-9 else None for p in P]
        rows = [(x0, list(cur))]
        ev = sorted({px for p in P for px, _ in p if px > x0 + 1e-9})
        for ex in ev:
            rows.append((ex, list(cur)))
            for i_, p in enumerate(P):
                for px, pv in p:
                    if abs(px - ex) < 1e-9:
                        cur[i_] = pv
            rows.append((ex, list(cur)))
        end = max(q.get('until', xb) for q in S)
        rows.append((end, list(cur)))
        fmt_x = sp.get('x_label', lambda v: self.num(v, 2))
        b = _Book()
        b.add('Mốc' if self.lang == 'vi' else 'Point', [fmt_x(r[0]) for r in rows])
        cx = b.add('x', [r[0] for r in rows], fmt='0.000')
        X = _XChart(self, b)
        allv = [p[1] for q in S for p in q['points']]
        lo, hi, st, dt, lw = self._yscale(allv, sp, zero=sp.get('zero', True))
        top = y
        cols = [q.get('color', PALETTE[i % len(PALETTE)]) for i, q in enumerate(S)]
        if len(S) > 1:
            top += self._put_legend(X, x, y, w, [(q['name'], c, 'line') for q, c in zip(S, cols)])
        ends = [(i, cur[i], f"{q.get('label', q['name'])} {self.num(cur[i], dec)}{unit}") for i, q in enumerate(S)]
        rp = max(self.tw(t, 10, 'ui_semi') for _, _, t in ends) + 0.25
        px, py, pw, ph = x + lw, top + 0.1, w - lw - rp, y + h - 0.4 - top - 0.1
        sy = lambda v: py + ph - (v - lo) / ((hi - lo) or 1) * ph
        offs = self._end_offsets([(i, sy(v)) for i, v, _ in ends], 0.22, py, py + ph, h)
        last = len(rows) - 1
        xs_ = [r[0] for r in rows]
        sers = []
        for i, q in enumerate(S):
            ys_ = [r[1][i] for r in rows]
            cy = b.add(q['name'], ys_, fmt=_nf(dec))
            sers.append(X.ser(q['name'], cy, ys_, x=(cx, xs_), line=cols[i], lw=2.0, cap='flat',
                              dpts={last: {'marker': {'symbol': 'circle', 'size': 6, 'fill': cols[i]}}},
                              labels={'show': ('ser', 'val'), 'only': [last], 'pos': 'r', 'fmt': _nf(dec, unit),
                                      'color': INK, 'off': {last: (0, offs.get(i, 0))}}))
        sers.sort(key=lambda q: 0 if q['line'] == MUTE else 1)
        X.group('scatter', sers)
        X.val_ax(1, 2, xa, xb, sp.get('x_step', 1), sp.get('x_fmt', '0'), pos='b', grid=False, line=INK, color=INK2)
        X.val_ax(2, 1, lo, hi, st, _nf(dt), crosses=xa)
        gf = X.place(s.shapes, (x, y, w, h), (px, py, pw, ph), 'Chart · step line')
        sx = lambda v: px + (v - xa) / ((xb - xa) or 1) * pw
        self._ann_n(s, [(sx(a['x']), sy(a['y']), a['text'], a.get('tier', 'pri'), a.get('dx', 0.3), a.get('dy', -0.4))
                        for a in sp.get('annotations', [])], (x, y, w, h))
        return gf

    # ── area: single, stacked, 100% stacked ──
    def _n_area(self, s, x, y, w, h, sp, grouping=None):
        cats, S = [str(c) for c in sp['categories']], sp['series']
        n, dec, unit = len(cats), sp.get('dec', 0), sp.get('unit', '')
        grouping = grouping or sp.get('grouping', 'stacked' if len(S) > 1 else 'standard')
        pct = grouping == 'percentStacked'
        cols = [MUTE if q.get('muted') else q.get('color', PALETTE[i % len(PALETTE)]) for i, q in enumerate(S)]
        tot = [sum((q['values'][k] or 0) for q in S) for k in range(n)]
        b = _Book()
        cc = b.add(None, cats)
        X = _XChart(self, b)
        top = y
        if len(S) > 1:
            top += self._put_legend(X, x, y, w, [(q['name'], c, 'rect') for q, c in zip(S, cols)])
        if sp.get('y_title'):
            top += 0.28
        if pct:
            lo, hi, st, dt, lw = 0, 1, 0.25, 0, self.tw('100%', 10, 'ui') + 0.14
            fmt = '0%'
        else:
            lo, hi, st, dt, lw = self._yscale((tot if grouping == 'stacked' else [v for q in S for v in q['values']])
                                              + [0], sp, zero=True)
            fmt = _nf(dt)
        labels = [f"{q.get('label', q['name'])} {self.num((q['values'][-1] or 0) / (tot[-1] or 1) * 100, 0)}%"
                  if len(S) > 1 else self.num(q['values'][-1], dec) + unit for q in S]
        rp = max(self.tw(t, 10, 'ui_semi') for t in labels) + 0.3
        px, py, pw, ph = x + lw, top + 0.08, w - lw - rp, y + h - 0.36 - top - 0.08
        sers = []
        for i, q in enumerate(S):
            col = b.add(q['name'], q['values'], fmt=_nf(dec))
            sers.append(X.ser(q['name'], col, q['values'], (cc, cats), fill=cols[i], line=PAPER if len(S) > 1 else cols[i],
                              lw=1.0 if len(S) > 1 else 2.0, alpha=None if len(S) > 1 else 0.85))
        X.group('area', sers, grouping=grouping)
        X.cat_ax(1, 2, line=INK, skip=self._skip(cats, pw))
        X.val_ax(2, 1, lo, hi, st, fmt, between='midCat')
        gf = X.place(s.shapes, (x, y, w, h), (px, py, pw, ph), 'Chart · area')
        sy = lambda v: py + ph - (v - lo) / ((hi - lo) or 1) * ph
        cv = _Cv(self, s, x, y, w, h, 'Chart labels')
        if sp.get('y_title'):
            cv.label(x, top - 0.2, sp['y_title'], 9.5, 'ui', INK2)
        cum, mids = 0.0, []
        for i, q in enumerate(S):
            v = q['values'][-1] or 0
            share = v / (tot[-1] or 1)
            a0, a1 = (cum, cum + share) if pct else (cum * tot[-1], (cum + share) * tot[-1])
            if grouping == 'standard':
                a0, a1 = v, v
            mids.append([sy((a0 + a1) / 2), i])
            cum += share
        spread(mids, 0.22, py, py + ph)
        for yy, i in mids:
            cv.label(px + pw + 0.1, yy, labels[i], 10, 'ui_semi', INK)
        for a in sp.get('annotations', []):
            k = a['at']
            v = a.get('value', tot[k])
            self._ann(cv, self._xs(n, px, pw, True)[k], sy(v), a['text'], a.get('tier', 'pri'), a.get('dx', 0.3),
                      a.get('dy', -0.4))
        return gf

    def _n_area100(self, s, x, y, w, h, sp):
        return self._n_area(s, x, y, w, h, sp, 'percentStacked')

    # ── columns and bars ──
    def _n_bar(self, s, x, y, w, h, sp, horizontal=False, grouping='clustered'):
        cats = [str(c) for c in sp['categories']]
        S = sp.get('series') or [{'name': sp.get('name', ''), 'values': sp['values'], 'color': sp.get('color', BLUE)}]
        n, m = len(cats), len(S)
        dec, unit = sp.get('dec', 1), sp.get('unit', '')
        stacked = grouping != 'clustered'
        pct = grouping == 'percentStacked'
        hi_ = sp.get('highlight')
        hi_ = set(hi_ if isinstance(hi_, (list, tuple)) else ([] if hi_ is None else [hi_]))
        basis = sp.get('basis') or [None] * n
        cols = [MUTE if q.get('muted') else q.get('color', PALETTE[i % len(PALETTE)]) for i, q in enumerate(S)]
        if m == 1 and not stacked:
            cols = [S[0].get('color', sp.get('color', BLUE))]
        tots = [sum((q['values'][k] or 0) for q in S) for k in range(n)]
        b = _Book()
        cc = b.add(None, cats)
        cat = (cc, cats)
        X = _XChart(self, b)
        top = y
        leg = [(q['name'], c, 'rect') for q, c in zip(S, cols)] if m > 1 else []
        if any(bb in ('estimate', 'plan') for bb in basis):
            leg.append((sp.get('basis_label', self.T['estimate'] + ' / ' + self.T['plan']), cols[0] if m > 1 or not hi_
                        else cols[0], 'hollow'))
        hidden = []
        if leg:
            top += self._put_legend(X, x, y, w, leg)
        if sp.get('y_title') and not horizontal:
            top += 0.28
        allv = [v for q in S for v in q['values'] if v is not None]
        show_totals = stacked and not pct and sp.get('totals', True)
        if pct:
            lo, hi, st, dt = 0, 1, 0.25, 0
            lw = self.tw('100%', 10, 'ui') + 0.14
            afmt = '0%'
        else:
            lo, hi, st, dt, lw = self._yscale((tots if stacked else allv) + [0], sp, zero=True)
            afmt = _nf(dt)
        lab = sp.get('labels', 'auto')
        vfmt = _nf(dec, unit, sp.get('sign', False))
        sers = []
        if not horizontal:
            px, py = x + lw, top + 0.12
            pw, ph = w - lw - 0.1, y + h - py - (0.62 if any(len(c) > 9 for c in cats) and n > 6 else 0.36)
            slot = pw / n
            gap, t = self._gap(slot, 1 if stacked else m)
        else:
            lwc = min(sp.get('label_w', 3.4), max(self.tw(c, 10.5, 'ui_med') for c in cats) + 0.2)
            vw = (max(self.tw(self.num(v, dec, sp.get('sign', False)) + unit, 10, 'ui_semi') for v in
                      (tots if show_totals else allv)) + 0.2) if not pct else 0.15
            axis = sp.get('axis', stacked and not pct)
            px, py = x + lwc, top + (0.05 if not axis else 0.05)
            pw, ph = w - lwc - vw, y + h - py - (0.32 if axis else 0.05)
            slot = ph / n
            gap, t = self._gap(slot, 1 if stacked else m, BAR_MAX * 0.9, 0.64)
        for j, q in enumerate(S):
            col = b.add(q['name'], q['values'], fmt=_nf(dec))
            c = cols[j]
            dp = {}
            for k in range(n):
                fillk = c
                if m == 1 and not stacked and hi_ and k not in hi_:
                    fillk = MUTE
                if basis[k] in ('estimate', 'plan'):
                    dp[k] = {'fill': mix(fillk, 'FFFFFF', 0.78), 'line': fillk if fillk != MUTE else INK3, 'lw': 1.0,
                             'dash': 'sysDash'}
                elif fillk != c:
                    dp[k] = {'fill': fillk}
            labels = None
            vals = q['values']
            if not stacked and lab not in (False, 'none'):
                only = []
                for k in range(n):
                    if vals[k] is None:
                        continue
                    txt = self.num(vals[k], dec, sp.get('sign', False)) + unit
                    fits = (slot / m > self.tw(txt, 9.5, 'ui_semi') + 0.06) if not horizontal else True
                    if lab is True or lab == 'all' or (lab == 'auto' and (fits or k in hi_ or k == n - 1)) or \
                            (lab == 'hi' and k in hi_):
                        only.append(k)
                labels = {'show': ('val',), 'pos': 'outEnd', 'fmt': vfmt, 'only': only, 'size': 9.5,
                          'color': INK2, 'color_at': {k: INK for k in hi_}}
            elif pct and lab is not False:
                only = [k for k in range(n) if vals[k] and (vals[k] / (tots[k] or 1)) * pw > 0.45]
                labels = {'show': ('val',), 'pos': 'ctr', 'fmt': _nf(sp.get('dec', 0), '%'), 'only': only,
                          'size': 9.5, 'color': on(c),
                          'text': {k: self.num(vals[k] / tots[k] * 100, sp.get('dec', 0)) + '%' for k in only}}
                if not horizontal:
                    labels['only'] = [k for k in range(n) if vals[k] and vals[k] / (tots[k] or 1) * ph > 0.22]
                    labels['text'] = {k: self.num(vals[k] / tots[k] * 100, sp.get('dec', 0)) + '%'
                                      for k in labels['only']}
            sers.append(X.ser(q['name'], col, vals, cat, fill=c, line=PAPER if stacked else None,
                              lw=1.0 if stacked else 0.75, dpts=dp, labels=labels))
        X.group('bar', sers, dir='bar' if horizontal else 'col', grouping=grouping, gap=gap,
                overlap=100 if stacked else (sp.get('overlap', -8 if m > 1 else 0)))
        if show_totals:                                # invisible clustered twin on secondary axes carries totals
            forms = ['=' + '+'.join(b.cell(1 + j, k) for j in range(m)) for k in range(n)]
            tc = b.add(sp.get('total_name', 'Tổng' if self.lang == 'vi' else 'Total'), tots, fmt=_nf(dec),
                       formulas=forms)
            ts = X.ser(b.cols[tc][0], tc, tots, cat, fill=None, line=None,
                       labels={'show': ('val',), 'pos': 'outEnd', 'fmt': _nf(dec, unit), 'size': 9.5, 'color': INK2})
            X.group('bar', [ts], axes=(3, 4), dir='bar' if horizontal else 'col', gap=gap)
            hidden.append(ts['i'])
        if X.legend:
            X.legend['hide'] = hidden
        if not horizontal:
            csz = 10 if pw / n > 0.45 else 9
            X.cat_ax(1, 2, line=INK, skip=self._skip(cats, pw, csz), size=csz)
            X.val_ax(2, 1, lo, hi, st, afmt, grid=sp.get('grid', True), delete=sp.get('value_axis') is False)
            if show_totals:
                X.cat_ax(3, 4, delete=True)
                X.val_ax(4, 3, lo, hi, st, afmt, grid=False, delete=True, pos='r', crosses='max')
        else:
            X.cat_ax(1, 2, pos='l', reverse=True, line=INK if lo == 0 else None, lbl='low', size=10.5, color=INK)
            X.val_ax(2, 1, lo, hi, st, afmt, pos='b', grid=axis, delete=not axis, crosses='max')
            if show_totals:
                X.cat_ax(3, 4, pos='l', reverse=True, delete=True)
                X.val_ax(4, 3, lo, hi, st, afmt, pos='t', grid=False, delete=True, crosses='max')
        gf = X.place(s.shapes, (x, y, w, h), (px, py, pw, ph), 'Chart · ' + ('bar' if horizontal else 'column'))
        if sp.get('y_title') and not horizontal:
            _Cv(self, s, x, y, w, h, 'Chart labels').label(x, top - 0.2, sp['y_title'], 9.5, 'ui', INK2)
        pts = []
        xs = self._xs(n, px, pw)
        sy = lambda v: py + ph - (v - lo) / ((hi - lo) or 1) * ph
        for a in sp.get('annotations', []):
            k = a['at']
            v = S[a.get('series', 0)]['values'][k]
            pts.append((xs[k], sy(v) - 0.25, a['text'], a.get('tier', 'pri'), a.get('dx', 0.3), a.get('dy', -0.3)))
        self._ann_n(s, pts, (x, y, w, h))
        return gf

    def _n_barh(self, s, x, y, w, h, sp):
        return self._n_bar(s, x, y, w, h, sp, horizontal=True)

    def _n_stacked(self, s, x, y, w, h, sp):
        return self._n_bar(s, x, y, w, h, sp, grouping='stacked')

    def _n_stacked100(self, s, x, y, w, h, sp):
        return self._n_bar(s, x, y, w, h, sp, grouping='percentStacked')

    def _n_stacked_h(self, s, x, y, w, h, sp):
        return self._n_bar(s, x, y, w, h, sp, horizontal=True, grouping='stacked')

    def _n_stacked100_h(self, s, x, y, w, h, sp):
        return self._n_bar(s, x, y, w, h, sp, horizontal=True, grouping='percentStacked')

    def _n_histogram(self, s, x, y, w, h, sp):
        """data = raw values, edges = bin edges; counts are COUNTIFS formulas over the raw column in the workbook."""
        data, E_ = sp['data'], sp['edges']
        nb = len(E_) - 1
        cats = sp.get('bin_labels') or [f'{self.num(E_[i], sp.get("edge_dec", 0))}–{self.num(E_[i + 1], sp.get("edge_dec", 0))}'
                                       for i in range(nb)]
        counts = [sum(1 for v in data if E_[i] <= v < E_[i + 1] or (i == nb - 1 and v == E_[-1])) for i in range(nb)]
        b = _Book()
        cc = b.add(sp.get('x_title', 'Khoảng'), cats)
        lo_c, hi_c = b.add('Từ' if self.lang == 'vi' else 'From', E_[:-1]), b.add('Đến' if self.lang == 'vi' else 'To', E_[1:])
        rawc = 4
        raw_ref = f'${_colname(rawc)}$2:${_colname(rawc)}${len(data) + 1}'
        forms = [f'=COUNTIFS({raw_ref},">="&{b.cell(lo_c, i)},{raw_ref},"<"&{b.cell(hi_c, i)})' for i in range(nb)]
        vc = b.add(sp.get('name', 'Số lượng' if self.lang == 'vi' else 'Count'), counts, fmt='0', formulas=forms)
        b.add(sp.get('data_name', 'Dữ liệu' if self.lang == 'vi' else 'Data'), data)
        X = _XChart(self, b)
        lo, hi, st, dt, lw = self._yscale(counts + [0], {}, zero=True)
        st = max(1, math.ceil(st))
        hi = math.ceil(max(counts) / st) * st
        top = y + (0.28 if sp.get('y_title') else 0)
        px, py, pw, ph = x + lw, top + 0.1, w - lw - 0.1, y + h - top - 0.1 - 0.55
        hi_ = sp.get('highlight', [])
        dp = {k: {'fill': MUTE} for k in range(nb) if hi_ and k not in hi_}
        X.group('bar', [X.ser(b.cols[vc][0], vc, counts, (cc, cats), fill=sp.get('color', BLUE), line=PAPER, lw=1.5,
                              dpts=dp, labels={'show': ('val',), 'pos': 'outEnd', 'fmt': '0', 'size': 9.5,
                                               'only': [k for k in range(nb) if counts[k]]})], gap=4)
        X.cat_ax(1, 2, line=INK)
        X.val_ax(2, 1, 0, hi, st, '0')
        gf = X.place(s.shapes, (x, y, w, h), (px, py, pw, ph), 'Chart · histogram')
        cv = _Cv(self, s, x, y, w, h, 'Chart labels')
        if sp.get('y_title'):
            cv.label(x, y + 0.08, sp['y_title'], 9.5, 'ui', INK2)
        if sp.get('x_title'):
            cv.label(px + pw, y + h - 0.02, sp['x_title'], 9.5, 'ui', INK2, ha='r', va='b')
        return gf

    def _n_diverging(self, s, x, y, w, h, sp):
        cats = [str(c) for c in sp['categories']]
        vals = sp['values']
        n, dec, unit = len(cats), sp.get('dec', 1), sp.get('unit', '')
        b = _Book()
        cc = b.add(None, cats)
        vc = b.add(sp.get('name', 'Giá trị' if self.lang == 'vi' else 'Value'), vals, fmt=_nf(dec))
        pos_n, neg_n = sp.get('pos_label', '> 0'), sp.get('neg_label', '< 0')
        pc = b.add(pos_n, [v if v is not None and v >= 0 else None for v in vals], fmt=_nf(dec),
                   formulas=[f'=IF({b.cell(vc, k)}>=0,{b.cell(vc, k)},NA())' for k in range(n)])
        nc = b.add(neg_n, [v if v is not None and v < 0 else None for v in vals], fmt=_nf(dec),
                   formulas=[f'=IF({b.cell(vc, k)}<0,{b.cell(vc, k)},NA())' for k in range(n)])
        X = _XChart(self, b)
        top = y + self._put_legend(X, x, y, w, [(pos_n, sp.get('pos_color', BLUE), 'rect'),
                                                (neg_n, sp.get('neg_color', REDS), 'rect')])
        vv = [v for v in vals if v is not None]
        a_, b_, st, dt, _ = self._yscale(vv + [0], {}, zero=True)
        lwc = max(self.tw(c, 10.5, 'ui_med') for c in cats) + 0.2
        vw = max(self.tw(self.num(v, dec, True) + unit, 10, 'ui_semi') for v in vv) + 0.18
        px, py, pw, ph = x + lwc + vw, top + 0.05, w - lwc - 2 * vw, y + h - top - 0.1
        slot = ph / n
        gap, _ = self._gap(slot, 1, BAR_MAX * 0.9, 0.64)
        lab = {'show': ('val',), 'pos': 'outEnd', 'fmt': _nf(dec, unit, True), 'size': 10, 'color': INK}
        X.group('bar', [X.ser(pos_n, pc, b.cols[pc][1], (cc, cats), fill=sp.get('pos_color', BLUE), labels=lab),
                        X.ser(neg_n, nc, b.cols[nc][1], (cc, cats), fill=sp.get('neg_color', REDS), labels=lab)],
                dir='bar', gap=gap, overlap=100)
        X.cat_ax(1, 2, pos='l', reverse=True, lbl='low', line=INK2, lw=1.0, size=10.5, color=INK)
        X.val_ax(2, 1, a_, b_, st, _nf(dt), pos='b', grid=False, delete=True, crosses='max')
        return X.place(s.shapes, (x, y, w, h), (px, py, pw, ph), 'Chart · diverging bar')

    def _n_lollipop(self, s, x, y, w, h, sp):
        """Line series with markers only (the heads) + minus error bars of the value's own length (the sticks)."""
        cats = [str(c) for c in sp['categories']]
        vals = sp['values']
        n, dec, unit = len(cats), sp.get('dec', 1), sp.get('unit', '')
        hi_ = set(sp.get('highlight', []))
        b = _Book()
        cc = b.add(None, cats)
        vc = b.add(sp.get('name', 'Giá trị'), vals, fmt=_nf(dec))
        sc = b.add('Que (= giá trị)' if self.lang == 'vi' else 'Stick (= value)', vals,
                   formulas=[f'={b.cell(vc, k)}' for k in range(n)])
        X = _XChart(self, b)
        top = y + (0.28 if sp.get('y_title') else 0)
        lo, hi, st, dt, lw = self._yscale(vals + [0], sp, zero=True)
        wrap = any(self.tw(c, 10, 'ui') > w / n - 0.1 for c in cats)
        px, py, pw, ph = x + lw, top + 0.15, w - lw - 0.1, y + h - top - 0.15 - (0.55 if wrap else 0.36)
        c = sp.get('color', BLUE)
        dp = {k: {'marker': {'symbol': 'circle', 'size': 9, 'fill': MUTE if hi_ and k not in hi_ else c,
                             'line': PAPER, 'lw': 1}} for k in range(n)}
        X.group('line', [X.ser(b.cols[vc][0], vc, vals, (cc, cats), line=None,
                               marker={'symbol': 'circle', 'size': 9, 'fill': c, 'line': PAPER, 'lw': 1}, dpts=dp,
                               err=[{'type': 'minus', 'minus': (sc, vals), 'line': INK3, 'lw': 1.5}],
                               labels={'show': ('val',), 'pos': 't', 'fmt': _nf(dec, unit), 'size': 9.5,
                                       'color': INK2, 'color_at': {k: INK for k in hi_}})])
        X.cat_ax(1, 2, line=INK)
        X.val_ax(2, 1, lo, hi, st, _nf(dt))
        gf = X.place(s.shapes, (x, y, w, h), (px, py, pw, ph), 'Chart · lollipop')
        if sp.get('y_title'):
            _Cv(self, s, x, y, w, h, 'Chart labels').label(x, y + 0.08, sp['y_title'], 9.5, 'ui', INK2)
        return gf

    def _n_dot(self, s, x, y, w, h, sp):
        """Dot plot: one marker per series and category, no lines; optional reference line and range connector."""
        cats = [str(c) for c in sp['categories']]
        S = sp['series']
        n, dec, unit = len(cats), sp.get('dec', 1), sp.get('unit', '')
        b = _Book()
        cc = b.add(None, cats)
        X = _XChart(self, b)
        refs = sp.get('refs', [])
        leg = [(q['name'], q.get('color', PALETTE[i % len(PALETTE)]), 'dot') for i, q in enumerate(S)] + \
            [(r['label'], INK2, 'dash') for r in refs]
        top = y + self._put_legend(X, x, y, w, leg)
        allv = [v for q in S for v in q['values'] if v is not None] + [r['value'] for r in refs]
        lo, hi, st, dt, lw = self._yscale(allv, sp, zero=sp.get('zero', False))
        wrap = any(self.tw(c, 10, 'ui') > w / n - 0.1 for c in cats)
        px, py, pw, ph = x + lw, top + 0.1, w - lw - 0.1, y + h - top - 0.1 - (0.55 if wrap else 0.36)
        sers = []
        for i, q in enumerate(S):
            col = b.add(q['name'], q['values'], fmt=_nf(dec))
            c = q.get('color', PALETTE[i % len(PALETTE)])
            sers.append(X.ser(q['name'], col, q['values'], (cc, cats), line=None,
                              marker={'symbol': q.get('symbol', 'circle'), 'size': 9, 'fill': c, 'line': PAPER, 'lw': 1},
                              labels={'show': ('val',), 'pos': q.get('label_pos', 'r'), 'fmt': _nf(dec, unit),
                                      'size': 9, 'color': INK2} if q.get('labels', True) else None))
        for r in refs:
            rc = b.add(r['label'], [r['value']] * n)
            sers.insert(0, X.ser(r['label'], rc, b.cols[rc][1], (cc, cats), line=INK2, lw=1.0, dash='dash'))
        X.group('line', sers)
        X.cat_ax(1, 2, line=INK if lo <= 0 <= hi else BASE)
        X.val_ax(2, 1, lo, hi, st, _nf(dt))
        return X.place(s.shapes, (x, y, w, h), (px, py, pw, ph), 'Chart · dot plot')

    def _n_dumbbell(self, s, x, y, w, h, sp):
        """Two marker-only line series; the connector is a custom error bar on series A (b − a, as formulas)."""
        rows = sp['rows']
        cats = [r['name'] for r in rows]
        A = [r.get('a') for r in rows]
        B = [r.get('b') for r in rows]
        n, dec, unit = len(cats), sp.get('dec', 1), sp.get('unit', '')
        la, lb = sp.get('labels', ('A', 'B'))
        ca, cb = sp.get('colors', (INK3, BLUE))
        b = _Book()
        cc = b.add(None, cats)
        ac, bc = b.add(la, A, fmt=_nf(dec)), b.add(lb, B, fmt=_nf(dec))
        up = b.add('Nối lên' if self.lang == 'vi' else 'Up', [max((q or 0) - (p or 0), 0) if p is not None and q is not None
                                                             else None for p, q in zip(A, B)],
                   formulas=[f'=MAX({b.cell(bc, k)}-{b.cell(ac, k)},0)' for k in range(n)])
        dn = b.add('Nối xuống' if self.lang == 'vi' else 'Down', [max((p or 0) - (q or 0), 0) if p is not None and q is not None
                                                                 else None for p, q in zip(A, B)],
                   formulas=[f'=MAX({b.cell(ac, k)}-{b.cell(bc, k)},0)' for k in range(n)])
        X = _XChart(self, b)
        top = y + self._put_legend(X, x, y, w, [(la, ca, 'dot'), (lb, cb, 'dot')])
        lo, hi, st, dt, lw = self._yscale([v for v in A + B if v is not None], sp, zero=sp.get('zero', False))
        wrap = any(self.tw(c, 10, 'ui') > w / n - 0.12 for c in cats)
        px, py, pw, ph = x + lw, top + 0.1, w - lw - 0.1, y + h - top - 0.1 - (0.55 if wrap else 0.36)
        X.group('line', [
            X.ser(la, ac, A, (cc, cats), line=None, marker={'symbol': 'circle', 'size': 8, 'fill': ca, 'line': PAPER},
                  err=[{'type': 'both', 'plus': (up, b.cols[up][1]), 'minus': (dn, b.cols[dn][1]), 'line': MUTE,
                        'lw': 3.0}]),
            X.ser(lb, bc, B, (cc, cats), line=None, marker={'symbol': 'circle', 'size': 9, 'fill': cb, 'line': PAPER},
                  labels={'show': ('val',), 'fmt': _nf(dec, unit), 'size': 9.5, 'color': INK,
                          'pos': 't', 'pos_at': {k: ('t' if (B[k] or 0) >= (A[k] or 0) else 'b') for k in range(n)},
                          'only': [k for k in range(n) if B[k] is not None]})])
        X.cat_ax(1, 2, line=BASE)
        X.val_ax(2, 1, lo, hi, st, _nf(dt))
        return X.place(s.shapes, (x, y, w, h), (px, py, pw, ph), 'Chart · dumbbell')

    def _n_slope(self, s, x, y, w, h, sp):
        S, pa = sp['series'], sp['periods']
        dec, unit = sp.get('dec', 1), sp.get('unit', '')
        b = _Book()
        cc = b.add(None, list(pa))
        X = _XChart(self, b)
        vals = [v for q in S for v in q['values']]
        lo, hi = min(vals), max(vals)
        pad = (hi - lo) * 0.06
        lo, hi = lo - pad, hi + pad
        lab_l = max(self.tw(f"{q['name']} {self.num(q['values'][0], dec)}{unit}", 10.5, 'ui_semi') for q in S) + 0.3
        lab_r = max(self.tw(f"{q['name']} {self.num(q['values'][1], dec)}{unit}", 10.5, 'ui_semi') for q in S) + 0.3
        span = min(w - lab_l - lab_r, 5.6)
        px = x + lab_l + (w - lab_l - lab_r - span) / 2
        py, pw, ph = y + 0.15, span, h - 0.55
        sy = lambda v: py + ph - (v - lo) / ((hi - lo) or 1) * ph
        offl = self._end_offsets([(i, sy(q['values'][0])) for i, q in enumerate(S)], 0.24, py - 0.1, py + ph + 0.1, h)
        offr = self._end_offsets([(i, sy(q['values'][1])) for i, q in enumerate(S)], 0.24, py - 0.1, py + ph + 0.1, h)
        ci = 0
        sers = []
        for i, q in enumerate(S):
            if q.get('hi'):
                c = q.get('color', PALETTE[ci % len(PALETTE)])
                ci += 1
            else:
                c = MUTE
            col = b.add(q['name'], q['values'], fmt=_nf(dec))
            strong = bool(q.get('hi'))
            sers.append(X.ser(q['name'], col, q['values'], (cc, list(pa)), line=c, lw=2.0 if strong else 1.5,
                              marker={'symbol': 'circle', 'size': 7, 'fill': c, 'line': PAPER},
                              labels={'show': ('ser', 'val'), 'only': [0, 1], 'fmt': _nf(dec, unit), 'size': 10.5,
                                      'color': INK if strong else INK2, 'semi': strong, 'pos': 'r',
                                      'pos_at': {0: 'l', 1: 'r'}, 'off': {0: (0, offl.get(i, 0)), 1: (0, offr.get(i, 0))}}))
        sers.sort(key=lambda q: 0 if q['line'] == MUTE else 1)
        X.group('line', sers)
        X.cat_ax(1, 2, pos='b', line=None, lbl='high', size=11, color=INK)
        X.val_ax(2, 1, lo, hi, None, '0', grid=False, delete=True, between='midCat')
        gf = X.place(s.shapes, (x, y, w, h), (px, py, pw, ph), 'Chart · slope')
        cv = _Cv(self, s, x, y, w, h, 'Chart rules')
        for xx in (px, px + pw):
            cv.seg(xx, py - 0.05, xx, py + ph + 0.05, BASE, 0.75, cap='flat')
        return gf

    def _n_bump(self, s, x, y, w, h, sp):
        """Ranks as RANK() formulas over the raw values (kept in the workbook); value axis reversed, 1 on top."""
        P, S = [str(p) for p in sp['periods']], sp['series']
        k_, m_ = len(S), len(P)
        dec, unit = sp.get('dec', 1), sp.get('unit', '')
        hl = sp.get('highlight', [])
        ranks = []
        for j in range(m_):
            order = sorted(range(k_), key=lambda i: -(S[i]['values'][j] if S[i]['values'][j] is not None else -1e9))
            r = [0] * k_
            for pos, i in enumerate(order):
                r[i] = pos + 1
            ranks.append(r)
        b = _Book()
        cc = b.add(None, P)
        rank_cols = [b.add(q['name'], [ranks[j][i] for j in range(m_)], fmt='0') for i, q in enumerate(S)]
        raw0 = len(b.cols)
        raw_cols = [b.add(q['name'] + (' (giá trị)' if self.lang == 'vi' else ' (value)'), q['values'], fmt=_nf(dec))
                    for q in S]
        for i, rc in enumerate(rank_cols):
            row = b.cols[rc]
            forms = []
            for j in range(m_):
                rng = f'${_colname(raw0)}${j + 2}:${_colname(raw0 + k_ - 1)}${j + 2}'
                forms.append(f'=RANK({b.cell(raw_cols[i], j)},{rng},0)')
            b.cols[rc] = (row[0], row[1], row[2], forms)
        X = _XChart(self, b)
        cols = {}
        ci = 0
        for i, q in enumerate(S):
            if q['name'] in hl:
                cols[i] = q.get('color', PALETTE[ci % len(PALETTE)])
                ci += 1
            else:
                cols[i] = MUTE
        wl = max(self.tw(q['name'], 10, 'ui_med') for q in S) + 0.35
        wr = max(self.tw(f"{q['name']} {self.num(q['values'][-1], dec)}{unit}", 10, 'ui_semi') for q in S) + 0.35
        px, py, pw, ph = x + wl, y + 0.12, w - wl - wr, h - 0.5
        sers = []
        for i, q in enumerate(S):
            c = cols[i]
            strong = c != MUTE
            sers.append(X.ser(q['name'], rank_cols[i], [ranks[j][i] for j in range(m_)], (cc, P), line=c,
                              lw=2.25 if strong else 1.5, marker={'symbol': 'circle', 'size': 6 if strong else 5,
                                                                  'fill': c, 'line': PAPER},
                              labels={'show': ('ser',), 'only': [0, m_ - 1], 'pos': 'r', 'pos_at': {0: 'l', m_ - 1: 'r'},
                                      'size': 10, 'color': INK if strong else INK2, 'semi': strong,
                                      'text': {m_ - 1: f"{q['name']} {self.num(q['values'][-1], dec)}{unit}"}}))
        sers.sort(key=lambda q: 0 if q['line'] == MUTE else 1)
        X.group('line', sers)
        X.cat_ax(1, 2, line=None, lbl='high', size=10, color=INK2)
        X.val_ax(2, 1, 0.5, k_ + 0.5, 1, '0', grid=False, delete=True, reverse=True, between='midCat')
        return X.place(s.shapes, (x, y, w, h), (px, py, pw, ph), 'Chart · bump')

    # ── combo, waterfall, tornado, pyramid, funnel ──
    def _n_combo(self, s, x, y, w, h, sp):
        """Columns on the left axis + a line on the right axis. Use only for two different units, name both axes,
        and keep both zero lines aligned (done here)."""
        cats = [str(c) for c in sp['categories']]
        bs, ls = sp['bar'], sp['line']
        n = len(cats)
        b = _Book()
        cc = b.add(None, cats)
        bcol = b.add(bs['name'], bs['values'], fmt=_nf(bs.get('dec', 1)))
        lcol = b.add(ls['name'], ls['values'], fmt=_nf(ls.get('dec', 1)))
        X = _XChart(self, b)
        top = y + self._put_legend(X, x, y, w, [(bs['name'], bs.get('color', BLUE), 'rect'),
                                                (ls['name'], ls.get('color', ORANGE), 'line')]) + 0.28
        bl0, bh0, bst, bdt, blw = self._yscale(bs['values'] + [0], {}, zero=True)
        pos_int = round(bh0 / bst)
        lv = [v for v in ls['values'] if v is not None]
        rst = nice(0, max(max(lv), 0) / max(pos_int, 1) * 1.0001, 1)[3]
        while max(lv) > rst * pos_int + 1e-9:
            rst = nice(0, rst * 1.5, 1)[3]
        neg_int = math.ceil(-min(min(lv), 0) / rst - 1e-9)
        llo, lhi = -neg_int * rst, pos_int * rst
        blo = -neg_int * bst
        rdt = step_dec(rst)
        rlw = max(self.tw(self.num(t * rst, rdt), 10, 'ui') for t in range(-neg_int, pos_int + 1)) + 0.16
        px, py, pw, ph = x + blw, top + 0.08, w - blw - rlw, y + h - top - 0.08 - 0.36
        gap, _ = self._gap(pw / n)
        X.group('bar', [X.ser(bs['name'], bcol, bs['values'], (cc, cats), fill=bs.get('color', BLUE))], gap=gap)
        last = max(k for k in range(n) if ls['values'][k] is not None)
        X.group('line', [X.ser(ls['name'], lcol, ls['values'], (cc, cats), line=ls.get('color', ORANGE), lw=2.0,
                               marker={'symbol': 'circle', 'size': 5, 'fill': ls.get('color', ORANGE), 'line': PAPER},
                               labels={'show': ('val',), 'only': [last], 'pos': 't',
                                       'fmt': _nf(ls.get('dec', 1), ls.get('unit', '')), 'color': INK})], axes=(3, 4))
        X.cat_ax(1, 2, line=INK, skip=self._skip(cats, pw), lbl='low')
        X.val_ax(2, 1, blo, bh0, bst, '#,##0;;0', grid=True)
        X.cat_ax(3, 4, delete=True)
        X.val_ax(4, 3, llo, lhi, rst, _nf(rdt), pos='r', grid=False, crosses='max')
        gf = X.place(s.shapes, (x, y, w, h), (px, py, pw, ph), 'Chart · combo')
        cv = _Cv(self, s, x, y, w, h, 'Axis titles')
        cv.label(x, top - 0.2, bs.get('axis_title', bs['name']), 9.5, 'ui_semi', INK2)
        cv.label(x + w, top - 0.2, ls.get('axis_title', ls['name']), 9.5, 'ui_semi', INK2, ha='r')
        return gf

    def _n_waterfall(self, s, x, y, w, h, sp):
        """Stacked columns: invisible base + up + down + total, all derived by formulas from the input column."""
        steps = sp['steps']
        n = len(steps)
        dec, unit = sp.get('dec', 0), sp.get('unit', '')
        cats = [st['name'] for st in steps]
        run, rows = 0.0, []
        for st in steps:
            if st.get('total'):
                v = st['value'] if st.get('value') is not None else run
                rows.append(('T', v, run))
                run = v
            else:
                rows.append(('D', st['value'], run))
                run += st['value']
        b = _Book()
        cc = b.add(None, cats)
        kc = b.add('Loại' if self.lang == 'vi' else 'Kind', ['Tổng' if r[0] == 'T' else 'Thay đổi' for r in rows])
        ic = b.add('Giá trị nhập' if self.lang == 'vi' else 'Input', [r[1] for r in rows], fmt=_nf(dec))
        cum = []
        run = 0.0
        for r in rows:
            run = r[1] if r[0] == 'T' else run + r[1]
            cum.append(run)
        cmc = b.add('Luỹ kế' if self.lang == 'vi' else 'Running total', cum, fmt=_nf(dec),
                    formulas=[f'=IF({b.cell(kc, k)}="Tổng",{b.cell(ic, k)},{b.cell(len(b.cols), k - 1) if k else "0"}+{b.cell(ic, k)})'
                              for k in range(n)])
        if steps[-1].get('total') and steps[-1].get('value') is None:
            col = b.cols[ic]
            forms = [None] * (n - 1) + [f'={b.cell(cmc, n - 2)}']
            b.cols[ic] = (col[0], col[1], col[2], forms)
        prev = lambda k: b.cell(cmc, k - 1) if k else '0'
        base = [0 if r[0] == 'T' else min(cum[k - 1] if k else 0, cum[k]) for k, r in enumerate(rows)]
        upv = [0 if r[0] == 'T' or r[1] < 0 else r[1] for r in rows]
        dnv = [0 if r[0] == 'T' or r[1] >= 0 else -r[1] for r in rows]
        tov = [r[1] if r[0] == 'T' else 0 for r in rows]
        T = f'"Tổng"'
        bcol = b.add('Nền (ẩn)' if self.lang == 'vi' else 'Base (hidden)', base, fmt=_nf(dec),
                     formulas=[f'=IF({b.cell(kc, k)}={T},0,MIN({prev(k)},{b.cell(cmc, k)}))' for k in range(n)])
        pos_n = sp.get('pos_label', 'Tăng' if self.lang == 'vi' else 'Adds')
        neg_n = sp.get('neg_label', 'Giảm' if self.lang == 'vi' else 'Subtracts')
        tot_n = sp.get('total_label', 'Tổng' if self.lang == 'vi' else 'Total')
        ucol = b.add(pos_n, upv, fmt=_nf(dec),
                     formulas=[f'=IF(AND({b.cell(kc, k)}<>{T},{b.cell(ic, k)}>=0),{b.cell(ic, k)},0)' for k in range(n)])
        dcol = b.add(neg_n, dnv, fmt=_nf(dec),
                     formulas=[f'=IF(AND({b.cell(kc, k)}<>{T},{b.cell(ic, k)}<0),-{b.cell(ic, k)},0)' for k in range(n)])
        tcol = b.add(tot_n, tov, fmt=_nf(dec),
                     formulas=[f'=IF({b.cell(kc, k)}={T},{b.cell(ic, k)},0)' for k in range(n)])
        topv = [max(cum[k - 1] if k and rows[k][0] == 'D' else 0, cum[k]) for k in range(n)]
        lcol = b.add('Đỉnh nhãn (ẩn)' if self.lang == 'vi' else 'Label top (hidden)', topv, fmt=_nf(dec),
                     formulas=[f'=MAX({b.cell(bcol, k)}+{b.cell(ucol, k)}+{b.cell(dcol, k)}+{b.cell(tcol, k)},0)'
                               for k in range(n)])
        X = _XChart(self, b)
        leg = [(tot_n, INK, 'rect'), (pos_n, BLUE, 'rect'), (neg_n, REDS, 'rect')]
        plan = [k for k, st in enumerate(steps) if st.get('basis') in ('estimate', 'plan')]
        if plan:
            leg.append((sp.get('basis_label', self.T['estimate'] + ' / ' + self.T['plan']), INK, 'hollow'))
        top = y + self._put_legend(X, x, y, w, leg) + (0.28 if sp.get('y_title') else 0)
        lo, hi, st_, dt, lw = self._yscale(cum + [0] + topv, sp, zero=True)
        px, py, pw, ph = x + lw, top + 0.15, w - lw - 0.1, y + h - top - 0.15 - 0.62
        gap, _ = self._gap(pw / n, 1, BAR_MAX * 1.25, 0.58)
        cat = (cc, cats)
        sb = X.ser(b.cols[bcol][0], bcol, base, cat, fill=None, line=None)
        su = X.ser(pos_n, ucol, upv, cat, fill=BLUE, line=None)
        sd = X.ser(neg_n, dcol, dnv, cat, fill=REDS, line=None)
        stt = X.ser(tot_n, tcol, tov, cat, fill=INK, line=None,
                    dpts={k: {'fill': mix(INK, 'FFFFFF', 0.8), 'line': INK, 'lw': 1.0, 'dash': 'sysDash'} for k in plan})
        texts = {k: (self.num(rows[k][1], dec) if rows[k][0] == 'T' else self.num(rows[k][1], dec, True)) + unit
                 for k in range(n)}
        sl = X.ser(b.cols[lcol][0], lcol, topv, cat, fill=None, line=None,
                   labels={'show': ('val',), 'pos': 'outEnd', 'text': texts, 'size': 10, 'color': INK,
                           'color_at': {k: INK2 for k in range(n) if rows[k][0] == 'D'}})
        X.group('bar', [sb, stt, su, sd], grouping='stacked', gap=gap)
        X.group('bar', [sl], axes=(3, 4), gap=gap)
        X.cat_ax(1, 2, line=INK)
        X.val_ax(2, 1, lo, hi, st_, _nf(dt))
        X.cat_ax(3, 4, delete=True)
        X.val_ax(4, 3, lo, hi, st_, _nf(dt), grid=False, delete=True, pos='r', crosses='max')
        gf = X.place(s.shapes, (x, y, w, h), (px, py, pw, ph), 'Chart · waterfall')
        if sp.get('y_title'):
            _Cv(self, s, x, y, w, h, 'Chart labels').label(x, top - 0.2, sp['y_title'], 9.5, 'ui', INK2)
        return gf

    def _n_tornado(self, s, x, y, w, h, sp):
        """Clustered bars, overlap 100: change from the base when one driver is at its low / high value."""
        rows, base = sp['rows'], sp['base']
        dec, unit = sp.get('dec', 2), sp.get('unit', '')
        la, lb = sp.get('labels', ('Thấp', 'Cao'))
        ca, cb = sp.get('colors', (BLUE, ORANGE))
        cats = [r['name'] for r in rows]
        n = len(rows)
        b = _Book()
        cc = b.add(None, cats)
        lc = b.add(la + (' (mức)' if self.lang == 'vi' else ' (level)'), [r['low'] for r in rows], fmt=_nf(dec))
        hc = b.add(lb + (' (mức)' if self.lang == 'vi' else ' (level)'), [r['high'] for r in rows], fmt=_nf(dec))
        bc = b.add(sp.get('base_label', self.T['base']), [base] + [None] * (n - 1), fmt=_nf(dec))
        B1 = f'${_colname(bc)}$2'
        dl = b.add(la, [r['low'] - base for r in rows], fmt=_nf(dec, '', True),
                   formulas=[f'={b.cell(lc, k)}-{B1}' for k in range(n)])
        dh = b.add(lb, [r['high'] - base for r in rows], fmt=_nf(dec, '', True),
                   formulas=[f'={b.cell(hc, k)}-{B1}' for k in range(n)])
        X = _XChart(self, b)
        top = y + self._put_legend(X, x, y, w, [(la, ca, 'rect'), (lb, cb, 'rect')]) + 0.3
        dv = [r['low'] - base for r in rows] + [r['high'] - base for r in rows]
        m = max(abs(v) for v in dv)
        a_, b_, st, dt = nice(-m, m, 4, True)[0], nice(-m, m, 4, True)[1], nice(-m, m, 4, True)[3], 0
        dt = step_dec(st)
        lwc = min(3.8, max(self.tw(c, 10.5, 'ui_med') for c in cats) + 0.2)
        px, py, pw, ph = x + lwc, top + 0.05, w - lwc - 0.15, y + h - top - 0.05 - 0.36
        gap, _ = self._gap(ph / n, 1, BAR_MAX * 0.8, 0.6)
        lab = {'show': ('val',), 'pos': 'outEnd', 'fmt': _nf(dec, '', True), 'size': 9.5, 'color': INK2}
        X.group('bar', [X.ser(la, dl, b.cols[dl][1], (cc, cats), fill=ca, labels=lab),
                        X.ser(lb, dh, b.cols[dh][1], (cc, cats), fill=cb, labels=lab)], dir='bar', gap=gap, overlap=100)
        X.cat_ax(1, 2, pos='l', reverse=True, lbl='low', line=INK, lw=1.25, size=10.5, color=INK)
        X.val_ax(2, 1, a_, b_, st, _nf(dt, '', True), pos='b', crosses='max')
        gf = X.place(s.shapes, (x, y, w, h), (px, py, pw, ph), 'Chart · tornado')
        zx = px + (0 - a_) / (b_ - a_) * pw
        _Cv(self, s, x, y, w, h, 'Chart labels').label(
            zx, top - 0.16, f"{sp.get('base_label', self.T['base'])} {self.num(base, dec)}{unit} = 0", 10, 'ui_semi', INK,
            ha='c')
        return gf

    def _n_pyramid(self, s, x, y, w, h, sp):
        """Clustered bars, overlap 100: left side stored negative (formula), shown unsigned by the number format;
        the comparison year is an outlined twin on secondary axes with the same scale."""
        B, L, R = sp['bands'], sp['left'], sp['right']
        cmp_ = sp.get('compare')
        dec, unit = sp.get('dec', 1), sp.get('unit', '')
        cl, cr = sp.get('colors', (BLUE, ORANGE))
        n = len(B)
        b = _Book()
        cc = b.add(None, list(B))
        lin = b.add(L['name'] + (' (nhập)' if self.lang == 'vi' else ' (input)'), L['values'], fmt=_nf(dec))
        lneg = b.add(L['name'], [-v for v in L['values']], fmt=_nf(dec), formulas=[f'=-{b.cell(lin, k)}' for k in range(n)])
        rc = b.add(R['name'], R['values'], fmt=_nf(dec))
        X = _XChart(self, b)
        leg = [(L['name'], cl, 'rect'), (R['name'], cr, 'rect')] + ([(cmp_['name'], INK, 'outline')] if cmp_ else [])
        top = y + self._put_legend(X, x, y, w, leg)
        allv = L['values'] + R['values'] + ((cmp_['left'] + cmp_['right']) if cmp_ else [])
        mx_ = nice(0, max(allv), 3)
        lim, st = mx_[1], mx_[3]
        lwc = max(self.tw(c, 9.5, 'ui') for c in B) + 0.2
        px, py, pw, ph = x + lwc, top + 0.05, w - lwc - 0.1, y + h - top - 0.05 - 0.34
        gap = 18
        fmt = _nf(step_dec(st), unit) + ';' + _nf(step_dec(st), unit).split(';')[0] if False else \
            f'#,##0{"." + "0" * step_dec(st) if step_dec(st) else ""}"{unit}";#,##0{"." + "0" * step_dec(st) if step_dec(st) else ""}"{unit}"'
        sers = [X.ser(L['name'], lneg, b.cols[lneg][1], (cc, list(B)), fill=cl),
                X.ser(R['name'], rc, R['values'], (cc, list(B)), fill=cr)]
        X.group('bar', sers, dir='bar', gap=gap, overlap=100)
        if cmp_:
            cin = b.add(cmp_['name'] + (' · trái (nhập)' if self.lang == 'vi' else ' · left (input)'), cmp_['left'])
            cln = b.add(cmp_['name'] + (' · trái' if self.lang == 'vi' else ' · left'), [-v for v in cmp_['left']],
                        formulas=[f'=-{b.cell(cin, k)}' for k in range(n)])
            crc = b.add(cmp_['name'] + (' · phải' if self.lang == 'vi' else ' · right'), cmp_['right'])
            c1 = X.ser(cmp_['name'], cln, b.cols[cln][1], (cc, list(B)), fill=None, line=INK, lw=1.0, dash='sysDash')
            c2 = X.ser(b.cols[crc][0], crc, cmp_['right'], (cc, list(B)), fill=None, line=INK, lw=1.0, dash='sysDash')
            X.group('bar', [c1, c2], axes=(3, 4), dir='bar', gap=gap, overlap=100)
        X.cat_ax(1, 2, pos='l', lbl='low', line=INK, size=9.5, color=INK2)
        X.val_ax(2, 1, -lim, lim, st, fmt, pos='b', grid=True)
        if cmp_:
            X.cat_ax(3, 4, pos='l', delete=True)
            X.val_ax(4, 3, -lim, lim, st, fmt, pos='t', grid=False, delete=True, crosses='max')
        gf = X.place(s.shapes, (x, y, w, h), (px, py, pw, ph), 'Chart · population pyramid')
        cy = lambda band: py + ph - (B.index(band) + 0.5) * ph / n
        sx = lambda v: px + (v + lim) / (2 * lim) * pw
        pts = []
        for a_ in sp.get('annotations', []):
            side = a_.get('side', 'r')
            v = (cmp_ if a_.get('compare') else None)
            vv = (cmp_['right' if side == 'r' else 'left'] if a_.get('compare') else (R if side == 'r' else L)['values'])[
                B.index(a_['band'])]
            pts.append((sx(vv if side == 'r' else -vv), cy(a_['band']), a_['text'], a_.get('tier', 'pri'),
                        a_.get('dx', 0.35 if side == 'r' else -0.35), a_.get('dy', 0)))
        self._ann_n(s, pts, (x, y, w, h))
        return gf

    def _n_funnel(self, s, x, y, w, h, sp):
        """Stacked bars centred by an invisible padding series = (max − value) / 2 (formula)."""
        st_ = sp['stages']
        cats = [q['name'] for q in st_]
        vals = [q['value'] for q in st_]
        n, dec, unit = len(cats), sp.get('dec', 1), sp.get('unit', '')
        b = _Book()
        cc = b.add(None, cats)
        vc = b.add(sp.get('name', 'Giá trị' if self.lang == 'vi' else 'Value'), vals, fmt=_nf(dec))
        mxr = f'${_colname(vc)}$2:${_colname(vc)}${n + 1}'
        pc = b.add('Đệm (ẩn)' if self.lang == 'vi' else 'Padding (hidden)', [(max(vals) - v) / 2 for v in vals],
                   formulas=[f'=(MAX({mxr})-{b.cell(vc, k)})/2' for k in range(n)])
        X = _XChart(self, b)
        lwc = min(3.6, max(self.tw(c, 11, 'ui_med') for c in cats) + 0.25)
        px, py, pw, ph = x + lwc, y + 0.1, w - lwc - 0.2, h - 0.2
        gap, _ = self._gap(ph / n, 1, 0.62, 0.7)
        ramp = [mix(sp.get('color', BLUE), 'FFFFFF', t) for t in [0.0, 0.25, 0.45, 0.6, 0.7][:n]]
        share = sp.get('show_share', abs(vals[0] - 100) > 1e-9)
        texts = {k: self.num(vals[k], dec) + unit + (f"  ({self.num(vals[k] / vals[0] * 100, 1)}%)" if k and share else '')
                 for k in range(n)}
        X.group('bar', [X.ser(b.cols[pc][0], pc, b.cols[pc][1], (cc, cats), fill=None, line=None),
                        X.ser(b.cols[vc][0], vc, vals, (cc, cats), fill=sp.get('color', BLUE),
                              dpts={k: {'fill': ramp[k]} for k in range(n)},
                              labels={'show': ('val',), 'pos': 'ctr', 'text': texts, 'size': 12,
                                      'color_at': {k: on(ramp[k]) for k in range(n)}})],
                dir='bar', grouping='stacked', gap=gap)
        X.cat_ax(1, 2, pos='l', reverse=True, lbl='low', line=None, size=11, color=INK)
        X.val_ax(2, 1, 0, max(vals), None, '0', pos='b', grid=False, delete=True, crosses='max')
        return X.place(s.shapes, (x, y, w, h), (px, py, pw, ph), 'Chart · funnel')

    def _n_bullet(self, s, x, y, w, h, sp):
        """One small native chart per indicator: grey bands (stacked), actual (thin column, secondary axis, same
        scale), target (marker-only line series, dash marker)."""
        R = sp['rows']
        k = len(R)
        gx = 0.35
        cw = (w - gx * (k - 1)) / k
        cv = _Cv(self, s, x, y, w, h, 'Bullet labels')
        head = 1.3
        top = y + self._legend(cv, [(sp.get('actual_label', 'Thực hiện' if self.lang == 'vi' else 'Actual'),
                                     sp.get('color', INK), 'rect'),
                                    (sp.get('target_label', 'Mục tiêu' if self.lang == 'vi' else 'Target'), RED, 'dashkey'),
                                    (sp.get('range_label', 'Vùng tham chiếu' if self.lang == 'vi' else 'Reference bands'),
                                     'D9D7CF', 'rect')]) + 0.1
        last = None
        for i, r in enumerate(R):
            cx = x + i * (cw + gx)
            dec, unit = r.get('dec', 1), r.get('unit', '')
            cv.seg(cx, top, cx + cw, top, INK, 1.25, cap='flat')
            nl = self.nlines(r['name'], 11, 'ui_semi', cw)
            self._tx(cv.s, cx, top + 0.08, cw, self.lh(11, 1.05, nl), r['name'], 11, INK, 'ui_semi', line=1.05)
            yy = top + 0.1 + self.lh(11, 1.05, nl)
            cv.runs(cx, yy + 0.25, [(self.num(r['value'], dec) + unit, {'role': 'ui_bold', 'size': 20})])
            if r.get('value_text'):
                ns = self.nlines(r['value_text'], 9.5, 'ui', cw)
                self._tx(cv.s, cx, yy + 0.5, cw, self.lh(9.5, 1.1, ns), r['value_text'], 9.5, INK2, 'ui', line=1.1)
            mx_ = r['max']
            bands = [0] + list(r.get('ranges', [])) + [mx_]
            b = _Book()
            cc = b.add(None, [r.get('short', r['name'])])
            bcols = []
            for q in range(len(bands) - 1):
                bcols.append(b.add(f"Vùng {q + 1}" if self.lang == 'vi' else f'Band {q + 1}', [bands[q + 1] - bands[q]]))
            ac = b.add(sp.get('actual_label', 'Thực hiện' if self.lang == 'vi' else 'Actual'), [r['value']], fmt=_nf(dec))
            sc = b.add('Thanh (= thực hiện)' if self.lang == 'vi' else 'Bar (= actual)', [r['value']],
                       formulas=[f'={b.cell(ac, 0)}'])
            tc = b.add(sp.get('target_label', 'Mục tiêu' if self.lang == 'vi' else 'Target'), [r['target']], fmt=_nf(dec))
            X = _XChart(self, b)
            cat = (cc, b.cols[cc][1])
            st = nice(0, mx_, 4)[3]
            lw = max(self.tw(self.num(t, step_dec(st)), 9, 'ui') for t in (0, mx_)) + 0.12
            fy = top + head + 0.1
            fh = y + h - fy
            pw_ = cw - lw - 0.05
            band_w = min(0.62, pw_ * 0.6)
            shades = ['EEECE6', 'E4E2DB', 'D9D7CF', 'CFCDC4']
            X.group('bar', [X.ser(b.cols[c][0], c, b.cols[c][1], cat, fill=shades[q % 4], line=None)
                            for q, c in enumerate(bcols)], grouping='stacked', gap=(pw_ - band_w) / band_w * 100)
            col = r.get('color', sp.get('color', INK))
            X.group('line', [X.ser(b.cols[ac][0], ac, [r['value']], cat, line=None, marker=None,
                                   err=[{'type': 'minus', 'minus': (sc, [r['value']]), 'line': col,
                                         'lw': band_w * 0.36 * 72}]),
                             X.ser(b.cols[tc][0], tc, [r['target']], cat, line=None,
                                   marker={'symbol': 'dash', 'size': min(72, int(band_w * 72 * 0.95)), 'fill': RED,
                                           'line': RED, 'lw': 0.75})])
            X.cat_ax(1, 2, line=INK, lbl='none')
            X.val_ax(2, 1, 0, mx_, st, _nf(step_dec(st)), grid=False, size=9)
            X.place(s.shapes, (cx, fy, cw, fh), (cx + lw, fy + 0.05, pw_, fh - 0.15), 'Chart · bullet')
        return cv.grp

    # ── distributions and relationships ──
    def _n_box(self, s, x, y, w, h, sp):
        """Stacked columns (q1 invisible, q1→median, median→q3) + custom error bars (whiskers to min / max). The
        five numbers are QUARTILE.INC / MIN / MAX formulas over the raw data kept in the workbook."""
        G = sp['groups']
        n = len(G)
        dec, unit = sp.get('dec', 1), sp.get('unit', '')
        cats = [g['name'] for g in G]
        st5 = [(min(g['values']), _quart(g['values'], .25), _quart(g['values'], .5), _quart(g['values'], .75),
                max(g['values'])) for g in G]
        b = _Book()
        cc = b.add(None, cats)
        raw0 = 11
        nr = max(len(g['values']) for g in G)

        def rng(i):
            c = _colname(raw0 + i)
            return f'${c}$2:${c}${len(G[i]["values"]) + 1}'
        names = (('Nhỏ nhất', 'Q1', 'Trung vị', 'Q3', 'Lớn nhất') if self.lang == 'vi' else
                 ('Min', 'Q1', 'Median', 'Q3', 'Max'))
        fn = ('MIN({r})', 'QUARTILE.INC({r},1)', 'MEDIAN({r})', 'QUARTILE.INC({r},3)', 'MAX({r})')
        sc = [b.add(names[j], [q[j] for q in st5], fmt=_nf(dec), formulas=['=' + fn[j].format(r=rng(i)) for i in range(n)])
              for j in range(5)]
        c_base = b.add('Q1 (nền ẩn)' if self.lang == 'vi' else 'Q1 (hidden base)', [q[1] for q in st5],
                       formulas=[f'={b.cell(sc[1], i)}' for i in range(n)])
        c_lo = b.add('Q1→trung vị' if self.lang == 'vi' else 'Q1→median', [q[2] - q[1] for q in st5],
                     formulas=[f'={b.cell(sc[2], i)}-{b.cell(sc[1], i)}' for i in range(n)])
        c_hi = b.add('Trung vị→Q3' if self.lang == 'vi' else 'Median→Q3', [q[3] - q[2] for q in st5],
                     formulas=[f'={b.cell(sc[3], i)}-{b.cell(sc[2], i)}' for i in range(n)])
        c_wl = b.add('Râu dưới' if self.lang == 'vi' else 'Lower whisker', [q[1] - q[0] for q in st5],
                     formulas=[f'={b.cell(sc[1], i)}-{b.cell(sc[0], i)}' for i in range(n)])
        c_wh = b.add('Râu trên' if self.lang == 'vi' else 'Upper whisker', [q[4] - q[3] for q in st5],
                     formulas=[f'={b.cell(sc[4], i)}-{b.cell(sc[3], i)}' for i in range(n)])
        while len(b.cols) < raw0:
            b.add(None, [])
        for g in G:
            b.add(g['name'] + (' (số liệu gốc)' if self.lang == 'vi' else ' (raw data)'), g['values'], fmt=_nf(dec))
        X = _XChart(self, b)
        top = y + 0.32
        lo, hi, st, dt, lw = self._yscale([q[0] for q in st5] + [q[4] for q in st5], sp, zero=sp.get('zero', False))
        px, py, pw, ph = x + lw, top + 0.1, w - lw - 2.4, y + h - top - 0.1 - 0.36
        gap, _ = self._gap(pw / n, 1, 0.7, 0.45)
        cat = (cc, cats)
        c = sp.get('color', BLUE)
        texts = {i: f"{names[2].lower()} {self.num(st5[i][2], dec)}{unit}" for i in range(n)}
        X.group('bar', [
            X.ser(b.cols[c_base][0], c_base, [q[1] for q in st5], cat, fill=None, line=None,
                  err=[{'type': 'minus', 'minus': (c_wl, b.cols[c_wl][1]), 'line': INK, 'lw': 1.25}]),
            X.ser(b.cols[c_lo][0], c_lo, b.cols[c_lo][1], cat, fill=mix(c, 'FFFFFF', 0.45), line=PAPER, lw=1.0),
            X.ser(b.cols[c_hi][0], c_hi, b.cols[c_hi][1], cat, fill=c, line=PAPER, lw=1.0,
                  err=[{'type': 'plus', 'plus': (c_wh, b.cols[c_wh][1]), 'line': INK, 'lw': 1.25}])],
            grouping='stacked', gap=gap)
        X.cat_ax(1, 2, line=BASE, size=10.5, color=INK, crosses=lo)
        X.val_ax(2, 1, lo, hi, st, _nf(dt))
        gf = X.place(s.shapes, (x, y, w, h), (px, py, pw, ph), 'Chart · box plot')
        cv = _Cv(self, s, x, y, w, h, 'Box plot key')
        if sp.get('y_title'):
            cv.label(x, y + 0.1, sp['y_title'], 9.5, 'ui', INK2)
        sy = lambda v: py + ph - (v - lo) / ((hi - lo) or 1) * ph
        xs = self._xs(n, px, pw)
        for i in range(n):
            cv.label(xs[i] + pw / n * 0.2 + 0.06, sy(st5[i][2]), texts[i], 9.5, 'ui_semi', INK)
        kx = px + pw + 0.45
        ky = py + 0.1
        key = [(names[4], 'max'), (names[3], 'q3'), (names[2], 'med'), (names[1], 'q1'), (names[0], 'min')]
        cv.label(kx, ky, 'Cách đọc' if self.lang == 'vi' else 'How to read', 9.5, 'ui_semi', INK2, caps=True,
                 spacing=0.8, va='t')
        bx, by = kx + 0.05, ky + 0.45
        cv.seg(bx + 0.15, by, bx + 0.15, by + 0.35, INK, 1.25, cap='flat')
        cv.rect(bx, by + 0.35, 0.3, 0.45, c)
        cv.rect(bx, by + 0.8, 0.3, 0.45, mix(c, 'FFFFFF', 0.45))
        cv.seg(bx + 0.15, by + 1.25, bx + 0.15, by + 1.6, INK, 1.25, cap='flat')
        for t, yy in zip([q[0] for q in key], (by, by + 0.35, by + 0.8, by + 1.25, by + 1.6)):
            cv.seg(bx + 0.34, yy, bx + 0.46, yy, INK3, 0.5, cap='flat')
            cv.label(bx + 0.52, yy, t, 9.5, 'ui', INK2)
        cv.label(kx, by + 1.95, sp.get('method', 'Tứ phân vị kiểu QUARTILE.INC' if self.lang == 'vi' else
                                       'Quartiles: QUARTILE.INC'), 8.5, 'ui', INK3)
        return gf

    def _n_scatter(self, s, x, y, w, h, sp, bubble=False):
        P = sp['points']
        xd, yd = sp.get('x_dec', 0), sp.get('y_dec', 1)
        b = _Book()
        X = _XChart(self, b)
        groups = sp.get('groups')                  # [{name, color, test: lambda p}] -> one series per group
        hl = [p for p in P if p.get('hi')]
        if not groups:
            groups = ([{'name': sp.get('hi_label', ''), 'color': sp.get('color', BLUE), 'pts': hl},
                       {'name': sp.get('other_label', ''), 'color': MUTE, 'pts': [p for p in P if not p.get('hi')]}]
                      if hl else [{'name': sp.get('name', ''), 'color': sp.get('color', BLUE), 'pts': P}])
        else:
            groups = [dict(g, pts=[p for p in P if p.get('group') == g['name']]) for g in groups]
        leg = [(g['name'], g['color'], 'dot') for g in groups if g['name']]
        if sp.get('trend') and not bubble:
            leg.append((self.T['trend'], INK2, 'dash'))
        top = y + (self._put_legend(X, x, y, w, leg) if leg else 0) + 0.3
        xa, xb, xt, xst = nice(min(p['x'] for p in P), max(p['x'] for p in P), 5, zero=sp.get('x_zero', False))
        ya, yb, yt, yst = nice(min(p['y'] for p in P), max(p['y'] for p in P), 4, zero=sp.get('y_zero', False))
        lw = max(self.tw(self.num(t, step_dec(yst)), 10, 'ui') for t in yt) + 0.14
        px, py, pw, ph = x + lw, top + 0.05, w - lw - 0.3, y + h - top - 0.05 - 0.62
        sx = lambda v: px + (v - xa) / ((xb - xa) or 1) * pw
        sy = lambda v: py + ph - (v - ya) / ((yb - ya) or 1) * ph
        smax = max((p.get('size', 1) for p in P), default=1)
        # label placement (same rules as the shape renderer), written as native per-point labels
        boxes = []
        place = {}
        for p in sorted([p for p in P if p.get('label')], key=lambda p: 0 if p.get('hi') else 1):
            qx, qy = sx(p['x']), sy(p['y'])
            t = p.get('text', p['name'])
            tw_, th = self.tw(t, 9.5, 'ui_semi') + 0.05, 0.18
            r_ = (math.sqrt(p.get('size', 1) / smax) * 0.42 + 0.05) if bubble else 0.09
            for pos, dx, dy, ha in (('r', r_, 0, 'l'), ('l', -r_, 0, 'r'), ('t', 0, -r_ - 0.08, 'c'),
                                    ('b', 0, r_ + 0.08, 'c')):
                lx = qx + dx if ha == 'l' else (qx + dx - tw_ if ha == 'r' else qx - tw_ / 2)
                bb = (lx, qy + dy - th / 2, lx + tw_, qy + dy + th / 2)
                if bb[2] > x + w or bb[0] < px:
                    continue
                if not any(bb[0] < o[2] and bb[2] > o[0] and bb[1] < o[3] and bb[3] > o[1] for o in boxes):
                    break
            boxes.append(bb)
            place[id(p)] = pos
        sers = []
        for gi, g in enumerate(groups):
            pts = g['pts']
            if not pts:
                continue
            xs_, ys_ = [p['x'] for p in pts], [p['y'] for p in pts]
            nm = g['name'] or ('Điểm' if self.lang == 'vi' else 'Points')
            nmc = b.add(nm + (' · tên' if self.lang == 'vi' else ' · name'), [p['name'] for p in pts])
            cx = b.add(sp.get('x_title', 'x'), xs_, fmt=_nf(xd))
            cy = b.add(nm, ys_, fmt=_nf(yd))
            texts = {k: p.get('text', p['name']) for k, p in enumerate(pts) if p.get('label')}
            lab = {'text': texts, 'pos': 'r', 'pos_at': {k: place.get(id(p), 'r') for k, p in enumerate(pts)},
                   'size': 9.5, 'color': INK if g['color'] != MUTE else INK2, 'show': ('val',)} if texts else None
            kw = dict(labels=lab)
            if bubble:
                sz = b.add(sp.get('size_title', 'size'), [p.get('size', 1) for p in pts], fmt='#,##0')
                sers.append(X.ser(nm, cy, ys_, x=(cx, xs_), size=(sz, b.cols[sz][1]), fill=g['color'], alpha=0.72,
                                  line=PAPER, lw=0.75, **kw))
            else:
                sers.append(X.ser(nm, cy, ys_, x=(cx, xs_), line=None, marker={'symbol': 'circle',
                                                                               'size': 7 if g['color'] != MUTE else 6,
                                                                               'fill': g['color'], 'line': PAPER,
                                                                               'lw': 0.75}, **kw))
        if sp.get('trend') and not bubble:
            xs_, ys_ = [p['x'] for p in P], [p['y'] for p in P]
            cx = b.add(sp.get('x_title', 'x') + (' (tất cả)' if self.lang == 'vi' else ' (all)'), xs_)
            cy = b.add(self.T['trend'], ys_)
            n_ = len(P)
            mx_, my_ = sum(xs_) / n_, sum(ys_) / n_
            sxx = sum((v - mx_) ** 2 for v in xs_)
            sxy = sum((a - mx_) * (c - my_) for a, c in zip(xs_, ys_))
            syy = sum((v - my_) ** 2 for v in ys_)
            self.last_r = sxy / math.sqrt(sxx * syy)
            tr = X.ser(self.T['trend'], cy, ys_, x=(cx, xs_), line=None, marker=None,
                       trend={'line': INK2, 'lw': 1.25, 'dash': 'dash'})
            sers.insert(0, tr)
        X.group('bubble' if bubble else 'scatter', sers, scale=sp.get('bubble_scale', 70))
        X.val_ax(1, 2, xa, xb, xst, _nf(step_dec(xst)), pos='b', grid=True, line=INK if not sp.get('x_zero') else INK)
        X.val_ax(2, 1, ya, yb, yst, _nf(step_dec(yst)), crosses=xa if xa > 0 else 'autoZero')
        gf = X.place(s.shapes, (x, y, w, h), (px, py, pw, ph), 'Chart · ' + ('bubble' if bubble else 'scatter'))
        cv = _Cv(self, s, x, y, w, h, 'Axis titles')
        if sp.get('y_title'):
            cv.label(x, top - 0.18, sp['y_title'], 9.5, 'ui', INK2)
        if sp.get('x_title'):
            cv.label(px + pw, py + ph + 0.36, sp['x_title'], 9.5, 'ui', INK2, ha='r', va='t')
        if sp.get('trend') and not bubble:
            cv.label(px + pw - 0.05, py + ph - 0.14, f"{self.T['trend']}: r = {self.num(self.last_r, 2)}", 9.5, 'ui_semi',
                     INK2, ha='r')
        if bubble and sp.get('size_note'):
            cv.label(x, py + ph + 0.36, sp['size_note'], 9.5, 'ui', INK2, va='t')
        return gf

    def _n_bubble(self, s, x, y, w, h, sp):
        return self._n_scatter(s, x, y, w, h, sp, bubble=True)

    # ── parts of a whole: donut, pie, gauge, marimekko ──
    def _n_donut(self, s, x, y, w, h, sp, pie=False):
        parts = sp['parts']
        dec, unit = sp.get('dec', 0), sp.get('unit', '')
        cols = [MUTE if p.get('muted') else p.get('color', PALETTE[i % len(PALETTE)]) for i, p in enumerate(parts)]
        b = _Book()
        cc = b.add(None, [p['name'] for p in parts])
        vc = b.add(sp.get('name', 'Giá trị' if self.lang == 'vi' else 'Value'), [p['value'] for p in parts],
                   fmt=_nf(sp.get('value_dec', dec)))
        X = _XChart(self, b)
        side = min(h, w * 0.5)
        tot = sum(p['value'] for p in parts)
        shares = [p['value'] / tot * 100 for p in parts]
        lab = {'show': ('pct',), 'fmt': '0%', 'size': 10.5, 'semi': True,
               'only': [i for i, v in enumerate(shares) if v >= 4],
               'color_at': {i: on(cols[i]) for i in range(len(parts))}}
        X.group('pie' if pie else 'doughnut', [X.ser(b.cols[vc][0], vc, b.cols[vc][1], (cc, b.cols[cc][1]),
                                                     fill=cols[0], line=PAPER, lw=1.5, labels=lab,
                                                     dpts={i: {'fill': c} for i, c in enumerate(cols)})],
                first=sp.get('first', 0), hole=sp.get('hole', 58))
        gf = X.place(s.shapes, (x, y, side, side), (x + 0.1, y + 0.1, side - 0.2, side - 0.2),
                     'Chart · ' + ('pie' if pie else 'donut'))
        cv = _Cv(self, s, x, y, w, h, 'Chart key')
        if not pie and sp.get('center'):
            cv.label(x + side / 2, y + side / 2 - 0.13, sp['center'][0], 20, 'ui_bold', INK, ha='c')
            cv.label(x + side / 2, y + side / 2 + 0.2, sp['center'][1], 9.5, 'ui', INK2, ha='c')
        lx, ly = x + side + 0.5, y + 0.15
        vw = max(self.tw(self.num(v, sp.get('share_dec', 1)) + '%', 18, 'ui_semi') for v in shares)
        for i, p in enumerate(parts):
            cv.rrect(lx, ly + 0.09, 0.16, 0.16, cols[i], r=0.03)
            cv.label(lx + 0.28, ly + 0.17, self.num(shares[i], sp.get('share_dec', 1)) + '%', 18, 'ui_semi', INK)
            tx_ = p['name'] + (f" · {self.num(p['value'], sp.get('value_dec', dec))}{unit}" if sp.get('show_values', True)
                               else '')
            nl = self.nlines(tx_, 11, 'ui', x + w - (lx + 0.45 + vw))
            self._tx(cv.s, lx + 0.45 + vw, ly + 0.07, x + w - (lx + 0.45 + vw), self.lh(11, 1.1, nl) + 0.04, tx_, 11,
                     INK2, 'ui', line=1.1)
            ly += max(0.55, self.lh(11, 1.1, nl) + 0.25)
        return gf

    def _n_pie(self, s, x, y, w, h, sp):
        return self._n_donut(s, x, y, w, h, sp, pie=True)

    def _n_gauge(self, s, x, y, w, h, sp):
        """Half doughnut: value, remainder (= max − value) and an invisible lower half (= max), as formulas."""
        v, mx_ = sp['value'], sp.get('max', 100)
        dec, unit = sp.get('dec', 1), sp.get('unit', '%')
        b = _Book()
        cc = b.add(None, [sp.get('value_label', 'Đã đạt' if self.lang == 'vi' else 'Reached'),
                          sp.get('rest_label', 'Còn lại' if self.lang == 'vi' else 'Remaining'),
                          '(nửa dưới, ẩn)' if self.lang == 'vi' else '(lower half, hidden)'])
        vc = b.add(sp.get('name', 'Giá trị' if self.lang == 'vi' else 'Value'), [v, mx_ - v, mx_], fmt=_nf(dec),
                   formulas=[None, '=$C$2-B2', '=$C$2'])
        b.add('Thang tối đa' if self.lang == 'vi' else 'Scale max', [mx_])
        X = _XChart(self, b)
        side = min(w * 0.8, h * 1.7)
        gx = x + (w - side) / 2
        y = y + max(0, (h - side / 2 - 0.45) / 2)
        X.group('doughnut', [X.ser(b.cols[vc][0], vc, [v, mx_ - v, mx_], (cc, b.cols[cc][1]), fill=BLUE, line=PAPER,
                                   lw=1.5, dpts={0: {'fill': sp.get('color', BLUE)}, 1: {'fill': 'E6E4DE'},
                                                 2: {'fill': None, 'line': None}})], first=270, hole=sp.get('hole', 66))
        gf = X.place(s.shapes, (gx, y, side, side), (gx + 0.05, y + 0.05, side - 0.1, side - 0.1), 'Chart · gauge')
        cv = _Cv(self, s, x, y, w, h, 'Gauge labels')
        cy = y + side / 2
        cv.label(gx + side / 2, cy - 0.32, self.num(v, dec) + unit, 40, 'ui_bold', INK, ha='c')
        cv.label(gx + side / 2, cy + 0.05, sp.get('caption', ''), 11, 'ui', INK2, ha='c')
        cv.label(gx + side * 0.04, cy + 0.2, self.num(0, 0), 9.5, 'ui', INK2, ha='c')
        cv.label(gx + side * 0.96, cy + 0.2, self.num(mx_, 0) + unit, 9.5, 'ui', INK2, ha='c')
        if sp.get('target') is not None:
            ang = math.pi * (1 - sp['target'] / mx_)
            r0, r1 = side / 2 * sp.get('hole', 66) / 100 - 0.05, side / 2 - 0.02
            cx_ = gx + side / 2
            cv.seg(cx_ + r0 * math.cos(ang), cy - r0 * math.sin(ang), cx_ + r1 * math.cos(ang),
                   cy - r1 * math.sin(ang), RED, 2.5, cap='flat')
        return gf

    def _n_marimekko(self, s, x, y, w, h, sp):
        """Native variable-width 100% columns: each column is a run of `res` zero-gap slices (widths by largest
        remainder), one empty slice between columns. Edit composition in the input table; widths are the slice
        counts (re-run the script to change widths)."""
        C, S = sp['columns'], sp['series']
        res = sp.get('resolution', 200)
        widths = [c['width'] for c in C]
        cnt = largest_remainder(widths, res)
        cols = [q.get('color', PALETTE[i % len(PALETTE)]) for i, q in enumerate(S)]
        rows, owner = [], []
        for j, c in enumerate(C):
            rows += [j] * cnt[j]
            if j < len(C) - 1:
                rows.append(None)
        b = _Book()
        cats = [str(i + 1) for i in range(len(rows))]
        cc = b.add('Lát' if self.lang == 'vi' else 'Slice', cats)
        inp0 = len(S) + 2
        X = _XChart(self, b)
        sers_cols = []
        for i, q in enumerate(S):
            vals = [None if r is None else C[r]['values'][i] for r in rows]
            forms = [None if r is None else f'=${_colname(inp0 + 1 + i)}${r + 2}' for r in rows]
            sers_cols.append(b.add(q['name'], vals, formulas=forms))
        b.add(None, [])
        b.add('Cột' if self.lang == 'vi' else 'Column', [c['name'] for c in C])
        for i, q in enumerate(S):
            b.add(q['name'] + (' (nhập)' if self.lang == 'vi' else ' (input)'), [c['values'][i] for c in C])
        b.add('Độ rộng' if self.lang == 'vi' else 'Width', widths)
        top = y + self._put_legend(X, x, y, w, [(q['name'], cols[i], 'rect') for i, q in enumerate(S)])
        lw = self.tw('100%', 10, 'ui') + 0.14
        px, py, pw, ph = x + lw, top + 0.05, w - lw - 0.05, y + h - top - 0.05 - 0.62
        X.group('bar', [X.ser(q['name'], sers_cols[i], b.cols[sers_cols[i]][1], (cc, cats), fill=cols[i], line=None)
                        for i, q in enumerate(S)], grouping='percentStacked', gap=0)
        X.cat_ax(1, 2, line=None, lbl='none')
        X.val_ax(2, 1, 0, 1, 0.25, '0%', grid=False)
        gf = X.place(s.shapes, (x, y, w, h), (px, py, pw, ph), 'Chart · marimekko')
        cv = _Cv(self, s, x, y, w, h, 'Column labels')
        sl = pw / len(rows)
        k = 0
        tw_ = sum(widths)
        for j, c in enumerate(C):
            x0 = px + k * sl
            cw = cnt[j] * sl
            k += cnt[j] + 1
            t1 = c.get('short', c['name'])
            t2 = self.num(c['width'] / tw_ * 100, 0) + '%'
            if self.tw(t1, 9.5, 'ui_med') < cw - 0.04:
                cv.label(x0 + cw / 2, py + ph + 0.08, t1, 9.5, 'ui_med', INK, ha='c', va='t')
                cv.label(x0 + cw / 2, py + ph + 0.3, t2, 9, 'ui', INK2, ha='c', va='t')
            elif self.tw(t2, 9, 'ui') < cw:
                cv.label(x0 + cw / 2, py + ph + 0.08, t2, 9, 'ui', INK2, ha='c', va='t')
            # segment shares inside the column when they fit
            acc = 0
            tot = sum(c['values'])
            for i, q in enumerate(S):
                v = c['values'][i] / tot
                seg_h = v * ph
                t = self.num(v * 100, 0) + '%'
                if seg_h > 0.24 and cw > self.tw(t, 9, 'ui_semi') + 0.08:
                    cv.label(x0 + cw / 2, py + ph - (acc + v / 2) * ph, t, 9, 'ui_semi', on(cols[i]), ha='c')
                acc += v
        return gf

    def _n_radar(self, s, x, y, w, h, sp):
        axes_, S = sp['axes'], sp['series']
        dec, unit = sp.get('dec', 0), sp.get('unit', '')
        b = _Book()
        cc = b.add(None, list(axes_))
        X = _XChart(self, b)
        top = y + self._put_legend(X, x, y, w, [(q['name'], q.get('color', PALETTE[i % len(PALETTE)]), 'line')
                                                for i, q in enumerate(S)])
        mx_ = nice(0, max(v for q in S for v in q['values']), 4)
        fw = min(w, (y + h - top) * 1.7)
        rx = x + (w - fw) / 2
        sers = []
        for i, q in enumerate(S):
            col = b.add(q['name'], q['values'], fmt=_nf(dec))
            c = q.get('color', PALETTE[i % len(PALETTE)])
            sers.append(X.ser(q['name'], col, q['values'], (cc, list(axes_)), line=c, lw=2.0,
                              marker={'symbol': 'circle', 'size': 5, 'fill': c, 'line': PAPER}))
        X.group('radar', sers)
        X.cat_ax(1, 2, line=HAIR, size=10.5, color=INK, grid=True)
        X.val_ax(2, 1, 0, mx_[1], mx_[3], _nf(step_dec(mx_[3]), unit), size=9)
        # automatic layout: PowerPoint fills the frame; LibreOffice draws radars smaller than the frame
        return X.place(s.shapes, (rx, top, fw, y + h - top), None, 'Chart · radar')

    # ── small multiples, sparklines ──
    def _nmini(self, s, x, y, w, h, cats, vals, kind='col', polarity=True, color=BLUE, dec=1, name='Series',
               axis=False, base=True, markers=True):
        """A tiny native chart (column or line) with no axes: sparklines, small multiples, KPI tiles."""
        cats = [str(c) for c in cats]
        b = _Book()
        cc = b.add(None, cats)
        vc = b.add(name, vals, fmt=_nf(dec))
        X = _XChart(self, b)
        vv = [v for v in vals if v is not None]
        n = len(vals)
        if kind == 'col':
            lo, hi = min(vv + [0]), max(vv + [0])
            gap, _ = self._gap(w / n, 1, BAR_MAX * 0.6, 0.66)
            dp = {k: {'fill': (REDS if polarity and v < 0 else (BLUE if polarity else color))}
                  for k, v in enumerate(vals) if v is not None}
            X.group('bar', [X.ser(name, vc, vals, (cc, cats), fill=color if not polarity else BLUE, dpts=dp)], gap=gap)
            X.cat_ax(1, 2, line=BASE if base else None, lbl='none')
        else:
            lo, hi = min(vv), max(vv)
            last = max(k for k in range(n) if vals[k] is not None)
            kmin = min(range(n), key=lambda k: vals[k] if vals[k] is not None else 1e18)
            dp = {last: {'marker': {'symbol': 'circle', 'size': 5, 'fill': color if not markers else BLUE,
                                    'line': PAPER}}}
            if markers and kmin != last:
                dp[kmin] = {'marker': {'symbol': 'circle', 'size': 4, 'fill': MUTE, 'line': PAPER}}
            X.group('line', [X.ser(name, vc, vals, (cc, cats), line=color, lw=1.5, dpts=dp)])
            X.cat_ax(1, 2, line=BASE if (base and lo < 0 < hi) else None, lbl='none')
        pad = (hi - lo) * 0.08 or 1
        X.val_ax(2, 1, lo if (kind == 'col' and lo >= 0) else lo - (pad if kind != 'col' else 0),
                 hi + (pad if kind != 'col' else 0), None, _nf(dec), grid=False, delete=not axis,
                 between='between' if kind == 'col' else 'midCat')
        return X.place(s.shapes, (x, y, w, h), (x, y, w, h), 'Chart · mini')

    def _n_multiples(self, s, x, y, w, h, sp):
        P = sp['panels']
        cols = sp.get('cols', 4)
        rows = math.ceil(len(P) / cols)
        gx, gy = 0.4, 0.28
        cv = _Cv(self, s, x, y, w, h, 'Small multiples')
        top = y
        if sp.get('legend', True) and sp.get('polarity', True):
            top += self._legend(cv, [(sp.get('pos_label', 'Dương' if self.lang == 'vi' else 'Positive'), BLUE, 'rect'),
                                     (sp.get('neg_label', 'Âm' if self.lang == 'vi' else 'Negative'), REDS, 'rect')])
        cw = (w - gx * (cols - 1)) / cols
        ch = (y + h - top - gy * (rows - 1)) / rows
        for i, p in enumerate(P):
            r, c = divmod(i, cols)
            px, py = x + c * (cw + gx), top + r * (ch + gy)
            cv.seg(px, py, px + cw, py, INK, 1.0, cap='flat')
            cv.label(px, py + 0.06, p['title'], 10, 'ui_semi', INK2, va='t')
            last = [v for v in p['values'] if v is not None][-1]
            dec = p.get('dec', sp.get('dec', 1))
            cv.runs(px, py + 0.46, [(self.num(last, dec), {'role': 'ui_semi', 'size': 17, 'color': INK}),
                                    (' ' + p.get('unit', sp.get('unit', '')), {'role': 'ui', 'size': 9.5, 'color': INK2}),
                                    ('  ' + str(p['categories'][-1]), {'role': 'ui', 'size': 9, 'color': INK3})])
            my, mh = py + 0.72, ch - 0.72 - 0.26
            vv = [v for v in p['values'] if v is not None]
            self._nmini(s, px, my, cw - 0.55, mh, p['categories'], p['values'], p.get('kind', 'col'),
                        sp.get('polarity', True), p.get('color', BLUE), dec, p['title'])
            lo_, hi_ = min(vv + [0]), max(vv + [0])
            cv.label(px + cw - 0.5, my + 0.02, self.num(hi_, 0), 8, 'ui', INK3, va='t')
            if lo_ < 0:
                cv.label(px + cw - 0.5, my + mh - 0.02, self.num(lo_, 0), 8, 'ui', INK3, va='b')
            cv.label(px, my + mh + 0.05, str(p['categories'][0]), 8.5, 'ui', INK3, va='t')
            cv.label(px + cw - 0.55, my + mh + 0.05, str(p['categories'][-1]), 8.5, 'ui', INK3, ha='r', va='t')
        return cv.grp

    def _ntable(self, s, x, y, w, rows, col_w, row_h, fills=None, styles=None, name='Table', borders='rows'):
        """Native table: rows = [[text|None]], fills[i][j] = hex | None, styles[i][j] = dict(size, role, color, align)."""
        nr, nc = len(rows), len(rows[0])
        gf = s.shapes.add_table(nr, nc, E(x), E(y), E(w), E(sum(row_h) if isinstance(row_h, list) else row_h * nr))
        gf.name = name
        tbl = gf.table
        tblPr = gf._element.graphic.graphicData.tbl.tblPr
        sid = tblPr.find(qn('a:tableStyleId'))
        if sid is None:
            sid = OxmlElement('a:tableStyleId')
            tblPr.append(sid)
        sid.text = '{2D5ABB26-0587-4C30-8999-92F81FD0307C}'
        tbl.first_row = False
        tbl.horz_banding = False
        for j, cw in enumerate(col_w):
            tbl.columns[j].width = E(cw)
        for i in range(nr):
            tbl.rows[i].height = E(row_h[i] if isinstance(row_h, list) else row_h)
            for j in range(nc):
                c = tbl.cell(i, j)
                st = (styles[i][j] if styles else None) or {}
                f = fills[i][j] if fills else None
                if f:
                    c.fill.solid()
                    c.fill.fore_color.rgb = rgb(f)
                else:
                    c.fill.background()
                c.margin_left = c.margin_right = E(st.get('pad', 0.06))
                c.margin_top = c.margin_bottom = E(0.01)
                c.vertical_anchor = MSO_ANCHOR.MIDDLE
                tf = c.text_frame
                tf.word_wrap = st.get('wrap', True)
                v = rows[i][j]
                p = tf.paragraphs[0]
                p.alignment = {'l': PP_ALIGN.LEFT, 'r': PP_ALIGN.RIGHT, 'c': PP_ALIGN.CENTER}[st.get('align', 'l')]
                if isinstance(v, list):                      # multi-paragraph cell: [(text, style)]
                    for k, (t, so) in enumerate(v):
                        pp = p if k == 0 else tf.add_paragraph()
                        pp.alignment = p.alignment
                        self._run(pp, t, size=so.get('size', 10), color=so.get('color', INK), role=so.get('role', 'ui'))
                else:
                    self._run(p, '' if v is None else str(v), size=st.get('size', 10), color=st.get('color', INK),
                              role=st.get('role', 'ui'), caps=st.get('caps', False))
                tcPr = c._tc.get_or_add_tcPr()
                for tag in ('a:lnL', 'a:lnR', 'a:lnT', 'a:lnB'):
                    for el in tcPr.findall(qn(tag)):
                        tcPr.remove(el)
                bl = st.get('borders', {})
                for k, tag in enumerate(('a:lnL', 'a:lnR', 'a:lnT', 'a:lnB')):
                    side = tag[-1]
                    spec = bl.get(side)
                    ln = OxmlElement(tag)
                    if spec:
                        col, wpt = spec
                        ln.set('w', str(int(wpt * 12700)))
                        sf = OxmlElement('a:solidFill')
                        clr = OxmlElement('a:srgbClr')
                        clr.set('val', col)
                        sf.append(clr)
                        ln.append(sf)
                    else:
                        ln.set('w', '0')
                        ln.append(OxmlElement('a:noFill'))
                    tcPr.insert(k, ln)
        return gf

    def _n_spark_table(self, s, x, y, w, h, sp):
        R = sp['rows']
        H = sp.get('headers', ('', '', '', ''))
        n = len(R)
        cw = [w * 0.34, w * 0.34, w * 0.14, w * 0.18]
        rh = min(0.58, (h - 0.34) / n)
        rows = [[H[0], H[1], H[2], H[3]]]
        styles = [[dict(size=9, role='ui_semi', color=INK2, caps=True, align=a, borders={'B': (INK, 1.0)})
                   for a in ('l', 'l', 'r', 'r')]]
        fills = [[None] * 4]
        for i, r in enumerate(R):
            dec, unit = r.get('dec', 1), r.get('unit', '')
            v = r['values']
            first = next(q for q in v if q is not None)
            ch = v[-1] - first
            ctext = self.num(ch, dec, sign=True) + r.get('chg_unit', ' đ.%' if self.lang == 'vi' else ' pp')
            name_cell = [(r['name'], {'size': 11, 'role': 'ui_med', 'color': INK})]
            if r.get('sub'):
                name_cell.append((r['sub'], {'size': 8.5, 'role': 'ui', 'color': INK3}))
            rows.append([name_cell, '', self.num(v[-1], dec) + unit, ctext])
            bd = {'B': (HAIR, 0.5)}
            styles.append([dict(borders=bd, pad=0.08), dict(borders=bd), dict(size=12, role='ui_semi', align='r',
                                                                              borders=bd),
                           dict(size=10.5, color=INK2, align='r', borders=bd, pad=0.08)])
            fills.append([ZEBRA if i % 2 else None] * 4)
        gf = self._ntable(s, x, y, w, rows, cw, [0.32] + [rh] * n, fills, styles, 'Sparkline table')
        for i, r in enumerate(R):
            yy = y + 0.32 + i * rh
            self._nmini(s, x + cw[0] + 0.12, yy + 0.09, cw[1] - 0.6, rh - 0.18, sp.get('years') and
                        list(range(len(r['values']))) or list(range(len(r['values']))), r['values'], 'line', False,
                        INK2, r.get('dec', 1), r['name'])
        return gf

    # ── tables with cell fills: heatmap, calendar heatmap, waffle ──
    def _n_heatmap(self, s, x, y, w, h, sp):
        R, C, V = sp['rows'], sp['cols'], sp['values']
        dec = sp.get('dec', 1)
        allv = [v for r in V for v in r if v is not None]
        div = sp.get('scale', 'diverging') == 'diverging'
        vmax = sp.get('vmax') or (max(abs(v - sp.get('center', 0)) for v in allv) if div else max(allv))
        cv = _Cv(self, s, x, y, w, 0.4, 'Heatmap legend')
        lx, ly = x, y + 0.12
        if sp.get('legend_title'):
            cv.label(lx, ly, sp['legend_title'], 9.5, 'ui_semi', INK)
            lx += self.tw(sp['legend_title'], 9.5, 'ui_semi') + 0.2
        if div:
            c0 = sp.get('center', 0)
            stops = [c0 - vmax, c0 - vmax / 2, c0, c0 + vmax / 2, c0 + vmax]
        else:
            lo = sp.get('vmin', 0)
            stops = [lo + (vmax - lo) * t for t in (0, .25, .5, .75, 1)]
        for v in stops:
            cv.rrect(lx, ly - 0.08, 0.26, 0.16, self._heat_color(sp, v, vmax), r=0.03)
            t = self.num(v, dec, sign=div)
            cv.label(lx + 0.31, ly, t, 9, 'ui', INK2)
            lx += 0.31 + self.tw(t, 9, 'ui') + 0.18
        top = y + 0.42
        lw = max(self.tw(r, 10, 'ui_med') for r in R) + 0.2
        ccw = (w - lw) / len(C)
        rh = min(sp.get('row_max', 0.38), (y + h - top - 0.3) / len(R))
        rows = [[''] + [str(c) for c in C]]
        styles = [[dict(size=9, color=INK2, align='c')] * (len(C) + 1)]
        fills = [[None] * (len(C) + 1)]
        for i, r in enumerate(R):
            row, st, fl = [r], [dict(size=10, role='ui_med', align='r', pad=0.1)], [None]
            for j in range(len(C)):
                v = V[i][j]
                col = self._heat_color(sp, v, vmax)
                row.append('' if v is None else (self.num(v, dec) if sp.get('show_values', True) else ''))
                st.append(dict(size=8.5, role='ui_med', color=on(col), align='c', pad=0.02,
                               borders={'L': (PAPER, 1.5), 'R': (PAPER, 1.5), 'T': (PAPER, 1.5), 'B': (PAPER, 1.5)}))
                fl.append(col)
            rows.append(row)
            styles.append(st)
            fills.append(fl)
        return self._ntable(s, x, top, w, rows, [lw] + [ccw] * len(C), [0.28] + [rh] * len(R), fills, styles,
                            'Heatmap (table)')

    def _n_calendar(self, s, x, y, w, h, sp):
        """Calendar heatmap: rows = years, columns = months (or weeks × weekdays), one native table cell per period."""
        return self._n_heatmap(s, x, y, w, h, dict(sp, row_max=sp.get('row_max', 0.62)))

    def _n_waffle(self, s, x, y, w, h, sp):
        """10 × 10 native table; each cell = 1% (largest-remainder rounding), filled in reading order."""
        parts = sp['parts']
        cells = largest_remainder([p['value'] for p in parts], 100)
        cols = [MUTE if p.get('muted') else p.get('color', PALETTE[i % len(PALETTE)]) for i, p in enumerate(parts)]
        seq = [c for c, k in zip(cols, cells) for _ in range(k)]
        side = min(h, w * 0.5)
        g = side / 10
        rows = [[''] * 10 for _ in range(10)]
        fills = [[seq[r * 10 + c] if r * 10 + c < len(seq) else 'F3F2EE' for c in range(10)] for r in range(10)]
        bd = {k: (PAPER, 2.0) for k in 'LRTB'}
        styles = [[dict(size=2, borders=bd) for _ in range(10)] for _ in range(10)]
        gf = self._ntable(s, x, y, side, rows, [g] * 10, [g] * 10, fills, styles, 'Waffle (table)')
        cv = _Cv(self, s, x, y, w, h, 'Waffle key')
        lx, ly = x + side + 0.4, y + 0.05
        fv = sp.get('fmt')
        lab = lambda q: fv(q['value']) if fv else self.num(q['value'], q.get('dec', sp.get('dec', 0))) + sp.get('unit', '%')
        vw = max(self.tw(lab(q), 18, 'ui_semi') for q in parts)
        for i, p in enumerate(parts):
            cv.rrect(lx, ly + 0.09, 0.16, 0.16, cols[i], r=0.03)
            cv.label(lx + 0.28, ly + 0.17, lab(p), 18, 'ui_semi', INK)
            nl = self.nlines(p['name'], 11, 'ui', x + w - (lx + 0.45 + vw))
            self._tx(cv.s, lx + 0.45 + vw, ly + 0.07, x + w - (lx + 0.45 + vw), self.lh(11, 1.1, nl) + 0.04, p['name'],
                     11, INK2, 'ui', line=1.1)
            ly += max(0.58, self.lh(11, 1.1, nl) + 0.3)
        if sp.get('caption'):
            cv.label(lx, y + side - 0.1, sp['caption'], 9.5, 'ui', INK3)
        return gf

    # ── treemap (squarified shapes; numbers in the notes) ──
    def _c_treemap(self, cv, sp):
        items = sorted(sp['items'], key=lambda it: -it['value'])
        dec, unit = sp.get('dec', 0), sp.get('unit', '')
        groups = sp.get('groups')
        top = cv.y
        gcol = {}
        if groups:
            for i, g in enumerate(groups):
                gcol[g['name']] = g.get('color', PALETTE[i % len(PALETTE)])
            top += self._legend(cv, [(g['name'], gcol[g['name']], 'rect') for g in groups])
        tot = sum(it['value'] for it in items)
        rects = squarify([it['value'] for it in items], cv.x, top, cv.w, cv.y1 - top)
        for it, (rx, ry, rw, rh) in zip(items, rects):
            c = it.get('color') or gcol.get(it.get('group'), BLUE)
            if it.get('muted'):
                c = MUTE
            cv.rect(rx, ry, rw, rh, c, line=cv.bg, lw=1.5)
            share = it['value'] / tot * 100
            t1, t2 = it.get('short', it['name']), self.num(share, sp.get('share_dec', 1)) + '%'
            tc = on(c)
            if rw > self.tw(t1, 10, 'ui_semi') + 0.14 and rh > 0.42:
                cv.label(rx + 0.07, ry + 0.07, t1, 10, 'ui_semi', tc, va='t')
                cv.label(rx + 0.07, ry + 0.27, t2, 9.5, 'ui', tc, va='t')
            elif rw > self.tw(t2, 8.5, 'ui') + 0.08 and rh > 0.22:
                cv.label(rx + 0.04, ry + 0.04, t2, 8.5, 'ui', tc, va='t')


class Deck(_Native):
    def __init__(self, lang='vi', safe_fonts=False, embed_fonts=True, editable=True, brand='Vietnam Dashboard',
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
        self._catalog = []
        self._chapter = ''
        self._catalog_slide = None
        self.last_r = None

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

    # ── slide chrome (12-column grid; the same positions on every slide) ──
    def _slide(self, bg=PAPER):
        s = self.prs.slides.add_slide(self.prs.slide_layouts[6])
        s.background.fill.solid()
        s.background.fill.fore_color.rgb = rgb(bg)
        self.n += 1
        s._bg = bg
        self._cur = s
        return s

    def _rule(self, s, y, color=RULE, w=0.75, x0=MX, x1=SW - MX):
        r = _clean(s.shapes.add_connector(MSO_CONNECTOR.STRAIGHT, E(x0), E(y), E(x1), E(y)))
        _line(r, color, w, cap='flat')
        return r

    def balance(self, text, size, role, width):
        """Narrowest width that keeps the same number of lines (CSS text-wrap: balance), plus a little slack."""
        nl = self.nlines(text, size, role, width)
        if nl < 2:
            return min(width, self.tw(text, size, role) + 0.12)
        lo, hi = width * 0.4, width
        for _ in range(20):
            mid = (lo + hi) / 2
            if self.nlines(text, size, role, mid) <= nl:
                hi = mid
            else:
                lo = mid
        return min(width, hi * 1.04 + 0.1)

    def _head(self, s, kicker, headline, dek='', width=None):
        """Hairline, tracked kicker, Newsreader SemiBold headline (24 → 20 pt, ≤ 2 balanced lines, leading 1.08),
        optional dek. Returns the chart top, fixed at CT unless a long dek pushes it down."""
        width = width or HEAD_W
        self._rule(s, RULE_Y)
        self._tx(s.shapes, MX, KICK_Y, 10, 0.22, kicker, 11, RED, 'ui_semi', caps=True, spacing=1.3)
        size = 24
        while size > 20 and self.nlines(headline, size, 'head_semi', width) > 2:
            size -= 1
        nl = self.nlines(headline, size, 'head_semi', width)
        bw = self.balance(headline, size, 'head_semi', width)
        hh = self.lh(size, 1.08, nl)
        self._tx(s.shapes, MX, HEAD_Y, bw, hh + 0.06, headline, size, INK, 'head_semi', line=1.08)
        y = HEAD_Y + hh + 0.08
        if dek:
            nd = self.nlines(dek, 13.5, 'ui', DEK_W)
            self._tx(s.shapes, MX, y, DEK_W, self.lh(13.5, 1.4, nd), dek, 13.5, INK2, 'ui', line=1.4)
            y += self.lh(13.5, 1.4, nd)
        return max(CT, y + 0.2)

    def _foot(self, s, source):
        self._rule(s, FOOT_Y, RULE, 0.5)
        if source:
            size, sw = 9, SW - 2 * MX - 2.4
            while size > 7.5 and self.nlines(source, size, 'ui', sw) > 1:
                size -= 0.5
            two = self.nlines(source, size, 'ui', sw) > 1
            self._tx(s.shapes, MX, FOOT_Y + 0.08, sw, 0.36, source, size, INK3, 'ui', line=1.1 if two else None)
        tb = self._tx(s.shapes, SW - MX - 2.2, FOOT_Y + 0.08, 2.2, 0.2,
                      [[(self.brand + '   ', {'color': INK3, 'role': 'ui'}), (str(self.n), {'color': INK, 'role': 'ui_semi'})]],
                      9, INK3, align='r')
        self._numbers.append(tb)

    def _note(self, s, x, y, w, note, size=13):
        """Two-line note: the finding (ink) then one caveat (grey), Plex 13 pt at 1.42 leading."""
        if not note:
            return 0
        lead, follow = (note if isinstance(note, (list, tuple)) else (note, None))
        paras = [{'text': lead, 'size': size, 'color': INK, 'role': 'ui', 'line': 1.42, 'after': 3}]
        if follow:
            paras.append({'text': follow, 'size': size, 'color': INK2, 'role': 'ui', 'line': 1.42})
        h = self._note_h(note, w, size)
        self._tx(s.shapes, x, y, w, h, paras)
        return h

    def _note_h(self, note, w, size=13):
        if not note:
            return 0
        lead, follow = (note if isinstance(note, (list, tuple)) else (note, None))
        h = self.lh(size, 1.42, self.nlines(lead, size, 'ui', w)) + 3 / 72
        if follow:
            h += self.lh(size, 1.42, self.nlines(follow, size, 'ui', w))
        return h + 0.03

    def _panel(self, s, x, y, w, h, fill=SURF):
        p = _clean(s.shapes.add_shape(MSO_SHAPE.RECTANGLE, E(x), E(y), E(w), E(h)))
        _fill(p, fill)
        _line(p, None)
        return p

    def _notes(self, s, text):
        """Speaker notes: the diagram's source numbers, so they can be updated by hand."""
        if text:
            s.notes_slide.notes_text_frame.text = text

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
            elif kind == 'dashkey':
                cv.rect(cx, cy - 0.025, kw, 0.05, color)
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
    DIAGRAMS = ('tilemap', 'flow', 'levers', 'timeline', 'treemap')         # no PowerPoint chart type: editable shapes
    TABLES = ('heatmap', 'calendar', 'waffle')                              # native tables with cell fills

    def _chart(self, s, x, y, w, h, spec, bg=None):
        t = spec['type']
        ed = spec.get('editable', self.editable)
        if t in self.DIAGRAMS or t == 'tiles' or (not ed and hasattr(self, '_c_' + t)):
            cv = _Cv(self, s, x, y, w, h, 'Chart · ' + t, bg or getattr(s, '_bg', PAPER))
            getattr(self, '_c_' + t)(cv, spec)
            if t in self.DIAGRAMS:
                self._notes(s, self._spec_notes(spec))
            return cv.grp
        return getattr(self, '_n_' + t)(s, x, y, w, h, spec)

    def how(self, spec):
        """How a spec is built: 'chart' (native chart + workbook), 'table' (native table), 'shapes' (editable
        shapes, numbers in the notes) or 'drawn' (shape-drawn, editable=False)."""
        t = spec['type']
        if t in self.DIAGRAMS:
            return 'shapes'
        if t == 'tiles':
            return 'shapes+chart'
        if not spec.get('editable', self.editable) and hasattr(self, '_c_' + t):
            return 'drawn'
        if t in self.TABLES:
            return 'table'
        if t == 'spark_table':
            return 'table+chart'
        return 'chart'

    def _spec_notes(self, sp):
        t = sp['type']
        L = [f'Số liệu nguồn của sơ đồ ({t}) — sửa ở đây rồi sửa hình tương ứng:' if self.lang == 'vi'
             else f'Source numbers of this diagram ({t}) — edit here, then edit the shapes:']
        f = lambda v: self.num(v, sp.get('dec', 1)) + sp.get('unit', '')
        if t == 'flow':
            names = {nd['id']: nd['name'] for col in sp['columns'] for nd in col}
            L += [f'{names[a]} → {names[b]}: {f(v)}' for a, b, v in sp['links']]
        elif t == 'levers':
            names = {it['id']: it['name'] for col in sp['columns'] for it in col['items']}
            for col in sp['columns']:
                L.append(col['title'] + ': ' + '; '.join(it['name'] for it in col['items']))
            L += [f'{names[a]} → {names[b]}' for a, b in sp['links']]
        elif t == 'timeline':
            L += [f"{e['date']} · {e['title']} — {e.get('text', '')}" + (' (sắp tới)' if e.get('future') else '')
                  for e in sp['events']]
        elif t == 'tilemap':
            L += [f"{q['name']}: {f(q['value']) if q.get('value') is not None else '—'}" for q in sp['tiles']]
        elif t == 'treemap':
            tot = sum(it['value'] for it in sp['items'])
            L += [f"{it['name']}: {f(it['value'])} ({self.num(it['value'] / tot * 100, 1)}%)"
                  for it in sorted(sp['items'], key=lambda q: -q['value'])]
        return '\n'.join(L)

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
                if sp.get('editable', self.editable):
                    self._nmini(cv.slide, px, yy + 0.08, iw - 0.5, 0.55, k.get('spark_cats') or list(range(len(sv))), sv,
                                k.get('spark_kind', 'line'), False, k.get('spark_color', BLUE), k.get('dec', 1),
                                k['label'], markers=False)
                else:
                    self._mini(cv, px, yy + 0.08, iw - 0.5, 0.55, list(range(len(sv))), sv, k.get('spark_kind', 'line'),
                               False, k.get('spark_color', BLUE), k.get('dec', 1), axis=False)
                yy += 0.75
            if k.get('sub'):
                nl = self.nlines(k['sub'], 9.5, 'ui', iw)
                self._tx(cv.s, px, y + th_ - 0.18 - nl * 0.17, iw, nl * 0.17 + 0.04, k['sub'], 9.5, INK3, 'ui', line=1.0)

    # ── slide types ──
    def _register(self, method, how):
        if method:
            self._catalog.append((self._chapter, method, self.n, how))

    def cover(self, kicker, title, subtitle='', date='', chart=None, note=''):
        s = self._slide(SURF)
        self._rule(s, 0.6, INK, 1.25)
        self._tx(s.shapes, MX, 0.78, 8, 0.3, self.brand, 11, INK, 'ui_semi', caps=True, spacing=1.6)
        self._tx(s.shapes, SW - MX - 4, 0.78, 4, 0.3, date, 11, INK2, 'ui', align='r')
        tw = gw(7) if chart else gw(10)
        self._tx(s.shapes, MX, 2.0, tw, 0.3, kicker, 12, RED, 'ui_semi', caps=True, spacing=1.6)
        size = 44
        while size > 32 and self.nlines(title, size, 'head_semi', tw) > 3:
            size -= 2
        nl = self.nlines(title, size, 'head_semi', tw)
        bw = self.balance(title, size, 'head_semi', tw)
        self._tx(s.shapes, MX, 2.42, bw, self.lh(size, 1.04, nl) + 0.1, title, size, INK, 'head_semi', line=1.04)
        y = 2.42 + self.lh(size, 1.04, nl) + 0.3
        if subtitle:
            ns = self.nlines(subtitle, 16, 'ui', min(tw, gw(6)))
            self._tx(s.shapes, MX, y, min(tw, gw(6)), self.lh(16, 1.4, ns), subtitle, 16, INK2, 'ui', line=1.4)
        if chart:
            self._chart(s, gx(8), 2.0, gw(4), 3.6, chart, bg=SURF)
        self._rule(s, SH - 0.95, INK, 0.75)
        if note:
            self._tx(s.shapes, MX, SH - 0.82, SW - 2 * MX, 0.4, note, 10, INK2, 'ui')
        return s

    def catalog(self, kicker, headline, dek=''):
        """Index slide: filled at save() with every slide registered through `method=` (name, slide, how built)."""
        s = self._slide()
        self._catalog_slide = (s, kicker, headline, dek, self.n)
        return s

    def _fill_catalog(self):
        if not getattr(self, '_catalog_slide', None):
            return
        s, kicker, headline, dek, num = self._catalog_slide
        self.n, n_save = num, self.n
        top = self._head(s, kicker, headline, dek)
        HOW = ({'chart': 'biểu đồ', 'table': 'bảng', 'table+chart': 'bảng+bđ', 'shapes': 'hình',
                'shapes+chart': 'hình+bđ', 'drawn': 'hình vẽ', 'text': 'chữ'} if self.lang == 'vi' else
               {'chart': 'chart', 'table': 'table', 'table+chart': 'table+ch', 'shapes': 'shapes',
                'shapes+chart': 'shapes+ch', 'drawn': 'drawn', 'text': 'text'})
        lines = []
        last = object()
        for ch, m, sn, how in self._catalog:
            if ch != last:
                lines.append(('h', ch or '', None, None))
                last = ch
            lines.append(('e', m, sn, HOW.get(how, how)))
        ncol = 3
        per = math.ceil(len(lines) / ncol) + 1
        cols, cur, chap = [], [], ''
        for ln in lines:
            if ln[0] == 'h':
                chap = ln[1]
                if len(cur) >= per - 2:              # never leave a heading alone at the foot of a column
                    cols.append(cur)
                    cur = []
            elif len(cur) >= per:
                cols.append(cur)
                cur = [('h', chap + (' (tiếp)' if self.lang == 'vi' else ' (cont.)'), None, None)]
            cur.append(ln)
        cols.append(cur)
        cw = (SW - 2 * MX - GUT * (len(cols) - 1)) / len(cols)
        rowh = min(0.26, (FOOT_Y - 0.2 - top) / max(len(c) for c in cols))
        for ci, col in enumerate(cols):
            x = MX + ci * (cw + GUT)
            y = top
            for kind, a, sn, how in col:
                if kind == 'h':
                    if y > top:
                        y += 0.07
                    self._tx(s.shapes, x, y, cw, rowh, a, 9, RED, 'ui_semi', caps=True, spacing=0.9, anchor='m')
                    self._rule(s, y + rowh - 0.02, RULE, 0.5, x, x + cw)
                else:
                    self._tx(s.shapes, x, y, 0.32, rowh, f'{sn}', 10.5, INK, 'ui_semi', anchor='m')
                    self._tx(s.shapes, x + 0.34, y, cw - 0.34 - 0.7, rowh, a, 10.5, INK, 'ui', anchor='m', wrap=False)
                    self._tx(s.shapes, x + cw - 0.7, y, 0.7, rowh, how, 9, INK3, 'ui', align='r', anchor='m')
                y += rowh
        self._foot(s, 'Biểu đồ = biểu đồ PowerPoint gốc (Edit Data); bảng = bảng gốc; bđ = kèm biểu đồ gốc nhỏ; hình = nhóm '
                      'hình sửa được, số liệu trong ghi chú trang.' if self.lang == 'vi' else
                   'Number = slide. "chart": native PowerPoint chart with embedded workbook (Edit Data); "table": native '
                   'table; "ch": with small native charts; "shapes": editable grouped shapes, numbers in the slide notes.')
        self.n = n_save

    def section(self, number, title, dek='', items=()):
        s = self._slide(SURF)
        self._chapter = title
        self._rule(s, 0.6, INK, 1.25)
        num = f'{number:02d}' if isinstance(number, int) else str(number)
        self._tx(s.shapes, MX, 1.55, 4, 1.6, num, 110, RED, 'head_semi', line=0.9)
        tw = gw(7)
        nl = self.nlines(title, 36, 'head_semi', tw)
        self._tx(s.shapes, MX, 3.4, self.balance(title, 36, 'head_semi', tw), self.lh(36, 1.05, nl) + 0.1, title, 36,
                 INK, 'head_semi', line=1.05)
        if dek:
            nd = self.nlines(dek, 15, 'ui', gw(6))
            self._tx(s.shapes, MX, 3.4 + self.lh(36, 1.05, nl) + 0.2, gw(6), self.lh(15, 1.42, nd), dek, 15, INK2, 'ui',
                     line=1.42)
        if items:
            x = gx(8)
            self._tx(s.shapes, x, 1.7, SW - MX - x, 0.3, self.T['in_chapter'], 10, INK2, 'ui_semi', caps=True,
                     spacing=1.2)
            y = 2.1
            for it in items:
                self._rule(s, y, RULE, 0.75, x, SW - MX)
                nl2 = self.nlines(it, 12.5, 'ui', SW - MX - x)
                self._tx(s.shapes, x, y + 0.09, SW - MX - x, self.lh(12.5, 1.25, nl2) + 0.05, it, 12.5, INK, 'ui',
                         line=1.25)
                y += self.lh(12.5, 1.25, nl2) + 0.24
        self._foot(s, '')
        return s

    def hero(self, kicker, headline, big, unit='', dek='', counts=(), chart=None, chart_title='', source='', note=None,
             method=None):
        s = self._slide()
        top = self._head(s, kicker, headline)
        lw = gw(6)
        size = 88
        while size > 56 and self.tw(big, size, 'ui_bold') + self.tw(' ' + unit, 24, 'ui_semi') > lw:
            size -= 4
        self._tx(s.shapes, MX, top - 0.12, lw, size / 72 * 1.05, [[(big, {'role': 'ui_bold', 'size': size}),
                                                                  (' ' + unit, {'role': 'ui_semi', 'size': 24,
                                                                                'color': INK2})]], line=0.9)
        y = top - 0.02 + size / 72 * 1.05
        if dek:
            nd = self.nlines(dek, 14, 'ui', lw - 0.3)
            self._tx(s.shapes, MX, y, lw - 0.3, self.lh(14, 1.45, nd), dek, 14, INK2, 'ui', line=1.45)
            y += self.lh(14, 1.45, nd) + 0.3
        if counts:
            cv = _Cv(self, s, MX, y, lw, 1.1, 'Counters')
            n = len(counts)
            cw = lw / n
            mxv = max(abs(c[2]) for c in counts if len(c) > 2) if any(len(c) > 2 for c in counts) else None
            for i, c in enumerate(counts):
                x = MX + i * cw
                cv.seg(x, y, x + cw - 0.2, y, INK, 1.25, cap='flat')
                cv.label(x, y + 0.08, c[0], 9.5, 'ui_semi', INK2, va='t', caps=True, spacing=0.8)
                cv.label(x, y + 0.5, c[1], 22, 'ui_semi', INK)
                if len(c) > 2 and mxv:
                    cv.rect(x, y + 0.8, cw - 0.3, 0.07, 'EEECE6')
                    cv.bar(x, y + 0.8, x + (cw - 0.3) * c[2] / mxv, y + 0.87, BLUE if i == n - 1 else MUTE, 'r', r=0.035)
        if chart:
            px = gx(6) + 0.1
            pw = SW - MX - px
            ph = FOOT_Y - 0.25 - top
            self._panel(s, px, top, pw, ph)
            if chart_title:
                self._tx(s.shapes, px + 0.3, top + 0.22, pw - 0.6, 0.3, chart_title, 11, INK, 'ui_semi')
            self._chart(s, px + 0.3, top + 0.6, pw - 0.6, ph - 0.85, chart, bg=SURF)
        if note:
            self._note(s, MX, FOOT_Y - 0.2 - self._note_h(note, lw - 0.2), lw - 0.2, note)
        self._foot(s, source)
        self._register(method, 'text' if not chart else self.how(chart))
        return s

    def kpis(self, kicker, headline, tiles, note=None, source='', method=None):
        return self.chart(kicker, headline, {'type': 'tiles', 'tiles': tiles}, note=note, source=source, method=method)

    def chart(self, kicker, headline, spec, note=None, source='', dek='', panel=None, chart_title='', method=None,
              notes=None):
        s = self._slide()
        top = self._head(s, kicker, headline, dek)
        if panel:
            pw = gw(4)
            cw = SW - 2 * MX - pw - GUT * 2
            if chart_title:
                self._tx(s.shapes, MX, top, cw, 0.3, chart_title, 11, INK, 'ui_semi')
                top += 0.36
            self._chart(s, MX, top, cw, FOOT_Y - 0.25 - top, spec)
            px = SW - MX - pw
            pt = top - (0.36 if chart_title else 0)
            self._panel(s, px, pt, pw, FOOT_Y - 0.25 - pt)
            self._side(s, px + 0.28, pt + 0.28, pw - 0.56, panel, note)
        else:
            nh = self._note_h(note, NOTE_W)
            bottom = FOOT_Y - 0.2 - (nh + 0.22 if note else 0.05)
            if chart_title:
                self._tx(s.shapes, MX, top, SW - 2 * MX, 0.3, chart_title, 11, INK, 'ui_semi')
                top += 0.36
            self._chart(s, MX, top, SW - 2 * MX, bottom - top, spec)
            if note:
                self._note(s, MX, FOOT_Y - 0.16 - nh, NOTE_W, note)
        self._foot(s, source)
        self._notes(s, notes)
        self._register(method, self.how(spec))
        return s

    def _side(self, s, x, y, w, panel, note):
        if panel.get('label'):
            self._tx(s.shapes, x, y, w, 0.25, panel['label'], 9.5, INK2, 'ui_semi', caps=True, spacing=1.0)
            y += 0.32
        if panel.get('value'):
            size = 38
            while size > 24 and self.tw(panel['value'], size, 'ui_bold') + self.tw(' ' + panel.get('unit', ''), 14,
                                                                                     'ui_semi') > w:
                size -= 2
            self._tx(s.shapes, x, y, w, size / 72 * 1.1, [[(panel['value'], {'role': 'ui_bold', 'size': size}),
                                                           (' ' + panel.get('unit', ''), {'role': 'ui_semi', 'size': 14,
                                                                                          'color': INK2})]], line=0.9)
            y += size / 72 * 1.1 + 0.1
        if panel.get('sub'):
            n = self.nlines(panel['sub'], 10.5, 'ui', w)
            self._tx(s.shapes, x, y, w, self.lh(10.5, 1.35, n), panel['sub'], 10.5, INK2, 'ui', line=1.35)
            y += self.lh(10.5, 1.35, n) + 0.15
        self._rule(s, y + 0.05, RULE, 0.75, x, x + w)
        y += 0.22
        if note:
            lead, follow = (note if isinstance(note, (list, tuple)) else (note, None))
            n1 = self.nlines(lead, 13, 'ui', w)
            self._tx(s.shapes, x, y, w, self.lh(13, 1.42, n1), lead, 13, INK, 'ui', line=1.42)
            y += self.lh(13, 1.42, n1) + 0.12
            if follow:
                n2 = self.nlines(follow, 12, 'ui', w)
                self._tx(s.shapes, x, y, w, self.lh(12, 1.42, n2), follow, 12, INK2, 'ui', line=1.42)
                y += self.lh(12, 1.42, n2) + 0.1
        for b in panel.get('bullets', []):
            n = self.nlines(b, 11, 'ui', w - 0.2)
            cv = _Cv(self, s, x, y, w, 0.1, 'Bullet', SURF)
            cv.rect(x, y + 0.08, 0.07, 0.07, INK)
            self._tx(s.shapes, x + 0.2, y, w - 0.2, self.lh(11, 1.3, n), b, 11, INK, 'ui', line=1.3)
            y += self.lh(11, 1.3, n) + 0.1

    def two_charts(self, kicker, headline, left, right, titles=('', ''), note=None, source='', dek='', method=None):
        s = self._slide()
        top = self._head(s, kicker, headline, dek)
        cw = gw(6)
        nh = self._note_h(note, NOTE_W)
        bottom = FOOT_Y - 0.2 - (nh + 0.22 if note else 0.05)
        for i, (sp, tt) in enumerate(zip((left, right), titles)):
            x = gx(6 * i)
            t2 = top
            if tt:
                self._tx(s.shapes, x, top, cw, 0.3, tt, 11.5, INK, 'ui_semi')
                t2 += 0.42
            self._chart(s, x, t2, cw, bottom - t2, sp)
        if note:
            self._note(s, MX, FOOT_Y - 0.16 - nh, NOTE_W, note)
        self._foot(s, source)
        self._register(method, self.how(left))
        return s

    def table(self, kicker, headline, header, rows, source='', col_widths=None, number_cols=(), note=None, method=None):
        s = self._slide()
        top = self._head(s, kicker, headline)
        n_r, n_c = len(rows) + 1, len(header)
        nh = self._note_h(note, NOTE_W)
        avail = FOOT_Y - 0.3 - top - (nh + 0.25 if note else 0)
        rh = min(0.34, avail / n_r)
        cw = col_widths or [(SW - 2 * MX) / n_c] * n_c
        body = []
        styles = []
        fills = []
        for i in range(n_r):
            row, st, fl = [], [], []
            for j in range(n_c):
                v = header[j] if i == 0 else rows[i - 1][j]
                if isinstance(v, (int, float)) and not isinstance(v, bool) and i:
                    v = self.num(v, 1) if isinstance(v, float) else str(v)
                row.append('—' if v is None else str(v))
                al = 'r' if j in number_cols else 'l'
                if i == 0:
                    st.append(dict(size=9, role='ui_semi', color=INK2, caps=True, align=al, pad=0.1,
                                   borders={'T': (INK, 1.0), 'B': (INK, 0.75)}))
                else:
                    st.append(dict(size=10.5, role='ui_med' if j == 0 else 'ui', color=INK3 if v is None else INK,
                                   align=al, pad=0.1, borders={'B': (HAIR, 0.5)}))
                fl.append(None if i == 0 or i % 2 else ZEBRA)
            body.append(row)
            styles.append(st)
            fills.append(fl)
        self._ntable(s, MX, top, sum(cw), body, cw, rh, fills, styles, 'Table')
        if note:
            self._note(s, MX, FOOT_Y - 0.16 - nh, NOTE_W, note)
        self._foot(s, source)
        self._register(method, 'table')
        return s

    def watch(self, kicker, headline, items, source='', note=None, method=None):
        s = self._slide()
        top = self._head(s, kicker, headline)
        n = len(items)
        cw = (SW - 2 * MX - GUT * 2 * (n - 1)) / n
        y0 = top + 0.1
        for i, it in enumerate(items):
            d, t, x = it[:3]
            cx = MX + i * (cw + GUT * 2)
            cv = _Cv(self, s, cx, y0, cw, 3.5, 'Watch item')
            cv.seg(cx, y0, cx + cw, y0, INK, 2.0, cap='flat')
            cv.label(cx, y0 + 0.28, d, 12, 'ui_semi', RED, spacing=0.5)
            nt = self.nlines(t, 16, 'ui_semi', cw)
            self._tx(s.shapes, cx, y0 + 0.5, cw, self.lh(16, 1.25, nt), t, 16, INK, 'ui_semi', line=1.25)
            nx = self.nlines(x, 13, 'ui', cw)
            self._tx(s.shapes, cx, y0 + 0.5 + self.lh(16, 1.25, nt) + 0.14, cw, self.lh(13, 1.45, nx), x, 13, INK2, 'ui',
                     line=1.45)
            if len(it) > 3 and it[3]:
                yb = FOOT_Y - 0.3 - 0.85
                cv.seg(cx, yb, cx + cw, yb, RULE, 0.75, cap='flat')
                big, small = (it[3] if isinstance(it[3], (list, tuple)) else (it[3], ''))
                cv.runs(cx, yb + 0.45, [(big, {'role': 'ui_bold', 'size': 30}),
                                        ('  ' + small, {'role': 'ui', 'size': 11, 'color': INK2})])
        self._foot(s, source)
        self._register(method, 'text')
        return s

    def quote(self, kicker, quote, who, role='', note=None, source='', facts=(), method=None):
        s = self._slide()
        self._rule(s, RULE_Y)
        self._tx(s.shapes, MX, KICK_Y, 10, 0.22, kicker, 11, RED, 'ui_semi', caps=True, spacing=1.3)
        qw = gw(8) if facts else gw(11)
        self._panel(s, MX, HEAD_Y + 0.1, qw, FOOT_Y - 0.3 - HEAD_Y - 0.1)
        bar = _clean(s.shapes.add_shape(MSO_SHAPE.RECTANGLE, E(MX), E(HEAD_Y + 0.1), E(0.05), E(FOOT_Y - 0.4 - HEAD_Y)))
        _fill(bar, INK)
        _line(bar, None)
        self._tx(s.shapes, MX + 0.4, HEAD_Y + 0.05, 1.2, 1.2, '“', 90, RED, 'head_semi', line=0.8)
        iw = qw - 1.0
        size = 22
        while size > 15 and self.lh(size, 1.3, self.nlines(quote, size, 'head_med', iw)) > 3.1:
            size -= 1
        nl = self.nlines(quote, size, 'head_med', iw)
        y = HEAD_Y + 1.0
        self._tx(s.shapes, MX + 0.5, y, iw, self.lh(size, 1.3, nl) + 0.1, quote, size, INK, 'head_med', line=1.3)
        y += self.lh(size, 1.3, nl) + 0.3
        self._tx(s.shapes, MX + 0.5, y, iw, 0.5, [[('— ' + who, {'role': 'ui_semi', 'size': 12, 'color': INK})],
                                                  [(role, {'role': 'ui', 'size': 10.5, 'color': INK2})]], line=1.3)
        if facts:
            x = gx(9)
            w = SW - MX - x
            y = HEAD_Y + 0.25
            for t, v, sub in facts:
                c = _clean(s.shapes.add_connector(MSO_CONNECTOR.STRAIGHT, E(x), E(y), E(x), E(y + 1.0)))
                _line(c, INK, 2.5, cap='flat')
                self._tx(s.shapes, x + 0.2, y, w - 0.2, 0.25, t, 9.5, INK2, 'ui_semi', caps=True, spacing=0.8)
                self._tx(s.shapes, x + 0.2, y + 0.27, w - 0.2, 0.5, v, 24, INK, 'ui_bold', line=0.9)
                self._tx(s.shapes, x + 0.2, y + 0.73, w - 0.2, 0.3, sub, 9.5, INK3, 'ui')
                y += 1.35
        if note:
            self._note(s, MX + 0.5, FOOT_Y - 0.55 - self._note_h(note, iw, 12), iw, note, size=12)
        self._foot(s, source)
        self._register(method, 'text')
        return s

    def sources(self, kicker, headline, items, method=(), source='', method_name=None):
        """items: [(publisher, what, period, url)]; method: list of short lines."""
        s = self._slide()
        top = self._head(s, kicker, headline)
        mw = gw(4) if method else 0
        lw = SW - 2 * MX - (mw + GUT * 2 if method else 0)
        colw = (lw - GUT * 2) / 2
        half = math.ceil(len(items) / 2)
        for ci in range(2):
            y = top
            x = MX + ci * (colw + GUT * 2)
            for k, it in enumerate(items[ci * half:(ci + 1) * half]):
                pub, what, period, url = (list(it) + ['', '', '', ''])[:4]
                self._rule(s, y, RULE, 0.75, x, x + colw)
                nw = self.nlines(what, 10.5, 'ui', colw - 0.05)
                paras = [[(f'{ci * half + k + 1:>2}  ', {'role': 'ui_semi', 'color': RED, 'size': 10}),
                          (pub, {'role': 'ui_semi', 'size': 11, 'color': INK})],
                         {'text': what, 'size': 10.5, 'color': INK2, 'role': 'ui', 'line': 1.25}]
                meta = ' · '.join(q for q in (period, url) if q)
                if meta:
                    paras.append({'text': meta, 'size': 9, 'color': INK3, 'role': 'ui', 'before': 1})
                h = self.lh(11) + self.lh(10.5, 1.25, nw) + (self.lh(9) + 0.02 if meta else 0) + 0.04
                self._tx(s.shapes, x, y + 0.07, colw, h, paras)
                y += h + 0.16
        if method:
            px = SW - MX - mw
            self._panel(s, px, top, mw, FOOT_Y - 0.25 - top)
            self._tx(s.shapes, px + 0.25, top + 0.22, mw - 0.5, 0.25, self.T['method'], 9.5, INK2, 'ui_semi', caps=True,
                     spacing=1.0)
            y = top + 0.58
            for m in method:
                n = self.nlines(m, 10.5, 'ui', mw - 0.7)
                b = _clean(s.shapes.add_shape(MSO_SHAPE.RECTANGLE, E(px + 0.25), E(y + 0.08), E(0.07), E(0.07)))
                _fill(b, INK)
                _line(b, None)
                self._tx(s.shapes, px + 0.45, y, mw - 0.7, self.lh(10.5, 1.3, n), m, 10.5, INK, 'ui', line=1.3)
                y += self.lh(10.5, 1.3, n) + 0.12
        self._foot(s, source)
        self._register(method_name, 'text')
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
        self._fill_catalog()
        self.embedded = self._embed_fonts() if self.embed else 0
        self.prs.save(path)
        return path
