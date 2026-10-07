"""vn_deck — build PowerPoint decks in the Vietnam Dashboard's editorial data-story style (python-pptx >= 0.6).

Every chart is a native, editable PowerPoint chart (no pictures of charts). One finding per slide: red kicker,
serif headline that states the finding, the chart, a two-line note (finding, then one caveat), and a source line.

    from vn_deck import Deck
    d = Deck(lang='vi')                                    # 'vi' or 'en' — one language per deck
    d.title('BÁO CÁO KINH TẾ', 'Lãi suất huy động: vì sao khó giảm', 'Cập nhật 7/10/2026')
    d.hero('TỔNG QUAN', 'Lãi suất 12 tháng lên 5,9%', '5,9', '%/năm', 'Tăng 1,3 điểm % trong 12 tháng.',
           source='Nguồn: Vietcombank, NHNN.')
    d.chart('CHƯƠNG 1 · LÃI SUẤT', 'Lãi suất tăng trở lại từ 2025',
            {'type': 'line', 'categories': ['2021', '2022', '2023', '2024', '2025', '2026'],
             'series': [{'name': 'Tiết kiệm 12 tháng', 'values': [5.6, 5.5, 6.8, 4.7, 4.6, 5.9]},
                        {'name': 'Tái cấp vốn', 'values': [4.0, 6.0, 4.5, 4.5, 4.5, 4.5], 'muted': True}],
             'number_format': '0.0', 'y_title': '%/năm'},
            note=('Lãi suất 12 tháng 5,9% (9/2026), cao nhất từ 2023.', 'Lãi suất điều hành không đổi.'),
            source='Nguồn: Vietcombank; NHNN.')
    d.save('deck.pptx')

Chart types: line, bar (columns), barh (ranked bars, sorted by you), stacked, stacked_h, waffle (parts of 100%),
bignumbers (2–4 KPI tiles). Series options: color, muted (grey), dashed (estimate/plan/projection), values with None
for gaps (never 0). Use d.table() for data appendices and d.watch() for the closing "what to watch" slide.
"""
from pptx import Presentation
from pptx.chart.data import CategoryChartData
from pptx.dml.color import RGBColor
from pptx.enum.chart import XL_CHART_TYPE, XL_LEGEND_POSITION, XL_TICK_LABEL_POSITION
from pptx.enum.dml import MSO_LINE_DASH_STYLE
from pptx.enum.shapes import MSO_SHAPE
from pptx.enum.text import MSO_ANCHOR, PP_ALIGN
from pptx.util import Emu, Inches, Pt

# ── tokens (same values as the dashboard) ──
INK, INK2, INK3 = '16181D', '5B6170', '8A8F99'
RED, PAPER, SURF, MUTE, RULE, HAIR = 'C2362F', 'FFFFFF', 'FAF8F3', 'C9CCD2', 'DEDBD2', 'E1E0D9'
PALETTE = ['2A78D6', 'EB6834', '1BAF7A', 'EDA100', '4A3AA7', 'E34948', '008300']
FONT_HEAD, FONT_UI = 'Newsreader', 'IBM Plex Sans'          # Google Fonts; set safe_fonts=True → Georgia / Arial
W, H = Inches(13.333), Inches(7.5)
MX = Inches(0.6)                                             # side margin


def rgb(h): return RGBColor.from_string(h)


class Deck:
    def __init__(self, lang='vi', safe_fonts=False, template=None):
        self.lang = lang
        self.fh, self.fu = ('Georgia', 'Arial') if safe_fonts else (FONT_HEAD, FONT_UI)
        self.prs = Presentation(template) if template else Presentation()
        self.prs.slide_width, self.prs.slide_height = W, H
        self.n = 0

    # ── primitives ──
    def _slide(self, bg=PAPER):
        s = self.prs.slides.add_slide(self.prs.slide_layouts[6])
        s.background.fill.solid(); s.background.fill.fore_color.rgb = rgb(bg)
        self.n += 1
        return s

    def _text(self, s, x, y, w, h, text, size=14, color=INK, bold=False, font=None, align=PP_ALIGN.LEFT,
              anchor=MSO_ANCHOR.TOP, spacing=None, caps=False, line=1.15):
        tb = s.shapes.add_textbox(x, y, w, h); tf = tb.text_frame; tf.word_wrap = True
        tf.margin_left = tf.margin_right = tf.margin_top = tf.margin_bottom = 0; tf.vertical_anchor = anchor
        parts = text if isinstance(text, list) else [text]
        for i, t in enumerate(parts):
            p = tf.paragraphs[0] if i == 0 else tf.add_paragraph()
            p.alignment = align; p.line_spacing = line
            r = p.add_run(); r.text = t.upper() if caps else t
            f = r.font; f.size = Pt(size); f.bold = bold; f.color.rgb = rgb(color); f.name = font or self.fu
            if spacing is not None:
                rPr = r._r.get_or_add_rPr(); rPr.set('spc', str(int(spacing * 100)))
        return tb

    def _rule(self, s, y, color=INK, weight=3):
        ln = s.shapes.add_connector(1, MX, y, W - MX, y); ln.line.color.rgb = rgb(color); ln.line.width = Pt(weight)

    def _head(self, s, kicker, headline):
        self._rule(s, Inches(0.42))
        self._text(s, MX, Inches(0.62), Inches(9), Inches(0.3), kicker, 11, RED, True, caps=True, spacing=1.5)
        self._text(s, MX, Inches(0.95), Inches(11.5), Inches(1.1), headline, 28, INK, True, self.fh, line=1.05)

    def _foot(self, s, source):
        y = H - Inches(0.55)
        ln = s.shapes.add_connector(1, MX, y, W - MX, y); ln.line.color.rgb = rgb(RULE); ln.line.width = Pt(0.5)
        if source: self._text(s, MX, y + Inches(0.08), Inches(10.8), Inches(0.35), source, 9, INK3)
        self._text(s, W - MX - Inches(1), y + Inches(0.08), Inches(1), Inches(0.3), str(self.n), 9, INK3,
                   align=PP_ALIGN.RIGHT)

    def _note(self, s, x, y, w, note):
        if not note: return
        lead, follow = (note if isinstance(note, (list, tuple)) else (note, None))
        tb = self._text(s, x, y, w, Inches(0.8), lead, 13, INK, line=1.25)
        if follow:
            p = tb.text_frame.add_paragraph(); p.line_spacing = 1.25
            r = p.add_run(); r.text = follow; r.font.size = Pt(12); r.font.color.rgb = rgb(INK2); r.font.name = self.fu

    # ── chart ──
    def _chart(self, s, x, y, w, h, spec):
        t = spec['type']
        if t == 'waffle': return self._waffle(s, x, y, w, h, spec)
        if t == 'bignumbers': return self._tiles(s, x, y, w, h, spec)
        kind = {'line': XL_CHART_TYPE.LINE_MARKERS if spec.get('markers') else XL_CHART_TYPE.LINE,
                'bar': XL_CHART_TYPE.COLUMN_CLUSTERED, 'barh': XL_CHART_TYPE.BAR_CLUSTERED,
                'stacked': XL_CHART_TYPE.COLUMN_STACKED, 'stacked_h': XL_CHART_TYPE.BAR_STACKED}[t]
        cd = CategoryChartData(); cd.categories = spec['categories']
        for se in spec['series']: cd.add_series(se['name'], se['values'], spec.get('number_format', 'General'))
        gf = s.shapes.add_chart(kind, x, y, w, h, cd); ch = gf.chart
        ch.font.size = Pt(11); ch.font.name = self.fu; ch.font.color.rgb = rgb(INK2)
        multi = len(spec['series']) > 1
        ch.has_title = False
        ch.has_legend = multi
        if multi:
            ch.legend.position = XL_LEGEND_POSITION.TOP; ch.legend.include_in_layout = False
            ch.legend.font.size = Pt(11); ch.legend.font.color.rgb = rgb(INK)
        va, ca = ch.value_axis, ch.category_axis
        va.has_major_gridlines = True; va.major_gridlines.format.line.color.rgb = rgb(HAIR)
        va.major_gridlines.format.line.width = Pt(0.75); va.format.line.fill.background()
        va.tick_labels.font.size = Pt(10); va.tick_labels.font.color.rgb = rgb(INK3)
        if spec.get('number_format'): va.tick_labels.number_format = spec['number_format']; va.tick_labels.number_format_is_linked = False
        if spec.get('y_min') is not None: va.minimum_scale = spec['y_min']
        if spec.get('y_max') is not None: va.maximum_scale = spec['y_max']
        ca.format.line.color.rgb = rgb(INK); ca.format.line.width = Pt(1); ca.has_major_gridlines = False
        ca.tick_labels.font.size = Pt(10); ca.tick_labels.font.color.rgb = rgb(INK2)
        ca.tick_label_position = XL_TICK_LABEL_POSITION.LOW
        if t in ('barh', 'stacked_h'): ca.reverse_order = True     # first category at the top, as listed
        if spec.get('y_title'):
            va.has_title = True; va.axis_title.text_frame.text = spec['y_title']
            f = va.axis_title.text_frame.paragraphs[0].runs[0].font; f.size = Pt(10); f.color.rgb = rgb(INK3); f.bold = False
        plot = ch.plots[0]
        if t in ('bar', 'barh', 'stacked', 'stacked_h'):
            plot.gap_width = spec.get('gap', 80 if t in ('bar', 'stacked') else 50)
            if t.startswith('stacked'): plot.overlap = 100
        hi = spec.get('highlight')                       # index of the one category to emphasise (bars)
        for i, (se, ps) in enumerate(zip(spec['series'], plot.series)):
            col = MUTE if se.get('muted') else se.get('color', PALETTE[i % len(PALETTE)])
            if t == 'line':
                ln = ps.format.line; ln.color.rgb = rgb(col); ln.width = Pt(se.get('width', 2.25))
                if se.get('dashed'): ln.dash_style = MSO_LINE_DASH_STYLE.DASH
                ps.smooth = False
            else:
                ps.format.fill.solid(); ps.format.fill.fore_color.rgb = rgb(col)
                ps.format.line.color.rgb = rgb(PAPER); ps.format.line.width = Pt(1 if t.startswith('stacked') else 0)
                if hi is not None and not multi:
                    for j in range(len(spec['categories'])):
                        pt = ps.points[j]; pt.format.fill.solid()
                        pt.format.fill.fore_color.rgb = rgb(col if j == hi else MUTE)
            if spec.get('labels'):
                ps.data_labels.show_value = True; ps.data_labels.font.size = Pt(10)
                ps.data_labels.font.color.rgb = rgb(INK); ps.data_labels.number_format = spec.get('number_format', 'General')
                ps.data_labels.number_format_is_linked = False
        return gf

    def _waffle(self, s, x, y, w, h, spec):
        """spec: parts=[{name, value, color?}] summing to ~100 (1 cell = 1%); legend at right."""
        parts = spec['parts']; cells = []
        for i, p in enumerate(parts):
            cells += [p.get('color', MUTE if p.get('muted') else PALETTE[i % len(PALETTE)])] * int(round(p['value']))
        cells = (cells + [HAIR] * 100)[:100]
        side = min(h, w * 0.55); g = side / 10; gap = int(g * 0.12)
        for k, c in enumerate(cells):
            r_, c_ = divmod(k, 10)
            sh = s.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, x + int(c_ * g), y + int(r_ * g), int(g) - gap, int(g) - gap)
            sh.adjustments[0] = 0.18; sh.fill.solid(); sh.fill.fore_color.rgb = rgb(c); sh.line.fill.background()
        lx, ly = x + int(side) + Inches(0.4), y
        for i, p in enumerate(parts):
            c = p.get('color', MUTE if p.get('muted') else PALETTE[i % len(PALETTE)])
            sq = s.shapes.add_shape(MSO_SHAPE.RECTANGLE, lx, ly + Inches(0.08), Inches(0.16), Inches(0.16))
            sq.fill.solid(); sq.fill.fore_color.rgb = rgb(c); sq.line.fill.background()
            self._text(s, lx + Inches(0.3), ly, Inches(0.8), Inches(0.35), spec.get('fmt', '{:.0f}').format(p['value']), 15, INK, True)
            self._text(s, lx + Inches(1.05), ly + Inches(0.03), max(Inches(2.2), w - int(side) - Inches(1.5)), Inches(0.5), p['name'], 12, INK2)
            ly += Inches(0.55)

    def _tiles(self, s, x, y, w, h, spec):
        """spec: tiles=[{label, value, unit, sub}] — 2 to 4 tiles."""
        t = spec['tiles']; n = len(t); gap = Inches(0.4); tw = int((w - gap * (n - 1)) / n)
        y = y + max(0, int((h - Inches(2.6)) / 2))       # centre the row of tiles in the space
        for i, k in enumerate(t):
            tx = x + i * (tw + gap)
            ln = s.shapes.add_connector(1, tx, y, tx + tw, y); ln.line.color.rgb = rgb(INK); ln.line.width = Pt(2)
            self._text(s, tx, y + Inches(0.15), tw, Inches(0.3), k['label'], 11, INK2, True, caps=True, spacing=1)
            tb = self._text(s, tx, y + Inches(0.5), tw, Inches(1.3), k['value'], 66, INK, True, self.fh)
            if k.get('unit'):
                r = tb.text_frame.paragraphs[0].add_run(); r.text = ' ' + k['unit']
                r.font.size = Pt(16); r.font.color.rgb = rgb(INK2); r.font.name = self.fu
            if k.get('sub'): self._text(s, tx, y + Inches(1.85), tw, Inches(0.7), k['sub'], 13, INK2)

    # ── slide types ──
    def title(self, kicker, title, subtitle='', date=''):
        s = self._slide(SURF)
        self._rule(s, Inches(2.2), INK, 4)
        self._text(s, MX, Inches(2.45), Inches(10), Inches(0.4), kicker, 13, RED, True, caps=True, spacing=2)
        self._text(s, MX, Inches(2.9), Inches(11.5), Inches(2), title, 44, INK, True, self.fh, line=1.02)
        if subtitle: self._text(s, MX, Inches(4.75), Inches(10), Inches(0.8), subtitle, 18, INK2)
        if date: self._text(s, MX, H - Inches(1.0), Inches(6), Inches(0.4), date, 12, INK3)
        return s

    def hero(self, kicker, headline, big, unit='', dek='', chart=None, source=''):
        s = self._slide(); self._head(s, kicker, headline)
        tb = self._text(s, MX, Inches(2.35), Inches(5.2), Inches(1.6), big, 96, INK, True, self.fh, line=0.95)
        if unit:
            r = tb.text_frame.paragraphs[0].add_run(); r.text = ' ' + unit
            r.font.size = Pt(24); r.font.color.rgb = rgb(INK2); r.font.name = self.fu
        if dek: self._text(s, MX, Inches(4.2), Inches(5), Inches(2), dek, 15, INK2, line=1.3)
        if chart: self._chart(s, Inches(6.2), Inches(2.2), W - Inches(6.2) - MX, Inches(4.4), chart)
        self._foot(s, source); return s

    def chart(self, kicker, headline, spec, note=None, source='', dek=''):
        s = self._slide(); self._head(s, kicker, headline)
        top = Inches(2.15)
        if dek: self._text(s, MX, top, Inches(11), Inches(0.5), dek, 14, INK2); top += Inches(0.5)
        ch_h = H - top - Inches(0.75) - (Inches(0.95) if note else 0)
        self._chart(s, MX, top, W - 2 * MX, ch_h, spec)
        if note: self._note(s, MX, top + ch_h + Inches(0.1), Inches(11.5), note)
        self._foot(s, source); return s

    def two_charts(self, kicker, headline, left, right, titles=('', ''), note=None, source=''):
        s = self._slide(); self._head(s, kicker, headline)
        top, gap = Inches(2.15), Inches(0.5); cw = int((W - 2 * MX - gap) / 2)
        ch_h = H - top - Inches(1.1) - (Inches(0.95) if note else 0)
        for i, (sp, tt) in enumerate(zip((left, right), titles)):
            x = MX + i * (cw + gap)
            if tt: self._text(s, x, top, cw, Inches(0.35), tt, 13, INK, True)
            self._chart(s, x, top + Inches(0.4), cw, ch_h, sp)
        if note: self._note(s, MX, top + ch_h + Inches(0.5), Inches(11.5), note)
        self._foot(s, source); return s

    def table(self, kicker, headline, header, rows, source='', col_widths=None, number_cols=()):
        s = self._slide(); self._head(s, kicker, headline)
        n_r, n_c = len(rows) + 1, len(header)
        tbl = s.shapes.add_table(n_r, n_c, MX, Inches(2.15), W - 2 * MX, Inches(0.36) * n_r).table
        for j, hname in enumerate(header):
            if col_widths: tbl.columns[j].width = Inches(col_widths[j])
        for i in range(n_r):
            for j in range(n_c):
                c = tbl.cell(i, j); v = header[j] if i == 0 else rows[i - 1][j]
                c.text = '—' if v is None else str(v); c.fill.solid()
                c.fill.fore_color.rgb = rgb(SURF if i == 0 else (PAPER if i % 2 else 'F6F5F1'))
                c.margin_left = c.margin_right = Inches(0.08); c.margin_top = c.margin_bottom = Inches(0.03)
                p = c.text_frame.paragraphs[0]; p.alignment = PP_ALIGN.RIGHT if j in number_cols else PP_ALIGN.LEFT
                f = p.runs[0].font; f.size = Pt(11 if i else 10); f.bold = i == 0; f.name = self.fu
                f.color.rgb = rgb(INK if i else INK2)
        self._foot(s, source); return s

    def watch(self, kicker, headline, items, source=''):
        """items: up to 3 × (date, title, text)."""
        s = self._slide(); self._head(s, kicker, headline)
        n = len(items); gap = Inches(0.4); cw = int((W - 2 * MX - gap * (n - 1)) / n)
        for i, (d, t, x) in enumerate(items):
            cx = MX + i * (cw + gap)
            ln = s.shapes.add_connector(1, cx, Inches(2.4), cx + cw, Inches(2.4)); ln.line.color.rgb = rgb(INK); ln.line.width = Pt(2)
            self._text(s, cx, Inches(2.6), cw, Inches(0.3), d, 12, RED, True, spacing=0.5)
            self._text(s, cx, Inches(3.0), cw, Inches(0.9), t, 17, INK, True, line=1.15)
            self._text(s, cx, Inches(3.95), cw, Inches(2.4), x, 13, INK2, line=1.3)
        self._foot(s, source); return s

    def section(self, number, title, dek=''):
        s = self._slide(SURF)
        self._text(s, MX, Inches(2.6), Inches(3), Inches(1.4), str(number), 88, RED, True, self.fh)
        self._text(s, MX, Inches(4.1), Inches(11), Inches(1.2), title, 36, INK, True, self.fh)
        if dek: self._text(s, MX, Inches(5.3), Inches(10), Inches(0.8), dek, 16, INK2)
        return s

    def save(self, path):
        self.prs.save(path); return path
