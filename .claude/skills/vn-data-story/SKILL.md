---
name: vn-data-story
description: Build PowerPoint (.pptx) decks in the Vietnam Dashboard's editorial data-story style — red uppercase kickers, Newsreader serif headlines that state the finding, IBM Plex Sans text, one finding per slide, charts drawn in the dashboard palette with rounded bar ends (or native editable charts on request), two-line notes, dated sources, "what to watch" close. 30+ chart and slide types (line, fan, area, bars, stacked, waffle, waterfall, tornado, dumbbell, slope, bump, heatmap, scatter, small multiples, sparkline table, pyramid, tile map, Sankey flow, lever diagram, timeline, bullet, KPI tiles). Output is ALWAYS a .pptx file (never HTML, Markdown or an artifact page). Use when asked for a deck, slides, a presentation or a report "in our dashboard style" / "vn-data-story", or to turn dashboard data, economic or banking figures into slides.
---

# vn-data-story — PowerPoint only

**Output rule:** the deliverable is always a `.pptx` built with `scripts/vn_deck.py` (python-pptx + stdlib only).
Never produce HTML, an artifact page or Markdown as the result. If the user asks for a web page, say this skill
makes PowerPoint decks only.

Files: `scripts/vn_deck.py` (the library; its module docstring lists every method and spec key),
`scripts/example_deck.py` (a 38-slide Vietnamese sample that uses **every** style with the dashboard's real
figures; run it and copy patterns from it), `fonts/` (Newsreader + IBM Plex Sans TTFs, OFL licences).

## How to build a deck
1. **Plan the story**: one finding per slide — `cover` → `section` → `hero` → `kpis` → chapters of `chart` slides
   → `watch` → `table` appendix → `sources`. Pick the chart type from the table below by the question it answers.
2. **Write a script**: `sys.path.insert(0, '<skill dir>/scripts')`, `from vn_deck import Deck`,
   `d = Deck(lang='vi')` (or `'en'`), slide methods, `d.save(path)`. Put the data in arrays and **compute every
   number in headlines and notes** from them (`d.num(v, 2)` formats in the deck language) — never type a figure.
3. **Render and look at every slide** before handing over:
   `soffice --headless --convert-to pdf deck.pptx && pdftoppm -r 60 -png deck.pdf pg` (use `-r 110` to inspect).
   Check the checklist at the end. Fix the spec (shorter labels, fewer rows, `dx/dy` of annotations) and re-render.
4. Deliver the .pptx and say which fonts note applies (below).

## Slide methods (`Deck`)
| Method | Use |
|---|---|
| `cover(kicker, title, subtitle, date, chart=None, note='')` | Cover, cream, optional motif chart on the right |
| `section(number, title, dek, items=())` | Chapter divider: big red number, contents list |
| `hero(kicker, headline, big, unit, dek, counts=[(label, text, value)], chart, chart_title, source, note)` | Headline number + count-style counters with progress bars + mini chart in a cream panel |
| `kpis(kicker, headline, tiles, note, source)` | 2–4 KPI tiles: value, delta arrow (blue up / red down, `tone` overrides), optional sparkline |
| `chart(kicker, headline, spec, note=(finding, caveat), source, dek, panel=None, chart_title)` | One chart. `panel={'label','value','unit','sub','bullets'}` → chart left + cream side panel carrying the key number and the note |
| `two_charts(kicker, headline, left, right, titles, note, source)` | Two charts side by side |
| `table(kicker, headline, header, rows, source, col_widths, number_cols, note)` | Appendix: zebra rows, right-aligned numbers, `None` → "—" |
| `watch(kicker, headline, [(date, title, text, (big, small))…], source)` | 3 dated "what to watch" cards with a countdown block |
| `quote(kicker, quote, who, role, note, source, facts=[(label, value, sub)])` | Analyst note / callout with fact column |
| `sources(kicker, headline, [(publisher, what, period, url)…], method=[…], source)` | Sources & methodology |

## Chart spec types (`spec['type']`)
| Type | Answers | Key spec fields |
|---|---|---|
| `line` | How did it move? | `categories`, `series[{name, values, color, muted, dashed, dash_from, end_label, label}]`, `dec`, `unit`, `y_min/y_max`, `zero`, `forecast_from` + `forecast_label` (shaded zone), `refs[{value,label}]`, `annotations[{series, at, text, tier:'pri'/'sup', dx, dy, value}]`, `band` |
| `fan` | Projection with uncertainty | `categories`, `actual`, `base`, `low`, `high`, `names` (80% band, dashed base, zone) |
| `area` | Stacked totals over time | `categories`, `series` (end labels = share of last total) |
| `bar` | Compare periods / one highlighted | `categories`, `series` (2+ = grouped), `highlight`, `basis` (`'estimate'`/`'plan'` → hollow dashed), `labels` (`'auto'`,`True`,`'hi'`,`False`) |
| `barh` | Ranking | `categories`, `values` (sorted by you; first on top), `highlight`, `axis` |
| `diverging` | Positive vs negative | `categories`, `values`, `pos_label/neg_label` (blue > 0, red < 0) |
| `stacked` / `stacked_h` | Composition + total | `categories`, `series`, `totals`, `basis` (rounded top segment only, 2px gaps) |
| `stacked100_h` | Parts of one whole per row | `categories`, `series` (normalised to 100, % inside segments) |
| `waffle` | Share of a whole, 1 cell = 1% | `parts[{name, value, color, muted}]`, `dec` (largest-remainder rounding) |
| `waterfall` | Start → contributions → end | `steps[{name, value, total, basis}]` (last total computed if `value` omitted) |
| `tornado` | Sensitivity | `base`, `rows[{name, low, high}]`, `labels` |
| `dumbbell` | Before / after per row | `rows[{name, a, b}]`, `labels` |
| `slope` | Two periods | `periods`, `series[{name, values:[a,b], hi}]` |
| `bump` | Rank over time | `periods`, `series[{name, values}]` (ranked inside), `highlight[names]` |
| `heatmap` | Many rows × periods | `rows`, `cols`, `values`, `scale` (`diverging`/`sequential`), `vmax` (clip), `pos_color`, `legend_title` |
| `scatter` | Relationship | `points[{name, x, y, label, hi, text}]`, `x_title`, `y_title`, `trend` (OLS line + r) |
| `multiples` | Same metric, many panels, own scales | `panels[{title, categories, values, kind:'col'/'line'}]`, `cols` |
| `spark_table` | Indicator table with trends | `rows[{name, sub, values, dec, unit, up_color, down_color}]`, `headers` |
| `pyramid` | Age structure | `bands`, `left`, `right`, `compare` (dashed outline = projection) |
| `tilemap` | Provinces / regions | `tiles[{name, short, col, row, value, panel}]`, `panels`, `breaks` (one-hue ramp) |
| `flow` | Sources → uses (Sankey) | `columns[[{id, name, color}]]`, `links[(src, dst, value)]` |
| `levers` | Instrument → channel → outcome | `columns[{title, items[{id, name, sub}]}]`, `links[(a, b)]` (adjacent or same column), `highlight` |
| `timeline` | Dated beats | `events[{date, title, text, future}]`, `today` |
| `bullet` | Actual vs target | `rows[{name, sub, value, target, max, ranges, unit, dec, value_text, color}]` |
| `tiles` | KPI tiles (used by `kpis`) | `tiles[{label, value, unit, delta, delta_unit, delta_label, tone, spark, spark_kind, sub}]` |

## Shape-drawn vs native editable charts
- **Shape-drawn (default)** — every chart is drawn with shapes in one group per chart, one scale per chart, to the
  dashboard look: bars ≤ 0.34 in with a **rounded data end only** (`ROUND_2_SAME_RECTANGLE`, rotated for
  horizontal and negative bars, baseline square), 2px surface gaps between stacked segments, 2.25pt lines with
  round joins, ringed dots, hairline grids, legend above, direct end labels, primary/supporting annotations. Use
  for presentations, reports and anything the audience reads. Numbers can still be edited as text; the data
  cannot be re-plotted.
- **Native (`Deck(editable=True)` or `spec['editable'] = True`)** — real PowerPoint charts with an embedded
  workbook, styled to match (Plex fonts, palette, no title, legend on top, hairline grid, gap width, data labels).
  Use **only when the audience must edit the data**. Supported for `line, area, bar, barh, diverging, stacked,
  stacked_h, stacked100_h, scatter`; all other types are always shape-drawn. PowerPoint cannot round bar ends and
  number formats follow the viewer's locale (2,650.1 even in a VI deck) — say so in the note.

## Fonts and embedding
- Roles map to the real faces: headlines `Newsreader` bold, kicker/labels `IBM Plex Sans SemiBold` / `Medium`,
  body `IBM Plex Sans`, big numbers `IBM Plex Sans` bold (as on the page). Exact family names matter: the 500/600
  weights are separate families ("IBM Plex Sans SemiBold", "Newsreader Medium"…). Text is measured with the real
  font metrics from `fonts/` so labels, wrapping and headline sizing fit.
- `Deck(embed_fonts=True)` (default) embeds the families the deck uses, the way PowerPoint does: each TTF wrapped
  as uncompressed **EOT** in `/ppt/fonts/fontN.fntdata` (`application/x-fontdata`, font relationships) plus
  `<p:embeddedFontLst>` with regular/bold `r:id`s and `embedTrueTypeFonts="1"` (~1.2 MB). (PowerPoint's `.fntdata`
  is EOT; the GUID-XOR obfuscation is Word's `.odttf` convention and is not used.) **PowerPoint (Windows, Mac
  16.17+) shows the embedded fonts; LibreOffice, Keynote and Google Slides ignore them** — on those, install
  `fonts/*.ttf` first (Linux: copy to `~/.fonts` and run `fc-cache`). The deck re-opens in python-pptx and
  LibreOffice with the parts intact.
- `Deck(safe_fonts=True)` → Georgia / Arial everywhere, nothing embedded: use when the deck goes to machines you
  don't control and must look identical in any app.

## Style (built in — keep it)
- **Colour:** ink `#16181D`, secondary `#5B6170`, sources `#8A8F99`, kicker `#C2362F`, cream `#FAF8F3`, muted
  `#C9CCD2`, hairlines `#DEDBD2`/`#E1E0D9`. Palette in fixed order: blue `#2A78D6`, orange `#EB6834`, aqua
  `#1BAF7A`, yellow `#EDA100`, magenta `#E87BA4`, green `#008300`, violet `#4A3AA7`, red `#E34948`. Colour follows
  the entity across the deck; one accent on grey for emphasis; polarity blue ↔ red with a grey midpoint; ordered
  bins use one-hue ramps. Text is ink or grey, never a series colour.
- **Layout:** 16:9 (13.33 × 7.5 in), 0.65 in margins; heavy ink rule, kicker, headline (auto 28→21 pt, ≤ 2
  lines), chart, two-line note, hairline footer with source + "Vietnam Dashboard  n". Cream panels for side notes,
  KPI tiles, hero chart, quotes, watch cards.

## Content rules
- **Headline = the finding**, number-first when possible, ≤ ~15 words; every number in it appears on the slide and
  is computed from the data. No hype, no verdicts the data doesn't show (r = 0.53 is "moderate", not "weak").
- **Note = two lines:** the finding with its anchor (value, period), then one caveat, contrast or consequence.
- **Every data slide has a source line** with publisher and period; each figure carries its basis (final /
  estimate / plan / 9M). Mark derived values with ≈ and say how (e.g. "chi vượt thu ≈ chi − thu, not the official
  deficit"). Illustrative content says "minh hoạ / illustrative" in the source.
- **Honest forms:** bars start at zero (no truncated bar axes — use a change bridge instead); estimates, plans and
  projections are dashed, hollow or outlined and labelled; model output says "model, not a forecast";
  part-to-whole only when the parts sum to one whole; never a dual axis.
- **Never mix definitions in one series** (registered vs disbursed FDI; 9-month vs full-year; consolidated vs
  parent bank figures; SBV vs IMF FSI NPL).
- **Gaps stay gaps:** `None`, never 0, never interpolated; say "not published".
- **One language per deck**; VI number style 2.650,1 and U+2212 minus via `d.num`. Descriptive only — no
  investment advice. "What to watch" items must be in the future at delivery.

## Checklist before delivering
- Rendered every slide and looked at it: no clipped or overlapping text, nothing outside the slide or the chart
  box, no label colliding with ticks or other labels, even spacing, empty areas filled or intentional.
- Each chart answers its headline; scale starts at zero for bars; legend above for 2+ series; bar order intended;
  highlighted element is the one the headline names.
- Numbers in headline/notes match the chart; sources, periods and basis on every data slide; appendix table.
- Fonts: embedded (PowerPoint) or `fonts/` installed (LibreOffice / Keynote) or `safe_fonts=True`.
