---
name: vn-data-story
description: Build PowerPoint (.pptx) decks in the Vietnam Dashboard's editorial data-story style — red tracked kickers, Newsreader headlines that state the finding, IBM Plex Sans text, one finding per slide, two-line notes, dated sources, a "what to watch" close. Charts are native, data-editable PowerPoint charts by default (Edit Data works). 51 methods, each with a sample slide — lines, areas, fan, combo, columns, bars, lollipop, dot, dumbbell, slope, bump, diverging, tornado, waterfall, donut, waffle, treemap, marimekko, funnel, histogram, box plot, scatter, bubble, pyramid, radar, bullet, gauge, small multiples, sparkline table, heatmaps, tile map, Sankey, lever diagram, timeline, KPI tiles, tables. Output is ALWAYS a .pptx file — for web pages use vn-data-story-html. Use when asked for a deck, slides or a presentation "in our dashboard style" / "vn-data-story", or to turn economic or banking figures into slides.
---

# vn-data-story — PowerPoint only

**Output rule:** the deliverable is always a `.pptx` built with `scripts/vn_deck.py` (python-pptx + xlsxwriter). For an HTML page or artifact, use the separate skill `vn-data-story-html`.
Never produce HTML, an artifact page or Markdown as the result. If the user asks for a web page, say this skill
makes PowerPoint decks only.

Files: `scripts/vn_deck.py` (the library; its module docstring lists every method and spec key),
`scripts/example_deck.py` → `scripts/example.pptx` (61 slides, Vietnamese, real dashboard figures: cover, catalogue,
8 chapters, **one slide per method** — copy patterns from it), `scripts/check_deck.py` (editability check),
`fonts/` (Newsreader + IBM Plex Sans TTFs, OFL licences).

## How to build a deck
1. **Plan the story**: one finding per slide — `cover` → (`catalog`) → `section` → `hero` / `kpis` → chapters of
   `chart` slides → `watch` → `table` appendix → `sources`. Pick the form from the catalogue below by the question.
2. **Write a script**: `sys.path.insert(0, '<skill dir>/scripts')`, `from vn_deck import Deck`, `d = Deck(lang='vi')`
   (or `'en'`), slide methods, `d.save(path)`. Put the data in arrays and **compute every number in headlines and
   notes** from them (`d.num(v, 2)` formats in the deck language) — never type a figure. Pass `method='…'` to list a
   slide in the catalogue slide.
3. **Render and look at every slide**: `soffice --headless --convert-to pdf deck.pptx && pdftoppm -r 60 -png deck.pdf pg`
   (`-r 110` to inspect). Fix the spec (shorter labels, fewer categories, `dx/dy` of annotations) and re-render.
4. **Check editability**: `python3 scripts/check_deck.py deck.pptx` — every chart must have an embedded workbook
   that opens and series linked to it; it also counts native-chart, native-table and shape-diagram slides.
5. Deliver the .pptx and say which fonts note applies (below).

## Editable by default
- `Deck(editable=True)` (default): every chart is a **native PowerPoint chart with an embedded workbook** — right-click
  → Edit Data opens the numbers. vn_deck writes the workbook itself (xlsxwriter) and the chart XML itself (schema
  order), so it can do what python-pptx cannot: several chart groups in one plot, secondary axes, per-point colours,
  error bars, trend lines, label positions and offsets, hidden helper series. **Helper columns are Excel formulas**
  (waterfall base/up/down, fan band = high − low, funnel padding, box-plot MIN/QUARTILE.INC/MEDIAN, histogram
  COUNTIFS, bump RANK, stacked totals, lollipop sticks, dumbbell connectors): edit the input column and the chart
  follows. Legends are small inline keys drawn above the plot (shapes), so helper series never appear in them.
- Forms with no PowerPoint chart type stay editable: **native tables with cell fills** (heatmap, calendar heatmap,
  waffle; sparkline table = native table + native mini charts) or **grouped shapes with editable text** (treemap,
  tile map, Sankey, lever diagram, timeline) whose source numbers are written into the **slide notes**.
- `Deck(editable=False)` or `spec['editable'] = False`: the shape-drawn version with **rounded data ends** for the
  types that have one (line, fan, area, bar, barh, diverging, stacked, stacked_h, stacked100_h, waffle, waterfall,
  tornado, dumbbell, slope, bump, heatmap, scatter, multiples, spark_table, pyramid, bullet). Trade-off: rounded bar
  ends and pixel-exact label placement, but the data cannot be re-plotted. Native charts cannot round bar ends.
- Static overlays on native charts (annotation callouts, forecast-zone caption, end shares on area charts, marimekko
  column names, box-plot median text, gauge centre number) are text boxes: they do not move if the data is edited.
- Numbers: VI decks tag number formats with the Vietnamese locale (`[$-42A]`), which LibreOffice honours (2.650,1);
  PowerPoint formats chart numbers in the viewer's system locale (2,650.1 on an English Windows). Labels written as
  text (waterfall, funnel, slide text) are always in the deck language.

## Catalogue — method → how it is built → when to use
| Method (spec `type` / slide method) | Built as | Use when |
|---|---|---|
| Line `line` | line chart; forecast zone = full-height column series; target = dashed constant series | How did it move? |
| Multi-line highlight `line` + `muted` | line chart, grey context lines | One series against its peers |
| Step line `step` | XY scatter, each change point doubled; shared X column | Rates/levels that change by decision |
| Area `area` (1 series) | area chart | Level over time, one series |
| Stacked area `area` | stacked area | Composition and total over time |
| 100% stacked area `area100` | percent-stacked area | Shares over time |
| Fan `fan` | line + stacked area (hidden lower bound + band) | Projection with uncertainty |
| Combo `combo` | column + line on a secondary axis, zero lines aligned | Two **different units** only; name both axes |
| Column `bar` | clustered column, highlight by point colour, plans hollow dashed | Compare periods |
| Grouped column `bar` (2+ series) | clustered column | Two series per period |
| Ranked bar `barh` | clustered bar, reversed categories, value labels | Ranking |
| Grouped bar `barh` (2+ series) | clustered bar | Two periods per row |
| Stacked column / bar `stacked` / `stacked_h` | stacked + invisible clustered twin carrying the total label | Composition + total |
| 100% column / bar `stacked100` / `stacked100_h` | percent-stacked, % labels inside | Parts of one whole per row |
| Lollipop `lollipop` | marker-only line + error-bar sticks | Many similar bars, less ink |
| Dot plot `dot` | marker-only line chart (+ dashed reference) | Several estimates per item; axis need not start at 0 |
| Dumbbell `dumbbell` | two marker-only series + error-bar connector | Before / after per row |
| Slope `slope` | 2-category line chart, labels at both ends | Two points in time |
| Bump `bump` | line chart of RANK() formulas, reversed axis | Rank over time |
| Diverging bar `diverging` | two bar series (+/−), overlap 100 | Positive vs negative |
| Tornado `tornado` | clustered bar of level − base, overlap 100 | Sensitivity |
| Waterfall `waterfall` | stacked column: hidden base + up + down + total (formulas) | Start → contributions → end |
| Donut / pie `donut` / `pie` | doughnut / pie, shares + side key | Few parts of one whole (bars usually read better) |
| Waffle `waffle` | native 10 × 10 table, filled cells | Share of a whole, 1 cell = 1% |
| Treemap `treemap` | squarified grouped shapes; numbers in notes | Many parts of one whole |
| Marimekko `marimekko` | 100% stacked column of zero-gap slices (width = slice count) | Size **and** mix at once |
| Funnel `funnel` | stacked bar centred by hidden padding | Nested stages of one process |
| Histogram `histogram` | column, gap 4, COUNTIFS over raw data | Distribution |
| Box plot `box` | stacked column + error-bar whiskers, quartile formulas | Distributions side by side |
| Scatter `scatter` | XY scatter + linear trendline, point labels | Relationship (say r, not causation) |
| Bubble `bubble` | bubble chart, groups by colour | Relationship + size |
| Population pyramid `pyramid` | clustered bar, left negative + unsigned format, outlined projection twin | Age structure |
| Radar `radar` | radar chart | Shape comparison of few items (bars read better for values) |
| Bullet `bullet` | one small chart per row: bands + error-bar actual + dash-marker target | Actual vs target |
| Gauge `gauge` | half doughnut (hidden lower half) + target tick | One number against one mark |
| Small multiples `multiples` | grid of native mini charts, own scales | Same metric, many panels |
| Sparkline table `spark_table` | native table + native mini line charts | Indicator table with trends |
| Heatmap `heatmap` | native table, diverging/sequential fills | Many rows × periods |
| Calendar heatmap `calendar` | native table, rows = years, cols = months | Seasonal / monthly pattern |
| Tile map `tilemap` | grouped shapes, one-hue ramp; notes | Provinces / regions |
| Sankey `flow` | grouped shapes; notes | Sources → uses |
| Lever / causal diagram `levers` | shapes + connectors; notes | Instrument → channel → outcome |
| Timeline `timeline` | shapes; notes | Dated beats |
| KPI tiles `kpis` | shapes + native sparklines | 2–4 headline indicators |
| Hero number `hero` | big number + counters + native chart | Opening number |
| Table `table` | native table | Appendix figures |
| Quote `quote` | text + fact column | Analyst note |
| What to watch `watch` | 3 dated cards with countdown | Close (dates must be in the future) |
| Sources `sources` | text + method panel | Sources & methodology |
| Catalogue `catalog` | filled at save from `method=` | Index of methods / slides |

## Typography and layout (matches the page's story CSS)
- Headline: **Newsreader SemiBold** 24 → 20 pt, leading 1.08, ≤ 2 lines, **balanced** wrapping inside 10 columns.
  Kicker: IBM Plex Sans SemiBold 11 pt, uppercase, tracked, #C2362F. Dek: Plex 13.5 pt / 1.4, #5B6170.
  Note: Plex 13 pt / 1.42, finding in ink, caveat in #5B6170, 7-column measure, aligned to the chart's left edge.
  Sources: Plex 9 pt #8A8F99. Chart text: Plex 10 pt #5B6170 (written into each chart's txPr, so it survives Edit
  Data). Big numbers: Plex Bold. Plex and Newsreader digits are tabular by default.
- Grid: 16:9, 0.6 in margins, 12 columns with 0.2 in gutters; hairline at 0.42 in, kicker 0.56 in, headline 0.84 in,
  **chart top 1.92 in on every chart slide**, footer hairline 7.02 in. Side panel = 4 columns, cream #FAF8F3.
- Charts: hairline grid #E1E0D9 0.5 pt, no value-axis line, only the baseline in ink, no chart borders, no tick marks,
  lines 2 pt round caps, small markers, bars ≤ 0.34 in, legends as inline keys above the plot or direct end labels.
- Colour: palette in fixed order blue #2A78D6, orange #EB6834, aqua #1BAF7A, yellow #EDA100, magenta #E87BA4, green
  #008300, violet #4A3AA7, red #E34948; colour follows the entity across the deck; one accent on grey #C9CCD2 for
  emphasis; polarity blue ↔ red with a grey midpoint; ordered bins one-hue ramps. Text is ink or grey, never a series
  colour.

## Fonts and embedding
- Exact family names matter: "Newsreader SemiBold", "Newsreader Medium", "IBM Plex Sans", "IBM Plex Sans Medium",
  "IBM Plex Sans SemiBold". Text is measured with the real metrics from `fonts/`, so wrapping and balancing fit.
- `Deck(embed_fonts=True)` (default) embeds the families the deck uses the way PowerPoint does (EOT `.fntdata` parts +
  `<p:embeddedFontLst>`, `embedTrueTypeFonts="1"`). **PowerPoint (Windows, Mac 16.17+) shows the embedded fonts;
  LibreOffice, Keynote and Google Slides ignore them** — install `fonts/*.ttf` first (Linux: `~/.fonts` + `fc-cache`).
- `Deck(safe_fonts=True)` → Georgia / Arial everywhere, nothing embedded.

## Known renderer differences (LibreOffice preview vs PowerPoint)
- LibreOffice ignores `tickLblSkip` (PowerPoint skips crowded category labels); keep ≤ ~12 categories on narrow charts.
- LibreOffice draws radar charts smaller than their frame and does not show white rings around markers.
- XY data must not start in workbook column A (some apps read it as categories); vn_deck puts labels there.

## Content rules
- **Headline = the finding**, number-first when possible, ≤ ~15 words; every number in it is computed from the data.
  No hype, no verdicts the data doesn't show (r = 0.53 is "moderate").
- **Note = two lines:** the finding with its anchor (value, period), then one caveat, contrast or consequence.
- **Every data slide has a source line** with publisher and period; each figure carries its basis (final / estimate /
  plan / 9M). Mark derived values with ≈ and say how. Illustrative content says "minh hoạ / illustrative".
- **Honest forms:** bars start at zero; estimates, plans and projections are dashed, hollow or outlined and labelled;
  model output says "model, not a forecast"; part-to-whole only when the parts sum to one whole; a second axis only
  for a second unit, both axes named, zero lines aligned (`combo`).
- **Never mix definitions in one series**; **gaps stay gaps** (`None`, never 0, never interpolated).
- One language per deck; descriptive only — no investment advice. "What to watch" items must be in the future.

## Checklist before delivering
- Rendered every slide and looked at it: no clipped or overlapping text, nothing outside the slide or the chart box,
  no label colliding with ticks or other labels, even spacing, empty areas filled or intentional.
- `check_deck.py` reports no problems; every chart slide has a native chart with a workbook (diagram forms: shapes +
  notes; heatmap / calendar / waffle: native tables).
- Each chart answers its headline; bars start at zero; legend above for 2+ series; highlighted element is the one
  the headline names.
- Numbers in headline/notes match the chart; sources, periods and basis on every data slide; appendix table.
- Fonts: embedded (PowerPoint) or `fonts/` installed (LibreOffice / Keynote) or `safe_fonts=True`.
