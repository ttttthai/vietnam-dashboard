---
name: vn-data-story
description: Build PowerPoint (.pptx) decks in the Vietnam Dashboard's editorial data-story style — red uppercase kickers, serif headlines that state the finding, one finding per slide, native editable charts in the dashboard palette, two-line notes, dated sources, "what to watch" close. Output is ALWAYS a .pptx file (never HTML, Markdown or an artifact page). Use when asked for a deck, slides, a presentation or a report "in our dashboard style" / "vn-data-story", or to turn dashboard data, economic or banking figures into slides.
---

# vn-data-story — PowerPoint only

**Output rule:** the deliverable is always a `.pptx` file built with `scripts/vn_deck.py` (python-pptx). Never
produce HTML, an artifact page or Markdown as the result, even if a page would be quicker. If the user asks for
a web page, say this skill makes PowerPoint decks only.

## How to build a deck
1. Plan the story first: one finding per slide, in this order — title → hero (the headline number) → chapters
   (optionally `section` dividers) → "what to watch" → data appendix (`table`) → sources.
2. Write a short Python script that imports `vn_deck` (`sys.path.insert(0, '<skill dir>/scripts')`) and calls
   `Deck(lang='vi'|'en')` then the slide methods (see the module docstring for a full example).
3. Save to the output folder, then **render and look** before handing it over:
   `soffice --headless --convert-to pdf deck.pptx && pdftoppm -r 50 -png deck.pdf pg` — check every slide for
   overlapping or clipped text, empty areas, wrong bar order and labels the chart doesn't reach.
4. Deliver the .pptx file (and mention the font note below).

## Slide methods (`Deck`)
| Method | Use |
|---|---|
| `title(kicker, title, subtitle, date)` | Cover, cream background |
| `hero(kicker, headline, big, unit, dek, chart=None, source)` | The headline number + one chart |
| `chart(kicker, headline, spec, note=(finding, caveat), source, dek)` | One chart, full width |
| `two_charts(kicker, headline, left, right, titles, note, source)` | Comparison, side by side |
| `section(number, title, dek)` | Chapter divider |
| `table(kicker, headline, header, rows, source, col_widths, number_cols)` | Data appendix (None → "—") |
| `watch(kicker, headline, [(date, title, text)×≤3], source)` | Closing "what to watch" |

Chart `spec` types: `line`, `bar`, `barh` (ranked; sort the data yourself, first item shows at the top),
`stacked`, `stacked_h`, `waffle` (`parts` summing to 100, 1 cell = 1%), `bignumbers` (2–4 `tiles`). Series options:
`color`, `muted` (grey context), `dashed` (estimate / plan / projection), `None` values for gaps. Spec options:
`number_format` (e.g. `'0.0'`, `'#,##0'`), `y_title`, `y_min`/`y_max`, `labels` (data labels), `highlight`
(index of the one bar to colour; the rest go grey), `markers`.

## Style (already built into vn_deck — keep it)
- **Type:** headlines `Newsreader` (serif), text `IBM Plex Sans`. Install both from Google Fonts for exact
  rendering; otherwise PowerPoint substitutes. `Deck(safe_fonts=True)` uses Georgia / Arial for decks that will be
  opened on machines without those fonts.
- **Colour:** ink `#16181D`, secondary `#5B6170`, sources `#8A8F99`, kicker `#C2362F`, cream `#FAF8F3`, muted
  `#C9CCD2`. Series palette in order: `#2A78D6` blue, `#EB6834` orange, `#1BAF7A` green, `#EDA100` amber,
  `#4A3AA7` violet, `#E34948` red, `#008300` deep green. One highlighted series, the rest muted; red only for the
  kicker, a negative or a warning.
- **Layout:** 16:9 (13.33 × 7.5 in); heavy ink rule at the top, kicker, headline, chart, note, hairline footer
  with source and slide number.

## Content rules
- **Headline = the finding** ("Credit outpaced deposits by 6.7 points"), never a topic label. Every number in the
  headline appears on the slide.
- **Note = two lines:** the finding with its anchor (value, period), then one caveat, contrast or consequence.
- **Every slide has a source line** with publisher and period; each figure carries its basis (final / estimate /
  plan / 9M). Mark derived values with ≈ and say how.
- **Never mix definitions in one series** (cash execution vs official deficit; SBV remittances vs BoP secondary
  income; registered vs disbursed FDI; consolidated vs parent bank figures).
- **Gaps stay gaps:** `None`, never 0, never interpolated; say "not published" in the note.
- **Estimates and projections** are dashed series and say so ("model, not a forecast" for model output).
- **One language per deck** (`lang='vi'` or `'en'`); Vietnamese number style in VI text (2.650,1). Chart number
  formats follow the viewer's PowerPoint locale.
- Descriptive only — no investment advice.

## Checklist before delivering
- Rendered every slide and looked at it; no clipped or overlapping text; nothing outside the slide.
- Each chart answers its headline; legends only where there are 2+ series; bar order intended.
- Sources, periods and basis on every data slide; appendix table for the key numbers.
