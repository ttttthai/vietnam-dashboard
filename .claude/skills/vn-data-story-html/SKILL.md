---
name: vn-data-story-html
description: Build HTML pages (web pages, Claude artifacts, dashboards, data stories) in the Vietnam Dashboard's editorial data-story style — Newsreader serif headlines that state the finding, IBM Plex Sans text, red kickers, ink-on-cream SVG charts with rounded bar ends, value-first tooltips, a data-table twin for every chart, chapter structure, bilingual VI/EN, sourced and dated numbers. Output is ALWAYS HTML (a single self-contained .html file or an artifact page), never PowerPoint, Word or images. Use when asked for a web page, HTML report, artifact or interactive data story "in our dashboard style" / "vn-data-story-html". For slide decks use the PowerPoint skill vn-data-story instead.
---

# vn-data-story-html — HTML only

**Output rule:** the deliverable is always HTML: one self-contained `.html` file (or a published artifact page built from it). Never produce a .pptx, .docx or chart images as the result; if the user wants slides, point them to the PowerPoint skill `vn-data-story`.

## Quick start
1. Copy `assets/starter.html` (self-contained: fonts from Google Fonts, the story CSS and chart helpers inlined, a VI/EN toggle, a hero and one chapter as examples).
2. Replace `DATA` with your sourced figures and write chapters in `renderStory()` with `stChap(id, kicker, headline, dek, body)`; draw charts with the helpers (see §4) inside `.st-viz` containers, register redraws with `stRW`, and finish with `stTables(root); stObserve(root)`.
3. To change the shared CSS/JS, edit `reference/story.css` / `reference/story-core.js` and run `python3 scripts/build_starter.py` to rebuild the starter.
4. Render and check before delivering (Playwright/Chromium or the browser): VI and EN, 1360px and 420px, no console errors, no horizontal scroll.


The house style of `vietnam_dashboard.html`. It reads like a printed economic report that has come alive:
calm paper surfaces, one strong serif headline per chapter, a red uppercase kicker, a single finding per chart,
and every number dated and sourced. Apply it as a whole — the type, colour, chapter anatomy, chart rules and
copy rules work together.

Reference files (copied verbatim from the page; keep them in sync when the page's design system changes):
- `reference/story.css` — `:root` tokens and every `.st-*` story rule (chapter, kicker, headline, dek, big number,
  notes, legends, motion, "what to watch").
- `reference/story-core.js` — the chart helpers (`stChap`, `stBarV/H`, `stStackV/H`, `stDot`, `stLine`,
  `stLegend`, `stNote`, `stAnn`, `stAxisY`, `stNice`, `stTipKV`, `stCross`, `stTables`, `stCW/stRW`, `stObserve`,
  `stCountUp`). Reuse them rather than re-inventing marks.

## 1. Type
- **Headlines:** `Newsreader` (Google Fonts, `opsz` 6..72, weights 500–700, `subset=vietnamese`), fallback
  `Georgia, 'Times New Roman', serif`. Chapter headline `.st-h`: 700, `clamp(22px, 2.6vw, 32px)/1.18`, max 30ch,
  `text-wrap: balance`. The headline **states the finding** ("Revenue 2,650 tn in 2025, up 29% on 2024"), never a
  topic label.
- **Text and numbers:** `IBM Plex Sans` 400–700, fallback `system-ui, -apple-system, 'Segoe UI', sans-serif`.
  Dek 15px/1.55, max 68ch; notes 14px/1.55, max 72ch; sources 11–12px; `font-variant-numeric: tabular-nums` on
  tables and aligned figures.
- **Kicker:** 600 11.5px Plex, uppercase, letter-spacing .12em, red `#c2362f` ("CHAPTER 3 · STATE BUDGET").
- **Big number** (hero only): 700, `clamp(46px, 7vw, 88px)`, counts up once on reveal; unit in a small grey `span`.

## 2. Colour
| Role | Value |
|---|---|
| Ink (headlines, primary marks, rules) | `#16181d` (page chrome `--text #0f172a`) |
| Secondary text | `#5b6170`; tertiary / sources `#8a8f99` |
| Kicker, "overdue", emphasis | `#c2362f` |
| Chart surface / dot rings | `#faf8f3` (`ST_SURF`) — warm cream, not white |
| Muted / "other" / not itemised | `#c9ccd2` (`ST_MUTE`) |
| Hairlines | `#dedbd2` (chapter rules), `#e1e5eb` (cards) |
| Page / card | `--bg #f5f6f8`, `--bg2 #ffffff` |
| Series palette (in order) | `#2a78d6` blue · `#eb6834` orange · `#1baf7a` green · `#eda100` amber · `#4a3aa7` violet · `#e34948` red · `#008300` deep green |
| Report blue (links, active tab) | `#0b5394` |

Rules: one highlighted series, the rest muted; red only for the kicker, a negative or a warning; never colour
alone — pair with labels, line style (dashed = estimate/plan/projection) or markers (hollow = provisional).
The dashboard is light-only by design; a new artifact that must follow a dark viewer theme keeps these hues and
defines dark tokens (ink → `#e8e6e1`, surface → `#17181b`) rather than inverting.

## 3. Chapter anatomy
Each tab is a story of numbered chapters shown as **sub-tabs** (only the selected chapter is visible; deep links
`#<tab>-<n>`; remembered per viewer):
1. **Hero** — kicker, finding headline, big number with unit, one-sentence dek, hero chart, freshness line
   ("data checked … · next release … (publisher)").
2. **Chapters 1…n** — `stChap(id, kicker, headline, dek, body)`; body = chart(s) + `stNote(finding, caveat)` +
   collapsed data table + `stSrc(sources)`.
3. **"What to watch"** — three dated items (`.st-watch`: red date, bold title, grey sentence).
Chapters fade/slide in once (`.st-chap.in`); bars grow from the baseline, lines draw on; everything is static
under `prefers-reduced-motion`.

## 4. Charts
- Pick the form from the question: change over time → line; parts of a whole → waffle (1 cell = 1%) or a single
  stacked bar; ranking → horizontal bars sorted; flows → Sankey; mechanism → lever/causal diagram.
- **Marks:** bars ≤ 24px thick with a 4px rounded *data end* only (baseline square); 2px surface gap between stacked
  segments; lines 2px, round joins, nulls break the line; dots r ≥ 4 with a 2px cream ring and a 24px hit area.
- **Legend above the chart**, same place every time; direct end-labels on lines where they fit.
- **Annotations:** primary (ink, bold) points at the headline's evidence; supporting (muted, smaller) is context.
- **Tooltips:** value first, label second; line charts use a crosshair listing every series at that x.
- **Every chart has a table twin** (`stTables`) — collapsed `<details>` "Data table · n rows".
- **Width-aware:** measure the container (`stCW`), draw in real pixels so 11–12px labels stay legible at 420px,
  register a redraw (`stRW`). Never let the page scroll sideways.
- Estimates / plans / projections: dashed + "estimate"/"plan" badge; model output says "dashboard model — not a
  forecast". Gaps and unpublished breakdowns are drawn as grey "not itemised" / written "not published", never 0.

## 5. Copy
- Bilingual: every string through `stT(vi, en)`; Vietnamese number format in VI (2.650,1), English in EN.
- Each number carries its period, basis (final / estimate / plan / 9M) and source; derived values are marked ≈
  and say how they were derived. Different definitions are never mixed in one series (e.g. cash execution vs
  official deficit; SBV remittances vs BoP secondary income; registered vs disbursed FDI).
- Notes explain *why*, in plain language, in two lines: the finding with its anchor, then one caveat or consequence.
- Descriptive only — no investment advice, no "should buy/undervalued".

## 6. Checklist before shipping
- Headline states a finding that the chart visibly proves; numbers in text equal the numbers in the data.
- Legend, source, period, basis badge and table twin present; dashed/hollow used for non-final values.
- VI and EN at 1360px and 420px: no console errors, no horizontal scroll, labels not overlapping.
- Reduced motion shows the final state; keyboard reaches every interactive mark (≥ 24px targets).

## Files
- `assets/starter.html` — ready-to-copy, self-contained page (no build step needed).
- `reference/story.css` — `:root` tokens, `.st-*` story rules, tooltip styles (from the dashboard).
- `reference/story-core.js` — chart and story helpers plus the tooltip/escape helpers they need
  (`dotTipShow/Move/Hide`, `ciE`); set `APP_LANG` ('vi'|'en') and `APP_LOC` ('vi-VN'|'en-US') before it runs.
- `scripts/starter.src.html` + `scripts/build_starter.py` — source of the starter and the inliner.

## Artifact notes
For a claude.ai artifact: fonts load from Google Fonts (allowed); keep everything else inline (the starter already
is); the page must work at 420px with a 16px gutter; theme tokens are light-only by design — if a dark viewer theme
must be supported, add dark token values (ink → `#e8e6e1`, surface → `#17181b`) rather than inverting.
