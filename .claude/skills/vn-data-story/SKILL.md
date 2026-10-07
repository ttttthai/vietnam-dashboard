---
name: vn-data-story
description: The Vietnam Dashboard's editorial data-story style — a light, report-like look (Newsreader serif headlines, IBM Plex Sans text, red kickers, ink-on-cream charts), chapter-by-chapter storytelling with sub-tabs, thin rounded-end chart marks, value-first tooltips, a data-table twin for every chart, bilingual VI/EN copy and sourced, dated numbers. Use when building or restyling a page, chart, report, slide or artifact that should look and read like the Vietnam Dashboard, or when asked to apply "our dashboard style" / "vn-data-story" to new work.
---

# vn-data-story

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

## Using it elsewhere
- **New artifact / page:** load Google Fonts (`IBM Plex Sans` + `Newsreader`, Vietnamese subset), paste
  `reference/story.css` tokens and `.st-*` rules, include `reference/story-core.js` (plus a tiny `dotTipShow/
  dotTipMove/dotTipHide` and `ciE` escape), then compose chapters with `stChap`.
- **Design mode / design systems:** the tokens in §2 and type in §1 are the design-system values; a "Vietnam
  Dashboard" design system can be generated from this file.
