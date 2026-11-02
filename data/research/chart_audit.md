# Chart audit: Vietnam Dashboard (all six tabs)

As of 2026-10-06 · Research · read-only review of `vietnam_dashboard.html` (story layer, explore cards, appendices) and the 13 Flourish charts in `data/flourish.json`.
Rules applied: dataviz (form heuristic, one axis, colour by job, thin marks, table twin), chart-honesty (the seven tests), chart-annotations (title = claim, annotation = locator), data-storytelling, d3-viz. Flourish catalogue: the 63 @flourish templates (the "security canary" template was ignored).
Function names are the stable reference. Line numbers move because the page is being edited.

## How each decision was made

- **Native is the default.** Native charts have computed headlines, VI/EN, table twins, linked highlights and replay controls, and they work offline and on the claude.ai copy. Flourish charts are English only, the user has to publish them, they show only as a link on the published copy, and they don't work offline.
- **Flourish wins only where it adds something** the native chart can't reasonably do: animation over many time steps, rich filters or search, or an explorable scatter with a time slider. A Flourish line or column chart that repeats a native chart adds nothing.
- `[P]`: the chart draws into a fixed `viewBox` 1000 px wide, so on a 375 px phone its 12 px labels come out about 4.5 px tall. The fix is cross-cutting (see Top 10 #4). The econ projection charts (`ecW`) already draw at container width and are not tagged.
- Effort: S ≈ under 1 h, M ≈ half a day, L ≈ a day or more.

## 1. Summary counts

| Decision | Count |
|---|---|
| KEEP | 62 |
| IMPROVE NATIVE | 22 |
| FLOURISH REPLACE | 0 |
| FLOURISH ADD | 3 (1 upgrades an existing chart, 1 is new, 1 is optional) |
| DROP / MERGE | 24 (12 of them are Flourish duplicates) |
| **Total rows** | **111** (rows 28, 73, 74, 76, 78, 79, 80, 92, 109 each group 2–6 closely related charts) |

**Flourish verdict:** of the 13 Flourish charts built, keep 1: the province scatter, upgraded with a time slider. The other 12 are 10 line-bar-pie charts, the credit Sankey and the drafts Gantt. Each repeats a native chart on the same chapter that is already better: it has computed text, VI/EN, a table twin and linked highlight. Take them off the page. Keep them in the Flourish account only if the user wants English embeds for other sites.
The account also holds 7 "Showcase/Demo … (illustrative)" charts with invented banking data. Never embed them.

## 2. Full table

### Economy (econ)

| # | Chapter | Chart (fn / id) | Current form | Decision | Proposed form | Rationale | Effort |
|---|---|---|---|---|---|---|---|
| 1 | st-hero | `stGrowth` | Columns 2011–25 + 9M bar, dashed forecast per institution, target diamonds | KEEP | — | Actuals, forecasts and target each have their own mark; zero baseline; width-aware | — |
| 2 | st-hero | Flourish 30475879 | line-bar-pie copy of #1 | DROP | — | Same data, less information (no part-year flag), English only | S |
| 3 | st-formula | `stEquation` | HTML blocks with share bars | KEEP | — | Bars are proportional to % of GDP, linked highlight to #5 | — |
| 4 | st-formula | `stGiants` | 3 horizontal bars (GDP=100, X, M) + gap bracket | KEEP | — | Shows trade larger than GDP; the bracket marks X − M | — |
| 5 | st-formula | `stMix` | 4 lines, % of GDP 2015–25, crosshair [P] | KEEP | — | Right form for shares over time; end labels + legend | — |
| 6 | st-bop | `stSankey` + net/gross + ▶ years | Hand-built Sankey [P] | KEEP | Below 600 px: two ranked bar lists (in / out) instead of a 1000 px Sankey | Colouring by direction and the year replay beat Flourish's Sankey; the problem is legibility on phones only | M |
| 7 | st-bop | `stBopMult` | 4 small-multiple signed bars, one shared scale | KEEP | — | One shared scale, polarity colour; correct | — |
| 8 | st-bop | `stBopFacts` | 3 sparkline tiles | KEEP | — | Context only; zero-based sparklines | — |
| 9 | st-bop | `ecBopQ` | KPI tiles for the latest quarter | KEEP | — | Two numbers per item; a chart would add nothing | — |
| 10 | st-budget | `stWaffles` (year buttons) | Two 10×10 waffles; revenue has 7 hues | IMPROVE NATIVE | Revenue: 4 named classes + grey "other" (fold crude oil, grants, fees). Add the land-use-fee series to #11: the chapter note cites it, but no chart marks it | 7 hues include squares under 1%. The note's claim has no mark to point at (annotation rule) | S |
| 11 | st-budget | `stBudLine` | Revenue / spending / dev-spending lines + deficit bars, one tn-VND axis, dashed estimates | IMPROVE NATIVE | Compute the y max (now fixed at 3,500); add the land-use-fee line (hollow points for estimate and plan) | One axis is fine. A hard-coded scale will clip the next plan year | S |
| 12 | st-budget | `ecDef` | Deficit columns by basis + target diamond + average line | KEEP | — | Basis is encoded (solid / faded / outlined); honest | — |
| 13 | st-budget | `ecDebt` | Bullet: plan range vs warning level vs ceiling | KEEP | — | Bullet is the right form; states that no actual series exists | — |
| 14 | st-inv | `stInvArea` | Stacked area by owner (levels) + % end labels | KEEP | — | Level and share both readable; seams follow the spec | — |
| 15 | st-inv | `ecFdi` | Columns + part-year bar + target band | KEEP | — | Part-year is faded and labelled; band for a range target | — |
| 16 | st-cpi | `stHeat` | 12 × 24 diverging heatmap [P] | IMPROVE NATIVE | Add a stepped colour legend with values (scale saturates at ±8%); shorten row labels below 600 px | Best form, but the readable range is only in the dek text. The 310 px label column eats a phone screen | S |
| 17 | st-cpi | `stFan` | Actual line + dashed base case + low–high band + target | KEEP | — | Labelled as a dashboard estimate; band, not a single line | — |
| 18 | st-cpi | `ecCpiY` | Annual average + institution forecasts + target | KEEP | — | One series per institution, never averaged | — |
| 19 | st-cpi | Flourish 30476088 | line-bar-pie copy of #18 | DROP | — | Duplicate | S |
| 20 | st-2030 | `ecPcChart` | Line + IMF path + target diamond + "≈ USD short" bracket | KEEP | — | The gap is computed and marked; good annotation | — |
| 21 | st-2030 | Flourish 30476086 | line-bar-pie copy of #20 | DROP | — | Duplicate | S |
| 22 | st-2030 | `ecUsdChart` | Converted actual + IMF path | KEEP | — | "≈" conversion is stated | — |
| 23 | st-2030 | `ecNow` | KPI tiles vs targets | KEEP | — | Part-year vs annual targets: tiles avoid a misleading progress bar | — |
| 24 | appendix | `renderEconTop` (#ec-root: formula, BoP ledger, budget ledger) | HTML ledgers | DROP / MERGE | Rely on the story's table twins (`stTables`) | Repeats chapters 1–3 | S |
| 25 | appendix | `ecb-gdppc` (`updateSpecialCharts` 'grdp') | **Dual axis**: real-GDP bars (left) + growth line (right) | IMPROVE NATIVE | Growth columns only (GDP level stays in the KPI tile), or two stacked panels sharing the year axis | Dual axis: where the two scales meet is arbitrary and suggests a correlation that isn't there | S |
| 26 | appendix | `ecb-income-card` | Area with gradient fill | KEEP | Drop the gradient (flat 2 px line + light fill) | Decorative gradient only | S |
| 27 | appendix | `ecb-cpi-card` | Annual CPI columns | DROP | — | National only; repeats #18 | S |
| 28 | appendix | `ecb-fdi`, `ecb-trade`, `ecb-labour` | Columns / grouped columns with region/province drill-down | KEEP | Label province values "model" on the chart, not only in the ⓘ | The only province drill-down; model basis must be visible | S |
| 29 | appendix | `drawGdpChart` (#gdp-sectors, slider 2000–2025) | 4 sector bars + 16 sub-activity bars at one year | FLOURISH ADD | **Bar chart race** (`bar-chart-race`, 110485) of the 16 NSO activities' share of GDP, 2010–2025; keep the native card as the VI/offline fallback | Real 16-year ranking change (manufacturing vs agriculture vs trade) is the one thing a race does better. The slider shows one year at a time and loses the trend | M |
| 30 | appendix | `ecb-cpi-monthly` (`renderCpiAt`) | 24-month line + projection modes | DROP / MERGE | Keep the KPI tiles and the basket table | Same series and same projection as #17 | S |
| 31 | appendix | CPI basket "Xu hướng 24T" (`inlineBarsSvg`) | 24 mini bars per group | IMPROVE NATIVE | Signed bars from a zero line (blue below, red above), or one heat-strip row per group to match #16 | **Defect:** `Math.abs()` plus a truncated base. Transport's −6.89% YoY draws as its tallest bar; 15 negative months look like inflation | S |

### Society (social)

| # | Chapter | Chart (fn / id) | Current form | Decision | Proposed form | Rationale | Effort |
|---|---|---|---|---|---|---|---|
| 32 | soc-st-hero | `socStPop` | Actual line + NSO band + UN dashed, y 75–120 m | KEEP | — | A line may start above zero; projections are dashed | — |
| 33 | soc-st-hero | Flourish 30476092 | line-bar-pie copy of #32 | DROP | — | Duplicate | S |
| 34 | soc-st-tfr | `socStStrip` | Strip plot, 34 dots, 2 hues at the 2.1 threshold | KEEP | — | Exemplary: threshold line, extremes labelled | — |
| 35 | soc-st-tfr | `socStMult` | 4 strip multiples | IMPROVE NATIVE | Density on a log scale; national reference tick on every strip | Linear 0–4,500/km² piles about 30 provinces into the first 15% of the axis | S |
| 36 | soc-st-age | `socStAge` | Two 100% stacked bars (2025, 2050) | IMPROVE NATIVE | Real age–sex pyramid 2025 vs 2050 (UN WPP 2024 / NSO), 2050 as an outline over 2025; keep the 3-band summary as labels | One true pyramid replaces both this and the synthetic explorer pyramid (#42) | M |
| 37 | soc-st-rich | `socStMap` (GRDP / TFR toggle) | Native choropleth, 34 merged provinces, linked highlight | KEEP | Optional: add urban %, growth and density modes (the paint code is already generic) | Custom boundaries, VI/EN and linked highlight already work. A Flourish projection map would duplicate it | S |
| 38 | soc-st-rich | `socStGrdp` | 34 sorted bars, 2 accented, national line [P] | KEEP | — | Sorted, zero-based, national reference line | — |
| 39 | soc-st-rich | Flourish 30475882 (scatter) | GRDP pc × TFR, 34 dots | FLOURISH ADD (keep + upgrade) | `scatter` 110463 with **time slider 2020 → 2024/25** (PROV_PREV 2020 TFR, SOC_GRDP 2020 GRDP), size = population, colour = 6 regions, search box | The one Flourish chart native lacks: an explorable scatter with movement. Retitle to the chapter's claim or place it in soc-st-tfr; its title (fertility) differs from the chapter headline (GRDP gap) | S–M |
| 40 | soc-st-rank | `socStRanks` | Rank dot strip, 9 indicators, 3 accented | KEEP | — | Position on a percentile strip is the right form | — |
| 41 | soc-st-rel | `socStWaffle` | Two waffles, 7 hues | IMPROVE NATIVE | Keep the whole-population waffle as 2 parts (religion vs none). Make "among those with a religion" sorted bars in one hue | Three of the seven religion classes are 1% or less, too thin to read as waffle squares | S |
| 42 | explore | `renderDemoPyramid` (#demo-svg, ▶ 2000–2045) | Pyramid from 3 hand-typed anchor distributions, interpolated, same shape scaled for each province | IMPROVE NATIVE | Replace with UN WPP 2024 single-year age–sex data (national), keep ▶. Drop the province mode, or title it "model" on the chart | **Synthetic data drawn as a measurement.** "(mô hình)" appears only in the dek | M |
| 43 | explore | `renderStudent` (#stu-svg) | Stacked bars 2010–2029 from the synthetic pyramid × assumed enrolment rates | DROP | Restore only with the NSO enrolment series by level | A model of a model, extended to 2029 | S |
| 44 | explore | `renderPopGrowth` (#popg-wrap) | Population line 2000–2050 | DROP | — | Repeats #32 | S |
| 45 | explore | `drawMap` (#map-svg) | Region-coloured selector map | KEEP | — | Selector, not a data map | — |
| 46 | explore | `renderSocSummary` | KPI / rank tiles | KEEP | — | Text tiles | — |

### Policies (money)

| # | Chapter | Chart (fn / id) | Current form | Decision | Proposed form | Rationale | Effort |
|---|---|---|---|---|---|---|---|
| 47 | pol-st-hero | `polStRates` | Step lines, refinancing accented, others grey | KEEP | — | Emphasis form; annotated last cut | — |
| 48 | pol-st-hero | Flourish 30476094 | line-bar-pie copy of #47 | DROP | — | Duplicate | S |
| 49 | pol-st-moves | `polStDiv` | Diverging stacked quarterly columns | KEEP | — | Polarity up/down, identity by colour; "frequency, not strength" stated | — |
| 50 | pol-st-moves | Flourish 30476096 | line-bar-pie copy of #49 | DROP | — | Duplicate | S |
| 51 | pol-st-moves | `polStGroups` | Diverging bars by instrument group | KEEP | — | Shared zero, sorted by net | — |
| 52 | pol-st-cliff | `polStExpiry` | Dots on a **power-0.55 time axis**, sized by count | IMPROVE NATIVE | Linear time axis with an explicit axis break for 2028–2031, or a dated list (the cards already exist) | Non-linear time spacing makes 2027–31 look sooner than it is; only the tick spacing hints at it | S |
| 53 | pol-st-steps | `polStMult` | 8 step multiples, own scales, shared time scrubber | KEEP | — | Own scales stated in the dek; the scrubber is a strong native feature | — |
| 54 | appendix | `ecb-lsr` | Deposit / OMO / refinancing lines | KEEP | — | Owned data, one axis | — |
| 55 | appendix | `ecb-macro` | M2-growth bars + CPI line on one % axis | IMPROVE NATIVE | Two lines (or two panels) | Bar + line pairs two different measures and suggests a causal link | S |
| 56 | appendix | `ecb-sbv-rates` | Policy-rate lines (yMax fixed at 6) | DROP | — | Repeats #47 | S |
| 57 | appendix | `ecb-credit-money` | Credit YTD vs M2 YTD lines | KEEP | — | Owned, one axis | — |
| 58 | appendix | `ecb-vnibor` | Overnight vs OMO lines | KEEP | — | — | — |
| 59 | appendix | `ecb-money-supply` | M2 and deposit levels | KEEP | — | Zero-based | — |
| 60 | appendix | `ecb-fx-detail` | Reserves bars, excluding gold | MERGE | One chart with both definitions (excl. gold / incl. gold), placed in the FS chapter-5 slot | #77 shows incl. gold; two charts of "reserves" with different numbers confuse readers | S |
| 61 | appendix | `ecb-fis-budget` | % of plan bars + even-pace tick | KEEP | — | Bullet form, status colour with value labels | — |
| 62 | appendix | `ecb-fis-pubinv` | % of plan columns by year (last = YTD) | KEEP | Add central vs local as two bullet bars (absorbs #109) | — | S |
| 63 | appendix | `ecb-fis-debt` | Debt range bullet | DROP | — | Repeats #13 | S |
| 64 | explore | `polTimeline` (#pol-tl) | Lane dot timeline by group, coloured by direction | KEEP | — | Explore view with document links | — |

### Financial system (banks)

| # | Chapter | Chart (fn / id) | Current form | Decision | Proposed form | Rationale | Effort |
|---|---|---|---|---|---|---|---|
| 65 | fs-st-hero | `fsStGap` | Credit vs deposit growth lines + grey gap wash + projections | KEEP | — | — | — |
| 66 | fs-st-hero | Flourish 30476098 | line-bar-pie copy of #65 | DROP | — | Duplicate | S |
| 67 | fs-st-depth | `fsStDepth` | Private credit / GDP and M2 / GDP lines + hollow press estimate + scenario | KEEP | — | Estimate and scenario are marked differently from the actuals | — |
| 68 | fs-st-depth | Flourish 30476097 | line-bar-pie, credit/GDP scenario | DROP | — | Duplicate. Its title puts a dashboard scenario ("hits 178% of GDP by 2030") in the headline slot | S |
| 69 | fs-st-depth | `fsStMMult` | 4 monthly mini lines | KEEP | — | — | — |
| 70 | fs-st-flow | `fsStWaffle` + drill into "other services" | Waffle 1 square = 1% that recolours on drill-down | KEEP | — | Native drill keeps the whole in view; better than a one-source Sankey | — |
| 71 | fs-st-flow | Flourish 30475880 (Sankey) | One source → 6 sectors → parts | DROP | — | A one-source Sankey is a bar chart; repeats #70 and #73 | S |
| 72 | fs-st-flow | `fs-st-sgrow` (in `fsStWaffle`) | Paired bars: 2025 full year (grey) vs 7M-2026 (blue) | IMPROVE NATIVE | Compare like for like: 7M-2026 vs 7M-2025 if Finance has it; otherwise two separate panels titled "12 months" and "7 months" | Side-by-side bars invite comparing 12-month and 7-month growth | S |
| 73 | fs-st-flow | `fsStLB`, `fsStOSChart` | Coverage stack, per-bank bars, other-services stack + sub-bars | KEEP | — | "≈" derivations are marked | — |
| 74 | fs-st-why | `fsStMech`, `fsStCf` | Mechanism / counterfactual diagrams | KEEP | — | Diagram earns its place; numbers come from data | — |
| 75 | fs-st-why | `fsStWMult` (`stMiniLine`) | 4 mini lines, **x = index** | IMPROVE NATIVE | Time-scaled x (quarters as dates) | LDR skips 2025-Q3, so Q2 → Q4 draws as one step | S |
| 76 | fs-st-safe | `fsStNpl`, `fsStSMult` | NPL under 2 definitions + 4 soundness minis | KEEP | Add the third definition (incl. VAMC & potential) from #84 | — | S |
| 77 | fs-st-sbv | `fsStRes` | Reserve bars, peak and latest accented | KEEP | Absorbs #60 | — | — |
| 78 | fs-st-pipe / fs-st-2030 | Sequence diagrams; projection multiples + targets | Diagrams / small multiples | KEEP | — | — | — |
| 79 | explore | `fs-ch-levels`, `fs-ch-fx` (indexed), `fs-ch-liq`, `fs-ch-car`, `fs-ch-sbv` | `_drawSeries` lines | KEEP | — | Reference lines for the caps; FX indexed to a common base | — |
| 80 | explore | `fs-ch-growth`, `fs-ch-depth`, `fs-ch-secgrowth` | Lines / table bars | DROP | — | Repeat #65, #67 and #72 | S |
| 81 | explore | `fs-ch-gap` | Credit and deposit level lines + gap bars on one axis | IMPROVE NATIVE | Plot the gap alone (tn VND or % of deposits) | Gap bars (~1k) under 16k-level lines are almost invisible | S |
| 82 | explore | `fsSankey` (deposits → banks → sectors) | Two-sided Sankey | KEEP | — | Two-sided flow is a real Sankey case | — |
| 83 | explore | `fs-ch-rates` | 5 lines (deposit, OMO, O/N, CPI, real rate) | IMPROVE NATIVE | Split the real rate into its own panel; at most 4 series | 5 series + 2 dash styles; the derived real rate competes with the observed rates | S |
| 84 | explore | `fs-ch-npl` | 3 NPL definitions | MERGE | Into #76 | — | S |
| 85 | explore | `fs-ch-roe` | ROE / ROA lines, `yMin: 0` | IMPROVE NATIVE | Auto minimum with a zero line | **Defect:** the −20.1% 2023 ROE is drawn below the axis, off the plot | S |
| 86 | appendix | `drawMiniBars` (#bk-indicators, 10 tiles) | Mini bars, **"zoomed y-range to amplify change"**, gradient | IMPROVE NATIVE | Sparkline (line) with first and last values, no gradient. Put "model" in the tile when `synthetic_history` | Truncated bars exaggerate change; the history is modelled from FY2024 ratios | S |
| 87 | appendix | `drawDonut` #bk-bs-assets / #bk-bs-leq | Donuts with 8+ slices, legend truncated to fit | DROP | Keep the list beside it; optionally one 100% bar | The list already carries the values; too many slices to compare by angle | S |
| 88 | appendix | `drawBSListClickable`, `drawISList`, modal (`inlineBarsSvg`) | In-row mini bars | IMPROVE NATIVE | Zero-based, signed; mark modelled periods | Same abs / truncation defect as #31 (provisions and losses lose their sign) | S |
| 89 | appendix | `drawDonut` #bk-lend-sector / #bk-fund-source | Donuts | IMPROVE NATIVE | 100% stacked bar like its siblings, or sorted bars | Consistency; more than 5 parts | S |
| 90 | appendix | `drawHStack` × 6 | 100% bars, `preserveAspectRatio="none"` | IMPROVE NATIVE | Draw at container width (no stretch) | Stretching distorts the in-bar text and the legend | S |
| 91 | appendix | `drawISList` (#bk-is-list) | Hierarchical list | IMPROVE NATIVE | Add a waterfall: net interest income + fees + other − opex − provisions = pre-tax profit | A waterfall is the form for an income statement | M |
| 92 | appendix | `#bk-table` league table, `#bk-alert-strip` | Sortable table, alert list | KEEP | — | Live prices and sorting; a Flourish table would lose the live update | — |

### Party & Government (party)

| # | Chapter | Chart (fn / id) | Current form | Decision | Proposed form | Rationale | Effort |
|---|---|---|---|---|---|---|---|
| 93 | pg-st-hero | `pgStGrowth` | Growth columns + ≥10% line + 2021–25 average | IMPROVE NATIVE | Show what the target implies: actual 2026 (9M) + the **required average 2027–2030** to reach a ≥10% 2026–30 average | Today it repeats the Economy hero; the required path is the Party-tab question | S |
| 94 | pg-st-gap | `pgStDumb`, `pgStTargets` | Dumbbell 2025 → 2030 target; target cards | KEEP | — | Dumbbell is the right form for gap to target | — |
| 95 | pg-st-wave | `pgStBars` + replay | Monthly columns stacked by 5 levels | KEEP | — | Paired with #97 for comparing levels; annotated peaks | — |
| 96 | pg-st-wave | Flourish 30476099 | line-bar-pie quarterly stack | DROP | — | Repeats #95 and #97 | S |
| 97 | pg-st-wave | `pgStMult` | Per-level quarterly multiples, one shared scale | KEEP | — | — | — |
| 98 | pg-st-pil | `pgStPillars` | Stacked bars by level, sorted [P] | KEEP | — | — | — |
| 99 | pg-st-chain | `pgStFlow` | Mechanism diagram with counts and median lags | KEEP | — | — | — |
| 100 | pg-st-chain | `pgStRows` + replay | Implementation-chain timeline | KEEP | — | Strong native replay | — |
| 101 | pg-st-pipe | `pgStDrafts` | Milestone → expected-adoption bars, session band, today line [P] | KEEP | Wrap the 420 px label column on phones | Richer than the Flourish Gantt (session band, VI) | S |
| 102 | pg-st-pipe | Flourish 30476100 (Gantt) | Gantt copy of #101 | DROP | — | Duplicate | S |
| 103 | explore | `pgGraph` | Time-lane document network | KEEP | — | Time axis + chains; a force layout (Flourish network) would lose time | — |

### Investing (invest)

| # | Chapter | Chart (fn / id) | Current form | Decision | Proposed form | Rationale | Effort |
|---|---|---|---|---|---|---|---|
| 104 | iv-st-hero | `ivStGdp` | Quarterly columns + outlined "Q4 needed" + target line | KEEP | — | Estimate is outlined and labelled | — |
| 105 | iv-st-macro | `ivStDemand` | Single-hue bars | KEEP | — | — | — |
| 106 | iv-st-macro | Flourish 30476104 (public investment, central vs local) | line-bar-pie | DROP (rebuild natively) | Two native bullet bars vs the 75% even pace (reuse `_planBars`) | Two numbers; native gives VI, pace marker and works offline | S |
| 107 | iv-st-market | `ivStVni` | Forecast dots + close mark | KEEP | — | Dot strip; opinions labelled | — |
| 108 | iv-st-stocks | `ivStCards` | Target strips, own scale per stock | KEEP | — | Own scales stated | — |
| 109 | explore | `ivSpark`, `ivPriceSvg` (+MA50/200), `ivRsiSvg`, screen / compare tables | Sparklines, price lines, tables | KEEP | — | Lines may start above zero; live data | — |

### Cross-cutting

| # | Item | Decision | Proposed | Rationale | Effort |
|---|---|---|---|---|---|
| 110 | Dead code `renderBubble`, `updateScatterCharts`, `drawGroupedBars` (no callers; the last uses a zoomed axis) | DROP | Delete | Not rendered; risk of reuse | S |
| 111 | Optional hook: Flourish `draw-the-line` (110479), e.g. "guess Vietnam's population to 2050" or "credit / GDP 2015–2025" | FLOURISH ADD (optional) | EN-only engagement block at the top of the Society or Financial hero | A guessing interaction native doesn't have. Low priority; must use real data, unlike the existing illustrative demo | M |
Rows tagged `[P]` keep their form, but they also need the width-aware fix in Top 10 #4.

### Templates considered and not recommended

projection map / 3D region map: the native map already does custom boundaries; adding metric modes natively is cheaper. Line chart race: no province or bank rank series with enough real time steps; bank histories are modelled. Hierarchy / treemap and marimekko: the native waffles and drill already work. Network and chord: `pgGraph` needs a time axis. Table and cards: native tables have live data and VI. Parliament, radar, pictogram, survey, calendar, number ticker: no fitting data, or native already covers it (count-up, waffles). Heatmap: the native `stHeat` is better.

## 3. Top 10 changes, in priority order

1. **CPI basket and bank in-row trend bars (`inlineBarsSvg`, #31, #88).** Drop `Math.abs()` and the truncated base, and draw signed bars from zero. Today Transport's −6.89% YoY is drawn as its tallest bar. S.
2. **Dual axis on the GDP explorer card (`ecb-gdppc`, #25).** Use growth columns only, or two panels. S.
3. **Synthetic population pyramid and student charts (#42, #43, with #36).** Replace the pyramid with real UN WPP 2024 age–sex data, as one pyramid in chapter 2 and in the explorer. Drop the student chart until NSO enrolment data is sourced. M.
4. **Phone legibility.** About 25 story charts use `stSvg(1000, …)`, so labels come out about 4.5 px on a phone. Port the econ `ecW` pattern (draw at container width, redraw on resize) to the label-heavy ones first: `stHeat`, `stSankey` (switch to ranked lists below 600 px), `socStGrdp`, `socStRanks`, `pgStDrafts`, `pgStPillars`, `pgStRows`, `stMix`, `stBudLine`, `polStDiv`, `fsStDepth`, `fsStNpl`. M–L.
5. **Take the 12 duplicate Flourish charts off the page** (#2, 19, 21, 33, 48, 50, 66, 68, 71, 96, 102, 106) by removing their entries from `data/flourish.json`. This also spares the user publishing 12 charts that add nothing. S.
6. **Upgrade the one Flourish chart that earns its place (#39):** a scatter with a 2020 → 2025 time slider, size = population, colour = region, search. Align its title with the chapter it sits in. S–M.
7. **Bank appendix honesty (#86–#90):** truncated "amplify change" mini bars become sparklines; drop or replace the donuts; stop stretching the 100% bars; mark modelled history on the chart. S.
8. **Axis honesty batch:** the power-0.55 expiry axis (#52), the clipped −20% ROE (#85), index-spaced quarters (#75), and 12-month vs 7-month sector growth side by side (#72). S each.
9. **De-duplicate the appendices** (#24, 27, 30, 44, 56, 60, 63, 80, 84; #93 becomes the "required path" chart). Fewer charts saying the same thing twice, with different numbers in the reserves case. S–M.
10. **One new Flourish chart where it clearly wins: a bar chart race of the 16 NSO activities' share of GDP, 2010–2025 (#29).** It uses real data, and the 16-year ranking change is something the year slider can't show. Optionally add the draw-the-line hook (#111). M.

## 4. Honesty and quality defects (regardless of tool)

| Defect | Where | Test | Fix |
|---|---|---|---|
| Negative values drawn as positive bars (`Math.abs`) with a truncated base (`yMin = min − 25% range`) | `inlineBarsSvg`: CPI basket 24-month trend (Transport min −6.89%, 15 negative months; Post & telecom 18 negative months; Education 6), bank BS/IS lists, line-item modal | Truncated axis + wrong sign | Signed, zero-based bars |
| Dual axis (bars on the left scale, line on the right) | `ecb-gdppc` in `updateSpecialCharts('grdp')` | Dual axis | Two charts, or growth only |
| Truncated bars by design ("zoomed y-range to amplify change") | `drawMiniBars` (#bk-indicators); `drawGroupedBars` (dead) | Truncated axis | Sparklines, or a zero base |
| Synthetic data drawn as if measured | `calcDemo` (3 hand-typed anchor distributions, same shape for every province), `calcStudent` (pyramid × assumed rates, to 2029). "Model" appears only in the dek / ⓘ, not on the chart | Missing basis on the mark | Real data, or "model" in the chart title; drop students |
| Non-linear time axis | `polStExpiry`: `Math.pow(t, 0.55)` | Wrong type (distorted scale) | Linear axis with an explicit break |
| Values clipped off the plot | `fs-ch-roe`: `yMin: 0` with the 2023 ROE at −20.1% | Truncated axis | Auto minimum + zero line |
| Uneven periods spaced evenly | `stMiniLine` in `fsStWMult` (LDR: 2025-Q2, Q4, 2026-Q1, Q2) | Wrong type | Time-scaled x |
| Unlike periods side by side | `fs-st-sgrow`: 2025 full year vs 7M-2026 bars | Title / comparison mismatch | Like-for-like periods, or separate panels |
| Pie misuse | `drawDonut`: balance-sheet donuts with 8+ slices and a truncated legend; lending-by-sector donut | Pie misuse | Bars / list / 100% bar |
| Stretched text | `drawHStack`: `preserveAspectRatio="none"` with in-bar labels | Legibility | Draw at container width |
| Too many classes or series | Revenue waffle: 7 hues incl. squares under 1%; religion waffle: 7 hues incl. classes of 1% or less; `fs-ch-rates`: 5 series | Colour / series rules | Fold into "other"; one hue; at most 4 series |
| Skewed linear scale | `socStMult` density 0–4,500/km² | Readability | Log scale |
| Hard-coded scale | `stBudLine` y 0–3,500 tn VND; `ecb-sbv-rates` yMax 6 | Future clipping | Compute from the data |
| Annotation with no mark | Budget chapter note on land-use fees (474 tn in the 2026 plan vs 233 tn in 2024) has no chart that shows the series | Annotation rule | Add the series to `stBudLine` |
| Colour scale only in prose | `stHeat` (saturates at ±8%) | Missing units / legend | Stepped legend with values |
| Title does not match the chapter's claim | Flourish scatter "All 8 southern provinces below replacement…" sits in the GRDP-gap chapter; Flourish credit/GDP chart puts a dashboard scenario in the headline | Title vs data / claim | Retitle, or move to soc-st-tfr; drop the scenario chart |
| Two reserve figures | `ecb-fx-detail` (excl. gold) vs `fsStRes` (incl. gold) on different tabs | Consistency | One chart, both definitions labelled |
| Decorative gradients | `drawBarChart`, `ecb-gdppc`, `ecb-income-card`, `drawMiniBars` | Marks spec | Flat fills |
| Phone legibility | About 25 story charts in a fixed 1000 px `viewBox` | Accessibility | Width-aware drawing (`ecW`) |
| Illustrative charts in the Flourish account | 7 "Showcase/Demo … (illustrative)" visualisations with invented banking data | Fabricated data | Never embed; delete if not needed |

Verified clean (sample): story bars and columns are zero-based; no 3D; no story chart uses a dual axis; projections are always dashed with hollow points and a "Projection →" divider; part-year values are faded or outlined and labelled; `stFinish` → `stTables` builds a table twin for each story root's charts (not checked one by one).
