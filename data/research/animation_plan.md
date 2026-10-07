# Animation plan — every chart: last 8 periods + projections to end-2030

As of 2026-10-07 · Research (phase 1 = inventory and work plan only; no data file, agent file or page edited).
Request (user): "animation for all charts if possible, showing how the indicator moved at least during the last
8 periods and projections until end of 2030 (period can be quarterly, semi-annual, or annual)."
Machine-readable version: `data/research/animation_plan.json` (one entry per chart, same ids as
`freshness_audit.json`). Built from `freshness_audit.json` (142 charts), `inventory.json`, `source_log.json`,
`data/*.json`, `strategy_directives.json`, `server.py` and the `render*Story` code (read-only).

## 1. Summary by tab

| Tab | Charts | Animatable, ≥8 periods held now | Animatable, needs history (<8) | No animation | Projection to 2030 already held | Needs projection work (partial / source to add) | History only (no credible projection) | Dashboard model/scenario only | Already animated today |
|---|---|---|---|---|---|---|---|---|---|
| Economy (econ) | 32 | 26 | 3 | 3 | 4 | 18 | 6 | 1 | 5 |
| Society (social) | 12 | 3 | 7 | 2 | 3 | 2 | 5 | 0 | 2 |
| Policies (money) | 15 | 10 | 2 | 3 | 0 | 4 | 7 | 1 | 6 |
| Finance (banks) | 37 | 23 | 5 | 9 | 1 | 2 | 21 | 4 | 3 |
| Investing (invest) | 9 | 2 | 2 | 5 | 0 | 1 | 3 | 0 | 0 |
| Simulation (sim) | 27 | 19 | 6 | 2 | 0 | 6 | 16 | 3 | 3 |
| Party/Strategy (party) | 10 | 6 | 1 | 3 | 1 | 1 | 5 | 0 | 4 |
| **Total** | **142** | **89** | **26** | **27** | **9** | **34** | **63** | **9** | **23** |

Projection columns count animatable charts only (115). "History only" includes 9 charts where a projection is
meaningless (`n/a`: document counts, decision counts, % of an annual plan). Eight charts need a user decision (§3).

Reading the counts:
- **89 charts can be animated now** with data already held. 23 of them already have a time control (BoP year replay,
  pyramid ▶, GDP-sector slider, Flourish race, CPI/monetary/fiscal `addTimeline` cards, Policy scrubber, Party replays,
  `fsLv` scrubber, Simulation sliders). Two of the 23 (F36, F37) animate **synthetic** bank periods (see §3).
- **26 charts need history** before they reach 8 periods. Most of the effort is in provincial cross-sections
  (Society: 34 merged provinces) and bank-level statements (Finance/server). The priority-watch gaps are E09 (BoP
  quarterly, 2 of 8 quarters) and E16 (FDI registered by component, 1 period).
- **Only 9 charts hold a projection to 2030 today** (GDP growth, GDP per person, GDP in USD, population, age pyramid,
  Finance projection board, Party growth chart). 34 more can get one from a credible source (mostly the IMF WEO of
  13/10/2026 and official 2026–2030 targets already held in `economy.json`).
- **27 charts are not animated**: KPI tiles (their series animate elsewhere), text lists, diagrams, tables, and two
  compositions that exist for one period only (F09 "other services", S10 religion — census-based).

## 2. Projection-source policy (applies to every agent)

Order of preference for any line drawn past the last actual period:
1. **Institutional forecast with a horizon to 2030 or beyond** — IMF WEO (April/October; database to 2031).
   Use the latest vintage; keep the previous vintage only as a note. The October 2026 WEO is due **13/10/2026**:
   every IMF row (`gdp_growth_pct_imf`, `gdp_usd_bn_imf`, `gdp_per_capita_usd_imf`, `cpi_pct_imf`, Finance
   `real_gdp_growth_pct`, `cpi_pct`) should be refreshed from it, and its CPI 2028–2030 fills the current gaps.
2. **Shorter-horizon institutions** — World Bank (EAP/VEU, ~3 years), ADB ADO and AMRO (~2 years), Fed SEP,
   EIA STEO, World Bank Commodity Markets Outlook (~2 years). Draw only the years they publish; never extend.
3. **Official targets** — NQ 25/2026/QH16 (5-year plan), NQ 26/2026/QH16 (finance plan), NQ 27/2026/QH16 (public
   investment), NQ 10-NQ/TW (FDI), NQ 26-NQ/TW (tourism, 22/08/2026), NQ 68-NQ/TW (enterprises), NQ 06-NQ/TW
   (urbanisation), QĐ 1679/QĐ-TTg (population, TFR 2.1), QĐ 493/QĐ-TTg (export growth), SBV yearly credit target,
   QĐ 986/QĐ-TTg (banking; only NPL 3% is held). Draw a target as a **target** (dashed marker or band), not a path.
   **Five-year totals and averages stay totals/averages**: show them as a band or annotation; do not split a total
   into invented yearly values. A yearly band derived as total ÷ 5 is allowed only if labelled "derived average".
4. **Demography** — UN WPP 2024 medium (with low/high) and the NSO 2019–2069 projection, both already held.
   For annual frames 2026–2030 add WPP single years instead of interpolating between 2025 and 2030.
5. **Dashboard models** — only where a model exists and is documented: CPI monthly model (to Dec-2027),
   Simulation deposit-rate model (6 months, to Mar-2027), Finance counterfactual and credit scenario. Label as
   "dashboard model/scenario". Do **not** stretch model horizons to 2030.
6. Otherwise **"none — history only"**. This covers: policy rates, interbank and FX rates, reserves, M2 levels,
   bank ratios except NPL, provincial cross-sections, asset prices (VN-Index, gold, SJC, real estate — projecting
   these would read as investment advice), and bank/stock-level data.

Rules: no invented numbers, no interpolation of missing history (collect it or leave the gap visible), keep provider
and vintage per point (bank ratios mix Yuanta/VDSC/MBS/VIS; FDI first release vs revised), never splice definitions
(three remittance series, two reserves definitions, IMF general government vs state budget, SBV credit vs IMF FSI
loans). IMF WEO is not reachable from the sandbox (source_log): agents use WEO PDFs/tables via search excerpts or
the main session fetches the database.

## 3. Decisions needed from the user

1. **F36 / F37 — bank statements and breakdowns already animate synthetic periods** (`server.py`: only FY2024 is
   reported; FY2019–FY2023 and quarters to Q1/2026 are "FY2024 × fixed scale factors"). Disable these timelines until
   reported statements are collected, or keep them with a "model" label? Research recommends disabling.
2. **E27 — FDI province drill-down** uses `PROVS.econ` modelled values (2010 base × growth), not published data.
   Remove the province animation until official provincial FDI history exists, or label it "model"?
3. **F01 (and F04, F20) — Finance credit/deposit/credit-to-GDP paths to 2030 are assumptions** (credit 15%/yr,
   deposits 11.11%/yr). Keep them as a labelled "scenario" to 2030, or stop at the 2026 official credit target?
4. **E18 — CPI model inputs (`CPI_FC_SCEN/TEMPLATE/DRIVERS`) and `CPI_BASKET` live in the page**, unowned. Move them
   into `economy.json` so the Economy agent owns them (proposal for the main session)?
5. **E30 — `GDP_SECTORS` page constant holds 46 values per sector (2000–2045)**; 2026–2045 have no source (trimmed at
   runtime by `GDP_LAST_YEAR`, so not shown). Delete the unsourced tail and move the constant into `economy.json`?
6. **P13 / F32 / F17 — one FX-reserves definition** (incl./excl. gold; IMF IL vs SBV statements) must be chosen before
   the reserves charts are animated together.
7. **S03 / S04 / S06–S08, S12 — provincial history on the 34 merged provinces.** Levels (GRDP, population) can be
   summed from the 63 old provinces; rates (TFR, life expectancy, sex ratio) cannot. Recompute with births/women
   weights (Society, effort L), or animate on the old 63-province basis up to 2024?
8. **Scope of "8 periods" for monthly charts.** 41 animatable charts are monthly. Research reads the request as "≥8 periods at
   the chart's own frequency" (8 months is met by all monthly charts marked ready). If the user wants **8 quarters /
   years** for monthly series too, the monthly charts with short windows (24 months held: CPI, FX, rates, credit)
   either switch to quarter-end frames (meets 8 quarters) or need 2+ more years of history.

## 4. Per-agent work lists (priority order)

Effort: S ≤ half a day (data held, mostly wiring), M = collect ≤ ~10 points or add projection rows, L = multi-period
cross-section or method work. Every item is a proposal for the owning agent; nothing is applied without the user's
yes (standing "ask before updating" rule).

### Economy (`data/economy.json`, `data/invest_macro.json`)
1. **After 13/10/2026 — IMF WEO October 2026** (S): refresh all `projections.series.*_imf` rows, add 2031, fill
   `cpi_pct_imf` 2028–2030; unlocks E01, E19, E20, E21, E24, E26, G01, F20 (Finance copies).
2. **FDI (priority watch #1)** — E15 (S): fill `fdi_disbursed_busd_target` from NQ 10-NQ/TW as a 2026–2030 cumulative
   band (no yearly split); E16 (M): registered by component (new / adjusted / capital contributions) + disbursed for
   FY2018–FY2025 from FIA/NSO year-end reports, with vintage per year; registered 200–300 bn target band.
3. **Tourism + remittances (priority watch #2–#3)** — E08 (M): copy NQ 26-NQ/TW 45–50 million international-visitor
   target (confirm target year and URL) into `projections.targets`; outbound departures 2018–2025; keep SBV national,
   KNOMAD/BoP and HCMC remittance series separate; no remittance projection beyond KNOMAD's year.
4. **BoP quarterly** — E09 (M): Q4-2023 … Q3-2025 and Q2-2026 from SBV quarterly BoP (cross-check IMF BOP); current
   account, goods, secondary income (remittance proxy), travel.
5. **Public debt** — E13 (M): public debt % GDP 2018–2025 (MoF bulletins); 2026 plan; 60% ceiling (NQ 26/2026/QH16);
   IMF WEO gross debt.
6. **Budget and investment targets** — E12 (S) deficit average band 2027–2030; E14, E05, E03 (M) investment-share
   bands (NQ 27/2026/QH16); E10/E11 (S/M) five-year totals as annotation, IMF general-government rows labelled.
7. **E29 (M)**: fix the broken labour card first; IIP 11–12% band (NQ 25), enterprise target (NQ 68-NQ/TW).
8. **E04 / E28 (M)**: export growth target band (QĐ 493) applied to the 2025 base, labelled "target path".
9. **I01 / I02 (M, with Investing)**: quarterly GDP growth Q1-2024 … Q4-2025; real growth of demand components
   2018–2025.
10. **P14 / P15 / M17 (M, with Policy)**: monthly YTD budget execution Jan–Aug 2026 and 2025; public-investment
    disbursement % of plan 2018–2023 (collect once; Policy and Finance copy).
11. **E27 (L, after user decision 2)**: official provincial FDI by year on the 34-province basis.
12. **E18 / E30 (after decisions 4–5)**: take ownership of the CPI model inputs and GDP-sector constants.

### Finance (`data/finance.json`, `data/simulation.json`)
1. **After 13/10** (S): refresh `projections.series.real_gdp_growth_pct` and `cpi_pct` from WEO Oct-2026 (F20, M19).
2. **F01 / F04 (after decision 3)** (S/M): label credit/deposit/credit-to-GDP paths "scenario" or cut at 2026;
   F04 needs WB private credit/GDP and M2/GDP 2023–2025 or one consistent SBV/NSO credit-to-GDP 2018–2025.
3. **Rates history** — P08/F28 (M): private-bank 12-month deposit rate monthly Oct-2024 … Jun-2026 (MBS); M23 (M):
   SBV average new-loan rate monthly Oct-2024 … Sep-2026.
4. **Bank metrics quarterly** — F13/M22 (M): COF, NIM, CASA, LDR back to Q3-2024 with provider per point.
5. **NPL** — F15/F29 (M): SBV on-balance NPL quarterly 2023-Q3 … 2024-Q4; broad NPL semi-annual 2023–2025; NPL 3%
   2030 target already held.
6. **Sim panel gaps** (M each): M03 10-year G-bond monthly; M12 SJC monthly; M15 Hanoi primary price quarterly;
   M16 corporate-bond issuance/outstanding 2018–2025; M19 core CPI monthly; M07 Aug/Sep-2026; M13/M14 single gaps.
7. **Credible short-horizon projections** (S): M04 Fed SEP median path + longer run; M21 EIA STEO Brent; M11 World Bank
   CMO gold — published years only.
8. **F24 (M)**: LDR / short-term-funds semi-annual points 2023-06, 2024-06, 2024-12, 2025-06.
9. **F26 (M)**: SBV balance-sheet empty sub-series and 2025 actuals (IMF IFS).
10. **F08, F35–F37 (L, with main session; after decision 1)**: reported bank statements FY2018–FY2025 and 8 quarters;
    sector loan notes for 17 banks Q3-2024 … Q1-2026.
11. **M18 (L)**: monthly budget balance and Treasury deposits (with Economy).

### Society (`data/society.json`)
1. **S01 / S05 / S11 (S)**: add UN WPP 2024 single years 2026–2030 (population; age–sex 2018–2030) so frames are
   annual without interpolation.
2. **S04 national ticks (M)**: national TFR, sex ratio at birth, life expectancy, urban % for 2017–2025 (NSO PxWeb,
   WDI); targets QĐ 1679 (TFR 2.1; verify the other numeric targets) and NQ 06 (urban >50%).
3. **S06 / S07 / S08 / S12 (L, after decision 7)**: GRDP and GRDP per person 2018, 2019, 2021–2023 on the 34-province
   basis (levels additive; per person needs population).
4. **S03 / S04 provinces (L, after decision 7)**: provincial TFR, life expectancy, urban %, sex ratio 2017–2023.
5. **S09 (L)**: yearly world ranks 2018–2025 from WDI; projected GDP ranks from IMF WEO and population rank from WPP.

### Policy (`data/policy.json`)
1. **P01 / P02 / P05 (S)**: no data work — histories are complete; confirm each instrument's step series covers
   2021–2026 for the monthly frames; add scheduled future values (VAT expiry 31/12/2026, LDR cap 01/12/2026) as
   "scheduled", not forecast.
2. **P15 (M, with Economy)**: public-investment disbursement % of plan 2018–2023 (one collection shared with M17).
3. No projections for rates or instruments (SBV publishes no path) — history only.

### Strategy (`strategy_directives.json`)
1. **G02 (M, with Economy and Society)**: 2018–2024 actuals for each 2030 target indicator (manufacturing share —
   already in `GDP_SUB_OFFICIAL`; urbanisation; digital-economy share; TFP contribution; private-sector share);
   end-point target only, no path between 2025 and 2030 except GDP per person (IMF).
2. **NQ 26-NQ/TW tourism and NQ 68-NQ/TW enterprise targets**: confirm wording/year so Economy can copy them.
3. G04/G05/G08/G10 are already animated — no work.

### Investing (`invest_research.json`; macro block owned by Economy)
1. **I01 (M, Economy collects)**: 8 quarters of GDP growth so the Q1–Q4 chart can run 2024 → 2026.
2. **I02 (M)**: demand-component growth history.
3. No projections for VN-Index or stocks (I04, I06) — investment-advice rule.

### Simulation (data owned by Finance in `simulation.json`; listed separately as requested)
1. **Panel gaps** — M03, M12, M15, M16, M18, M19 (core CPI), M22, M23 (see Finance 3–6, 11).
2. **Projections** — keep the 6-month model (M01, M24, M25) as is; do not extend to 2030. Add only Fed SEP (M04),
   EIA STEO (M21), World Bank CMO (M11) for their published years.
3. **M17** copies the shared public-investment history (Economy/Policy).

### Ed (page; proposals for the main session)
1. One shared animation helper on top of the existing `stTimeCtl` / `addTimeline`: frames = last N periods (default
   the last 8 at the chart's frequency, all held periods on demand) + projection frames to 2030 drawn dashed with
   a vintage label ("IMF WEO Oct-2026", "target NQ 25/2026/QH16", "dashboard scenario"); targets as markers/bands.
2. Charts are complete at rest (last actual + projections); ▶ replays; respect `prefers-reduced-motion` (already
   the convention in the story code).
3. Disable F36/F37 synthetic timelines pending decision 1.

## 5. Proposed rollout order for Ed

1. **Economy tab first** — 26 charts ready now, the only tab with projections held for its headline charts
   (E01, E20, E21, E24) and the priority-watch charts (E08, E15). Start right after the 13/10 IMF WEO refresh so the
   first release uses the new vintage. Wave 1: E01, E20, E21, E24, E19/E26, E15, E14, E12, E05, E07, E17.
2. **Party/Strategy** — G01 reuses the Economy projections (S); the replays already exist.
3. **Finance (banks)** — 23 ready (mostly history-only, monthly/annual); F20 projection board and F01 once
   decision 3 is made; F06/F07 step-through by month.
4. **Policies (money)** — 10 ready; most cards already have `addTimeline`; mainly restyling to the shared helper.
5. **Simulation** — 19 ready monthly panels; the model already scrubs; add Fed/Brent/gold short horizons later.
6. **Society** — population and pyramid now (S01, S05, S11); provincial charts wait for the L-effort history and
   decision 7.
7. **Investing** — last: two charts need history (I01, I02), the rest are text, cards or live data.


## 6. Per-chart table

Columns: kind · animation · frequency · periods held (first–last, count) · ≥8 · projection status · effort · owner · priority. Full detail (missing periods, sources with reachability, projection text, notes) is in the JSON.


### Economy (econ)

| Id | Chart | Kind | Animation | Freq | Held | ≥8 | Projection | Effort | Owner | P |
|---|---|---|---|---|---|---|---|---|---|---|
| E01 | stGrowth (growth 2011–25 + 9M bar + forecasts + target) | time_series | timeline | A (Q for 2026) | 2010–2025 (+Q1–Q3 2026, 9M-2026) (16) | yes | exists_to_2030 | S | Economy, Ed | 1 |
| E02 | Hero KPI (GDP level, 9M growth) | diagram/table | none | A | 2025–9M-2026 (1) | n/a | n/a | — | Economy | 5 |
| E03 | stEquation (C+I+G+X−M blocks) | cross_section | step through years | A | 2015–2025 (11) | yes | partial | M | Economy, Ed | 3 |
| E04 | stGiants (X, M vs GDP) | time_series | timeline | A | 2015–2025 (11) | yes | partial | M | Economy, Ed | 3 |
| E05 | stMix (% of GDP 2015–25) | time_series | timeline | A | 2015–2025 (11) | yes | partial | S | Economy, Ed | 3 |
| E06 | stSankey + net/gross + year replay | cross_section | step through years (exists) | A | 2015–2025 (11) | yes | none_history_only | S | Economy, Ed | 4 |
| E07 | stBopMult (4 flows 2015–25) | time_series | timeline | A | 2015–2025 (11) | yes | partial | M | Economy, Ed | 2 |
| E08 | stBopFacts (tourism, remittances, labour sparklines) | time_series | timeline | A (M/Q available for arrivals; Q for HCMC remittances) | 2015–2025 (+9M-2026) (11) | yes | to_add | M | Economy, Ed | 1 |
| E09 | ecBopQ (latest-quarter BoP tiles) | time_series | timeline | Q | Q4-2025–Q1-2026 (2) | **no** | none_history_only | M | Economy | 1 |
| E10 | stWaffles (revenue/spending by year) | cross_section | step through years | A | 2018–2026 (plan/estimate) (9) | yes | partial | S | Economy, Ed | 3 |
| E11 | stBudLine (revenue, spending, deficit, land-use fees) | time_series | timeline | A | 2018–2026 (9) | yes | partial | M | Economy, Ed | 3 |
| E12 | ecDef (deficit by basis + target) | time_series | timeline | A | 2018–2026 (9) | yes | partial | S | Economy | 2 |
| E13 | ecDebt (public-debt plan bullet) | time_series | timeline | A | — | **no** | to_add | M | Economy | 2 |
| E14 | stInvArea (investment by owner + 9M point) | time_series | timeline | A | 2015–2025 (+9M-2026 growth) (11) | yes | partial | M | Economy, Ed | 2 |
| E15 | ecFdi (FDI disbursed columns + 9M + target band) | time_series | timeline | A (M YTD available) | 2010–2025 (+9M-2026) (16) | yes | to_add | S | Economy, Ed | 1 |
| E16 | ecFdiReg (registered by component + disbursed, derived ratio) | time_series | timeline | A | 9M-2026–9M-2026 (1) | **no** | to_add | M | Economy | 1 |
| E17 | stHeat (CPI groups × 24 months) | time_series | timeline | M | T10/24–T9/26 (24) | yes | none_history_only | S | Ed, Economy | 3 |
| E18 (D) | stFan (CPI + dashboard projection to Dec-2027) | model | model scrub | M | T10/24–T9/26 (24) | yes | model_only | M | Economy, Ed | 2 |
| E19 | ecCpiY (annual CPI + institution forecasts + target) | time_series | timeline | A | 2010–2025 (16) | yes | partial | S | Economy | 1 |
| E20 | ecPcChart (GDP per person + IMF path + 8,500 target) | time_series | timeline | A | 2010–2025 (16) | yes | exists_to_2030 | S | Economy, Ed | 1 |
| E21 | ecUsdChart (GDP USD + IMF path) | time_series | timeline | A | 2015–2025 (11) | yes | exists_to_2030 | S | Economy, Ed | 1 |
| E22 | ecNow (KPI tiles vs targets) | diagram/table | none | YTD | 9M-2026–9M-2026 (1) | n/a | n/a | — | Economy | 5 |
| E23 | What to watch (VAT expiry, 2026 deficit, BoP) | diagram/table | none | event | — | n/a | n/a | — | Ed, Economy | 5 |
| E24 | ecb-gdppc (growth card, year slider ≤2025) | time_series | timeline (exists) | A | 2010–2025 (16) | yes | exists_to_2030 | S | Ed | 2 |
| E25 | ecb-income-card | time_series | timeline | A (biennial survey years) | 2010–2025 (12) | yes | none_history_only | S | Economy, Ed | 4 |
| E26 | ecb-cpi-card (annual CPI bars) | time_series | timeline | A | 2010–2025 (16) | yes | partial | S | Ed | 3 |
| E27 (D) | ecb-fdi (FDI registered, province drill-down) + KPI tiles | cross_section | step through years | A | 2010–2025 (16) | yes | to_add | L | Economy, Ed | 2 |
| E28 | ecb-trade (exports/imports) + KPI | time_series | timeline | A (M available) | 2010–2025 (16) | yes | partial | M | Economy, Ed | 3 |
| E29 | ecb-labour (enterprises, unemployment, PCI, IIP) | time_series | timeline | A | 2010–2025 (16) | yes | partial | M | Economy, Ed | 2 |
| E30 (D) | drawGdpChart (#gdp-sectors, slider 2000–2025) | time_series | timeline (exists) | A | 2010 (2000 for 4 sectors)–2025 (16) | yes | partial | M | Economy, Ed | 3 |
| E31 | Flourish bar-chart race 30476438 (16 activities 2010–25) | time_series | timeline (exists) | A | 2010–2025 (16) | yes | none_history_only | S | Main session | 5 |
| E32 | CPI explorer tiles + basket table (renderCpiAt, kpi-cpi-*) | time_series | timeline (exists) | M | T10/24–T9/26 (24) | yes | none_history_only | S | Economy, Ed | 5 |

### Society (social)

| Id | Chart | Kind | Animation | Freq | Held | ≥8 | Projection | Effort | Owner | P |
|---|---|---|---|---|---|---|---|---|---|---|
| S01 | socStPop (population 2000–25 + NSO/UN projections) | time_series | timeline | A (proj. 5-yearly) | 2000–2025 (26) | yes | exists_to_2030 | S | Society, Ed | 1 |
| S02 | Hero dek (growth %/yr, rank) | diagram/table | none | A | 2024–2025 (1) | n/a | n/a | — | Society | 5 |
| S03 (D) | socStStrip (TFR by province, 2.1 line) | cross_section | step through years | A | 2024–2024 (1) | **no** | none_history_only | L | Society | 3 |
| S04 | socStMult (urban, life expectancy, sex ratio, density) | cross_section | step through years | A | 2024–2024 (1) | **no** | partial | L | Society | 3 |
| S05 | socStAge (age–sex 2025 vs 2050) | distribution | step through years | 5-yearly (+2019, 2023) | 2000–2050 (13) | yes | exists_to_2030 | S | Society, Ed | 2 |
| S06 | socStMap (GRDP / TFR toggle) | cross_section | step through years | A | 2020–2025 (3) | **no** | none_history_only | L | Society | 3 |
| S07 | socStGrdp (34 provinces GRDP per person) + 9M growth note | cross_section | step through years | A | 2020–2025 (3) | **no** | none_history_only | L | Society, Ed | 3 |
| S08 | Flourish scatter 30475882 (GRDP pc × TFR, 2020→2025 slider) | cross_section | step through years (exists) | A | 2020–2025 (3) | **no** | none_history_only | M | Society, Main session | 3 |
| S09 | socStRanks (world rank strip) | cross_section | step through years | A | prev (2018/2020)–now (2023/2025) (2) | **no** | to_add | L | Society | 4 |
| S10 | socStWaffle (religion) | distribution | none | census | 2009–2023 (3) | n/a | n/a | — | Society | 5 |
| S11 | renderDemoPyramid (#demo-svg, ▶ 2000–2050) | distribution | step through years (exists) | 5-yearly | 2000–2050 (13) | yes | exists_to_2030 | S | Society | 4 |
| S12 | drawMap selector + renderSocSummary tiles | cross_section | step through years | A | 2024/2025–2025 (1) | **no** | none_history_only | L | Society, Ed | 4 |

### Policies (money)

| Id | Chart | Kind | Animation | Freq | Held | ≥8 | Projection | Effort | Owner | P |
|---|---|---|---|---|---|---|---|---|---|---|
| P01 | polStRates (policy-rate steps) | time_series | timeline | M (step series) | 2021-01–2026-09 (69) | yes | none_history_only | S | Policy, Ed | 2 |
| P02 | polStDiv (easing/tightening per quarter) | time_series | timeline | Q | 2023-Q1–2026-Q4 (15) | yes | n/a | S | Ed | 3 |
| P03 | polStGroups | cross_section | step through years | Q | 2023-Q1–2026-Q4 (15) | yes | n/a | S | Ed | 4 |
| P04 | polStExpiry (+ cards) | diagram/table | none | event | — | n/a | n/a | — | Policy | 5 |
| P05 | polStMult (8 dials, scrubber) | time_series | timeline (exists) | M | 2023–2026-10 | yes | partial | S | Policy, Ed | 3 |
| P06 | What to watch (future-dated moves) | diagram/table | none | event | — | n/a | n/a | — | Policy | 5 |
| P07 | Stance, polTimeline (#pol-tl), instrument tables, news | diagram/table | none | event | — | n/a | n/a | — | Policy | 5 |
| P08 | ecb-lsr (deposit / OMO / refinancing lines) + kpi-lsr-* | time_series | timeline (exists) | M | T10/24–T9/26 (24) | yes | model_only | M | Finance | 2 |
| P09 | ecb-macro (M2 growth + CPI, annual) + FX/CPI/reserves KPI | time_series | timeline | A | 2015–2025 (11) | yes | partial | S | Finance, Economy | 3 |
| P10 | ecb-credit-money (credit YTD vs M2 YTD) | time_series | timeline (exists) | M | T11/24–T9/26 (23) | yes | partial | S | Finance | 3 |
| P11 | ecb-vnibor (overnight vs OMO) | time_series | timeline (exists) | M | 2024-10–2026-09 (24) | yes | none_history_only | S | Finance | 4 |
| P12 | ecb-money-supply (M2, deposits levels) | time_series | timeline (exists) | M | T12/24–T7/26 (20) | yes | none_history_only | S | Finance | 4 |
| P13 (D) | ecb-fx-detail (reserves excl. gold) + kpi-fxr | time_series | timeline | A (M in Finance) | 2010–2025 (16) | yes | none_history_only | M | Economy, Finance, Ed | 2 |
| P14 | ecb-fis-budget (% of plan, even pace) | time_series | timeline (exists) | M YTD | 9M-2026–9M-2026 (1) | **no** | n/a | M | Economy | 3 |
| P15 | ecb-fis-pubinv (% of plan by year) | time_series | timeline | A + M YTD | 2024–9M-2026 (5) | **no** | partial | M | Policy, Economy | 3 |

### Finance (banks)

| Id | Chart | Kind | Animation | Freq | Held | ≥8 | Projection | Effort | Owner | P |
|---|---|---|---|---|---|---|---|---|---|---|
| F01 (D) | fsStGap (credit vs deposit growth + projections) | time_series | timeline | A | 2015–2025 (11) | yes | model_only | S | Finance, Ed | 1 |
| F02 | Hero KPI credit/GDP 146% | diagram/table | none | A | 2025–2025 (1) | n/a | n/a | — | Finance | 5 |
| F03 | fsAltBlock (deposit alternatives 2010–25 + 2026 YTD) | time_series | timeline | A | 2010–2025 (+2026 YTD) (16) | yes | none_history_only | S | Finance, Ed | 3 |
| F04 | fsStDepth (private credit/GDP, M2/GDP + press 146%) | time_series | timeline | A | 2015–2022 (8) | yes | model_only | M | Finance | 2 |
| F05 | fsStMMult (4 monthly minis) | time_series | timeline | M | T11/24–T9/26 (23) | yes | none_history_only | S | Finance, Ed | 3 |
| F06 | fsStWaffle (credit by sector) | cross_section | step through years | M | 2025-01–2026-07 (19) | yes | none_history_only | S | Finance, Ed | 2 |
| F07 | fs-st-sgrow (2025 FY vs 2026 YTD by sector) | cross_section | step through years | M | 2025-01–2026-07 (19) | yes | none_history_only | S | Finance, Ed | 3 |
| F08 | fsStLB (listed banks vs SBV total) | cross_section | step through years | Q | Q2-2026–Q2-2026 (1) | **no** | none_history_only | L | Finance | 4 |
| F09 | fsStOSChart (inside "other services") | cross_section | none | M | 2026-07–2026-07 (1) | n/a | n/a | — | Finance | 5 |
| F10 | fsStFacts (flow facts) | diagram/table | none | — | — | n/a | n/a | — | Finance | 5 |
| F11 | fsStMech (mechanism diagram with data) | diagram/table | none | — | — | n/a | n/a | — | Finance | 5 |
| F12 | fsStCf (counterfactual) | model | model scrub | A | — | n/a | model_only | S | Finance, Ed | 4 |
| F13 | fsStWMult (cost of funds, NIM, CASA, LDR) | time_series | timeline | Q | 2024-Q4–2026-Q2 (6) | **no** | none_history_only | M | Finance | 3 |
| F14 | fsStDrivers (10 ranked causes) | diagram/table | none | — | — | n/a | n/a | — | Finance | 5 |
| F15 | fsStNpl (NPL, 2 definitions) | time_series | timeline | A (Q for SBV) | 2015–2025 (11) | yes | partial | S | Finance | 2 |
| F16 | fsStSMult (IMF FSI minis) | time_series | timeline | A | 2015–2025 (11) | yes | none_history_only | S | Finance, Ed | 3 |
| F17 | fsStRes (year-end reserves incl. gold) | time_series | timeline | A | 2014–2025 (12) | yes | none_history_only | S | Finance, Ed | 3 |
| F18 | fsPipe (plumbing diagram facts) | diagram/table | none | — | — | n/a | n/a | — | Finance | 5 |
| F19 | fsLvDraw (who controls money supply) | diagram/table | timeline (exists) | M | 2023-01–2026-10 (46) | yes | n/a | S | Finance | 5 |
| F20 | Projection multiples vs targets | model | timeline | A | 2015–2025 (11) | yes | exists_to_2030 | S | Finance, Ed | 1 |
| F21 | What to watch | diagram/table | none | event | — | n/a | n/a | — | Finance | 5 |
| F22 | fs-ch-levels (M2, deposits, credit levels) | time_series | timeline | M | T12/24–T9/26 (22) | yes | none_history_only | S | Finance, Ed | 4 |
| F23 | fs-ch-fx (indexed USD/VND, DXY) | time_series | timeline | M | 2024-10–2026-09 (24) | yes | none_history_only | S | Finance, Ed | 3 |
| F24 | fs-ch-liq (LDR, short-term funds for MLT loans) | time_series | timeline | H (irregular) | 2016-12–2026-06 (14) | yes | none_history_only | M | Finance | 4 |
| F25 | fs-ch-car | time_series | timeline | A | 2015–2026-03 (11) | yes | none_history_only | S | Finance, Ed | 4 |
| F26 | fs-ch-sbv (SBV balance sheet) | time_series | timeline | A | 2014–2024 (11) | yes | none_history_only | M | Finance | 4 |
| F27 | fs-ch-gap (credit–deposit gap) | time_series | timeline | M | T1/25–T7/26 (16) | yes | none_history_only | S | Finance, Ed | 4 |
| F28 | fs-ch-rates (deposit, OMO, O/N, CPI, real rate) | time_series | timeline | M | 2024-10–2026-09 (24) | yes | model_only | S | Finance, Ed | 3 |
| F29 | fs-ch-npl (3 definitions) | time_series | timeline | A + Q | 2015–2026-06 (11) | yes | partial | M | Finance | 3 |
| F30 | fs-ch-roe (ROE/ROA) | time_series | timeline | A | 2015–2025 (11) | yes | none_history_only | S | Finance, Ed | 4 |
| F31 | fs-ch-omo (SBV operations) | time_series | timeline | M (from daily) | 2024-08–2026-10 (27) | yes | none_history_only | S | Finance, Ed | 4 |
| F32 | fs-ch-res (monthly reserves) | time_series | timeline | M | 2014-01–2026-07 (151) | yes | none_history_only | S | Finance, Ed | 3 |
| F33 | #bk-table league table (28 banks) | diagram/table | none | A | FY2024–FY2024 (1) | n/a | n/a | — | Main session, Finance | 5 |
| F34 | #bk-alert-strip | diagram/table | none | A | FY2024–FY2024 (1) | n/a | n/a | — | Main session | 5 |
| F35 | #bk-indicators (10 sparklines) | time_series | timeline | A | FY2024–FY2024 (1) | **no** | none_history_only | L | Finance, Main session | 4 |
| F36 (D) | #bk-bs-assets/leq, #bk-is-list (+ donuts, modal) | time_series | timeline (exists) | A/Q | FY2024–FY2024 (quarters to Q1/2026 are synthetic) (1) | **no** | none_history_only | L | Main session, Finance | 2 |
| F37 (D) | #bk-lend-*/#bk-fund-* (breakdown 100% bars) | cross_section | step through years (exists) | A | FY2024–FY2024 (1) | **no** | none_history_only | L | Main session, Finance | 2 |

### Investing (invest)

| Id | Chart | Kind | Animation | Freq | Held | ≥8 | Projection | Effort | Owner | P |
|---|---|---|---|---|---|---|---|---|---|---|
| I01 | ivStGdp (Q1–Q3 2026 + Q4 needed) | time_series | timeline | Q | Q1-2026–Q3-2026 (3) | **no** | partial | M | Economy, Investing | 2 |
| I02 | ivStDemand (9M demand components) | time_series | timeline | A / 9M | 9M-2026–9M-2026 (1) | **no** | none_history_only | M | Economy, Investing | 3 |
| I03 | ivStReason (macro support/risks) | diagram/table | none | — | — | n/a | n/a | — | Investing | 5 |
| I04 | ivStVni (forecasts + end-Sep close) | time_series | timeline | M (D available) | 2021-01–2026-09 (69) | yes | none_history_only | S | Investing, Ed | 4 |
| I05 | ivStReason (market) | diagram/table | none | — | — | n/a | n/a | — | Investing | 5 |
| I06 | ivStCards (POW, PVT, GAS, GEE targets) | diagram/table | none | — | — | n/a | n/a | — | Investing | 5 |
| I07 | Event calendar | diagram/table | none | event | — | n/a | n/a | — | Investing | 5 |
| I08 | #iv-macro (ivRenderMacro) | diagram/table | none | Q/M | 2026–2026 (1) | n/a | n/a | — | Economy | 5 |
| I09 | #iv-market, #iv-screen, compare, price/RSI charts | time_series | timeline | D | daily–2026-10 | yes | none_history_only | S | Main session | 5 |

### Simulation (sim)

| Id | Chart | Kind | Animation | Freq | Held | ≥8 | Projection | Effort | Owner | P |
|---|---|---|---|---|---|---|---|---|---|---|
| M01 | Hero (12-month deposit rate path + drivers) | model | model scrub | M | 2021-01–2026-09 (43) | yes | model_only | S | Finance, Ed | 2 |
| M02 | Panel: policy rates (refi, rediscount, O/N, OMO) | time_series | timeline | M | 2021-01–2026-09 (69) | yes | none_history_only | S | Finance, Ed | 3 |
| M03 | Panel: 10-year government bond yield | time_series | timeline | M | 2021-12–2026-09 (12) | yes | none_history_only | M | Finance | 3 |
| M04 | Panel: Fed funds upper, Fed − refi | time_series | timeline | M | 2021-01–2026-09 (69) | yes | to_add | S | Finance | 4 |
| M05 | Panel: interbank O/N | time_series | timeline | M | 2024-10–2026-09 (24) | yes | none_history_only | S | Finance, Ed | 4 |
| M06 | Panel: credit YTD / YoY | time_series | timeline | M | 2024-11–2026-09 (23) | yes | partial | S | Finance, Ed | 4 |
| M07 | Panel: deposit YTD / YoY; credit–deposit gap | time_series | timeline | M | 2025-01–2026-07 (16) | yes | none_history_only | S | Finance | 3 |
| M08 | Panel: LDR (SBV, simple listed), LDR cap | time_series | timeline | M (irregular) | 2021-12–2026-06 (12) | yes | none_history_only | S | Finance, Ed | 4 |
| M09 | Panel: M2 YoY / YTD, cash/M2 | time_series | timeline | M | 2024-12–2026-07 (18) | yes | none_history_only | S | Finance, Ed | 4 |
| M10 | Panel: VN-Index, VN-Index 12m | time_series | timeline | M | 2021-01–2026-09 (69) | yes | none_history_only | S | Finance, Ed | 4 |
| M11 | Panel: world gold (USD, 12m, m/m) | time_series | timeline | M | 2021-01–2026-09 (69) | yes | partial | S | Finance | 4 |
| M12 | Panel: SJC gold sell (monthly) | time_series | timeline | M | 2021-12–2026-09 (13) | yes | none_history_only | M | Finance | 4 |
| M13 | Panel: margin lending | time_series | timeline | Q | 2022-03–2026-06 (10) | yes | none_history_only | S | Finance | 4 |
| M14 | Panel: new securities accounts | time_series | timeline | M | 2021-01–2026-08 (21) | yes | none_history_only | S | Finance | 4 |
| M15 | Panel: real estate (Hanoi primary price) | time_series | timeline | Q/A | 2022-Q4–2026-Q2 (5) | **no** | none_history_only | M | Finance | 4 |
| M16 | Panel: corporate bonds (issuance, outstanding) | time_series | timeline | A + M 2026 | 2022–2026-08 (9) | **no** | none_history_only | M | Finance | 4 |
| M17 | Panel: public investment | time_series | timeline | A | 2024–9M-2026 (3) | **no** | partial | M | Finance, Economy, Policy | 3 |
| M18 | Panel: Treasury deposits; monthly budget balance | time_series | timeline | M | 2026-09–2026-09 (1) | **no** | none_history_only | L | Finance, Economy | 4 |
| M19 | Panel: CPI, core CPI, real deposit rate | time_series | timeline | M | 2021-04–2026-09 (51) | yes | partial | S | Finance, Economy | 3 |
| M20 | Panel: USD/VND, DXY | time_series | timeline | M | 2021-12–2026-09 (27) | yes | none_history_only | S | Finance, Ed | 4 |
| M21 | Panel: Brent | time_series | timeline | M | 2021-01–2026-08 (68) | yes | partial | S | Finance | 4 |
| M22 | Panel: cost of funds, NIM, CASA (listed) | time_series | timeline | Q | 2024-Q4–2026-Q2 (6) | **no** | none_history_only | M | Finance | 3 |
| M23 | Panel: SBV new-loan average rate | time_series | timeline | M | 2024-12–2026-08 (4) | **no** | none_history_only | M | Finance | 3 |
| M24 | Projection fan + scenarios (6 months to Mar-2027) | model | model scrub (exists) | M | 2021-01–2026-09 (43) | yes | model_only | S | Finance, Ed | 3 |
| M25 | Sliders | model | model scrub (exists) | — | — | n/a | model_only | S | Finance | 5 |
| M26 | Model card, coefficients, backtest | diagram/table | none (exists) | — | — | n/a | n/a | — | Finance | 5 |
| M27 | Indicators / coverage table | diagram/table | none | — | — | n/a | n/a | — | Finance | 5 |

### Party/Strategy (party)

| Id | Chart | Kind | Animation | Freq | Held | ≥8 | Projection | Effort | Owner | P |
|---|---|---|---|---|---|---|---|---|---|---|
| G01 | pgStGrowth (growth + 9M + required 2027–30 average) | time_series | timeline | A | 2010–2025 (+9M-2026) (16) | yes | exists_to_2030 | S | Strategy, Ed | 1 |
| G02 | pgStDumb (2025 → 2030 targets) | cross_section | step through years | A | 2025–2030 (targets) (1) | **no** | partial | M | Strategy, Economy, Society | 2 |
| G03 | pgStTargets cards | diagram/table | none | — | — | n/a | n/a | — | Strategy | 5 |
| G04 | pgStBars + replay (documents per month by level) | time_series | timeline (exists) | M | 2024-07–2026-10 (28) | yes | n/a | S | Strategy | 5 |
| G05 | pgStMult (per-level quarterly) | time_series | timeline (exists) | Q | 2024-Q3–2026-Q4 (10) | yes | n/a | S | Strategy | 5 |
| G06 | pgStPillars | cross_section | step through years | Q | 2024-Q3–2026-Q4 (10) | yes | n/a | S | Strategy, Ed | 4 |
| G07 | pgStFlow (mechanism, median lags) | diagram/table | none | — | — | n/a | n/a | — | Strategy | 5 |
| G08 | pgStRows + replay | time_series | timeline (exists) | event | 2024-07–2026-10 | yes | n/a | S | Strategy | 5 |
| G09 | pgStDrafts (expected adoption, session band, today line) | diagram/table | none | event | — | n/a | n/a | — | Strategy | 5 |
| G10 | pgGraph + pgNews | diagram/table | timeline (exists) | event | 2024-07–2026-10 | yes | n/a | S | Strategy | 5 |

(D) = needs a user decision (§3).
