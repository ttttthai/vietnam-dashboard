# SBV Annual Report → Financial-system tab: section map and blueprint

Owner: Finance · Prepared 2026-10-08 · Data file: `data/finance.json` (`FINSYS`) · Reuses `POLICY.mon` (policy.json) and `ECON_OFFICIAL`/`ECONFLOW` (economy.json) read-only.

## 0. What was read

| Edition | Status | How it was used |
|---|---|---|
| **Annual Report 2024** (English PDF, sbv.gov.vn, URL timestamp 2026-01-23) — latest published | **Not re-read this session**: sbv.gov.vn is blocked by the sandbox egress policy (403 on curl and WebFetch, 2026-10-08). | Structure from a prior session's check (Parts I–IV + appendices; App. 1 SBV policy rates, App. 2 OMO/SBV bills, RRR table, BOP) and the `FINSYS.sbv.operations` entries that cite it (OMO 251 sessions, bills 169 sessions, RRR, policy rates). |
| Annual Report 2025 | Not published as of 2026-10-08 (AR2023 posted 25/12/2024; AR2024 ~23/01/2026). Expect ~Dec-2026/Jan-2027. | — |
| AR2021, AR2010 | Table-of-contents excerpts only (search engines). The AR2010 mirror (economica.vn) is also blocked. | Confirms the standing layout: Part I economy & monetary developments (incl. CI operations), Part II SBV activities starting with monetary policy, then FX management, supervision, payments/banking technology. |
| Press summaries, SBV statements | Search excerpts (WebSearch). | All new numbers in this revision. |

The table of contents below is a **reconstruction** of the standard SBV report layout (consistent across 2010, 2021 and 2024 evidence). It is not a verbatim copy of AR2024 page titles. Check it against the PDF when sbv.gov.vn is reachable.

## 1. Reconstructed table of contents of a standard SBV Annual Report

- Governor's message; organisation chart; Board of Governors
- **Part I: World and Vietnamese economy**: world economy; Vietnam GDP, inflation, BOP, budget; monetary developments (M2, deposits, credit); operations of credit institutions
- **Part II: SBV activities**
  1. Monetary policy: interest rates (refinancing, rediscount, overnight, deposit and lending caps), OMO and SBV bills, refinancing, reserve requirements, credit growth targets and structure, money supply
  2. Exchange-rate and foreign-exchange management: central rate, band, FX interventions, FX reserves; gold-market management
  3. Banking inspection and supervision; restructuring of CIs and NPL resolution (weak/special-control banks, VAMC, Resolution 42)
  4. Payments and banking technology: non-cash payments, cards, QR, mobile and internet, interbank e-payment system, ATM/POS
  5. Currency issuance and vault management (notes in circulation, printing, cash management)
  6. Legal framework (laws, decrees, circulars issued)
  7. International cooperation and integration
  8. Other functions: credit information (CIC), anti-money-laundering, financial inclusion, communication, training and research
- **Part III: Organisation and personnel** (in some editions merged into the front matter)
- **Part IV: Orientations for the next year**
- **Appendices (statistical)**: SBV policy rates; OMO/SBV bill auctions; reserve-requirement ratios; balance of payments; (in some years) money and credit tables
- Not included: **SBV financial statements**. The report has no SBV balance sheet or income statement (`FINSYS.sbv.notes`).

## 2. Section-by-section map

Legend: **D** = data in `finance.json` (or other file), **T** = current tab chapter that shows it, **New** = added in this revision.

### 2.1 Macro context (Part I)
- Key indicators: GDP growth, CPI, BOP, budget, credit/GDP, M2/GDP.
- D: `annual.m2_to_gdp_pct_wb`, `private_credit_to_gdp_pct_wb` (to 2022), `ratios.credit_to_gdp_pct` (2025: 146%), `projections.*`; economy.json carries GDP, CPI, BOP.
- T: Ch.1 "A bank-financed economy" (depth line), "To 2030".
- Gaps: WB depth series stop in 2022; 2023–2025 M2/GDP needs SBV M2 ÷ NSO nominal GDP. That is a computation, so label it ≈.
- Best source: World Bank WDI; SBV M2 + NSO GDP.
- Chart: keep the **depth line** (credit/GDP and M2/GDP, 2015–2025), with a hollow point for press figures.

### 2.2 Money supply: M2, M1, currency in circulation (Part I monetary developments; issuance chapter)
- Key indicators: M2 level and growth; cash outside banks (M0); cash/M2; deposits by holder; reserve money growth.
- D:
  - Monthly (T12/24–T7/26): `monthly.m2_level`, `m2_ytd`, `cash_to_m2_pct`, `deposits_residents`, `deposits_econ_orgs`. **New:** `monthly.cash_in_circulation_derived_bn` (≈ M2 × cash share), `monthly.cash_in_circulation_flag`, `monthly.m2_methodology` (old to T9/25, new from T10/25).
  - Annual: `annual.m2` (mixed basis, existing). **New:** `annual.m2_imf_monetary_survey_bn` (IMF = SBV M2, 2015–2024), `annual.currency_outside_banks_imf_bn` (2015–2024), `annual.m0_adb_bn` (2015–2024), `annual.m1_adb_bn` (2020, 2021, 2023, 2024), `annual.cash_to_m2_pct_imf_derived`, `annual.money_definitions` (definitions, conflicts and sources).
  - `sbv.balance_sheet.series.reserve_money_yoy_pct` (growth only, 2014–2024).
- T: the hero shows credit/GDP rather than M2. Ch.7 "Who controls the money supply" quotes cash/M2 in a note. **No chart shows M2 or cash levels.**
- Gaps: official SBV M0 level (SBV publishes only the ratio); M1 from SBV; reserve-money level; 2025 IMF/ADB annual cash; T8/26–T9/26 M2 (not yet published).
- **Methodology break:** in Oct-2025 the SBV changed its M2/deposit compilation. Levels before and after are not comparable (Dec-2025 ÷ Dec-2024 gives 8.5%, while the SBV like-for-like growth is 15.7%). Every chart that spans Oct-2025 must mark the break.
- Charts:
  1. **Hero KPI pair**: M2 = 20,455,644 bn (T7/26, +5.20% YTD) and **money in circulation ≈ 2,041,473 bn (≈9.98% of M2, T7/26, derived)**, with an official anchor of 1,630,775 bn at end-2024 (IMF/ADB).
  2. **Stacked area, annual 2015–2024**: cash outside banks against the rest of M2 (deposits), with a cash-share line on a second panel. Shows that about 90% of "money" is bank deposits.
  3. **Monthly bars of derived cash (T1/25–T7/26)**, hatched for months flagged `month_mapping_ambiguous`, plus a vertical rule at T10/25 (methodology break). Shows the Tet peak (Feb-2026 ≈2.37 m bn).

### 2.3 Credit growth and structure (Part I; Part II monetary policy)
- Key indicators: credit growth against target, credit by sector, term (short vs medium/long), currency (VND/FX), credit–deposit gap, priority sectors, real estate.
- D: `annual.credit*`, `monthly.credit_*` (with cut-off dates and basis), `sectors.*` (19 periods), `flows.*` (real estate, MLT share 48.5% at Jul-2026, FX loan share 3.25%, SMEs, green credit, policy credit), `other_services_breakdown`, `listed_banks_by_sector`, `why.series.credit_vs_deposit_growth`.
- T: Hero (gap chart), Ch.2 (waffle, growth bars, other-services breakdown, facts).
- Gaps: annual credit levels 2015–2021 (SBV definition); term and currency split as a time series.
- Charts: keep the waffle and growth bars. Add a **credit growth vs target** chart (annual bars against target diamonds from `POLICY.mon.instruments.credit_growth_target`).

### 2.4 Deposits and mobilisation
- Key indicators: deposits by holder (residents vs economic organisations), by currency, CASA.
- D: `monthly.deposits_*`, `flows` (residents' share, FX liabilities share 5.67%), `why.series.casa*`.
- T: partly in Ch.3.
- Gap: deposits by currency and term over time (SBV publishes no breakdown in accessible form).
- Chart: **two-line chart** of residents and economic-organisation deposits, monthly, with the Oct-2025 reclassification break marked.

### 2.5 Interest rates (Part II monetary policy; App. 1)
- Key indicators: refinancing, rediscount, overnight, OMO rate, deposit caps, priority lending cap, interbank, average deposit and lending rates.
- D: `POLICY.mon.instruments` (rate histories); `why.series` (interbank ON monthly, 12-month deposit, average lending, cost of funds, NIM); `alternatives.series.deposit_12m`, `deposit_ifs`.
- T: Ch.3 scrollytelling and mechanism; Ch.7 levers board.
- Gap: a long annual history of SBV average lending and deposit rates (SBV statements only).
- Chart: **step chart** of policy rates (refinancing, rediscount, OMO) with interbank ON and the 12-month deposit rate as lines, Jan-2023 to now.

### 2.6 Open-market operations, SBV bills, refinancing (App. 2)
- D: `sbv.operations` (weekly OMO net flow and outstanding, bills, Jan-2025 to Sep-2026), AR2024 App. 2 entries; `sbv.plumbing`.
- T: Ch.6 "How the SBV moves money" (pipes), explore section OMO chart.
- Chart: keep the **weekly net-injection bars plus outstanding line**.

### 2.7 Reserve requirements
- D: `sbv.operations` (item `rrr`), `POLICY.mon.instruments.reserve_requirement`.
- T: Ch.7 levers board.
- Chart: a **small table or tile** is enough, since the rates are unchanged (VND 3%/1%, FX 8%/6%, with the reduced rate for banks taking over weak banks).

### 2.8 Exchange rate and FX market (Part II FX management)
- Key indicators: central rate, ±5% band, bank and free-market USD/VND, interventions, DXY/Fed.
- D: `why.series.usdvnd_monthly` (central, bank and free market, 24 months), `POLICY.mon.instruments.central_rate` (year-end points 2023–2025 and 2026 path), `fx_band`, `fx_intervention`; `ECON_OFFICIAL.usd_vnd` (annual average).
- T: Ch.3 step 4.
- Gap: year-end central rates 2016–2022 (not verified via search).
- Chart: **band chart**: central rate with the ±5% ceiling and floor as a shaded band, bank and free-market rates as lines, intervention dates as markers.

### 2.9 FX reserves and gold (Part II FX management)
- D: `sbv.balance_sheet.series.fx_reserves_busd` (IMF incl. gold, annual 2014–2025), `monthly_supplement` (total, ex-gold, gold, 2014-M01–2026-M07), `fx_reserves_definition`, `gross_reserves_imf_art4_busd`; gold policy and auctions in `POLICY.mon.instruments.gold_policy` (2024 auctions: 9 sessions, ~48k taels; direct sales 305,600 taels to 29/10/2024); SJC prices in `alternatives.series.gold_sjc`.
- T: Ch.5 "The central bank" (annual reserves).
- Gap: reserves in months of imports (IMF Art. IV has this); SJC–world premium as a monthly series.
- Charts: **monthly area of reserves** split into foreign currency and gold, with the 111.8 bn peak (Jan-2022) annotated. A **gold timeline strip** (auctions → direct sales → Decree 232 end of monopoly) over the SJC price line.

### 2.10 Banking system: size, capital, profitability, liquidity (Part I CI operations; supervision)
- Key indicators: total assets, charter capital, equity, CAR (Basel II), LDR (Circular 22), short-term funds used for medium/long-term loans, ROA/ROE.
- D: `ratios.total_assets_ci_vnd_bn` (2016–2026-06, **new** 2026-05 point), **new** `ratios.charter_capital_ci_vnd_bn` (2026-05), `car_basel2_banks_tt41_pct`, `car_imf_fsi_pct`, `ldr_sbv_tt22_pct`, `short_term_funds_for_mlt_loans_pct`, `roa/roe_sbv_pct`, `roa/roe_imf_fsi_pct`, `nim_listed_banks_pct`; 17-bank appendix from the server.
- T: Ch.4 "How sound is it?" (small multiples), appendix.
- Gaps: charter capital and equity by group, year-ends 2016–2025 (SBV table blocked); asset shares by bank group.
- Charts: a **line of total assets** (2016–2026) with a credit/total-assets ratio. **Small multiples** for CAR, LDR, ST→MLT, ROA/ROE, each with its regulatory cap drawn as a rule (LDR 85% → 95% from 1 Dec 2026; ST→MLT 40%).

### 2.11 Asset quality, NPL resolution, restructuring of weak banks (Part II supervision and restructuring)
- Key indicators: on-balance NPL, gross NPL (incl. VAMC and potential), NPL excluding weak banks, provisions coverage, VAMC purchases and resolution, Resolution 42 results, compulsory transfers, SCB.
- D: `ratios.npl_imf_fsi_pct` (2015–2025), `npl_onbalance_sbv_pct` (quarterly 2025–2026). **New:** `ratios.npl_onbalance_sbv_annual_pct` (2016–2023), `npl_onbalance_commercial_banks_pct` (2023–2024), `npl_commercial_banks_excl_5_weak_pct`, `npl_onbalance_commercial_banks_vnd_bn`, 4 earlier points in `npl_incl_vamc_and_potential_pct` (2016, 2018–2020), and `npl_resolution` (VAMC 2025 and cumulative, Resolution 42). Also `provisions_to_npl_imf_fsi_pct` and `POLICY.mon.instruments.compulsory_transfer`.
- T: Ch.4 (NPL lines under two definitions).
- Charts: a **three-definition NPL line** (on-balance SBV, gross incl. VAMC and potential, IMF FSI), with a separate dashed "excl. 5 weak banks" line. Do not merge the series. A **VAMC/resolution bar pair** (annual purchases and resolution) with a timeline of compulsory transfers (CBBank/OceanBank Oct-2024, GPBank/DongA Jan-2025, SCB pending).

### 2.12 Payments and digital banking (Part II payments and banking technology)
- Key indicators: non-cash transaction volume and value, value/GDP, growth by channel (internet, mobile, QR, card/POS, ATM), cards in circulation, payment accounts, ATM/POS counts, interbank system.
- D: **new** `payments` (annual 2023–2025 totals, value/GDP 25× and 28×, 2026 YTD H1 and 8M, channel growth 2023–2025, infrastructure: 164 m cards, 232 m personal accounts, ATM −1.09%, POS +19.86%, targets 25× GDP/80% accounts/cash<8% M2/30× GDP in 2030).
- T: **not covered** (only the biometrics rule in Policy).
- Gaps: 2019–2022 totals; ATM/POS and card levels; per-channel levels.
- Charts: **bars of annual volume and value** (2023–2025, plus 2026 8M as a partial bar), with a value/GDP line. **Diverging channel bars** for 2025 growth (QR, internet, mobile up; ATM down). **Target tracker**: 25× GDP (met, ≈28×), cash <8% of M2 (missed, 10.78%), 80% accounts (met).

### 2.13 Currency issuance and cash management (Part II issuance and vault)
- Key indicators: currency in circulation, denomination mix, printing cost, counterfeit seizures.
- D: cash in circulation via 2.2. Printing cost is a null line in `sbv.income_statement.items.currency_printing_cost`.
- Gaps: currency issued, denominations, printing cost (only in the AR text, not reachable).
- Chart: fold into 2.2. A **seasonality strip** of derived monthly cash (Tet peak) is the one chart this section supports with current data.

### 2.14 Financial inclusion
- D: **new** `financial_inclusion` (end-2025: 86.97% payment-account basis vs 88.96% "aged 15+ with bank account", an unreconciled conflict; targets 80% for 2025 and 95% for 2030; 232 m personal payment accounts).
- T: not covered.
- Gap: annual series 2019–2024; Global Findex values for Vietnam.
- Chart: a **KPI tile with a progress bar** to the 2030 target, showing both end-2025 figures side by side with their bases.

### 2.15 Supervision, legal framework, international cooperation
- D: `POLICY.mon.instruments` (Law on CIs 2024/2025, Circulars 14/2025 Basel III, 50/2026 LDR, 31/2024 classification, 35/2025 special loans, deposit insurance), `projections.targets`.
- T: Ch.7 levers board covers rules.
- Chart: a **legal timeline** (one row per law, decree or circular, with effective dates) instead of a numeric chart. International cooperation has no numbers; mention it in text only.

### 2.16 Number and types of credit institutions (Part I or III)
- D: **new** `institutions` (aggregator or broker counts by type, PCFs 1,178 at end-2024, PCF aggregates Oct-2025).
- Gap: official SBV counts by year.
- Chart: a **unit/icon chart** of institutions by type (the counts are labelled by source and date; there is no stated official total).

### 2.17 SBV financial statements and balance sheet
- D: `sbv.balance_sheet` (IMF aggregates: NFA, net claims on government, currency outside banks, reserve-money growth, 2014–2024), `sbv.income_statement` (only combined budget lines; SBV surplus 2024 52.741 trn).
- T: Ch.5 and the explore section.
- Gap: the report has no statements. The IMF Art. IV 2026 has not been published.
- Chart: keep the **SBV items per IMF** bars (NFA vs net claims on government), labelled "IMF Monetary Survey, not SBV statements".

## 3. Proposed tab structure (sub-tabs)

Hero (always visible): **"Money in Vietnam today"**
- Big numbers: **Total money supply M2: 20,455,644 bn VND** (T7/26, +5.20% YTD, new methodology) | **Money in circulation (cash outside banks): ≈2,041,473 bn VND = 9.98% of M2** (T7/26, derived). Official anchor: 1,630,775 bn at end-2024 (IMF/ADB).
- Sub-line: credit 20.75 m bn (+11.59% YTD at 30/9/2026) against deposits; credit/GDP 146%.
- Mini-chart: monthly derived cash bars plus the M2 line, with the Oct-2025 break marked.

| # | Sub-tab | Sections covered | Charts (form) |
|---|---|---|---|
| 1 | **Money & credit** | 2.1, 2.2, 2.3, 2.4 | M2 composition stacked area (annual); monthly cash and M2; depth line (% GDP); credit growth vs target; deposits by holder; credit–deposit gap |
| 2 | **Where credit goes** | 2.3 | waffle by sector; sector growth bars; other-services breakdown; RE, bonds and margin facts |
| 3 | **Rates & monetary policy** | 2.5, 2.6, 2.7 | policy-rate step chart with interbank and deposit lines; OMO weekly bars with outstanding; RRR tile; mechanism diagram and drivers (existing Ch.3); levers board (existing Ch.7); SBV plumbing (existing Ch.6) |
| 4 | **Exchange rate, reserves & gold** | 2.8, 2.9 | central rate with ±5% band, bank and free-market lines; monthly reserves area (FX and gold); gold policy timeline over the SJC price |
| 5 | **Banking system health** | 2.10, 2.11, 2.16 | total-assets line; CAR/LDR/ST→MLT/ROA-ROE small multiples with caps; three-definition NPL lines; VAMC and resolution bars plus transfer timeline; institutions unit chart |
| 6 | **Payments & inclusion** | 2.12, 2.13, 2.14 | annual non-cash volume and value bars with value/GDP line; 2025 channel growth diverging bars; target tracker; cash seasonality strip; inclusion progress tile |
| 7 | **The SBV & rules** | 2.15, 2.17 | SBV items per IMF bars; legal timeline; "To 2030" targets and scenarios (existing) |
| — | What to watch · Explore · 17-bank appendix | — | unchanged |

Rendering notes for Ed:
- Methodology flags: draw a break at T10/25 whenever `monthly.m2_methodology` changes. Hatch bars where `cash_in_circulation_flag == 'month_mapping_ambiguous'`. Always show "≈" for derived cash.
- Annual M2: use `m2_imf_monetary_survey_bn` (2015–2024) for composition charts, not `annual.m2` (mixed WDI and SBV basis).
- Keep the definitions separate. NPL has four series with notes. FX reserves come incl. gold (Finance) or excl. gold (Economy). LDR follows Circular 22 (SBV) or loans/customer deposits (listed banks).
- Policy rates, central rate, gold and transfers: read from `POLICY.mon.instruments[*].series/history`. Do not copy them into FINSYS.

## 4. Remaining gaps (by priority)
1. Official SBV level of cash in circulation and currency issued, M1, and reserve-money levels: not published or not reachable. Derived values are labelled ≈.
2. M2 and cash/M2 for T8/26–T9/26: not yet published (SBV lag about 2 months).
3. Payment totals 2019–2022; ATM/POS and card levels; per-channel levels.
4. Year-end central rate 2016–2022; reserves in months of imports.
5. Charter capital, equity and CAR by bank group at year-ends; official counts of institutions by type.
6. Whole-system on-balance NPL at end-2024 (only the commercial-bank figure of 4.35% was found); gross NPL 2017 and 2021–2024.
7. AR2024 text itself: re-read when sbv.gov.vn is reachable and replace §1 with the verbatim contents.
