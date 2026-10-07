# Freshness audit: does every chart show the latest 2026 figure?

As of 2026-10-07 · Research · **check-only** (no data file, page or server code edited). Twin: `freshness_audit.json` (same rows, machine-readable).

Method: each chart, KPI block and table on the 7 tabs (story chapters, sub-tabs, explore and appendix cards, server-fed parts) was mapped to the series it draws and the latest period it shows. The latest *published* period was checked by web search (excerpt-level: nso.gov.vn, sbv.gov.vn, mof.gov.vn and press pages are blocked from the sandbox, so values come from search-result excerpts unless stated) and by vnstock for market data (VN-Index 07/10 intraday 1,750.15; 06/10 close 1,759.08 matches `FINSYS.alternatives.ytd`). vnstock Finance hosts (VCI iq, MAS, KBS) are still blocked here (tested 07/10), so no bank fundamentals could be pulled.

Status: **current** = shows the latest published period · **stale** = a newer 2026 figure exists (or is already in our own files but not drawn) · **waiting** = nothing newer published yet · **structural** = annual series where full-year 2026 cannot exist yet · **broken** = the chart shows something inconsistent with its data.

## 1. Summary counts

| Status | Rows |
|---|---|
| stale | 22 |
| broken | 1 |
| waiting | 26 |
| structural | 26 |
| current | 67 |
| **total** | **142** |

By tab:

| Tab | stale | broken | waiting | structural | current | total |
|---|---|---|---|---|---|---|
| Party & Government (party) | 0 | 0 | 0 | 2 | 8 | 10 |
| Society (social) | 7 | 0 | 0 | 4 | 1 | 12 |
| Economy (econ) | 0 | 1 | 6 | 13 | 12 | 32 |
| Policies (money) | 1 | 0 | 4 | 1 | 9 | 15 |
| Financial system (banks) | 7 | 0 | 10 | 5 | 15 | 37 |
| Investing (invest) | 0 | 0 | 1 | 0 | 8 | 9 |
| Simulation (sim) | 7 | 0 | 5 | 1 | 14 | 27 |

By owner agent (first-named owner):

| Owner | stale | broken | waiting | structural | current | total |
|---|---|---|---|---|---|---|
| Finance | 10 | 0 | 19 | 7 | 29 | 65 |
| Economy | 0 | 1 | 6 | 11 | 13 | 31 |
| Society | 6 | 0 | 0 | 4 | 1 | 11 |
| Strategy | 0 | 0 | 0 | 2 | 8 | 10 |
| Policy | 0 | 0 | 0 | 0 | 8 | 8 |
| Main session / server | 5 | 0 | 0 | 1 | 1 | 7 |
| Investing | 0 | 0 | 1 | 0 | 6 | 7 |
| Ed | 1 | 0 | 0 | 1 | 1 | 3 |

Reading the counts: no chart shows a 2026 figure that is *older than the release cycle allows* for the monthly NSO block (GDP, CPI, FDI, tourism, budget, public investment all show the 03/10 release). Staleness sits in (a) Society's provincial vital rates (2024 reference while the 1/4/2025 survey tables are out), (b) Simulation panels fed by sparse hand-curated series, (c) the server's bank fundamentals (FY2024), and (d) a few values already in our files that the page does not draw (2025 national vital rates, 2026 monthly reserves).

## 2. Priority watch (user standing task)

| Rank | Series | Chart rows | Held / shown | Latest published | Status | Next |
|---|---|---|---|---|---|---|
| 1 | FDI registered vs disbursed | E15, E16, E27 | 9M-2026 reg 50.36 bn (new 29.24 + adjusted 14.15 + M&A 6.97), disbursed 21.07 bn, ratio 41.8% (derived) | same (NSO/FIA 03/10) | current (E27 appendix: structural, could show 9M) | 03/11/2026 (Jan–Oct) |
| 2 | Remittances | E08 (+ BoP E06/E09) | HCMC 2025 10.34 bn; H1-2026 4.037 bn; national 2025 none (speech "over 16 bn"); BoP Q1-2026 | HCMC 9M-2026 not out — every "9 tháng" hit is 9M-2023 (5.485 bn) or 9M-2025; no KNOMAD 2025 figure found; BoP Q2-2026 still unpublished | waiting | HCMC ~20–25/10; BoP check 20/10 |
| 3 | Tourism | E08 | 9M-2026 17.68 m (+14.5%), Sep 1.77 m; China 3.9 m, Korea 3.0 m; receipts 9M 13.06 bn | same, re-confirmed 07/10 (baovanhoa, baoquocte, vietnamplus) | current | 03/11/2026 |

Conflict noted (not averaged): HCMC 2025 remittances 10.34 bn (+8.3%, actual; held) vs 10.5 bn (+10.5%, Region-2 year-end estimate, vneconomy). The held actual is right; the estimate is an earlier vintage.

## 3. Chart tables by tab

### Party & Government (party)

| # | Chapter | Chart | Series (file · path) | Showing | Latest published | Status | Proposed update (value · period · source) | Owner | Next |
|---|---|---|---|---|---|---|---|---|---|
| G01 | pg-st-hero | pgStGrowth (growth + 9M + required 2027–30 average) | ECON_OFFICIAL.gdp_growth; ytd_2026 | 9M-2026 9.01% | same | **current** |  | Strategy (texts) / Economy (data) | 2026-11-03 |
| G02 | pg-st-gap | pgStDumb (2025 → 2030 targets) | strategy_directives.json vision targets; ECON_OFFICIAL | 2025 actuals | 2025 (annual) | **structural** |  | Strategy |  |
| G03 | pg-st-gap | pgStTargets cards | strategy_directives.vision | 06/10 | same | **structural** |  | Strategy |  |
| G04 | pg-st-wave | pgStBars + replay (documents per month by level) | strategy_directives.directives | to 02/10/2026 | same | **current** |  | Strategy | twice weekly in NA session |
| G05 | pg-st-wave | pgStMult (per-level quarterly) | same | same | same | **current** |  | Strategy |  |
| G06 | pg-st-pil | pgStPillars | directives × pillars | same | same | **current** |  | Strategy |  |
| G07 | pg-st-chain | pgStFlow (mechanism, median lags) | directives + relations | same | same | **current** |  | Strategy |  |
| G08 | pg-st-chain | pgStRows + replay | same | same | same | **current** |  | Strategy |  |
| G09 | pg-st-pipe | pgStDrafts (expected adoption, session band, today line) | directives with stage=draft | NA session 17/10–20/11 upcoming | same | **current** | Update stages as votes happen | Strategy | 2026-10-17..11-20 |
| G10 | explore | pgGraph + pgNews | directives, relations, news | news to 06/10 (CC plenum 4 in progress) | same | **current** |  | Strategy |  |

### Society (social)

| # | Chapter | Chart | Series (file · path) | Showing | Latest published | Status | Proposed update (value · period · source) | Owner | Next |
|---|---|---|---|---|---|---|---|---|---|
| S01 | soc-st-hero | socStPop (population 2000–25 + NSO/UN projections) | society.json POP_SERIES.actual/gso/un | 2025 102.3 m (prelim) | 2025; 2026 average population due Jan-2027 | **structural** |  | Society | 2027-01-03 |
| S02 | soc-st-hero | Hero dek (growth %/yr, rank) | POP_SERIES.nat (year 2024) ; WORLD_RANK.pop | "grew 1.03% a year (2024)" | 2025 prelim already held in POP_SERIES.nat_latest: growth 0.99%, TFR 1.93, urban 38.64% | **stale** | Either Society rolls POP_SERIES.nat to 2025 (prelim, labelled) or Ed reads nat_latest; values already in society.json | Society / Ed |  |
| S03 | soc-st-tfr | socStStrip (TFR by province, 2.1 line) | SOC_PROV_DATA[*].tfr (reference 2024) | reference 2024 (e.g. HCMC 1.43, Đồng Tháp 1.71, Vĩnh Long 1.60, Cần Thơ 1.55, Cà Mau 1.58) | NSO population-change survey 1/4/2025 by 34 provinces: HCMC 1.51, Đồng Tháp 1.61, Vĩnh Long 1.70, Cần Thơ 1.63, Cà Mau 1.55 (national 1.93) | **stale** | Replace tfr (and cbr/cdr/urban/sex_ratio/life_exp) with the 2025-reference provincial table; verify each value in the NSO PDF (excerpt-level now) · [src1](https://thuvienso.quochoi.vn/bitstream/11742/110630/1/Cuc%20thong%20ke.%20Ket%20qua%20chu%20yeu%20dieu%20tra%20bien%20dong%20dan%20so%20va%20ke%20hoach%20hoa%20gia%20dinh%20thoi%20diem%2001.4.2025.pdf) [src2](https://nhandan.vn/dan-so-va-lao-dong-viec-lam-tiep-tuc-on-dinh-nam-2025-tao-da-cho-2026-post946357.html) | Society | 1/4/2026 survey results ~late 2026/2027 |
| S04 | soc-st-tfr | socStMult (urban, life expectancy, sex ratio, density) | SOC_PROV_DATA (ref 2024); POP_SERIES.nat (2024) national ticks | reference 2024 | 2025 survey tables (same source as S03) | **stale** | Same refresh as S03; national ticks from nat_latest | Society |  |
| S05 | soc-st-age | socStAge (age–sex 2025 vs 2050) | POP_SERIES.age_sex (UN WPP 2024) | WPP 2024 | WPP 2024 (next revision postponed to 2027) | **structural** |  | Society | WPP 2027 |
| S06 | soc-st-rich | socStMap (GRDP / TFR toggle) | SOC_GRDP grdp_pc 2025; SOC_PROV_DATA.tfr | GRDP 2025 (est); TFR ref 2024 | GRDP current; TFR 2025 published (see S03) | **stale** | TFR mode: as S03 | Society |  |
| S07 | soc-st-rich | socStGrdp (34 provinces GRDP per person) + 9M growth note | SOC_GRDP.grdp_pc_mvnd[2025]; growth_9m[2026] | 2025; 9M-2026 growth for 30 of 34 (missing Điện Biên, Quảng Trị, Huế, Gia Lai) | NSO 9M-2026 provincial estimates released 3–6/10; values for the four not found in excerpts (Gia Lai H1 8.21%; Huế H1 9–9.5%) | **stale** | Fill growth_9m[2026] for the 4 provinces from provincial statistics offices / the round-up article; leave null if not found · [src1](https://tapchikinhtetaichinh.vn/12-tinh-thanh-pho-tang-truong-grdp-2-con-so-trong-9-thang-nam-2026-168393.html) | Society | 2026-12-29 (Q4/annual GRDP, confirm) |
| S08 | soc-st-rich | Flourish scatter 30475882 (GRDP pc × TFR, 2020→2025 slider) | data/flourish/provinces_scatter.csv | TFR ref 2024 | TFR 2025 (S03) | **stale** | Rebuild CSV after S03 | Main session |  |
| S09 | soc-st-rank | socStRanks (world rank strip) | WORLD_RANK (WDI Jul-2026) | pop/GDP 2025, vital rates 2024 | WDI Jul-2026 vintage | **current** |  | Society | WDI Dec-2026 |
| S10 | soc-st-rel | socStWaffle (religion) | RELIGION (census 2019; BTG 2023) | 2019 / 2023 | same (next census 2029) | **structural** |  | Society |  |
| S11 | explore | renderDemoPyramid (#demo-svg, ▶ 2000–2050) | POP_SERIES.age_sex | WPP 2024 | same | **structural** |  | Society | WPP 2027 |
| S12 | explore | drawMap selector + renderSocSummary tiles | SOC_PROV_DATA, SOC_GRDP, POP_SERIES.nat_latest | 2025 pop; ref-2024 vitals | 2025 vitals (S03) | **stale** | Follows S03 | Society |  |

### Economy (econ)

| # | Chapter | Chart | Series (file · path) | Showing | Latest published | Status | Proposed update (value · period · source) | Owner | Next |
|---|---|---|---|---|---|---|---|---|---|
| E01 | st-hero | stGrowth (growth 2011–25 + 9M bar + forecasts + target) | economy.json ECON_OFFICIAL.gdp_growth; ECONFLOW.gdp.growth_real_pct.9M_2026_yoy; ECONFLOW.projections.series.gdp_growth_pct_* | 2025 8.02%; 9M-2026 9.01%; IMF Apr-26 7.1, IMF Jul-26 7.5, WB Oct-26 7.4, ADB Sep-26 7.8, AMRO 7.5 | NSO 9M-2026 9.01% (03/10); IMF WEO Oct-2026 not yet out (13/10) | **waiting** | After 13/10: replace gdp_growth_pct_imf with WEO Oct-2026 (keep Apr as vintage note); drop gdp_growth_pct_imf_jul2026 if superseded | Economy | 2026-10-13 (IMF WEO) |
| E02 | st-hero | Hero KPI (GDP level, 9M growth) | ECONFLOW.gdp.GDP[2025]; growth_real_pct.9M_2026_yoy.GDP | 2025 12.85 quadrillion VND; 9M-2026 +9.01% | same | **current** |  | Economy | 2026-11-03 (NSO Oct report; GDP next 2027-01-03) |
| E03 | st-formula | stEquation (C+I+G+X−M blocks) | ECONFLOW.gdp.{C,I,G,X,M} | 2025 (estimate) | 2025; 9M-2026 has growth rates only, no expenditure levels | **structural** | Optional: note 9M-2026 real growth by component (final consumption +8.51%, GCF +17.88%, X +21.29%, M +27.19%), labelled "9M, real growth", not shares | Economy | 2027-01-03 (2026 estimate) |
| E04 | st-formula | stGiants (X, M vs GDP) | ECONFLOW.gdp.X/M (WDI) | 2025 | 2025 | **structural** |  | Economy | WDI Dec-2026 |
| E05 | st-formula | stMix (% of GDP 2015–25) | ECONFLOW.gdp.pct_gdp | 2015–2025 | 2025 | **structural** |  | Economy | 2027-01-03 |
| E06 | st-bop | stSankey + net/gross + year replay | ECONFLOW.bop.s (IMF BPM6/SBV) | 2025 | 2025 annual; Q1-2026 only quarter published | **structural** |  | Economy | annual 2026 BoP ~Q1-2027 |
| E07 | st-bop | stBopMult (4 flows 2015–25) | ECONFLOW.bop.s | 2015–2025 | 2025 | **structural** |  | Economy |  |
| E08 | st-bop | stBopFacts (tourism, remittances, labour sparklines) | ECONFLOW.tour, .remit, .labour | arrivals 9M-2026 17.68 m; HCMC remittances 2025 10.34 bn (+H1-2026 4.037); workers sent 8M-2026 90,319 | tourism 9M current (NSO 03/10); HCMC 9M remittances not yet out; DOLAB 9M-2026 not found | **waiting ★2** | HCMC 9M-2026 remittances after ~20–25/10 (beware 9M-2023 articles ranking first: 5.485 bn); DOLAB 9M workers when out | Economy | 2026-10-20..25 (HCMC); DOLAB ~early Oct |
| E09 | st-bop | ecBopQ (latest-quarter BoP tiles) | ECONFLOW.bop.quarterly | Q1-2026 (CA +2.716 bn; overall −1.078 bn) | Q2-2026 still not found (searched 07/10; only Q1 coverage in press) | **waiting ★2** | Re-check 20/10; do not store the unconfirmed 11.1 bn CA-deficit claim | Economy | 2026-10-20 (check) |
| E10 | st-budget | stWaffles (revenue/spending by year) | ECONFLOW.budget | 2018–2024 final, 2025 estimate, 2026 plan | same; 9M-2026 execution held in exec_9M2026 | **current** |  | Economy | 2026-11-03 (MoF 10M) |
| E11 | st-budget | stBudLine (revenue, spending, deficit, land-use fees) | ECONFLOW.budget | 2018–2026 (2026 plan) | same | **current** |  | Economy |  |
| E12 | st-budget | ecDef (deficit by basis + target) | ECONFLOW.budget.deficit_pct_gdp; projections.budget_deficit_pct_gdp_target | 2025 ~3.6% est; 2026 plan 4.2% | same | **current** |  | Economy | NA session 17/10–20/11 (2027 budget) |
| E13 | st-budget | ecDebt (public-debt plan bullet) | ECONFLOW.projections.series.public_debt_pct_gdp_plan | 2026 plan 36–37% | no 2026 actual exists; MoF 2025 estimate only in speeches | **structural** |  | Economy | NA session (2027 borrowing plan) |
| E14 | st-inv | stInvArea (investment by owner + 9M point) | ECONFLOW.inv | 2015–2025 + 9M-2026 (3,109.6 tn) | 9M-2026 (NSO 03/10) | **current** |  | Economy | 2027-01-03 (FY2026) |
| E15 | st-inv | ecFdi (FDI disbursed columns + 9M + target band) | ECON_OFFICIAL.fdi_dis_musd; ytd_2026.fdi_dis_musd_9M | 2010–2025 + 9M-2026 21.07 bn | 9M-2026 21.07 bn (+12.1%), NSO/FIA 03/10 | **current ★1** |  | Economy | 2026-11-03 (Jan–Oct) |
| E16 | st-inv | ecFdiReg (registered by component + disbursed, derived ratio) | ECON_OFFICIAL.ytd_2026.fdi_reg_components_9M; fdi_dis_to_reg_pct_9M | 9M-2026 reg 50.36 bn = new 29.24 + adjusted 14.15 + M&A 6.97; ratio 41.8% (derived) | same (03/10) | **current ★1** |  | Economy | 2026-11-03 |
| E17 | st-cpi | stHeat (CPI groups × 24 months) | CPI_DETAIL; CPI_BASKET (page constant) | T10/24–T9/26 | T9/26 (NSO 03/10) | **current** |  | Economy (CPI_BASKET is an unowned page constant — Ed) | 2026-11-03 |
| E18 | st-cpi | stFan (CPI + dashboard projection to Dec-2027) | CPI_YOY_24; CPI_FC_SCEN/TEMPLATE/DRIVERS (page) | actual to T9/26 (5.08%) | T9/26 | **current** |  | Economy | 2026-11-03 |
| E19 | st-cpi | ecCpiY (annual CPI + institution forecasts + target) | ECON_OFFICIAL.cpi_avg; CPI_FC_INST; projections.series.cpi_pct_* | 2025 3.31%; 9M-2026 4.52%; IMF Apr-26 4.9, WB May-26 4.2/3.8, ADB Sep 4.3, AMRO 4.3 | IMF Oct WEO due 13/10; WB Oct EAP CPI figure not found in excerpts | **waiting** | After 13/10: IMF row → WEO Oct-2026. Check WB 2027 CPI: one excerpt reads 3.7% (held 3.8%) — conflict, do not average | Economy | 2026-10-13 |
| E20 | st-2030 | ecPcChart (GDP per person + IMF path + 8,500 target) | ECON_OFFICIAL.gdppc_usd; projections.gdp_per_capita_usd_imf | 2025 5,026 USD; IMF Apr-26 path | IMF Oct WEO 13/10 | **waiting** | Swap IMF path to WEO Oct-2026 after 13/10 | Economy | 2026-10-13 |
| E21 | st-2030 | ecUsdChart (GDP USD + IMF path) | ECONFLOW.gdp.GDP / fxYear; projections.gdp_usd_bn_imf | 2025 + IMF Apr-26 | IMF Oct WEO 13/10 | **waiting** | Same as E20 | Economy | 2026-10-13 |
| E22 | st-2030 | ecNow (KPI tiles vs targets) | ECON_OFFICIAL.ytd_2026 | 9M-2026 | 9M-2026 | **current** |  | Economy | 2026-11-03 |
| E23 | st-watch | What to watch (VAT expiry, 2026 deficit, BoP) | POLICY.fis vat_cut_2pct; ECONFLOW.budget; bop | 31/12/2026; 2026 plan; 2025 E&O | dates still in the future | **current** |  | Ed / Economy |  |
| E24 | appendix | ecb-gdppc (growth card, year slider ≤2025) | ECON_OFFICIAL.gdp_growth/gdppc_usd | 2010–2025 | 2025 annual; 9M-2026 9.01% | **structural** | Optional: a faded 9M-2026 point labelled "9 tháng/9M" | Economy (data) / Ed (display) |  |
| E25 | appendix | ecb-income-card | ECON_OFFICIAL.income_month_k | 2025 6,005k VND/month (VHLSS per capita) | 2025; Q3-2026 9.2 m is worker income — different concept | **structural** | Do not splice the 9.2 m labour-income figure | Economy | VHLSS 2026 in 2027 |
| E26 | appendix | ecb-cpi-card (annual CPI bars) | ECON_OFFICIAL.cpi_avg | 2010–2025 | 2025; 9M-2026 avg 4.52% | **structural** | Optional faded 9M-2026 bar (4.52%, labelled 9M) | Economy / Ed |  |
| E27 | appendix | ecb-fdi (FDI registered, province drill-down) + KPI tiles | ECON_OFFICIAL.fdi_reg/dis_musd; PROVS (model) | 2010–2025; provinces = model scaled to national | 2025; 9M-2026 50.36 / 21.07 bn | **structural ★1** | Show 9M-2026 tiles labelled part-year; keep "model" on province values | Economy / Ed | 2026-11-03 |
| E28 | appendix | ecb-trade (exports/imports) + KPI | ECON_OFFICIAL.export_busd/import_busd | 2025 475.0 / 454.9 bn | 9M-2026 X 434.3 bn (+24.5%), M 453.72 bn (+36.7%), balance −19.42 bn | **structural** | Optional 9M-2026 tiles labelled part-year (already in ytd_2026) | Economy / Ed | 2026-11-03 |
| E29 | appendix | ecb-labour (enterprises, unemployment, PCI, IIP) | ECON_OFFICIAL.ent_active/unemp/pci_median/iip_growth | year 2025 selected: PCI tile shows 67.7 — the 2024 value (pci_median[2025] is null; calcEcon falls back to the nearest year without saying so) | VCCI PCI 2025 published 15/05/2026: median 63.90 (first PCI on 34 provinces, PCI 2.0 method — not comparable with 2024 67.67) | **broken** | Economy: pci_median[2025] null → 63.90 with a vintage/method note (PCI 2.0, 34 provinces). Ed: label any nearest-year fallback with its year · [src1](https://www.vcci.com.vn/tin-tuc/lan-dau-cong-bo-xep-hang-nang-luc-canh-tranh-34-tinh-thanh-pho) [src2](https://tapchicongthuong.vn/buc-tranh-nang-luc-canh-tranh-cap-tinh-sau-sap-nhap-va-top-5-dia-phuong-co-chat-luong-dieu-hanh-xuat-sac-nhat-517890.htm) | Economy + Ed | PCI 2026 ~May 2027 |
| E30 | appendix | drawGdpChart (#gdp-sectors, slider 2000–2025) | GDP_SECTORS / GDP_SUB_OFFICIAL (unowned page constants) | 2000–2025 (2026–45 values truncated at GDP_LAST_YEAR) | 2025 annual; 9M-2026 structure agri 10.65 / ind 38.35 / svc 42.97 / tax 8.03 | **structural** | Optional 9M-2026 structure, labelled. Note: 2000–2009 GDP_SECTORS values are unsourced round numbers in an unowned constant (Ed/main session) | Ed (unowned) / Economy |  |
| E31 | appendix | Flourish bar-chart race 30476438 (16 activities 2010–25) | data/flourish/gdp_by_activity_race.csv | 2010–2025 | 2025 | **structural** |  | Main session |  |
| E32 | appendix | CPI explorer tiles + basket table (renderCpiAt, kpi-cpi-*) | CPI_LATEST; CPI_BASKET | T9/26 | T9/26 | **current** |  | Economy | 2026-11-03 |

### Policies (money)

| # | Chapter | Chart | Series (file · path) | Showing | Latest published | Status | Proposed update (value · period · source) | Owner | Next |
|---|---|---|---|---|---|---|---|---|---|
| P01 | pol-st-hero | polStRates (policy-rate steps) | policy.json POLICY.mon.instruments[refinancing, omo, rediscount, …] | to 06/10/2026 (refi 4.5, OMO 4.5) | no change since | **current** |  | Policy | weekly |
| P02 | pol-st-moves | polStDiv (easing/tightening per quarter) | POLICY.*.instruments[].history | to Q4-2026 (06/10) | same | **current** |  | Policy | weekly; NA session 17/10–20/11 |
| P03 | pol-st-moves | polStGroups | same | same | same | **current** |  | Policy |  |
| P04 | pol-st-cliff | polStExpiry (+ cards) | POLICY instruments[].expires | 31/12/2026 cluster | no extension document yet | **current** |  | Policy | NA session; 15/12 check |
| P05 | pol-st-steps | polStMult (8 dials, scrubber) | POL_MULT series (central rate, credit target/actual, VAT, …) | central rate 25,643 (06/10); credit actual 11.59% (30/9) | same | **current** |  | Policy |  |
| P06 | pol-st-watch | What to watch (future-dated moves) | history with future dates (LDR 95% 01/12) | future | same | **current** |  | Policy |  |
| P07 | explore | Stance, polTimeline (#pol-tl), instrument tables, news | POLICY.mon/fis | news to 06/10 | same | **current** |  | Policy |  |
| P08 | appendix | ecb-lsr (deposit / OMO / refinancing lines) + kpi-lsr-* | finance.json FINSYS.why.series.deposit12m_private_banks_mbs, avg_lending_rate_sbv; POLICY | deposit 8.4% (Aug-26, MBS); lending 8.4–10.7 (Aug) | Sep MBS note not found | **waiting** |  | Finance | ~mid-Oct |
| P09 | appendix | ecb-macro (M2 growth + CPI, annual) + FX/CPI/reserves KPI | FINSYS.annual; ECON_OFFICIAL.usd_vnd/cpi_avg/fx_reserves_busd | 2025 | 2025 annual | **structural** |  | Finance / Economy |  |
| P10 | appendix | ecb-credit-money (credit YTD vs M2 YTD) | FINSYS.monthly.credit_ytd / m2_ytd | credit T9/26 11.59%; M2 T7/26 5.20% | SBV Aug M2 table not found yet | **waiting** | Also: T8/26 and T9/26 credit points are rounded statement figures (28/8 ~20.5 quadrillion; 30/9 20.75) spliced after SBV month-end table values — label the basis | Finance | SBV Aug table ~mid–late Oct |
| P11 | appendix | ecb-vnibor (overnight vs OMO) | FINSYS.why.series.interbank_on_monthly | Sep-26 7.0% (month-end) | Sep average (Vietcap) not yet | **waiting** | Replace month-end with Sep average when Vietcap publishes; keep type | Finance | ~mid-Oct |
| P12 | appendix | ecb-money-supply (M2, deposits levels) | FINSYS.monthly.m2_level/deposits_level | T7/26 | Aug table not found | **waiting** |  | Finance | ~mid–late Oct |
| P13 | appendix | ecb-fx-detail (reserves excl. gold) + kpi-fxr | ECON_OFFICIAL.fx_reserves_busd | 2025 85.58 bn (annual) | IMF IL monthly to 2026-M07 already in finance.json (excl. gold 82.813; total 86.101) | **stale** | Show the latest monthly point (Jul-2026, labelled IMF IL, excl. gold 82.8 bn) — value already held in FINSYS.sbv.balance_sheet.monthly_supplement | Ed (wiring) / Economy+Finance (one definition) | IMF IL monthly ~6 weeks lag |
| P14 | appendix | ecb-fis-budget (% of plan, even pace) | ECONFLOW.budget.exec_9M2026 | 9M-2026 | 9M-2026 (MoF, 03/10) | **current** |  | Economy | 2026-11-03 |
| P15 | appendix | ecb-fis-pubinv (% of plan by year) | POLICY.fis public_investment_annual; invest_macro | to 30/09/2026 62.9% | same | **current** |  | Policy / Economy | ~2026-11-03 |

### Financial system (banks)

| # | Chapter | Chart | Series (file · path) | Showing | Latest published | Status | Proposed update (value · period · source) | Owner | Next |
|---|---|---|---|---|---|---|---|---|---|
| F01 | fs-st-hero | fsStGap (credit vs deposit growth + projections) | FINSYS.annual; why.series.credit_vs_deposit_growth | to 28/09/2026 (credit 10.89, deposit 9.78) | credit 30/9 11.59% (held in monthly/flows); no matching 30/9 deposit figure found | **current** | Minor: hero dek quotes 11.59% (30/9) above a chart ending 28/9 — note the cut-off | Finance | ~2026-11-03 (end-Oct statement) |
| F02 | fs-st-hero | Hero KPI credit/GDP 146% | FINSYS.ratios.credit_to_gdp_pct | 2025-12 | 2025 | **structural** |  | Finance |  |
| F03 | fs-st-hero | fsAltBlock (deposit alternatives 2010–25 + 2026 YTD) | FINSYS.alternatives | annual to 2025; YTD 06/10 (gold, VN-Index, USD); real-estate YTD null | Savills Q2-2026 Hanoi primary ~116 m VND/m² (+27% y/y; CBRE 95 m) | **stale** | ytd.real_estate: add Q2-2026 y/y (+27%, Savills, labelled Q2 y/y, not YTD) or keep null with note; Q3 reports due mid-Oct · [src1](https://cafef.vn/chung-cu-ha-noi-vang-bong-can-ho-duoi-70-trieu-dong-m2-188260814143655062.chn) | Finance | Savills/CBRE Q3 ~mid-Oct |
| F04 | fs-st-depth | fsStDepth (private credit/GDP, M2/GDP + press 146%) | FINSYS.annual.*_wb; ratios.credit_to_gdp_pct | WB to 2022; press 2025 | same | **structural** |  | Finance | WDI Dec-2026 |
| F05 | fs-st-depth | fsStMMult (4 monthly minis) | FINSYS.monthly m2_ytd, deposits_ytd, credit_ytd, credit_yoy | credit T9/26; M2/deposits T7/26 | SBV Aug tables not found | **waiting** | Label the T8/T9 statement-basis credit points (see P10) | Finance | ~mid–late Oct |
| F06 | fs-st-flow | fsStWaffle (credit by sector) | FINSYS.sectors | 2026-07 | Aug table not found | **waiting** |  | Finance | ~mid–late Oct |
| F07 | fs-st-flow | fs-st-sgrow (2025 FY vs 2026 YTD by sector) | FINSYS.flows "Credit growth by sector…" | FY2025 vs Jul-2026 | same | **waiting** |  | Finance |  |
| F08 | fs-st-flow | fsStLB (listed banks vs SBV total) | FINSYS.listed_banks_by_sector | Q2-2026 (30/6) | Q3 FS due 20–31/10 | **waiting** |  | Finance | 2026-10-20..31 |
| F09 | fs-st-flow | fsStOSChart (inside "other services") | FINSYS.other_services_breakdown | Jun–Aug 2026 components; total Jul | same | **current** |  | Finance |  |
| F10 | fs-st-flow | fsStFacts (flow facts) | FINSYS.flows | green credit 828k (stated 9/6/2026); margin 453.8k (Jun) | SBV H1 press conference: green credit "over 780,000 bn" (excerpt) | **current** | Conflict to resolve (do not average): 828k (SBV-GIZ forum 9/6) vs >780k (SBV H1 briefing 2/7). Margin Q2: press tallies 446k (80/85 firms) / ~435k vs held 453.8k — coverage differs | Finance | margin Q3 late Oct |
| F11 | fs-st-why | fsStMech (mechanism diagram with data) | FINSYS.why.series; CPI | CPI Sep; deposit Aug | same | **current** |  | Finance |  |
| F12 | fs-st-why | fsStCf (counterfactual) | FINSYS (model) | model | n/a | **current** |  | Finance |  |
| F13 | fs-st-why | fsStWMult (cost of funds, NIM, CASA, LDR) | FINSYS.why.series.cof/nim/casa/ldr | 2026-Q2 | Q3 bank FS 20–31/10 (MBS forecasts only so far) | **waiting** |  | Finance | 2026-10-31 |
| F14 | fs-st-why | fsStDrivers (10 ranked causes) | FINSYS.why.drivers | 06/10 | same | **current** |  | Finance |  |
| F15 | fs-st-safe | fsStNpl (NPL, 2 definitions) | FINSYS.ratios.npl_imf_fsi_pct; npl_onbalance_sbv_pct | IMF 2025 4.21; SBV 6/2026 3.31 | same (no Aug figure found) | **current** |  | Finance | SBV Q3 ~Nov |
| F16 | fs-st-safe | fsStSMult (IMF FSI minis) | FINSYS.ratios.*_imf_fsi_pct | 2025 | 2025 (annual) | **structural** |  | Finance | IMF FSI 2026 in 2027 |
| F17 | fs-st-sbv | fsStRes (year-end reserves incl. gold) | FINSYS.sbv.balance_sheet.series.fx_reserves_busd | 2025 86.95 bn | IMF IL monthly to Jul-2026 (86.10 bn) already in monthly_supplement | **structural** | Add a labelled "Jul-2026 (IMF IL, monthly)" point from data already held; Aug-2026 IL not verified | Finance / Ed |  |
| F18 | fs-st-pipe | fsPipe (plumbing diagram facts) | FINSYS.sbv.plumbing.facts | OMO 2/10; gov-bond issued YTD to 16/09; Treasury deposits 30/6 | Sep gov-bond auction totals published (amount not pinned) | **stale** | Update gov_bond_issued_2026_ytd to 30/09 from HNX/MoF monthly total · [src1](https://vnbusiness.vn/thang-9-huy-dong-trai-phieu-chinh-phu-tang-gan-20.html) | Finance |  |
| F19 | fs-st-levers | fsLvDraw (who controls money supply) | FINSYS.sbv.plumbing; monthly | Jul/Sep | same | **current** |  | Finance |  |
| F20 | fs-st-2030 | Projection multiples vs targets | FINSYS.projections (IMF Apr-26 real GDP/CPI; WB Oct-26) | IMF Apr-2026 vintage | IMF WEO Oct-2026 on 13/10 | **waiting** | Same vintage switch as Economy E01/E19; WB note: "+1.1 pp" is vs the April EAP 6.3%; a mid-May 2026 WB vintage said 6.8% — label the comparison vintage | Finance | 2026-10-13 |
| F21 | fs-st-watch | What to watch | POLICY ldr_cap; credit target; deposit rate | 01/12/2026; 31/12/2026 | future | **current** |  | Finance |  |
| F22 | explore | fs-ch-levels (M2, deposits, credit levels) | FINSYS.monthly | credit T9/26; M2/deposits T7/26 | Aug tables pending | **waiting** |  | Finance | ~mid–late Oct |
| F23 | explore | fs-ch-fx (indexed USD/VND, DXY) | FINSYS.why.series.usdvnd_monthly, dxy_month_end | Sep-2026 | Sep-2026 | **current** |  | Finance | ~2026-11-01 |
| F24 | explore | fs-ch-liq (LDR, short-term funds for MLT loans) | FINSYS.ratios.ldr_sbv_tt22_pct etc. | 2026-06 | latest SBV stat found 31/5 (78.25%) — held Jun is newer | **current** |  | Finance |  |
| F25 | explore | fs-ch-car | FINSYS.ratios.car_* | Basel II 2026-03; IMF 2025 | same | **current** |  | Finance |  |
| F26 | explore | fs-ch-sbv (SBV balance sheet) | FINSYS.sbv.balance_sheet | mostly not published; IMF Art IV projections | same | **structural** |  | Finance |  |
| F27 | explore | fs-ch-gap (credit–deposit gap) | FINSYS.monthly | T7/26 | Aug table pending | **waiting** |  | Finance |  |
| F28 | explore | fs-ch-rates (deposit, OMO, O/N, CPI, real rate) | FINSYS.why.series; CPI | Aug/Sep | Sep MBS pending | **waiting** |  | Finance | ~mid-Oct |
| F29 | explore | fs-ch-npl (3 definitions) | FINSYS.ratios | 6/2026 | same | **current** |  | Finance |  |
| F30 | explore | fs-ch-roe (ROE/ROA) | FINSYS.ratios.roe/roa | 2025 | same | **current** |  | Finance |  |
| F31 | explore | fs-ch-omo (SBV operations) | FINSYS.sbv.operations | to 02/10/2026 | daily | **current** |  | Finance | weekly |
| F32 | explore | fs-ch-res (monthly reserves) | monthly_supplement.fx_reserves_* | 2026-M07 | Aug-2026 IL not verified | **waiting** |  | Finance |  |
| F33 | appendix | #bk-table league table (28 banks) | /api/banks (server.py VN_BANK_FUNDAMENTALS FY2024 + banks_vnstock.json market + Finance Q2-2026 loans) | prices 06/10 (live refresh); fundamentals FY2024; latest loans Q2-2026 | FY2025 audited (by 31/03/2026) and Q2-2026 interim FS published; vnstock Finance hosts (VCI/MAS/KBS) blocked from sandbox (tested 07/10) | **stale** | Proposal (server/main session): rebuild banks_vnstock.json where Finance hosts are reachable (Render), or let Finance curate FY2025 + Q2-2026 fundamentals; Q3-2026 due 20–31/10 | Finance + main session (server) | 2026-10-31 |
| F34 | appendix | #bk-alert-strip | derived from FY2024 fundamentals | FY2024 | FY2025/Q2-2026 | **stale** | Follows F33 | main session (server) |  |
| F35 | appendix | #bk-indicators (10 sparklines) | /api/banks history (FY2024 reported; synthetic excluded) | FY2024 | FY2025/Q2-2026 | **stale** | Follows F33 | main session (server) |  |
| F36 | appendix | #bk-bs-assets/leq, #bk-is-list (+ donuts, modal) | /api/banks/statements (FY2024 × fixed scale factors; quarter view labelled to Q1/2026, flagged "model") | FY2024-based model; labelled model | Q2-2026 actual FS exist | **stale** | Replace modelled system statements with reported aggregates (FY2025, Q2-2026) or hide quarter view | main session (server) |  |
| F37 | appendix | #bk-lend-*/#bk-fund-* (breakdown 100% bars) | /api/banks/breakdown (FY2024 shares) | FY2024 model | Finance holds Q2-2026 sector loans for 17 banks | **stale** | Use FINSYS.listed_banks_by_sector (Q2-2026) for lending by sector | main session (server) / Finance |  |

### Investing (invest)

| # | Chapter | Chart | Series (file · path) | Showing | Latest published | Status | Proposed update (value · period · source) | Owner | Next |
|---|---|---|---|---|---|---|---|---|---|
| I01 | iv-st-hero | ivStGdp (Q1–Q3 2026 + Q4 needed) | invest_research.json _macro (as_of 03/10); invest_macro.json MACRO.gdp | Q3-2026 9.95%; Q4 needed ~12.6% | same | **current** |  | Investing / Economy | 2027-01-03 (Q4) |
| I02 | iv-st-macro | ivStDemand (9M demand components) | _macro.financials | 9M-2026 | same | **current** |  | Investing |  |
| I03 | iv-st-macro | ivStReason (macro support/risks) | _macro.reasoning | 03/10 | same | **current** |  | Investing |  |
| I04 | iv-st-market | ivStVni (forecasts + end-Sep close) | _market.forecasts; VN-Index end-Sep 1,768.62 | end-Sep close (labelled) | VN-Index 06/10 close 1,759.08; 07/10 intraday 1,750.15 (vnstock VCI) | **current** | Chart is explicitly end-Sep; live level is in the explore tools | Investing |  |
| I05 | iv-st-market | ivStReason (market) | _market.reasoning | 03/10 | same | **current** |  | Investing |  |
| I06 | iv-st-stocks | ivStCards (POW, PVT, GAS, GEE targets) | invest_research.json per stock | H1-2026 results; targets to 03/10 | Q3 results 20–31/10 | **waiting** |  | Investing | 2026-10-31 |
| I07 | iv-st-cal | Event calendar | _market/_stock events | future events | same | **current** |  | Investing |  |
| I08 | explore | #iv-macro (ivRenderMacro) | /api/invest/context → data/invest_macro.json | 9M-2026 / 30/09 | same | **current** |  | Economy (file owner) | 2026-11-03 |
| I09 | explore | #iv-market, #iv-screen, compare, price/RSI charts | /api/invest/* (vnstock live) | live (server refresh) | live | **current** |  | server | daily |

### Simulation (sim)

| # | Chapter | Chart | Series (file · path) | Showing | Latest published | Status | Proposed update (value · period · source) | Owner | Next |
|---|---|---|---|---|---|---|---|---|---|
| M01 | sim-st-hero | Hero (12-month deposit rate path + drivers) | simulation.json SIM.panel dep12_vcb, dep12_private_mbs | VCB Sep-26 5.9%; private avg Aug-26 8.4% | Sep MBS pending | **current** |  | Finance |  |
| M02 | sim-st-story | Panel: policy rates (refi, rediscount, O/N, OMO) | SIM.panel.series.*_rate | Sep-2026 | Sep-2026 | **current** |  | Finance |  |
| M03 | sim-st-story | Panel: 10-year government bond yield | SIM.panel.series.gov_bond_10y | Jul-2026 4.36% | Sep-2026 auctions 4.67–4.80% (+0.17 pp vs Aug); Aug auctions held (27,756 bn raised) | **stale** | Add Aug and Sep-2026 points (HNX auction winning yields; Sep 4.67–4.80% range — store the range or the month-end auction value, with source) · [src1](https://vnbusiness.vn/thang-9-huy-dong-trai-phieu-chinh-phu-tang-gan-20.html) [src2](https://baodauthau.vn/huy-dong-hon-27756-ty-dong-trai-phieu-chinh-phu-qua-dau-thau-trong-thang-8-post207553.html) | Finance | HNX weekly |
| M04 | sim-st-story | Panel: Fed funds upper, Fed − refi | SIM.panel.series.fed_funds_upper | Sep-2026 4.00% | FOMC 16/09/2026 raised to 3.75–4.00% (confirmed) | **current** |  | Finance | FOMC 2026-10-28 |
| M05 | sim-st-story | Panel: interbank O/N | SIM.panel.series.interbank_on | Sep-2026 (month-end 7.0) | Sep average pending | **waiting** |  | Finance | ~mid-Oct |
| M06 | sim-st-story | Panel: credit YTD / YoY | SIM.panel.series.credit_ytd/yoy | Sep-2026 11.59 / 16.69 | same | **current** |  | Finance | ~2026-11-03 |
| M07 | sim-st-story | Panel: deposit YTD / YoY; credit–deposit gap | SIM.panel.series.deposit_ytd/yoy, credit_deposit_gap_* | Jul-2026 | statement basis already in finance.json: 22/8 8.77%, 28/9 9.78% YTD; SBV Aug table pending | **stale** | Either add the statement-basis points (labelled "statement, cut-off date") or keep table basis and mark Aug/Sep as pending; projection driver credit_deposit_gap is Jul | Finance | ~mid–late Oct |
| M08 | sim-st-story | Panel: LDR (SBV, simple listed), LDR cap | SIM.panel.series.ldr_* | Jun-2026 / Q2-2026 | same | **current** |  | Finance |  |
| M09 | sim-st-story | Panel: M2 YoY / YTD, cash/M2 | SIM.panel.series.m2_*, cash_to_m2 | Jul-2026 | Aug pending | **waiting** |  | Finance | ~mid–late Oct |
| M10 | sim-st-story | Panel: VN-Index, VN-Index 12m | SIM.panel.series.vnindex | Sep-2026 1,768.62 | Sep (month series) | **current** |  | Finance | 2026-11-01 |
| M11 | sim-st-story | Panel: world gold (USD, 12m, m/m) | SIM.panel.series.gold_world_* | Sep-2026 | Sep | **current** |  | Finance |  |
| M12 | sim-st-story | Panel: SJC gold sell (monthly) | SIM.panel.series.sjc_gold_sell | points: Dec-25 152.8, Jan-26 184.2, 6/10 143.5; Feb–Sep 2026 empty | month-end SJC quotes Feb–Sep 2026 are published daily (e.g. 25/8 ~147.6–150.6) | **stale** | Fill month-end SJC sell quotes Feb–Sep 2026 from dated press quotes (one URL per month); keep nulls where no month-end quote is found | Finance | daily |
| M13 | sim-st-story | Panel: margin lending | SIM.panel.series.margin_lending | Jun-2026 453.8k | Q3 tallies late Oct | **waiting** | See F10 coverage conflict | Finance | 2026-10-31 |
| M14 | sim-st-story | Panel: new securities accounts | SIM.panel.series.new_stock_accounts | Jul-2025 226,000 (last point) | VSDC monthly through Sep-2026 (e.g. Jan-2026 ~245,000) | **stale** | Add Aug-2025…Sep-2026 monthly VSDC counts; Jan-2026 ~245,000 found. Beware 2024 articles ("Sep: 172,605") ranking first · [src1](https://mekongasean.vn/gan-245000-tai-khoan-chung-khoan-mo-moi-thang-dau-nam-2026-51691.html) | Finance | VSDC ~5th of month |
| M15 | sim-st-story | Panel: real estate (Hanoi primary price) | SIM.panel.series.real_estate | 2025-Q4 102 m VND/m² (Savills) | Savills Q2-2026 ~116 m VND/m² (+27% y/y); CBRE Q2 ~95 m | **stale** | Add 2026-Q2 (Savills) with provider label; Q3 reports due mid-Oct · [src1](https://cafef.vn/chung-cu-ha-noi-vang-bong-can-ho-duoi-70-trieu-dong-m2-188260814143655062.chn) | Finance | mid-Oct (Q3) |
| M16 | sim-st-story | Panel: corporate bonds (issuance, outstanding) | SIM.panel.series.corporate_bonds | issuance: 2022 only; outstanding Jun-2026 1,435,040 | 8M-2026 issuance ~349,000 bn (Aug 32,029 bn, VBMA) | **stale** | Add 2026 YTD issuance (8M ~349,000 bn: private ~296,000 + public ~53,000), plus 2023–2025 annual totals if sourced · [src1](https://doanhnhan.baophapluat.vn/thi-truong-trai-phieu-doanh-nghiep-huy-dong-gan-349-000-ty-dong-sau-8-thang.html) | Finance | VBMA Sep report ~mid-Oct |
| M17 | sim-st-story | Panel: public investment | SIM.panel.series.public_investment | 9M-2026 62.9% | same | **current** |  | Finance (copied from Economy/Policy) | ~2026-11-03 |
| M18 | sim-st-story | Panel: Treasury deposits; monthly budget balance | SIM.panel.series.treasury_deposits, budget_balance_monthly | points to 2026-06; budget balance empty (gap) | MoF monthly execution exists (9M cash surplus ~313.6 tn, held in economy.json exec_9M2026) | **stale** | Fill budget_balance_monthly at least for quarter-ends from Economy's exec data (cash basis, labelled), or drop the empty panel | Finance | ~2026-11-03 |
| M19 | sim-st-story | Panel: CPI, core CPI, real deposit rate | SIM.panel.series.cpi_yoy/core/real_dep_rate | Sep-2026 | Sep | **current** |  | Finance | 2026-11-03 |
| M20 | sim-st-story | Panel: USD/VND, DXY | SIM.panel.series.usdvnd_*, dxy | Sep-2026 | Sep | **current** |  | Finance | 2026-11-01 |
| M21 | sim-st-story | Panel: Brent | SIM.panel.series.brent (EIA via github datasets) | Aug-2026 91.08 | Sep monthly average not verified (EIA STEO 07/10) | **waiting** | Re-download after the dataset updates | Finance | ~2026-10-10 |
| M22 | sim-st-story | Panel: cost of funds, NIM, CASA (listed) | SIM.panel.series.cof/nim/casa_listed | Q2-2026 | Q3 FS late Oct | **waiting** |  | Finance | 2026-10-31 |
| M23 | sim-st-story | Panel: SBV new-loan average rate | SIM.panel.series.lending_new_avg_sbv | points to 2025-08 only (not drawn on the monthly grid) | SBV range Aug-2026 held in finance.json (8.4–10.7, different concept: SOCB/JSCB new+outstanding) | **structural** | Do not splice the range with new-loan averages | Finance |  |
| M24 | sim-st-proj | Projection fan + scenarios (6 months to Mar-2027) | SIM.projection (start Sep-2026) | start 5.9% (Sep) | drivers: gap Jul, CPI Sep, gold Sep, VNI Sep | **current** | Rebuild after M07 and the Aug SBV tables | Finance |  |
| M25 | sim-st-proj | Sliders | SIM.sliders | Sep-2026 | same | **current** |  | Finance |  |
| M26 | sim-st-model | Model card, coefficients, backtest | SIM.model/backtest | calibrated 06/10 | n/a | **current** |  | Finance |  |
| M27 | sim-st-src | Indicators / coverage table | SIM.indicators_added, gaps | 06/10 | gaps list (26) — several fillable (M12, M14, M16, M18) | **current** |  | Finance |  |

★n = priority-watch rank.

## 4. Broken

- **E29 ecb-labour (enterprises, unemployment, PCI, IIP)** — year 2025 selected: PCI tile shows 67.7 — the 2024 value (pci_median[2025] is null; calcEcon falls back to the nearest year without saying so). Latest published: VCCI PCI 2025 published 15/05/2026: median 63.90 (first PCI on 34 provinces, PCI 2.0 method — not comparable with 2024 67.67). Fix: Economy: pci_median[2025] null → 63.90 with a vintage/method note (PCI 2.0, 34 provinces). Ed: label any nearest-year fallback with its year. Sources: https://www.vcci.com.vn/tin-tuc/lan-dau-cong-bo-xep-hang-nang-luc-canh-tranh-34-tinh-thanh-pho, https://tapchicongthuong.vn/buc-tranh-nang-luc-canh-tranh-cap-tinh-sau-sap-nhap-va-top-5-dia-phuong-co-chat-luong-dieu-hanh-xuat-sac-nhat-517890.htm. (search-engine excerpt, 2026-10-07 (page fetch blocked by egress proxy); median 63.90 from result-set summary, outlet not pinned — confirm in VCCI report)

Near-misses (not classified broken, but the owner should label them):
- `FINSYS.monthly.credit_level/credit_ytd` T8/26 (20,500,000; 10.24%, cut-off 28/8, "about 20.5 quadrillion") and T9/26 (20,750,000; 11.59%, 30/9 statement) are rounded statement figures appended after exact SBV month-end table values (T6/T7). One line, two bases — drawn in P10, F05, F22 and the Sim credit panels.
- Financial-system hero dek quotes credit +11.59% (30/9) above `fsStGap`, which ends 28/9 (10.89%).
- Society hero reads `POP_SERIES.nat` (2024: growth 1.03%) although `nat_latest` (2025 prelim: 0.99%, TFR 1.93) is in the same file (S02).
- Outbound departures 9M-2026 −21.2% vs Q3 +16.4% (same NSO excerpt) still unreconciled (Economy, carried from the priority-watch pass).

## 5. Prioritised update list per owner (proposals only — nothing applied)

Each item: file · field · current → proposed · period · source · verification. All values are excerpt-level unless stated; the owning agent re-verifies before editing and asks the user first (standing rule).

### Economy (`data/economy.json`, `data/invest_macro.json`)
1. **PCI 2025 (E29, broken tile).** `ECON_OFFICIAL.pci_median[2025]` null → **63.90** · PCI 2025 (VCCI, 15/05/2026) · https://www.vcci.com.vn/tin-tuc/lan-dau-cong-bo-xep-hang-nang-luc-canh-tranh-34-tinh-thanh-pho · median from a result-set summary, outlet not pinned — confirm in the VCCI report. Add a method note: first PCI on 34 provinces, "PCI 2.0" — not comparable with 2024 (67.67, 63 provinces).
2. **IMF WEO Oct-2026 (E01, E19, E20, E21; same day as Finance F20)** — on/after 13/10 (09:00 Bangkok): replace `projections.series.gdp_growth_pct_imf`, `gdp_usd_bn_imf`, `gdp_per_capita_usd_imf`, `cpi_pct_imf` and the `CPI_FC_INST` "IMF (WEO Apr-2026)" row; keep the April vintage in a note. https://www.imf.org/en/publications/weo/issues/2026/10/13/world-economic-outlook-october-2026
3. **WB CPI 2027 conflict (E19).** Held `cpi_pct_wb` 2027 = 3.8 (May VEU); one excerpt reads 3.7. Re-read the VEU table; check whether the Oct-2026 EAP Update states a Vietnam CPI forecast. Do not average.
4. **Waiting, priority watch:** HCMC 9M-2026 remittances (~20–25/10), SBV BoP Q2-2026 (check 20/10), DOLAB 9M workers sent (held 8M: 90,319), FDI/tourism Jan–Oct on 03/11.
5. Optional part-year labels in appendix cards (E24, E26, E27, E28, E30): the 9M-2026 values are already in `ECON_OFFICIAL.ytd_2026` / `ECONFLOW.gdp.growth_real_pct` — a display change for Ed, no new data.

### Society (`data/society.json`)
1. **Provincial vital rates to the 1/4/2025 survey (S03, S04, S06, S08, S12).** `SOC_PROV_DATA[*].tfr` (and urban_pct, sex_ratio, cbr, cdr, life_exp) reference 2024 → 2025 reference. Excerpt values: HCMC 1.43 → **1.51**, Đồng Tháp 1.71 → **1.61**, Vĩnh Long 1.60 → **1.70**, Cần Thơ 1.55 → **1.63**, Cà Mau 1.58 → **1.55** (national 1.93). Source: NSO "Kết quả chủ yếu Điều tra biến động dân số và KHHGĐ thời điểm 01/4/2025" https://thuvienso.quochoi.vn/bitstream/11742/110630/1/Cuc%20thong%20ke.%20Ket%20qua%20chu%20yeu%20dieu%20tra%20bien%20dong%20dan%20so%20va%20ke%20hoach%20hoa%20gia%20dinh%20thoi%20diem%2001.4.2025.pdf (PDF on the NA library; try it — it may be reachable where nso.gov.vn is not). Keep the 2024 values as `PROV_PREV`-style history if the page needs a comparison. Then the Flourish scatter CSV is rebuilt (main session).
2. **National reference year (S02).** Roll `POP_SERIES.nat` to 2025 (prelim; already held in `nat_latest`) or ask Ed to read `nat_latest` — the hero still says 1.03% (2024).
3. **GRDP 9M-2026 for 4 provinces (S07).** `SOC_GRDP[Điện Biên|Quảng Trị|Huế|Gia Lai].growth_9m["2026"]` null → values from provincial statistics offices; lead: https://tapchikinhtetaichinh.vn/12-tinh-thanh-pho-tang-truong-grdp-2-con-so-trong-9-thang-nam-2026-168393.html. Leave null if not found (Gia Lai H1 8.21%, Huế H1 9–9.5% are H1, not 9M).
4. Waiting: WDI December (ranks), population 2026 (03/01/2027), GRDP Q4 (29/12, confirm).

### Policy (`data/policy.json`)
Nothing stale (registry to 06/10). Watch: NA session 17/10–20/11 (2027 budget, any VAT/fuel-relief extension beyond 31/12/2026), central rate weekly. Green-credit figure in `mon` instruments (~828 tn, 9/6) — see the Finance conflict below.

### Finance (`data/finance.json`, `data/simulation.json`)
1. **Sim gov-bond 10Y (M03).** `SIM.panel.series.gov_bond_10y` last Jul-2026 4.36 → add Aug-2026 and Sep-2026 (Sep auctions 4.67–4.80%, +0.17 pp vs Aug) · HNX auctions · https://vnbusiness.vn/thang-9-huy-dong-trai-phieu-chinh-phu-tang-gan-20.html (excerpt; article year not pinned — confirm on HNX).
2. **Sim deposit growth (M07).** `deposit_ytd/yoy`, `credit_deposit_gap_*` stop at Jul-2026 while `FINSYS.why.series.credit_vs_deposit_growth` already holds 22/8 8.77% and 28/9 9.78% YTD (statement basis). Decide the basis; label it; the projection driver `drivers_now.credit_deposit_gap` is Jul.
3. **Sim corporate bonds (M16).** `corporate_bonds.issuance` has 2022 only → add 2026 YTD: 8M ≈ 349,000 bn (private ≈ 296,000, public ≈ 53,000; Aug 32,029 bn, 33 issues) · VBMA via https://doanhnhan.baophapluat.vn/thi-truong-trai-phieu-doanh-nghiep-huy-dong-gan-349-000-ty-dong-sau-8-thang.html.
4. **Sim real estate (M15) and alternatives YTD (F03).** Hanoi primary (Savills) 2025-Q4 102 → add 2026-Q2 ≈ **116 m VND/m² (+16% q/q, +27% y/y)**; CBRE Q2 ≈ 95 m (different provider — separate series). Lead: https://cafef.vn/chung-cu-ha-noi-vang-bong-can-ho-duoi-70-trieu-dong-m2-188260814143655062.chn (outlet for 116 not pinned). Q3 reports due mid-Oct — may be better to wait one week and add Q3 directly.
5. **Sim new securities accounts (M14).** Last point Jul-2025 → add monthly VSDC Aug-2025…Sep-2026; found Jan-2026 ≈ 245,000 (https://mekongasean.vn/gan-245000-tai-khoan-chung-khoan-mo-moi-thang-dau-nam-2026-51691.html). Pitfall: "Sep: 172,605 / 7.76 m individual accounts" articles are 2024.
6. **Sim SJC monthly (M12).** Fill month-end SJC sell quotes Feb–Sep 2026 (one dated URL per month; e.g. 25/8 147.6–150.6 is not month-end).
7. **Sim budget balance (M18).** Empty panel; quarter-end cash balances can be copied from Economy (`exec_9M2026.balance` 313,600 bn surplus 9M, cash basis) — or drop the panel.
8. **Plumbing fact (F18).** `sbv.plumbing.facts.gov_bond_issued_2026_ytd` (261,108 bn to 16/09) → to 30/09 from the HNX/MoF September total.
9. **Conflicts to record, not average (F10):** green credit 828,000 bn (stated 9/6/2026, SBV–GIZ forum; held) vs "over 780,000 bn" (SBV H1 briefing 2/7/2026, excerpt https://thitruongtaichinhtiente.vn/den-cuoi-thang-6-2026-tang-truong-tin-dung-dat-7-41-83990.html); margin Q2-2026 453,800 (held) vs 446,000 (80/85 firms, https://vneconomy.vn/du-no-margin-ky-luc-hon-446-nghin-ty-dong-phan-lon-tap-trung-vao-hoat-dong-cho-vay-theo-deal-rieng.htm) vs ≈435,000 (estimate) — coverage differs.
10. **Labels:** statement-basis credit points T8/T9 in `FINSYS.monthly` (see §4); WB "+1.1 pp" in `projections.series.real_gdp_growth_pct_wb` is vs the April-2026 EAP (6.3%, https://en.vneconomy.vn/wb-forecasts-vietnams-2026-gdp-growth-at-63.htm); a mid-May 2026 World Bank vintage put 2026 at 6.8% (https://www.businesstoday.com.my/2026/05/15/world-bank-expects-vietnams-economy-to-slow-to-6-8-in-2026/), so the step from the latest earlier vintage is +0.6 pp — state which vintage the "+1.1 pp" compares with.
11. **Bank fundamentals (F33–F37)** with the main session: FY2025 audited and Q2-2026 interim are published; the server still shows FY2024 (+ modelled history). Finance already holds Q2-2026 loans by sector for 17 banks (`listed_banks_by_sector`) — usable for `#bk-lend-sector`.
12. Waiting (do not poll before): SBV Aug M2/deposit/sector tables (~mid–late Oct), MBS Sep deposit note and Vietcap Sep interbank average (~mid-Oct), Brent Sep (~10/10), bank and securities-firm Q3 FS (20–31/10), IMF WEO (13/10), FOMC 28/10.

### Strategy (`strategy_directives.json`)
Nothing stale (directives to 02/10, news to 06/10; Central Committee plenum 4 in progress). NA session 17/10–20/11: update draft stages twice weekly.

### Investing (`invest_research.json`, served via `/api/invest/research`)
Nothing stale. Stock cards (POW, PVT, GAS, GEE) wait for Q3 results (20–31/10). The VN-Index chart is explicitly "end-September" (1,768.62); the live level is in the explore tools.

### Ed / main session (page and server — proposals only)
1. E29: label any nearest-year fallback in `calcEcon` (today the 2025 PCI tile silently shows 2024).
2. P13 / F17: draw the latest monthly IMF IL reserve point (Jul-2026: 86.10 bn incl. gold, 82.81 bn excl. gold) already in `FINSYS.sbv.balance_sheet.monthly_supplement`.
3. S02: read `POP_SERIES.nat_latest` in the Society hero if Society keeps `nat` at 2024.
4. F33–F37: server bank fundamentals — rebuild `banks_vnstock.json` on a host where vnstock Finance sources are reachable (Render), or source FY2025 / Q2-2026 from Finance; retire the FY2024 scale-factor statements.
5. E30: `GDP_SECTORS` 2000–2009 values are unsourced round numbers in an unowned page constant — source them or start the chart in 2010 (`GDP_SUB_OFFICIAL` starts 2010).

## 6. Sources used this run (search excerpts unless noted)
- https://www.nso.gov.vn/bai-top/2026/10/bao-cao-tinh-hinh-kinh-te-xa-hoi-quy-iii-va-9-thang-nam-2026/
- https://www.imf.org/en/publications/weo/issues/2026/10/13/world-economic-outlook-october-2026
- https://baovanhoa.vn/du-lich/khach-quoc-te-den-viet-nam-9-thang-2026-tang-145-271196.html
- https://vneconomy.vn/ty-gia-on-dinh-giua-luc-can-doi-ngoai-te-kem-thuan-loi.htm
- https://www.nso.gov.vn/tin-tuc-thong-ke/2026/10/thong-cao-bao-chi-ve-tinh-hinh-gia-thang-chin-quy-iii-va-9-thang-nam-2026/
- https://www.businesstoday.com.my/2026/05/15/world-bank-expects-vietnams-economy-to-slow-to-6-8-in-2026/
- https://vneconomy.vn/xuat-nhap-khau-9-thang-dat-tren-888-ty-usd-tai-lap-trang-thai-xuat-sieu.htm
- https://www.vcci.com.vn/tin-tuc/lan-dau-cong-bo-xep-hang-nang-luc-canh-tranh-34-tinh-thanh-pho
- https://tapchicongthuong.vn/buc-tranh-nang-luc-canh-tranh-cap-tinh-sau-sap-nhap-va-top-5-dia-phuong-co-chat-luong-dieu-hanh-xuat-sac-nhat-517890.htm
- https://thuvienso.quochoi.vn/bitstream/11742/110630/1/Cuc%20thong%20ke.%20Ket%20qua%20chu%20yeu%20dieu%20tra%20bien%20dong%20dan%20so%20va%20ke%20hoach%20hoa%20gia%20dinh%20thoi%20diem%2001.4.2025.pdf
- https://nhandan.vn/dan-so-va-lao-dong-viec-lam-tiep-tuc-on-dinh-nam-2025-tao-da-cho-2026-post946357.html
- https://tapchikinhtetaichinh.vn/12-tinh-thanh-pho-tang-truong-grdp-2-con-so-trong-9-thang-nam-2026-168393.html
- https://cafef.vn/chung-cu-ha-noi-vang-bong-can-ho-duoi-70-trieu-dong-m2-188260814143655062.chn
- https://thitruongtaichinhtiente.vn/den-cuoi-thang-6-2026-tang-truong-tin-dung-dat-7-41-83990.html
- https://vneconomy.vn/du-no-margin-ky-luc-hon-446-nghin-ty-dong-phan-lon-tap-trung-vao-hoat-dong-cho-vay-theo-deal-rieng.htm
- https://tapchikinhtetaichinh.vn/loi-nhuan-ngan-hang-quy-iii-du-bao-tang-19-nhom-quoc-doanh-dan-dat-168063.html
- https://vnbusiness.vn/thang-9-huy-dong-trai-phieu-chinh-phu-tang-gan-20.html
- https://en.vneconomy.vn/wb-forecasts-vietnams-2026-gdp-growth-at-63.htm
- https://baodauthau.vn/huy-dong-hon-27756-ty-dong-trai-phieu-chinh-phu-qua-dau-thau-trong-thang-8-post207553.html
- https://www.securities.io/fomc-raises-federal-funds-target-range-to-3-3-4-to-4-percent/
- https://mekongasean.vn/gan-245000-tai-khoan-chung-khoan-mo-moi-thang-dau-nam-2026-51691.html
- https://doanhnhan.baophapluat.vn/thi-truong-trai-phieu-doanh-nghiep-huy-dong-gan-349-000-ty-dong-sau-8-thang.html
- https://vneconomy.vn/kieu-hoi-ve-tp-ho-chi-minh-nam-2025-uoc-dat-105-ty-usd.htm
- https://www.vietnamplus.vn/du-lich-viet-don-177-trieu-khach-sau-9-thang-thi-truong-nao-dang-tang-toc-post1139852.vnp
- vnstock_data 3.3.1 (VCI Quote) — VN-Index daily to 07/10/2026 (run from the sandbox, 2026-10-07)
