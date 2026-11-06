# Cross-tab consistency report

## Run 2026-11-06 (weekly pre-approved run)

Files read after all seven tab agents finished, with the page already embedded. Primary checks: NSO October report docx (read directly), Decree 13/2026 text, IMF DataMapper API. Status: **pass**, **fixed** (corrected in the owner's file this run), **differs** (real conflict, both kept), **decision** (user's call). Older tables below are kept as history.

| # | Check | Tabs and values | Status | Ruling and action |
|---|---|---|---|---|
| W1 | Public investment 10M | MoF disbursement 708,567.4 bn = 74.1% of 955,867.4 bn to 31/10 (Economy, Policy, Investing); NSO 783.9 tn = 69.05%, +19.8% (Economy, Finance); budget development spending ~735 tn, 65.6%, +49.7% | **fixed** | Three concepts. NSO calls its figure "vốn đầu tư thực hiện từ nguồn NSNN", not disbursement. **Headline for disbursement: MoF.** Finance driver 8 called the NSO figure "disbursement"; relabelled and the MoF figure added. Economy's URL pointed at the budget infographic (206259), moved to 206473. Yoy also differs by concept: MoF +217.5 tn (19.7 pts), NSO +19.8%, development spending +49.7%. |
| W2 | Disbursement rate over time | 62.9% (30/9), 65.9% (22/10) on 1,022,607; 74.1% (31/10) on 955,867.4 | **fixed** (label) | Not comparable. On the old base 31/10 is ~69.3% (derived). The October gain is 65.6 tn (derived). The 66,739.6 bn base cut is unexplained in every source read. The 69.3 and NSO's 69.05 look alike and are unrelated. Policy now says so in its current text. |
| W3 | Budget revenue 10M | NSO 2,438.9 tn, 96.4%, +11.0% (Economy) vs 2,448.5 tn, 96.8%, +11.5% (Policy lead, Nhân Dân citing MoF) | **fixed** | **Lead: 2,438.9.** It is in the NSO report (read), in Thời báo Tài chính citing the MoF (02/11) and at the Government briefing (04/11: 2.44 quadrillion, 96.4%, +11%), and it continues the 9M column (2,187.3 + October 251.5). Policy switched: stance, current text, history; revenue less spending ~330.2 tn. 2,448.5 stays as the alternate. Cause of the gap unknown. |
| W4 | Credit latest | 11.94% at 26/10 (Finance) vs ~12.27% (Policy) | **differs**, labels **fixed** | Both kept, not averaged, neither is end-October. 11.94 at 26/10, above 20.8 quadrillion, is in VnExpress's own text, not a quote of the Deputy Governor, so Finance's basis note was reworded; it is still single-source. 12.27 is in the Governor's remarks with no date. Policy now carries both; Finance now records the 12.27. The page labels the T10/26 point with its cut-off (`fsCutLab`), so no false month-end appears. |
| W5 | Finance "credit slowed" | Market view only; not in any data string | pass (data) | Wording for the Narrator: cumulative growth **rose** 11.59 to 11.94. Only the pace was slower: +0.35 pt in 26 days of October vs +1.40 pt in September (derived; month-ends usually run high). Do not write "slowed" without "pace". |
| W6 | Credit y/y at 30/9 | Policy +16.6 vs Finance +16.69 | **fixed** | 16.69 (VNBA page read). Policy corrected. |
| W7 | VND deposits vs credit | Policy +12.65 vs +10.2 at 21/10; Finance had no such point | **fixed** | Added to Finance driver 1 as VND-only scope. Confirmed in the bnews briefing report. Not comparable with the system 10.19 / 7.60 at end-August. |
| W8 | Central rate | 25,615 at 6/11 (both; page read), 25,644 at 29/10, 25,637 at 30/10 month-end (Finance) | pass; one **fixed** | Dates differ, all right. Depreciation 1.93% and +1.97% are the two sides of the same move. Finance note "-21 đồng" was an arithmetic slip, now -22 (25,637 to 25,615). |
| W9 | SBV swap | Policy: up to USD 2bn, 182 days, spot 24,399, forward 24,704; Finance: "size undisclosed", "from 3/11" | **fixed** | Page read (Vietstock 04/11). Finance driver, summary, operation and flow updated. Announced 3/11, planned 6/11, value 9/11, result unknown. |
| W10 | Government bonds | 368,422 bn to 30/10 (BVSC, 92%) vs 345.9 tn (MoF, 86.5%, no cut-off) | **differs** | Both on the 400 tn plan (both percentages check). They may be different cut-offs: 345.9 is close to 343,079 at 21/10. That is inference, not confirmed. The ~42 tn "remaining" in the BVSC-based article matches neither. Keep Policy's lead (the dated figure) and await the HNX October total. |
| W11 | Bond plan 500 to 400 tn | Policy current_value 400 tn | pass with caveat | Two lines support it: the BVSC-based article and the Government briefing (86.5% of 400 tn). No legal document found. Keep 400 with "document not identified". Finance's 59.8% and 52.2% are on the old 500 base: note added, chapter text due a refresh. |
| W12 | Trade 10M | Exports 499.24, imports 511.31, balance -12.07, US +140.10, China -134.94, US exports 159.40 (Economy, Geopolitics, Investing) | pass | All equal the NSO text. |
| W13 | FDI 10M | 54.85 (+74.0%), 24.08 (+13.1%), components 30.73 + 16.13 + 7.99 | pass; flag **fixed** | All equal the NSO text. `MACRO.fdi.economy_sync` resolved; Investing's URL moved to the NSO report. |
| W14 | CPI October | 5.09 y/y, 4.57 avg, 4.41 vs Dec, 0.20 m/m, core 4.04 / 4.24 (Economy, Finance, Policy, Investing) | pass | Equal to NSO. Investing's old `yoy_sep`, `avg9m` stay on purpose, labelled, with `latest` beside them. Page tiles still read the old keys (P-13). |
| W15 | IMF vintage | Economy and Society WEO Oct-2026; Finance CPI WEO Apr-2026, growth to 2027 only, scenarios Apr-2026 | **fixed** | API read directly: 8.4/7.5/5.9/5.9/5.7 and 4.7/4.9/4.7/4.6/4.2. Finance aligned. Nominal GDP and credit/GDP recomputed by the stated formula (the old values reproduced exactly first): credit/GDP base 2030 178.1 to 168.1. No narrated figure depends on it. |
| W16 | Public debt 2026 | Plan 36-37% (Economy projections, page sentence) vs Government 33-34% (Oct estimate, Economy addendum) | **decision** | See update_plan.md. Recommendation: keep the plan series as the January plan and show the October estimate beside it. Not changed. |
| W17 | Debt in Policy budget_2027 note | "government debt ~33-34% end-2027" | **fixed** | The cited article says public debt 33-34% end-2026, 34-35% end-2027 (page read; Dân Việt agrees). |
| W18 | NA session close | Strategy and Policy 21/11; one Policy VI note 20/11; calendar 20/11 | **fixed** | 21/11. Phase 1 ends 13/11. |
| W19 | Fuel measures | Excise 0% to 31/3/2027 (Economy, Policy); environment tax and duty to 31/12/2026 | pass | Same dates and resolutions. |
| W20 | IMF ranks | Society 542.13 / 5,259 / 8.4 vs Economy 542.1 / 5,259 / 8.4 | pass | Same vintage. |
| W21 | Decree 13/2026 GRDP | Calendar said 29 Dec | **fixed** | Text read: 29 November. |

Closed from the 2026-10-06 tables: A6 and A7 (Finance: no month shift; rows are month-end), A13-A15 (Economy's 2025 budget column now 2,650,100 / 2,401,500 / 3.6), A26 (green credit 828,000 held), A9 and A23 (usd_vnd 2025 = 24,976).

---

## Run 2026-10-06 (first run)

Run: 2026-10-06 (Research, first run). Files read: `data/economy.json` (read while the Economy agent was
still running, so re-check its rows next run), `data/society.json`, `data/policy.json`, `data/finance.json`,
`strategy_directives.json`, `invest.py` (MACRO), `server.py`, plus the data constants inside
`vietnam_dashboard.html` that no data file feeds.

Status: **pass** = values agree, or differ only by a stated definition or date · **differs** = a real conflict ·
**stale** = superseded by a newer release · **suspect** = the evidence points to an error that needs checking.

## A. Figures that appear on more than one tab

| # | Check | Value A (where) | Value B (where) | Status | Which is right / action |
|---|---|---|---|---|---|
| A1 | GDP growth 9M / Q3-2026 | 9.01 / 9.95 (Economy `growth_real_pct`, `ECON_OFFICIAL.ytd_2026`) | 9.01 (Finance drivers text); 9.01 / 9.95 (`invest.py` MACRO) | pass | NSO 03/10/2026. |
| A2 | GDP growth 2025 | 8.02 (`ECON_OFFICIAL.gdp_growth`) | 8.019 (Society `WORLD_RANK.g_growth`, WDI) | pass | Same figure, WDI copy. |
| A3 | CPI Sep-2026 y/y, 9M average | 5.08 / 4.52 (Economy `CPI_LATEST`) | 5.08 / 4.52 (Finance `sbv.constraints`, `why.summary`); 5.08 / 4.52 (`invest.py`) | pass | NSO 03/10/2026. |
| A4 | CPI target 2026 | ~4.5 (Economy `CPI_FC_INST` "NQ 244/2025/QH15"; Finance `projections.targets.cpi_2026`; Finance constraints) | **4.0** (`invest.py` MACRO.cpi.target, Investing tab) | **differs** | 4.5 is right (NA 2026 plan resolution). Fix `invest.py` (proposal P-6). |
| A5 | Credit growth YTD to 30/9 | 11.59 (Finance `monthly.credit_ytd` T9/26) | 11.59 (Policy `credit_growth_target`, `mon.stance`) | pass | SBV. NSO's 10.89 to 28/9 (Finance `why.credit_vs_deposit_growth`) is a different cut-off and is labelled. |
| A6 | Credit level Jun/Jul-2026 | T6/26 = 20,150,411 bn, +8.37% (Finance `monthly`, also `listed_banks_by_sector.sbv_total_bn` "end-Jun") | SBV via press: end-Jun **+7.41%** (~19.97 tn) [baochinhphu 02/07/2026]; **29/7: 20.15 tn, +8.38%** [baochinhphu 03/08/2026] | **suspect** | The T6/26 level equals the 29-Jul figure. Either the SBV table's month label or our mapping is shifted by one month for Jun–Jul 2026 (2025 rows agree with press). T7/26 (20,262,110; +8.97%) has no press match. Finance to re-read the SBV table headers; until then the bank-coverage ratio (17 banks at 30/6 vs system) compares different dates. |
| A7 | Finance `why.credit_vs_deposit_growth` 22-Aug point | credit 8.38 (labelled "22-Aug, VND-only, MoF deputy minister") | SBV: 8.38 = to 29/7 | **suspect** | Probably the 29-Jul SBV figure re-quoted later. Finance to verify the source text. |
| A8 | Central rate | 25,643 on 6/10 (Policy `central_rate`, stance) | 25,636 on 2/10 (Finance `sbv.constraints[fx_pressure]`, depreciation 2.01%); 25,627 end-Sep (Finance `usdvnd_monthly`) | pass (dated) | All correct for their dates; Finance's constraint item will drift. Suggest Finance quote Policy's latest date at each run. |
| A9 | USD/VND annual average 2025 | null (`ECON_OFFICIAL.usd_vnd`) | ≈24,978 implied by NSO GRDP per capita USD (Society); legacy `FX_USD[15]` = 25,450 (page) | **differs** | NSO-implied 24,978 is the documented one; the legacy constant has no source. Economy to fill `usd_vnd` 2025 with the NSO rate. |
| A10 | FX reserves end-2024 / end-2025 | 83.08 / 85.58 (`ECON_OFFICIAL.fx_reserves_busd`, legacy `FX_RESERVES`) | 83.931 / 86.945 (Finance `sbv.balance_sheet.fx_reserves_busd`, IMF IL); 83.1 (Finance `gross_reserves_imf_art4_busd`, constraints) | differs (definition) | Finance's IMF IL series and Art. IV figures differ by ~0.9–1.4 bn — likely gold/definition. Economy's origin is "IMF reserves (not re-verified)". Both owners should record the IMF series code; do not mix. |
| A11 | FX reserves latest | ~87.6 bn at 18/06/2026 (Economy `bop.note`, Finance constraints, Policy `fx_intervention`) | 86.10 bn Jul-2026 (Finance `sbv.notes`, IMF IL) | pass (dated) | Both sourced; different dates/definitions. |
| A12 | Policy rates | refinancing 4.5, OMO 4.5 since 4 Dec 2025 (Policy) | Finance constraints 4.5; **legacy `SBV_OMO_24` ends at 4.00**; legacy KPI "last change 14/06/2023" | **differs** | Policy is right (OMO 4.5 from 04/12/2025; last policy-rate change effective 19/06/2023, Decision 1123/QĐ-NHNN). Legacy constants are wrong (proposal P-7). |
| A13 | Budget 2025 revenue | 2,681,500 (Economy `budget.revenue.total` 2025); 2,681,469 (`ECON_OFFICIAL.budget_rev_bn`) | 2,650,100 (page `FIS.budget`, MoF Jan-2026); "~2.65 quadrillion" (Policy `budget_outturn`) | **differs** | MoF Jan-2026 execution (2,650,100) is the only traced figure. Economy's 2025 column is marked UNTRACED by Economy itself. |
| A14 | Budget 2025 spending | 3,312,600 (Economy `budget.expenditure.total`) | 2,401,500 (`ECON_OFFICIAL.budget_exp_bn`, `FIS.budget`) | **differs** | Same as A13 — 2,401,500 (MoF) is traced. |
| A15 | Budget deficit 2025 | 3.3% GDP (Economy `budget.deficit_pct_gdp`) | ~3.6% (Policy `budget_outturn` series, MoF report 06/01/2026) | **differs** | Policy's 3.6% is sourced; Economy's 3.3 is untraced. |
| A16 | Budget 2026 plan | revenue 2,529,467; deficit 605,800 = 4.2% (Economy) | 4.2%, 605.8 tn (Policy `budget_deficit_target`, `FIS.budget`) | pass | NA resolution on the 2026 budget. |
| A17 | 9M-2026 budget execution | 2,187.3 tn rev; 1,873.7 tn exp; surplus 313.6 tn (Economy `exec_9M2026`) | same (Policy `budget_outturn`, `FIS.budget`) | pass | MoF, early Oct. |
| A18 | Public investment 2026 | plan ~995.4 tn; 9M 62.9% (Policy) | plan 991,931.6 bn; 642,961.6 disbursed; 62.9% (`invest.py`) | pass (definition) | Same rate; plan bases differ (PM-assigned vs MoF reporting base). Label the basis. |
| A19 | GDP 2026 target | ≥10% (Strategy `vision`, Finance `projections.targets`, invest.py) | — | pass | NQ 25/2026/QH16 and the 2026 plan resolution. |
| A20 | GDP per capita 2025 | 5,026 USD (`ECON_OFFICIAL.gdppc_usd`, NSO) | 5,066 USD (Society `WORLD_RANK.g_gdp_pc`, WDI) | pass (source) | Different sources; the world-ranks chart must say "World Bank". |
| A21 | Population 2025 | 102.345 m (Society `POP_SERIES.actual`, Economy `pop_m`, sum of 34 provinces 102.345) | 101.599 m (Society `WORLD_RANK.pop`, WDI/UN) | pass (source) | Province sum ties to the national NSO total exactly; WDI uses UN WPP estimates. |
| A22 | Province GRDP vs GDP | Σ GRDP 2025 = 12,780,578 bn (Society) | GDP 2025 = 12,847,571 bn (Economy) | pass | Σ GRDP is 0.5% below GDP (unallocated items) — plausible; GRDP levels are derived (per-capita × population). |
| A23 | NSO-implied FX rate | 24,165 (2024) / 24,978 (2025) implied by Society GRDP USD | `ECON_OFFICIAL.usd_vnd` 2024 = 24,164.89; GDP per capita VND/USD 2025 = 24,976 | pass | Consistent. |
| A24 | Credit/GDP and M2/GDP end-2025 | Finance `credit_to_gdp_pct` 146 (press); computed: credit 18,594,930 / GDP 12,847,571 = 144.7%; M2 19,444,476 / GDP = 151.3% | legacy money card KPI strings: **M2/GDP "146%", credit/GDP "142%"** | **differs** | The hard-coded KPI strings are stale and swap the order (M2/GDP is higher than credit/GDP). Compute from FINSYS (P-7). |
| A25 | Credit growth 2025 | 19.07 (Finance `annual`, Dec-25 table); 19.01 (Finance `why`, 31/12 press) | legacy `CREDIT_YTD_24` peaks 15.50 for 2025; `CREDIT_YOY_24` ~15.5 | **differs** | Finance (SBV) is right; legacy constants are wrong. |
| A26 | Green credit | 750,000 bn at 2025-11 (Finance `flows`) | ~828 tn (Policy `green_credit`, 2026-06) | stale | Finance to add the 2026-06 point (Policy's source). |
| A27 | SME credit | 4.1 tn, Aug-2026 (Finance `flows`) | SBV via press: >4.1 tn, +12.4% at end-Aug | pass | |
| A28 | Bank deposit / lending rates | 12m private banks 8.4% (Aug-2026), lending 8.4–10.7% (Finance `why`) | `server.py fetch_rates`: deposit Big4 12m 4.70, JSCB 5.20, lending 7.50 ("manual reference") | **stale** | Finance values are sourced; server placeholder is outdated (P-5). |
| A29 | CPI 2026 in legacy macro card | 3.5 (`CPI_YOY[16]`, page) | 9M avg 4.52; forecasts 4.0–5.4 | **differs** | Unsourced extension; remove or label (P-7). |
| A30 | FX reserves 2026–2029 in legacy card | 107 / 110 / 113 / 115 bn (`FX_RESERVES[16..19]`) | ~86–88 bn actual mid-2026 | **differs** | Invented extrapolation shown with a quality badge "A". Remove (P-7). |

## B. Staleness against today's releases

| Item | Owner | Latest in file | Newer release | Status |
|---|---|---|---|---|
| IMF WEO in `CPI_FC_INST` and `FINSYS.projections` | Economy, Finance | Apr-2026 (14/04) | Oct-2026 WEO publishes **13/10/2026** | due 15/10 |
| World Bank forecast | Economy (`CPI_FC_INST` WB 15/05), Finance projections | May-2026 / none | EAP Update **06/10/2026**: 2026 growth 7.4% (+1.1 pp) | due now |
| M2 / deposits | Finance | T7/26 | T8/26 table expected mid-late Oct (2-month lag) | waiting |
| Credit by sector | Finance | 2026-07 | Aug-2026 expected mid-late Oct | waiting |
| SBV prudential / NPL | Finance | 2026-06 | Q3 ~Dec | not due |
| Listed-bank metrics | Finance | Q2-2026 | Q3 FS 20–31 Oct | due end-Oct |
| Corporate bonds | Finance | 2026-08 | Sep report ~10–20 Oct | due ~20 Oct |
| Interbank monthly average Sep | Finance | point values only | Vietcap Sep report ~mid-Oct | due ~15 Oct |
| `ECON_OFFICIAL` 2025 gaps (ent_active, pci_median, usd_vnd) | Economy | null | NSO 2025 figures exist (Jan-2026 report); PCI 2025 (VCCI, spring 2026) | due (gap) |
| Remittances 2025 national | Economy | null | SBV statements Jan-2026 — only "over 16 bn" found | gap |
| Workers sent abroad 2026 | Economy | null | year total in Jan-2027 | not due |
| BoP 2025 | Economy | 2025 present | — | pass |
| `ECONFLOW.budget` 2025 column | Economy | untraced | MoF Jan-2026 execution | differs (A13–A15) |
| `WORLD_RANK` | Society | WDI July-2026 | next WDI Dec-2026 | not due |
| Population, demographics | Society | 2025p | Jan-2027 | not due |
| Provincial GRDP | Society | 2025 + 9M-2026 growth | Q4/annual on 29 Dec (Decree 13/2026) | not due |
| Religion (Committee) | Society | 31-12-2023 | ad hoc | not due |
| Policy news / stance | Policy | 06/10 | — | pass |
| Strategy drafts (11 at NA session) | Strategy | 06/10 | session 17/10–20/11 | due during session |
| Investing macro (`invest.py`, `invest_research.json`) | unassigned | 03/10 | next NSO report 03/11 | pass (but A4) |
| Server bank fundamentals | unassigned | FY2024 + model histories | FY2025 audited (by 31/03/2026), Q2-2026 interim | **stale** |

## C. Internal checks run (all pass unless listed above)
- Society: 34 province keys; Σ `SOC_PROV_DATA.pop` (2025) = 102.34534 m = `POP_SERIES.actual[2025]`; Σ `pop_2024` = 101.344 m. ✔
- Strategy: 121 directives (100 in force, 15 draft, 2 adopted, 4 expired); no adopted document has a passed effective date; no expired end-date mismatch; all 0 broken relation/news ids. ✔
- Policy: future-dated history entries feed "what to watch": LDR cap 95% and LCR/NSFR from 01/12/2026, registration-fee step 01/03/2027 — all in the future. Expiry cliff 31/12/2026: VAT 8%, fuel environmental tax, fuel import duty, 50% fee cuts, tax deferral. ✔
- Economy: `ECON_OFFICIAL.gdp_vnd_bn[2025]` = `ECONFLOW.gdp.GDP[2025]` = 12,847,571. ✔ 2024 net exports ≠ X − M (Economy's own QC note; X/M are a WDI vintage).

## D. Code-level accuracy risks (not data files)
1. `server.py _build_histories()` fabricates multi-year bank histories from FY2024 with scale factors and
   `_bank_metric_jitter` ("realistic back-extrapolation"). These are shown as bank statements/histories. They are
   not published figures (Rule 5). → P-5.
2. `server.py fetch_rates()` returns fixed reference rates (see A28). → P-5.
3. Page legacy constants (`FX_USD`, `CPI_YOY`, `FX_RESERVES`, `M0`, `M1`, `M2`, `*_24` series, `RATE_*`, `CREDIT_SECTOR`,
   KPI strings in `MONEY_CARDS`) belong to no agent and contain unsourced extrapolations to 2029. → P-7.
4. `calcEconModel` uses `liveUSDVND` (a 2026 market mid-rate from er-api) for every year ≥ 2024. → P-8.
5. README says the scheduler runs at 15:30; `server.py` runs it at 00:00. → P-9.
