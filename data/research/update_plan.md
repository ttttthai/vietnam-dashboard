# Update plan

As of 2026-11-13 (Research, weekly pre-approved run). Evidence: `consistency_report.md` (X# this run, W# last run). Dates: `release_calendar.json`. Jobs: `schedule.json` (proposals only; the main session asks the user). The 2026-11-06 plan is kept below for history; its open decisions and P-# proposals still stand unless closed here.

## Priority watch (user standing task)
| Rank | Series | Held | Latest published | Status | Next |
|---|---|---|---|---|---|
| 1 | FDI registered vs disbursed | 10M 54.85 / 24.08 bn | same (NSO 03/11) | current; no large registration/withdrawal in this week's reports | 03/12/2026 |
| 2 | Remittances | HCMC 9M-2026 ~6.87 bn (−13.8%; Q3 +3.6% y/y); national null | same (Region 2 via Tiền Phong 20/10) | **closed**: HCMC 9M now held (it was on time; last week's "overdue" was a search miss). Base 7.97 vs 7.94 noted (X10). BoP Q3 not out | HCMC FY 15–25/01/2027; BoP Q3 Jan–Feb 2027 |
| 3 | Tourism | 10M 19.54 m; markets held | same (NSO 03/11) | current; receipts and outbound not published | 03/12/2026 |

## Fixed this run (pre-approved weekly run; see CHANGES in the report)
X1 swap take-up (Finance notes, Policy note), X3 real-estate credit units (Finance note), X4 public investment 05/11 point (Policy), X5 stale 2027-budget event (Investing), X6 Fed odds labels and stale story facts (Geopolitics), X7 central rate in Finance constraint 2, X8 Brent in Geopolitics chokepoint.

## Flags for the main session, Ed and the Narrator (not Research's files)
1. **Page `renderEconStory` "what to watch" card (~line 5612)** still says the 2027 budget is "Đề xuất, chưa biểu quyết" and takes `deficit_pct_gdp[last]` as the 2026 plan. Economy proposes: read `ECONFLOW.budget.plan_2027_resolution`, index 2026 with `years.indexOf(2026)` before appending a 2027 column. Proposal only (P-17).
2. **`data/narration.json`** (Narrator): 2027 budget "not yet voted" in 4 places (overview watch scene, a figure label, two tab watch scenes); central rate 25,615 (6/11) in 4 scenes, now 25,623 (12/11); Fed "~66%" via `board.story[8]` source note, now 88.8/11.2 (11/11); Brent 98.29 (5/11) and diesel 28,690, now 92.76 (11/11) and 26,820 (12/11); public investment 74.1% (31/10) can stay dated or move to 74.37% at 05/11 on the new base (say "new base", do not say "+0.3 pt").
3. **Units**: real-estate credit is 5.21 quadrillion (5,210 tn), not "5.21tn".

## Due by agent

### Economy
1. 03/12: NSO November report (CPI, FDI, tourism, investment); MoF 11M. Nothing due before.
2. 2027 revenue by source (null) when the resolution text is published.
3. Still open from 06/11: BoP Q2 secondary income; 2025 yearbook reconciliation; VCCI PCI 2025; public-debt sentence (W16, user decision).

### Finance
1. SBV September tables (M2, deposits, credit by sector): window 10–20/11; check in the 20/11 run.
2. Week of 13/11 in `market_6m` is provisional (Thursday 12/11): replace with Friday 13/11 values next run.
3. Swap: if the SBV publishes an exact take-up, store it as a value next to the cap. FX purchase volumes (Mon/Wed).
4. Vietcap October report (~15/11); Q3 NPL, CAR, ROA/ROE.
5. `why.drivers[7]` and Policy now differ in date only (31/10 vs 05/11): optional refresh to 05/11 with the base note.

### Policy
1. Next weekly run: add the MoF weekly point to ~12/11 (letter expected 18–19/11) in the same run as Investing (`policy-invest-public-weekly`).
2. NA phase 2 votes 20–21/11 (reserve 23/11): Land, Housing, RE Business, State Budget, Investment, Electricity, Tax, Bidding laws; resolution numbers for the 2027 plan and budget.
3. Year-end cliff: VAT 2% cut extension ("studying", 12/11), fee cuts and fuel environment-tax relief expire 31/12: run 15/12.
4. Circulars 48/2026 and 53/2026 not added; HNX October bond total (W10).

### Investing
1. Next weekly MoF point (~12/11); tell Policy in the report.
2. GEE rename vote result; GAS dividend 20/11; primary HOSE filings for POW/PVT/GAS.
3. `industries.json` rebuild still blocked (decision 2 of 06/11).

### Society
1. Provincial GRDP 29/11 (Sunday; check 30/11): Q4 + second FY2026 estimate. 2025 preliminary still not on PX.
2. WDI: check `lastupdated` in each weekly run (off-cycle 08/10 and 11/11).

### Strategy
1. 13/11 votes (Metrology, Securities law, 2025 accounts) not found: re-check; phase 2 votes 20–21/11.
2. Petrol trading decree (replacing ND 83): submission deadline 10/11 passed, no evidence.
3. Three key_points over 160 characters (NQ18-TW-2017, NQ98-QH15, KH1413-UBND-HCM-2025).

### Geopolitics
1. Fuel period 19/11 (MOPS averages: MoIT page 404; try Báo Lào Cai's table); routes and vn_retail still on 5/11.
2. ASEAN–US Manila 15–17/11 (dates differ 15–17 vs 16–18); APEC Shenzhen 18–19/11; OPEC+ 29/11; Iran end-November; FOMC 9/12.
3. Keep Fed odds equal to Finance's stored value with its date.

### Living
1. Fuel adjustment Thu 19/11 15:00; LPG December ~1/12; Nov CPI 03/12.
2. Electricity VAT 8% vs 10% (basket uses pre-VAT 419,000); 2026 water tariffs HCMC/Da Nang; Grab per-km fares.

## Waiting on a release (do not poll before)
| Release | Expected | Owners |
|---|---|---|
| SBV Sep tables; Vietcap Oct report | 10–20/11/2026; ~15/11 | Finance |
| MoF weekly public investment (to ~12/11) | 18–19/11/2026 | Investing, Policy |
| Fuel price period | Thursdays (19/11, 26/11) 15:00 | Living, Geopolitics |
| NA phase 2 votes, session close | 20–21/11/2026 (reserve 23/11) | Strategy, Policy, Economy |
| GAS dividend record | 20/11/2026 | Investing |
| Provincial GRDP Q4 + FY2026 second estimate | 29/11/2026 (Sunday; check 30/11) | Society |
| OPEC+; Iran decision | 29/11/2026; end-November | Geopolitics |
| LDR cap 95%, Circular 50 | 01/12/2026 | Policy, Finance |
| NSO November report + CPI; MoF 11M | 03/12/2026; 01–05/12 | Economy, Policy, Investing |
| FOMC | 08–09/12/2026 | Finance, Geopolitics |
| ADB Dec supplement, OECD EO, WB IDS, WDI Dec | December 2026 | Economy, Society |
| HCMC remittances FY2026 | 15–25/01/2027 | Economy |

## New proposals (2026-11-13, for the main session; not applied)
- **P-17 `renderEconStory` watch card**: see flag 1.
- **P-18 schedule**: two jobs proposed in `schedule.json` (`policy-invest-public-weekly`, `fx-ops-weekly`) run inside the existing Thursday weekly run; no new routine needed.

---

# Previous plan, 2026-11-06 (kept for history)

As of 2026-11-06 (Research, weekly pre-approved run). Evidence and rulings: `consistency_report.md` (W#). Dates: `release_calendar.json`. Jobs: `schedule.json` (proposals only; the main session asks the user). The 2026-10-06 plan is kept below for history; its open server and page proposals (P-#) still stand.

Access note: from this environment curl and WebFetch now reach nso.gov.vn, pxweb, api.worldbank.org, mof.gov.vn, hnx.vn and most press. sbv.gov.vn and imf.org are intermittent. See `source_log.json`. Prefer primary documents and mark them "primary".

## Rulings every tab follows from now on
1. **Public investment:** "disbursement" means the MoF series. NSO's figure is "realised investment from state-budget capital". Budget "development spending" is a third concept. Never compare 74.1% with earlier rates on the 1,022,607 base.
2. **Budget revenue:** lead with the NSO line (2,438.9 tn for 10M); keep the 2,448.5 tn press line as an alternate. Review if a later release explains the gap.
3. **Credit:** store every statement with its cut-off and speaker. Do not call any figure end-of-month until the SBV table says so.
4. **IMF vintage:** Economy, Finance and Society read the same DataMapper pull on the same day.

## Priority watch (user standing task)
| Rank | Series | Held | Latest published | Status | Next |
|---|---|---|---|---|---|
| 1 | FDI registered vs disbursed | 10M 54.85 / 24.08 bn | same, NSO 03/11 (read) | current; ratio 43.9% derived, 67.5% a year ago | 03/12/2026 |
| 2 | Remittances | HCMC H1 4.037; national null | national only "over 16 bn" (speech) | **HCMC 9M overdue**; BoP Q2 held, secondary income null | each weekly run; FY 20-25/01/2027 |
| 3 | Tourism | 10M 19.54 m; markets held | same, NSO 03/11 | current; 78.1% of the 25 m target (derived); receipts and outbound not published | 03/12/2026 |

## Waiting for a decision from the user (not changed)
1. **Public debt sentence (W16).** Page lines ~7026 and ~7226 use `ECONFLOW.projections.series.public_debt_pct_gdp_plan` 36-37% for end-2026, while the Government told the NA in October 33-34% (end-2027 34-35%). Both are real: the first is the January borrowing plan, the second the latest estimate. **Recommendation:** keep the plan series unchanged and labelled "January 2026 plan", add the October estimate beside it, and let the page sentence lead with the estimate. Ceiling unchanged. Ed changes the sentence; Economy adds a key `public_debt_pct_gdp_estimate`.
2. **`data/industries.json` rebuild.** `/tmp/vnenv` is missing, the STOCKS input was not found, and the Vietcap endpoint failed TLS; a curl was denied by the auto-mode classifier. Not retried. Needs a rebuild on the Mac or a decision. File stays at as_of 30/10.
3. **`invest_research.json`:** outside any agent's ownership, notes dated 03-04/10, and the POW summary compares price with analyst targets, close to the no-advice rule. Assign an owner.

## Due by agent

### Economy
1. 03/12 run: NSO November report, CPI, FDI, tourism; roll `ytd_latest`. CPI window: `CPI_MONTHS_24` must roll with the data (page constant, needed a page patch this month; see P-15).
2. HCMC remittances 9M (overdue); DOLAB 9M/10M; World Bank October CPI row; HSBC.
3. BoP Q2 secondary income, portfolio, loans: try the SBV table again.
4. 2025 yearbook reconciliation: `gdp.basis` 2025 still "estimate" though the yearbook is out. Dedicated job.
5. NA vote on the 2027 plan (07/11, unconfirmed): `draft_2027` notes.
6. VCCI PCI 2025 median still null.

### Finance
1. Sep tables (M2, deposits, sectors): 10-20/11. End-Nov statement: 01-05/12.
2. Credit end-October: only the SBV table can settle 11.94 vs 12.27. Update the T10/26 level then.
3. Refresh `sbv.plumbing.facts.gov_bond_issued_2026_ytd` and `analysis.chapters.plumbing` (still 299,100 at 30/9 and 52.2% of the 500,000 plan); use the 400,000 base with its caveat, same figures as Policy.
4. Replace the 5/11 VN-Index close with the 6/11 close in `market_6m`.
5. Swap result: add the amount done (operations entry value is null on purpose).
6. Sector NIM and COF: pick one provider or mark the break on the chart; Q2 to Q3 steps mix providers.
7. `alternatives.ytd` as_of 2026-10-06; `analysis.chapters.why.drivers[2]` still cites September CPI (dated, not wrong).
8. Q3 profit falls at Sacombank and HDBank: read before anyone repeats them (search summary only).

### Policy
1. Swap take-up; OMO and overnight entries for 2-6/11.
2. Identify the document for the 500 to 400 tn plan cut and the 955,867.4 bn plan base.
3. Votes 20-21/11; VAT, fee and fuel-tax extension before 31/12. Run on 22/11 and 15/12.
4. HNX October bond total to settle W10.

### Investing
1. Q3 primary filings for POW, PVT, GAS, GEE (all press so far). POW H1 and GAS conflicts need the filing.
2. NSO 03/12: refresh `cpi.latest`, `public_invest.latest`, `fdi`.

### Society
1. Provincial GRDP 29-30/11 (first Q4 date under the decree; see what is really published). 2025 preliminary GRDP due 29/09 not yet seen.
2. `tools/build/grdp/imf_*.json` still hold the April pull.
3. WDI December; first 2026 TFR in January.

### Strategy
1. 07/11 plan vote; 20-21/11 votes; check the budget resolutions' vote date.
2. `DT-ND-KHCN-2026` expected date passed; `DT-ND-XANGDAU-2026` submission by 10/11.

### Geopolitics
1. USTR dates (primary notice), Census table direct, US payrolls and China October trade.
2. 29/11 IP date is carried over, not re-checked.

### Narrator and Ed (from this run)
1. Society's projection swap leaves 14 lines saying 116.9 m / 2066 in `data/narration.json` and `story_map*.json`; correct is 115.2 m / 2059.
2. Do not write "credit slowed" (W5), do not compare 74.1% with 62.9% (W2), do not call 783.9 tn disbursement (W1).
3. Finance chapter 3 "why rates won't fall" needs a qualifier: new-business rates fell in late October by directive.

## Waiting on a release (do not poll before)
| Release | Expected | Owners |
|---|---|---|
| NA vote on the 2027 plan | 07/11/2026 (unconfirmed) | Strategy, Economy |
| To Lam in Japan; Abu Dhabi talks | 8-11/11; 10-11/11 | Strategy, Geopolitics |
| SBV Sep tables, Vietcap Oct report | 10-20/11/2026 | Finance |
| NA session close | 21/11/2026 | Strategy, Policy, Economy |
| Provincial GRDP Q4 + second full-year estimate | 29/11/2026 (Sunday; check 30/11) | Society |
| OPEC+; USTR IP date | 29/11/2026 | Geopolitics |
| LDR cap 95%, LCR/NSFR, Circular 50 | 01/12/2026 | Policy, Finance |
| NSO November report + CPI; MoF 11M | 03/12/2026; 01-05/12 | Economy, Policy, Investing |
| FOMC | 08-09/12/2026 | Finance, Geopolitics |
| ADB Dec supplement, OECD EO, WB IDS, WDI Dec | December 2026 | Economy, Society |
| NSO full-year 2026 + population | 03/01/2027 | Economy, Society |
| Listed-bank Q4 FS / FY2026 audited | 20-31/01/2027 / by 31/03/2027 | Finance |
| FY2025 budget final account | NA spring 2027 | Economy, Policy |

## New proposals for server / page / build (2026-11-06, for the main session to review, not applied)
- **P-12 `tools/build/embed_data.py` import guard.** The module has no `__main__` guard: Economy imported it for a test and it wrote all data into the page (reverted). Wrap the work in `main()`.
- **P-13 Investing tiles.** Page tiles ("Đã giải ngân 9T", "CPI T9 YoY", "BQ 9T") and `invest.py` line ~473 read the 30/09 and Sep keys. Switch to `p.latest` and `c.latest`, with the plan base stated. `macro.focus` and `macro.events` need a renderer.
- **P-14 Society labels.** Page line ~7328 and comment ~12159 still say 2019–2069; lines ~12313-12317 hard-code "2024", so `*_2025` keys are not shown.
- **P-15 `CPI_MONTHS_24`.** A page constant that must roll with embedded `CPI_YOY_24`; a monthly manual patch is fragile. Move it into `data/economy.json` so the label travels with the data.
- **P-16 Geopolitics `ground_news`.** The field is filled but no page code reads it.

---

# Previous plan, 2026-10-06 (kept for history; many items since closed, see consistency_report.md)

As of 2026-10-06 (Research, first run; priority-watch pass added the same day). Ordered by priority within each owner. Evidence and the "which is
right" reasoning are in `consistency_report.md` (refs A#/B#). Release dates come from `release_calendar.json`;
the proposed recurring jobs are in `schedule.json` (none created — the main session asks the user).

Access note for every agent: no source host is reachable from the sandbox (curl CONNECT 403 and WebFetch
EGRESS_BLOCKED for nso.gov.vn, sbv.gov.vn, mof.gov.vn, imf.org, api.worldbank.org, all press). Only WebSearch
works and its budget is shared — plan each run's queries (see `source_log.json`).

## Priority watch (user standing task) — FDI, remittances, tourism
Full detail, derived ratios, traps and news in `data/research/priority_watch.json` (as of 2026-10-06).

| Rank | Series | Held | Latest published | Status | Next |
|---|---|---|---|---|---|
| 1 | FDI registered vs disbursed | 9M-2026 reg 50,360 / dis 21,070 m USD | same, NSO/FIA 03/10/2026 (excerpt) | values current; components + ratio not held | 03/11/2026 |
| 2 | Remittances | national 2025/2026 null; HCMC 2025 10.34, H1-2026 4.037 | national only "over 16 bn" (speech); HCMC H1 | HCMC 9M due ~20–25/10; SBV BoP Q2-2026 overdue | 20–25/10/2026 |
| 3 | Tourism | 9M 17,680 k arrivals; Sep 1.77 m | same, NSO 03/10/2026 (excerpt) | current; by-market not held | 03/11/2026 |

Derived (Research, not published): disbursed/registered = **41.8%** 9M-2026 (8M 42.5%; implied 9M-2025 65.8%;
FY2025 71.9%, FY2024 66.3%, FY2023 58.9%). Sep-2026 alone (9M − 8M): registered 9.73 bn, disbursed 3.82 bn.

### Economy — priority items (do with the 04/11 run unless marked)
1. **FDI components (additive keys, `ECON_OFFICIAL.ytd_2026`)**: new 29.24 bn (3,108 projects), adjusted 14.15 bn
   (948 times, +25.1%), capital contribution & share purchase 6.97 bn (+44%); sum = 50.36. Source: NSO 9M report
   https://www.nso.gov.vn/bai-top/2026/10/bao-cao-tinh-hinh-kinh-te-xa-hoi-quy-iii-va-9-thang-nam-2026/ and
   https://www.vietnamplus.vn/9-thang-nam-2026-von-dau-tu-nuoc-ngoai-dang-ky-vao-viet-nam-tang-764-post1140075.vnp
   (excerpt). Keep components every month so the page can show *why* registered jumps (Sep: Can Gio port >4.9 bn).
2. **FDI vintage label** `ECON_OFFICIAL.fdi_reg_musd`: 2022 29,288.2 and 2023 39,390.3 are the revised vintage;
   first releases were 27.72 / 36.61 bn (https://taisancong.vn/viet-nam-thu-hut-3661-ty-usd-von-fdi-nam-2023-von-giai-ngan-lap-ky-luc-27805.html);
   2024–2025 are first releases. Not wrong — add a per-year vintage note in `ECON_OFFICIAL.sources`.
3. **Remittances**: `ECONFLOW.remit.national_sbv_busd` 2023 = 2024 = 16.0 with no URL — cite or set null.
   2025 national stays **null** (only "over 16 bn", Foreign Minister,
   https://vietnamnet.vn/en/vn-sets-remittance-record-of-over-16-billion-in-2025-says-foreign-minister-2478410.html — already in note).
   Add HCMC quarterly split Q1 2.004 / Q2 2.032 (H1 4.037) to `hcmc_H1_2026_busd` if wanted. **Run ~25/10** for HCMC 9M-2026
   (9M-2025 comparator: 7.94 bn, +6.3%). Check whether Region 2 figures cover merged HCMC (from 01/07/2025).
4. **BoP Q2-2026** (`ECONFLOW.bop.quarterly`): overdue; check ~20/10. Do not store the unconfirmed 11.1 bn CA-deficit claim.
5. **Tourism** (additive, e.g. `ECONFLOW.tour.by_market_9M2026`): China 3.9 m (22.4%), Korea 3.0 m (17.3%), Russia ~1.1 m
   (+160.6%), Europe +55%; target 25 m (71% reached); 2026 revenue target ~1,125 tn VND. Only store values whose period is
   unambiguous (several market growth rates in the excerpts mix Q3 and 9M). Add the VNAT 2025 revenue URL
   (https://vietnamnews.vn/society/1743207/viet-nam-s-international-tourism-sees-best-year-in-2025-with-arrivals-hitting-over-21-million.html)
   to `tour.sources.total_tourism_revenue_vnat_trn_vnd`. Re-check outbound 9M −21.2% vs Q3 +16.4% against the NSO table.
6. **Page copy guard** (for Ed via Economy's report): BoP `fdi_in` is ~80% of NSO disbursed every year 2015–2025 — never
   label BoP FDI as "disbursement"; registered includes M&A (share purchases), so "FDI pledges" ≠ new factories.

### Investing (`data/invest_macro.json`)
1. Optional additive `MACRO.fdi` block mirroring Economy exactly (9M-2026 reg 50.36 / dis 21.07 bn, yoy, components,
   period, same NSO URL, `derived_ratio_pct` 41.8 labelled derived). Needs a rendering change in `renderInvStory` (Ed/main
   session) — describe, do not edit code. Not before Economy has stored the components (one source of truth).

## Due now / this week

### Economy (`data/economy.json`) — after its current run finishes
1. **Budget 2025 column (A13–A15).** `ECONFLOW.budget.revenue.total[2025]` 2,681,500 →
   2,650,100; `expenditure.total[2025]` 3,312,600 → 2,401,500; `deficit_pct_gdp[2025]` 3.3 → ~3.6 (MoF
   year-end execution, Jan-2026, as already held in `budget.mof_execution_2025_jan2026` and in Policy
   `budget_outturn`). Also `ECON_OFFICIAL.budget_rev_bn[2025]` 2,681,469 → 2,650,100. Keep `basis` "estimate".
   If the owner keeps the old column, it must cite a source.
2. **Forecast vintages.** World Bank EAP Update (06/10/2026: Vietnam 2026 growth 7.4%) — add if it states CPI;
   IMF WEO Oct-2026 publishes **13/10** → replace the "IMF (WEO Apr-2026)" row in `CPI_FC_INST` on/after 15/10.
   Coordinate with Finance (same vintage in `FINSYS.projections`).
3. **Gaps** in `ECON_OFFICIAL` 2025: `usd_vnd` (NSO-implied 24,978 is used by Society GRDP USD — A9/A23),
   `ent_active`, `pci_median` (VCCI PCI 2025).
4. Next scheduled refresh: **04/11/2026** (NSO October report and CPI on 03/11; MoF 10M budget same days).

### Finance (`data/finance.json`)
1. **Check the Jun/Jul-2026 credit label (A6, A7).** Press dates SBV's 20.15 tn / +8.38% to **29/07**; end-June
   was **+7.41%**. Our `monthly` T6/26 holds 20,150,411 (+8.37%) and T7/26 20,262,110 (+8.97%). Re-read the SBV
   table headers; fix the month mapping if shifted (it affects `sectors.periods`, `other_services_breakdown.total_bn`
   and `listed_banks_by_sector.sbv_total_bn`). Also re-source the "22-Aug 8.38%" point in `why.credit_vs_deposit_growth`.
2. **Projections vintage**: `projections.series.real_gdp_growth_pct` (IMF Apr-2026) → IMF Oct-2026 WEO after 13/10;
   note WB 7.4% (06/10). Same day as Economy.
3. **Green credit (A26)**: add the ~828 tn (2026-06) point Policy already cites.
4. Due in October: corporate bonds Sep (VBMA/Fiin ~10–20/10); Vietcap Sep interbank average (~mid-Oct);
   SBV M2/deposits and credit-by-sector Aug tables (mid–late Oct); listed-bank Q3 FS (20–31/10).
5. FX reserves: record the IMF series code and definition next to `sbv.balance_sheet.fx_reserves_busd` (A10).

### Policy (`data/policy.json`)
1. Weekly refresh (news within 3 months). Nothing wrong found; Policy holds the most current central rate and budget outturn.
2. Prepare for **NA session 17/10–20/11/2026**: 2027 budget, any extension of the 31/12/2026 VAT/fuel relief —
   run on **21/11** and again **15/12** for the expiry cliff.

### Strategy (`strategy_directives.json`)
1. Twice-weekly during the NA session: 11 tracked drafts are expected to be voted (Land, Housing, Real-estate
   business, State Budget, Legal Documents, Investment, Electricity, Data Security, health-sector laws, Marine
   resources & islands, plus the 2027 socio-economic plan resolution) — update `stage`,
   `ref`, dates on adoption; DT-ND-KHCN-2026 expected "2026-10".

### Society (`data/society.json`)
Nothing due before 29/12/2026 (provincial GRDP Q4/annual) and Jan-2027 (population). WDI December update → ranks.
Note for chart labels: world ranks use WDI population/GDP per capita (101.6 m; 5,066 USD), not NSO (102.3 m; 5,026 USD).

### Ed
1. Monthly "what to watch" date check (1st of month). 2. Once P-7 is decided, the legacy money/macro cards in
the Policies tab need to read from Policy/Finance data or be removed — Ed owns that code.

### Unassigned (needs the main session)
- `invest.py` MACRO (CPI target 4.0 → 4.5, A4) and `invest_research.json` `_macro/_market`: propose **Economy** owns
  MACRO (refresh in `econ-monthly-nso`), Ed/Investing story keeps `_market`.
- `server.py` rates/bank histories, page legacy constants: see proposals.

## Waiting on a release (do not poll before)
| Release | Expected | Owners |
|---|---|---|
| SBV BoP Q2-2026 (overdue) | check 20/10/2026 | Economy (priority #1/#2) |
| HCMC remittances 9M-2026 (SBV Region 2) | ~20–25/10/2026 | Economy (priority #2) |
| NSO/FIA Jan–Oct FDI + arrivals | 03/11/2026 | Economy (priority #1/#3), Investing mirror |
| IMF WEO Oct-2026 | 13/10/2026 | Economy, Finance |
| Fed FOMC | 28/10 and 09/12/2026 | Finance |
| NSO Oct report + CPI | 03/11/2026 | Economy (+Policy stance, Investing macro) |
| SBV credit to end-Oct | 01–05/11/2026 | Finance, Policy |
| NA session close | 20/11/2026 | Strategy, Policy, Economy (2027 budget/plan) |
| LDR cap 95%, LCR/NSFR start | 01/12/2026 | Policy (stage), Finance (notes) |
| ADB Dec supplement, OECD EO, WB IDS, WDI Dec | December 2026 | Economy, Society |
| Provincial GRDP Q4/2026 | 29/12/2026 (confirm) | Society |
| NSO full-year 2026 + population 2026 | 03/01/2027 | Economy, Society |
| Listed-bank Q4 FS / FY2026 audited | 20–31/01/2027 / by 31/03/2027 | Finance, server bank data |
| FY2025 budget final account | NA spring 2027 | Economy, Policy |

## Proposals for server / page / build (for the main session to review — not applied)
- **P-1 Freshness endpoint.** Add `GET /api/freshness` in `server.py` that reads `data/research/release_calendar.json`
  and `inventory.json` and returns items whose `next_expected` has passed but whose `latest_period` has not moved.
  Runs with the existing midnight scheduler; zero web cost. Lets Ed show a "data as of" badge per chapter.
- **P-2 Embed `data/research` into nothing.** Keep research files out of `OWNERS` in `embed_data.py` (they are
  ops files, not page data); add a `--check` step in the agents' workflow that fails if a data file's `_meta` lacks
  `as_of`. (Small build change: `embed_data.py` prints each file's `_meta.as_of`.)
- **P-3 Server-side API pulls (Render has open egress; the sandbox does not).** Monthly job in `server.py`:
  World Bank WDI `lastupdated` for the WORLD_RANK indicators and IMF DataMapper `NGDP_RPCH`/`PCPIPCH` for VNM →
  write `data/auto/*.json` snapshots for the Society/Economy/Finance agents to read (agents still decide what to embed).
- **P-4 Fed funds / DXY** could come from the same server job (FRED needs a key; federalreserve.gov press-release
  scrape is fragile) — low priority.
- **P-5 `server.py` bank data and rates.** (a) Replace `fetch_rates()` placeholder with values read from
  `data/policy.json` (refinancing/OMO) and `data/finance.json` (`why.series.deposit12m_private_banks_mbs`,
  `avg_lending_rate_sbv`) so the server never contradicts the tabs. (b) Remove or clearly label
  `_build_histories()` synthetic histories (scale factors + jitter) — show only reported periods; move bank
  fundamentals to FY2025/Q2-2026 from Finance's `listed_banks_by_sector`.
- **P-6 `invest.py` MACRO** → move into a data file owned by Economy (e.g. `data/economy.json` key `INVEST_MACRO`
  served by the server, or a small `data/invest_macro.json`), fix CPI target 4.5.
- **P-7 Page legacy constants** (`FX_USD`, `CPI_YOY`, `FX_RESERVES`, `M0`, `M1`, `M2`, `SBV_*_24`, `CREDIT_*_24`,
  `M2_YOY_24`, `VNIBOR_*_24`, `RATE_*`, `CREDIT_SECTOR`, `MONEY_CARDS` KPI strings, `FIS`): either (a) add them to
  `OWNERS` (Economy: FX_USD/CPI_YOY/FX_RESERVES/FIS; Finance: M0–M2, credit/VNIBOR/rate series, CREDIT_SECTOR)
  with sources and **no values after the last published year**, or (b) have Ed rewire the cards to `ECON_OFFICIAL`,
  `FINSYS`, `POLICY` and delete the constants. (b) is preferred — one source per figure.
- **P-8** `calcEconModel`: use `ECON_OFFICIAL.usd_vnd` per year; use `liveUSDVND` only for the current year.
- **P-9** README: scheduler time (00:00, not 15:30) and vnstock note.
- **P-10 Priority-watch schedule (2026-10-06).** All 19 routines are PAUSED until the repo is attached. When re-enabled,
  the main session may ask the user to add: `priority-remit-hcmc` (25th of Jan/Apr/Jul/Oct, first 25/10/2026) and
  `priority-bop-quarterly` (one-shot 20/10/2026, then 20th of Jan/Apr/Jul/Oct, can share the fin-monthly-tables run);
  FDI and tourism need no new trigger (they ride `econ-monthly-nso` on the 4th); the FDI news scan rides
  `research-weekly-audit`. Details in `schedule.json`.
- **P-11 Page (Ed).** Show FDI registered by component (stacked new/adjusted/M&A) next to disbursed, and the derived
  ratio labelled "≈, derived"; once Economy stores the components.
