# Update plan

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
