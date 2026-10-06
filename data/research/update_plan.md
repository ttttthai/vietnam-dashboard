# Update plan

As of 2026-10-06 (Research, first run). Ordered by priority within each owner. Evidence and the "which is
right" reasoning are in `consistency_report.md` (refs A#/B#). Release dates come from `release_calendar.json`;
the proposed recurring jobs are in `schedule.json` (none created — the main session asks the user).

Access note for every agent: no source host is reachable from the sandbox (curl CONNECT 403 and WebFetch
EGRESS_BLOCKED for nso.gov.vn, sbv.gov.vn, mof.gov.vn, imf.org, api.worldbank.org, all press). Only WebSearch
works and its budget is shared — plan each run's queries (see `source_log.json`).

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
