# Research learnings log

Append a dated section each run. Never rewrite past entries; if a rule changes, add a new entry citing the evidence.

## 2026-10-06 — first run (built from scratch)

### What I learned
1. **NSO release day is the 3rd, not the 6th.** Decree 13/2026/NĐ-CP (13 Jan 2026, in force 10 Apr 2026)
   moved the monthly/quarterly socio-economic report, GDP and CPI to the 3rd of the next month, and provincial
   GRDP to the 29th of the quarter's last month. Evidence: decree coverage
   (https://baochinhphu.vn/chinh-phu-thay-doi-lich-pho-bien-mot-so-thong-tin-thong-ke-quan-trong-102260114171529601.htm)
   and observed releases on 03/07/2026 (Q2) and 03/10/2026 (Q3, a Saturday). Every agent definition said "~6th".
   GRDP Q4/annual date (29 Dec vs 29 Nov) to confirm against the decree text.
2. **IMF WEO Oct-2026 = 13 Oct 2026** (Annual Meetings, Bangkok; chapters 5–6 Oct) — IMF briefing 01/10/2026.
3. **World Bank EAP Update Oct-2026 came out today (06/10)**: Vietnam 2026 growth 7.4%, +1.1 pp. Not in any file yet.
4. **SBV credit**: statements in the first days of the month with a cut-off (29/7 → 3/8; 30/9 → 3/10); end-June
   2026 was +7.41%, 29/7 +8.38% (20.15 tn). Finance's T6/26 row equals the 29/7 figure → possible label shift.
5. SBV tables (M2, deposits, credit by sector) lag ~2 months; prudential/NPL quarterly with ~2–3 months lag.
6. Agents' earlier pitfalls confirmed and logged: WPP 2026 postponed to 2027; WDI July/December; bank FS notes
   only in PDFs; IMF WEO April/October.
7. **Reachability:** from this sandbox *every* data, government, institution and press host is blocked —
   curl CONNECT 403 on 80 URLs, WebFetch EGRESS_BLOCKED on nso.gov.vn, sbv.gov.vn, imf.org, api.worldbank.org,
   vnexpress.net, vietnamplus.vn, en.wikipedia.org. Reachable: pypi.org, registry.npmjs.org, api.github.com,
   raw.githubusercontent.com. WebSearch works (shared budget; I used 6 queries). The server's own market and FX
   sources (Vietcap, er-api, frankfurter) are blocked here too, so the sandbox server log shows empty indices/FX.
8. **Unowned data in code** is the largest accuracy risk: the page's legacy cards in the Policies tab
   (FX_USD, CPI_YOY, FX_RESERVES to 2029, OMO 4.00, credit 2025 ≈15.5%, M2/credit-to-GDP strings), `server.py`
   synthetic bank histories (scale + jitter) and static rates, and `invest.py` MACRO (CPI target 4.0 vs 4.5).
9. The most consistent tab on shared figures today is Policy (central rate 6/10, 2025 deficit ~3.6%); the
   Economy 2025 budget column is the clearest cross-tab conflict.

### Page/agent classification (which source pages suit which agent)
Recorded per host in `source_log.json` (`best_for_agent`, `pages`). Short version: NSO monthly report → Economy
(+Policy stance, Finance quotes); NSO population/GRDP → Society; SBV tables → Finance, SBV decisions → Policy;
MoF execution → Economy + Policy; NA/quochoi + Party portals → Strategy (+Policy for tax/budget laws);
IMF/WB/ADB/AMRO/OECD → Economy (CPI_FC_INST) + Finance (projections); WDI/UN → Society; thanhnien daily FX →
Finance month-end series; static1.vietstock.vn broker PDFs → Finance (money market, bonds).

### Upgrades made (agent definitions)
- `.claude/agents/economy.md`: NSO/CPI release day 6th → 3rd (Decree 13/2026); new section "Release timing &
  access" (MoF timing, final accounts, forecast vintages incl. WEO 13 Oct and WB EAP 6 Oct, annual-only series,
  blocked hosts, 2025 budget and CPI-target pitfalls).
- `.claude/agents/society.md`: new "Release timing & access" (GRDP on the 29th, population in January, WDI
  July/December, WPP 2027, blocked hosts, WB-vs-NSO labelling).
- `.claude/agents/finance.md`: new "Release timing & access" (credit statement cut-offs, ~2-month table lag,
  month-label check with the 2026 evidence, bank FS deadlines, bond/money-market report days, FOMC dates,
  projection vintages, blocked hosts, central-rate date).
- `.claude/agents/policy.md`: new "Release timing & access" (weekly cadence, NA session 17/10–20/11/2026,
  31/12/2026 expiry cliff, 1/12/2026 effective dates, keep stance numbers equal to Economy/Finance).
- `.claude/agents/strategy.md`: new "Release timing & access" (NA session drafts, monthly stage re-derivation,
  blocked hosts).
- `.claude/agents/ed.md`: new "Data hygiene notes" (unowned legacy constants; monthly watch-date check).
All edits keep each file's structure and ownership rules.

### Proposed (not applied): see `update_plan.md` P-1 … P-9 and `schedule.json` (18 jobs, none created).

### Next run should
- Re-read `data/economy.json` after the Economy agent's run and re-check A9, A13–A15.
- Confirm the GRDP Q4 date in Decree 13/2026; confirm SBV table month labels (A6) with Finance's answer.
- Record the actual IMF WEO release (13/10) and the next NSO release (03/11) as evidence, and adjust confidence.

## 2026-10-06 — priority-watch pass (user standing task: FDI registered vs disbursed, remittances, tourism)

### What I learned (12 web searches, all excerpt-level; no page fetches attempted — hosts blocked as logged above)
1. **FDI 9M-2026 (NSO/FIA, released 03/10/2026):** registered 50.36 bn (+76.4%) = new 29.24 bn (3,108 projects, ×2.4)
   + adjusted 14.15 bn (948 times, +25.1%) + capital contribution & share purchase 6.97 bn (+44%) — components sum
   exactly. Disbursed 21.07 bn (+12.1%, five-year 9M high). 8M: 40.63 / 17.25 bn. Economy's totals match; the
   components are not held. FIA now publishes with NSO on the 3rd (both under MoF) — new calendar id `fia_fdi_monthly`.
2. **Derived ratio** (labelled derived in priority_watch.json): disbursed/registered 41.8% (9M-2026) vs implied 65.8%
   (9M-2025) and 71.9% (FY2025). The 2026 drop is a registration surge (mega-projects: Can Gio port >4.9 bn, AI data
   centre ~2.1 bn in HCMC), not weak disbursement.
3. **BoP FDI inflow ≈ 80% of NSO disbursed in every year 2015–2025** — the held BoP series is not an independent
   cross-check of disbursement (likely SBV estimation method; not confirmed).
4. **Vintage mix in `ECON_OFFICIAL.fdi_reg_musd`:** 2022/2023 are revised (29,288.2 / 39,390.3) vs first releases
   27.72 / 36.61 bn; 2024–2025 are first releases. FIA's yoy rates use the revised base.
5. **Remittances:** still no SBV national figure for 2025 (only "over 16 bn", Foreign Minister) or 2026. HCMC (SBV Region 2):
   Q1 2.004, Q2 2.032, H1 4.037 bn (−22.8%); FY outlook 8.6–8.9 bn; 9M-2025 comparator 7.94 bn. Release lag ~3–4 weeks after
   quarter end (22/07/2026, 23/01/2026) → new calendar id `remittances_hcmc`, next ~20–25/10/2026. A "9 tháng gần 8 tỷ"
   article ranks first in searches but is 9M-2025 — pitfall logged.
6. **SBV BoP Q2-2026** not found (overdue). Corrected my earlier calendar text that said no 2026 quarterly BoP existed
   (Q1-2026 is held via VnEconomy).
7. **Tourism:** 9M 17.68 m (+14.5%), ~71% of 25 m target; China 3.9 m (22.4%), Korea 3.0 m (17.3%), Russia ~1.1 m
   (+160.6%), Europe +55%; several market growth rates in press mix Q3 and 9M. 2025 revenue >1,000 tn VND, 2026 target
   ~1,125 tn. NSO 3rd-of-month rule re-confirmed (8M tourism item published Sep-2026; 9M on Sat 03/10); next 03/11 (Tue).
   Outbound 9M −21.2% vs Q3 +16.4% flagged for checking.

### Files written / changed
- New `data/research/priority_watch.json` (ranked items, sub-items, derived ratios, traps, conflicts, news_watch).
- `inventory.json`: `priority_watch` rank on econ.tourism / econ.remittances / econ.bop / econ.official_*; new families
  `econ.fdi_registered_disbursed`, `inv.macro_fdi` (proposed key).
- `release_calendar.json`: new `fia_fdi_monthly`, `remittances_hcmc`, `wb_knomad` (low confidence), `vnat_tourism`;
  evidence added to `nso_monthly_report`, `sbv_bop` (rule rewritten, correction noted), `remittances`.
- `schedule.json`: FDI/tourism series added to `econ-monthly-nso`; proposed `priority-remit-hcmc`, `priority-bop-quarterly`,
  `priority-fdi-news-scan` (inside the weekly audit), `priority-annual-remit-tour`; `priority_watch_note` (routines paused).
- `source_log.json`: new hosts FIA, SBV Region 2 (via press), VNAT; press entry access tip for priority series.
- `update_plan.md`: priority-watch section at the top; waiting-on-release rows; proposals P-10, P-11.

### Upgrades made (agent definitions)
- `.claude/agents/economy.md`: new "Priority watch" bullet under Release timing & access (FDI components, vintage and
  BoP-vs-disbursed traps, provincial FDI; three remittance definitions, speech-number rule, HCMC quarterly lag and merged
  territory; tourism market periods, receipts vs revenue).
- `.claude/agents/investing.md`: new section "Sourcing notes for priority series" (mirror Economy's FDI values exactly
  incl. components, label derived ratio, release on the 3rd; remittance/tourism definitions).
Both keep their structure and ownership rules.

### Next run should
- ~25/10: HCMC 9M remittances; 20/10: SBV BoP Q2-2026; 03/11: Jan–Oct FDI components + arrivals — update
  priority_watch.json `latest_published` and the derived ratio.
- Verify the merged-HCMC basis of Region 2 remittances, and the outbound-travel sign.

## 2026-10-07 — freshness audit (user request: "check every single chart, make sure you have latest 2026 figures"; check-only)

### What I did
- Mapped 142 chart / KPI / table rows on all 7 tabs (incl. the new Simulation tab and the server-fed bank appendix and
  invest tools) to their series and latest period; checked latest published periods with ~35 web searches (excerpt-level)
  and vnstock. Output: `freshness_audit.md` + `.json`. Counts: current 67, structural 26, waiting 26, stale 22, broken 1.
- No data file, page or server code edited.

### What I learned
1. **The monthly NSO block is fully current** (03/10 release everywhere: GDP, CPI, FDI, tourism, budget, public investment).
   Staleness lives in curated sparse series (Simulation panels), Society's provincial vitals, and the server's bank data.
2. **PCI 2025 was released 15/05/2026** (VCCI; first on 34 provinces; "PCI 2.0"); median 63.90 (excerpt, outlet not pinned).
   Not comparable with 2024 (67.67). New calendar id `vcci_pci`. The econ appendix tile silently shows the 2024 value for 2025
   (nearest-year fallback in `calcEcon`) — first "broken" item.
3. **Provincial vital rates for 1/4/2025 are public** (NSO survey "Kết quả chủ yếu … 01/4/2025", PDF on thuvienso.quochoi.vn):
   e.g. HCMC TFR 1.51 (held 1.43, ref 2024). New calendar id `nso_pop_change_survey` (low confidence on timing).
4. **IMF WEO Oct-2026: 13/10 09:00 Bangkok** confirmed on imf.org (page id 2026/10/13).
5. **World Bank vintages:** April EAP 6.3% → mid-May 6.8% → Oct EAP 7.4%. Finance's "+1.1 pp" compares with April; label it.
6. **FOMC 16/09/2026 hike to 3.75–4.00%** confirmed (held correctly). New calendar id `fed_fomc`.
7. **SBV BoP Q2-2026 still unpublished** on 07/10 (>3 months after quarter end) — rule stays "check 20/10".
8. **Search-date pitfall (again):** result headers such as "Thứ Tư, 22/07/2026" on thesaigontimes/vnba are crawl/page dates,
   not article dates — the "9 tháng kiều hối TP.HCM 5.485 bn" hits are 9M-2023. Same for "Sep: 172,605 new accounts" (2024).
9. **Gov-bond auction monthly totals and yields** appear in press within ~1 week of month end (Sep 10Y 4.67–4.80%).
10. **Corporate-bond issuance** (VBMA) monthly with data to ~28th: Aug-2026 32,029 bn; 8M ~349,000 bn.
11. **Conflicts logged (not averaged):** green credit 828k (9/6, held) vs >780k (SBV H1 briefing 2/7); margin Q2 453.8k (held)
    vs 446k (80/85 firms) vs ~435k; WB 2027 CPI 3.8 (held) vs 3.7 (excerpt); HCMC 2025 remittances 10.34 actual vs 10.5 estimate.
12. **Reachability 07/10:** vnstock VCI *quote* (trading.vietcap.com.vn) works from the sandbox with one retry; vnstock *Finance*
    (iq.vietcap.com.vn, masboard.masvn.com, kbbuddywts.kbsec.com.vn) still CONNECT 403. Bank fundamentals cannot be refreshed here.

### Files written / changed
- New: `data/research/freshness_audit.md`, `data/research/freshness_audit.json`.
- `inventory.json`: `last_checked` 2026-10-07 and a `freshness_2026_10_07` block on 51 families (48 existing + 3 new); new families `econ.pci`,
  `sim.panel`, `page.gdp_sectors`.
- `release_calendar.json`: evidence added to `imf_weo`, `sbv_bop`, `remittances_hcmc`, `wb_eap_update`, `hnx_gbond`,
  `vbma_fiin_bonds`, `sbv_prudential`; new ids `vcci_pci`, `nso_pop_change_survey`, `property_quarterly`, `vsdc_accounts`,
  `fed_fomc`, `eia_brent`.
- No agent-definition edits this run (proposals only; the run was scoped check-only).

### Next run should
- 13/10: record the WEO release and hand Economy/Finance the new vintage. 20/10: BoP Q2 + SBV Aug tables. ~25/10: HCMC 9M remittances.
- Confirm PCI 2025 median and the provincial 2025 vitals in the primary documents (try the thuvienso.quochoi.vn PDF; the
  NA library host was not probed for reachability yet).
- Consider adding to `.claude/agents/finance.md` (Simulation section): the sparse-series list (gov_bond_10y, SJC monthly, VSDC
  accounts, Savills/CBRE, VBMA issuance, budget balance) with their release rhythm — proposal pending the user's go-ahead.
