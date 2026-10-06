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
