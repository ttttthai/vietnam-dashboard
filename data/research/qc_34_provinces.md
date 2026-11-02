# QC — every provincial figure on the 34-province basis (NQ 202/2025/QH15)

Research agent · run 2026-10-11 · **read-only audit**: no page, data or agent file was edited.
Scope: `vietnam_dashboard.html` (PROVS, HC_KEY_MAP, HK_OLD, map/replay, SOC_* constants, story chapters, Explore pop-ups
`CI.*`, summary panel `ss*`, econ appendix cards, i18n), `data/*.json`, `strategy_directives.json`, `invest_research.json`,
`server.py`, `data/flourish/provinces_scatter.csv`.
Line numbers are approximate (the page was being edited during the audit). The function names are the stable anchors.

Fix classes: **(a)** page/text fix the main session can do · **(b)** data fix, value already known · **(c)** needs new data.

---

## Summary (prioritised)

| # | Pri | Where (tab · element) | Problem | Class |
|---|---|---|---|---|
| F1 | P1 | Society · map tooltip + Explore pop-up "Bản đồ 34 tỉnh/thành" | Uses hand-typed `PROVS.pop`. National total 103.04 m vs NSO 102.35 m. Errors from −28% to +54% per province. | a (+b) |
| F2 | P1 | Economy appendix "GDP & GRDP" card + Explore pop-ups `grdp`/`income`/`fdi`/`trade` (region/province selected) | Provincial GRDP comes from the `PROVS.econ` model, although NSO 34-unit GRDP for 2018–2025 is already in `SOC_GRDP`. In 2025 the model gets HCMC +49%, Hà Nội +57% and Lai Châu −60%; the Southeast shows 42.1% of GDP against an official 31.3%. | a/b (2018–25), c (2010–17) |
| F3 | P1 | Economy appendix: GRDP card and Labour card KPI tiles | Modelled provincial values have no "model" label, and the quality badge shows "A". Unemployment, IIP and PCI show the **national** value under a province name. | a |
| F4 | P2 | Society summary panel · "Dân số" and "Diện tích" sub-labels | For the 11 unchanged units, "NQ 202: x tr" shows NSO's 2024 average population, not an NQ 202 figure. The area is labelled NQ 202, but for these 11 it is the NSO 2025 area. | a (or b) |
| F5 | P2 | Society summary · GRDP in USD | Rate hard-coded at 24,977 instead of `fxYear(2025)` (24,976) or Society's 24,978. | a |
| F6 | P2 | Society summary · "x% GDP cả nước" | Numerator is the sum of provincial GRDP, denominator is NSO GDP. The six regions add up to 99.48% for 2025. | a |
| F7 | P3 | Society summary · regional GRDP growth | Averaged using current-price GRDP weights from the same year, with no "≈" mark. | a |
| F8 | P2 | Economy · remittances, HCMC line (`ECONFLOW.remit.hcmc_busd`, `hcmc_H1_2026_busd`) | Boundary break: 2015–2024 cover the old HCMC. Whether 2025 and H1-2026 cover the merged HCMC (with Bình Dương and BR-VT) is unconfirmed. | c |
| F9 | P3 | Policies · Yagi debt relief (TT 53/2024) note "26 tỉnh" | Right for 2024 (old units), but it now reads as 26 of the current 34. | a |
| F10 | P3 | Party & Government · NQ 18 (2017), HCMC NQ 57 plan, NQ 98 key points | They name district-level units / TP Thủ Đức as live addressees or pending tasks. | a |
| F11 | P3 | Party & Government · "chính quyền 3 cấp" (EN "three-tier government") | Sits next to "two-tier local government" without saying which three levels. | a |
| F12 | P3 | Code comment + Explore pop-up text | Regions are attributed to NQ 202. The six regions are defined by NQ 306/NQ-CP (05/10/2025). | a |
| F13 | P3 | Society story ch.1/ch.3 notes | "quy đổi về 34 tỉnh" does not say that NSO did the conversion (its PL.V02/PL.V03 tables), not the page. | a |
| F14 | P4 | EN region-4 label | 'South Central Coast & Highlands' (`socRegName`) vs 'South Central Coast & Central Highlands' elsewhere. | a |
| F15 | P4 | Dead code `getMetric()` | Per-province model accessor that nothing calls. | a |
| F16 | P4 | Finance data, PCF counts "57/58 provinces" (2021, 2024) | Right for the date; needs an "old 63-unit basis" tag if it is ever shown. | a |
| C1 | — | Provincial FDI, exports, unemployment, enterprises, income, GRDP 2010–2017 on the 34 basis | Not held, so the page models them. | c |

Passes (details in §3): the merger mapping (23 merged + 11 unchanged), the 63→34 replay, region assignment against NQ 306,
SOC_PROV_DATA rates, PROV_PREV, the SOC_GRDP method, RELIGION aggregation, population totals, district-level wording in the
current-structure text, and every "34"/"63" count against its date.

---

## 1. Findings

### F1 · P1 · Hand-typed `PROVS.pop` shown as population (a, optionally b)
- **Where:**
  - `drawMap()` mouseover tooltip, ~line 2550: `Dân số: ${fmtNum(p.pop,0)}`. Seen on the Society tab, Explore map hover.
  - `CI.map()`, ~lines 11189–11204: the "i" pop-up "Bản đồ 34 tỉnh/thành". It builds the region table (Dân số theo vùng), the national total, the "Đông dân nhất" line and the province-in-region table from `p.pop`.
- **What is wrong:** `PROVS.pop` is a hand-typed mix: some values are NQ 202 "quy mô dân số", some are rounded guesses. It is not the NSO average population the rest of the page uses (`SOC_PROV_DATA.pop`, which sums exactly to the national 102,345,340). The pop-up also says "dân số: ước tính 2025 nhập tay".
- **Evidence** (`PROVS.pop` → NSO 2025p `SOC_PROV_DATA.pop`):
  - Quảng Ngãi 2,900,000 → 1,881,240 (+54%)
  - Gia Lai 2,300,000 → 3,182,650 (−28%)
  - Khánh Hòa 1,600,000 → 1,898,550 (−16%)
  - Lâm Đồng 2,800,000 → 3,356,380 (−17%)
  - Cà Mau 1,800,000 → 2,150,440 (−16%)
  - Huế 1,300,000 → 1,190,340 (+9%)
  - Hà Nội 9,200,000 → 8,857,530
  - HCMC 14,000,000 → 13,803,800
  - Sum 103,043,000 vs 102,345,340.

  Region table as shown vs correct:

  | Region | Units | Shown (m, %) | NSO 2025p (m, %) |
  |---|---|---|---|
  | V1 Trung du & MN phía Bắc | 9 | 13.44 · 13.0% | **12.73 · 12.4%** |
  | V2 ĐB sông Hồng | 6 | 26.20 · 25.4% | **25.07 · 24.5%** |
  | V3 Bắc Trung Bộ | 5 | 11.40 · 11.1% | **11.39 · 11.1%** |
  | V4 DH Nam Trung Bộ & Tây Nguyên | 6 | 15.00 · 14.6% | **16.02 · 15.7%** |
  | V5 Đông Nam Bộ | 3 | 21.00 · 20.4% | **21.28 · 20.8%** |
  | V6 ĐB sông Cửu Long | 5 | 16.00 · 15.5% | **15.85 · 15.5%** |
  | Total | 34 | 103.04 | **102.35** |

- **Fix (a):** in both places replace `p.pop` with `_ssPop(p)`, which already exists and reads `SOC_PROV_DATA[p.n].pop`. In `CI.map()` change the qText to `Danh mục tỉnh: NQ 202/2025/QH15 · vùng: NQ 306/NQ-CP · dân số: NSO, dân số trung bình 2025 (sơ bộ)`.
- **Optional (b):** drop the `pop` field from `PROVS`, or overwrite it with `SOC_PROV_DATA.pop`, so nothing can read it again. The correct values are the `pop` field of each `SOC_PROV_DATA` entry (NSO PX PL.V02, 2025 preliminary average; source URL in `data/society.json` `_meta.sources.SOC_PROV_DATA`).

### F2 · P1 · Provincial GRDP/growth/income/FDI/trade drawn from the `PROVS.econ` model while official data exist (a/b for 2018–2025, c for 2010–2017)
- **Where:** `calcEcon()` / `calcEconModel()` (~2251–2300), used by:
  - `updateSpecialCharts()` for the "GDP & GRDP", "FDI & Thương mại" and "Lao động & Doanh nghiệp" cards in the Economy appendix (~2825–3034);
  - `ciBreakdown()` / `CI.grdp()` / `CI.income()` / `CI.fdi()` / `CI.trade()` / `CI.labour()` Explore pop-ups (~11167–11298).
  
  These views draw values whenever a region or province is selected on the map. For "Cả nước" the pop-up tables also break the country down by region.
- **What is wrong:** each province's value = an unsourced 2010 base × a fixed growth rate, rescaled so the 34 add up to NSO's national number. NSO's own 34-unit GRDP for 2018–2025 (levels, per head, growth 2020–2025) is already embedded as `SOC_GRDP`, and the Society summary panel uses it. So the same province shows two different GRDPs on two tabs.
- **Evidence:** 2025 model allocation (calcEcon) vs NSO (`SOC_GRDP.grdp_bn['2025']`), bn VND:

  | Province | Model | NSO | Gap |
  |---|---|---|---|
  | Lai Châu | 15,925 | 39,807 | −60% |
  | Thái Nguyên | 82,960 | 192,385 | −57% |
  | Sơn La | 41,597 | 95,738 | −57% |
  | Hà Nội | 2,485,044 | 1,588,060 | +57% |
  | Lào Cai | 69,668 | 141,887 | −51% |
  | TP. HCM | 4,440,444 | 2,972,536 | +49% |
  | Phú Thọ | 216,279 | 412,151 | −48% |
  | Đắk Lắk | 124,785 | 229,539 | −46% |
  | Vĩnh Long | 151,859 | 278,466 | −46% |
  | Huế | 108,579 | 89,600 | +21% |

  Regional shares of GDP, 2025 (model → NSO):
  - V1 4.3% → 8.6%
  - V2 32.2% → 30.3%
  - V3 5.2% → 7.1%
  - V4 9.1% → 12.3%
  - V5 42.1% → 31.3%
  - V6 7.0% → 10.5%

  Growth 2025 (model constant g → NSO):
  - Quảng Ninh 8.0 → 11.89
  - Hải Phòng 8.2 → 11.81
  - Phú Thọ 7.5 → 10.52
  - Ninh Bình 7.2 → 10.65
  - HCMC 8.9 → 7.53

  Also, the `CI.grdp()` summary says "GDP thực ~X nghìn tỷ", but the level is scaled to `ECON_OFFICIAL.gdp_vnd_bn`, which is at **current prices**.
- **Fix:**
  - **(a/b) 2018–2025**, values known: when `selType !== 'all'`, read `SOC_GRDP[p.n].grdp_bn_series[y]` (sum over the selection) and per head = Σ grdp_bn / Σ `pop_avg_thousand_series`. Growth 2020–2025 = `growth_series[y]`; the region value is a weighted mean, marked "≈" (see F7). 2018–2019 growth is null for the 23 merged units, so show "—" rather than model values.
  - **(a) 2010–2017**: show the provincial view as "no 34-unit data", or keep the model with an explicit "mô hình" label (F3).
  - **(c) 2010–2017**: build the series the same way as `SOC_GRDP` 2018–2019, i.e. the sum of old-63 NSO E03.12 GRDP per head × E02.03-07 population. That needs the E03.12/E02.03-07 rows for 2010–2017 (NSO PX; GitHub mirror QuocAnhHo/Vietnam-socioeconomic-statistics, if it carries those years). Owner: Society.
  - **(a)** change "GDP thực" to "GDP (giá hiện hành)" in `CI.grdp()` and its breakdown table title.
  - FDI/exports/income/enterprises: see C1.

### F3 · P1 · Model values without a model label; national values shown under a province name (a)
- **Where:** Economy appendix:
  - "GDP & GRDP" card tiles `kpi-gdppc`, `kpi-gdppc-real`, `kpi-grdp`, `kpi-growth`, `kpi-income` (~1591–1595, filled ~3005–3009);
  - "Lao động & Doanh nghiệp" tiles `kpi-unemp`, `kpi-biz`, `kpi-pci`, `kpi-iip` (~1663–1666, filled ~3025–3033);
  - the quality badges `qb-card-*` come from `getQuality(yi)` (~2184), which looks at the year only.
- **What is wrong:**
  - With a province or region selected, these tiles show modelled numbers with no label. Only the growth chart caption says "tỉnh: mô hình", and only the FDI card has a model note (`ecb-fdi-model`). The badge still shows "A".
  - In `calcEconModel` the unemployment, PCI and IIP formulas do not depend on the province list. `calcEcon` then rescales them to the official national figure, so **every province shows the national unemployment rate, IIP and PCI** as if they were its own.
  - In the Explore pop-ups, the breakdown table titles ("GDP thực theo vùng", "FDI đăng ký theo tỉnh") do not say "mô hình". The qText "mixed" is the only hint.
- **Fix (a):**
  - Add model notes like `ecb-fdi-model` under the GRDP and Labour cards whenever `selType !== 'all'`.
  - Show the badge as "M" (model) when `selType !== 'all'` and the value is not from `SOC_GRDP`.
  - Label unemployment, IIP and PCI tiles "(cả nước)" when a province or region is selected, or show "—".
  - Append " (mô hình)" to `ciBreakdownTable` titles when the source is `calcEcon`.

### F4 · P2 · Population/area sub-labels attribute NSO figures to NQ 202 (a; or b)
- **Where:** `renderSocSummary()` ~10655 (`TB 2025 (NSO, sơ bộ) · NQ 202: …`) and ~10657 (area sub-label `NQ 202/2025/QH15`). Seen on the Society Explore summary panel when one province is selected.
- **Evidence:** for the 11 unchanged units, `pop_nq` equals `pop_2024` exactly (e.g. Hà Nội 8,717,630 = 8,717,630; Huế 1,178,550 = 1,178,550). NQ 202 gives no population for units it does not change. `_meta.sources` says the 11 units' area is the NSO 2025 area. Across the 23 merged units, NQ "quy mô dân số" sums to 88.0 m, and the 34-unit total of `pop_nq` is 111.7 m (the NQ figures count residents on a wider basis).
- **Fix (a):** show the "NQ 202" population line only when `merged_from.length > 0`. Make the area sub-label `NQ 202/2025/QH15` for merged units and `NSO 2025` for the 11 others.
- **Alternative (b):** Society sets `pop_nq: null` for the 11 unchanged units, with a note.

### F5 · P2 · Hard-coded FX in the provincial GRDP-in-USD tile (a)
- **Where:** `ssGdpSection()` ~10598 (`pcM2 * 1e6 / 24977`) and ~10601 (`usdRate = … : 24977`).
- **Evidence:** `ECON_OFFICIAL.usd_vnd[2025]` = 24,976 (NSO-implied); Society's `grdp_pc_usd` uses 24,978. The effect is under 0.01%, but the number is typed in, not derived.
- **Fix:** use `fxYear(yr)` for both, as the national tile does.

### F6 · P2 · Region/province share of GDP mixes bases (a)
- **Where:** `ssGdpSection()` ~10604, sub-label `${grdp / natG * 100}% GDP cả nước`.
- **Evidence:** numerator = Σ provincial GRDP (NSO PL.V03); denominator = national GDP. For 2025, Σ34 GRDP = 12,780,578 bn vs GDP 12,847,571 bn, so the six regional shares add to **99.48%**.
- **Fix:** use Σ34 GRDP as the denominator (`% tổng GRDP 34 tỉnh`), or add the note "tổng GRDP các tỉnh thấp hơn GDP 0,5%".

### F7 · P3 · Regional GRDP growth weighting (a)
- **Where:** `ssMetric()` case `'growth'` ~10542.
- **What:** the region's real growth = provincial growth weighted by that year's current-price GRDP. The correct weights are the previous year's GRDP at constant prices, which is not held.
- **Fix:** prefix the regional value with "≈" (as the vital rates already do). Society could add constant-price levels later (c, low priority).

### F8 · P2 · HCMC remittance series crosses the 1/7/2025 boundary change (c)
- **Where:** `data/economy.json` → `ECONFLOW.remit.hcmc_busd` (2015–2025) and `hcmc_H1_2026_busd`. Drawn as the HCMC line in the Economy remittances chart.
- **Evidence:**
  - 2024 = 9.547 bn USD (old HCMC). 2025 = 10.34 bn, "+8.3% vs 2024" (SBV Khu vực 2, 22/01/2026; [vneconomy](https://vneconomy.vn/hon-1034-ty-usd-kieu-hoi-chuyen-ve-tp-ho-chi-minh-trong-nam-2025.htm), [sggp](https://www.sggp.org.vn/kieu-hoi-ve-tphcm-nam-2025-dat-hon-103-ty-usd-post834785.html)). H1-2026 = 4.037 bn, −22.8% yoy.
  - None of the excerpts say whether the post-July figures include Bình Dương and Bà Rịa – Vũng Tàu. `ECONFLOW.remit.definitions.hcmc_busd` already says "not confirmed".
- **Fix:**
  - **(c)** Economy to confirm the coverage of the SBV Khu vực 2 figure for 2025 and 2026, and whether it is restated for 2024.
  - **(a)** until then, label the line "TP.HCM (SBV Khu vực 2; phạm vi sau 1/7/2025 chưa xác nhận)".
  - The same boundary question applies to the HCMC apartment price series (`FINSYS.alternatives.series.real_estate.hcmc`, `moc_2025.hcmc`). Whether CBRE/Savills/MoC "HCMC" now includes Bình Dương and BR-VT is unconfirmed (c, low).

### F9 · P3 · "26 tỉnh" (TT 53/2024/TT-NHNN, Yagi) reads as current units (a)
- **Where:** `data/policy.json` (owner Policy), the entry with `note_vi` "26 tỉnh; không giới hạn số lần; trả nợ cuối cùng tới 31/12/2027." / `note_en` "26 provinces; …". Shown in the Policies tab timeline.
- **Fix:** "26 tỉnh, thành (địa giới trước 1/7/2025)" / "26 provinces (pre-July-2025 boundaries)". The count is correct for its date.

### F10 · P3 · District-level units named as live in strategy items (a)
- **Where:** `strategy_directives.json` (owner Strategy), shown in the Party & Government tab directive cards:
  1. NQ 18-NQ/TW (2017), `status: "Đang thực hiện"`, key point "… đến 2030 cơ bản hoàn thành sắp xếp cấp huyện, xã theo tiêu chuẩn" (~line 814).
  2. HCMC plan implementing NQ 57 (stage `in_force`), key point "Giao sở, ngành, quận, huyện, TP Thủ Đức xây dựng kế hoạch …" (~line 4176).
  3. NQ 98/2023/QH15 key point "… bộ máy chính quyền TP và TP Thủ Đức" (~line 3824).
- **What is wrong:** district-level units and TP Thủ Đức ceased on 01/07/2025 (NQ 202/2025/QH15; amended Constitution, Arts 110–111).
- **Fix:** keep the historical text and add a qualifier.
  - (1) "(cấp huyện đã kết thúc từ 01/7/2025 theo NQ 60-NQ/TW)".
  - (2), (3) "(quận, huyện và TP Thủ Đức kết thúc hoạt động từ 01/7/2025; nhiệm vụ chuyển cho phường, xã)".

### F11 · P3 · "chính quyền 3 cấp" vs "chính quyền địa phương 2 cấp" (a)
- **Where:** `strategy_directives.json` ~583 (Đại hội XIV key task), ~1002 and ~2084 (NQ 66 / NQ 140/NQ-CP targets); i18n EN "three-tier government model" / "3-tier government model".
- **Fix:** keep the Vietnamese wording. In EN write "three-level government (central – provincial – commune)" so it does not seem to contradict "two-tier local government".

### F12 · P3 · Source of the region grouping (a)
- **Where:**
  - comment above `PROVS` (~2121: "official per Nghị quyết 202/2025/QH15 & bản đồ chính thức");
  - `CI.map()` "why" text ("Bản đồ tô màu theo 6 vùng kinh tế – xã hội");
  - `renderSocSummary()` sub "34 tỉnh, thành phố · 6 vùng kinh tế – xã hội".
- **Fix:** cite **NQ 306/NQ-CP (05/10/2025)**, which adjusts the National Master Plan 2021–2030 and sets the six regions for the 34 units. NQ 202 defines the provinces only.

### F13 · P3 · Wording of the conversion to 34 provinces (a)
- **Where:** `renderSocStory()` ch.1 note and source (~6638–6640: "Số liệu 2024 đã quy đổi về địa giới 34 tỉnh"); `socStMap` note (~7029/7034); ch.3 source (~6666).
- **Fix:** "Số liệu 2024 do Cục Thống kê tính lại theo địa giới 34 tỉnh (bảng PL.V02)". In ch.3 add "GRDP 2020 theo bảng NSO tính lại cho 34 tỉnh (PL.V03)". The data are NSO's own back-casts, not page averages (see §3), and the reader should know that.

### F14 · P4 · EN region-4 name (a)
- `socRegName` (~6619) uses 'South Central Coast & Highlands'. Elsewhere (i18n, Flourish CSV) it is 'South Central Coast & Central Highlands'. Use the latter.

### F15 · P4 · Dead code (a)
- `getMetric(p, yi, metric)` (~3043) returns per-province model values and has no callers. Remove it so it cannot be wired to a chart unlabelled.

### F16 · P4 · Counts on the old basis inside Finance data (a, only if displayed)
- `data/finance.json` `FINSYS…peoples_credit_funds`: "58 provinces" (31/12/2024), "57 provinces" (31/12/2021). Both are correct for their dates. If a tooltip or table shows them, add "(địa giới 63 tỉnh)".

### C1 · Data the page models because none is held on the 34 basis (c)
Needed to retire the `PROVS.econ` model. All belong to Economy unless noted.

| Series | Source to collect | Note |
|---|---|---|
| FDI registered and disbursed by province, 2010–2025 | FIA (MoF) annual/monthly "by locality" tables; NSO yearbook FDI by province | Old-63 years summed to 34. Registered counts can be summed; check the treatment of multi-province projects. |
| Exports by province | Customs (by locality of the declaring firm) or provincial statistics offices | Concept differs from the national total; show it as a separate measure. |
| Unemployment by province | NSO LFS provincial annual tables | Rates, so recompute from levels (unemployed ÷ labour force), not averaged. |
| Active enterprises by province | NSO enterprise census / yearbook | Counts, summable. |
| Income per head by province | NSO VHLSS (2-year rhythm) | Recompute as Σ income ÷ Σ population, not a simple mean. |
| GRDP 2010–2017 on the 34 basis | NSO E03.12 × E02.03-07, old-63 rows | Same method as `SOC_GRDP` 2018–2019 (Society). |

---

## 2. Totals: do the 34 add up to the national figure?

| Measure | Σ 34 provinces | National | Gap | Verdict |
|---|---|---|---|---|
| Population 2025 (`SOC_PROV_DATA.pop`) | 102,345,340 | 102.3453 m (`ECON_OFFICIAL.pop_m`, NSO 2025p) | 0 | pass |
| Population 2024 (`pop_2024`) | 101,343,760 | 101.3438 m | 0 | pass |
| Population 2020 (`PROV_PREV.pop`) | 97,582,700 | 97.5827 m | 0 | pass |
| Population 2018–2025 (`SOC_GRDP.pop_avg_thousand_series`) | 95,385.2 / 96,484.0 / 97,582.7 / 98,504.5 / 99,468.0 / 100,309.2 / 101,343.8 / 102,345.3 thousand | `pop_m` same years | 0 | pass |
| Population, hand-typed `PROVS.pop` | 103,043,000 | 102,345,340 | +697,660 (+0.68%) | **differs → F1** |
| Population, NQ 202 `pop_nq` | 111,724,080 | — | different concept (NQ "quy mô dân số"; 11 units = NSO 2024) | label only → F4 |
| Area (`area_km2`) | 331,351.7 km² | 331,338 km² (2023 land statistics, 33,133,831 ha, QĐ 3411/QĐ-BTNMT) · 332,144 km² (2024 land inventory, 33,214,437 ha, QĐ 456/QĐ-BNNMT) | +14 km² (+0.004%) vs 2023 · −793 km² (−0.24%) vs 2024 | pass; NQ 202 areas match the 2023 vintage, the 2024 inventory is larger |
| GRDP 2018 / 2019 | 7,028,194 / 7,711,472 bn | GDP 7,009,042 / 7,707,200 | +0.27% / +0.06% | pass (GRDP ≠ GDP by design) |
| GRDP 2020 / 2021 / 2022 / 2023 | 8,030,658 / 8,521,564 / 9,665,255 / 10,287,121 | 8,044,386 / 8,487,476 / 9,621,372 / 10,319,059 | −0.17% / +0.40% / +0.46% / −0.31% | pass |
| GRDP 2024 / 2025 | 11,394,517 / 12,780,578 | 11,510,329 / 12,847,571 | **−1.01%** / −0.52% | pass, but F6 (share denominator) |
| GRDP 2025, `PROVS.econ` model | = GDP by construction | — | provincial split wrong by up to ±60% | **differs → F2** |
| Religion, census 2019 (pop, followers) | 96,208,984 · 13,162,879 | same | 0 | pass |
| Religion, census 2009 | 85,846,997 · 15,651,467 | same | 0 | pass |

Internal check: for every province and year 2020/2024/2025, `grdp_bn` = `grdp_pc_mvnd` × `pop_avg_thousand` within 0.2%.
`SOC_GRDP` 2025 population = `SOC_PROV_DATA.pop` and 2020 population = `PROV_PREV.pop` for all 34.

---

## 3. Checks that passed

- **Merger mapping:** `SOC_PROV_DATA.merged_from`, `HC_KEY_MAP` (64 topo polygons → 34; Cù Lao Chàm → Đà Nẵng), `NAME_FRAG_MAP`, `HK_OLD` and the `SOC_HEX`/`SOC_HEX_GEO` tables (34 ids each) all match NQ 202/2025/QH15. There are 23 merged units (built from 52 old provinces) and 11 unchanged units (Hà Nội, Huế, Cao Bằng, Điện Biên, Hà Tĩnh, Lai Châu, Lạng Sơn, Nghệ An, Quảng Ninh, Thanh Hóa, Sơn La). Six are centrally-run cities. The replay counter computes 63 − 29 = 34.
- **Region assignment** (`PROVS.v`) matches the six regions of **NQ 306/NQ-CP, 05/10/2025** (excerpts from [thuvienphapluat](https://thuvienphapluat.vn/hoi-dap-phap-luat/6-vung-kinh-te-sau-sap-nhap-2025-the-nao-138065840.html), [luatvietnam](https://luatvietnam.vn/linh-vuc-khac/dinh-huong-phat-trien-6-vung-kinh-te-xa-hoi-quoc-gia-thoi-ky-2021-2030-theo-nghi-quyet-306-883-104592-article.html), [baolamdong](https://baolamdong.vn/print/394955.html); page fetch blocked, search-excerpt verified):

  | Region | Units | Page members |
  |---|---|---|
  | V1 Trung du & miền núi phía Bắc | 9 | Tuyên Quang, Cao Bằng, Lai Châu, Lào Cai, Thái Nguyên, Điện Biên, Lạng Sơn, Sơn La, Phú Thọ |
  | V2 Đồng bằng sông Hồng | 6 | Bắc Ninh, Quảng Ninh, Hà Nội, Hải Phòng, Hưng Yên, Ninh Bình |
  | V3 Bắc Trung Bộ | 5 | Thanh Hóa, Nghệ An, Hà Tĩnh, Quảng Trị, Huế |
  | V4 Duyên hải Nam Trung Bộ & Tây Nguyên | 6 | Đà Nẵng, Quảng Ngãi, Gia Lai, Đắk Lắk, Khánh Hòa, Lâm Đồng |
  | V5 Đông Nam Bộ | 3 | Đồng Nai, Tây Ninh, TP. Hồ Chí Minh |
  | V6 Đồng bằng sông Cửu Long | 5 | Đồng Tháp, An Giang, Vĩnh Long, Cần Thơ, Cà Mau |

  **No doubtful assignments.** The units that moved region through the merger are placed correctly:
  - Tây Ninh (with Long An) → V5;
  - Lâm Đồng (with Bình Thuận, Đắk Nông) → V4;
  - Quảng Trị (with Quảng Bình) → V3;
  - Gia Lai (with Bình Định) and Đắk Lắk (with Phú Yên) → V4.

  Not checked: whether NSO's statistical tables group regions the same way as NQ 306.
- **SOC_PROV_DATA rates** (TFR, life expectancy, urban %, sex ratio, CBR, CDR):
  - The values come from NSO's own PL.V02 tables compiled for the 34 new units (reference 2024). They are **not** averaged by the page, so there are no unweighted or simple averages.
  - `vitals_ref_year` is 2024 for all 34. The `*_2025` keys (TFR for 6 units) are not rendered, so no years are mixed on screen.
  - Density = 2025 population ÷ area. Regional rates use population weights with "≈" (urban % is exact with these weights; TFR and life expectancy are approximate and correctly marked "≈").
- **PROV_PREV** (2020) also comes from the NSO PL.V02 34-unit tables, so the 2020 rank comparisons are on the same basis.
- **SOC_GRDP method:**
  - 2020–2025 come from the NSO PL.V03.01/02 34-unit tables.
  - For the 23 merged units, 2018–2019 = Σ old-province GRDP ÷ Σ population. Levels are summed and per-head values recomputed, never averaged. The method reproduces NSO's 34-unit values for 2020–2025 within 0.01 m VND.
  - Growth 2018–2019 for merged units is left null rather than averaged.
  - `socConvData` uses 2020→2025, both NSO 34-unit years.
- **Flourish** `provinces_scatter.csv`: 34 provinces × 2020/2024/2025, region labels as per NQ 306.
- **RELIGION.prov:** census counts (2019, 2009) summed from whole old provinces into the 34 units; the totals are exact. NQ 202 merged whole provinces, so no partial units need splitting.
- **District level:**
  - The page's own description of the current structure uses province–commune (governance diagram lane "Địa phương (tỉnh, xã)", nodes HĐND/UBND xã).
  - The strategy text says "34 đơn vị cấp tỉnh (28 tỉnh, 6 thành phố) và 3.321 đơn vị cấp xã; chính quyền địa phương 2 cấp từ 01/7/2025".
  - No "cấp huyện / quận, huyện" is used as a current unit in the page code. The data-file exceptions are listed in F10.
- **"34"/"63" against dates:**
  - "63 tỉnh → 34" (NQ 202, 12/6/2025, in force; new governments from 01/7/2025);
  - PCI 1.0 "63 provinces, 2010–2024" vs PCI 2.0 "34 provinces, 2025", kept as separate series;
  - "30/34 tỉnh … năm 2027" target;
  - "15 địa phương dưới bình quân" (public investment, 9M-2026);
  - Finance PCF "57/58 provinces" (2021, 2024).
  
  All are correct for the date they refer to.
- **server.py** has no provincial endpoints. **invest_research.json** has no provincial figures. Plant names ("Cà Mau 1&2", "Ninh Thuận 1&2" nuclear) are project names, not administrative units.

---

## 4. Proposed order of work (for the main session; ask the user before applying)

1. F1 (a): two code edits, using `_ssPop(p)` in the tooltip and `CI.map()`.
2. F3 (a): model labels and badges in the Economy appendix and Explore tables; "(cả nước)" on unemployment, IIP and PCI tiles.
3. F2 (a/b): switch the provincial/regional GRDP views to `SOC_GRDP` for 2018–2025; fix "GDP thực" → "GDP (giá hiện hành)".
4. F4, F5, F6, F7 (a): summary-panel labels, FX, share denominator, "≈".
5. F9 → Policy, F10/F11 → Strategy, F8 → Economy (confirmation search), C1 → Economy/Society (collection).
6. F12–F16 (a): wording and cleanup.

Sources used this run:
- NQ 306/NQ-CP regions: https://thuvienphapluat.vn/hoi-dap-phap-luat/6-vung-kinh-te-sau-sap-nhap-2025-the-nao-138065840.html , https://luatvietnam.vn/linh-vuc-khac/dinh-huong-phat-trien-6-vung-kinh-te-xa-hoi-quoc-gia-thoi-ky-2021-2030-theo-nghi-quyet-306-883-104592-article.html , https://baolamdong.vn/print/394955.html (search excerpts; hosts not resolvable from the sandbox)
- National land area 2024 (33,214,437 ha, QĐ 456/QĐ-BNNMT) and 2023 (33,133,831 ha, QĐ 3411/QĐ-BTNMT): https://nongnghiepmoitruong.vn/ca-nuoc-con-gan-955-nghin-ha-dat-chua-su-dung-d797116.html , https://binhphuoc.gov.vn/uploads/binhphuoc/news/2024_10/quyet-dinh-3411-qd-btnmt.pdf (search excerpts)
- HCMC remittances 2025: https://vneconomy.vn/hon-1034-ty-usd-kieu-hoi-chuyen-ve-tp-ho-chi-minh-trong-nam-2025.htm , https://www.sggp.org.vn/kieu-hoi-ve-tphcm-nam-2025-dat-hon-103-ty-usd-post834785.html (search excerpts; no boundary statement found)
