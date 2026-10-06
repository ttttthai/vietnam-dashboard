import json
exec(open('data.py').read())
Y=list(range(2018,2027))
def ser(k): return [D[y].get(k) for y in Y]
rev_map={'total':'total','domestic':'domestic','crude_oil':'crude_oil','import_export':'import_export','grants':'grants','vat':None,'cit':None,'pit':'pit','special_consumption':None,'env_protection':'env_protection','land_use_fees':'land_use_fees','lottery':'lottery','fees_charges':'fees_charges'}
exp_map={'total':'exp_total','development_investment':'development_investment','recurrent':'recurrent','interest':'interest','aid':'aid','reserve_fund':'reserve_fund'}
out={'years':Y,'basis':[D[y]['basis'] for y in Y],'unit':'billion VND (tỷ đồng)'}
out['revenue']={k:(ser(v) if v else [None]*len(Y)) for k,v in rev_map.items()}
out['expenditure']={k:ser(v) for k,v in exp_map.items()}
out['principal_repayment']=ser('principal_repayment')
out['deficit']=ser('deficit')
out['deficit_pct_gdp']=ser('deficit_pct_gdp')
out['total_borrowing']=ser('total_borrowing')
out['extra']={
 'revenue_land_house_total':ser('land_house_total'),
 'revenue_soe_sector':ser('soe'),'revenue_fdi_sector':ser('fdi'),'revenue_non_state_sector':ser('non_state'),
 'revenue_import_export_gross':ser('ie_gross'),'revenue_vat_on_imports':ser('ie_vat_imports'),'revenue_sct_on_imports':ser('ie_sct_imports'),'vat_refunds':ser('vat_refund'),
 'expenditure_national_reserves':ser('national_reserves'),'expenditure_contingency_plan_only':ser('contingency'),
 'carryover_revenue_from_prior_year':ser('carryover_rev'),'total_revenue_resources_incl_carryover':ser('total_resources'),'carryover_expenditure_to_next_year':ser('carryover_exp'),
}
# sources
src={}
allkeys={**{('revenue.'+k):v for k,v in rev_map.items() if v},**{('expenditure.'+k):v for k,v in exp_map.items()},
 'principal_repayment':'principal_repayment','deficit':'deficit','deficit_pct_gdp':'deficit_pct_gdp','total_borrowing':'total_borrowing'}
for name,k in allkeys.items():
    src[name]={str(y):D[y]['src'] for y in Y if D[y].get(k) is not None}
for k in out['extra']:
    pass
src['extra.*']={str(y):D[y]['src'] for y in Y}
src['extra.expenditure_national_reserves']={str(y):D[y].get('src_nr',D[y]['src']) for y in Y if D[y].get('national_reserves') is not None}
src['cross_checks']={
 '2019':'https://thoibaotaichinhvietnam.vn/nam-2019-chi-ngan-sach-dat-935-du-toan-51121.html (Govt QT2019 report: ĐTPT 420,779.784 / recurrent 995,647.182 bn before NA reclassification of 1,065 bn; NA appendix used)',
 '2022':'https://datafiles.chinhphu.vn/cpp/files/vbpq/2024/7/1686-btc.signed.pdf vs NQ 132/2024/QH15 appendix — identical',
 '2023':'https://datafiles.chinhphu.vn/cpp/files/vbpq/2025/7/223-qh15.signed.pdf (NQ 223/2025/QH15) — identical to QĐ 2557/QĐ-BTC',
 '2024':'https://ckns.mof.gov.vn/Lists/News/DispForm.aspx?ID=66 (MoF thuyết minh QT 2024, published 22/05/2026) — identical to NQ 21/2026/QH16',
}
out['sources']=src
out['notes']=[
 "Units: billion VND (tỷ đồng). 2018 and 2019 source tables are in million VND (triệu đồng) and were rounded to the nearest billion.",
 "2018–2024 = final accounts (quyết toán). 2018: NQ 114/2020/QH14 appendices; 2019: NQ 22/2021/QH15 appendices; 2020–2023: MoF công khai quyết toán decisions (QĐ 1420/QĐ-BTC 2022, 1518/QĐ-BTC 2023, 1686/QĐ-BTC 2024, 2557/QĐ-BTC 2025; Biểu 26/27-CK-NSNN); 2024: NQ 21/2026/QH16 (24/4/2026) Phụ lục I (Mẫu biểu 58) & II (59), matching MoF thuyết minh published 22/5/2026.",
 "revenue.total = 'Thu NSNN' (domestic + crude oil + import-export balance + grants) and EXCLUDES carried-over revenue (thu chuyển nguồn), prior-year local surplus (kết dư) and drawdowns from the financial reserve fund. The NA-resolution headline 'tổng số thu cân đối NSNN' INCLUDES those items — it is stored in extra.total_revenue_resources_incl_carryover (e.g. 2023: 3,023,547 vs 1,770,776).",
 "expenditure.total = 'Chi NSNN' (excludes chi chuyển nguồn sang năm sau). The NA headline 'tổng số chi cân đối NSNN' includes carry-over spending (extra.carryover_expenditure_to_next_year). Principal repayment (chi trả nợ gốc) is financing, outside expenditure.total.",
 "Expenditure components listed (development_investment, interest, aid, recurrent, reserve_fund) do not include 'chi dự trữ quốc gia' (national reserves), stored separately in extra.expenditure_national_reserves. MoF Biểu 26 for 2020–2023 omits that line; for 2022 (1,990) and 2023 (1,212) it was taken from the NA resolution appendices. For 2020 and 2021 it was not read from a source; the residual (Chi NSNN minus listed components) is 1,566 (2020) and 3,119 (2021) and is presumably national reserves — left null.",
 "Recurrent spending includes wage-reform outlays allocated to sectors. Contingency (dự phòng) appears only in plans (2026: 100,402); in final accounts it is spent within the sectoral lines.",
 "Deficit (bội chi NSNN) is the official figure: total spending incl. carry-over minus total resources excl. local-budget surplus. It is NOT equal to revenue.total minus expenditure.total.",
 "deficit_pct_gdp uses the GDP stated in each source: 2018 (GDP 5,542,300), 2019 (6,037,348) and 2020 (6,293,145) use pre-2021-revaluation GDP; 2021 onward use revalued GDP (2023: 10,320,300; 2024: 11,511,867). Ratios for 2018–2020 are therefore not comparable to 2021+ (revaluation raised GDP by roughly a quarter).",
 "total_borrowing = tổng mức vay của NSNN (deficit financing + borrowing for principal repayment). 2018 and 2019 differ from deficit+principal because some local principal was repaid from surpluses; 2023 deficit excludes 28,000 bn carried over from 2022 to repay principal (per QĐ 2557 note).",
 "2019: the NA reclassified 2,240.21 bn of MoF recurrent budget to development investment; NA appendix figures (ĐTPT 421,845; recurrent 994,582) are used rather than the Government report (420,780 / 995,647).",
 "2025 = MoF supplementary assessment of 2025 execution ('Báo cáo đánh giá bổ sung kết quả thực hiện NSNN năm 2025', ckns.mof.gov.vn, 13/04/2026), not final accounts; values were given in nghìn tỷ with one decimal (precision ±50 bn). Final accounts for 2025 are not yet approved as of Oct 2026.",
 "2025 expenditure.total (3,312,600) is MoF's 'tổng chi NSNN ước' and INCLUDES ~714.6k bn of spending authorised from 2025 revenue overruns plus ~63.2k bn from wage-reform savings, much of it likely to be carried over; within-budget execution was ~2,534,800. Components given: ĐTPT 1,039,100 (1,184,600 if spending funded by 2021–2024 central revenue overruns carried into 2025 is included), interest 108,100, recurrent 1,683,300; aid, reserve fund, principal repayment and total borrowing for 2025 were not reported (null). For reference the 2025 plan (NQ 159/2024/QH15 and borrowing plan) had central borrowing of 804,242 and principal repayment of 361,142 — not used.",
 "2025 grants (5,900) = amount recorded into the budget so far. 2025 land/house total not reported; land-use fees (498,600) and land rent (75,600) were.",
 "2026 = plan (dự toán) from NQ 245/2025/QH15 (13/11/2025). expenditure.total 3,159,106 includes contingency 100,402 and 23,839 from local wage-reform carryover (revenue side shows this separately, outside revenue.total). total_borrowing 985,784 = 'tổng mức vay' (606,139 for deficit + 379,645 for principal).",
 "Tax-by-type: Vietnamese budget tables report domestic revenue by ownership sector (SOE / FDI / non-state, stored in extra) with VAT, CIT and SCT on domestic production embedded inside those sector lines, so revenue.vat, revenue.cit and revenue.special_consumption are null for every year (no official national split found). VAT and SCT on imports are in extra.revenue_vat_on_imports / extra.revenue_sct_on_imports. PIT, environmental protection tax (domestic only; import BVMT is in the import-export block), fees & charges, land-use fees, and lottery are reported directly.",
 "fees_charges = 'Các loại phí, lệ phí' (includes registration fee lệ phí trước bạ). land_use_fees = 'Thu tiền sử dụng đất' only; extra.revenue_land_house_total adds land/house taxes, land rent and state-housing sales.",
]
json.dump(out,open('budget.json','w'),ensure_ascii=False,indent=1)
