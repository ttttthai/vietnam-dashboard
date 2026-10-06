import json,csv
IL='https://api.imf.org/external/sdmx/2.1/data/IMF.STA,IL/VNM...?startPeriod=2010 (IMF International Liquidity dataset; indicator TRGNV_REVS = total reserves incl. gold, USD)'
CR19='https://www.imf.org/-/media/files/publications/cr/2019/1vnmea2019002.pdf (IMF CR 19/235, 2019 Art. IV, Table 5 Monetary Survey)'
CR21='https://www.imf.org/-/media/Files/Publications/CR/2021/English/1VNMEA2021001.ashx (IMF CR 21/42, 2020 Art. IV, Table 5)'
CR23='https://www.imf.org/-/media/files/publications/cr/2023/english/1vnmea2023003.pdf (IMF CR 23/338, 2023 Art. IV, Table 5)'
CR24='https://www.imf.org/-/media/files/publications/cr/2024/english/1vnmea2024001-print-pdf.pdf (IMF CR 24/306, 2024 Art. IV, Table 5)'
CR25='https://www.imf.org/-/media/files/publications/cr/2025/english/1vnmea2025001-source-pdf.pdf (IMF CR 25/283, 2025 Art. IV, Tables 1,3,5)'
years=list(range(2014,2027))
def ser(d): return [d.get(y) for y in years]
# --- IMF IL reserves
rows=list(csv.DictReader(open('bs_IL.csv')))
def il(ind,freq,unit='USD'):
    return {x['TIME_PERIOD']:round(float(x['OBS_VALUE'])/1e9,3) for x in rows if x['INDICATOR']==ind and x['FREQUENCY']==freq and x['UNIT']==unit and x['OBS_VALUE']}
trg_m=il('TRGNV_REVS','M'); fx_m=il('RXF11FX_REVS','M'); gold_m=il('RGOLDNV_REVS','M')
fx_annual={y:trg_m.get(f'{y}-M12') for y in years}
nfa={2014:722,2015:614,2016:806,2017:1097,2018:1263,2019:1813,2020:2199,2021:2536,2022:2049,2023:2209,2024:2034}
ncg={2014:7,2015:83,2016:31,2017:-10,2018:-97,2019:-276,2020:-485,2021:-551,2022:-450,2023:-784,2024:-460}
cob={2014:625,2015:727,2016:851,2017:978,2018:1085,2019:1198,2020:1338,2021:1520,2022:1353,2023:1426,2024:1631}
rmy={2014:18.7,2015:19.3,2016:12.8,2017:20.4,2018:6.9,2019:24.0,2020:10.0,2021:12.5,2022:-13.1,2023:11.0,2024:9.4}
m2={2014:5179,2015:6020,2016:7126,2017:8195,2018:9212,2019:10574,2020:12111,2021:13402,2022:14227,2023:15999,2024:17915}
gross_art4={2021:109.4,2022:86.7,2023:92.3,2024:83.1}
nulls=[None]*len(years)
bs={"freq":"A","periods":[str(y) for y in years],"unit":"trillion VND (end-year); fx_reserves_busd in bn USD; reserve_money_yoy_pct in %",
 "series":{
  "nfa":ser(nfa),"fx_reserves_busd":ser(fx_annual),"claims_govt":nulls,"claims_banks":nulls,"other_assets":nulls,
  "reserve_money":nulls,"currency_outside_banks":ser(cob),"bank_reserves":nulls,"govt_deposits":nulls,"sbv_bills":nulls,"capital_other":nulls,
  "net_claims_govt_sbv":ser(ncg),"reserve_money_yoy_pct":ser(rmy),"gross_reserves_imf_art4_busd":ser(gross_art4),"m2_memo":ser(m2)},
 "series_notes":{
  "nfa":"Net foreign assets of the SBV (IMF Monetary Survey, line 'State Bank of Vietnam (SBV)' under NFA). Latest vintage used per year: 2014 CR19/235; 2015-2018 CR21/42; 2019 CR23/338; 2020-2023 CR24/306 (2021-2023 identical in CR25/283); 2024 CR25/283. Note revisions: 2023 was 2,379 in CR23 projection vs 2,209 actual; 2024 was 2,073 in CR24 (proj) vs 2,034 in CR25.",
  "fx_reserves_busd":"December value of IMF IL indicator TRGNV_REVS (total reserves incl. gold at national valuation), raw USD/1e9. IMF Art. IV 'gross international reserves (excl. government deposits)' differ slightly - see gross_reserves_imf_art4_busd. 2026 annual null (see monthly_supplement through 2026-07).",
  "net_claims_govt_sbv":"SBV net claims on government (claims minus central government/State Treasury deposits), from IMF Monetary Survey line 'Net claims on government - SBV'. Negative = government deposits at SBV exceed SBV claims. Gross claims_govt and govt_deposits are not published separately -> null.",
  "currency_outside_banks":"Currency outside banks (IMF Monetary Survey; matches SBV M0 1,425,567/1,630,775 bn for 2023/2024 on TradingEconomics). Same vintages as nfa (2014 CR19; 2015-2018 CR21; 2019 CR23; 2020-2023 CR24/25; 2024 CR25).",
  "reserve_money_yoy_pct":"Reserve money y/y % (IMF memo item). Levels not published by SBV/IMF -> reserve_money null. 2014-2018 CR19/CR21; 2019 CR21; 2020-2023 CR24/CR25; 2024 CR25.",
  "gross_reserves_imf_art4_busd":"IMF Art. IV Table 1/3 'Gross international reserves (excludes government deposits)' (CR25/283).",
  "m2_memo":"Total liquidity M2 (IMF Monetary Survey) for context.",
  "claims_banks/other_assets/reserve_money/bank_reserves/govt_deposits/sbv_bills/capital_other":"Not publicly disseminated: SBV does not publish a central bank survey/balance sheet (IMF MFS_CBS has no Vietnam data; Vietnam NSDP lists 'Central Bank Survey' with metadata only, no data link). SBV bills outstanding available only weekly in 2025 from market reports - see operations."},
 "projections":{"source":CR25,"periods":["2025","2026"],"nfa":[2069,2118],"reserve_money_yoy_pct":[12.5,11.5],"gross_reserves_imf_art4_busd":[79.3,79.2],"m2":[20146,22460],"note":"IMF staff projections made mid-2025; not actuals."},
 "monthly_supplement":{
   "fx_reserves_total_busd":{"periods":sorted(k for k in trg_m if k>='2014'),"values":[trg_m[k] for k in sorted(k for k in trg_m if k>='2014')],"source":IL,"note":"TRGNV_REVS total reserves incl. gold (bn USD). Official SBV disclosure 87.6bn USD on 18/06/2026 vs IMF 89.55 for 2026-M06."},
   "fx_reserves_ex_gold_fx_busd":{"periods":sorted(k for k in fx_m if k>='2014'),"values":[fx_m[k] for k in sorted(k for k in fx_m if k>='2014')],"source":IL+" indicator RXF11FX_REVS (foreign currency reserves)"},
   "gold_reserves_busd":{"periods":sorted(k for k in gold_m if k>='2014'),"values":[gold_m[k] for k in sorted(k for k in gold_m if k>='2014')],"source":IL+" indicator RGOLDNV_REVS (gold, national valuation)"},
 },
 "sources":{"nfa":[CR19,CR21,CR23,CR24,CR25],"net_claims_govt_sbv":[CR19,CR21,CR23,CR24,CR25],"currency_outside_banks":[CR19,CR21,CR23,CR24,CR25,"https://tradingeconomics.com/vietnam/money-supply-m0"],"reserve_money_yoy_pct":[CR19,CR21,CR24,CR25],"fx_reserves_busd":IL,"gross_reserves_imf_art4_busd":CR25,"m2_memo":[CR19,CR21,CR23,CR24,CR25],
   "imf_mfs_cbs_check":"https://api.imf.org/external/sdmx/2.1/data/IMF.STA,MFS_CBS/VNM...?startPeriod=2014 (returns no Vietnam observations)",
   "nsdp":"https://nsdp.nso.gov.vn/index.htm (Central Bank Survey: DSBB metadata only)"}}
# derived monthly currency in circulation = M2 x cash/M2 ratio (SBV)
SBVM2='https://sbv.gov.vn (Dữ liệu thống kê > Tổng phương tiện thanh toán và Tiền gửi của khách hàng tại TCTD; API /o/article/v1.0/articles contentStructureId=10038520)'
SBVC='https://sbv.gov.vn (Dữ liệu thống kê > Tiền mặt lưu thông trên tổng phương tiện thanh toán; contentStructureId=10041267)'
m2m={'2025-10':18728084,'2025-11':18938260,'2025-12':19444476,'2026-01':19578695,'2026-02':19641068,'2026-03':19635094,'2026-04':19818534,'2026-05':19974985,'2026-06':20414616,'2026-07':20455644}
cr={'2025-10':10.68,'2025-11':10.78,'2025-12':10.78,'2026-01':11.52,'2026-02':12.09,'2026-03':11.11,'2026-04':10.77,'2026-05':10.46,'2026-06':10.11,'2026-07':9.98}
ps=sorted(m2m)
bs["monthly_supplement"]["currency_in_circulation_derived"]={"periods":ps,"unit":"trillion VND","m2":[round(m2m[p]/1000,1) for p in ps],"cash_to_m2_pct":[cr[p] for p in ps],
  "values":[round(m2m[p]*cr[p]/100/1000,1) for p in ps],"source":[SBVM2,SBVC],
  "note":"DERIVED = SBV M2 balance x SBV 'cash in circulation / M2' ratio. Only from Oct-2025 (new SBV methodology; earlier 2025 entries are dated by publication, month mapping ambiguous). Not an official SBV level."}
# ---------- income statement
yrs=list(range(2015,2026))
def ys(d): return [d.get(y) for y in yrs]
QT18='https://luatvietnam.vn/tai-chinh/quyet-dinh-1108-qd-btc-cong-bo-cong-khai-quyet-toan-ngan-sach-nha-nuoc-2018-188919-d1.html'
QT19='https://caselaw.vn/van-ban-phap-luat/394182-quyet-dinh-so-1592-qd-btc-ngay-19-08-2021-cua-bo-truong-bo-tai-chinh-cong-bo-cong-khai-quyet-toan-ngan-sach-nha-nuoc-nam-2019'
QT21='https://hethongphapluat.com/quyet-dinh-1518-qd-btc-nam-2023-cong-bo-cong-khai-quyet-toan-ngan-sach-nha-nuoc-nam-2021-do-bo-truong-bo-tai-chinh-ban-hanh.html'
QT22='https://ckns.mof.gov.vn/Lists/News/DispForm.aspx?ID=49'
QT23='https://luatvietnam.vn/tai-chinh/quyet-dinh-2557-qd-btc-cua-bo-tai-chinh-ve-viec-cong-bo-cong-khai-quyet-toan-ngan-sach-nha-nuoc-nam-2023-406560-d1.html'
QT24='https://ckns.mof.gov.vn/Lists/News/DispForm.aspx?ID=66 (also https://luatvietnam.vn/tai-chinh/quyet-dinh-1229-qd-btc-2026-cong-bo-quyet-toan-ngan-sach-nha-nuoc-nam-2024-435559-d1.html)'
E25='https://vneconomy.vn/nam-2025-ngan-sach-nha-nuoc-thang-du-hon-248-nghin-ty-dong.htm'
inc={"years":[str(y) for y in yrs],"unit":"trillion VND",
 "items":{
  "interest_income_fx_reserves":[None]*len(yrs),"interest_income_loans_to_CIs":[None]*len(yrs),"fx_revaluation_gains":[None]*len(yrs),
  "currency_printing_cost":[None]*len(yrs),"interest_paid_sbv_bills_deposits":[None]*len(yrs),"operating_expenses":[None]*len(yrs),
  "income_expenditure_surplus":[None]*len(yrs),
  "combined_line_budget_estimate":ys({2018:118.6,2019:109.5,2021:106.4,2023:77.236,2024:89.349}),
  "combined_line_settlement":ys({2018:137.72,2019:134.335,2021:75.351,2023:133.303,2024:138.682,2025:158.3}),
  "sbv_surplus_budget_estimate":ys({2024:18.2})},
 "budget_contribution":ys({2024:52.741}),
 "item_notes":{
  "income/expense lines":"SBV does not publish financial statements: SBV annual reports 2014-2024 (sbv.gov.vn > Báo cáo thường niên) contain no income statement or balance sheet (checked AR2024 table of contents: Parts I-IV + appendices on rates/OMO/RRR/BOP only). All P&L lines null.",
  "budget_contribution":"SBV-only 'chênh lệch thu, chi NHNN nộp ngân sách'. Only 2024 found: 52,741 bn VND (settlement), +34,541 bn vs estimate, 'chủ yếu do thu lãi hoạt động đầu tư ở nước ngoài của NHNN tăng mạnh'.",
  "sbv_surplus_budget_estimate":"2024 estimate computed as 52,741 - 34,541 = 18,200 bn VND from the same MoF sentence.",
  "combined_line":"MoF budget line 'Thu hồi vốn, thu cổ tức, lợi nhuận, lợi nhuận sau thuế, chênh lệch thu, chi của NHNN' (SOE capital recovery + dividends + SOE profits + SBV surplus) - an upper bound, NOT SBV-only. 2025 is MoF preliminary estimate (Jan-2026), not settlement. 2022 settlement: line was 15,528 bn (-16.5%) below estimate (level not found). 2019: +24,835 bn vs estimate (consistent with 134,335-109,500). 2015-2017, 2020, 2022 levels not found."},
 "sources":{"2018":QT18,"2019":[QT19,"https://ckns.mof.gov.vn/Lists/News/DispForm.aspx?ID=25"],"2021":QT21,"2022":QT22,"2023":QT23,"2024":QT24,"2025":E25,
   "sbv_annual_report_2024":"https://sbv.gov.vn/documents/20117/185410/Annual+report+2024.pdf/cd8d130f-a169-47c6-46a3-ddbe66b98302?t=1769157476979",
   "law_context":"https://vietnamfinance.vn/thong-doc-noi-ve-nguy-co-xuat-hien-khoan-am-nghin-ty-cua-ngan-hang-nha-nuoc-d148823.html (Aug-2026: Governor says SBV financial result could turn negative by 'vài ba nghìn tỷ đồng'; positive results go fully to the budget, no mechanism yet for negatives)"}}
# ---------- operations
ops=json.load(open('sbv_ops.json'))
AR24='https://sbv.gov.vn/documents/20117/185410/Annual+report+2024.pdf/cd8d130f-a169-47c6-46a3-ddbe66b98302?t=1769157476979'
extra=[
 {"date":"2024","period":"year","item":"other","value":5375,"unit":"bn VND","note":"OMO purchases (repo) 2024: 251 sessions, tenors 7/14d, avg winning volume/session 5,375 bn, rate 4.0-4.5% (SBV AR2024 App.2)","url":AR24},
 {"date":"2024","period":"year","item":"other","value":5658,"unit":"bn VND","note":"SBV bill sales 2024: 169 sessions, tenors 7/14/28d, avg winning volume/session 5,658 bn, rate 1.32-4.5% (SBV AR2024 App.2)","url":AR24},
 {"date":"2024-08-05","period":"point","item":"omo_rate","value":4.25,"unit":"%","note":"OMO repo rate cut 4.5%->4.25% (SBV AR2024)","url":AR24},
 {"date":"2024-09-16","period":"point","item":"omo_rate","value":4.0,"unit":"%","note":"OMO repo rate cut to 4.0% (SBV AR2024)","url":AR24},
 {"date":"2024-12","period":"point","item":"refinancing_rate","value":4.5,"unit":"%","note":"Refinancing 4.5%, rediscount 3.0%, overnight interbank e-payment lending 5.0% all 2024 (SBV AR2024 App.1)","url":AR24},
 {"date":"2024","period":"year","item":"rrr","value":3.0,"unit":"%","note":"RRR VND <12m 3%, >=12m 1%; FX <12m 8%, >=12m 6%, offshore CI deposits 1% (unchanged 2024)","url":AR24},
 {"date":"2026-06-30","period":"point","item":"state_treasury_deposits","value":700000,"unit":"bn VND","note":"KBNN deposits at Big4 'hơn 700.000 tỷ' end-Jun-2026, of which ~553-554k term deposits (VCB 185,255; BIDV 184,255; CTG 184,240)","url":"https://baodautu.vn/noi-tran-tien-gui-kho-bac-len-50-big-4-ngan-hang-co-them-hang-tram-nghin-ty-cho-vay-d659909.html"},
]
allops=sorted(ops+extra,key=lambda x:str(x['date']))
out={"balance_sheet":bs,"income_statement":inc,"operations":allops,
 "notes":[
  "Generated 2026-10-05. Never-invent policy: null wherever no published figure was found; derived values are explicitly labelled.",
  "Vietnam does not disseminate an SBV balance sheet / central bank survey: IMF MFS_CBS & MFS_MA have no VNM data; NSDP 'Central Bank Survey' row has metadata only; SBV annual reports have no financial statements. Hence only IMF Art. IV aggregates (SBV NFA, SBV net claims on government, currency outside banks, reserve-money growth) are available, annual, 2014-2024 (+IMF projections 2025-26).",
  "No 2026 IMF Art. IV published as of 2026-10-05 (latest CR 25/283, Aug-2025).",
  "FX reserves: IMF IL monthly to 2026-M07 (TRGNV 86.10 bn USD Jul-2026; 89.55 Jun-2026). SBV official: ~87.6 bn USD on 18/06/2026 (peak 111.8 bn Jan-2022).",
  "Operations log (436 points from sub-research + 7 added) covers weekly OMO net flow/outstanding Jan-2025 to 25-Sep-2026, SBV bills (issued Jan-Mar 2025 and Jun-Jul 2025; zero outstanding from 28/07/2025, none issued since), FX sales/swaps, RRR, State Treasury deposits at banks. CAUTION flags in notes: Vietstock OMO outstanding after 29/06/2026 looks wrong - prefer VBMA values.",
  "State Treasury deposits reported are at commercial banks (Big4), not at the SBV; deposits at SBV are only visible net in IMF 'net claims on government - SBV'.",
  "SBV surplus remitted to budget: only 2024 (52.741 trn VND) found SBV-only; other years only as combined MoF line incl. SOE dividends/divestment."]}
json.dump(out,open('sbv_bs.json','w'),ensure_ascii=False,indent=1)
print('ok',len(allops))
