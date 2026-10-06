import json
Y=list(range(2015,2027))
def ser(d): return [d.get(y) for y in Y]
B12="http://static1.vietstock.vn/edocs/14239/document_4_.pdf"   # Ban tin no cong so 12 (11/2021), 2016-2020
B14="http://static1.vietstock.vn/edocs/14241/document_5_.pdf"   # so 14 (7/2022), 2017-2021
B16="http://static1.vietstock.vn/edocs/14236/document_1_.pdf"   # so 16 (8/2023), 2018-2022
B17="https://static1.vietstock.vn/edocs/14237/document_2_.pdf"  # so 17 (1/2024), 2019-6/2023
B21="https://finance.vietstock.vn/downloadedoc/18952"           # so 21 (12/2025), 2021-6/2025
KN="https://data360api.worldbank.org/data360/data?DATABASE_ID=WB_KNOMAD&INDICATOR=WB_KNOMAD_MRI&REF_AREA=VNM"
WBAPI="https://api.worldbank.org/v2/country/VNM/indicator/{}?format=json&date=2014:2025"

remit_sbv={2021:12.5,2023:16.0,2024:16.0}
src_sbv={2021:"https://vneconomy.vn/nam-2021-kieu-hoi-ve-viet-nam-uoc-tinh-khoang-12-5-ty-usd.htm",
 2023:"https://vnexpress.net/kieu-hoi-ve-viet-nam-dat-ky-luc-16-ty-usd-4706903.html",
 2024:"https://tienphong.vn/thay-gi-tu-dong-kieu-hoi-cao-ky-luc-chay-ve-viet-nam-post1774379.tpo"}

remit_wb={2015:8.050696859,2016:8.556092341,2017:9.405606825,2018:10.191360839,2019:10.885224766,2020:10.714571746,2021:12.722088864,2022:13.2,2023:14.0}
remit_wb={k:round(v,3) for k,v in remit_wb.items()}
remit_wb_proj={2024:14.5,2025:15.0}
remit_wb_old={2015:{"v":12.25,"url":"https://www.sggp.org.vn/viet-nam-nhan-1225-ty-usd-kieu-hoi-trong-nam-2015-post217503.html"},
 2017:{"v":13.8,"url":"https://www.voatiengviet.com/a/ngan-hang-the-gioi-tinh-khong-kieu-hoi-viet-nam/5079584.html"},
 2018:{"v":15.9,"url":"https://www.voatiengviet.com/a/ngan-hang-the-gioi-tinh-khong-kieu-hoi-viet-nam/5079584.html"},
 2019:{"v":16.7,"url":"https://vietnamplus.vn/nam-2019-luong-kieu-hoi-ve-viet-nam-uoc-dat-167-ty-usd/615187.vnp"},
 2021:{"v":18.1,"url":"https://vneconomy.vn/nam-2021-kieu-hoi-ve-viet-nam-uoc-tinh-khoang-12-5-ty-usd.htm"},
 2022:{"v":19.0,"url":"https://vnexpress.net/hon-190-ty-usd-kieu-hoi-ve-viet-nam-trong-30-nam-4694419.html"}}

hcmc={2015:5.5,2016:5.0,2017:5.2,2018:5.0,2019:5.3,2020:6.1,2021:6.6,2022:6.603,2023:9.46,2024:9.547,2025:10.34}
src_hcmc={2015:"https://vietstock.vn/2015/12/nam-2015-kieu-hoi-ve-tphcm-uoc-dat-55-ty-usd-758-450118.htm",
 2016:"http://cafef.vn/tai-sao-kieu-hoi-nam-2016-khong-nhu-du-uoc-20161208162421819.chn",
 2017:"https://tuoitre.vn/5-2-ti-usd-kieu-hoi-ve-tp-hcm-trong-nam-2017-20171222194404624.htm",
 2018:"https://vneconomy.vn/nam-2018-5-ty-usd-kieu-hoi-do-ve-tphcm.htm",
 2019:"https://vov.vn/kinh-te/53-ty-usd-kieu-hoi-ve-tphcm-nam-2019-998239.vov",
 2020:"https://tuoitre.vn/kieu-hoi-tphcm-bat-ngo-lap-ky-luc-ca-nam-2020-dat-61-ti-usd-giap-tet-tang-manh-20210210084031875.htm",
 2021:"https://baodautu.vn/kieu-hoi-ve-tphcm-dat-khoang-66-ty-usd-trong-nam-2021-d159978.html",
 2022:"https://vnexpress.net/kieu-hoi-ve-tp-hcm-dat-hon-6-6-ty-usd-4563095.html",
 2023:"https://www.sggp.org.vn/nan-dong-kieu-hoi-vao-ha-tang-bai-3-kieu-hoi-chay-ve-dau-post735715.html",
 2024:"https://ttbc-hcm.gov.vn/luong-kieu-hoi-ve-tphcm-nam-2024-dat-hon-95-ty-usd-48262.html",
 2025:"https://tphcm.chinhphu.vn/kieu-hoi-ve-tphcm-dat-hon-1034-ty-usd-nam-2025-10126012222501171.htm"}

workers={2015:115980,2016:126289,2017:134751,2018:142860,2019:152530,2020:78641,2021:45058,2022:142779,2023:159986,2024:158588,2025:144345}
VP="https://www.vietnamplus.vn/xuat-khau-lao-dong-vuot-muc-100000-nguoi-trong-6-nam-lien-tiep/616366.vnp"
src_w={2015:VP,2016:VP,2017:VP,2018:VP,
 2019:"https://vi.wikipedia.org/wiki/Xu%E1%BA%A5t_kh%E1%BA%A9u_lao_%C4%91%E1%BB%99ng_Vi%E1%BB%87t_Nam",
 2020:"https://vneconomy.vn/lao-dong-di-lam-viec-o-nuoc-ngoai-giam-manh-trong-nam-2021.htm",
 2021:"https://vneconomy.vn/lao-dong-di-lam-viec-o-nuoc-ngoai-giam-manh-trong-nam-2021.htm",
 2022:"https://baochinhphu.vn/xuat-khau-lao-dong-phuc-hoi-manh-me-dua-hon-142000-nguoi-di-lam-viec-102230106165036477.htm",
 2023:"https://nhandan.vn/nam-2023-gan-160-nghin-lao-dong-viet-nam-di-lam-viec-o-nuoc-ngoai-post791359.html",
 2024:"https://nhandan.vn/nam-2024-hon-158-nghin-lao-dong-di-lam-viec-o-nuoc-ngoai-post857345.html",
 2025:"https://dantri.com.vn/lao-dong-viec-lam/hon-144000-lao-dong-ra-nuoc-ngoai-huong-toi-thi-truong-thu-nhap-cao-20251226225850596.htm"}

stock={"2018":{"n":500000,"text":"khoảng 500.000 lao động VN làm việc ở nước ngoài (Bộ trưởng Đào Ngọc Dung, chất vấn QH 6/2018)","url":"https://baodautu.vn/moi-nam-lao-dong-viet-nam-tai-nuoc-ngoai-gui-ve-nuoc-khoang-3-ty-usd-d83574.html"},
 "2023":{"n":650000,"text":"khoảng 650.000 lao động VN đang làm việc tại 40 quốc gia/vùng lãnh thổ (DOLAB, 10/2023)","url":"https://vov.vn/xa-hoi/650000-lao-dong-viet-nam-dang-lam-viec-o-nuoc-ngoai-post1055715.vov"},
 "2024":{"n":700000,"text":"hơn 700.000 lao động VN đang làm việc ở nước ngoài theo hợp đồng (Bộ LĐTBXH, 27/12/2024)","url":"https://znews.vn/hon-700000-nguoi-viet-dang-lam-viec-o-nuoc-ngoai-gui-ve-4-ty-usdnam-post1520599.html"},
 "2025":{"n":900000,"text":"gần 900.000 lao động VN đang làm việc ở nước ngoài tính đến cuối 2025 (Bộ Nội vụ; báo 6/2026). Oct-2025 statement: ~860.000 (vneconomy 31/10/2025)","url":"https://tuoitre.vn/nld/gan-900000-lao-dong-viet-dang-lam-viec-o-nuoc-nao-196260608211624841.htm"}}

stmts=[
 {"year":2018,"text":"bình quân mỗi năm lao động VN làm việc ở nước ngoài theo hợp đồng gửi về khoảng 3 tỷ USD (Bộ trưởng LĐTBXH Đào Ngọc Dung, QH 20/6/2018)","url":"https://baodautu.vn/moi-nam-lao-dong-viet-nam-tai-nuoc-ngoai-gui-ve-nuoc-khoang-3-ty-usd-d83574.html"},
 {"year":2023,"text":"khoảng 3,5–4 tỷ USD/năm (Thứ trưởng LĐTBXH Nguyễn Bá Hoan, hội thảo 27/12/2023)","url":"https://dantri.com.vn/lao-dong-viec-lam/nguoi-viet-di-lam-viec-o-nuoc-ngoai-gui-ve-4-ty-usd-kieu-hoi-moi-nam-20231227175509375.htm"},
 {"year":2024,"text":"hơn 700.000 lao động... gửi về khoảng 3,5–4 tỷ USD mỗi năm (Bộ LĐTBXH, hội nghị 27/12/2024)","url":"https://znews.vn/hon-700000-nguoi-viet-dang-lam-viec-o-nuoc-ngoai-gui-ve-4-ty-usdnam-post1520599.html"},
 {"year":2025,"text":"người lao động gửi về nước khoảng 6,5–7 tỷ USD mỗi năm (Q. Cục trưởng Cục QLLĐNN, Bộ Nội vụ Vũ Trường Giang, 30/10/2025)","url":"https://www.vietnamplus.vn/hang-nam-lao-dong-di-lam-viec-o-nuoc-ngoai-gui-ve-nuoc-khoang-65-7-ty-usd-post1073744.vnp"},
 {"year":2026,"text":"nguồn kiều hối từ lực lượng này ước đạt 6–7 tỉ USD mỗi năm (Bộ Nội vụ; ~900.000 lao động cuối 2025; báo 8/6/2026)","url":"https://tuoitre.vn/nld/gan-900000-lao-dong-viet-dang-lam-viec-o-nuoc-nao-196260608211624841.htm"}]

# MoF national external debt (gross repayments, incl. short-term rollover), million USD -> bn
tot={2016:56330.29,2017:81994.12,2018:97324.81,2019:86480.49,2020:112629.84,2021:131617.00,2022:152054.06,2023:131224.52,2024:138253.70}
pri={2016:54758.86,2017:79612.33,2018:93964.56,2019:82956.26,2020:109866.54,2021:129642.08,2022:149196.12,2023:125555.85,2024:133454.39}
inte={2016:1571.43,2017:2381.80,2018:3360.25,2019:3524.23,2020:2763.30,2021:1974.93,2022:2857.94,2023:5668.68,2024:4799.31}
src_nat={2016:B12,2017:B14,2018:B16,2019:B17,2020:B17,2021:B21,2022:B21,2023:B21,2024:B21}
bn=lambda d:{k:round(v/1000,3) for k,v in d.items()}
pct={2016:3.90,2017:6.1,2018:7.0,2019:5.9,2020:5.70,2021:6.19,2022:6.91,2023:7.68,2024:7.84}
src_pct={2016:B12,2017:B12,2018:B16,2019:B16,2020:B21,2021:B21,2022:B21,2023:B21,2024:B21}
gov_t={2016:2111.24,2017:1973.38,2018:2286.06,2019:2556.14,2020:3508.12,2021:3156.35,2022:3412.18,2023:3551.89,2024:4560.98}
gov_p={2016:1520.02,2017:1335.63,2018:1574.40,2019:1820.88,2020:2785.97,2021:2529.21,2022:2790.81,2023:2729.35,2024:3738.01}
gov_i={2016:591.22,2017:637.75,2018:711.66,2019:735.26,2020:722.15,2021:627.15,2022:621.37,2023:822.54,2024:822.97}
gua_t={2016:1600.65,2017:1873.48,2018:2009.20,2019:2074.93,2020:1890.82,2021:1875.01,2022:1570.97,2023:1607.17,2024:1643.08}
gua_p={2016:1259.00,2017:1471.55,2018:1580.23,2019:1604.34,2020:1535.23,2021:1665.18,2022:1275.02,2023:1260.25,2024:1345.76}
gua_i={2016:341.65,2017:401.93,2018:428.97,2019:470.60,2020:355.59,2021:209.83,2022:295.95,2023:346.92,2024:297.31}
src_gua={2016:B12,2017:B12,2018:B16,2019:B16,2020:B16,2021:B21,2022:B21,2023:B21,2024:B21}
pub_t={k:round((gov_t[k]+gua_t[k])/1000,3) for k in gov_t}

wb_tds={2015:6825443949.5,2016:7719553295.4,2017:14390394668.5,2018:18898201021.3,2019:17249257565.3,2020:17111738453.5,2021:20700645830.8,2022:26221997164.7,2023:28625876473.9,2024:33590426649.4}
wb_tds_pct={2015:3.93021313872606,2016:4.06868318587895,2017:6.28582677474731,2018:7.26567450189829,2019:6.0937874484832,2020:5.86707620715429,2021:6.06864858953762,2022:6.76588446873016,2023:7.54231510337596,2024:7.72326875884348}
wb_ppg={2015:2833091642.4,2016:3661431440.3,2017:3687257572.5,2018:4134551702.5,2019:4444651827.8,2020:5111361498.3,2021:4507491541.2,2022:4404377082.5,2023:4645961920.1,2024:5850609464.2}
oda={2015:450664573,2016:398200818,2017:437490527,2018:425693504,2019:348249148,2020:493561358,2021:578241575,2022:481569258,2023:525673644}

out={"years":Y,
"remit_sbv_busd":ser(remit_sbv),
"remit_wb_busd":ser(remit_wb),
"remit_wb_projection_busd":ser(remit_wb_proj),
"remit_wb_older_vintage_press":{str(k):v for k,v in remit_wb_old.items()},
"remit_hcmc_busd":ser(hcmc),
"workers_sent":ser(workers),
"workers_stock":stock,
"worker_remit_statements":stmts,
"ext_debt_service_busd":ser(bn(tot)),
"ext_debt_principal_busd":ser(bn(pri)),
"ext_debt_interest_busd":ser(bn(inte)),
"ext_debt_service_pct_exports":ser(pct),
"ext_debt_service_pct_exports_old_definition":{"2015":{"v":12.4,"url":"https://www.phs.vn/tin-tuc/nghia-vu-tra-no-nuoc-ngoai-cua-quoc-gia-da-vuot-gioi-han-cho-phep/5204910"},"2016":{"v":29.7,"url":"https://www.phs.vn/tin-tuc/nghia-vu-tra-no-nuoc-ngoai-cua-quoc-gia-da-vuot-gioi-han-cho-phep/5204910"},"2025":{"v":"khoảng 6–7% (ước, Báo cáo CP trình QH kỳ 10)","url":"https://vneconomy.vn/no-cong-nam-2025-thap-xa-nguong-tran-nhu-cau-vay-nam-2026-tang-gan-19.htm"}},
"gov_ext_debt_service_busd":ser(bn(gov_t)),
"gov_ext_debt_principal_busd":ser(bn(gov_p)),
"gov_ext_debt_interest_busd":ser(bn(gov_i)),
"guaranteed_ext_debt_service_busd":ser(bn(gua_t)),
"guaranteed_ext_debt_principal_busd":ser(bn(gua_p)),
"guaranteed_ext_debt_interest_busd":ser(bn(gua_i)),
"public_ext_debt_service_busd_gov_plus_guaranteed":ser(pub_t),
"wb_ids_total_debt_service_busd":ser({k:round(v/1e9,3) for k,v in wb_tds.items()}),
"wb_ids_total_debt_service_pct_exports_gni_basis":ser({k:round(v,2) for k,v in wb_tds_pct.items()}),
"wb_ids_ppg_debt_service_busd":ser({k:round(v/1e9,3) for k,v in wb_ppg.items()}),
"oda_grants_wb_busd":ser({k:round(v/1e9,3) for k,v in oda.items()}),
"partial_2026":{
  "remit_hcmc_q1_2026_busd":{"v":2.004,"url":"https://nhipsongkinhdoanh.vn/quy-i-2026--kieu-hoi-ve-tp--ho-chi-minh-dat-hon-2-004-ty-usd-27808.htm"},
  "remit_hcmc_h1_2026_busd":{"v":4.037,"yoy_pct":-22.8,"q2_2026":2.032,"url":"https://tphcm.chinhphu.vn/kieu-hoi-ve-tphcm-dat-hon-4-ty-usd-trong-nua-dau-nam-2026-101260722183446092.htm"},
  "remit_hcmc_2026_forecast_busd":{"v":"8,6–8,9 (NHNN KV2 forecast)","url":"https://tphcm.chinhphu.vn/kieu-hoi-ve-tphcm-dat-hon-4-ty-usd-trong-nua-dau-nam-2026-101260722183446092.htm"},
  "remit_hcmc_9m_2026_busd":None,
  "workers_sent_8m_2026":{"v":90319,"plan_2026":112000,"pct_plan":80.64,"yoy_pct":5.29,"aug_2026":10901,"url":"https://nhandan.vn/tam-thang-hon-90000-lao-dong-di-lam-viec-o-nuoc-ngoai-theo-hop-dong-post988075.html"},
  "ext_debt_h1_2025_mof":{"total_service_busd":82.247,"principal_busd":80.461,"interest_busd":1.786,"gov_ext_service_busd":1.502,"guaranteed_ext_service_busd":0.716,"url":B21},
  "remit_hcmc_h1_2025_busd":{"v":5.23,"url":"https://tienphong.vn/thay-gi-tu-dong-kieu-hoi-cao-ky-luc-chay-ve-viet-nam-post1774379.tpo"},
  "remit_hcmc_9m_2025_busd":{"v":7.969,"url":"https://thitruongtaichinhtiente.vn/thay-gi-tu-con-so-gan-8-ty-usd-kieu-hoi-ve-tp-ho-chi-minh-trong-9-thang-dau-nam-73298.html"}},
"sources":{
 "remit_sbv_busd":{str(k):v for k,v in src_sbv.items()},
 "remit_wb_busd":{str(y):KN for y in remit_wb},
 "remit_wb_projection_busd":{"2024":"https://documents1.worldbank.org/curated/en/099714008132436612/pdf/IDU1a9cf73b51fcad1425a1a0dd1cc8f2f3331ce.pdf","2025":"https://documents1.worldbank.org/curated/en/099714008132436612/pdf/IDU1a9cf73b51fcad1425a1a0dd1cc8f2f3331ce.pdf"},
 "remit_hcmc_busd":{str(k):v for k,v in src_hcmc.items()},
 "workers_sent":{str(k):v for k,v in src_w.items()},
 "ext_debt_service_busd":{str(k):v for k,v in src_nat.items()},
 "ext_debt_principal_busd":{str(k):v for k,v in src_nat.items()},
 "ext_debt_interest_busd":{str(k):v for k,v in src_nat.items()},
 "ext_debt_service_pct_exports":{str(k):v for k,v in src_pct.items()},
 "gov_ext_debt_service_busd":{str(k):v for k,v in src_nat.items()},
 "guaranteed_ext_debt_service_busd":{str(k):v for k,v in src_gua.items()},
 "wb_ids_total_debt_service_busd":WBAPI.format("DT.TDS.DECT.CD"),
 "wb_ids_total_debt_service_pct_exports_gni_basis":WBAPI.format("DT.TDS.DECT.EX.ZS"),
 "wb_ids_ppg_debt_service_busd":WBAPI.format("DT.TDS.DPPG.CD"),
 "oda_grants_wb_busd":WBAPI.format("BX.GRT.EXTA.CD.WD"),
 "mof_bulletins":{"so12_11-2021_2016-2020":B12,"so14_7-2022_2017-2021":B14,"so15_3-2023_2018-6/2022":"http://static1.vietstock.vn/edocs/14235/document.pdf","so16_8-2023_2018-2022":B16,"so17_1-2024_2019-6/2023":B17,"so21_12-2025_2021-6/2025":B21,"official_listing_unreachable":"https://qln.mof.gov.vn/ban-tin-no-cong.htm"}},
"notes":[
 "remit_sbv: only years with an explicit SBV (NHNN) nationwide statement. 2021=12.5 (SBV official, vs WB 18.1, vneconomy 29/12/2021); 2023≈16 (+32% y/y, SBV Vụ Quản lý ngoại hối, Đào Xuân Tuấn); 2024≈16 (reported as SBV, tienphong; other outlets e.g. vtv/baodautu report 16 unattributed). 2015–2020, 2022, 2025 null: SBV published no clear nationwide total (2016: SBV-HCMC deputy director estimated ~9 bn nationwide as of 11/2016 — cafef; vtcnews 9/2019 cites 2016=11.88 unattributed). 2022 implied ~12.1 from the 2023 +32% statement but not stated, so null. 2025 nationwide not published; press only says HCMC >60% of national total.",
 "remit_wb: current World Bank/KNOMAD vintage (Data360 WB_KNOMAD_MRI, 'Remittance inflows (US$ million)'), fetched 2026-10-05; values for 2015–2021 are heavily revised DOWN versus earlier WB press estimates (e.g. 2019 16.7, 2021 18.1, 2022 19.0 in older vintage — see remit_wb_older_vintage_press). WB notes Vietnam has BPM5->BPM6 data gaps and uses estimates. WDI BX.TRF.PWKR.CD.DT is empty for VNM 2005+ (confirmed). 2024/2025 only WB projections from Migration & Development Brief 40 (June 2024): 14.5 and 15.0; MDB 39 (Dec 2023) projected 2024 at 14.6. No WB actual for 2024/2025 found (KNOMAD briefs discontinued).",
 "remit_hcmc: SBV HCMC branch (from 7/2025: SBV Region 2 branch, covering the merged HCMC incl. former Bình Dương & Bà Rịa–Vũng Tàu) year-end figures as published (often 'ước' estimates in Dec/Jan). 2016=~5.0 is a 12/2016 projection (cafef); 2017=5.2 article says +4.5% vs 2016. 2021=6.6 (estimate, 12/2021), but the 1/2023 report says 2022=6.603 was -6.7% y/y, implying a revised 2021 ≈7.08 (not stated directly, not used). 2024 = 9.547 (press also rounds to 9.55/9.6). 2025=10.34 (+8.3%); earlier 12/2025 estimate was 10.5.",
 "workers_sent: DOLAB (Cục QLLĐNN, MOLISA; from 3/2025 Bộ Nội vụ). 2015–2018 from vietnamplus 2/1/2020 table. 2019: 152,530 final (vi.wikipedia; VOV 10/2023 says 'gần 153.000'); the 2/1/2020 preliminary figure was 147,387. 2020=78,641; 2021=45,058; 2022=142,779; 2023=159,986; 2024=158,588; 2025=144,345 (111% of 130k plan). 2026 plan 112,000; 8M/2026 = 90,319.",
 "workers_stock: stated approximations only (not series). vietnamplus 30/10/2025 also mentions ~636,000 workers sent in 2021–2025 (cumulative flow vs 500k plan), not a stock.",
 "worker_remit_statements: kept verbatim ranges; officially quoted figures jumped from 3.5–4 bn (MOLISA, 12/2023 and 12/2024) to 6.5–7 bn (MoHA, 10/2025).",
 "ext_debt_service_busd/principal/interest: MoF 'Nợ nước ngoài của quốc gia' table, 'TỔNG TRẢ NỢ TRONG KỲ' (Trả nợ gốc + lãi và phí), USD million converted to bn (÷1000). IMPORTANT: this is GROSS repayment incl. rolled-over short-term enterprise/bank debt, so principal is ~93–149 bn/yr — not comparable to WB IDS debt service. Latest available vintage used per year (2016 bulletin 12; 2017 bulletin 14; 2018 bulletin 16; 2019–2020 bulletin 17; 2021–2024 bulletin 21; 2024 marked (P) preliminary). 2015 and 2025 full-year not found -> null (H1-2025 in partial_2026).",
 "ext_debt_service_pct_exports: MoF indicator 3 'Nghĩa vụ trả nợ nước ngoài của quốc gia so với tổng kim ngạch XK hàng hoá và dịch vụ'. 2016–2020: medium/long-term only (excl. short-term); from 2021: excl. short-term principal (bulletin 21 footnote). 2016=3.90 and 2017=6.1 from bulletin 12. Older definition (incl. short-term principal) in Government report to NA: 2015=12.4%, 2016=29.7% (phs.vn 11/2017) — stored separately. 2025 Gov estimate 'khoảng 6–7%' (report to NA 10th session, 10/2025).",
 "gov_ext_debt_service: Government direct external debt (Vay và trả nợ của Chính phủ – Nợ nước ngoài), from the same national table rows. guaranteed_ext_debt_service: 'Nợ được Chính phủ bảo lãnh – Nợ nước ngoài' rows. public_ext_debt_service = gov + guaranteed (my sum of the two published rows; local-government foreign debt is on-lent and not separately added).",
 "wb_ids_*: World Bank International Debt Statistics via WDI API (DT.TDS.DECT.CD total external debt service; DT.TDS.DECT.EX.ZS % of exports of goods, services and primary income; DT.TDS.DPPG.CD PPG debt service). 2025 null. IDS principal/interest split series were not retrievable (API returned 'indicator not found').",
 "oda_grants_wb_busd: WB WDI BX.GRT.EXTA.CD.WD 'Grants, excluding technical cooperation (BoP, current US$)' 2015–2023; 2024–2025 null. No MoF annual 'viện trợ không hoàn lại' series collected.",
 "qln.mof.gov.vn (official bulletin listing) was unreachable (DNS failure) on 2026-10-05; bulletin PDFs taken from vietstock mirrors of the MoF Bản tin nợ công."]}
json.dump(out,open("remit.json","w"),ensure_ascii=False,indent=1)
