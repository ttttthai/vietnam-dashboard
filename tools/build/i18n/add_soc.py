import json
d=json.load(open('en_extra.json'))
E={
"Tổng quan dân số ·":"Population overview ·",
"34 tỉnh, thành phố · 6 vùng kinh tế – xã hội":"34 provinces & cities · 6 socio-economic regions",
"TB 2025 (NSO, sơ bộ)":"2025 average (NSO, preliminary)",
"Tỷ trọng cả nước":"Share of national total","Diện tích":"Area","Mật độ":"Density","Đô thị hoá":"Urbanisation",
"2024 · dân số thành thị":"2024 · urban population","Mức sinh (TFR)":"Fertility (TFR)","2024 · con/phụ nữ":"2024 · children per woman",
"Tuổi thọ TB":"Life expectancy","Sinh thô · Chết thô":"Crude birth · death rate","Giới tính khi sinh":"Sex ratio at birth",
"2024 · bé trai/100 bé gái":"2024 · boys per 100 girls","Tỷ số giới tính":"Sex ratio","2024 · nam/100 nữ":"2024 · males per 100 females",
"mô hình":"model","bấm để chọn":"click to select","Dân số theo tỉnh":"Population by province","Dân số theo tỉnh trong vùng":"Population by province in the region",
"Hợp nhất từ":"Merged from","NQ 202/2025/QH15":"Resolution 202/2025/QH15",
"Trung du & miền núi phía Bắc":"Northern midlands & mountains","Đồng bằng sông Hồng":"Red River Delta","Bắc Trung Bộ":"North Central",
"Duyên hải Nam Trung Bộ & Tây Nguyên":"South Central Coast & Central Highlands","Đông Nam Bộ":"Southeast","Đồng bằng sông Cửu Long":"Mekong Delta",
"Nguồn: Cục Thống kê (NSO), bảng PL.V02 cho 34 đơn vị mới — dân số TB 2025 sơ bộ; đô thị hoá, TFR, tuổi thọ, tỷ suất sinh/chết, tỷ số giới tính năm 2024. Diện tích, danh sách sáp nhập: NQ 202/2025/QH15 (quy mô dân số trong NQ cao hơn số TB của NSO). Cấp vùng (≈) = bình quân gia quyền theo dân số. Cơ cấu tuổi, học sinh: mô hình áp phân bố tuổi quốc gia — không phải số điều tra của tỉnh.":
 "Source: National Statistics Office (NSO), tables PL.V02 for the 34 new units — 2025 average population (preliminary); urbanisation, TFR, life expectancy, crude birth/death rates and sex ratio for 2024. Area and merger lists: Resolution 202/2025/QH15 (its population figures are higher than NSO averages). Regional values (≈) are population-weighted averages. Age structure and pupils: model applying the national age profile — not provincial survey data.",
"Dân số cả nước 2000–2050 · thực tế & dự báo":"National population 2000–2050 · actual & projections",
"Thực tế NSO":"NSO actual","LHQ WPP 2024":"UN WPP 2024","NSO 2019–2069":"NSO 2019–2069",
"Dân số Việt Nam 2000–2050":"Vietnam population 2000–2050","Mốc dân số":"Population milestones","Thực tế / LHQ":"Actual / UN","NSO dự báo":"NSO projection","dự báo":"projection",
"LHQ dự báo 2050:":"UN projection for 2050:",
"Mức sinh đã xuống dưới mức sinh thay thế (TFR < 2,1) nên dân số tăng chậm dần và sẽ đạt đỉnh rồi giảm.":"Fertility is below replacement level (TFR < 2.1), so population growth keeps slowing and will peak, then decline.",
"Tuổi thọ tăng làm nhóm 65+ tăng nhanh → già hoá dân số, tỷ số phụ thuộc tăng trở lại sau năm 2035.":"Rising life expectancy makes the 65+ group grow fast → population ageing; the dependency ratio rises again after 2035.",
"Vùng tô xám nhạt = giai đoạn dự báo; dải cam = khoảng phương án thấp–cao của LHQ.":"Light-grey area = projection period; orange band = UN low–high variant range.",
"Đang cập nhật số liệu dân số…":"Updating population data…",
"Cơ cấu GDP theo ngành":"GDP structure by sector",
"Nông, lâm & thủy sản":"Agriculture, forestry & fishery","Nông, lâm nghiệp & thủy sản":"Agriculture, forestry & fishery",
"Giáo dục & Y tế":"Education & health","QLNN, ANQP & BĐXH bắt buộc":"Public admin, defence & social security",
"Điện, khí đốt, nước & rác thải":"Electricity, gas, water & waste","Tài chính, ngân hàng & BH":"Finance, banking & insurance",
"Thuế sản phẩm trừ trợ cấp":"Product taxes less subsidies","Dịch vụ khác":"Other services",
"NSO chính thức":"NSO official","NSO sơ bộ":"NSO preliminary","NSO ước tính":"NSO estimate",
"chuỗi cũ (trước điều chỉnh GDP 2021) · ngành con ≈ cơ cấu 2010":"old series (before the 2021 GDP revision) · sub-sectors ≈ 2010 structure",
"★ dự báo · ngành con ≈ giữ cơ cấu 2025":"★ forecast · sub-sectors ≈ 2025 structure held",
"Năm > 2025: dự báo/ngoại suy":"Year > 2025: forecast/extrapolation","Chuỗi cũ trước điều chỉnh GDP 2021":"Old series before the 2021 GDP revision",
"NSO — GDP giá hiện hành theo ngành":"NSO — GDP at current prices by sector",
"Nguồn: NSO, GDP theo giá hiện hành phân theo ngành (PxWeb V03.02, V03.04-05; chuỗi điều chỉnh 2021). 2010–2025 là số NSO (2024 sơ bộ, 2025 ước tính); trước 2010 là chuỗi cũ, sau 2025 là dự báo — ngành con ở các năm đó (≈) giữ cơ cấu trong ngành của năm NSO gần nhất.":
 "Source: NSO, GDP at current prices by economic activity (PxWeb V03.02, V03.04-05; 2021 revised series). 2010–2025 are NSO figures (2024 preliminary, 2025 estimate); before 2010 is the old series, after 2025 a forecast — sub-sectors in those years (≈) keep the within-sector structure of the nearest NSO year.",
"Lãi suất huy động & cho vay · 24 tháng":"Deposit & lending rates · 24 months",
"BQ 12T NHTM tư nhân (MBS)":"Avg 12M rate, private banks (MBS)","Cho vay BQ giao dịch mới (NHNN)":"Avg new-loan rate (SBV)","OMO · Tái cấp vốn":"OMO · Refinancing",
"Kỳ hạn tiền gửi Vietcombank":"Vietcombank deposit tenor",
"BQ 12T NHTM tư nhân (MBS):":"Avg 12M rate, private banks (MBS):","Cho vay BQ giao dịch mới (NHNN công bố):":"Avg new-loan rate (SBV-announced):",
"Chưa có số NHNN công bố cho giai đoạn này.":"No SBV-announced figure for this period yet.",
"VCB: biểu tại quầy · MBS: BQ niêm yết 12T · NHNN: số công bố (rời rạc) · OMO/tái cấp vốn: NHNN":"VCB: counter schedule · MBS: avg listed 12M · SBV: announced figures (sparse) · OMO/refinancing: SBV",
"Giữa T12/2025 nhóm Big 4 tăng lãi suất huy động lần đầu sau gần 2 năm (+0,5–0,6 điểm %) khi thanh khoản thắt lại: lãi suất qua đêm liên ngân hàng lên ~7% và NHNN nâng lãi suất OMO từ 4,0% lên 4,5% (4/12/2025).":
 "In mid-Dec 2025 the Big 4 raised deposit rates for the first time in almost 2 years (+0.5–0.6 pp) as liquidity tightened: overnight interbank rates reached ~7% and the SBV lifted the OMO rate from 4.0% to 4.5% (4 Dec 2025).",
"T3/2026: cuộc đua huy động — Vietcombank đưa 12T lên 5,9%, 24T lên 6,5%; BQ niêm yết 12T của khối tư nhân (MBS) vọt lên trên 8% (một phần do MBS chuyển sang số khảo sát thực tế).":
 "Mar 2026: deposit race — Vietcombank raised 12M to 5.9% and 24M to 6.5%; the private banks' average listed 12M rate (MBS) jumped above 8% (partly because MBS switched to actual-survey data).",
"Sau cuộc họp NHNN với 46 ngân hàng (9/4/2026), Big 4 giảm lãi suất dài hạn (VCB 24T 6,5% → 6,0% từ 13/4). NHNN nhiều lần chỉ đạo giảm lãi suất cho vay (CV 2342, 3972, 4190/NHNN; từ T8/2026 cho vay DNNVV thấp hơn ít nhất 1 điểm %).":
 "After the SBV met 46 banks (9 Apr 2026), the Big 4 cut long-term rates (VCB 24M 6.5% → 6.0% from 13 Apr). The SBV repeatedly instructed banks to lower lending rates (letters 2342, 3972, 4190/NHNN; from Aug 2026 SME loans at least 1 pp below average).",
"Giai đoạn T10/24–T11/25 biểu lãi suất Big 4 gần như đứng yên; khối tư nhân dao động quanh 4,9–5,3%.":"From Oct-24 to Nov-25 the Big 4 schedule barely moved; private banks hovered around 4.9–5.3%.",
"Chênh lệch Big 4 − tư nhân phản ánh lợi thế vốn rẻ của ngân hàng quốc doanh; biểu tại quầy thấp hơn lãi suất online.":"The Big 4 − private gap reflects state-owned banks' cheaper funding; counter rates are below online rates.",
"MBS: chuỗi gãy từ T3/26 (khảo sát thực tế), T12/25 suy từ báo cáo T1/26, T9/26 chưa công bố. NHNN: số cho vay BQ công bố không định kỳ, định nghĩa có thể khác nhau giữa các lần.":
 "MBS: series break from Mar-26 (actual survey), Dec-25 derived from the Jan-26 report, Sep-26 not yet published. SBV: average lending rates are announced irregularly and definitions may differ between announcements.",
"Nguồn: Vietcombank — biểu lãi suất tại quầy, cá nhân (đổi ~17/12/2025, ~23/3/2026, 13/4/2026; theo VietnamBiz, VnExpress, Thời báo Tài chính). MBS Research, Báo cáo Thị trường Tiền tệ — BQ lãi suất niêm yết 12T của 14–18 ngân hàng; T12/25 suy từ báo cáo T1/26; từ T3/26 MBS dùng số khảo sát thực tế nên không so sánh trực tiếp với 2025; T9/26 chưa công bố. NHNN — lãi suất cho vay BQ giao dịch mới tại các lần họp báo/báo cáo (điểm rời rạc; báo cáo 6T/2025 của NHNN nêu 6,85% tại 30/6/2025, khác số họp báo 6,38%). NHNN — OMO 4,0% → 4,5% từ 4/12/2025; tái cấp vốn 4,5% không đổi. Biểu tại quầy thấp hơn lãi suất online (VCB Digibank 12T ~6,8% T6/26).":
 "Sources: Vietcombank — counter deposit schedule for individuals (changed ~17 Dec 2025, ~23 Mar 2026, 13 Apr 2026; via VietnamBiz, VnExpress, Thoi bao Tai chinh). MBS Research, Money Market Report — average listed 12M rate of 14–18 banks; Dec-25 derived from the Jan-26 report; from Mar-26 MBS uses actual-survey data, so not directly comparable with 2025; Sep-26 not yet published. SBV — average lending rate on new transactions at press conferences/reports (sparse points; the SBV H1/2025 report gives 6.85% at 30 Jun 2025 vs 6.38% at the press conference). SBV — OMO 4.0% → 4.5% from 4 Dec 2025; refinancing unchanged at 4.5%. Counter rates are below online rates (VCB Digibank 12M ~6.8% in Jun-26).",
"toàn bộ CPI tổng và 11 nhóm T10/24–T9/26 lấy từ bảng số liệu tháng của Cục Thống kê (NSO, “Biểu 1 – Cả nước”); khoản mục con lấy từ bảng số liệu hoặc phần thuyết minh “Tổng quan CPI” khi NSO có công bố YoY. Khoản mục NSO không công bố hiển thị “—”. Trọng số khoản mục con là ước tính.":
 "headline CPI and all 11 groups for Oct-24–Sep-26 come from the NSO monthly data tables (“Biểu 1 – Cả nước”); sub-items come from those tables or the “Tổng quan CPI” commentary where NSO publishes a YoY figure. Items NSO does not publish show “—”. Sub-item weights are estimates.",
}
T={
"{0} người/km²":"{0} people/km²","hạng {0}/34":"rank {0}/34","Cơ cấu tuổi · {0}":"Age structure · {0}",
"mô hình · phụ thuộc {0} · già hoá {1}":"model · dependency {0} · ageing index {1}",
"TB 2025 (NSO, sơ bộ) · NQ 202: {0}":"2025 average (NSO, prelim.) · Res. 202: {0}",
"{0} tỉnh, thành phố":"{0} provinces & cities","Vùng {0} – {1}":"Region {0} – {1}",
"Vietcombank {0} (tại quầy)":"Vietcombank {0} (counter)","Lãi suất huy động & cho vay · {0}":"Deposit & lending rates · {0}",
"(số gần nhất {0}) — cao hơn VCB 12T {1}.":"(latest {0}) — {1} above VCB 12M.","OMO {0} · tái cấp vốn {1}.":"OMO {0} · refinancing {1}.",
"Biểu Vietcombank · {0}":"Vietcombank schedule · {0}","So với {0}":"vs {0}",
"người, bình quân {0}/năm.":"people, avg {0}/yr.","Tăng trưởng chậm lại: {0}/năm giai đoạn 2015–{1}.":"Growth slowing: {0}/yr over 2015–{1}.",
"; đỉnh ~{0} vào {1}.":"; peak ~{0} in {1}.",
"Thực tế {0}–{1}: NSO · Dự báo: LHQ WPP 2024 (phương án trung bình, dải thấp–cao) và NSO/UNFPA 2019–2069":"Actual {0}–{1}: NSO · Projections: UN WPP 2024 (medium variant, low–high band) and NSO/UNFPA 2019–2069",
"Cơ cấu GDP theo ngành · {0}":"GDP structure by sector · {0}",
"Chi tiết ngành lớn nhất: {0}":"Largest sector detail: {0}",
"{0} → {1} triệu":"{0} → {1} million",
}
d['exact'].update(E); d['templates'].update(T)
json.dump(d,open('en_extra.json','w'),ensure_ascii=False,indent=1)
print(len(E),len(T))
