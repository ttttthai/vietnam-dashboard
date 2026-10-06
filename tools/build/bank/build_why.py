#!/usr/bin/env python3
"""Build why_rates.json — why Vietnamese banks have not cut lending rates (2025–2026).
Every number carries a URL + article date. Interbank/FX series merged from agent outputs if present."""
import json, os

D = os.path.dirname(os.path.abspath(__file__))

U = dict(
    yuanta="https://mekongasean.vn/nim-ngan-hang-phuc-hoi-chung-khoan-yuanta-chi-ra-nhom-co-loi-the-ve-chi-phi-von-58898.html",
    tnck_cof="https://www.tinnhanhchungkhoan.vn/chi-phi-von-ngan-hang-vao-nhip-tang-moi-post390221.html",
    vdsc_q3="https://vnbusiness.vn/tien-gui-khong-ky-han-giam-nim-ngan-hang-tiep-tuc-chiu-suc-ep.html",
    tbtc_cof="https://thoibaotaichinhvietnam.vn/chi-phi-von-ngan-hang-ngay-cang-dat-do-kho-quay-lai-vung-thap-204130.html",
    congthuong_casa="https://congthuong.vn/casa-giam-dien-rong-ngan-hang-them-ap-luc-chi-phi-von-469327.html",
    ktsg_casa="https://tuoitre.vn/saigontimes/xu-huong-bien-dong-ty-le-casa-nam-2025/",
    vis_may="https://thoibaotaichinhvietnam.vn/vis-rating-lo-ngai-thanh-khoan-that-chat-no-xau-tang-khien-ngan-hang-them-ap-luc-198036.html",
    mbs_ldr="https://thoibaotaichinhvietnam.vn/chenh-lech-tin-dung-huy-dong-len-14-trieu-ty-dong-ap-luc-ldr-bot-cang-khi-tinh-20-tien-gui-kho-bac-197524.html",
    nsi="https://phapluat.suckhoedoisong.vn/tong-tai-san-he-thong-ngan-hang-dat-226-trieu-ty-dong-nim-chiu-suc-ep-trong-quy-ii2026-283986.html",
    fili_nim="https://fili.vn/2026/02/nim-ngan-hang-giam-tren-dien-rong-nam-2026-se-ra-sao-757-1402723.htm",
    dnse_nim="https://www.dnse.com.vn/senses/tin-tuc/nim-ngan-hang-tiem-can-ay-9-nam-ap-luc-chi-phi-von-chua-ha-nhiet-35195123",
    ncdt_nim="https://nhipcaudautu.vn/tai-chinh/vong-xoay-cua-nim-3365085/",
    fiin="https://vietnambiz.vn/tang-truong-tin-dung-vuot-xa-huy-dong-ap-luc-thanh-khoan-keo-dai-tu-2025-sang-2026-2026420152432400.htm",
    vnb_243="https://vietnambiz.vn/tinh-den-243-tang-truong-tin-dung-dat-215-trong-khi-huy-dong-von-chi-tang-044-2026449331850.htm",
    dnse_h2="https://www.dnse.com.vn/senses/tin-tuc/ngan-hang-nua-cuoi-2026-nong-chuyen-lai-suat-kho-bai-toan-loi-nhuan-35251787",
    vnn_0916="https://vietnamnet.vn/tin-dung-tang-nhanh-hon-huy-dong-lai-suat-ngan-hang-kho-giam-cuoi-nam-2554327.html",
    docnhanh_0903="https://docnhanh.vn/kinh-te/lai-suat-huy-dong-vuot-9-ap-luc-von-ngan-hang-tang-tintuc1052480",
    bdt_2025="https://baodauthau.vn/khoang-lech-lon-giua-huy-dong-von-va-tang-truong-tin-dung-post191801.html",
    ddn_146="https://diendandoanhnghiep.vn/tang-truong-tin-dung-2025-du-kien-19-he-so-tin-dung-tren-gdp-khoang-146-10168295.html",
    tt_2025="https://tuoitre.vn/tang-truong-tin-dung-nam-2025-hon-19-nhung-nam-2026-se-thap-hon-20260110193917581.htm",
    tttt_2024="https://thitruongtaichinhtiente.vn/ket-thuc-nam-2024-tin-dung-tang-15-08-65089.html",
    nqs_gov="https://nguoiquansat.vn/thong-doc-nhnn-noi-thang-thieu-von-lai-suat-buoc-phai-tang-304817.html",
    dff_dg="https://dff.vn/pho-thong-doc-nhnn-nganh-ngan-hang-se-chiu-suc-ep-lon-ve-cung-ung-von-cho-nen-kinh-te-p20260219001639773.html",
    nqs_qh="https://nguoiquansat.vn/pho-thong-doc-ngan-hang-nha-nuoc-bao-cao-voi-lanh-dao-quoc-hoi-ve-loat-van-de-trong-tam-nua-dau-nam-2026-302694.html",
    cafeland_dg="https://cafeland.vn/tin-tuc/pho-thong-doc-nhnn-thong-tin-ve-dinh-huong-dieu-hanh-chinh-sach-tien-te-cuoi-nam-2026-153856.html",
    hieu_0929="https://cafef.vn/ts-nguyen-tri-hieu-toi-khong-nhin-thay-du-dia-cho-cac-ngan-hang-giam-lai-suat-cho-vay-18826092911053628.chn",
    hieu_0423="https://nhadautu.vn/cslai-suat-nam-2026-the-kho-cua-cac-ngan-hang-thuong-mai-d104490.html",
    tbnh_1001="https://thoibaonganhang.vn/vi-sao-lai-suat-cho-vay-kho-giam-188325-188325.html",
    zn_0925="https://znews.vn/lai-suat-cho-vay-kho-dao-chieu-nam-nay-post1685436.html",
    mbs_dec25="https://thoibaotaichinhvietnam.vn/mbs-du-bao-ba-nhan-to-gay-suc-ep-day-lai-suat-huy-dong-tang-nam-2026-189573.html",
    nqs_shinhan="https://nguoiquansat.vn/mot-dieu-khien-nhieu-lanh-dao-ngan-hang-cung-lo-ngai-trong-nam-2026-298077.html",
    cafef_gtcg="https://cafef.vn/ap-luc-nguon-von-nhieu-ngan-hang-day-manh-phat-hanh-giay-to-co-gia-188260916154251385.chn",
    tckttc_bond="https://tapchikinhtetaichinh.vn/ngan-hang-bat-dong-san-chiem-91-luong-trai-phieu-phat-hanh-167100.html",
    vnx_17="https://vnexpress.net/lai-suat-lien-ngan-hang-vot-len-17-5014072.html",
    vnx_q1="https://vnexpress.net/lai-suat-lien-ngan-hang-bien-dong-manh-trong-3-thang-dau-nam-5057688.html",
    nqs_ib64="https://nguoiquansat.vn/cu-soc-lai-suat-qua-dem-17-he-lo-quy-mo-6-4-trieu-ty-dong-tren-thi-truong-2-cua-27-nhtm-niem-yet-293280.html",
    dantri_0930="https://dantri.com.vn/kinh-doanh/lai-suat-lien-ngan-hang-xuong-05-thap-nhat-hon-4-nam-20260930080335372.htm",
    omo45="https://vietnambiz.vn/lan-dau-sau-hon-14-thang-nhnn-nang-lai-suat-omo-len-45nam-20251258115523.htm",
    vs_fwd="https://vietstock.vn/2026/03/nhnn-tung-don-can-thiep-ty-gia-phat-tin-hieu-on-dinh-vnd-757-1416142.htm",
    vne_kb="https://vneconomy.vn/tu-18-noi-quy-dinh-tinh-tien-gui-kho-bac-nha-nuoc-ngan-hang-co-them-du-dia-cho-vay.htm",
    nqs_mof="https://nguoiquansat.vn/bo-tai-chinh-de-xuat-tang-tien-gui-kho-bac-tai-ngan-hang-bom-them-thanh-khoan-cho-nen-kinh-te-314054.html",
    nqs_fed="https://nguoiquansat.vn/fed-tang-lai-suat-chenh-lech-usd-vnd-thu-hep-lai-suat-viet-nam-se-ra-sao-317285.html",
    fed_cnbc="https://www.cnbc.com/2026/09/16/fed-rate-decision-september-2026.html",
    tttt_fx="https://thitruongtaichinhtiente.vn/ty-gia-va-lai-suat-cuoi-nam-2026-suc-ep-tu-ben-ngoai-va-la-chan-tu-noi-luc-85717.html",
    nqs_vnd="https://nguoiquansat.vn/vndirect-research-du-bao-dien-bien-ty-gia-lai-suat-cuoi-nam-2026-317361.html",
    vne_hong="https://vneconomy.vn/thong-doc-ngan-hang-nha-nuoc-lai-suat-cho-vay-kho-giam-them.htm",
    tttt_hong25="https://thitruongtaichinhtiente.vn/thong-doc-nguyen-thi-hong-chia-se-ve-viec-can-bang-lai-suat-tin-dung-truoc-ap-luc-lam-phat-va-ty-gia-69489.html",
    cpi_sep="https://tapchicongthuong.vn/9-thang-nam-2026--cpi-tang-4-52-lam-phat-co-ban-tang-4-26-554941.htm",
    tn_lend="https://thanhnien.vn/lai-suat-cho-vay-binh-quan-tien-dong-len-107-nam-185260921142331886.htm",
    vne_npl="https://vneconomy.vn/den-cuoi-thang-62026-no-xau-bat-dong-san-tang-105-so-cuoi-nam-ngoai.htm",
    mbs_3q26="https://www.mbs.com.vn/files/uploads/2026/09/DU-BAO-KQKD-3Q26_NGAN-HANG_20260924.pdf",
    tbtc_c25="https://thoibaotaichinhvietnam.vn/chinh-thuc-nang-ty-le-von-ngan-han-cho-vay-trung-dai-han-len-40-sau-nhieu-nam-siet-chat-199482.html",
    car_ndt="https://nhadautu.vn/lan-song-tang-von-ngan-hang-2026-cuoc-dua-dap-ung-basel-iii-va-bo-dem-an-toan-moi-d105019.html",
    pi_9m="https://baochinhphu.vn/giai-ngan-von-dau-tu-cong-9-thang-nam-2026-mot-so-bo-nganh-dia-phuong-con-cham-102261003011740764.htm",
    gdp_9m="https://vietnamnet.vn/gdp-9-thang-tang-9-01-cpi-tang-4-52-nen-kinh-te-them-dong-luc-cuoi-nam-2561344.html",
    tckttc_h2="https://tapchikinhtetaichinh.vn/lai-suat-ngan-hang-kho-giam-manh-trong-nua-cuoi-nam-2026-162438.html",
    sme_7125="https://bnews.vn/ngan-hang-nha-nuoc-chi-dao-giam-toi-thieu-1-lai-suat-cho-vay-voi-doanh-nghiep-nho-va-vua/432172.html",
    sbv_letters="https://danviet.vn/ngan-hang-nha-nuoc-thong-tin-nong-ve-mat-bang-lai-suat-nam-2026-d1428978.html",
    l2342="https://znews.vn/nhnn-yeu-cau-giam-it-nhat-1-lai-vay-cho-doanh-nghiep-nho-va-vua-post1676151.html",
    tt50="https://vietnambiz.vn/nhnn-nang-tran-ty-le-cho-vay-tren-tien-gui-len-95-cong-bo-hai-chi-tieu-quan-ly-thanh-khoan-lcr-va-nsfr-2026101144912870.htm",
    acbs_cdr="https://www.tinnhanhchungkhoan.vn/ty-le-cdr-moi-theo-du-thao-sua-doi-thong-tu-22-acbs-du-bao-nhieu-nha-bang-co-the-vuot-tran-85-post390225.html",
    q1_cafebiz="https://cafebiz.vn/tin-dung-dang-chay-nhanh-hon-huy-dong-von-176260512075343822.chn",
)


def m(name, value, unit, date, url, note=None):
    d = {"name": name, "value": value, "unit": unit, "date": date, "url": url}
    if note:
        d["note"] = note
    return d


def q(who, text_vi, date, url, text_en=None):
    d = {"who": who, "text_vi": text_vi, "date": date, "url": url}
    if text_en:
        d["text_en"] = text_en
    return d


drivers = [
 {
  "id": "credit_deposit_gap",
  "rank": 1,
  "title_vi": "Tín dụng tăng nhanh hơn huy động → LDR cao, cuộc đua lãi suất huy động",
  "title_en": "Credit outpacing deposits → high LDR and a deposit-rate race",
  "direction": "up_pressure",
  "explain_vi": "Năm 2025 tín dụng tăng ~19% trong khi huy động chỉ ~14%; sang 2026 khoảng lệch tiếp tục mở rộng, buộc ngân hàng tranh giành tiền gửi bằng lãi suất cao. Khi chi phí đầu vào tăng, lãi suất cho vay không thể giảm.",
  "explain_en": "Credit grew ~19% in 2025 versus ~14% for deposits, and the gap kept widening in 2026, forcing banks to bid up deposit rates. With input costs rising, lending rates cannot fall.",
  "metrics": [
   m("Tăng trưởng tín dụng 2025 (toàn hệ thống)", 19.01, "% YTD", "2026-01-10", U["tt_2025"]),
   m("Tăng trưởng huy động đến 24/12/2025", 14.11, "% YTD", "2026-01-06", U["bdt_2025"], "same date credit +17.87%"),
   m("Tín dụng vs huy động (FiinRatings) 2025", "~19 vs ~11.4", "% YoY", "2026-04-21", U["fiin"], "FiinRatings basis (customer deposits)"),
   m("Tín dụng 2026 YTD đến 24/3", 2.15, "% YTD", "2026-04-09", U["vnb_243"], "huy động chỉ +0.44% cùng kỳ"),
   m("Tín dụng 2026 YTD đến 15/6", 6.38, "% YTD", "2026-07-28", U["dnse_h2"], "huy động +4.3%"),
   m("Tín dụng 2026 YTD đến 3/9", 9.98, "% YTD", "2026-09-16", U["vnn_0916"], "huy động +8.33% (BVSC); cũng ở docnhanh"),
   m("Khoảng cách tín dụng – huy động (MBS)", 1.4, "triệu tỷ đồng (7.22% dư nợ)", "2026-05-18", U["mbs_ldr"], "tháng 4/2026"),
   m("LDR đơn giản (cho vay/tiền gửi KH) toàn ngành Q2/2026 (Yuanta)", 114, "%", "2026-08-25", U["yuanta"], "Q2/2025: 108%"),
   m("LDR theo quy định (NSI) Q2/2026", 88, "%", "2026-08-23", U["nsi"], "trần quy định 85% (TT22) cho từng NH; có điều chỉnh tiền gửi KBNN"),
   m("Q1/2026: dư nợ 27 NH +~4% vs tiền gửi +~0.6%", 4.0, "% QTD", "2026-05-12", U["q1_cafebiz"]),
  ],
  "quotes": [
   q("Phạm Đức Ấn, Thống đốc NHNN", "Chúng ta tập trung vào tăng trưởng kinh tế, tập trung vào tín dụng, vì vậy khi thiếu hụt vốn thì đương nhiên lãi suất nâng cao. Lãi suất huy động đầu vào tăng thì lãi suất đầu ra cũng tăng, tác động trực tiếp đến doanh nghiệp", "2026-07-18", U["nqs_gov"]),
   q("Phạm Thanh Hà, Phó Thống đốc NHNN", "Ngành ngân hàng sẽ chịu sức ép lớn về cung ứng vốn cho nền kinh tế (huy động tăng chậm hơn tín dụng, nền kinh tế phụ thuộc lớn vào vốn ngân hàng) — tóm lược", "2026-02-19", U["dff_dg"]),
   q("BVSC", "Tăng trưởng huy động thấp hơn tăng trưởng tín dụng đang tạo áp lực lên nguồn vốn của các ngân hàng", "2026-09-16", U["vnn_0916"]),
   q("TS. Cấn Văn Lực, KTS trưởng BIDV", "Tín dụng tăng nhanh buộc ngân hàng đẩy mạnh huy động, trong khi người dân có nhiều kênh đầu tư khác (vàng, chứng khoán, BĐS, tài sản số) nên lãi suất huy động phải đủ hấp dẫn — tóm lược", "2026-10-01", U["tbnh_1001"]),
  ],
 },
 {
  "id": "cost_of_funds",
  "rank": 2,
  "title_vi": "Chi phí vốn tăng mạnh, CASA giảm, phải phát hành GTCG lãi 8–10%",
  "title_en": "Surging cost of funds, falling CASA, costly 8–10% paper issuance",
  "direction": "up_pressure",
  "explain_vi": "COF toàn ngành lên 4,79% (Q2/2026, +105 bps svck), cao nhất từ giữa 2023; lãi huy động 12T nhóm NH tư nhân tăng từ 5,82% (12/2025) lên ~8,6% (7/2026). Tiền gửi không kỳ hạn chuyển sang có kỳ hạn và ngân hàng phải phát hành GTCG/trái phiếu lãi 8–10%, nên lãi vay mới phải tăng để giữ NIM.",
  "explain_en": "Sector COF hit 4.79% in Q2-2026 (+105 bps YoY), the highest since mid-2023; private banks' 12M deposit rate rose from 5.82% (Dec-2025) to ~8.6% (Jul-2026). CASA migrated to term deposits and banks issued paper at 8–10%, so new-loan rates had to rise to protect margins.",
  "metrics": [
   m("COF toàn ngành Q2/2026 (Yuanta)", 4.79, "%", "2026-08-25", U["yuanta"], "+1.05 điểm % svck"),
   m("COF Q1/2026 vs Q4/2025", 4.2, "% (Q4/25: 3.88%)", "2026-05-14", U["tnck_cof"]),
   m("Chi phí lãi 27 NH niêm yết H1/2026", 437418, "tỷ đồng (+49% svck)", "2026-09-26", U["tbtc_cof"], "thu nhập lãi chỉ +33.6%"),
   m("LS huy động 12T bình quân nhóm NH tư nhân giữa 12/2025 (MBS)", 5.82, "%", "2025-12-26", U["mbs_dec25"]),
   m("LS huy động 12T bình quân cuối 7/2026 (MBS)", 8.63, "% (+2.82 điểm % YTD)", "2026-08-17", U["congthuong_casa"]),
   m("CASA toàn ngành Q2/2026 (Yuanta)", 20.9, "% (-1.21 điểm % svck)", "2026-08-25", U["yuanta"]),
   m("CASA bình quân giản đơn 27 NH 30/6/2026", 14.7, "% (đầu năm 15.4%)", "2026-08-17", U["congthuong_casa"]),
   m("Lãi suất phát hành GTCG", "8–10", "%/năm (vs COF bình quân 4.8%)", "2026-09-16", U["cafef_gtcg"], "TCBS: GTCG 1.96 triệu tỷ, +16.7% YTD, 9.8% nguồn vốn"),
   m("Lãi suất danh nghĩa TPDN ngân hàng T8/2026 (VBMA)", 8.83, "%", "2026-09-16", U["tckttc_bond"]),
   m("NIM toàn ngành Q2/2026 (Yuanta)", 3.15, "% (-0.1 điểm % svck)", "2026-08-25", U["yuanta"]),
  ],
  "quotes": [
   q("TS. Nguyễn Trí Hiếu", "Các ngân hàng đang cần vốn, đang huy động vốn với lãi suất cao, chính vì thế làm sao họ có thể cho vay với lãi suất hạ được?", "2026-09-29", U["hieu_0929"]),
   q("TCBS Research", "Chi phí huy động khó hạ nhanh vì tiền gửi vẫn tăng chậm hơn tín dụng", "2026-09-26", U["tbtc_cof"]),
   q("Đại diện VPBank", "Chi phí hoán đổi hiện nay chạy ở khoảng 3 - 4%", "2026-09-26", U["tbtc_cof"]),
   q("MBS Research", "Mặt bằng NIM toàn ngành trong Q3/26 vẫn chịu áp lực từ chi phí vốn neo cao, đặc biệt tại nhóm NHTMCP có TTTD nhanh hoặc phụ thuộc nhiều hơn vào huy động kỳ hạn", "2026-09-24", U["mbs_3q26"]),
  ],
 },
 {
  "id": "liquidity_interbank",
  "rank": 3,
  "title_vi": "Thanh khoản căng: lãi suất liên ngân hàng cao/biến động, NHNN không hạ lãi suất điều hành",
  "title_en": "Tight liquidity: high/volatile interbank rates; SBV not cutting policy rates",
  "direction": "up_pressure",
  "explain_vi": "Lãi suất qua đêm lập kỷ lục 17% (3/2/2026), nhiều phiên >10% trong T3 và ~11% đầu T6; NHNN nâng lãi suất OMO từ 4,0% lên 4,5% (4/12/2025) và hút ròng mạnh trong T3/2026 khi tỷ giá căng. Ngân hàng nhỏ phụ thuộc vốn liên ngân hàng ngắn hạn đắt đỏ, nên giữ lãi vay cao.",
  "explain_en": "Overnight interbank hit a record 17% (3-Feb-2026), several sessions >10% in March and ~11% in early June; SBV lifted the OMO rate 4.0→4.5% (4-Dec-2025) and drained liquidity in March-2026 under FX stress. Smaller banks relying on costly short interbank funds kept loan rates high.",
  "metrics": [
   m("LS qua đêm liên NH đỉnh", 17.0, "%/năm", "2026-02-04", U["vnx_17"], "phiên 3/2/2026; 1 tuần 15%"),
   m("LS qua đêm 3/3/2026", 10.93, "%/năm", "2026-04-07", U["vnx_q1"]),
   m("LS qua đêm 30/9/2026 (sau 0.2% ngày 29/9)", 7.0, "%/năm", "2026-10-03", U["dantri_0930"], "biến động cực mạnh cuối quý"),
   m("LS OMO (cầm cố GTCG)", 4.5, "%/năm (từ 4.0%)", "2025-12-05", U["omo45"], "nâng ngày 4/12/2025, lần đầu sau 14 tháng"),
   m("Dư nợ cho vay cầm cố OMO kỷ lục", 441200, "tỷ đồng", "2026-02-04", U["vnx_17"]),
   m("NHNN hút ròng qua OMO 3–23/3/2026", 187539, "tỷ đồng", "2026-03-25", U["vs_fwd"]),
   m("Nguồn vốn liên NH mở rộng/tổng tài sản 27 NH Q1/2026", 29.6, "%", "2026-05-22", U["nqs_ib64"]),
   m("Tiền gửi KBNN tại TCTD Q1/2026", 626.7, "nghìn tỷ đồng (624.2 tại 4 NHTM NN)", "2026-07-31", U["vne_kb"]),
   m("Tỷ lệ tiền gửi có kỳ hạn KBNN được tính vào nguồn vốn (từ 1/8/2026)", 50, "% (trước đó 20%)", "2026-07-31", U["vne_kb"], "QĐ 1743/QĐ-NHNN, hiệu lực đến 31/7/2028"),
  ],
  "quotes": [
   q("Nguyễn Hoàn Niên, Shinhan Securities", "Lãi suất vay mượn giữa các ngân hàng phản ánh áp lực thanh khoản nhưng chưa chắc kéo lãi suất tiền gửi tăng theo", "2026-04-07", U["vnx_q1"]),
   q("TS. Nguyễn Trí Hiếu", "Nếu thanh khoản không ổn định, ngân hàng sẽ không có dư địa cũng như ý chí để cho vay hiệu quả", "2026-04-23", U["hieu_0423"]),
   q("Trần Quốc Phương, Thứ trưởng Bộ Tài chính", "Bộ Tài chính đề xuất nâng giới hạn tiền gửi có kỳ hạn của Kho bạc tại NHTM để hỗ trợ thanh khoản — tóm lược", "2026-09-03", U["nqs_mof"]),
  ],
 },
 {
  "id": "fx_fed",
  "rank": 4,
  "title_vi": "Áp lực tỷ giá & chênh lệch lãi suất USD–VND (Fed tăng lãi suất)",
  "title_en": "Exchange-rate pressure & USD–VND rate differential (Fed hiking)",
  "direction": "limits_cut",
  "explain_vi": "Hạ lãi suất VND sẽ thu hẹp chênh lệch với USD và đẩy tỷ giá lên; VND mất giá ~3,2% năm 2025, giá bán USD chạm trần biên độ ±5% trong T3 và T7/2026, dự trữ chỉ ~1,85 tháng nhập khẩu nên NHNN giữ lãi suất liên ngân hàng cao và bán kỳ hạn. Fed đảo chiều tăng lên 3,75–4% (16/9/2026) càng hạn chế dư địa nới lỏng, dù tỷ giá thị trường đã dịu từ T8/2026.",
  "explain_en": "Cutting VND rates would narrow the USD differential and lift USD/VND; the dong lost ~3.2% in 2025, bank USD quotes hit the ±5% band ceiling in Mar and Jul-2026, and reserves cover only ~1.85 months of imports, so the SBV kept interbank rates high and sold forwards. The Fed's hike to 3.75–4% (16-Sep-2026) further limits easing room, even though market FX pressure eased from Aug-2026.",
  "metrics": [
   m("Fed funds target (sau 16/9/2026)", "3.75–4.00", "%", "2026-09-16", "https://www.federalreserve.gov/newsevents/pressreleases/monetary20260916a.htm", "lần tăng đầu tiên kể từ 2023; trước đó giữ 3.50–3.75% suốt 5 kỳ họp đầu 2026"),
   m("Tỷ giá trung tâm 2025", 3.2, "% tăng (lên 25,121)", "2026-01-01", "https://thanhnien.vn/gia-usd-hom-nay-112026-tang-32-trong-nam-2025-185260101083650774.htm", "VND mất giá ~3.2% tại NHTM"),
   m("Hợp đồng bán USD kỳ hạn 180 ngày của NHNN (24/3/2026)", 26850, "VND/USD (giao ngay ~26,351, sát trần 26,364)", "2026-03-25", U["vs_fwd"], "2.69 tỷ USD bán trong ngày (bbw.vn); 2025: 3 đợt ở 26,550"),
   m("Giá bán USD tại NHTM sát trần (27/7/2026)", 26500, "VND/USD (> mức, trần 26,523)", "2026-08-03", "https://vietstock.vn/2026/08/cau-truc-nghia-vu-ngoai-te-nua-cuoi-2026-va-gioi-han-du-dia-dieu-hanh-ty-gia-757-1475665.htm"),
   m("Dự trữ ngoại hối", 87.6, "tỷ USD ≈ 1.85 tháng nhập khẩu (IMF khuyến nghị 3–3.5)", "2026-08-03", "https://vietstock.vn/2026/08/cau-truc-nghia-vu-ngoai-te-nua-cuoi-2026-va-gioi-han-du-dia-dieu-hanh-ty-gia-757-1475665.htm"),
   m("Thâm hụt thương mại 7T/2026", 20.5, "tỷ USD (cùng kỳ thặng dư 10.4)", "2026-09-22", U["tttt_fx"]),
   m("USD/VND liên NH so với đầu năm (29/9/2026)", -1.7, "% (VND tăng giá)", "2026-09-30", "https://thanhnien.vn/gia-vang-usd-lai-suat-cung-ha-185260929225520444.htm", "áp lực dịu từ T8–9 nhờ FDI 17.25 tỷ USD 8T; nhưng tỷ giá trung tâm +2.0% YTD"),
  ],
  "quotes": [
   q("Nguyễn Thị Hồng, Thống đốc NHNN (khi đó)", "Nếu Ngân hàng Nhà nước giảm lãi suất thì tỷ giá lại tăng", "2024-11-11", U["vne_hong"]),
   q("ACBS (qua Người Quan Sát)", "Ràng buộc chính sách bất đối xứng: tăng trưởng trong nước không cần tăng lãi suất, nhưng chênh lệch lãi suất hạn chế dư địa nới lỏng — tóm lược", "2026-09-17", U["nqs_fed"]),
   q("Vũ Bình Minh, HSBC (qua TC Kinh tế Tài chính)", "Nhu cầu vốn của nền kinh tế gia tăng trong khi Việt Nam phải duy trì chênh lệch lãi suất VND–USD phù hợp — tóm lược", "2026-07-22", U["tckttc_h2"]),
  ],
 },
 {
  "id": "inflation_real_rates",
  "rank": 5,
  "title_vi": "Lạm phát ~5% → lãi suất thực, giới hạn hạ lãi suất huy động",
  "title_en": "~5% inflation → real-rate floor on deposit rates",
  "direction": "limits_cut",
  "explain_vi": "CPI T9/2026 tăng 5,08% svck, bình quân 9 tháng 4,52% — sát/vượt mục tiêu ~4,5%; NHNN không thể nới lỏng và ngân hàng phải giữ lãi suất huy động dương thực để giữ tiền gửi (vàng, BĐS, chứng khoán cạnh tranh).",
  "explain_en": "Sep-2026 CPI was +5.08% YoY (9M avg 4.52%), at/above the ~4.5% target; the SBV cannot ease and banks must keep positive real deposit rates to retain savers who can switch to gold, property or stocks.",
  "metrics": [
   m("CPI T9/2026 svck", 5.08, "%", "2026-10-03", U["cpi_sep"], "Cục Thống kê"),
   m("CPI bình quân 9T/2026", 4.52, "%", "2026-10-03", U["cpi_sep"]),
   m("Lạm phát cơ bản bình quân 9T/2026", 4.26, "%", "2026-10-03", U["cpi_sep"]),
   m("Lãi suất thực 12T (ước tính = 8.63% − 5.08%)", 3.55, "điểm %", "2026-10-05", U["congthuong_casa"], "derived: MBS 12M avg end-Jul-2026 minus CPI Sep-2026 YoY"),
  ],
  "quotes": [
   q("Nguyễn Thị Hồng, Thống đốc NHNN (khi đó)", "Lạm phát khi xuất hiện thì rất nhanh nhưng để kiểm soát giảm lại thì rất khó", "2025-08-07", U["tttt_hong25"]),
   q("Phân tích Techcombank (qua Thời báo Ngân hàng)", "Dư địa giảm lãi suất huy động đã thu hẹp khi áp lực lạm phát quay lại và nhu cầu vốn cho hạ tầng, dự án lớn còn cao — tóm lược", "2026-10-01", U["tbnh_1001"]),
   q("VNDirect (qua Znews)", "Dời kỳ vọng hạ lãi suất từ Q4/2026 sang đầu 2027 do lạm phát và môi trường lãi suất quốc tế — tóm lược", "2026-09-25", U["zn_0925"]),
  ],
 },
 {
  "id": "credit_risk_npl",
  "rank": 6,
  "title_vi": "Nợ xấu & chi phí dự phòng → phần bù rủi ro tín dụng",
  "title_en": "NPLs & provisioning → credit-risk premium",
  "direction": "limits_cut",
  "explain_vi": "Tỷ lệ nợ xấu nội bảng toàn hệ thống 3,31% (6/2026), giá trị tuyệt đối vẫn tăng; nợ xấu BĐS +10,5% so cuối 2025. Ngân hàng tư nhân tăng trích lập mạnh (dự báo +28,8% svck Q3/26), nên phải giữ biên lãi vay để bù rủi ro.",
  "explain_en": "System on-balance NPL ratio was 3.31% (Jun-2026) with absolute NPLs still rising; real-estate NPLs +10.5% vs end-2025. Private banks are provisioning heavily (MBS: +28.8% YoY in Q3-26), so lending spreads stay wide to price risk.",
  "metrics": [
   m("Nợ xấu nội bảng toàn hệ thống cuối 6/2026", 3.31, "% (cuối 2025: 3.44%)", "2026-08-13", U["vne_npl"], "loại 5 NH kiểm soát đặc biệt: 1.51%"),
   m("Nợ xấu rộng (gồm VAMC, tiềm ẩn)", 3.77, "% (cuối 2025: 3.97%)", "2026-08-13", U["vne_npl"]),
   m("Nợ xấu BĐS so cuối 2025", 10.5, "% tăng", "2026-08-13", U["vne_npl"]),
   m("Chi phí trích lập nhóm NHTMCP tư nhân Q3/26 (MBS dự báo)", 28.8, "% svck", "2026-09-24", U["mbs_3q26"], "nhóm quốc doanh -18.5%"),
   m("Tỷ lệ nợ có vấn đề (VIS Rating) Q1/2026", 2.2, "% (+11 bps)", "2026-05-27", U["vis_may"]),
  ],
  "quotes": [
   q("VIS Rating", "Chúng tôi kỳ vọng tỷ lệ hình thành nợ xấu sẽ gia tăng trong năm 2026, đặc biệt ở các ngân hàng quy mô nhỏ", "2026-05-27", U["vis_may"]),
   q("Nguyễn Quang Huy, ĐH Nguyễn Trãi", "Với ngân hàng, lãi suất cho vay phải bù đắp chi phí huy động, vận hành, dự phòng rủi ro và bảo đảm an toàn — tóm lược", "2026-10-01", U["tbnh_1001"]),
  ],
 },
 {
  "id": "prudential_ratios",
  "rank": 7,
  "title_vi": "Ràng buộc an toàn: LDR 85%, vốn ngắn hạn cho vay trung dài hạn, CAR/Basel III",
  "title_en": "Prudential limits: 85% LDR, short-for-long funding cap, CAR/Basel III",
  "direction": "limits_cut",
  "explain_vi": "Trần LDR 85% và trần 30% vốn ngắn hạn cho vay trung dài hạn (đến 30/6/2026) buộc ngân hàng tìm vốn dài hạn đắt hơn; 80% tiền gửi là ngắn hạn trong khi ~50% tín dụng là trung dài hạn. Lộ trình Basel III (TT 14/2025) đòi hỏi tăng vốn, hạn chế giảm lãi suất.",
  "explain_en": "The 85% LDR cap and 30% short-for-long funding cap (until 30-Jun-2026) push banks to costlier long-term funding; 80% of deposits are short-term while ~50% of credit is medium/long-term. The Basel III roadmap (Circular 14/2025) requires more capital.",
  "metrics": [
   m("Trần vốn ngắn hạn cho vay trung dài hạn", 40, "% (từ 30%, hiệu lực 1/7/2026)", "2026-06-22", U["tbtc_c25"], "TT 25/2026/TT-NHNN; 30% áp dụng từ 1/10/2023"),
   m("Tỷ trọng vốn huy động ngắn hạn", 80, "%", "2026-07-18", U["nqs_gov"], "Thống đốc Phạm Đức Ấn"),
   m("Tỷ trọng cho vay trung dài hạn trong dư nợ (Yuanta)", 33.1, "% Q2/2026 (Q2/25: 30.4%)", "2026-08-25", U["yuanta"]),
   m("TT 14/2025/TT-NHNN (Basel III): CET1 tối thiểu", 4.5, "% (Tier1 ≥6%, thêm bộ đệm bảo toàn vốn; hiệu lực 15/9/2025)", "2025-06-30", "https://thuvienphapluat.vn/van-ban/Tien-te-Ngan-hang/Thong-tu-14-2025-TT-NHNN-ty-le-an-toan-von-doi-voi-ngan-hang-thuong-mai-632909.aspx", "chia cổ tức tiền mặt gắn với đáp ứng bộ đệm; làn sóng tăng vốn 2026 (Nhà Đầu Tư)"),
   m("Trần LDR theo TT 50/2026 (ban hành 30/9/2026)", 95, "% (từ 85%, hiệu lực 1/12/2026)", "2026-10-01", U["tt50"], "loại trừ cho vay liên NH; trừ 100% tiền gửi KKH và 80% có kỳ hạn của KBNN; LCR/NSFR từ 10/2028 — đến 30/11/2026 trần 85% vẫn ràng buộc"),
  ],
  "quotes": [
   q("Phạm Chí Quang, Vụ trưởng Vụ CSTT NHNN", "80% tiền gửi là ngắn hạn trong khi khoảng 50% tín dụng là trung dài hạn, gây áp lực thanh khoản — tóm lược", "2026-01-06", U["bdt_2025"]),
   q("Phạm Thanh Hà, Phó Thống đốc NHNN", "Điều hành chính sách tiền tệ theo hướng chủ động, linh hoạt", "2026-08-04", U["cafeland_dg"]),
  ],
 },
 {
  "id": "public_investment_demand",
  "rank": 8,
  "title_vi": "Nhu cầu vốn lớn: đầu tư công, dự án hạ tầng, mục tiêu tăng trưởng 2 con số",
  "title_en": "Strong funding demand: public investment, megaprojects, double-digit growth target",
  "direction": "up_pressure",
  "explain_vi": "Giải ngân đầu tư công 9T/2026 ~643 nghìn tỷ (62,9% kế hoạch 1,02 triệu tỷ), GDP 9T +9,01%, mục tiêu ≥10%; các dự án lớn (APEC 2027, hạ tầng) cần vốn đối ứng từ ngân hàng, giữ cầu tín dụng cao nên ngân hàng không chịu áp lực giảm giá vốn.",
  "explain_en": "Public-investment disbursement reached ~VND643tn in 9M-2026 (62.9% of the VND1.02 quadrillion plan), 9M GDP +9.01% and a ≥10% target; megaprojects (APEC 2027, infrastructure) need bank co-financing, keeping loan demand strong.",
  "metrics": [
   m("Giải ngân đầu tư công đến 30/9/2026", 642961.6, "tỷ đồng (62.9% KH)", "2026-10-03", U["pi_9m"]),
   m("Kế hoạch vốn ĐTC 2026 giao", 1022607, "tỷ đồng", "2026-10-03", U["pi_9m"]),
   m("GDP 9T/2026", 9.01, "% svck (Q3: 9.95%)", "2026-10 (đầu tháng)", U["gdp_9m"]),
  ],
  "quotes": [
   q("MBS (qua TC Kinh tế Tài chính)", "Nhu cầu tín dụng được dự báo sẽ tăng trở lại khi hàng loạt dự án hạ tầng quy mô lớn được triển khai", "2026-07-22", U["tckttc_h2"]),
  ],
 },
 {
  "id": "bond_market_bank_dependence",
  "rank": 9,
  "title_vi": "Kênh trái phiếu yếu, nền kinh tế phụ thuộc vốn ngân hàng (tín dụng/GDP ~146%)",
  "title_en": "Weak bond market, bank-dominated financing (credit/GDP ~146%)",
  "direction": "up_pressure",
  "explain_vi": "Tín dụng/GDP ~146% cuối 2025 — cao nhất nhóm thu nhập trung bình thấp; 8T/2026 phát hành TPDN 382,8 nghìn tỷ nhưng 91% là ngân hàng và BĐS (BĐS lãi ~11%), nên doanh nghiệp vẫn dồn về vay ngân hàng, giữ cầu vốn và lãi suất cao.",
  "explain_en": "Credit/GDP reached ~146% at end-2025, the highest among lower-middle-income peers; 8M-2026 corporate bond issuance of VND382.8tn was 91% banks and real estate (RE coupons ~11%), so firms still rely on bank loans.",
  "metrics": [
   m("Tín dụng/GDP 2025 (ước)", 146, "%", "2025-12-30", U["ddn_146"], "Phạm Chí Quang"),
   m("Phát hành TPDN 8T/2026", 382800, "tỷ đồng", "2026-09-16", U["tckttc_bond"]),
   m("Tỷ trọng ngân hàng + BĐS trong phát hành TPDN 8T/2026", 90.9, "%", "2026-09-16", U["tckttc_bond"], "NH 50.6%, BĐS 40.3%"),
   m("Lãi suất TPDN BĐS bình quân T8/2026", 10.99, "%", "2026-09-16", U["tckttc_bond"]),
  ],
  "quotes": [
   q("Phạm Chí Quang, Vụ trưởng Vụ CSTT NHNN", "Hệ số tín dụng trên GDP dự kiến đạt khoảng 146%, mức cao nhất trong các nước có thu nhập trung bình thấp", "2025-12-30", U["ddn_146"]),
  ],
 },
 {
  "id": "policy_offsets",
  "rank": 10,
  "title_vi": "Chỉ đạo hành chính của NHNN chỉ tác động hạn chế",
  "title_en": "SBV moral suasion has had limited effect",
  "direction": "limits_cut",
  "explain_vi": "NHNN liên tục yêu cầu giảm lãi suất (CV 2342 ngày 30/3, 3972 ngày 14/5, 4190 ngày 21/5, 7125/NHNN-TD tháng 8: cho vay SME thấp hơn ≥1 điểm %), nới LDR/KBNN và trần 40%, nhưng lãi suất cho vay bình quân T8/2026 vẫn 8,4–10,7%, cao hơn cuối 2025 ~1,8 điểm % — vì nguyên nhân là chi phí vốn và thanh khoản chứ không phải ý chí ngân hàng.",
  "explain_en": "The SBV repeatedly instructed banks to cut (letters 2342 on 30-Mar, 3972 on 14-May, 4190 on 21-May, 7125/NHNN-TD in Aug: SME loans ≥1pp below average) and relaxed LDR/Treasury and the 40% cap, yet Aug-2026 average lending rates were still 8.4–10.7%, ~1.8pp above end-2025, because the binding constraint is funding cost and liquidity.",
  "metrics": [
   m("LS cho vay bình quân VND (mới + hiện hữu) T8/2026", "8.4–10.7", "%/năm (+1.8 điểm % so cuối 2025)", "2026-09-21", U["tn_lend"]),
   m("CV 7125/NHNN-TD: LS ưu đãi SME thấp hơn bình quân", 1, "điểm % tối thiểu, từ T8/2026", "2026-08-10", U["sme_7125"]),
   m("CV 2342/NHNN-CSTT (30/3/2026) yêu cầu ổn định mặt bằng lãi suất", 2342, "số hiệu", "2026-03-31", "https://diendandoanhnghiep.vn/ngan-hang-nha-nuoc-yeu-cau-cac-tctd-can-doi-nguon-von-on-dinh-mat-bang-lai-suat-10175404.html", "ngày bài báo ước theo ngày ban hành; xác nhận qua tìm kiếm"),
   m("CV 3972 (14/5) và 4190/NHNN-CSTT (21/5)", 4190, "số hiệu", "2026-05-23", U["sbv_letters"]),
  ],
  "quotes": [
   q("NHNN", "Yêu cầu tổ chức tín dụng thực hiện nghiêm chỉ đạo giảm mặt bằng lãi suất", "2026-05-23", U["sbv_letters"]),
   q("TS. Nguyễn Trí Hiếu", "Mặc dù lãi suất huy động tại một số ngân hàng có dấu hiệu giảm theo chỉ đạo, nhưng lãi suất cho vay vẫn đứng yên hoặc thậm chí có xu hướng tăng", "2026-04-23", U["hieu_0423"]),
  ],
 },
]

series = {
 "cof": {
  "periods": ["2025-Q2", "2025-Q3", "2025-Q4", "2026-Q1", "2026-Q2"],
  "values": [3.74, 3.9, 3.88, 4.2, 4.79],
  "unit": "%",
  "src": [U["yuanta"] + " (derived 4.79-1.05)", U["vdsc_q3"], U["tnck_cof"], U["tnck_cof"], U["yuanta"]],
  "note": "Listed-bank sector COF; mixed providers (Yuanta, VDSC, TNCK/CFA) - methodologies differ slightly; Q2-2025 derived from Yuanta YoY change.",
 },
 "nim": {
  "periods": ["2024-FY", "2025-Q3", "2025-Q4", "2025-FY", "2026-Q1", "2026-Q2"],
  "values": [3.26, 3.0, 3.04, 2.93, 2.87, 3.15],
  "unit": "%",
  "src": [U["fili_nim"], U["vdsc_q3"], U["dnse_nim"], U["fili_nim"], U["ncdt_nim"], U["yuanta"]],
  "note": "Listed-bank averages (27-28 banks); FY from VietstockFinance; Q3-25 VDSC; Q4-25 vietnambiz; Q1-26 Nhip Cau Dau Tu (28 banks); Q2-26 Yuanta (27 banks, not strictly comparable). MBS forecasts Q3-26 NIM flat-to-lower at private banks.",
 },
 "casa": {
  "periods": ["2024-Q4", "2025-Q2", "2025-Q4", "2026-Q1", "2026-Q2"],
  "values": [20.79, 22.11, 22.06, 18.0, 20.9],
  "unit": "% of customer deposits",
  "src": [U["ktsg_casa"], U["yuanta"] + " (derived 20.9+1.21)", U["ktsg_casa"], U["vis_may"], U["yuanta"]],
  "note": "Industry weighted CASA; Q1-26 from VIS Rating (different coverage, down 2pp); simple avg of 27 banks at 30-Jun-2026 = 14.7% (Wigroup via Cong Thuong).",
 },
 "ldr": {
  "periods": ["2025-Q2", "2025-Q4", "2026-Q1", "2026-Q2"],
  "values": [108, 110, 100, 114],
  "unit": "% loans/customer deposits",
  "src": [U["yuanta"], U["fili_nim"], U["mbs_ldr"], U["yuanta"]],
  "note": "Simple LDR. 2025-Q4 '~110%, highest in a decade' (VinaCapital); 2026-Q1 = 27 listed banks (MBS, narrower definition, +2.3pp vs end-2025). Regulatory LDR (Circular 22 basis) ~88% in Q2-2026 per NSI vs 85% cap.",
 },
 "credit_vs_deposit_growth": {
  "dates": ["2024-12-31", "2025-12-24", "2025-12-31", "2026-03-24", "2026-06-15", "2026-08-22", "2026-09-03"],
  "credit": [15.08, 17.87, 19.01, 2.15, 6.38, 8.38, 9.98],
  "deposit": [None, 14.11, None, 0.44, 4.3, 8.77, 8.33],
  "unit": "% YTD",
  "src": [U["tttt_2024"], U["bdt_2025"], U["tt_2025"], U["vnb_243"], U["dnse_h2"], U["vnn_0916"], U["docnhanh_0903"]],
  "note": "SBV system data as quoted in press. 22-Aug-2026 figures are VND-only (MoF Deputy Minister Tran Quoc Phuong; deposits briefly exceeded credit). 2024 system deposit growth not found (M2 +9.42% to 25-Dec-2024 is not a deposit measure).",
 },
 "deposit12m_private_banks_mbs": {
  "dates": ["2025-12-15", "2026-07-31", "2026-08-31"],
  "values": [5.82, 8.63, 8.4],
  "article_dates": ["2025-12-26", "2026-08-17", "2026-09-19"],
  "unit": "%",
  "src": [U["mbs_dec25"], U["congthuong_casa"], U["nqs_fed"]],
  "note": "MBS avg 12M deposit rate; Dec-2025 = private-bank group; Jul-2026 per Cong Thuong citing MBS; Aug-2026 = 'commercial banks ~8.4%, +259 bps YTD' (Nguoi Quan Sat 19-Sep-2026) - groups differ slightly.",
 },
 "avg_lending_rate_sbv": {
  "dates": ["2024-12", "2026-06", "2026-07", "2026-08"],
  "values": ["~6.65 (avg)", "8.1–10.5", "8.3–10.5", "8.4–10.7"],
  "unit": "%/yr (SOCB & JSCB, new + outstanding)",
  "src": ["https://tapchinganhang.gov.vn/tang-truong-tin-dung-cho-nen-kinh-te-nhin-lai-nam-2024-va-dinh-huong-nam-2025-15279.html",
          "https://diendandoanhnghiep.vn/lai-suat-cho-vay-moi-va-cu-con-du-no-o-muc-8-1-10-5-nam-10182254.html",
          "https://phapluat.suckhoedoisong.vn/lai-suat-cho-vay-thang-72026-tai-19-ngan-hang-khong-giam-noi-cao-nhat-len-1134nam-283666.html",
          U["tn_lend"]],
  "note": "2024 figure is SBV average lending rate (~6.65%, Tap chi Ngan hang); 2026 values are SBV-reported ranges.",
 },
}

# --- merge agent series if available ---
ib_path = os.path.join(D, "interbank_series.json")
fx_path = os.path.join(D, "fx_series.json")
if os.path.exists(ib_path):
    ib = json.load(open(ib_path))
    series["interbank_on_monthly"] = {k: ib.get(k) for k in ["months", "values", "types", "notes", "urls", "dates"] if k in ib}
    series["interbank_on_monthly"]["src"] = "Vietnamese press citing SBV daily interbank data; see urls per month; types: avg / month_end / point"
    if ib.get("deposit12m_mbs"):
        series["deposit12m_mbs_monthly"] = ib["deposit12m_mbs"]
    if ib.get("omo_notes"):
        series["interbank_on_monthly"]["omo_notes"] = ib["omo_notes"]
else:
    series["interbank_on_monthly"] = {"months": [], "values": [], "src": "PENDING"}
if os.path.exists(fx_path):
    fx = json.load(open(fx_path))
    series["usdvnd_monthly"] = {"months": fx["months"], "central": fx["central"], "vcb_sell": fx["vcb_sell"], "free_sell": fx.get("free_sell"),
        "urls": {k: fx["urls"].get(k) for k in ["central", "vcb_sell", "free_sell"]},
        "dates": {k: fx["dates"].get(k) for k in ["central", "vcb_sell", "free_sell"]} if isinstance(fx.get("dates"), dict) else fx.get("dates"),
        "ytd": fx.get("ytd"), "reserves": fx.get("reserves"), "notes": fx.get("notes")}
    series["usdvnd_monthly"]["src"] = "SBV central rate / Vietcombank selling rate at or near month-end, per press; see urls"
    series["fed_funds_upper"] = {"months": fx.get("months"), "values": fx.get("fed_upper"),
                                 "src": (fx.get("urls") or {}).get("fed_upper") if isinstance(fx.get("urls"), dict) else None}
    if fx.get("dxy") and any(v is not None for v in fx["dxy"]):
        series["dxy_month_end"] = {"months": fx.get("months"), "values": fx.get("dxy"),
                                   "src": (fx.get("urls") or {}).get("dxy") if isinstance(fx.get("urls"), dict) else None}
    if fx.get("events"):
        series["fx_events"] = fx["events"]
else:
    series["usdvnd_monthly"] = {"months": [], "central": [], "src": "PENDING"}

summary_vi = [
 "Nguyên nhân gốc: tín dụng tăng nhanh hơn huy động (2025: ~19% vs ~14%; 3/9/2026: 9,98% vs 8,33%), LDR đơn giản toàn ngành lên ~114% (Q2/2026) → ngân hàng đua lãi suất huy động (12T nhóm tư nhân 5,82% → ~8,6%).",
 "Chi phí vốn tăng vọt: COF 4,79% Q2/2026 (+105 bps svck), CASA giảm còn ~20,9%, chi phí lãi H1/2026 +49%, phải phát hành GTCG/trái phiếu lãi 8–10% → lãi vay T8/2026 8,4–10,7%, cao hơn cuối 2025 ~1,8 điểm %.",
 "Thanh khoản và tỷ giá trói tay NHNN: liên ngân hàng qua đêm từng 17% (3/2/2026), OMO nâng lên 4,5% (12/2025), NHNN hút ròng để giữ tỷ giá (bán kỳ hạn 26.850), dự trữ ~2 tháng nhập khẩu; Fed tăng lên 3,75–4% (16/9/2026).",
 "Lạm phát ~5% (CPI T9/2026 +5,08%) giữ lãi suất thực dương ~3,5 điểm %; nợ xấu 3,31% và trích lập cao ở NH tư nhân giữ phần bù rủi ro.",
 "Chỉ đạo hành chính (CV 2342, 3972, 4190, 7125; SME −1 điểm %) và nới LDR/KBNN, trần 40% chỉ giảm nhẹ áp lực; VNDirect dời kỳ vọng hạ lãi suất sang đầu 2027.",
]
summary_en = [
 "Root cause: credit outgrowing deposits (2025 ~19% vs ~14%; 3-Sep-2026 9.98% vs 8.33%), simple sector LDR ~114% (Q2-26) → a deposit-rate race (private banks' 12M rate 5.82% → ~8.6%).",
 "Cost of funds surged: COF 4.79% in Q2-26 (+105 bps YoY), CASA down to ~20.9%, H1-26 interest expense +49%, paper issued at 8–10% → Aug-2026 lending rates 8.4–10.7%, ~1.8pp above end-2025.",
 "Liquidity and FX tie the SBV's hands: overnight interbank hit 17% (3-Feb-2026), OMO raised to 4.5% (Dec-2025), SBV drained VND to defend the dong (180-day forwards at 26,850), reserves ~2 months of imports; Fed hiked to 3.75–4% (16-Sep-2026).",
 "~5% inflation (Sep-2026 CPI +5.08%) keeps real deposit rates ~3.5pp positive; 3.31% NPLs and heavy private-bank provisioning keep risk premia wide.",
 "Moral suasion (letters 2342, 3972, 4190, 7125; SME −1pp) and LDR/Treasury/40%-cap relaxations only soften pressure; VNDirect now sees meaningful easing only from early 2027.",
]

out = {
 "as_of": "2026-10-05",
 "drivers": drivers,
 "series": series,
 "summary_vi": summary_vi,
 "summary_en": summary_en,
 "gaps": [],
}
json.dump(out, open(os.path.join(D, "why_rates.json"), "w"), ensure_ascii=False, indent=1)
print("ok", len(drivers), list(series))
