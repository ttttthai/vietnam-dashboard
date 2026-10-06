#!/usr/bin/env python3
"""Build monetary.json — registry of SBV (NHNN) monetary & macro-prudential instruments, Jan-2023 .. 2026-10-05.
Every history entry carries a URL that was fetched/seen during research. No invented values."""
import json, os

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "monetary.json")

def h(date, frm, to, doc, direction, vi, en, url):
    return {"date": date, "from": frm, "to": to, "doc": doc, "direction": direction,
            "note_vi": vi, "note_en": en, "url": url}

U = {
 "qd313": "https://www.div.gov.vn/nhnn-dieu-chinh-cac-muc-lai-suat-dieu-hanh-tu-ngay-15-3",
 "qd574": "https://thuvienphapluat.vn/chinh-sach-phap-luat-moi/vn/ho-tro-phap-luat/chinh-sach-moi/47507/tu-ngay-03-4-2023-muc-lai-suat-tai-cap-von-la-5-5-nam",
 "qd575": "https://vneconomy.vn/ngan-hang-nha-nuoc-cung-luc-ban-hanh-5-quyet-dinh-giam-lai-suat.htm",
 "qd950": "https://baochinhphu.vn/nhnn-tiep-tuc-ha-lai-suat-tu-ngay-25-5-102230523184540694.htm",
 "qd1123": "https://xaydungchinhsach.chinhphu.vn/nhnn-tiep-tuc-dieu-chinh-lai-suat-dieu-hanh-119230616150218505.htm",
 "h1_2026": "https://div.gov.vn/ngan-hang-nha-nuoc-hop-bao-ve-ket-qua-dieu-hanh-chinh-sach-tien-te-va-hoat-dong-ngan-hang-6-thang-dau-nam-2026",
 "noreversal": "https://theleader.vn/ngan-hang-nha-nuoc-khang-dinh-khong-dao-chieu-chinh-sach-tien-te-d46705.html",
 "omo_2024_up": "https://dttc.sggp.org.vn/nhnn-bat-ngo-tang-lai-suat-omo-post114268.html",
 "omo_2024_aug": "https://vtv.vn/kinh-te/ngan-hang-nha-nuoc-ha-lai-suat-omo-xuong-425-nam-20240807150146643.htm",
 "omo_2024_sep": "https://www.tinnhanhchungkhoan.vn/ngan-hang-nha-nuoc-giam-lai-suat-omo-ve-4nam-truoc-khi-fed-cat-lai-suat-usd-post354014.html",
 "omo_2025_mar": "https://cafebiz.vn/nong-ngan-hang-nha-nuoc-dung-phat-hanh-tin-phieu-cung-ung-thanh-khoan-omo-len-toi-91-ngay-17625030516411875.chn",
 "omo_2025_dec": "https://vneconomy.vn/ngan-hang-nha-nuoc-nang-lai-suat-cho-vay-ngan-han-tren-thi-truong-mo.htm",
 "omo_2025_dec2": "https://fili.vn/2025/12/nhnn-bat-ngo-nang-lai-suat-tren-kenh-mua-ky-han-len-45-phia-sau-la-gi-757-1378696.htm",
 "omo_2026_may": "https://vietstock.vn/2026/06/lai-suat-lien-ngan-hang-sat-moc-8-nhnn-dao-chieu-bom-rong-30733-ty-qua-omo-757-1448897.htm",
 "omo_2026_sep": "https://congthuong.vn/thanh-khoan-cang-ngan-hang-nha-nuoc-kich-hoat-lai-cac-van-ho-tro-474231.html",
 "omo_2026_w39": "https://vietstock.vn/2026/09/tuan-21-2509-nhnn-dao-chieu-bom-rong-hon-34000-ty-757-1496294.htm",
 "bills_2023": "https://vietstock.vn/2023/09/ngay-2109-nhnn-phat-hanh-gan-10000-ty-dong-tin-phieu-ky-han-28-ngay-757-1108779.htm",
 "bills_2024": "https://www.vietnamplus.vn/nhnn-hut-ve-15000-ty-dong-qua-tin-phieu-phien-113-sau-4-thang-tam-ngung-post933952.vnp",
 "usd0": "https://thuvienphapluat.vn/van-ban/Tien-te-Ngan-hang/Quyet-dinh-2589-QD-NHNN-lai-suat-toi-da-tien-gui-do-la-tai-to-chuc-tin-dung-chi-nhanh-ngan-hang-nuoc-ngoai-298167.aspx",
 "cr_2023": "https://vietnambiz.vn/bien-dong-ty-gia-nam-2023-va-lua-chon-dieu-hanh-cua-nhnn-202412164441747.htm",
 "cr_2024": "https://baodautu.vn/vang-vung-moc-2600-usdounce-ty-gia-trung-tam-khep-lai-nam-2024-tang-chua-den-2-d237224.html",
 "cr_2025": "https://thanhnien.vn/gia-usd-hom-nay-112026-tang-32-trong-nam-2025-185260101083650774.htm",
 "cr_2026_04": "https://nhadautu.vn/ty-gia-usd-chiu-ap-luc-neo-cao-d104083.html",
 "cr_2026_06": "https://infographics.vn/interactive-ty-gia-trung-tam-ngay-30-6-2026-1-usd-25206-vnd/241269.vna",
 "cr_2026_0728": "https://vietnamfinance.vn/ty-gia-trung-tam-pha-ky-luc-sau-nua-nam-lang-song-d148217.html",
 "cr_2026_0813": "https://thanhnien.vn/gia-usd-hom-nay-1382026-ty-gia-trung-tam-tiep-tuc-tang-manh-185260813084243286.htm",
 "cr_2026_0915": "https://fili.vn/2026/09/ngan-hang-nha-nuoc-nang-ty-gia-trung-tam-len-ky-luc-25617-vndusd-757-1492435.htm",
 "cr_2026_0930": "https://vietstock.vn/2026/10/tinh-den-289-tin-dung-toan-nen-kinh-te-tang-1089-757-1498652.htm",
 "cr_2026_1002": "https://doanhnghiephoinhap.vn/ty-gia-usd-hom-nay-310-2026-dong-usd-giam-nhe-sau-bao-cao-viec-lam-my-150564.html",
 "fx_2024_spot": "https://vov.gov.vn/tu-hom-nay-194-nhnn-cong-khai-ban-ngoai-te-can-thiep-ty-gia-bat-dau-ha-nhiet-19-dtnew-772506",
 "fx_2025_fwd": "https://baodautu.vn/ngan-hang-nha-nuoc-ban-ngoai-te-co-ky-han-can-thiep-thi-truong-tu-tuan-sau-cho-phep-huy-ngang-d368448.html",
 "fx_2026_fwd": "https://vietstock.vn/2026/03/nhnn-tung-don-can-thiep-ty-gia-phat-tin-hieu-on-dinh-vnd-757-1416142.htm",
 "fx_options": "https://doanhnghiepkinhtexanh.vn/ngan-hang-nha-nuoc-de-xuat-them-cong-cu-moi-de-can-thiep-thi-truong-ngoai-hoi-a49969.html",
 "fx_reserves": "https://cafef.vn/du-tru-ngoai-hoi-con-876-ty-usd-ngan-hang-nha-nuoc-muon-sua-doi-nhieu-quy-dinh-188260624112155679.chn",
 "gold_2024": "https://cafebiz.vn/2024-nam-cua-vang-va-nhung-dien-bien-chua-tung-co-176241226063412241.chn",
 "gold_2024_jun": "https://baodautu.vn/dung-dau-thau-vang-trien-khai-phuong-an-binh-on-moi-tu-36-d216168.html",
 "nd232": "https://baochinhphu.vn/chinh-thuc-xoa-bo-co-che-nha-nuoc-doc-quyen-san-xuat-vang-mieng-102250826145041005.htm",
 "tt34_2025": "https://www.sggp.org.vn/ngan-hang-nha-nuoc-ban-hanh-thong-tu-huong-dan-cap-phep-cho-nhap-khau-vang-post817386.html",
 "tt82_2025": "https://cafebiz.vn/chinh-thuc-cho-phep-nang-trang-thai-vang-o-nhieu-ngan-hang-176260110102321266.chn",
 "gold_exchange_pm": "https://xaydungchinhsach.chinhphu.vn/thu-tuong-chi-dao-mot-so-nhiem-vu-giai-phap-trong-tam-dieu-hanh-chinh-sach-tien-te-chinh-sach-tai-khoa-2026-119260208172743313.htm",
 "gold_11apps": "https://baoxaydung.vn/ngan-hang-nha-nuoc-tiep-nhan-11-ho-so-xin-cap-phep-san-xuat-vang-mieng-192260414174709777.htm",
 "sbv_0814": "https://cafef.vn/ngan-hang-nha-nuoc-neu-dinh-huong-dieu-hanh-lai-suat-ty-gia-tin-dung-va-thi-truong-vang-thoi-gian-toi-188260814081132009.chn",
 "rrr_tt23": "https://vnexpress.net/ngan-hang-nhan-chuyen-giao-bat-buoc-duoc-giam-50-du-tru-bat-buoc-4926880.html",
 "rrr_2026": "https://vietnambiz.vn/4-ngan-hang-nhan-tin-giam-50-ty-le-du-tru-bat-buoc-co-phieu-tang-vot-2026211144943372.htm",
 "tt50": "https://vietnambiz.vn/nhnn-nang-tran-ty-le-cho-vay-tren-tien-gui-len-95-cong-bo-hai-chi-tieu-quan-ly-thanh-khoan-lcr-va-nsfr-2026101144912870.htm",
 "tt50b": "https://www.vietnamplus.vn/ngan-hang-nha-nuoc-nang-tran-ty-le-cho-vay-tren-tien-gui-len-95-post1139453.vnp",
 "tt50c": "https://cafef.vn/thong-tu-50-tao-buoc-chuyen-moi-trong-quan-ly-rui-ro-ngan-hang-188261004081728421.chn",
 "kbnn_roadmap": "https://baodautu.vn/chinh-thuc-noi-ty-le-ldr-20-tien-gui-kho-bac-se-lam-diu-thanh-khoan-big-4-them-du-dia-bom-tin-dung-d596694.html",
 "tt08_2026": "https://cafef.vn/khi-ngan-hang-doi-cach-tinh-ty-le-an-toan-von-theo-thong-tu-08-2026-tt-nhnn-18826052007433474.chn",
 "qd1743": "https://cafef.vn/nong-ngan-hang-nha-nuoc-nang-ty-le-tinh-tien-gui-kho-bac-nha-nuoc-vao-ldr-len-50-ap-dung-chinh-thuc-tu-ngay-mai-1-8-188260731150341967.chn",
 "tt25_2026": "https://doanhnhan.baophapluat.vn/ngan-hang-nha-nuoc-noi-tran-von-ngan-han-cho-vay-trung-dai-han-len-40-tu-1-7.html",
 "tt25_2026_doc": "https://congbao.chinhphu.vn/van-ban/thong-tu-so-25-2026-tt-nhnn-469825.htm",
 "tt14_2025": "https://cafef.vn/da-co-thong-tu-thay-the-thong-tu-41-cua-nhnn-ve-ty-le-an-toan-von-ap-dung-basel-iii-188250716111139315.chn",
 "tt22_2023": "https://xaydungchinhsach.chinhphu.vn/giam-he-so-rui-ro-cho-nhieu-khoan-vay-11924011710423981.htm",
 "rw_2025": "https://cafef.vn/cho-vay-kinh-doanh-bat-dong-san-ap-dung-he-so-rui-ro-200-188250924140541635.chn",
 "tt02_2023": "https://xaydungchinhsach.chinhphu.vn/thong-tu-moi-cua-nhnn-quy-dinh-to-chuc-tin-dung-duoc-co-cau-lai-thoi-han-tra-no-giu-nguyen-nhom-no-11923042410285193.htm",
 "tt02_ext": "https://cafef.vn/thong-tu-quan-trong-ve-co-cau-giu-nguyen-nhom-no-cua-nganh-ngan-hang-sap-het-hieu-luc-188241119113708193.chn",
 "tt53_2024": "https://vietnambiz.vn/nhnn-cho-phep-co-cau-lai-khoan-no-anh-huong-boi-bao-yagi-den-het-nam-2025-20241210122722958.htm",
 "tt31_2024": "https://thuvienphapluat.vn/van-ban/Tien-te-Ngan-hang/Thong-tu-31-2024-TT-NHNN-phan-loai-tai-san-co-trong-hoat-dong-cua-ngan-hang-thuong-mai-616233.aspx",
 "nd86_2024": "https://luatvietnam.vn/tai-chinh/nghi-dinh-86-2024-nd-cp-phuong-phap-trich-lap-du-phong-rui-ro-cua-to-chuc-tin-dung-359985-d1.html",
 "cg_2023": "https://baochinhphu.vn/nhnn-nam-2023-tang-truong-tin-dung-toan-he-thong-khoang-14-102230710184453473.htm",
 "cg_2024": "https://baochinhphu.vn/nhnn-chi-tieu-tang-tin-dung-nam-2024-la-15-102240102155126698.htm",
 "cg_2024_act": "https://thoibaonganhang.vn/tin-dung-ca-nam-2024-tang-1508-vuot-muc-tieu-dat-ra-159667.html",
 "cg_2025": "https://thuvienphapluat.vn/chinh-sach-phap-luat-moi/vn/ho-tro-phap-luat/tai-chinh/79053/chi-thi-01-nhnn-tang-truong-tin-dung-nam-2025-khoang-16",
 "cg_2025_jul": "https://baochinhphu.vn/nhnn-tang-chi-tieu-tin-dung-bom-von-cho-nen-kinh-te-102250731174957903.htm",
 "cg_2025_act": "https://tuoitre.vn/tang-truong-tin-dung-nam-2025-hon-19-nhung-nam-2026-se-thap-hon-20260110193917581.htm",
 "cg_2026": "https://baodautu.vn/ngan-hang-nha-nuoc-ban-hanh-quy-dinh-moi-ve-tin-dung-nam-2026-ham-phanh-tin-dung-bat-dong-san-d487178.html",
 "cg_2026_sep": "https://mekongasean.vn/du-no-tin-dung-tang-1159-gan-409000-ty-dong-cho-vay-uu-dai-60280.html",
 "room_pm": "https://baochinhphu.vn/thu-tuong-yeu-cau-nhnn-khan-truong-thi-diem-bo-room-tin-dung-102250807092619905.htm",
 "cv4551": "https://thoibaotaichinhvietnam.vn/noi-room-co-chon-loc-cho-25-ngan-hang-tin-dung-duoc-dan-vao-nha-o-xa-hoi-va-khu-cong-nghiep-198294.html",
 "room_18": "https://vietstock.vn/2026/07/nhnn-noi-ve-viec-loai-tru-18-du-an-trong-diem-khoi-room-tin-dung-757-1461971.htm",
 "room_18b": "https://vnexpress.net/khoan-vay-cho-loat-du-an-lon-khong-tinh-vao-han-muc-tang-truong-tin-dung-5089165.html",
 "room_hotel": "https://znews.vn/nhnn-noi-tin-dung-cho-nha-hang-khach-san-khu-nghi-duong-post1683843.html",
 "sh_2024": "https://cand.vn/dia-oc/goi-120-nghin-ty-cho-vay-nha-o-xa-hoi-da-tang-len-145-nghin-ty-dong-i748026",
 "sh_120": "https://vnexpress.net/goi-120-000-ty-dong-cho-nha-xa-hoi-van-e-du-4-lan-giam-lai-vay-4886245.html",
 "sh_2026": "https://thoibaotaichinhvietnam.vn/lai-suat-vay-mua-nha-o-xa-hoi-len-65nam-nguoi-tre-van-duoc-huong-muc-thap-hon-thi-truong-199756.html",
 "sh_disb": "https://vietstock.vn/2026/07/sau-3-nam-145000-ty-danh-cho-nha-o-xa-hoi-giai-ngan-duoc-bao-nhieu-757-1468002.htm",
 "sh_sep": "https://vietstock.vn/2026/09/von-dat-doanh-nghiep-bat-dong-san-kho-tiep-can-goi-tin-dung-145000-ty-4220-1495838.htm",
 "agri": "https://xaydungchinhsach.chinhphu.vn/trien-khai-chuong-trinh-tin-dung-100000-ty-dong-doi-voi-linh-vuc-nong-lam-thuy-san-119250415175353965.htm",
 "infra_2025": "https://vnexpress.net/21-ngan-hang-danh-goi-tin-dung-500-000-ty-cho-ha-tang-cong-nghe-so-4892207.html",
 "infra_dec": "https://baochinhphu.vn/danh-500-nghin-ty-dong-cho-vay-uu-dai-ha-tang-dien-giao-thong-cong-nghe-chien-luoc-10225121218545832.htm",
 "cv7125": "https://vietstock.vn/2026/08/nhnn-chi-dao-giam-toi-thieu-1-lai-suat-cho-vay-voi-doanh-nghiep-nho-va-vua-757-1478752.htm",
 "cv7125b": "https://thitruongtaichinhtiente.vn/chuong-trinh-tin-dung-danh-cho-doanh-nghiep-nho-va-vua-linh-vuc-uu-tien-them-luc-day-cho-muc-tieu-tang-truong-hai-con-so-84867.html",
 "green_2026": "https://diendandoanhnghiep.vn/nhnn-dang-trinh-du-thao-nghi-dinh-ho-tro-lai-suat-2-voi-cac-khoan-vay-xanh-10180196.html",
 "green_2026b": "https://tuoitre.vn/ho-kinh-doanh-sap-duoc-ho-tro-lai-suat-2-khi-dau-tu-du-an-xanh-100260623165341005.htm",
 "cv4462": "https://www.div.gov.vn/ngan-hang-nha-nuoc-yeu-cau-cac-to-chuc-tin-dung-tiep-tuc-giam-tu-1-2-lai-suat-cho-vay",
 "ct01_2025": "https://baochinhphu.vn/nhnn-yeu-cau-ngan-hang-giam-lai-suat-cho-vay-ho-tro-tang-truong-kinh-te-102250805105637032.htm",
 "cv2342": "https://vov.vn/kinh-te/nhnn-yeu-cau-cac-ngan-hang-thuong-mai-giam-lai-suat-huy-dong-va-cho-vay-post1294357.vov",
 "tb117": "https://vietstock.vn/2026/05/lai-suat-tien-gui-dau-thang-5-dong-loat-ha-nhiet-757-1440643.htm",
 "cv3972": "https://vietnamnet.vn/nhnn-yeu-cau-kiem-tra-viec-giam-lai-suat-tai-cac-ngan-hang-thuong-mai-2518305.html",
 "cv4190": "https://vietstock.vn/2026/05/nhnn-yeu-cau-kiem-tra-viec-giam-lai-suat-tai-cac-ngan-hang-thuong-mai-757-1445574.htm",
 "cafef_0524": "https://cafef.vn/nhnn-quyet-tam-ha-nhiet-lai-suat-188260524085014122.chn",
 "hidden_rates": "https://tuoitre.vn/nld/chan-chinh-viec-tang-lai-suat-huy-dong-196260616213452584.htm",
 "ct_2024": "https://thitruongtaichinhtiente.vn/chinh-thuc-chuyen-giao-bat-buoc-cb-cho-vietcombank-oceanbank-cho-mb-63437.html",
 "ct_2025": "https://vietstock.vn/2025/01/chinh-thuc-chuyen-giao-bat-buoc-gpbank-cho-vpbank-va-donga-bank-cho-hdbank-757-1262773.htm",
 "scb_2025": "https://thuvienphapluat.vn/chinh-sach-phap-luat-moi/vn/ho-tro-phap-luat/chinh-sach-moi/84670/yeu-cau-tap-trung-hoan-thien-phuong-an-co-cau-lai-ngan-hang-scb",
 "scb_2026": "https://cafef.vn/thong-tin-moi-nhat-ve-scb-va-cac-ngan-hang-duoc-chuyen-giao-bat-buoc-188260822144124164.chn",
 "tt35_2025": "https://thuvienphapluat.vn/chinh-sach-phap-luat-moi/vn/ho-tro-phap-luat/chinh-sach-moi/99858/thong-tu-35-2025-quy-dinh-ve-trinh-tu-cho-vay-dac-biet-doi-voi-to-chuc-tin-dung",
 "tt02_2026": "https://luatvietnam.vn/tai-chinh/thong-tu-02-2026-tt-nhnn-sua-doi-cho-vay-dac-biet-to-chuc-tin-dung-431010-d1.html",
 "law32": "https://thuvienphapluat.vn/van-ban/Tien-te-Ngan-hang/Luat-Cac-to-chuc-tin-dung-32-2024-QH15-577203.aspx",
 "law96": "https://luatvietnam.vn/linh-vuc-khac/diem-moi-cua-luat-cac-to-chuc-tin-dung-sua-doi-2025-883-103000-article.html",
 "pcf_1266": "https://thuonghieucongluan.com.vn/moi-xa-chi-con-toi-da-mot-quy-tin-dung-nhan-dan-hoan-tat-sap-xep-truoc-nam-2035-a325143.html",
 "di_350": "https://vietstock.vn/2026/07/tu-ngay-13072026-han-muc-chi-tra-tien-bao-hiem-cua-bao-hiem-tien-gui-viet-nam-la-350-trieu-dong-757-1467309.htm",
 "qd2345": "https://thitruongtaichinhtiente.vn/phan-loai-giao-dich-ngan-hang-phai-xac-thuc-sinh-trac-hoc-theo-quyet-dinh-2345-qd-nhnn-58169.html",
 "tt45_2025": "https://thuvienphapluat.vn/chinh-sach-phap-luat-moi/vn/ho-tro-phap-luat/chinh-sach-moi/99746/tu-05-01-2026-bat-buoc-xac-thuc-sinh-trac-hoc-khi-phat-hanh-the-ngan-hang",
 "tt77_2025": "https://thuvienphapluat.vn/ma-so-thue/phap-luat-thue/tu-ngay-0172026-quy-dinh-moi-ve-sinh-trac-hoc-theo-han-muc-cu-the-danh-cho-doanh-nghiep-va-ho-kinh--227245.html",
}

instruments = []

# ---------------------------------------------------------------- RATES
instruments.append({
 "id": "refinancing_rate", "group": "rates",
 "name_vi": "Lãi suất tái cấp vốn", "name_en": "Refinancing rate",
 "current_vi": "4,5%/năm (giữ nguyên từ 19/6/2023; NHNN xác nhận giữ nguyên lãi suất điều hành trong năm 2026)",
 "current_en": "4.5% p.a. (unchanged since 19 Jun 2023; SBV confirmed policy rates unchanged through 2026)",
 "unit": "%", "current_value": 4.5, "effective": "2023-06-19",
 "history": [
  h("2023-03-15","6.0%","6.0%","313/QĐ-NHNN","neutral","Giữ nguyên 6%, trong khi giảm lãi suất tái chiết khấu và qua đêm.","Held at 6% while rediscount and overnight rates were cut.",U["qd313"]),
  h("2023-04-03","6.0%","5.5%","574/QĐ-NHNN","easing","Giảm 0,5 điểm % (QĐ ký 31/3/2023).","Cut 50bp (decision signed 31 Mar 2023).",U["qd574"]),
  h("2023-05-25","5.5%","5.0%","950/QĐ-NHNN","easing","Giảm 0,5 điểm % (QĐ ngày 23/5/2023).","Cut 50bp (decision dated 23 May 2023).",U["qd950"]),
  h("2023-06-19","5.0%","4.5%","1123/QĐ-NHNN","easing","Lần giảm thứ 4 trong năm 2023; mức hiện hành.","Fourth cut of 2023; current level.",U["qd1123"]),
  h("2026-07-02","4.5%","4.5%",None,"neutral","Họp báo 6 tháng: NHNN giữ nguyên lãi suất điều hành, khẳng định không đảo chiều chính sách.","H1-2026 press briefing: policy rates kept unchanged; SBV says no policy reversal.",U["h1_2026"]),
 ],
 "series": {"dates": ["2023-01-01","2023-04-03","2023-05-25","2023-06-19","2026-10-05"], "values": [6.0,5.5,5.0,4.5,4.5]},
 "why_vi": "Neo chi phí vốn từ NHNN cho các TCTD; tín hiệu định hướng mặt bằng lãi suất.",
 "why_en": "Anchors the cost of central-bank funding for banks and signals the intended rate level."
})
instruments.append({
 "id": "rediscount_rate", "group": "rates",
 "name_vi": "Lãi suất tái chiết khấu", "name_en": "Rediscount rate",
 "current_vi": "3,0%/năm (từ 19/6/2023)", "current_en": "3.0% p.a. (since 19 Jun 2023)",
 "unit": "%", "current_value": 3.0, "effective": "2023-06-19",
 "history": [
  h("2023-03-15","4.5%","3.5%","313/QĐ-NHNN","easing","Giảm 1 điểm %.","Cut 100bp.",U["qd313"]),
  h("2023-06-19","3.5%","3.0%","1123/QĐ-NHNN","easing","Giảm 0,5 điểm %; mức hiện hành.","Cut 50bp; current level.",U["qd1123"]),
 ],
 "series": {"dates": ["2023-01-01","2023-03-15","2023-06-19","2026-10-05"], "values": [4.5,3.5,3.0,3.0]},
 "why_vi": "Sàn của hành lang lãi suất; chi phí chiết khấu giấy tờ có giá ngắn hạn tại NHNN.",
 "why_en": "Floor of the rate corridor; cost of discounting short-term papers at the SBV."
})
instruments.append({
 "id": "overnight_rate", "group": "rates",
 "name_vi": "Lãi suất cho vay qua đêm trong thanh toán điện tử liên ngân hàng (và cho vay bù đắp thiếu hụt vốn trong thanh toán bù trừ)",
 "name_en": "Overnight lending rate in inter-bank electronic payments (and clearing shortfall loans)",
 "current_vi": "5,0%/năm (từ 19/6/2023)", "current_en": "5.0% p.a. (since 19 Jun 2023)",
 "unit": "%", "current_value": 5.0, "effective": "2023-06-19",
 "history": [
  h("2023-03-15","7.0%","6.0%","313/QĐ-NHNN","easing","Giảm 1 điểm %.","Cut 100bp.",U["qd313"]),
  h("2023-05-25","6.0%","5.5%","950/QĐ-NHNN","easing","Giảm 0,5 điểm %.","Cut 50bp.",U["qd950"]),
  h("2023-06-19","5.5%","5.0%","1123/QĐ-NHNN","easing","Giảm 0,5 điểm %; mức hiện hành.","Cut 50bp; current level.",U["qd1123"]),
 ],
 "series": {"dates": ["2023-01-01","2023-03-15","2023-05-25","2023-06-19","2026-10-05"], "values": [7.0,6.0,5.5,5.0,5.0]},
 "why_vi": "Trần mềm cho lãi suất liên ngân hàng qua đêm (cửa sổ cho vay cuối ngày).",
 "why_en": "Soft ceiling for overnight interbank rates (end-of-day lending window)."
})
instruments.append({
 "id": "omo_rate", "group": "rates",
 "name_vi": "Lãi suất OMO (cho vay cầm cố giấy tờ có giá qua thị trường mở) & thực tiễn đấu thầu",
 "name_en": "OMO rate (open-market reverse repo) & auction practice",
 "current_vi": "4,5%/năm cho mọi kỳ hạn (từ 4/12/2025); chào thầu kỳ hạn 7–91 ngày, đấu thầu khối lượng với lãi suất cố định; tháng 9/2026 kết hợp mở lại hoán đổi USD/VND 7 ngày",
 "current_en": "4.5% p.a. across tenors (since 4 Dec 2025); 7–91-day tenors, volume auctions at a fixed rate; Sept-2026 also reopened 7-day USD/VND swaps",
 "unit": "%", "current_value": 4.5, "effective": "2025-12-04",
 "history": [
  h("2024-04-23","4.0%","4.25%",None,"tightening","Tăng lãi suất OMO lần đầu để hỗ trợ tỷ giá.","First OMO hike, to support the VND.",U["omo_2024_up"]),
  h("2024-05-22","4.25%","4.5%",None,"tightening","Tăng lần 2 trong 1 tháng; lãi suất tín phiếu 28 ngày 3,9%→4,0%.","Second hike in a month; 28-day bill rate 3.9%→4.0%.",U["omo_2024_up"]),
  h("2024-08-07","4.5%","4.25%",None,"easing","Hạ lần đầu sau 7 tháng khi áp lực tỷ giá giảm.","First cut in 7 months as FX pressure eased.",U["omo_2024_aug"]),
  h("2024-09-16","4.25%","4.0%",None,"easing","Hạ về 4% trước khi Fed giảm lãi suất.","Cut to 4% ahead of the Fed's cut.",U["omo_2024_sep"]),
  h("2025-03-05","OMO ≤ ~28 ngày","OMO tới 91 ngày",None,"easing","Dừng phát hành tín phiếu, cung ứng thanh khoản OMO kỳ hạn tới 91 ngày.","Stopped bill issuance and offered OMO liquidity up to 91 days.",U["omo_2025_mar"]),
  h("2025-12-04","4.0%","4.5%",None,"tightening","Nâng đồng loạt kỳ hạn 7/14/28/91 ngày lên 4,5% sau khi LS qua đêm vọt 7,48% (3/12/2025).","Raised all tenors (7/14/28/91d) to 4.5% after overnight interbank hit 7.48% (3 Dec 2025).",U["omo_2025_dec"]),
  h("2026-05-29","4.5%","4.5%",None,"neutral","Tuần 25–29/5: bơm ròng 30.733 tỷ, kỳ hạn 7–56 ngày; LS liên NH qua đêm ~7%, 6 tháng 8,34%.","Week of 25–29 May: net 30.7tn injection, 7–56d; overnight interbank ~7%, 6m 8.34%.",U["omo_2026_may"]),
  h("2026-09-21","4.5%","4.5%",None,"easing","Thanh khoản căng: chào OMO 7–91 ngày ở 4,5% và mở lại hoán đổi USD/VND 7 ngày tối đa 2 tỷ USD.","Tight liquidity: 7–91d OMO at 4.5% plus reopened 7-day USD/VND swaps up to USD 2bn.",U["omo_2026_sep"]),
 ],
 "series": {"dates": ["2024-01-01","2024-04-23","2024-05-22","2024-08-07","2024-09-16","2025-12-04","2026-10-05"], "values": [4.0,4.25,4.5,4.25,4.0,4.5,4.5]},
 "why_vi": "Công cụ điều tiết thanh khoản hằng ngày, định hướng lãi suất liên ngân hàng; là lãi suất 'thực' quan trọng nhất hiện nay.",
 "why_en": "Daily liquidity tool steering interbank rates; the de-facto operative policy rate.",
 "notes_vi": "Các bước giảm lãi suất OMO trong 2023 (về 4%) chưa được xác minh ngày cụ thể.",
 "notes_en": "Exact dates of the 2023 OMO cuts (down to 4%) were not verified."
})
instruments.append({
 "id": "sbv_bills", "group": "rates",
 "name_vi": "Phát hành tín phiếu NHNN (hút thanh khoản)", "name_en": "SBV bill issuance (liquidity absorption)",
 "current_vi": "Không phát hành kể từ 5/3/2025 (không tìm thấy phiên phát hành nào năm 2026); NHNN chủ yếu bơm/hút qua OMO",
 "current_en": "No issuance since 5 Mar 2025 (no 2026 auctions found); SBV operates mainly via OMO injections/maturities",
 "unit": "text", "current_value": None, "effective": "2025-03-05",
 "history": [
  h("2023-09-21","tạm ngưng từ 3/2023","phát hành lại","tín phiếu 28 ngày","tightening","Lần đầu sau 6 tháng: ~9.995 tỷ, LS 0,69%; đến 26/9 hút ròng ~50.000 tỷ.","First issuance in 6 months: ~VND 10tn at 0.69%; ~50tn absorbed by 26 Sep.",U["bills_2023"]),
  h("2024-03-11","tạm ngưng 4 tháng","phát hành lại",None,"tightening","Hút 15.000 tỷ qua tín phiếu sau 4 tháng tạm ngưng (hỗ trợ tỷ giá).","Absorbed 15tn via bills after a 4-month pause (FX support).",U["bills_2024"]),
  h("2025-03-05","phát hành (LS giảm 4,0%→3,1%)","dừng phát hành",None,"easing","Dừng phát hành tín phiếu, chuyển sang bơm OMO dài hơn.","Bill issuance stopped; switched to longer OMO injections.",U["omo_2025_mar"]),
 ],
 "series": None,
 "why_vi": "Hút bớt VND dư thừa để đẩy lãi suất liên ngân hàng lên, giảm chênh lệch lãi suất USD–VND và áp lực tỷ giá.",
 "why_en": "Drains excess VND to lift interbank rates, narrowing the USD–VND rate gap and easing FX pressure."
})
instruments.append({
 "id": "deposit_cap_demand", "group": "rates",
 "name_vi": "Trần lãi suất tiền gửi không kỳ hạn & kỳ hạn dưới 1 tháng (VND)", "name_en": "Deposit-rate cap: demand & <1-month (VND)",
 "current_vi": "0,5%/năm (từ 3/4/2023)", "current_en": "0.5% p.a. (since 3 Apr 2023)",
 "unit": "%", "current_value": 0.5, "effective": "2023-04-03",
 "history": [
  h("2023-04-03","1.0%","0.5%","575/QĐ-NHNN","easing","Giảm trần 0,5 điểm % (QĐ 31/3/2023); giữ nguyên tại QĐ 951 (25/5) và 1124 (19/6/2023).","Cap cut 50bp (decision 31 Mar 2023); kept in decisions 951 (25 May) and 1124 (19 Jun 2023).",U["qd575"]),
 ],
 "series": {"dates": ["2023-01-01","2023-04-03","2026-10-05"], "values": [1.0,0.5,0.5]},
 "why_vi": "Kiểm soát chi phí vốn ngắn hạn, hạn chế cạnh tranh lãi suất huy động.",
 "why_en": "Caps short-term funding costs and limits deposit-rate competition."
})
instruments.append({
 "id": "deposit_cap_1_6m", "group": "rates",
 "name_vi": "Trần lãi suất tiền gửi kỳ hạn 1 tháng đến dưới 6 tháng (VND)", "name_en": "Deposit-rate cap: 1 to <6 months (VND)",
 "current_vi": "4,75%/năm (QTDND/TCVM 5,25%); kỳ hạn từ 6 tháng trở lên do thị trường quyết định", "current_en": "4.75% p.a. (people's credit funds/MFIs 5.25%); ≥6-month rates market-determined",
 "unit": "%", "current_value": 4.75, "effective": "2023-06-19",
 "history": [
  h("2023-04-03","6.0%","5.5%","575/QĐ-NHNN","easing","QTDND 6,5%→6,0%.","People's credit funds 6.5%→6.0%.",U["qd575"]),
  h("2023-05-25","5.5%","5.0%","951/QĐ-NHNN","easing","QTDND/TCVM 6,0%→5,5%.","PCFs/MFIs 6.0%→5.5%.",U["qd950"]),
  h("2023-06-19","5.0%","4.75%","1124/QĐ-NHNN","easing","QTDND/TCVM 5,5%→5,25%; mức hiện hành. Giữa 2026 nhiều NH niêm yết kịch trần 4,75%.","PCFs/MFIs 5.5%→5.25%; current. By mid-2026 many banks quote at the 4.75% cap.",U["qd1123"]),
 ],
 "series": {"dates": ["2023-01-01","2023-04-03","2023-05-25","2023-06-19","2026-10-05"], "values": [6.0,5.5,5.0,4.75,4.75]},
 "why_vi": "Neo lãi suất huy động ngắn hạn, gián tiếp kéo giảm lãi suất cho vay.",
 "why_en": "Anchors short-term deposit rates and, indirectly, lending rates."
})
instruments.append({
 "id": "lending_cap_priority", "group": "rates",
 "name_vi": "Trần lãi suất cho vay ngắn hạn VND với 5 lĩnh vực ưu tiên", "name_en": "Short-term VND lending-rate cap for priority sectors",
 "current_vi": "4,0%/năm (QTDND/TCVM 5,0%) — từ 19/6/2023", "current_en": "4.0% p.a. (PCFs/MFIs 5.0%) — since 19 Jun 2023",
 "unit": "%", "current_value": 4.0, "effective": "2023-06-19",
 "history": [
  h("2023-03-15","5.5%","5.0%","314/QĐ-NHNN","easing","QTDND/TCVM 6,5%→6,0%.","PCFs/MFIs 6.5%→6.0%.",U["qd313"]),
  h("2023-04-03","5.0%","4.5%","576/QĐ-NHNN","easing","QTDND 6,0%→5,5%.","PCFs 6.0%→5.5%.",U["qd575"]),
  h("2023-06-19","4.5%","4.0%","1125/QĐ-NHNN","easing","QTDND/TCVM 5,5%→5,0%; mức hiện hành.","PCFs/MFIs 5.5%→5.0%; current.",U["qd1123"]),
 ],
 "series": {"dates": ["2023-01-01","2023-03-15","2023-04-03","2023-06-19","2026-10-05"], "values": [5.5,5.0,4.5,4.0,4.0]},
 "why_vi": "Giảm chi phí vay cho nông nghiệp, xuất khẩu, DNNVV, công nghiệp hỗ trợ, công nghệ cao.",
 "why_en": "Lowers borrowing costs for agriculture, exporters, SMEs, supporting industry and high-tech."
})
instruments.append({
 "id": "moral_suasion", "group": "rates",
 "name_vi": "Chỉ đạo hành chính về lãi suất (công văn, họp với NHTM, thanh tra)", "name_en": "Moral suasion on rates (letters, bank meetings, inspections)",
 "current_vi": "Yêu cầu giảm/giữ ổn định mặt bằng lãi suất; thanh tra NH tăng lãi suất 'ngầm'; chỉ tiêu tín dụng 2027 sẽ bị giảm với NH không tuân thủ",
 "current_en": "Banks told to cut/hold rates; inspections of hidden deposit-rate add-ons; 2027 credit quotas to be cut for non-compliant banks",
 "unit": "text", "current_value": None, "effective": "2026-08-13",
 "history": [
  h("2024-05-31","—","giảm LS cho vay 1–2%","4462/NHNN-CSTT","easing","Yêu cầu TCTD tiếp tục giảm lãi suất cho vay 1–2%/năm, ổn định LS huy động.","Asked banks to cut lending rates a further 1–2pp and keep deposit rates stable.",U["cv4462"]),
  h("2025-01-20","—","ổn định LS huy động, giảm LS cho vay","01/CT-NHNN","easing","Chỉ thị 2025: ổn định LS tiền gửi, giảm LS cho vay; công khai LS cho vay bình quân, chênh lệch LS.","2025 directive: stabilise deposit rates, cut lending rates; publish average lending rates and spreads.",U["ct01_2025"]),
  h("2026-03-30","—","ổn định mặt bằng LS","2342/NHNN-CSTT","neutral","Yêu cầu TCTD ổn định mặt bằng lãi suất, tuân thủ quy định lãi suất.","Banks told to stabilise market rates and comply with rate rules.",U["cv2342"]),
  h("2026-04-09","—","giảm LS tiền gửi ≥6 tháng & LS cho vay","117/TB-NHNN (10/4/2026)","easing","Họp với NHTM: giảm LS tiền gửi phát sinh mới kỳ hạn ≥6 tháng, giảm LS niêm yết và LS cho vay; NH giảm 0,1–0,3 điểm %.","Meeting with banks: cut new ≥6m deposit rates, posted and lending rates; banks cut 0.1–0.3pp.",U["tb117"]),
  h("2026-05-14","—","kiểm tra việc giảm LS","3972/NHNN-CSTT","neutral","Giao NHNN khu vực kiểm tra việc thực hiện giảm mặt bằng LS.","Regional SBV branches ordered to inspect compliance with rate cuts.",U["cv3972"]),
  h("2026-05-21","—","đôn đốc, thanh kiểm tra","4190/NHNN-CSTT","neutral","Đôn đốc kiểm tra; thanh tra NH có LS huy động/cho vay cao hơn bình quân.","Stepped-up inspections of banks with above-average deposit/lending rates.",U["cv4190"]),
  h("2026-06-16","—","chấn chỉnh LS thực tế cao hơn niêm yết",None,"neutral","NHNN Khu vực 1 yêu cầu không 'giảm LS hình thức', không cộng thêm lãi/quà làm LS thực tế cao hơn niêm yết (thực tế tới 8,8%).","Region-1 branch banned token cuts and add-ons pushing actual rates above posted (up to 8.8%).",U["hidden_rates"]),
  h("2026-08-13","—","gắn chỉ tiêu tín dụng 2027 với tuân thủ giảm LS",None,"easing","Họp với Thủ tướng & NHTM: NH không tuân thủ chỉ đạo giảm LS sẽ bị giao chỉ tiêu tín dụng 2027 thấp hơn; NHNN nói dư địa CSTT hỗ trợ tăng trưởng 'rất hạn chế'.","Meeting with PM & banks: non-compliant banks get lower 2027 credit quotas; SBV says room for monetary support is 'very limited'.",U["sbv_0814"]),
 ],
 "series": None,
 "why_vi": "Kéo giảm lãi suất thị trường mà không cắt lãi suất điều hành (vì ràng buộc tỷ giá, lạm phát).",
 "why_en": "Pushes market rates down without cutting policy rates (constrained by FX and inflation)."
})

# ---------------------------------------------------------------- FX
instruments.append({
 "id": "central_rate", "group": "fx",
 "name_vi": "Tỷ giá trung tâm VND/USD", "name_en": "Central (reference) VND/USD rate",
 "current_vi": "25.636 đ/USD (công bố 2/10/2026); +~2,0% so với cuối 2025 (25.627 ngày 30/9 = +2,01%); kỷ lục mới liên tiếp từ 28/7/2026",
 "current_en": "VND 25,636/USD (2 Oct 2026); ~+2.0% YTD (25,627 on 30 Sep = +2.01%); successive record highs since 28 Jul 2026",
 "unit": "VND", "current_value": 25636, "effective": "2026-10-02",
 "history": [
  h("2023-12-29","~23.690 (đầu 2023)","23.866",None,"neutral","Cả năm 2023 tỷ giá trung tâm tăng ~1,1% (VND mất giá ~3% trên thị trường).","Central rate +~1.1% in 2023 (market VND −~3%).",U["cr_2023"]),
  h("2024-12-31","23.866","24.335",None,"neutral","Cả năm 2024 tăng 1,96%; giá USD NHTM tăng ~4,6%.","+1.96% in 2024; bank USD prices +~4.6%.",U["cr_2024"]),
  h("2025-12-31","24.335","25.121",None,"neutral","Cả năm 2025 tăng 794 đồng (~3,2%); đỉnh cũ 25.298 (8/2025).","+794 dong (~3.2%) in 2025; previous peak 25,298 (Aug 2025).",U["cr_2025"]),
  h("2026-06-30","25.121","25.206",None,"neutral","Nửa đầu 2026 gần như đi ngang (+~0,3% so cuối 2025); 6/4/2026 ở 25.107.","Nearly flat in H1-2026 (~+0.3% vs end-2025); 25,107 on 6 Apr.",U["cr_2026_06"]),
  h("2026-07-28","25.293","25.306",None,"neutral","Vượt đỉnh lịch sử 8/2025; biên độ ±5%: sàn 24.040,7, trần 26.571,3.","Broke the Aug-2025 record; ±5% band 24,040.7–26,571.3.",U["cr_2026_0728"]),
  h("2026-08-13","~25.321 (cuối 7)","25.566",None,"neutral","Tăng 245 đồng so với cuối tháng 7 — điều chỉnh mạnh nhất năm.","+245 dong vs end-July — steepest adjustment of the year.",U["cr_2026_0813"]),
  h("2026-09-15","25.566","25.617",None,"neutral","Kỷ lục mới 25.617.","New record 25,617.",U["cr_2026_0915"]),
  h("2026-10-02","25.627 (30/9)","25.636",None,"neutral","Tỷ giá tham khảo Sở GD NHNN 24.405–26.867.","SBV transaction-office reference rates 24,405–26,867.",U["cr_2026_1002"]),
 ],
 "series": {"dates": ["2023-12-29","2024-12-31","2025-12-31","2026-04-06","2026-06-30","2026-07-28","2026-08-13","2026-09-15","2026-09-30","2026-10-02"],
            "values": [23866,24335,25121,25107,25206,25306,25566,25617,25627,25636]},
 "why_vi": "Neo kỳ vọng tỷ giá; điều chỉnh dần theo USD-index, cán cân thương mại và chênh lệch lãi suất.",
 "why_en": "Anchors FX expectations; adjusted gradually with the USD index, trade balance and rate differentials."
})
instruments.append({
 "id": "fx_band", "group": "fx",
 "name_vi": "Biên độ tỷ giá USD/VND", "name_en": "USD/VND trading band",
 "current_vi": "±5% quanh tỷ giá trung tâm (từ 17/10/2022); không thay đổi trong 2023–10/2026",
 "current_en": "±5% around the central rate (since 17 Oct 2022); unchanged 2023–Oct 2026",
 "unit": "%", "current_value": 5.0, "effective": "2022-10-17",
 "history": [
  h("2026-07-28","±5%","±5%",None,"neutral","Biên độ hiện hành: sàn 24.040,7 – trần 26.571,3 (ngày 28/7/2026).","Band on 28 Jul 2026: 24,040.7–26,571.3.",U["cr_2026_0728"]),
 ],
 "series": None,
 "why_vi": "Cho phép tỷ giá linh hoạt hơn để hấp thụ cú sốc bên ngoài mà không phải can thiệp liên tục.",
 "why_en": "Lets the rate absorb external shocks without constant intervention."
})
instruments.append({
 "id": "fx_intervention", "group": "fx",
 "name_vi": "Can thiệp ngoại hối: bán giao ngay, bán kỳ hạn (có quyền hủy ngang), hoán đổi; dự trữ ngoại hối", "name_en": "FX intervention: spot sales, cancellable forward sales, swaps; reserves",
 "current_vi": "Bán USD kỳ hạn 180 ngày có quyền hủy ngang (giá 26.850, 3/2026); hoán đổi USD/VND 7 ngày (tối đa 2 tỷ USD, 21/9/2026). Dự trữ ~87,6 tỷ USD (18/6/2026). Dự thảo sửa NĐ 50/2014 bổ sung quyền chọn ngoại tệ, quyền chọn vàng",
 "current_en": "180-day cancellable forward USD sales (at 26,850, Mar-2026); 7-day USD/VND swaps (up to USD 2bn, 21 Sep 2026). Reserves ~USD 87.6bn (18 Jun 2026). Draft amendment to Decree 50/2014 adds currency & gold options",
 "unit": "text", "current_value": None, "effective": "2026-09-21",
 "history": [
  h("2024-04-19","—","bán giao ngay 25.450",None,"tightening","Công khai bán USD can thiệp cho NH có trạng thái âm, giá 25.450; cả năm 2024 ước >9 tỷ USD.","Open spot USD sales to short-position banks at 25,450; >USD 9bn sold in 2024 (market est.).",U["fx_2024_spot"]),
  h("2025-08-25","—","bán kỳ hạn 180 ngày giá 26.550",None,"tightening","Bán kỳ hạn có quyền hủy ngang 2–3 lần.","Forward sales with 2–3 cancellation rights.",U["fx_2025_fwd"]),
  h("2026-03-24","—","bán kỳ hạn 180 ngày giá 26.850",None,"tightening","Giá cao hơn giao ngay ~1,8% (26.360); USD tự do lập đỉnh 28.055 (30–31/3).","~1.8% above spot (26,360); free-market USD peaked at 28,055 (30–31 Mar).",U["fx_2026_fwd"]),
  h("2026-06-24","mua/bán, hoán đổi","+ quyền chọn ngoại tệ, quyền chọn vàng","Dự thảo sửa NĐ 50/2014/NĐ-CP","neutral","Dự trữ ~87,6 tỷ USD (18/6/2026), giảm 21,6% so đỉnh 111,8 tỷ (1/2022).","Reserves ~USD 87.6bn (18 Jun 2026), −21.6% from the USD 111.8bn peak (Jan 2022).",U["fx_options"]),
  h("2026-09-21","—","hoán đổi USD/VND 7 ngày ≤2 tỷ USD",None,"easing","Mở lại kênh hoán đổi (thực chất bơm VND ngắn hạn).","Reopened swap window (effectively short-term VND liquidity).",U["omo_2026_sep"]),
 ],
 "series": None,
 "why_vi": "Ổn định thị trường ngoại tệ, hạn chế đầu cơ; bán kỳ hạn giúp phát tín hiệu mà tiết kiệm dự trữ.",
 "why_en": "Stabilises the FX market; cancellable forwards signal resolve while conserving reserves."
})
instruments.append({
 "id": "usd_deposit_rate", "group": "fx",
 "name_vi": "Trần lãi suất tiền gửi USD", "name_en": "USD deposit-rate cap",
 "current_vi": "0%/năm cho cá nhân và tổ chức (QĐ 2589/QĐ-NHNN, từ 18/12/2015) — không đổi 2023–2026",
 "current_en": "0% p.a. for individuals and organisations (Decision 2589/QĐ-NHNN, since 18 Dec 2015) — unchanged 2023–2026",
 "unit": "%", "current_value": 0.0, "effective": "2015-12-18",
 "history": [],
 "series": None,
 "why_vi": "Chống đô la hóa, khuyến khích nắm giữ VND.",
 "why_en": "De-dollarisation: discourages holding USD deposits."
})
instruments.append({
 "id": "gold_policy", "group": "fx",
 "name_vi": "Quản lý thị trường vàng (vàng miếng, nhập khẩu, sàn giao dịch)", "name_en": "Gold-market policy (bars, imports, exchange)",
 "current_vi": "NĐ 232/2025 bỏ độc quyền SX vàng miếng từ 10/10/2025; TT 34/2025 hướng dẫn cấp phép; 11 hồ sơ xin phép (4/2026), NHNN vẫn đang xem xét (8/2026) — chưa thấy giấy phép nào được công bố; sàn vàng: dự thảo NQ thí điểm (chưa vận hành)",
 "current_en": "Decree 232/2025 ended the gold-bar monopoly from 10 Oct 2025; Circular 34/2025 licensing rules; 11 applications (Apr-2026) still under review (Aug-2026) — no licence found published; gold exchange: pilot resolution drafted, not yet operating",
 "unit": "text", "current_value": None, "effective": "2025-10-10",
 "history": [
  h("2024-04-22","—","đấu thầu vàng miếng SJC",None,"neutral","9 phiên, 6 thành công, ~48.000 lượng.","9 auctions (6 successful), ~48k taels.",U["gold_2024"]),
  h("2024-06-03","đấu thầu","bán trực tiếp qua 4 NHTMNN + SJC",None,"neutral","Đến 29/10/2024: 44 phiên, 305.600 lượng (~11,46 tấn).","By 29 Oct 2024: 44 sessions, 305.6k taels (~11.46t).",U["gold_2024_jun"]),
  h("2025-10-10","Nhà nước độc quyền SX vàng miếng","bỏ độc quyền; cấp phép DN (vốn ≥1.000 tỷ) và NH (vốn ≥50.000 tỷ)","232/2025/NĐ-CP (26/8/2025)","easing","Nhập khẩu vàng nguyên liệu theo hạn mức hằng năm.","Raw-gold imports under annual quotas.",U["nd232"]),
  h("2025-10-10","—","thủ tục cấp phép SX vàng miếng, nhập khẩu","34/2025/TT-NHNN","neutral","Hồ sơ hạn mức trước 15/11, NHNN cấp trước 15/12 hằng năm.","Quota applications by 15 Nov; SBV allocates by 15 Dec each year.",U["tt34_2025"]),
  h("2026-02-12","trạng thái vàng ≤2% vốn","≤5% với NH được SX vàng miếng","82/2025/TT-NHNN","easing","Nới trạng thái vàng cho NH sản xuất vàng miếng.","Higher gold position limit for bar-producing banks.",U["tt82_2025"]),
  h("2026-02-08","—","yêu cầu sớm lập sàn vàng","NQ 23/NQ-CP (7/2/2026)","neutral","Chính phủ yêu cầu đưa sàn giao dịch vàng vào hoạt động; mốc 2/2026 bị lỡ.","Government ordered a gold exchange to start; Feb-2026 target missed.",U["gold_exchange_pm"]),
  h("2026-04-14","9 hồ sơ (cuối 2025)","11 hồ sơ",None,"neutral","NHNN tiếp nhận 11 hồ sơ xin phép SX vàng miếng.","SBV has 11 applications for bar-production licences.",U["gold_11apps"]),
  h("2026-08-13","—","tiếp tục xem xét cấp phép, phân bổ hạn mức nhập khẩu",None,"neutral","Định hướng NHNN tại cuộc họp với Thủ tướng.","SBV orientation at the PM meeting.",U["sbv_0814"]),
 ],
 "series": None,
 "why_vi": "Thu hẹp chênh lệch giá vàng trong nước–thế giới, chống vàng hóa và áp lực lên tỷ giá.",
 "why_en": "Narrow the domestic–world gold premium; curb gold hoarding and its FX pressure."
})

# ---------------------------------------------------------------- PRUDENTIAL
instruments.append({
 "id": "reserve_requirement", "group": "prudential",
 "name_vi": "Tỷ lệ dự trữ bắt buộc", "name_en": "Reserve requirement ratios",
 "current_vi": "VND: 3% (KKH & <12 tháng), 1% (≥12 tháng); ngoại tệ: 8% / 6%. NH nhận chuyển giao bắt buộc (VCB, MB, VPBank, HDBank) được giảm 50% → 1,5% / 0,5%",
 "current_en": "VND: 3% (demand & <12m), 1% (≥12m); FX: 8% / 6%. Banks taking over weak banks (VCB, MB, VPBank, HDBank) get 50% off → 1.5% / 0.5%",
 "unit": "%", "current_value": 3.0, "effective": "2025-10-01",
 "history": [
  h("2025-10-01","3% / 1%","1,5% / 0,5% cho NH nhận chuyển giao bắt buộc","23/2025/TT-NHNN (12/8/2025, sửa TT 30/2019)","easing","Thực thi quyền giảm 50% DTBB theo Luật TCTD 2024.","Implements the 50% RRR cut granted by the 2024 Credit Institutions Law.",U["rrr_tt23"]),
  h("2026-02-10","—","hướng dẫn áp dụng cho 4 NH",None,"easing","Văn bản hướng dẫn của Cục QLGS: VCB, MB, VPBank, HDBank (≈23% dư nợ toàn hệ thống).","Supervision Dept guidance for VCB, MB, VPBank, HDBank (~23% of system loans).",U["rrr_2026"]),
 ],
 "series": None,
 "why_vi": "Công cụ thanh khoản/tiền tệ; ưu đãi giảm DTBB để bù đắp chi phí tái cơ cấu ngân hàng yếu kém.",
 "why_en": "Liquidity tool; the 50% cut compensates banks for absorbing failed lenders."
})
instruments.append({
 "id": "ldr_cap", "group": "prudential",
 "name_vi": "Trần tỷ lệ dư nợ cho vay/tổng tiền gửi (LDR)", "name_en": "Loan-to-deposit ratio (LDR) cap",
 "current_vi": "85% (TT 22/2019) — nâng lên 95% từ 1/12/2026 theo TT 50/2026/TT-NHNN (30/9/2026); NH đạt LCR & NSFR ≥100% được miễn LDR",
 "current_en": "85% (Circular 22/2019) — rises to 95% from 1 Dec 2026 under Circular 50/2026/TT-NHNN (30 Sep 2026); banks meeting LCR & NSFR ≥100% exempt",
 "unit": "%", "current_value": 85.0, "effective": "2020-01-01",
 "history": [
  h("2026-12-01","85%","95%","50/2026/TT-NHNN (ký 30/9/2026)","easing","Đã ban hành, hiệu lực 1/12/2026; loại khỏi dư nợ phần cho vay TCTD khác và tái cấp vốn không nhằm hỗ trợ thanh khoản; bổ sung nguồn vốn vay nước ngoài, giấy tờ có giá.","Issued; effective 1 Dec 2026; excludes interbank loans and non-liquidity refinancing from loans; adds foreign borrowing and valuable papers to funding.",U["tt50"]),
 ],
 "series": {"dates": ["2023-01-01","2026-10-05","2026-12-01"], "values": [85,85,95]},
 "why_vi": "Giới hạn rủi ro thanh khoản; nới để giảm áp lực huy động, hạ nhiệt cuộc đua lãi suất và mở dư địa tín dụng.",
 "why_en": "Limits liquidity risk; loosened to ease funding pressure and the deposit-rate race and open credit room."
})
instruments.append({
 "id": "treasury_deposits_ldr", "group": "prudential",
 "name_vi": "Tỷ lệ tiền gửi có kỳ hạn của Kho bạc Nhà nước được tính vào tổng tiền gửi (mẫu số LDR)", "name_en": "Share of State Treasury term deposits counted in LDR funding",
 "current_vi": "50% (QĐ 1743/QĐ-NHNN, từ 1/8/2026 đến 31/7/2028)", "current_en": "50% (Decision 1743/QĐ-NHNN, 1 Aug 2026 – 31 Jul 2028)",
 "unit": "%", "current_value": 50.0, "effective": "2026-08-01",
 "history": [
  h("2024-01-01","50%","40%","26/2022/TT-NHNN (lộ trình)","tightening","Lộ trình: 2023 50%, 2024 40%, 2025 20%, từ 2026 0%.","Roadmap: 50% (2023), 40% (2024), 20% (2025), 0% from 2026.",U["kbnn_roadmap"]),
  h("2025-01-01","40%","20%","26/2022/TT-NHNN (lộ trình)","tightening","Theo lộ trình.","Per roadmap.",U["kbnn_roadmap"]),
  h("2026-01-01","20%","0%","26/2022/TT-NHNN (lộ trình)","tightening","Loại trừ toàn bộ tiền gửi có kỳ hạn KBNN — gây áp lực lên Big 4.","All Treasury term deposits excluded — pressure on the Big 4.",U["kbnn_roadmap"]),
  h("2026-05-15","0%","20%","08/2026/TT-NHNN","easing","Cho tính 20% tiền gửi có kỳ hạn KBNN; ước giảm LDR NHTMNN 1,1–1,5 điểm %.","20% allowed back; SOCB LDRs down ~1.1–1.5pp.",U["tt08_2026"]),
  h("2026-08-01","20%","50%","1743/QĐ-NHNN (30/7/2026)","easing","Thống đốc nâng tỷ lệ theo thẩm quyền tại TT 22/2019 sửa bởi TT 25/2026; áp dụng 2 năm.","Governor raised the share under Circ. 22/2019 as amended by Circ. 25/2026; valid 2 years.",U["qd1743"]),
 ],
 "series": {"dates": ["2023-01-01","2024-01-01","2025-01-01","2026-01-01","2026-05-15","2026-08-01"], "values": [50,40,20,0,20,50]},
 "why_vi": "Nới nguồn vốn tính LDR cho NH giữ nhiều tiền KBNN (Big 4, MB), giảm áp lực tăng lãi suất huy động.",
 "why_en": "Adds countable funding for banks holding Treasury cash (Big 4, MB), easing deposit-rate pressure.",
 "notes_vi": "TT 50/2026 (hiệu lực 1/12/2026) cũng quy định cách tính tiền gửi KBNN; các nguồn báo chí mô tả khác nhau (loại trừ 80% hay tính 80%) — cần đối chiếu văn bản gốc.",
 "notes_en": "Circular 50/2026 (eff. 1 Dec 2026) re-specifies Treasury deposits; press summaries conflict (80% excluded vs 80% counted) — check the gazette text."
})
instruments.append({
 "id": "st_funds_mlt", "group": "prudential",
 "name_vi": "Tỷ lệ tối đa nguồn vốn ngắn hạn cho vay trung, dài hạn", "name_en": "Max share of short-term funds used for medium/long-term loans",
 "current_vi": "40% (TT 25/2026/TT-NHNN ngày 22/6/2026, hiệu lực 1/7/2026)", "current_en": "40% (Circular 25/2026/TT-NHNN of 22 Jun 2026, effective 1 Jul 2026)",
 "unit": "%", "current_value": 40.0, "effective": "2026-07-01",
 "history": [
  h("2023-10-01","34%","30%","08/2020/TT-NHNN (lộ trình)","tightening","Bước cuối lộ trình 40%→37%→34%→30%.","Final step of the 40→37→34→30% roadmap.",U["tt25_2026"]),
  h("2026-07-01","30%","40%","25/2026/TT-NHNN","easing","Nới trở lại 40%; bãi bỏ TT 08/2020 và TT 08/2026.","Relaxed back to 40%; repeals Circulars 08/2020 and 08/2026.",U["tt25_2026"]),
 ],
 "series": {"dates": ["2023-01-01","2023-10-01","2026-07-01"], "values": [34,30,40]},
 "why_vi": "Hạn chế rủi ro kỳ hạn; nới để tăng vốn trung dài hạn cho hạ tầng, sản xuất (mục tiêu tăng trưởng 2 con số).",
 "why_en": "Caps maturity mismatch; loosened to fund long-term investment for double-digit growth.",
 "notes_vi": "TT 50/2026 thay thế TT 22/2019 từ 1/12/2026 — chưa xác minh tỷ lệ này có giữ 40% hay không.",
 "notes_en": "Circular 50/2026 replaces Circular 22/2019 from 1 Dec 2026 — not verified whether 40% is retained."
})
instruments.append({
 "id": "lcr_nsfr", "group": "prudential",
 "name_vi": "Chuẩn thanh khoản Basel III: LCR, NSFR, tỷ lệ đòn bẩy", "name_en": "Basel III liquidity: LCR, NSFR, leverage ratio",
 "current_vi": "Chưa bắt buộc. Từ 1/10/2028: LCR ≥50%, +10 điểm %/năm, 100% từ 1/10/2033; NSFR 90% (2028), 95% (2029), 100% (1/10/2030). Tỷ lệ đòn bẩy áp dụng khi Thống đốc quyết định. Được áp dụng tự nguyện sớm",
 "current_en": "Not yet binding. From 1 Oct 2028: LCR ≥50%, +10pp/yr, 100% by 1 Oct 2033; NSFR 90% (2028), 95% (2029), 100% (1 Oct 2030). Leverage ratio when the Governor decides. Voluntary early adoption allowed",
 "unit": "text", "current_value": None, "effective": "2026-12-01",
 "history": [
  h("2026-12-01","không có","LCR/NSFR theo lộ trình","50/2026/TT-NHNN (ký 30/9/2026)","neutral","NH tự nguyện áp dụng sớm phải duy trì 100% trong 90 ngày; đạt 100% cả hai được miễn trần LDR.","Early adopters must hold 100% for 90 days; meeting both at 100% exempts from LDR cap.",U["tt50b"]),
 ],
 "series": None,
 "why_vi": "Chuyển từ trần hành chính (LDR) sang quản trị thanh khoản theo chất lượng nguồn vốn (Basel III).",
 "why_en": "Shift from administrative caps (LDR) to funding-quality liquidity rules (Basel III)."
})
instruments.append({
 "id": "car_basel3", "group": "prudential",
 "name_vi": "Tỷ lệ an toàn vốn (CAR) theo Basel III", "name_en": "Capital adequacy (CAR) — Basel III",
 "current_vi": "TT 14/2025/TT-NHNN (30/6/2025, hiệu lực 15/9/2025): CET1 ≥4,5%, Tier 1 ≥6%, CAR ≥8% + đệm bảo toàn vốn lên tới 2,5% (CAR tổng 10,5%), đệm phản chu kỳ 0–2,5%; bắt buộc đầy đủ từ 1/1/2030",
 "current_en": "Circular 14/2025/TT-NHNN (30 Jun 2025, eff. 15 Sep 2025): CET1 ≥4.5%, Tier 1 ≥6%, CAR ≥8% + conservation buffer up to 2.5% (10.5% total), countercyclical 0–2.5%; fully binding from 1 Jan 2030",
 "unit": "%", "current_value": 8.0, "effective": "2025-09-15",
 "history": [
  h("2025-09-15","CAR ≥8% (TT 41/2016)","CET1/Tier1/CAR + đệm vốn","14/2025/TT-NHNN","tightening","Thay TT 41/2016; đệm bảo toàn vốn tăng dần 0,625%→2,5%.","Replaces Circ. 41/2016; conservation buffer phased 0.625%→2.5%.",U["tt14_2025"]),
 ],
 "series": None,
 "why_vi": "Tăng sức chống chịu vốn, buộc NH tăng vốn tự có trước khi mở rộng tín dụng.",
 "why_en": "Raises loss-absorbing capital, forcing banks to build capital before expanding credit."
})
instruments.append({
 "id": "re_risk_weights", "group": "prudential",
 "name_vi": "Hệ số rủi ro cho vay bất động sản / miễn trừ nhà ở xã hội", "name_en": "Real-estate risk weights / social-housing carve-outs",
 "current_vi": "Cho vay kinh doanh BĐS: 200%; tài trợ dự án BĐS khu công nghiệp: 160%; cho vay mua nhà ở xã hội/nhà theo chương trình Chính phủ: 20–50% (theo LTV, DSC)",
 "current_en": "Real-estate business lending: 200%; industrial-park RE project finance: 160%; social housing / government-programme housing loans: 20–50% (by LTV, DSC)",
 "unit": "text", "current_value": None, "effective": "2025-09-15",
 "history": [
  h("2024-07-01","25–100% (NOXH); 200% (BĐS KCN)","tối đa 50% (NOXH, tối thiểu 20%); 160% (BĐS KCN); 50% (nông nghiệp NT)","22/2023/TT-NHNN (17/1/2024)","easing","Sửa TT 41/2016: giảm hệ số rủi ro cho NOXH, BĐS khu công nghiệp, nông nghiệp nông thôn.","Amends Circ. 41/2016: lower weights for social housing, industrial-park RE, agri/rural loans.",U["tt22_2023"]),
  h("2025-09-15","—","giữ 200% KD BĐS; NOXH 20–50%","14/2025/TT-NHNN","neutral","Khung CAR mới giữ phân biệt BĐS thương mại vs NOXH.","New CAR framework keeps the commercial-RE vs social-housing split.",U["rw_2025"]),
 ],
 "series": None,
 "why_vi": "Hạn chế dòng vốn vào BĐS đầu cơ, ưu đãi vốn cho NOXH và khu công nghiệp.",
 "why_en": "Discourage speculative property lending; favour social housing and industrial parks."
})
instruments.append({
 "id": "debt_restructuring", "group": "prudential",
 "name_vi": "Cơ cấu lại thời hạn trả nợ, giữ nguyên nhóm nợ", "name_en": "Debt restructuring with loan-group forbearance",
 "current_vi": "Không còn chương trình chung đang hiệu lực: TT 02/2023 hết hạn 31/12/2024; TT 53/2024 (bão Yagi) hết hạn 31/12/2025; chưa thấy gia hạn năm 2026",
 "current_en": "No general scheme in force: Circular 02/2023 expired 31 Dec 2024; Circular 53/2024 (Typhoon Yagi) expired 31 Dec 2025; no 2026 extension found",
 "unit": "text", "current_value": None, "effective": "2025-12-31",
 "history": [
  h("2023-04-24","không có","cơ cấu nợ, giữ nhóm đến 30/6/2024","02/2023/TT-NHNN (23/4/2023)","easing","Cơ cấu tối đa 12 tháng; đến cuối 2023 ~188.000 KH, 183.500 tỷ.","Up to 12-month restructuring; ~188k borrowers, VND 183.5tn by end-2023.",U["tt02_2023"]),
  h("2024-06-18","đến 30/6/2024","đến 31/12/2024","06/2024/TT-NHNN","easing","Gia hạn thêm 6 tháng.","Extended 6 months.",U["tt02_ext"]),
  h("2024-12-04","—","cơ cấu nợ bão Yagi đến 31/12/2025","53/2024/TT-NHNN","easing","26 tỉnh; không giới hạn số lần; trả nợ cuối cùng tới 31/12/2027.","26 provinces; unlimited times; final repayment up to 31 Dec 2027.",U["tt53_2024"]),
 ],
 "series": None,
 "why_vi": "Giãn nợ cho DN/hộ gặp khó, tránh nợ xấu tăng đột biến; đổi lại che giấu rủi ro.",
 "why_en": "Relieves stressed borrowers and avoids an NPL spike, at the cost of masking risk."
})
instruments.append({
 "id": "loan_classification", "group": "prudential",
 "name_vi": "Phân loại nợ & trích lập dự phòng", "name_en": "Loan classification & provisioning",
 "current_vi": "TT 31/2024/TT-NHNN (5 nhóm nợ, hiệu lực 1/7/2024) + NĐ 86/2024/NĐ-CP về trích lập dự phòng (hiệu lực 11/7/2024)",
 "current_en": "Circular 31/2024/TT-NHNN (5 loan groups, eff. 1 Jul 2024) + Decree 86/2024/NĐ-CP on provisioning (eff. 11 Jul 2024)",
 "unit": "text", "current_value": None, "effective": "2024-07-11",
 "history": [
  h("2024-07-01","TT 11/2021","TT 31/2024","31/2024/TT-NHNN","neutral","Thay TT 11/2021 về phân loại tài sản có.","Replaces Circ. 11/2021 on asset classification.",U["tt31_2024"]),
  h("2024-07-11","TT 11/2021","NĐ 86/2024","86/2024/NĐ-CP","neutral","Mức trích, phương pháp trích lập dự phòng cụ thể và chung.","Specific and general provisioning rates/methods.",U["nd86_2024"]),
 ],
 "series": None,
 "why_vi": "Đo lường đúng nợ xấu và đệm dự phòng sau khi hết cơ chế giữ nhóm nợ.",
 "why_en": "Measure NPLs properly and build provisions once forbearance ends."
})

# ---------------------------------------------------------------- CREDIT
instruments.append({
 "id": "credit_growth_target", "group": "credit",
 "name_vi": "Chỉ tiêu tăng trưởng tín dụng năm", "name_en": "Annual credit-growth target",
 "current_vi": "2026: ~15% (giảm từ ~16% năm 2025). Thực hiện đến 30/9/2026: +11,59% so cuối 2025 (+16,6% so cùng kỳ); 20,75 triệu tỷ đồng",
 "current_en": "2026: ~15% (down from ~16% in 2025). Actual to 30 Sep 2026: +11.59% YTD (+16.6% y/y); VND 20.75 quadrillion",
 "unit": "%", "current_value": 15.0, "effective": "2026-01-01",
 "history": [
  h("2023-07-10","—","định hướng 14–15%",None,"neutral","Thực hiện cả năm 2023: 13,71%.","2023 actual: 13.71%.",U["cg_2023"]),
  h("2024-01-02","—","~15%",None,"easing","Giao hết chỉ tiêu từ đầu năm; điều chỉnh 28/8 và 28/11/2024; thực hiện 15,08%.","Full quota assigned up-front; adjusted 28 Aug & 28 Nov 2024; actual 15.08%.",U["cg_2024"]),
  h("2025-01-20","15%","~16%","01/CT-NHNN","easing","Giao theo xếp hạng 2023 × hệ số chung.","Allocated by 2023 rating × common coefficient.",U["cg_2025"]),
  h("2025-07-31","—","nâng chỉ tiêu cho TCTD",None,"easing","NHNN tăng chỉ tiêu tín dụng giữa năm; thực hiện cả năm ~19,01%.","Mid-year quota increases; 2025 actual ~19.01%.",U["cg_2025_jul"]),
  h("2026-01-10","~16%","~15%",None,"tightening","Trần từng NH = điểm xếp hạng 2024 × 2,6% (2025: 3,5%); quý I không quá 25% chỉ tiêu năm.","Bank cap = 2024 rating score × 2.6% (2025: 3.5%); Q1 ≤25% of annual quota.",U["cg_2026"]),
 ],
 "series": {"dates": ["2023-12-31","2024-12-31","2025-12-31","2026-09-30"], "values": [13.71,15.08,19.01,11.59]},
 "series_label_vi": "Tăng trưởng tín dụng thực tế so với cuối năm trước (%)",
 "series_label_en": "Actual credit growth vs previous year-end (%)",
 "why_vi": "Kiểm soát tổng phương tiện thanh toán, lạm phát và rủi ro; phân bổ theo xếp hạng NH.",
 "why_en": "Controls money growth, inflation and risk; allocated by bank ratings."
})
instruments.append({
 "id": "credit_room_mechanism", "group": "credit",
 "name_vi": "Cơ chế 'room' tín dụng & loại trừ khỏi hạn mức", "name_en": "Credit-quota ('room') mechanism & carve-outs",
 "current_vi": "Vẫn giao room theo xếp hạng; 2026 loại trừ khỏi hạn mức: dư nợ mới 18 dự án trọng điểm (Vingroup, Sun Group, Masterise); NOXH, KCN/KCX, nhà cho thuê không tính vào trần tăng trưởng tín dụng BĐS; từ 9/2026 thêm nhà hàng, khách sạn, khu nghỉ dưỡng. Room 2027 sẽ gắn với tuân thủ giảm lãi suất; lộ trình bỏ room chưa hoàn tất",
 "current_en": "Quotas still allocated by rating; 2026 carve-outs: new lending to 18 strategic projects (Vingroup, Sun Group, Masterise); social housing, industrial/EPZ, rental housing excluded from the real-estate growth cap; from Sep-2026 also hotels/resorts. 2027 quotas tied to rate-cut compliance; removal of quotas not yet done",
 "unit": "text", "current_value": None, "effective": "2026-09-17",
 "history": [
  h("2025-08-07","room hành chính","yêu cầu thí điểm bỏ room từ 2026",None,"easing","Thủ tướng yêu cầu NHNN khẩn trương xây dựng lộ trình, thí điểm bỏ giao chỉ tiêu tín dụng.","PM told SBV to pilot removal of credit quotas.",U["room_pm"]),
  h("2025-12-31","—","NOXH, KCN/KCX không tính vào trần tăng trưởng tín dụng BĐS (25 TCTD)","4551/NHNN-CSTT","easing","Áp dụng năm 2026; tín dụng BĐS mỗi TCTD không vượt tốc độ tăng tín dụng chung.","For 2026; each bank's RE credit growth capped at its overall growth.",U["cv4551"]),
  h("2026-06-23","—","loại trừ dư nợ mới của 18 dự án trọng điểm khỏi room",None,"easing","Tổng nhu cầu vốn ~752.138 tỷ đồng (sân bay Phú Quốc, đường sắt Bến Thành–Cần Giờ, Hà Nội–Quảng Ninh...).","~VND 752tn demand (Phu Quoc airport, Ben Thanh–Can Gio rail, Hanoi–Quang Ninh rail...).",U["room_18b"]),
  h("2026-09-17","—","nhà hàng, khách sạn, khu du lịch, nghỉ dưỡng không tính vào trần tín dụng BĐS",None,"easing","Áp dụng cho 25 TCTD, năm 2026.","25 banks, for 2026.",U["room_hotel"]),
  h("2026-08-13","—","room 2027 giảm với NH không tuân thủ giảm LS",None,"neutral","Room trở thành công cụ kỷ luật lãi suất.","Quotas used to enforce rate guidance.",U["sbv_0814"]),
 ],
 "series": None,
 "why_vi": "Dẫn vốn vào dự án lớn, NOXH, KCN, du lịch để đạt tăng trưởng 2 con số mà vẫn siết BĐS đầu cơ.",
 "why_en": "Channel credit to megaprojects, social housing, industrial parks and tourism for double-digit growth while curbing speculative RE."
})

# ---------------------------------------------------------------- TARGETED
instruments.append({
 "id": "social_housing_pkg", "group": "targeted",
 "name_vi": "Chương trình tín dụng nhà ở xã hội (120.000 → 145.000 tỷ đồng)", "name_en": "Social-housing credit programme (VND 120tn → 145tn)",
 "current_vi": "145.000 tỷ, 9 NH; LS ưu đãi người mua (gồm người <35 tuổi) 6,5%/năm 5 năm đầu, 7,5% 10 năm sau (H2-2026, CV 5340/NHNN-CSTT); chủ đầu tư ~7%. Giải ngân ~12.440 tỷ (8,5%) đến 5/2026",
 "current_en": "VND 145tn, 9 banks; buyer rate (incl. under-35s) 6.5% for first 5 years, 7.5% next 10 (H2-2026, letter 5340/NHNN-CSTT); developers ~7%. Disbursed ~VND 12.44tn (8.5%) by May 2026",
 "unit": "%", "current_value": 6.5, "effective": "2026-07-01",
 "history": [
  h("2024-07-01","8% (CĐT) / 7,5% (người mua)","7% (CĐT) / 6,5% (người mua)",None,"easing","Giảm 1 điểm %; thấp hơn ~2 điểm % so với ban đầu (khởi động 2023 theo NQ 33/NQ-CP).","Cut 1pp; ~2pp below launch rates (launched 2023 under Resolution 33/NQ-CP).",U["sh_2024"]),
  h("2024-10-23","120.000 tỷ","145.000 tỷ",None,"easing","Thêm 5 NHTMCP (HDBank, MB, VPBank, Techcombank, TPBank) mỗi NH 5.000 tỷ.","Five joint-stock banks added VND 5tn each.",U["sh_2024"]),
  h("2025-07-01","6,1%","5,9%",None,"easing","LS ưu đãi người mua (chương trình người trẻ <35 tuổi khởi động 31/5/2025 ở 6,1%).","Buyer rate (under-35 programme launched 31 May 2025 at 6.1%).",U["sh_2026"]),
  h("2026-01-01","5,9%","5,6%",None,"easing","Kỳ H1-2026.","H1-2026 period.",U["sh_2026"]),
  h("2026-07-01","5,6%","6,5%","5340/NHNN-CSTT","tightening","Tăng 0,9 điểm %; vẫn thấp hơn 2 điểm % LS trung hạn bình quân của 4 NHTMNN.","+0.9pp; still 2pp below the Big-4 average medium-term rate.",U["sh_2026"]),
 ],
 "series": {"dates": ["2025-05-31","2025-07-01","2026-01-01","2026-07-01"], "values": [6.1,5.9,5.6,6.5]},
 "series_label_vi": "LS ưu đãi người mua NOXH (5 năm đầu, %)",
 "series_label_en": "Preferential social-housing buyer rate (first 5 years, %)",
 "why_vi": "Tăng cung và khả năng mua nhà ở xã hội, nhà ở công nhân.",
 "why_en": "Boost supply and affordability of social/worker housing."
})
instruments.append({
 "id": "agri_forestry_fishery_pkg", "group": "targeted",
 "name_vi": "Chương trình tín dụng nông, lâm, thủy sản", "name_en": "Agriculture-forestry-fisheries credit programme",
 "current_vi": "Quy mô >100.000 tỷ đồng (mở rộng từ lâm sản, thủy sản sang toàn bộ nông–lâm–thủy sản), 15 NH tham gia; LS thấp hơn bình quân",
 "current_en": "Over VND 100tn (expanded from forestry/fisheries to all agri-forestry-fisheries), 15 banks; below-average rates",
 "unit": "VND", "current_value": 100000, "effective": "2025-04-15",
 "history": [
  h("2025-04-15","~60.000 tỷ (lâm sản, thủy sản)","trên 100.000 tỷ (nông, lâm, thủy sản)","2756/NHNN-TD; NQ 46/NQ-CP (8/3/2025)","easing","Chương trình khởi động 7/2023 với 15.000 tỷ, nâng 30.000 tỷ (cuối 2023), 60.000 tỷ (2024).","Launched Jul-2023 at VND 15tn, raised to 30tn (end-2023), 60tn (2024).",U["agri"]),
 ],
 "series": None,
 "why_vi": "Hỗ trợ ngành xuất khẩu nông–thủy sản gặp khó khăn đơn hàng.",
 "why_en": "Support agri/fisheries exporters hit by weak orders."
})
instruments.append({
 "id": "infra_tech_pkg", "group": "targeted",
 "name_vi": "Chương trình tín dụng 500.000 tỷ đồng cho hạ tầng điện, giao thông, công nghệ chiến lược", "name_en": "VND 500tn programme for power, transport and strategic-tech infrastructure",
 "current_vi": "Tối đa 500.000 tỷ đến 2030; giai đoạn 2025–2026 ~100.000 tỷ; LS thấp hơn bình quân 1–1,5%/năm tối thiểu 2 năm; 21 NH; vướng danh mục dự án từ các Bộ",
 "current_en": "Up to VND 500tn through 2030; ~100tn in 2025–26; 1–1.5pp below average rates for ≥2 years; 21 banks; held up by ministry project lists",
 "unit": "VND", "current_value": 500000, "effective": "2025-12-12",
 "history": [
  h("2025-05-29","—","21 NH đăng ký đủ 500.000 tỷ",None,"easing","Thủ tướng đề nghị giảm LS ít nhất 1,5%.","PM asked for cuts of at least 1.5pp.",U["infra_2025"]),
  h("2025-12-12","—","hướng dẫn triển khai 2 giai đoạn",None,"easing","GĐ1 2025–2026 ~20% quy mô; GĐ2 2027–2030.","Phase 1 2025–26 ~20% of size; phase 2 2027–30.",U["infra_dec"]),
 ],
 "series": None,
 "why_vi": "Tài trợ hạ tầng chiến lược và chuyển đổi số theo NQ 57.",
 "why_en": "Finance strategic infrastructure and digital transformation."
})
instruments.append({
 "id": "sme_growth_pkg", "group": "targeted",
 "name_vi": "Chương trình tín dụng hướng đến động lực tăng trưởng & DNNVV (giảm ≥1 điểm %)", "name_en": "Credit programme for growth drivers & SMEs (≥1pp below average)",
 "current_vi": "LS VND thấp hơn tối thiểu 1%/năm so với LS bình quân cùng kỳ hạn của chính NH; 19 NH, ~409.000 tỷ; mới 10 NH giải ngân, dư nợ ~18.600 tỷ (cuối 9/2026); đến hết 2028",
 "current_en": "VND rates ≥1pp below each bank's own average for the tenor; 19 banks, ~VND 409tn; only 10 banks disbursing, ~VND 18.6tn outstanding (end-Sep 2026); through 2028",
 "unit": "VND", "current_value": 409000, "effective": "2026-08-07",
 "history": [
  h("2026-08-07","—","giảm ≥1%/năm cho DNNVV & lĩnh vực ưu tiên","7125/NHNN-TD","easing","Ban đầu 4 NHTMNN 220.000 tỷ (Agribank 70.000; BIDV, VCB, VietinBank mỗi NH 50.000).","Initially 4 SOCBs VND 220tn (Agribank 70tn; BIDV, VCB, VietinBank 50tn each).",U["cv7125"]),
  h("2026-10-03","220.000 tỷ (4 NH)","~409.000 tỷ (19 NH)",None,"easing","Ưu đãi 1–3,6 điểm %; chương trình lúa chất lượng cao ~5.000 tỷ.","Discounts 1–3.6pp; high-quality rice programme ~VND 5tn.",U["cg_2026_sep"]),
 ],
 "series": None,
 "why_vi": "Hạ chi phí vốn cho DNNVV, nông nghiệp, CN hỗ trợ, công nghệ cao, xuất khẩu, kinh tế số, AI, bán dẫn, dự án xanh.",
 "why_en": "Cut funding costs for SMEs, agri, supporting industry, high-tech, exports, digital, AI, semiconductors, green projects."
})
instruments.append({
 "id": "green_credit", "group": "targeted",
 "name_vi": "Tín dụng xanh & hỗ trợ lãi suất 2% cho dự án xanh", "name_en": "Green credit & 2% interest subsidy for green projects",
 "current_vi": "Dư nợ xanh ~828.000 tỷ (82 TCTD); NĐ hỗ trợ LS 2%/năm từ NSNN cho DN tư nhân, hộ kinh doanh làm dự án xanh/kinh tế tuần hoàn: đang trình Chính phủ (6/2026), chưa ban hành",
 "current_en": "Green loans ~VND 828tn (82 institutions); decree for a 2pp budget-funded subsidy for private firms/households on green & circular projects: submitted (Jun-2026), not yet issued",
 "unit": "text", "current_value": None, "effective": "2026-06-09",
 "history": [
  h("2026-06-09","—","dự thảo NĐ hỗ trợ LS 2%",None,"easing","Phó Thống đốc Nguyễn Ngọc Cảnh công bố tại diễn đàn NHNN–GIZ.","Announced by Deputy Governor Nguyen Ngoc Canh at SBV–GIZ forum.",U["green_2026"]),
  h("2026-06-23","—","hộ kinh doanh sắp được hỗ trợ 2%",None,"easing","Chờ tiêu chí dự án xanh từ Bộ NN&MT.","Awaits green-project criteria from the environment ministry.",U["green_2026b"]),
 ],
 "series": None,
 "why_vi": "Thúc đẩy chuyển đổi xanh, ESG.",
 "why_en": "Promote green transition and ESG."
})

# ---------------------------------------------------------------- RESTRUCTURING
instruments.append({
 "id": "compulsory_transfer", "group": "restructuring",
 "name_vi": "Chuyển giao bắt buộc ngân hàng yếu kém & xử lý SCB", "name_en": "Compulsory transfer of weak banks & SCB resolution",
 "current_vi": "Đã chuyển giao 4 NH: CBBank→Vietcombank, OceanBank→MB (17/10/2024); GPBank→VPBank, DongA Bank→HDBank (17/1/2025). SCB: chưa có phương án cuối cùng; 30/6/2026 NHNN trình phương án xử lý tài sản thu hồi",
 "current_en": "4 banks transferred: CBBank→Vietcombank, OceanBank→MB (17 Oct 2024); GPBank→VPBank, DongA Bank→HDBank (17 Jan 2025). SCB: no final plan; 30 Jun 2026 SBV submitted a plan for recovered assets",
 "unit": "text", "current_value": None, "effective": "2025-01-17",
 "history": [
  h("2024-10-17","kiểm soát đặc biệt","chuyển giao bắt buộc CB→VCB, OceanBank→MB",None,"neutral","Thành NH TNHH MTV 100% vốn của VCB/MB.","Became 100%-owned subsidiaries of VCB/MB.",U["ct_2024"]),
  h("2025-01-17","kiểm soát đặc biệt","GPBank→VPBank, DongA Bank→HDBank",None,"neutral","Quyền lợi người gửi tiền được bảo đảm.","Depositors protected.",U["ct_2025"]),
  h("2025-05-08","—","yêu cầu hoàn thiện phương án SCB","NQ 124/NQ-CP","neutral","Hạn trình phương án SCB trước 15/9/2025.","SCB plan due by 15 Sep 2025.",U["scb_2025"]),
  h("2026-06-30","—","trình phương án xử lý tài sản thu hồi nợ của SCB",None,"neutral","Các NH nhận chuyển giao phần lớn đã có lãi hoặc giảm lỗ.","Transferred banks mostly profitable or loss-narrowing.",U["scb_2026"]),
 ],
 "series": None,
 "why_vi": "Xử lý dứt điểm NH yếu kém, bảo vệ người gửi tiền, ổn định hệ thống.",
 "why_en": "Resolve failed banks, protect depositors, stabilise the system."
})
instruments.append({
 "id": "special_loans", "group": "restructuring",
 "name_vi": "Cho vay đặc biệt", "name_en": "Special loans (emergency/resolution lending)",
 "current_vi": "TT 35/2025/TT-NHNN (14/10/2025) sửa bởi TT 02/2026/TT-NHNN (31/3/2026, hiệu lực 1/5/2026): NHNN, TCTD khác, BHTG cho vay đặc biệt; NHNN cho BHTG vay 0%, không TSBĐ để chi trả bảo hiểm",
 "current_en": "Circular 35/2025/TT-NHNN (14 Oct 2025) amended by Circular 02/2026/TT-NHNN (31 Mar 2026, eff. 1 May 2026): SBV, other banks and DIV can extend special loans; SBV may lend to DIV at 0% unsecured to pay claims",
 "unit": "text", "current_value": None, "effective": "2026-05-01",
 "history": [
  h("2025-10-14","khung cũ","cho vay đặc biệt: rút tiền hàng loạt, phục hồi, chuyển giao bắt buộc","35/2025/TT-NHNN","neutral","Theo Luật TCTD sửa đổi 2025 (LS 0%, có/không TSBĐ).","Under the 2025 amended CI Law (0% rate, secured or unsecured).",U["tt35_2025"]),
  h("2026-05-01","—","bổ sung BHTG là bên vay/bên cho vay đặc biệt","02/2026/TT-NHNN","easing","NHNN xét trong 20 ngày làm việc.","SBV decides within 20 working days.",U["tt02_2026"]),
 ],
 "series": None,
 "why_vi": "Mạng an toàn chống rút tiền hàng loạt và hỗ trợ tái cơ cấu.",
 "why_en": "Safety net against bank runs and for restructuring."
})
instruments.append({
 "id": "credit_institutions_law", "group": "restructuring",
 "name_vi": "Luật Các tổ chức tín dụng 2024 & sửa đổi 2025", "name_en": "Law on Credit Institutions 2024 & 2025 amendment",
 "current_vi": "Luật 32/2024/QH15 (hiệu lực 1/7/2024) sửa bởi Luật 96/2025/QH15 (hiệu lực 15/10/2025): quyền thu giữ TSBĐ (Điều 198a), cho vay đặc biệt 0%, can thiệp sớm",
 "current_en": "Law 32/2024/QH15 (eff. 1 Jul 2024) amended by Law 96/2025/QH15 (eff. 15 Oct 2025): collateral seizure right (Art. 198a), 0% special loans, early intervention",
 "unit": "text", "current_value": None, "effective": "2025-10-15",
 "history": [
  h("2024-07-01","Luật TCTD 2010","Luật 32/2024/QH15 (thông qua 18/1/2024)","32/2024/QH15","neutral","Hạn chế sở hữu chéo, ưu đãi NH nhận chuyển giao (giảm 50% DTBB...).","Tighter cross-ownership limits; incentives for acquirers (50% RRR cut...).",U["law32"]),
  h("2025-10-15","—","bổ sung thu giữ TSBĐ, cho vay đặc biệt 0%","96/2025/QH15 (27/6/2025)","neutral","Luật hóa quyền thu giữ TSBĐ từ NQ 42.","Codifies collateral seizure from Resolution 42.",U["law96"]),
 ],
 "series": None,
 "why_vi": "Khung pháp lý xử lý nợ xấu, NH yếu kém, an toàn hệ thống.",
 "why_en": "Legal basis for NPL resolution, weak-bank handling and stability."
})
instruments.append({
 "id": "pcf_restructuring", "group": "restructuring",
 "name_vi": "Cơ cấu lại hệ thống Quỹ tín dụng nhân dân & Ngân hàng Hợp tác xã 2026–2030", "name_en": "People's credit funds & Co-opBank restructuring 2026–2030",
 "current_vi": "QĐ 1266/QĐ-NHNN (11/6/2026): mỗi xã tối đa 1 QTDND, hoàn tất sắp xếp trước 2035",
 "current_en": "Decision 1266/QĐ-NHNN (11 Jun 2026): max one people's credit fund per commune, consolidation by 2035",
 "unit": "text", "current_value": None, "effective": "2026-06-11",
 "history": [
  h("2026-06-11","—","đề án cơ cấu lại QTDND 2026–2030","1266/QĐ-NHNN; 1267/QĐ-NHNN (kế hoạch hành động)","neutral","Quy định mới về địa bàn, vốn, quản trị, kết nối hệ thống.","New rules on service area, capital, governance, system links.",U["pcf_1266"]),
 ],
 "series": None,
 "why_vi": "Làm lành mạnh khu vực tài chính vi mô sau sáp nhập đơn vị hành chính cấp xã.",
 "why_en": "Clean up the micro-finance tier after commune mergers."
})

# ---------------------------------------------------------------- OTHER
instruments.append({
 "id": "deposit_insurance", "group": "other",
 "name_vi": "Hạn mức chi trả bảo hiểm tiền gửi", "name_en": "Deposit-insurance payout limit",
 "current_vi": "350 triệu đồng/người/TCTD (TT 05/2026/TT-NHNN, hiệu lực 13/7/2026), gồm gốc và lãi",
 "current_en": "VND 350m per depositor per institution (Circular 05/2026/TT-NHNN, eff. 13 Jul 2026), principal + interest",
 "unit": "VND", "current_value": 350000000, "effective": "2026-07-13",
 "history": [
  h("2026-07-13","125 triệu (QĐ 32/2021/QĐ-TTg)","350 triệu","05/2026/TT-NHNN","easing","Tăng 180%, theo Luật BHTG 2025.","+180%, under the 2025 Deposit Insurance Law.",U["di_350"]),
 ],
 "series": {"dates": ["2023-01-01","2026-07-13"], "values": [125000000,350000000]},
 "why_vi": "Củng cố niềm tin người gửi tiền, giảm nguy cơ rút tiền hàng loạt.",
 "why_en": "Bolsters depositor confidence and reduces run risk."
})
instruments.append({
 "id": "payments_biometrics", "group": "other",
 "name_vi": "Thanh toán không dùng tiền mặt & xác thực sinh trắc học", "name_en": "Cashless payments & biometric authentication",
 "current_vi": "QĐ 2345 (từ 1/7/2024): sinh trắc học với giao dịch >10 triệu hoặc >20 triệu/ngày; TT 45/2025 (từ 5/1/2026) đối chiếu sinh trắc học trực tiếp khi phát hành thẻ; TT 77/2025 (từ 1/7/2026) quy định mới an toàn thanh toán trực tuyến; 162,9 triệu hồ sơ KH cá nhân đã đối chiếu (26/6/2026)",
 "current_en": "Decision 2345 (from 1 Jul 2024): biometrics for transfers >VND 10m or >20m/day; Circular 45/2025 (from 5 Jan 2026): in-person biometric check for card issuance; Circular 77/2025 (from 1 Jul 2026): new online-payment security rules; 162.9m individual profiles verified (26 Jun 2026)",
 "unit": "text", "current_value": None, "effective": "2026-07-01",
 "history": [
  h("2024-07-01","OTP","sinh trắc học >10 triệu/giao dịch, >20 triệu/ngày","2345/QĐ-NHNN (18/12/2023)","neutral","Chống lừa đảo, tài khoản 'mượn danh'.","Anti-fraud, anti-mule accounts.",U["qd2345"]),
  h("2026-01-05","—","đối chiếu sinh trắc học khi phát hành thẻ","45/2025/TT-NHNN (sửa TT 18/2024)","neutral","Bỏ yêu cầu cư trú 12 tháng với người nước ngoài.","Drops 12-month residency rule for foreigners.",U["tt45_2025"]),
  h("2026-07-01","—","quy định mới thanh toán trực tuyến (hạn mức sinh trắc học cho DN, hộ KD)","77/2025/TT-NHNN","neutral","Mở rộng xác thực sinh trắc học sang tài khoản tổ chức.","Extends biometric checks to business accounts.",U["tt77_2025"]),
 ],
 "series": None,
 "why_vi": "An toàn thanh toán số, chống gian lận; hỗ trợ không tiền mặt.",
 "why_en": "Digital-payment security and anti-fraud; supports cashless adoption."
})

stance_vi = ("Lãi suất điều hành giữ nguyên từ 6/2023 (tái cấp vốn 4,5%), OMO 4,5% từ 12/2025; thay vì cắt lãi suất, NHNN nới mạnh các công cụ an toàn vĩ mô "
             "(LDR 95% từ 1/12/2026, vốn ngắn hạn cho vay trung dài hạn 40%, tính 50% tiền gửi KBNN) và dùng chỉ đạo hành chính, gói tín dụng ưu đãi để kéo lãi suất xuống, "
             "trong khi để tỷ giá trung tâm tăng ~2% từ đầu năm và tuyên bố dư địa nới lỏng tiền tệ 'rất hạn chế'.")
stance_en = ("Policy rates unchanged since Jun-2023 (refinancing 4.5%), OMO at 4.5% since Dec-2025; instead of rate cuts the SBV is loosening macro-prudential limits "
             "(LDR cap 95% from 1 Dec 2026, 40% short-term funds for long loans, 50% of Treasury deposits counted) and leaning on moral suasion and subsidised credit "
             "programmes to push rates down, while letting the central rate rise ~2% YTD and calling its room for monetary easing 'very limited'.")

news = [
 {"date":"2026-10-04","title_vi":"Thông tư 50 tạo bước chuyển mới trong quản lý rủi ro ngân hàng","title_en":"Circular 50 marks a shift in bank risk management","url":U["tt50c"],"instrument_ids":["ldr_cap","lcr_nsfr"]},
 {"date":"2026-10-03","title_vi":"Tỷ giá USD hôm nay 3/10/2026: Đồng USD giảm nhẹ sau báo cáo việc làm Mỹ (tỷ giá trung tâm 25.636)","title_en":"USD rate 3 Oct 2026: dollar eases after US jobs data (central rate 25,636)","url":U["cr_2026_1002"],"instrument_ids":["central_rate","fx_band"]},
 {"date":"2026-10-03","title_vi":"Dư nợ tín dụng tăng 11,59%, gần 409.000 tỷ đồng cho vay ưu đãi","title_en":"Credit up 11.59% YTD; nearly VND 409tn in preferential lending","url":U["cg_2026_sep"],"instrument_ids":["credit_growth_target","sme_growth_pkg"]},
 {"date":"2026-10-03","title_vi":"Giá vàng tăng 42,67% trong 9 tháng đầu năm 2026","title_en":"Gold prices up 42.67% in the first nine months of 2026","url":"https://soha.vn/gia-vang-tang-4267-trong-9-thang-dau-nam-2026-198261003091901729.htm","instrument_ids":["gold_policy"]},
 {"date":"2026-10-01","title_vi":"NHNN nâng trần tỷ lệ cho vay trên tiền gửi lên 95%, công bố hai chỉ tiêu quản lý thanh khoản LCR và NSFR","title_en":"SBV lifts LDR cap to 95%, unveils LCR and NSFR liquidity ratios","url":U["tt50"],"instrument_ids":["ldr_cap","lcr_nsfr","treasury_deposits_ldr"]},
 {"date":"2026-10-01","title_vi":"Lãi suất ngân hàng hôm nay 1/10/2026: Ai được hưởng lãi suất 10%/năm?","title_en":"Bank rates 1 Oct 2026: who gets 10% a year?","url":"https://vietnamnet.vn/lai-suat-ngan-hang-hom-nay-1-10-2026-ai-duoc-huong-lai-suat-10-nam-2560542.html","instrument_ids":["deposit_cap_1_6m","moral_suasion"]},
 {"date":"2026-09-28","title_vi":"Tuần 21-25/09: NHNN đảo chiều bơm ròng hơn 34,000 tỷ","title_en":"Week 21–25 Sep: SBV swings to a net VND 34tn injection","url":U["omo_2026_w39"],"instrument_ids":["omo_rate"]},
 {"date":"2026-09-25","title_vi":"Vốn đắt, doanh nghiệp bất động sản khó tiếp cận gói tín dụng 145.000 tỷ","title_en":"Costly funds keep developers from the VND 145tn social-housing package","url":U["sh_sep"],"instrument_ids":["social_housing_pkg"]},
 {"date":"2026-09-23","title_vi":"Thanh khoản 'căng', Ngân hàng Nhà nước kích hoạt lại các 'van' hỗ trợ","title_en":"Liquidity tight, SBV reopens support valves (OMO, USD/VND swaps)","url":U["omo_2026_sep"],"instrument_ids":["omo_rate","fx_intervention"]},
 {"date":"2026-09-17","title_vi":"NHNN nới tín dụng cho nhà hàng, khách sạn, khu nghỉ dưỡng","title_en":"SBV eases credit for restaurants, hotels and resorts","url":U["room_hotel"],"instrument_ids":["credit_room_mechanism"]},
 {"date":"2026-09-15","title_vi":"Ngân hàng Nhà nước nâng tỷ giá trung tâm lên kỷ lục 25,617 VND/USD","title_en":"SBV lifts central rate to a record 25,617 VND/USD","url":U["cr_2026_0915"],"instrument_ids":["central_rate"]},
 {"date":"2026-08-22","title_vi":"Thông tin mới nhất về SCB và các ngân hàng được chuyển giao bắt buộc","title_en":"Latest on SCB and the compulsorily transferred banks","url":U["scb_2026"],"instrument_ids":["compulsory_transfer"]},
 {"date":"2026-08-14","title_vi":"Ngân hàng Nhà nước nêu định hướng điều hành lãi suất, tỷ giá, tín dụng và thị trường vàng thời gian tới","title_en":"SBV sets out guidance on rates, FX, credit and gold","url":U["sbv_0814"],"instrument_ids":["moral_suasion","credit_room_mechanism","gold_policy","fx_intervention"]},
 {"date":"2026-08-13","title_vi":"Giá USD hôm nay 13.8.2026: Tỷ giá trung tâm tiếp tục tăng mạnh","title_en":"USD 13 Aug 2026: central rate keeps climbing sharply","url":U["cr_2026_0813"],"instrument_ids":["central_rate"]},
 {"date":"2026-08-10","title_vi":"Ngân hàng Nhà nước chỉ đạo giảm tối thiểu 1% lãi suất cho vay với doanh nghiệp nhỏ và vừa","title_en":"SBV orders lending rates at least 1pp lower for SMEs","url":"https://thethaovanhoa.vn/ngan-hang-nha-nuoc-chi-dao-giam-toi-thieu-1-lai-suat-cho-vay-voi-doanh-nghiep-nho-va-vua-2026081016421037.htm","instrument_ids":["sme_growth_pkg","moral_suasion"]},
 {"date":"2026-07-31","title_vi":"Nóng: Ngân hàng Nhà nước nâng tỷ lệ tính tiền gửi Kho bạc Nhà nước vào LDR lên 50%, áp dụng chính thức từ ngày mai (1/8)","title_en":"SBV raises share of Treasury deposits counted in LDR to 50% from 1 Aug","url":U["qd1743"],"instrument_ids":["treasury_deposits_ldr","ldr_cap"]},
 {"date":"2026-07-28","title_vi":"Tỷ giá trung tâm phá kỷ lục sau nửa năm 'lặng sóng'","title_en":"Central rate breaks record after a calm half-year","url":U["cr_2026_0728"],"instrument_ids":["central_rate","fx_band"]},
 {"date":"2026-07-20","title_vi":"Sau 3 năm, 145.000 tỷ dành cho nhà ở xã hội giải ngân được bao nhiêu?","title_en":"Three years on, how much of the VND 145tn social-housing package is disbursed?","url":U["sh_disb"],"instrument_ids":["social_housing_pkg"]},
 {"date":"2026-07-17","title_vi":"Chính thức: Nâng hạn mức trả bảo hiểm tiền gửi tối đa lên 350 triệu đồng","title_en":"Deposit-insurance payout limit officially raised to VND 350m","url":"https://cafef.vn/chinh-thuc-nang-han-muc-tra-bao-hiem-tien-gui-toi-da-len-350-trieu-dong-188260717152845123.chn","instrument_ids":["deposit_insurance"]},
 {"date":"2026-07-03","title_vi":"NHNN nói về việc loại trừ 18 dự án trọng điểm khỏi room tín dụng","title_en":"SBV explains excluding 18 key projects from credit quotas","url":U["room_18"],"instrument_ids":["credit_room_mechanism"]},
 {"date":"2026-07-03","title_vi":"Ngân hàng Nhà nước khẳng định không đảo chiều chính sách tiền tệ","title_en":"SBV says it is not reversing monetary policy","url":U["noreversal"],"instrument_ids":["refinancing_rate","omo_rate","moral_suasion"]},
 {"date":"2026-07-02","title_vi":"Ngân hàng Nhà nước họp báo về kết quả điều hành chính sách tiền tệ và hoạt động ngân hàng 6 tháng đầu năm 2026","title_en":"SBV press briefing on H1-2026 monetary policy and banking results","url":U["h1_2026"],"instrument_ids":["refinancing_rate","credit_growth_target","credit_room_mechanism","payments_biometrics"]},
]

data = {"as_of": "2026-10-05", "stance_vi": stance_vi, "stance_en": stance_en,
        "instruments": instruments, "news": news}

with open(OUT, "w", encoding="utf-8") as f:
    json.dump(data, f, ensure_ascii=False, indent=1)
print("wrote", OUT, len(instruments), "instruments,", len(news), "news")
