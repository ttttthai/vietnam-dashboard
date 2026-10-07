"""Sample deck: every visualization style of vn_deck, built in Vietnamese with the dashboard's real figures.

Values are copied from the dashboard's data files as of 2026-10-07 (data/economy.json, finance.json, society.json,
simulation.json, policy.json, research/release_calendar.json) and labelled with publisher + period on every slide.
Every number in a headline or note is computed from these arrays at build time (never typed).

    python3 example_deck.py [out.pptx]          -> example.pptx next to this script by default
"""
import os
import sys
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from vn_deck import Deck, BLUE, ORANGE, AQUA, YELLOW, GREEN, VIOLET, INK, REDS  # noqa: E402

TODAY = date(2026, 10, 7)
d = Deck(lang='vi')
n = d.num
yrs = lambda a, b: [str(y) for y in range(a, b + 1)]

# ── data (copied from data/*.json; see each source line) ──
GDP_G = [6.42, 6.41, 5.5, 5.55, 6.42, 6.99, 6.69, 6.94, 7.47, 7.36, 2.87, 2.55, 8.54, 4.98, 7.04, 8.02]   # 2010–2025, NSO
IMF_G = [7.1, 6.7, 6.2, 5.6, 5.4]                    # 2026–2030, IMF WEO Apr-2026
Q26 = {'Q1': 8.15, 'Q2': 8.81, 'Q3': 9.95}            # GDP yoy by quarter 2026, NSO 03/10/2026
G9M, TARGET_G = 9.01, 10.0                            # 9M-2026 yoy; NQ 244/2025/QH15 target ">= 10%"
CPI24 = [2.89, 2.77, 2.94, 3.63, 2.91, 3.13, 3.12, 3.24, 3.57, 3.19, 3.24, 3.38,
         3.25, 3.58, 3.48, 2.53, 3.35, 4.65, 5.46, 5.6, 4.69, 4.45, 4.89, 5.08]                     # T10/24–T9/26 yoy
CR_G = [18.0, 18.25, 18.28, 13.89, 13.65, 12.2, 13.6, 14.2, 13.78, 15.09, 19.07]                   # 2015–2025 credit
DEP_G = [13.59, 17.75, 14.59, 12.29, 14.54, 13.3, 10.3, 5.99, 13.2, 10.66, 15.42]                  # deposits
M2_G = [14.91, 17.88, 14.26, 12.7, 14.8, 14.5, 10.7, 6.2, 12.5, 11.97, 15.7]                       # M2
CR_YTD_SEP25, CR_YTD_SEP26 = 13.86, 11.59
EXP9M, EXP9M_YOY, FDI9M, FDI9M_YOY = 434.3, 24.5, 21.07, 12.1
EXP_Y = [162.0167, 176.5808, 215.1186, 243.6968, 264.2672, 282.6289, 336.1668, 371.7154, 354.721, 405.9354, 474.9978]  # 2015–2025
FDI_DIS = [11.0003, 11.0001, 10.0466, 11.5, 12.5, 14.5, 15.8, 17.5, 19.1, 20.38, 19.98, 19.74, 22.396, 23.183,
           25.351, 27.62]                                                                            # 2010–2025 bn USD
INV = {'state': [556380, 587110, 616459, 630142, 643094, 734735, 719293, 832062, 968187, 1030281, 1233578],
       'nonstate': [881760, 988651, 1173901, 1361156, 1557937, 1605050, 1719354, 1868642, 1917451, 2063810, 2237087],
       'fdi': [318100, 351103, 396200, 435102, 469440, 463280, 458081, 521975, 550204, 608621, 679842]}   # 2015–2025 bn VND
SHARE15 = {'C': 59.33, 'I': 32.11, 'G': 10.65, 'NX': 0.93}
SHARE25 = {'C': 53.21, 'I': 30.8, 'G': 9.38, 'NX': 6.15}
GDP24 = {'C': 6164.6, 'G': 1017.8, 'I': 3520.4, 'NX': 801.8, 'D': 5.7, 'GDP': 11510.3}             # tn VND, current prices
GDP25 = {'C': 6836.4, 'G': 1204.6, 'I': 3956.4, 'NX': 790.4, 'D': 59.8, 'GDP': 12847.6}
ACT = {  # share of GDP %, current prices, NSO PxWeb V03.02/V03.04-05 (2010–2025)
    'Chế biến, chế tạo': [17.13, 18.69, 20.27, 20.69, 20.37, 20.96, 21.49, 22.63, 23.37, 23.79, 23.95, 24.46, 24.66, 24.11, 24.33, 24.53],
    'Nông, lâm, thủy sản': [15.38, 16.26, 16.2, 15.22, 14.88, 14.47, 13.82, 12.93, 12.31, 11.78, 12.66, 12.6, 11.88, 11.87, 12.03, 11.65],
    'Bán buôn, bán lẻ': [7.4, 7.62, 8.2, 8.45, 8.76, 9.14, 9.2, 9.17, 9.15, 9.34, 9.59, 9.38, 9.61, 9.76, 9.75, 9.72],
    'Khai khoáng': [6.8, 7.79, 7.52, 6.84, 6.62, 4.25, 3.33, 3.28, 3.51, 3.0, 2.4, 2.49, 3.27, 2.82, 2.43, 2.1],
    'Xây dựng': [6.27, 5.46, 5.31, 5.09, 5.05, 5.44, 5.52, 5.58, 5.71, 5.86, 6.0, 5.99, 6.06, 6.11, 6.03, 6.13],
    'Vận tải, kho bãi': [4.72, 4.6, 4.64, 4.85, 4.94, 4.93, 5.07, 4.88, 4.95, 5.03, 4.81, 4.45, 4.67, 4.94, 5.18, 5.3],
    'Tài chính, ngân hàng': [4.49, 4.4, 4.38, 4.49, 4.34, 4.47, 4.46, 4.4, 4.35, 4.39, 4.45, 4.73, 4.74, 4.82, 4.81, 4.8],
    'Bất động sản': [5.14, 4.92, 4.67, 4.61, 4.5, 4.51, 4.49, 4.29, 4.02, 3.91, 3.84, 3.65, 3.48, 3.59, 3.49, 3.49],
    'Điện, khí đốt': [2.39, 2.22, 2.31, 2.51, 2.8, 3.12, 3.28, 3.41, 3.46, 3.66, 3.9, 3.94, 4.0, 4.05, 4.23, 4.4]}
CPI_M = ['T10/25', 'T11/25', 'T12/25', 'T1/26', 'T2/26', 'T3/26', 'T4/26', 'T5/26', 'T6/26', 'T7/26', 'T8/26', 'T9/26']
CPI_GRP = {  # MoM % by CPI group, NSO "Biểu 1 – Cả nước"
    'Hàng ăn & DV ăn uống': [0.59, 0.95, 0.75, 0.2, 2.02, -0.59, 0.58, -0.14, -0.07, -0.07, -0.03, 0.19],
    'Đồ uống & thuốc lá': [0.12, 0.1, 0.06, 0.58, 1.18, 0.37, 0.85, 0.21, 0.13, 0.18, 0.12, 0.35],
    'May mặc, giày dép': [0.18, 0.12, 0.2, 0.25, 0.55, 0.01, 0.52, 0.13, -0.09, 0.16, 0.11, 0.04],
    'Nhà ở, điện, nước': [0.01, -0.1, 0.06, 0.7, 0.56, 0.77, 2.59, 0.96, 0.46, -0.07, 0.1, 0.28],
    'Thiết bị gia đình': [0.2, 0.17, 0.16, 0.26, 0.57, 0.33, 0.78, 0.17, 0.17, 0.14, 0.19, 0.08],
    'Thuốc & y tế': [0.04, 0.06, 0.03, 0.19, 0.14, 0.38, 0.13, 0.1, 0.03, 0.07, 0.05, 0.24],
    'Giao thông': [-0.81, 1.07, -1.08, -2.32, 1.23, 12.85, -0.81, 0.83, -4.85, -2.02, 4.09, 3.97],
    'Bưu chính, viễn thông': [0.03, -0.06, 0.02, -0.15, 0.01, 0.18, 0.17, 0.04, 0.04, 0.1, 0.08, -0.14],
    'Giáo dục': [0.51, 0.05, 0.01, 0.05, 0.09, 0.1, 0.08, 0.03, 0.01, 0.24, 0.47, 1.41],
    'Văn hoá, giải trí': [0.06, 0.01, -0.12, 0.07, 1.36, -0.05, 0.63, 0.48, 0.66, 0.52, 0.03, -0.28],
    'Hàng hoá, DV khác': [0.43, 0.3, 0.19, 0.41, 1.3, 0.13, 0.64, 0.14, -0.06, 1.96, 0.15, 0.1]}
FSI = {  # IMF FSI, deposit takers, 2015–2025 (%)
    'npl': [2.76, 2.64, 2.11, 2.13, 1.77, 1.87, 1.6, 2.32, 5.41, 4.85, 4.21],
    'car': [12.77, 12.64, 12.1, 11.95, 11.79, 11.14, 11.31, 11.55, 11.74, 12.33, 12.06],
    'roa': [0.64, 0.69, 0.8, 1.15, 1.21, 1.2, 1.54, 1.55, -1.03, 1.43, 1.49],
    'roe': [5.63, 6.77, 8.23, 12.3, 12.39, 11.91, 14.94, 14.52, -20.1, 18.43, 19.95],
    'prov': [45.73, 46.37, 41.34, 50.69, 52.49, 44.5, 64.07, 50.6, 66.84, 68.62, 66.77]}
DEP12_M = ['1/25', '2/25', '3/25', '4/25', '5/25', '6/25', '7/25', '8/25', '9/25', '10/25', '11/25', '12/25',
           '1/26', '2/26', '3/26', '4/26', '5/26', '6/26', '7/26', '8/26', '9/26']
DEP12 = [4.6] * 9 + [None] * 3 + [5.2, 5.5, 5.9, 5.9, 5.9, 5.9, 5.9, 5.9, 5.9]                   # VCB 12M posted rate
PRJ_M = ['10/26', '11/26', '12/26', '1/27', '2/27', '3/27']
PRJ = {'base': [6.0723, 6.2584, 6.4592, 6.6765, 6.8765, 7.0843], 'low': [5.9194, 6.0422, 6.1944, 6.3707, 6.5346, 6.7098],
       'high': [6.2251, 6.4746, 6.724, 6.9822, 7.2183, 7.4588]}                                   # SIM.projection2
TORNADO = [('VN-Index, lợi suất 12 tháng', 6.6757, 7.6033), ('NHNN thay đổi lãi suất điều hành', 6.8033, 7.3653),
           ('Tỷ giá trung tâm, thay đổi 12 tháng', 6.9093, 7.4093), ('Chênh lệch vàng SJC – thế giới', 6.8929, 7.3179),
           ('Đà tăng giá căn hộ Hà Nội', 6.9143, 7.2643), ('Lạm phát CPI (y/y)', 6.9263, 7.2263),
           ('Tín dụng khác tăng thêm', 6.9419, 7.2267), ('Cán cân thương mại hàng hoá', 7.1795, 6.9629)]
TORNADO_BASE = 7.0843
BUD_Y = yrs(2018, 2026)
BUD_BASIS = ['final'] * 7 + ['estimate', 'plan']
EXP = {'total': [1435435, 1526893, 1709524, 1708088, 1750790, 1936912, 2148477, 2401500, 3159106],
       'dev': [393304, 421845, 576432, 540046, 615640, 723839, 720862, 732000, 1120227],
       'rec': [931859, 994582, 1013449, 1061316, 1034250, 1117207, 1320495, 1553000, 1808996],
       'int': [106584, 107065, 106466, 101778, 96084, 89323, 102571, 109400, 121131]}                  # bn VND
REV24 = {'Thu nội địa': 1723643, 'Dầu thô': 58646, 'Xuất nhập khẩu': 272006, 'Viện trợ': 3249}    # 2024 final
REV25 = {'Thu nội địa': 2279900, 'Xuất nhập khẩu': 319800, 'Dầu thô': 48000}                       # 2025 estimate
BOP25 = {'Hàng hoá (ròng)': 475.059 - 433.166, 'Dịch vụ (ròng)': 30.307 - 40.537,
         'Thu nhập sơ cấp (ròng)': 5.388 - 19.787, 'Thu nhập thứ cấp (ròng)': 19.766 - 3.895,
         'FDI (ròng)': 21.29, 'Đầu tư gián tiếp (ròng)': -4.931, 'Đầu tư khác (ròng)': -19.769,
         'Lỗi và sai sót': -25.863}
CA25, RES25 = 33.135, 3.862
BOP = {'Cán cân vãng lai': [-2.041, 0.625, -1.649, 5.899, 13.101, 15.06, -4.628, 1.402, 25.793, 30.175, 33.135],
       'FDI ròng': [10.7, 11.6, 13.62, 14.902, 15.635, 15.42, 15.341, 15.226, 20.05, 19.57, 21.29],
       'Đầu tư gián tiếp ròng': [-0.065, 0.228, 2.069, 3.021, 2.997, -1.256, 0.281, 1.512, -1.189, -5.713, -4.931],
       'Đầu tư khác ròng': [-9.668, -1.101, 4.339, -9.457, 0.742, -5.68, 15.273, -7.268, -21.699, -21.886, -19.769],
       'Lỗi và sai sót': [-4.958, -2.962, -5.835, -8.334, -9.221, -6.912, -11.977, -33.617, -17.347, -31.313, -25.863],
       'Thay đổi dự trữ': [-6.032, 8.39, 12.544, 6.031, 23.254, 16.632, 14.29, -22.745, 5.608, -9.167, 3.862]}
# provinces (34 after the 2025 merger): GRDP growth 2025 %, GRDP per capita 2025 USD, 9M-2026 growth %, tile position
PROV = [  # name, short, growth25, pc_usd25, g9m26, col, row, panel
    ('Lai Châu', 'Lai Châu', 7.52, 3180, 9.62, 1, 0, 0), ('Lào Cai', 'Lào Cai', 8.14, 3390, 9.09, 2, 0, 0),
    ('Tuyên Quang', 'Tuyên Quang', 6.4, 2186, 8.04, 3, 0, 0), ('Cao Bằng', 'Cao Bằng', 7.22, 1985, 7.96, 4, 0, 0),
    ('Điện Biên', 'Điện Biên', 7.34, 2137, None, 0, 1, 0), ('Sơn La', 'Sơn La', 8.03, 2858, 5.59, 1, 1, 0),
    ('Phú Thọ', 'Phú Thọ', 10.52, 4458, 10.01, 2, 1, 0), ('Thái Nguyên', 'Thái Nguyên', 6.33, 4500, 11.24, 3, 1, 0),
    ('Lạng Sơn', 'Lạng Sơn', 8.06, 2841, 7.52, 4, 1, 0), ('Hà Nội', 'Hà Nội', 8.16, 7178, 8.85, 2, 2, 0),
    ('Bắc Ninh', 'Bắc Ninh', 10.27, 5852, 11.81, 3, 2, 0), ('Quảng Ninh', 'Quảng Ninh', 11.89, 10458, 12.54, 4, 2, 0),
    ('Ninh Bình', 'Ninh Bình', 10.65, 3570, 11.31, 2, 3, 0), ('Hưng Yên', 'Hưng Yên', 8.78, 3958, 11.22, 3, 3, 0),
    ('Hải Phòng', 'Hải Phòng', 11.81, 7086, 12.08, 4, 3, 0), ('Thanh Hóa', 'Thanh Hóa', 8.27, 3543, 8.54, 2, 4, 0),
    ('Nghệ An', 'Nghệ An', 8.44, 2707, 9.75, 2, 5, 0), ('Hà Tĩnh', 'Hà Tĩnh', 8.78, 3624, 12.36, 3, 5, 0),
    ('Quảng Trị', 'Quảng Trị', 8.0, 3099, None, 2, 6, 1), ('Huế', 'Huế', 8.5, 3014, None, 3, 7, 1),
    ('Đà Nẵng', 'Đà Nẵng', 9.18, 4430, 10.31, 4, 7, 1), ('Quảng Ngãi', 'Quảng Ngãi', 10.02, 4078, 9.16, 3, 8, 1),
    ('Gia Lai', 'Gia Lai', 7.2, 3405, None, 4, 8, 1), ('Đắk Lắk', 'Đắk Lắk', 6.68, 3225, 8.22, 3, 9, 1),
    ('Khánh Hòa', 'Khánh Hòa', 7.11, 4417, 11.21, 4, 9, 1), ('Lâm Đồng', 'Lâm Đồng', 6.42, 4214, 8.51, 3, 10, 1),
    ('Tây Ninh', 'Tây Ninh', 9.52, 4629, 10.3, 1, 10, 1), ('Đồng Nai', 'Đồng Nai', 9.63, 6037, 10.01, 2, 10, 1),
    ('TP. Hồ Chí Minh', 'TP.HCM', 7.53, 8622, 9.06, 2, 11, 1), ('Đồng Tháp', 'Đồng Tháp', 7.38, 3364, 7.44, 1, 11, 1),
    ('An Giang', 'An Giang', 8.39, 3212, 9.23, 0, 11, 1), ('Vĩnh Long', 'Vĩnh Long', 5.84, 3299, 7.32, 1, 12, 1),
    ('Cần Thơ', 'Cần Thơ', 7.23, 3800, 7.89, 0, 12, 1), ('Cà Mau', 'Cà Mau', 7.23, 3147, 7.37, 0, 13, 1)]
STRUCT25 = {'Hà Nội': (1.86, 22.25, 66.44, 9.45), 'TP. Hồ Chí Minh': (1.7, 35.2, 52.2, 10.9),
            'Quảng Ninh': (4.6, 46.2, 37.8, 11.4), 'Hải Phòng': (4.55, 53.96, 35.2, 6.29),
            'Đồng Nai': (12.13, 54.97, 26.42, 6.48), 'Lai Châu': (11.0, 53.64, 30.0, 5.36),
            'Bắc Ninh': (6.52, 71.13, 19.41, 2.94), 'Đắk Lắk': (38.47, 17.67, 40.07, 3.79)}            # agri, ind, svc, tax
BANDS = ['0-4', '5-9', '10-14', '15-19', '20-24', '25-29', '30-34', '35-39', '40-44', '45-49', '50-54', '55-59', '60-64',
         '65-69', '70-74', '75-79', '80+']
POP = {'m25': [3.51, 3.82, 4.41, 3.72, 3.23, 3.14, 3.9, 4.26, 3.98, 3.28, 2.97, 2.62, 2.32, 1.71, 1.08, 0.59, 0.42],
       'f25': [3.18, 3.54, 4.23, 3.63, 3.21, 3.12, 3.91, 4.24, 4.01, 3.38, 3.1, 2.86, 2.7, 2.27, 1.55, 0.98, 1.11],
       'm50': [2.75, 2.95, 3.02, 2.95, 2.88, 3.08, 3.35, 3.86, 3.22, 2.8, 2.7, 3.26, 3.42, 2.98, 2.19, 1.63, 1.72],
       'f50': [2.58, 2.76, 2.83, 2.75, 2.67, 2.83, 3.17, 3.8, 3.25, 2.89, 2.8, 3.47, 3.69, 3.39, 2.69, 2.21, 3.46]}  # % of total
OLD65 = {'2025': 9.72, '2050': 20.27}
BUDGET_REV_9M, BUDGET_REV_PLAN26 = 2187.3, 2529.467                                                   # tn VND
CPI_AVG_9M, CPI_TARGET = 4.52, 4.5
CREDIT_TARGET26 = 15.0
FDI_TARGET_LOW = 30.0                                                                                # NQ 10-NQ/TW, bn USD/yr

# ── 1. cover ──
d.cover('Báo cáo kinh tế · ngân hàng · bộ mẫu trình bày',
        f'Kinh tế 9 tháng 2026: GDP tăng {n(G9M, 2)}%, tín dụng chạy trước huy động',
        'Ba mươi ba cách trình bày dữ liệu theo phong cách Vietnam Dashboard — số liệu thật của dashboard, '
        'mỗi trang một phát hiện.',
        'Cập nhật 7/10/2026',
        chart={'type': 'bar', 'categories': yrs(2015, 2025), 'series': [{'name': 'GDP', 'values': GDP_G[5:]}],
               'dec': 1, 'unit': '', 'highlight': 10, 'labels': 'hi', 'grid': False, 'y_title': 'Tăng trưởng GDP, %'},
        note='Nguồn số liệu: NSO, NHNN, Bộ Tài chính, IMF, World Bank, UN WPP 2024 và mô hình của dashboard. '
             'Mô tả số liệu — không phải khuyến nghị đầu tư.')

# ── chapter 1 ──
d.section(1, 'Tăng trưởng', 'GDP 9 tháng, các con số chính, dự báo đến 2030 và cơ cấu nền kinh tế.',
          ['Con số nổi bật và bốn chỉ số', 'Tăng trưởng 2010–2025 và dự báo IMF', 'Cơ cấu chi tiêu 2015 → 2025',
           'Thứ hạng các ngành', 'Ai đầu tư, FDI giải ngân'])

best = max(range(len(GDP_G)), key=lambda i: GDP_G[i])
d.hero('Tổng quan · 9 tháng 2026', f'GDP 9 tháng 2026 tăng {n(G9M, 2)}%; riêng quý 3 tăng {n(Q26["Q3"], 2)}%',
       n(G9M, 2), '%',
       f'So với cùng kỳ 2025. Tốc độ tăng dần qua từng quý; mục tiêu cả năm của Quốc hội là từ {n(TARGET_G, 0)}%. '
       f'Mức cả năm cao nhất 2010–2025 là {n(GDP_G[best], 2)}% ({2010 + best}).',
       counts=[(k + '/2026', n(v, 2) + '%', v) for k, v in Q26.items()],
       chart={'type': 'bar', 'categories': yrs(2015, 2025), 'series': [{'name': 'GDP', 'values': GDP_G[5:]}],
              'dec': 2, 'unit': '%', 'highlight': 10, 'labels': 'auto'},
       chart_title='Tăng trưởng GDP cả năm, 2015–2025 (%)',
       note=(f'Quý 3/2026 tăng {n(Q26["Q3"], 2)}%, cao hơn {n(Q26["Q3"] - Q26["Q1"], 2)} điểm % so với quý 1.',
             'Số 9 tháng so với cùng kỳ, không so trực tiếp với số cả năm ở biểu đồ bên phải.'),
       source='Nguồn: NSO — thông cáo quý III và 9 tháng 2026 (03/10/2026); GDP năm theo chuỗi đánh giá lại '
              '(2023 = 4,98%, 2024 = 7,04%); 2025 ước tính.')

cpi_d = CPI24[-1] - CPI24[-13]
d.kpis('Bốn chỉ số · tháng 9/2026',
       f'Xuất khẩu 9 tháng tăng {n(EXP9M_YOY, 1)}%; lạm phát lên {n(CPI24[-1], 2)}%',
       [{'label': 'CPI so cùng kỳ', 'value': n(CPI24[-1], 2), 'unit': '%', 'delta': cpi_d, 'delta_unit': ' điểm %',
         'delta_dec': 2, 'tone': REDS, 'delta_label': 'so với T9/2025', 'spark': CPI24[-12:],
         'sub': 'Tháng 9/2026. Đường nhỏ: 12 tháng gần nhất.'},
        {'label': 'Tín dụng từ đầu năm', 'value': n(CR_YTD_SEP26, 2), 'unit': '%',
         'delta': CR_YTD_SEP26 - CR_YTD_SEP25, 'delta_dec': 2, 'delta_unit': ' điểm %',
         'delta_label': 'so với 30/9/2025', 'spark': CR_G, 'spark_kind': 'col',
         'sub': 'Đến 30/9/2026. Cột nhỏ: tăng trưởng cả năm 2015–2025.'},
        {'label': 'Xuất khẩu 9 tháng', 'value': n(EXP9M, 1), 'unit': 'tỷ USD', 'delta': EXP9M_YOY, 'delta_unit': '%',
         'delta_label': 'so với 9T/2025', 'spark': EXP_Y, 'spark_kind': 'col',
         'sub': 'Hàng hoá, tháng 1–9/2026. Cột nhỏ: cả năm 2015–2025.'},
        {'label': 'FDI giải ngân 9 tháng', 'value': n(FDI9M, 2), 'unit': 'tỷ USD', 'delta': FDI9M_YOY,
         'delta_unit': '%', 'delta_label': 'so với 9T/2025', 'spark': FDI_DIS[5:], 'spark_kind': 'col',
         'sub': 'Vốn thực hiện, tháng 1–9/2026. Cột nhỏ: cả năm 2015–2025.'}],
       note=(f'CPI tháng 9/2026 tăng {n(CPI24[-1], 2)}% so với cùng kỳ, cao hơn {n(cpi_d, 2)} điểm % so với '
             f'một năm trước; tín dụng tăng {n(CR_YTD_SEP26, 2)}% từ đầu năm.',
             f'Tín dụng tăng chậm hơn cùng kỳ 2025 ({n(CR_YTD_SEP25, 2)}%), dù vẫn nhanh hơn huy động.'),
       source='Nguồn: NSO (CPI, xuất khẩu, FDI; 03/10/2026); NHNN (tín dụng đến 30/9/2026). Mũi tên: xanh = tăng, '
              'đỏ = giảm, trừ CPI (đỏ = giá tăng).')

act_series = GDP_G + [None] * 5
imf_series = [None] * 15 + [GDP_G[-1]] + IMF_G
d.chart('Chương 1 · Tăng trưởng', f'IMF dự báo tăng trưởng chậm dần về {n(IMF_G[-1], 1)}% năm 2030, '
                                  f'thấp hơn mục tiêu {n(TARGET_G, 0)}%/năm',
        {'type': 'line', 'categories': yrs(2010, 2030), 'dec': 1, 'unit': '%', 'y_min': 0, 'y_max': 12,
         'series': [{'name': 'Thực tế (NSO)', 'values': act_series, 'color': INK, 'end_label': False, 'markers': None},
                    {'name': 'IMF WEO 4/2026', 'label': 'IMF', 'values': imf_series, 'color': BLUE, 'dashed': True}],
         'forecast_from': 16, 'forecast_label': 'Dự báo',
         'refs': [{'value': TARGET_G, 'label': f'Mục tiêu từ {n(TARGET_G, 0)}%/năm (NQ 25/2026/QH16)'}],
         'annotations': [{'series': 0, 'at': 15, 'text': f'2025: {n(GDP_G[-1], 2)}%', 'tier': 'pri', 'dx': -0.35,
                          'dy': -0.55},
                         {'series': 0, 'at': 11, 'text': f'2021: {n(GDP_G[11], 2)}% (Covid-19)', 'tier': 'sup',
                          'dx': -0.35, 'dy': 0.3}]},
        note=(f'IMF (4/2026) dự báo {n(IMF_G[0], 1)}% năm 2026, giảm dần về {n(IMF_G[-1], 1)}% năm 2030; '
              f'chín tháng 2026 thực tế đã tăng {n(G9M, 2)}%.',
              'Mục tiêu là mức sàn chính sách, không phải dự báo; IMF sẽ cập nhật trong WEO tháng 10/2026.'),
        source='Nguồn: NSO (2010–2025, 2025 ước tính); IMF World Economic Outlook 4/2026; Quốc hội — NQ 25/2026/QH16. '
               'Vùng tô: giai đoạn dự báo.')

d.chart('Chương 1 · Cơ cấu chi tiêu',
        f'Tiêu dùng hộ giảm từ {n(SHARE15["C"], 1)}% xuống {n(SHARE25["C"], 1)}% GDP trong mười năm',
        {'type': 'slope', 'periods': ('2015', '2025'), 'dec': 1, 'unit': '%',
         'series': [{'name': 'Tiêu dùng hộ (C)', 'values': [SHARE15['C'], SHARE25['C']], 'hi': True, 'color': BLUE},
                    {'name': 'Tích luỹ tài sản (I)', 'values': [SHARE15['I'], SHARE25['I']]},
                    {'name': 'Chi tiêu Nhà nước (G)', 'values': [SHARE15['G'], SHARE25['G']]},
                    {'name': 'Xuất khẩu ròng (X − M)', 'values': [SHARE15['NX'], SHARE25['NX']], 'hi': True,
                     'color': ORANGE}]},
        panel={'label': 'Xuất khẩu ròng', 'value': n(SHARE25['NX'] / SHARE15['NX'], 1), 'unit': 'lần',
               'sub': f'Tỷ trọng trong GDP: {n(SHARE15["NX"], 2)}% (2015) → {n(SHARE25["NX"], 2)}% (2025).'},
        note=(f'Tiêu dùng hộ mất {n(SHARE15["C"] - SHARE25["C"], 1)} điểm % tỷ trọng GDP; xuất khẩu ròng tăng từ '
              f'{n(SHARE15["NX"], 1)}% lên {n(SHARE25["NX"], 1)}%.',
              'Tỷ trọng theo giá hiện hành; 2025 là ước tính. Sai số thống kê không vẽ.'),
        source='Nguồn: NSO — GDP theo mục đích sử dụng, giá hiện hành (2024 sơ bộ, 2025 ước tính).')

YB = [2010, 2013, 2016, 2019, 2022, 2025]
rank = lambda name, y: 1 + sum(1 for k in ACT if ACT[k][y - 2010] > ACT[name][y - 2010])
d.chart('Chương 1 · Các ngành', f'Khai khoáng tụt từ hạng {rank("Khai khoáng", 2010)} xuống hạng '
                                f'{rank("Khai khoáng", 2025)} về đóng góp vào GDP; chế biến giữ hạng 1',
        {'type': 'bump', 'periods': [str(y) for y in YB], 'dec': 1, 'unit': '%',
         'highlight': ['Chế biến, chế tạo', 'Khai khoáng'],
         'series': [{'name': k, 'values': [ACT[k][y - 2010] for y in YB]} for k in ACT]},
        note=(f'Chế biến, chế tạo chiếm {n(ACT["Chế biến, chế tạo"][-1], 1)}% GDP năm 2025, từ '
              f'{n(ACT["Chế biến, chế tạo"][0], 1)}% năm 2010; khai khoáng còn {n(ACT["Khai khoáng"][-1], 1)}%.',
              'Xếp hạng trong chín ngành lớn nhất theo tỷ trọng giá hiện hành; số bên phải là tỷ trọng năm 2025.'),
        source='Nguồn: NSO PxWeb — cơ cấu GDP theo ngành kinh tế, giá hiện hành (V03.02, V03.04–05), '
               'chuỗi đánh giá lại 2021; 2025 ước tính.')

tot25 = INV['state'][-1] + INV['nonstate'][-1] + INV['fdi'][-1]
d.chart('Chương 1 · Đầu tư', f'Khu vực ngoài nhà nước bỏ {n(INV["nonstate"][-1] / tot25 * 100, 0)}% vốn đầu tư '
                             f'toàn xã hội năm 2025',
        {'type': 'area', 'categories': yrs(2015, 2025), 'dec': 0, 'unit': '', 'y_title': 'Nghìn tỷ đồng',
         'series': [{'name': 'Ngoài nhà nước', 'values': [v / 1000 for v in INV['nonstate']], 'color': BLUE},
                    {'name': 'Nhà nước', 'values': [v / 1000 for v in INV['state']], 'color': ORANGE},
                    {'name': 'FDI', 'values': [v / 1000 for v in INV['fdi']], 'color': AQUA}]},
        note=(f'Vốn đầu tư thực hiện đạt {n(tot25 / 1000, 0)} nghìn tỷ năm 2025, gấp '
              f'{n(tot25 / (INV["state"][0] + INV["nonstate"][0] + INV["fdi"][0]), 1)} lần năm 2015.',
              'Chín tháng 2026, vốn Nhà nước tăng 17,3%, nhanh hơn ngoài nhà nước (14,0%) và FDI (14,5%).'),
        source='Nguồn: NSO V04.01 — vốn đầu tư thực hiện toàn xã hội theo nguồn, giá hiện hành; 9T/2026: NSO 03/10/2026.')

mx_fdi = max(range(len(FDI_DIS)), key=lambda i: FDI_DIS[i])
d.chart('Chương 1 · FDI', f'FDI giải ngân đạt {n(FDI_DIS[-1], 1)} tỷ USD năm 2025, cao nhất từ 2010',
        {'type': 'bar', 'categories': yrs(2010, 2025), 'series': [{'name': 'FDI giải ngân', 'values': FDI_DIS}],
         'dec': 1, 'unit': '', 'highlight': mx_fdi, 'y_title': 'Tỷ USD', 'labels': 'auto'},
        panel={'label': '9 tháng 2026', 'value': n(FDI9M, 2), 'unit': 'tỷ USD',
               'sub': f'Tăng {n(FDI9M_YOY, 1)}% so với cùng kỳ; mục tiêu NQ 10-NQ/TW: {n(FDI_TARGET_LOW, 0)}–40 tỷ USD/năm.'},
        note=(f'FDI giải ngân năm 2025 đạt {n(FDI_DIS[-1], 2)} tỷ USD, gấp {n(FDI_DIS[-1] / FDI_DIS[0], 1)} lần năm 2010.',
              'Vốn đăng ký là chỉ tiêu khác, không cộng chung; 2025 ước tính.'),
        source='Nguồn: NSO / Cục Đầu tư nước ngoài — FDI thực hiện (giải ngân), tỷ USD; 9T/2026: NSO 03/10/2026.')

# ── chapter 2 ──
d.section(2, 'Giá cả, tiền tệ và ngân hàng', 'Lạm phát theo nhóm hàng, tín dụng so với huy động, sức khoẻ ngân hàng '
                                             'và mô hình lãi suất huy động của dashboard.',
          ['Bản đồ nhiệt CPI theo nhóm hàng', 'Tín dụng, huy động, M2', 'Bảng chỉ số ngân hàng có biểu đồ nhỏ',
           'Lãi suất 12 tháng: biểu đồ quạt', 'Độ nhạy: biểu đồ lốc xoáy', 'Các đòn bẩy của NHNN', 'Nhận định'])

g_last = {k: v[-1] for k, v in CPI_GRP.items()}
top_g = max(g_last, key=g_last.get)
d.chart('Chương 2 · Lạm phát', f'{top_g} tăng {n(g_last[top_g], 2)}% riêng tháng 9/2026, mạnh nhất trong '
                               f'{len(CPI_GRP)} nhóm hàng',
        {'type': 'heatmap', 'rows': list(CPI_GRP), 'cols': CPI_M, 'values': list(CPI_GRP.values()), 'dec': 2,
         'scale': 'diverging', 'vmax': 2, 'pos_color': REDS, 'neg_color': BLUE,
         'legend_title': 'Thay đổi giá so với tháng trước (%); đỏ = tăng, xanh = giảm, màu giữ ở mức ±2'},
        note=(f'Giao thông biến động mạnh nhất: +{n(CPI_GRP["Giao thông"][5], 2)}% tháng 3/2026 rồi '
              f'{n(CPI_GRP["Giao thông"][8], 2)}% tháng 6/2026.',
              'Màu được giới hạn ở ±2% để các nhóm khác vẫn đọc được; con số trong ô là giá trị thật.'),
        source='Nguồn: NSO — Thông cáo báo chí tình hình giá hằng tháng (Biểu 1 – Cả nước), T10/2025–T9/2026.')

gap25 = CR_G[-1] - DEP_G[-1]
d.chart('Chương 2 · Tín dụng', f'Tín dụng tăng {n(CR_G[-1], 2)}% năm 2025, nhanh hơn huy động {n(gap25, 2)} điểm %',
        {'type': 'line', 'categories': yrs(2015, 2025), 'dec': 2, 'unit': '%', 'zero': True, 'tick_dec': 0,
         'series': [{'name': 'Huy động', 'values': DEP_G, 'muted': True},
                    {'name': 'M2', 'values': M2_G, 'muted': True},
                    {'name': 'Tín dụng', 'values': CR_G, 'color': BLUE}],
         'annotations': [{'series': 0, 'at': 7, 'text': f'2022: huy động chỉ {n(DEP_G[7], 2)}%', 'tier': 'pri',
                          'value': DEP_G[7], 'dx': 0.4, 'dy': 0.35}]},
        note=(f'Khoảng cách tín dụng − huy động lớn nhất năm 2022 ({n(CR_G[7] - DEP_G[7], 2)} điểm %), '
              f'năm 2025 là {n(gap25, 2)} điểm %.',
              f'Tín dụng nhanh hơn huy động trong {sum(1 for a, b in zip(CR_G, DEP_G) if a > b)}/11 năm 2015–2025.'),
        source='Nguồn: NHNN; IMF FSI (tiền gửi khách hàng); tăng trưởng cuối năm so với cuối năm trước.')

d.chart('Chương 2 · Sức khoẻ ngân hàng', f'Nợ xấu {n(FSI["npl"][-1], 2)}% năm 2025, gấp '
                                         f'{n(FSI["npl"][-1] / FSI["npl"][0], 1)} lần năm 2015; ROE lên {n(FSI["roe"][-1], 1)}%',
        {'type': 'spark_table', 'years': [2015, 2025],
         'headers': ('Chỉ tiêu', 'Diễn biến 2015–2025', '2025', 'So với 2015'),
         'rows': [{'name': 'Nợ xấu / dư nợ', 'sub': 'IMF FSI, %', 'values': FSI['npl'], 'dec': 2, 'unit': '%',
                   'up_color': REDS, 'down_color': BLUE},
                  {'name': 'Hệ số an toàn vốn (CAR)', 'sub': 'IMF FSI, %', 'values': FSI['car'], 'dec': 2, 'unit': '%'},
                  {'name': 'Dự phòng / nợ xấu', 'sub': 'IMF FSI, %', 'values': FSI['prov'], 'dec': 1, 'unit': '%'},
                  {'name': 'ROA', 'sub': 'IMF FSI, %', 'values': FSI['roa'], 'dec': 2, 'unit': '%'},
                  {'name': 'ROE', 'sub': 'IMF FSI, %', 'values': FSI['roe'], 'dec': 1, 'unit': '%'},
                  {'name': 'Tăng trưởng tín dụng', 'sub': 'NHNN, % cuối năm', 'values': CR_G, 'dec': 2, 'unit': '%'},
                  {'name': 'Tăng trưởng huy động', 'sub': 'NHNN, % cuối năm', 'values': DEP_G, 'dec': 2, 'unit': '%'}]},
        note=(f'Nợ xấu theo IMF FSI tăng vọt lên {n(FSI["npl"][8], 2)}% năm 2023 rồi giảm còn {n(FSI["npl"][-1], 2)}%; '
              f'ROE âm năm 2023 ({n(FSI["roe"][8], 1)}%).',
              'Số IMF FSI gồm các tổ chức nhận tiền gửi; khác số nợ xấu nội bảng của NHNN. Chấm xám: điểm thấp nhất.'),
        source='Nguồn: IMF Financial Soundness Indicators (FSIC, Việt Nam, 2015–2025); NHNN.')

d.chart('Chương 2 · Lãi suất huy động', f'Mô hình: lãi suất 12 tháng có thể lên {n(PRJ["base"][-1], 2)}% vào 3/2027 '
                                        f'(khoảng 80%: {n(PRJ["low"][-1], 2)}–{n(PRJ["high"][-1], 2)}%)',
        {'type': 'fan', 'categories': DEP12_M + PRJ_M, 'actual': DEP12, 'base': PRJ['base'], 'low': PRJ['low'],
         'high': PRJ['high'], 'dec': 2, 'unit': '%', 'y_min': 4, 'y_max': 8,
         'names': {'actual': 'Vietcombank 12 tháng', 'base': 'Mô hình cơ sở', 'band': 'Khoảng 80%'},
         'forecast_label': 'Mô hình, không phải dự báo'},
        note=(f'Lãi suất niêm yết 12 tháng của Vietcombank lên {n(DEP12[-1], 1)}% từ tháng 3/2026, từ '
              f'{n(DEP12[0], 1)}% suốt 2025; mô hình đưa đường cơ sở lên {n(PRJ["base"][-1], 2)}% sau sáu tháng.',
              'Tháng 10–12/2025 chưa có số liệu (để trống). Khoảng 80% = cơ sở ± 1,28 × sai số chuẩn × √tháng.'),
        source='Nguồn: Vietcombank (lãi suất niêm yết); mô hình cấu trúc v2 của Vietnam Dashboard (7/10/2026) — '
               'minh hoạ, không phải dự báo chính thức.')

top_t = max(TORNADO, key=lambda r: abs(r[2] - r[1]))
d.chart('Chương 2 · Độ nhạy', f'{top_t[0]} là biến số tác động lớn nhất: chênh {n(abs(top_t[2] - top_t[1]), 2)} '
                              f'điểm % lãi suất 12 tháng vào 3/2027',
        {'type': 'tornado', 'base': TORNADO_BASE, 'dec': 2, 'unit': '%',
         'labels': ('Biến ở mức thấp', 'Biến ở mức cao'), 'base_label': 'Cơ sở 3/2027',
         'rows': [{'name': nm, 'low': lo, 'high': hi} for nm, lo, hi in TORNADO]},
        note=(f'Mỗi thanh: lãi suất mô hình tháng 3/2027 khi một biến ở mức thấp hoặc cao, các biến khác giữ cơ sở '
              f'({n(TORNADO_BASE, 2)}%).',
              'Cán cân thương mại tác động ngược chiều: thặng dư cao làm lãi suất mô hình thấp hơn.'),
        source='Nguồn: Vietnam Dashboard — mô hình cấu trúc v2, phân tích độ nhạy 21 biến (8 biến lớn nhất); '
               'minh hoạ, không phải dự báo.')

d.chart('Chương 2 · Cơ chế truyền dẫn', 'Lãi suất điều hành đến lạm phát qua ba tầng: thị trường liên ngân hàng, '
                                       'lãi suất bán lẻ, rồi tín dụng',
        {'type': 'levers', 'highlight': ['refi', 'ib', 'dep', 'credit', 'cpi', 'gdp'],
         'columns': [{'title': 'Công cụ NHNN', 'items': [
             {'id': 'refi', 'name': 'Lãi suất điều hành', 'sub': 'tái cấp vốn, tái chiết khấu'},
             {'id': 'omo', 'name': 'Thị trường mở (OMO)', 'sub': 'bơm / hút, tín phiếu'},
             {'id': 'fx', 'name': 'Mua bán ngoại tệ', 'sub': 'dự trữ, tỷ giá trung tâm'}]},
             {'title': 'Thị trường tiền tệ', 'items': [
                 {'id': 'ib', 'name': 'Lãi suất liên ngân hàng', 'sub': 'chi phí vốn ngắn hạn'},
                 {'id': 'liq', 'name': 'Thanh khoản hệ thống', 'sub': 'LDR, tiền gửi KBNN'}]},
             {'title': 'Ngân hàng', 'items': [
                 {'id': 'dep', 'name': 'Lãi suất huy động, cho vay', 'sub': 'niêm yết'},
                 {'id': 'credit', 'name': 'Tín dụng', 'sub': 'trong chỉ tiêu NHNN giao'}]},
             {'title': 'Kết quả', 'items': [
                 {'id': 'cpi', 'name': 'Lạm phát (CPI)', 'sub': 'giá tiêu dùng'},
                 {'id': 'gdp', 'name': 'Tăng trưởng GDP', 'sub': 'đầu tư, tiêu dùng'}]}],
         'links': [('refi', 'ib'), ('omo', 'ib'), ('omo', 'liq'), ('fx', 'liq'), ('ib', 'dep'), ('liq', 'dep'),
                   ('dep', 'credit'), ('credit', 'cpi'), ('credit', 'gdp')]},
        note=('Đường đậm: kênh lãi suất — từ lãi suất điều hành qua liên ngân hàng, lãi suất bán lẻ tới tín dụng, '
              'CPI và GDP.', 'Sơ đồ định tính, không đo độ mạnh của từng kênh; chỉ tiêu tín dụng giới hạn trực tiếp dư nợ.'),
        source='Nguồn: Vietnam Dashboard — Hệ thống tài chính, Chương 7 (các đòn bẩy cung tiền); khung minh hoạ.')

d.quote('Chương 2 · Nhận định',
        'Tín dụng tăng nhanh hơn huy động trong 9/11 năm 2015–2025, trừ 2019 (13,65% so với 14,54%) và 2020 '
        '(12,2% so với 13,3%); khoảng chênh mở rộng từ 2021, lớn nhất năm 2022 (14,2% so với 5,99%) và 2024 '
        '(15,09% so với 10,66%).',
        'Vietnam Dashboard', 'Phân tích Hệ thống tài chính · 6/10/2026',
        facts=[('Năm tín dụng nhanh hơn', f'{sum(1 for a, b in zip(CR_G, DEP_G) if a > b)}/11', '2015–2025, NHNN'),
               ('Chênh lệch 2022', f'{n(CR_G[7] - DEP_G[7], 2)} đ.%', f'{n(CR_G[7], 1)}% so với {n(DEP_G[7], 2)}%'),
               ('Cho vay vượt tiền gửi', '2023', '13,30 so với 13,26 triệu tỷ (IMF FSI)')],
        note=('Nguyên nhân chính theo phân tích: cầu vốn từ mục tiêu tăng trưởng cao và chỉ tiêu tín dụng nới rộng.',
              'Trích nguyên văn; các con số bên phải tính lại từ chuỗi NHNN và IMF FSI của dashboard.'),
        source='Nguồn: Vietnam Dashboard — FINSYS.analysis (6/10/2026), dựa trên NHNN, IMF FSI.')

# ── chapter 3 ──
d.section(3, 'Ngân sách và cán cân thanh toán', 'Chi ngân sách theo nhóm, cơ cấu thu chi, dòng tiền ngân sách và '
                                               'các dòng tiền qua biên giới.',
          ['Chi ngân sách 2018–2026', 'Cầu nối chi 2025 → 2026', 'Cơ cấu thu và chi: biểu đồ ô vuông', 'Từ nguồn thu đến khoản chi',
           'Cán cân thanh toán 2025', 'Sáu dòng tiền, 2015–2025'])

other = [t - a - b - c for t, a, b, c in zip(EXP['total'], EXP['dev'], EXP['rec'], EXP['int'])]
rec_sh = [r / t * 100 for r, t in zip(EXP['rec'][:7], EXP['total'][:7])]
d.chart('Chương 3 · Chi ngân sách', f'Chi thường xuyên chiếm {n(min(rec_sh), 0)}–{n(max(rec_sh), 0)}% tổng chi mỗi năm '
                                    f'2018–2024; dự toán 2026 là {n(EXP["total"][-1] / 1000, 0)} nghìn tỷ',
        {'type': 'stacked', 'categories': BUD_Y[:7] + ['2025 ƯT', '2026 DT'], 'dec': 0, 'unit': '',
         'y_title': 'Nghìn tỷ đồng', 'basis': BUD_BASIS, 'basis_label': 'Ước tính (ƯT) / dự toán (DT)',
         'series': [{'name': 'Chi thường xuyên', 'values': [v / 1000 for v in EXP['rec']], 'color': BLUE},
                    {'name': 'Đầu tư phát triển', 'values': [v / 1000 for v in EXP['dev']], 'color': ORANGE},
                    {'name': 'Trả lãi', 'values': [v / 1000 for v in EXP['int']], 'color': AQUA},
                    {'name': 'Khác', 'values': [v / 1000 for v in other], 'muted': True}]},
        note=(f'Chi đầu tư phát triển dự toán {n(EXP["dev"][-1] / 1000, 0)} nghìn tỷ năm 2026, gấp '
              f'{n(EXP["dev"][-1] / EXP["dev"][-2], 1)} lần ước 2025.',
              'Dự toán là kế hoạch Quốc hội giao, không phải số thực chi; 2018–2024 là quyết toán.'),
        source='Nguồn: Bộ Tài chính, Nghị quyết Quốc hội — quyết toán 2018–2024, ước thực hiện 2025 (1/2026), '
               'dự toán 2026.')

parts = [('Chi thường xuyên', 'rec'), ('Đầu tư phát triển', 'dev'), ('Trả lãi', 'int')]
dT = (EXP['total'][-1] - EXP['total'][-2]) / 1000
dDev = (EXP['dev'][-1] - EXP['dev'][-2]) / 1000
d.chart('Chương 3 · Cầu nối chi ngân sách', f'Chi ngân sách dự toán 2026 tăng {n(dT, 0)} nghìn tỷ so với ước 2025; '
                                            f'đầu tư phát triển góp {n(dDev / dT * 100, 0)}%',
        {'type': 'waterfall', 'dec': 0, 'unit': '', 'y_title': 'Nghìn tỷ đồng',
         'steps': [{'name': 'Chi 2025 (ước tính)', 'value': EXP['total'][-2] / 1000, 'total': True}] +
                  [{'name': nm, 'value': (EXP[k][-1] - EXP[k][-2]) / 1000} for nm, k in parts] +
                  [{'name': 'Khác', 'value': (other[-1] - other[-2]) / 1000},
                   {'name': 'Chi 2026 (dự toán)', 'total': True, 'basis': 'plan'}],
         'basis_label': 'Dự toán (kế hoạch)'},
        note=(f'Đầu tư phát triển tăng {n(dDev, 0)} nghìn tỷ, chi thường xuyên tăng '
              f'{n((EXP["rec"][-1] - EXP["rec"][-2]) / 1000, 0)} nghìn tỷ trong dự toán 2026.',
              'So sánh dự toán với ước thực hiện: dự toán là kế hoạch, thực chi thường thấp hơn.'),
        source='Nguồn: Bộ Tài chính — ước thực hiện NSNN 2025 (1/2026); Nghị quyết Quốc hội về dự toán NSNN 2026. '
               '"Khác" = tổng chi trừ ba nhóm (≈, tính toán).')

rev_tot25 = sum(REV25.values())
exp25 = {'Chi thường xuyên': EXP['rec'][-2], 'Đầu tư phát triển': EXP['dev'][-2], 'Trả lãi': EXP['int'][-2],
         'Khác': other[-2]}
d.two_charts('Chương 3 · Cơ cấu thu chi', f'Thu nội địa chiếm {n(REV25["Thu nội địa"] / rev_tot25 * 100, 0)}% thu, '
                                          f'chi thường xuyên {n(exp25["Chi thường xuyên"] / EXP["total"][-2] * 100, 0)}% '
                                          f'chi ngân sách 2025',
             {'type': 'waffle', 'parts': [{'name': k, 'value': v / rev_tot25 * 100, 'color': c}
                                          for (k, v), c in zip(REV25.items(), (BLUE, ORANGE, AQUA))]},
             {'type': 'waffle', 'dec': 1, 'parts': [{'name': k, 'value': v / EXP['total'][-2] * 100, 'color': c,
                                           'muted': k == 'Khác'} for (k, v), c in zip(exp25.items(),
                                                                                      (BLUE, ORANGE, AQUA, None))]},
             titles=('Thu ngân sách 2025 (ước tính) — mỗi ô 1%', 'Chi ngân sách 2025 (ước tính) — mỗi ô 1%'),
             note=(f'Thu ngân sách 2025 ước {n(rev_tot25 / 1000, 1)} nghìn tỷ từ ba nguồn chính; chi ước '
                   f'{n(EXP["total"][-2] / 1000, 1)} nghìn tỷ.',
                   'Viện trợ 2025 chưa công bố nên không có ô; "Khác" nhỏ hơn 0,5% nên không đủ một ô. '
                   'Ô làm tròn theo phần dư lớn nhất để tổng đúng 100.'),
             source='Nguồn: Bộ Tài chính — ước thực hiện NSNN 2025 (công bố 1/2026).')

rev24 = sum(REV24.values())
gap24 = EXP['total'][6] - rev24
o24 = other[6]
d.chart('Chương 3 · Dòng tiền ngân sách', f'Chi thường xuyên chiếm {n(EXP["rec"][6] / EXP["total"][6] * 100, 0)}% '
                                          f'chi ngân sách 2024; thu nội địa trả cho {n(REV24["Thu nội địa"] / EXP["total"][6] * 100, 0)}%',
        {'type': 'flow', 'dec': 1, 'unit': '',
         'columns': [[{'id': 'dom', 'name': 'Thu nội địa', 'color': BLUE},
                      {'id': 'xnk', 'name': 'Thu xuất nhập khẩu', 'color': ORANGE},
                      {'id': 'oil', 'name': 'Dầu thô', 'color': YELLOW},
                      {'id': 'aid', 'name': 'Viện trợ', 'color': AQUA},
                      {'id': 'gap', 'name': 'Chi vượt thu (≈)', 'color': REDS}],
                     [{'id': 'tot', 'name': 'Tổng chi 2024', 'color': INK}],
                     [{'id': 'rec', 'name': 'Chi thường xuyên', 'color': '8A8F99'},
                      {'id': 'dev', 'name': 'Đầu tư phát triển', 'color': '8A8F99'},
                      {'id': 'int', 'name': 'Trả lãi', 'color': '8A8F99'},
                      {'id': 'oth', 'name': 'Khác', 'color': '8A8F99'}]],
         'links': [('dom', 'tot', REV24['Thu nội địa'] / 1000), ('xnk', 'tot', REV24['Xuất nhập khẩu'] / 1000),
                   ('oil', 'tot', REV24['Dầu thô'] / 1000), ('aid', 'tot', REV24['Viện trợ'] / 1000),
                   ('gap', 'tot', gap24 / 1000),
                   ('tot', 'rec', EXP['rec'][6] / 1000), ('tot', 'dev', EXP['dev'][6] / 1000),
                   ('tot', 'int', EXP['int'][6] / 1000), ('tot', 'oth', o24 / 1000)]},
        note=(f'Tổng chi 2024 là {n(EXP["total"][6] / 1000, 1)} nghìn tỷ; tổng thu {n(rev24 / 1000, 1)} nghìn tỷ, '
              f'phần chi vượt thu ≈ {n(gap24 / 1000, 1)} nghìn tỷ.',
              '"Chi vượt thu" tính bằng chi trừ thu trên hai dòng này — không phải bội chi chính thức (2,8% GDP).'),
        source='Nguồn: Bộ Tài chính — quyết toán NSNN 2024 (nghìn tỷ đồng). Độ dày dải tỷ lệ với giá trị.')

eo = BOP25['Lỗi và sai sót']
bop_sorted = sorted(BOP25.items(), key=lambda kv: -kv[1])
d.chart('Chương 3 · Cán cân thanh toán', f'Lỗi và sai sót {n(eo, 1)} tỷ USD năm 2025, bằng '
                                         f'{n(abs(eo) / CA25 * 100, 0)}% thặng dư vãng lai',
        {'type': 'diverging', 'categories': [k for k, _ in bop_sorted], 'values': [v for _, v in bop_sorted],
         'dec': 1, 'unit': '', 'pos_label': 'Tiền vào (ròng, tỷ USD)', 'neg_label': 'Tiền ra (ròng, tỷ USD)'},
        panel={'label': 'Dự trữ ngoại hối 2025', 'value': n(RES25, 1, sign=True), 'unit': 'tỷ USD',
               'sub': f'Thặng dư vãng lai {n(CA25, 1)} tỷ USD nhưng dự trữ chỉ tăng {n(RES25, 1)} tỷ.'},
        note=(f'Hàng hoá ròng mang về {n(BOP25["Hàng hoá (ròng)"], 1)} tỷ USD; lỗi và sai sót cùng đầu tư khác '
              f'kéo ra {n(-(eo + BOP25["Đầu tư khác (ròng)"]), 1)} tỷ.',
              'Tổng các dòng bằng thay đổi dự trữ; dấu theo BPM6 của IMF.'),
        source='Nguồn: IMF Balance of Payments (BPM6) theo số NHNN, năm 2025.')

neg_years = sum(1 for v in BOP['Lỗi và sai sót'] if v < 0)
d.chart('Chương 3 · Sáu dòng tiền', f'Lỗi và sai sót âm cả {neg_years}/11 năm 2015–2025; '
                                    f'FDI ròng dương mọi năm',
        {'type': 'multiples', 'cols': 3, 'dec': 1, 'unit': 'tỷ USD',
         'pos_label': 'Tiền vào (dương)', 'neg_label': 'Tiền ra (âm)',
         'panels': [{'title': k, 'categories': yrs(2015, 2025), 'values': v} for k, v in BOP.items()]},
        note=(f'Lỗi và sai sót lớn nhất năm 2022 ({n(min(BOP["Lỗi và sai sót"]), 1)} tỷ USD); '
              f'dự trữ giảm {n(-BOP["Thay đổi dự trữ"][7], 1)} tỷ cùng năm.',
              'Mỗi ô một thang đo riêng: so sánh hình dạng, không so độ cao giữa các ô.'),
        source='Nguồn: IMF Balance of Payments (BPM6) theo số NHNN, 2015–2025, tỷ USD.')

# ── chapter 4 ──
d.section(4, 'Vùng và dân số', '34 tỉnh, thành sau sắp xếp năm 2025: tăng trưởng, cơ cấu, thu nhập; tháp dân số '
                               'hôm nay và năm 2050.',
          ['Bản đồ ô: tăng trưởng GRDP 2025', 'Xếp hạng mười hai địa phương', 'Trước và sau: 2025 so với 9T/2026',
           'Cơ cấu kinh tế tám địa phương', 'Thu nhập và tăng trưởng', 'Tháp dân số 2025 và 2050'])

over8 = sum(1 for p in PROV if p[2] > 8)
d.chart('Chương 4 · Địa phương', f'{over8}/{len(PROV)} tỉnh, thành tăng trưởng GRDP trên 8% năm 2025',
        {'type': 'tilemap', 'dec': 2, 'unit': '%', 'breaks': [7, 8, 9, 10],
         'legend_title': 'Tăng trưởng GRDP 2025 (%)', 'panels': ['Miền Bắc', 'Miền Trung và miền Nam'],
         'tiles': [{'name': p[0], 'short': p[1], 'value': p[2], 'col': p[5], 'row': p[6], 'panel': p[7]} for p in PROV]},
        panel={'label': 'Nhanh nhất', 'value': n(max(p[2] for p in PROV), 2), 'unit': '%',
               'sub': f'{max(PROV, key=lambda p: p[2])[0]}; chậm nhất {min(PROV, key=lambda p: p[2])[0]} '
                      f'({n(min(p[2] for p in PROV), 2)}%).'},
        note=(f'Tăng trưởng GRDP 2025 dao động từ {n(min(p[2] for p in PROV), 2)}% đến {n(max(p[2] for p in PROV), 2)}%.',
              'Ô sắp xếp theo vị trí địa lý gần đúng, không theo diện tích; hai khối Bắc và Trung–Nam đặt cạnh nhau.'),
        source='Nguồn: NSO và cục thống kê địa phương — GRDP 2025 (ước tính), 34 đơn vị sau sắp xếp hành chính.')

top12 = sorted(PROV, key=lambda p: -p[2])[:12]
PV = {p[0]: p for p in PROV}
d.chart('Chương 4 · Xếp hạng', f'{top12[0][0]} tăng {n(top12[0][2], 2)}% năm 2025, nhanh nhất cả nước; '
                               f'{sum(1 for p in PROV if p[2] >= 10)} địa phương đạt từ 10%',
        {'type': 'barh', 'categories': [p[0] for p in top12], 'values': [p[2] for p in top12], 'dec': 2, 'unit': '%',
         'highlight': [i for i, p in enumerate(top12) if p[2] >= 10]},
        note=(f'Mười hai địa phương tăng nhanh nhất đều đạt từ {n(top12[-1][2], 2)}%; Hà Nội ({n(PV["Hà Nội"][2], 2)}%) '
              f'và TP.HCM ({n(PV["TP. Hồ Chí Minh"][2], 2)}%) không nằm trong nhóm này.',
              'Thanh xanh: từ 10% trở lên; xám: dưới 10%. Số 2025 là ước tính.'),
        source='Nguồn: NSO và cục thống kê địa phương — tăng trưởng GRDP 2025 (giá so sánh).')

big = [p for p in PROV if p[0] in ('TP. Hồ Chí Minh', 'Hà Nội', 'Hải Phòng', 'Đồng Nai', 'Bắc Ninh', 'Quảng Ninh',
                                   'Thanh Hóa', 'Tây Ninh', 'Lâm Đồng', 'Ninh Bình', 'Phú Thọ', 'Cần Thơ')]
big.sort(key=lambda p: -p[4])
acc = max(big, key=lambda p: p[4] - p[2])
dec_ = [p[0] for p in big if p[4] < p[2]]
d.chart('Chương 4 · Tăng tốc', f'{acc[0]} tăng tốc mạnh nhất: {n(acc[4], 2)}% trong 9 tháng 2026, '
                               f'từ {n(acc[2], 2)}% cả năm 2025',
        {'type': 'dumbbell', 'dec': 2, 'unit': '%', 'labels': ('Cả năm 2025', '9 tháng 2026'),
         'rows': [{'name': p[0], 'a': p[2], 'b': p[4]} for p in big]},
        note=(f'{len(big) - len(dec_)}/{len(big)} địa phương lớn tăng nhanh hơn trong 9 tháng 2026 so với cả năm 2025'
              + (f'; chỉ {", ".join(dec_)} chậm lại.' if dec_ else '.'),
              '9 tháng so với cùng kỳ năm trước, không phải số cả năm; hai kỳ so sánh chỉ mang tính chỉ báo.'),
        source='Nguồn: NSO và cục thống kê địa phương — GRDP 2025 (ước tính) và 9 tháng 2026 (công bố 9–10/2026).')

d.chart('Chương 4 · Cơ cấu kinh tế', f'Dịch vụ chiếm {n(STRUCT25["Hà Nội"][2], 0)}% GRDP Hà Nội; công nghiệp – '
                                     f'xây dựng chiếm {n(STRUCT25["Bắc Ninh"][1], 0)}% ở Bắc Ninh',
        {'type': 'stacked100_h', 'categories': list(STRUCT25), 'dec': 0,
         'series': [{'name': 'Dịch vụ', 'values': [v[2] for v in STRUCT25.values()], 'color': BLUE},
                    {'name': 'Công nghiệp, xây dựng', 'values': [v[1] for v in STRUCT25.values()], 'color': ORANGE},
                    {'name': 'Nông, lâm, thủy sản', 'values': [v[0] for v in STRUCT25.values()], 'color': GREEN},
                    {'name': 'Thuế sản phẩm', 'values': [v[3] for v in STRUCT25.values()], 'color': YELLOW}]},
        note=(f'Đắk Lắk là nơi nông nghiệp chiếm tỷ trọng cao nhất trong tám địa phương ({n(STRUCT25["Đắk Lắk"][0], 0)}%).',
              'Mỗi thanh là 100% GRDP năm 2025 (giá hiện hành); các phần cộng lại đúng một tổng.'),
        source='Nguồn: NSO và cục thống kê địa phương — cơ cấu GRDP 2025, giá hiện hành.')

xs = [p[3] / 1000 for p in PROV]
ys = [p[2] for p in PROV]
mx, my = sum(xs) / len(xs), sum(ys) / len(ys)
r = sum((a - mx) * (b - my) for a, b in zip(xs, ys)) / (sum((a - mx) ** 2 for a in xs) * sum((b - my) ** 2 for b in ys)) ** 0.5
lab = {'Hà Nội', 'TP. Hồ Chí Minh', 'Quảng Ninh', 'Hải Phòng', 'Bắc Ninh', 'Đồng Nai', 'Cao Bằng', 'Vĩnh Long',
       'Ninh Bình', 'Phú Thọ', 'Tuyên Quang', 'Thái Nguyên'}
d.chart('Chương 4 · Thu nhập và tăng trưởng', f'Địa phương có GRDP đầu người cao hơn có xu hướng tăng nhanh hơn: '
                                              f'r = {n(r, 2)} trên {len(PROV)} địa phương',
        {'type': 'scatter', 'x_title': 'GRDP bình quân đầu người 2025, nghìn USD', 'y_title': 'Tăng trưởng GRDP 2025, %',
         'x_dec': 0, 'y_dec': 1, 'trend': True, 'hi_label': 'Địa phương được ghi tên',
         'other_label': 'Địa phương khác',
         'points': [{'name': p[0], 'x': p[3] / 1000, 'y': p[2], 'label': p[0] in lab, 'hi': p[0] in lab,
                     'text': p[1]} for p in PROV]},
        note=(f'Quảng Ninh vừa giàu nhất ({n(PV["Quảng Ninh"][3] / 1000, 1)} nghìn USD/người) vừa tăng nhanh nhất '
              f'({n(PV["Quảng Ninh"][2], 2)}%); TP.HCM giàu thứ hai nhưng chỉ tăng {n(PV["TP. Hồ Chí Minh"][2], 2)}%.',
              f'r = {n(r, 2)}: tương quan dương, mức vừa, không cho biết quan hệ nhân quả; 2025 ước tính.'),
        source='Nguồn: NSO và cục thống kê địa phương — GRDP và GRDP bình quân đầu người 2025 (USD).')

d.chart('Chương 4 · Dân số', f'Người từ 65 tuổi sẽ chiếm {n(OLD65["2050"], 1)}% dân số năm 2050, gấp '
                             f'{n(OLD65["2050"] / OLD65["2025"], 1)} lần năm 2025',
        {'type': 'pyramid', 'bands': BANDS, 'unit': '%', 'dec': 1,
         'left': {'name': 'Nam 2025', 'values': POP['m25']}, 'right': {'name': 'Nữ 2025', 'values': POP['f25']},
         'compare': {'name': '2050 (dự báo)', 'left': POP['m50'], 'right': POP['f50']},
         'annotations': [{'band': '80+', 'side': 'r', 'compare': True,
                          'text': f'Nữ 80+: {n(POP["f25"][-1], 1)}% → {n(POP["f50"][-1], 1)}%', 'dx': 0.45, 'dy': 0.0}]},
        panel={'label': 'Tỷ trọng 65+', 'value': n(OLD65['2025'], 1) + ' → ' + n(OLD65['2050'], 1), 'unit': '%',
               'sub': '2025 → 2050, phương án trung bình của Liên Hợp Quốc.'},
        note=(f'Nhóm 0–14 tuổi giảm từ 22,7% xuống 16,9% dân số; nhóm 65+ tăng từ {n(OLD65["2025"], 1)}% lên '
              f'{n(OLD65["2050"], 1)}%.',
              'Viền đứt là dự báo 2050 (UN WPP 2024, phương án trung bình), không phải số đếm.'),
        source='Nguồn: Liên Hợp Quốc — World Population Prospects 2024, Việt Nam; % tổng dân số, 31/12 mỗi năm.')

# ── chapter 5 ──
d.section(5, 'Mục tiêu, lịch và theo dõi', 'Tiến độ so với mục tiêu năm 2026, các mốc chính sách đã qua và sắp tới, '
                                           'phụ lục số liệu.',
          ['Tiến độ so với mục tiêu', 'Dòng thời gian 2026', 'Ba điều cần theo dõi', 'Phụ lục số liệu',
           'Biểu đồ gốc chỉnh sửa được', 'Nguồn và phương pháp'])

d.chart('Chương 5 · Tiến độ', f'Chín tháng: GDP đạt {n(G9M, 2)}% so với mục tiêu từ {n(TARGET_G, 0)}%; thu ngân sách '
                              f'đạt {n(BUDGET_REV_9M / BUDGET_REV_PLAN26 * 100, 0)}% dự toán',
        {'type': 'bullet', 'rows': [
            {'name': 'Tăng trưởng GDP', 'sub': '9 tháng so cùng kỳ; vùng đậm = dự báo IMF–ADB', 'value': G9M,
             'target': TARGET_G, 'max': 12, 'ranges': [7.1, 7.8], 'unit': '%', 'dec': 2,
             'value_text': f'mục tiêu từ {n(TARGET_G, 0)}%'},
            {'name': 'Lạm phát bình quân', 'sub': '9 tháng; mục tiêu là mức trần', 'value': CPI_AVG_9M,
             'target': CPI_TARGET, 'max': 6, 'unit': '%', 'dec': 2, 'color': REDS,
             'value_text': f'trần {n(CPI_TARGET, 1)}% — vượt {n(CPI_AVG_9M - CPI_TARGET, 2)} điểm %'},
            {'name': 'Tăng trưởng tín dụng', 'sub': 'từ đầu năm đến 30/9; định hướng cả năm', 'value': CR_YTD_SEP26,
             'target': CREDIT_TARGET26, 'max': 20, 'unit': '%', 'dec': 2,
             'value_text': f'= {n(CR_YTD_SEP26 / CREDIT_TARGET26 * 100, 0)}% định hướng {n(CREDIT_TARGET26, 0)}%'},
            {'name': 'Thu ngân sách', 'sub': '9 tháng, nghìn tỷ; so với dự toán cả năm', 'value': BUDGET_REV_9M,
             'target': BUDGET_REV_PLAN26, 'max': 3000, 'unit': '', 'dec': 0,
             'value_text': f'= {n(BUDGET_REV_9M / BUDGET_REV_PLAN26 * 100, 1)}% dự toán {n(BUDGET_REV_PLAN26, 0)}'},
            {'name': 'FDI giải ngân', 'sub': '9 tháng, tỷ USD; mục tiêu 30–40/năm', 'value': FDI9M,
             'target': FDI_TARGET_LOW, 'max': 40, 'unit': '', 'dec': 2,
             'value_text': f'= {n(FDI9M / FDI_TARGET_LOW * 100, 0)}% mức thấp của mục tiêu'}]},
        note=(f'Sau chín tháng, thu ngân sách đã đạt {n(BUDGET_REV_9M / BUDGET_REV_PLAN26 * 100, 1)}% dự toán; lạm phát '
              f'bình quân {n(CPI_AVG_9M, 2)}%, vượt trần {n(CPI_TARGET, 1)}%.',
              'So sánh số 9 tháng với mục tiêu cả năm chỉ cho biết tiến độ, không phải kết quả cả năm.'),
        source='Nguồn: NSO (03/10/2026); NHNN; Bộ Tài chính; Quốc hội — NQ 244/2025/QH15; Bộ Chính trị — NQ 10-NQ/TW; '
               'IMF WEO 4/2026; ADB ADO 9/2026.')

days = lambda y, m, dd: (date(y, m, dd) - TODAY).days
d.chart('Chương 5 · Dòng thời gian', 'Năm 2026: năm mốc chính sách đã qua và ba mốc trong quý IV',
        {'type': 'timeline', 'today': 'Hôm nay 7/10/2026', 'events': [
            {'date': '08/6/2026', 'title': 'NQ 10-NQ/TW', 'text': 'Mục tiêu FDI giải ngân 30–40 tỷ USD/năm'},
            {'date': '27/6/2026', 'title': 'Nghị định 245/2026', 'text': 'Gia hạn nộp thuế GTGT, TNDN, TNCN'},
            {'date': '01/8/2026', 'title': 'Tiền gửi KBNN', 'text': '50% được tính vào nguồn vốn khi tính LDR'},
            {'date': '9/2026', 'title': 'Công văn 8509/NHNN', 'text': 'Loại cho vay khách sạn, nghỉ dưỡng khỏi tín dụng BĐS'},
            {'date': '03/10/2026', 'title': 'Số liệu quý III', 'text': f'GDP 9 tháng tăng {n(G9M, 2)}%'},
            {'date': '13/10/2026', 'title': 'IMF WEO tháng 10', 'text': 'Cập nhật dự báo tăng trưởng', 'future': True},
            {'date': '01/12/2026', 'title': 'Thông tư 50/2026', 'text': 'Trần LDR 95%, LCR/NSFR có hiệu lực',
             'future': True},
            {'date': '31/12/2026', 'title': 'Hết hạn giảm thuế GTGT', 'text': 'Thuế suất 8% về lại 10%', 'future': True}]},
        note=('Năm mốc đã qua tác động tới vốn, thuế và tín dụng; ba mốc sắp tới thay đổi dự báo, thanh khoản và giá.',
              'Khoảng cách giữa các mốc không theo tỷ lệ thời gian; chấm rỗng = mốc tương lai.'),
        source='Nguồn: Bộ Chính trị; Chính phủ; NHNN; NSO; IMF — lịch công bố của dashboard (research/release_calendar), '
               'văn bản trong data/policy.json.')

d.watch('Tiếp theo · Điều cần theo dõi', 'Ba mốc trong quý IV/2026 sẽ cho biết bức tranh thay đổi thế nào',
        [('13/10/2026', 'IMF công bố WEO tháng 10',
          f'Dự báo tháng 4 là {n(IMF_G[0], 1)}% cho 2026; chín tháng thực tế đã tăng {n(G9M, 2)}%. '
          'Mức điều chỉnh sẽ cho thấy IMF đánh giá đà tăng quý 3 bền đến đâu.', (str(days(2026, 10, 13)), 'ngày nữa')),
         ('01/12/2026', 'Trần LDR 95% có hiệu lực',
          f'Thông tư 50/2026 nâng trần LDR và áp dụng LCR/NSFR. LDR toàn hệ thống là 77,1% (6/2026); '
          'tác động chính ở các ngân hàng sát trần.', (str(days(2026, 12, 1)), 'ngày nữa')),
         ('31/12/2026', 'Hết hạn giảm 2 điểm % thuế GTGT',
          f'Thuế suất 8% về lại 10% nếu không gia hạn. CPI tháng 9/2026 đang ở {n(CPI24[-1], 2)}% so với cùng kỳ.',
          (str(days(2026, 12, 31)), 'ngày nữa'))],
        source='Nguồn: IMF (lịch WEO); NHNN — Thông tư 50/2026; Chính phủ — chính sách giảm thuế GTGT (data/policy.json); '
               'NSO. Số ngày tính đến 7/10/2026.')

d.table('Phụ lục · Số liệu', 'Bảng số liệu chính, 2023 – 9 tháng 2026',
        ['Chỉ tiêu', 'Đơn vị', '2023', '2024', '2025', '9T/2026', 'Nguồn'],
        [['Tăng trưởng GDP', '%', n(4.98, 2), n(7.04, 2), n(8.02, 2), n(G9M, 2), 'NSO'],
         ['Lạm phát bình quân', '%', n(3.25, 2), n(3.63, 2), n(3.31, 2), n(CPI_AVG_9M, 2), 'NSO'],
         ['Xuất khẩu hàng hoá', 'tỷ USD', n(354.721, 1), n(405.9354, 1), n(474.9978, 1), n(EXP9M, 1), 'NSO'],
         ['FDI giải ngân', 'tỷ USD', n(23.183, 2), n(25.351, 2), n(27.62, 2), n(FDI9M, 2), 'NSO, Cục ĐTNN'],
         ['Tăng trưởng tín dụng', '%', n(13.78, 2), n(15.09, 2), n(19.07, 2), n(CR_YTD_SEP26, 2) + ' (YTD)', 'NHNN'],
         ['Tăng trưởng huy động', '%', n(13.2, 2), n(10.66, 2), n(15.42, 2), None, 'NHNN, IMF FSI'],
         ['Nợ xấu (IMF FSI)', '%', n(5.41, 2), n(4.85, 2), n(4.21, 2), None, 'IMF'],
         ['Thu ngân sách', 'nghìn tỷ', n(1770.776, 1), n(2057.544, 1), n(2650.1, 1), n(BUDGET_REV_9M, 1), 'Bộ Tài chính'],
         ['Chi ngân sách', 'nghìn tỷ', n(1936.912, 1), n(2148.477, 1), n(2401.5, 1), n(1873.7, 1), 'Bộ Tài chính'],
         ['Cán cân vãng lai', 'tỷ USD', n(25.793, 1), n(30.175, 1), n(33.135, 1), None, 'IMF BPM6'],
         ['Dân số trung bình', 'triệu người', n(100.3092, 2), n(101.3438, 2), n(102.3453, 2), None, 'NSO']],
        number_cols=(2, 3, 4, 5), col_widths=[3.0, 1.35, 1.35, 1.35, 1.35, 1.6, 2.033],
        note=('Dấu "—": chưa công bố cho kỳ đó, không phải bằng 0.',
              '2025 là ước tính; tín dụng 9T/2026 là tăng từ đầu năm, không so được với số cuối năm.'),
        source='Nguồn: NSO; NHNN; IMF FSI và BPM6; Bộ Tài chính — số liệu dashboard đến 7/10/2026.')

d.chart('Phụ lục · Biểu đồ gốc', 'Biểu đồ gốc PowerPoint: cùng dữ liệu FDI, có thể sửa số trong Excel',
        {'type': 'bar', 'editable': True, 'categories': yrs(2010, 2025),
         'series': [{'name': 'FDI giải ngân (tỷ USD)', 'values': [round(v, 2) for v in FDI_DIS]}], 'dec': 1,
         'highlight': mx_fdi, 'labels': True},
        note=('Biểu đồ gốc (editable=True) dùng khi người nhận cần sửa số liệu; bản vẽ bằng hình giữ đúng phong cách.',
              'PowerPoint không bo tròn đầu cột trong biểu đồ gốc; nhãn và màu vẫn theo hệ thống của dashboard.'),
        source='Nguồn: NSO / Cục Đầu tư nước ngoài — FDI thực hiện 2010–2025, tỷ USD.')

d.sources('Phụ lục · Nguồn và phương pháp', 'Nguồn số liệu, kỳ số liệu và cách tính',
          [('NSO — Cục Thống kê', 'GDP, CPI, xuất khẩu, FDI, vốn đầu tư; GRDP địa phương', '2010 – 9T/2026',
            'nso.gov.vn · pxweb.nso.gov.vn'),
           ('Ngân hàng Nhà nước (NHNN)', 'Tín dụng, huy động, M2; văn bản điều hành', '2015 – 9/2026', 'sbv.gov.vn'),
           ('Bộ Tài chính', 'Quyết toán, ước thực hiện và dự toán NSNN', '2018 – 2026', 'mof.gov.vn'),
           ('IMF', 'WEO 4/2026; Financial Soundness Indicators; BoP (BPM6)', '2015 – 2030', 'imf.org · data.imf.org'),
           ('Liên Hợp Quốc', 'World Population Prospects 2024, phương án trung bình', '2025, 2050', 'population.un.org'),
           ('ADB', 'Asian Development Outlook 9/2026 (dự báo tăng trưởng)', '2026 – 2027', 'adb.org'),
           ('Vietnam Dashboard', 'Mô hình lãi suất huy động v2; phân tích hệ thống tài chính', '7/10/2026',
            'data/simulation.json · data/finance.json'),
           ('Quốc hội, Chính phủ, Bộ Chính trị', 'NQ 244/2025/QH15, NQ 25/2026/QH16, NQ 10-NQ/TW, NĐ 245/2026',
            '2025 – 2026', 'data/policy.json')],
          method=['Mọi con số trong tiêu đề và ghi chú được tính từ dữ liệu khi dựng slide.',
                  'Ước tính, dự toán, dự báo: nét đứt, viền đứt hoặc ô rỗng, có ghi chú.',
                  'Chỉ tiêu phái sinh đánh dấu ≈ và nói rõ cách tính (ví dụ "chi vượt thu").',
                  'Khoảng trống để trống ("—"), không thay bằng 0, không nội suy.',
                  'Không trộn định nghĩa trong một chuỗi; số 9 tháng không so trực tiếp với số cả năm.',
                  'Mô hình của dashboard là minh hoạ, không phải dự báo chính thức.',
                  'Mô tả số liệu — không phải khuyến nghị đầu tư.'],
          source='Vietnam Dashboard · bộ mẫu vn-data-story · số liệu đến 7/10/2026.')

out = sys.argv[1] if len(sys.argv) > 1 else os.path.join(os.path.dirname(os.path.abspath(__file__)), 'example.pptx')
d.save(out)
print(out, d.n, 'slides; embedded font files:', d.embedded)
