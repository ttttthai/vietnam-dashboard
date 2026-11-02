"""Sample deck: one slide per visualization method of vn_deck (51 methods in 8 chapters + catalogue), Vietnamese,
built with the dashboard's real figures. Charts are native PowerPoint charts (Edit Data works) unless the form has no
PowerPoint chart type (tables with cell fills, or grouped shapes with the source numbers in the slide notes).

Values are copied from the dashboard's data files as of 2026-10-07 (data/economy.json, finance.json, society.json,
simulation.json, policy.json) and labelled with publisher + period on every slide. Every number in a headline or note
is computed from these arrays at build time (never typed). Illustrative content says "minh hoạ".

    python3 example_deck.py [out.pptx]          -> example.pptx next to this script by default
"""
import os
import sys
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from vn_deck import Deck, BLUE, ORANGE, AQUA, YELLOW, GREEN, INK, REDS, MUTE, _quart  # noqa: E402

TODAY = date(2026, 10, 7)
d = Deck(lang='vi')                                   # editable=True is the default: native charts
n = d.num
yrs = lambda a, b: [str(y) for y in range(a, b + 1)]

# ── data (copied from data/*.json; see each source line) ──
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
GDPPC25 = 5026                                   # GDP per capita 2025, USD (ECON_OFFICIAL.gdppc_usd)

# ── extra series for the full catalogue (copied from data/*.json on 2026-10-07) ──
FX_RES = [12.47, 13.54, 25.57, 25.89, 34.19, 28.25, 36.53, 49.08, 55.45, 78.33, 94.83, 109.37, 86.54, 92.24, 83.08,
          85.58]                                  # 2010–2025, bn USD, excl. gold (economy.json ECON_OFFICIAL)
EXP14 = 150.2171                                  # exports 2014, bn USD (ECON_OFFICIAL.export_busd)
IMP_Y = [165.7759, 174.9784, 213.2153, 237.2416, 253.6965, 262.791, 332.9697, 359.7801, 326.6069, 381.3037,
         454.9264]                                # imports 2015–2025, bn USD
CR_SEC_M = ['1/25', '2/25', '3/25', '4/25', '5/25', '6/25', '7/25', '8/25', '9/25', '10/25', '11/25', '12/25', '1/26',
            '2/26', '3/26', '4/26', '5/26', '6/26', '7/26']
CR_SEC = {  # credit outstanding by sector, tn VND (finance.json FINSYS.sectors.levels / 1000, rounded)
    'Nông, lâm, thủy sản': [1023, 1022, 1037, 1048, 1057, 1076, 1073, 1083, 1098, 1107, 1119, 1135, 1144, 1153, 1175,
                             1192, 1203, 1225, 1233],
    'Công nghiệp, xây dựng': [3925, 3926, 3985, 4048, 4069, 4122, 4126, 4179, 4267, 4314, 4336, 4354, 4404, 4461, 4558,
                               4628, 4685, 4736, 4782],
    'Thương mại, vận tải, viễn thông': [4421, 4427, 4577, 4620, 4668, 4769, 4735, 4765, 4793, 4794, 4850, 4888, 4903,
                                         4870, 4944, 4974, 4987, 5081, 5088],
    'Dịch vụ khác': [6334, 6360, 6627, 6731, 6872, 7196, 7281, 7429, 7624, 7793, 7942, 8219, 8366, 8358, 8510, 8653,
                     8829, 9109, 9159]}
CR_GROW = {  # credit growth by sector, % vs end of previous year (finance.json FINSYS.flows, SBV)
    'Nông, lâm, thủy sản': (11.01, 8.63), 'Công nghiệp': (10.09, 10.44), 'Xây dựng': (17.46, 8.59),
    'Thương mại': (9.62, 2.76), 'Vận tải, viễn thông': (24.75, 16.59), 'Dịch vụ khác': (30.18, 11.44),
    'Toàn nền kinh tế': (19.07, 8.97)}
OMO = [(date(2024, 1, 1), 4.5), (date(2024, 8, 5), 4.25), (date(2024, 9, 16), 4.0), (date(2025, 12, 4), 4.5)]
REFI = [(date(2024, 1, 1), 4.5)]                  # refinancing rate 4.5% since 19/6/2023 (finance.json FINSYS.sbv)
RATE_ASOF = date(2026, 10, 2)
CPI_FC = [('IMF', 4.9, 4.6), ('World Bank', 4.2, 3.8), ('ADB', 4.3, 4.0), ('AMRO', 4.3, 3.9), ('OECD', 5.2, 4.6),
          ('Standard Chartered', 4.4, 3.3)]       # CPI forecasts 2026 / 2027 (economy.json CPI_FC_INST)
CPI_TARGET26 = 4.5
# GRDP 2025 by province: bn VND, avg population (thousand), structure (agri, ind, svc, tax) — society.json SOC_GRDP
GRDP = {'TP. Hồ Chí Minh': (2972536, 13803.8), 'Hà Nội': (1588060, 8857.53), 'Hải Phòng': (734519, 4149.87),
        'Đồng Nai': (677638, 4493.7), 'Bắc Ninh': (522116, 3572.35), 'Phú Thọ': (412151, 3701.53),
        'Quảng Ninh': (368707, 1411.58), 'Lâm Đồng': (353258, 3356.38), 'Tây Ninh': (344556, 2980.28),
        'Ninh Bình': (342779, 3843.77), 'Thanh Hóa': (333754, 3771.77), 'Hưng Yên': (319824, 3235.17),
        'Đà Nẵng': (316054, 2856.38), 'Cần Thơ': (306118, 3224.94), 'An Giang': (296383, 3694.6),
        'Đồng Tháp': (285983, 3403.78), 'Vĩnh Long': (278466, 3379.61), 'Gia Lai': (270640, 3182.65),
        'Nghệ An': (236488, 3497.75), 'Đắk Lắk': (229539, 2849.2), 'Khánh Hòa': (209446, 1898.55),
        'Thái Nguyên': (192385, 1711.58), 'Quảng Ngãi': (191614, 1881.24), 'Cà Mau': (169047, 2150.44),
        'Lào Cai': (141887, 1675.67), 'Quảng Trị': (123401, 1594.13), 'Hà Tĩnh': (120845, 1335.14),
        'Sơn La': (95738, 1341.22), 'Tuyên Quang': (95471, 1748.25), 'Huế': (89600, 1190.34),
        'Lạng Sơn': (58223, 820.63), 'Lai Châu': (39807, 501.16), 'Điện Biên': (35631, 667.42),
        'Cao Bằng': (27914, 562.93)}
STRUCT_ALL = {'TP. Hồ Chí Minh': (1.7, 35.2, 52.2, 10.9), 'Hà Nội': (1.86, 22.25, 66.44, 9.45),
              'Hải Phòng': (4.55, 53.96, 35.2, 6.29), 'Đồng Nai': (12.13, 54.97, 26.42, 6.48),
              'Bắc Ninh': (6.52, 71.13, 19.41, 2.94), 'Phú Thọ': (11.01, 48.82, 28.24, 11.93),
              'Quảng Ninh': (4.6, 46.2, 37.8, 11.4), 'Lâm Đồng': (38.59, 20.87, 35.83, 4.71),
              'Ninh Bình': (11.04, 47.05, 34.39, 7.52), 'Thanh Hóa': (13.25, 47.94, 32.93, 5.87)}
REGION = {}
for p_ in PROV:
    REGION[p_[0]] = 'Miền Bắc' if p_[7] == 0 else ('Miền Nam' if p_[0] in (
        'Tây Ninh', 'Đồng Nai', 'TP. Hồ Chí Minh', 'Đồng Tháp', 'An Giang', 'Vĩnh Long', 'Cần Thơ', 'Cà Mau')
        else 'Miền Trung, Tây Nguyên')
PI23 = {'total_pct': 82.47, 'ttg_pct': 93.12}     # public investment 2023: disbursed % of total plan / of PM plan
BUD_REV_9M_PCT = BUDGET_REV_9M / BUDGET_REV_PLAN26 * 100


def dec_year(dt):
    d0 = date(dt.year, 1, 1)
    return dt.year + (dt - d0).days / ((date(dt.year + 1, 1, 1) - d0).days)


SRC_PROV = 'Nguồn: NSO và cục thống kê địa phương — GRDP 2025 (ước tính), 34 đơn vị sau sắp xếp hành chính.'

# ═══ 0. cover + catalogue ═══
d.cover('Báo cáo kinh tế · ngân hàng · bộ mẫu trình bày',
        f'Kinh tế 9 tháng 2026: GDP tăng {n(G9M, 2)}%, tín dụng chạy trước huy động',
        'Năm mươi mốt cách trình bày dữ liệu theo phong cách Vietnam Dashboard — biểu đồ PowerPoint gốc, sửa được số '
        'liệu; mỗi trang một phát hiện.',
        'Cập nhật 7/10/2026',
        chart={'type': 'bar', 'categories': yrs(2018, 2025), 'series': [{'name': 'Tăng trưởng GDP (%)',
                                                                         'values': GDP_G[8:]}],
               'dec': 1, 'highlight': 7, 'labels': 'hi', 'grid': False, 'y_title': 'Tăng trưởng GDP, %'},
        note='Nguồn số liệu: NSO, NHNN, Bộ Tài chính, IMF, World Bank, UN WPP 2024 và mô hình của dashboard. '
             'Mô tả số liệu — không phải khuyến nghị đầu tư.')
d.catalog('Mục lục · danh mục cách trình bày', 'Năm mươi mốt cách trình bày, mỗi cách một trang',
          'Mỗi dòng: số trang, tên cách trình bày và cách dựng trong PowerPoint.')

# ═══ 1. opening numbers ═══
d.section(1, 'Con số mở đầu', 'Một con số lớn, bốn chỉ số và một nhận định — cách mở một báo cáo.',
          ['Con số lớn', 'Bốn thẻ chỉ số', 'Trích dẫn nhận định'])
best = max(range(len(GDP_G)), key=lambda i: GDP_G[i])
d.hero('Tổng quan · 9 tháng 2026', f'GDP 9 tháng 2026 tăng {n(G9M, 2)}%; riêng quý 3 tăng {n(Q26["Q3"], 2)}%',
       n(G9M, 2), '%',
       f'So với cùng kỳ 2025. Tốc độ tăng dần qua từng quý; mục tiêu cả năm của Quốc hội là từ {n(TARGET_G, 0)}%. '
       f'Mức cả năm cao nhất 2010–2025 là {n(GDP_G[best], 2)}% ({2010 + best}).',
       counts=[(k + '/2026', n(v, 2) + '%', v) for k, v in Q26.items()],
       chart={'type': 'bar', 'categories': yrs(2015, 2025), 'series': [{'name': 'GDP', 'values': GDP_G[5:]}],
              'dec': 1, 'unit': '', 'highlight': 10, 'labels': 'auto'},
       chart_title='Tăng trưởng GDP cả năm, 2015–2025 (%)',
       note=(f'Quý 3/2026 tăng {n(Q26["Q3"], 2)}%, cao hơn {n(Q26["Q3"] - Q26["Q1"], 2)} điểm % so với quý 1.',
             'Số 9 tháng so với cùng kỳ, không so trực tiếp với số cả năm ở biểu đồ bên phải.'),
       source='Nguồn: NSO — thông cáo quý III và 9 tháng 2026 (03/10/2026); GDP năm theo chuỗi đánh giá lại; '
              '2025 ước tính.', method='Con số lớn (hero)')
cpi_d = CPI24[-1] - CPI24[-13]
d.kpis('Bốn chỉ số · tháng 9/2026', f'Xuất khẩu 9 tháng tăng {n(EXP9M_YOY, 1)}%; lạm phát lên {n(CPI24[-1], 2)}%',
       [{'label': 'CPI so cùng kỳ', 'value': n(CPI24[-1], 2), 'unit': '%', 'delta': cpi_d, 'delta_unit': ' điểm %',
         'delta_dec': 2, 'tone': REDS, 'delta_label': 'so với T9/2025', 'spark': CPI24[-12:],
         'spark_cats': CPI_M, 'sub': 'Tháng 9/2026. Đường nhỏ: 12 tháng gần nhất.'},
        {'label': 'Tín dụng từ đầu năm', 'value': n(CR_YTD_SEP26, 2), 'unit': '%',
         'delta': CR_YTD_SEP26 - CR_YTD_SEP25, 'delta_dec': 2, 'delta_unit': ' điểm %',
         'delta_label': 'so với 30/9/2025', 'spark': CR_G, 'spark_kind': 'col', 'spark_cats': yrs(2015, 2025),
         'sub': 'Đến 30/9/2026. Cột nhỏ: tăng trưởng cả năm 2015–2025.'},
        {'label': 'Xuất khẩu 9 tháng', 'value': n(EXP9M, 1), 'unit': 'tỷ USD', 'delta': EXP9M_YOY, 'delta_unit': '%',
         'delta_label': 'so với 9T/2025', 'spark': EXP_Y, 'spark_kind': 'col', 'spark_cats': yrs(2015, 2025),
         'sub': 'Hàng hoá, tháng 1–9/2026. Cột nhỏ: cả năm 2015–2025.'},
        {'label': 'FDI giải ngân 9 tháng', 'value': n(FDI9M, 2), 'unit': 'tỷ USD', 'delta': FDI9M_YOY,
         'delta_unit': '%', 'delta_label': 'so với 9T/2025', 'spark': FDI_DIS[5:], 'spark_kind': 'col',
         'spark_cats': yrs(2015, 2025), 'sub': 'Vốn thực hiện, tháng 1–9/2026. Cột nhỏ: cả năm 2015–2025.'}],
       note=(f'CPI tháng 9/2026 tăng {n(CPI24[-1], 2)}% so với cùng kỳ, cao hơn {n(cpi_d, 2)} điểm % so với một năm '
             f'trước; tín dụng tăng {n(CR_YTD_SEP26, 2)}% từ đầu năm.',
             f'Tín dụng tăng chậm hơn cùng kỳ 2025 ({n(CR_YTD_SEP25, 2)}%), dù vẫn nhanh hơn huy động.'),
       source='Nguồn: NSO (CPI, xuất khẩu, FDI; 03/10/2026); NHNN (tín dụng đến 30/9/2026). Mũi tên: xanh = tăng, '
              'đỏ = giảm, trừ CPI (đỏ = giá tăng).', method='Thẻ chỉ số (KPI)')
d.quote('Chương 1 · Nhận định',
        'Tín dụng tăng nhanh hơn huy động trong 9/11 năm 2015–2025, trừ 2019 (13,65% so với 14,54%) và 2020 '
        '(12,2% so với 13,3%); khoảng chênh mở rộng từ 2021, lớn nhất năm 2022 (14,2% so với 5,99%) và 2024 '
        '(15,09% so với 10,66%).',
        'Vietnam Dashboard', 'Phân tích Hệ thống tài chính · 6/10/2026',
        facts=[('Năm tín dụng nhanh hơn', f'{sum(1 for a, b in zip(CR_G, DEP_G) if a > b)}/11', '2015–2025, NHNN'),
               ('Chênh lệch 2022', f'{n(CR_G[7] - DEP_G[7], 2)} đ.%', f'{n(CR_G[7], 1)}% so với {n(DEP_G[7], 2)}%'),
               ('Cho vay vượt tiền gửi', '2023', '13,30 so với 13,26 triệu tỷ (IMF FSI)')],
        note=('Nguyên nhân chính theo phân tích: cầu vốn từ mục tiêu tăng trưởng cao và chỉ tiêu tín dụng nới rộng.',
              'Trích nguyên văn; các con số bên phải tính lại từ chuỗi NHNN và IMF FSI của dashboard.'),
        source='Nguồn: Vietnam Dashboard — FINSYS.analysis (6/10/2026), dựa trên NHNN, IMF FSI.',
        method='Trích dẫn nhận định')

# ═══ 2. change over time ═══
d.section(2, 'Diễn biến theo thời gian', 'Đường, bậc thang, vùng, quạt dự báo và biểu đồ kết hợp hai trục.',
          ['Đường và đường tô điểm', 'Bậc thang lãi suất', 'Vùng, vùng chồng, vùng 100%', 'Quạt dự báo',
           'Cột + đường, hai trục'])
act_series = GDP_G + [None] * 5
imf_series = [None] * 15 + [GDP_G[-1]] + IMF_G
d.chart('Chương 2 · Tăng trưởng', f'IMF dự báo tăng trưởng chậm dần về {n(IMF_G[-1], 1)}% năm 2030, '
                                  f'thấp hơn mục tiêu {n(TARGET_G, 0)}%/năm',
        {'type': 'line', 'categories': yrs(2010, 2030), 'dec': 1, 'unit': '%', 'y_min': 0, 'y_max': 12,
         'series': [{'name': 'Thực tế (NSO)', 'values': act_series, 'color': INK, 'end_label': False, 'markers': None},
                    {'name': 'IMF WEO 4/2026', 'label': 'IMF', 'values': imf_series, 'color': BLUE, 'dashed': True}],
         'forecast_from': 16, 'forecast_label': 'Dự báo',
         'refs': [{'value': TARGET_G, 'label': f'Mục tiêu từ {n(TARGET_G, 0)}%/năm'}],
         'annotations': [{'series': 0, 'at': 15, 'text': f'2025: {n(GDP_G[-1], 2)}%', 'dx': -0.35, 'dy': -0.5},
                         {'series': 0, 'at': 11, 'text': f'2021: {n(GDP_G[11], 2)}% (Covid-19)', 'tier': 'sup',
                          'dx': -0.45, 'dy': 0.16}]},
        note=(f'IMF (4/2026) dự báo {n(IMF_G[0], 1)}% năm 2026, giảm dần về {n(IMF_G[-1], 1)}% năm 2030; chín tháng '
              f'2026 thực tế đã tăng {n(G9M, 2)}%.',
              'Mục tiêu là mức sàn chính sách, không phải dự báo; IMF sẽ cập nhật trong WEO tháng 10/2026.'),
        source='Nguồn: NSO (2010–2025, 2025 ước tính); IMF World Economic Outlook 4/2026; Quốc hội — NQ 25/2026/QH16. '
               'Vùng tô: giai đoạn dự báo.', method='Đường (line)')
gap25 = CR_G[-1] - DEP_G[-1]
d.chart('Chương 2 · Tín dụng', f'Tín dụng tăng {n(CR_G[-1], 2)}% năm 2025, nhanh hơn huy động {n(gap25, 2)} điểm %',
        {'type': 'line', 'categories': yrs(2015, 2025), 'dec': 2, 'unit': '%', 'zero': True, 'tick_dec': 0,
         'series': [{'name': 'Huy động', 'values': DEP_G, 'muted': True},
                    {'name': 'M2', 'values': M2_G, 'muted': True},
                    {'name': 'Tín dụng', 'values': CR_G, 'color': BLUE}],
         'annotations': [{'series': 0, 'at': 7, 'text': f'2022: huy động chỉ {n(DEP_G[7], 2)}%',
                          'value': DEP_G[7], 'dx': 0.4, 'dy': 0.35}]},
        note=(f'Khoảng cách tín dụng − huy động lớn nhất năm 2022 ({n(CR_G[7] - DEP_G[7], 2)} điểm %), năm 2025 là '
              f'{n(gap25, 2)} điểm %.',
              f'Tín dụng nhanh hơn huy động trong {sum(1 for a, b in zip(CR_G, DEP_G) if a > b)}/11 năm 2015–2025.'),
        source='Nguồn: NHNN; IMF FSI (tiền gửi khách hàng); tăng trưởng cuối năm so với cuối năm trước.',
        method='Nhiều đường, tô một đường')
omo_last = OMO[-1][1]
d.chart('Chương 2 · Lãi suất điều hành', f'Lãi suất OMO về lại {n(omo_last, 1)}% từ 12/2025, bằng lãi suất tái cấp vốn',
        {'type': 'step', 'dec': 2, 'unit': '%', 'y_min': 0, 'y_max': 5, 'x_min': 2024, 'x_max': 2027, 'x_step': 1,
         'x_label': lambda v: f'{int(v)}-{min(12, int(round((v % 1) * 12)) + 1):02d}',
         'series': [{'name': 'Tái cấp vốn', 'points': [(dec_year(a), v) for a, v in REFI], 'color': MUTE,
                     'until': dec_year(RATE_ASOF)},
                    {'name': 'Thị trường mở (OMO)', 'label': 'OMO', 'points': [(dec_year(a), v) for a, v in OMO],
                     'color': BLUE, 'until': dec_year(RATE_ASOF)}],
         'annotations': [{'x': dec_year(OMO[2][0]), 'y': OMO[2][1], 'text': f'16/9/2024: hạ xuống {n(OMO[2][1], 2)}%',
                          'dx': 0.3, 'dy': 0.45},
                         {'x': dec_year(OMO[3][0]), 'y': OMO[3][1], 'text': f'4/12/2025: lên {n(OMO[3][1], 2)}%',
                          'dx': -0.3, 'dy': -0.4}]},
        note=(f'NHNN hạ lãi suất OMO hai lần năm 2024 (xuống {n(OMO[2][1], 2)}%) rồi nâng lại {n(omo_last, 2)}% từ '
              f'4/12/2025; tái cấp vốn giữ {n(REFI[0][1], 2)}%.',
              'Đường bậc thang vì lãi suất đổi theo quyết định, không thay đổi dần giữa các mốc.'),
        source='Nguồn: NHNN — Báo cáo thường niên 2024; thông báo OMO đến 02/10/2026 (data/finance.json, FINSYS.sbv).',
        method='Đường bậc thang (step)')
res_max = max(range(len(FX_RES)), key=lambda i: FX_RES[i])
d.chart('Chương 2 · Dự trữ ngoại hối', f'Dự trữ ngoại hối {n(FX_RES[-1], 1)} tỷ USD cuối 2025, thấp hơn đỉnh '
                                       f'{n(FX_RES[res_max], 1)} tỷ năm {2010 + res_max}',
        {'type': 'area', 'categories': yrs(2010, 2025), 'series': [{'name': 'Dự trữ ngoại hối', 'values': FX_RES,
                                                                     'color': BLUE}],
         'dec': 1, 'unit': ' tỷ USD', 'y_title': 'Tỷ USD, cuối năm, không gồm vàng',
         'annotations': [{'at': res_max, 'value': FX_RES[res_max], 'text': f'{2010 + res_max}: {n(FX_RES[res_max], 1)}',
                          'dx': 0.35, 'dy': -0.3}]},
        note=(f'Dự trữ giảm {n(FX_RES[res_max] - FX_RES[-1], 1)} tỷ USD từ đỉnh {2010 + res_max}; năm 2025 tăng '
              f'{n(FX_RES[-1] - FX_RES[-2], 1)} tỷ.',
              'Không gồm vàng; số IMF "gồm vàng" cao hơn khoảng 1,4 tỷ USD năm 2025 — không trộn hai định nghĩa.'),
        source='Nguồn: IMF International Liquidity (dự trữ không gồm vàng), cuối năm 2010–2025.', method='Vùng (area)')
tot25 = INV['state'][-1] + INV['nonstate'][-1] + INV['fdi'][-1]
d.chart('Chương 2 · Đầu tư', f'Khu vực ngoài nhà nước bỏ {n(INV["nonstate"][-1] / tot25 * 100, 0)}% vốn đầu tư '
                             f'toàn xã hội năm 2025',
        {'type': 'area', 'categories': yrs(2015, 2025), 'dec': 0, 'unit': '', 'y_title': 'Nghìn tỷ đồng',
         'series': [{'name': 'Ngoài nhà nước', 'values': [v / 1000 for v in INV['nonstate']], 'color': BLUE},
                    {'name': 'Nhà nước', 'values': [v / 1000 for v in INV['state']], 'color': ORANGE},
                    {'name': 'FDI', 'values': [v / 1000 for v in INV['fdi']], 'color': AQUA}]},
        note=(f'Vốn đầu tư thực hiện đạt {n(tot25 / 1000, 0)} nghìn tỷ năm 2025, gấp '
              f'{n(tot25 / (INV["state"][0] + INV["nonstate"][0] + INV["fdi"][0]), 1)} lần năm 2015.',
              'Chín tháng 2026, vốn Nhà nước tăng 17,3%, nhanh hơn ngoài nhà nước (14,0%) và FDI (14,5%).'),
        source='Nguồn: NSO V04.01 — vốn đầu tư thực hiện toàn xã hội theo nguồn, giá hiện hành; 9T/2026: NSO 03/10/2026.',
        method='Vùng chồng (stacked area)')
crt = [sum(v[k] for v in CR_SEC.values()) for k in range(len(CR_SEC_M))]
oth0, oth1 = CR_SEC['Dịch vụ khác'][0] / crt[0] * 100, CR_SEC['Dịch vụ khác'][-1] / crt[-1] * 100
d.chart('Chương 2 · Tín dụng theo ngành', f'Dịch vụ khác chiếm {n(oth1, 1)}% dư nợ tháng 7/2026, từ {n(oth0, 1)}% '
                                          f'đầu năm 2025',
        {'type': 'area100', 'categories': CR_SEC_M, 'dec': 0,
         'series': [{'name': k, 'values': v, 'color': c} for (k, v), c in zip(CR_SEC.items(), (GREEN, ORANGE, AQUA, BLUE))]},
        note=(f'Tỷ trọng dịch vụ khác tăng {n(oth1 - oth0, 1)} điểm % trong 19 tháng; nông nghiệp giữ quanh '
              f'{n(CR_SEC["Nông, lâm, thủy sản"][-1] / crt[-1] * 100, 0)}%.',
              'Mỗi tháng là 100% dư nợ bốn khối ngành; "dịch vụ khác" gồm bất động sản, tiêu dùng, tài chính.'),
        source='Nguồn: NHNN — dư nợ tín dụng đối với nền kinh tế theo ngành, 1/2025–7/2026 (data/finance.json).',
        method='Vùng chồng 100%')
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
               'minh hoạ, không phải dự báo chính thức.', method='Quạt dự báo (fan)')
EXP_G = [(b / a - 1) * 100 for a, b in zip([EXP14] + EXP_Y[:-1], EXP_Y)]
neg_y = [2015 + i for i, g in enumerate(EXP_G) if g < 0]
d.chart('Chương 2 · Xuất khẩu', f'Xuất khẩu đạt {n(EXP_Y[-1], 1)} tỷ USD năm 2025, tăng {n(EXP_G[-1], 1)}%; '
                                f'chỉ giảm năm {", ".join(map(str, neg_y))}',
        {'type': 'combo', 'categories': yrs(2015, 2025),
         'bar': {'name': 'Kim ngạch xuất khẩu (tỷ USD, trục trái)', 'values': EXP_Y, 'dec': 0, 'color': MUTE,
                 'axis_title': 'Tỷ USD'},
         'line': {'name': 'Tăng trưởng so với năm trước (%, trục phải)', 'values': EXP_G, 'dec': 1, 'unit': '%',
                  'color': BLUE, 'axis_title': '% so với năm trước'}},
        note=(f'Năm {neg_y[0]} xuất khẩu giảm {n(-EXP_G[neg_y[0] - 2015], 1)}%; năm 2025 tăng {n(EXP_G[-1], 1)}% lên '
              f'{n(EXP_Y[-1], 1)} tỷ USD.',
              'Hai trục, hai đơn vị: cột đọc theo trục trái, đường theo trục phải; hai vạch 0 thẳng hàng. '
              'Không so độ cao cột với đường.'),
        source='Nguồn: NSO — kim ngạch xuất khẩu hàng hoá 2014–2025; tăng trưởng tính từ kim ngạch (≈).',
        method='Kết hợp cột + đường (hai trục)')

# ═══ 3. comparison and ranking ═══
d.section(3, 'So sánh và xếp hạng', 'Cột, thanh, kẹo mút, chấm, quả tạ, độ dốc, thứ hạng, phân kỳ và lốc xoáy.',
          ['Cột và cột nhóm', 'Thanh ngang xếp hạng và nhóm', 'Kẹo mút, chấm, quả tạ', 'Độ dốc, thứ hạng',
           'Phân kỳ, lốc xoáy'])
mx_fdi = max(range(len(FDI_DIS)), key=lambda i: FDI_DIS[i])
d.chart('Chương 3 · FDI', f'FDI giải ngân đạt {n(FDI_DIS[-1], 1)} tỷ USD năm 2025, cao nhất từ 2010',
        {'type': 'bar', 'categories': yrs(2010, 2025), 'series': [{'name': 'FDI giải ngân', 'values': FDI_DIS}],
         'dec': 1, 'unit': '', 'highlight': mx_fdi, 'y_title': 'Tỷ USD', 'labels': 'auto'},
        panel={'label': '9 tháng 2026', 'value': n(FDI9M, 2), 'unit': 'tỷ USD',
               'sub': f'Tăng {n(FDI9M_YOY, 1)}% so với cùng kỳ; mục tiêu NQ 10-NQ/TW: {n(FDI_TARGET_LOW, 0)}–40 tỷ USD/năm.'},
        note=(f'FDI giải ngân năm 2025 đạt {n(FDI_DIS[-1], 2)} tỷ USD, gấp {n(FDI_DIS[-1] / FDI_DIS[0], 1)} lần năm 2010.',
              'Vốn đăng ký là chỉ tiêu khác, không cộng chung; 2025 ước tính.'),
        source='Nguồn: NSO / Cục Đầu tư nước ngoài — FDI thực hiện (giải ngân), tỷ USD; 9T/2026: NSO 03/10/2026.',
        method='Cột (column)')
tb = [x - m for x, m in zip(EXP_Y, IMP_Y)]
d.chart('Chương 3 · Thương mại', f'Xuất khẩu vượt nhập khẩu {sum(1 for v in tb if v > 0)}/11 năm 2015–2025; năm 2025 '
                                 f'xuất siêu {n(tb[-1], 1)} tỷ USD',
        {'type': 'bar', 'categories': yrs(2015, 2025), 'dec': 0, 'y_title': 'Tỷ USD', 'labels': False,
         'series': [{'name': 'Xuất khẩu', 'values': EXP_Y, 'color': BLUE},
                    {'name': 'Nhập khẩu', 'values': IMP_Y, 'color': ORANGE}]},
        note=(f'Năm 2025: xuất khẩu {n(EXP_Y[-1], 1)}, nhập khẩu {n(IMP_Y[-1], 1)} tỷ USD; năm 2015 nhập siêu '
              f'{n(-tb[0], 1)} tỷ.', 'Hàng hoá, không gồm dịch vụ; chín tháng 2026 nhập siêu 19,4 tỷ USD (NSO).'),
        source='Nguồn: NSO — xuất, nhập khẩu hàng hoá 2015–2025 (tỷ USD).', method='Cột nhóm (grouped column)')
top12 = sorted(PROV, key=lambda p: -p[2])[:12]
PV = {p[0]: p for p in PROV}
d.chart('Chương 3 · Xếp hạng', f'{top12[0][0]} tăng {n(top12[0][2], 2)}% năm 2025, nhanh nhất cả nước; '
                               f'{sum(1 for p in PROV if p[2] >= 10)} địa phương đạt từ 10%',
        {'type': 'barh', 'categories': [p[0] for p in top12], 'values': [p[2] for p in top12], 'dec': 2, 'unit': '%',
         'highlight': [i for i, p in enumerate(top12) if p[2] >= 10]},
        note=(f'Mười hai địa phương tăng nhanh nhất đều đạt từ {n(top12[-1][2], 2)}%; Hà Nội ({n(PV["Hà Nội"][2], 2)}%) '
              f'và TP.HCM ({n(PV["TP. Hồ Chí Minh"][2], 2)}%) không nằm trong nhóm này.',
              'Thanh xanh: từ 10% trở lên; xám: dưới 10%. Số 2025 là ước tính.'),
        source='Nguồn: NSO và cục thống kê địa phương — tăng trưởng GRDP 2025 (giá so sánh).',
        method='Thanh ngang xếp hạng')
cg = list(CR_GROW.items())
fast26 = max(cg[:-1], key=lambda kv: kv[1][1])
d.chart('Chương 3 · Tín dụng theo ngành', f'Bảy tháng 2026, tín dụng vận tải, viễn thông tăng {n(fast26[1][1], 2)}%, '
                                          f'nhanh nhất; toàn nền kinh tế {n(CR_GROW["Toàn nền kinh tế"][1], 2)}%',
        {'type': 'barh', 'categories': [k for k, _ in cg], 'dec': 2, 'unit': '%',
         'series': [{'name': 'Cả năm 2025', 'values': [v[0] for _, v in cg], 'color': MUTE},
                    {'name': '7 tháng 2026', 'values': [v[1] for _, v in cg], 'color': BLUE}]},
        note=(f'Năm 2025 dịch vụ khác tăng {n(CR_GROW["Dịch vụ khác"][0], 2)}%; bảy tháng 2026 mới tăng '
              f'{n(CR_GROW["Dịch vụ khác"][1], 2)}%.',
              'Hai kỳ dài khác nhau (12 tháng và 7 tháng, cùng tính từ đầu năm) — so thứ tự, không so độ dài thanh.'),
        source='Nguồn: NHNN — tăng trưởng tín dụng theo ngành so với cuối năm trước, 12/2025 và 7/2026.',
        method='Thanh ngang nhóm (grouped bar)')
pc12 = sorted(PROV, key=lambda p: -p[3])[:12]
d.chart('Chương 3 · Thu nhập', f'{pc12[0][0]} có GRDP bình quân {n(pc12[0][3] / 1000, 1)} nghìn USD/người năm 2025, gấp '
                               f'{n(pc12[0][3] / pc12[-1][3], 1)} lần địa phương thứ 12',
        {'type': 'lollipop', 'categories': [p[1] for p in pc12], 'values': [p[3] / 1000 for p in pc12], 'dec': 1,
         'y_title': 'GRDP bình quân đầu người 2025, nghìn USD', 'highlight': [0]},
        note=(f'Mười hai địa phương giàu nhất từ {n(pc12[-1][3] / 1000, 1)} đến {n(pc12[0][3] / 1000, 1)} nghìn USD/người; '
              f'cả nước {n(GDPPC25 / 1000, 1)} nghìn.',
              'Kẹo mút thay cột khi nhiều thanh gần bằng nhau: ít mực hơn, đầu chấm cho biết giá trị.'),
        source='Nguồn: NSO và cục thống kê địa phương — GRDP bình quân đầu người 2025 (USD, ước tính).',
        method='Kẹo mút (lollipop)')
hi_fc = max(CPI_FC, key=lambda r: r[1])
lo_fc = min(CPI_FC, key=lambda r: r[1])
d.chart('Chương 3 · Dự báo lạm phát', f'Sáu tổ chức dự báo CPI 2026 từ {n(lo_fc[1], 1)}% đến {n(hi_fc[1], 1)}%; '
                                      f'đều thấp hơn cho 2027',
        {'type': 'dot', 'categories': [r[0] for r in CPI_FC], 'dec': 1, 'unit': '%', 'y_min': 3, 'y_max': 5.5,
         'series': [{'name': 'Dự báo 2026', 'values': [r[1] for r in CPI_FC], 'color': BLUE},
                    {'name': 'Dự báo 2027', 'values': [r[2] for r in CPI_FC], 'color': ORANGE, 'label_pos': 'r'}],
         'refs': [{'value': CPI_TARGET26, 'label': f'Trần mục tiêu 2026: {n(CPI_TARGET26, 1)}%'}]},
        note=(f'{hi_fc[0]} dự báo cao nhất ({n(hi_fc[1], 1)}%), {lo_fc[0]} thấp nhất ({n(lo_fc[1], 1)}%); bình quân 9 '
              f'tháng thực tế {n(CPI_AVG_9M, 2)}%.',
              'Các dự báo công bố ở các thời điểm khác nhau (4–9/2026); trục bắt đầu từ 3% vì là chấm, không phải cột.'),
        source='Nguồn: IMF WEO 4/2026; World Bank VEU 5/2026; ADB 9/2026; AMRO 9/2026; OECD 6/2026; Standard Chartered '
               '7/2026 (data/economy.json, CPI_FC_INST).', method='Biểu đồ chấm (dot plot)')
big = [p for p in PROV if p[0] in ('TP. Hồ Chí Minh', 'Hà Nội', 'Hải Phòng', 'Đồng Nai', 'Bắc Ninh', 'Quảng Ninh',
                                   'Thanh Hóa', 'Tây Ninh', 'Lâm Đồng', 'Ninh Bình', 'Phú Thọ', 'Cần Thơ')]
big.sort(key=lambda p: -p[4])
acc = max(big, key=lambda p: p[4] - p[2])
dec_ = [p[0] for p in big if p[4] < p[2]]
d.chart('Chương 3 · Tăng tốc', f'{acc[0]} tăng tốc mạnh nhất: {n(acc[4], 2)}% trong 9 tháng 2026, '
                               f'từ {n(acc[2], 2)}% cả năm 2025',
        {'type': 'dumbbell', 'dec': 2, 'unit': '%', 'labels': ('Cả năm 2025', '9 tháng 2026'),
         'rows': [{'name': p[1], 'a': p[2], 'b': p[4]} for p in big]},
        note=(f'{len(big) - len(dec_)}/{len(big)} địa phương lớn tăng nhanh hơn trong 9 tháng 2026 so với cả năm 2025'
              + (f'; chỉ {", ".join(dec_)} chậm lại.' if dec_ else '.'),
              '9 tháng so với cùng kỳ năm trước, không phải số cả năm; hai kỳ so sánh chỉ mang tính chỉ báo.'),
        source='Nguồn: NSO và cục thống kê địa phương — GRDP 2025 (ước tính) và 9 tháng 2026 (công bố 9–10/2026).',
        method='Quả tạ (dumbbell)')
d.chart('Chương 3 · Cơ cấu chi tiêu',
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
        source='Nguồn: NSO — GDP theo mục đích sử dụng, giá hiện hành (2024 sơ bộ, 2025 ước tính).',
        method='Độ dốc (slope)')
YB = [2010, 2013, 2016, 2019, 2022, 2025]
rank = lambda name, y: 1 + sum(1 for k in ACT if ACT[k][y - 2010] > ACT[name][y - 2010])
d.chart('Chương 3 · Các ngành', f'Khai khoáng tụt từ hạng {rank("Khai khoáng", 2010)} xuống hạng '
                                f'{rank("Khai khoáng", 2025)} về đóng góp vào GDP; chế biến giữ hạng 1',
        {'type': 'bump', 'periods': [str(y) for y in YB], 'dec': 1, 'unit': '%',
         'highlight': ['Chế biến, chế tạo', 'Khai khoáng'],
         'series': [{'name': k, 'values': [ACT[k][y - 2010] for y in YB]} for k in ACT]},
        note=(f'Chế biến, chế tạo chiếm {n(ACT["Chế biến, chế tạo"][-1], 1)}% GDP năm 2025, từ '
              f'{n(ACT["Chế biến, chế tạo"][0], 1)}% năm 2010; khai khoáng còn {n(ACT["Khai khoáng"][-1], 1)}%.',
              'Xếp hạng trong chín ngành lớn nhất theo tỷ trọng giá hiện hành; số bên phải là tỷ trọng năm 2025.'),
        source='Nguồn: NSO PxWeb — cơ cấu GDP theo ngành kinh tế, giá hiện hành, chuỗi đánh giá lại 2021; 2025 ước tính.',
        method='Thứ hạng (bump)')
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
        source='Nguồn: IMF Balance of Payments (BPM6) theo số NHNN, năm 2025.', method='Thanh phân kỳ (diverging)')
top_t = max(TORNADO, key=lambda r: abs(r[2] - r[1]))
d.chart('Chương 3 · Độ nhạy', f'{top_t[0]} là biến số tác động lớn nhất: chênh {n(abs(top_t[2] - top_t[1]), 2)} '
                              f'điểm % lãi suất 12 tháng vào 3/2027',
        {'type': 'tornado', 'base': TORNADO_BASE, 'dec': 2, 'unit': '%',
         'labels': ('Biến ở mức thấp', 'Biến ở mức cao'), 'base_label': 'Cơ sở 3/2027',
         'rows': [{'name': nm, 'low': lo, 'high': hi} for nm, lo, hi in TORNADO]},
        note=(f'Mỗi thanh: lãi suất mô hình tháng 3/2027 lệch bao nhiêu điểm % so với cơ sở ({n(TORNADO_BASE, 2)}%) khi '
              f'một biến ở mức thấp hoặc cao.',
              'Cán cân thương mại tác động ngược chiều: thặng dư cao làm lãi suất mô hình thấp hơn.'),
        source='Nguồn: Vietnam Dashboard — mô hình cấu trúc v2, phân tích độ nhạy 21 biến (8 biến lớn nhất); '
               'minh hoạ, không phải dự báo.', method='Lốc xoáy (tornado)')

# ═══ 4. composition ═══
d.section(4, 'Cơ cấu và đóng góp', 'Phần của một tổng: cột chồng, thanh chồng, thác nước, vành khuyên, ô vuông, '
                                   'bản đồ cây, Marimekko và phễu.',
          ['Cột chồng và cột 100%', 'Thanh chồng và thanh 100%', 'Thác nước', 'Vành khuyên, ô vuông',
           'Bản đồ cây, Marimekko, phễu'])
other = [t - a - b - c for t, a, b, c in zip(EXP['total'], EXP['dev'], EXP['rec'], EXP['int'])]
rec_sh = [r / t * 100 for r, t in zip(EXP['rec'][:7], EXP['total'][:7])]
d.chart('Chương 4 · Chi ngân sách', f'Chi thường xuyên chiếm {n(min(rec_sh), 0)}–{n(max(rec_sh), 0)}% tổng chi mỗi năm '
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
               'dự toán 2026.', method='Cột chồng (stacked column)')
inv_tot = [a + b + c for a, b, c in zip(INV['state'], INV['nonstate'], INV['fdi'])]
st_sh = [a / t * 100 for a, t in zip(INV['state'], inv_tot)]
d.chart('Chương 4 · Đầu tư theo nguồn', f'Khu vực Nhà nước chiếm {n(st_sh[-1], 1)}% vốn đầu tư năm 2025, thấp nhất là '
                                        f'{n(min(st_sh), 1)}% năm {2015 + st_sh.index(min(st_sh))}',
        {'type': 'stacked100', 'categories': yrs(2015, 2025), 'dec': 0,
         'series': [{'name': 'Nhà nước', 'values': INV['state'], 'color': ORANGE},
                    {'name': 'Ngoài nhà nước', 'values': INV['nonstate'], 'color': BLUE},
                    {'name': 'FDI', 'values': INV['fdi'], 'color': AQUA}]},
        note=(f'Tỷ trọng vốn Nhà nước từ {n(st_sh[0], 1)}% (2015) xuống {n(min(st_sh), 1)}% rồi lên {n(st_sh[-1], 1)}% '
              f'(2025).', 'Mỗi cột là 100% vốn đầu tư thực hiện năm đó, giá hiện hành; màu theo cùng nguồn như trang vùng chồng.'),
        source='Nguồn: NSO V04.01 — vốn đầu tư thực hiện toàn xã hội theo nguồn, giá hiện hành, 2015–2025.',
        method='Cột chồng 100%')
SH8 = ['TP. Hồ Chí Minh', 'Hà Nội', 'Hải Phòng', 'Đồng Nai', 'Bắc Ninh', 'Phú Thọ', 'Quảng Ninh', 'Ninh Bình']
abs8 = {k: [GRDP[k][0] / 1000 * s_ / 100 for s_ in STRUCT_ALL[k]] for k in SH8}
d.chart('Chương 4 · Quy mô kinh tế', f'TP.HCM tạo {n(GRDP["TP. Hồ Chí Minh"][0] / 1000, 0)} nghìn tỷ GRDP năm 2025, gấp '
                                     f'{n(GRDP["TP. Hồ Chí Minh"][0] / GRDP["Hà Nội"][0], 1)} lần Hà Nội',
        {'type': 'stacked_h', 'categories': SH8, 'dec': 0, 'unit': '',
         'series': [{'name': nm, 'values': [abs8[k][j] for k in SH8], 'color': c}
                    for j, (nm, c) in enumerate((('Nông, lâm, thủy sản', GREEN), ('Công nghiệp, xây dựng', ORANGE),
                                                 ('Dịch vụ', BLUE), ('Thuế sản phẩm', YELLOW)))]},
        note=(f'Dịch vụ của TP.HCM ≈ {n(abs8["TP. Hồ Chí Minh"][2], 0)} nghìn tỷ, công nghiệp của Bắc Ninh ≈ '
              f'{n(abs8["Bắc Ninh"][1], 0)} nghìn tỷ.',
              'Các phần ≈ tỷ trọng ngành × GRDP (tính toán, giá hiện hành); tổng ở cuối thanh là GRDP công bố.'),
        source=SRC_PROV + ' Cơ cấu ngành 2025, giá hiện hành.', method='Thanh ngang chồng')
d.chart('Chương 4 · Cơ cấu kinh tế', f'Dịch vụ chiếm {n(STRUCT25["Hà Nội"][2], 0)}% GRDP Hà Nội; công nghiệp – '
                                     f'xây dựng chiếm {n(STRUCT25["Bắc Ninh"][1], 0)}% ở Bắc Ninh',
        {'type': 'stacked100_h', 'categories': list(STRUCT25), 'dec': 0,
         'series': [{'name': 'Dịch vụ', 'values': [v[2] for v in STRUCT25.values()], 'color': BLUE},
                    {'name': 'Công nghiệp, xây dựng', 'values': [v[1] for v in STRUCT25.values()], 'color': ORANGE},
                    {'name': 'Nông, lâm, thủy sản', 'values': [v[0] for v in STRUCT25.values()], 'color': GREEN},
                    {'name': 'Thuế sản phẩm', 'values': [v[3] for v in STRUCT25.values()], 'color': YELLOW}]},
        note=(f'Đắk Lắk là nơi nông nghiệp chiếm tỷ trọng cao nhất trong tám địa phương ({n(STRUCT25["Đắk Lắk"][0], 0)}%).',
              'Mỗi thanh là 100% GRDP năm 2025 (giá hiện hành); các phần cộng lại đúng một tổng.'),
        source='Nguồn: NSO và cục thống kê địa phương — cơ cấu GRDP 2025, giá hiện hành.',
        method='Thanh ngang chồng 100%')
parts = [('Chi thường xuyên', 'rec'), ('Đầu tư phát triển', 'dev'), ('Trả lãi', 'int')]
dT = (EXP['total'][-1] - EXP['total'][-2]) / 1000
dDev = (EXP['dev'][-1] - EXP['dev'][-2]) / 1000
d.chart('Chương 4 · Cầu nối chi ngân sách', f'Chi ngân sách dự toán 2026 tăng {n(dT, 0)} nghìn tỷ so với ước 2025; '
                                            f'đầu tư phát triển góp {n(dDev / dT * 100, 0)}%',
        {'type': 'waterfall', 'dec': 0, 'unit': '', 'y_title': 'Nghìn tỷ đồng',
         'steps': [{'name': 'Chi 2025 (ước tính)', 'value': EXP['total'][-2] / 1000, 'total': True}] +
                  [{'name': nm, 'value': (EXP[k][-1] - EXP[k][-2]) / 1000} for nm, k in parts] +
                  [{'name': 'Khác', 'value': (other[-1] - other[-2]) / 1000},
                   {'name': 'Chi 2026 (dự toán)', 'total': True, 'basis': 'plan'}]},
        note=(f'Đầu tư phát triển tăng {n(dDev, 0)} nghìn tỷ, chi thường xuyên tăng '
              f'{n((EXP["rec"][-1] - EXP["rec"][-2]) / 1000, 0)} nghìn tỷ trong dự toán 2026.',
              'Cột cuối viền đứt: dự toán là kế hoạch, thực chi thường thấp hơn. Sửa cột "Giá trị nhập" trong Edit Data, '
              'các cột còn lại tự tính lại.'),
        source='Nguồn: Bộ Tài chính — ước thực hiện NSNN 2025 (1/2026); Nghị quyết Quốc hội về dự toán NSNN 2026. '
               '"Khác" = tổng chi trừ ba nhóm (≈, tính toán).', method='Thác nước (waterfall)')
rev24 = sum(REV24.values())
d.chart('Chương 4 · Thu ngân sách', f'Thu nội địa chiếm {n(REV24["Thu nội địa"] / rev24 * 100, 1)}% thu ngân sách 2024',
        {'type': 'donut', 'dec': 0, 'unit': ' nghìn tỷ', 'value_dec': 1,
         'parts': [{'name': k, 'value': v / 1000, 'color': c} for (k, v), c in zip(REV24.items(), (BLUE, YELLOW, ORANGE, AQUA))],
         'center': (n(rev24 / 1000, 0), 'nghìn tỷ đồng, 2024')},
        note=(f'Dầu thô còn {n(REV24["Dầu thô"] / rev24 * 100, 1)}%, viện trợ {n(REV24["Viện trợ"] / rev24 * 100, 2)}% — '
              f'quá nhỏ để thấy trên vòng.',
              'Vành khuyên chỉ dùng khi các phần cộng thành một tổng và có ít phần; muốn so các phần với nhau, thanh '
              'ngang dễ đọc hơn.'),
        source='Nguồn: Bộ Tài chính — quyết toán NSNN 2024 (nghìn tỷ đồng).', method='Vành khuyên (donut)')
rev_tot25 = sum(REV25.values())
exp25 = {'Chi thường xuyên': EXP['rec'][-2], 'Đầu tư phát triển': EXP['dev'][-2], 'Trả lãi': EXP['int'][-2],
         'Khác': other[-2]}
d.two_charts('Chương 4 · Cơ cấu thu chi', f'Thu nội địa chiếm {n(REV25["Thu nội địa"] / rev_tot25 * 100, 0)}% thu, '
                                          f'chi thường xuyên {n(exp25["Chi thường xuyên"] / EXP["total"][-2] * 100, 0)}% '
                                          f'chi ngân sách 2025',
             {'type': 'waffle', 'parts': [{'name': k, 'value': v / rev_tot25 * 100, 'color': c}
                                          for (k, v), c in zip(REV25.items(), (BLUE, ORANGE, YELLOW))]},
             {'type': 'waffle', 'dec': 1, 'parts': [{'name': k, 'value': v / EXP['total'][-2] * 100, 'color': c,
                                                     'muted': k == 'Khác'}
                                                    for (k, v), c in zip(exp25.items(), (BLUE, ORANGE, AQUA, None))]},
             titles=('Thu ngân sách 2025 (ước tính) — mỗi ô 1%', 'Chi ngân sách 2025 (ước tính) — mỗi ô 1%'),
             note=(f'Thu ngân sách 2025 ước {n(rev_tot25 / 1000, 1)} nghìn tỷ từ ba nguồn chính; chi ước '
                   f'{n(EXP["total"][-2] / 1000, 1)} nghìn tỷ.',
                   '"Khác" nhỏ hơn 0,5% nên không đủ một ô; ô làm tròn theo phần dư lớn nhất để tổng đúng 100.'),
             source='Nguồn: Bộ Tài chính — ước thực hiện NSNN 2025 (công bố 1/2026).', method='Ô vuông (waffle)')
gtot = sum(v[0] for v in GRDP.values())
top2 = (GRDP['TP. Hồ Chí Minh'][0] + GRDP['Hà Nội'][0]) / gtot * 100
d.chart('Chương 4 · Bản đồ kinh tế', f'TP.HCM và Hà Nội tạo {n(top2, 0)}% GRDP của 34 tỉnh, thành năm 2025',
        {'type': 'treemap', 'dec': 0, 'unit': ' tỷ đồng', 'share_dec': 1,
         'groups': [{'name': 'Miền Bắc', 'color': BLUE}, {'name': 'Miền Trung, Tây Nguyên', 'color': ORANGE},
                    {'name': 'Miền Nam', 'color': AQUA}],
         'items': [{'name': k, 'short': PV[k][1], 'value': v[0], 'group': REGION[k]} for k, v in GRDP.items()]},
        note=(f'TP.HCM {n(GRDP["TP. Hồ Chí Minh"][0] / gtot * 100, 1)}%, Hà Nội {n(GRDP["Hà Nội"][0] / gtot * 100, 1)}%; '
              f'20 địa phương nhỏ nhất cộng lại {n(sum(sorted(v[0] for v in GRDP.values())[:20]) / gtot * 100, 1)}%.',
              'Diện tích ô tỷ lệ với GRDP theo giá hiện hành; tổng 34 địa phương khác GDP cả nước một chút.'),
        source=SRC_PROV + ' Số liệu từng ô trong ghi chú trang.', method='Bản đồ cây (treemap)')
MK = list(STRUCT_ALL)
mk_tot = sum(GRDP[k][0] for k in MK)
d.chart('Chương 4 · Quy mô và cơ cấu', f'Mười địa phương lớn nhất: TP.HCM chiếm {n(GRDP["TP. Hồ Chí Minh"][0] / mk_tot * 100, 0)}% '
                                       f'GRDP nhóm, dịch vụ {n(STRUCT_ALL["TP. Hồ Chí Minh"][2], 0)}%',
        {'type': 'marimekko',
         'series': [{'name': 'Dịch vụ', 'color': BLUE}, {'name': 'Công nghiệp, xây dựng', 'color': ORANGE},
                    {'name': 'Nông, lâm, thủy sản', 'color': GREEN}, {'name': 'Thuế sản phẩm', 'color': YELLOW}],
         'columns': [{'name': k, 'short': PV[k][1], 'width': GRDP[k][0],
                      'values': [STRUCT_ALL[k][2], STRUCT_ALL[k][1], STRUCT_ALL[k][0], STRUCT_ALL[k][3]]} for k in MK]},
        note=('Độ rộng cột = tỷ trọng GRDP trong nhóm mười; chiều cao các khúc = cơ cấu ngành của từng địa phương.',
              'Cột hẹp khó đọc: dùng Marimekko khi cần thấy cùng lúc quy mô và cơ cấu; sửa cơ cấu ở bảng nhập trong '
              'Edit Data, độ rộng cố định khi dựng.'),
        source=SRC_PROV + ' Mười địa phương có GRDP lớn nhất và đủ số liệu cơ cấu (Tây Ninh thiếu cơ cấu).',
        method='Marimekko')
pi_ttg = PI23['total_pct'] / PI23['ttg_pct'] * 100
d.chart('Chương 4 · Giải ngân đầu tư công', f'Năm 2023 giải ngân {n(PI23["total_pct"], 1)}% tổng kế hoạch, '
                                            f'{n(PI23["ttg_pct"], 1)}% kế hoạch Thủ tướng giao',
        {'type': 'funnel', 'dec': 1, 'unit': '',
         'stages': [{'name': 'Tổng kế hoạch (gồm vốn kéo dài)', 'value': 100.0},
                    {'name': 'Kế hoạch Thủ tướng giao (≈)', 'value': pi_ttg},
                    {'name': 'Đã giải ngân', 'value': PI23['total_pct']}]},
        note=(f'Kế hoạch Thủ tướng giao ≈ {n(pi_ttg, 1)}% tổng kế hoạch (= {n(PI23["total_pct"], 2)} ÷ '
              f'{n(PI23["ttg_pct"], 2)}); phần đã giải ngân là {n(PI23["total_pct"], 1)}.',
              'Chỉ số, tổng kế hoạch = 100. Năm 2026 giải ngân 9 tháng 62,9% kế hoạch — chưa có số cả năm.'),
        source='Nguồn: Bộ Kế hoạch và Đầu tư — báo cáo giải ngân kế hoạch 2023 (31/1/2024), data/policy.json; '
               'tầng giữa tính toán (≈).', method='Phễu (funnel)')

# ═══ 5. distribution and relationship ═══
d.section(5, 'Phân phối và quan hệ', 'Tần suất, hộp, phân tán, bong bóng, tháp dân số và mạng nhện.',
          ['Tần suất tăng trưởng 34 địa phương', 'Hộp: Bắc và Trung–Nam', 'Phân tán và bong bóng', 'Tháp dân số',
           'Mạng nhện cơ cấu'])
gr25 = [p[2] for p in PROV]
EDG = [5, 6, 7, 8, 9, 10, 11, 12]
cnt = [sum(1 for v in gr25 if EDG[i] <= v < EDG[i + 1]) for i in range(len(EDG) - 1)]
mode = max(range(len(cnt)), key=lambda i: cnt[i])
d.chart('Chương 5 · Phân phối', f'{cnt[mode]}/{len(gr25)} địa phương tăng trưởng {n(EDG[mode], 0)}–{n(EDG[mode + 1], 0)}% '
                                f'năm 2025, nhóm đông nhất',
        {'type': 'histogram', 'data': gr25, 'edges': EDG, 'highlight': [mode], 'y_title': 'Số địa phương',
         'x_title': 'Tăng trưởng GRDP 2025, %', 'bin_labels': [f'{a}–{b}%' for a, b in zip(EDG, EDG[1:])]},
        note=(f'{sum(cnt[3:])} địa phương tăng từ 8% trở lên; chỉ {cnt[0]} địa phương dưới 6%.',
              'Mỗi khoảng gồm cận dưới, không gồm cận trên; số đếm là công thức COUNTIFS trên cột số liệu gốc trong '
              'Edit Data.'), source=SRC_PROV, method='Tần suất (histogram)')
grp = {}
for p in PROV:
    key = 'Miền Bắc' if p[7] == 0 else 'Trung, Nam'
    grp.setdefault(key + ' · 2025', []).append(p[2])
    if p[4] is not None:
        grp.setdefault(key + ' · 9T/2026', []).append(p[4])
order = ['Miền Bắc · 2025', 'Miền Bắc · 9T/2026', 'Trung, Nam · 2025', 'Trung, Nam · 9T/2026']
med = {k: _quart(v, 0.5) for k, v in grp.items()}
d.chart('Chương 5 · Phân tán theo vùng', f'Trung vị tăng trưởng miền Bắc lên {n(med["Miền Bắc · 9T/2026"], 2)}% trong 9 '
                                         f'tháng 2026, từ {n(med["Miền Bắc · 2025"], 2)}% năm 2025',
        {'type': 'box', 'dec': 2, 'unit': '%', 'y_title': 'Tăng trưởng GRDP, %',
         'groups': [{'name': k, 'values': grp[k]} for k in order]},
        note=(f'Ở Trung, Nam trung vị từ {n(med["Trung, Nam · 2025"], 2)}% lên {n(med["Trung, Nam · 9T/2026"], 2)}%; '
              f'hộp 9 tháng cao hơn ở cả hai khối.',
              f'9 tháng 2026 có {len(grp["Miền Bắc · 9T/2026"]) + len(grp["Trung, Nam · 9T/2026"])}/34 địa phương '
              'công bố; 9 tháng so với cùng kỳ, không phải số cả năm.'),
        source=SRC_PROV + ' 9T/2026: công bố 9–10/2026.', method='Hộp (box plot)')
xs_ = [p[3] / 1000 for p in PROV]
ys_ = [p[2] for p in PROV]
mx, my = sum(xs_) / len(xs_), sum(ys_) / len(ys_)
r = sum((a - mx) * (b - my) for a, b in zip(xs_, ys_)) / (sum((a - mx) ** 2 for a in xs_) *
                                                           sum((b - my) ** 2 for b in ys_)) ** 0.5
lab = {'Hà Nội', 'TP. Hồ Chí Minh', 'Quảng Ninh', 'Hải Phòng', 'Bắc Ninh', 'Đồng Nai', 'Cao Bằng', 'Vĩnh Long',
       'Ninh Bình', 'Phú Thọ', 'Tuyên Quang', 'Thái Nguyên'}
d.chart('Chương 5 · Thu nhập và tăng trưởng', f'Địa phương có GRDP đầu người cao hơn có xu hướng tăng nhanh hơn: '
                                              f'r = {n(r, 2)} trên {len(PROV)} địa phương',
        {'type': 'scatter', 'x_title': 'GRDP bình quân đầu người 2025, nghìn USD', 'y_title': 'Tăng trưởng GRDP 2025, %',
         'x_dec': 0, 'y_dec': 1, 'trend': True, 'hi_label': 'Địa phương được ghi tên', 'other_label': 'Địa phương khác',
         'points': [{'name': p[0], 'x': p[3] / 1000, 'y': p[2], 'label': p[0] in lab, 'hi': p[0] in lab,
                     'text': p[1]} for p in PROV]},
        note=(f'Quảng Ninh vừa giàu nhất ({n(PV["Quảng Ninh"][3] / 1000, 1)} nghìn USD/người) vừa tăng nhanh nhất '
              f'({n(PV["Quảng Ninh"][2], 2)}%); TP.HCM giàu thứ hai nhưng chỉ tăng {n(PV["TP. Hồ Chí Minh"][2], 2)}%.',
              f'r = {n(r, 2)}: tương quan dương, mức vừa, không cho biết quan hệ nhân quả; đường xu hướng là đường '
              f'xu hướng tuyến tính của PowerPoint.'),
        source=SRC_PROV, method='Phân tán (scatter)')
popb = max(GRDP, key=lambda k: GRDP[k][1])
d.chart('Chương 5 · Quy mô dân số', f'TP.HCM đông dân nhất ({n(GRDP[popb][1] / 1000, 1)} triệu người) nhưng tăng '
                                    f'{n(PV[popb][2], 2)}%, chậm hơn {sum(1 for p in PROV if p[2] > PV[popb][2])} địa phương',
        {'type': 'bubble', 'x_title': 'GRDP bình quân đầu người 2025, nghìn USD', 'y_title': 'Tăng trưởng GRDP 2025, %',
         'x_dec': 0, 'y_dec': 1, 'size_title': 'Dân số (nghìn người)', 'bubble_scale': 55,
         'size_note': 'Diện tích bong bóng tỷ lệ với dân số trung bình 2025.',
         'groups': [{'name': 'Miền Bắc', 'color': BLUE}, {'name': 'Miền Trung, Tây Nguyên', 'color': ORANGE},
                    {'name': 'Miền Nam', 'color': AQUA}],
         'points': [{'name': p[0], 'x': p[3] / 1000, 'y': p[2], 'size': GRDP[p[0]][1], 'group': REGION[p[0]],
                     'label': p[0] in ('TP. Hồ Chí Minh', 'Hà Nội', 'Quảng Ninh', 'Hải Phòng', 'Đồng Nai', 'Bắc Ninh'),
                     'text': p[1]} for p in PROV]},
        note=(f'Hà Nội ({n(GRDP["Hà Nội"][1] / 1000, 1)} triệu người) tăng {n(PV["Hà Nội"][2], 2)}%; Quảng Ninh nhỏ '
              f'({n(GRDP["Quảng Ninh"][1] / 1000, 1)} triệu) nhưng tăng {n(PV["Quảng Ninh"][2], 2)}%.',
              'Ba biến trên một hình: chỉ dùng khi biến thứ ba (dân số) giúp đọc hai biến kia; so diện tích, không so '
              'đường kính.'), source=SRC_PROV + ' Dân số trung bình 2025.', method='Bong bóng (bubble)')
d.chart('Chương 5 · Dân số', f'Người từ 65 tuổi sẽ chiếm {n(OLD65["2050"], 1)}% dân số năm 2050, gấp '
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
        source='Nguồn: Liên Hợp Quốc — World Population Prospects 2024, Việt Nam; % tổng dân số, 31/12 mỗi năm.',
        method='Tháp dân số')
RD = ['Hà Nội', 'Bắc Ninh', 'Đắk Lắk']
d.chart('Chương 5 · Ba kiểu kinh tế', f'Ba hình dạng: dịch vụ {n(STRUCT25["Hà Nội"][2], 0)}% ở Hà Nội, công nghiệp '
                                      f'{n(STRUCT25["Bắc Ninh"][1], 0)}% ở Bắc Ninh, nông nghiệp {n(STRUCT25["Đắk Lắk"][0], 0)}% ở Đắk Lắk',
        {'type': 'radar', 'axes': ['Nông, lâm, thủy sản', 'Công nghiệp, xây dựng', 'Dịch vụ', 'Thuế sản phẩm'], 'unit': '%',
         'series': [{'name': k, 'values': list(STRUCT25[k]), 'color': c} for k, c in zip(RD, (BLUE, ORANGE, GREEN))]},
        note=('Mỗi trục là tỷ trọng một khối ngành trong GRDP 2025; hình càng lệch về trục nào, ngành đó càng lớn.',
              'Mạng nhện khó đọc giá trị chính xác và thứ tự trục làm đổi hình; dùng để so hình dạng, còn so số thì '
              'dùng thanh 100%.'),
        source='Nguồn: NSO và cục thống kê địa phương — cơ cấu GRDP 2025, giá hiện hành.', method='Mạng nhện (radar)')

# ═══ 6. progress, grids and maps ═══
d.section(6, 'Tiến độ, lưới và bản đồ', 'So với mục tiêu, nhiều ô nhỏ, bảng có biểu đồ, bản đồ nhiệt và bản đồ ô.',
          ['Bullet và đồng hồ', 'Sáu dòng tiền: nhiều ô nhỏ', 'Bảng chỉ số ngân hàng', 'Bản đồ nhiệt, lịch nhiệt',
           'Bản đồ ô 34 địa phương'])
d.chart('Chương 6 · Tiến độ', f'Chín tháng: GDP đạt {n(G9M, 2)}% so với mục tiêu từ {n(TARGET_G, 0)}%; thu ngân sách '
                              f'đạt {n(BUD_REV_9M_PCT, 0)}% dự toán',
        {'type': 'bullet', 'rows': [
            {'name': 'Tăng trưởng GDP', 'value': G9M, 'target': TARGET_G, 'max': 12, 'ranges': [7.1, 7.8], 'unit': '%',
             'dec': 2, 'value_text': f'9 tháng; mục tiêu từ {n(TARGET_G, 0)}%; vùng đậm = dự báo IMF–ADB'},
            {'name': 'Lạm phát bình quân', 'value': CPI_AVG_9M, 'target': CPI_TARGET, 'max': 6, 'unit': '%', 'dec': 2,
             'color': REDS, 'value_text': f'trần {n(CPI_TARGET, 1)}% — vượt {n(CPI_AVG_9M - CPI_TARGET, 2)} điểm %'},
            {'name': 'Tín dụng', 'value': CR_YTD_SEP26, 'target': CREDIT_TARGET26, 'max': 20, 'unit': '%', 'dec': 2,
             'value_text': f'từ đầu năm; = {n(CR_YTD_SEP26 / CREDIT_TARGET26 * 100, 0)}% định hướng {n(CREDIT_TARGET26, 0)}%'},
            {'name': 'Thu ngân sách', 'value': BUDGET_REV_9M, 'target': BUDGET_REV_PLAN26, 'max': 3000, 'unit': '',
             'dec': 0, 'value_text': f'nghìn tỷ; = {n(BUD_REV_9M_PCT, 1)}% dự toán {n(BUDGET_REV_PLAN26, 0)}'},
            {'name': 'FDI giải ngân', 'value': FDI9M, 'target': FDI_TARGET_LOW, 'max': 40, 'unit': '', 'dec': 2,
             'value_text': f'tỷ USD; = {n(FDI9M / FDI_TARGET_LOW * 100, 0)}% mức thấp của mục tiêu 30–40'}]},
        note=(f'Sau chín tháng, thu ngân sách đã đạt {n(BUD_REV_9M_PCT, 1)}% dự toán; lạm phát bình quân '
              f'{n(CPI_AVG_9M, 2)}%, vượt trần {n(CPI_TARGET, 1)}%.',
              'So sánh số 9 tháng với mục tiêu cả năm chỉ cho biết tiến độ, không phải kết quả cả năm.'),
        source='Nguồn: NSO (03/10/2026); NHNN; Bộ Tài chính; Quốc hội — NQ 244/2025/QH15; Bộ Chính trị — NQ 10-NQ/TW; '
               'IMF WEO 4/2026; ADB ADO 9/2026.', method='Bullet (so với mục tiêu)')
d.chart('Chương 6 · Thu ngân sách', f'Chín tháng 2026 thu ngân sách đạt {n(BUD_REV_9M_PCT, 1)}% dự toán cả năm',
        {'type': 'gauge', 'value': BUD_REV_9M_PCT, 'max': 100, 'dec': 1, 'unit': '%', 'target': 75,
         'caption': f'{n(BUDGET_REV_9M, 1)} / {n(BUDGET_REV_PLAN26, 1)} nghìn tỷ đồng'},
        panel={'label': 'Cách đọc', 'value': n(BUD_REV_9M_PCT - 75, 1, sign=True), 'unit': 'điểm %',
               'sub': 'Vạch đỏ ở 75% = mức "đều tay" sau 9/12 tháng. Thu vượt mức đều tay vì thuế dồn vào đầu năm.'},
        note=(f'Đã thu {n(BUDGET_REV_9M, 1)} nghìn tỷ trên dự toán {n(BUDGET_REV_PLAN26, 1)} nghìn tỷ.',
              'Đồng hồ chỉ cho một con số so với một mốc; muốn so nhiều chỉ tiêu, dùng bullet.'),
        source='Nguồn: Bộ Tài chính — thu NSNN 9 tháng 2026 (NSO 03/10/2026); dự toán 2026 (Nghị quyết Quốc hội).',
        method='Đồng hồ (gauge)')
neg_years = sum(1 for v in BOP['Lỗi và sai sót'] if v < 0)
d.chart('Chương 6 · Sáu dòng tiền', f'Lỗi và sai sót âm cả {neg_years}/11 năm 2015–2025; FDI ròng dương mọi năm',
        {'type': 'multiples', 'cols': 3, 'dec': 1, 'unit': 'tỷ USD',
         'pos_label': 'Tiền vào (dương)', 'neg_label': 'Tiền ra (âm)',
         'panels': [{'title': k, 'categories': yrs(2015, 2025), 'values': v} for k, v in BOP.items()]},
        note=(f'Lỗi và sai sót lớn nhất năm 2022 ({n(min(BOP["Lỗi và sai sót"]), 1)} tỷ USD); '
              f'dự trữ giảm {n(-BOP["Thay đổi dự trữ"][7], 1)} tỷ cùng năm.',
              'Mỗi ô một biểu đồ gốc với thang đo riêng: so sánh hình dạng, không so độ cao giữa các ô.'),
        source='Nguồn: IMF Balance of Payments (BPM6) theo số NHNN, 2015–2025, tỷ USD.',
        method='Nhiều ô nhỏ (small multiples)')
d.chart('Chương 6 · Sức khoẻ ngân hàng', f'Nợ xấu {n(FSI["npl"][-1], 2)}% năm 2025, gấp '
                                         f'{n(FSI["npl"][-1] / FSI["npl"][0], 1)} lần năm 2015; ROE lên {n(FSI["roe"][-1], 1)}%',
        {'type': 'spark_table', 'years': [2015, 2025],
         'headers': ('Chỉ tiêu', 'Diễn biến 2015–2025', '2025', 'So với 2015'),
         'rows': [{'name': 'Nợ xấu / dư nợ', 'sub': 'IMF FSI, %', 'values': FSI['npl'], 'dec': 2, 'unit': '%'},
                  {'name': 'Hệ số an toàn vốn (CAR)', 'sub': 'IMF FSI, %', 'values': FSI['car'], 'dec': 2, 'unit': '%'},
                  {'name': 'Dự phòng / nợ xấu', 'sub': 'IMF FSI, %', 'values': FSI['prov'], 'dec': 1, 'unit': '%'},
                  {'name': 'ROA', 'sub': 'IMF FSI, %', 'values': FSI['roa'], 'dec': 2, 'unit': '%'},
                  {'name': 'ROE', 'sub': 'IMF FSI, %', 'values': FSI['roe'], 'dec': 1, 'unit': '%'},
                  {'name': 'Tăng trưởng tín dụng', 'sub': 'NHNN, % cuối năm', 'values': CR_G, 'dec': 2, 'unit': '%'},
                  {'name': 'Tăng trưởng huy động', 'sub': 'NHNN, % cuối năm', 'values': DEP_G, 'dec': 2, 'unit': '%'}]},
        note=(f'Nợ xấu theo IMF FSI tăng vọt lên {n(FSI["npl"][8], 2)}% năm 2023 rồi giảm còn {n(FSI["npl"][-1], 2)}%; '
              f'ROE âm năm 2023 ({n(FSI["roe"][8], 1)}%).',
              'Số IMF FSI gồm các tổ chức nhận tiền gửi; khác số nợ xấu nội bảng của NHNN. Chấm xanh: năm cuối; chấm '
              'xám: điểm thấp nhất.'),
        source='Nguồn: IMF Financial Soundness Indicators (FSIC, Việt Nam, 2015–2025); NHNN.',
        method='Bảng có biểu đồ nhỏ (sparklines)')
g_last = {k: v[-1] for k, v in CPI_GRP.items()}
top_g = max(g_last, key=g_last.get)
d.chart('Chương 6 · Lạm phát theo nhóm', f'{top_g} tăng {n(g_last[top_g], 2)}% riêng tháng 9/2026, mạnh nhất trong '
                                         f'{len(CPI_GRP)} nhóm hàng',
        {'type': 'heatmap', 'rows': list(CPI_GRP), 'cols': CPI_M, 'values': list(CPI_GRP.values()), 'dec': 2,
         'scale': 'diverging', 'vmax': 2, 'pos_color': REDS, 'neg_color': BLUE,
         'legend_title': 'So với tháng trước (%); đỏ = tăng, xanh = giảm, màu giữ ở ±2'},
        note=(f'Giao thông biến động mạnh nhất: +{n(CPI_GRP["Giao thông"][5], 2)}% tháng 3/2026 rồi '
              f'{n(CPI_GRP["Giao thông"][8], 2)}% tháng 6/2026.',
              'Bảng PowerPoint gốc: mỗi ô là một ô bảng có màu nền; màu giới hạn ở ±2% để các nhóm khác vẫn đọc được.'),
        source='Nguồn: NSO — Thông cáo báo chí tình hình giá hằng tháng (Biểu 1 – Cả nước), T10/2025–T9/2026.',
        method='Bản đồ nhiệt (heatmap)')
CAL = {2024: [None] * 9 + CPI24[0:3], 2025: CPI24[3:15], 2026: CPI24[15:] + [None] * 3}
hi_m = max(range(len(CPI24)), key=lambda i: CPI24[i])
d.chart('Chương 6 · Lịch lạm phát', f'CPI so cùng kỳ vượt 5% trong {sum(1 for v in CPI24 if v > 5)} tháng của năm 2026, '
                                    f'cao nhất {n(CPI24[hi_m], 2)}%',
        {'type': 'calendar', 'rows': [str(y) for y in CAL], 'cols': [f'T{m}' for m in range(1, 13)],
         'values': list(CAL.values()), 'dec': 2, 'scale': 'sequential', 'vmin': 2, 'vmax': 6, 'color': REDS,
         'legend_title': 'CPI so cùng kỳ năm trước (%)'},
        note=(f'Từ tháng 3/2026 CPI so cùng kỳ không dưới {n(min(CPI24[17:]), 2)}%; cả năm 2025 dao động '
              f'{n(min(CPI24[3:15]), 2)}–{n(max(CPI24[3:15]), 2)}%.',
              'Hàng = năm, cột = tháng; ô trống: ngoài giai đoạn 24 tháng của dashboard, không phải 0.'),
        source='Nguồn: NSO — CPI so với cùng kỳ năm trước, T10/2024–T9/2026 (data/economy.json, CPI_YOY_24).',
        method='Lịch nhiệt (calendar heatmap)')
over8 = sum(1 for p in PROV if p[2] > 8)
d.chart('Chương 6 · Địa phương', f'{over8}/{len(PROV)} tỉnh, thành tăng trưởng GRDP trên 8% năm 2025',
        {'type': 'tilemap', 'dec': 2, 'unit': '%', 'breaks': [7, 8, 9, 10],
         'legend_title': 'Tăng trưởng GRDP 2025 (%)', 'panels': ['Miền Bắc', 'Miền Trung và miền Nam'],
         'tiles': [{'name': p[0], 'short': p[1], 'value': p[2], 'col': p[5], 'row': p[6], 'panel': p[7]} for p in PROV]},
        panel={'label': 'Nhanh nhất', 'value': n(max(p[2] for p in PROV), 2), 'unit': '%',
               'sub': f'{max(PROV, key=lambda p: p[2])[0]}; chậm nhất {min(PROV, key=lambda p: p[2])[0]} '
                      f'({n(min(p[2] for p in PROV), 2)}%).'},
        note=(f'Tăng trưởng GRDP 2025 dao động từ {n(min(p[2] for p in PROV), 2)}% đến {n(max(p[2] for p in PROV), 2)}%.',
              'Ô xếp theo vị trí địa lý gần đúng, không theo diện tích; số liệu từng ô trong ghi chú trang.'),
        source=SRC_PROV, method='Bản đồ ô (tile map)')

# ═══ 7. diagrams and calendar ═══
d.section(7, 'Sơ đồ, dòng chảy và lịch', 'Dòng tiền ngân sách, cơ chế truyền dẫn, dòng thời gian và điều cần theo dõi.',
          ['Từ nguồn thu đến khoản chi', 'Các đòn bẩy của NHNN', 'Dòng thời gian 2026', 'Ba điều cần theo dõi'])
gap24 = EXP['total'][6] - rev24
o24 = other[6]
d.chart('Chương 7 · Dòng tiền ngân sách', f'Chi thường xuyên chiếm {n(EXP["rec"][6] / EXP["total"][6] * 100, 0)}% '
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
              '"Chi vượt thu" = chi trừ thu trên hai dòng này, không phải bội chi chính thức (2,8% GDP). Số liệu từng '
              'dải trong ghi chú trang.'),
        source='Nguồn: Bộ Tài chính — quyết toán NSNN 2024 (nghìn tỷ đồng). Độ dày dải tỷ lệ với giá trị.',
        method='Dòng chảy (Sankey)')
d.chart('Chương 7 · Cơ chế truyền dẫn', 'Lãi suất điều hành đến lạm phát qua ba tầng: thị trường liên ngân hàng, '
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
              'CPI và GDP.', 'Sơ đồ định tính (minh hoạ), không đo độ mạnh của từng kênh.'),
        source='Nguồn: Vietnam Dashboard — Hệ thống tài chính, Chương 7 (các đòn bẩy cung tiền); khung minh hoạ.',
        method='Sơ đồ đòn bẩy / nhân quả')
days = lambda y, m, dd: (date(y, m, dd) - TODAY).days
d.chart('Chương 7 · Dòng thời gian', 'Năm 2026: năm mốc chính sách đã qua và ba mốc trong quý IV',
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
        source='Nguồn: Bộ Chính trị; Chính phủ; NHNN; NSO; IMF — lịch công bố của dashboard, văn bản trong data/policy.json.',
        method='Dòng thời gian')
d.watch('Tiếp theo · Điều cần theo dõi', 'Ba mốc trong quý IV/2026 sẽ cho biết bức tranh thay đổi thế nào',
        [('13/10/2026', 'IMF công bố WEO tháng 10',
          f'Dự báo tháng 4 là {n(IMF_G[0], 1)}% cho 2026; chín tháng thực tế đã tăng {n(G9M, 2)}%. '
          'Mức điều chỉnh cho thấy IMF đánh giá đà tăng quý 3 bền đến đâu.', (str(days(2026, 10, 13)), 'ngày nữa')),
         ('01/12/2026', 'Trần LDR 95% có hiệu lực',
          'Thông tư 50/2026 nâng trần LDR và áp dụng LCR/NSFR. LDR toàn hệ thống là 77,1% (6/2026); '
          'tác động chính ở các ngân hàng sát trần.', (str(days(2026, 12, 1)), 'ngày nữa')),
         ('31/12/2026', 'Hết hạn giảm 2 điểm % thuế GTGT',
          f'Thuế suất 8% về lại 10% nếu không gia hạn. CPI tháng 9/2026 đang ở {n(CPI24[-1], 2)}% so với cùng kỳ.',
          (str(days(2026, 12, 31)), 'ngày nữa'))],
        source='Nguồn: IMF (lịch WEO); NHNN — Thông tư 50/2026; Chính phủ — chính sách giảm thuế GTGT (data/policy.json); '
               'NSO. Số ngày tính đến 7/10/2026.', method='Điều cần theo dõi')

# ═══ 8. appendix ═══
d.section(8, 'Phụ lục', 'Bảng số liệu chính và nguồn, phương pháp.', ['Bảng số liệu', 'Nguồn và phương pháp'])
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
        number_cols=(2, 3, 4, 5), col_widths=[3.0, 1.35, 1.35, 1.35, 1.35, 1.6, 2.133],
        note=('Dấu "—": chưa công bố cho kỳ đó, không phải bằng 0.',
              '2025 là ước tính; tín dụng 9T/2026 là tăng từ đầu năm, không so được với số cuối năm.'),
        source='Nguồn: NSO; NHNN; IMF FSI và BPM6; Bộ Tài chính — số liệu dashboard đến 7/10/2026.',
        method='Bảng số liệu')
d.sources('Phụ lục · Nguồn và phương pháp', 'Nguồn số liệu, kỳ số liệu và cách tính',
          [('NSO — Cục Thống kê', 'GDP, CPI, xuất nhập khẩu, FDI, vốn đầu tư; GRDP địa phương', '2010 – 9T/2026',
            'nso.gov.vn · pxweb.nso.gov.vn'),
           ('Ngân hàng Nhà nước (NHNN)', 'Tín dụng theo ngành, huy động, M2, lãi suất điều hành', '2015 – 10/2026',
            'sbv.gov.vn'),
           ('Bộ Tài chính, Bộ KH&ĐT', 'Quyết toán, ước thực hiện và dự toán NSNN; giải ngân đầu tư công', '2018 – 2026',
            'mof.gov.vn'),
           ('IMF', 'WEO 4/2026; FSI; BoP (BPM6); dự trữ ngoại hối', '2010 – 2030', 'imf.org · data.imf.org'),
           ('Liên Hợp Quốc', 'World Population Prospects 2024, phương án trung bình', '2025, 2050', 'population.un.org'),
           ('ADB, World Bank, AMRO, OECD, SCB', 'Dự báo tăng trưởng và lạm phát', '2026 – 2027', 'data/economy.json'),
           ('Vietnam Dashboard', 'Mô hình lãi suất huy động v2; phân tích hệ thống tài chính', '7/10/2026',
            'data/simulation.json · data/finance.json'),
           ('Quốc hội, Chính phủ, Bộ Chính trị', 'NQ 244/2025/QH15, NQ 25/2026/QH16, NQ 10-NQ/TW, NĐ 245/2026',
            '2025 – 2026', 'data/policy.json')],
          method=['Mọi con số trong tiêu đề và ghi chú được tính từ dữ liệu khi dựng slide.',
                  'Biểu đồ là biểu đồ PowerPoint gốc: bấm Edit Data để sửa số; cột phụ (nền ẩn, dải quạt, tứ phân vị…) '
                  'là công thức Excel.',
                  'Ước tính, dự toán, dự báo: nét đứt, viền đứt hoặc ô rỗng, có ghi chú.',
                  'Chỉ tiêu phái sinh đánh dấu ≈ và nói rõ cách tính.',
                  'Khoảng trống để trống ("—"), không thay bằng 0, không nội suy.',
                  'Mô hình của dashboard là minh hoạ, không phải dự báo chính thức.',
                  'Mô tả số liệu — không phải khuyến nghị đầu tư.'],
          source='Vietnam Dashboard · bộ mẫu vn-data-story · số liệu đến 7/10/2026.', method_name='Nguồn và phương pháp')

out = sys.argv[1] if len(sys.argv) > 1 else os.path.join(os.path.dirname(os.path.abspath(__file__)), 'example.pptx')
d.save(out)
print(out, d.n, 'slides; catalogue entries:', len(d._catalog), '; embedded font files:', d.embedded)
