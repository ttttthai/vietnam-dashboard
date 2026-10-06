import json
from pxparse import rows, num, yr

PX = 'https://pxweb.nso.gov.vn/pxweb/vi/'
U = {l.split(' ')[0]: l.split(' ')[1] for l in open('urls.txt').read().split('\n') if l.strip()}
NA = PX + 'T%c3%a0i%20kho%e1%ba%a3n%20qu%e1%bb%91c%20gia/T%c3%a0i%20kho%e1%ba%a3n%20qu%e1%bb%91c%20gia/'
U_V0308 = NA + 'V03.08.px/'
U_V0311 = NA + 'V03.11.px/'
U_V0313 = NA + 'V03.13-14.px/'
U_V0316 = NA + 'V03.16.px/'
U_V0320 = NA + 'V03.20.px/'
U_9M26 = 'https://www.nso.gov.vn/du-lieu-va-so-lieu-thong-ke/2026/10/thong-cao-bao-chi-tinh-hinh-kinh-te-xa-hoi-quy-iii-va-9-thang-nam-2026/'
WB = 'https://api.worldbank.org/v2/country/VNM/indicator/{}?format=json'
IDS = 'https://api.worldbank.org/v2/sources/6/country/VNM/series/{}/counterpart-area/WLD/time/all?format=json'
IMF = 'https://api.imf.org/external/sdmx/2.1/data/IMF.STA,BOP/VNM?startPeriod=2014'

YEARS = list(range(2015, 2026))


def table_rowwise(f):
    """header row of years, then label rows (first block only = values)."""
    R = rows(f)
    hdr = R[7]
    cols = [yr(h) for h in hdr]
    out = {}
    for r in R[8:]:
        if len(r) < 2:
            if out:  # second block (structure %) begins after a single-cell row
                if r[0].startswith(('Cơ cấu', 'Nhập khẩu')):
                    pass
            continue
        out.setdefault(r[0].strip(), dict(zip(cols, [num(x) for x in r[1:]])))
    return out


def blocks(f):
    """split into blocks by single-cell rows; returns {blockname:{label:{year:val}}}"""
    R = rows(f)
    hdr = R[7]
    cols = [yr(h) for h in hdr]
    res = {}
    cur = None
    for r in R[8:]:
        if len(r) == 1:
            if r[0].strip() and not r[0].startswith(('V0', 'Chú thích', 'Triệu', 'Nghìn')):
                cur = r[0].strip()
            continue
        if cur is None:
            cur = '_'
        res.setdefault(cur, {})[r[0].strip()] = dict(zip(cols, [num(x) for x in r[1:]]))
    return res


def colwise(f):
    """year in first column, columns = headers"""
    R = rows(f)
    hdr = R[7]
    res = {}
    cur = '_'
    for r in R[8:]:
        if len(r) == 1:
            if r[0].strip():
                cur = r[0].strip()
            continue
        y = yr(r[0])
        if y is None:
            continue
        res.setdefault(cur, {})[y] = dict(zip([h.strip() for h in hdr], [num(x) for x in r[1:]]))
    return res


def ser(d, years=YEARS, scale=1.0, nd=2):
    return [None if d.get(y) is None else round(d[y] * scale, nd) for y in years]


def wb(code):
    d = json.load(open(f'wb/{code}.json'))
    return {int(r['date']): r['value'] for r in d[1] if r['value'] is not None}


def ids(code):
    d = json.load(open(f'ids_{code}.json'))
    return {int(r['variable'][1]['value']): r['value'] for r in d['source']['data'] if r['value'] is not None}


# ---------------- PART A: GDP by expenditure ----------------
b = blocks('V03.08.html')
v = b['Giá trị (Tỷ đồng)']
GDP = v['TỔNG SỐ']; I = v['Tích luỹ tài sản']; GFCF = v['Tổng tài sản cố định']; INV = v['Thay đổi tồn kho']
FC = v['Tiêu dùng cuối cùng(*)']; G = v['Nhà nước']; C = v['Hộ dân cư']; NX = v['Chênh lệch xuất khẩu hàng hoá và dịch vụ']; DISC = v['Sai số']
X = {k: v / 1e9 for k, v in wb('NE.EXP.GNFS.CN').items()}
M = {k: v / 1e9 for k, v in wb('NE.IMP.GNFS.CN').items()}
wb_disc = {k: v / 1e9 for k, v in wb('NY.GDP.DISC.CN').items()}
chk = []
for y in YEARS:
    nx_wb = X[y] - M[y]
    chk.append(round(nx_wb - NX[y], 1))
gdp_exp = {
    'years': YEARS,
    'C': ser(C), 'G': ser(G), 'final_consumption': ser(FC), 'I': ser(I), 'GFCF': ser(GFCF), 'inventories': ser(INV),
    'X': ser(X), 'M': ser(M), 'net_exports': ser(NX), 'discrepancy': ser(DISC), 'GDP': ser(GDP),
    'pct_gdp': {k: [round(s[y] / GDP[y] * 100, 2) for y in YEARS] for k, s in
                {'C': C, 'G': G, 'I': I, 'GFCF': GFCF, 'inventories': INV, 'X': X, 'M': M, 'net_exports': NX, 'discrepancy': DISC}.items()},
    'basis': ['official'] * 9 + ['preliminary (Sơ bộ)', 'estimate (Ước tính)'],
    'unit': 'bn VND (tỷ đồng), current prices',
    'growth_real_pct': {
        '9M_2026_yoy': {'final_consumption': 8.51, 'gross_capital_formation': 17.88, 'exports_gs': 21.29, 'imports_gs': 27.19, 'GDP': 9.01},
        'Q3_2026_yoy': {'final_consumption': 8.96, 'gross_capital_formation': 21.39, 'exports_gs': 23.27, 'imports_gs': 28.75},
        'source': U_9M26},
    'sources': {
        'C,G,final_consumption,I,GFCF,inventories,net_exports,discrepancy,GDP': U_V0308 + ' (NSO PxWeb V03.08 Sử dụng GDP theo giá hiện hành, updated 15/08/2026)',
        'X': WB.format('NE.EXP.GNFS.CN') + ' (World Bank WDI, reproduces NSO national accounts; LCU/1e9)',
        'M': WB.format('NE.IMP.GNFS.CN') + ' (World Bank WDI; LCU/1e9)',
        'pct_gdp': 'derived: component / GDP x 100 (NSO V03.08 also publishes the same structure %)',
        'growth_real_pct': U_9M26,
    },
    'check_X_minus_M_vs_NSO_net_exports_bn': dict(zip(YEARS, chk)),
}

# ---------------- investment by owner ----------------
c = colwise('V04.01.html')
cur = c['Giá thực tế (Tỷ đồng)']
c8 = colwise('V04.08.html')['Giá hiện hành (Tỷ đồng)']
invest = {
    'years': YEARS + ['9M2026'],
    'total': [cur[y]['Tổng số'] for y in YEARS] + [3109600.0],
    'state': [cur[y]['Kinh tế  Nhà nước'] for y in YEARS] + [931000.0],
    'nonstate': [cur[y]['Kinh tế ngoài nhà nước'] for y in YEARS] + [1649200.0],
    'fdi': [cur[y]['Khu vực có vốn  đầu tư nước ngoài'] for y in YEARS] + [529400.0],
    'state_of_which_public_investment': [c8[y]['Vốn đầu tư công'] for y in YEARS] + [None],
    'state_of_which_soe_and_other': [c8[y]['Vốn của các doanh nghiệp Nhà nước và nguồn vốn khác'] for y in YEARS] + [None],
    'basis': ['official'] * 10 + ['preliminary (Sơ bộ)', 'estimate 9M (ước tính, rounded to 0.1 trillion)'],
    'growth_9M2026_yoy_pct': {'total': 15.1, 'state': 17.3, 'nonstate': 14.0, 'fdi': 14.5},
    'unit': 'bn VND (tỷ đồng), current prices — vốn đầu tư thực hiện toàn xã hội (realised social investment; NOT the same concept as GFCF)',
    'sources': {
        'total,state,nonstate,fdi 2015-2025': U['V04.01.px'] + ' (NSO PxWeb V04.01)',
        'state split public/SOE 2015-2025': U['V04.08.px'] + ' (NSO PxWeb V04.08; note 2020 state total 736,609 here vs 734,735 in V04.01)',
        '9M2026': U_9M26,
    },
}

# ---------------- BOP (IMF BPM6) ----------------
imf = json.load(open('bop_imf_parsed.json'))
A, Q = imf['A'], imf['Q']


def ia(k, years=YEARS):
    s = A.get(k, {})
    return [round(float(s[str(y)]) / 1e9, 3) if str(y) in s else None for y in years]


QS = ['2025-Q1', '2025-Q2', '2025-Q3', '2025-Q4', '2026-Q1']


def iq(k):
    s = Q.get(k, {})
    return [round(float(s[p]) / 1e9, 3) if p in s else None for p in QS]


def diff(a, b):
    return [None if x is None or y is None else round(x - y, 3) for x, y in zip(a, b)]


bopkeys = {
    'goods_x': 'CD_T|G', 'goods_m': 'DB_T|G', 'services_x': 'CD_T|S', 'services_m': 'DB_T|S',
    'primary_income_in': 'CD_T|IN1', 'primary_income_out': 'DB_T|IN1',
    'secondary_income_in': 'CD_T|IN2', 'secondary_income_out': 'DB_T|IN2',
    'current_account': 'NETCD_T|CAB',
    'fdi_in': 'L_NIL_T|D_F', 'fdi_out': 'A_NFA_T|D_F',
    'portfolio_in': 'L_NIL_T|P_F', 'portfolio_out': 'A_NFA_T|P_F',
    'loans_net': 'L_NIL_T|O_F4', 'loans_net_government': 'L_NIL_T|O_F4_S13', 'loans_net_other_sectors': 'L_NIL_T|O_F4_S1Z',
    'loans_net_other_sectors_longterm': 'L_NIL_T|O_F4_S1Z_L', 'loans_net_other_sectors_shortterm': 'L_NIL_T|O_F4_S1Z_S',
    'currency_deposits_assets': 'A_NFA_T|O_F2', 'currency_deposits_liabilities': 'L_NIL_T|O_F2',
    'other_inv_assets': 'A_NFA_T|O_F', 'other_inv_liabilities': 'L_NIL_T|O_F',
    'errors_omissions': 'NETCD_T|EO', 'reserves_change': 'A_T|R_F',
}
S = {k: ia(code) for k, code in bopkeys.items()}
SQ = {k: iq(code) for k, code in bopkeys.items()}
for D in (S, SQ):
    D['portfolio_net'] = diff(D['portfolio_in'], D['portfolio_out'])
    D['other_inv_net'] = diff(D['other_inv_liabilities'], D['other_inv_assets'])
    D['fdi_net'] = diff(D['fdi_in'], D['fdi_out'])

# services by type: NSO V09.15 (trade-in-services statistics, million USD)
sv = blocks('V09.15.html')
sx, sm = sv['Xuất khẩu'], sv['Nhập khẩu']
svc = {}
for lab, key in [('Dịch vụ du lịch', 'travel'), ('Dịch vụ vận tải', 'transport'), ('Dịch vụ bảo hiểm', 'insurance'),
                 ('Dịch vụ tài chính', 'financial'), ('Dịch vụ bưu chính viễn thông', 'telecom_postal'),
                 ('Dịch vụ Chính phủ', 'government'), ('Dịch vụ khác', 'other'), ('Tổng số', 'total')]:
    svc[key + '_x'] = ser(sx[lab], scale=1 / 1000, nd=3)
    svc[key + '_m'] = ser(sm[lab], scale=1 / 1000, nd=3)
for k in ['travel_x', 'travel_m', 'transport_x', 'transport_m']:
    S[k] = svc[k]
S['ip_m'] = [None] * len(YEARS)
S['comp_emp_in'] = [None] * len(YEARS)
S['comp_emp_out'] = [None] * len(YEARS)
S['inv_income_in'] = [None] * len(YEARS)
S['inv_income_out'] = [None] * len(YEARS)
S['remit_in'] = [None] * len(YEARS)   # filled later from SBV statements
S['remit_out'] = [None] * len(YEARS)

# customs basis trade NSO V09.01
cw = colwise('V09.01.html')['Giá trị (Triệu đô la Mỹ)']
customs = {'years': YEARS + ['9M2026'],
           'exports': [round(cw[y]['Xuất khẩu'] / 1000, 3) for y in YEARS] + [434.30],
           'imports': [round(cw[y]['Nhập khẩu'] / 1000, 3) for y in YEARS] + [453.72],
           'balance': [round(cw[y]['Cân đối (*)'] / 1000, 3) for y in YEARS] + [-19.42],
           'unit': 'bn USD', 'source': U['V09.01.px'] + ' (NSO PxWeb V09.01, customs basis); 9M2026: ' + U_9M26}

# NSO/SBV V03.20 cross-check
b20 = blocks('V03.20.html')['Trị giá (Triệu đô la Mỹ)']
Y20 = list(range(2018, 2026))
sbv = {'years': Y20, 'unit': 'bn USD', 'source': U_V0320 + ' (NSO PxWeb V03.20 Cán cân thanh toán quốc tế, compiled by SBV; 2025 = Sơ bộ)',
       'sign_note': 'Financial account here uses SBV presentation: positive = net inflow; reserve assets negative = increase.'}
for lab, key in [('CÁN CÂN VÃNG LAI', 'current_account'), ('Cán cân hàng hóa - Xuất khẩu (FOB)', 'goods_x'), ('Cán cân hàng hóa - Nhập khẩu (FOB)', 'goods_m'),
                 ('Dịch vụ - Xuất khẩu', 'services_x'), ('Dịch vụ - Nhập khẩu', 'services_m'), ('Thu nhập - Thu', 'income_in'), ('Thu nhập - Chi', 'income_out'),
                 ('Chuyển giao vãng lai - Thu', 'transfers_in'), ('Chuyển giao vãng lai - Chi', 'transfers_out'), ('CÁN CÂN TÀI CHÍNH', 'financial_account'),
                 ('Đầu tư trực tiếp của nước ngoài  vào Việt Nam', 'fdi_in'), ('Đầu tư trực tiếp của Việt Nam ra nước ngoài', 'fdi_out'),
                 ('Đầu tư gián tiếp', 'portfolio_net'), ('Đầu tư khác (Tài sản có) - Tiền và tiền gửi', 'currency_deposits_assets'),
                 ('Đầu tư khác (Tài sản nợ) - Vay, trả nợ nước ngoài', 'loans_net'), ('Đầu tư khác (Ròng)', 'other_inv_net'),
                 ('LỖI VÀ SAI SÓT', 'errors_omissions'), ('CÁN CÂN TỔNG THỂ', 'overall_balance'), ('Tài sản dự trữ', 'reserve_assets')]:
    sbv[key] = [None if b20[lab].get(y) is None else round(b20[lab][y] / 1000, 3) for y in Y20]

ext = {
    'years': list(range(2015, 2025)),
    'debt_service_total': ser({k: v / 1e9 for k, v in ids('DT.TDS.DECT.CD').items()}, years=range(2015, 2025), nd=3),
    'principal_longterm': ser({k: v / 1e9 for k, v in ids('DT.AMT.DLXF.CD').items()}, years=range(2015, 2025), nd=3),
    'interest_total': ser({k: v / 1e9 for k, v in ids('DT.INT.DECT.CD').items()}, years=range(2015, 2025), nd=3),
    'principal_public_ppg': ser({k: v / 1e9 for k, v in ids('DT.AMT.DPPG.CD').items()}, years=range(2015, 2025), nd=3),
    'interest_public_ppg': ser({k: v / 1e9 for k, v in ids('DT.INT.DPPG.CD').items()}, years=range(2015, 2025), nd=3),
    'debt_stock': ser({k: v / 1e9 for k, v in wb('DT.DOD.DECT.CD').items()}, years=range(2015, 2025), nd=3),
    'unit': 'bn USD',
    'sources': {k: IDS.format(c) + ' (World Bank International Debt Statistics, updated 2025-12-03)' for k, c in
                [('debt_service_total', 'DT.TDS.DECT.CD'), ('principal_longterm', 'DT.AMT.DLXF.CD'), ('interest_total', 'DT.INT.DECT.CD'),
                 ('principal_public_ppg', 'DT.AMT.DPPG.CD'), ('interest_public_ppg', 'DT.INT.DPPG.CD')]},
    'note': 'IDS 2025+ values are scheduled projections on existing debt and are excluded. TDS = long-term principal + total interest + IMF repurchases/short-term interest.',
}
ext['sources']['debt_stock'] = WB.format('DT.DOD.DECT.CD')

oda = {'years': list(range(2015, 2024)), 'grants_excl_tc_busd': ser({k: v / 1e9 for k, v in wb('BX.GRT.EXTA.CD.WD').items()}, years=range(2015, 2024), nd=3),
       'source': WB.format('BX.GRT.EXTA.CD.WD') + ' (OECD-DAC grants excl. technical cooperation, via WDI)'}

bop = {
    'years': YEARS,
    'series': S,
    'quarterly': {'periods': QS, 'series': SQ, 'source': IMF + ' (IMF BOP dataset, FREQUENCY=Q; latest quarter available 2026-Q1)'},
    'services_by_type_nso': dict(years=YEARS, unit='bn USD', source=U['V09.15.px'] + ' (NSO PxWeb V09.15 Xuất khẩu, nhập khẩu dịch vụ; 2025 = Sơ bộ)',
                                 note='NSO trade-in-services statistics; totals differ from SBV/IMF BOP services (e.g. 2018 exports 18.06 vs BOP 14.79).', **svc),
    'services_9M2026': {'services_x': 26.10, 'services_m': 34.31, 'travel_x': 13.06, 'transport_x': 7.81, 'transport_m': 16.40, 'travel_m': 10.85,
                        'freight_insurance_on_imports_m': 14.39, 'source': U_9M26},
    'customs_trade': customs,
    'sbv_nso_v0320': sbv,
    'external_debt_service': ext,
    'oda_grants': oda,
    'unit': 'bn USD',
    'sign_convention': 'BPM6 (IMF): fdi_in/portfolio_in/loans_net/other_inv_liabilities = net incurrence of liabilities (+ = inflow); fdi_out/portfolio_out/currency_deposits_assets/other_inv_assets = net acquisition of financial assets by residents (+ = outflow); *_net = liabilities - assets (+ = net inflow); reserves_change = net acquisition of reserve assets (+ = reserves rose); errors_omissions as published.',
    'sources': {
        'goods_x,goods_m,services_x,services_m,primary_income_*,secondary_income_*,current_account,fdi_*,portfolio_*,loans_*,currency_deposits_*,other_inv_*,errors_omissions,reserves_change': IMF + ' (IMF BOP dataset v21, BPM6, FREQUENCY=A, updated 2026-10-01)',
        'travel_x,travel_m,transport_x,transport_m': U['V09.15.px'] + ' (NSO services trade statistics, not SBV BOP)',
        'ip_m,comp_emp_*,inv_income_*': 'not published: Vietnam reports primary income only as a total in BOP (IMF BOP has no IN1 breakdown; WB BM.GSR.ROYL.CD empty for VNM)',
        'remit_in,remit_out': 'IMF BOP has secondary income totals only; see remittances block',
    },
}

# ---------------- tourism ----------------
arr = colwise('V10.04.html')
a4 = blocks('V10.04.html')
tv = a4[list(a4)[0]]
t1 = blocks('V10.01.html')[list(blocks('V10.01.html'))[0]]
tourism = {
    'years': YEARS + ['9M2026'],
    'intl_arrivals_thousand': ser(tv['TỔNG SỐ'], nd=1) + [17700.0],
    'arrivals_by_air_thousand': ser(tv['Đường hàng không'], nd=1) + [14700.0],
    'arrivals_by_road_thousand': ser(tv['Đường bộ'], nd=1) + [2700.0],
    'arrivals_by_sea_thousand': ser(tv['Đường thủy'], nd=1) + [243.4],
    'travel_receipts_busd': svc['travel_x'] + [13.06],
    'travel_payments_busd': svc['travel_m'] + [10.85],
    'vietnamese_outbound_departures_thousand': [None] * len(YEARS) + [4300.0],
    'outbound_tourists_served_by_travel_agencies_thousand': ser(t1['Khách Việt Nam đi du lịch nước ngoài (Nghìn lượt khách)'], nd=1) + [None],
    'accommodation_revenue_bn_vnd': ser(t1['Doanh thu của các cơ sở lưu trú (Tỷ đồng)'], nd=1) + [None],
    'travel_agency_revenue_bn_vnd': ser(t1['Doanh thu của các cơ sở lữ hành (Tỷ đồng)'], nd=1) + [None],
    'total_tourism_revenue_vnat_trn_vnd': [None, None, None, 620.0, 755.0, 312.2, 180.0, 495.0, 678.0, 840.0, 1000.0, None],
    'basis': ['official'] * 10 + ['preliminary', '9M estimate'],
    'sources': {
        'intl_arrivals_thousand, arrivals_by_*': U['V10.04.px'] + ' (NSO PxWeb V10.04); 9M2026: ' + U_9M26,
        'travel_receipts_busd, travel_payments_busd': U['V09.15.px'] + ' (NSO services trade, Dịch vụ du lịch); identical to WB ST.INT.RCPT.CD/ST.INT.XPND.CD 2015-2020 except 2020 payments; 9M2026: ' + U_9M26,
        'vietnamese_outbound_departures_thousand': U_9M26 + ' (lượt người Việt Nam xuất cảnh, 9M2026 only)',
        'outbound_tourists_served_by_travel_agencies_thousand, accommodation_revenue, travel_agency_revenue': U['V10.01.px'] + ' (NSO PxWeb V10.01)',
        'total_tourism_revenue_vnat_trn_vnd': {
            '2018': 'https://vneconomy.vn/tong-thu-tu-khach-du-lich-nam-2018-dat-hon-620000-ty-dong.htm (\'hơn 620.000 tỷ đồng\')',
            '2019': 'https://images.vietnamtourism.gov.vn/vn/dmdocuments/2021/bao_cao_thuong_nien_2019_final.pdf (VNAT Annual Report 2019: 755 nghìn tỷ, of which international 421, domestic 334)',
            '2020': 'https://tuoitre.vn/saigontimes/tong-thu-tu-khach-du-lich-cua-viet-nam-giam-575-000-ti-dong-sau-hai-nam-dich-106410249.htm',
            '2021': 'https://tuoitre.vn/saigontimes/tong-thu-tu-khach-du-lich-cua-viet-nam-giam-575-000-ti-dong-sau-hai-nam-dich-106410249.htm (\'khoảng 180.000 tỉ\')',
            '2022,2023,2024': 'https://thongke.tourism.vn/index.php/news/items/298 (VNAT statistics portal)',
            '2025': 'https://nhandan.vn/nam-2025-danh-dau-su-tang-truong-vuot-bac-cua-du-lich-viet-nam-post933648.html (\'hơn 1 triệu tỷ đồng\' — lower bound, preliminary)'},
    },
    'notes': ['VNAT total tourism revenue (tổng thu từ khách du lịch) includes domestic tourists; not comparable with BOP travel receipts.',
              '2015-2017 VNAT totals not verified from an official source -> null.'],
}

json.dump({'gdp_exp': gdp_exp, 'invest_by_owner': invest, 'bop': bop, 'tourism': tourism}, open('part1.json', 'w'), ensure_ascii=False, indent=1)
print('X-M minus NSO net exports (bn VND):', gdp_exp['check_X_minus_M_vs_NSO_net_exports_bn'])
print({k: v[-1] for k, v in S.items()})
