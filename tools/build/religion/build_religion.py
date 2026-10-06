import json, glob, openpyxl, os

os.chdir(os.path.dirname(os.path.abspath(__file__)))
demo = json.load(open('../demo/demo.json'))
UNITS = demo['provinces']
ALIAS = {'Hoà Bình': 'Hòa Bình', 'Thừa Thiên Huế': 'Huế', 'Hồ Chí Minh': 'TP Hồ Chí Minh'}
TOP = ['Công giáo', 'Phật giáo', 'Tin lành', 'Cao Đài', 'Phật giáo Hòa Hảo', 'Hồi giáo']

URL_KQ2019 = 'https://www.nso.gov.vn/wp-content/uploads/2019/12/Ket-qua-toan-bo-Tong-dieu-tra-dan-so-va-nha-o-2019.pdf'
URL_KQCY2019 = 'https://www.nso.gov.vn/wp-content/uploads/2019/12/TDT-Dan-so-2019-1.pdf'
URL_KQ2009 = 'https://www.nso.gov.vn/wp-content/uploads/2019/03/KQ-toan-bo.pdf'
URL_TC2009 = 'https://www.nso.gov.vn/su-kien/2019/03/thong-cao-bao-chi-tong-dieu-tra-dan-so-va-nha-o-nam-2009-cong-bo-ket-qua-dieu-tra-toan-bo/'
URL_ODM = 'https://data.vietnam.opendevelopmentmekong.net/dataset/population-by-religion-in-vietnam-in-2019'
URL_ODM_X = 'https://data.vietnam.opendevelopmentmekong.net/vi/dataset/830ab51d-9d87-4dc5-a74f-f327d2ead6e8/resource/06e21f6d-2c26-4ad3-b8f6-2869d444d1a6/download/religion-followers-by-province.xlsx'
URL_WP = 'https://cms.btgcp.gov.vn/upload/documents/25_08_2023/-2023-08-25-18-52-04.pdf'
URL_WP_NEWS = 'https://baotintuc.vn/thoi-su/ra-mat-sach-trang-ton-giao-va-chinh-sach-ton-giao-o-viet-nam-20230309104943196.htm'
URL_2023 = 'https://danviet.vn/truong-ban-ton-giao-chinh-phu-con-tinh-trang-loi-dung-tin-nguong-ton-giao-de-chong-pha-kich-dong-20240815140750165.htm'
URL_2023b = 'https://vietnamnet.vn/viet-nam-co-16-ton-giao-va-gan-28-trieu-tin-do-2424794.html'
URL_2025 = 'http://www.cema.gov.vn/hanh-trinh-70-nam-ban-ton-giao-chinh-phu.htm'
PX = 'https://pxweb.nso.gov.vn/pxweb/vi/PLV02D%c3%a2n%20s%e1%bb%91%20v%c3%a0%20lao%20%c4%91%e1%bb%99ng/PLV02D%c3%a2n%20s%e1%bb%91%20v%c3%a0%20lao%20%c4%91%e1%bb%99ng/'


def pct(a, b, nd=2):
    return None if a is None or not b else round(a / b * 100, nd)


# ---------- national 2019 (GSO Kết quả toàn bộ, Biểu 3) ----------
POP19 = 96208984
NOREL19 = 83046105
REL19 = {'Công giáo': 5866169, 'Phật giáo': 4606543, 'Tin lành': 960558, 'Cao Đài': 556234,
         'Phật giáo Hòa Hảo': 983079, 'Hồi giáo': 70934, "Tôn giáo Baha'i": 2153,
         'Tịnh độ Cư sỹ Phật hội Việt Nam': 2306, 'Đạo Tứ Ân Hiếu nghĩa': 30416, 'Bửu Sơn Kỳ Hương': 2975,
         'Giáo hội Phật đường Nam Tông Minh Sư đạo': 260, 'Hội thánh Minh Lý đạo - Tam Tông Miếu': 193,
         'Chăm Bà la môn': 64547, 'Giáo hội Các thành hữu Ngày sau của Chúa Giê su Ky tô Việt Nam (Mormon)': 4281,
         'Phật giáo Hiếu Nghĩa Tà Lơn': 401, 'Giáo hội Cơ đốc Phục lâm Việt Nam': 11830}
F19 = sum(REL19.values())
assert F19 + NOREL19 == POP19, (F19, NOREL19, POP19)
others19 = F19 - sum(REL19[k] for k in TOP)
census2019 = {
    'date': '2019-04-01', 'population': POP19, 'followers': F19, 'followers_pct': pct(F19, POP19),
    'no_religion': NOREL19, 'no_religion_pct': pct(NOREL19, POP19),
    'n_religions_listed': 16,
    'by_religion': {k: {'n': REL19[k], 'pct_pop': pct(REL19[k], POP19, 3), 'pct_followers': pct(REL19[k], F19)} for k in TOP},
    'others': {'n': others19, 'pct_pop': pct(others19, POP19, 3), 'pct_followers': pct(others19, F19),
               'detail': {k: v for k, v in REL19.items() if k not in TOP}},
    'urban_rural_note': 'Biểu 3 also splits urban/rural and sex (not reproduced).',
    'source': 'GSO, Kết quả toàn bộ Tổng điều tra dân số và nhà ở năm 2019, Biểu 3 (p.210)', 'url': URL_KQ2019,
    'press_summary': 'GSO: 16 religions; 13.2 million followers = 13.7% of population; Catholics 5.9m (44.6% of followers, 6.1% of pop.), Buddhists 4.6m (35.0%, 4.8%).',
    'press_url': URL_KQCY2019,
}

# ---------- national 2009 (GSO Kết quả toàn bộ 2009, Biểu 7) ----------
r09 = json.load(open('rel2009_raw.json'))
p09 = json.load(open('pop2009.json'))
POP09 = sum(v['pop'] for v in p09.values())
assert POP09 == 85846997
R09 = r09['TOÀN QUỐC']['rel']
F09 = r09['TOÀN QUỐC']['total']
assert sum(R09.values()) == F09 == 15651467
others09 = F09 - sum(R09[k] for k in TOP)
census2009 = {
    'date': '2009-04-01', 'population': POP09, 'followers': F09, 'followers_pct': pct(F09, POP09),
    'no_religion_derived': POP09 - F09,
    'n_religions_listed': 13,
    'by_religion': {k: {'n': R09[k], 'pct_pop': pct(R09[k], POP09, 3), 'pct_followers': pct(R09[k], F09)} for k in TOP},
    'others': {'n': others09, 'detail': {k: v for k, v in R09.items() if k not in TOP}},
    'source': 'GSO, Tổng điều tra dân số và nhà ở Việt Nam năm 2009: Kết quả toàn bộ, Biểu 7 (p.281); population from Biểu 1',
    'url': URL_KQ2009,
    'note': "Biểu 7 total 15,651,467 includes 30 'không xác định tôn giáo'. GSO 2009 press release instead says 15.672 million followers (+932 thousand vs 1999) — small unexplained gap vs the table.",
    'press_url': URL_TC2009,
}
change = {k: {'n_2009': R09[k], 'n_2019': REL19[k], 'diff': REL19[k] - R09[k], 'pct_change': pct(REL19[k] - R09[k], R09[k], 1)} for k in TOP}
change['_total_followers'] = {'n_2009': F09, 'n_2019': F19, 'diff': F19 - F09, 'pct_change': pct(F19 - F09, F09, 1),
                              'share_pop_2009': pct(F09, POP09), 'share_pop_2019': pct(F19, POP19)}

# ---------- Ban Tôn giáo Chính phủ ----------
btgcp = {
    'latest': {
        'date': '2023-12-31', 'religions': 16, 'organizations': 43, 'followers': 27700000, 'followers_text': 'trên 27,7 triệu tín đồ',
        'followers_pct': 27, 'followers_pct_text': 'chiếm trên 27% dân số', 'dignitaries_chuc_sac': 54500,
        'officials_chuc_viec': 145000, 'places_of_worship': 29890, 'belief_facilities': 51000,
        'population_with_belief_or_religion_pct_text': 'hơn 95% dân số theo tín ngưỡng và tôn giáo',
        'stated_by': 'Trưởng Ban Tôn giáo Chính phủ Vũ Hoài Bắc, hội nghị Ban Tuyên giáo TƯ 15/8/2024',
        'url': URL_2023, 'url_alt': URL_2023b,
        'note': 'figures are rounded ("khoảng 54.500 chức sắc, gần 145.000 chức việc, 29.890 cơ sở thờ tự, khoảng 51.000 cơ sở tín ngưỡng")',
    },
    'mid_2025': {
        'date': '2025-07 (article dated 30/07/2025; "tính đến giữa năm 2025")', 'religions': 16, 'organizations': 43,
        'followers_text': 'gần 28 triệu tín đồ', 'dignitaries_text': 'hơn 54 nghìn chức sắc, nhà tu hành',
        'places_of_worship_text': 'hàng vạn cơ sở thờ tự', 'population_with_belief_or_religion_pct_text': '95%',
        'source': 'Bộ Dân tộc và Tôn giáo (cema.gov.vn), "Hành trình 70 năm Ban Tôn giáo Chính phủ"', 'url': URL_2025,
    },
    'white_paper_2023': {
        'title': 'Sách trắng "Tôn giáo và chính sách tôn giáo ở Việt Nam" (Ban Tôn giáo Chính phủ, ra mắt 09/3/2023)',
        'date': '2021-12-31', 'religions': 16, 'organizations_recognized': 36,
        'organizations_registered': '04 tổ chức + 01 pháp môn tu hành (được cấp chứng nhận đăng ký hoạt động)',
        'followers': 26500000, 'followers_text': 'trên 26,5 triệu tín đồ', 'followers_pct': 27,
        'dignitaries_chuc_sac': 54000, 'officials_chuc_viec': 135000, 'places_of_worship': 29658,
        'by_religion_dec2021': {
            'Phật giáo': {'followers_text': 'khoảng 14 triệu tín đồ đã quy y', 'monks_nuns': 54169, 'places_of_worship': 18544},
            'Công giáo': {'followers_text': 'trên 7 triệu tín đồ, khoảng 7% dân số', 'bishops': 46, 'priests_text': 'hơn 5 nghìn linh mục'},
            'Tin lành': {'followers_text': 'hơn 1,2 triệu', 'dignitaries_text': 'hơn 2.300 chức sắc', 'places_of_worship_text': 'gần 900'},
            'Cao Đài': {'followers_text': 'hơn 1,2 triệu', 'dignitaries_text': 'hơn 13 nghìn chức sắc', 'places_text': '1.300 cơ sở tôn giáo, 38 tỉnh/thành'},
            'Phật giáo Hòa Hảo': {'followers_text': 'hơn 1,5 triệu', 'officials_text': '4 nghìn chức việc', 'places_of_worship': 50},
            'Hồi giáo': {'followers_text': 'hơn 80 nghìn (Islam ~30 nghìn, Bàni ~50 nghìn)', 'places_of_worship': 89},
            "Baha'i": {'followers_text': 'khoảng 7 nghìn, 45 tỉnh/thành'},
        },
        'url': URL_WP, 'news_url': URL_WP_NEWS,
    },
    'url': URL_2023,
    'date': '2023-12-31',
}
gap = {
    'census2019_followers': F19, 'btgcp_followers_2021': 26500000, 'btgcp_followers_2023': 27700000,
    'ratio_btgcp2023_to_census': round(27700000 / F19, 2),
    'buddhists_census2019': REL19['Phật giáo'], 'buddhists_white_paper_2021_text': 'khoảng 14 triệu tín đồ đã quy y',
    'catholics_census2019': REL19['Công giáo'], 'catholics_white_paper_2021_text': 'trên 7 triệu',
    'explanation': (
        "Neither GSO nor Ban Tôn giáo publishes an official reconciliation. Definitional basis visible in the sources: "
        "(1) the census records each person's self-declared answer to the question '[TÊN] có theo đạo, tôn giáo nào không? Nếu có: đạo, tôn giáo gì?' "
        "(2009 census questionnaire, printed in the 2009 Kết quả toàn bộ appendix); people who practise ancestor worship/folk belief or attend pagodas "
        "but do not declare a religion are counted as 'không theo tôn giáo'. "
        "(2) Ban Tôn giáo figures are administrative counts compiled from the religious organisations themselves, e.g. the White Paper counts Buddhists as "
        "'khoảng 14 triệu tín đồ đã quy y' (people who took refuge) reported by the Vietnam Buddhist Sangha. The gap is largest for Buddhism (4.6m vs ~14m); "
        "Catholic (5.9m vs >7m), Hòa Hảo (1.0m vs >1.5m) and Cao Đài (0.56m vs >1.2m) gaps are smaller. Buddhist press (Giác Ngộ, 21/12/2019) reported that "
        "clergy/officials attributed the low census figure to reluctance to declare religion ('tâm lý ngại khai yếu tố tôn giáo') — an opinion, not an official methodology note."),
    'urls': [URL_WP, URL_KQ2009, 'https://giacngo.vn/so-nguoi-theo-phat-giao-con-46-trieu-dung-thu-hai-o-viet-nam-post49910.html'],
}

# ---------- provinces ----------
ws = openpyxl.load_workbook('odm_prov.xlsx').worksheets[1]
odm = {}
for r in list(ws.iter_rows(values_only=True))[1:]:
    if r[0]:
        odm[ALIAS.get(r[1], r[1])] = {'code': int(r[0]), 'pop': r[2], 'no_religion': r[3], 'followers': r[4]}
assert sum(v['pop'] for v in odm.values()) == POP19 and sum(v['followers'] for v in odm.values()) == F19
code2name = {v['code']: k for k, v in odm.items()}

p19 = {}
for f in sorted(glob.glob('prov2019_*.json')):
    for k, v in json.load(open(f)).items():
        k = ALIAS.get(k, k)
        o = odm[k]
        s = sum(v['religions'].values())
        ok = v['pop'] == o['pop'] and v['no_religion'] == o['no_religion'] and s == o['followers']
        if not ok:
            print('VALIDATION FAIL', k, v['pop'], o['pop'], v['no_religion'], o['no_religion'], s, o['followers'])
            continue
        p19[k] = v
print('2019 by-religion validated for', len(p19), 'old provinces')

o09 = {}
for k, v in r09.items():
    if k == 'TOÀN QUỐC':
        continue
    name = code2name[int(v['code'])]
    tot = v['total'] or sum(v['rel'].values())
    assert sum(v['rel'].values()) == tot, k
    o09[name] = {'followers': tot, 'rel': v['rel']}
for k, v in p09.items():
    o09[code2name[int(v['code'])]]['pop'] = v['pop']
assert len(o09) == 63


def breakdown(rel, pop, fol):
    out = {}
    for k in TOP:
        n = rel.get(k, 0)
        out[k] = {'n': n, 'pct': pct(n, pop), 'pct_followers': pct(n, fol)}
    oth = fol - sum(out[k]['n'] for k in TOP)
    out['Khác'] = {'n': oth, 'pct': pct(oth, pop), 'pct_followers': pct(oth, fol)}
    return out


provinces = {}
old63 = {}
for u in UNITS:
    mf = u['merged_from']
    pop = sum(odm[m]['pop'] for m in mf)
    fol = sum(odm[m]['followers'] for m in mf)
    rec = {'merged_from': mf, 'pop2019': pop, 'followers': fol, 'followers_pct': pct(fol, pop),
           'no_religion': pop - fol}
    have = [m for m in mf if m in p19]
    if len(have) == len(mf):
        rel = {}
        for m in mf:
            for k, v in p19[m]['religions'].items():
                rel[k] = rel.get(k, 0) + v
        rec['by_religion'] = breakdown(rel, pop, fol)
        rec['by_religion_year'] = 2019
        top = max(TOP, key=lambda k: rec['by_religion'][k]['n'])
        rec['largest_religion'] = top
    else:
        rec['by_religion'] = None
        rec['by_religion_missing_2019'] = [m for m in mf if m not in p19]
    # 2009 comparison (complete for all 63)
    pop9 = sum(o09[m]['pop'] for m in mf)
    fol9 = sum(o09[m]['followers'] for m in mf)
    rel9 = {}
    for m in mf:
        for k, v in o09[m]['rel'].items():
            rel9[k] = rel9.get(k, 0) + v
    rec['census2009'] = {'pop': pop9, 'followers': fol9, 'followers_pct': pct(fol9, pop9), 'by_religion': breakdown(rel9, pop9, fol9)}
    provinces[u['short_name']] = rec
for m, o in odm.items():
    old63[m] = {'code': o['code'], 'pop2019': o['pop'], 'followers2019': o['followers'], 'followers_pct2019': pct(o['followers'], o['pop']),
                'religions2019': p19[m]['religions'] if m in p19 else None, 'source2019': p19[m]['url'] if m in p19 else None,
                'pop2009': o09[m]['pop'], 'followers2009': o09[m]['followers'], 'religions2009': o09[m]['rel']}

# check sums
assert sum(p['pop2019'] for p in provinces.values()) == POP19
assert sum(p['followers'] for p in provinces.values()) == F19
assert sum(p['census2009']['followers'] for p in provinces.values()) == F09

prov2020 = json.load(open('prov2020.json'))
prov_2020 = {k: v for k, v in prov2020.items() if k != 'CẢ NƯỚC'}
assert set(prov_2020) == set(provinces)

out = {
    'meta': {'generated': '2026-10-05', 'units': 34, 'note': 'Provincial religion data are census (self-declared) counts on old 63-province boundaries aggregated to the 34 units of NQ 202/2025/QH15 via demo.json merged_from. pct = % of total population; pct_followers = % of religious followers.'},
    'national': {'census2019': census2019, 'census2009': census2009, 'change_2009_2019': change, 'btgcp': btgcp, 'census_vs_btgcp': gap},
    'provinces': provinces,
    'old63': old63,
    'prov_2020': prov_2020,
    'prov_2020_national': prov2020['CẢ NƯỚC'],
    'sources': {
        'census2019_national': URL_KQ2019,
        'census2019_major_findings': URL_KQCY2019,
        'census2019_province_totals': {'dataset': URL_ODM, 'xlsx': URL_ODM_X, 'note': 'Open Development Mekong re-publication of GSO 2019 census province totals (total, no religion, followers). Verified: 63-province sums equal GSO national totals exactly (96,208,984; 83,046,105; 13,162,879).'},
        'census2019_province_by_religion': {k: {'url': v['url'], 'table': v.get('table')} for k, v in sorted(p19.items())},
        'census2009': URL_KQ2009, 'census2009_press': URL_TC2009,
        'btgcp_2023': URL_2023, 'btgcp_2023_alt': URL_2023b, 'btgcp_mid2025': URL_2025, 'white_paper': URL_WP, 'white_paper_news': URL_WP_NEWS,
        'prov_2020': {'base': PX, 'tables': {'pop_urban_sex': 'PL.V02.02-04', 'sex_ratio': 'PL.V02.05', 'cbr_cdr': 'PL.V02.07-09', 'tfr': 'PL.V02.10', 'life_exp': 'PL.V02.17', 'area_2025': 'PL.V02.01'},
                      'note': 'NSO PxWeb tables for the 34 new units start in 2020 — no 2019 values published. pop = average population 2020 (persons, from thousands). urban_pct = urban/total average population 2020. sex_ratio = males per 100 females (population). Area is only published for 2025 (area_km2_2025).'},
    },
}
json.dump(out, open('religion.json', 'w'), ensure_ascii=False, indent=1)
miss = {k: v['by_religion_missing_2019'] for k, v in provinces.items() if v['by_religion'] is None}
print('units with full 2019 by-religion:', 34 - len(miss)); print('missing:', miss)
