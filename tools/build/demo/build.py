import json, openpyxl, gzip, csv
from pxparse import parse

OLD = "https://pxweb.nso.gov.vn/pxweb/vi/D%c3%a2n%20s%e1%bb%91%20v%c3%a0%20lao%20%c4%91%e1%bb%99ng/D%c3%a2n%20s%e1%bb%91%20v%c3%a0%20lao%20%c4%91%e1%bb%99ng/"
NEW = "https://pxweb.nso.gov.vn/pxweb/vi/PLV02D%c3%a2n%20s%e1%bb%91%20v%c3%a0%20lao%20%c4%91%e1%bb%99ng/PLV02D%c3%a2n%20s%e1%bb%91%20v%c3%a0%20lao%20%c4%91%e1%bb%99ng/"
WB = "https://api.worldbank.org/v2/country/VNM/indicator/{}?format=json&per_page=100"
NQ = "https://baochinhphu.vn/nghi-quyet-cua-quoc-hoi-ve-sap-xep-don-vi-hanh-chinh-cap-tinh-102250612191145158.htm"
YEARS = list(range(2000, 2026))


def yr(label):
    return int(label[-4:])


def r(x, n):
    return None if x is None else round(x, n)


# ---------- national ----------
d, v, o = parse("v0202.px")  # [measure(3), year(36), breakdown(5)]
pop = {}; growth = {}; urban = {}
for i, y in enumerate(v[1]):
    Y = yr(y)
    pop[Y] = r(o[(0, i, 0)] / 1000, 3)
    growth[Y] = r(o[(1, i, 0)], 2)
    urban[Y] = r(o[(2, i, 3)], 2)

d, v, o = parse("old_v0215.px")
tfr = {yr(y): r(o[(i, 0)], 2) for i, y in enumerate(v[0])}


def xl(path):
    ws = openpyxl.load_workbook(path).active
    return [row for row in ws.iter_rows(values_only=True) if any(c is not None for c in row)]


rows = xl("old_V02.08.px.xlsx")
srb = {}
mode = None
for row in rows[2:]:
    if row[0]:
        mode = row[0]
    if mode and "khi sinh" in mode:
        srb[yr(row[1])] = row[2]

rows = xl("old_V02.11.px.xlsx")
cbr = {}; cdr = {}
mode = None
for row in rows[2:]:
    if row[0]:
        mode = row[0]
    if mode == "TỔNG SỐ":
        cbr[yr(row[1])] = row[2]; cdr[yr(row[1])] = row[3]

rows = xl("old_V02.24.px.xlsx")
hdr = rows[1]
le = {}
for row in rows:
    if row[0] == "CẢ NƯỚC":
        for h, val in zip(hdr[1:], row[1:]):
            if h and isinstance(val, (int, float)):
                le[yr(h)] = val

national = {
    "years": YEARS,
    "pop_m": [pop.get(y) for y in YEARS],
    "growth_pct": [growth.get(y) for y in YEARS],
    "cbr": [cbr.get(y) for y in YEARS],
    "cdr": [cdr.get(y) for y in YEARS],
    "tfr": [tfr.get(y) for y in YEARS],
    "srb": [srb.get(y) for y in YEARS],
    "life_exp": [le.get(y) for y in YEARS],
    "urban_pct": [urban.get(y) for y in YEARS],
    "units": {"pop_m": "million, average population (dân số trung bình)", "growth_pct": "% per year (tỷ lệ tăng dân số)",
              "cbr": "per 1000", "cdr": "per 1000", "tfr": "children per woman", "srb": "boys per 100 girls",
              "life_exp": "years at birth", "urban_pct": "% of average population living in urban areas"},
    "notes": [
        "All values are NSO/GSO official (PX-Web database, Statistical Yearbook tables). 2025 = 'Sơ bộ' (preliminary).",
        "cbr, cdr, tfr series in the NSO table start in 2001 -> 2000 is null. life_exp NSO series covers 2005 and 2009-2025 -> other years null.",
        "World Bank series (values = UN WPP 2024 estimates) are given separately in wb_crosscheck; not mixed into the GSO arrays.",
    ],
    "sources": {
        "pop_m": OLD + "V02.02.px/", "growth_pct": OLD + "V02.02.px/", "urban_pct": OLD + "V02.02.px/ (Cơ cấu % thành thị)",
        "cbr": OLD + "V02.11.px/", "cdr": OLD + "V02.11.px/", "tfr": OLD + "V02.15.px/",
        "srb": OLD + "V02.08.px/", "life_exp": OLD + "V02.24.px/",
    },
}

wb = {}
for ind, key in [("SP.POP.TOTL", "pop_m"), ("SP.POP.GROW", "growth_pct"), ("SP.DYN.CBRT.IN", "cbr"), ("SP.DYN.CDRT.IN", "cdr"),
                 ("SP.DYN.TFRT.IN", "tfr"), ("SP.DYN.LE00.IN", "life_exp"), ("SP.URB.TOTL.IN.ZS", "urban_pct")]:
    js = json.load(open(f"wb_{ind}.json"))
    m = {int(x["date"]): x["value"] for x in js[1]}
    vals = []
    for y in YEARS:
        val = m.get(y)
        if val is not None:
            val = round(val / 1e6, 3) if key == "pop_m" else round(val, 3)
        vals.append(val)
    wb[key] = vals
wb["source"] = {k: WB.format(i) for i, k in [("SP.POP.TOTL", "pop_m"), ("SP.POP.GROW", "growth_pct"), ("SP.DYN.CBRT.IN", "cbr"),
                                              ("SP.DYN.CDRT.IN", "cdr"), ("SP.DYN.TFRT.IN", "tfr"), ("SP.DYN.LE00.IN", "life_exp"),
                                              ("SP.URB.TOTL.IN.ZS", "urban_pct")]}
wb["note"] = "World Bank WDI (last updated 2026-07-13). pop_m is mid-year total and equals UN WPP 2024 estimates/medium projection; differs from GSO average population (e.g. 2024: WB 100.988 vs GSO 101.344)."
national["wb_crosscheck"] = wb

# ---------- projections ----------
gso = {
    "unit": "million persons (GSO tables in thousands, divided by 1000)",
    "medium": {"2025": 101.571, "2030": 105.219, "2035": 108.478, "2040": 111.375, "2045": 113.667, "2050": 115.249},
    "low": {"2025": 101.398, "2030": 104.740, "2035": 107.608, "2040": 110.047, "2045": 111.792, "2050": 112.745},
    "high": {"2025": 101.624, "2030": 105.417, "2035": 108.929, "2040": 112.187, "2045": 114.938, "2050": 117.073},
    "horizon_2069": {"medium": 116.894, "low": 111.106, "high": 121.981},
    "peak": {"year": 2066, "pop_m": 116.916, "variant": "medium"},
    "peak_low": {"year": 2054, "pop_m": 112.974},
    "peak_high": None,
    "age_2050": {"0-14": 18.86, "15-64": 62.33, "65+": 18.81, "variant": "medium",
                 "note": "Shares (%) computed from Biểu A.3 (medium variant, 5-year groups, 2050 column): 0-14 = 21,738k, 65+ = 21,675k, 15-64 = 71,836k of 115,249k. The report does not print 2050 shares directly."},
    "other_facts": ["Report text: 2069 population 116.9m (medium), 111.1m (low), 122.0m (high).",
                    "Medium variant: population 'stops growing' in 2064-2069; ageing (65+ >=14%) starts 2036; golden structure ends 2039.",
                    "High variant still rising in 2069 (no peak within horizon) -> peak_high null."],
    "yearly_medium_selected": {"2019": 96.209, "2020": 97.259, "2021": 98.184, "2022": 99.077, "2023": 99.939, "2024": 100.770},
    "source": "https://vietnam.unfpa.org/sites/default/files/pub-pdf/sach_dan_so_va_du_bao_dan_so_a4_vn_2106.pdf (GSO/UNFPA, 'Dự báo dân số Việt Nam giai đoạn 2019-2069', Hà Nội 11-2020; Biểu A.1, A.3, Biểu 2.1)",
}

un = {"unit": "million persons, 1 July", "medium": {}, "low": {}, "high": {}}
for f, variants in [("WPP2024_Demographic_Indicators_Medium.csv.gz", ["Medium"]),
                    ("WPP2024_Demographic_Indicators_OtherVariants.csv.gz", ["Low", "High"])]:
    with gzip.open(f, "rt", encoding="utf-8", errors="ignore") as fh:
        for x in csv.DictReader(fh):
            if x["LocID"] == "704" and x["Variant"] in variants and x["TPopulation1July"]:
                t = int(x["Time"])
                un.setdefault("_all_" + x["Variant"], {})[t] = float(x["TPopulation1July"]) / 1000
for vname, key in [("Medium", "medium"), ("Low", "low"), ("High", "high")]:
    ser = un.pop("_all_" + vname)
    un[key] = {str(y): round(ser[y], 3) for y in range(2025, 2051, 5)}
    sy = {y: s for y, s in ser.items() if 2024 <= y <= 2100}
    py = max(sy, key=sy.get)
    if key == "medium":
        un["peak"] = {"year": py, "pop_m": round(sy[py], 3)}
    elif key == "low":
        un["peak_low"] = {"year": py, "pop_m": round(sy[py], 3)}
    else:
        un["peak_high"] = None if py == 2100 else {"year": py, "pop_m": round(sy[py], 3)}
un["note"] = "High variant keeps growing to 2100 -> peak_high null. Values from official WPP 2024 CSV (LocID 704); UN data-portal API requires a token so the CSV bulk file was used."
un["source"] = "https://population.un.org/wpp/assets/Excel%20Files/1_Indicator%20(Standard)/CSV_FILES/WPP2024_Demographic_Indicators_Medium.csv.gz ; .../WPP2024_Demographic_Indicators_OtherVariants.csv.gz"

# ---------- provinces ----------
NQ202 = {  # name: (area, pop, merged_from)
    "Tuyên Quang": (13795.50, 1865270, ["Hà Giang", "Tuyên Quang"]),
    "Lào Cai": (13256.92, 1778785, ["Yên Bái", "Lào Cai"]),
    "Thái Nguyên": (8375.21, 1799489, ["Bắc Kạn", "Thái Nguyên"]),
    "Phú Thọ": (9361.38, 4022638, ["Vĩnh Phúc", "Hòa Bình", "Phú Thọ"]),
    "Bắc Ninh": (4718.60, 3619433, ["Bắc Giang", "Bắc Ninh"]),
    "Hưng Yên": (2514.81, 3567943, ["Thái Bình", "Hưng Yên"]),
    "Hải Phòng": (3194.72, 4664124, ["Hải Phòng", "Hải Dương"]),
    "Ninh Bình": (3942.62, 4412264, ["Hà Nam", "Nam Định", "Ninh Bình"]),
    "Quảng Trị": (12700.00, 1870845, ["Quảng Bình", "Quảng Trị"]),
    "Đà Nẵng": (11859.59, 3065628, ["Đà Nẵng", "Quảng Nam"]),
    "Quảng Ngãi": (14832.55, 2161755, ["Kon Tum", "Quảng Ngãi"]),
    "Gia Lai": (21576.53, 3583693, ["Bình Định", "Gia Lai"]),
    "Khánh Hòa": (8555.86, 2243554, ["Ninh Thuận", "Khánh Hòa"]),
    "Lâm Đồng": (24233.07, 3872999, ["Đắk Nông", "Bình Thuận", "Lâm Đồng"]),
    "Đắk Lắk": (18096.40, 3346853, ["Phú Yên", "Đắk Lắk"]),
    "TP Hồ Chí Minh": (6772.59, 14002598, ["TP Hồ Chí Minh", "Bà Rịa - Vũng Tàu", "Bình Dương"]),
    "Đồng Nai": (12737.18, 4491408, ["Bình Phước", "Đồng Nai"]),
    "Tây Ninh": (8536.44, 3254170, ["Long An", "Tây Ninh"]),
    "Cần Thơ": (6360.83, 4199824, ["Cần Thơ", "Sóc Trăng", "Hậu Giang"]),
    "Vĩnh Long": (6296.20, 4257581, ["Bến Tre", "Trà Vinh", "Vĩnh Long"]),
    "Đồng Tháp": (5938.64, 4370046, ["Tiền Giang", "Đồng Tháp"]),
    "Cà Mau": (7942.39, 2606672, ["Bạc Liêu", "Cà Mau"]),
    "An Giang": (9888.91, 4952238, ["Kiên Giang", "An Giang"]),
}
ORDER = ["Tuyên Quang", "Cao Bằng", "Lai Châu", "Lào Cai", "Thái Nguyên", "Điện Biên", "Lạng Sơn", "Sơn La", "Phú Thọ", "Bắc Ninh",
         "Quảng Ninh", "Hà Nội", "Hải Phòng", "Hưng Yên", "Ninh Bình", "Thanh Hóa", "Nghệ An", "Hà Tĩnh", "Quảng Trị", "Huế", "Đà Nẵng",
         "Quảng Ngãi", "Gia Lai", "Khánh Hòa", "Đắk Lắk", "Lâm Đồng", "Đồng Nai", "TP Hồ Chí Minh", "Tây Ninh", "Đồng Tháp", "An Giang",
         "Vĩnh Long", "Cần Thơ", "Cà Mau"]
CITY = {"Hà Nội", "Hải Phòng", "Huế", "Đà Nẵng", "TP Hồ Chí Minh", "Cần Thơ"}


def norm(n):
    return "TP Hồ Chí Minh" if n == "TP. Hồ Chí Minh" else n


def rowmap(path, start):
    out = {}
    for row in xl(path)[start:]:
        if row[0]:
            out[norm(row[0])] = row
    return out


a01 = rowmap("new_PL.V02.01.px.xlsx", 3)           # area, pop2025, density
p02 = rowmap("new_PL.V02.02-04.px.xlsx", 3)        # total 2020..2025 (1..6), urban 2020..2025 (19..24)
sr05 = rowmap("new_PL.V02.05.px.xlsx", 2)          # 2020..2025 (1..6)
cb07 = rowmap("new_PL.V02.07-09.px.xlsx", 3)       # cbr 1..6, cdr 7..12
le17 = rowmap("new_PL.V02.17.px.xlsx", 2)
# TFR with full precision from PX file
d, v, o = parse("new_v0210.px")
tfr_rows = [norm(n) for n in [row[0] for row in xl("new_PL.V02.10.px.xlsx")[2:]]]
assert len(tfr_rows) == len(v[0]), (len(tfr_rows), len(v[0]))
yi = {yr(y): j for j, y in enumerate(v[1])}
tfrmap = {n: {Y: o[(i, j)] for Y, j in yi.items()} for i, n in enumerate(tfr_rows)}

provinces = []
for n in ORDER:
    A, P, S, C, L = a01[n], p02[n], sr05[n], cb07[n], le17[n]
    tot24, tot25 = P[5], P[6]
    urb24, urb25 = P[23], P[24]
    rec = {"name": ("Thành phố " if n in CITY else "Tỉnh ") + n.replace("TP ", ""), "short_name": n,
           "type": "city" if n in CITY else "province"}
    if n in NQ202:
        area, popv, mf = NQ202[n]
        rec.update({"area_km2": area, "pop": popv, "pop_basis": "NQ202/2025/QH15 'quy mô dân số' (persons)",
                    "merged_from": mf, "merged": True})
    else:
        rec.update({"area_km2": A[1], "pop": round(tot24 * 1000), "pop_basis": "NSO average population 2024 (not merged; NQ202 states no figure)",
                    "merged_from": [n], "merged": False})
    rec.update({
        "area_km2_nso_2025": A[1],
        "pop_avg_2024_nso": round(tot24 * 1000),
        "pop_avg_2025p_nso": round(tot25 * 1000),
        "density_2025p_nso": A[3],
        "urban_pct": round(urb24 / tot24 * 100, 2),
        "urban_pct_2025p": round(urb25 / tot25 * 100, 2),
        "srb": None,
        "sex_ratio_pop": S[5],
        "sex_ratio_pop_2025p": S[6],
        "tfr": r(tfrmap[n][2024], 2),
        "tfr_2025p": r(tfrmap[n][2025], 2),
        "cbr": C[5], "cdr": C[11],
        "life_exp": L[5],
        "data_year": 2024,
        "source": {"area_pop_merge": NQ if n in NQ202 else OLD.replace(OLD, NEW) + "PL.V02.01.px/",
                   "nso_tables": NEW + " (PL.V02.01, PL.V02.02-04, PL.V02.05, PL.V02.07-09, PL.V02.10, PL.V02.17)"},
    })
    provinces.append(rec)

# sanity: sums
s24 = sum(p["pop_avg_2024_nso"] for p in provinces)
print("sum NSO 2024 avg pop 34 units (k):", s24 / 1000, "national:", p02["CẢ NƯỚC"][5])
print("sum NQ202-or-NSO pop:", sum(p["pop"] for p in provinces))

out = {
    "meta": {"generated": "2026-10-04", "notes": [
        "Never interpolated. null = not published in the source used.",
        "Province indicators (urban_pct, sex_ratio_pop, tfr, cbr, cdr, life_exp) are NSO's own back-cast series for the new 34 units (PX-Web folder 'PLV02'), reference year 2024 unless suffixed _2025p (preliminary).",
        "urban_pct = NSO urban average population / total average population x100 (computed from the same NSO table).",
        "srb (sex ratio at birth) is published by NSO only nationally and by region, not by province -> null; sex_ratio_pop = males per 100 females of total population.",
        "NSO PL.V02.06 (SRB by region, new units) shows values ~99-101 for 2020-2025, i.e. it appears to repeat the population sex ratio — not used.",
        "NQ202 'quy mô dân số' figures (23 merged units) are administrative population counts used for the merger dossier and are systematically HIGHER than NSO average population (e.g. TP HCM 14.00m vs NSO 2024 avg 13.61m; An Giang 4.95m vs 3.68m). Do not mix with NSO series; for comparisons across all 34 use pop_avg_2024_nso / pop_avg_2025p_nso.",
        "For the 11 non-merged units NQ202 gives no area/population, so area_km2 = NSO 2025 area and pop = NSO 2024 average population (see pop_basis).",
        "Urban shares for some provinces drop sharply in 2025p (e.g. Cao Bằng 25.5% -> 14.8%) reflecting the 1/7/2025 commune-level reorganisation reclassifying urban areas, not real de-urbanisation.",
    ]},
    "national": national,
    "projections": {"gso_2019_2069": gso, "un_wpp2024": un},
    "provinces": provinces,
}
json.dump(out, open("demo.json", "w"), ensure_ascii=False, indent=1)
print("ok")
