import json, openpyxl
D = '../demo/'
demo = json.load(open(D + 'demo.json'))
names = [p['short_name'] for p in demo['provinces']]
def norm(n): return 'TP Hồ Chí Minh' if n == 'TP. Hồ Chí Minh' else n
def rm(f):
    out = {}; hdr = None
    for row in openpyxl.load_workbook(D + f).active.iter_rows(values_only=True):
        if row[0]: out[norm(row[0].strip())] = row
    return out
a01, p02, s05, c07, t10, l17 = [rm(f) for f in ['new_PL.V02.01.px.xlsx','new_PL.V02.02-04.px.xlsx','new_PL.V02.05.px.xlsx','new_PL.V02.07-09.px.xlsx','new_PL.V02.10.px.xlsx','new_PL.V02.17.px.xlsx']]
# column checks
hdr02 = list(openpyxl.load_workbook(D+'new_PL.V02.02-04.px.xlsx').active.iter_rows(values_only=True))[2:4]
assert hdr02[0][1]=='Tổng số' and hdr02[1][1]=='2020' and hdr02[0][19]=='Thành thị' and hdr02[1][19]=='2020' and hdr02[0][7]=='Nam' and hdr02[0][13]=='Nữ', hdr02
h07 = list(openpyxl.load_workbook(D+'new_PL.V02.07-09.px.xlsx').active.iter_rows(values_only=True))[2:4]
assert h07[0][1]=='Tỷ suất sinh thô' and h07[1][1]=='2020' and h07[0][7]=='Tỷ suất chết thô' and h07[1][7]=='2020'
for f in ['new_PL.V02.05.px.xlsx','new_PL.V02.10.px.xlsx','new_PL.V02.17.px.xlsx']:
    assert list(openpyxl.load_workbook(D+f).active.iter_rows(values_only=True))[2][1]=='2020'
h01 = list(openpyxl.load_workbook(D+'new_PL.V02.01.px.xlsx').active.iter_rows(values_only=True))[2:4]
assert h01[0][1]=='2025' and 'Diện tích' in h01[1][1]
res = {}
for n in ['CẢ NƯỚC'] + names:
    P = p02[n]
    res[n] = {
        'pop': round(P[1]*1000), 'pop_male': round(P[7]*1000), 'pop_female': round(P[13]*1000),
        'urban_pct': round(P[19]/P[1]*100, 2),
        'tfr': t10[n][1], 'cbr': c07[n][1], 'cdr': c07[n][7], 'life_exp': l17[n][1],
        'sex_ratio': s05[n][1],
        'area_km2_2025': a01[n][1],
    }
json.dump(res, open('prov2020.json','w'), ensure_ascii=False, indent=1)
for k in ['CẢ NƯỚC','Hà Nội','TP Hồ Chí Minh','Cà Mau']: print(k, res[k])
print(len(res), sum(res[n]['pop'] for n in names), res['CẢ NƯỚC']['pop'])
