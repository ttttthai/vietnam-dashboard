import re, json
L = open('kq2009.txt', encoding='utf-8').read().split('\n')
start = 11235
# find end: next "Biểu - Table 8"
end = next(i for i in range(start+10, len(L)) if re.search(r'Biểu - Table 8\b', L[i]))
num = r'(\d{1,3}(?:\.\d{3})*|-)'
def n(s): return 0 if s == '-' else int(s.replace('.', ''))
CODES = {'01':'Phật giáo','02':'Công giáo','03':'Phật giáo Hòa Hảo','04':'Hồi giáo','05':'Cao Đài','06':'Minh Sư Đạo','07':'Minh Lý Đạo','08':'Tin lành','09':'Tịnh độ Cư sĩ Phật hội Việt Nam','10':'Đạo Tứ Ân Hiếu Nghĩa','11':'Bửu Sơn Kỳ Hương','12':"Baha'i",'13':'Bà La Môn'}
out = {}; cur = None
for i in range(start, end):
    s = L[i]
    m = re.match(r'^\s*(\d{1,2})\.\s+([^\d]+?)\s+' + num + r'\s', s)
    if m and not re.match(r'^\s*\d{2}\s{2,}', s):
        cur = m.group(2).strip(); out[cur] = {'code': m.group(1), 'total': n(m.group(3)), 'rel': {}}; continue
    m = re.match(r'^\s*TOÀN QUỐC', s)
    if m: cur = 'TOÀN QUỐC'; out[cur] = {'rel': {}}; continue
    if re.match(r'^V\d\.', s): cur = None; continue
    m = re.match(r'^\s*(\d{2})\s{2,}(.+?)\s{2,}' + num + r'\s', s)
    if m and cur and m.group(1) in CODES:
        out[cur]['rel'][CODES[m.group(1)]] = n(m.group(3)); continue
    m = re.match(r'^\s*Không xác định tôn giáo.*?\s{2,}' + num + r'\s', s)
    if m and cur: out[cur]['rel']['Không xác định'] = n(m.group(1))
    m = re.match(r'^\s*Tổng số - Total\s+' + num, s)
    if m and cur == 'TOÀN QUỐC': out[cur]['total'] = n(m.group(1))
bad = []
for k, v in out.items():
    sm = sum(v['rel'].values())
    if sm != v.get('total'): bad.append((k, v.get('total'), sm))
print(len(out), 'provinces+national'); print('mismatch', bad)
json.dump(out, open('rel2009_raw.json', 'w'), ensure_ascii=False, indent=1)
