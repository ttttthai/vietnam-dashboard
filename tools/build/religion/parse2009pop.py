import re, json
L = open('kq2009.txt', encoding='utf-8').read().split('\n')
out = {}
for s in L[330:420]:
    m = re.match(r'^\s*(\d{2})\s+(.+?)\s{2,}(\d{1,3}(?:\.\d{3})+)\s', s)
    if m: out[m.group(2).strip()] = {'code': m.group(1), 'pop': int(m.group(3).replace('.', ''))}
print(len(out), sum(v['pop'] for v in out.values()))
json.dump(out, open('pop2009.json', 'w'), ensure_ascii=False, indent=1)
print(list(out))
