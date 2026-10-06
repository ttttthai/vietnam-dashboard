import json
d=json.load(open('chunk_exact_4.json'))
T={}
for m in ('b1','b2','b3'):
    T.update(__import__(m).T)
out={s:T[i] for i,s in enumerate(d)}
json.dump(out,open('en_exact_4.json','w'),ensure_ascii=False,indent=1)
a=json.load(open('chunk_exact_4.json')); b=json.load(open('en_exact_4.json'))
miss=[s for s in a if s not in b or not b[s].strip()]
print(f"{len(a)} in, {len(b)} out, missing {len(miss)}")
