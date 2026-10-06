import re,sys
for f in sys.argv[1:]:
    t=re.sub(r'\s+',' ',open(f,errors='ignore').read())
    print('=====',f,'|',t[:160])
    sents=re.split(r'(?<=[.;•▪])\s',t)
    out=[]
    for s in sents:
        if re.search(r'(qua đêm|LSLNH|ON)',s) and re.search(r'(bình quân|trung bình)',s) and re.search(r'\d',s) and len(s)<700:
            out.append('AVG: '+s)
        elif re.search(r'(hút ròng|bơm ròng)',s) and re.search(r'tháng',s) and len(s)<500:
            out.append('OMO: '+s)
    for m in re.finditer(r'Qua đêm 1 tuần[^A-Za-z]{0,300}',t):
        out.append('TAB: '+m.group(0)[:250])
    seen=set()
    for o in out:
        if o[:120] in seen: continue
        seen.add(o[:120]); print(o[:600])
