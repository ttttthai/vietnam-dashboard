import re,sys
for f in sys.argv[1:]:
    t=re.sub(r'\s+',' ',open(f,errors='ignore').read())
    print('=====',f,t[:75])
    seen=set()
    for pat in [r'(trung bình lãi suất|lãi suất huy động)[^.]{0,60}(12 tháng|12T)[^.]{0,160}',r'(tỷ giá liên N[^ ]*|tỷ giá trung tâm)[^.]{0,160}']:
        for m in re.finditer(pat,t):
            s=m.group(0)
            if re.search(r'\d',s) and s[:50] not in seen:
                seen.add(s[:50]); print(' -',s[:260])
