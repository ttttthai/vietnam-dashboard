import re,sys
for f in sys.argv[1:]:
    t=re.sub(r'\s+',' ',open(f,errors='ignore').read())
    i=t.find('Ngày báo cáo'); print('=====',f,'|',t[:120],'|',t[i:i+25] if i>=0 else '')
    for m in re.finditer(r'\d{1,2}/\d{1,2}/20\d\d (\d+,\d+ ){3}\d+,\d+',t): print('TAB',m.group(0))
    seen=set()
    for m in re.finditer(r'(bình quân|trung bình)',t):
        s=t[max(0,m.start()-220):m.end()+220]
        if ('qua đêm' in s or 'LSLNH' in s) and s[:80] not in seen:
            seen.add(s[:80]); print('AVG>>',s)
    for m in re.finditer(r'(hút ròng|bơm ròng)[^.]{0,40}',t):
        s=t[max(0,m.start()-250):m.end()+100]
        if 'tháng' in s and s[:60] not in seen:
            seen.add(s[:60]); print('OMO>>',s)
