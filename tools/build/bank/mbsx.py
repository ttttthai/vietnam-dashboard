import re,sys
for f in sys.argv[1:]:
    t=re.sub(r'\s+',' ',open(f,errors='ignore').read())
    print('=====',f, t[:420])
    seen=-2000
    for m in re.finditer(r'qua đêm|hút ròng|bơm ròng|kỳ hạn 12 tháng|tỷ giá liên ngân hàng|tỷ giá trung tâm',t):
        if m.start()-seen<600: continue
        seen=m.start()
        print('>>',t[max(0,m.start()-300):m.end()+350])
