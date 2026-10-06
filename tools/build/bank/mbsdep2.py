import re,sys
for f in sys.argv[1:]:
    t=re.sub(r'\s+',' ',open(f,errors='ignore').read())
    print('=====',f,t[:72])
    seen=set()
    for pat in [r'trung bình lãi suất (huy động )?(kỳ hạn )?(12 tháng|12T) của (các NHTM|nhóm NHTM( tư nhân)?)',r'tỷ giá (liên NH|liên ngân hàng)( kết tháng| đến cuối T\d+| tăng| giảm| ở mức)',r'tỷ giá trung tâm (và liên NH )?(niêm yết|giảm|tăng|đạt|đến)']:
        for m in re.finditer(pat,t):
            s=t[m.start():m.start()+230]
            if s[:60] not in seen:
                seen.add(s[:60]); print(' -',s)
