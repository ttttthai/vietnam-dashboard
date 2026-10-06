import sys,re,subprocess,html,os
UA="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/126 Safari/537.36"
kw=sys.argv[2] if len(sys.argv)>2 else r'qua đêm'
rid=sys.argv[1]
h=subprocess.run(['curl','-sL','-m','30','-A',UA,f'https://finance.vietstock.vn/bao-cao-phan-tich/{rid}/x.htm'],capture_output=True).stdout.decode('utf-8','ignore')
title=re.search(r'<title>(.*?)</title>',h,re.S)
print('TITLE',rid,title.group(1).strip() if title else None)
pdfs=sorted(set(re.findall(r'(?:https?:)?//static1\.vietstock\.vn/edocs/[^"\'<> ]+\.pdf',h)))
print('PDFS',pdfs)
if not pdfs: sys.exit()
u=pdfs[0]; u='https:'+u if u.startswith('//') else u
fn=f'vs{rid}.pdf'
if not os.path.exists(fn): subprocess.run(['curl','-sL','-m','60','-A',UA,u,'-o',fn])
subprocess.run(['pdftotext',fn,f'vs{rid}.txt'])
t=re.sub(r'\s+',' ',open(f'vs{rid}.txt',errors='ignore').read())
print('HEAD',t[:300])
last=-1000
for m in re.finditer(kw,t):
    if m.start()-last<500: continue
    last=m.start()
    print('>>',t[max(0,m.start()-350):m.end()+450])
