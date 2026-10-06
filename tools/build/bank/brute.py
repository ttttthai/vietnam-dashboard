import datetime, subprocess, time
out=open('mbs_found.txt','a')
d=datetime.date(2024,11,1)
urls=[]
while d<=datetime.date(2026,10,5):
    if d.day<=22 and d.weekday()<5:
        for suf in ['','-1']:
            urls.append(f"https://www.mbs.com.vn/files/uploads/{d.year}/{d.month:02d}/BC_TTTiente_{d:%Y%m%d}{suf}.pdf")
    d+=datetime.timedelta(days=1)
print(len(urls),flush=True)
i=0
while i<len(urls):
    u=urls[i]
    c=subprocess.run(['curl','-s','-o','/dev/null','-m','20','-A','Mozilla/5.0','-w','%{http_code}','-I',u],capture_output=True,text=True).stdout
    if c=='429':
        print('429 at',i,flush=True); time.sleep(40); continue
    if c in('200','206'):
        out.write(u+'\n'); out.flush(); print('FOUND',u,flush=True)
    i+=1
    time.sleep(0.8)
print('DONE',flush=True)
