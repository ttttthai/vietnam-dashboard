import re,html,sys,subprocess
host=sys.argv[1]
seen=set()
for path in ['an-pham-thong-ke','nien-giam-thong-ke']:
  for pg in range(1,30):
    url=f'https://{host}/{path}' + (f'?page={pg}' if pg>1 else '')
    s=subprocess.run(['curl','-s','-m','20',url],capture_output=True).stdout.decode('utf-8','ignore')
    new=0
    for m in re.finditer(r'<a[^>]+href="([^"]+)"[^>]*>(.*?)</a>',s,re.S):
      u=m.group(1);t=html.unescape(re.sub('<[^>]+>','',m.group(2))).strip()
      if ('storage' in u or re.search(r'/(an-pham-thong-ke|nien-giam-thong-ke)/\S',u)) and (u,t) not in seen:
        seen.add((u,t));new+=1;print(u,'|',t[:120])
    if new==0: break
