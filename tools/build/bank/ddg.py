import sys,subprocess,urllib.parse,re,html
q=sys.argv[1]
UA="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0 Safari/537.36"
r=subprocess.run(['curl','-s','-m','30','-A',UA,'https://html.duckduckgo.com/html/?q='+urllib.parse.quote(q)],capture_output=True,text=True).stdout
for m in re.finditer(r'class="result__a" href="([^"]+)"[^>]*>(.*?)</a>.*?class="result__snippet"[^>]*>(.*?)</a>',r,re.S):
    u=m.group(1)
    mm=re.search(r'uddg=([^&]+)',u)
    if mm: u=urllib.parse.unquote(mm.group(1))
    t=html.unescape(re.sub('<[^>]+>','',m.group(2))); s=html.unescape(re.sub('<[^>]+>','',m.group(3)))
    print('-',t,'|',u,'\n   ',s[:300])
if 'result__a' not in r: print('NO RESULTS',len(r),r[:300])
