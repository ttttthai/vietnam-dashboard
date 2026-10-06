import sys,urllib.parse,subprocess,re,html,time
for q in sys.argv[1:]:
  s=subprocess.run(['curl','-s','-m','25','-A','Mozilla/5.0','https://html.duckduckgo.com/html/?q='+urllib.parse.quote(q)],capture_output=True).stdout.decode('utf-8','ignore')
  print('##',q)
  seen=set()
  for m in re.finditer(r'class="result__a" href="([^"]+)"[^>]*>(.*?)</a>',s,re.S):
    u=m.group(1); mm=re.search(r'uddg=([^&]+)',u)
    if mm: u=urllib.parse.unquote(mm.group(1))
    if u in seen: continue
    seen.add(u); print(' ',u,'|',html.unescape(re.sub('<[^>]+>','',m.group(2)))[:90])
  time.sleep(8)
