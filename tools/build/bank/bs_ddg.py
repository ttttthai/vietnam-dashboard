import sys,subprocess,re,html,urllib.parse
q=sys.argv[1]
UA="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0 Safari/537.36"
h=subprocess.run(['curl','-sL','-m','40','-A',UA,'https://html.duckduckgo.com/html/?q='+urllib.parse.quote(q)],capture_output=True,text=True).stdout
for m in re.finditer(r'class="result__a" href="([^"]+)"[^>]*>(.*?)</a>.*?class="result__snippet"[^>]*>(.*?)</a>',h,re.S):
    u=m.group(1)
    if 'uddg=' in u: u=urllib.parse.unquote(re.search(r'uddg=([^&]+)',u).group(1))
    print('*',html.unescape(re.sub('<[^>]+>','',m.group(2))).strip(),'|',u); print('   ',html.unescape(re.sub('<[^>]+>','',m.group(3))).strip()[:300])
if not h or 'result__a' not in h: print('NO RESULTS', len(h), h[:300])
