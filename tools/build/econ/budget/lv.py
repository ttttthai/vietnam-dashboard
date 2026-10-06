import re,html,sys,subprocess
url,out=sys.argv[1],sys.argv[2]
subprocess.run(['curl','-sL','-m','60','-A','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36',url,'-o',out+'.html'])
s=open(out+'.html',encoding='utf-8',errors='ignore').read()
t=re.sub(r'<script.*?</script>|<style.*?</style>','',s,flags=re.S)
t=re.sub(r'</(td|th)>',' | ',t); t=re.sub(r'</(tr|p|div|li|h\d)>|<br\s*/?>','\n',t)
t=html.unescape(re.sub(r'<[^>]+>','',t)); t=re.sub(r'[ \t]+',' ',t); t=re.sub(r'\n\s*\n+','\n',t)
open(out+'.txt','w').write(t)
print(len(t))
for m in re.finditer(r'(Phụ lục [IVX]+[^\n]{0,80}|dự trữ quốc gia[^\n]{0,200})',t): print(m.group(0)[:200].replace('\n',' '))
print([u for u in re.findall(r'href="([^"]+\.(?:pdf|docx?|xlsx?))"',s)])
