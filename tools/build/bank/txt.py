import re,sys,html
h=open(sys.argv[1],encoding='utf8',errors='ignore').read()
h=re.sub(r'(?s)<(script|style)[^>]*>.*?</\1>',' ',h)
h=re.sub(r'(?s)<!--.*?-->',' ',h)
h=re.sub(r'</(td|th)>',' | ',h); h=re.sub(r'</(tr|p|div|h\d|li)>','\n',h)
t=html.unescape(re.sub(r'<[^>]+>',' ',h))
t=re.sub(r'[ \t\xa0]+',' ',t); t=re.sub(r'\n\s*\n+','\n',t)
print(t)
