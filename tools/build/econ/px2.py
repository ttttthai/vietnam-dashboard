import sys, re, html, urllib.parse, http.cookiejar, urllib.request
# usage: px2.py url out [minyear]
def fetch(url, out, minyear=2014):
    cj = http.cookiejar.CookieJar()
    op = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
    op.addheaders = [('User-Agent', 'Mozilla/5.0')]
    t = op.open(url, timeout=90).read().decode('utf-8', 'ignore')
    form = []
    for m in re.finditer(r'<input[^>]*>', t):
        s = m.group(0)
        if 'type="hidden"' in s:
            n = re.search(r'name="([^"]*)"', s); v = re.search(r'value="([^"]*)"', s)
            if n: form.append((html.unescape(n.group(1)), html.unescape(v.group(1)) if v else ''))
    for m in re.finditer(r'<select[^>]*name="([^"]*ValuesListBox)"[^>]*>(.*?)</select>', t, re.S):
        name = html.unescape(m.group(1))
        pairs=[(html.unescape(o.group(1)),html.unescape(o.group(2))) for o in re.finditer(r'<option[^>]*value="([^"]*)"[^>]*>(.*?)</option>', m.group(2), re.S)]
        def yr(o):
            mm=re.search(r'(19|20)\d\d',o); return int(mm.group(0)) if mm else None
        if pairs and all(yr(tx) for v,tx in pairs) and len(pairs)>12:
            pairs=[(v,tx) for v,tx in pairs if yr(tx)>=minyear]
        for v,tx in pairs: form.append((name,v))
    ps = re.search(r'<select[^>]*name="([^"]*(?:Presentation|Output)[^"]*)"', t)
    if ps: form.append((html.unescape(ps.group(1)), 'tableViewLayout1'))
    form.append(('ctl00$ContentPlaceHolderMain$VariableSelector1$VariableSelector1$ButtonViewTable', 'Tiếp tục'))
    r = op.open(url, data=urllib.parse.urlencode(form).encode(), timeout=120)
    t2 = r.read().decode('utf-8', 'ignore'); open(out, 'w').write(t2); print(r.geturl(), len(t2))
fetch(sys.argv[1], sys.argv[2], int(sys.argv[3]) if len(sys.argv)>3 else 2014)
