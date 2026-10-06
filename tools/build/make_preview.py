"""Build a self-contained preview of the dashboard that works without the server or the network.

Snapshots the read-only API responses from a running server, embeds them behind a fetch() shim, and inlines
the CDN libraries (d3, topojson) and the Vietnam map, so the file opens in a file preview (no http server).

Usage:  python3 tools/build/make_preview.py [server_url] [--libs DIR] [--out PATH]
  server_url  default http://127.0.0.1:8001
  --libs DIR  folder holding d3.min.js, topojson.min.js and vn-all.topo.json (optional; without it the
              page keeps its CDN script tags and the map loads only if the CDN is reachable)
  --out PATH  default vietnam_dashboard_preview.html in the repo root (git-ignored)
  --artifact PATH  also write a variant for publishing as a claude.ai page: <title> first, no outer
              document tags (the host adds them), language switch translates in place (?lang= is dropped)
"""
import json, os, re, sys, urllib.request

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
args = sys.argv[1:]
base = next((a for a in args if a.startswith('http')), 'http://127.0.0.1:8001').rstrip('/')
libs = args[args.index('--libs') + 1] if '--libs' in args else None
out = args[args.index('--out') + 1] if '--out' in args else os.path.join(ROOT, 'vietnam_dashboard_preview.html')

# read-only endpoints the page calls on load or on tab open (the refresh/analyze/screen calls stay live-only)
ENDPOINTS = ['/api/strategy', '/api/invest/research', '/api/invest/context', '/api/snapshot', '/api/i18n/en',
             '/api/logs?limit=200'] + [f'/api/banks{p}?period={q}' for q in ('year', 'quarter') for p in ('', '/breakdown', '/statements')]
data = {}
for ep in ENDPOINTS:
    try:
        with urllib.request.urlopen(base + ep, timeout=60) as r:
            if r.status == 200: data[ep] = json.loads(r.read().decode('utf-8'))
    except Exception as e:
        print('skip', ep, '-', e)
print('snapshotted', len(data), 'endpoints')

html = open(os.path.join(ROOT, 'vietnam_dashboard.html'), encoding='utf-8').read()
esc = lambda s: s.replace('</', '<\\/')
topo = None
if libs:
    for name, tag in [('d3.min.js', 'd3/7.8.5/d3.min.js'), ('topojson.min.js', 'topojson/3.0.2/topojson.min.js')]:
        i = html.find(tag)
        if i < 0: continue
        a = html.rfind('<script', 0, i); b = html.index('</script>', i) + len('</script>')
        html = html[:a] + '<script>' + esc(open(os.path.join(libs, name), encoding='utf-8').read()) + '</script>' + html[b:]
    tp = os.path.join(libs, 'vn-all.topo.json')
    if os.path.exists(tp): topo = json.load(open(tp, encoding='utf-8'))

shim = ('<script>/* preview build: API responses snapshotted ' + base + ' — no server needed */(function(){'
        'var D=' + esc(json.dumps(data, ensure_ascii=False, separators=(',', ':'))) + ';'
        'var T=' + (esc(json.dumps(topo, separators=(',', ':'))) if topo else 'null') + ';'
        'var of=window.fetch?window.fetch.bind(window):null;'
        'function ok(o){return Promise.resolve(new Response(JSON.stringify(o),{status:200,headers:{"Content-Type":"application/json"}}));}'
        'window.fetch=function(u,o){var s=String(u&&u.url||u),i=s.indexOf("/api/"),k=i>=0?s.slice(i):null;'
        'if(k&&D[k]!==undefined)return ok(D[k]);if(k&&D[k.split("?")[0]]!==undefined)return ok(D[k.split("?")[0]]);'
        'if(T&&s.indexOf("vn-all.topo.json")>=0)return ok(T);'
        'if(k)return Promise.resolve(new Response(JSON.stringify({error:"preview build: live server not available"}),{status:503,headers:{"Content-Type":"application/json"}}));'
        'return of?of(u,o):Promise.reject(new Error("no fetch"));};'
        'window.PREVIEW_BUILD=true;})();</script>')
i = html.index('<head>') + len('<head>')
html = html[:i] + shim + html[i:]
open(out, 'w', encoding='utf-8').write(html)
print('wrote', out, round(len(html) / 1024 / 1024, 2), 'MB')

if '--artifact' in args:
    a_out = args[args.index('--artifact') + 1]
    t = re.sub(r'<title>[^<]*</title>', '', html, count=1)
    for tag in ('<!DOCTYPE html>', '<html lang="vi">', '<head>', '<body>'): t = t.replace(tag, '', 1)
    for tag in ('</head>', '</body>', '</html>'):
        k = t.rfind(tag); t = t[:k] + t[k + len(tag):] if k >= 0 else t
    t = t.replace("if (/^(https?|file):$/.test(location.protocol)) {", "if (false && /^(https?|file):$/.test(location.protocol)) {   /* published copy: ?lang= is dropped, translate in place */", 1)
    t = '<title>Vietnam Dashboard</title>\n<style>html,body{background:#faf8f3}</style>\n' + t.lstrip()
    open(a_out, 'w', encoding='utf-8').write(t)
    print('wrote', a_out)

