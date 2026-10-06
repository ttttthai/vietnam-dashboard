import json,urllib.request,sys,concurrent.futures as cf
codes=sys.argv[1:]
def f(c):
    u=f"https://api.worldbank.org/v2/country/VNM/indicator/{c}?format=json&per_page=100&date=2010:2026"
    try:
        d=json.load(urllib.request.urlopen(urllib.request.Request(u,headers={'User-Agent':'Mozilla/5.0'}),timeout=60))
    except Exception as e: return c,None,str(e)
    if len(d)<2 or not d[1]: return c,None,str(d[0])[:150]
    json.dump(d,open(f'wb/{c}.json','w'))
    v={r['date']:r['value'] for r in d[1]}
    return c,d[1][0]['indicator']['value'],{k:v[k] for k in sorted(v) if v[k] is not None and k>='2014'}
with cf.ThreadPoolExecutor(8) as ex:
    for c,name,v in ex.map(f,codes): print(c,'|',name,'|',v)
