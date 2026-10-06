import urllib.request, json, os
D=os.path.dirname(os.path.abspath(__file__))
def get(u):
    with urllib.request.urlopen(u, timeout=120) as r: return json.load(r)
c=get("https://api.worldbank.org/v2/country?format=json&per_page=400")
json.dump(c,open(f"{D}/countries.json","w"))
inds="SP.POP.TOTL AG.SRF.TOTL.K2 AG.LND.TOTL.K2 EN.POP.DNST SP.URB.TOTL.IN.ZS SP.DYN.TFRT.IN SP.DYN.LE00.IN SP.DYN.CBRT.IN SP.DYN.CDRT.IN SP.POP.BRTH.MF SP.POP.65UP.TO.ZS SP.POP.DPND".split()
for i in inds:
    try:
        d=get(f"https://api.worldbank.org/v2/country/all/indicator/{i}?format=json&per_page=20000&date=2014:2025")
        json.dump(d,open(f"{D}/{i}.json","w")); print(i, d[0])
    except Exception as e: print(i,"ERR",e)
