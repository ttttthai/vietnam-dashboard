import json, os
D=os.path.dirname(os.path.abspath(__file__))
c=json.load(open(f"{D}/countries.json"))[1]
iso3={x['id'] for x in c if x['region']['id']!='NA'}
print("countries", len(iso3))
inds="SP.POP.TOTL AG.SRF.TOTL.K2 AG.LND.TOTL.K2 EN.POP.DNST SP.URB.TOTL.IN.ZS SP.DYN.TFRT.IN SP.DYN.LE00.IN SP.DYN.CBRT.IN SP.DYN.CDRT.IN SP.POP.BRTH.MF SP.POP.65UP.TO.ZS SP.POP.DPND".split()
res={}
for i in inds:
    rows=json.load(open(f"{D}/{i}.json"))[1]
    data={}
    for r in rows:
        if r['countryiso3code'] in iso3 and r['value'] is not None:
            data.setdefault(int(r['date']),{})[r['countryiso3code']]=r['value']
    cov={y:len(v) for y,v in sorted(data.items())}
    vn={y:data[y].get('VNM') for y in sorted(data)}
    print(i, cov, "VN:", {y:v for y,v in vn.items() if v is not None and y>=2019})
    res[i]=data
json.dump({i:{str(y):v for y,v in d.items()} for i,d in res.items()}, open(f"{D}/clean.json","w"))
