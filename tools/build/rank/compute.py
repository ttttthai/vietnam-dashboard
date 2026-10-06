import json, os
D=os.path.dirname(os.path.abspath(__file__))
data=json.load(open(f"{D}/clean.json"))
spec=[("pop","SP.POP.TOTL","Population, total",2025),
("area","AG.SRF.TOTL.K2","Surface area (sq. km)",2023),
("land","AG.LND.TOTL.K2","Land area (sq. km)",2023),
("density","EN.POP.DNST","Population density (people per sq. km of land area)",2023),
("urban","SP.URB.TOTL.IN.ZS","Urban population (% of total population)",2025),
("tfr","SP.DYN.TFRT.IN","Fertility rate, total (births per woman)",2024),
("life_exp","SP.DYN.LE00.IN","Life expectancy at birth, total (years)",2024),
("cbr","SP.DYN.CBRT.IN","Birth rate, crude (per 1,000 people)",2024),
("cdr","SP.DYN.CDRT.IN","Death rate, crude (per 1,000 people)",2024),
("srb","SP.POP.BRTH.MF","Sex ratio at birth (male births per female births)",2024),
("age65","SP.POP.65UP.TO.ZS","Population ages 65 and above (% of total population)",2025),
("dependency","SP.POP.DPND","Age dependency ratio (% of working-age population)",2025)]
def rank(vals, v):  # desc, competition ranking
    return 1+sum(1 for x in vals if x>v)
out={}
for key,code,label,yr in spec:
    py=yr-5
    a=data[code][str(yr)]; b=data[code][str(py)]
    common=set(a)&set(b)
    def blk(d,y):
        v=d['VNM']
        return {"year":y,"value":round(v,4) if v<1e5 else v,"rank":rank([d[k] for k in common],v),"of":len(common),
                "rank_all":rank(list(d.values()),v),"of_all":len(d)}
    out[key]={"indicator":code,"label":label,"direction":"desc","now":blk(a,yr),"prev":blk(b,py),
              "source":f"https://api.worldbank.org/v2/country/all/indicator/{code}?format=json&per_page=20000&date={py}:{yr}"}
    print(key,out[key]['now'],out[key]['prev'])
json.dump(out,open(f"{D}/wb_ranks.json","w"),indent=1)
