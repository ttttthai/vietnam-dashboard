import json
c=json.load(open('wb_countries.json'))[1]
countries={x['id'] for x in c if x['region']['id']!='NA'}
print('countries',len(countries))
out={}
for key,i in [('gdp','NY.GDP.MKTP.CD'),('gdp_pc','NY.GDP.PCAP.CD'),('gdp_pc_ppp','NY.GDP.PCAP.PP.CD'),('growth','NY.GDP.MKTP.KD.ZG')]:
    d=json.load(open('wb_'+i+'.json'))[1]
    by={}
    for r in d:
        if r['countryiso3code'] in countries and r['value'] is not None:
            by.setdefault(int(r['date']),{})[r['countryiso3code']]=r['value']
    vyears=sorted(y for y in by if 'VNM' in by[y])
    print(key, 'VNM years', vyears[-6:], {y:len(by[y]) for y in vyears[-6:]})
    res={}
    latest=vyears[-1]
    for tag,y in [('now',latest),('prev',latest-5)]:
        vals=by[y]; v=vals['VNM']
        rank=1+sum(1 for k,x in vals.items() if x>v)
        res[tag]={'year':y,'value':v,'rank':rank,'of':len(vals)}
    out[key]=res
    print(key,res)
json.dump(out,open('wb_rank.json','w'),indent=1)
