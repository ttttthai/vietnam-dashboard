import json, openpyxl, unicodedata
D='../demo/'
N=lambda s: unicodedata.normalize('NFC',s).replace('TP. ','TP ').replace('Ð','Đ').strip()
demo=json.load(open(D+'demo.json'))
shorts=[p['short_name'] for p in demo['provinces']]
def js(f):
    ds=json.load(open(f)); ids=ds['dimension']['id']; sz=ds['dimension']['size']
    provs=[N(x) for x in ds['dimension'][ids[0]]['category']['label'].values()]
    yrs=[x[-4:] for x in ds['dimension'][ids[1]]['category']['label'].values()]
    v=ds['value']; return {p:{y:v[i*sz[1]+j] for j,y in enumerate(yrs)} for i,p in enumerate(provs)}
gr=js('n_PL.V03.01.json'); pc=js('n_PL.V03.02.json')
ws=openpyxl.load_workbook(D+'new_PL.V02.02-04.px.xlsx').active
rows=list(ws.iter_rows(values_only=True)); hdr=rows[3]
pop={}
for r in rows[4:]:
    if r[0]: pop[N(r[0])]={str(hdr[j])[-4:]:r[j] for j in range(1,7)}
usd_nat={'2020':3551.92,'2024':4700.0,'2025':5025.85}
vnd_nat={'2020':82.44,'2024':113.58,'2025':125.53}
rate={y:vnd_nat[y]*1e6/usd_nat[y] for y in usd_nat}
print('implied VND/USD',rate)
out={}
for s in shorts:
    k=s
    assert k in gr and k in pc and k in pop, k
    o={'grdp_bn':{},'growth':{},'grdp_pc_mvnd':{},'grdp_pc_usd':{},'pop_avg_thousand':{}}
    for y in ['2020','2024','2025']:
        o['growth'][y]=round(gr[k][y]-100,2)
        o['grdp_pc_mvnd'][y]=round(pc[k][y],2)
        o['pop_avg_thousand'][y]=pop[k][y]
        o['grdp_bn'][y]=round(pc[k][y]*pop[k][y])
        o['grdp_pc_usd'][y]=round(pc[k][y]*1e6/rate[y])
    o['growth_series']={y:round(gr[k][y]-100,2) for y in gr[k]}
    o['grdp_pc_mvnd_series']={y:round(pc[k][y],2) for y in pc[k]}
    out[s]=o
# national check
for y in ['2020','2024','2025']:
    tot=sum(out[s]['grdp_bn'][y] for s in shorts)
    print(y,'sum34',tot,'national pc*pop',round(pc['Cả nước'][y]*pop['CẢ NƯỚC'][y]), 'growth nat',round(gr['Cả nước'][y]-100,2))
json.dump({'provinces':out,'national':{'growth':{y:round(gr['Cả nước'][y]-100,2) for y in gr['Cả nước']},'pc':{y:round(pc['Cả nước'][y],2) for y in pc['Cả nước']}},'rate':rate},open('base.json','w'),ensure_ascii=False,indent=1)
for s in shorts: print(s, out[s]['grdp_bn'], out[s]['growth'], out[s]['grdp_pc_usd'])
