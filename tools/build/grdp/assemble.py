import json, glob, os, unicodedata
N=lambda s: unicodedata.normalize('NFC',s).replace('TP. ','TP ').replace('Thành phố ','').replace('Tỉnh ','').strip()
base=json.load(open('base.json')); wb=json.load(open('wb_rank.json')); imf=json.load(open('imf_rank.json'))
demo=json.load(open('../demo/demo.json'))
mf={p['short_name']:p['merged_from'] for p in demo['provinces']}
st={}
for f in sorted(glob.glob('struct/*.json')):
    for k,v in json.load(open(f)).items():
        k2=N(k)
        if k2 in ('Hồ Chí Minh','TP.Hồ Chí Minh','TP.HCM','TP HCM'): k2='TP Hồ Chí Minh'
        st[k2]=v
PX_NEW="https://pxweb.nso.gov.vn/pxweb/vi/PLV03T%c3%a0i%20kho%e1%ba%a3n%20qu%e1%bb%91c%20gia/PLV03T%c3%a0i%20kho%e1%ba%a3n%20qu%e1%bb%91c%20gia/"
PX_POP="https://pxweb.nso.gov.vn/pxweb/vi/PLV02D%c3%a2n%20s%e1%bb%91%20v%c3%a0%20lao%20%c4%91%e1%bb%99ng/PLV02D%c3%a2n%20s%e1%bb%91%20v%c3%a0%20lao%20%c4%91%e1%bb%99ng/PL.V02.02-04.px/"
PX_NAT="https://pxweb.nso.gov.vn/pxweb/vi/T%c3%a0i%20kho%e1%ba%a3n%20qu%e1%bb%91c%20gia/T%c3%a0i%20kho%e1%ba%a3n%20qu%e1%bb%91c%20gia/V03.01.px/"
NSO_PR="https://www.nso.gov.vn/tin-tuc-thong-ke/2026/01/thong-cao-bao-chi-ve-ket-qua-tang-truong-tong-san-pham-tren-dia-ban-grdp-cua-34-tinh-thanh-pho-nam-2025/"
provs={}
missing=[]
for s,o in base['provinces'].items():
    p={'grdp_bn':o['grdp_bn'],'growth':o['growth'],'grdp_pc_mvnd':o['grdp_pc_mvnd'],'grdp_pc_usd':o['grdp_pc_usd'],
       'pop_avg_thousand':o['pop_avg_thousand'],'growth_series':o['growth_series'],'grdp_pc_mvnd_series':o['grdp_pc_mvnd_series'],
       'merged_from':mf[s]}
    x=st.get(s)
    if x and any(x.get(k) is not None for k in ('agri','ind','svc','tax')):
        p['structure_2025']={k:x.get(k) for k in ('agri','ind','svc','tax')}
        p['structure_source']=x.get('url'); p['structure_note']=x.get('note')
    else:
        p['structure_2025']=None; missing.append(s)
        if x: p['structure_note']=x.get('note')
    if x:
        p['provincial_reported_2025']={k:x.get(k) for k in ('grdp_bn_2025','grdp_pc_mvnd_2025','grdp_pc_usd_2025')}
    p['basis']=("NSO series compiled for the 34 new units (back-cast 2020-2023 official; 2024 'Sơ bộ' preliminary; 2025 'Ước tính' estimate). "
        "growth = NSO PL.V03.01 index (prev yr=100) minus 100, constant 2010 prices. grdp_pc_mvnd = NSO PL.V03.02. "
        "grdp_bn DERIVED = NSO per-capita (mn VND) x NSO average population (thousand, PL.V02.02-04); not a directly published NSO level. "
        "grdp_pc_usd DERIVED = grdp_pc_mvnd / NSO-implied national VND-USD rate (national GDP pc VND / national GDP pc USD in NSO V03.01). "
        "No aggregation from 63 old provinces was needed.")
    p['source']={'growth':PX_NEW+'PL.V03.01.px/','grdp_pc':PX_NEW+'PL.V03.02.px/','population':PX_POP,'press_release_2025':NSO_PR}
    provs[s]=p
def wbfmt(k):
    r=wb[k]; return r
world={k:{'now':wb[k]['now'],'prev':wb[k]['prev']} for k in wb}
# 2024 alt (fuller coverage)
import json as J
c=J.load(open('wb_countries.json'))[1]; countries={x['id'] for x in c if x['region']['id']!='NA'}
for k,i in [('gdp','NY.GDP.MKTP.CD'),('gdp_pc','NY.GDP.PCAP.CD'),('gdp_pc_ppp','NY.GDP.PCAP.PP.CD'),('growth','NY.GDP.MKTP.KD.ZG')]:
    d=J.load(open('wb_'+i+'.json'))[1]; by={}
    for r in d:
        if r['countryiso3code'] in countries and r['value'] is not None: by.setdefault(int(r['date']),{})[r['countryiso3code']]=r['value']
    for tag,y in [('alt_2024',2024),('alt_2019',2019)]:
        vals=by[y]; v=vals['VNM']; world[k][tag]={'year':y,'value':v,'rank':1+sum(1 for x in vals.values() if x>v),'of':len(vals)}
    world[k]['units']={'gdp':'current US$','gdp_pc':'current US$','gdp_pc_ppp':'current international $ (PPP)','growth':'annual %, constant prices'}[k]
world['note']=("World Bank WDI (lastupdated 2026-07-13), countries only (217 economies with region.id != 'NA'), rank 1 = highest among economies with data that year. "
  "2025 coverage is incomplete (186 economies for GDP; e.g. UAE, Monaco, Liechtenstein, Bermuda, Cayman missing), which flatters 2025 ranks, especially per capita; alt_2024/alt_2019 give a fuller-coverage comparison.")
world['imf_weo_apr2026']={'vintage':'WEO April 2026 (IMF DataMapper API, last-modified 2026-04-08); Oct-2026 WEO not yet released as of 2026-10-05',
  'units':{'gdp':'bn US$','gdp_pc':'US$','gdp_pc_ppp':'intl $','growth':'%'},'ranks':imf,
  'note':'Ranked among IMF DataMapper country list (excludes groups); 2025 = IMF estimate, 2026 = projection.'}
nat={'gdp_bn_vnd':{'2020':8044385.73,'2024':11510328.92,'2025':12847571.22},'gdp_pc_mvnd':{'2020':82.44,'2024':113.58,'2025':125.53},
     'gdp_pc_usd':{'2020':3551.92,'2024':4700,'2025':5025.85},'growth':{y:base['national']['growth'][y] for y in ('2020','2024','2025')},
     'implied_vnd_per_usd':{y:round(v,1) for y,v in base['rate'].items()},'source':PX_NAT,
     'sum_of_34_grdp_bn':{y:sum(provs[s]['grdp_bn'][y] for s in provs) for y in ('2020','2024','2025')},
     'note':'Sum of derived 34-unit GRDP is ~0.2-1.0% below national GDP (rounding of published per-capita/population and GDP items not allocated to provinces).'}
out={'provinces':provs,'national':nat,'world_rank':world,
 'sources':{'nso_grdp_growth_34':PX_NEW+'PL.V03.01.px/','nso_grdp_pc_34':PX_NEW+'PL.V03.02.px/','nso_pop_34':PX_POP,'nso_national_accounts':PX_NAT,
  'nso_press_release_grdp_2025':NSO_PR,'wb_api':'https://api.worldbank.org/v2/country/all/indicator/{NY.GDP.MKTP.CD|NY.GDP.PCAP.CD|NY.GDP.PCAP.PP.CD|NY.GDP.MKTP.KD.ZG}?format=json&per_page=20000&date=2015:2026',
  'wb_countries':'https://api.worldbank.org/v2/country?format=json&per_page=400','imf_datamapper':'https://www.imf.org/external/datamapper/api/v1/{NGDPD|NGDPDPC|PPPPC|NGDP_RPCH}',
  'merger':'NQ 202/2025/QH15 (merged_from from demo.json)'},
 'meta':{'generated':'2026-10-05','structure_missing':missing,
  'crosscheck':'Derived 2025 GRDP vs press-quoted NSO levels: TP HCM 2,972,536 vs 2,972,939 bn; Hà Nội 1,588,060 vs 1,587,379 bn (<0.05% diff).'}}
json.dump(out,open('grdp.json','w'),ensure_ascii=False,indent=1)
print('structure missing:',missing)
