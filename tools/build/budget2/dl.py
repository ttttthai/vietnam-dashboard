import json,subprocess,os,re
def post(svc,payload):
    r=subprocess.run(['curl','-s','-m','60','-X','POST','-H','Content-Type: application/json','-d',json.dumps(payload),'https://ckns.mof.gov.vn/_vti_bin/'+svc],capture_output=True,text=True)
    try:
        d=json.loads(r.stdout)
        while isinstance(d,str) and d: d=json.loads(d)
        return d
    except Exception: return None
base=dict(SiteId='e9e24430-ec5e-4b44-8a1d-a3d06c1e6ed2',WebId='60d972cc-567c-448d-b0a3-81ae171a2fe1',ListId='dde649ff-8645-4e57-b9bc-c3f640ecd468',KeySearch='',DeparmentId=0,DepartmentTypeId=3)
combos=[(4,0),(5,0),(6,13),(6,19),(6,15),(6,17),(6,20),(6,21),(11,0),(7,0),(13,0)]
index=[]
for t,p in combos:
    ys=post('ReportService.svc/GetYearReports',dict(base,PeriodDetailTypeId=p,ReportTypeId=t)) or []
    years=sorted(y['Year'] for y in ys)
    print(t,p,years,flush=True)
    for y in years:
        if y<2017: continue
        r=post('ReportService.svc/GetReportV4',dict(base,Year=y,PeriodDetailTypeId=p,ReportTypeId=t))
        if not r: continue
        d=f'files/t{t}_p{p}_{y}'; os.makedirs(d,exist_ok=True)
        for kind in ['FileBaoCao','FileQuyetDinh','FileThuyetTrinh']:
            for f in r.get(kind) or []:
                fn=re.sub(r'[/\\:]','_',f['FileName'])
                out=os.path.join(d,fn)
                url='https://ckns.mof.gov.vn/_layouts/LacViet.App/Pages/DownloadFilePage.aspx?FileUrl='+f['Url']
                if not os.path.exists(out):
                    subprocess.run(['curl','-s','-L','-m','120','-o',out,url])
                index.append(dict(type=t,period=p,year=y,kind=kind,group=f.get('GroupName'),name=f.get('ReportName'),file=out,url=url,size=os.path.getsize(out) if os.path.exists(out) else 0,pub=r.get('PublishDateText')))
json.dump(index,open('files/index.json','w'),ensure_ascii=False,indent=1)
print(len(index))
