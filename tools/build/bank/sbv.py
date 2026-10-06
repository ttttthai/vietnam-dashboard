import re,sys,json,subprocess,urllib.parse,os
UA="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0 Safari/537.36"
def get(url,out):
    subprocess.run(['curl','-sL','-m','60','--compressed','-H','User-Agent: '+UA,'-H','Accept: text/html,application/json,*/*','-H','Accept-Language: vi-VN,vi;q=0.9','-H','Referer: https://sbv.gov.vn/',url,'-o',out])
    return open(out,encoding='utf8',errors='ignore').read()
def page(slug,name):
    url='https://sbv.gov.vn/vi/'+urllib.parse.quote(slug)
    h=get(url,name+'.html')
    cfgs=re.findall(r"scopeKey:\s*'(\d+)',\s*structureId:\s*'(\d+)'",h)
    cfgs+=re.findall(r"scopeKey=(\d+)&(?:amp;)?contentStructureId=(\d+)",h)
    labels=re.findall(r'const labels = (\[.*?\]);',h)
    return url,h,list(dict.fromkeys(cfgs)),labels
def api(scope,struct,name):
    out=[];p=1
    while True:
        u=f'https://sbv.gov.vn/o/article/v1.0/articles?scopeKey={scope}&contentStructureId={struct}&pageSize=200&page={p}'
        t=get(u,name+f'_api{p}.json')
        try: d=json.loads(t)
        except Exception as e: print('ERR',t[:200]);break
        out+=d.get('articles',[])
        if p>=d.get('lastPage',1): break
        p+=1
    return out
if __name__=='__main__':
    slug,name=sys.argv[1],sys.argv[2]
    url,h,cfgs,labels=page(slug,name)
    print(url,len(h),cfgs,labels[:3])
    allrec={}
    for s,st in cfgs:
        arts=api(s,st,name+'_'+st)
        uniq={}
        for a in arts: uniq[a['articleId']]=a
        print('struct',st,len(uniq))
        allrec[st]=[{'title':a.get('title'),'pub':a.get('datePublished','')[:10],'fields':a.get('fields')} for a in uniq.values()]
    json.dump(allrec,open(name+'_all.json','w'),ensure_ascii=False,indent=1)
