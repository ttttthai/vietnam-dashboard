import re,html,subprocess,urllib.parse,json,sys
UA="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0 Safari/537.36"
P="com_liferay_asset_publisher_web_portlet_AssetPublisherPortlet_INSTANCE_whgo"
items=[]
for cur in range(1,21):
    u=f"https://sbv.gov.vn/vi/thong-ke-mot-so-chi-tieu-co-ban?p_p_id={P}&p_p_lifecycle=0&p_p_state=normal&p_p_mode=view&_{P}_redirect=%2Fvi%2Fthong-ke-mot-so-chi-tieu-co-ban&_{P}_delta=10&p_r_p_resetCur=false&_{P}_cur={cur}"
    out=f'ctcbp/p{cur}.html'
    subprocess.run(['curl','-sL','-m','60','--compressed','-H','User-Agent: '+UA,'-H','Accept-Language: vi-VN',u,'-o',out])
    h=open(out,encoding='utf8',errors='ignore').read()
    for m in re.finditer(r'<a href="(https://sbv\.gov\.vn/vi/thong-ke-mot-so-chi-tieu-co-ban/-/asset_publisher/whgo/content/[^"]+)"[^>]*>\s*(?:<[^>]+>\s*)*([^<]{5,200})',h):
        items.append((html.unescape(m.group(2)).strip(),html.unescape(m.group(1))))
    print(cur,len(h),len(items))
json.dump(items,open('ctcb_items.json','w'),ensure_ascii=False,indent=0)
