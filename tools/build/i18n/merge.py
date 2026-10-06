import json, re, glob, os
D=os.path.dirname(os.path.abspath(__file__))
exact={}
for f in sorted(glob.glob(os.path.join(D,'en_exact_*.json'))): exact.update(json.load(open(f)))
tpl=json.load(open(os.path.join(D,'en_templates.json')))
ex=json.load(open(os.path.join(D,'en_extra.json'))); exact.update(ex['exact']); tpl.update(ex['templates'])
# Vietnamese number formats → English (5.008 → 5,008 ; 3.410,64 → 3,410.64 ; 9,3 → 9.3) in English values only
def fixnum(v):
    v = re.sub(r'(?<![\d.,])(\d{1,3}(?:\.\d{3})+)(,\d+)?(?![\d.,]*\d)', lambda m: m.group(1).replace('.', ',') + (m.group(2).replace(',', '.') if m.group(2) else ''), v)
    v = re.sub(r'(?<![\d.,])(\d+),(\d{1,2})(?![\d])', r'\1.\2', v)
    return v
exact={k: fixnum(v) for k,v in exact.items()}
tpl={k: fixnum(v) for k,v in tpl.items()}
bad=[k for k,v in tpl.items() if sorted(re.findall(r'\{\d+\}',k))!=sorted(re.findall(r'\{\d+\}',v))]
for k in bad: tpl.pop(k)
json.dump({'lang':'en','exact':exact,'templates':tpl}, open('/Users/hoangthai/CLAUDECODE/VIETNAM DASHBOARD/i18n_en.json','w'), ensure_ascii=False, indent=0)
print('exact',len(exact),'templates',len(tpl),'dropped',len(bad))
for k in ["Lợi nhuận H1/2026 kỷ lục (LNST 5.008 tỷ, +306%) nhờ Nhơn Trạch 3&4 vận hành thương mại từ 01/01/2026 cộng ~2.475 tỷ thu nhập bất thường. Giá hiện tại (~12.700) thấp hơn mọi giá mục tiêu đã công bố (16.000–18.400)."]:
    print(exact.get(k))

# Embed the same dictionary into the dashboard so English works without the server (file://)
H='/Users/hoangthai/CLAUDECODE/VIETNAM DASHBOARD/vietnam_dashboard.html'
h=open(H,encoding='utf-8').read()
blob=json.dumps({'lang':'en','exact':exact,'templates':tpl}, ensure_ascii=False, separators=(',',':')).replace('</','<\\/')
a=h.index('<!--I18N_EN_START-->'); b=h.index('<!--I18N_EN_END-->')+len('<!--I18N_EN_END-->')
h=h[:a]+'<!--I18N_EN_START--><script id="i18n-en" type="application/json">'+blob+'</script><!--I18N_EN_END-->'+h[b:]
open(H,'w',encoding='utf-8').write(h); print('embedded', len(blob)//1024, 'KB')
