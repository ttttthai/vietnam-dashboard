import json
from build_part1 import blocks, U_V0313, U_V0316
Y=list(range(2018,2026))
r=blocks('V03.13-14.html')['Giá trị (Tỷ đồng)']; e=blocks('V03.16.html')['Giá trị (Tỷ đồng)']
def s(d): return [d.get(y) for y in Y]
rev={'total':'TỔNG THU(*)','domestic_excl_oil':'Thu trong nước (Không kể thu từ dầu thô)','from_soes':'Thu từ doanh nghiệp Nhà nước(**)',
 'from_fdi_enterprises':'Thu từ doanh nghiệp có vốn đầu tư nước ngoài','from_private_sector':'Thu từ khu vực công, thương nghiệp, dịch vụ ngoài quốc doanh',
 'pit':'Thuế thu nhập cá nhân','env_protection_tax':'Thuế bảo vệ môi trường','fees_charges':'Thu phí, lệ phí','of_which_registration_fee':'Trong đó: Lệ phí trước bạ',
 'land_house_revenues':'Các khoản thu về nhà đất','other_domestic':'Các khoản thu khác','crude_oil':'Thu từ dầu thô',
 'import_export_balance':'Thu cân đối ngân sách từ hoạt động xuất nhập khẩu','import_export_gross':'Tổng số thu từ hoạt động xuất, nhập khẩu','vat_refunds':'Hoàn thuế giá trị gia tăng','grants':'Thu viện trợ'}
exp={'total_excl_principal':'TỔNG CHI(*)','development_investment':'Chi đầu tư phát triển(**)','socio_economic_recurrent_incl_interest':'Chi phát triển sự nghiệp kinh tế - xã hội(***)',
 'education_training':'Chi sự nghiệp giáo dục, đào tạo','science_technology':'Chi sự nghiệp khoa học, công nghệ','reserve_fund':'Chi bổ sung quĩ dự trữ tài chính'}
out={'years':Y,'basis':['final (yearbook)']*7+['estimate (Ước tính 2025)'],'unit':'bn VND',
 'revenue':{k:s(r[v]) for k,v in rev.items()},'expenditure':{k:s(e[v]) for k,v in exp.items()},
 'sources':{'revenue':U_V0313+' (NSO PxWeb V03.13-14 Thu ngân sách Nhà nước; adjusted per 2015 Budget Law, incl. lottery, excl. carried-over revenue)',
            'expenditure':U_V0316+' (NSO PxWeb V03.16 Chi ngân sách Nhà nước; incl. government-bond-financed spending, excl. principal repayment)'}}
json.dump(out,open('nso_budget.json','w'),ensure_ascii=False,indent=1)
print(json.dumps(out,ensure_ascii=False)[:1500])
