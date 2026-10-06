import json
out={}
def add(name,pop,nr,rel,url,table,exp_pop,exp_f):
    s=sum(rel.values())
    assert pop==exp_pop, (name,pop,exp_pop)
    assert s==exp_f, (name,s,exp_f)
    assert nr+s==pop, (name,nr,s,pop)
    out[name]={"pop":pop,"no_religion":nr,"religions":rel,"url":url,"table":table}
add("Thừa Thiên Huế",1128620,894139,{"Phật giáo":191594,"Công giáo":42477,"Tin lành":235,"Cao Đài":121,"Phật giáo Hòa Hảo":33,"Hồi giáo":5,"Tôn giáo Baha'i":10,"Tịnh độ Cư sĩ Phật hội Việt Nam":1,"Chăm Bà la môn":5},
    "https://docs.google.com/document/d/1UNcU0AfibV3Et75L4KXw-dlaKakv5ZT3/edit (linked from https://thongkehue.nso.gov.vn/bao-cao-so-lieu-thong-ke/D%C3%A2n-s%E1%BB%91-v%C3%A0-Lao-%C4%91%E1%BB%99ng/5)",
    "Biểu 1.3: Cơ cấu dân số theo tôn giáo (Toàn tỉnh row, Chung column), Kết quả TĐT DS&NO 2019 tỉnh Thừa Thiên Huế",1128620,234481)
add("Hà Tĩnh",1288866,1142453,{"Phật giáo":784,"Công giáo":145577,"Tin lành":41,"Cao Đài":2,"Phật giáo Hòa Hảo":3,"Hồi giáo":3,"Tôn giáo Baha'i":1,"Chăm Bà la môn":1,"Giáo hội Các thành hữu Ngày sau của Chúa Giê su Ky tô Việt Nam (Mormon)":1},
    "https://thongkehatinh.nso.gov.vn/storage/manager/an_pham/XJdVcsX-3d38c850-41e1-468f-8854-dc6e4d2cefbd.pdf",
    "Biểu 4. Dân số chia theo tôn giáo, thành thị, nông thôn, giới tính và đơn vị hành chính cấp huyện, 01/4/2019 (TOÀN TỈNH row, Tổng số/Chung), pp.154-155",1288866,146413)
json.dump(out,open('prov2019_g2.json','w'),ensure_ascii=False,indent=1)
print(list(out))
