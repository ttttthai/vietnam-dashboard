import json,sys,os
OUT='/private/tmp/claude-501/-Users-hoangthai-CLAUDECODE-VIETNAM-DASHBOARD---pycache--/4950ec61-8c8c-4a3e-9769-13c5d94d4082/scratchpad/religion/prov2019_g1.json'
EXP={"Hà Nội":(8053663,202526),"Hà Giang":(854679,19871),"Cao Bằng":(530341,23880),"Bắc Kạn":(313905,17371),"Tuyên Quang":(784811,30518),"Lào Cai":(730420,32739),"Điện Biên":(598856,59384),"Lai Châu":(460196,39583),"Sơn La":(1248415,18022),"Yên Bái":(821030,52800),"Hoà Bình":(854131,10513),"Thái Nguyên":(1286751,33753),"Lạng Sơn":(781655,4778),"Quảng Ninh":(1320324,17654),"Bắc Giang":(1803950,25312),"Phú Thọ":(1463726,145952)}
def add(name,pop,nr,rel,url,table):
    ep,ef=EXP[name]
    s=sum(rel.values())
    print(name,'pop ok' if pop==ep else f'POP MISMATCH {pop} vs {ep}','| followers',s,'ok' if s==ef else f'MISMATCH vs {ef}','| pop-nr',pop-nr)
    d=json.load(open(OUT)) if os.path.exists(OUT) else {}
    d[name]={"pop":pop,"no_religion":nr,"religions":rel,"url":url,"table":table}
    json.dump(d,open(OUT,'w'),ensure_ascii=False,indent=1)
