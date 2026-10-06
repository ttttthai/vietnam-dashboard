import urllib.parse,sys,re
for f in sys.argv[1:]:
  for l in open(f):
    u=urllib.parse.unquote_plus(l.strip())
    low=u.lower()
    if re.search(r'\.(css|js)(\?|\s)',low): continue
    if any(k in low for k in ['dân số','dan-so','dan so','danso','dan_so','tdt','tđt','điều tra','dieu tra','dieu-tra','2019','ket qua','kết quả','ketqua','tdtds','ton giao','tôn giáo']): print(u)
