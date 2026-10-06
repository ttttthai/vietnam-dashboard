import sys,datetime
lo,hi,start,end=int(sys.argv[1]),int(sys.argv[2]),sys.argv[3],sys.argv[4]
s=datetime.date.fromisoformat(start); e=datetime.date.fromisoformat(end)
for i in range(lo,hi):
    d=s
    while d<=e:
        if d.weekday()<6: print(i,d.isoformat())
        d+=datetime.timedelta(days=1)
