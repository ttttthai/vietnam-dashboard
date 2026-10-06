import subprocess, calendar, os
months=[(2024,m) for m in (10,11,12)]+[(2025,m) for m in range(1,13)]+[(2026,m) for m in range(1,10)]
for y,m in months:
    found=None
    for k in (1,2,0):
        py,pm=(y,m+k) if m+k<=12 else (y+1,m+k-12)
        folder=f"{calendar.month_name[pm]}{py}"
        for name in [f"VBMA_BAO%20CAO%20TTTP%20THANG%20{m}%20{y}.pdf", f"VBMA_BAO%20CAO%20TTTP%20THANG%20{m:02d}%20{y}.pdf"]:
            u=f"https://vbma.org.vn/storage/reports/{folder}/{name}"
            c=subprocess.run(['curl','-s','-o','/dev/null','-m','20','-A','Mozilla/5.0','-w','%{http_code} %{content_type}','-I',u],capture_output=True,text=True).stdout
            if c.startswith('200') and 'pdf' in c:
                found=u;break
        if found: break
    print(y,m,found,flush=True)
