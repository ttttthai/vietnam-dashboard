import re,html,subprocess
def rows(f):
    out=subprocess.run(['python3','../gso/parse.py',f],capture_output=True,text=True).stdout.split('\n')
    return [l.split('\t') for l in out]
def num(s):
    s=s.strip()
    if s in ('..','','-','...'): return None
    try: return float(s.replace('.','').replace(',','.'))
    except ValueError: return None
def yr(s):
    m=re.search(r'(20\d\d)',s); return int(m.group(1)) if m else None
