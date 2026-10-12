"""Check data/narration.json (the Narrator agent's film scripts) before it is embedded.

Errors (exit 1): bad structure, unknown tab, a chapter or target that is not on the page (per
data/research/story_map.json — rebuild it with tools/qa/story_map.py after story changes), a scene without both
languages. Warnings: a number in the narration that appears in no data file (data/*.json, strategy_directives.json, invest_research.json,
the story map's own text) — every figure the narrator speaks must come from the dashboard's data; overlong scenes;
VI and EN scenes that disagree on their numbers.

Usage: python3 tools/qa/narration_check.py [--strict]   (--strict: warnings also fail)
"""
import json, os, re, sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
TABS = {'party', 'social', 'econ', 'money', 'banks', 'invest', 'sim'}
FILMS = TABS | {'overview'}


def nums_vi(t):   # Vietnamese: "." thousands, "," decimals → canonical strings
    out = set()
    for m in re.findall(r'\d+(?:[.,]\d+)*', t):
        out.add(m.replace('.', '').replace(',', '.') if re.fullmatch(r'\d{1,3}(?:\.\d{3})+(?:,\d+)?', m) else m.replace(',', '.'))
    return out


def nums_en(t):
    return {m.replace(',', '') for m in re.findall(r'\d+(?:[.,]\d+)*', t)}


def canon(x):
    try:
        f = float(x); return [str(int(f))] if f == int(f) else [('%g' % f), str(f)]
    except ValueError: return [x]


def corpus():
    vals, text = set(), []
    def walk(v):
        if isinstance(v, bool) or v is None: return
        if isinstance(v, (int, float)):
            for d in (0, 1, 2):
                r = round(v, d); vals.update(canon(str(int(r)) if d == 0 else str(r)))
            for k in (100, 1e-3, 1e-6, 1e-9):   # shares ↔ %, units ↔ thousand / million / billion
                w = v * k
                for d in (0, 1, 2): vals.update(canon(str(round(w, d))))
        elif isinstance(v, str): text.append(v)
        elif isinstance(v, dict): [walk(x) for x in v.values()]
        elif isinstance(v, list): [walk(x) for x in v]
    files = [os.path.join(ROOT, 'data', f) for f in os.listdir(os.path.join(ROOT, 'data')) if f.endswith('.json') and f != 'narration.json']
    files += [os.path.join(ROOT, 'strategy_directives.json'), os.path.join(ROOT, 'invest_research.json'), os.path.join(ROOT, 'data', 'research', 'story_map.json')]
    for f in files:
        if os.path.exists(f):
            try: walk(json.load(open(f, encoding='utf-8')))
            except Exception: pass
    for t in text: vals |= nums_vi(t) | nums_en(t)
    return vals


def main():
    strict = '--strict' in sys.argv
    N = json.load(open(os.path.join(ROOT, 'data', 'narration.json'), encoding='utf-8'))
    smap_p = os.path.join(ROOT, 'data', 'research', 'story_map.json')
    if not os.path.exists(smap_p): sys.exit('data/research/story_map.json missing — run tools/qa/story_map.py first')
    smap = json.load(open(smap_p, encoding='utf-8'))
    chaps = {t: {c['chap']: {x['target'] for x in c['charts']} for s in (smap.get(t) or []) for c in s['chapters']} for t in TABS}
    known = corpus(); err, warn, n = [], [], 0
    films = (N.get('NARRATION') or {}).get('films')
    if not isinstance(films, dict): sys.exit('NARRATION.films must be an object')
    for fid, F in films.items():
        if fid not in FILMS: err.append(f'{fid}: unknown film id (use a tab id or "overview")')
        for i, sc in enumerate(F.get('scenes') or []):
            n += 1; at = f'{fid}[{i}]'
            for k in ('tab', 'chap', 'vi', 'en'):
                if not sc.get(k): err.append(f'{at}: missing "{k}"')
            t = sc.get('tab')
            if t not in TABS: err.append(f'{at}: unknown tab {t!r}'); continue
            if fid in TABS and t != fid: warn.append(f'{at}: scene in film "{fid}" points at tab "{t}"')
            if sc.get('chap') not in chaps[t]: err.append(f'{at}: chapter {sc.get("chap")!r} not on tab {t} (see story_map.json)'); continue
            if sc.get('target') and sc['target'] not in chaps[t][sc['chap']]: err.append(f'{at}: target {sc["target"]!r} not in chapter {sc["chap"]}')
            vi, en = sc.get('vi', ''), sc.get('en', '')
            for lang, txt in (('vi', vi), ('en', en)):
                if len(txt) > 330: warn.append(f'{at}.{lang}: {len(txt)} chars (keep a scene ≤ ~300 so it is read in ~20 s)')
            nv, ne = nums_vi(vi), nums_en(en)
            for x in sorted(nv | ne):
                if re.fullmatch(r'(19|20)\d\d', x) or (x.isdigit() and int(x) <= 12): continue   # years, small counts
                if not set(canon(x)) & known: warn.append(f'{at}: number {x} not found in any data file')
            if {x for x in nv if not re.fullmatch(r'(19|20)\d\d', x)} != {x for x in ne if not re.fullmatch(r'(19|20)\d\d', x)}:
                warn.append(f'{at}: VI and EN carry different numbers ({sorted(nv ^ ne)})')
    for e in err: print('ERROR', e)
    for w in warn: print('warn ', w)
    print(f'{len(films)} films, {n} scenes · {len(err)} errors, {len(warn)} warnings')
    sys.exit(1 if err or (strict and warn) else 0)


main()
