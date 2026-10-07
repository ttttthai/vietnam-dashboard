"""Embed each tab's data file (data/<tab>.json) into vietnam_dashboard.html.

The page keeps its datasets as top-level constants (`const ECONFLOW = {...};`) so it also works from
file:// with no server. Each tab agent owns one JSON file whose top-level keys are those constant names;
this script writes them back into the page. Keys starting with "_" (e.g. "_meta") are not embedded.
Only the files listed in OWNERS are page data. data/research/ (Research agent ops files), data/auto/ (server
API snapshots) and data/invest_macro.json (served by the server) are never embedded.
Every run prints each file's _meta.as_of and warns when it is missing.

Usage:
  python3 tools/build/embed_data.py            # embed every data/*.json into the page
  python3 tools/build/embed_data.py economy    # embed only data/economy.json
  python3 tools/build/embed_data.py --check    # report differences, write nothing
  python3 tools/build/embed_data.py --check --strict   # also exit 1 if a file's _meta.as_of is missing
  python3 tools/build/embed_data.py --extract  # (one-off) create data/*.json from the page's current constants
"""
import json, os, re, sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
PAGE = os.path.join(ROOT, 'vietnam_dashboard.html')
DATA = os.path.join(ROOT, 'data')
OWNERS = {   # file → constants it owns (and the agent that maintains it)
    'economy': (['ECONFLOW', 'ECON_OFFICIAL', 'CPI_YOY_24', 'CPI_LATEST', 'CPI_DETAIL', 'CPI_FC_INST'], 'Economy'),
    'society': (['SOC_PROV_DATA', 'POP_SERIES', 'SOC_GRDP', 'WORLD_RANK', 'PROV_PREV', 'RELIGION'], 'Society'),
    'policy': (['POLICY'], 'Policy'),
    'finance': (['FINSYS'], 'Finance'),
    'simulation': (['SIM'], 'Finance'),   # Simulation tab: monthly panel, story beats, scenario model (tools/build/simulate.py)
    'flourish': (['FLOURISH_VIZ'], 'Main session'),   # Flourish chart registry (story slots read it)
}

def locate(html, name):
    """Return (start, end) of the JSON value of `const NAME = <json>;` in the page."""
    m = re.search(r'^const ' + re.escape(name) + r' = ', html, re.M)
    if not m: raise SystemExit(f'constant {name} not found in page')
    _, n = json.JSONDecoder().raw_decode(html[m.end():])
    return m.end(), m.end() + n

def dump(v):   # one line, same style as the page
    return json.dumps(v, ensure_ascii=False, separators=(', ', ': '))

def main():
    args = sys.argv[1:]
    if any(a in ('-h', '--help') for a in args): print(__doc__); return
    bad = [a for a in args if (a.startswith('-') and a not in ('--check', '--extract', '--strict')) or (not a.startswith('-') and a not in OWNERS)]
    if bad: raise SystemExit(f'unknown argument(s): {bad}; use one of {list(OWNERS)}, --check, --strict, --extract, --help')
    html = open(PAGE, encoding='utf-8').read()
    if '--extract' in args:
        os.makedirs(DATA, exist_ok=True)
        for f, (names, agent) in OWNERS.items():
            out = {'_meta': {'owner': agent + ' agent', 'note': 'Top-level keys are the page constants this file feeds; run tools/build/embed_data.py after editing.'}}
            for n in names:
                a, b = locate(html, n); out[n] = json.loads(html[a:b])
            json.dump(out, open(os.path.join(DATA, f + '.json'), 'w', encoding='utf-8'), ensure_ascii=False, indent=1)
            print('extracted', f, names)
        return
    files = [a for a in args if not a.startswith('--')] or list(OWNERS)
    changed, no_asof = [], []
    for f in files:
        path = os.path.join(DATA, f + '.json')
        if not os.path.exists(path): print('missing', path); continue
        d = json.load(open(path, encoding='utf-8'))
        as_of = (d.get('_meta') or {}).get('as_of') if isinstance(d.get('_meta'), dict) else None
        if as_of: print(f'{f}.json  _meta.as_of = {as_of}')
        else: print(f'WARNING {f}.json: _meta.as_of missing (add "as_of": "YYYY-MM-DD" to _meta)'); no_asof.append(f)
        for n, v in d.items():
            if n.startswith('_'): continue
            if n not in OWNERS.get(f, ([], ''))[0]: raise SystemExit(f'{f}.json may not write {n} (not in its owned constants)')
            a, b = locate(html, n); new = dump(v)
            if json.loads(html[a:b]) != v:
                changed.append(f'{f}:{n}'); html = html[:a] + new + html[b:]
    if '--check' in args:
        print('would change:', changed or 'nothing')
        if '--strict' in args and no_asof: raise SystemExit(f'_meta.as_of missing in: {no_asof}')
        return
    if changed: open(PAGE, 'w', encoding='utf-8').write(html)
    print('embedded:', changed or 'no changes')

main()
