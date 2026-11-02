"""Build the read-only film storyboard page from data/narration.json and the story maps.

One page, every film the Narrator wrote: each scene in order with its estimated start time, the kicker, the
narration (VI/EN), its figure cards, and what is on screen (tab › chapter › chart, from data/research/story_map*.json).
Output: a self-contained HTML body for a claude.ai artifact (the publish skeleton adds <html>/<head>/<body>).

Usage: python3 tools/build/storyboard.py OUT.html
"""
import html, json, os, sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
TAB = {'overview': ('Phim toàn cảnh', 'The big picture'), 'party': ('Chính trị', 'Politics'), 'social': ('Xã hội', 'Society'),
       'econ': ('Kinh tế', 'Economy'), 'banks': ('Hệ thống tài chính', 'Financial system'), 'invest': ('Thị trường chứng khoán', 'Stock Market'),
       'sim': ('Mô phỏng', 'Simulation'), 'geo': ('Địa chính trị', 'Geopolitics')}


def onscreen(maps, sc):
    out = {}
    for lang, m in maps.items():
        tab = m.get(sc.get('tab')) or []
        for sub in tab:
            for c in sub['chapters']:
                if c['chap'] == sc.get('chap'):
                    cap = ''
                    if sc.get('target'):
                        cap = next((x['caption'] for x in c['charts'] if x['target'] == sc['target']), '')
                    out[lang] = {'sub': sub['label'], 'head': c['headline'] or c['kicker'], 'chart': cap}
    return out


def main():
    out = sys.argv[1] if len(sys.argv) > 1 else 'storyboard.html'
    N = json.load(open(os.path.join(ROOT, 'data', 'narration.json'), encoding='utf-8'))
    maps = {'vi': json.load(open(os.path.join(ROOT, 'data', 'research', 'story_map.json'), encoding='utf-8')),
            'en': json.load(open(os.path.join(ROOT, 'data', 'research', 'story_map_en.json'), encoding='utf-8'))}
    films = []
    order = ['overview', 'party', 'social', 'econ', 'banks', 'invest', 'sim', 'geo']
    F = N['NARRATION']['films']
    for fid in [f for f in order if f in F] + [f for f in F if f not in order]:
        f = F[fid]; scenes = []
        for sc in f.get('scenes', []):
            scenes.append({'tab': sc.get('tab'), 'k': sc.get('k') or {}, 'vi': sc.get('vi', ''), 'en': sc.get('en', ''), 'figs': sc.get('figs') or [],
                           'big': bool(sc.get('big')), 'pop': bool(sc.get('pop')), 'play': sc.get('play', True) is not False and bool(sc.get('target')),
                           'on': onscreen(maps, sc), 'src': sc.get('src', '')})
        films.append({'id': fid, 'name': TAB.get(fid, (fid, fid)), 'title': f.get('title') or {}, 'scenes': scenes})
    data = {'as_of': N.get('_meta', {}).get('as_of', ''), 'tabs': TAB, 'films': films}
    tpl = open(os.path.join(os.path.dirname(__file__), 'storyboard_template.html'), encoding='utf-8').read()
    page = tpl.replace('/*DATA*/null', json.dumps(data, ensure_ascii=False, separators=(',', ':')).replace('</', '<\\/'))
    open(out, 'w', encoding='utf-8').write(page)
    print('wrote', out, f'{len(page) / 1024:.0f} KB,', sum(len(f["scenes"]) for f in films), 'scenes in', len(films), 'films')


main()
