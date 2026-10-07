"""Build assets/starter.html: inline reference/story.css and reference/story-core.js into scripts/starter.src.html.

    python3 scripts/build_starter.py            # writes assets/starter.html (self-contained, one file)
"""
import os

HERE = os.path.dirname(os.path.abspath(__file__)); ROOT = os.path.dirname(HERE)
src = open(os.path.join(HERE, 'starter.src.html'), encoding='utf-8').read()
css = open(os.path.join(ROOT, 'reference', 'story.css'), encoding='utf-8').read()
js = open(os.path.join(ROOT, 'reference', 'story-core.js'), encoding='utf-8').read()
out = src.replace('/*@@STORY_CSS@@*/', css).replace('/*@@STORY_JS@@*/', js)
os.makedirs(os.path.join(ROOT, 'assets'), exist_ok=True)
path = os.path.join(ROOT, 'assets', 'starter.html')
open(path, 'w', encoding='utf-8').write(out)
print('wrote', path, len(out), 'bytes')
