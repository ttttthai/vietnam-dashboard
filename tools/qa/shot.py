"""Reliable dashboard screenshots for QA (used by the main session and every agent).

Waits for the server with polling (never `sleep`), opens a tab by id, a story chapter by index or name, expands
<details> sections, and captures the full height of an element or the page. Prints console errors and horizontal
overflow, and exits 1 when there are page errors.

Usage:
  python3 tools/qa/shot.py --tab banks --chap "Tự khám phá" --open fs-appendix --sel "#fs-appendix" --out a.png
  python3 tools/qa/shot.py --tab econ --chap 3 --lang en --width 420 --out budget_en.png
  python3 tools/qa/shot.py --list --tab banks          # print the tab's chapter names and exit

Tabs: party, social, econ, money (Chính sách), banks (Hệ thống tài chính), invest, sim.
Options: --port (default 8002; env DASH_PORT), --wait seconds for the server (default 120), --js "<expr>" to run
before capture (its result is printed), --sel "-" (default) for the visible chapter, "page" for the whole page.
"""
import argparse, json, os, sys, time, urllib.request

CHROME = '/opt/pw-browsers/chromium-1194/chrome-linux/chrome'


def wait_server(url, secs):
    t0 = time.time()
    while time.time() - t0 < secs:
        try:
            if urllib.request.urlopen(url, timeout=5).status == 200: return True
        except Exception: pass
        time.sleep(2)
    return False


def main():
    a = argparse.ArgumentParser()
    a.add_argument('--tab', required=True); a.add_argument('--chap'); a.add_argument('--open', action='append', default=[])
    a.add_argument('--sel', default='-'); a.add_argument('--out', default='shot.png'); a.add_argument('--lang', default='vi')
    a.add_argument('--width', type=int, default=1360); a.add_argument('--port', type=int, default=int(os.environ.get('DASH_PORT', 8002)))
    a.add_argument('--wait', type=int, default=120); a.add_argument('--js'); a.add_argument('--list', action='store_true')
    o = a.parse_args()
    base = f'http://127.0.0.1:{o.port}/'
    if not wait_server(base, o.wait):
        sys.exit(f'server not answering at {base} after {o.wait}s — start it: AUTO_FETCH_ON_STARTUP=0 python3 -m uvicorn server:app --port {o.port}')
    from playwright.sync_api import sync_playwright
    errs = []
    with sync_playwright() as p:
        b = p.chromium.launch(executable_path=CHROME if os.path.exists(CHROME) else None)
        pg = b.new_page(viewport={'width': o.width, 'height': 1000})
        net = []   # failed requests: blocked CDNs in the sandbox are expected (the page falls back to local copies)
        pg.on('console', lambda m: errs.append(m.text) if m.type == 'error' and not m.text.startswith('Failed to load resource') else None)
        pg.on('requestfailed', lambda r: net.append(f'{r.url[:90]} ({r.failure})'))
        pg.on('response', lambda r: net.append(f'{r.status} {r.url[:90]}') if r.status >= 400 else None)
        pg.on('pageerror', lambda e: errs.append('PAGEERR ' + str(e)))
        pg.goto(f'{base}?lang={o.lang}', wait_until='load')
        pg.wait_for_function("document.querySelector('.tab-btn[data-tab]')", timeout=30000)
        if not pg.evaluate(f"!!document.querySelector('.tab-btn[data-tab=\"{o.tab}\"]')"):
            sys.exit('unknown tab ' + o.tab + '; tabs: ' + ', '.join(pg.evaluate("[...document.querySelectorAll('.tab-btn[data-tab]')].map(x=>x.dataset.tab)")))
        pg.evaluate(f"document.querySelector('.tab-btn[data-tab=\"{o.tab}\"]').click()"); pg.wait_for_timeout(2000)
        tabs_js = "[...document.querySelectorAll('[role=tab].st-subtab')].filter(x=>x.offsetParent)"
        names = pg.evaluate(f"{tabs_js}.map(x=>x.textContent.trim())")
        if o.list:
            print(json.dumps(names, ensure_ascii=False)); b.close(); return
        if o.chap is not None:
            i = int(o.chap) if o.chap.isdigit() else next((k for k, n in enumerate(names) if o.chap.lower() in n.lower()), None)
            if i is None or i >= len(names): sys.exit(f'chapter {o.chap!r} not found; chapters: {names}')
            pg.evaluate(f"{tabs_js}[{i}].click()"); pg.wait_for_timeout(1500)
        for d in o.open:
            pg.evaluate(f"(()=>{{const d=document.getElementById({json.dumps(d)}); if(d){{d.open=true;}}}})()")
        pg.evaluate("document.querySelectorAll('.st-chap').forEach(x=>x.classList.add('in'))"); pg.wait_for_timeout(1500)
        if o.js:
            print('JS:', json.dumps(pg.evaluate(o.js), ensure_ascii=False)[:3000]); pg.wait_for_timeout(800)
        sel = o.sel
        if sel == '-': h = pg.evaluate_handle("[...document.querySelectorAll('.st-chap')].find(x=>x.offsetParent) || document.body")
        elif sel == 'page': h = None
        else:
            h = pg.query_selector(sel)
            if not h: sys.exit('no element ' + sel)
        if h is not None:
            el = h.as_element(); hgt = pg.evaluate("e=>Math.ceil(e.getBoundingClientRect().height)", el)
            if hgt == 0: sys.exit(f'{sel} is not visible (closed <details>? use --open <id>)')
            pg.set_viewport_size({'width': o.width, 'height': min(16000, hgt + 120)}); pg.wait_for_timeout(800)
            el.scroll_into_view_if_needed(); el.screenshot(path=o.out)
        else:
            pg.screenshot(path=o.out, full_page=True)
        sw = pg.evaluate("document.documentElement.scrollWidth")
        print(f'saved {o.out} · scrollWidth {sw} (viewport {o.width}){" OVERFLOW" if sw > o.width + 1 else ""}')
        print('errors:', errs or 'none'); print('network:', net or 'none'); b.close()
    if any(e.startswith('PAGEERR') for e in errs): sys.exit(1)


if __name__ == '__main__':
    main()
