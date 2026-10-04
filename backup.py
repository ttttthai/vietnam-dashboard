"""
Local backup for the Vietnam Dashboard — everything the system needs, kept on this Mac.

    BACKUP/
      market.db            SQLite: daily OHLCV for every listed stock + indices, symbol metadata
      snapshots/           dated .zip of all project files (dashboard, server, engine, research data, configs, logs)
      manifest.json        last backup time, row counts, sizes
      backup_log.jsonl     one line per run

Usage:
    .venv/bin/python backup.py            # asks for confirmation in the terminal
    .venv/bin/python backup.py --yes      # no question (used after the macOS dialog said yes)
    .venv/bin/python backup.py --files-only

Prices come from Vietcap (VCI) public endpoints via invest.py (standard library only).
The first run downloads full history (~15–20 min, ~300 MB); later runs only fetch new sessions.
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
import zipfile
from datetime import date, datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent
BK = ROOT / "BACKUP"
DB = BK / "market.db"
SNAP = BK / "snapshots"
MANIFEST = BK / "manifest.json"
LOG = BK / "backup_log.jsonl"
KEEP_SNAPSHOTS = 6
INDICES = ["VNINDEX", "VN30", "HNX30", "VN100"]
PROJECT_FILES = ["vietnam_dashboard.html", "server.py", "invest.py", "invest_research.json", "i18n_en.json", "strategy_directives.json", ".claude/agents/strategy.md", "backup.py",
                 "requirements.txt", "render.yaml", "runtime.txt", "README.md", ".gitignore",
                 ".claude/launch.json", ".refresh_log.jsonl", "BACKUP/ask_and_backup.sh"]
PAUSE = 0.35          # seconds between VCI requests (be polite)

sys.path.insert(0, str(ROOT))
import invest  # noqa: E402


def log(msg: str) -> None:
    print(f"[{datetime.now():%H:%M:%S}] {msg}", flush=True)


def db_connect() -> sqlite3.Connection:
    con = sqlite3.connect(DB)
    con.executescript("""
        CREATE TABLE IF NOT EXISTS prices (
            symbol TEXT NOT NULL, d TEXT NOT NULL,
            o REAL, h REAL, l REAL, c REAL, v INTEGER,
            PRIMARY KEY (symbol, d)) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS symbols (
            symbol TEXT PRIMARY KEY, type TEXT, board TEXT, name TEXT, en_name TEXT, icb TEXT,
            listed_shares INTEGER, updated TEXT);
        CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT);
    """)
    return con


def fetch_series(sym: str, bars: int) -> dict | None:
    for attempt in range(3):
        try:
            d = invest._post(f"{invest._VCI}/chart/OHLCChart/gap-chart",
                             {"timeFrame": "ONE_DAY", "symbols": [sym], "to": int(time.time()), "countBack": bars})
            if not d or not d[0].get("c"):
                return None
            return d[0]
        except Exception as e:  # rate limit / network: back off and retry
            log(f"  {sym}: {str(e)[:60]} — thử lại ({attempt + 1}/3)")
            time.sleep(3 * (attempt + 1))
    return None


def backup_prices(con: sqlite3.Connection) -> dict:
    listing = invest._get(f"{invest._VCI}/price/symbols/getAll")
    stocks = [x for x in listing if x.get("type") == "STOCK"]
    now = datetime.now(timezone.utc).isoformat()
    con.executemany("INSERT OR REPLACE INTO symbols(symbol,type,board,name,en_name,icb,updated) VALUES(?,?,?,?,?,?,?)",
                    [(x["symbol"], x.get("type"), x.get("board"), x.get("organName"), x.get("enOrganName"), x.get("icbCode2"), now) for x in stocks]
                    + [(s, "INDEX", None, s, s, None, now) for s in INDICES])
    shares = invest.listed_shares([x["symbol"] for x in stocks if x.get("board") != "DELISTED"])
    con.executemany("UPDATE symbols SET listed_shares=? WHERE symbol=?", [(v, k) for k, v in shares.items()])
    con.commit()

    syms = INDICES + [x["symbol"] for x in stocks]
    last = dict(con.execute("SELECT symbol, MAX(d) FROM prices GROUP BY symbol").fetchall())
    added = failed = empty = 0
    t0 = time.time()
    for i, s in enumerate(syms, 1):
        prev = last.get(s)
        bars = 6000 if not prev else min(6000, max(5, (date.today() - date.fromisoformat(prev)).days + 5))
        r = fetch_series(s, bars)
        if r is None:
            empty += 1
        else:
            num = lambda x: None if x is None else float(x)
            rows = [(s, datetime.fromtimestamp(int(t), timezone.utc).date().isoformat(), num(o), num(h), num(l), float(c), int(float(v or 0)))
                    for t, o, h, l, c, v in zip(r["t"], r["o"], r["h"], r["l"], r["c"], r["v"])
                    if t is not None and c is not None]          # skip sessions VCI returns without a close
            before = con.total_changes
            # prices are split-adjusted by VCI → refresh overlapping rows too
            con.executemany("INSERT OR REPLACE INTO prices VALUES(?,?,?,?,?,?,?)", rows)
            added += con.total_changes - before
        if i % 50 == 0:
            con.commit()
            eta = (time.time() - t0) / i * (len(syms) - i)
            log(f"  {i}/{len(syms)} mã · +{added:,} dòng · còn ~{eta / 60:.0f} phút")
        time.sleep(PAUSE)
    con.commit()
    con.execute("INSERT OR REPLACE INTO meta VALUES('prices_updated', ?)", (now,))
    con.commit()
    total = con.execute("SELECT COUNT(*), COUNT(DISTINCT symbol), MIN(d), MAX(d) FROM prices").fetchone()
    return {"symbols_requested": len(syms), "rows_written": added, "no_data": empty + failed,
            "rows_total": total[0], "symbols_with_data": total[1], "first_date": total[2], "last_date": total[3]}


def backup_files() -> dict:
    SNAP.mkdir(parents=True, exist_ok=True)
    name = SNAP / f"snapshot_{datetime.now():%Y-%m-%d_%H%M}.zip"
    included = []
    with zipfile.ZipFile(name, "w", zipfile.ZIP_DEFLATED) as z:
        for f in PROJECT_FILES:
            p = ROOT / f
            if p.exists():
                z.write(p, f)
                included.append(f)
        # capture what the running server currently serves (live FX, banks, logs) if it is up
        try:
            import urllib.request
            for ep in ("snapshot", "logs?limit=400", "banks?period=year", "banks/statements?period=year", "banks/breakdown?period=year"):
                with urllib.request.urlopen(f"http://127.0.0.1:8001/api/{ep}", timeout=10) as r:
                    z.writestr(f"api/{ep.split('?')[0].replace('/', '_')}.json", r.read())
            included.append("api/*.json (server đang chạy)")
        except Exception:
            included.append("(server không chạy — bỏ qua bản chụp API)")
    old = sorted(SNAP.glob("snapshot_*.zip"))[:-KEEP_SNAPSHOTS]
    for p in old:
        p.unlink()
    return {"snapshot": name.name, "size_kb": round(name.stat().st_size / 1024), "files": included, "deleted_old": [p.name for p in old]}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--yes", action="store_true", help="không hỏi lại")
    ap.add_argument("--files-only", action="store_true", help="chỉ sao lưu file, không tải giá")
    a = ap.parse_args()
    BK.mkdir(exist_ok=True)
    if not a.yes:
        ans = input("Sao lưu dữ liệu Vietnam Dashboard vào thư mục BACKUP? [y/N] ").strip().lower()
        if ans not in ("y", "yes", "c", "co", "có"):
            print("Đã huỷ.")
            return 1
    t0 = time.time()
    result = {"started": datetime.now(timezone.utc).isoformat()}
    try:
        result["files"] = backup_files()
        log(f"Đã lưu {result['files']['snapshot']} ({result['files']['size_kb']} KB)")
        if not a.files_only:
            log("Đang tải giá từ VCI…")
            with db_connect() as con:
                result["prices"] = backup_prices(con)
            log(f"market.db: {result['prices']['rows_total']:,} dòng, {result['prices']['symbols_with_data']} mã")
        result["status"] = "ok"
    except Exception as e:
        result["status"] = "error"
        result["error"] = str(e)[:300]
        log(f"LỖI: {e}")
    result["duration_s"] = round(time.time() - t0)
    result["db_size_mb"] = round(DB.stat().st_size / 1e6, 1) if DB.exists() else 0
    with open(LOG, "a", encoding="utf-8") as f:
        f.write(json.dumps(result, ensure_ascii=False) + "\n")
    if result["status"] == "ok":
        MANIFEST.write_text(json.dumps({"last_backup": result["started"], **result}, ensure_ascii=False, indent=1), encoding="utf-8")
    return 0 if result["status"] == "ok" else 2


if __name__ == "__main__":
    sys.exit(main())
