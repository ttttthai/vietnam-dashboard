"""
Investment screening engine for the "Đầu tư" tab.

Price data comes straight from Vietcap (VCI) public chart endpoints — the same
upstream vnstock uses — via the standard library only (vnstock is quarantined
on PyPI since 2026-09-24).

Design rule: every "certainty" number is an empirical base rate measured on the
symbol's own history (how often the same setup was followed by a gain over the
same holding period), reported with its sample size. Nothing here is a forecast
or a recommendation.
"""
from __future__ import annotations

import gzip
import json
import logging
import math
import threading
import time
import urllib.request
from datetime import date, datetime, timezone
from typing import Any

log = logging.getLogger("vn-dashboard.invest")

# ─── Rules (master perimeter) ──────────────────────────────────────
MAX_SYMBOLS = 4
MAX_HORIZON = 63            # trading days ≈ 3 months
HORIZONS = {1: 21, 2: 42, 3: 63}
CERTAINTY_TARGET = 0.80
MIN_SAMPLE = 8              # below this a win rate is reported as "thiếu mẫu"
MIN_VOLUME = 100_000        # avg matched shares / session over the last 20 sessions
MIN_CHARTER_BN = 5_000      # charter capital, bn VND (≈ listed shares × 10,000đ par)
PAR_VND = 10_000

# ─── VCI client ────────────────────────────────────────────────────
_VCI = "https://trading.vietcap.com.vn/api"
_HDR = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://trading.vietcap.com.vn/",
    "Origin": "https://trading.vietcap.com.vn",
}
_HIST_TTL = 30 * 60
_META_TTL = 24 * 3600
_cache: dict[str, tuple[float, Any]] = {}
_lock = threading.Lock()


def _cached(key: str, ttl: float, fn):
    with _lock:
        hit = _cache.get(key)
        if hit and time.time() - hit[0] < ttl:
            return hit[1]
    val = fn()
    with _lock:
        _cache[key] = (time.time(), val)
    return val


def _read(req: urllib.request.Request) -> Any:
    with urllib.request.urlopen(req, timeout=20) as r:
        raw = r.read()
        if r.headers.get("Content-Encoding") == "gzip" or raw[:2] == b"\x1f\x8b":
            raw = gzip.decompress(raw)
        return json.loads(raw)


def _get(url: str) -> Any:
    return _read(urllib.request.Request(url, headers=_HDR))


def _post(url: str, body: dict) -> Any:
    return _read(urllib.request.Request(url, data=json.dumps(body).encode(), method="POST",
                                        headers={**_HDR, "Content-Type": "application/json"}))


def history(symbol: str, bars: int = 1500) -> dict[str, list] | None:
    """Daily OHLCV, oldest first. ~6 years at 1500 bars."""
    def load():
        d = _post(f"{_VCI}/chart/OHLCChart/gap-chart",
                  {"timeFrame": "ONE_DAY", "symbols": [symbol], "to": int(time.time()), "countBack": bars})
        if not d or not d[0].get("c"):
            return None
        r = d[0]
        return {
            "t": [datetime.fromtimestamp(int(x), timezone.utc).date().isoformat() for x in r["t"]],
            "o": [float(x) for x in r["o"]], "h": [float(x) for x in r["h"]],
            "l": [float(x) for x in r["l"]], "c": [float(x) for x in r["c"]],
            "v": [float(x) for x in r["v"]],
        }
    return _cached(f"hist:{symbol}:{bars}", _HIST_TTL, load)


def group_members(group: str) -> list[str]:
    return _cached(f"group:{group}", _META_TTL,
                   lambda: [x["symbol"] for x in _get(f"{_VCI}/price/symbols/getByGroup?group={group}")])


def listed_shares(symbols: list[str]) -> dict[str, int]:
    """Listed share count per symbol from the VCI price board (batched, cached 24h)."""
    out: dict[str, int] = {}
    missing = []
    with _lock:
        for s in symbols:
            hit = _cache.get(f"listed:{s}")
            if hit and time.time() - hit[0] < _META_TTL:
                out[s] = hit[1]
            else:
                missing.append(s)
    for k in range(0, len(missing), 50):
        try:
            rows = _post(f"{_VCI}/price/symbols/getList", {"symbols": missing[k:k + 50]})
        except Exception as e:
            log.warning("listed_shares failed: %s", e)
            continue
        for x in rows or []:
            li = x.get("listingInfo") or {}
            if li.get("symbol") and li.get("listedShare"):
                out[li["symbol"]] = int(li["listedShare"])
                with _lock:
                    _cache[f"listed:{li['symbol']}"] = (time.time(), int(li["listedShare"]))
    return out


def eligibility(symbol: str, vols: list[float], shares: int | None) -> dict:
    vol20 = sum(vols[-20:]) / min(20, len(vols))
    charter = shares * PAR_VND / 1e9 if shares else None
    checks = [
        {"key": "volume", "label": "KL khớp TB 20 phiên", "value": round(vol20),
         "threshold": MIN_VOLUME, "unit": "cp/phiên", "pass": vol20 >= MIN_VOLUME},
        {"key": "charter", "label": "Vốn điều lệ (ước tính)", "value": round(charter) if charter else None,
         "threshold": MIN_CHARTER_BN, "unit": "tỷ đồng", "pass": None if charter is None else charter >= MIN_CHARTER_BN},
    ]
    return {"checks": checks, "eligible": all(c["pass"] for c in checks)}


def symbol_meta() -> dict[str, dict]:
    def load():
        return {x["symbol"]: {"name": x.get("organShortName") or x.get("organName") or "",
                              "name_en": x.get("enOrganShortName") or x.get("enOrganName") or "",
                              "board": x.get("board"), "icb": x.get("icbCode2")}
                for x in _get(f"{_VCI}/price/symbols/getAll") if x.get("type") == "STOCK"}
    return _cached("meta", _META_TTL, load)


# ─── Sector themes (ICB supersector → macro theme) ─────────────────
ICB_NAMES = {
    "0500": "Dầu khí", "1300": "Hoá chất", "1700": "Tài nguyên cơ bản (thép…)",
    "2300": "Xây dựng & Vật liệu", "2700": "Hàng & DV công nghiệp", "3300": "Ô tô & phụ tùng",
    "3500": "Thực phẩm & Đồ uống", "3700": "Hàng cá nhân & Gia dụng", "4500": "Y tế",
    "5300": "Bán lẻ", "5500": "Truyền thông", "5700": "Du lịch & Giải trí", "6500": "Viễn thông",
    "7500": "Tiện ích (điện, nước, gas)", "8300": "Ngân hàng", "8500": "Bảo hiểm",
    "8600": "Bất động sản", "8700": "Dịch vụ tài chính", "9500": "Công nghệ",
}
# Sectors whose revenue is directly tied to public-investment disbursement
PUBLIC_INVEST_ICB = {"2300", "1700", "2700"}
# Sectors most sensitive to an accelerating GDP / credit cycle
GROWTH_ICB = {"8300", "8700", "8600", "5300", "3500", "3700", "2700", "9500"}

# ─── Macro context (hand-entered, sourced, as of 2026-10-03) ───────
MACRO = {
    "as_of": "2026-10-03",
    "gdp": {
        "q1": 8.15, "q2": 8.81, "q3": 9.95, "ytd9m": 9.01, "target": 10.0,
        # Approximate: Q4 ≈ 28% of annual GDP → required Q4 ≈ (10 − 9.01×0.72)/0.28
        "q4_required_est": 12.5,
        "source": "Cục Thống kê (NSO), công bố 03/10/2026",
        "url": "https://nhandan.vn/tang-truong-gdp-9-thang-nam-2026-dat-901-nho-cac-chinh-sach-thuc-day-kinh-te-post992684.html",
    },
    "public_invest": {
        "plan_bn": 991931.6, "disbursed_bn": 642961.6, "pct": 62.9, "pct_yoy_pts": 12.9,
        "sep_vs_aug_speed": 1.58,
        "source": "Bộ Tài chính, lũy kế đến 30/09/2026",
        "url": "https://baochinhphu.vn/giai-ngan-von-dau-tu-cong-9-thang-nam-2026-mot-so-bo-nganh-dia-phuong-con-cham-102261003011740764.htm",
    },
    "cpi": {
        "yoy_sep": 5.08, "avg9m": 4.52, "core9m": 4.26, "target": 4.5,   # NA 2026 socio-economic resolution: CPI ~4.5% (baochinhphu 13/11/2025)
        "source": "Cục Thống kê (NSO), CPI tháng 9/2026",
        "url": "https://markettimes.vn/cpi-thang-9-2026-tang-0-62-chu-yeu-do-gia-xang-dau-va-hoc-phi-132253.html",
    },
}


# ─── Indicators ────────────────────────────────────────────────────
def rsi(c: list[float], n: int = 14) -> list[float | None]:
    out: list[float | None] = [None] * len(c)
    if len(c) <= n:
        return out
    gains = [max(c[i] - c[i - 1], 0) for i in range(1, n + 1)]
    losses = [max(c[i - 1] - c[i], 0) for i in range(1, n + 1)]
    ag, al = sum(gains) / n, sum(losses) / n
    out[n] = 100.0 if al == 0 else 100 - 100 / (1 + ag / al)
    for i in range(n + 1, len(c)):
        d = c[i] - c[i - 1]
        ag = (ag * (n - 1) + max(d, 0)) / n
        al = (al * (n - 1) + max(-d, 0)) / n
        out[i] = 100.0 if al == 0 else 100 - 100 / (1 + ag / al)
    return out


def sma(c: list[float], n: int) -> list[float | None]:
    out: list[float | None] = [None] * len(c)
    s = 0.0
    for i, x in enumerate(c):
        s += x
        if i >= n:
            s -= c[i - n]
        if i >= n - 1:
            out[i] = s / n
    return out


_PIV = 5          # pivot half-width (sessions)
_DB_WIN = 120     # look-back window for the two bottoms
_DB_GAP = 15      # min sessions between bottoms
_DB_TOL = 0.04    # bottoms within 4% of each other
_DB_RALLY = 0.06  # neckline ≥ 6% above the higher bottom


def _pivot_lows(lo: list[float]) -> list[int]:
    k = _PIV
    return [j for j in range(k, len(lo) - k) if lo[j] == min(lo[j - k:j + k + 1])]


def double_bottom_at(i: int, lo, hi, c, pivots: list[int]) -> dict | None:
    """Most recent valid double bottom using only data known at bar i (no look-ahead)."""
    known = [j for j in pivots if i - _DB_WIN <= j and j + _PIV <= i]
    for b in range(len(known) - 1, 0, -1):
        j2 = known[b]
        for a in range(b - 1, -1, -1):
            j1 = known[a]
            if j2 - j1 < _DB_GAP:
                continue
            l1, l2 = lo[j1], lo[j2]
            if abs(l2 - l1) / l1 > _DB_TOL:
                continue
            neck_idx = max(range(j1, j2 + 1), key=lambda k: hi[k])
            neck = hi[neck_idx]
            if neck < max(l1, l2) * (1 + _DB_RALLY):
                continue
            floor = min(l1, l2)
            if min(lo[j2:i + 1]) < floor * 0.97:      # broke down through the bottoms → invalid
                continue
            above = [k for k in range(j2 + 1, i + 1) if c[k] > neck]
            return {"j1": j1, "j2": j2, "neck_idx": neck_idx, "neck": neck, "floor": floor,
                    "breakout": above[0] if above else None,
                    "status": "confirmed" if above else "forming"}
        # only consider the latest second bottom
        break
    return None


# ─── Backtests ─────────────────────────────────────────────────────
def _wilson_low(k: int, n: int, z: float = 1.96) -> float | None:
    if n == 0:
        return None
    p = k / n
    den = 1 + z * z / n
    centre = p + z * z / (2 * n)
    adj = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n))
    return (centre - adj) / den


def _evidence(entries: list[int], c, lo, H: int, target: float, t: list[str]) -> dict:
    rets, mdds, rows = [], [], []
    for i in entries:
        if i + H >= len(c):
            continue
        r = c[i + H] / c[i] - 1
        mdd = min(lo[i + 1:i + H + 1]) / c[i] - 1
        rets.append(r)
        mdds.append(mdd)
        rows.append({"date": t[i], "ret": round(r * 100, 2), "mdd": round(mdd * 100, 2)})
    n = len(rets)
    k = sum(1 for r in rets if r >= target)
    srt = sorted(rets)
    return {
        "n": n, "wins": k,
        "win_rate": round(k / n * 100, 1) if n else None,
        "wilson_low": round(_wilson_low(k, n) * 100, 1) if n else None,
        "median_ret": round(srt[n // 2] * 100, 2) if n else None,
        "worst_ret": round(srt[0] * 100, 2) if n else None,
        "median_mdd": round(sorted(mdds)[n // 2] * 100, 2) if n else None,
        "events": rows[-12:],
        "verdict": _verdict(k, n),
    }


def _verdict(k: int, n: int) -> str:
    if n < MIN_SAMPLE:
        return "insufficient"
    return "pass" if k / n >= CERTAINTY_TARGET else "fail"


def _dedup(idx: list[int], gap: int) -> list[int]:
    out: list[int] = []
    for i in idx:
        if not out or i - out[-1] >= gap:
            out.append(i)
    return out


def analyze(symbol: str, horizon: int = 63, target_pct: float = 0.0, detail: bool = True) -> dict:
    horizon = max(5, min(int(horizon), MAX_HORIZON))
    target = target_pct / 100
    h = history(symbol)
    if not h or len(h["c"]) < 260:
        return {"symbol": symbol, "error": "Không đủ dữ liệu giá (cần ≥ 1 năm)"}
    t, o, hi, lo, c, v = h["t"], h["o"], h["h"], h["l"], h["c"], h["v"]
    n = len(c)
    last = n - 1
    r = rsi(c)
    ma50, ma200 = sma(c, 50), sma(c, 200)
    pivots = _pivot_lows(lo)
    meta = (symbol_meta() or {}).get(symbol, {})
    icb = meta.get("icb")

    # ── 1. RSI oversold ──
    cross_up = [i for i in range(15, n) if r[i - 1] is not None and r[i - 1] < 30 <= r[i]]
    rsi_now = r[last]
    recent_low = min(x for x in r[-10:] if x is not None)
    if rsi_now < 30:
        rsi_state = "oversold"
    elif cross_up and last - cross_up[-1] <= 10:
        rsi_state = "reversal"
    elif rsi_now < 35:
        rsi_state = "near"
    else:
        rsi_state = "none"
    ev_rsi = _evidence(_dedup(cross_up, 10), c, lo, horizon, target, t)

    # ── 2. Double bottom ──
    db_now = double_bottom_at(last, lo, hi, c, pivots)
    if db_now and db_now["status"] == "confirmed" and last - db_now["breakout"] > 15:
        db_now["status"] = "stale"          # breakout too long ago to still be an entry
    breakouts: list[int] = []
    seen = set()
    for i in range(_DB_WIN, n):
        d = double_bottom_at(i, lo, hi, c, pivots)
        if d and d["breakout"] == i and (d["j1"], d["j2"]) not in seen:
            seen.add((d["j1"], d["j2"]))
            breakouts.append(i)
    ev_db = _evidence(_dedup(breakouts, 10), c, lo, horizon, target, t)

    # ── 3. Year-end / calendar cycle: same calendar day in prior years ──
    today = date.fromisoformat(t[last])
    season_entries = []
    for y in sorted({int(x[:4]) for x in t}):
        if y >= today.year:
            continue
        try:
            anchor = today.replace(year=y).isoformat()
        except ValueError:
            anchor = date(y, today.month, 28).isoformat()
        idx = next((i for i, d in enumerate(t) if d >= anchor), None)
        if idx is not None and idx > 0 and t[idx][:4] == str(y):
            season_entries.append(idx)
    ev_season = _evidence(season_entries, c, lo, horizon, target, t)

    # ── Base rate: any day, same horizon (is a signal better than random entry?) ──
    ev_base = _evidence(list(range(200, n - horizon, 5)), c, lo, horizon, target, t)
    ev_base.pop("events", None)

    # ── 4/5. Macro themes (qualitative, sector-linked) ──
    pi = MACRO["public_invest"]
    theme_public = icb in PUBLIC_INVEST_ICB
    theme_growth = icb in GROWTH_ICB

    # ── Other angles ──
    def ret_n(k):
        return round((c[last] / c[last - k] - 1) * 100, 2) if last - k >= 0 else None
    daily = [c[i] / c[i - 1] - 1 for i in range(last - 59, last + 1)]
    mean = sum(daily) / len(daily)
    vol = math.sqrt(sum((x - mean) ** 2 for x in daily) / (len(daily) - 1)) * math.sqrt(252) * 100
    val20 = sum(c[i] * v[i] for i in range(last - 19, last + 1)) / 20 / 1e9   # bn VND / session
    hi52, lo52 = max(hi[-252:]), min(lo[-252:])

    checks = [
        {"key": "rsi", "label": "RSI quá bán",
         "state": "pass" if rsi_state in ("oversold", "reversal") else ("partial" if rsi_state == "near" else "fail"),
         "value": f"RSI14 = {rsi_now:.1f} (thấp nhất 10 phiên: {recent_low:.1f})",
         "evidence": ev_rsi},
        {"key": "db", "label": "Mô hình 2 đáy",
         "state": {"confirmed": "pass", "forming": "partial"}.get(db_now["status"] if db_now else "", "fail"),
         "value": (f"{ {'confirmed':'Đã vượt neckline','forming':'Đang hình thành','stale':'Đã breakout > 15 phiên'}[db_now['status']] } · "
                   f"đáy {db_now['floor']:,.0f} · neckline {db_now['neck']:,.0f}") if db_now else "Không phát hiện",
         "evidence": ev_db},
        {"key": "season", "label": "Chu kỳ cuối năm",
         "state": ("insufficient" if ev_season["n"] < 3 else
                   "pass" if (ev_season["win_rate"] or 0) >= 80 else
                   "partial" if (ev_season["win_rate"] or 0) >= 60 else "fail"),
         "value": f"Vào lệnh ngày {today:%d/%m} các năm trước: {ev_season['wins']}/{ev_season['n']} năm đạt mục tiêu",
         "evidence": ev_season},
        {"key": "public", "label": "Đầu tư công sắp giải ngân",
         "state": "pass" if theme_public else "fail",
         "value": (f"Ngành hưởng lợi trực tiếp · còn ~{(pi['plan_bn']-pi['disbursed_bn'])/1000:,.0f} nghìn tỷ cần giải ngân trong Q4"
                   if theme_public else "Ngành không hưởng lợi trực tiếp từ giải ngân đầu tư công"),
         "evidence": None},
        {"key": "growth", "label": "Tăng trưởng còn dư địa",
         "state": "pass" if theme_growth else "partial",
         "value": (f"9T: {MACRO['gdp']['ytd9m']}% vs mục tiêu ≥{MACRO['gdp']['target']:.0f}% → Q4 cần ~{MACRO['gdp']['q4_required_est']}% (ước tính)"
                   + (" · ngành nhạy với chu kỳ tăng trưởng" if theme_growth else " · ngành ít nhạy")),
         "evidence": None},
    ]

    # Best quantitative evidence among signals that are active right now
    active = [ck for ck in checks[:3] if ck["state"] in ("pass", "partial") and ck["evidence"]]
    best = max(active, key=lambda ck: (ck["evidence"]["n"] >= MIN_SAMPLE, ck["evidence"]["win_rate"] or 0), default=None)

    out = {
        "symbol": symbol, "name": meta.get("name", ""), "name_en": meta.get("name_en", ""), "board": meta.get("board"),
        "icb": icb, "sector": ICB_NAMES.get(icb or "", "—"),
        "as_of": t[last], "price": c[last], "chg_pct": round((c[last] / c[last - 1] - 1) * 100, 2),
        "horizon": horizon, "target_pct": target_pct,
        "eligibility": eligibility(symbol, v, listed_shares([symbol]).get(symbol)),
        "checks": checks,
        "score": sum({"pass": 1, "partial": 0.5}.get(ck["state"], 0) for ck in checks),
        "base": ev_base,
        "best": ({"key": best["key"], "label": best["label"], **{k: best["evidence"][k] for k in
                  ("n", "win_rate", "wilson_low", "verdict", "median_ret", "worst_ret")}} if best else None),
        "angles": {
            "rsi": round(rsi_now, 1), "rsi_state": rsi_state,
            "ma50": round(ma50[last], 2), "ma200": round(ma200[last], 2),
            "above_ma200": ma200[last] is not None and c[last] > ma200[last],
            "ret_1m": ret_n(21), "ret_3m": ret_n(63), "ret_12m": ret_n(252),
            "from_52w_high": round((c[last] / hi52 - 1) * 100, 2),
            "from_52w_low": round((c[last] / lo52 - 1) * 100, 2),
            "vol_ann": round(vol, 1), "value_20d_bn": round(val20, 1),
        },
    }
    if detail:
        out["outlook"] = return_ranges(c)
        out["research"] = research().get(symbol)
        k0 = max(0, n - 260)
        out["chart"] = {
            "t": t[k0:], "c": c[k0:], "h": hi[k0:], "l": lo[k0:],
            "ma50": [None if x is None else round(x, 2) for x in ma50[k0:]],
            "ma200": [None if x is None else round(x, 2) for x in ma200[k0:]],
            "rsi": [None if x is None else round(x, 2) for x in r[k0:]],
            "db": ({"j1": db_now["j1"] - k0, "j2": db_now["j2"] - k0, "neck": db_now["neck"],
                    "breakout": (db_now["breakout"] - k0) if db_now["breakout"] else None,
                    "status": db_now["status"]} if db_now and db_now["j1"] >= k0 else None),
        }
    return out


# ─── Outlook: historical return ranges (not a forecast) ────────────
OUTLOOK_WINDOWS = {"6m": 126, "12m": 252}


def return_ranges(c: list[float]) -> dict:
    """Distribution of past 6- and 12-month returns (every 5th session, so windows overlap less)."""
    out = {}
    last = c[-1]
    for key, w in OUTLOOK_WINDOWS.items():
        rets = sorted(c[i + w] / c[i] - 1 for i in range(0, len(c) - w, 5))
        n = len(rets)
        if n < 10:
            out[key] = {"n": n}
            continue
        pct = lambda q: rets[min(n - 1, int(q * n))]
        bands = {f"p{int(q*100)}": round(pct(q) * 100, 1) for q in (0.1, 0.25, 0.5, 0.75, 0.9)}
        out[key] = {"n": n, "sessions": w, **bands,
                    "win_rate": round(sum(1 for r in rets if r > 0) / n * 100, 1),
                    "price": {k: round(last * (1 + v / 100)) for k, v in bands.items()}}
    return out


# ─── Hand-curated research (invest_research.json, sourced, as-of dated) ─
_RESEARCH_FILE = __import__("pathlib").Path(__file__).with_name("invest_research.json")
_research_cache: dict = {"mtime": None, "data": {}}


def research() -> dict:
    """Reloads automatically when the JSON file is edited."""
    try:
        m = _RESEARCH_FILE.stat().st_mtime
        if m != _research_cache["mtime"]:
            _research_cache["data"] = json.loads(_RESEARCH_FILE.read_text(encoding="utf-8"))
            _research_cache["mtime"] = m
    except FileNotFoundError:
        _research_cache["data"] = {}
    except Exception as e:
        log.warning("invest_research.json invalid: %s", e)
    return _research_cache["data"]


def screen(group: str = "VN30", horizon: int = 63, target_pct: float = 0.0) -> dict:
    syms = group_members(group)
    listed_shares(syms)          # one batched call warms the cache for every row
    rows, errors = [], []
    for s in syms:
        try:
            a = analyze(s, horizon, target_pct, detail=False)
            if "error" in a:
                errors.append(f"{s}: {a['error']}")
                continue
            rows.append({k: a[k] for k in ("symbol", "name", "name_en", "sector", "price", "chg_pct", "score", "best", "angles", "eligibility")}
                        | {"checks": [{"key": ck["key"], "state": ck["state"]} for ck in a["checks"]]})
        except Exception as e:
            errors.append(f"{s}: {str(e)[:80]}")
    rows.sort(key=lambda r: (not r["eligibility"]["eligible"], -r["score"], -(r["best"]["win_rate"] if r["best"] else 0)))
    return {"group": group, "horizon": horizon, "target_pct": target_pct, "count": len(rows),
            "rows": rows, "errors": errors, "generated_at": datetime.now(timezone.utc).isoformat()}


def market() -> dict:
    h = history("VNINDEX", 1500)
    if not h:
        return {}
    c = h["c"]
    r = rsi(c)
    ma200 = sma(c, 200)
    last = len(c) - 1
    return {"symbol": "VNINDEX", "as_of": h["t"][last], "close": c[last],
            "chg_pct": round((c[last] / c[last - 1] - 1) * 100, 2),
            "rsi": round(r[last], 1), "ma200": round(ma200[last], 2),
            "ret_3m": round((c[last] / c[last - 63] - 1) * 100, 2),
            "from_52w_high": round((c[last] / max(h["h"][-252:]) - 1) * 100, 2),
            "spark": c[-120:], "outlook": return_ranges(c)}
