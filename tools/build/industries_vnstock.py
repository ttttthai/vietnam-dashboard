#!/usr/bin/env python3
"""
Build data/industries.json — ICB level-2 industry cards (HOSE + HNX + UPCoM) from vnstock_data (Vietcap / VCI).

Run on the user's Mac, in the sponsor venv (never with public-PyPI vnstock):
    VNSTOCK_TELEMETRY=off /tmp/vnenv/bin/python tools/build/industries_vnstock.py [--dry-run] [--refresh]
                                                   [--fin-top 25] [--no-finance] [--as-of YYYY-MM-DD] [--out PATH]

NOT RUNNABLE IN THE CLOUD SANDBOX: vnstock_data is not installed there (and must never be installed from public PyPI),
and iq.vietcap.com.vn (Finance) is blocked by that sandbox's proxy. The committed data/industries.json of 2026-11-01
was built in the sandbox from the Vietcap trading endpoints directly (see its _meta.method); this script rebuilds the
same schema from vnstock and additionally fills the fundamentals (P/E, P/B, ROE, dividend yield, quarterly growth).

* Key: read only by the library itself (env VNSTOCK_API_KEY or ~/.vnstock/api_key.json) — never read, printed,
  logged or written here.
* Classification: Listing(source='VCI').industries_icb() (ICB names VI/EN, level 2) and symbols_by_industries()
  (icb_code2 per symbol); exchange/type from symbols_by_exchange(). Universe = type STOCK on HSX/HNX/UPCOM.
* Market: Trading(source='VCI').price_board → close (match price, else reference), listed shares, foreign room.
  mcap = close × listed shares (bn VND). Stocks ex-rights whose new shares are not yet listed are understated
  (listed shares lag) — the build prints such suspects (price −20 % or more over 5 sessions with unchanged shares).
* Returns: Quote(source='VCI').history daily adjusted closes. ret_ytd = close(as_of) vs close on the last session of
  the previous year; ret_1m = vs the last session on/before the same day one month earlier. Industry figure =
  cap-weighted Σ(shares_now × close_now) / Σ(shares_now × adj_close_base) − 1 over constituents with both closes.
* Fundamentals (--fin-top N largest stocks per industry by cap; default 25): Finance(source='VCI', period='quarter')
  .ratio() and .income_statement(). Industry aggregates: P/E = Σmcap / Σ(mcap/PE) (aggregate earnings yield,
  loss-makers included when the source reports a negative P/E), P/B = Σmcap / Σ(mcap/PB), ROE = aggregate
  earnings / aggregate book (= P/B ÷ P/E), dividend yield = cap-weighted mean, rev/NPAT yoy = Σ latest quarter /
  Σ same quarter a year earlier − 1 over stocks reporting both (quarter = latest one reported by ≥ 50 % of the
  sample's cap). Coverage (share of industry cap in the sample) is written next to every aggregate.
* Values are only ever copied or aggregated from responses — null when not returned, never estimated.
* Descriptive notes (note_vi / note_en / note_src) are carried over from the existing file by ICB code: they are
  written by the Investing agent, not generated. Descriptive only — no recommendations.
* History (--history-from YEAR, default 2018; 0 skips): IND_ICB.history = year-end cap-weighted industry price
  indices over today's constituents (rebased 100 at end-YEAR, chain-linked yearly with weights = adjusted close ×
  current listed shares at the start of each year), calendar-year returns (last year = YTD), approximate year-end caps,
  constituents with data, VN-Index year-end closes, plus 24 month-ends rebased 100 at the first month. Pure function
  history_block(); method text in HISTORY_BASIS. Needs ~2,000 daily bars per stock (Quote.history start date).
* Every call: retries with backoff via vnstock_layer.call; responses cached under /tmp/vnstock_ind_cache (24 h).
"""
from __future__ import annotations

import argparse
import bisect
import calendar
import json
import os
import pickle
import re
import sys
import time
import unicodedata
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

os.environ["VNSTOCK_TELEMETRY"] = "off"
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
import vnstock_layer as V  # noqa: E402

CACHE = Path("/tmp/vnstock_ind_cache")
CACHE_TTL = 24 * 3600
OUT = ROOT / "data" / "industries.json"
RETRY = {"tries": 5, "base_wait": 2.0, "max_wait": 20.0}
BOARDS = {"HSX": "HOSE", "HOSE": "HOSE", "HNX": "HNX", "UPCOM": "UPCoM"}

# Fallback names (Vietcap ICB level 2 as used in the dashboard) when industries_icb() returns nothing
ICB2 = {
    "0500": ("Dầu khí", "Oil & Gas"), "1300": ("Hóa chất", "Chemicals"),
    "1700": ("Tài nguyên cơ bản", "Basic Resources"), "2300": ("Xây dựng và Vật liệu", "Construction & Materials"),
    "2700": ("Hàng & Dịch vụ công nghiệp", "Industrial Goods & Services"),
    "3300": ("Ô tô và phụ tùng", "Automobiles & Parts"), "3500": ("Thực phẩm và đồ uống", "Food & Beverage"),
    "3700": ("Hàng cá nhân & Gia dụng", "Personal & Household Goods"), "4500": ("Y tế", "Health Care"),
    "5300": ("Bán lẻ", "Retail"), "5500": ("Truyền thông", "Media"), "5700": ("Du lịch và Giải trí", "Travel & Leisure"),
    "6500": ("Viễn thông", "Telecommunications"), "7500": ("Điện, nước & xăng dầu khí đốt", "Utilities"),
    "8300": ("Ngân hàng", "Banks"), "8500": ("Bảo hiểm", "Insurance"), "8600": ("Bất động sản", "Real Estate"),
    "8700": ("Dịch vụ tài chính", "Financial Services"), "9500": ("Công nghệ Thông tin", "Technology"),
}
RATIO_PATS = {
    "pe": [r"^(pe|p_e|price_to_earnings?|p_e_ratio)$", r"(^|_)p_?e(_|$)", r"price_to_earn"],
    "pb": [r"^(pb|p_b|price_to_book|p_b_ratio)$", r"(^|_)p_?b(_|$)", r"price_to_book"],
    "div_yield": [r"^(dividend_yield|div_yield|ty_suat_co_tuc)(_pct|_percent)?$", r"dividend_yield|ty_suat_co_tuc"],
}
IS_PATS = {
    "revenue": [r"^(doanh_thu_thuan|net_revenue|net_sales|revenue|total_operating_income|tong_thu_nhap_hoat_dong)$",
                r"^doanh_thu_thuan", r"net_(revenue|sales)", r"tong_thu_nhap_hoat_dong|total_operating_income"],
    "npat": [r"^(loi_nhuan_sau_thue_cua_co_dong_cong_ty_me|net_profit_attributable.*|attributable_to_.*parent.*)$",
             r"loi_nhuan_sau_thue_cua_co_dong_(cong_ty_)?me|attributable.*(parent|owners)",
             r"^(loi_nhuan_sau_thue|net_profit|profit_after_tax)$"],
}


def norm(s) -> str:
    s = unicodedata.normalize("NFKD", str(s or "")).replace("đ", "d").replace("Đ", "D")
    s = "".join(c for c in s if not unicodedata.combining(c)).lower()
    s = re.sub(r"^([ivxlcdm]+|[0-9]+(\.[0-9]+)*|[a-z])[\.\)]\s+", "", s)
    return re.sub(r"[^a-z0-9]+", "_", s).strip("_")


def num(v) -> float | None:
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    return None if x != x else x


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def cached(key: str, fn, refresh: bool):
    CACHE.mkdir(parents=True, exist_ok=True)
    p = CACHE / (re.sub(r"[^A-Za-z0-9_.-]+", "_", key) + ".pkl")
    if not refresh and p.exists() and time.time() - p.stat().st_mtime < CACHE_TTL:
        with open(p, "rb") as f:
            return pickle.load(f)
    val = fn()
    if val is not None:
        with open(p, "wb") as f:
            pickle.dump(val, f)
    return val


def col(df, *names):
    for n in names:
        if n in df.columns:
            return n
    low = {norm(c): c for c in df.columns}
    for n in names:
        if norm(n) in low:
            return low[norm(n)]
    return None


# ─── classification ────────────────────────────────────────────────
def classification(mod, refresh: bool):
    L = lambda: mod.Listing(source="VCI")  # noqa: E731
    icb = cached("industries_icb", lambda: V.call(lambda: L().industries_icb(), "industries_icb", **RETRY), refresh)
    by_ind = cached("symbols_by_industries", lambda: V.call(lambda: L().symbols_by_industries(), "symbols_by_ind", **RETRY),
                    refresh)
    by_ex = cached("symbols_by_exchange", lambda: V.symbols_by_exchange(**RETRY), refresh)
    if by_ind is None or by_ex is None or not len(by_ind) or not len(by_ex):
        sys.exit("Listing(VCI).symbols_by_industries()/symbols_by_exchange() returned nothing — cannot classify")
    names = {k: {"vi": v[0], "en": v[1], "src": "fallback table"} for k, v in ICB2.items()}
    if icb is not None and len(icb):
        c_code, c_lvl = col(icb, "icb_code", "icbCode"), col(icb, "level")
        c_vi, c_en = col(icb, "icb_name", "icbName"), col(icb, "en_icb_name", "enIcbName")
        for _, r in icb.iterrows():
            code = str(r[c_code]).zfill(4)
            if c_lvl and str(r[c_lvl]) not in ("2", "2.0"):
                continue
            names[code] = {"vi": r[c_vi] if c_vi else names.get(code, {}).get("vi"),
                           "en": r[c_en] if c_en else names.get(code, {}).get("en"),
                           "src": "Listing(VCI).industries_icb()"}
    s_ind, s_c2 = col(by_ind, "symbol", "ticker"), col(by_ind, "icb_code2", "icbCode2")
    icb_of = {str(r[s_ind]): str(r[s_c2]).split(".")[0].zfill(4) for _, r in by_ind.iterrows() if num(r[s_c2]) is not None}
    e_sym, e_ex, e_type = col(by_ex, "symbol"), col(by_ex, "exchange", "board"), col(by_ex, "type")
    e_name = col(by_ex, "organ_short_name", "organ_name")
    e_name_en = col(by_ex, "en_organ_short_name", "en_organ_name")
    stocks = []
    for _, r in by_ex.iterrows():
        ex = str(r[e_ex]).upper()
        if (e_type and str(r[e_type]) != "STOCK") or ex not in BOARDS:
            continue
        s = str(r[e_sym])
        if s in icb_of:
            stocks.append({"sym": s, "icb": icb_of[s], "board": BOARDS[ex], "name": r[e_name] if e_name else s,
                           "name_en": r[e_name_en] if e_name_en else None})
    return names, stocks


# ─── market ────────────────────────────────────────────────────────
def board(mod, syms: list[str], refresh: bool) -> dict:
    out = {}
    for k in range(0, len(syms), 50):
        b = syms[k:k + 50]
        df = cached(f"board_{k}_{len(syms)}", lambda: V.call(
            lambda: mod.Trading(source="VCI", symbol=b[0]).price_board(b), "price_board", **RETRY), refresh)
        if df is None or not len(df):
            continue
        for _, r in df.iterrows():
            s = V._pick(r, ("listing", "symbol"))
            if not s:
                continue
            px = num(V._pick(r, ("match", "match_price"))) or num(V._pick(r, ("listing", "ref_price")))
            if px is not None and px < 1000:            # thousand-VND convention
                px *= 1000
            tr, cr = num(V._pick(r, ("match", "total_room"))), num(V._pick(r, ("match", "current_room")))
            out[str(s)] = {"px": px, "ls": num(V._pick(r, ("listing", "listed_share"))),
                           "date": V._pick(r, ("listing", "trading_date")),
                           "foreign_held": (tr - cr) if tr and cr is not None and tr >= cr else None}
    return out


def close_on_or_before(h: dict, day: str) -> float | None:
    best = None
    for t, c in zip(h["t"], h["c"]):
        if t <= day:
            best = c
        else:
            break
    return best


def returns(syms: list[str], as_of: str, refresh: bool, bars: int = 320) -> tuple[dict, dict, dict]:
    """→ (per-symbol 1M/YTD closes, bases, raw daily histories {sym: {t, c}} for the history block)."""
    d = date.fromisoformat(as_of)
    ytd_base = date(d.year - 1, 12, 31).isoformat()
    y, m = (d.year, d.month - 1) if d.month > 1 else (d.year - 1, 12)
    m1_base = date(y, m, min(d.day, calendar.monthrange(y, m)[1])).isoformat()   # same day one month earlier
    out, raw = {}, {}
    for i, s in enumerate(syms):
        h = cached(f"hist{bars}_{s}", lambda: V.history(s, bars, **RETRY), refresh)
        if not h or not h.get("c"):
            continue
        raw[s] = {"t": h["t"], "c": h["c"]}
        c0 = close_on_or_before(h, as_of)
        out[s] = {"c": c0, "ytd": close_on_or_before(h, ytd_base), "m1": close_on_or_before(h, m1_base),
                  "last5": h["c"][-6:]}
        if i % 100 == 0:
            print(f"  history {i}/{len(syms)}", flush=True)
    return out, {"ytd_base": ytd_base, "m1_base": m1_base}, raw


# ─── history block (year-end and month-end industry indices) ──────
HISTORY_BASIS = {
    "vi": ("Chỉ số giá ngành tự tính, gia quyền vốn hóa, theo các mã niêm yết HIỆN NAY (HOSE/HNX/UPCoM, phân ngành ICB "
           "cấp 2 của Vietcap). Giá: giá đóng cửa điều chỉnh hằng ngày của Vietcap (chart gap-chart; cùng nguồn vnstock "
           "Quote VCI) tại phiên cuối mỗi năm (năm {last} = phiên {as_of}, tức từ đầu năm đến nay). Lợi suất năm Y = "
           "Σ(số CP niêm yết hiện tại × giá ĐC cuối Y) ÷ Σ(số CP hiện tại × giá ĐC cuối Y−1) − 1, chỉ gồm mã có giá ở cả hai "
           "thời điểm; tức là gia quyền theo vốn hóa đầu năm ước tính (giá ĐC × số CP hiện tại), tái cân bằng mỗi năm. "
           "Chỉ số = 100 tại cuối {first} (hoặc năm đầu tiên có dữ liệu), nối chuỗi các lợi suất năm. Mã chưa có giá thì "
           "bỏ qua; không nội suy. Chỉ là chỉ số giá (không cộng cổ tức tiền mặt trừ khi Vietcap đã điều chỉnh giá cho cổ "
           "tức tiền). Thiên lệch sống sót: chỉ gồm mã còn niêm yết hôm nay — mã đã hủy niêm yết/bị đình chỉ hẳn không có "
           "trong chuỗi, nên các năm đầu chỉ phản ánh những công ty tồn tại đến nay. Vốn hóa cuối năm (mcap_bn_vnd) chỉ "
           "là XẤP XỈ = giá ĐC × số CP hiện tại: giá ĐC đã chia cho cổ tức cổ phiếu/thưởng nên phần này gần đúng, nhưng "
           "phát hành thêm (chào bán, riêng lẻ, ESOP, sáp nhập) làm phóng đại vốn hóa quá khứ, và mã nay không còn niêm "
           "yết bị thiếu — không dùng như vốn hóa thị trường chính thức. n_with_data = số mã có giá tại cuối năm đó; "
           "cap_coverage_pct = tỷ trọng vốn hóa hôm nay của các mã đó."),
    "en": ("Self-computed cap-weighted industry price indices over TODAY's listed constituents (HOSE/HNX/UPCoM, Vietcap "
           "ICB level 2). Prices: Vietcap adjusted daily closes (chart gap-chart; the same feed as vnstock Quote VCI) on "
           "the last session of each year ({last} = session of {as_of}, i.e. year-to-date). Return in year Y = Σ(current "
           "listed shares × adj. close end-Y) ÷ Σ(current listed shares × adj. close end-(Y−1)) − 1 over stocks with "
           "both closes, i.e. weights = estimated year-start market cap (adj. close × current shares), rebalanced "
           "yearly. Index = 100 at end-{first} (or the first year with data), chain-linked from the yearly returns. Stocks "
           "without a price are skipped; nothing is interpolated. Price index (no reinvested cash dividends unless "
           "Vietcap's adjustment already strips them from past prices). Survivorship bias: only stocks listed today — "
           "delisted names are absent, so early years show only the companies that survived to today. Year-end market "
           "cap (mcap_bn_vnd) is APPROXIMATE = adj. close × current shares: adjusted prices already divide out stock "
           "dividends/bonus shares, but new issuance since (rights, private placements, ESOP, mergers) inflates past "
           "caps and delisted stocks are missing — not an official market capitalisation. n_with_data = constituents "
           "with a price at that year-end; cap_coverage_pct = their share of today's industry market cap."),
}


def _cob(t: list[str], c: list, day: str):
    """Last close on/before day (t ascending ISO dates); None when no bar on/before day."""
    i = bisect.bisect_right(t, day) - 1
    return c[i] if i >= 0 and c[i] else None


FLIP_JUMP, FLIP_REVERT, FLIP_MAX = 1.5, 5, 3


def _flip_windows(t: list[str], c: list) -> list[tuple[str, str]]:
    """Feed artefacts: a daily move beyond ×1.5 / ÷1.5 that returns to within 1 % of the pre-jump close within
    FLIP_REVERT sessions (seen on a few illiquid UPCoM series, e.g. DNN oscillating between two levels).
    → [(first bad date, first good date again)]."""
    out = []
    for i in range(1, len(c) - 1):
        a, b = c[i - 1], c[i]
        if not a or not b or 1 / FLIP_JUMP <= b / a <= FLIP_JUMP:
            continue
        for k in range(i + 1, min(i + 1 + FLIP_REVERT, len(c))):
            if c[k] and abs(c[k] / a - 1) < 0.01:
                out.append((t[i], t[k]))
                break
    return out


def history_block(stocks: list[dict], hist: dict, as_of: str, first_year: int = 2018, n_months: int = 24,
                  index_hist: dict | None = None) -> dict:
    """Pure function (no network). stocks: [{sym, icb, mcap (bn VND, today), ls (listed shares today)}];
    hist: {sym: {t: [ISO dates ascending], c: [adjusted closes]}}; index_hist: {"VNINDEX": {t, c}}.
    Data screen (skip, never repair): series with ≥ FLIP_MAX flips are dropped; a single checkpoint close that falls
    inside a flip window is treated as missing for that checkpoint."""
    d = date.fromisoformat(as_of)
    years = list(range(first_year, d.year + 1))
    yend = [f"{y}-12-31" for y in years[:-1]] + [as_of]
    k0 = d.year * 12 + d.month - 1 - (n_months - 1)                 # n_months ending at as_of's month
    months, (y, m) = [], (k0 // 12, k0 % 12 + 1)
    for _ in range(n_months):
        months.append(f"{y}-{m:02d}")
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    mend = [f"{x}-{calendar.monthrange(int(x[:4]), int(x[5:]))[1]:02d}" for x in months]
    mend[-1] = min(mend[-1], as_of)
    mem_all, dropped_series, dropped_closes = [], [], []
    for s in stocks:
        h = hist.get(s["sym"])
        if not s.get("mcap") or not s.get("ls") or not h or not h.get("t"):
            mem_all.append({**s, "mcap": s.get("mcap") or 0, "y": [None] * len(yend), "m": [None] * len(mend)})
            continue
        t = [str(x)[:10] for x in h["t"]]
        fw = _flip_windows(t, h["c"])
        if len(fw) >= FLIP_MAX:
            dropped_series.append({"ticker": s["sym"], "icb": s["icb"], "flips": len(fw),
                                   "mcap_bn_vnd": round(s["mcap"])})
            mem_all.append({**s, "y": [None] * len(yend), "m": [None] * len(mend)})
            continue

        def get(day, t=t, c=h["c"], fw=fw, sym=s["sym"]):
            i = bisect.bisect_right(t, day) - 1           # bar actually used for this checkpoint
            if i < 0 or not c[i]:
                return None
            if any(a <= t[i] < b for a, b in fw):
                rec = {"ticker": sym, "checkpoint": day, "bar": t[i]}
                if rec not in dropped_closes:
                    dropped_closes.append(rec)
                return None
            return c[i]
        mem_all.append({**s, "y": [get(x) for x in yend], "m": [get(x) for x in mend]})

    def series(mem, key, nper, full=True):
        cap_now = sum(r["mcap"] or 0 for r in mem)
        idx, ret, mc, n, cov = [], [], [], [], []
        for k in range(nper):
            have = [r for r in mem if r[key][k]]
            n.append(len(have))
            cov.append(round(100 * sum(r["mcap"] for r in have) / cap_now, 1) if cap_now else None)
            mc.append(round(sum(r[key][k] * r["ls"] for r in have) / 1e9) if have else None)
            if not idx or idx[-1] is None:
                ret.append(None)
                idx.append(100.0 if have else None)
                continue
            pair = [r for r in have if r[key][k - 1]]
            base = sum(r[key][k - 1] * r["ls"] for r in pair)
            if not base:
                ret.append(None)
                idx.append(None)
                continue
            rr = sum(r[key][k] * r["ls"] for r in pair) / base - 1
            ret.append(round(100 * rr, 2))
            idx.append(idx[-1] * (1 + rr))
        idx = [None if v is None else round(v, 2) for v in idx]
        if not full:
            return idx
        return {"index": idx, "ret_pct": ret, "mcap_bn_vnd": mc, "n_with_data": n, "cap_coverage_pct": cov,
                "low_coverage_years": [years[k] for k, v in enumerate(cov) if v is not None and v < 60]}

    codes = sorted({r["icb"] for r in mem_all})
    by_icb = {c: series([r for r in mem_all if r["icb"] == c], "y", len(years)) for c in codes}
    market = series(mem_all, "y", len(years))
    vh = (index_hist or {}).get("VNINDEX")
    vt = [str(x)[:10] for x in vh["t"]] if vh else []
    market["vnindex"] = [_cob(vt, vh["c"], x) for x in yend] if vh else [None] * len(yend)
    vv = market["vnindex"]
    market["vnindex_ret_pct"] = [None] + [round(100 * (b / a - 1), 2) if a and b else None for a, b in zip(vv, vv[1:])]
    dd = f"{as_of[8:10]}/{as_of[5:7]}"
    return {
        "years": years, "year_end_dates": yend, "ytd_year": years[-1],
        "labels_vi": [str(y) for y in years[:-1]] + [f"{years[-1]} (đến {dd})"],
        "labels_en": [str(y) for y in years[:-1]] + [f"{years[-1]} YTD"],
        "source": "Vietcap chart/OHLCChart/gap-chart adjusted daily closes (vnstock Quote(source='VCI').history); "
                  "current listed shares and ICB codes as in IND_ICB.industries. vnindex = VN-Index close at each "
                  "year-end (same feed); vnindex_ret_pct ≈ derived from those closes.",
        "basis_vi": HISTORY_BASIS["vi"].format(
            first=first_year, last=years[-1], as_of=as_of),
        "basis_en": HISTORY_BASIS["en"].format(first=first_year, last=years[-1], as_of=as_of),
        "by_icb": by_icb, "market": market,
        "data_screen": {"rule": f"daily move beyond ×{FLIP_JUMP} or ÷{FLIP_JUMP} that reverts to within 1% of the "
                                f"prior close within {FLIP_REVERT} sessions = feed artefact; ≥{FLIP_MAX} such flips → "
                                "series dropped; otherwise a year-/month-end close inside a flip is treated as missing",
                        "series_dropped": dropped_series, "closes_dropped": dropped_closes},
        "monthly": {"months": months, "month_end_dates": mend, "base": f"100 = {months[0]} month-end",
                    "by_icb": {c: series([r for r in mem_all if r["icb"] == c], "m", len(months), False) for c in codes},
                    "market": series(mem_all, "m", len(months), False),
                    "vnindex": [_cob(vt, vh["c"], x) for x in mend] if vh else [None] * len(mend)},
    }


# ─── fundamentals ──────────────────────────────────────────────────
def pick_rows(df, pats: dict) -> dict[str, dict[str, float]]:
    """→ {period: {field: value}} from a vnstock statement/ratio frame (long or wide)."""
    from tools.build.banks_vnstock import period_key, to_long  # reuse the bank builder's parsers
    out: dict[str, dict] = {}
    rows = to_long(df)
    for f, plist in pats.items():
        for pat in plist:
            for per, names, val, _ in rows:
                pk = period_key(per, "quarter")
                if pk is None or out.get(pk, {}).get(f) is not None:
                    continue
                if any(re.search(pat, norm(n)) for n in names if n is not None):
                    x = num(val)
                    if x is not None:
                        out.setdefault(pk, {})[f] = x
    return out


def fundamentals(sym: str, refresh: bool) -> dict:
    r = cached(f"ratio_{sym}", lambda: V.finance(sym, "ratio", "quarter", "VCI", **RETRY), refresh)
    i = cached(f"is_{sym}", lambda: V.finance(sym, "income_statement", "quarter", "VCI", **RETRY), refresh)
    rat = pick_rows(r, RATIO_PATS) if r is not None and len(r) else {}
    inc = pick_rows(i, IS_PATS) if i is not None and len(i) else {}
    last = rat[max(rat)] if rat else {}
    dy = last.get("div_yield")
    if dy is not None and abs(dy) < 1:                   # fraction → percent
        dy *= 100
    return {"pe": last.get("pe"), "pb": last.get("pb"), "div_yield": dy, "ratio_period": max(rat) if rat else None,
            "inc": inc}


def prev_year_q(pk: str) -> str:
    y, q = pk.split("-Q")
    return f"{int(y) - 1}-Q{q}"


def aggregate_fund(members: list[dict], ind_cap: float) -> dict:
    cap = lambda m: m["mcap"] or 0  # noqa: E731
    pe_s = [m for m in members if m.get("pe")]
    pb_s = [m for m in members if m.get("pb") and m["pb"] > 0]
    out = {"pe": None, "pb": None, "roe_pct": None, "div_yield_pct": None, "rev_yoy_pct": None, "npat_yoy_pct": None,
           "growth_quarter": None, "coverage_pct": {}}
    if pe_s:
        earn = sum(cap(m) / m["pe"] for m in pe_s)
        if earn > 0:
            out["pe"] = round(sum(cap(m) for m in pe_s) / earn, 1)
        out["coverage_pct"]["pe"] = round(100 * sum(cap(m) for m in pe_s) / ind_cap, 1)
    if pb_s:
        out["pb"] = round(sum(cap(m) for m in pb_s) / sum(cap(m) / m["pb"] for m in pb_s), 2)
        out["coverage_pct"]["pb"] = round(100 * sum(cap(m) for m in pb_s) / ind_cap, 1)
    both = [m for m in members if m.get("pe") and m.get("pb") and m["pb"] > 0]
    if both:
        e, b = sum(cap(m) / m["pe"] for m in both), sum(cap(m) / m["pb"] for m in both)
        out["roe_pct"] = round(100 * e / b, 1) if b else None
    dy = [m for m in members if m.get("div_yield") is not None]
    if dy:
        out["div_yield_pct"] = round(sum(cap(m) * m["div_yield"] for m in dy) / sum(cap(m) for m in dy), 2)
        out["coverage_pct"]["div_yield"] = round(100 * sum(cap(m) for m in dy) / ind_cap, 1)
    # growth quarter = latest quarter reported by ≥ 50 % of the sample's cap
    samp = sum(cap(m) for m in members) or 1
    qs = sorted({pk for m in members for pk in (m.get("inc") or {})}, reverse=True)
    for pk in qs:
        rep = [m for m in members if pk in (m.get("inc") or {})]
        if sum(cap(m) for m in rep) / samp < 0.5:
            continue
        out["growth_quarter"] = pk
        for f, key in (("revenue", "rev_yoy_pct"), ("npat", "npat_yoy_pct")):
            pairs = [(m["inc"][pk].get(f), m["inc"].get(prev_year_q(pk), {}).get(f)) for m in rep]
            pairs = [(a, b) for a, b in pairs if a is not None and b is not None]
            base = sum(b for _, b in pairs)
            if pairs and base > 0:
                out[key] = round(100 * (sum(a for a, _ in pairs) / base - 1), 1)
        out["coverage_pct"]["growth"] = round(100 * sum(cap(m) for m in rep) / ind_cap, 1)
        break
    return out


# ─── main ──────────────────────────────────────────────────────────
def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="print the summary, do not write")
    ap.add_argument("--refresh", action="store_true", help="ignore the /tmp cache")
    ap.add_argument("--fin-top", type=int, default=25, help="largest N stocks per industry for fundamentals")
    ap.add_argument("--no-finance", action="store_true")
    ap.add_argument("--as-of", help="price date (default: latest board trading date)")
    ap.add_argument("--out", default=str(OUT))
    ap.add_argument("--history-from", type=int, default=2018,
                    help="first year-end of IND_ICB.history (index = 100 there); 0 = skip the history block")
    a = ap.parse_args()

    st = V.status()
    if not st["active"]:
        sys.exit(f"vnstock layer unusable: {json.dumps({k: st[k] for k in ('importable', 'import_error', 'key_configured')})}")
    mod = V._module()
    fetched_at = now_iso()
    names, stocks = classification(mod, a.refresh)
    syms = [s["sym"] for s in stocks]
    print(f"universe: {len(syms)} stocks in {len({s['icb'] for s in stocks})} ICB-2 industries", flush=True)
    bd = board(mod, syms, a.refresh)
    dates = sorted({v["date"] for v in bd.values() if v.get("date")})
    as_of = a.as_of or (str(dates[-1])[:10] if dates else date.today().isoformat())
    bars = 320
    if a.history_from:                               # sessions from the first year-end to today, + margin
        bars = max(320, int((date.today() - date(a.history_from, 12, 31)).days * 0.70) + 120)
    rets, bases, raw = returns(syms, as_of, a.refresh, bars)

    for s in stocks:
        b, r = bd.get(s["sym"], {}), rets.get(s["sym"], {})
        s.update(px=b.get("px"), ls=b.get("ls"), fh=b.get("foreign_held"))
        s["mcap"] = s["px"] * s["ls"] / 1e9 if s["px"] and s["ls"] else None
        s["r_ytd"] = r["c"] / r["ytd"] - 1 if r.get("c") and r.get("ytd") else None
        s["r_m1"] = r["c"] / r["m1"] - 1 if r.get("c") and r.get("m1") else None
        l5 = r.get("last5") or []
        if len(l5) >= 2 and l5[0] and l5[-1] / l5[0] < 0.8:
            print(f"  check {s['sym']}: −{100 * (1 - l5[-1] / l5[0]):.0f}% over 5 sessions — ex-rights with unlisted "
                  f"new shares? (mcap may be understated)")

    mkt = sum(s["mcap"] or 0 for s in stocks)
    usable = (not a.no_finance) and V.host_reachable(V.FINANCE_HOSTS["VCI"])[0]
    inds = []
    for code in sorted({s["icb"] for s in stocks}):
        mem = sorted([s for s in stocks if s["icb"] == code], key=lambda s: -(s["mcap"] or 0))
        cap = sum(s["mcap"] or 0 for s in mem)

        def cw(key):
            m = [s for s in mem if s.get(key) is not None and s["mcap"]]
            if not m:
                return None, 0.0
            now = sum(s["mcap"] for s in m)
            base = sum(s["mcap"] / (1 + s[key]) for s in m)
            return round(100 * (now / base - 1), 2), round(100 * now / cap, 1) if cap else 0.0
        r1, c1 = cw("r_m1")
        ry, cy = cw("r_ytd")
        fo = [s for s in mem if s.get("fh") is not None and s["mcap"] and s["ls"]]
        fpct = round(100 * sum(s["fh"] * s["px"] for s in fo) / 1e9 / sum(s["mcap"] for s in fo), 1) if fo else None
        fund = {}
        if usable:
            for s in mem[:a.fin_top]:
                s.update(fundamentals(s["sym"], a.refresh))
            fund = aggregate_fund(mem[:a.fin_top], cap or 1)
        nm = names.get(code, {"vi": None, "en": None, "src": None})
        inds.append({
            "icb": code, "name_vi": nm["vi"], "name_en": nm["en"], "n_listed": len(mem),
            "n_by_exchange": {b: sum(1 for s in mem if s["board"] == b) for b in ("HOSE", "HNX", "UPCoM")},
            "mcap_bn_vnd": round(cap), "mcap_share_pct": round(100 * cap / mkt, 2) if mkt else None,
            "ret_1m_pct": r1, "ret_ytd_pct": ry, "ret_coverage_pct": {"1m": c1, "ytd": cy},
            "pe": fund.get("pe"), "pb": fund.get("pb"), "div_yield_pct": fund.get("div_yield_pct"),
            "roe_pct": fund.get("roe_pct"), "rev_yoy_pct": fund.get("rev_yoy_pct"),
            "npat_yoy_pct": fund.get("npat_yoy_pct"), "growth_quarter": fund.get("growth_quarter"),
            "fund_coverage_pct": fund.get("coverage_pct") or None,
            "foreign_own_pct": fpct,
            "top": [{"ticker": s["sym"], "name": s["name"], "exchange": s["board"],
                     "mcap_bn_vnd": round(s["mcap"]) if s["mcap"] else None,
                     "ret_ytd_pct": round(100 * s["r_ytd"], 2) if s.get("r_ytd") is not None else None}
                    for s in mem[:3]],
        })

    old = {}
    try:
        old = {i["icb"]: i for i in json.loads(Path(a.out).read_text(encoding="utf-8"))["IND_ICB"]["industries"]}
    except Exception:
        pass
    for i in inds:                                   # hand-written descriptive notes are kept, never generated
        o = old.get(i["icb"], {})
        i.update({k: o.get(k) for k in ("note_vi", "note_en", "note_src")})
    inds.sort(key=lambda i: -(i["mcap_bn_vnd"] or 0))

    hist = None
    if a.history_from:
        vni = cached(f"hist{bars}_VNINDEX", lambda: V.history("VNINDEX", bars, **RETRY), a.refresh)
        hist = history_block(stocks, raw, as_of, a.history_from,
                             index_hist={"VNINDEX": vni} if vni and vni.get("c") else None)
        early = [s["sym"] for s in stocks if s["sym"] in raw and raw[s["sym"]]["t"]
                 and str(raw[s["sym"]]["t"][0])[:10] > f"{a.history_from}-12-31" and (s["mcap"] or 0) > 50_000]
        if early:
            print(f"  note: large caps whose history starts after end-{a.history_from} (new listings, or the source "
                  f"returned fewer bars): {early[:15]}")
        hm = hist["market"]
        print(f"\nhistory {hist['years'][0]}–{hist['years'][-1]} (last = YTD {as_of}) · market index {hm['index']}")
        print(f"  cap coverage % {hm['cap_coverage_pct']} · VN-Index {hm['vnindex']}")

    print(f"\nas_of {as_of} · market {mkt:,.0f} bn VND · finance {'VCI' if usable else 'not reachable/skipped'}")
    print(f"{'icb':<6}{'industry':<32}{'n':>5}{'mcap bn':>12}{'share%':>8}{'1m%':>8}{'ytd%':>8}{'P/E':>7}{'P/B':>6}")
    for i in inds:
        f = lambda v, d=1: "—" if v is None else f"{v:.{d}f}"  # noqa: E731
        print(f"{i['icb']:<6}{(i['name_en'] or '')[:31]:<32}{i['n_listed']:>5}{i['mcap_bn_vnd']:>12,}"
              f"{f(i['mcap_share_pct'], 2):>8}{f(i['ret_1m_pct']):>8}{f(i['ret_ytd_pct']):>8}{f(i['pe']):>7}{f(i['pb'], 2):>6}")
    if a.dry_run:
        print("\n--dry-run: nothing written")
        return

    try:
        doc = json.loads(Path(a.out).read_text(encoding="utf-8"))
    except Exception:
        doc = {}
    meta = doc.get("_meta") or {}
    meta.update({"owner": "Investing agent", "as_of": as_of, "fetched_at": fetched_at,
                 "builder": "tools/build/industries_vnstock.py",
                 "sources": [f"vnstock_data {st['version']} via VCI: Listing.industries_icb/symbols_by_industries/"
                             "symbols_by_exchange, Trading.price_board, Quote.history"
                             + (", Finance.ratio/income_statement (quarter)" if usable else "")]})
    ind_doc = doc.get("IND_ICB") or {}
    ind_doc.update({"as_of": as_of, "exchanges": ["HOSE", "HNX", "UPCoM"], "industries": inds,
                    "market": {"n_listed": len(stocks), "mcap_bn_vnd": round(mkt)},
                    "return_bases": {"ytd": bases["ytd_base"], "1m": bases["m1_base"]}})
    if hist is not None:
        ind_doc["history"] = hist
    doc.update({"_meta": meta, "IND_ICB": ind_doc})
    Path(a.out).write_text(json.dumps(doc, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
    print(f"\nwrote {a.out}")


if __name__ == "__main__":
    main()
