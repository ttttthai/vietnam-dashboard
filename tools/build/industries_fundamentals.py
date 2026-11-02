#!/usr/bin/env python3
"""
Build IND_ICB.fundamentals in data/industries.json — revenue, net profit, total assets and equity per ICB level-2
industry and for the whole market, fiscal years 2018–2025 plus the latest trailing four quarters (TTM).

Source: Vietcap IQ financial statements, served through the Vietcap trading host (iq.vietcap.com.vn itself is not
reachable from the cloud sandbox; the same service answers under trading.vietcap.com.vn/api/iq-insight-service):
    GET https://trading.vietcap.com.vn/api/iq-insight-service/v1/company/{SYM}/financial-statement?section=INCOME_STATEMENT
    GET https://trading.vietcap.com.vn/api/iq-insight-service/v1/company/{SYM}/financial-statement?section=BALANCE_SHEET
    (field labels: .../company/{SYM}/financial-statement/metrics). Values are VND; this script converts to bn VND.
No vnstock, no API key: plain urllib with browser-like headers, one request per stock and section.

Usage (pure stdlib):
    # 1) pull raw statements (≤ 6 threads, backoff on 418/429); raw responses are stored gzipped, one file per call
    python3 tools/build/industries_fundamentals.py pull --stocks STOCKS.json --raw RAW_DIR [--threads 5]
    # 2) build the block and write it into data/industries.json (only IND_ICB.fundamentals is replaced/added)
    python3 tools/build/industries_fundamentals.py build --stocks STOCKS.json --raw RAW_DIR [--dry-run]
          [--out data/industries.json] [--ttm-min-cov 80]
    # 3) valuation: card pe / pb / roe_pct and market pe / pb (only those card/market keys + basis text change)
    python3 tools/build/industries_fundamentals.py valuation --stocks STOCKS.json --raw RAW_DIR [--dry-run]

STOCKS.json: list of {"sym", "icb" (ICB level-2 code, as IND_ICB.industries), "mcap" (bn VND, today's cap — the
same price-board snapshot as IND_ICB.industries), "name"} — the 1,518 listed stocks used for IND_ICB.

Method (also written into the block as basis_vi / basis_en):
* Constituents = stocks listed today (survivorship), classified by today's ICB code.
* Statement template detected from the data: bank (isb38/isb27 present), insurer (isi* present), securities
  (iss* present), otherwise corporate.
* revenue: corporate isa3 net sales; bank isb38 total operating income; securities isa3 net operating revenue
  (fallback isa1 operating revenue); insurer isi64 net revenue from insurance business (financial income excluded).
* net profit: isa22 profit after tax attributable to parent shareholders; isa20 PAT where isa22 is missing or zero
  while isa20 is not (counted in pat_fallback_n).
* total assets: bsa53. equity: parent shareholders' equity = bsa78 − bsa210 when bsa78 already includes minority
  interests (bsa54 + bsa78 = bsa96), else bsa78 (banks: minority shown separately).
* Annual = lengthReport 5 rows (Vietcap's fiscal-year label yearReport). TTM = sum of the four latest quarters
  (lengthReport 1–4) ending at the TTM quarter; balance-sheet items at that quarter end.
* A company-period counts only if revenue, net profit and total assets are all present; all sums and ratios of that
  period use the same set. Missing → skipped, never interpolated.
* Ratios are aggregates: margin = Σprofit ÷ Σrevenue, ROA = Σprofit ÷ Σassets (year-end), ROE = Σprofit ÷ Σequity
  (year-end, companies with equity > 0). YoY growth is like-for-like: Σ over companies with data in both periods.
"""
from __future__ import annotations

import argparse
import calendar
import collections
import concurrent.futures as cf
import gzip
import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import date

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DEFAULT_OUT = os.path.join(ROOT, "data", "industries.json")
BASE = "https://trading.vietcap.com.vn/api/iq-insight-service/v1/company/{}/financial-statement?section={}"
HDR = {"User-Agent": "Mozilla/5.0", "Referer": "https://trading.vietcap.com.vn/",
       "Origin": "https://trading.vietcap.com.vn", "Accept-Encoding": "gzip"}
SECTIONS = ("INCOME_STATEMENT", "BALANCE_SHEET")
YEARS = list(range(2018, 2026))
LOW_COV = 60.0
BN = 1e9
DISCLAIMER = "Thông tin mang tính mô tả, không phải khuyến nghị đầu tư. / Descriptive information only, not investment advice."


# ----------------------------------------------------------------------------------------------------------- pull
def _fetch(sym: str, sec: str, raw_dir: str, stats: dict) -> str:
    fn = os.path.join(raw_dir, f"{sym}_{sec}.json.gz")
    if os.path.exists(fn):
        return "cached"
    last = None
    for i in range(7):
        try:
            req = urllib.request.Request(BASE.format(sym, sec), headers=HDR)
            with urllib.request.urlopen(req, timeout=60) as r:
                raw = r.read()
            if raw[:2] == b"\x1f\x8b":
                raw = gzip.decompress(raw)
            d = json.loads(raw)
            with gzip.open(fn, "wb") as f:
                f.write(raw)
            return "ok" if d.get("data") else "nodata"
        except urllib.error.HTTPError as e:
            last = e.code
            if e.code in (418, 429, 503):
                stats["throttled"] += 1
                time.sleep(5 * (i + 1))
                continue
            if e.code in (400, 404):
                with gzip.open(fn, "wb") as f:
                    f.write(json.dumps({"_http": e.code}).encode())
                return f"http{e.code}"
            time.sleep(2 * (i + 1))
        except Exception as e:  # network hiccup
            last = str(e)[:80]
            time.sleep(2 * (i + 1))
    return f"ERR {last}"


def pull(stocks_path: str, raw_dir: str, threads: int, sections=SECTIONS, only=None) -> None:
    os.makedirs(raw_dir, exist_ok=True)
    syms = [r["sym"] for r in json.load(open(stocks_path, encoding="utf-8"))]
    if only:
        syms = [s for s in syms if s in set(only)]
    jobs = [(s, sec) for s in syms for sec in sections]
    stats = collections.Counter()
    with cf.ThreadPoolExecutor(max(1, min(threads, 6))) as ex:
        futs = [ex.submit(_fetch, s, sec, raw_dir, stats) for s, sec in jobs]
        for k, f in enumerate(cf.as_completed(futs)):
            stats[f.result().split()[0]] += 1
            if k % 200 == 0:
                print(k, len(jobs), dict(stats), flush=True)
    print("done", dict(stats))


# ---------------------------------------------------------------------------------------------------------- parse
def _load(raw_dir: str, sym: str, sec: str):
    fn = os.path.join(raw_dir, f"{sym}_{sec}.json.gz")
    if not os.path.exists(fn):
        return None
    with gzip.open(fn, "rb") as f:
        d = json.loads(f.read())
    return d.get("data") or None


def _latest_rows(rows):
    """Index rows by (yearReport, lengthReport); if duplicated keep the most recently updated."""
    out = {}
    for r in rows or []:
        k = (r.get("yearReport"), r.get("lengthReport"))
        if None in k:
            continue
        if k not in out or (r.get("updateDate") or "") > (out[k].get("updateDate") or ""):
            out[k] = r
    return out


def _nz(v):
    return v is not None and v != 0


def detect_template(is_rows) -> str:
    rows = list(is_rows.values())
    if any(_nz(r.get("isb38")) or _nz(r.get("isb27")) for r in rows):
        return "bank"
    if any(_nz(r.get(k)) for r in rows for k in r if k.startswith("isi")):
        return "insurer"
    if any(_nz(r.get(k)) for r in rows for k in r if k.startswith("iss")):
        return "securities"
    return "corporate"


def revenue_of(r, tpl):
    if tpl == "bank":
        return r.get("isb38")
    if tpl == "insurer":
        return r.get("isi64")
    if tpl == "securities":
        v = r.get("isa3")
        return v if _nz(v) else r.get("isa1")
    return r.get("isa3")


def profit_of(r):
    """(value, used_fallback)"""
    p, pat = r.get("isa22"), r.get("isa20")
    if p is not None and (p != 0 or not _nz(pat)):
        return p, False
    if pat is not None:
        return pat, True
    return None, False


def equity_of(b):
    eq, liab, tot, mi = b.get("bsa78"), b.get("bsa54"), b.get("bsa96"), b.get("bsa210") or 0
    if eq is None:
        return None
    if liab is not None and tot and mi and abs(liab + eq - tot) <= 0.001 * abs(tot):
        return eq - mi            # bsa78 includes minority interests (corporate VAS layout)
    return eq                     # minority shown outside bsa78 (bank layout) or none


def company_panel(raw_dir: str, sym: str):
    isd, bsd = _load(raw_dir, sym, "INCOME_STATEMENT"), _load(raw_dir, sym, "BALANCE_SHEET")
    if not isd or not bsd:
        return None
    is_y, is_q = _latest_rows(isd.get("years")), _latest_rows(isd.get("quarters"))
    bs_y, bs_q = _latest_rows(bsd.get("years")), _latest_rows(bsd.get("quarters"))
    tpl = detect_template({**is_y, **is_q})
    annual, quarterly = {}, {}
    for (y, L), r in is_y.items():
        if L != 5:
            continue
        b = bs_y.get((y, 5))
        p, fb = profit_of(r)
        annual[y] = {"rev": revenue_of(r, tpl), "np": p, "fb": fb,
                     "ta": b.get("bsa53") if b else None, "eq": equity_of(b) if b else None}
    for (y, q), r in is_q.items():
        if q not in (1, 2, 3, 4):
            continue
        b = bs_q.get((y, q))
        p, fb = profit_of(r)
        quarterly[(y, q)] = {"rev": revenue_of(r, tpl), "np": p, "fb": fb,
                             "ta": b.get("bsa53") if b else None, "eq": equity_of(b) if b else None}
    return {"tpl": tpl, "annual": annual, "quarterly": quarterly}


def _qminus(yq, n):
    y, q = yq
    idx = y * 4 + (q - 1) - n
    return (idx // 4, idx % 4 + 1)


def ttm_of(panel, end):
    qs = [_qminus(end, i) for i in range(4)]
    rows = [panel["quarterly"].get(k) for k in qs]
    if any(r is None or r["rev"] is None or r["np"] is None for r in rows):
        return None
    last = rows[0]
    return {"rev": sum(r["rev"] for r in rows), "np": sum(r["np"] for r in rows),
            "fb": any(r["fb"] for r in rows), "ta": last["ta"], "eq": last["eq"]}


def _ok(rec):
    return rec is not None and rec["rev"] is not None and rec["np"] is not None and rec["ta"] is not None


# ------------------------------------------------------------------------------------------------------ aggregate
def _r(x, nd=0):
    if x is None:
        return None
    return int(round(x)) if nd == 0 else round(x, nd)


def _pct(a, b):
    return None if a is None or not b else round(100.0 * a / b, 2)


def _growth(cur, prev):
    if cur is None or prev is None or prev <= 0:
        return None
    return round(100.0 * (cur / prev - 1), 2)


def aggregate(members, panels, mcap, recs_by_period, periods, prev_of, total_cap, top_period):
    """members: list of syms. recs_by_period[p][sym] -> record or None."""
    out = {k: [] for k in ("revenue_bn_vnd", "net_profit_bn_vnd", "total_assets_bn_vnd", "equity_bn_vnd",
                           "n_with_data", "coverage_pct", "net_margin_pct", "roa_pct", "roe_pct",
                           "revenue_yoy_pct", "net_profit_yoy_pct", "yoy_n_like_for_like", "pat_fallback_n")}
    for p in periods:
        recs = {s: recs_by_period[p].get(s) for s in members}
        ok = {s: r for s, r in recs.items() if _ok(r)}
        rev = sum(r["rev"] for r in ok.values()) / BN if ok else None
        np_ = sum(r["np"] for r in ok.values()) / BN if ok else None
        ta = sum(r["ta"] for r in ok.values()) / BN if ok else None
        eqs = {s: r for s, r in ok.items() if r["eq"] is not None and r["eq"] > 0}
        eq = sum(r["eq"] for r in eqs.values()) / BN if eqs else None
        np_eq = sum(r["np"] for r in eqs.values()) / BN if eqs else None
        cov = 100.0 * sum(mcap[s] for s in ok) / total_cap if total_cap else None
        out["revenue_bn_vnd"].append(_r(rev))
        out["net_profit_bn_vnd"].append(_r(np_))
        out["total_assets_bn_vnd"].append(_r(ta))
        out["equity_bn_vnd"].append(_r(eq))
        out["n_with_data"].append(len(ok))
        out["coverage_pct"].append(round(cov, 1) if cov is not None else None)
        out["net_margin_pct"].append(_pct(np_, rev) if rev and rev > 0 else None)
        out["roa_pct"].append(_pct(np_, ta))
        out["roe_pct"].append(_pct(np_eq, eq))
        out["pat_fallback_n"].append(sum(1 for r in ok.values() if r["fb"]))
        pp = prev_of.get(p)
        if pp is None:
            out["revenue_yoy_pct"].append(None); out["net_profit_yoy_pct"].append(None)
            out["yoy_n_like_for_like"].append(None)
            continue
        both = [s for s in ok if _ok(recs_by_period[pp].get(s))]
        if not both:
            out["revenue_yoy_pct"].append(None); out["net_profit_yoy_pct"].append(None)
            out["yoy_n_like_for_like"].append(0)
            continue
        c_rev = sum(ok[s]["rev"] for s in both); p_rev = sum(recs_by_period[pp][s]["rev"] for s in both)
        c_np = sum(ok[s]["np"] for s in both); p_np = sum(recs_by_period[pp][s]["np"] for s in both)
        out["revenue_yoy_pct"].append(_growth(c_rev, p_rev))
        out["net_profit_yoy_pct"].append(_growth(c_np, p_np))
        out["yoy_n_like_for_like"].append(len(both))
    out["low_coverage_periods"] = [periods[i] for i, c in enumerate(out["coverage_pct"])
                                   if c is None or c < LOW_COV]
    return out


def top_contributors(members, recs, names, n=3):
    ok = [(s, recs[s]) for s in members if _ok(recs.get(s))]
    tot = sum(r["np"] for _, r in ok)
    pos = sorted(ok, key=lambda x: -x[1]["np"])
    neg = sorted([x for x in ok if x[1]["np"] < 0], key=lambda x: x[1]["np"])
    f = lambda s, r: {"ticker": s, "name": names.get(s), "net_profit_bn_vnd": _r(r["np"] / BN),
                      "share_of_industry_pct": _pct(r["np"], tot) if tot > 0 else None}
    return [f(s, r) for s, r in pos[:n] if r["np"] > 0], [f(s, r) for s, r in neg[:n]]


# ---------------------------------------------------------------------------------------------------------- build
def build(stocks_path, raw_dir, out_path, ttm_min_cov, dry_run, cross_checks_path=None):
    stocks = json.load(open(stocks_path, encoding="utf-8"))
    data = json.load(open(out_path, encoding="utf-8"))
    ind = data["IND_ICB"]
    icbs = [x["icb"] for x in ind["industries"]]
    meta = {x["icb"]: x for x in ind["industries"]}
    mcap = {r["sym"]: float(r.get("mcap") or 0) for r in stocks}
    names = {r["sym"]: r.get("name") for r in stocks}
    by_icb = collections.defaultdict(list)
    for r in stocks:
        by_icb[r["icb"]].append(r["sym"])
    unknown = set(by_icb) - set(icbs)
    if unknown:
        sys.exit(f"stocks with ICB codes not in IND_ICB.industries: {unknown}")

    panels, tpl_count, missing = {}, collections.Counter(), []
    for r in stocks:
        p = company_panel(raw_dir, r["sym"])
        if p is None:
            missing.append(r["sym"]); continue
        panels[r["sym"]] = p
        tpl_count[p["tpl"]] += 1

    total_cap = sum(mcap.values())
    # TTM quarter: latest quarter whose 4-quarter window is complete for >= ttm_min_cov % of today's market cap
    cand = sorted({k for p in panels.values() for k in p["quarterly"]}, reverse=True)[:6]
    ttm_cov = {}
    for q in cand:
        cov = 100.0 * sum(mcap[s] for s, p in panels.items() if _ok(ttm_of(p, q))) / total_cap
        ttm_cov[f"Q{q[1]}/{q[0]}"] = round(cov, 1)
    ttm_q = next((q for q in cand if ttm_cov[f"Q{q[1]}/{q[0]}"] >= ttm_min_cov), None)
    if ttm_q is None:
        sys.exit(f"no TTM quarter reaches {ttm_min_cov}% coverage: {ttm_cov}")
    ttm_prev = _qminus(ttm_q, 4)

    periods = [str(y) for y in YEARS] + ["TTM"]
    recs = {str(y): {s: p["annual"].get(y) for s, p in panels.items()} for y in YEARS}
    recs["TTM"] = {s: ttm_of(p, ttm_q) for s, p in panels.items()}
    recs["_TTM_prev"] = {s: ttm_of(p, ttm_prev) for s, p in panels.items()}
    prev_of = {str(y): str(y - 1) for y in YEARS[1:]}
    prev_of["TTM"] = "_TTM_prev"

    ttm_label_vi = f"12 tháng đến Q{ttm_q[1]}/{ttm_q[0]}"
    ttm_label_en = f"TTM Q{ttm_q[1]}/{ttm_q[0]}"
    latest_fy = str(YEARS[-1])

    blk_ind = {}
    for icb in icbs:
        mem = by_icb.get(icb, [])
        cap = sum(mcap[s] for s in mem)
        agg = aggregate(mem, panels, mcap, recs, periods, prev_of, cap, latest_fy)
        top_fy, loss_fy = top_contributors(mem, recs[latest_fy], names)
        top_ttm, loss_ttm = top_contributors(mem, recs["TTM"], names)
        tpls = collections.Counter(panels[s]["tpl"] for s in mem if s in panels)
        low = agg.pop("low_coverage_periods")
        blk_ind[icb] = {"name_vi": meta[icb]["name_vi"], "name_en": meta[icb]["name_en"], "n_listed": len(mem),
                        "mcap_today_bn_vnd": _r(cap), "templates": dict(tpls), **agg,
                        "low_coverage_years": low,
                        "top_profit_" + latest_fy: top_fy, "largest_losses_" + latest_fy: loss_fy,
                        "top_profit_ttm": top_ttm, "largest_losses_ttm": loss_ttm}
    allsyms = [r["sym"] for r in stocks]
    magg = aggregate(allsyms, panels, mcap, recs, periods, prev_of, total_cap, latest_fy)
    magg["low_coverage_years"] = magg.pop("low_coverage_periods")
    magg["n_listed"] = len(allsyms)
    magg["mcap_today_bn_vnd"] = _r(total_cap)
    magg["templates"] = dict(tpl_count)
    magg["n_without_statements"] = len(missing)

    # market composition: share of market profit by industry, latest FY and TTM
    for key in (latest_fy, "TTM"):
        i = periods.index(key)
        tot = magg["net_profit_bn_vnd"][i]
        magg["profit_share_by_icb_" + ("ttm" if key == "TTM" else key)] = {
            icb: (_pct(blk_ind[icb]["net_profit_bn_vnd"][i], tot) if tot and blk_ind[icb]["net_profit_bn_vnd"][i]
                  is not None else None) for icb in icbs}

    # consistency: latest FY profit vs the sum of its four quarterly reports (Q4 is often pre-audit)
    qa_tot, qa_bad = 0, []
    ly = YEARS[-1]
    for sym, p in panels.items():
        a, qs = p["annual"].get(ly), [p["quarterly"].get((ly, q)) for q in (1, 2, 3, 4)]
        if _ok(a) and all(_ok(x) for x in qs):
            qa_tot += 1
            sq = sum(x["np"] for x in qs)
            if abs(sq - a["np"]) > max(0.05 * abs(a["np"]), 5 * BN):
                qa_bad.append((mcap[sym], sym, _r(a["np"] / BN), _r(sq / BN)))
    qa_bad.sort(reverse=True)
    quarter_check = {"fy": ly, "companies_compared": qa_tot, "mismatch_n": len(qa_bad),
                     "rule": "|Σ4 quarters − FY net profit| > max(5%, 5 bn VND)",
                     "largest_by_cap": [{"ticker": t, "fy_bn_vnd": f, "sum_quarters_bn_vnd": q}
                                        for _, t, f, q in qa_bad[:8]],
                     "note": "Differences are mostly audit adjustments booked in the audited FY report but not "
                             "restated in Q4; TTM uses quarterly reports as published."}

    probes = {}
    for s in ("VCB", "VIC", "HPG", "GAS", "FPT"):
        if s in panels:
            probes[s] = {y: ({k: (_r(v / BN) if isinstance(v, (int, float)) and k != "fb" else v)
                              for k, v in panels[s]["annual"][y].items()} if y in panels[s]["annual"] else None)
                         for y in (2024, 2025)}
            probes[s]["template"] = panels[s]["tpl"]

    if cross_checks_path:
        cross = json.load(open(cross_checks_path, encoding="utf-8"))
    else:  # keep the hand-written comparisons already in the file
        cross = (ind.get("fundamentals") or {}).get("cross_checks")

    block = {
        "as_of": ind.get("as_of"),
        "built": date.today().isoformat(),
        "periods": periods,
        "labels_vi": [str(y) for y in YEARS] + [ttm_label_vi],
        "labels_en": [str(y) for y in YEARS] + [ttm_label_en],
        "ttm": {"quarter": f"Q{ttm_q[1]}/{ttm_q[0]}", "label_vi": ttm_label_vi, "label_en": ttm_label_en,
                "quarters": [f"Q{q[1]}/{q[0]}" for q in reversed([_qminus(ttm_q, i) for i in range(4)])],
                "yoy_base_quarter": f"Q{ttm_prev[1]}/{ttm_prev[0]}",
                "market_cap_coverage_by_end_quarter_pct": ttm_cov, "min_coverage_rule_pct": ttm_min_cov},
        "latest_fy": int(latest_fy),
        "units": "bn VND (statement values in VND ÷ 1e9); ratios and growth in %; coverage = % of today's cap",
        "source": "Vietcap IQ financial statements (iq-insight-service, company/{SYM}/financial-statement, sections "
                  "INCOME_STATEMENT and BALANCE_SHEET; field labels from /financial-statement/metrics), served via "
                  "trading.vietcap.com.vn/api/iq-insight-service — one request per stock and section, pulled "
                  + date.today().isoformat() + "; classification and today's caps as IND_ICB.industries.",
        "source_url": "https://trading.vietcap.com.vn/api/iq-insight-service/v1/company/{SYM}/financial-statement?section=INCOME_STATEMENT",
        "fields": {
            "revenue_bn_vnd": "Σ top line. Corporates: net sales (Vietcap isa3 'Doanh thu thuần'). Banks: total "
                              "operating income (isb38 'Tổng thu nhập hoạt động' = net interest + fees + FX/securities "
                              "+ other). Securities firms: net operating revenue (isa3; isa1 if absent) — gross, "
                              "includes FVTPL gains. Insurers: net revenue from insurance business (isi64), "
                              "excluding financial-investment income. The market total therefore mixes these "
                              "definitions.",
            "net_profit_bn_vnd": "Σ profit after tax attributable to parent shareholders (isa22); PAT (isa20) used "
                                 "where isa22 is missing/zero but isa20 is not — count in pat_fallback_n.",
            "total_assets_bn_vnd": "Σ total assets at fiscal year-end (bsa53); TTM = at the TTM quarter end.",
            "equity_bn_vnd": "Σ parent shareholders' equity at period end (bsa78 less minority interests bsa210 "
                             "where bsa78 includes them); companies with equity ≤ 0 excluded from this sum and "
                             "from ROE.",
            "n_with_data": "Companies with revenue, net profit and total assets for the period (all sums use this set).",
            "coverage_pct": "Σ today's market cap of those companies ÷ today's industry cap, %.",
            "net_margin_pct": "≈ derived: net_profit ÷ revenue (null when revenue ≤ 0). Banks' denominator is "
                              "operating income, not sales.",
            "roa_pct": "≈ derived: net_profit ÷ period-end total assets (not average assets).",
            "roe_pct": "≈ derived: Σ net profit ÷ Σ period-end parent equity over companies with equity > 0 "
                       "(not average equity).",
            "revenue_yoy_pct / net_profit_yoy_pct": "≈ derived, like-for-like: Σ this period ÷ Σ previous period − 1 "
                                                    "over companies with data in both (yoy_n_like_for_like); TTM "
                                                    "vs the TTM one year earlier. Null when the base profit ≤ 0.",
            "low_coverage_years": "Periods whose coverage_pct < 60%.",
            "top_profit_<FY> / top_profit_ttm": "3 largest positive net-profit contributors and their share of the "
                                                "industry total; largest_losses_* = 3 biggest loss-makers.",
            "templates": "Count of constituents by statement layout detected (corporate / bank / securities / insurer).",
        },
        "basis_vi": (
            "Tổng hợp báo cáo tài chính theo ngành ICB cấp 2, gồm các mã đang niêm yết HÔM NAY trên HOSE/HNX/UPCoM "
            "(cùng danh sách và vốn hóa với IND_ICB.industries). Nguồn: báo cáo tài chính trên Vietcap IQ (số liệu "
            "VND, đổi sang tỷ đồng). Doanh thu: doanh thu thuần (DN thường); tổng thu nhập hoạt động (ngân hàng); "
            "doanh thu hoạt động thuần (CTCK, gồm lãi FVTPL); doanh thu thuần hoạt động kinh doanh bảo hiểm (DN bảo "
            "hiểm, không gồm thu nhập đầu tư tài chính). Lợi nhuận: LNST thuộc cổ đông công ty mẹ (dùng LNST nếu "
            "nguồn không tách). Tổng tài sản và vốn chủ sở hữu (của cổ đông công ty mẹ) tại cuối kỳ. Năm = năm tài "
            "chính theo nhãn của Vietcap; " + ttm_label_vi + " = tổng 4 quý gần nhất, bảng cân đối tại cuối quý đó. "
            "Mỗi kỳ chỉ cộng các công ty có đủ doanh thu, lợi nhuận và tổng tài sản; thiếu thì bỏ qua, không nội suy. "
            "Biên lợi nhuận, ROA, ROE là tỷ số tổng (≈, tính từ số cuối kỳ). Tăng trưởng so cùng kỳ tính trên cùng "
            "nhóm công ty có số liệu ở cả hai kỳ."),
        "basis_en": (
            "Financial statements aggregated by ICB level-2 industry over stocks listed TODAY on HOSE/HNX/UPCoM (same "
            "constituents and caps as IND_ICB.industries). Source: Vietcap IQ financial statements (VND, converted to "
            "bn VND). Revenue: net sales (corporates); total operating income (banks); net operating revenue "
            "(securities firms, incl. FVTPL gains); net revenue from insurance business (insurers, excl. investment "
            "income). Profit: PAT attributable to parent shareholders (PAT where not split out). Total assets and "
            "parent equity at period end. Years = fiscal years as labelled by Vietcap; " + ttm_label_en + " = sum of "
            "the latest four quarters, balance sheet at that quarter end. Each period sums only companies with "
            "revenue, profit and total assets; gaps are skipped, never interpolated. Margin, ROA and ROE are aggregate "
            "ratios (≈, period-end denominators). YoY growth is like-for-like over companies reporting both periods."),
        "caveats_vi": [
            "Thiên lệch sống sót: chỉ gồm mã đang niêm yết hôm nay; các năm đầu thiếu công ty đã hủy niêm yết, và "
            "thiếu công ty mới niêm yết sau này nếu nguồn không có báo cáo các năm trước.",
            "Phân ngành theo mã ICB hiện nay, áp cho cả các năm trước.",
            "Doanh thu toàn thị trường cộng các định nghĩa khác nhau (doanh thu thuần, thu nhập hoạt động ngân hàng, "
            "doanh thu CTCK/bảo hiểm) — chỉ nên đọc như tổng mô tả.",
            "Tổng tài sản ngân hàng (chủ yếu là cho vay) chiếm phần lớn tổng tài sản toàn thị trường; ROA toàn thị "
            "trường vì vậy thấp và chịu chi phối của ngân hàng.",
            "Số liệu hợp nhất khi công ty có công ty con (theo dữ liệu Vietcap); không loại trừ sở hữu chéo giữa "
            "các công ty niêm yết nên doanh thu/lợi nhuận/tài sản có thể bị đếm hai lần khi công ty mẹ và công ty "
            "con cùng niêm yết (ví dụ VIC hợp nhất VHM, VRE, VPL; MWG hợp nhất DMX; GVR hợp nhất PHR, DPR; GAS hợp "
            "nhất PGD) — tổng ngành Bất động sản và Bán lẻ bị ảnh hưởng rõ nhất.",
            "Thay đổi phạm vi hợp nhất làm gãy chuỗi: từ 2026 FPT không còn hợp nhất toàn bộ FPT Telecom (FOX) "
            "(doanh thu 9 tháng 2026 của FPT 40.023 tỷ đồng theo phương pháp mới), nên số " + ttm_label_vi + " của "
            "ngành Công nghệ không so sánh trực tiếp với 2025.",
            "Năm tài chính không trùng năm dương lịch ở một số ít công ty: dùng nhãn năm của Vietcap.",
            "Năm có độ phủ dưới 60% vốn hóa hiện tại của ngành được liệt kê trong low_coverage_years.",
            "Tỷ số tính trên số cuối kỳ, không phải bình quân; có thể khác số công ty tự công bố.",
            ttm_label_vi + ": chỉ gồm công ty đã có đủ 4 quý; báo cáo quý gần nhất có thể còn được điều chỉnh "
            "(soát xét/kiểm toán).",
        ],
        "caveats_en": [
            "Survivorship: only stocks listed today; early years miss delisted companies, and miss later listings "
            "where the source has no earlier statements.",
            "Classification uses today's ICB code for all years.",
            "Market revenue adds different definitions (net sales, bank operating income, broker/insurer top lines) — "
            "read it as a descriptive total only.",
            "Banks' balance sheets dominate market total assets, so market ROA is low and bank-driven.",
            "Consolidated figures where the company has subsidiaries (as served by Vietcap); cross-holdings between "
            "listed companies are not eliminated, so revenue/profit/assets are double-counted where a listed parent "
            "consolidates a listed subsidiary (e.g. VIC consolidates VHM, VRE, VPL; MWG consolidates DMX; GVR "
            "consolidates PHR, DPR; GAS consolidates PGD) — Real Estate and Retail totals are most affected.",
            "Changes in consolidation scope break series: from 2026 FPT no longer fully consolidates FPT Telecom "
            "(FOX) (FPT's 9M-2026 revenue VND 40,023 bn on the new basis), so Technology's " + ttm_label_en +
            " is not directly comparable with 2025.",
            "A few companies have non-calendar fiscal years: Vietcap's year label is used.",
            "Periods covering < 60% of the industry's current cap are listed in low_coverage_years.",
            "Ratios use period-end balances, not averages, and can differ from company-reported ratios.",
            ttm_label_en + ": companies with all four quarters only; the latest quarter can still be revised "
            "(review/audit).",
        ],
        "by_icb": blk_ind,
        "market": magg,
        "built_from": {
            "stocks": len(stocks), "with_statements": len(panels), "without_statements": sorted(missing)[:60],
            "without_statements_n": len(missing), "templates": dict(tpl_count),
            "endpoint_note": "iq.vietcap.com.vn is not reachable from the cloud sandbox (connection refused at the "
                             "proxy); the same iq-insight-service paths answer under trading.vietcap.com.vn/api. "
                             "trading.vietcap.com.vn/data-mt/graphql (older vnstock VCI finance) returns {} for "
                             "every query.",
            "quarter_vs_annual_check": quarter_check},
        "company_probe": probes,
        "cross_checks": cross,
        "disclaimer": DISCLAIMER,
    }
    if dry_run:
        m = block["market"]
        print(json.dumps({"periods": periods, "ttm": block["ttm"], "market": {k: m[k] for k in (
            "revenue_bn_vnd", "net_profit_bn_vnd", "total_assets_bn_vnd", "equity_bn_vnd", "n_with_data",
            "coverage_pct", "roe_pct", "net_profit_yoy_pct", "low_coverage_years")}, "templates": dict(tpl_count),
            "missing": len(missing), "probes": probes}, ensure_ascii=False, indent=1))
        return block
    ind["fundamentals"] = block
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=1)
        f.write("\n")
    print("wrote", out_path, "IND_ICB.fundamentals;", len(panels), "companies with statements")
    return block


# ------------------------------------------------------------------------------------------------------ valuation
VAL_MIN_END = (2026, 2)        # oldest acceptable latest quarter for every TTM window / balance sheet used below
FIN_ICB = {"8300", "8500", "8700"}   # Banks, Insurance, Financial Services: no D/E, current ratio, cover, turnover
PAR_VND = 10000                # Vietnamese par value per share; shares ≈ paid-in capital (bsa80) ÷ par


def _qrows(raw_dir, sym, sec):
    d = _load(raw_dir, sym, sec)
    return _latest_rows(d.get("quarters")) if d else {}


def _reported(row):
    return bool(row) and any(isinstance(v, (int, float)) and v != 0
                             for k, v in row.items() if k[:2] in ("cf", "is", "bs"))


def company_ttm_latest(panel, min_end=VAL_MIN_END, at=None):
    """Company's own latest four consecutive reported quarters (parent profit). Returns (np_vnd, end) or None.
    The window ends at the latest quarter with a profit figure; if that window has a gap the previous end is tried,
    as long as the end is not older than min_end. at=(y, q): the window must end exactly at that quarter."""
    if at is not None:
        rows = [panel["quarterly"].get(_qminus(at, i)) for i in range(4)]
        return (sum(r["np"] for r in rows), at) if all(r is not None and r["np"] is not None for r in rows) else None
    qs = sorted((k for k, v in panel["quarterly"].items() if v["np"] is not None), reverse=True)
    for end in qs[:2]:
        if end < min_end:
            break
        rows = [panel["quarterly"].get(_qminus(end, i)) for i in range(4)]
        if all(r is not None and r["np"] is not None for r in rows):
            return sum(r["np"] for r in rows), end
    return None


def company_book_latest(panel, min_end=VAL_MIN_END, at=None):
    """Parent shareholders' equity at the latest quarter-end with a balance sheet (not older than min_end).
    at=(y, q): at exactly that quarter-end."""
    if at is not None:
        r = panel["quarterly"].get(at)
        return (r["eq"], at) if r is not None and r["eq"] is not None else None
    qs = sorted((k for k, v in panel["quarterly"].items() if v["eq"] is not None), reverse=True)
    if qs and qs[0] >= min_end:
        return panel["quarterly"][qs[0]]["eq"], qs[0]
    return None


def _sum4(rows, key, absval=False):
    vals = [r.get(key) if r else None for r in rows]
    if any(v is None for v in vals):
        return None
    return sum(vals)


def company_inputs(raw_dir, sym):
    """Load once: (panel, quarterly IS rows, BS rows, CF rows) — or None without statements."""
    panel = company_panel(raw_dir, sym)
    if panel is None:
        return None
    return (panel,) + tuple(_qrows(raw_dir, sym, s) for s in ("INCOME_STATEMENT", "BALANCE_SHEET", "CASH_FLOW"))


def company_val(raw_dir, sym, min_end=VAL_MIN_END, at=None, inputs=None):
    """Per-company inputs for the card ratios (VND). Keys present only when the data exist.
    at=(y, q): every window/balance sheet is taken at exactly that quarter (used by `quarterly`); default = each
    company's own latest window not older than min_end (used by `valuation`). inputs: preloaded company_inputs()."""
    inputs = inputs or company_inputs(raw_dir, sym)
    if inputs is None:
        return None
    panel, isq, bsq, cfq = inputs
    Q = panel["quarterly"]
    rec = {"tpl": panel["tpl"]}
    t = company_ttm_latest(panel, min_end, at)
    if t:
        rec["np"], rec["end"] = t
        end = t[1]
        cur = [_qminus(end, i) for i in range(4)]
        ago = [_qminus(end, 4 + i) for i in range(4)]
        if all(Q.get(k) and Q[k]["np"] is not None for k in ago):
            rec["np_ago"] = sum(Q[k]["np"] for k in ago)
        if all(Q[k]["rev"] is not None for k in cur):
            rec["rev"] = sum(Q[k]["rev"] for k in cur)
            if "np_ago" in rec and all(Q[k]["rev"] is not None for k in ago):
                rec["rev_ago"] = sum(Q[k]["rev"] for k in ago)
        pbt = _sum4([isq.get(k) for k in cur], "isa16")
        if pbt is not None:
            ie = [isq[k].get("isa8") for k in cur]
            rec["pbt"], rec["int_exp"] = pbt, abs(sum(v or 0 for v in ie))   # isa8 is booked negative
        ta_end, ta_ago = Q[end]["ta"], (Q.get(ago[0]) or {}).get("ta")
        if ta_end is not None:
            rec["ta_end"], rec["ta_ago"] = ta_end, ta_ago
        sh_end = (bsq.get(end) or {}).get("bsa80")
        sh_ago = (bsq.get(ago[0]) or {}).get("bsa80")
        if sh_end and sh_ago and sh_end > 0 and sh_ago > 0:
            rec["sh_end"], rec["sh_ago"] = sh_end / PAR_VND, sh_ago / PAR_VND
    b = company_book_latest(panel, min_end, at)
    if b:
        rec["eq"], rec["bq"] = b
        br = bsq.get(b[1]) or {}
        rec["liab"] = br.get("bsa54")
        parts = [br.get(k) for k in ("bsa56", "bsa71", "bsa173")]
        rec["debt"] = sum(v or 0 for v in parts) if any(v is not None for v in parts) else None
        rec["ca"], rec["cl"] = br.get("bsa1"), br.get("bsa55")
    # dividends paid (cash-flow statement, own latest four reported quarters)
    cq = sorted((k for k, r in cfq.items() if k[1] in (1, 2, 3, 4) and _reported(r)), reverse=True)
    for end in ([at] if at is not None else cq[:2]):
        if end < min_end:
            break
        rows = [cfq.get(_qminus(end, i)) for i in range(4)]
        if all(_reported(r) for r in rows):
            rec["div"] = -sum((r.get("cfa32") or 0) for r in rows)   # payments booked negative
            rec["div_end"] = end
            break
    return rec


def _ratio(a, b, nd):
    return round(a / b, nd) if b and b > 0 else None


def _ql(q):
    return f"Q{q[1]}/{q[0]}"


def valuation_agg(members, mcap, cv, fin):
    """Aggregate ratios over `members` (syms). cv[sym] = company_val record. fin: financial-industry group."""
    cap = sum(mcap[s] for s in members) or None
    R = {s: cv[s] for s in members if s in cv and mcap[s] > 0}
    covp = lambda ss: round(100.0 * sum(mcap[s] for s in ss) / cap, 1) if cap else None
    out = {}
    # P/E
    pe_s = [s for s, r in R.items() if "np" in r]
    e = sum(R[s]["np"] for s in pe_s) / BN
    out.update(pe=round(sum(mcap[s] for s in pe_s) / e, 1) if pe_s and e > 0 else None, pe_n=len(pe_s),
               pe_coverage_pct=covp(pe_s), pe_loss_makers_n=sum(1 for s in pe_s if R[s]["np"] < 0))
    if pe_s and e <= 0:
        out["pe_null_reason"] = "aggregate TTM parent profit ≤ 0 (%s bn VND)" % _r(e)
    # P/B
    pb_s = [s for s, r in R.items() if r.get("eq") is not None and r["eq"] > 0]
    bk = sum(R[s]["eq"] for s in pb_s) / BN
    out.update(pb=_ratio(sum(mcap[s] for s in pb_s), bk, 2), pb_n=len(pb_s), pb_coverage_pct=covp(pb_s))
    both = [s for s in pb_s if "np" in R[s]]
    eb = sum(R[s]["eq"] for s in both)
    out["roe_pct"] = round(100.0 * sum(R[s]["np"] for s in both) / eb, 1) if both and eb > 0 else None
    out["roe_n"] = len(both)
    # dividend yield
    dv = [s for s, r in R.items() if "div" in r]
    dcap = sum(mcap[s] for s in dv)
    out.update(div_yield_pct=round(100.0 * sum(R[s]["div"] for s in dv) / BN / dcap, 2) if dcap else None,
               div_n=len(dv), div_payers_n=sum(1 for s in dv if R[s]["div"] > 0), div_coverage_pct=covp(dv))
    # growth sample: revenue and profit in both windows
    g = [s for s, r in R.items() if "rev_ago" in r]
    rv, rva = sum(R[s]["rev"] for s in g), sum(R[s]["rev_ago"] for s in g)
    nn, nna = sum(R[s]["np"] for s in g), sum(R[s]["np_ago"] for s in g)
    out.update(rev_yoy_pct=_growth(rv, rva) if g else None, npat_yoy_pct=_growth(nn, nna) if g else None,
               rev_yoy_n=len(g), rev_yoy_coverage_pct=covp(g))
    if g and nna <= 0:
        out["npat_yoy_null_reason"] = "year-earlier aggregate TTM profit ≤ 0"
    mix = collections.Counter()
    for s in g:
        mix[_ql(R[s]["end"])] += mcap[s]
    if not mix:
        out["growth_quarter"] = None
    elif len(mix) == 1:
        q = next(iter(mix))
        out["growth_quarter"] = f"TTM {q} vs TTM {q[:3]}{int(q[3:]) - 1}"
    else:
        tot = sum(mix.values())
        sh = lambda c: "<1" if 100.0 * c / tot < 0.5 else f"{100.0 * c / tot:.0f}"
        out["growth_quarter"] = "mixed: " + ", ".join(
            f"TTM {q} ({sh(c)}% of sample cap)" for q, c in sorted(mix.items(), key=lambda x: -x[1])
        ) + " — each vs its own year-earlier TTM"
    out["net_margin_pct"] = round(100.0 * nn / rv, 1) if g and rv > 0 else None
    out["net_margin_n"] = len(g)
    # EPS growth: same companies, both TTM windows, share counts at both window ends
    es = [s for s, r in R.items() if "np_ago" in r and "sh_end" in r]
    en, ea = sum(R[s]["np"] for s in es), sum(R[s]["np_ago"] for s in es)
    ea_ps = sum(R[s]["np_ago"] * R[s]["sh_end"] / R[s]["sh_ago"] for s in es)
    out.update(eps_growth_pct=_growth(en, ea) if es else None,
               eps_growth_per_share_pct=_growth(en, ea_ps) if es else None,
               eps_growth_n=len(es), eps_growth_coverage_pct=covp(es))
    if es and ea <= 0:
        out["eps_growth_null_reason"] = "year-earlier aggregate TTM earnings ≤ 0"
    # ROA (all groups, financials included): Σ TTM parent profit ÷ Σ average total assets
    ra = [s for s in pe_s if R[s].get("ta_end")]
    ta_avg = sum((R[s]["ta_end"] + R[s]["ta_ago"]) / 2 if R[s].get("ta_ago") else R[s]["ta_end"] for s in ra)
    out.update(roa_pct=round(100.0 * sum(R[s]["np"] for s in ra) / ta_avg, 2) if ra and ta_avg > 0 else None,
               roa_n=len(ra), roa_coverage_pct=covp(ra), roa_latest_assets_only_n=sum(1 for s in ra if not R[s].get("ta_ago")))
    # balance-sheet / coverage ratios (non-financial groups only)
    if fin:
        ls = [s for s in pb_s if R[s].get("liab") is not None]
        out.update(debt_to_equity=None, liab_to_equity=_ratio(sum(R[s]["liab"] for s in ls),
                                                               sum(R[s]["eq"] for s in ls), 2),
                   de_n=len(ls), de_coverage_pct=covp(ls), current_ratio=None, cr_n=0, cr_coverage_pct=None,
                   interest_coverage=None, ic_n=0, ic_coverage_pct=None, ic_no_debt_n=None,
                   asset_turnover=None, at_n=0, at_coverage_pct=None)
        return out
    ds = [s for s in pb_s if R[s].get("debt") is not None]
    out.update(debt_to_equity=_ratio(sum(R[s]["debt"] for s in ds), sum(R[s]["eq"] for s in ds), 2),
               de_n=len(ds), de_coverage_pct=covp(ds))
    cs = [s for s in pb_s if R[s].get("ca") and R[s].get("cl")]
    out.update(current_ratio=_ratio(sum(R[s]["ca"] for s in cs), sum(R[s]["cl"] for s in cs), 2),
               cr_n=len(cs), cr_coverage_pct=covp(cs))
    ic_all = [s for s in g if "pbt" in R[s]]
    ics = [s for s in ic_all if R[s]["int_exp"] > 0]
    ebit = sum(R[s]["pbt"] + R[s]["int_exp"] for s in ics)
    out.update(interest_coverage=_ratio(ebit, sum(R[s]["int_exp"] for s in ics), 1), ic_n=len(ics),
               ic_coverage_pct=covp(ics), ic_no_debt_n=len(ic_all) - len(ics))
    if ics and ebit <= 0:
        out["ic_negative_ebit"] = True
    ats = [s for s in g if R[s].get("ta_end")]
    avg = sum((R[s]["ta_end"] + R[s]["ta_ago"]) / 2 if R[s].get("ta_ago") else R[s]["ta_end"] for s in ats)
    out.update(asset_turnover=_ratio(sum(R[s]["rev"] for s in ats), avg, 2), at_n=len(ats),
               at_coverage_pct=covp(ats), at_latest_assets_only_n=sum(1 for s in ats if not R[s].get("ta_ago")))
    return out


CARD_NULLABLE = ("pe_null_reason", "npat_yoy_null_reason", "eps_growth_null_reason", "ic_negative_ebit",
                 "liab_to_equity")


def valuation(stocks_path, raw_dir, out_path, dry_run, dump_companies=None):
    """Fill IND_ICB.industries[] valuation / growth / balance-sheet ratio keys, the same keys in IND_ICB.market
    (balance-sheet ratios ex-financials) and pe/pb by exchange, plus the basis text. Caps = the stocks file's mcap
    (same snapshot as the cards' mcap_bn_vnd). Nothing else in the file changes."""
    stocks = json.load(open(stocks_path, encoding="utf-8"))
    data = json.load(open(out_path, encoding="utf-8"))
    ind = data["IND_ICB"]
    mcap = {r["sym"]: float(r.get("mcap") or 0) for r in stocks}
    icb_of = {r["sym"]: r["icb"] for r in stocks}
    board = {r["sym"]: {"HSX": "HOSE", "HOSE": "HOSE", "HNX": "HNX"}.get(r.get("board"), "UPCoM") for r in stocks}
    by_icb = collections.defaultdict(list)
    for r in stocks:
        by_icb[r["icb"]].append(r["sym"])
    for card in ind["industries"]:
        tot = sum(mcap[s] for s in by_icb.get(card["icb"], []))
        if abs(tot - card["mcap_bn_vnd"]) > 1:
            sys.exit(f"cap mismatch for {card['icb']}: stocks {tot:.0f} vs card {card['mcap_bn_vnd']}")
    cv = {}
    for r in stocks:
        c = company_val(raw_dir, r["sym"])
        if c:
            cv[r["sym"]] = c
    if dump_companies:
        with open(dump_companies, "w", encoding="utf-8") as fh:
            json.dump({s: {k: (list(v) if isinstance(v, tuple) else v) for k, v in c.items()} for s, c in cv.items()},
                      fh)
    res = {}
    for card in ind["industries"]:
        a = valuation_agg(by_icb.get(card["icb"], []), mcap, cv, card["icb"] in FIN_ICB)
        res[card["icb"]] = a
        for k in CARD_NULLABLE:
            card.pop(k, None)
        card.update(a)
    allsyms = list(mcap)
    m = valuation_agg(allsyms, mcap, cv, False)
    nonfin = [s for s in allsyms if icb_of[s] not in FIN_ICB]
    mn = valuation_agg(nonfin, mcap, cv, False)
    for k in ("debt_to_equity", "de_n", "de_coverage_pct", "current_ratio", "cr_n", "cr_coverage_pct",
              "interest_coverage", "ic_n", "ic_coverage_pct", "ic_no_debt_n", "asset_turnover", "at_n",
              "at_coverage_pct", "at_latest_assets_only_n"):
        m[k] = mn.get(k)
    m["ratios_ex_financials_note"] = ("debt_to_equity, current_ratio, interest_coverage, asset_turnover: market "
                                      "value excludes ICB 8300/8500/8700 (coverage % of that non-financial cap)")
    res["market"] = m
    mk = ind["market"]
    for k in CARD_NULLABLE:
        mk.pop(k, None)
    mk.update(m)
    for ex, blk in mk.get("by_exchange", {}).items():
        a = valuation_agg([s for s in allsyms if board[s] == ex], mcap, cv, True)
        res[ex] = a
        for k in ("pe", "pe_coverage_pct", "pb", "pb_coverage_pct", "div_yield_pct"):
            blk[k] = a[k]
    as_of = ind.get("as_of") or "2026-10-30"
    f = ind["basis"]["fields"]
    f.pop("pe, pb, div_yield_pct, roe_pct", None)
    f.pop("rev_yoy_pct, npat_yoy_pct, growth_quarter", None)
    rec = ("each company's own latest four consecutive reported quarters ending Q3/2026 or Q2/2026 (older → "
           "excluded)")
    f["sample and recency (valuation, growth and ratio keys)"] = (
        "All keys below are ≈ derived aggregates (Σ over companies ÷ Σ over the same companies; no medians), computed "
        "by tools/build/industries_fundamentals.py valuation from Vietcap IQ quarterly statements (consolidated "
        "where the company has subsidiaries; as served by trading.vietcap.com.vn/api/iq-insight-service). TTM = "
        + rec + "; year-earlier TTM = the four quarters ending one year before that company's end. Balance-sheet "
        "items at each company's latest reported quarter-end (Q3/2026 or Q2/2026). Caps = mcap_bn_vnd (close × "
        "listed shares, " + as_of + "). *_n = companies included; *_coverage_pct = their share of the industry "
        "cap. Not the exchange's or a broker's figures (those use index/free-float weights and their own EPS).")
    f["pe"] = ("Σ market cap ÷ Σ TTM profit after tax attributable to parent shareholders (isa22; isa20 where "
               "isa22 is missing). Loss-makers INCLUDED (pe_loss_makers_n); null when the aggregate TTM profit "
               "≤ 0 (pe_null_reason). Keys pe_n, pe_coverage_pct, pe_loss_makers_n.")
    f["pb"] = ("Σ market cap ÷ Σ parent shareholders' equity (bsa78 less minority interests bsa210 where bsa78 "
               "includes them) at the latest quarter-end; companies with equity ≤ 0 excluded. Banks: book = parent "
               "equity. Keys pb_n, pb_coverage_pct.")
    f["roe_pct"] = ("Σ TTM parent profit ÷ Σ latest parent equity over companies in both the pe and pb samples "
                    "(roe_n); period-end equity, not average. Not a reported ROE.")
    f["div_yield_pct"] = (
        "Σ cash dividends paid in the TTM ÷ Σ market cap of the same companies, %. Dividends = cash-flow statement "
        "line 'Cổ tức, lợi nhuận đã trả cho chủ sở hữu' (Vietcap cfa32, financing activities), " + rec + " of "
        "cash-flow statements (quarters are discrete in the source; a reported quarter with the line blank counts "
        "as 0). Stock dividends and bonus shares are not cash and are not in this line. In consolidated statements "
        "this line also includes dividends paid by subsidiaries to minority (non-controlling) shareholders — it "
        "cannot be separated in the source, so the yield is overstated for groups with large minority stakes; "
        "not adjusted. Cash paid ≠ declared: timing follows payment dates. div_n, div_payers_n, div_coverage_pct.")
    f["rev_yoy_pct, npat_yoy_pct, growth_quarter"] = (
        "Like-for-like TTM vs year-earlier TTM over companies with revenue and parent profit in all eight quarters "
        "(rev_yoy_n, rev_yoy_coverage_pct); revenue definitions as IND_ICB.fundamentals (net sales; banks total "
        "operating income; securities firms net operating revenue; insurers net insurance revenue). npat_yoy_pct "
        "null when the year-earlier aggregate profit ≤ 0. growth_quarter names the TTM end; a mixed sample is "
        "labelled 'mixed: TTM Q3/2026 (x% of sample cap), TTM Q2/2026 (y%) — each vs its own year-earlier TTM'. "
        "Differs from fundamentals.*_yoy_pct (TTM column), which uses one common window ending Q3/2026 and also "
        "requires total assets.")
    f["net_margin_pct"] = ("Σ TTM parent profit ÷ Σ TTM revenue over the rev_yoy sample (net_margin_n). Banks' "
                           "denominator is total operating income, brokers' net operating revenue, insurers' net "
                           "insurance revenue — not comparable with non-financial industries.")
    f["eps_growth_pct"] = (
        "Σ_i shares_now_i × TTM EPS_now_i ÷ Σ_i shares_now_i × TTM EPS_yearago_i − 1, with year-ago EPS restated to "
        "today's share count (so issuance/splits do not show as growth) — this reduces exactly to like-for-like TTM "
        "parent-profit growth over companies with both windows and share counts (eps_growth_n, "
        "eps_growth_coverage_pct). Vietcap's reported EPS (isa23) is not used: it is blank/0 for many companies and "
        "not split-adjusted. eps_growth_per_share_pct (supplementary) does NOT restate: shares = paid-in capital "
        "bsa80 ÷ VND 10,000 par at each window end, so any increase in shares (cash issues, stock dividends, "
        "conversions) counts as dilution; treasury shares not deducted.")
    f["debt_to_equity"] = (
        "×, non-financial industries: Σ interest-bearing debt ÷ Σ parent equity at the latest quarter-end, pb "
        "sample (de_n, de_coverage_pct). Debt = short-term borrowings & finance leases bsa56 + long-term "
        "borrowings & finance leases bsa71 + convertible bonds bsa173 (straight bonds issued are within bsa56/"
        "bsa71 in the VAS layout). Denominator is parent equity, so groups with large minority interests show a "
        "higher D/E than Vietcap's debtPerEquity (which divides by total equity incl. minorities, bsa78). "
        "Null for ICB 8300 Banks, 8500 Insurance, 8700 Financial Services (deposits/"
        "interbank/repo funding are operating liabilities); for those liab_to_equity = Σ total liabilities "
        "bsa54 ÷ Σ parent equity is given instead. Market value excludes those three industries.")
    f["current_ratio"] = ("×: Σ current assets bsa1 ÷ Σ current liabilities bsa55 at the latest quarter-end, pb "
                          "sample with both lines (cr_n, cr_coverage_pct). Null for ICB 8300, 8500, 8700 (the "
                          "bank/insurer/broker VAS formats do not split current/non-current, or not comparably).")
    f["interest_coverage"] = (
        "×: Σ TTM EBIT ÷ Σ TTM interest expense over companies in the rev_yoy sample that report interest expense "
        "> 0 (ic_n, ic_coverage_pct); ic_no_debt_n = companies with zero/blank interest expense (excluded). EBIT = "
        "profit before tax isa16 + interest expense isa8 ('Chi phí lãi vay', within financial expenses). Shown even "
        "if negative, with ic_negative_ebit = true. Null for ICB 8300, 8500, 8700.")
    f["asset_turnover"] = (
        "×: Σ TTM revenue ÷ Σ average total assets (bsa53, mean of the TTM-end quarter and four quarters earlier; "
        "latest only where the earlier one is missing — at_latest_assets_only_n), rev_yoy sample (at_n, "
        "at_coverage_pct). Null for ICB 8300, 8500, 8700.")
    f["roa_pct"] = (
        "%, ≈: Σ TTM parent-attributable profit ÷ Σ average total assets (bsa53, mean of the TTM-end quarter and four "
        "quarters earlier; latest only where the earlier one is missing — roa_latest_assets_only_n), P/E sample with "
        "assets (roa_n, roa_coverage_pct). Includes banks, insurers and brokers (for banks ROA is the usual measure; "
        "their balance sheets are 10× leveraged so ROA is far below other industries). Parent profit over total "
        "(incl. minorities') assets understates ROA slightly where minorities are large.")
    f["valuation caveats"] = (
        "(1) Double counting: a listed parent consolidating a listed subsidiary (VIC–VHM/VRE/VPL, MWG–DMX, GVR–PHR/"
        "DPR, GAS–PGD …) puts the subsidiary's profit, book, debt and dividends in both companies; cap is counted "
        "twice too, so ratios move roughly proportionally rather than in one direction, but the subsidiary's weight "
        "is overstated. (2) TTM timing: most cap uses Q4/2025–Q3/2026, the rest Q3/2025–Q2/2026 (see growth_quarter). "
        "(3) One-off gains (divestments, revaluations) in the window lower P/E and raise growth; quarterly reports "
        "are unaudited and may be revised. (4) Caps use listed shares; ex-rights shares not yet listed (e.g. TCB, "
        "ex-date 2026-10-29) understate cap. (5) Full listed universe incl. UPCoM and total (not free-float) cap — "
        "differs from VN-Index P/E. (6) Survivorship: today's constituents. Descriptive only, not investment advice.")
    if dry_run:
        print(json.dumps(res, ensure_ascii=False, indent=1))
        return res
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False, indent=1)
        fh.write("\n")
    print("wrote", out_path, "valuation keys on", len(ind["industries"]), "cards + market")
    return res


# ------------------------------------------------------------------------------------------------------ quarterly
Q_FIRST = (2018, 4)                       # first quarter-end of IND_ICB.quarterly (index = 100 there)
Q_RATIOS = {                              # quarterly ratio key → valuation_agg coverage key(s) for the 60 % rule
    "pe": ("pe_coverage_pct",), "pb": ("pb_coverage_pct",), "roe_pct": ("pe_coverage_pct", "pb_coverage_pct"),
    "roa_pct": ("roa_coverage_pct",), "net_margin_pct": ("rev_yoy_coverage_pct",),
    "debt_to_equity": ("de_coverage_pct",), "liab_to_equity": ("de_coverage_pct",),
    "current_ratio": ("cr_coverage_pct",), "interest_coverage": ("ic_coverage_pct",),
    "asset_turnover": ("at_coverage_pct",), "eps_growth_pct": ("eps_growth_coverage_pct",),
    "rev_yoy_pct": ("rev_yoy_coverage_pct",), "div_yield_pct": ("div_coverage_pct",)}
BS_EX_FIN = ("debt_to_equity", "current_ratio", "interest_coverage", "asset_turnover")


def _iso_day(x):
    s = str(x)
    if s.isdigit():                       # Vietcap gap-chart epoch seconds (UTC midnight of the session)
        from datetime import datetime, timezone
        return datetime.fromtimestamp(int(s), timezone.utc).date().isoformat()
    return s[:10]


def _month_before(day):
    y, m, d = int(day[:4]), int(day[5:7]), int(day[8:10])
    y, m = (y, m - 1) if m > 1 else (y - 1, 12)
    return f"{y}-{m:02d}-{min(d, calendar.monthrange(y, m)[1]):02d}"


def _cw_ret(mem, key_now, key_base, k, cap_all):
    """Cap-weighted price change Σ ls×c_now ÷ Σ ls×c_base − 1 over members with both closes (as the cards'
    ret_ytd_pct / ret_1m_pct). → (pct or None, coverage % of today's cap)."""
    pair = [r for r in mem if r[key_now][k] and r[key_base][k]]
    base = sum(r[key_base][k] * r["ls"] for r in pair)
    cov = round(100.0 * sum(r["mcap"] for r in pair) / cap_all, 1) if cap_all else None
    if not base:
        return None, cov
    return round(100.0 * (sum(r[key_now][k] * r["ls"] for r in pair) / base - 1), 2), cov


def quarterly(stocks_path, raw_dir, hist_path, out_path, as_of, min_cov, dry_run):
    """IND_ICB.quarterly: quarter-end price indices (history_block's screen and chain-linking, rebalanced
    quarterly), discrete-quarter revenue / parent profit / total assets, and the card ratios (valuation_agg) at each
    quarter-end with quarter-end caps and each company's trailing four quarters ending that quarter."""
    sys.path.insert(0, os.path.join(ROOT, "tools", "build"))
    import industries_vnstock as IV       # screened_closes / data screen shared with history_block
    stocks = json.load(open(stocks_path, encoding="utf-8"))
    data = json.load(open(out_path, encoding="utf-8"))
    ind = data["IND_ICB"]
    icbs = [x["icb"] for x in ind["industries"]]
    meta = {x["icb"]: x for x in ind["industries"]}
    mcap_today = {r["sym"]: float(r.get("mcap") or 0) for r in stocks}
    by_icb = collections.defaultdict(list)
    for r in stocks:
        by_icb[r["icb"]].append(r["sym"])
    allsyms = [r["sym"] for r in stocks]
    nonfin = [r["sym"] for r in stocks if r["icb"] not in FIN_ICB]

    # ---- axis
    ad = date.fromisoformat(as_of)
    last_q = (ad.year, (ad.month - 1) // 3 + 1)
    qs, k = [], Q_FIRST
    while k <= last_q:
        qs.append(k)
        k = _qminus(k, -1)
    qend = []
    for (y, q) in qs:
        m = 3 * q
        qend.append(min(f"{y}-{m:02d}-{calendar.monthrange(y, m)[1]:02d}", as_of))
    qtd = qend[-1] < f"{last_q[0]}-{3 * last_q[1]:02d}-{calendar.monthrange(last_q[0], 3 * last_q[1])[1]:02d}"
    dd = f"{as_of[8:10]}/{as_of[5:7]}"
    keys = [f"{y}-Q{q}" for y, q in qs]
    lab_vi = [f"Q{q}/{y}" for y, q in qs]
    lab_en = [f"Q{q} {y}" for y, q in qs]
    if qtd:
        lab_vi[-1] += f" (đến {dd})"
        lab_en[-1] += " QTD"

    # ---- prices (same data screen as history_block)
    hist = {s: {"t": [_iso_day(x) for x in h["t"]], "c": h["c"]}
            for s, h in json.load(open(hist_path, encoding="utf-8")).items() if isinstance(h, dict) and h.get("t")}
    px_in = [{"sym": r["sym"], "icb": r["icb"], "mcap": r.get("mcap"), "ls": r.get("ls")} for r in stocks]
    ybase = [f"{y - 1}-12-31" for y, _ in qs]
    m1 = [_month_before(d) for d in qend]
    mem, dropped_series, dropped_closes = IV.screened_closes(px_in, hist, {"q": qend, "py": ybase, "m1": m1})
    memo = {r["sym"]: r for r in mem}
    nq = len(qs)
    # quarter-end cap per stock (bn VND) = screened adj close × today's listed shares
    mcap_q = [{r["sym"]: (r["q"][i] * r["ls"] / BN if r["q"][i] and r.get("ls") else 0.0) for r in mem}
              for i in range(nq)]

    # ---- statements
    inputs, tpl_count = {}, collections.Counter()
    for r in stocks:
        c = company_inputs(raw_dir, r["sym"])
        if c:
            inputs[r["sym"]] = c
            tpl_count[c[0]["tpl"]] += 1
    panels = {s: c[0] for s, c in inputs.items()}
    tot_today = sum(mcap_today.values())
    qrec = [{s: p["quarterly"].get(q) for s, p in panels.items()} for q in qs]
    mkt_fcov = [round(100.0 * sum(mcap_today[s] for s, r in qrec[i].items() if _ok(r)) / tot_today, 1)
                for i in range(nq)]
    fund_ok = [c >= min_cov for c in mkt_fcov]
    fl = max((i for i in range(nq) if fund_ok[i]), default=None)
    if fl is None:
        sys.exit(f"no quarter reaches {min_cov}% statement coverage: {mkt_fcov}")
    fund_null_q = [keys[i] for i in range(fl + 1) if not fund_ok[i]]
    in_fund = [i <= fl and fund_ok[i] for i in range(nq)]

    # per-company valuation inputs at each quarter-end (exact window ending that quarter)
    cv = [{s: company_val(raw_dir, s, (0, 0), qs[i], inputs[s]) for s in inputs} if in_fund[i] else None
          for i in range(nq)]

    def group(members, fin, bs_members=None):
        cap_all = sum(mcap_today[s] for s in members)
        mm = [memo[s] for s in members]
        out = {k2: [] for k2 in ("index", "ret_pct", "mcap_bn_vnd", "n_price", "price_coverage_pct",
                                 "ret_ytd_pct", "ret_1m_pct", "revenue_bn_vnd", "net_profit_bn_vnd",
                                 "total_assets_bn_vnd", "n_fund", "fund_coverage_pct", *Q_RATIOS)}
        rcov = {k2: [] for k2 in Q_RATIOS}
        nulled = collections.defaultdict(list)
        for i in range(nq):
            have = [r for r in mm if r["q"][i]]
            out["n_price"].append(len(have))
            out["price_coverage_pct"].append(round(100.0 * sum(r["mcap"] for r in have) / cap_all, 1)
                                             if cap_all else None)
            out["mcap_bn_vnd"].append(_r(sum(r["q"][i] * r["ls"] for r in have) / BN) if have else None)
            idx = out["index"]
            if not idx or idx[-1] is None:                       # history_block chain-linking, per quarter
                out["ret_pct"].append(None)
                idx.append(100.0 if have else None)
            else:
                pair = [r for r in have if r["q"][i - 1]]
                base = sum(r["q"][i - 1] * r["ls"] for r in pair)
                if not base:
                    out["ret_pct"].append(None); idx.append(None)
                else:
                    rr = sum(r["q"][i] * r["ls"] for r in pair) / base - 1
                    out["ret_pct"].append(round(100 * rr, 2)); idx.append(idx[-1] * (1 + rr))
            for key, bkey in (("ret_ytd_pct", "py"), ("ret_1m_pct", "m1")):
                v, c = _cw_ret(mm, "q", bkey, i, cap_all)
                if (v is not None and (c is None or c < LOW_COV)) or (c is not None and 0 < c < LOW_COV):
                    nulled[key].append(keys[i]); v = None
                out[key].append(v)
            # discrete-quarter statement sums
            if in_fund[i]:
                ok = {s: qrec[i].get(s) for s in members if _ok(qrec[i].get(s))}
                out["revenue_bn_vnd"].append(_r(sum(r["rev"] for r in ok.values()) / BN) if ok else None)
                out["net_profit_bn_vnd"].append(_r(sum(r["np"] for r in ok.values()) / BN) if ok else None)
                out["total_assets_bn_vnd"].append(_r(sum(r["ta"] for r in ok.values()) / BN) if ok else None)
                out["n_fund"].append(len(ok))
                out["fund_coverage_pct"].append(round(100.0 * sum(mcap_today[s] for s in ok) / cap_all, 1)
                                                if cap_all else None)
                a = valuation_agg(members, mcap_q[i], cv[i], fin)
                # coverage as % of TODAY's industry cap: same samples, weights = today's cap of priced members
                masked = {s: (mcap_today[s] if mcap_q[i].get(s) else 0.0) for s in members}
                a2 = valuation_agg(members, masked, cv[i], fin)
                scale = sum(masked.values()) / cap_all if cap_all else 0
                if bs_members is not None:                       # market: balance-sheet ratios ex-financials
                    b = valuation_agg(bs_members, mcap_q[i], cv[i], False)
                    mb = {s: (mcap_today[s] if mcap_q[i].get(s) else 0.0) for s in bs_members}
                    b2 = valuation_agg(bs_members, mb, cv[i], False)
                    cap_nf = sum(mcap_today[s] for s in bs_members)
                    sc_nf = sum(mb.values()) / cap_nf if cap_nf else 0
                for key, ckeys in Q_RATIOS.items():
                    src, src2, sc = a, a2, scale
                    if bs_members is not None and key in BS_EX_FIN:
                        src, src2, sc = b, b2, sc_nf
                    v = src.get(key)
                    cs = [src2.get(ck) for ck in ckeys]
                    c = None if any(x is None for x in cs) else round(min(cs) * sc, 1)
                    rcov[key].append(c)
                    if (v is not None and (c is None or c < LOW_COV)) or (c is not None and 0 < c < LOW_COV):
                        nulled[key].append(keys[i]); v = None     # thin sample (listed even if v was null anyway)
                    out[key].append(v)
            else:
                for k2 in ("revenue_bn_vnd", "net_profit_bn_vnd", "total_assets_bn_vnd", "n_fund",
                           "fund_coverage_pct", *Q_RATIOS):
                    out[k2].append(None)
                for k2 in Q_RATIOS:
                    rcov[k2].append(None)
        out["index"] = [None if v is None else round(v, 2) for v in out["index"]]
        out["ratio_coverage_pct"] = rcov
        out["nulled_low_coverage"] = dict(nulled)
        pl = [keys[i] for i, c in enumerate(out["price_coverage_pct"]) if c is not None and c < LOW_COV]
        fl_ = [keys[i] for i, c in enumerate(out["fund_coverage_pct"]) if c is not None and c < LOW_COV]
        out["low_coverage_quarters"] = [x for x in keys if x in pl or x in fl_]
        out["low_coverage_quarters_price"], out["low_coverage_quarters_fund"] = pl, fl_
        return out

    blk_ind = {}
    for icb in icbs:
        g = group(by_icb.get(icb, []), icb in FIN_ICB)
        if icb in FIN_ICB:
            for k2 in BS_EX_FIN:
                g[k2] = [None] * nq
        else:
            g["liab_to_equity"] = [None] * nq
        blk_ind[icb] = {"name_vi": meta[icb]["name_vi"], "name_en": meta[icb]["name_en"], **g}
    market = group(allsyms, False, bs_members=nonfin)
    market["liab_to_equity"] = [None] * nq
    for icb in icbs:
        blk_ind[icb]["mcap_share_pct"] = [round(100.0 * a / b, 2) if a is not None and b else None
                                          for a, b in zip(blk_ind[icb]["mcap_bn_vnd"], market["mcap_bn_vnd"])]
    market["mcap_share_pct"] = [100.0 if v else None for v in market["mcap_bn_vnd"]]
    vh = hist.get("VNINDEX")
    market["vnindex"] = [IV._cob(vh["t"], vh["c"], d) for d in qend] if vh else [None] * nq
    vv = market["vnindex"]
    market["vnindex_ret_pct"] = [None] + [round(100 * (b / a - 1), 2) if a and b else None for a, b in zip(vv, vv[1:])]
    market["statement_coverage_pct"] = mkt_fcov

    # ---- sanity checks
    checks = {}
    fu = ind.get("fundamentals") or {}
    if fu:
        yrs = [p for p in fu["periods"] if p.isdigit()]
        rows = {}
        for y in yrs:
            ii = [keys.index(f"{y}-Q{q}") for q in (1, 2, 3, 4) if f"{y}-Q{q}" in keys]
            if len(ii) < 4:
                continue
            sq = [market["net_profit_bn_vnd"][i] for i in ii]
            fy = fu["market"]["net_profit_bn_vnd"][fu["periods"].index(y)]
            if None in sq or fy is None:
                continue
            rows[y] = {"sum_4_quarters_bn_vnd": sum(sq), "fundamentals_fy_bn_vnd": fy,
                       "gap_pct": round(100.0 * (sum(sq) / fy - 1), 2)}
        # same company set (all 4 quarters + FY present), to separate the sample effect from restatements
        for y in list(rows):
            yi = int(y)
            same = [s for s, p in panels.items() if _ok(p["annual"].get(yi))
                    and all(_ok(p["quarterly"].get((yi, q))) for q in (1, 2, 3, 4))]
            a_ = sum(panels[s]["annual"][yi]["np"] for s in same) / BN
            q_ = sum(panels[s]["quarterly"][(yi, q)]["np"] for s in same for q in (1, 2, 3, 4)) / BN
            rows[y]["same_companies_n"] = len(same)
            rows[y]["same_companies_gap_pct"] = round(100.0 * (q_ / a_ - 1), 2) if a_ else None
        checks["profit_sum_of_quarters_vs_fundamentals_fy_market"] = rows
    hi = ind.get("history") or {}
    if hi:
        cmp_ = {}
        for j, y in enumerate(hi["years"]):
            kq = f"{y}-Q4"
            if kq in keys:
                cmp_[str(y)] = {"quarterly_q4_index": market["index"][keys.index(kq)],
                                "history_year_end_index": hi["market"]["index"][j]}
        checks["market_index_q4_vs_history_year_end"] = cmp_
        cmp_i = {}
        for icb in icbs:
            hq = hi["by_icb"].get(icb, {}).get("index") or []
            dif = [abs(blk_ind[icb]["index"][keys.index(f"{y}-Q4")] / hq[j] - 1) * 100
                   for j, y in enumerate(hi["years"]) if f"{y}-Q4" in keys and hq[j]
                   and blk_ind[icb]["index"][keys.index(f"{y}-Q4")]]
            cmp_i[icb] = round(max(dif), 1) if dif else None
        checks["industry_q4_index_max_abs_gap_vs_history_pct"] = cmp_i
    checks["market_vs_vnindex_quarter_returns"] = {
        keys[i]: [market["ret_pct"][i], market["vnindex_ret_pct"][i]] for i in range(1, nq)}
    li = fl
    card_cmp = {}
    for key in ("pe", "pb", "roe_pct", "roa_pct", "net_margin_pct", "debt_to_equity", "current_ratio",
                "interest_coverage", "asset_turnover", "eps_growth_pct", "rev_yoy_pct", "div_yield_pct"):
        card_cmp[key] = {"market": [market[key][li], ind["market"].get(key)],
                         "by_icb": {icb: [blk_ind[icb][key][li], meta[icb].get(key)] for icb in icbs}}
    card_cmp["ret_ytd_pct"] = {"market": [market["ret_ytd_pct"][-1], ind["market"].get("ret_ytd_pct")],
                               "by_icb": {icb: [blk_ind[icb]["ret_ytd_pct"][-1], meta[icb].get("ret_ytd_pct")]
                                          for icb in icbs}}
    card_cmp["mcap_share_pct"] = {"by_icb": {icb: [blk_ind[icb]["mcap_share_pct"][-1], meta[icb].get("mcap_share_pct")]
                                             for icb in icbs}}
    checks["latest_vs_cards"] = {"quarter": keys[li], "pair": "[quarterly value at " + keys[li] + ", card value]",
                                 "values": card_cmp}

    fq = keys[fl]
    fql = lab_vi[fl]
    block = {
        "as_of": as_of, "built": date.today().isoformat(),
        "quarters": keys, "labels_vi": lab_vi, "labels_en": lab_en, "quarter_end_dates": qend,
        "qtd_quarter": keys[-1] if qtd else None,
        "fund_last_quarter": fq, "fund_min_coverage_rule_pct": min_cov,
        "fund_market_coverage_pct": dict(zip(keys, mkt_fcov)),
        "fund_quarters_nulled_below_rule": fund_null_q,
        "fund_quarters_after_last": keys[fl + 1:],
        "ratio_null_rule_pct": LOW_COV,
        "units": "bn VND; returns, margins, growth, yields in %; ratios ×; coverage = % of today's industry cap",
        "by_icb": blk_ind, "market": market,
        "fields": {
            "index / ret_pct": "Cap-weighted price index, 100 at end-Q4/2018, chain-linked quarterly: return in "
                               "quarter Q = Σ(current listed shares × adj. close end-Q) ÷ Σ(current listed shares × "
                               "adj. close end-(Q−1)) − 1 over stocks with both closes (as IND_ICB.history, but "
                               "rebalanced every quarter instead of every year). ret_pct null in the first quarter.",
            "mcap_bn_vnd": "≈ Σ adj. close at quarter end × today's listed shares (approximate, as history).",
            "mcap_share_pct": "≈ industry mcap_bn_vnd ÷ market mcap_bn_vnd at the same quarter end.",
            "ret_ytd_pct": "Cap-weighted price change from the previous year-end to the quarter end (as the cards' "
                           "ret_ytd_pct); null for 2018 (no end-2017 prices in the pull).",
            "ret_1m_pct": "Cap-weighted price change over the month to the quarter end (same day one month "
                          "earlier), as the cards' ret_1m_pct.",
            "price_coverage_pct / n_price": "Stocks with a screened close at the quarter end; their share of "
                                            "today's industry cap.",
            "revenue_bn_vnd / net_profit_bn_vnd": "Σ over the quarter itself (discrete quarter, not TTM), "
                                                  "definitions as IND_ICB.fundamentals (template-specific revenue; "
                                                  "profit after tax attributable to parent shareholders, PAT "
                                                  "fallback).",
            "total_assets_bn_vnd": "Σ total assets at the quarter end (bsa53).",
            "n_fund / fund_coverage_pct": "Companies with revenue, parent profit and total assets for that "
                                          "quarter (all three sums use this set); share of today's industry cap.",
            "pe, pb, roe_pct, roa_pct, net_margin_pct, debt_to_equity, liab_to_equity, current_ratio, "
            "interest_coverage, asset_turnover, eps_growth_pct, rev_yoy_pct, div_yield_pct":
                "Same definitions as the card valuation step (industries_fundamentals.py valuation_agg), evaluated at "
                "each quarter end: caps = quarter-end adj. close × today's listed shares; TTM = each company's four "
                "quarters ending exactly at that quarter (no fallback to an earlier window); balance sheet at that "
                "quarter end; year-earlier TTM = the four quarters before. Market debt_to_equity, current_ratio, "
                "interest_coverage, asset_turnover exclude ICB 8300/8500/8700; for those industries debt_to_equity "
                "etc. are null and liab_to_equity is given. net_margin_pct, interest_coverage and asset_turnover use "
                "the rev_yoy sample (eight quarters needed), so they start at Q4/2019. Run to fund_last_quarter only.",
            "ratio_coverage_pct": "Per ratio: share of today's industry cap in that ratio's sample (roe = the smaller "
                                  "of the pe and pb samples).",
            "nulled_low_coverage": "Quarters where a ratio or ret_ytd_pct/ret_1m_pct was computed but its sample "
                                   "covered < 60% of today's industry cap, so it is set to null.",
            "low_coverage_quarters": "Quarters where price or statement coverage < 60% of today's industry cap "
                                     "(detail in low_coverage_quarters_price / _fund); values are kept, flagged.",
            "market.vnindex / vnindex_ret_pct": "VN-Index close on/before each quarter end (same Vietcap feed); "
                                                "quarterly change ≈ derived from those closes.",
        },
        "source": "Prices: Vietcap chart/OHLCChart/gap-chart adjusted daily closes (vnstock Quote VCI feed), data "
                  "screen and listed shares as IND_ICB.history. Statements: Vietcap IQ financial statements "
                  "(INCOME_STATEMENT, BALANCE_SHEET, CASH_FLOW quarters) via trading.vietcap.com.vn/api/"
                  "iq-insight-service, as IND_ICB.fundamentals. Built by tools/build/industries_fundamentals.py "
                  "quarterly.",
        "source_url": "https://trading.vietcap.com.vn/api/iq-insight-service/v1/company/{SYM}/financial-statement?section=INCOME_STATEMENT",
        "basis_vi": (
            "Theo quý, từ Q4/2018 đến " + lab_vi[-1] + ", các mã niêm yết HÔM NAY (HOSE/HNX/UPCoM, ICB cấp 2 hiện "
            "nay). Chỉ số giá ngành gia quyền vốn hóa = 100 tại cuối Q4/2018, nối chuỗi theo quý (giá đóng cửa điều "
            "chỉnh của Vietcap × số CP niêm yết hiện tại, cùng bộ lọc dữ liệu với IND_ICB.history), tái cân bằng mỗi "
            "quý. Vốn hóa cuối quý chỉ XẤP XỈ: giá điều chỉnh × số cổ phiếu HIỆN NAY (cùng cách tính với "
            "IND_ICB.history) — phát hành thêm sau đó làm phóng đại vốn hóa quá khứ. Doanh thu và lợi nhuận là số "
            "của RIÊNG quý đó (không phải 12 tháng), lợi nhuận thuộc cổ đông công ty mẹ; tổng tài sản tại cuối quý. "
            "P/E, P/B, ROE, ROA, biên lợi nhuận, nợ vay/VCSH, thanh toán hiện hành, khả năng trả lãi, vòng quay tài "
            "sản, tăng EPS, tăng doanh thu, tỷ suất cổ tức: cùng định nghĩa với thẻ ngành nhưng tính tại từng cuối "
            "quý, với vốn hóa cuối quý và 4 quý gần nhất kết thúc tại quý đó của từng công ty (≈, số tổng hợp). Số "
            "liệu báo cáo tài chính chỉ đến " + fql + " (quý mới nhất có ≥ " + f"{min_cov:.0f}" + "% vốn hóa đã "
            "báo cáo); quý sau đó để trống. Tỷ số có mẫu dưới 60% vốn hóa ngành hiện nay thì để trống "
            "(nulled_low_coverage). Chỉ mang tính mô tả."),
        "basis_en": (
            "Quarterly, Q4 2018 to " + lab_en[-1] + ", over stocks listed TODAY (HOSE/HNX/UPCoM, today's ICB level "
            "2). Cap-weighted industry price index = 100 at end-Q4 2018, chain-linked quarterly (Vietcap adjusted "
            "closes × current listed shares, same data screen as IND_ICB.history), rebalanced every quarter. "
            "Quarter-end caps are APPROXIMATE: adjusted close × TODAY's share count (the same approximation as "
            "IND_ICB.history) — later issuance inflates past caps. Revenue and profit are for that quarter ALONE "
            "(not TTM), profit attributable to parent shareholders; total assets at the quarter end. P/E, P/B, ROE, "
            "ROA, net margin, debt/equity, current ratio, interest cover, asset turnover, EPS growth, revenue "
            "growth, dividend yield: the card definitions evaluated at each quarter end, with quarter-end caps and "
            "each company's four quarters ending that quarter (≈, aggregates). Statement data run to " + fq.replace(
                "-", " ") + " (latest quarter with ≥ " + f"{min_cov:.0f}" + "% of cap reported); later quarters are "
            "null. Ratios whose sample covers < 60% of today's industry cap are null (nulled_low_coverage). "
            "Descriptive only."),
        "caveats_vi": [
            "Thiên lệch sống sót: chỉ gồm mã còn niêm yết ngày " + f"{as_of[8:10]}/{as_of[5:7]}/{as_of[:4]}" +
            "; các quý đầu thiếu mã đã hủy niêm yết; mã niêm yết muộn chỉ vào chuỗi từ quý có giá/báo cáo.",
            "Phân ngành theo mã ICB hiện nay, áp cho mọi quý.",
            "Vốn hóa cuối quý = giá điều chỉnh × số CP hiện nay: xấp xỉ, phóng đại vốn hóa quá khứ của công ty phát "
            "hành thêm; P/E, P/B, tỷ suất cổ tức quá khứ chịu cùng sai lệch.",
            "Đếm hai lần khi công ty mẹ và công ty con cùng niêm yết (VIC–VHM/VRE/VPL, MWG–DMX, GVR–PHR/DPR, "
            "GAS–PGD…): doanh thu, lợi nhuận, tài sản, vốn hóa của công ty con nằm ở cả hai.",
            "Doanh thu toàn thị trường cộng nhiều định nghĩa (doanh thu thuần, thu nhập hoạt động ngân hàng, doanh "
            "thu CTCK/bảo hiểm) — chỉ là tổng mô tả.",
            "Số quý chưa kiểm toán, có thể được điều chỉnh; tổng 4 quý có thể khác số năm đã kiểm toán "
            "(xem sanity_checks). Doanh thu/lợi nhuận quý có tính mùa vụ.",
            "Thay đổi phạm vi hợp nhất làm gãy chuỗi (ví dụ FPT không còn hợp nhất toàn bộ FPT Telecom từ 2026).",
            "Quý có độ phủ dưới 60% vốn hóa hiện nay của ngành: liệt kê ở low_coverage_quarters (giá trị giữ "
            "nguyên); tỷ số dưới 60% thì để trống (nulled_low_coverage).",
            lab_vi[-1] + " chưa trọn quý." if qtd else "",
        ],
        "caveats_en": [
            "Survivorship: only stocks listed on " + as_of + "; early quarters miss delisted names; late listings "
            "enter from their first priced/reported quarter.",
            "Today's ICB classification is applied to every quarter.",
            "Quarter-end cap = adjusted close × today's share count: approximate, inflating past caps of companies "
            "that issued shares since; past P/E, P/B and dividend yield carry the same bias.",
            "Double counting where a listed parent consolidates a listed subsidiary (VIC–VHM/VRE/VPL, MWG–DMX, "
            "GVR–PHR/DPR, GAS–PGD …): the subsidiary's revenue, profit, assets and cap appear in both.",
            "Market revenue adds different definitions (net sales, bank operating income, broker/insurer top lines) "
            "— a descriptive total only.",
            "Quarterly reports are unaudited and can be revised; four quarters can differ from the audited year "
            "(see sanity_checks). Quarterly revenue and profit are seasonal.",
            "Changes in consolidation scope break series (e.g. FPT no longer fully consolidates FPT Telecom from "
            "2026).",
            "Quarters covering < 60% of the industry's current cap are listed in low_coverage_quarters (values "
            "kept); ratios below 60% are null (nulled_low_coverage).",
            lab_en[-1] + " is not a complete quarter." if qtd else "",
        ],
        "data_screen": {"rule": "as IND_ICB.history.data_screen", "series_dropped": dropped_series,
                        "closes_dropped": dropped_closes},
        "built_from": {"stocks": len(stocks), "with_statements": len(inputs), "templates": dict(tpl_count),
                       "with_price_history": sum(1 for s in allsyms if s in hist)},
        "sanity_checks": checks,
        "disclaimer": DISCLAIMER,
    }
    block["caveats_vi"] = [c for c in block["caveats_vi"] if c]
    block["caveats_en"] = [c for c in block["caveats_en"] if c]
    if dry_run:
        return block
    ind["quarterly"] = block
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False, indent=1)
        fh.write("\n")
    print("wrote", out_path, "IND_ICB.quarterly;", nq, "quarters, statements to", fq)
    return block


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    a = sub.add_parser("pull"); a.add_argument("--stocks", required=True); a.add_argument("--raw", required=True)
    a.add_argument("--threads", type=int, default=5)
    a.add_argument("--sections", default=",".join(SECTIONS),
                   help="comma list; CASH_FLOW is needed by `valuation` for div_yield_pct")
    a.add_argument("--only", default=None, help="comma list of symbols (test pulls)")
    b = sub.add_parser("build"); b.add_argument("--stocks", required=True); b.add_argument("--raw", required=True)
    b.add_argument("--out", default=DEFAULT_OUT); b.add_argument("--ttm-min-cov", type=float, default=80.0)
    b.add_argument("--cross-checks", default=None, help="JSON file with published-figure comparisons to embed")
    b.add_argument("--dry-run", action="store_true")
    v = sub.add_parser("valuation", help="fill card pe/pb/roe_pct and market pe/pb from the raw statements")
    v.add_argument("--stocks", required=True); v.add_argument("--raw", required=True)
    v.add_argument("--out", default=DEFAULT_OUT); v.add_argument("--dry-run", action="store_true")
    v.add_argument("--dump-companies", default=None, help="write per-company inputs (JSON) for spot checks")
    q = sub.add_parser("quarterly", help="IND_ICB.quarterly: quarter-end price indices, quarterly statements and "
                                         "card ratios at each quarter end")
    q.add_argument("--stocks", required=True); q.add_argument("--raw", required=True)
    q.add_argument("--hist", required=True, help="daily adjusted closes {SYM: {t: [epoch s | ISO], c: [...]}}, "
                                                 "incl. VNINDEX (the IND_ICB.history pull)")
    q.add_argument("--as-of", required=True, help="last price date (YYYY-MM-DD), as IND_ICB.as_of")
    q.add_argument("--min-cov", type=float, default=80.0,
                   help="statement quarters need >= this %% of today's market cap reported")
    q.add_argument("--out", default=DEFAULT_OUT); q.add_argument("--dry-run", help="write the block to this JSON "
                                                                                   "file instead of --out")
    args = ap.parse_args()
    if args.cmd == "pull":
        pull(args.stocks, args.raw, args.threads, tuple(args.sections.split(",")),
             args.only.split(",") if args.only else None)
    elif args.cmd == "quarterly":
        blk = quarterly(args.stocks, args.raw, args.hist, args.out, args.as_of, args.min_cov, bool(args.dry_run))
        if args.dry_run:
            with open(args.dry_run, "w", encoding="utf-8") as fh:
                json.dump(blk, fh, ensure_ascii=False, indent=1)
            print("dry run: block written to", args.dry_run)
    elif args.cmd == "valuation":
        valuation(args.stocks, args.raw, args.out, args.dry_run, args.dump_companies)
    else:
        build(args.stocks, args.raw, args.out, args.ttm_min_cov, args.dry_run, args.cross_checks)


if __name__ == "__main__":
    main()
