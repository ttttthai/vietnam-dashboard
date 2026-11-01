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
VAL_MIN_END = (2026, 2)   # oldest acceptable latest quarter for TTM earnings / book value


def company_ttm_latest(panel, min_end=VAL_MIN_END):
    """Company's own latest four consecutive reported quarters (parent profit). Returns (np_vnd, end) or None.
    The window ends at the latest quarter with a profit figure; if that window has a gap the previous end is tried,
    as long as the end is not older than min_end."""
    qs = sorted((k for k, v in panel["quarterly"].items() if v["np"] is not None), reverse=True)
    for end in qs[:2]:
        if end < min_end:
            break
        rows = [panel["quarterly"].get(_qminus(end, i)) for i in range(4)]
        if all(r is not None and r["np"] is not None for r in rows):
            return sum(r["np"] for r in rows), end
    return None


def company_book_latest(panel, min_end=VAL_MIN_END):
    """Parent shareholders' equity at the latest quarter-end with a balance sheet (not older than min_end)."""
    qs = sorted((k for k, v in panel["quarterly"].items() if v["eq"] is not None), reverse=True)
    if qs and qs[0] >= min_end:
        return panel["quarterly"][qs[0]]["eq"], qs[0]
    return None


def valuation_agg(members, mcap, ttm, book):
    cap = sum(mcap[s] for s in members)
    pe_s = [s for s in members if s in ttm and mcap[s] > 0]
    pb_s = [s for s in members if s in book and book[s][0] > 0 and mcap[s] > 0]
    both = [s for s in pe_s if s in set(pb_s)]
    e = sum(ttm[s][0] for s in pe_s) / BN
    b = sum(book[s][0] for s in pb_s) / BN
    c_pe, c_pb = sum(mcap[s] for s in pe_s), sum(mcap[s] for s in pb_s)
    e_both, b_both = sum(ttm[s][0] for s in both) / BN, sum(book[s][0] for s in both) / BN
    ends = collections.Counter(f"Q{ttm[s][1][1]}/{ttm[s][1][0]}" for s in pe_s)
    return {
        "pe": round(c_pe / e, 1) if pe_s and e > 0 else None,
        "pe_n": len(pe_s),
        "pe_coverage_pct": round(100.0 * c_pe / cap, 1) if cap else None,
        "pe_loss_makers_n": sum(1 for s in pe_s if ttm[s][0] < 0),
        "pe_earnings_ttm_bn_vnd": _r(e) if pe_s else None,
        "pe_ttm_end_quarters": dict(sorted(ends.items(), reverse=True)),
        "pb": round(c_pb / b, 2) if pb_s and b > 0 else None,
        "pb_n": len(pb_s),
        "pb_coverage_pct": round(100.0 * c_pb / cap, 1) if cap else None,
        "pb_book_bn_vnd": _r(b) if pb_s else None,
        "pb_neg_equity_excluded_n": sum(1 for s in members if s in book and book[s][0] <= 0),
        "roe_pct": round(100.0 * e_both / b_both, 1) if both and b_both > 0 else None,
        "roe_n": len(both),
        "_pe_null_reason": ("aggregate TTM earnings ≤ 0" if pe_s and e <= 0 else
                            "no company with four quarters ending Q2/2026 or later" if not pe_s else None),
    }


def valuation(stocks_path, raw_dir, out_path, dry_run):
    """Fill IND_ICB.industries[].pe/pb/roe_pct (+ pe_*/pb_*/roe_n keys), IND_ICB.market.pe/pb and basis text.
    Caps = the stocks file's mcap (same snapshot as the cards' mcap_bn_vnd). Nothing else in the file changes."""
    stocks = json.load(open(stocks_path, encoding="utf-8"))
    data = json.load(open(out_path, encoding="utf-8"))
    ind = data["IND_ICB"]
    mcap = {r["sym"]: float(r.get("mcap") or 0) for r in stocks}
    board = {r["sym"]: {"HSX": "HOSE", "HOSE": "HOSE", "HNX": "HNX"}.get(r.get("board"), "UPCoM") for r in stocks}
    by_icb = collections.defaultdict(list)
    for r in stocks:
        by_icb[r["icb"]].append(r["sym"])
    ttm, book = {}, {}
    for r in stocks:
        p = company_panel(raw_dir, r["sym"])
        if p is None:
            continue
        t, b = company_ttm_latest(p), company_book_latest(p)
        if t:
            ttm[r["sym"]] = t
        if b:
            book[r["sym"]] = b
    # consistency with the cards
    for card in ind["industries"]:
        tot = sum(mcap[s] for s in by_icb.get(card["icb"], []))
        if abs(tot - card["mcap_bn_vnd"]) > 1:
            sys.exit(f"cap mismatch for {card['icb']}: stocks {tot:.0f} vs card {card['mcap_bn_vnd']}")
    res = {}
    card_keys = ("pe", "pe_n", "pe_coverage_pct", "pe_loss_makers_n", "pb", "pb_n", "pb_coverage_pct", "roe_pct")
    for card in ind["industries"]:
        a = valuation_agg(by_icb.get(card["icb"], []), mcap, ttm, book)
        res[card["icb"]] = a
        for k in card_keys:
            card[k] = a[k]
        card["pe_null_reason"] = a["_pe_null_reason"]
        if card["pe_null_reason"] is None:
            del card["pe_null_reason"]
    m = valuation_agg(list(mcap), mcap, ttm, book)
    res["market"] = m
    mk = ind["market"]
    for k in card_keys:
        mk[k] = m[k]
    for ex, blk in mk.get("by_exchange", {}).items():
        a = valuation_agg([s for s in mcap if board[s] == ex], mcap, ttm, book)
        res[ex] = a
        for k in ("pe", "pe_coverage_pct", "pb", "pb_coverage_pct"):
            blk[k] = a[k]
    as_of = ind.get("as_of") or "2026-10-30"
    f = ind["basis"]["fields"]
    f.pop("pe, pb, div_yield_pct, roe_pct", None)
    f["pe"] = (
        "≈ derived, aggregate (cap-weighted harmonic) P/E = Σ market cap ÷ Σ trailing-four-quarter profit after tax "
        "attributable to parent shareholders. Caps = mcap_bn_vnd (close × listed shares, " + as_of + "). Earnings = "
        "each company's own latest four consecutive reported quarters ending Q3/2026 or Q2/2026 (older → excluded), "
        "Vietcap IQ quarterly statements (isa22; PAT isa20 where isa22 is missing), consolidated where the company "
        "has subsidiaries. Loss-makers are INCLUDED in the denominator (pe_loss_makers_n); pe is null when the "
        "aggregate earnings are ≤ 0 (pe_null_reason). pe_n = companies included; pe_coverage_pct = their share of "
        "the industry cap. Not the exchange's or a broker's index P/E (those use free-float/index weights and their "
        "own EPS) — see caveats.")
    f["pb"] = (
        "≈ derived, aggregate P/B = Σ market cap ÷ Σ parent shareholders' equity (bsa78 less minority interests "
        "where included) at each company's latest reported quarter-end (Q3/2026 or Q2/2026; older → excluded); "
        "companies with equity ≤ 0 excluded. pb_n / pb_coverage_pct as for pe. Banks: book = parent equity.")
    f["roe_pct"] = (
        "≈ derived: Σ TTM parent profit ÷ Σ latest parent equity over companies in both the pe and pb samples "
        "(period-end equity, not average; ends may differ by one quarter between companies). Not a reported ROE.")
    f["div_yield_pct"] = "null: no sourced per-company dividend series in this build (not estimated)."
    f["pe_* / pb_* keys"] = (
        "pe_n, pe_coverage_pct, pe_loss_makers_n, pb_n, pb_coverage_pct: sample size and share of industry cap. "
        "market.by_exchange.{HOSE,HNX,UPCoM}.pe/pb: same method by exchange (all listed stocks, not an index).")
    f["valuation caveats"] = (
        "(1) Double counting: a listed parent consolidating a listed subsidiary (VIC–VHM/VRE/VPL, MWG–DMX, GVR–PHR/"
        "DPR, GAS–PGD …) puts the subsidiary's profit and book in both companies; because cap is counted twice too, "
        "the ratios are affected roughly proportionally rather than biased one way, but the subsidiary's weight is "
        "overstated. (2) TTM timing: ~92% of market cap uses Q4/2025–Q3/2026; the rest Q3/2025–Q2/2026. (3) One-off "
        "gains in the window (e.g. large divestment or revaluation gains) lower P/E; quarterly reports are "
        "unaudited and may be revised. (4) Caps use listed shares; ex-rights shares not yet listed (e.g. TCB, ex-date "
        "2026-10-29) understate cap. (5) Full listed universe incl. UPCoM, total (not free-float) cap — differs from "
        "VN-Index P/E. Descriptive only, not investment advice.")
    if dry_run:
        print(json.dumps(res, ensure_ascii=False, indent=1))
        return res
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False, indent=1)
        fh.write("\n")
    print("wrote", out_path, "pe/pb/roe_pct on", len(ind["industries"]), "cards + market")
    return res


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
    args = ap.parse_args()
    if args.cmd == "pull":
        pull(args.stocks, args.raw, args.threads, tuple(args.sections.split(",")),
             args.only.split(",") if args.only else None)
    elif args.cmd == "valuation":
        valuation(args.stocks, args.raw, args.out, args.dry_run)
    else:
        build(args.stocks, args.raw, args.out, args.ttm_min_cov, args.dry_run, args.cross_checks)


if __name__ == "__main__":
    main()
