#!/usr/bin/env python3
"""
Build data/banks_vnstock.json — every listed commercial bank (HOSE / HNX / UPCoM) from vnstock_data.

Run (sponsor venv, never with public-PyPI vnstock):
    VNSTOCK_TELEMETRY=off /tmp/vnenv/bin/python tools/build/banks_vnstock.py [--refresh] [--banks VCB,ACB]
                                                                           [--no-finance] [--out PATH]

* Key: read only by the library itself (env VNSTOCK_API_KEY or ~/.vnstock/api_key.json); never printed here.
* Universe: Listing(source='VCI').symbols_by_exchange(), ICB code 8300 (Banks), type STOCK, not DELISTED.
* Market: Trading(source='VCI').price_board → close, trading date, listed shares, trading status.
* Fundamentals: Finance(source, symbol, period) balance_sheet / income_statement / ratio, annual 2019–2025 and the
  last 8 quarters up to Q2-2026; sources tried in order VCI → MAS → KBS → MBK, each pre-flighted (a source whose
  host is unreachable is skipped and recorded in _meta.coverage.sources). Values are only ever copied from the
  responses — null when a field/period is not returned, never estimated.
* Full balance sheet: every period entry also carries `bs: {item_key: bn VND}` — the source's balance-sheet rows
  mapped onto the TT49 bank template (BS_TEMPLATE; ordered list in _meta.bs_items). Rows matching no template
  item are listed per bank in fetch.unmapped. Served as 8-quarter mini-chart series by GET /api/banks/bs_history.
* Every call: retries (5 tries, 2–20 s backoff); responses cached under /tmp/vnstock_banks_cache (24 h; --refresh
  ignores the cache). Prints a coverage table at the end.
"""
from __future__ import annotations

import argparse
import json
import os
import pickle
import re
import sys
import time
import unicodedata
from datetime import datetime, timezone
from pathlib import Path

os.environ["VNSTOCK_TELEMETRY"] = "off"
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
import vnstock_layer as V  # noqa: E402

CACHE = Path("/tmp/vnstock_banks_cache")
CACHE_TTL = 24 * 3600
OUT = ROOT / "data" / "banks_vnstock.json"
RETRY = {"tries": 5, "base_wait": 2.0, "max_wait": 20.0}
YEARS = [str(y) for y in range(2019, 2026)]
QUARTERS = ["2024-Q3", "2024-Q4", "2025-Q1", "2025-Q2", "2025-Q3", "2025-Q4", "2026-Q1", "2026-Q2"]
SOURCES = ["VCI", "MAS", "KBS", "MBK"]
REPORTS = ["balance_sheet", "income_statement", "ratio"]
STATE_OWNED = {"VCB", "BID", "CTG"}            # Agribank (the 4th SOCB) is not listed
EXCHANGE_NAMES = {"HSX": "HOSE", "HOSE": "HOSE", "HNX": "HNX", "UPCOM": "UPCoM"}

# target field → (report, [regex on the normalised id/name, in priority order], exclusion regex)
MONEY = {"total_assets", "customer_loans", "customer_deposits", "equity", "net_interest_income",
         "total_operating_income", "operating_expenses", "pre_tax_profit", "net_profit"}
FIELDS: dict[str, tuple[str, list[str], str | None]] = {
    "total_assets": ("balance_sheet", [r"^(tong_cong_tai_san|tong_tai_san|total_assets?)$", r"tong_cong_tai_san",
                                       r"total_assets?$"], r"binh_quan|average"),
    "customer_loans": ("balance_sheet", [r"^(cho_vay_khach_hang|loans?_(to|and_advances_to)_customers?)$",
                                         r"^cho_vay_khach_hang", r"loans?_(to|and_advances_to)_customers?"],
                       r"du_phong|rong|provision|net"),
    "customer_deposits": ("balance_sheet", [r"^(tien_gui_cua_khach_hang|tien_gui_khach_hang|deposits?_from_customers?|"
                                            r"customer_deposits?)$", r"tien_gui_(cua_)?khach_hang",
                                            r"deposits?_(from|of)_customers?"], None),
    "equity": ("balance_sheet", [r"^(von_chu_so_huu|tong_von_chu_so_huu|owners?_equity|total_equity|"
                                 r"shareholders?_equity)$", r"von_chu_so_huu$", r"(owners?|shareholders?)_equity$"],
               r"no_phai_tra|liabilit|loi_ich|minority"),
    "net_interest_income": ("income_statement", [r"^(thu_nhap_lai_thuan|net_interest_income)$", r"thu_nhap_lai_thuan",
                                                 r"net_interest_income"], None),
    "total_operating_income": ("income_statement", [r"^(tong_thu_nhap_hoat_dong|total_operating_income)$",
                                                    r"tong_thu_nhap_hoat_dong", r"total_operating_income"], None),
    "operating_expenses": ("income_statement", [r"^(chi_phi_hoat_dong|tong_chi_phi_hoat_dong|operating_expenses?)$"],
                           None),
    "pre_tax_profit": ("income_statement", [r"^(tong_loi_nhuan_truoc_thue|loi_nhuan_truoc_thue|profit_before_tax|"
                                            r"pre_tax_profit)$", r"loi_nhuan_truoc_thue",
                                            r"(profit|income)_before_(income_)?tax"], None),
    "net_profit": ("income_statement", [r"^(loi_nhuan_sau_thue|net_profit|profit_after_tax)$", r"loi_nhuan_sau_thue",
                                        r"(net_profit|profit_after_tax)"], r"thieu_so|minority|non_controlling"),
    "roe": ("ratio", [r"^(roe|roae)(_pct|_percent)?$", r"(^|_)(roe|roae)(_|$)",
                      r"loi_nhuan_tren_von_chu_so_huu"], None),
    "roa": ("ratio", [r"^(roa|roaa)(_pct|_percent)?$", r"(^|_)(roa|roaa)(_|$)", r"loi_nhuan_tren_tong_tai_san"], None),
    "npl_ratio": ("ratio", [r"^(npl|npl_ratio|ty_le_no_xau)$", r"ty_le_no_xau|no_xau_tren_tong_du_no",
                            r"non_performing_loans?(_ratio)?$|bad_debt_ratio"], r"bao_phu|coverage"),
    "car": ("ratio", [r"^(car|capital_adequacy_ratio)$", r"he_so_an_toan_von|ty_le_an_toan_von|capital_adequacy"],
            None),
    "nim": ("ratio", [r"^nim$", r"bien_lai_thuan|net_interest_margin"], None),
    "cir": ("ratio", [r"^cir$", r"cost_to_income|chi_phi_hoat_dong_tren_tong_thu_nhap"], None),
    "ldr": ("ratio", [r"^ldr$", r"loans?_to_deposits?|cho_vay_tren_tien_gui"], None),
    "llr_coverage": ("ratio", [r"^(llr|llcr|npl_coverage)$", r"ty_le_bao_no_xau|bao_phu_no_xau|loan_loss_(reserve|coverage)"],
                     None),
}
RATIOS = {k for k, v in FIELDS.items() if v[0] == "ratio"}

# ─── full balance sheet (TT49 bank template) ───────────────────────
# (key, side, vi, en, parent, total, [regex on the normalised id/name, priority order], exclusion regex)
# side: which half of the statement the row must sit in ('asset' rows come before the total-assets row,
# 'liability'/'equity' rows after it); rows are classified only when the side is compatible or unknown.
_TCTD = r"(cac_)?(tctd|to_chuc_tin_dung|ctd)_khac"
_DERIV = r"^(cac_)?cong_cu_tai_chinh_phai_sinh"
BS_TEMPLATE: list[tuple] = [
    ("cash", "asset", "Tiền mặt, vàng bạc, đá quý", "Cash, gold and gemstones", None, False,
     [r"^tien_mat(_|$)", r"^cash(_and_(precious_metals|gold|gemstones|cash_equivalents).*)?$", r"^cash_"], r"tuong_duong"),
    ("sbv_deposits", "asset", "Tiền gửi tại NHNN", "Balances with the SBV", None, False,
     [r"^tien_gui_tai_(ngan_hang_nha_nuoc|nhnn)", r"^(balances?|deposits?)_(with|at)_(the_)?(sbv|state_bank)"], None),
    ("interbank_assets", "asset", "Tiền gửi và cho vay các TCTD khác", "Placements with and loans to other credit institutions",
     None, False,
     [rf"^tien_gui_va_cho_vay_{_TCTD}",
      r"^(placements?|deposits?)_(with|at)_(and_loans?_to_)?other_(credit_institutions?|banks?)",
      r"^(due_from|loans?_to)_other_(credit_institutions?|banks?)"], r"du_phong|provision"),
    ("trading_securities", "asset", "Chứng khoán kinh doanh", "Trading securities", None, False,
     [r"^chung_khoan_kinh_doanh(_rong)?$", r"^trading_securities(_net)?$"], r"du_phong|provision"),
    ("derivatives_asset", "asset", "Công cụ tài chính phái sinh và tài sản tài chính khác",
     "Derivatives and other financial assets", None, False,
     [_DERIV + r"_va_(cac_)?tai_san", r"^derivatives?.*assets?$", _DERIV + r"$", r"^derivatives?(_financial_instruments?)?$"],
     r"no_tai_chinh|liabilit"),
    ("customer_loans_net", "asset", "Cho vay khách hàng (thuần)", "Loans to customers, net", None, False,
     [r"^cho_vay_va_cho_thue_tai_chinh_(cho_)?khach_hang", r"^cho_vay_khach_hang_(rong|thuan)$",
      r"^(net_loans?_(to|and_advances_to)_customers?|loans?_(to|and_advances_to)_customers?_net)$"], r"du_phong|provision"),
    # unqualified "Cho vay khách hàng" / "Loans (and advances) to customers" = gross (as FIELDS.customer_loans), except
    # when the same label appears twice in a period (TT49 section + sub-line): then larger = gross, smaller = net
    ("customer_loans_gross", "asset", "Cho vay khách hàng (gộp)", "Loans to customers, gross", "customer_loans_net", False,
     [r"^cho_vay_khach_hang(_gop)?$", r"^(gross_)?loans?_(to|and_advances_to)_customers?(_gross)?$"],
     r"du_phong|provision"),
    ("loan_loss_provisions", "asset", "Dự phòng rủi ro cho vay khách hàng", "Provision for loans to customers",
     "customer_loans_net", False,
     [r"^du_phong_rui_ro_cho_vay(_va_cho_thue_tai_chinh)?_(cho_)?khach_hang", r"^du_phong_rui_ro_cho_vay$",
      r"provisions?_(for|against)_.*loans?(_and_advances)?_to_customers?|^(less_)?(provision_for_)?loan_loss_(provisions?|reserves?)$|^less_provision.*loans?"],
     None),
    ("investment_securities", "asset", "Chứng khoán đầu tư", "Investment securities", None, False,
     [r"^chung_khoan_dau_tu(_rong)?$", r"^investment_securities(_net)?$"], r"du_phong|provision"),
    ("long_term_investments", "asset", "Góp vốn, đầu tư dài hạn", "Long-term investments", None, False,
     [r"^gop_von_(va_)?dau_tu_dai_han", r"^dau_tu_dai_han$",
      r"^(long_term_investments?|investments?_in_other_entities.*|capital_contributions?.*long_term_investments?)$"],
     r"du_phong|provision|khac$|^other"),
    ("fixed_assets", "asset", "Tài sản cố định", "Fixed assets", None, False,
     [r"^tai_san_co_dinh(_rong)?$", r"^fixed_assets?(_net)?$"], None),
    ("investment_property", "asset", "Bất động sản đầu tư", "Investment property", None, False,
     [r"^bat_dong_san_dau_tu$", r"^investment_propert(y|ies)$"], None),
    ("other_assets", "asset", "Tài sản Có khác", "Other assets", None, False,
     [r"^(tai_san_co_khac|cac_tai_san_co_khac|tai_san_khac)$", r"^other_assets$"], None),
    ("total_assets", "asset", "Tổng tài sản", "Total assets", None, True,
     [r"^(tong_cong_tai_san|tong_tai_san(_co)?|total_assets?)$", r"^tong_cong_tai_san", r"^total_assets?$"],
     r"binh_quan|average"),
    ("gov_sbv_borrowings", "liability", "Các khoản nợ Chính phủ và NHNN", "Due to the Government and the SBV", None, False,
     [r"^(cac_khoan_)?no_chinh_phu_va_(ngan_hang_nha_nuoc|nhnn)", r"^(cac_khoan_)?no_(chinh_phu|nhnn|ngan_hang_nha_nuoc)",
      r"^(due_to|amounts?_due_to|borrowings?_from)_(the_)?(gov|government|sbv|state_bank)"], None),
    ("interbank_liabilities", "liability", "Tiền gửi và vay các TCTD khác", "Deposits and borrowings from other credit institutions",
     None, False,
     [rf"^tien_gui_va_vay_{_TCTD}", rf"^(tien_gui|vay)_(cua_)?{_TCTD}$",
      r"^(deposits?_and_borrowings?|deposits?_and_loans?|deposits?|borrowings?)_from_other_(credit_institutions?|banks?)",
      r"^due_to_other_(credit_institutions?|banks?)"], None),
    ("customer_deposits", "liability", "Tiền gửi của khách hàng", "Deposits from customers", None, False,
     [r"^(tien_gui_cua_khach_hang|tien_gui_khach_hang|deposits?_from_customers?|customer_deposits?)$",
      r"^tien_gui_(cua_)?khach_hang", r"^deposits?_(from|of|by)_customers?"], None),
    ("derivatives_liability", "liability", "Công cụ tài chính phái sinh và nợ tài chính khác",
     "Derivatives and other financial liabilities", None, False,
     [_DERIV + r"_va_(cac_)?(khoan_)?no_tai_chinh", r"^derivatives?.*liabilit", _DERIV + r"$",
      r"^derivatives?(_financial_instruments?)?$"], r"tai_san|assets?$"),
    ("trust_funds", "liability", "Vốn tài trợ, ủy thác đầu tư", "Grants, trusted funds and borrowings at risk", None, False,
     [r"^von_tai_tro", r"^von_uy_thac", r"(funds?_received_from|trust(ed)?_funds?|entrusted_funds?|grants?_and_(trust|entrust))"],
     None),
    ("valuable_papers", "liability", "Phát hành giấy tờ có giá", "Valuable papers issued", None, False,
     [r"^phat_hanh_giay_to_co_gia", r"giay_to_co_gia",
      r"valuable_papers?|debt_securities_issued|(certificates?_of_deposits?|bonds?)(_and_.*)?_issued|^issued_"], None),
    ("other_liabilities", "liability", "Các khoản nợ khác", "Other liabilities", None, False,
     [r"^(cac_khoan_no_khac|no_khac|cac_khoan_no_phai_tra_khac)$", r"^other_liabilities$"], None),
    ("total_liabilities", "liability", "Tổng nợ phải trả", "Total liabilities", None, True,
     [r"^(tong_no_phai_tra|tong_cong_no_phai_tra|no_phai_tra|total_liabilities)$"], r"von|equity"),
    ("charter_capital", "equity", "Vốn điều lệ", "Charter capital", None, False,
     [r"^von_dieu_le$", r"^(charter_capital|paid_in_capital|share_capital|registered_capital)$", r"^von_dieu_le"], None),
    ("share_premium", "equity", "Thặng dư vốn cổ phần", "Share premium", None, False,
     [r"^thang_du_von_co_phan$", r"^share_premium$", r"thang_du_von|share_premium"], None),
    ("reserves", "equity", "Quỹ của TCTD", "Reserves", None, False,
     [r"^quy_cua_(to_chuc_tin_dung|tctd|ngan_hang)$", r"^(cac_)?quy_(cua|du_tru|dau_tu)",
      r"^(reserves?|reserve_funds?|funds?_of_(the_)?(credit_institutions?|bank))$"], None),
    ("fx_revaluation", "equity", "Chênh lệch tỷ giá hối đoái", "Foreign exchange differences", None, False,
     [r"^chenh_lech_ty_gia", r"^(foreign_(currency|exchange)|fx)_(translation_)?(differences?|revaluation|reserves?)"], None),
    ("retained_earnings", "equity", "Lợi nhuận chưa phân phối", "Retained earnings", None, False,
     [r"^loi_nhuan_chua_phan_phoi", r"^(retained_(earnings|profits?)|undistributed_(earnings|profits?)|accumulated_(profits?|losses))"],
     None),
    ("minority_interest", "equity", "Lợi ích của cổ đông thiểu số", "Minority interest", None, False,
     [r"^loi_ich_(cua_)?(co_dong_)?(thieu_so|khong_kiem_soat)", r"^(minority_interests?|non_controlling_interests?)$",
      r"minority_interest|non_controlling_interest"], None),
    ("equity_total", "equity", "Vốn chủ sở hữu", "Total equity", None, True,
     [r"^(von_chu_so_huu|tong_von_chu_so_huu|tong_cong_von_chu_so_huu)$",
      r"^(total_)?(owners?|owner_s|shareholders?|shareholder_s|stockholders?)_equity$|^total_equity$"],
     r"no_phai_tra|liabilit"),
]
BS_KEYS = [t[0] for t in BS_TEMPLATE]
BS_ITEMS = [{"key": k, "side": s, "vi": vi, "en": en, "parent": p, "total": tot}
            for k, s, vi, en, p, tot, _, _ in BS_TEMPLATE]
_BS = {t[0]: t for t in BS_TEMPLATE}
# classification order: totals and specific labels first so broad patterns cannot steal their rows
_FIRST = ["total_assets", "total_liabilities", "equity_total", "loan_loss_provisions", "derivatives_asset",
          "derivatives_liability", "interbank_assets", "interbank_liabilities"]
BS_MATCH_ORDER = _FIRST + [k for k in BS_KEYS if k not in set(_FIRST) | {"customer_loans_net", "customer_loans_gross"}]


def norm(s) -> str:
    s = unicodedata.normalize("NFKD", str(s or "")).replace("đ", "d").replace("Đ", "D")
    s = "".join(c for c in s if not unicodedata.combining(c)).lower()
    s = re.sub(r"^([ivxlcdm]+|[0-9]+(\.[0-9]+)*|[a-z])[\.\)]\s+", "", s)       # "I. ", "1.2. ", "a) "
    return re.sub(r"[^a-z0-9]+", "_", s).strip("_")


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


# ─── cache ─────────────────────────────────────────────────────────
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


# ─── universe ──────────────────────────────────────────────────────
def universe(refresh: bool) -> tuple[list[dict], list[dict], list[dict]]:
    df = cached("listing_symbols_by_exchange", lambda: V.symbols_by_exchange(**RETRY), refresh)
    if df is None or not len(df):
        sys.exit("Listing(source='VCI').symbols_by_exchange() returned nothing — cannot build the bank universe")
    df = df[df["type"].astype(str) == "STOCK"]
    is_bank_name = df["organ_name"].astype(str).str.contains("Ngân hàng", case=False, na=False)
    banks, excluded, delisted = [], [], []
    for _, r in df[(df["icb_code2"].astype(str) == "8300") | is_bank_name].iterrows():
        rec = {"ticker": r["symbol"], "name": r.get("organ_short_name") or r["symbol"], "organ_name": r["organ_name"],
               "exchange": EXCHANGE_NAMES.get(str(r["exchange"]).upper(), str(r["exchange"])),
               "icb_code2": r.get("icb_code2")}
        commercial = str(r["organ_name"]).startswith("Ngân hàng Thương mại")
        if str(r["exchange"]).upper() == "DELISTED":
            (delisted if commercial else excluded).append({**rec, "reason": "exchange=DELISTED in VCI listing"})
        elif str(r.get("icb_code2")) == "8300" and commercial:
            banks.append(rec)
        else:
            excluded.append({**rec, "reason": "not a commercial bank (name/ICB)"})
    return sorted(banks, key=lambda b: b["ticker"]), excluded, delisted


# ─── fundamentals ──────────────────────────────────────────────────
def period_key(p, kind: str) -> str | None:
    m = re.match(r"^\s*(\d{4})(?:\s*[-/ ]?\s*Q?\s*([1-4]))?\s*$", str(p))
    if not m:
        m2 = re.match(r"^\s*Q([1-4])\s*[-/ ]\s*(\d{4})\s*$", str(p))
        if not m2:
            return None
        y, q = m2.group(2), m2.group(1)
    else:
        y, q = m.group(1), m.group(2)
    if kind == "year":
        return y if not q else None
    return f"{y}-Q{q}" if q else None


def to_long(df):
    """Accept vnstock long format (period, id, name, …, value) or a wide frame (items × periods)."""
    import pandas as pd
    if df is None or not len(df):
        return []
    if "value" in df.columns and "period" in df.columns:
        keys = [c for c in ("id", "name", "item") if c in df.columns]
        return [(r["period"], [r[k] for k in keys], r["value"], r.get("unit")) for _, r in df.iterrows()]
    keys = [c for c in ("id", "name", "item") if c in df.columns]
    if keys:
        long = df.melt(id_vars=keys, var_name="period", value_name="value")
        return [(r["period"], [r[k] for k in keys], r["value"], None) for _, r in long.iterrows()]
    if isinstance(df.index, pd.Index):      # periods as rows, items as columns
        out = []
        pcol = next((c for c in ("period", "report_period") if c in df.columns), None)
        ycol = next((c for c in ("yearReport", "year_report", "year", "nam") if c in df.columns), None)
        qcol = next((c for c in ("lengthReport", "length_report", "quarter", "quy") if c in df.columns), None)
        skip = {pcol, ycol, qcol, "ticker", "symbol", "organ_code", "CP", "Meta"}

        def per_of(r, idx):
            if pcol:
                return r[pcol]
            if ycol:                          # VCI style: yearReport + lengthReport (1–4 = quarter, 5 = year)
                q = _num(r[qcol]) if qcol else None
                y = int(_num(r[ycol]) or 0)
                return f"{y}-Q{int(q)}" if q in (1, 2, 3, 4) else str(y)
            return idx
        for idx, r in df.iterrows():
            for c in df.columns:
                if c not in skip:
                    out.append((per_of(r, idx), [c], r[c], None))
        return out
    return []


def extract(rows, report: str, kind: str):
    """→ ({period: {field: value}}, {field: matched raw label})"""
    wanted = {f: v for f, v in FIELDS.items() if v[0] == report}
    by_period: dict[str, dict] = {}
    labels: dict[str, str] = {}
    for f, (_, pats, excl) in wanted.items():
        for pat in pats:                      # lower-priority patterns only fill periods still missing
            for per, names, val, unit in rows:
                pk = period_key(per, kind)
                if pk is None or by_period.get(pk, {}).get(f) is not None:
                    continue
                for nm in names:
                    n = norm(nm)
                    if n and re.search(pat, n) and not (excl and re.search(excl, n)):
                        try:
                            x = float(val)
                        except (TypeError, ValueError):
                            break
                        if x != x:
                            break
                        by_period.setdefault(pk, {})[f] = x
                        labels.setdefault(f, str(nm))
                        break
    return by_period, labels


def _num(val) -> float | None:
    try:
        x = float(val)
    except (TypeError, ValueError):
        return None
    return None if x != x else x


def _match(item: str, names: list[str]) -> str | None:
    """The raw label of the first name matching any of the item's patterns (None when none does)."""
    _, _, _, _, _, _, pats, excl = _BS[item]
    for pat in pats:
        for nm in names:
            n = norm(nm)
            if n and re.search(pat, n) and not (excl and re.search(excl, n)):
                return str(nm)
    return None


def extract_bs(rows, kind: str):
    """Full balance sheet → ({period: {item_key: value}}, {item_key: matched raw label}, [unmapped labels]).

    Every source row is assigned to at most one template item per period. Rows before the total-assets row are
    the asset side, rows after it liabilities/equity (unknown when a period has no total-assets row). Lower-priority
    patterns only fill items still missing in that period. Customer loans: when the gross and net lines carry the
    same label (TT49 section "Cho vay khách hàng" and its sub-line "Cho vay khách hàng"), the larger value is gross
    and the smaller net (net = gross − provisions)."""
    per_rows: dict[str, list] = {}
    for per, names, val, _unit in rows:
        pk = period_key(per, kind)
        if pk is None:
            continue
        lst = per_rows.setdefault(pk, [])
        lst.append({"i": len(lst), "names": [x for x in names if x is not None and str(x).strip()], "v": _num(val)})
    by_period: dict[str, dict] = {}
    labels: dict[str, str] = {}
    claimed_any: set[str] = set()
    seen: dict[str, bool] = {}        # first raw label → has a numeric value somewhere
    for pk, lst in per_rows.items():
        out: dict[str, float] = {}
        ta = next((r["i"] for r in lst if _match("total_assets", r["names"])), None)
        for r in lst:
            r["side"] = None if ta is None else ("asset" if r["i"] <= ta else "liab_eq")
            r["claimed"] = False
            if r["names"]:
                lab = str(r["names"][-1])
                seen[lab] = seen.get(lab, False) or r["v"] is not None

        def ok_side(item, r):
            s = _BS[item][1]
            return r["side"] is None or (r["side"] == "asset") == (s == "asset")

        def claim(item):
            pats, excl = _BS[item][6], _BS[item][7]
            for pat in pats:
                for r in lst:
                    if r["claimed"] or r["v"] is None or not ok_side(item, r):
                        continue
                    for nm in r["names"]:
                        n = norm(nm)
                        if n and re.search(pat, n) and not (excl and re.search(excl, n)):
                            out[item] = r["v"]
                            r["claimed"] = True
                            labels.setdefault(item, str(nm))
                            return

        for item in BS_MATCH_ORDER:
            claim(item)
            if item == "loan_loss_provisions":       # loans right after provisions (before broad asset patterns)
                _claim_loans(lst, out, labels, ok_side)
        for r in lst:
            if r["claimed"] and r["names"]:
                claimed_any.add(str(r["names"][-1]))
        if out:
            by_period[pk] = out
    unmapped = [lab for lab, has_val in seen.items() if has_val and lab not in claimed_any]
    return by_period, labels, unmapped


def _claim_loans(lst, out, labels, ok_side) -> None:
    """Customer loans: explicit net labels → net; unqualified labels → gross, except one label repeated with
    different values in a period (section + sub-line) → larger gross, smaller net."""
    def cand(r, item):
        pats, ex = _BS[item][6], _BS[item][7]
        for nm in r["names"]:
            n = norm(nm)
            if n and any(re.search(p, n) for p in pats) and not re.search(ex, n):
                return n, str(nm)
        return None
    free = [r for r in lst if not r["claimed"] and r["v"] is not None and ok_side("customer_loans_net", r)]
    nets = [(r, *c) for r in free for c in [cand(r, "customer_loans_net")] if c]
    gros = [(r, *c) for r in free for c in [cand(r, "customer_loans_gross")] if c and not cand(r, "customer_loans_net")]

    def take(item, c):
        out[item] = c[0]["v"]
        c[0]["claimed"] = True
        labels.setdefault(item, c[2])
    if nets:
        take("customer_loans_net", nets[0])
    if not gros:
        return
    same = [c for c in gros if c[1] == gros[0][1]]
    if not nets and len(same) >= 2 and len({c[0]["v"] for c in same}) >= 2:
        take("customer_loans_gross", max(same, key=lambda c: c[0]["v"]))
        take("customer_loans_net", min(same, key=lambda c: c[0]["v"]))
    else:
        take("customer_loans_gross", gros[0])


def money_divisor(periods: dict) -> float:
    """vnstock returns VND for VCI statements; detect from total assets (a bank has > 1,000 bn VND)."""
    ta = [p.get("total_assets") for p in periods.values() if p.get("total_assets")]
    if not ta:
        vals = [abs(v) for p in periods.values() for k, v in p.items() if k in MONEY and v]
        big = max(vals) if vals else 0
    else:
        big = max(ta)
    return 1e9 if big > 1e10 else (1e3 if big > 1e7 else 1.0)


def fundamentals(sym: str, sources: list[str], refresh: bool):
    out = {"year": {}, "quarter": {}}
    meta = {"field_map": {}, "bs_map": {}, "sources_used": {}, "notes": [], "errors": {}, "unmapped": []}
    unmapped: set[str] = set()
    for kind in ("year", "quarter"):
        for report in REPORTS:
            for src in sources:
                df = cached(f"fin_{src}_{sym}_{kind}_{report}",
                            lambda: V.finance(sym, report, kind, src, **RETRY), refresh)
                if df is None or not len(df):
                    meta["errors"].setdefault(f"{kind}/{report}", []).append(f"{src}: empty")
                    continue
                rows = to_long(df)
                per, labels = extract(rows, report, kind)
                bs, bs_labels, bs_unmapped = extract_bs(rows, kind) if report == "balance_sheet" else ({}, {}, [])
                if not per and not bs:
                    meta["errors"].setdefault(f"{kind}/{report}", []).append(f"{src}: no mapped field")
                    continue
                if report == "ratio":
                    roes = [abs(p["roe"]) for p in per.values() if p.get("roe") is not None]
                    if roes and max(roes) <= 1.0:          # fractions → percent
                        per = {k: {f: v * 100 for f, v in p.items()} for k, p in per.items()}
                        meta["notes"].append(f"{src} {kind} ratio returned as fractions; ×100 to percent")
                    if kind == "quarter" and src == "VCI":
                        meta["notes"].append("VCI quarterly ratios are trailing-twelve-month (RATIO_TTM)")
                else:
                    d = money_divisor(per or bs)
                    if d != 1.0:
                        per = {k: {f: (v / d if f in MONEY else v) for f, v in p.items()} for k, p in per.items()}
                        bs = {k: {f: v / d for f, v in p.items()} for k, p in bs.items()}
                for pk, vals in bs.items():                # full balance sheet, bn VND
                    slot = out[kind].setdefault(pk, {}).setdefault("_bs", {})
                    for f, v in vals.items():
                        if slot.get(f) is None:
                            slot[f] = round(v, 1)
                    out[kind][pk].setdefault("_src", set()).add(src)
                meta["bs_map"].update({f"{src}:{f}": lab for f, lab in bs_labels.items()})
                unmapped.update(bs_unmapped)
                for pk, vals in per.items():
                    slot = out[kind].setdefault(pk, {})
                    for f, v in vals.items():
                        if slot.get(f) is None:
                            slot[f] = round(v, 4 if f in RATIOS else 1)
                    slot.setdefault("_src", set()).add(src)
                meta["field_map"].update({f"{src}:{report}:{f}": lab for f, lab in labels.items()})
                meta["sources_used"][f"{kind}/{report}"] = src
                break
    meta["unmapped"] = sorted(unmapped)
    return out, meta


def build_periods(raw: dict, keep: list[str], fetched_at: str, kind: str) -> dict:
    res = {}
    for pk in keep:
        p = raw.get(pk)
        if not p or not (any(v is not None for f, v in p.items() if f not in ("_src", "_bs")) or p.get("_bs")):
            continue
        bs = p.get("_bs") or {}
        res[pk] = {**{f: p.get(f) for f in FIELDS}, "bs": {k: bs[k] for k in BS_KEYS if bs.get(k) is not None},
                   "period": pk, "period_type": kind,
                   "source": "vnstock_data " + "/".join(sorted(p.get("_src", []))),
                   "basis": None, "basis_note": "vnstock does not label consolidated vs parent-only",
                   "fetched_at": fetched_at, "units": "bn VND; ratios in %"}
    return res


# ─── market ────────────────────────────────────────────────────────
def market(tickers: list[str], refresh: bool) -> dict[str, dict]:
    pb = cached("price_board_banks_" + "_".join(tickers), lambda: V.price_board(tickers, **RETRY), refresh) or {}
    out = {}
    for t in tickers:
        b = pb.get(t) or {}
        close = b.get("match_price") or None
        date, src = b.get("trading_date"), "vnstock_data Trading(VCI).price_board"
        if not close:                                     # board reset / no trade today → last daily bar
            h = cached(f"quote_{t}", lambda: V.history(t, 10, **RETRY), refresh)
            if h and h["c"]:
                close, date, src = h["c"][-1], h["t"][-1], "vnstock_data Quote(VCI).history"
        ls = b.get("listed_share")
        out[t] = {"close": float(close) if close else None, "date": date,
                  "listed_shares": ls, "market_cap_bn": round(close * ls / 1e9, 1) if close and ls else None,
                  "charter_capital_bn_derived": round(ls * 10_000 / 1e9, 1) if ls else None,
                  "trading_status": b.get("trading_status"), "is_delisted": b.get("is_delisted"),
                  "exchange_board": b.get("exchange"), "source": src if close or ls else None,
                  "note": "market_cap_bn = close × listed shares; charter capital derived as listed shares × 10,000 VND par"}
    return out


# ─── main ──────────────────────────────────────────────────────────
def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--refresh", action="store_true", help="ignore the /tmp cache")
    ap.add_argument("--banks", help="comma list (default: whole universe)")
    ap.add_argument("--no-finance", action="store_true")
    ap.add_argument("--out", default=str(OUT))
    a = ap.parse_args()

    st = V.status()
    if not st["active"]:
        sys.exit(f"vnstock layer unusable: {json.dumps({k: st[k] for k in ('importable', 'import_error', 'key_configured')})}")
    fetched_at = now_iso()
    banks, excluded, delisted = universe(a.refresh)
    if a.banks:
        want = {s.strip().upper() for s in a.banks.split(",")}
        banks = [b for b in banks if b["ticker"] in want]
    tickers = [b["ticker"] for b in banks]
    print(f"universe: {len(tickers)} banks — {', '.join(tickers)}", flush=True)

    src_status, usable = {}, []
    if not a.no_finance:
        for s in SOURCES:
            ok, why = V.host_reachable(V.FINANCE_HOSTS[s])
            src_status[s] = {"host": V.FINANCE_HOSTS[s], "reachable": ok, "detail": why}
            if ok:
                usable.append(s)
        print("finance sources:", {s: v["reachable"] for s, v in src_status.items()}, flush=True)

    mk = market(tickers, a.refresh)
    out_banks = []
    for b in banks:
        t = b["ticker"]
        fin, fmeta = fundamentals(t, usable, a.refresh) if usable else ({"year": {}, "quarter": {}},
                                                                        {"errors": {"all": ["no reachable finance source"]}})
        m = mk.get(t) or {}
        out_banks.append({
            "ticker": t, "name": b["name"], "organ_name": b["organ_name"], "exchange": b["exchange"],
            "state_owned": t in STATE_OWNED, "type": "SOCB" if t in STATE_OWNED else "JSCB",
            "listing_status": {"trading_status": m.get("trading_status"), "is_delisted": m.get("is_delisted")},
            "annual": build_periods(fin["year"], YEARS, fetched_at, "year"),
            "quarterly": build_periods(fin["quarter"], QUARTERS, fetched_at, "quarter"),
            "market": {k: v for k, v in m.items() if k not in ("trading_status", "is_delisted")},
            "fetch": fmeta,
        })
        print(f"  {t}: annual {len(out_banks[-1]['annual'])} · quarterly {len(out_banks[-1]['quarterly'])} · "
              f"bs items {len({k for p in out_banks[-1]['quarterly'].values() for k in p.get('bs', {})})} · "
              f"unmapped rows {len(fmeta.get('unmapped') or [])} · close {m.get('close')}", flush=True)

    cov = {b["ticker"]: {"annual": sorted(b["annual"]), "quarterly": sorted(b["quarterly"]),
                         "fields": sorted({f for p in list(b["annual"].values()) + list(b["quarterly"].values())
                                           for f in FIELDS if p.get(f) is not None}),
                         "bs_items": sorted({k for p in list(b["annual"].values()) + list(b["quarterly"].values())
                                             for k in (p.get("bs") or {})}),
                         "market": b["market"].get("close") is not None} for b in out_banks}
    n_fund = sum(1 for c in cov.values() if c["annual"] or c["quarterly"])
    doc = {
        "_meta": {
            "owner": "server (vnstock)", "as_of": fetched_at[:10], "fetched_at": fetched_at,
            "source": f"vnstock_data {st['version']} via VCI (Listing, Trading.price_board, Quote)"
                      + (f"; Finance via {'/'.join(usable)}" if usable else "; Finance: no source reachable"),
            "builder": "tools/build/banks_vnstock.py",
            "bs_items": BS_ITEMS,
            "bs_note": ("Each period's `bs` holds the full balance sheet mapped to the TT49 bank template (_meta.bs_items, "
                        "bn VND, as reported; provisions keep their sign). Source rows that match no template item are "
                        "listed per bank in fetch.unmapped."),
            "note": ("Universe = VCI listing, ICB 8300 (Banks), STOCK, not delisted. Fundamentals are copied from "
                     "vnstock Finance responses only (null when not returned, never estimated); units bn VND, ratios %. "
                     "Basis (consolidated/parent) is not labelled by vnstock. Agribank is unlisted. "
                     + ("" if n_fund else "No fundamentals in this build: every Finance host was unreachable from the "
                        "build machine — re-run where they are reachable (see coverage.sources).")),
            "coverage": {"banks": len(out_banks), "with_fundamentals": n_fund,
                         "with_market": sum(1 for c in cov.values() if c["market"]),
                         "annual_years": YEARS, "quarters": QUARTERS, "sources": src_status,
                         "excluded": excluded, "delisted_or_suspended": delisted + [
                             {"ticker": b["ticker"], "trading_status": b["listing_status"]["trading_status"]}
                             for b in out_banks if b["listing_status"]["is_delisted"]
                             or (b["listing_status"]["trading_status"] not in (None, "TRADING_ACTIVATED"))],
                         "per_bank": cov},
        },
        "banks": out_banks,
    }
    Path(a.out).write_text(json.dumps(doc, ensure_ascii=False, indent=1, default=lambda o: sorted(o) if isinstance(o, set) else str(o)) + "\n",
                           encoding="utf-8")
    print(f"\nwrote {a.out}\n")
    print(f"{'ticker':<7}{'exch':<7}{'annual':<16}{'quarterly':<20}{'fields':>7}{'bs':>5}  market")
    for t, c in cov.items():
        ex = next(b["exchange"] for b in out_banks if b["ticker"] == t)
        ann = f"{c['annual'][0]}–{c['annual'][-1]} ({len(c['annual'])})" if c["annual"] else "—"
        qtr = f"{c['quarterly'][0]}–{c['quarterly'][-1]} ({len(c['quarterly'])})" if c["quarterly"] else "—"
        print(f"{t:<7}{ex:<7}{ann:<16}{qtr:<20}{len(c['fields']):>7}{len(c['bs_items']):>5}  {'yes' if c['market'] else 'no'}")


if __name__ == "__main__":
    main()
