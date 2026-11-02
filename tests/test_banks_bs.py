"""Full balance-sheet mapping (tools/build/banks_vnstock.py) and GET /api/banks/bs_history (server.py). No network.

Run:  python3 -m unittest discover -s tests -v
The synthetic frames are shaped like vnstock_data Finance(...).balance_sheet() output: items as rows with periods
as columns (Vietnamese TT49 labels), periods as rows with items as columns (English VCI-style labels, yearReport /
lengthReport), and the long (period, id, name, value) layout.
"""
import json
import logging
import sys
import unittest
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tools" / "build"))
logging.disable(logging.CRITICAL)

import banks_vnstock as B  # noqa: E402
import server as S         # noqa: E402

# TT49 Vietnamese statement (bn VND) — note "Cho vay khách hàng" twice: section (net) and sub-line (gross)
VI_ROWS = [
    ("TÀI SẢN", None), ("I. Tiền mặt, vàng bạc, đá quý", 10), ("II. Tiền gửi tại NHNN", 20),
    ("III. Tiền gửi và cho vay các TCTD khác", 30), ("1. Tiền gửi tại các TCTD khác", 15), ("2. Cho vay các TCTD khác", 15),
    ("IV. Chứng khoán kinh doanh", 5), ("V. Các công cụ tài chính phái sinh và các tài sản tài chính khác", 1),
    ("VI. Cho vay khách hàng", 980), ("1. Cho vay khách hàng", 1000), ("2. Dự phòng rủi ro cho vay khách hàng", -20),
    ("VII. Chứng khoán đầu tư", 100), ("1. Chứng khoán đầu tư sẵn sàng để bán", 90),
    ("VIII. Góp vốn, đầu tư dài hạn", 3), ("IX. Tài sản cố định", 8), ("X. Bất động sản đầu tư", 0.5),
    ("XI. Tài sản Có khác", 40), ("TỔNG TÀI SẢN", 1197.5),
    ("I. Các khoản nợ Chính phủ và NHNN", 2), ("II. Tiền gửi và vay các TCTD khác", 50),
    ("III. Tiền gửi của khách hàng", 900),
    ("IV. Các công cụ tài chính phái sinh và các khoản nợ tài chính khác", 0.5),
    ("V. Vốn tài trợ, ủy thác đầu tư, cho vay TCTD chịu rủi ro", 1), ("VI. Phát hành giấy tờ có giá", 60),
    ("VII. Các khoản nợ khác", 20), ("TỔNG NỢ PHẢI TRẢ", 1033.5), ("VIII. Vốn chủ sở hữu", 150),
    ("1. Vốn của TCTD", 120), ("a. Vốn điều lệ", 100), ("c. Thặng dư vốn cổ phần", 20), ("2. Quỹ của TCTD", 10),
    ("3. Chênh lệch tỷ giá hối đoái", 0.1), ("5. Lợi nhuận chưa phân phối", 19.9),
    ("IX. Lợi ích của cổ đông thiểu số", 14), ("TỔNG NỢ PHẢI TRẢ VÀ VỐN CHỦ SỞ HỮU", 1197.5),
]
VI_EXPECT = {
    "cash": 10, "sbv_deposits": 20, "interbank_assets": 30, "trading_securities": 5, "derivatives_asset": 1,
    "customer_loans_net": 980, "customer_loans_gross": 1000, "loan_loss_provisions": -20, "investment_securities": 100,
    "long_term_investments": 3, "fixed_assets": 8, "investment_property": 0.5, "other_assets": 40,
    "total_assets": 1197.5, "gov_sbv_borrowings": 2, "interbank_liabilities": 50, "customer_deposits": 900,
    "derivatives_liability": 0.5, "trust_funds": 1, "valuable_papers": 60, "other_liabilities": 20,
    "total_liabilities": 1033.5, "charter_capital": 100, "share_premium": 20, "reserves": 10, "fx_revaluation": 0.1,
    "retained_earnings": 19.9, "minority_interest": 14, "equity_total": 150,
}
EN_COLS = [
    "Cash and precious metals", "Balances with the SBV", "Placements with and loans to other credit institutions",
    "Trading securities, net", "Derivatives and other financial assets", "Loans and advances to customers, net",
    "Loans and advances to customers", "Less: Provision for losses on loans and advances to customers",
    "Investment Securities", "Investment in other entities and long-term investments", "Fixed assets",
    "Investment properties", "Other Assets", "TOTAL ASSETS", "Due to Gov and borrowings from SBV",
    "Deposits and borrowings from other credit institutions", "Deposits from customers",
    "Derivatives and other financial liabilities", "Funds received from Gov, international and other institutions",
    "Convertible bonds/CDs and other valuable papers issued", "Other liabilities", "TOTAL LIABILITIES",
    "Charter capital", "Share premium", "Reserves", "Foreign Currency Difference reserve", "Retained Earnings",
    "Minority Interest", "OWNER'S EQUITY", "TOTAL RESOURCES",
]
EN_KEYS = ["cash", "sbv_deposits", "interbank_assets", "trading_securities", "derivatives_asset", "customer_loans_net",
           "customer_loans_gross", "loan_loss_provisions", "investment_securities", "long_term_investments",
           "fixed_assets", "investment_property", "other_assets", "total_assets", "gov_sbv_borrowings",
           "interbank_liabilities", "customer_deposits", "derivatives_liability", "trust_funds", "valuable_papers",
           "other_liabilities", "total_liabilities", "charter_capital", "share_premium", "reserves", "fx_revaluation",
           "retained_earnings", "minority_interest", "equity_total", None]


def vi_wide(periods=("2026-Q2", "2026-Q1"), scale=1.0):
    return pd.DataFrame({"item": [n for n, _ in VI_ROWS],
                         **{p: [None if v is None else v * scale for _, v in VI_ROWS] for p in periods}})


class Template(unittest.TestCase):
    def test_items_shape(self):
        self.assertEqual(B.BS_KEYS, [i["key"] for i in B.BS_ITEMS])
        self.assertEqual(len(B.BS_KEYS), len(set(B.BS_KEYS)))
        for i in B.BS_ITEMS:
            self.assertEqual(set(i), {"key", "side", "vi", "en", "parent", "total"})
            self.assertIn(i["side"], ("asset", "liability", "equity"))
            if i["parent"]:
                self.assertIn(i["parent"], B.BS_KEYS)
        self.assertEqual({i["key"] for i in B.BS_ITEMS if i["total"]}, {"total_assets", "total_liabilities", "equity_total"})
        self.assertEqual(sorted(B.BS_MATCH_ORDER + ["customer_loans_net", "customer_loans_gross"]), sorted(B.BS_KEYS))


class RowMapping(unittest.TestCase):
    def test_vietnamese_items_as_rows(self):
        per, labels, unmapped = B.extract_bs(B.to_long(vi_wide()), "quarter")
        self.assertEqual(sorted(per), ["2026-Q1", "2026-Q2"])
        self.assertEqual(per["2026-Q2"], {k: float(v) for k, v in VI_EXPECT.items()})
        self.assertEqual(labels["charter_capital"], "a. Vốn điều lệ")
        # sub-lines and the L+E total are reported, not silently dropped
        self.assertEqual(set(unmapped), {"1. Tiền gửi tại các TCTD khác", "2. Cho vay các TCTD khác",
                                         "1. Chứng khoán đầu tư sẵn sàng để bán", "1. Vốn của TCTD",
                                         "TỔNG NỢ PHẢI TRẢ VÀ VỐN CHỦ SỞ HỮU"})

    def test_year_kind_ignores_quarter_columns(self):
        per, _, _ = B.extract_bs(B.to_long(vi_wide(("2025", "2026-Q2"))), "year")
        self.assertEqual(list(per), ["2025"])

    def test_english_periods_as_rows(self):
        rows = [["VCB", 2026, 2] + [float(i + 1) for i in range(len(EN_COLS))],
                ["VCB", 2025, 5] + [float(i + 101) for i in range(len(EN_COLS))]]
        df = pd.DataFrame(rows, columns=["ticker", "yearReport", "lengthReport"] + EN_COLS)
        q, _, unmapped = B.extract_bs(B.to_long(df), "quarter")
        y, _, _ = B.extract_bs(B.to_long(df), "year")
        self.assertEqual(list(q), ["2026-Q2"])
        self.assertEqual(list(y), ["2025"])
        want = {k: float(i + 1) for i, k in enumerate(EN_KEYS) if k}
        self.assertEqual(q["2026-Q2"], want)
        self.assertEqual(unmapped, ["TOTAL RESOURCES"])      # yearReport/lengthReport/ticker are not items

    def test_long_format_with_ids(self):
        recs = [{"period": "2026-Q2", "id": B.norm(n), "name": n, "value": v} for n, v in VI_ROWS]
        per, _, _ = B.extract_bs(B.to_long(pd.DataFrame(recs)), "quarter")
        self.assertEqual(per["2026-Q2"]["customer_loans_gross"], 1000.0)
        self.assertEqual(per["2026-Q2"]["customer_loans_net"], 980.0)
        self.assertEqual(per["2026-Q2"]["equity_total"], 150.0)

    def test_bare_derivatives_label_split_by_side(self):
        rows = [("Công cụ tài chính phái sinh", 7), ("Tổng tài sản", 100),
                ("Công cụ tài chính phái sinh", 3), ("Tổng nợ phải trả", 90)]
        df = pd.DataFrame({"item": [n for n, _ in rows], "2026-Q2": [v for _, v in rows]})
        per, _, _ = B.extract_bs(B.to_long(df), "quarter")
        self.assertEqual(per["2026-Q2"]["derivatives_asset"], 7.0)
        self.assertEqual(per["2026-Q2"]["derivatives_liability"], 3.0)

    def test_single_unqualified_loans_is_gross_and_nan_skipped(self):
        df = pd.DataFrame({"item": ["Cho vay khách hàng", "Tiền gửi của khách hàng", "Tổng tài sản"],
                           "2026-Q2": [500.0, float("nan"), 900.0]})
        per, _, unmapped = B.extract_bs(B.to_long(df), "quarter")
        self.assertEqual(per["2026-Q2"], {"customer_loans_gross": 500.0, "total_assets": 900.0})
        self.assertEqual(unmapped, [])

    def test_units_divisor_from_bs(self):
        per, _, _ = B.extract_bs(B.to_long(vi_wide(scale=1e9)), "quarter")     # VND → bn VND
        self.assertEqual(B.money_divisor(per), 1e9)

    def test_build_periods_carries_bs(self):
        raw = {"2026-Q2": {"total_assets": 1197.5, "_src": {"VCI"}, "_bs": {"cash": 10.0, "total_assets": 1197.5}},
               "2026-Q1": {"_src": {"VCI"}, "_bs": {"cash": 9.0}}}
        out = B.build_periods(raw, B.QUARTERS, "2026-10-07T00:00:00+00:00", "quarter")
        self.assertEqual(out["2026-Q2"]["bs"], {"cash": 10.0, "total_assets": 1197.5})
        self.assertEqual(out["2026-Q1"]["bs"], {"cash": 9.0})      # bs-only period kept
        self.assertIsNone(out["2026-Q1"]["total_assets"])
        self.assertNotIn("_bs", out["2026-Q2"])


class FundamentalsPipeline(unittest.TestCase):
    """fundamentals() with V.finance stubbed (VND frames) and the /tmp cache bypassed."""
    def test_bs_flows_into_periods(self):
        from unittest import mock

        def fake_finance(sym, report, kind, src, **kw):
            if report != "balance_sheet":
                return None
            return vi_wide(("2026-Q2", "2026-Q1") if kind == "quarter" else ("2025",), scale=1e9)
        with mock.patch.object(B, "cached", lambda key, fn, refresh: fn()), \
                mock.patch.object(B.V, "finance", fake_finance):
            out, meta = B.fundamentals("AAA", ["VCI"], False)
        self.assertEqual(out["quarter"]["2026-Q2"]["_bs"]["customer_loans_gross"], 1000.0)   # VND → bn VND
        self.assertEqual(out["quarter"]["2026-Q2"]["total_assets"], 1197.5)
        self.assertEqual(out["year"]["2025"]["_bs"]["equity_total"], 150.0)
        self.assertIn("1. Vốn của TCTD", meta["unmapped"])
        self.assertEqual(meta["bs_map"]["VCI:charter_capital"], "a. Vốn điều lệ")
        per = B.build_periods(out["quarter"], B.QUARTERS, "t", "quarter")
        self.assertEqual(per["2026-Q1"]["bs"]["loan_loss_provisions"], -20.0)
        self.assertEqual(list(per["2026-Q1"]["bs"]), [k for k in B.BS_KEYS])      # template order


def vn_doc(banks):
    return {"_meta": {"as_of": "2026-10-07", "bs_items": B.BS_ITEMS}, "banks": banks}


def bank(t, quarterly=None):
    return {"ticker": t, "name": t + " bank", "exchange": "HOSE", "quarterly": quarterly or {}, "annual": {}}


def qp(bs, basis=None, source="vnstock_data VCI"):
    return {"bs": bs, "basis": basis, "source": source, "total_assets": bs.get("total_assets")}


class Endpoint(unittest.TestCase):
    def test_empty_file_returns_nulls(self):
        d = S._bs_history(vn_doc([bank("AAA"), bank("BBB")]), {})
        self.assertEqual(d["periods"], S.BS_HISTORY_QUARTERS)
        self.assertEqual(d["banks_total"], 2)
        self.assertEqual(d["banks_with_data"], 0)
        self.assertEqual(d["banks"]["AAA"]["values"]["cash"], [None] * 8)
        self.assertEqual(d["aggregate"]["values"]["total_assets"], [None] * 8)
        self.assertEqual(d["aggregate"]["coverage"]["total_assets"], [0] * 8)
        self.assertEqual(set(d["aggregate"]["values"]), set(B.BS_KEYS))

    def test_meta_without_bs_items_uses_fallback(self):
        d = S._bs_history({"_meta": {}, "banks": [bank("AAA")]}, {}, B.BS_ITEMS)
        self.assertEqual([i["key"] for i in d["items"]], B.BS_KEYS)

    def test_live_file_shape(self):
        d = json.loads(S.api_banks_bs_history().body)
        self.assertEqual(len(d["periods"]), 8)
        self.assertEqual([i["key"] for i in d["items"]], B.BS_KEYS)
        for b in d["banks"].values():
            self.assertEqual(set(b["values"]), set(B.BS_KEYS))
            self.assertTrue(all(len(v) == 8 for v in b["values"].values()))
            self.assertEqual(len(b["basis"]), 8)
            self.assertEqual(len(b["source"]), 8)

    def test_aggregate_and_coverage(self):
        d = S._bs_history(vn_doc([
            bank("AAA", {"2026-Q2": qp({"total_assets": 100.0, "cash": 1.0}), "2026-Q1": qp({"total_assets": 90.0})}),
            bank("BBB", {"2026-Q2": qp({"total_assets": 50.5}), "2023-Q4": qp({"total_assets": 1.0})}),
            bank("CCC"),
        ]), {})
        self.assertEqual(d["banks_total"], 3)
        self.assertEqual(d["banks_with_data"], 2)
        self.assertEqual(d["aggregate"]["values"]["total_assets"][-2:], [90.0, 150.5])
        self.assertEqual(d["aggregate"]["coverage"]["total_assets"], [0, 0, 0, 0, 0, 0, 1, 2])
        self.assertEqual(d["aggregate"]["values"]["cash"][-1], 1.0)
        self.assertEqual(d["aggregate"]["coverage"]["cash"][-1], 1)
        self.assertEqual(d["banks"]["AAA"]["source"][-1], "vnstock_data VCI")
        self.assertIsNone(d["banks"]["AAA"]["source"][0])
        self.assertEqual(d["aggregate"]["basis_mix"][-1], {"unlabelled": 2})


FIN = {"FINSYS": {"listed_banks_latest": {"banks": {
    "AAA": {"2026-06-30": {"basis": "consolidated", "total_assets": 999.0, "customer_loans": 700.0,
                           "customer_deposits": 600.0, "equity": 80.0, "items": {"cash": 5.0, "bogus_key": 1.0},
                           "source": "AAA Q2/2026 FS", "url": "https://example.invalid/aaa"},
            "2026-03-31": {"basis": "parent", "total_assets": 95.0, "customer_loans": 60.0, "source": "AAA Q1/2026 FS"},
            "2025-12-31": {"basis": "consolidated", "total_assets": 88.0, "equity": 7.0, "items": {"equity_total": 6.0}}},
    "ZZZ": {"2026-06-30": {"basis": "consolidated", "total_assets": 1.0}},   # not in the universe → ignored
}}}}


class FinanceMerge(unittest.TestCase):
    def setUp(self):
        self.d = S._bs_history(vn_doc([
            bank("AAA", {"2026-Q2": qp({"total_assets": 100.0}), "2025-Q4": qp({"cash": 2.0}, basis="consolidated")}),
            bank("BBB"),
        ]), FIN)
        self.a = self.d["banks"]["AAA"]

    def test_vnstock_period_not_mixed(self):
        # 2026-Q2: vnstock has data with an unlabelled basis → finance.json is not mixed in
        self.assertEqual(self.a["values"]["total_assets"][-1], 100.0)
        self.assertIsNone(self.a["values"]["customer_loans_gross"][-1])
        self.assertEqual(self.a["source"][-1], "vnstock_data VCI")

    def test_fills_empty_quarter(self):
        i = S.BS_HISTORY_QUARTERS.index("2026-Q1")
        self.assertEqual(self.a["values"]["total_assets"][i], 95.0)
        self.assertEqual(self.a["values"]["customer_loans_gross"][i], 60.0)
        self.assertEqual(self.a["basis"][i], "parent")
        self.assertEqual(self.a["source"][i], "data/finance.json FINSYS.listed_banks_latest (AAA Q1/2026 FS)")

    def test_same_basis_fills_missing_items_only(self):
        i = S.BS_HISTORY_QUARTERS.index("2025-Q4")
        self.assertEqual(self.a["values"]["cash"][i], 2.0)                 # vnstock value kept
        self.assertEqual(self.a["values"]["total_assets"][i], 88.0)        # filled (same labelled basis)
        self.assertEqual(self.a["values"]["equity_total"][i], 7.0)         # top-level equity wins over items
        self.assertTrue(self.a["source"][i].startswith("vnstock_data VCI + data/finance.json"))

    def test_mapping_and_unknown_keys(self):
        d = S._bs_history(vn_doc([bank("AAA")]), FIN)
        a = d["banks"]["AAA"]
        self.assertEqual(a["values"]["customer_loans_gross"][-1], 700.0)
        self.assertEqual(a["values"]["customer_deposits"][-1], 600.0)
        self.assertEqual(a["values"]["equity_total"][-1], 80.0)
        self.assertEqual(a["values"]["cash"][-1], 5.0)
        self.assertNotIn("bogus_key", a["values"])
        self.assertEqual(a["url"][-1], "https://example.invalid/aaa")
        self.assertNotIn("ZZZ", d["banks"])
        self.assertEqual(d["aggregate"]["coverage"]["total_assets"][-1], 1)
        self.assertEqual(d["aggregate"]["basis_mix"][-1], {"consolidated": 1})

    def test_date_quarter(self):
        self.assertEqual(S._date_quarter("2025-12-31"), "2025-Q4")
        self.assertEqual(S._date_quarter("2026-03-31"), "2026-Q1")
        self.assertIsNone(S._date_quarter("FY2025"))


if __name__ == "__main__":
    unittest.main()
