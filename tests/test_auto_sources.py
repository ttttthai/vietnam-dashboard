"""Unit tests for auto_sources.py (API parsing, graceful failure) and freshness.py.

Run:  python3 -m unittest discover -s tests -v
Fixtures in tests/fixtures/ are small files shaped like the real responses:
World Bank v2 ?format=json, IMF DataMapper /api/v1/<ind>/VNM, FRED fredgraph.csv.
"""
import json
import sys
import tempfile
import unittest
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import auto_sources as A  # noqa: E402
import freshness as F     # noqa: E402

FX = Path(__file__).parent / "fixtures"


def fx(name):
    p = FX / name
    return p.read_text(encoding="utf-8") if p.suffix == ".csv" else json.loads(p.read_text(encoding="utf-8"))


class WorldBankParsing(unittest.TestCase):
    def test_parse_vnm(self):
        p = A.parse_wb(fx("wb_vnm_NY.GDP.MKTP.CD.json"))
        self.assertEqual(p["lastupdated"], "2026-07-01")
        self.assertEqual(p["sourcename"], "World Development Indicators")
        self.assertEqual(A.wb_latest(p["rows"]), (2025, 514697215165.065))

    def test_error_payload_raises(self):
        with self.assertRaises(ValueError):
            A.parse_wb(fx("wb_error.json"))
        with self.assertRaises(ValueError):
            A.parse_wb({"not": "a list"})

    def test_rank_excludes_aggregates(self):
        econ = A.parse_wb_economies(fx("wb_countries.json"))
        self.assertNotIn("EAS", econ)
        self.assertIn("VNM", econ)
        rows = A.parse_wb(fx("wb_all_NY.GDP.MKTP.CD_2025.json"))["rows"]
        self.assertEqual(A.wb_rank(rows, "VNM", econ), {"rank": 4, "of": 5})     # USA, CHN, THA above; PRK null
        self.assertEqual(A.wb_rank(rows, "VNM", None), {"rank": 5, "of": 6})     # aggregate counted when unfiltered


class ImfAndFredParsing(unittest.TestCase):
    def test_imf(self):
        v = A.parse_imf(fx("imf_NGDP_RPCH_VNM.json"), "NGDP_RPCH")
        self.assertEqual(v[2025], 8.0)
        self.assertEqual(min(v), 2018)
        with self.assertRaises(ValueError):
            A.parse_imf({"values": {}}, "NGDP_RPCH")

    def test_fred(self):
        obs = A.parse_fred_csv(fx("fred_DFEDTARU.csv"))
        self.assertEqual(obs[-1], ("2025-01-02", 4.5))
        self.assertNotIn("2025-01-01", [d for d, _ in obs])                   # "." skipped
        self.assertEqual([c["date"] for c in A.fred_changes(obs)], ["2024-09-18", "2024-11-08", "2024-12-19"])
        with self.assertRaises(ValueError):
            A.parse_fred_csv("<html>blocked</html>")


def fake_get(url, as_text=False):
    """Serve fixtures for the URLs the fetchers build; other indicators get an API error payload."""
    if url == A.WB_COUNTRIES_URL:
        return fx("wb_countries.json")
    if "/country/VNM/indicator/NY.GDP.MKTP.CD" in url:
        return fx("wb_vnm_NY.GDP.MKTP.CD.json")
    if "/country/all/indicator/NY.GDP.MKTP.CD" in url and url.endswith("date=2025"):
        return fx("wb_all_NY.GDP.MKTP.CD_2025.json")
    if "api.worldbank.org" in url:
        return fx("wb_error.json")
    if url.endswith("/NGDP_RPCH/VNM"):
        return fx("imf_NGDP_RPCH_VNM.json")
    if "imf.org" in url:
        return {"values": {}}
    if "fredgraph.csv" in url:
        return fx("fred_DFEDTARU.csv")
    raise AssertionError(url)


def down_get(url, as_text=False):
    raise A.NetworkDown("blocked by sandbox")


class Fetchers(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self._dir, self._st = A.AUTO_DIR, A.STATUS_FILE
        A.AUTO_DIR = Path(self.tmp.name) / "auto"
        A.STATUS_FILE = A.AUTO_DIR / "_status.json"

    def tearDown(self):
        A.AUTO_DIR, A.STATUS_FILE = self._dir, self._st
        self.tmp.cleanup()

    def test_world_bank_snapshot(self):
        s = A.fetch_world_bank(get=fake_get)
        g = s["indicators"]["g_gdp"]
        self.assertEqual((g["latest_year"], g["lastupdated"]), (2025, "2026-07-01"))
        self.assertEqual(g["rank"]["rank"], 4)
        self.assertEqual(s["lastupdated"], "2026-07-01")
        self.assertTrue(any(e.startswith("pop:") for e in s["errors"]))       # other indicators failed, reported

    def test_run_writes_only_data_auto_and_survives_network_down(self):
        res = A.run(["world_bank", "imf_datamapper", "fed_funds"], get=fake_get)
        self.assertEqual(res["world_bank"]["status"], "partial")
        self.assertEqual(res["fed_funds"]["status"], "ok")
        written = sorted(p.name for p in A.AUTO_DIR.iterdir())
        self.assertEqual(written, ["_status.json", "fed_funds.json", "imf_datamapper.json", "world_bank.json"])
        ff = A.read_snapshot("fed_funds")
        self.assertEqual((ff["upper"], ff["lower"], ff["last_change"]), (4.5, 4.5, "2024-12-19"))

        before = A.read_snapshot("world_bank")
        logged = []
        res = A.run(["world_bank"], get=down_get, log_event=lambda *a, **k: logged.append((a, k)))
        self.assertEqual(res["world_bank"]["status"], "error")
        self.assertEqual(res["world_bank"]["last_success"], before["fetched_at"])
        self.assertEqual(A.read_snapshot("world_bank"), before)                  # previous snapshot kept
        self.assertEqual(logged[0][0][1:], ("auto:world_bank", "error"))

    def test_unknown_source_and_path_guard(self):
        self.assertEqual(A.run(["nope"], get=fake_get)["nope"]["status"], "error")
        with self.assertRaises(ValueError):
            A._safe_path("../economy")


class Freshness(unittest.TestCase):
    CAL = {"as_of": "2026-10-06", "series": [
        {"id": "m", "next_expected": ["2026-11-03 (Oct report)", "2026-12-03"]},
        {"id": "w", "next_expected": ["2026-10-01..05 (Sep)"]},
        {"id": "adhoc", "next_expected": ["ad hoc; something"]},
        {"id": "d", "next_expected": ["daily"]},
        {"id": "mon", "next_expected": ["2026-12 (mid-late)"]},
    ]}

    def rep(self, rows, today):
        with tempfile.TemporaryDirectory() as t:
            (Path(t) / "release_calendar.json").write_text(json.dumps(self.CAL))
            (Path(t) / "inventory.json").write_text(json.dumps({"families": rows}))
            return F.report(Path(t), today=today)

    def test_statuses(self):
        rows = [{"id": "a", "tab": "T", "calendar_id": "m", "last_checked": "2026-10-06"},
                {"id": "b", "tab": "T", "calendar_id": "adhoc"},
                {"id": "c", "tab": "U", "calendar_id": "d", "last_checked": "2026-10-01"},
                {"id": "e", "tab": "U", "calendar_id": None},
                {"id": "f", "tab": "U"}]
        s = {x["id"]: x["status"] for x in self.rep(rows, date(2026, 10, 6))["series"]}
        self.assertEqual(s, {"a": "fresh", "b": "waiting", "c": "overdue", "e": "waiting", "f": "waiting"})
        r = self.rep(rows, date(2026, 11, 2))
        self.assertEqual(r["series"][0]["status"], "due")
        self.assertEqual(r["tabs"]["T"]["status"], "due")
        r = self.rep(rows, date(2026, 11, 20))
        self.assertEqual((r["series"][0]["status"], r["series"][0]["days_overdue"]), ("overdue", 17))
        rows[0]["last_checked"] = "2026-11-10"                                   # checked after window, not moved
        self.assertEqual(self.rep(rows, date(2026, 11, 20))["series"][0]["status"], "waiting")
        rows[0]["last_changed"] = "2026-11-04"                                   # moved → next window (12-03)
        self.assertEqual(self.rep(rows, date(2026, 11, 20))["series"][0]["status"], "fresh")

    def test_parse_window_forms(self):
        self.assertEqual(F.parse_window("2026-12 (mid-late)"), {"start": date(2026, 12, 11), "end": date(2026, 12, 31)})
        self.assertEqual(F.parse_window("2027-06/07 (yearbook)")["end"], date(2027, 7, 31))
        self.assertEqual(F.parse_window("2027-H1 (2026 annual)")["end"], date(2027, 6, 30))
        self.assertEqual(F.parse_window("2029-2030")["start"], date(2029, 1, 1))
        self.assertEqual(F.parse_window("weekly"), {"every_days": 7})
        self.assertIsNone(F.parse_window("unknown — check monthly"))
        self.assertIsNone(F.parse_window(None))

    def test_missing_files(self):
        with tempfile.TemporaryDirectory() as t:
            r = F.report(Path(t), today=date(2026, 10, 6))
        self.assertEqual(r["series"], [])
        self.assertEqual(len(r["errors"]), 2)


if __name__ == "__main__":
    unittest.main()
