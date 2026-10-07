#!/usr/bin/env python3
"""Simulation tab: monthly panel, story beats, deposit-rate model and 6-month projection.

Writes data/simulation.json (key SIM). Owner: Finance agent.

Usage
  python3 tools/build/simulate.py                 # rebuild from stored/curated data (deterministic)
  python3 tools/build/simulate.py --fetch \
      --vnindex-json PATH --gold-csv PATH --brent-csv PATH
                                                  # refresh the three downloaded series first

Inputs (read-only): data/finance.json, data/economy.json, data/policy.json, data/invest_macro.json.
Downloaded series are cached inside data/simulation.json (panel.series.*.values) so a plain re-run
reproduces the same numbers without network access:
  * VN-Index month-end close: vnstock_data Quote('VNINDEX', source='vci').history(interval='1D')
  * World gold, USD/oz, monthly average: World Bank Pink Sheet via datahub (github datasets/gold-prices)
  * Brent, USD/bbl, monthly average: EIA via datahub (github datasets/oil-prices)

Rules followed: every value is a sourced observation (URL or the dashboard file it is copied from);
months without an observation stay null - nothing is interpolated. Step series (policy rates, posted
deposit rates) are "rate in force at month-end" built from dated decisions/quotes; where a posted rate
is carried between two identical dated quotes the month is flagged basis='carried'.
Derived series (gaps, 12-month changes, real rates) are arithmetic on stored series and flagged.
"""
import argparse
import csv
import json
import math
import sys
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "data"
OUT = DATA / "simulation.json"
AS_OF = "2026-10-07"

MONTHS = [f"{y}-{m:02d}" for y in range(2021, 2027) for m in range(1, 13)]
MONTHS = MONTHS[: MONTHS.index("2026-09") + 1]
PROJ_MONTHS = ["2026-10", "2026-11", "2026-12", "2027-01", "2027-02", "2027-03"]
IDX = {m: i for i, m in enumerate(MONTHS)}
N = len(MONTHS)


def tlabel_to_month(t):
    """'T10/24' -> '2024-10' (ISO 'YYYY-MM' passes through)."""
    if t[:1] != "T":
        return t[:7]
    m, y = t[1:].split("/")
    return f"20{y}-{int(m):02d}"


def nulls():
    return [None] * N


def rnd(x, k=2):
    return None if x is None else round(float(x), k)


# --------------------------------------------------------------------------------------------
# Curated observations (search-engine excerpts, 2026-10-06; press/SBV/NSO pages are blocked from
# the sandbox, so 'verified' says what was actually seen).
# --------------------------------------------------------------------------------------------

# Policy-rate decisions (rate in force from the effective date). Pre-2023 decisions verified via
# excerpt of Decision 1606/QD-NHNN (eff. 23/9/2022) and 1809/QD-NHNN (eff. 25/10/2022), which state
# the 'from' levels (refinancing 4.0, rediscount 2.5, overnight 5.0) in force since 1/10/2020.
POLICY_STEPS = {
    "refi_rate": [("2020-10-01", 4.0), ("2022-09-23", 5.0), ("2022-10-25", 6.0), ("2023-04-03", 5.5),
                  ("2023-05-25", 5.0), ("2023-06-19", 4.5)],
    "rediscount_rate": [("2020-10-01", 2.5), ("2022-09-23", 3.5), ("2022-10-25", 4.5), ("2023-03-15", 3.5),
                        ("2023-06-19", 3.0)],
    "sbv_overnight_rate": [("2020-10-01", 5.0), ("2022-09-23", 6.0), ("2022-10-25", 7.0), ("2023-03-15", 6.0),
                           ("2023-05-25", 5.5), ("2023-06-19", 5.0)],
}
POLICY_URLS = {
    "2022": ["https://thitruongtaichinhtiente.vn/ngan-hang-nha-nuoc-dieu-chinh-tang-lai-suat-dieu-hanh-va-lai-suat-tien-gui-ky-han-duoi-6-thang-42403.html",
             "https://thitruongtaichinhtiente.vn/ngan-hang-nha-nuoc-tang-lai-suat-dieu-hanh-lai-suat-tien-gui-ngan-han-lai-suat-cho-vay-ngan-han-42827.html"],
}

# FOMC upper bound decisions 2020-03 .. 2024-09 (finance.json carries Oct-2024 onward).
FED_STEPS = [("2020-03-16", 0.25), ("2022-03-17", 0.50), ("2022-05-05", 1.00), ("2022-06-16", 1.75),
             ("2022-07-28", 2.50), ("2022-09-22", 3.25), ("2022-11-03", 4.00), ("2022-12-15", 4.50),
             ("2023-02-02", 4.75), ("2023-03-23", 5.00), ("2023-05-04", 5.25), ("2023-07-27", 5.50),
             ("2024-09-19", 5.00)]

# Vietcombank 12-month VND savings rate, individuals, at the counter (Big-4 benchmark; VCB is usually
# the lowest of the four). value = rate in force at month-end (or latest quote in that month).
# basis: obs = dated quote in the month; event = change reported in the month (value after change);
#        carried = between two identical dated quotes with no reported change; derived = level implied
#        by a stated change (e.g. '-0.1 pp to 5.0%' implies 5.1% before).
VCB12 = {
    "2021-01": (5.6, "obs", "https://vietnambiz.vn/lai-suat-ngan-hang-vietcombank-moi-nhat-thang-1-2021-20210102155924454.htm", "quote 2/1/2021"),
    "2021-08": (5.5, "obs", "https://nguoiquansat.vn/lai-suat-tiet-kiem-vietcombank-moi-nhat-thang-8-2021-1627935747-37521.html", "Aug-2021 table"),
    "2021-09": (5.5, "carried", None, "between 5.5 (Aug-21) and 5.5 (Jan-22)"),
    "2021-10": (5.5, "carried", None, "between 5.5 (Aug-21) and 5.5 (Jan-22)"),
    "2021-11": (5.5, "carried", None, "between 5.5 (Aug-21) and 5.5 (Jan-22)"),
    "2021-12": (5.5, "carried", None, "between 5.5 (Aug-21) and 5.5 (Jan-22)"),
    "2022-01": (5.5, "obs", "https://vietnambiz.vn/lai-suat-ngan-hang-vietcombank-thang-1-2022-cao-nhat-la-bao-nhieu-2022011209283784.htm", "counter 5.5%, online 5.6%"),
    "2022-09": (6.4, "event", "https://vietnamfinance.vn/big-4-vao-cuoc-duong-dua-lai-suat-them-nong-d86460.html", "late Sep-2022 after SBV hike of 23/9: 12-month +0.8 pp to 6.4% (excerpt; the +0.8 implies 5.6% before, vs 5.5% counter quote in Jan-22: Feb-Aug 2022 left null)"),
    "2023-01": (7.4, "obs", "https://vietnambiz.vn/lai-suat-ngan-hang-vietcombank-moi-nhat-thang-12022-202313155749562.htm", "survey 3/1/2023 (the Oct/Nov-2022 step from 6.4 to 7.4 is not dated in the excerpts, so Oct-Dec 2022 are null)"),
    "2023-06": (6.3, "event", "https://nguoiquansat.vn/den-luot-ong-lon-vietcombank-giam-lai-suat-tien-gui-ky-han-ngan-chi-con-3-4-nam-80933.html", "cut to 6.3% for >=12 months announced 20/6/2023"),
    "2023-07": (6.3, "derived", "https://doanhnhan.baophapluat.vn/nhom-ngan-hang-big4-dong-loat-giam-lai-suat-tien-gui-52996.html", "23/8/2023 cut of 0.5 pp to 5.8% implies 6.3% through July"),
    "2023-08": (5.8, "event", "https://doanhnhan.baophapluat.vn/nhom-ngan-hang-big4-dong-loat-giam-lai-suat-tien-gui-52996.html", "Big-4 cut 23/8/2023: >=12 months -0.5 pp to 5.8%"),
    "2023-10": (5.1, "derived", "https://thitruongtaichinhtiente.vn/song-giam-lai-suat-chua-dut-vietcombank-tiep-tuc-giam-lai-suat-huy-dong-xuong-thap-ky-luc-52268.html", "cuts on 14/9, 3/10, 20/10; the 10/11 cut of a further 0.1 pp to 5.0% implies 5.1% at end-Oct (Sep-2023 level not stated: null)"),
    "2023-11": (5.0, "event", "https://thitruongtaichinhtiente.vn/song-giam-lai-suat-chua-dut-vietcombank-tiep-tuc-giam-lai-suat-huy-dong-xuong-thap-ky-luc-52268.html", "table effective 10/11/2023: >=12 months 5.0%"),
    "2023-12": (4.8, "event", "https://mekongasean.vn/nhom-big-4-ha-tiep-lai-suat-huy-dong-ky-han-12-thang-chi-con-48nam-15687.html", "mid-Dec-2023 cut to 4.8%"),
    "2024-01": (4.7, "event", "https://cafef.vn/lai-suat-ngan-hang-vietcombank-thang-1-2024-giam-manh-so-voi-thang-12-2023-188240108103124127.chn", "cut 12/1/2024 from 4.8% to 4.7%"),
    "2024-04": (4.6, "obs", "https://thuvienphapluat.vn/banan/tin-tuc/lai-suat-ngan-hang-vietcombank-thang-042024-10034.html", "Apr-2024: -0.1 pp to 4.6% (cut date late Mar/early Apr not pinned: Feb-Mar null)"),
    "2024-05": (4.6, "carried", None, "between 4.6 (Apr-24) and 4.6 (1/10/2024)"),
    "2024-06": (4.6, "carried", None, "between 4.6 (Apr-24) and 4.6 (1/10/2024)"),
    "2024-07": (4.6, "carried", None, "between 4.6 (Apr-24) and 4.6 (1/10/2024)"),
    "2024-08": (4.6, "carried", None, "between 4.6 (Apr-24) and 4.6 (1/10/2024)"),
    "2024-09": (4.6, "carried", None, "between 4.6 (Apr-24) and 4.6 (1/10/2024)"),
    "2024-10": (4.6, "obs", "https://thitruongtaichinhtiente.vn/lai-suat-tien-gui-tiet-kiem-tai-vietcombank-thang-10-2024-63046.html", "1/10/2024"),
    "2024-11": (4.6, "carried", None, "between 4.6 (Oct-24) and 4.6 (Jan-25)"),
    "2024-12": (4.6, "carried", None, "between 4.6 (Oct-24) and 4.6 (Jan-25)"),
    "2025-01": (4.6, "obs", "https://www.dnse.com.vn/senses/tin-tuc/lai-suat-ngan-hang-vietcombank-moi-nhat-thang-12025-34065700", "Jan-2025"),
    "2025-02": (4.6, "obs", "https://thitruongtaichinhtiente.vn/lai-suat-tien-gui-tiet-kiem-tai-vietcombank-trong-thang-2-2025-65536.html", "Feb-2025"),
    "2025-03": (4.6, "obs", "https://thitruongtaichinhtiente.vn/lai-suat-tai-ngan-hang-vietcombank-thang-3-2025-66110.html", "Mar-2025"),
    "2025-04": (4.6, "obs", "https://thitruongtaichinhtiente.vn/lai-suat-tien-gui-tiet-kiem-vietcombank-thang-4-2025-66864.html", "Apr-2025"),
    "2025-05": (4.6, "carried", None, "between 4.6 (Apr-25) and 4.6 (Jun-25)"),
    "2025-06": (4.6, "obs", "https://thitruongtaichinhtiente.vn/lai-suat-ngan-hang-vietcombank-moi-nhat-trong-thang-6-2025-68199.html", "Jun-2025: 12m 4.6%, 24m+ 4.7%"),
    "2025-07": (4.6, "carried", None, "between 4.6 (Jun-25) and 4.6 (Sep-25)"),
    "2025-08": (4.6, "carried", None, "between 4.6 (Jun-25) and 4.6 (Sep-25)"),
    "2025-09": (4.6, "obs", "https://www.dnse.com.vn/senses/tin-tuc/vietcombank-dang-trien-khai-lai-suat-tiet-kiem-bao-nhieu-trong-thang-9-35123237", "Sep-2025 (excerpt: 4.6% maintained since start of 2025). Oct-Dec 2025 not found: null"),
    "2026-01": (5.2, "obs", "https://thitruongtaichinhtiente.vn/lai-suat-tien-gui-tiet-kiem-vietcombank-thang-1-2026-75917.html", "Jan-2026; also cafef 6/1/2026"),
    "2026-02": (5.5, "event", "https://thitruongtaichinhtiente.vn/lai-suat-tien-gui-tiet-kiem-thang-2-2026-xu-huong-tang-tu-mat-bang-nen-thap-77528.html", "Feb-2026: +0.3 pp to 5.5%"),
    "2026-03": (5.9, "obs", "https://thitruongtaichinhtiente.vn/lai-suat-tien-gui-tiet-kiem-vietcombank-cuoi-thang-3-2026-80548.html", "end-Mar-2026: 12-13 months 5.9% (+0.4 pp)"),
    "2026-04": (5.9, "carried", None, "between 5.9 (end-Mar-26) and 5.9 (Jul-26)"),
    "2026-05": (5.9, "carried", None, "between 5.9 (end-Mar-26) and 5.9 (Jul-26)"),
    "2026-06": (5.9, "carried", None, "between 5.9 (end-Mar-26) and 5.9 (Jul-26)"),
    "2026-07": (5.9, "obs", "https://topi.vn/lai-suat-ngan-hang-vietcombank.html", "Jul-2026 counter 5.9% (rate aggregator page; another excerpt quotes 5.5% for late Jul, likely online/older - conflict)"),
    "2026-08": (5.9, "carried", None, "between 5.9 (Jul-26) and 5.9 (early Oct-26)"),
    "2026-09": (5.9, "carried", "https://vietbao.vn/gui-tiet-kiem-vietcombank-thang-102026-lai-suat-cao-nhat-6nam-609969.html", "carried to the early-Oct-2026 quote of 5.9% (vietbao). CONFLICT: vietnamnet 28/9/2026 says Big-4 posted 12-month rates were 6.8% (likely the highest Big-4 posting, not VCB)"),
}

# Interbank overnight - dated point observations before the monthly series starts (Oct-2024).
INTERBANK_POINTS = [
    {"date": "2021-07-30", "value": 1.0, "url": "https://thitruongtaichinhtiente.vn/print/36534.html", "note": "point; 2021 rates around/below 1%"},
    {"date": "2022-09-07", "value": 6.88, "url": "https://vietnamfinance.vn/dien-bien-la-lai-suat-qua-dem-lien-ngan-hang-len-dinh-9-thang-d107255.html", "note": "10-year high at the time (excerpt)"},
    {"date": "2022-10-04", "value": 7.74, "url": "https://thitruongtaichinhtiente.vn/lai-suat-qua-dem-lien-ngan-hang-tiep-tuc-lap-dinh-42578.html", "note": "record; intraday/other quotes up to 8.44% in early Oct-2022"},
    {"date": "2023-09-20", "value": 0.16, "url": "https://thoibaotaichinhvietnam.vn/lai-suat-cho-vay-qua-dem-thi-truong-lien-ngan-hang-chi-con-015-140163.html", "note": "near record low (0.15-0.16%)"},
    {"date": "2024-09-17", "value": 3.21, "url": "https://thoibaotaichinhvietnam.vn/lai-suat-qua-dem-lien-ngan-hang-di-vao-xu-huong-giam-deu-159602.html", "note": "point"},
]

# 10-year government bond, primary auction winning yield at HNX (State Treasury), end-of-month session.
GB10 = [
    {"date": "2021-12", "value": 2.08, "url": "https://vneconomy.vn/nam-2021-hnx-huy-dong-thanh-cong-332-467-ty-tang-2-9-so-voi-cung-ky.htm", "note": "end-Dec-2021"},
    {"date": "2022-10", "value": 4.0, "url": "https://vneconomy.vn/thang-10-lai-suat-phat-hanh-trai-phieu-chinh-phu-tang-tai-ky-han-10-nam-va-15-nam.htm", "note": "end-Oct-2022, +100 bp vs end-Sep"},
    {"date": "2022-12", "value": 4.9, "url": "https://nhandan.vn/thang-1-lai-suat-huy-dong-trai-phieu-chinh-phu-da-giam-tro-lai-post737903.html", "note": "highest level in Dec-2022 (excerpt 'cao nhất 4,9%'; may be max of month rather than month-end)"},
    {"date": "2023-10", "value": 2.42, "url": "https://thitruongtaichinhtiente.vn/thi-truong-trai-phieu-chinh-phu-thang-10-2023-lai-suat-phat-hanh-tang-tai-ky-han-10-nam-va-15-nam-52161.html", "note": "end-Oct-2023"},
    {"date": "2023-11", "value": 2.28, "url": "https://vneconomy.vn/thang-11-trai-phieu-chinh-phu-huy-dong-tren-hnx-tang-hon-68-so-voi-thang-truoc.htm", "note": "end-Nov-2023"},
    {"date": "2024-12", "value": 2.77, "url": "https://lsvn.vn/gan-30-600-ti-dong-duoc-huy-dong-thanh-cong-qua-kenh-dau-thau-trai-phieu-chinh-phu-a149403.html", "note": "end-2024"},
    {"date": "2025-12", "value": 4.0, "url": "https://baodauthau.vn/thang-122025-huy-dong-64581-ty-dong-trai-phieu-chinh-phu-qua-dau-thau-post192115.html", "note": "last auction of Dec-2025"},
    {"date": "2026-02", "value": 4.09, "url": "https://baodauthau.vn/2-thang-dau-nam-2026-huy-dong-duoc-hon-60000-ty-dong-trai-phieu-chinh-phu-post194923.html", "note": "end-Feb-2026"},
    {"date": "2026-06", "value": 4.35, "url": "https://baodauthau.vn/huy-dong-23375-ty-dong-trai-phieu-chinh-phu-qua-dau-thau-trong-thang-6-post202309.html", "note": "end-Jun-2026"},
    {"date": "2026-07", "value": 4.36, "url": "https://tapchikinhtetaichinh.vn/thang-7-2026-huy-dong-18-603-ty-dong-trai-phieu-chinh-phu-qua-dau-thau-164137.html", "note": "end-Jul-2026"},
    {"date": "2026-08", "value": 4.41, "url": "https://baodauthau.vn/huy-dong-hon-27756-ty-dong-trai-phieu-chinh-phu-qua-dau-thau-trong-thang-8-post207553.html", "note": "last Aug-2026 session, +5 bp vs last Jul session (HNX; search excerpt 2026-10-07)"},
    {"date": "2026-09", "value": 4.43, "url": "https://www.thestar.com.my/aseanplus/aseanplus-news/2026/09/27/vietnam-raises-us1bil-n-government-bond-auction-highest-volume-this-year", "note": "auction of 23/9/2026 (Reuters): 20,000 bn 10-year fully sold at 4.43%. A 30/9 auction probably followed (9M issuance implies ~11 tn after 23/9) but its 10-year yield was not found. Rejected: '10-year 4.67-4.80% in Sep' (vnbusiness excerpt) is from an earlier year (5-year at 5.08% above the 10-year)."},
]

# CPI y/y (NSO). Values before Oct-2024 only where a search excerpt confirmed the figure; others null.
CPI_EXCERPT = {
    "2021-04": (2.70, "https://www.nso.gov.vn/wp-content/uploads/2021/04/1.-Tong-quan-CPI-thang-4-va-4-thang-nam-2021.pdf"),
    "2021-05": (2.90, "https://www.nso.gov.vn/wp-content/uploads/2021/05/1.-Tong-quan-CPI-thang-5-va-5-thang-nam-2021.pdf"),
    "2021-09": (2.06, "https://www.nso.gov.vn/wp-content/uploads/2021/09/1-Thong-cao-Bao-chi-Gia_29.9.2021.pdf"),
    "2021-10": (1.77, "https://thoibaotaichinhvietnam.vn/cpi-in-october-down-02-percent-may-surge-in-remaining-months-gso-94454.html"),
    "2021-12": (1.81, "https://www.nso.gov.vn/wp-content/uploads/2021/12/1.Tong-quan-CPI-T12-va-nam-2021_Final.pdf"),
    "2022-04": (2.64, "https://english.haiquanonline.com.vn/tags/consumer-price-index-1951.tag"),
    "2022-06": (3.37, "https://www.nso.gov.vn/?p=38394"),
    "2022-07": (3.14, "https://vneconomy.vn/luong-thuc-xang-dau-day-cpi-thang-7-tang-nhe.htm"),
    "2022-08": (2.89, "https://english.vtv.vn/news/vietnams-cpi-up-258-in-january-august-20220829153242683.htm"),
    "2022-09": (3.94, "https://www.nso.gov.vn/gia/infographic/?paged=10"),
    "2022-10": (4.30, "https://voh.com.vn/kinh-te/chi-so-gia-tieu-dung-thang-10-2022-tang-0-15-454161.html"),
    "2022-11": (4.37, "https://thoibaotaichinhvietnam.vn/cpi-thang-112022-tang-039-117624.html"),
    "2022-12": (4.55, "https://www.dtnext.in/news/world/vietnams-cpi-up-315-in-2022"),
    "2023-01": (4.89, "https://www.vietnamplus.vn/ca-nam-2023-cpi-tang-325-dat-muc-tieu-quoc-hoi-de-ra-post918255.vnp"),
    "2023-03": (3.35, "https://english.haiquanonline.com.vn/tags/consumer-price-index-1951.tag"),
    "2023-06": (2.00, "https://www.vietnamplus.vn/ca-nam-2023-cpi-tang-325-dat-muc-tieu-quoc-hoi-de-ra-post918255.vnp"),
    "2023-07": (2.06, "https://www.vietnamplus.vn/ca-nam-2023-cpi-tang-325-dat-muc-tieu-quoc-hoi-de-ra-post918255.vnp"),
    "2023-08": (2.96, "https://thitruongtaichinhtiente.vn/cpi-thang-8-bat-tang-2-96-so-voi-cung-ky-nam-truoc-49773.html"),
    "2023-09": (3.66, "https://nguoiquansat.vn/tinh-hinh-gia-thang-9-quy-iii-va-9-thang-nam-2023-91775.html"),
    "2023-10": (3.59, "https://baoquocte.vn/ly-do-cpi-thang-102023-tang-32-va-so-voi-cung-ky-nam-2022-247894.html"),
    "2023-11": (3.45, "https://baodauthau.vn/cpi-thang-11-tang-345-post147209.html"),
    "2023-12": (3.58, "https://www.vietnamplus.vn/ca-nam-2023-cpi-tang-325-dat-muc-tieu-quoc-hoi-de-ra-post918255.vnp"),
    "2024-03": (3.97, "https://thitruongtaichinhtiente.vn/cpi-thang-5-tang-4-44-so-voi-cung-ky-59246.html"),
    "2024-04": (4.40, "https://thitruongtaichinhtiente.vn/cpi-thang-5-tang-4-44-so-voi-cung-ky-59246.html"),
    "2024-05": (4.44, "https://thitruongtaichinhtiente.vn/cpi-thang-5-tang-4-44-so-voi-cung-ky-59246.html"),
    "2024-06": (4.34, "https://mekongasean.vn/nhu-cau-an-uong-tang-cao-day-cpi-6-thang-2024-tang-hon-4-30276.html"),
    "2024-09": (2.63, "https://www.nso.gov.vn/wp-content/uploads/2024/10/Thong-cao-bao-chi-gia-thang-9.2024.pdf"),
}

# Credit growth YTD - SBV statements at stated cut-off dates (2021-2024). Stored as dated points:
# cut-offs vary (20th, 26th, month-end), so they are not forced onto months.
CREDIT_YTD_POINTS = [
    {"date": "2021-03-31", "value": 2.93, "url": "https://www.div.gov.vn/tang-truong-tin-dung-nua-dau-nam-2021-gap-doi-cung-ky-nam-truoc-", "note": "end-Q1-2021"},
    {"date": "2021-06-15", "value": 5.10, "url": "https://www.div.gov.vn/tang-truong-tin-dung-nua-dau-nam-2021-gap-doi-cung-ky-nam-truoc-"},
    {"date": "2021-10-07", "value": 7.42, "url": "https://vnbusiness.vn/het-quy-iii2021-nhieu-ngan-hang-da-dat-han-muc-tin-dung-nam-2021.html"},
    {"date": "2021-10-29", "value": 8.72, "url": "https://www.kbsec.com.vn/pic/Service/KBSV_FTM_TINDUNG_2021115.pdf", "note": "level 9,994,371 bn VND"},
    {"date": "2021-12-31", "value": 13.61, "url": "https://www.div.gov.vn/nam-2021-chinh-sach-tien-te-va-hoat-dong-ngan-hang-duoc-dieu-chinh-linh-hoat-thich-ung-voi-tinh-hinh-dich-benh-covid-19", "note": "SBV year-end; finance.json annual credit_growth 2021 = 13.6"},
    {"date": "2022-03-31", "value": 5.04, "url": "https://www.div.gov.vn/tin-dung-den-cuoi-quy-i-2022-tang-5-04-"},
    {"date": "2022-08-26", "value": 9.91, "url": "https://vnbusiness.vn/pho-thong-doc-nhnn-tin-dung-tang-truong-rat-cao-toi-504.html"},
    {"date": "2022-12-31", "value": 14.5, "url": "https://div.gov.vn/huong-den-thi-truong-von-an-toan-lanh-manh-giup-ngan-hang-giam-ganh-nang-tin-dung-", "note": "Governor's estimate; CONFLICT: finance.json annual credit_growth 2022 = 14.2"},
    {"date": "2023-03-20", "value": 1.61, "url": "https://vietnamfinance.vn/tac-dong-tien-tang-truong-tin-dung-cham-khi-nhu-cau-vay-von-tang-cao-d93216.html"},
    {"date": "2023-09-20", "value": 5.73, "url": "https://thitruongtaichinhtiente.vn/print/50433.html", "note": "mobilisation +5.8% at the same date"},
    {"date": "2023-11-30", "value": 9.15, "url": "https://viettimes.vn/tang-truong-tin-dung-nam-2023-dat-135-post172514.html"},
    {"date": "2023-12-31", "value": 13.71, "url": "data/policy.json POLICY.mon credit_growth_target series", "note": "CONFLICT: excerpt 'about 13.5%'; finance.json annual 13.78"},
    {"date": "2024-12-07", "value": 12.5, "url": "https://diendandoanhnghiep.vn/tang-truong-huy-dong-von-ky-luc-ho-tro-mo-rong-tin-dung-10148775.html", "note": "mobilisation +7.36% at the same date"},
    {"date": "2024-12-31", "value": 15.08, "url": "https://thitruongtaichinhtiente.vn/ket-thuc-nam-2024-tin-dung-tang-15-08-65089.html"},
]
DEPOSIT_YTD_POINTS = [
    {"date": "2023-06-30", "value": 4.6, "url": "https://viettimes.vn/tang-truong-tin-dung-nam-2023-dat-135-post172514.html", "note": "system mobilisation"},
    {"date": "2023-09-20", "value": 5.8, "url": "https://thitruongtaichinhtiente.vn/print/50433.html", "note": "mobilisation of credit institutions"},
    {"date": "2024-09-30", "value": 4.9, "url": "https://diendandoanhnghiep.vn/tang-truong-huy-dong-von-ky-luc-ho-tro-mo-rong-tin-dung-10148775.html", "note": "9M-2024 mobilisation"},
    {"date": "2024-12-07", "value": 7.36, "url": "https://diendandoanhnghiep.vn/tang-truong-huy-dong-von-ky-luc-ho-tro-mo-rong-tin-dung-10148775.html"},
]

# Average lending rate on NEW loans (SBV statements) - points.
LENDING_NEW_POINTS = [
    {"date": "2025-07-20", "value": 6.53, "url": "https://baodauthau.vn/lai-suat-cho-vay-moi-khoang-653-post182721.html", "note": "SBV: new-loan average, -0.4 pp vs end-2024"},
    {"date": "2025-08", "value": 6.23, "url": "https://baodauthau.vn/lai-suat-cho-vay-moi-khoang-653-post182721.html", "note": "excerpt: 6.23%, -0.7 pp vs end-2024 (outlet/date pinned only to the search result set)"},
]

# Margin lending by securities companies (outstanding), VND bn - quarter-end points.
MARGIN = [
    {"date": "2022-03", "value": 201824, "url": "https://viettimes.vn/du-no-margin-toan-thi-truong-uoc-tinh-230000-ty-dong-vao-cuoi-quy-12022-cao-nhat-tu-truoc-toi-nay-post155604.html", "note": "total loans of 106 securities firms; whole-market margin (incl. bank channels) estimated ~230,000 bn - different scope"},
    {"date": "2022-12", "value": 122400, "url": "https://vneconomy.vn/du-no-cho-vay-172-000-ty-dong-cao-nhat-7-quy-co-dang-lo.htm", "note": "Q4-2022; another tally gives ~104,000 bn (vneconomy) - scope differs"},
    {"date": "2023-12", "value": 180000, "url": "https://vneconomy.vn/du-no-cho-vay-172-000-ty-dong-cao-nhat-7-quy-co-dang-lo.htm", "note": "'nearly 180,000 bn'"},
    {"date": "2024-03", "value": 193300, "url": "https://thitruongtaichinhtiente.vn/den-het-quy-ii-2024-du-no-cho-vay-margin-dat-gan-218-900-ty-dong-muc-cao-nhat-trong-lich-su-60329.html"},
    {"date": "2024-06", "value": 218900, "url": "https://thitruongtaichinhtiente.vn/den-het-quy-ii-2024-du-no-cho-vay-margin-dat-gan-218-900-ty-dong-muc-cao-nhat-trong-lich-su-60329.html"},
    {"date": "2024-12", "value": 248800, "url": "https://legiang.it.com/thanh-khoan-sut-giam-vi-dau-du-no-margin-cong-ty-chung-khoan-van-dat-ky-luc-gan-250-ngan-ty", "note": "loans incl. advances"},
    {"date": "2025-06", "value": 280000, "url": "https://mekongasean.vn/cong-ty-chung-khoan-nao-bom-tien-manh-nhat-cho-vay-ky-quy-44040.html", "note": "30 largest firms only"},
    {"date": "2025-09", "value": 356000, "url": "https://vietnamfinance.vn/du-no-margin-tai-cong-ty-chung-khoan-vuot-muc-ky-luc-400000-ty-dong-d139125.html", "note": "margin, Q3-2025"},
    {"date": "2025-12", "value": 392000, "url": "https://thoibaotaichinhvietnam.vn/dang-sau-con-so-cho-vay-margin-cao-ky-luc-cua-khoi-cong-ty-chung-khoan-191699-191699.html", "note": "estimate, Q4-2025"},
    {"date": "2026-06", "value": 453800, "url": "https://vietstock.vn/2026/07/du-no-margin-lap-ky-luc-454-ngan-ty-dong-cuoc-dua-co-su-phan-hoa-830-1468883.htm", "note": "from finance.json flows (incl. advances; other tallies 445-446k)"},
]

# New securities accounts (VSDC; domestic investors).
ACCOUNTS_MONTH = [
    {"date": "2021-01", "value": 86269, "url": "https://thoibaotaichinhvietnam.vn/gan-200000-tai-khoan-chung-khoan-mo-moi-thang-dau-nam-100113.html"},
    {"date": "2021-05", "value": 114000, "url": "https://baodauthau.vn/thang-52021-gan-114-nghin-tai-khoan-chung-khoan-mo-moi-post107636.html", "note": "'nearly 114 thousand'"},
    {"date": "2021-11", "value": 220000, "url": "https://baodauthau.vn/hon-220000-tai-khoan-chung-khoan-ca-nhan-mo-moi-trong-thang-11-post117361.html", "note": "'over 220,000' (individuals)"},
    {"date": "2022-01", "value": 194835, "url": "https://thoibaotaichinhvietnam.vn/gan-200000-tai-khoan-chung-khoan-mo-moi-thang-dau-nam-100113.html"},
    {"date": "2022-05", "value": 476000, "url": "https://thoibaotaichinhvietnam.vn/hon-476000-tai-khoan-chung-khoan-duoc-mo-trong-thang-52022-106624.html", "note": "'over 476,000'"},
    {"date": "2023-01", "value": 36000, "url": "https://thitruongtaichinhtiente.vn/co-them-hon-36-nghin-tai-khoan-giao-dich-chung-khoan-mo-moi-trong-thang-1-giam-2-3-so-voi-thang-truoc-44265.html", "note": "'over 36 thousand'"},
    {"date": "2025-01", "value": 81000, "url": "https://thoibaotaichinhvietnam.vn/81000-tai-khoan-chung-khoan-mo-moi-thang-dau-nam-2025-170069.html"},
    {"date": "2025-05", "value": 191000, "url": "https://thoibaotaichinhvietnam.vn/gan-191000-tai-khoan-chung-khoan-mo-moi-trong-thang-5-so-tai-khoan-chung-khoan-can-moc-10-trieu-177893.html", "note": "'nearly 191,000'; total accounts passed 10 million"},
    {"date": "2025-07", "value": 226000, "url": "https://thoibaotaichinhvietnam.vn/hon-226-nghin-tai-khoan-chung-khoan-mo-moi-trong-thang-7-cao-nhat-trong-vong-11-thang-181243.html", "note": "'over 226 thousand', highest in 11 months"},
    {"date": "2025-08", "value": 257632, "url": "https://thitruongtaichinhtiente.vn/gan-260-000-tai-khoan-giao-dich-chung-khoan-mo-moi-trong-thang-8-70281.html", "note": "VSDC; total accounts ~10.7 m at end-Aug-2025"},
    {"date": "2025-10", "value": 310651, "url": "https://doanhnhan.baophapluat.vn/don-tin-thi-truong-duoc-nang-hang-co-hon-310000-tai-khoan-chung-khoan-duoc-mo-moi-trong-thang-102025-88541.html", "note": "domestic accounts +310,651 vs end-Sep-2025 (individuals +310,496); 11,305,254 at 30/10/2025. Sep-2025 not found (the '172,605/172,695 in Sep' articles are Sep-2024): null"},
    {"date": "2025-11", "value": 237000, "url": "https://doanhnhan.baophapluat.vn/so-luong-tai-khoan-chung-khoan-mo-moi-cham-day-4-thang-89547.html", "note": "'over 237,000', ~73,000 fewer than Oct, lowest in 4 months"},
    {"date": "2025-12", "value": 279383, "url": "https://vneconomy.vn/nha-dau-tu-ca-nhan-lai-o-at-mo-tai-khoan-chung-khoan-trong-thang-cuoi-nam-2025.htm", "note": "domestic accounts +279,383"},
    {"date": "2026-01", "value": 244700, "url": "https://thoibaotaichinhvietnam.vn/hon-244700-tai-khoan-chung-khoan-mo-moi-trong-thang-1-192048.html", "note": "'over 244,700' (also 'nearly 245,000', mekongasean)"},
    {"date": "2026-02", "value": 198159, "url": "https://nhandan.vn/ocop/ket-thuc-quy-i2026-viet-nam-co-hon-126-trieu-tai-khoan-chung-khoan-post954464.html", "note": "derived in the same release: Mar increase 345,979 is '147,820 more than Feb' -> 198,159; press 'over 198,000' (https://thoibaotaichinhvietnam.vn/hon-198-nghin-tai-khoan-chung-khoan-mo-moi-trong-thang-2-193524.html); 9-day Tet holiday"},
    {"date": "2026-03", "value": 345979, "url": "https://nhandan.vn/ocop/ket-thuc-quy-i2026-viet-nam-co-hon-126-trieu-tai-khoan-chung-khoan-post954464.html", "note": "domestic accounts +345,979; 12,661,417 accounts at end-Q1"},
    {"date": "2026-04", "value": 244700, "url": "https://mekongasean.vn/hon-244700-tai-khoan-chung-khoan-duoc-mo-moi-trong-thang-4-55018.html", "note": "'about 244,700', ~-30% vs Mar; individuals >244,300"},
    {"date": "2026-05", "value": 256500, "url": "https://vietnamfinance.vn/vn-index-lap-dinh-nha-dau-tu-o-at-mo-moi-tai-khoan-chung-khoan-d145805.html", "note": "'over 256,500' (individuals 255,700); total ~13.16 m"},
    {"date": "2026-06", "value": 268000, "url": "https://thoibaotaichinhvietnam.vn/nha-dau-tu-trong-nuoc-mo-moi-gan-268000-tai-khoan-chung-khoan-200329.html", "note": "'nearly 268,000'"},
    {"date": "2026-07", "value": 227300, "url": "https://tapchikinhtetaichinh.vn/them-227-nghin-tai-khoan-giao-dich-chung-khoan-duoc-bo-sung-trong-thang-7-2026-163901.html", "note": "'over 227,300'; 13.66 m accounts at end-Jul"},
    {"date": "2026-08", "value": 230000, "url": "https://tapchikinhtetaichinh.vn/so-tai-khoan-chung-khoan-ca-nhan-tuong-duong-gan-14-dan-so-166573.html", "note": "'nearly 230,000'; individuals >13.8 m at end-Aug. Sep-2026 VSDC figure not found by 2026-10-07 (an excerpt '158,302 in Sep' is Sep-2023): null"},
]
ACCOUNTS_YEAR = [
    {"period": "2021", "value": 1500000, "url": "https://baodauthau.vn/tai-khoan-chung-khoan-mo-moi-tang-manh-trong-nam-2022-post132874.html", "note": "'over 1.5 million'"},
    {"period": "2022", "value": 2600000, "url": "https://baodauthau.vn/nam-2022-gan-26-trieu-tai-khoan-chung-khoan-mo-moi-post133309.html", "note": "'nearly 2.6 million', record"},
    {"period": "2024-Jan-Sep", "value": 1570000, "url": "https://thoibaotaichinhvietnam.vn/tai-khoan-chung-khoan-mo-moi-trong-thang-8-o-muc-cao-nhat-hon-2-nam-qua-159157.html", "note": "individual accounts opened Jan-Sep 2024"},
    {"period": "2025", "value": 2600000, "url": "https://nhandan.vn/ocop/viet-nam-co-them-26-trieu-tai-khoan-chung-khoan-nam-2025-vuot-xa-muc-tieu-11-trieu-tai-khoan-vao-nam-2030-post935997.html", "note": "'+2.6 million' accounts in 2025 (>11.8 m total)"},
]

# Public investment (state-budget capital) disbursed, full year incl. the January extension month.
PUBLIC_INVEST_YEAR = [
    {"period": "2024", "value_bn": 624540.4, "pct_pm_plan": 91.45, "url": "https://thoibaotaichinhvietnam.vn/dau-an-dieu-hanh-quyet-liet-va-nen-tang-cho-but-pha-nam-2026-191834.html", "note": "13 months to 31/1/2025 (another excerpt: 84.47% of total plan, 93.06% of PM-assigned plan - different denominators)"},
    {"period": "2025", "value_bn": 858621.8, "pct_pm_plan": 94.8, "url": "https://thoibaotaichinhvietnam.vn/dau-an-dieu-hanh-quyet-liet-va-nen-tang-cho-but-pha-nam-2026-191834.html", "note": "to 31/1/2026; central 320,358.1 (74.5%), local 538,263.7 (113.1%); largest public-investment envelope ever (1,180,935.4 bn incl. local top-ups and carry-overs)"},
    {"period": "2026-9M", "value_bn": 642961.6, "pct_pm_plan": 62.9, "url": "https://baochinhphu.vn/giai-ngan-von-dau-tu-cong-9-thang-nam-2026-mot-so-bo-nganh-dia-phuong-con-cham-102261003011740764.htm", "note": "cumulative to 30/9/2026 (data/invest_macro.json); +202,559 bn vs 9M-2025"},
]

# State Treasury (KBNN) deposits at commercial banks - points (coverage differs by point).
TREASURY = [
    {"date": "2024-03", "value": 94000, "scope": "BIDV+VietinBank+Vietcombank", "url": "https://vtcnews.vn/3-ngan-hang-nao-dang-giu-gan-100-000-ty-dong-cua-kho-bac-nha-nuoc-ar868030.html"},
    {"date": "2024-06", "value": 292000, "scope": "Big-4", "url": "https://vietnamfinance.vn/kho-bac-nha-nuoc-gui-gan-292000-ty-dong-tai-nhom-big4-d114882.html"},
    {"date": "2025-09", "value": 460000, "scope": "Vietcombank+VietinBank+BIDV", "url": "https://vnbusiness.vn/kho-bac-nha-nuoc-co-460000-ty-dong-gui-tai-3-ngan-hang-nhom-big-4.html", "note": "+26% vs end-2024"},
    {"date": "2025-12", "value": 406000, "scope": "state-owned banks", "url": "https://vcci.com.vn/tin-tuc/kho-bac-nha-nuoc-gui-hon-400-nghin-ty-dong-tai-big4-ngan-hang", "note": "'over 406,000', +11% vs end-2024"},
    {"date": "2026-03", "value": 626700, "scope": "all banks", "url": "https://vnexpress.net/chinh-phu-muon-tang-tien-gui-cua-kho-bac-tai-ngan-hang-5091002.html", "note": "from finance.json flows"},
    {"date": "2026-06", "value": 700000, "scope": "all banks (approx.)", "url": "https://baodautu.vn/noi-tran-tien-gui-kho-bac-len-50-big-4-ngan-hang-co-them-hang-tram-nghin-ty-cho-vay-d659909.html", "note": "from finance.json flows"},
]

# Real estate.
RE_POINTS = {
    "hanoi_primary_apartment_price_mvnd_m2": [
        {"period": "2022-Q4", "value": 47, "url": "https://thitruongtaichinhtiente.vn/bat-chap-thi-truong-tram-lang-gia-ban-can-ho-so-cap-tai-ha-noi-tang-15-trong-nam-2022-44183.html", "provider": "Savills (via finance.json)"},
        {"period": "2023-Q4", "value": 58, "url": "https://baomoi.com/ha-noi-gia-can-ho-tang-20-quy-lien-tiep-rat-hiem-can-duoi-2-ti-dong-c48102695.epi", "provider": "Savills (via finance.json)"},
        {"period": "2024-Q4", "value": 75, "url": "https://vn.savills.com.vn/blog/article/220417/vietnam-viet/toan-canh-thi-truong-can-ho-q4-2024.aspx", "provider": "Savills (via finance.json)"},
        {"period": "2025-Q4", "value": 102, "url": "https://vtv.vn/trung-binh-102-trieu-dong-m2-gia-can-ho-tai-ha-noi-100260322082236072.htm", "provider": "Savills (via finance.json)"},
        {"period": "2026-Q2", "value": 116, "url": "https://cafef.vn/chung-cu-ha-noi-vang-bong-can-ho-duoi-70-trieu-dong-m2-188260814143655062.chn", "provider": "Savills Q2-2026 (latest quarter, not Q4; +16% q/q, +27% y/y; via finance.json)"},
    ],
    "hanoi_primary_apartment_price_cbre_mvnd_m2": [
        {"period": "2026-Q2", "value": 95, "url": "https://vietnamfinance.vn/nguon-cung-chung-cu-ha-noi-lap-ky-luc-gia-can-ho-moi-neo-cao-d147694.html", "provider": "CBRE (excl. VAT; +12% q/q, +21% y/y) - separate provider, never mixed with Savills"},
    ],
    "hanoi_hcmc_new_launch_price_moc_2025": [
        {"period": "2025-Q2", "city": "Hanoi", "value": 80, "url": "https://doanhnhan.baophapluat.vn/gia-chung-cu-tai-ha-noi-va-tp-hcm-lap-dinh-moi-cao-nhat-gan-mot-thap-ky-84861.html", "provider": "MoC quarterly report", "note": "+5.6% q/q"},
        {"period": "2025-FY", "city": "Hanoi", "value": 91, "url": "https://doanhnhan.baophapluat.vn/thi-truong-bat-dong-san-2025-phuc-hoi-ro-net-nhung-gia-nha-van-cao.html", "provider": "MoC", "note": "average new-launch apartment price end-2025"},
        {"period": "2025-FY", "city": "HCMC", "value": 87, "url": "https://doanhnhan.baophapluat.vn/thi-truong-bat-dong-san-2025-phuc-hoi-ro-net-nhung-gia-nha-van-cao.html", "provider": "MoC"},
    ],
    "supply_2025": {"projects_licensed": 93, "units": 37686, "yoy_pct": 17, "period": "2025 vs 2024", "url": "https://doanhnhan.baophapluat.vn/thi-truong-bat-dong-san-2025-phuc-hoi-ro-net-nhung-gia-nha-van-cao.html", "provider": "MoC (commercial housing projects licensed)", "note": "2025 apartment prices rose 'commonly 20-30%, >40% in some areas'"},
}

CORP_BOND = [
    {"period": "2022", "private_bn": 247976, "public_bn": 10599, "url": "https://vneconomy.vn/quy-mo-phat-hanh-trai-phieu-doanh-nghiep-giam-60-gan-164-000-ty-dong-duoc-mua-lai.htm", "note": "issuance; both about -65% vs 2021 (VBMA)"},
]


# --------------------------------------------------------------------------------------------
def step_monthly(steps):
    """Value in force at the last day of each month from (YYYY-MM-DD, value) decisions."""
    out = nulls()
    for i, m in enumerate(MONTHS):
        last = m + "-31"
        val = None
        for d, v in steps:
            if d <= last:
                val = v
        out[i] = val
    return out


def series(label_vi, label_en, unit, values, definition, source, url_or_note, verified, group, **kw):
    s = {"label_vi": label_vi, "label_en": label_en, "unit": unit, "freq": kw.pop("freq", "monthly"),
         "group": group, "values": values, "definition": definition, "source": source,
         "url_or_note": url_or_note, "verified": verified}
    s.update(kw)
    s["coverage"] = coverage(values) if s["freq"] == "monthly" else None
    return s


def coverage(values):
    idx = [i for i, v in enumerate(values) if v is not None]
    if not idx:
        return {"n": 0, "first": None, "last": None}
    return {"n": len(idx), "first": MONTHS[idx[0]], "last": MONTHS[idx[-1]]}


def pct_change(vals, lag):
    out = nulls()
    for i in range(lag, N):
        a, b = vals[i], vals[i - lag]
        if a is not None and b not in (None, 0):
            out[i] = round((a / b - 1) * 100, 2)
    return out


def align(arr, months):
    out = nulls()
    for m, v in zip(months, arr):
        if m in IDX:
            out[IDX[m]] = v
    return out


def diff(a, b, k=2):
    return [None if (x is None or y is None) else round(x - y, k) for x, y in zip(a, b)]


def load(name):
    return json.loads((DATA / name).read_text(encoding="utf-8"))


def read_csv_monthly(path, date_col=0, val_col=1):
    out = {}
    with open(path, newline="") as f:
        r = csv.reader(f)
        next(r)
        for row in r:
            out[row[date_col][:7]] = float(row[val_col])
    return out


# --------------------------------------------------------------------------------------------
def build_panel(prev, args):
    fin = load("finance.json")["FINSYS"]
    eco = load("economy.json")
    pol = load("policy.json")["POLICY"]["mon"]
    imac = load("invest_macro.json")["MACRO"]
    why = fin["why"]["series"]
    fm = fin["monthly"]
    fin_months = [tlabel_to_month(t) for t in fm["months"]]
    S = {}

    def from_fin_monthly(arr, months=fin_months):
        out = nulls()
        for m, v in zip(months, arr):
            if m in IDX and v is not None:
                out[IDX[m]] = v
        return out

    # ---------------- policy / government rates
    ins = {x["id"]: x for x in pol["instruments"]}
    for key, lv, le in [("refi_rate", "Lãi suất tái cấp vốn", "Refinancing rate"),
                        ("rediscount_rate", "Lãi suất tái chiết khấu", "Rediscount rate"),
                        ("sbv_overnight_rate", "Lãi suất cho vay qua đêm NHNN", "SBV overnight lending rate")]:
        S[key] = series(lv, le, "% p.a.", step_monthly(POLICY_STEPS[key]),
                        "Rate in force at month-end, from SBV decisions (step series).",
                        "SBV decisions; 2023+ as in data/policy.json POLICY.mon.instruments",
                        "2022 hikes: " + " ; ".join(POLICY_URLS["2022"]) + " | 2023 cuts: see policy.json history URLs",
                        "2022 decisions (1606, 1809/QĐ-NHNN) confirmed by search excerpt 2026-10-06; 2023+ = Policy tab", "policy",
                        decisions=[{"date": d, "value": v} for d, v in POLICY_STEPS[key]])
    omo = ins["omo_rate"]["series"]
    S["omo_rate"] = series("Lãi suất OMO (cầm cố)", "OMO repo rate", "% p.a.",
                           [v if m >= "2024-01" else None for m, v in zip(MONTHS, step_monthly(list(zip(omo["dates"], omo["values"]))))],
                           "Rate at which SBV lends via open-market repos, in force at month-end. Before 2024 not in the Policy tab series: null.",
                           "data/policy.json POLICY.mon.instruments[omo_rate]", "see policy.json history URLs (dttc.sggp, vtv, tinnhanhchungkhoan, vneconomy)",
                           "Policy tab", "policy")
    # Fed: pre-Oct-2024 from FOMC decisions, Oct-2024+ from finance.json
    fed = step_monthly(FED_STEPS)
    ff = from_fin_monthly(why["fed_funds_upper"]["values"], [tlabel_to_month(t) for t in why["fed_funds_upper"]["months"]])
    fed = [f if f is not None else (fed[i] if MONTHS[i] < "2024-10" else None) for i, f in enumerate(ff)]
    S["fed_funds_upper"] = series("Lãi suất Fed (cận trên)", "Fed funds target, upper bound", "%", fed,
                                  "Upper bound of the FOMC target range in force at month-end.",
                                  "FOMC statements; Oct-2024+ copied from finance.json why.series.fed_funds_upper",
                                  "https://www.federalreserve.gov/monetarypolicy/openmarket.htm",
                                  "2022 and Jul-2023 moves confirmed by search excerpt; others per FOMC record; Oct-2024+ per finance.json", "external")
    S["fed_minus_refi"] = series("Chênh lệch Fed − tái cấp vốn", "Fed upper bound minus SBV refinancing rate", "pp",
                                 diff(fed, S["refi_rate"]["values"]),
                                 "Derived: fed_funds_upper − refi_rate. Proxy for the USD-VND policy-rate gap (the OMO rate has no pre-2024 series).",
                                 "derived", "derived from panel", "derived", "external", derived=True)
    gb = nulls()
    for o in GB10:
        gb[IDX[o["date"]]] = o["value"]
    S["gov_bond_10y"] = series("Lợi suất TPCP 10 năm (trúng thầu)", "10-year government bond, auction winning yield", "% p.a.", gb,
                               "Primary-auction winning yield of 10-year State Treasury bonds at HNX, end-of-month session (one Dec-2022 value is the month's high). Primary market, not secondary yields.",
                               "HNX auction results as reported by press", "per observation", "search excerpts 2026-10-06", "policy",
                               freq_note="sparse month points; other months null", observations=GB10)

    # ---------------- bank rates
    dep = nulls(); basis = [None] * N; dep_src = [None] * N
    for m, (v, b, u, note) in VCB12.items():
        dep[IDX[m]] = v; basis[IDX[m]] = b; dep_src[IDX[m]] = {"url": u, "note": note}
    S["dep12_vcb"] = series("Lãi suất tiết kiệm 12 tháng – Vietcombank (Big4)", "12-month deposit rate – Vietcombank (Big-4 benchmark)", "% p.a.", dep,
                            "Vietcombank posted 12-month VND savings rate for individuals at the counter, interest at maturity, in force at month-end. A Big-4 posted rate: it understates what private banks pay and excludes special/negotiated rates. Not an SBV average (no SBV monthly average series is published in reach).",
                            "Vietcombank rate tables via press (per month in 'per_month')", "https://vietnambiz.vn/lai-suat-ngan-hang-vietcombank.html",
                            "search excerpts 2026-10-06", "bank", basis=basis, per_month=dep_src,
                            conflicts=["Sep-2026: 5.9% (VCB, carried to early-Oct quote) vs 'Big-4 posted 12M 6.8%' (vietnamnet 28/9/2026) - kept VCB; the 6.8% is probably another Big-4 bank's posting.",
                                       "Sep-2022: '+0.8 pp to 6.4%' implies 5.6% before vs Jan-2022 counter quote 5.5% (online 5.6%)."])
    mbs = nulls()
    for d, v in zip(why["deposit12m_private_banks_mbs"]["dates"], why["deposit12m_private_banks_mbs"]["values"]):
        mbs[IDX[d[:7]]] = v
    S["dep12_private_mbs"] = series("Lãi suất 12 tháng bình quân – NH tư nhân (MBS)", "12-month deposit rate, private-bank average (MBS)", "% p.a.", mbs,
                                    "MBS average 12-month deposit rate; Dec-2025 = private-bank group; Jul/Aug-2026 'commercial banks' - groups differ slightly; may include special rates. Different definition from dep12_vcb: never merged.",
                                    "copied from finance.json why.series.deposit12m_private_banks_mbs", "; ".join(why["deposit12m_private_banks_mbs"]["src"]),
                                    "per finance.json (search excerpts)", "bank")
    S["lending_new_avg_sbv"] = series("Lãi suất cho vay bình quân mới (NHNN)", "Average lending rate on new loans (SBV)", "% p.a.",
                                      nulls(), "SBV-reported average lending rate of commercial banks on new transactions. Only point statements exist; stored in 'observations'. 2026 SBV statements give ranges (8.1–10.7%, new + outstanding, see finance.json why.series.avg_lending_rate_sbv) - a different measure, not merged.",
                                      "SBV statements via press", "per observation", "search excerpts 2026-10-06", "bank",
                                      freq="points", observations=LENDING_NEW_POINTS + [{"date": d, "value": v, "url": u, "note": "finance.json avg_lending_rate_sbv (different measure: SOCB & JSCB, new + outstanding)"} for d, v, u in zip(why["avg_lending_rate_sbv"]["dates"], why["avg_lending_rate_sbv"]["values"], why["avg_lending_rate_sbv"]["src"])])
    ib = why["interbank_on_monthly"]
    S["interbank_on"] = series("Lãi suất liên ngân hàng qua đêm", "Interbank overnight rate", "% p.a.",
                               from_fin_monthly(ib["values"], [tlabel_to_month(t) for t in ib["months"]]),
                               "VND overnight interbank rate; monthly values Oct-2024+ are a mix of month averages and month-end prints (see 'types'). Before Oct-2024 only dated points ('observations'), not monthly values.",
                               "finance.json why.series.interbank_on_monthly (per-month URLs there) + dated press points", "per month in finance.json; points per observation",
                               "search excerpts 2026-10-06", "bank",
                               types=align(ib["types"], [tlabel_to_month(t) for t in ib["months"]]),
                               observations=INTERBANK_POINTS)

    # ---------------- credit & funding
    credit_level = from_fin_monthly(fm["credit_level"])
    dep_level = from_fin_monthly(fm["deposits_level"])
    m2_level = from_fin_monthly(fm["m2_level"])
    S["credit_ytd"] = series("Tăng trưởng tín dụng so với đầu năm", "Credit growth YTD", "%", from_fin_monthly(fm["credit_ytd"]),
                             "Credit to the economy vs previous year-end (SBV). Monthly values Nov-2024+ from finance.json (SBV month-end tables/statements; check cut-off notes there). 2021-2024 only at stated cut-off dates ('observations').",
                             "finance.json FINSYS.monthly.credit_ytd + SBV statements", "https://sbv.gov.vn/du-no-tin-dung-doi-voi-nen-kt-dttktt", "per finance.json / search excerpts", "credit",
                             observations=CREDIT_YTD_POINTS,
                             basis=align(fm["credit_basis"], fin_months), cutoff=align(fm["credit_cutoff"], fin_months),
                             basis_note="Per month: 'sbv_table' = SBV month-end table row; 'statement' = SBV/Government statement at the cut-off date in 'cutoff' (rounded level). Aug/Sep-2026 are statement points (28/8, 30/9).")
    cy = from_fin_monthly(fm["credit_yoy"])
    der_cy = pct_change(credit_level, 12)
    S["credit_yoy"] = series("Tăng trưởng tín dụng so với cùng kỳ", "Credit growth y/y", "%",
                             [a if a is not None else b for a, b in zip(cy, der_cy)],
                             "Credit to the economy, y/y. finance.json credit_yoy where stated; otherwise derived from SBV month-end levels (credit_level) 12 months apart (flag in 'derived_months').",
                             "finance.json FINSYS.monthly", "https://sbv.gov.vn/du-no-tin-dung-doi-voi-nen-kt-dttktt", "per finance.json", "credit",
                             derived_months=[MONTHS[i] for i in range(N) if cy[i] is None and der_cy[i] is not None],
                             basis=[b if b is not None else (("derived_from_statement_level" if cb == "statement" else "derived_from_sbv_table_levels") if d is not None else None)
                                    for b, d, cb in zip(align(fm["credit_yoy_basis"], fin_months), der_cy, align(fm["credit_basis"], fin_months))],
                             basis_note="'statement' = SBV-stated y/y; 'computed_from_sbv_table_levels' (finance.json) / 'derived_from_sbv_table_levels' (here) = table level vs table level 12 months earlier; 'derived_from_statement_level' = rounded statement level (Aug-2026: ~20.5 quadrillion at 28/8) vs the Aug-2025 table month-end - indicative only (rounding ~±0.15 pp, cut-offs 3 days apart); not used by the model (deposit y/y for Aug-2026 is null).")
    S["deposit_ytd"] = series("Tăng trưởng huy động (dân cư + TCKT) so với đầu năm", "Deposit growth YTD (residents + organisations)", "%",
                              from_fin_monthly(fm["deposits_ytd"]),
                              "Customer deposits of residents + economic organisations at credit institutions vs previous year-end (SBV money-supply tables, ~2-month lag). 2021-2024 only stated mobilisation points ('observations' - mobilisation, a broader concept).",
                              "finance.json FINSYS.monthly.deposits_ytd", "SBV statistics via finance.json", "per finance.json", "credit",
                              observations=DEPOSIT_YTD_POINTS + [
                                  {"date": c, "value": v, "basis": b, "url": u, "note": nt}
                                  for c, v, b, u, nt in [(fm["deposits_ytd_statement_cutoff"][i], fm["deposits_ytd_statement"][i], fm["deposits_ytd_statement_basis"][i],
                                                          *{"vnd_mobilisation_statement": ("https://vnexpress.net/huy-dong-von-cua-ngan-hang-tang-nhanh-hon-tin-dung-5116224.html",
                                                                                           "statement basis, cut-off 22/8/2026: VND mobilisation only (MoF, Government press conference 3/9/2026); not the SBV table measure"),
                                                            "mobilisation_nso_report": ("https://vietstock.vn/2026/10/tinh-den-289-tin-dung-toan-nen-kinh-te-tang-1089-757-1498652.htm",
                                                                                        "statement basis, cut-off 28/9/2026: mobilisation per NSO Q3-2026 report (credit +10.89% same date); not the SBV table measure")}[fm["deposits_ytd_statement_basis"][i]])
                                                         for i in range(len(fm["months"])) if fm["deposits_ytd_statement"][i] is not None]])
    S["deposit_yoy"] = series("Tăng trưởng huy động so với cùng kỳ", "Deposit growth y/y", "%", pct_change(dep_level, 12),
                              "Derived from SBV month-end deposit levels (residents + organisations) 12 months apart; null where either level is missing. Note an Oct-2025 reclassification between residents and organisations (total unaffected).",
                              "derived from finance.json FINSYS.monthly.deposits_level", "derived", "derived", "credit", derived=True)
    S["credit_deposit_gap_yoy"] = series("Khoảng cách tăng trưởng tín dụng − huy động (y/y)", "Credit minus deposit growth gap (y/y)", "pp",
                                         diff(S["credit_yoy"]["values"], S["deposit_yoy"]["values"]),
                                         "Derived: credit_yoy − deposit_yoy (same-month).", "derived", "derived", "derived", "credit", derived=True)
    S["credit_deposit_gap_ytd"] = series("Khoảng cách tín dụng − huy động (từ đầu năm)", "Credit minus deposit growth gap (YTD)", "pp",
                                         diff(S["credit_ytd"]["values"], S["deposit_ytd"]["values"]),
                                         "Derived: credit_ytd − deposit_ytd (same month; cut-offs may differ by a few days).", "derived", "derived", "derived", "credit", derived=True)
    ann = fin["annual"]
    S["annual_growth"] = {"label_vi": "Tăng trưởng tín dụng, huy động, M2 theo năm", "label_en": "Annual credit, deposit and M2 growth",
                          "unit": "% y/y (year-end)", "freq": "annual", "group": "credit",
                          "periods": [y for y in ann["years"] if y >= 2019],
                          "credit": [v for y, v in zip(ann["years"], ann["credit_growth"]) if y >= 2019],
                          "deposit": [v for y, v in zip(ann["years"], ann["deposit_growth"]) if y >= 2019],
                          "m2": [v for y, v in zip(ann["years"], ann["m2_growth"]) if y >= 2019],
                          "gap": [rnd(c - d) for y, c, d in zip(ann["years"], ann["credit_growth"], ann["deposit_growth"]) if y >= 2019],
                          "definition": "Copied from finance.json FINSYS.annual (credit_growth, deposit_growth, m2_growth); gap derived = credit − deposit. Deposit growth 2016-2025 is computed from IMF FSI customer deposits (deposit takers) per finance.json sources - not the same as the SBV monthly deposit series.",
                          "source": "finance.json FINSYS.annual + FINSYS.sources", "url_or_note": "see finance.json sources.deposit_growth_by_year", "verified": "per finance.json"}
    ldr = nulls()
    for o in fin["ratios"]["ldr_sbv_tt22_pct"]:
        if o["d"] in IDX:
            ldr[IDX[o["d"]]] = o["v"]
    S["ldr_sbv"] = series("Tỷ lệ LDR toàn hệ thống (TT22)", "System loan-to-deposit ratio (Circular 22)", "%", ldr,
                          "SBV LDR under Circular 22/2019 (loans / deposits, both as defined in the circular; whole system excl. VBSP & PCFs). Not the simple loans/customer-deposits ratio (~114% in Q2-2026, see 'ldr_simple_listed').",
                          "finance.json FINSYS.ratios.ldr_sbv_tt22_pct", "https://sbv.gov.vn/vi/thong-ke-mot-so-chi-tieu-co-ban", "per finance.json", "credit",
                          freq_note="sparse months")
    cap = step_monthly([("2020-01-01", 85.0)])
    S["ldr_cap"] = series("Trần LDR", "LDR cap", "%", cap,
                          "Regulatory cap for commercial banks under Circular 22/2019 (85%); Circular 50/2026/TT-NHNN raises it to 95% from 1 Dec 2026 (outside the panel; used in projections).",
                          "data/policy.json POLICY.mon.instruments[ldr_cap]", "https://vietnambiz.vn/nhnn-nang-tran-ty-le-cho-vay-tren-tien-gui-len-95-cong-bo-hai-chi-tieu-quan-ly-thanh-kho", "Policy tab", "credit",
                          upcoming={"date": "2026-12-01", "value": 95, "doc": "Circular 50/2026/TT-NHNN"})
    S["ldr_simple_listed"] = {"label_vi": "LDR đơn giản (cho vay/tiền gửi khách hàng)", "label_en": "Simple LDR (loans / customer deposits)", "unit": "%", "freq": "quarterly", "group": "credit",
                              "periods": why["ldr"]["periods"], "values": why["ldr"]["values"], "definition": why["ldr"]["note"],
                              "source": "finance.json why.series.ldr", "url_or_note": "; ".join(why["ldr"]["src"]), "verified": "per finance.json"}
    S["m2_yoy"] = series("Tăng trưởng M2 so với cùng kỳ", "M2 growth y/y", "%", pct_change(m2_level, 12),
                         "Derived from SBV month-end M2 levels 12 months apart. SBV changed the M2 methodology from Oct-2025, so y/y across that break is indicative only.",
                         "derived from finance.json FINSYS.monthly.m2_level", "derived", "derived", "credit", derived=True)
    S["m2_ytd"] = series("Tăng trưởng M2 so với đầu năm", "M2 growth YTD", "%", from_fin_monthly(fm["m2_ytd"]),
                         "SBV M2 vs previous year-end.", "finance.json FINSYS.monthly.m2_ytd", "SBV statistics via finance.json", "per finance.json", "credit")

    # ---------------- where savings went
    pv = (prev or {}).get("panel", {}).get("series", {})
    vni = nulls()
    if args.vnindex_json:
        raw = json.loads(Path(args.vnindex_json).read_text())
        for m, (d, c) in raw.items():
            if m in IDX:
                vni[IDX[m]] = c
        vni_pre = {m: c for m, (d, c) in raw.items() if "2019-12" < m < "2021-01"}
    else:
        vni = pv.get("vnindex", {}).get("values", nulls())
        vni_pre = pv.get("vnindex", {}).get("pre_2021", {})
    S["vnindex"] = series("VN-Index", "VN-Index", "points", vni,
                          "HOSE VN-Index, close of the last trading day of the month (price index, no dividends).",
                          "vnstock_data Quote('VNINDEX', source='vci').history(interval='1D'), pulled 2026-10-06", "https://trading.vietcap.com.vn",
                          "vnstock (VCI) 2026-10-06", "savings", pre_2021=vni_pre)
    vmap = dict(vni_pre)
    vmap.update({m: v for m, v in zip(MONTHS, vni) if v is not None})
    v12 = nulls()
    for i, m in enumerate(MONTHS):
        pm = f"{int(m[:4]) - 1}-{m[5:]}"
        if m in vmap and vmap.get(pm):
            v12[i] = round((vmap[m] / vmap[pm] - 1) * 100, 2)
    S["vnindex_12m"] = series("VN-Index – lợi suất 12 tháng", "VN-Index 12-month return", "%", v12,
                              "Derived: % change of month-end VN-Index vs 12 months earlier (price only).", "derived", "derived", "derived", "savings", derived=True)

    def ext_monthly(key, csv_path, start="2020-01"):
        if csv_path:
            raw = read_csv_monthly(csv_path)
            vals = [raw.get(m) for m in MONTHS]
            pre = {m: raw[m] for m in raw if "2019-12" < m < "2021-01"}
            return vals, pre
        return pv.get(key, {}).get("values", nulls()), pv.get(key, {}).get("pre_2021", {})

    gold, gold_pre = ext_monthly("gold_world_usd", args.gold_csv)
    S["gold_world_usd"] = series("Vàng thế giới (USD/oz, TB tháng)", "World gold price (USD/oz, monthly average)", "USD/oz", gold,
                                 "World Bank Commodity Markets (Pink Sheet) monthly average gold price, via datahub core/gold-prices. Monthly AVERAGE (not month-end); USD terms. SBV-controlled SJC bar prices trade at a large, variable premium - see sjc_gold_sell.",
                                 "World Bank Pink Sheet via https://github.com/datasets/gold-prices (data/monthly.csv)", "https://raw.githubusercontent.com/datasets/gold-prices/main/data/monthly.csv",
                                 "downloaded 2026-10-06", "savings", pre_2021=gold_pre)
    g_ext = dict(gold_pre); g_ext.update({m: v for m, v in zip(MONTHS, gold) if v is not None})
    g12 = nulls()
    for i, m in enumerate(MONTHS):
        y, mo = int(m[:4]), int(m[5:])
        pm = f"{y-1}-{mo:02d}"
        if g_ext.get(m) and g_ext.get(pm):
            g12[i] = round((g_ext[m] / g_ext[pm] - 1) * 100, 2)
    S["gold_world_12m"] = series("Vàng thế giới – thay đổi 12 tháng", "World gold 12-month change", "%", g12,
                                 "Derived: % change of the monthly average vs the same month a year earlier (USD).", "derived", "derived", "derived", "savings", derived=True)
    S["gold_world_mom"] = series("Vàng thế giới – thay đổi so với tháng trước", "World gold month-on-month change", "%",
                                 [None] + [None if (gold[i] is None or gold[i - 1] is None) else round((gold[i] / gold[i - 1] - 1) * 100, 2) for i in range(1, N)],
                                 "Derived: % change of the monthly average vs previous month (USD).", "derived", "derived", "derived", "savings", derived=True)
    alt = dict(fin["alternatives"]["series"])
    alt["years"] = fin["alternatives"]["years"]
    sjc_obs = []
    for y in range(2020, 2026):
        p = alt["gold_sjc"]["points"][str(y)]
        sjc_obs.append({"date": f"{y}-12", "value": alt["gold_sjc"]["level_vnd_per_tael"][alt["years"].index(y)], "quote": p.get("quote"), "url": p.get("url")})
    sjc_obs += [{"date": "2026-01", "value": 184.2, "quote": "SJC 181.7–184.2 on 29/1/2026 (record)", "url": "https://baovanhoa.vn/kinh-te/gia-vang-hom-nay-2912026-sjc-tang-soc-lap-dinh-moi-201009.html"},
                {"date": "2026-10", "value": fin["alternatives"]["ytd"]["gold_sjc"]["level"], "quote": fin["alternatives"]["ytd"]["gold_sjc"]["quote"] + " on 6/10/2026", "url": fin["alternatives"]["ytd"]["gold_sjc"]["url"]}]
    sjc_obs[-1:-1] = [{"date": p["d"][:7], "value": p["v"], "quote": p["quote"] + " on " + p["d"], "url": p["u"], "note": p["n"]}
                      for p in alt["gold_sjc"].get("month_end_2026", {}).get("points", []) if p.get("v") is not None]
    sjc = nulls()
    for o in sjc_obs:
        if o["date"] in IDX:
            sjc[IDX[o["date"]]] = o["value"]
    S["sjc_gold_sell"] = series("Vàng miếng SJC (giá bán)", "SJC gold bar, sell price", "million VND per tael", sjc,
                                "SJC bar sell price at the last session of the year (Dec values), the Jan-2026 record (29/1), month-end quotes Feb-Sep 2026 (finance.json alternatives.series.gold_sjc.month_end_2026; Jun-2026 null, Sep-2026 derived from the 1/10 stated change) and the 6-Oct-2026 quote (in observations). Full monthly history before 2026 could not be pulled (sjc.com.vn/simplize blocked): other months null.",
                                "finance.json FINSYS.alternatives.series.gold_sjc + press", "per observation", "search excerpts", "savings", observations=sjc_obs)
    # SJC premium vs world gold converted (year-end points only, derived)
    fx_ye = {f"{y}-12": alt["usd"]["level"][alt["years"].index(y)] for y in range(2020, 2026)}
    prem = []
    for o in sjc_obs:
        m = o["date"]
        g = g_ext.get(m)
        fx = fx_ye.get(m)
        if g and fx:
            world_mvnd_tael = g * fx * 37.5 / 31.1035 / 1e6
            prem.append({"date": m, "sjc": o["value"], "world_converted": round(world_mvnd_tael, 2),
                         "premium_mvnd": round(o["value"] - world_mvnd_tael, 2), "premium_pct": round((o["value"] / world_mvnd_tael - 1) * 100, 1)})
    S["sjc_premium"] = {"label_vi": "Chênh lệch giá vàng SJC so với thế giới", "label_en": "SJC premium over world gold", "unit": "million VND per tael / %",
                        "freq": "annual", "group": "savings", "observations": prem,
                        "definition": "Derived: SJC year-end sell price minus world gold converted at the SBV year-end central rate (USD/oz × VND/USD × 37.5 g / 31.1035 g per oz). World gold is the December monthly AVERAGE (Pink Sheet), not the 31-Dec price, so the premium is approximate.",
                        "source": "derived", "url_or_note": "derived from sjc_gold_sell, gold_world_usd, finance.json alternatives.usd", "verified": "derived", "derived": True}
    mg = nulls()
    for o in MARGIN:
        mg[IDX[o["date"]]] = o["value"]
    S["margin_lending"] = series("Dư nợ cho vay ký quỹ (CTCK)", "Margin lending by securities firms", "VND bn", mg,
                                 "Outstanding loans of securities companies to investors (margin, some tallies incl. advances) at quarter-end. Coverage differs by tally (all firms vs top-30) - see notes; not a consistent index.",
                                 "press tallies of securities firms' financial statements", "per observation", "search excerpts 2026-10-06", "savings",
                                 freq="quarterly (sparse)", observations=MARGIN)
    ac = nulls()
    for o in ACCOUNTS_MONTH:
        ac[IDX[o["date"]]] = o["value"]
    S["new_stock_accounts"] = series("Tài khoản chứng khoán mở mới", "New securities trading accounts", "accounts per month", ac,
                                     "New domestic investor accounts opened in the month (VSDC). Sparse month points + annual totals in 'annual'.",
                                     "VSDC via press", "per observation", "search excerpts 2026-10-06", "savings",
                                     observations=ACCOUNTS_MONTH, annual=ACCOUNTS_YEAR)
    S["real_estate"] = {"label_vi": "Bất động sản: giá căn hộ sơ cấp và nguồn cung", "label_en": "Real estate: primary apartment prices and supply",
                        "unit": "million VND/m² (prices)", "freq": "quarterly/annual", "group": "savings", "series": RE_POINTS,
                        "definition": "Hanoi average primary (new-launch) apartment asking price per Savills, Q4 of each year plus the latest quarter (2026-Q2, labelled); CBRE Q2-2026 kept in its own list (different basis, excl. VAT); MoC new-launch averages for 2025 (Hanoi and HCMC separately). Different providers and launch mix each quarter - not a constant-quality index, not spliced. HCMC Savills Q4 series not built (excerpts inconsistent, see finance.json).",
                        "source": "Savills Vietnam (via finance.json alternatives.real_estate) and Ministry of Construction reports", "url_or_note": "per observation", "verified": "search excerpts"}
    S["corporate_bonds"] = {"label_vi": "Trái phiếu doanh nghiệp", "label_en": "Corporate bonds", "unit": "VND bn", "freq": "annual/points", "group": "savings",
                            "issuance": CORP_BOND,
                            "issuance_2026_cumulative": fin["flows"]["Corporate bond issuance, 2026 cumulative (VBMA)"],
                            "issuance_monthly_2026": fin["flows"]["Corporate bond issuance by month, 2026 (VBMA, as disclosed by month-end)"],
                            "outstanding": fin["flows"]["Corporate bonds outstanding"],
                            "definition": "Issuance (VBMA; private placement and public) and outstanding (FiinGroup/VBMA via finance.json). Annual issuance for 2021, 2023-2025 not confirmed in excerpts: missing, not estimated. 2026: VBMA cumulative YTD points (8M ~349,000 bn) and month figures as disclosed by each month-end (Mar and Jul null); VBMA revises cumulatives upward for late disclosures, so months do not sum to the cumulative - see finance.json sources.corporate_bond_issuance_2026.",
                            "source": "VBMA / FiinGroup via press; finance.json flows", "url_or_note": "per observation", "verified": "search excerpts"}

    # ---------------- fiscal
    st = eco["ECONFLOW"]["inv"]
    S["public_investment"] = {"label_vi": "Đầu tư công: giải ngân vốn NSNN", "label_en": "Public investment disbursed (state budget)", "unit": "VND bn / % of PM-assigned plan",
                              "freq": "annual", "group": "fiscal", "observations": PUBLIC_INVEST_YEAR,
                              "state_sector_investment_nso": {"periods": st["years"], "values_bn": st["state"],
                                                              "definition": "NSO realised investment by the state sector (current prices) - broader than budget capital disbursement; copied from data/economy.json ECONFLOW.inv.state."},
                              "monthly_note": "Monthly cumulative disbursement % for 2021-2025 not collected (only 2M-2022 8.61%, H1-2023 30.49% vs H1-2022 27.75% seen in excerpts); left out rather than interpolated.",
                              "definition": "MoF-reported disbursement of state-budget investment capital for the plan year (incl. the January extension month). % of the plan assigned by the Prime Minister.",
                              "source": "Ministry of Finance via press; data/invest_macro.json", "url_or_note": "per observation", "verified": "search excerpts 2026-10-06"}
    tr = nulls()
    S["treasury_deposits"] = series("Tiền gửi Kho bạc Nhà nước tại NHTM", "State Treasury deposits at commercial banks", "VND bn", tr,
                                    "KBNN deposits at banks; each point has a different coverage (3 banks, Big-4, all banks) - see 'observations'. Not a consistent monthly series: monthly values null.",
                                    "bank financial statements / Government statements via press", "per observation", "search excerpts", "fiscal",
                                    freq="points", observations=TREASURY,
                                    ldr_inclusion={"dates": ins["treasury_deposits_ldr"]["series"]["dates"], "values_pct": ins["treasury_deposits_ldr"]["series"]["values"],
                                                   "note": "Share of Treasury term deposits that may count as LDR funding (Policy tab): 50% (2023) -> 40% -> 20% -> 0% (2026-01) -> 20% (15/5/2026) -> 50% (1/8/2026)."})
    ex9 = eco["ECONFLOW"]["budget"]["exec_9M2026"]
    bud_obs = [
        {"date": "2026-03", "period": "Q1-2026 cumulative", "value": 299300, "revenue": 829400, "expenditure": 530100,
         "url": "https://thitruongtaichinhtiente.vn/thu-ngan-sach-nha-nuoc-quy-i-2026-dat-829-nghin-ty-dong-tang-11-4-so-voi-cung-ky-80853.html",
         "note": "revenue 829.4 tn (32.8% of plan) - spending 530.1 tn (16.8%) = +299.3 tn (derived), MoF estimate, cash basis; search excerpt 2026-10-07"},
        {"date": "2026-08", "period": "Jan-Aug 2026 cumulative", "value": 415500, "revenue": 2023800, "expenditure": 1608300,
         "url": "https://thoibaotaichinhvietnam.vn/infographics-thu-chi-ngan-sach-nha-nuoc-8-thang-nam-2026-203305.html",
         "note": "revenue 2,023.8 tn (80% of plan) - spending 1,608.3 tn (50.9%) = +415.5 tn (derived), MoF estimate, cash basis; search excerpt 2026-10-07"},
        {"date": "2026-09", "period": "Jan-Sep 2026 cumulative", "value": ex9["balance"], "revenue": ex9["revenue_total"], "expenditure": ex9["expenditure_total"],
         "url": ex9["source"], "note": "copied from data/economy.json ECONFLOW.budget.exec_9M2026 (" + ex9["balance_note"] + ")"},
    ]
    bb = nulls()
    sep_rev, sep_exp = ex9["revenue_total"] - 2023800, ex9["expenditure_total"] - 1608300
    bb[IDX["2026-09"]] = sep_rev - sep_exp
    S["budget_balance_monthly"] = series("Cân đối NSNN theo tháng", "Monthly budget balance", "VND bn", bb,
                                         "Revenue minus spending in the month, MoF cash-basis estimates (not the NA-defined deficit). Only Sep-2026 is filled: derived as the difference of two cumulative MoF estimates "
                                         f"(9M − 8M: revenue {sep_rev:,} − spending {sep_exp:,} = {sep_rev - sep_exp:,} bn); cumulative estimates are revised between releases, so the month value is approximate. "
                                         "Other months null (no consistent monthly series in reach). Cumulative year-to-date balances in 'observations' (Q1, 8M, 9M); 9M = economy.json exec_9M2026.",
                                         "Ministry of Finance budget execution via press; data/economy.json", "per observation", "search excerpts 2026-10-07 / Economy tab", "fiscal",
                                         observations=bud_obs, derived_months=["2026-09"])

    # ---------------- external & inflation
    cpi = nulls(); cpi_src = [None] * N
    for m, (v, u) in CPI_EXCERPT.items():
        cpi[IDX[m]] = v; cpi_src[IDX[m]] = u
    eco_months = [tlabel_to_month(t) for t in eco["CPI_DETAIL"].keys()]
    for m, v in zip(eco_months, eco["CPI_YOY_24"]):
        cpi[IDX[m]] = v; cpi_src[IDX[m]] = "data/economy.json CPI_YOY_24"
    S["cpi_yoy"] = series("CPI so với cùng kỳ", "CPI y/y", "%", cpi,
                          "NSO headline CPI vs same month of previous year. Oct-2024+ copied from data/economy.json (CPI_YOY_24). Earlier months only where a search excerpt confirmed the NSO figure; the rest are null (not recalled or interpolated).",
                          "NSO monthly CPI releases", "per month in 'per_month'", "search excerpts 2026-10-06 / Economy tab", "external", per_month=cpi_src)
    core = nulls()
    core[IDX["2026-09"]] = eco["CPI_LATEST"].get("core_yoy")
    S["core_cpi_yoy"] = series("Lạm phát cơ bản so với cùng kỳ", "Core inflation y/y", "%", core,
                               "NSO core inflation y/y. Only Sep-2026 (4.45%) in the Economy tab; other months not collected (null). 9M-2026 average core = 4.26% (invest_macro.json).",
                               "data/economy.json CPI_LATEST.core_yoy", eco["CPI_LATEST"]["source"]["url"], "Economy tab", "external")
    um = why["usdvnd_monthly"]
    fx = from_fin_monthly(um["central"], [tlabel_to_month(t) for t in um["months"]])
    for y in range(2020, 2024):
        if f"{y}-12" in IDX:
            fx[IDX[f"{y}-12"]] = alt["usd"]["level"][alt["years"].index(y)]
    S["usdvnd_central"] = series("Tỷ giá trung tâm USD/VND", "USD/VND central rate", "VND per USD", fx,
                                 "SBV central rate at/near month-end. Oct-2024+ from finance.json (usdvnd_monthly); Dec-2021..Dec-2023 year-end values from finance.json alternatives.usd. Other months null.",
                                 "finance.json why.series.usdvnd_monthly / alternatives.usd", "per finance.json URLs", "per finance.json", "external")
    S["usdvnd_12m"] = series("Tỷ giá trung tâm – thay đổi 12 tháng", "USD/VND central rate, 12-month change", "%", pct_change(fx, 12),
                             "Derived; positive = VND weaker.", "derived", "derived", "derived", "external", derived=True)
    dx = why["dxy_month_end"]
    S["dxy"] = series("Chỉ số USD (DXY)", "US dollar index (DXY)", "index", from_fin_monthly(dx["values"], [tlabel_to_month(t) for t in dx["months"]]),
                      "DXY as quoted in Vietnamese morning FX articles near month-end (finance.json). Before Oct-2024 not collected: null.",
                      "finance.json why.series.dxy_month_end", "per finance.json URLs", "per finance.json", "external")
    br, br_pre = ext_monthly("brent", args.brent_csv)
    S["brent"] = series("Dầu Brent (USD/thùng, TB tháng)", "Brent crude (USD/bbl, monthly average)", "USD/bbl", br,
                        "EIA Europe Brent spot FOB, monthly average, via datahub core/oil-prices.", "EIA via https://github.com/datasets/oil-prices",
                        "https://raw.githubusercontent.com/datasets/oil-prices/main/data/brent-monthly.csv", "downloaded 2026-10-06", "external", pre_2021=br_pre)
    S["real_dep_rate"] = series("Lãi suất tiền gửi thực (VCB 12T − CPI)", "Real deposit rate (VCB 12M − CPI y/y)", "pp",
                                diff(dep, cpi), "Derived: dep12_vcb − cpi_yoy (ex-post, same month).", "derived", "derived", "derived", "bank", derived=True)
    S["cash_to_m2"] = series("Tiền mặt/M2", "Cash in circulation / M2", "%", from_fin_monthly(fm["cash_to_m2_pct"]),
                             "SBV ratio of cash outside banks to M2.", "finance.json FINSYS.monthly.cash_to_m2_pct", "SBV statistics via finance.json", "per finance.json", "credit")
    for k, lv, le in [("cof", "Chi phí vốn (NH niêm yết)", "Cost of funds, listed banks"), ("casa", "Tỷ lệ CASA", "CASA ratio"), ("nim", "NIM (NH niêm yết)", "NIM, listed banks")]:
        S[k + "_listed"] = {"label_vi": lv, "label_en": le, "unit": why[k]["unit"], "freq": "quarterly", "group": "bank",
                            "periods": why[k]["periods"], "values": why[k]["values"], "definition": why[k]["note"],
                            "source": f"finance.json why.series.{k}", "url_or_note": "; ".join(x for x in why[k]["src"] if x), "verified": "per finance.json"}
    S["credit_target"] = {"label_vi": "Chỉ tiêu tăng trưởng tín dụng", "label_en": "Credit growth target", "unit": "%", "freq": "annual", "group": "credit",
                          "periods": ["2023", "2024", "2025", "2026"], "values": ["14-15", 15, 16, 15],
                          "definition": "SBV orientation for the year (2025 later raised for individual banks).", "source": "data/policy.json POLICY.mon.instruments[credit_growth_target]",
                          "url_or_note": "see policy.json history URLs", "verified": "Policy tab"}

    order_groups = ["policy", "bank", "credit", "savings", "fiscal", "external"]
    return {"months": MONTHS, "groups": order_groups, "series": S}


# --------------------------------------------------------------------------------------------
# Model
# --------------------------------------------------------------------------------------------
def v(P, k):
    return P["series"][k]["values"]


def at(P, k, m):
    return v(P, k)[IDX[m]]


def lagged(a, k):
    return [None] * k + a[:-k] if k else list(a)


def dif(a):
    return [None] + [None if a[i] is None or a[i - 1] is None else round(a[i] - a[i - 1], 4) for i in range(1, len(a))]


def mean(xs):
    xs = [x for x in xs if x is not None]
    return sum(xs) / len(xs) if xs else None


def ols(y, X, names, mask=None):
    rows = [i for i in range(N) if y[i] is not None and all(x[i] is not None for x in X) and (mask is None or mask[i])]
    Y = np.array([y[i] for i in rows], float)
    Xm = np.column_stack([np.ones(len(rows))] + [np.array([x[i] for i in rows], float) for x in X])
    b = np.linalg.lstsq(Xm, Y, rcond=None)[0]
    e = Y - Xm @ b
    n, k = Xm.shape
    s2 = float(e @ e / (n - k))
    se = np.sqrt(np.diag(s2 * np.linalg.pinv(Xm.T @ Xm)))
    tss = float((Y - Y.mean()) @ (Y - Y.mean()))
    return {"coef": {nm: round(float(c), 5) for nm, c in zip(["const"] + names, b)},
            "se": {nm: round(float(c), 5) for nm, c in zip(["const"] + names, se)},
            "r2": round(1 - float(e @ e) / tss, 3) if tss > 0 else None,
            "resid_sd": round(math.sqrt(s2), 4), "n": n,
            "sample": f"{MONTHS[rows[0]]}..{MONTHS[rows[-1]]}", "rows": rows, "b": b}


def strip(r):
    return {k: r[k] for k in ("coef", "se", "r2", "resid_sd", "n", "sample")}


def estimation_attempts(P):
    dep = v(P, "dep12_vcb")
    D = dif(dep)
    gold, vni, cpi, fg, refi, fed = (v(P, k) for k in ("gold_world_12m", "vnindex_12m", "cpi_yoy", "fed_minus_refi", "refi_rate", "fed_funds_upper"))
    real_gap = [None if c is None or d is None else c - d for c, d in zip(cpi, dep)]
    full = ols(D, [lagged(gold, 1), lagged(vni, 1), lagged(real_gap, 1), lagged(fg, 1)], ["gold12_l1", "vni12_l1", "cpi_minus_dep_l1", "fed_minus_refi_l1"])
    core = ols(D, [lagged(gold, 1), lagged(vni, 1)], ["gold12_l1", "vni12_l1"])
    early = ols(D, [lagged(gold, 1), lagged(vni, 1)], ["gold12_l1", "vni12_l1"], [m <= "2025-03" for m in MONTHS])
    level = ols(dep, [refi, fed, lagged(cpi, 1)], ["refi", "fed_upper", "cpi_l1"])
    # out-of-sample: fit to 2025-03, dynamic forecast 2025-04..09 with actual drivers
    b = early["b"]
    path, r = [], dep[IDX["2025-03"]]
    for m in ["2025-04", "2025-05", "2025-06", "2025-07", "2025-08", "2025-09"]:
        i = IDX[m]
        r = r + b[0] + b[1] * gold[i - 1] + b[2] * vni[i - 1]
        path.append({"month": m, "predicted": round(r, 2), "actual": dep[i]})
    rmse = math.sqrt(mean([(x["predicted"] - x["actual"]) ** 2 for x in path]))
    # annual cross-check of the funding-gap effect controlling for policy-rate changes
    ag = P["series"]["annual_growth"]
    yrs = [2021, 2022, 2023, 2024, 2025]
    jan = {2021: dep[IDX["2021-01"]], 2022: dep[IDX["2022-01"]], 2023: dep[IDX["2023-01"]], 2024: dep[IDX["2024-01"]], 2025: dep[IDX["2025-01"]], 2026: dep[IDX["2026-01"]]}
    jr = {y: refi[IDX[f"{y}-01"]] for y in range(2021, 2027)}
    yA = np.array([jan[y + 1] - jan[y] for y in yrs])
    gapA = np.array([ag["gap"][ag["periods"].index(y)] for y in yrs])
    drA = np.array([jr[y + 1] - jr[y] for y in yrs])
    XA = np.column_stack([np.ones(5), drA, gapA])
    bA = np.linalg.lstsq(XA, yA, rcond=None)[0]
    eA = yA - XA @ bA
    seA = np.sqrt(np.diag(float(eA @ eA / 2) * np.linalg.inv(XA.T @ XA)))
    # regression-only projection: core_change equation, drivers held at latest values
    bc = core["b"]
    g_now, v_now = gold[IDX["2026-09"]], vni[IDX["2026-09"]]
    r, reg_path = dep[IDX["2026-09"]], []
    for _ in PROJ_MONTHS:
        r = r + bc[0] + bc[1] * g_now + bc[2] * v_now
        reg_path.append(round(r, 2))
    ep = [i for i in range(IDX["2025-10"], IDX["2026-09"] + 1)]
    pred_ep = sum(bc[0] + bc[1] * gold[i - 1] + bc[2] * vni[i - 1] for i in ep)
    actual_ep = dep[IDX["2026-09"]] - dep[IDX["2025-09"]]
    share_opp = pred_ep / actual_ep if actual_ep else None
    nz = sum(1 for x in D if x not in (None, 0, 0.0))
    npairs = sum(1 for x in D if x is not None)
    nobs = sum(1 for x in dep if x is not None)
    c, e_ = core["coef"], early["coef"]
    return {
        "verdict_en": (f"Too thin to be the forecasting model. The deposit-rate series (Vietcombank 12-month posted rate) has {nobs} observed months, {npairs} consecutive month pairs and "
                       f"only {nz} non-zero monthly changes; the 2022 tightening months are missing, no policy-rate change falls inside the change sample, and the credit-deposit gap "
                       f"exists for only {sum(1 for x in v(P, 'credit_deposit_gap_yoy') if x is not None)} months, so neither can be estimated. What can be estimated - gold and VN-Index "
                       f"12-month returns - has the expected sign (full sample {c['gold12_l1']} and {c['vni12_l1']} pp per month per pp; fitted to 2025-03 {e_['gold12_l1']} and {e_['vni12_l1']}, "
                       f"less precise) and passes the 2025-04..09 backtest (RMSE {round(rmse, 3)} pp), but that window is flat, so it tests no turning point. Fed-minus-refinancing has the wrong "
                       f"sign, and the annual funding-gap check has the wrong sign once policy-rate changes are controlled for (2 degrees of freedom). The gold+stocks equation alone accounts for "
                       f"{round(100 * share_opp)}% of the Sep-2025..Sep-2026 rise, while analysts rank the credit-deposit gap first: the 2026 episode cannot separate the two because they rose together. "
                       f"The projection therefore uses a calibrated scenario model with an explicit, adjustable attribution; the regression-only path is reported alongside."),
        "verdict_vi": (f"Quá mỏng để làm mô hình dự báo. Chuỗi lãi suất (12 tháng niêm yết của Vietcombank) có {nobs} tháng quan sát, {npairs} cặp tháng liền nhau và chỉ {nz} lần thay đổi; "
                       f"thiếu các tháng thắt chặt 2022, không có lần đổi lãi suất điều hành nào trong mẫu thay đổi, và chênh lệch tín dụng–huy động chỉ có "
                       f"{sum(1 for x in v(P, 'credit_deposit_gap_yoy') if x is not None)} tháng, nên không ước lượng được hai biến này. Phần ước lượng được - lợi suất 12 tháng của vàng và VN-Index - "
                       f"đúng dấu (toàn mẫu {c['gold12_l1']} và {c['vni12_l1']} điểm %/tháng cho mỗi điểm %; mẫu đến 3/2025 {e_['gold12_l1']} và {e_['vni12_l1']}, kém chính xác hơn) và qua được "
                       f"kiểm định 4–9/2025 (RMSE {round(rmse, 3)} điểm %), nhưng giai đoạn đó đi ngang nên không kiểm được điểm đảo chiều. Chênh lệch Fed − tái cấp vốn sai dấu; kiểm tra theo năm "
                       f"cho chênh lệch vốn cũng sai dấu khi đã tính thay đổi lãi suất điều hành. Riêng phương trình vàng + cổ phiếu giải thích {round(100 * share_opp)}% mức tăng 9/2025–9/2026, trong khi "
                       f"giới phân tích xếp chênh lệch tín dụng–huy động hàng đầu: giai đoạn 2026 không tách được hai yếu tố vì chúng tăng cùng lúc. Vì vậy dự phóng dùng mô hình kịch bản hiệu chỉnh "
                       f"với tỷ lệ phân bổ công khai, có thể điều chỉnh; đường dự phóng chỉ theo hồi quy được báo cáo kèm."),
        "regression_only_projection": {"equation": "core_change", "drivers_held": {"gold_world_12m": g_now, "vnindex_12m": v_now}, "months": PROJ_MONTHS, "path": reg_path,
                                       "note": "What the estimable part alone implies: with gold and stock 12-month returns below their 2024-25 levels, it projects easing. It omits the funding gap and inflation."},
        "regression_attribution_2026": {"opportunity_cost_share": round(share_opp, 3), "predicted_rise_pp": round(pred_ep, 3), "actual_rise_pp": round(actual_ep, 3)},
        "regressions": [
            {"id": "full_change", "dep_var": "Δ dep12_vcb (pp, month)", **strip(full), "note": "fed_minus_refi wrong sign (expected +); cpi_minus_dep ~0"},
            {"id": "core_change", "dep_var": "Δ dep12_vcb (pp, month)", **strip(core), "note": "signs as expected, but driven by 2026"},
            {"id": "core_change_to_2025_03", "dep_var": "Δ dep12_vcb (pp, month)", **strip(early), "note": "same equation fitted to 2025-03: coefficients ~0"},
            {"id": "level", "dep_var": "dep12_vcb (level)", **strip(level), "note": "refinancing pass-through positive; Fed coefficient negative (wrong sign); residuals autocorrelated (levels), so SEs are understated"},
            {"id": "annual_gap_check", "dep_var": "Δ VCB 12M rate, January to January (pp)", "coef": {"const": round(float(bA[0]), 3), "d_refi": round(float(bA[1]), 3), "credit_minus_deposit_gap": round(float(bA[2]), 3)},
             "se": {"const": round(float(seA[0]), 3), "d_refi": round(float(seA[1]), 3), "credit_minus_deposit_gap": round(float(seA[2]), 3)}, "n": 5, "sample": "2021..2025 (annual)",
             "note": "gap from finance.json annual (IMF-FSI deposit basis); 2 degrees of freedom - indicative only; gap coefficient wrong sign"},
        ],
        "backtest_regression": {"fit_to": "2025-03", "equation": "core_change_to_2025_03", "kind": "true out-of-sample, dynamic, actual drivers",
                                "path": path, "rmse": round(rmse, 3),
                                "note": "Actual rate was flat at 4.6%; the early-sample equation (coefficients ~0) also predicts a near-flat path, i.e. the backtest is passed trivially and says nothing about turning points."},
    }


SHARES = {"credit_deposit_gap": 0.5, "cpi_yoy": 0.25, "gold_world_12m": 0.125, "vnindex_12m": 0.125}


def calibrate(P):
    dep, refi = v(P, "dep12_vcb"), v(P, "refi_rate")
    # (1) policy pass-through: 2022 tightening, January to January
    pass_through = (dep[IDX["2023-01"]] - dep[IDX["2022-01"]]) / (refi[IDX["2023-01"]] - refi[IDX["2022-01"]])
    pass_2023 = (dep[IDX["2024-01"]] - dep[IDX["2023-01"]]) / (refi[IDX["2024-01"]] - refi[IDX["2023-01"]])
    # (2) neutral levels: months when the posted rate was flat at 4.6% (2024-04..2025-09)
    flat = [m for m in MONTHS if "2024-04" <= m <= "2025-09"]
    neutral = {"cpi_yoy": mean(at(P, "cpi_yoy", m) for m in flat),
               "gold_world_12m": mean(at(P, "gold_world_12m", m) for m in flat),
               "vnindex_12m": mean(at(P, "vnindex_12m", m) for m in flat),
               "credit_deposit_gap": 4.0}
    gap_refs = {"annual_2024_gap_imf_basis": P["series"]["annual_growth"]["gap"][P["series"]["annual_growth"]["periods"].index(2024)],
                "ytd_gap_2025_09_sbv_basis": round(at(P, "credit_ytd", "2025-09") - at(P, "deposit_ytd", "2025-09"), 2)}
    # (3) the 2026 episode: Sep-2025 -> Sep-2026, refinancing unchanged
    ep = [m for m in MONTHS if "2025-10" <= m <= "2026-09"]
    rise = dep[IDX["2026-09"]] - dep[IDX["2025-09"]]
    per_month = rise / 12
    excess = {"credit_deposit_gap": mean(at(P, "credit_deposit_gap_yoy", m) for m in ep) - neutral["credit_deposit_gap"],
              "cpi_yoy": mean(at(P, "cpi_yoy", m) for m in ep) - neutral["cpi_yoy"],
              "gold_world_12m": mean(at(P, "gold_world_12m", m) for m in ep) - neutral["gold_world_12m"],
              "vnindex_12m": mean(at(P, "vnindex_12m", m) for m in ep) - neutral["vnindex_12m"]}
    coef = {k: per_month * SHARES[k] / excess[k] for k in SHARES}
    resid_changes = [x for x in dif(dep) if x is not None]
    sd = float(np.std(resid_changes, ddof=1))
    return {"pass_through": pass_through, "pass_2023": pass_2023, "neutral": neutral, "gap_refs": gap_refs, "rise": rise,
            "per_month": per_month, "excess": excess, "coef": coef, "sd": sd, "n_changes": len(resid_changes),
            "episode_months": ep, "flat_months": flat}


DRIVER_META = {
    "credit_deposit_gap": ("Chênh lệch tăng trưởng tín dụng − huy động (y/y)", "Credit minus deposit growth gap (y/y)", "pp", "credit_deposit_gap_yoy"),
    "cpi_yoy": ("Lạm phát CPI (y/y)", "CPI inflation (y/y)", "%", "cpi_yoy"),
    "gold_world_12m": ("Vàng thế giới, lợi suất 12 tháng", "World gold, 12-month return", "%", "gold_world_12m"),
    "vnindex_12m": ("VN-Index, lợi suất 12 tháng", "VN-Index, 12-month return", "%", "vnindex_12m"),
}


def latest(P, key):
    vals = v(P, key)
    for i in range(N - 1, -1, -1):
        if vals[i] is not None:
            return vals[i], MONTHS[i]
    return None, None


def run_path(r0, policy_path, driver_paths, cal):
    """driver_paths[k][h] = driver level in month h-1 (lag 1); policy_path[h] = policy change in month h."""
    out, r = [], r0
    for h in range(len(PROJ_MONTHS)):
        r = r + cal["pass_through"] * policy_path[h] + sum(cal["coef"][k] * (driver_paths[k][h] - cal["neutral"][k]) for k in SHARES)
        out.append(round(r, 2))
    return out


def lin(a, b, n=6):
    return [round(a + (b - a) * (h + 1) / n, 3) for h in range(n)]


def project(P, cal, est):
    r0 = at(P, "dep12_vcb", "2026-09")
    so = est["regression_attribution_2026"]["opportunity_cost_share"]
    rc = {r["id"]: r for r in est["regressions"]}["core_change"]["coef"]
    wg = rc["gold12_l1"] * cal["excess"]["gold_world_12m"]
    wv = rc["vni12_l1"] * cal["excess"]["vnindex_12m"]
    rest = max(0.0, 1 - min(so, 1.0))
    reg_shares = {"gold_world_12m": round(min(so, 1.0) * wg / (wg + wv), 3), "vnindex_12m": round(min(so, 1.0) * wv / (wg + wv), 3),
                  "credit_deposit_gap": round(rest / 2, 3), "cpi_yoy": round(rest / 2, 3)}
    cur = {k: latest(P, DRIVER_META[k][3]) for k in SHARES}
    hold = {k: [cur[k][0]] * 6 for k in SHARES}
    zero = [0.0] * 6
    base = run_path(r0, zero, hold, cal)
    z = 1.2816
    low = [round(b - z * cal["sd"] * math.sqrt(h + 1), 2) for h, b in enumerate(base)]
    high = [round(b + z * cal["sd"] * math.sqrt(h + 1), 2) for h, b in enumerate(base)]
    # driver paths are the lagged inputs: month h uses the level reached at h-1
    cut_dr = {"credit_deposit_gap": [cur["credit_deposit_gap"][0]] + lin(cur["credit_deposit_gap"][0], 4.0)[:5],
              "cpi_yoy": [cur["cpi_yoy"][0]] + lin(cur["cpi_yoy"][0], 4.0)[:5],
              "gold_world_12m": [cur["gold_world_12m"][0]] + lin(cur["gold_world_12m"][0], 0.0)[:5],
              "vnindex_12m": hold["vnindex_12m"]}
    cut_pol = [0, 0, 0, -0.5, 0, 0]
    tight_dr = {"credit_deposit_gap": [cur["credit_deposit_gap"][0]] + lin(cur["credit_deposit_gap"][0], 8.0)[:5],
                "cpi_yoy": [cur["cpi_yoy"][0]] + lin(cur["cpi_yoy"][0], 5.5)[:5],
                "gold_world_12m": [cur["gold_world_12m"][0]] + lin(cur["gold_world_12m"][0], 30.0)[:5],
                "vnindex_12m": [cur["vnindex_12m"][0]] + lin(cur["vnindex_12m"][0], 15.0)[:5]}
    tight_pol = [0, 0, 0.5, 0, 0, 0]
    scen = {
        "rates_cut": {"label_vi": "Nới lỏng / giảm áp lực", "label_en": "Easing / pressure relief",
                      "assumptions_vi": "NHNN giảm lãi suất điều hành 0,5 điểm % trong 1/2027; chênh lệch tín dụng–huy động thu hẹp dần về 4 điểm % đến 3/2027 (trần LDR 95% từ 1/12/2026, 50% tiền gửi KBNN được tính vào LDR); CPI giảm dần về 4%; lợi suất 12 tháng của vàng về 0%; VN-Index giữ nguyên.",
                      "assumptions_en": "SBV cuts policy rates by 0.5 pp in Jan-2027; the credit-deposit gap narrows steadily to 4 pp by Mar-2027 (95% LDR cap from 1 Dec 2026, 50% of Treasury deposits counted in LDR); CPI eases to 4%; gold's 12-month return falls to 0%; VN-Index return unchanged.",
                      "policy_change": cut_pol, "drivers": cut_dr, "path": run_path(r0, cut_pol, cut_dr, cal)},
        "status_quo": {"label_vi": "Giữ nguyên", "label_en": "Status quo",
                       "assumptions_vi": "Mọi động lực giữ ở mức mới nhất: chênh lệch tín dụng–huy động, CPI, lợi suất 12 tháng của vàng và VN-Index; lãi suất điều hành không đổi.",
                       "assumptions_en": "All drivers held at their latest values (credit-deposit gap, CPI, gold and VN-Index 12-month returns); policy rates unchanged.",
                       "policy_change": zero, "drivers": hold, "path": base},
        "tighter": {"label_vi": "Thắt chặt hơn", "label_en": "Tighter",
                    "assumptions_vi": "NHNN nâng lãi suất điều hành 0,5 điểm % trong 12/2026 (ví dụ để giữ tỷ giá); chênh lệch tín dụng–huy động nới lên 8 điểm %; CPI lên 5,5%; lợi suất 12 tháng của vàng lên 30% và VN-Index lên 15%.",
                    "assumptions_en": "SBV raises policy rates by 0.5 pp in Dec-2026 (e.g. to defend the dong); the credit-deposit gap widens to 8 pp; CPI rises to 5.5%; gold's 12-month return rises to 30% and VN-Index's to 15%.",
                    "policy_change": tight_pol, "drivers": tight_dr, "path": run_path(r0, tight_pol, tight_dr, cal)},
    }
    # sensitivity to the attribution shares (the key judgement)
    sens = []
    for name, sh in [("gap_heavy", {"credit_deposit_gap": 0.8, "cpi_yoy": 0.1, "gold_world_12m": 0.05, "vnindex_12m": 0.05}),
                     ("inflation_heavy", {"credit_deposit_gap": 0.25, "cpi_yoy": 0.6, "gold_world_12m": 0.075, "vnindex_12m": 0.075}),
                     ("opportunity_cost_heavy", {"credit_deposit_gap": 0.25, "cpi_yoy": 0.15, "gold_world_12m": 0.3, "vnindex_12m": 0.3}),
                     ("regression_implied", reg_shares)]:
        c2 = dict(cal); c2["coef"] = {k: cal["per_month"] * sh[k] / cal["excess"][k] for k in SHARES}
        sens.append({"shares": sh, "id": name, "status_quo_path": run_path(r0, zero, hold, c2)})
    return {"months": PROJ_MONTHS, "start": {"month": "2026-09", "value": r0, "series": "dep12_vcb"},
            "base": base, "low": low, "high": high,
            "band": f"80% band = base ± 1.28 × {round(cal['sd'], 3)} pp × √h (s.d. of the {cal['n_changes']} observed monthly changes of the posted rate)",
            "scenarios": scen, "share_sensitivity": sens,
            "regression_only": est["regression_only_projection"],
            "attribution_note_en": "The direction of the next six months depends on how the 2026 rise is attributed: if mostly to the funding gap and inflation (default), rates keep edging up while those stay high; if mostly to gold and stock returns (regression-implied), rates ease as those returns cool. See share_sensitivity.",
            "attribution_note_vi": "Hướng đi 6 tháng tới phụ thuộc cách phân bổ mức tăng 2026: nếu chủ yếu do chênh lệch vốn và lạm phát (mặc định), lãi suất tiếp tục nhích lên khi các yếu tố này còn cao; nếu chủ yếu do lợi suất vàng và cổ phiếu (theo hồi quy), lãi suất hạ dần khi các lợi suất đó nguội đi. Xem share_sensitivity.", "drivers_now": {k: {"value": cur[k][0], "month": cur[k][1]} for k in SHARES},
            "kind": "dashboard model — not a forecast",
            "note_vi": "Mô hình của dashboard, không phải dự báo. Điểm xuất phát là lãi suất niêm yết 12 tháng của Vietcombank (thấp nhất nhóm Big4); ngân hàng tư nhân trả cao hơn khoảng 2,5 điểm % (MBS: 8,4% tháng 8/2026). Không phải khuyến nghị đầu tư.",
            "note_en": "Dashboard model, not a forecast. The start point is Vietcombank's posted 12-month rate (the lowest of the Big-4); private banks pay about 2.5 pp more (MBS: 8.4% in Aug-2026). Not investment advice."}


def model_block(P, cal, est):
    drivers = [{"key": "policy_change", "label_vi": "Thay đổi lãi suất điều hành (NHNN)", "label_en": "Change in SBV policy rate", "coef": round(cal["pass_through"], 3), "se": None,
                "sign_expected": "+", "lag": 0, "neutral": 0, "basis": "calibrated",
                "evidence": f"2022: VCB 12M {at(P,'dep12_vcb','2022-01')}% (Jan-22) -> {at(P,'dep12_vcb','2023-01')}% (Jan-23) while refinancing rose {at(P,'refi_rate','2022-01')} -> {at(P,'refi_rate','2023-01')}%: pass-through {round(cal['pass_through'],2)}. 2023 easing gave {round(cal['pass_2023'],2)} (cuts plus easing funding) - the lower value is used."}]
    for k in SHARES:
        lv, le, unit, sk = DRIVER_META[k]
        drivers.append({"key": k, "series": sk, "label_vi": lv, "label_en": le, "unit": unit, "coef": round(cal["coef"][k], 5), "se": None, "sign_expected": "+", "lag": 1,
                        "neutral": round(cal["neutral"][k], 2), "basis": "calibrated", "share_of_2026_rise": SHARES[k],
                        "evidence": f"2026 episode: mean {round(cal['excess'][k] + cal['neutral'][k], 2)} vs neutral {round(cal['neutral'][k], 2)} (excess {round(cal['excess'][k], 2)}); assigned {int(SHARES[k]*100)}% of the {round(cal['per_month'],4)} pp/month rise."})
    excluded = [
        {"key": "fed_minus_refi", "reason_en": "Wrong sign in every regression: the Fed cut through 2025 and early 2026 while Vietnamese deposit rates rose; Fed hikes in 2022-23 coincided with VN rate cuts in 2023.", "reason_vi": "Sai dấu trong mọi hồi quy: Fed giảm lãi suất suốt 2025 và đầu 2026 trong khi lãi suất huy động Việt Nam tăng; Fed tăng 2022-23 trùng lúc Việt Nam giảm 2023."},
        {"key": "real_dep_rate", "reason_en": "Coefficient ~0 and insignificant (inflation's effect is kept through cpi_yoy in the calibrated model).", "reason_vi": "Hệ số ~0, không có ý nghĩa (tác động lạm phát giữ qua cpi_yoy)."},
        {"key": "interbank_on", "reason_en": "Monthly series only from 2024-10 (earlier: dated points).", "reason_vi": "Chuỗi theo tháng chỉ từ 10/2024."},
        {"key": "ldr_sbv", "reason_en": "Sparse (12 months) and far from the cap on the SBV definition (77.1% vs 85% in Jun-2026); the simple LDR (~114%) is quarterly and listed-bank based.", "reason_vi": "Thưa (12 tháng) và còn xa trần theo định nghĩa NHNN (77,1% so với 85%, 6/2026); LDR đơn giản (~114%) theo quý."},
        {"key": "public_investment / treasury_deposits", "reason_en": "Annual or scattered points with changing coverage; enter only through the credit-deposit gap and the story.", "reason_vi": "Chỉ có số năm hoặc điểm rời rạc; đi vào mô hình gián tiếp qua chênh lệch tín dụng–huy động."},
    ]
    return {
        "method_en": ("Calibrated scenario model (fallback, because the regressions are not credible - see estimation). Monthly change of the Vietcombank 12-month posted rate = "
                      f"{round(cal['pass_through'],2)} × policy-rate change + Σ coefficient × (driver last month − neutral level). Neutral levels = averages over 2024-04..2025-09, "
                      "when the posted rate stayed at 4.6%. Coefficients scale the 2026 rise (Sep-2025 4.6% → Sep-2026 5.9%, refinancing unchanged) by an ASSUMED split: "
                      "50% funding gap, 25% inflation, 12.5% gold, 12.5% stocks; 'share_sensitivity' shows other splits."),
        "method_vi": ("Mô hình kịch bản hiệu chỉnh (phương án dự phòng vì hồi quy không đủ tin cậy - xem phần ước lượng). Thay đổi hằng tháng của lãi suất niêm yết 12 tháng Vietcombank = "
                      f"{round(cal['pass_through'],2).__str__().replace('.', ',')} × thay đổi lãi suất điều hành + Σ hệ số × (động lực tháng trước − mức trung tính). Mức trung tính = trung bình 4/2024–9/2025, "
                      "khi lãi suất niêm yết đứng ở 4,6%. Hệ số được suy ra từ mức tăng 2026 (4,6% tháng 9/2025 → 5,9% tháng 9/2026, lãi suất tái cấp vốn không đổi) theo tỷ lệ phân bổ GIẢ ĐỊNH: "
                      "50% chênh lệch vốn, 25% lạm phát, 12,5% vàng, 12,5% cổ phiếu; 'share_sensitivity' cho các tỷ lệ khác."),
        "equation": ("Δr_t = {p} × Δpolicy_t + {g} × (gap_(t−1) − {gn}) + {c} × (CPI_(t−1) − {cn}) + {o} × (gold12_(t−1) − {on}) + {s} × (VNI12_(t−1) − {sn})"
                     .format(p=round(cal["pass_through"], 3), g=round(cal["coef"]["credit_deposit_gap"], 4), gn=round(cal["neutral"]["credit_deposit_gap"], 2),
                             c=round(cal["coef"]["cpi_yoy"], 4), cn=round(cal["neutral"]["cpi_yoy"], 2), o=round(cal["coef"]["gold_world_12m"], 5),
                             on=round(cal["neutral"]["gold_world_12m"], 2), s=round(cal["coef"]["vnindex_12m"], 5), sn=round(cal["neutral"]["vnindex_12m"], 2))),
        "target": {"series": "dep12_vcb", "definition": "Vietcombank 12-month posted VND savings rate (counter), month-end"},
        "drivers": drivers, "excluded_drivers": excluded,
        "calibration": {"pass_through_2022": round(cal["pass_through"], 3), "pass_through_2023": round(cal["pass_2023"], 3),
                        "neutral_window": f"{cal['flat_months'][0]}..{cal['flat_months'][-1]}", "neutral": {k: round(x, 3) for k, x in cal["neutral"].items()},
                        "neutral_gap_note": f"No SBV-basis y/y gap exists for the flat window; 4.0 pp is an assumption between the 2024 annual gap {cal['gap_refs']['annual_2024_gap_imf_basis']} pp (IMF-FSI deposits) and the Sep-2025 YTD gap {cal['gap_refs']['ytd_gap_2025_09_sbv_basis']} pp (SBV, 9 months), both periods of flat rates.",
                        "episode_window": f"{cal['episode_months'][0]}..{cal['episode_months'][-1]}", "episode_rise_pp": round(cal["rise"], 2), "rise_per_month": round(cal["per_month"], 4),
                        "excess": {k: round(x, 3) for k, x in cal["excess"].items()}, "shares": SHARES, "shares_basis": "assumption (judgement), informed by the ranked drivers in finance.json why.drivers (rank 1 = credit-deposit gap)"},
        "fit": {"r2": None, "n": None, "sample": None, "note": "Calibrated, not fitted: no R². Diagnostics of the attempted regressions (R², n, sample, SEs) are in 'estimation'; the estimable core equation has R² %s on n = %s (%s)." % (
            {r["id"]: r for r in est["regressions"]}["core_change"]["r2"], {r["id"]: r for r in est["regressions"]}["core_change"]["n"], {r["id"]: r for r in est["regressions"]}["core_change"]["sample"])},
        "estimation": est,
    }


def backtest_block(P, cal, est):
    # in-sample replay of the calibrated model over Feb..Sep 2026 (NOT out-of-sample: calibrated on this window)
    dep = v(P, "dep12_vcb")
    path, r = [], dep[IDX["2026-01"]]
    for m in MONTHS[IDX["2026-02"]: IDX["2026-09"] + 1]:
        i = IDX[m]
        x = {}
        for k in SHARES:
            val = v(P, DRIVER_META[k][3])[i - 1]
            if val is None:  # gap for Aug-2026 not yet published: use last published value, flagged
                j = i - 1
                while v(P, DRIVER_META[k][3])[j] is None:
                    j -= 1
                val = v(P, DRIVER_META[k][3])[j]
            x[k] = val
        r = r + cal["pass_through"] * 0 + sum(cal["coef"][k] * (x[k] - cal["neutral"][k]) for k in SHARES)
        path.append({"month": m, "predicted": round(r, 2), "actual": dep[i]})
    rmse = math.sqrt(mean([(p["predicted"] - p["actual"]) ** 2 for p in path]))
    return {"regression_out_of_sample": est["backtest_regression"],
            "calibrated_in_sample_replay": {"start": {"month": "2026-01", "value": dep[IDX["2026-01"]]}, "path": path, "rmse": round(rmse, 3),
                                            "kind": "in-sample replay (the model was calibrated on 2025-10..2026-09) - shows fit, not forecasting skill",
                                            "note": "Sep-2026 uses the Jul-2026 credit-deposit gap (Aug not yet published)."},
            "summary_en": "No honest out-of-sample test of the calibrated model is possible: the only rate-rise episode with full driver data is the one used to calibrate it.",
            "summary_vi": "Không thể kiểm định ngoài mẫu một cách trung thực cho mô hình hiệu chỉnh: giai đoạn tăng lãi suất duy nhất có đủ dữ liệu động lực chính là giai đoạn dùng để hiệu chỉnh."}


def sliders_block(P, cal):
    out = [{"key": "policy_change", "label_vi": "Thay đổi lãi suất điều hành", "label_en": "Policy-rate change", "unit": "pp", "current": 0, "min": -1.0, "max": 1.0, "step": 0.25,
            "coef": round(cal["pass_through"], 4), "neutral": 0, "lag": 0, "applies": "once (level shift from the month of change)"}]
    rng = {"credit_deposit_gap": (0, 12, 0.5), "cpi_yoy": (2, 8, 0.1), "gold_world_12m": (-30, 80, 5), "vnindex_12m": (-40, 80, 5)}
    for k in SHARES:
        lv, le, unit, sk = DRIVER_META[k]
        cur, m = latest(P, sk)
        lo, hi, st = rng[k]
        out.append({"key": k, "series": sk, "label_vi": lv, "label_en": le, "unit": unit, "current": cur, "current_month": m, "min": lo, "max": hi, "step": st,
                    "coef": round(cal["coef"][k], 6), "neutral": round(cal["neutral"][k], 3), "lag": 1, "applies": "every month while held"})
    return {"items": out, "start_rate": {"value": at(P, "dep12_vcb", "2026-09"), "month": "2026-09", "series": "dep12_vcb"},
            "formula_en": "r(h) = start_rate + coef_policy × policy_change + h × Σ_k coef_k × (slider_k − neutral_k), h = 1..6 months; band ± 1.28 × sd × √h with sd = %s" % round(cal["sd"], 4),
            "formula_vi": "r(h) = lãi suất xuất phát + hệ số_điều hành × thay đổi lãi suất điều hành + h × Σ hệ số_k × (giá trị thanh trượt_k − mức trung tính_k), h = 1..6 tháng",
            "sd": round(cal["sd"], 4), "check": "With all sliders at 'current', the formula reproduces projection.base exactly."}


# ============================================================================================
# Model v2 - structural monthly VND funding model (main model from 2026-10-07; v1 kept for comparison)
# ============================================================================================
# Liquidity block:  FG_t (VND tn) = w_credit*credit - w_fx*S*netFX - w_fis*fiscal_injection - w_sbv*(SBV FX purchases + OMO net)
#                   weights estimated on annual 2016-2025 data (SBV credit, IMF-FSI deposits, BoP/NSO/customs FX, MoF budget).
# Pressure index:   P_t (pp of deposits) = sum of the last 12 months of 100*FG/D (D = deposits at the previous year-end).
# Rate equation:    dr_t = pi*dpolicy_t + kappa*(P_{t-1} - P*) + g_vni*(VNI12_{t-1} - n) + g_gold*(gold12_{t-1} - n)
#                          + calibrated add-ons (CPI, USD/VND, Fed change, SJC premium, real-estate prices),
#                   estimated by interval regression on the irregularly observed VCB 12-month rate.
# Everything the page needs to recompute a path is in SIM.model2 (no matrix algebra at run time).

V2_MONTHS = [f"{y}-{m:02d}" for y in range(2020, 2027) for m in range(1, 13)]
V2_MONTHS = V2_MONTHS[: V2_MONTHS.index("2026-09") + 1]
V2_IDX = {m: i for i, m in enumerate(V2_MONTHS)}
V2N = len(V2_MONTHS)

# Ministry of Finance budget execution, cumulative from January (VND tn = nghìn tỷ đồng), cash-basis estimates.
# rev = total revenue, exp = total spending, dev = development-investment spending, bal = stated balance when
# spending is not given. Search-engine excerpts 2026-10-07 (MoF/press pages blocked from the sandbox).
BUDGET_CUM = {
    "2025-01": {"rev": 276.6, "exp": 134.4, "url": "https://thoibaotaichinhvietnam.vn/infographics-thu-chi-ngan-sach-nha-nuoc-thang-12025-169885.html"},
    "2025-02": {"rev": 499.8, "url": "https://baodauthau.vn/thu-ngan-sach-nha-nuoc-2-thang-dau-nam-2025-dat-tren-254-du-toan-post175639.html", "note": "2M spending not found: Feb-Mar flows are a 2-month interval average"},
    "2025-03": {"rev": 721.3, "exp": 428.2, "dev": 78.7, "url": "https://thitruongtaichinhtiente.vn/quy-i-2025-ngan-sach-nha-nuoc-thang-du-293-nghin-ty-dong-67154.html"},
    "2025-04": {"rev": 944.1, "exp": 595.4, "url": "https://nhandan.vn/thu-ngan-sach-nha-nuoc-tang-hon-26-trong-4-thang-dau-nam-post877837.html"},
    "2025-05": {"rev": 1139.6, "exp": 833.8, "dev": 199.3, "url": "https://thitruongtaichinhtiente.vn/5-thang-dau-nam-ngan-sach-nha-nuoc-boi-thu-hon-305-nghin-ty-dong-68216.html"},
    "2025-06": {"rev": 1330.0, "exp": 1100.0, "dev": 268.1, "approx": True, "url": "https://mekongasean.vn/nua-dau-nam-2025-tong-thu-nsnn-tang-283-dat-khoang-133-trieu-ty-dong-43434.html", "note": "rounded: revenue 'khoảng 1,33 triệu tỷ', spending '1,1 triệu tỷ'"},
    "2025-07": {"rev": 1572.3, "url": "https://thoibaotaichinhvietnam.vn/infographics-thu-ngan-sach-nha-nuoc-7-thang-uoc-dat-1572300-ty-do-ng-181322.html", "note": "7M spending not found: Jul-Aug flows are a 2-month interval average"},
    "2025-08": {"rev": 1740.0, "bal": 289.7, "approx": True, "url": "https://vneconomy.vn/chi-thuong-xuyen-gap-24-lan-chi-dau-tu-phat-trien-trong-8-thang.htm", "note": "revenue 'gần 1.740 nghìn tỷ'; surplus 289.7 tn as stated (spending not stated)"},
    "2025-09": {"rev": 1901.6, "exp": 1591.3, "url": "https://thitruongtaichinhtiente.vn/9-thang-nam-2025-thu-ngan-sach-nha-nuoc-dat-96-7-du-toan-ca-nam-70887.html", "note": "CONFLICT: another excerpt gives 9M spending ~1,625 tn (63.1% of plan, +30.6%); 1,591.3 (61.7%) used; revenue also quoted as 'gần 1.888' in an earlier release"},
    "2025-10": {"rev": 2145.0, "exp": 1830.8, "dev": 486.1, "url": "https://doanhnhan.baophapluat.vn/kinh-te-10-thang-2025-fdi-thuc-hien-cao-nhat-5-nam-iip-tang-92-chi-ngan-sach-tang-471-88465.html"},
    "2025-11": {"rev": 2397.7, "exp": 2049.7, "dev": 553.3, "url": "https://thoibaotaichinhvietnam.vn/infographics-thu-ngan-sach-nha-nuoc-11-thang-uoc-dat-2397700-ty-do-ng-188622.html"},
    "2025-12": {"rev": 2650.1, "exp": 2401.5, "dev": 732.0, "url": "data/economy.json ECONFLOW.budget.mof_execution_2025_jan2026"},
    "2026-01": {"rev": 370.7, "exp": 163.0, "url": "https://mekongasean.vn/thu-ngan-sach-nha-nuoc-thang-12026-uoc-dat-3707-nghin-ty-dong-51682.html"},
    "2026-02": {"rev": 601.3, "exp": 311.0, "url": "https://thoibaotaichinhvietnam.vn/thu-ngan-sach-2-thang-dat-238-du-toan-192621.html"},
    "2026-03": {"rev": 829.4, "exp": 530.1, "dev": 116.1, "url": "https://daibieunhandan.vn/thu-ngan-sach-nha-nuoc-quy-i-2026-dat-hon-829-nghin-ty-dong-10412177.html", "note": "CONFLICT: an earlier release quotes Q1 revenue ~820 tn (thoibaotaichinhvietnam); 829.4 used (later estimate)"},
    "2026-04": {"rev": 1114.0, "exp": 668.2, "dev": 153.2, "url": "https://thoibaotaichinhvietnam.vn/infographics-thu-chi-ngan-sach-nha-nuoc-4-thang-dau-nam-2026-196849.html", "note": "CONFLICT: another excerpt cites development spending 191.1 tn for 4M; 153.2 (MoF infographic) used"},
    "2026-05": {"rev": 1339.7, "exp": 845.4, "url": "https://thitruongtaichinhtiente.vn/5-thang-dau-nam-2026-thu-ngan-sach-nha-nuoc-dat-53-du-toan-tang-15-4-so-voi-cung-ky-83359.html"},
    "2026-06": {"rev": 1568.2, "exp": 1149.1, "url": "https://mekongasean.vn/thu-ngan-sach-6-thang-dat-62-du-toan-nam-2026-56934.html"},
    "2026-07": {"rev": 1833.0, "exp": 1364.3, "dev": 418.9, "url": "https://www.vietnamplus.vn/thu-ngan-sach-trong-bay-thang-dat-tren-1834-nghin-ty-dong-bang-725-du-toan-post1127807.vnp"},
    "2026-08": {"rev": 2023.8, "exp": 1608.3, "dev": 514.6, "url": "https://www.vietnamplus.vn/thu-ngan-sach-nha-nuoc-8-thang-uoc-dat-80-du-toan-post1133887.vnp"},
    "2026-09": {"rev": 2187.3, "exp": 1873.7, "dev": 657.5, "url": "data/economy.json ECONFLOW.budget.exec_9M2026"},
}
# NSO: FDI disbursed (vốn FDI thực hiện), cumulative from January, USD bn.
FDI_CUM = {
    "2025-01": (1.51, "https://thitruongtaichinhtiente.vn/viet-nam-thu-hut-hon-4-3-ty-usd-von-fdi-trong-thang-dau-nam-2025-65571.html"),
    "2025-02": (2.95, "https://vneconomy.vn/vietnam-attracts-nearly-7-bln-in-fdi-in-first-two-months.htm"),
    "2025-05": (8.90, "https://thesaigontimes.vn/saigontimes/giai-ngan-von-fdi-5-thang-dat-89-ti-do-la-cao-nhat-5-nam-qua/"),
    "2025-07": (13.6, "https://doanhnhan.baophapluat.vn/von-fdi-thuc-hien-cao-nhat-cua-7-thang-trong-5-nam-qua-85116.html"),
    "2025-08": (15.4, "https://vnbusiness.vn/kinh-te-8-thang-fdi-thuc-hien-cao-nhat-trong-5-nam-hon-209-nghin-doanh-nghiep-thanh-lap-moi.html"),
    "2025-09": (18.80, "https://thitruongtaichinhtiente.vn/von-fdi-thuc-hien-9-thang-nam-2025-dat-muc-cao-nhat-trong-5-nam-qua-70916.html"),
    "2025-10": (21.3, "https://doanhnhan.baophapluat.vn/kinh-te-10-thang-2025-fdi-thuc-hien-cao-nhat-5-nam-iip-tang-92-chi-ngan-sach-tang-471-88465.html"),
    "2025-11": (23.6, "https://thitruongtaichinhtiente.vn/von-fdi-thuc-hien-9-thang-nam-2025-dat-muc-cao-nhat-trong-5-nam-qua-70916.html"),
    "2025-12": (27.62, "data/economy.json ECON_OFFICIAL.fdi_dis_musd"),
    "2026-01": (1.68, "https://baolaocai.vn/fdi-tang-toc-dau-nam-thang-12026-ghi-nhan-muc-giai-ngan-ky-luc-trong-5-nam-post893792.html"),
    "2026-02": (3.21, "https://tapchikinhtetaichinh.vn/von-fdi-giai-ngan-2-thang-dau-nam-tiep-tuc-lap-dinh-cao-nhat-trong-5-nam-qua-150206.html"),
    "2026-03": (5.41, "https://thitruongtaichinhtiente.vn/von-fdi-thuc-hien-quy-i-2026-lap-dinh-5-nam-dau-tu-ra-nuoc-ngoai-tiep-da-but-pha-80895.html"),
    "2026-04": (7.40, "https://thanhtra.com.vn/dau-tu-72A9E3223/fdi-thuc-hien-4-thang-dau-nam-2026-dat-74-ty-usd-tang-98-49ad30925.html"),
    "2026-05": (9.75, "https://tapchikinhtetaichinh.vn/von-dau-tu-nuoc-ngoai-thuc-hien-5-thang-giu-da-tang-truong-manh-tiep-tuc-dan-dau-trong-5-nam-157937.html"),
    "2026-06": (13.03, "https://thoibaotaichinhvietnam.vn/von-fdi-thuc-hien-6-thang-dau-nam-cao-nhat-trong-5-nam-qua-200091.html"),
    "2026-07": (15.2, "https://mekongasean.vn/von-fdi-thuc-hien-tai-viet-nam-7-thang-dat-152-ty-usd-58025.html"),
    "2026-08": (17.25, "https://thitruongtaichinhtiente.vn/8-thang-nam-2026-von-dau-tu-cong-tang-18-5-fdi-thuc-hien-dat-17-25-ty-usd-85291.html"),
    "2026-09": (21.07, "data/economy.json ECON_OFFICIAL.ytd_2026.fdi_dis_musd_9M"),
}
# Customs/NSO goods trade balance, cumulative from January, USD bn (+ = surplus).
TRADE_CUM = {
    "2025-03": (3.16, "https://baodauthau.vn/quy-i2025-tong-kim-ngach-xuat-nhap-khau-uoc-dat-hon-202-ty-usd-post176838.html"),
    "2025-04": (3.79, "https://cafeland.vn/tin-tuc/xuat-nhap-khau-4-thang-2025-duy-tri-da-tang-xuat-sieu-379-ty-usd-137912.html"),
    "2025-05": (4.67, "https://baodauthau.vn/5-thang-dau-nam-2025-can-can-thuong-mai-hang-hoa-xuat-sieu-467-ty-usd-post180012.html"),
    "2025-06": (7.63, "https://baodauthau.vn/xuat-sieu-uoc-dat-763-ty-usd-trong-nua-dau-nam-2025-post181191.html"),
    "2025-07": (10.18, "https://baodauthau.vn/7-thang-nam-2025-viet-nam-xuat-sieu-1018-ty-usd-post182845.html"),
    "2025-08": (13.99, "https://www.tinnhanhchungkhoan.vn/can-can-thuong-mai-hang-hoa-xuat-sieu-1399-ty-usd-trong-8-thang-post376106.html"),
    "2025-09": (16.82, "https://vov.vn/kinh-te/9-thang-viet-nam-xuat-sieu-1682-ty-usd-post1235705.vov"),
    "2025-10": (19.56, "https://vneconomy.vn/xuat-sieu-gan-20-ty-usd-thu-ngan-sach-nganh-hai-quan-10-thang-vuot-moc-379000-ty-dong.htm"),
    "2025-12": (20.03, "https://baodauthau.vn/infographic-nam-2025-ca-nuoc-xuat-sieu-2003-ty-usd-post191866.html"),
    "2026-01": (-1.78, "https://baodauthau.vn/thang-12026-can-can-thuong-mai-hang-hoa-nhap-sieu-178-ty-usd-post193707.html"),
    "2026-02": (-2.95, "https://baomoi.com/cuc-hai-quan-het-2-thang-nam-2026-ca-nuoc-nhap-sieu-295-ti-usd-c54648473.epi"),
    "2026-03": (-3.64, "https://baodauthau.vn/quy-i2026-can-can-thuong-mai-hang-hoa-nhap-sieu-364-ty-usd-post196540.html"),
    "2026-04": (-7.1, "https://vietnamfinance.vn/viet-nam-nhap-sieu-hon-7-ty-usd-sau-4-thang-nam-2026-d144298.html"),
    "2026-06": (-16.65, "https://vneconomy.vn/xuat-nhap-khau-6-thang-dau-nam-tang-manh-ca-nuoc-nhap-sieu-1665-ty-usd.htm"),
    "2026-07": (-20.52, "https://baodauthau.vn/7-thang-nam-2026-ca-nuoc-nhap-sieu-2052-ty-usd-post204196.html"),
    "2026-08": (-20.46, "https://baodauthau.vn/8-thang-nam-2026-ca-nuoc-nhap-sieu-2046-ty-usd-post206269.html"),
    "2026-09": (-19.42, "data/economy.json ECON_OFFICIAL.ytd_2026.trade_balance_busd_9M"),
}
TRADE_CONFLICTS = ["Jan-Feb 2025 cumulative balance not found: Jan-Mar 2025 months are a 3-month interval average of the Q1 surplus.",
                   "2M-2026: customs 2.95 bn deficit used; NSO quotes 2.96-2.98.",
                   "Sep-2026: 9M (−19.42) minus 8M (−20.46) gives +1.04 bn; the same NSO release states the September surplus as +1.27 bn (8M revised). Cumulatives used as published."]
# Real-estate credit (SBV figures via MoC/press), VND bn outstanding.
RE_BUSINESS = [  # 'kinh doanh BĐS' (developers/real-estate business), MoC quarterly report
    {"date": "2025-03", "value": 1560000, "url": "https://vnexpress.net/du-no-tin-dung-bat-dong-san-dat-2-trieu-ty-dong-quy-iv-2025-5006978.html", "note": "'over 1.56 quadrillion' (finance.json flows)"},
    {"date": "2025-06", "value": 1740000, "url": "https://vietnamfinance.vn/tin-dung-kinh-doanh-bat-dong-san-vuot-moc-2-trieu-ty-dong-d138953.html", "note": "'over 1.74 quadrillion' (search excerpt 2026-10-07)"},
    {"date": "2025-09", "value": 1890000, "url": "https://vnbusiness.vn/tin-dung-bat-dong-san-vuot-moc-2-trieu-ty-dong.html", "note": "'over 1.89 quadrillion' (search excerpt 2026-10-07)"},
    {"date": "2025-12", "value": 2000000, "url": "https://vnexpress.net/du-no-tin-dung-bat-dong-san-dat-2-trieu-ty-dong-quy-iv-2025-5006978.html", "note": "'over 2 quadrillion' (finance.json flows)"},
    {"date": "2026-03", "value": 2235305, "url": "https://vnbusiness.vn/hon-25-trieu-ty-dong-tin-dung-chay-vao-bat-dong-san-va-bai-toan-ap-luc-chi-phi-von.html", "note": "31/3/2026, stated as the base of the Q2 increase (search excerpt 2026-10-07)"},
    {"date": "2026-06", "value": 2519378, "url": "https://dantri.com.vn/bat-dong-san/du-no-kinh-doanh-bat-dong-san-vuot-25-trieu-ty-dong-20260822144250909.htm", "note": "30/6/2026, +284,073 bn q/q (+12.71%)"},
]
SAVILLS_HN_YOY = [  # Hanoi primary apartment asking price, % y/y (derived from finance.json Savills points; 2026-Q2 as published)
    {"period": "2023-Q4", "from": "2023-10", "to": "2024-09", "value": 23.4, "note": "derived 58/47 (Q4-23 vs Q4-22)"},
    {"period": "2024-Q4", "from": "2024-10", "to": "2025-09", "value": 29.3, "note": "derived 75/58"},
    {"period": "2025-Q4", "from": "2025-10", "to": "2026-03", "value": 36.0, "note": "derived 102/75"},
    {"period": "2026-Q2", "from": "2026-04", "to": "2026-09", "value": 27.0, "note": "Savills Q2-2026 +27% y/y as published"},
]
REMIT_SBV = [  # SBV kiều hối (channel-based) - shown in the panel, not used by the model (BoP secondary income is used)
    {"period": "2021", "value": 12.5, "url": None, "note": "per data/economy.json (origin not re-verified)"},
    {"period": "2023", "value": 16.0, "url": "https://vnexpress.net/kieu-hoi-ve-viet-nam-dat-ky-luc-16-ty-usd-4706903.html"},
    {"period": "2024", "value": 16.0, "url": "https://vnexpress.net/khoang-16-ty-usd-kieu-hoi-ve-viet-nam-trong-nam-2024-4832487.html"},
    {"period": "2025", "value": 18.0, "url": "https://baophapluat.vn/kieu-hoi-nam-2025-dat-muc-ky-luc-gan-18-ty-usd.html", "note": "'gần 18 tỷ USD' (search excerpt 2026-10-07). CONFLICT/new: data/economy.json leaves 2025 null with only 'over 16 bn' (Foreign Minister) - Economy owns this series"},
    {"period": "2026-H1 (HCMC only)", "value": 4.037, "url": "https://vneconomy.vn/kieu-hoi-ve-tp-ho-chi-minh-hon-4-ty-usd-trong-6-thang-dau-nam-2026.htm", "note": "HCMC, -22.8% y/y; no national 2026 figure"},
]
SJC_PREMIUM_PRESS = [
    {"date": "2026-08-28", "value": 3.0, "basis": "SJC sell vs world spot at bank USD rate", "url": "https://theleader.vn/gia-vang-hom-nay-28-8-2026-ap-luc-chot-loi-keo-vang-roi-moc-4600-usd-d47484.html", "note": "'giá bán ra trong nước cao hơn 3 triệu đồng/lượng', record-low gap"},
    {"date": "2026-09-19", "value": 9.3, "basis": "SJC sell 147.6 vs world ~4,378 USD/oz converted ~138.3", "url": "https://baolaocai.vn/gia-vang-hom-nay-209-thi-truong-tam-lang-truoc-tuan-giao-dich-moi-post909869.html", "note": "search excerpt 2026-10-07; exact article within the result set not pinned"},
]

# ---- calibrated (not estimated) parts of the rate equation: coefficient, neutral, basis --------------------------
V2_CALIBRATED = {
    "cpi_yoy": {"coef": 0.02, "neutral": None, "lag": 1, "basis": "calibrated: free estimate has the wrong sign (CPI history has gaps and moves with the funding index); 0.02 pp/month per pp of CPI above its 2024-04..2025-09 mean = ~0.24 pp a year, below v1's 0.03 to limit double counting with the funding index", "sens": [0.0, 0.04]},
    "usdvnd_12m": {"coef": 0.02, "neutral": 2.0, "lag": 1, "basis": "calibrated: monthly central-rate history starts Oct-2024, so the 12-month change exists only from Oct-2025; neutral 2.0% ~ the only flat-window observation (1.97%, Sep-2025). Depreciation above it pushes the SBV to drain VND liquidity", "sens": [0.0, 0.05]},
    "fed_change": {"coef": 0.10, "neutral": 0.0, "lag": 0, "basis": "calibrated: v1 regressions give the Fed level the wrong sign (Fed cut 2025 while VN rates rose); a small level shift of 0.1 pp per 1 pp Fed move is kept for the external channel not already working through SBV FX sales and policy rates", "sens": [0.0, 0.3]},
    "sjc_premium": {"coef": 0.005, "neutral": None, "lag": 1, "basis": "calibrated: no monthly premium history before 2026 (year-end points only); 0.005 pp/month per million VND/tael above the Dec-2024 premium", "sens": [0.0, 0.015]},
    "re_price_momentum": {"coef": 0.002, "neutral": 26.35, "lag": 1, "basis": "calibrated: Savills Hanoi primary price y/y is annual/quarterly and not a constant-quality index; neutral = mean of the 2023-Q4 and 2024-Q4 y/y (23.4%, 29.3%)", "sens": [0.0, 0.006]},
}


def v2_load():
    fin = load("finance.json")["FINSYS"]
    eco = load("economy.json")
    return fin, eco


def _cum_to_flows(cum, months, yearly_reset=True):
    """Monthly flows from cumulative-from-January points. Months between two points get the interval average
    (basis 'interval_avg_<k>m'); a month whose cumulative and previous cumulative are both stated is 'cum_diff'."""
    out, basis = {}, {}
    for y in sorted({int(m[:4]) for m in cum}):
        last_m, last_v = 0, 0.0
        for m in sorted(k for k in cum if k.startswith(str(y))):
            mo = int(m[5:])
            k = mo - last_m
            per = (cum[m] - last_v) / k
            for j in range(last_m + 1, mo + 1):
                mm = f"{y}-{j:02d}"
                if mm in months:
                    out[mm] = per
                    basis[mm] = "cum_diff" if k == 1 else f"interval_avg_{k}m"
            last_m, last_v = mo, cum[m]
    return out, basis


def v2_inputs(P):
    """Monthly lever history on V2_MONTHS (2020-01..2026-09) + basis flags + annual blocks for the liquidity regression."""
    fin, eco = v2_load()
    eo, bop = eco["ECON_OFFICIAL"], eco["ECONFLOW"]["bop"]
    y0 = eo["y0"]
    A = lambda k: {y0 + i: v for i, v in enumerate(eo[k])}
    usd_a, rev_a, exp_a, fdi_a, ex_a, im_a = A("usd_vnd"), A("budget_rev_bn"), A("budget_exp_bn"), A("fdi_dis_musd"), A("export_busd"), A("import_busd")
    by = bop["years"]
    B = lambda k, y: bop["s"][k][by.index(y)]
    ann = fin["annual"]
    cg = dict(zip(ann["years"], ann["credit_growth"]))
    dep_fsi = dict(zip(ann["years"], ann["deposits"]))
    # SBV credit levels at year-end: 2022-2025 stated; earlier years chained back from 2024 with credit_growth (derived)
    lvl = {2024: 15616077.0}
    for y in range(2024, 2014, -1):
        lvl[y - 1] = lvl[y] / (1 + cg[y] / 100)
    lvl_basis = {y: ("stated" if y in (2022, 2023, 2024) else "derived: chained back from the 2024 level with finance.json annual credit_growth") for y in lvl}
    lvl[2025] = 18594930.02
    lvl_basis[2025] = "stated (SBV table Dec-2025)"
    pan = P["series"]
    cen = {m: v for m, v in zip(MONTHS, pan["usdvnd_central"]["values"]) if v is not None}

    def S_of(m):  # VND per USD used to convert USD flows
        return (cen[m], "central_rate_month") if m in cen else (usd_a[int(m[:4])], "annual_average_rate")

    H = {k: [None] * V2N for k in ("credit", "fdi", "remit", "tour", "tb", "income", "fx_vnd", "rev", "exp", "dev", "fis", "sbv_fx", "omo", "re_credit", "S")}
    HB = {k: [None] * V2N for k in H}
    # ---- credit: 2021-2023 interval averages between dated SBV statements, 2020 & 2024 annual/12, 2025+ SBV table
    ytd_pts = {2021: {"2021-03": 2.93, "2021-06": 5.10, "2021-10": 8.72}, 2022: {"2022-03": 5.04, "2022-08": 9.91}, 2023: {"2023-03": 1.61, "2023-09": 5.73, "2023-11": 9.15}}
    for y in range(2020, 2025):
        cum = {f"{y}-12": (lvl[y] - lvl[y - 1]) / 1000}
        for m, p in ytd_pts.get(y, {}).items():
            cum[m] = lvl[y - 1] * p / 100 / 1000
        fl, bs = _cum_to_flows(cum, V2_MONTHS)
        for m, v in fl.items():
            H["credit"][V2_IDX[m]] = v
            HB["credit"][V2_IDX[m]] = "annual_even" if y in (2020, 2024) else "interval_avg_statements"
    fm = fin["monthly"]
    cl = {tlabel_to_month(t): v for t, v in zip(fm["months"], fm["credit_level"]) if v is not None}
    cb = {tlabel_to_month(t): b for t, b in zip(fm["months"], fm["credit_basis"])}
    for m in V2_MONTHS:
        if m >= "2025-01":
            pm = V2_MONTHS[V2_IDX[m] - 1]
            if m in cl and pm in cl:
                H["credit"][V2_IDX[m]] = (cl[m] - cl[pm]) / 1000
                HB["credit"][V2_IDX[m]] = "sbv_table_diff" if cb.get(m) == "sbv_table" else "statement_level_diff (rounded level)"
    # ---- FX: 2020-2024 annual/12 (NSO FDI, BoP secondary income & travel credit, customs balance, BoP primary income)
    for m in V2_MONTHS:
        i, y = V2_IDX[m], int(m[:4])
        s, sb = S_of(m)
        H["S"][i], HB["S"][i] = s, sb
        if y <= 2024:
            H["fdi"][i] = fdi_a[y] / 1000 / 12
            H["tb"][i] = (ex_a[y] - im_a[y]) / 12
            for k in ("fdi", "tb"):
                HB[k][i] = "annual_even"
        if y <= 2025:
            H["remit"][i] = B("secondary_income_in", y) / 12
            H["tour"][i] = B("travel_x", y) / 12
            H["income"][i] = -(B("primary_income_in", y) - B("primary_income_out", y)) / 12
            for k in ("remit", "tour", "income"):
                HB[k][i] = "annual_even"
            H["sbv_fx"][i] = B("reserves_change", y) * s / 1000 / 12
            HB["sbv_fx"][i] = "annual_even (BoP reserves change)"
    fdi_fl, fdi_b = _cum_to_flows({m: v for m, (v, u) in FDI_CUM.items()}, V2_MONTHS)
    tb_fl, tb_b = _cum_to_flows({m: v for m, (v, u) in TRADE_CUM.items()}, V2_MONTHS)
    for m in V2_MONTHS:
        i = V2_IDX[m]
        if m >= "2025-01":
            H["fdi"][i], HB["fdi"][i] = fdi_fl[m], fdi_b[m]
            H["tb"][i], HB["tb"][i] = tb_fl[m], tb_b[m]
        if m >= "2026-01":
            H["tour"][i], HB["tour"][i] = eco["ECONFLOW"]["tour"]["travel_receipts_busd"][-1] / 9, "9M_even (NSO 9M-2026 service exports)"
            H["remit"][i], HB["remit"][i] = B("secondary_income_in", 2025) / 12, "assumption: 2025 monthly average carried (no 2026 national figure)"
            H["income"][i], HB["income"][i] = -(B("primary_income_in", 2025) - B("primary_income_out", 2025)) / 12, "assumption: 2025 monthly average carried"
            H["sbv_fx"][i], HB["sbv_fx"][i] = 0.0, "assumption: 0 (2026 SBV FX operations not published)"
        H["fx_vnd"][i] = (H["fdi"][i] + H["remit"][i] + H["tour"][i] + H["tb"][i] - H["income"][i]) * H["S"][i] / 1000
    # ---- fiscal: 2020-2024 annual/12 (final accounts), 2025-26 MoF monthly execution (cumulative differences)
    for m in V2_MONTHS:
        i, y = V2_IDX[m], int(m[:4])
        if y <= 2024:
            H["rev"][i], H["exp"][i] = rev_a[y] / 1000 / 12, exp_a[y] / 1000 / 12
            H["fis"][i] = H["exp"][i] - H["rev"][i]
            for k in ("rev", "exp", "fis"):
                HB[k][i] = "annual_even (final accounts)"
    bal = {m: (d["rev"] - d["exp"]) if "exp" in d else d.get("bal") for m, d in BUDGET_CUM.items()}
    bal = {m: v for m, v in bal.items() if v is not None}
    b_fl, b_b = _cum_to_flows(bal, V2_MONTHS)
    r_fl, r_b = _cum_to_flows({m: d["rev"] for m, d in BUDGET_CUM.items()}, V2_MONTHS)
    e_fl, e_b = _cum_to_flows({m: d["exp"] for m, d in BUDGET_CUM.items() if "exp" in d}, V2_MONTHS)
    d_fl, d_b = _cum_to_flows({m: d["dev"] for m, d in BUDGET_CUM.items() if "dev" in d}, V2_MONTHS)
    for m in V2_MONTHS:
        if m >= "2025-01":
            i = V2_IDX[m]
            H["fis"][i], HB["fis"][i] = -b_fl[m], b_b[m] + (" (approx. cumulative)" if any(BUDGET_CUM.get(k, {}).get("approx") for k in (m,)) else "")
            H["rev"][i], HB["rev"][i] = r_fl.get(m), r_b.get(m)
            H["exp"][i], HB["exp"][i] = e_fl.get(m), e_b.get(m)
            H["dev"][i], HB["dev"][i] = d_fl.get(m), d_b.get(m)
    # ---- SBV OMO net injection (VBMA month-end repo outstanding minus SBV bills outstanding), 2025-02..
    omo, bills = {}, {}
    for x in fin["sbv"]["operations"]:
        if x["period"] != "point":
            continue
        m = x["date"][:7]
        if x["item"] == "omo_outstanding" and "VBMA" in (x.get("note") or ""):
            if m not in omo or x["date"] >= omo[m][0]:
                omo[m] = (x["date"], x["value"])
        if x["item"] == "sbv_bills_outstanding":
            if m not in bills or x["date"] >= bills[m][0]:
                bills[m] = (x["date"], x["value"])
    net, last_b = {}, None
    for m in V2_MONTHS:
        if m in bills:
            last_b = bills[m][1]
        if m in omo and last_b is not None:
            net[m] = (omo[m][1] - (bills[m][1] if m in bills else last_b), "VBMA month-end" + ("" if m in bills else "; bills carried at last reported level (0 after 28/7/2025: none reported)"))
    for m in V2_MONTHS:
        pm = V2_MONTHS[V2_IDX[m] - 1] if V2_IDX[m] else None
        if m in net and pm in net:
            H["omo"][V2_IDX[m]] = (net[m][0] - net[pm][0]) / 1000
            HB["omo"][V2_IDX[m]] = net[m][1]
    # ---- real-estate credit (total incl. home loans) interval averages between finance.json points
    re_pts = []
    for key in ("Real estate credit, total (dư nợ tín dụng BĐS)", "Real estate credit, total"):
        re_pts += [(o["d"], o["v"], o["u"]) for o in fin["flows"].get(key, [])]
    re_pts.sort()
    for (d0, v0, u0), (d1, v1, u1) in zip(re_pts, re_pts[1:]):
        k = V2_IDX[d1] - V2_IDX[d0]
        for j in range(V2_IDX[d0] + 1, V2_IDX[d1] + 1):
            H["re_credit"][j] = (v1 - v0) / 1000 / k
            HB["re_credit"][j] = f"interval_avg_{k}m ({d0}->{d1})"
    annual = {"lvl": lvl, "lvl_basis": lvl_basis, "dep_fsi": dep_fsi, "usd_a": usd_a, "rev_a": rev_a, "exp_a": exp_a, "fdi_a": fdi_a, "ex_a": ex_a, "im_a": im_a, "B": B,
              "re_pts": re_pts, "omo_points": omo}
    return H, HB, annual


def v2_liquidity_regression(annual, years):
    """Annual: (credit increase - deposit increase) on credit increase, net FX inflow (VND), fiscal injection, SBV reserve purchases (VND)."""
    lvl, D, B = annual["lvl"], annual["dep_fsi"], annual["B"]
    rows = []
    for y in years:
        S = annual["usd_a"][y] / 1000
        netfx = (annual["fdi_a"][y] / 1000 + B("secondary_income_in", y) + B("travel_x", y) + (annual["ex_a"][y] - annual["im_a"][y])
                 + (B("primary_income_in", y) - B("primary_income_out", y))) * S
        dC = (lvl[y] - lvl[y - 1]) / 1000
        dD = (D[y] - D[y - 1]) / 1000
        rows.append({"year": y, "gap": dC - dD, "credit": dC, "netfx": netfx, "fiscal": (annual["exp_a"][y] - annual["rev_a"][y]) / 1000,
                     "sbv_fx": B("reserves_change", y) * S})
    Y = np.array([r["gap"] for r in rows])
    X = np.column_stack([np.ones(len(rows))] + [np.array([r[k] for r in rows]) for k in ("credit", "netfx", "fiscal", "sbv_fx")])
    b = np.linalg.lstsq(X, Y, rcond=None)[0]
    e = Y - X @ b
    n, k = X.shape
    s2 = float(e @ e / (n - k))
    se = np.sqrt(np.diag(s2 * np.linalg.inv(X.T @ X)))
    r2 = 1 - float(e @ e) / float((Y - Y.mean()) @ (Y - Y.mean()))
    names = ["const", "credit", "netfx", "fiscal", "sbv_fx"]
    return {"coef": {nm: round(float(c), 4) for nm, c in zip(names, b)}, "se": {nm: round(float(c), 4) for nm, c in zip(names, se)},
            "r2": round(r2, 3), "n": n, "sample": f"{years[0]}..{years[-1]} (annual)", "resid_sd_tn": round(math.sqrt(s2), 1),
            "rows": [{k: (round(v, 1) if isinstance(v, float) else v) for k, v in r.items()} for r in rows]}


def v2_weights(liq):
    c = liq["coef"]
    return {"credit": c["credit"], "fx": -c["netfx"], "fiscal": -c["fiscal"], "sbv": -c["sbv_fx"]}


def v2_fg(H, W):
    """FG_t in VND tn (constant omitted - it is absorbed in P*). OMO net counts like SBV FX purchases (assumption)."""
    out = [None] * V2N
    for i in range(V2N):
        c, fx, fis, sfx = H["credit"][i], H["fx_vnd"][i], H["fis"][i], H["sbv_fx"][i]
        if None in (c, fx, fis, sfx):
            continue
        omo = H["omo"][i] if H["omo"][i] is not None else 0.0
        out[i] = W["credit"] * c - W["fx"] * fx - W["fiscal"] * fis - W["sbv"] * (sfx + omo)
    return out


def v2_denominator(m, dep_fsi):
    return dep_fsi[int(m[:4]) - 1] / 1000.0  # VND tn, previous year-end (IMF-FSI customer deposits)


def v2_pressure(FG, dep_fsi):
    pp = [None if FG[i] is None else 100.0 * FG[i] / v2_denominator(V2_MONTHS[i], dep_fsi) for i in range(V2N)]
    P = [None] * V2N
    for i in range(11, V2N):
        w = pp[i - 11: i + 1]
        if None not in w:
            P[i] = sum(w)
    return pp, P


def v2_interval_rows(Pn, Pidx, start, end, exclude=None):
    dep, refi = v(Pn, "dep12_vcb"), v(Pn, "refi_rate")
    gold, vni = v(Pn, "gold_world_12m"), v(Pn, "vnindex_12m")
    obs = [i for i in range(N) if dep[i] is not None]
    rows = []
    for a, b in zip(obs, obs[1:]):
        if MONTHS[a] < start or MONTHS[b] > end:
            continue
        if exclude and not (MONTHS[b] < exclude[0] or MONTHS[a] >= exclude[1]):
            continue
        x = {"pol": 0.0, "P": 0.0, "gold": 0.0, "vni": 0.0, "c": 0.0}
        ok = True
        for i in range(a + 1, b + 1):
            pm = MONTHS[i - 1]
            p = Pidx[V2_IDX[pm]]
            if p is None or gold[i - 1] is None or vni[i - 1] is None:
                ok = False
                break
            x["pol"] += refi[i] - refi[i - 1]
            x["P"] += p
            x["gold"] += gold[i - 1]
            x["vni"] += vni[i - 1]
            x["c"] += 1
        if ok:
            rows.append({"from": MONTHS[a], "to": MONTHS[b], "dr": round(dep[b] - dep[a], 4), "len": b - a, "x": x})
    return rows


def v2_fit_rate(rows, names=("pol", "P", "gold", "vni", "c")):
    Y = np.array([r["dr"] for r in rows])
    X = np.array([[r["x"][k] for k in names] for r in rows])
    b = np.linalg.lstsq(X, Y, rcond=None)[0]
    e = Y - X @ b
    n, k = X.shape
    s2 = float(e @ e / (n - k))
    se = np.sqrt(np.diag(s2 * np.linalg.pinv(X.T @ X)))
    r2c = 1 - float(e @ e) / float((Y - Y.mean()) @ (Y - Y.mean()))
    lens = np.array([r["len"] for r in rows], float)
    sig_m = math.sqrt(float(np.mean(e ** 2 / lens)))
    coef = {nm: float(c) for nm, c in zip(names, b)}
    return {"coef": coef, "se": {nm: float(c) for nm, c in zip(names, se)}, "r2": round(r2c, 3), "n": n,
            "sample": f"{rows[0]['from']}..{rows[-1]['to']}", "sigma_month": sig_m,
            "intervals": [{"from": r["from"], "to": r["to"], "dr": r["dr"], "fitted": round(float(f), 3)} for r, f in zip(rows, X @ b)]}


def v2_pstar(coef, neutral):
    # const c = -kappa*P* - g_vni*n_vni - g_gold*n_gold  ->  P*
    return -(coef["c"] + coef["vni"] * neutral["vnindex_12m"] + coef["gold"] * neutral["gold_world_12m"]) / coef["P"]


def v2_neutrals(Pn):
    flat = [m for m in MONTHS if "2024-04" <= m <= "2025-09"]
    out = {}
    for k in ("cpi_yoy", "gold_world_12m", "vnindex_12m"):
        vals = [at(Pn, k, m) for m in flat if at(Pn, k, m) is not None]
        out[k] = sum(vals) / len(vals)
    return out


def v2_simulate_history(Pn, Pidx, coef, pstar, neutral, start, end, addons=None):
    """Dynamic replay from the observed rate at `start`, with actual drivers. addons: dict key->(coef, neutral, series) or None."""
    dep, refi = v(Pn, "dep12_vcb"), v(Pn, "refi_rate")
    gold, vni = v(Pn, "gold_world_12m"), v(Pn, "vnindex_12m")
    r = dep[IDX[start]]
    out = []
    for i in range(IDX[start] + 1, IDX[end] + 1):
        pm = MONTHS[i - 1]
        dr = coef["pol"] * (refi[i] - refi[i - 1]) + coef["P"] * (Pidx[V2_IDX[pm]] - pstar) \
            + coef["vni"] * (vni[i - 1] - neutral["vnindex_12m"]) + coef["gold"] * (gold[i - 1] - neutral["gold_world_12m"])
        used = []
        for k, (cf, nt, ser) in (addons or {}).items():
            x = ser[i - 1]
            if x is not None:
                dr += cf * (x - nt)
                used.append(k)
        r += dr
        out.append({"month": MONTHS[i], "predicted": round(r, 3), "actual": dep[i], "addons_used": used})
    return out


def _rmse(path):
    e = [(p["predicted"] - p["actual"]) ** 2 for p in path if p["actual"] is not None]
    return round(math.sqrt(sum(e) / len(e)), 3) if e else None


# ---- browser-reproducible projection --------------------------------------------------------------------------------
def _lever_x(lv, settings, h):
    """Value a lever contributes to the RATE in month h: lag 1 -> previous month (latest observed value for h = 0)."""
    path = settings.get(lv["key"], lv["baseline_path"])
    if lv["lag"] == 1:
        return lv["latest_value"] if h == 0 else path[h - 1]
    return path[h]


def project_v2(model, settings=None):
    """Reference implementation - re-implementable in browser JS with + - * / and one loop (no matrix algebra).
    model = SIM.model2; settings = {lever_key: [6 monthly values]} overriding baseline paths (missing keys = baseline).
      for h in 0..5:
        r  += kappa*(P - P_star) + sum over rate levers of rate_coef*(x_h - neutral)     (x_h per _lever_x)
        P  += sum over liquidity levers of 100*liquidity_weight*path[h]/D_tn - rolloff_pp[h]
    Returns the 6 month-end rates, unrounded."""
    settings = settings or {}
    p = model["params"]
    r, P = p["r0"], p["P0"]
    out = []
    for h in range(6):
        dr = p["kappa"] * (P - p["P_star"])
        for lv in model["levers"]:
            if lv["rate_coef"] is not None:
                dr += lv["rate_coef"] * (_lever_x(lv, settings, h) - lv["neutral"])
        r = r + dr
        out.append(r)
        for lv in model["levers"]:
            if lv["liquidity_weight"] is not None:
                P = P + 100.0 * lv["liquidity_weight"] * settings.get(lv["key"], lv["baseline_path"])[h] / p["D_tn"]
        P = P - p["rolloff_pp"][h]
    return out


def contributions_v2(model, settings=None):
    """Cumulative contribution of each lever (its funding-gap part via kappa and its direct rate part) plus the inherited
    pressure (kappa*(P0 - P*)) and the roll-off of the months leaving the 12-month window. Sums to path - r0."""
    settings = settings or {}
    p = model["params"]
    kappa = p["kappa"]
    keys = [lv["key"] for lv in model["levers"]] + ["inherited_pressure", "funding_rolloff"]
    cum = {k: 0.0 for k in keys}
    out = {k: [] for k in keys}
    padd = {lv["key"]: 0.0 for lv in model["levers"]}
    roll = 0.0
    for h in range(6):
        cum["inherited_pressure"] += kappa * (p["P0"] - p["P_star"])
        cum["funding_rolloff"] += -kappa * roll
        for lv in model["levers"]:
            d = kappa * padd[lv["key"]] if lv["liquidity_weight"] is not None else 0.0
            if lv["rate_coef"] is not None:
                d += lv["rate_coef"] * (_lever_x(lv, settings, h) - lv["neutral"])
            cum[lv["key"]] += d
        for k in keys:
            out[k].append(cum[k])
        for lv in model["levers"]:
            if lv["liquidity_weight"] is not None:
                padd[lv["key"]] += 100.0 * lv["liquidity_weight"] * settings.get(lv["key"], lv["baseline_path"])[h] / p["D_tn"]
        roll += p["rolloff_pp"][h]
    return out


def _r(x, k=4):
    return None if x is None else round(float(x), k)


def _seasonal(H, key, months):
    return [H[key][V2_IDX[m]] for m in months]


def v2_panel_series(H, HB, FG, pp, Pidx, annual):
    """Monthly lever series for SIM.panel (months 2021-01..2026-09)."""
    def cut(arr):
        return [None if arr[V2_IDX[m]] is None else _r(arr[V2_IDX[m]], 3) for m in MONTHS]

    def bas(arr):
        return [arr[V2_IDX[m]] for m in MONTHS]
    S = {}
    S["fdi_disbursed_m"] = series("FDI giải ngân (theo tháng)", "FDI disbursed, monthly", "USD bn per month", cut(H["fdi"]),
                                  "NSO 'vốn FDI thực hiện' (disbursed, an estimate revised next month). 2025-2026: differences of the cumulative-from-January figures (interval averages where an intermediate cumulative was not found, see 'basis'); 2021-2024: annual total / 12. Not BoP FDI (~80% of NSO disbursement).",
                                  "NSO monthly socio-economic reports via press; annual from data/economy.json ECON_OFFICIAL.fdi_dis_musd", "per point in 'cumulative'", "search excerpts 2026-10-07", "fx",
                                  basis=bas(HB["fdi"]), cumulative=[{"month": m, "value": v_, "url": u} for m, (v_, u) in sorted(FDI_CUM.items())])
    S["remittances_bop_m"] = series("Kiều hối & chuyển giao vãng lai (BoP)", "Remittances & current transfers (BoP secondary income, credit)", "USD bn per month", cut(H["remit"]),
                                    "BoP secondary income, credit (all current transfers incl. remittances) / 12 per year (one definition 2015-2025). 2026: no national figure - model carries the 2025 average (flagged in 'basis'). SBV channel-based kiều hối kept separately in 'sbv_kieu_hoi' (different definition, never merged).",
                                    "data/economy.json ECONFLOW.bop.s.secondary_income_in (IMF BPM6 from SBV)", "see economy.json bop.sources", "Economy tab", "fx",
                                    basis=bas(HB["remit"]), sbv_kieu_hoi=REMIT_SBV)
    S["tourism_receipts_m"] = series("Thu từ khách quốc tế (du lịch)", "International travel receipts", "USD bn per month", cut(H["tour"]),
                                     "BoP travel credit / 12 per year (2021-2025); 2026: NSO 9M-2026 service-export travel receipts 13.06 bn / 9. Monthly arrivals are not used to shape months.",
                                     "data/economy.json ECONFLOW.tour.travel_receipts_busd", "https://www.nso.gov.vn/bai-top/2026/10/bao-cao-tinh-hinh-kinh-te-xa-hoi-quy-iii-va-9-thang-nam-2026/", "Economy tab", "fx",
                                     basis=bas(HB["tour"]))
    S["trade_balance_m"] = series("Cán cân thương mại hàng hóa", "Goods trade balance", "USD bn per month", cut(H["tb"]),
                                  "Customs/NSO goods balance (+ = surplus). 2025-2026: differences of cumulative-from-January balances (interval averages where a cumulative was not found); 2021-2024: annual (export − import, data/economy.json) / 12. Customs basis, not the BoP goods balance (2025: 20.0 vs 41.9 bn).",
                                  "Customs/NSO via press; data/economy.json ECON_OFFICIAL export/import", "per point in 'cumulative'", "search excerpts 2026-10-07", "fx",
                                  basis=bas(HB["tb"]), cumulative=[{"month": m, "value": v_, "url": u} for m, (v_, u) in sorted(TRADE_CUM.items())], conflicts=TRADE_CONFLICTS)
    S["income_outflow_m"] = series("Chuyển lợi nhuận/thu nhập ra nước ngoài (ròng)", "Net primary-income outflow (profit & interest repatriation)", "USD bn per month", cut(H["income"]),
                                   "−(BoP primary income credit − debit) / 12 per year; positive = net outflow. 2026: 2025 average carried (assumption; SBV Q1-2026 reports only investment income net −1.23 bn, a narrower concept).",
                                   "data/economy.json ECONFLOW.bop.s.primary_income_in/out", "see economy.json bop.sources", "Economy tab", "fx", basis=bas(HB["income"]))
    S["fx_net_inflow_vnd_m"] = series("Dòng ngoại tệ ròng quy VND (mô hình)", "Net FX inflow in VND (model input)", "VND tn per month", cut(H["fx_vnd"]),
                                      "Derived: (FDI disbursed + remittances (BoP) + travel receipts + goods balance − net primary-income outflow) × USD/VND (month central rate from Oct-2024, annual average rate before) / 1000.",
                                      "derived", "derived", "derived", "fx", derived=True)
    S["budget_revenue_m"] = series("Thu NSNN theo tháng", "State budget revenue, monthly", "VND tn per month", cut(H["rev"]),
                                   "MoF cash-basis execution: differences of cumulative estimates 2025-2026 (interval averages where a cumulative was not found); 2021-2024 final accounts / 12 (different basis: includes carry-overs). Cumulative points with URLs in 'cumulative'.",
                                   "Ministry of Finance via press; data/economy.json", "per point", "search excerpts 2026-10-07", "fiscal",
                                   basis=bas(HB["rev"]), cumulative=[{"month": m, **{k: d[k] for k in d}} for m, d in sorted(BUDGET_CUM.items())])
    S["budget_spending_m"] = series("Chi NSNN theo tháng", "State budget spending, monthly", "VND tn per month", cut(H["exp"]),
                                    "As budget_revenue_m. 2-month gaps (Feb-Mar and Jul-Aug 2025) where the cumulative spending was not found are null here; the fiscal injection for those months uses the stated balances (interval average).",
                                    "Ministry of Finance via press", "see budget_revenue_m.cumulative", "search excerpts 2026-10-07", "fiscal", basis=bas(HB["exp"]))
    S["dev_investment_m"] = series("Chi đầu tư phát triển theo tháng", "Development-investment spending, monthly", "VND tn per month", cut(H["dev"]),
                                   "MoF development-investment spending (part of budget spending), differences/interval averages of cumulative estimates; null where not found. Not the public-investment disbursement rate (different scope).",
                                   "Ministry of Finance via press", "see budget_revenue_m.cumulative", "search excerpts 2026-10-07", "fiscal", basis=bas(HB["dev"]))
    S["fiscal_net_injection_m"] = series("Bơm ròng từ ngân sách (chi − thu)", "Fiscal net injection (spending − revenue)", "VND tn per month", cut(H["fis"]),
                                         "Spending minus revenue in the month (negative = budget surplus, money moving from customer deposits to State Treasury accounts). 2025-2026 from MoF cumulative balances; 2021-2024 annual / 12.",
                                         "derived from MoF execution", "see budget_revenue_m.cumulative", "derived", "fiscal", basis=bas(HB["fis"]),
                                         land_use_fees_annual={"years": [2018, 2019, 2020, 2021, 2022, 2023, 2024, "2026 plan"],
                                                               "values_tn": [147.8, 153.7, 173.0, 185.1, 208.5, 153.8, 232.9, 474.2],
                                                               "note": "Land-use fees (thu tiền sử dụng đất), final accounts; 2025 not itemised in MoF releases (null; 'thu từ nhà, đất' 575.5 tn is a broader aggregate); 2026 = National Assembly plan. Monthly land-fee collections are not published nationally.",
                                                               "source": "data/economy.json ECONFLOW.budget.revenue.land_use_fees"})
    S["sbv_omo_net_m"] = series("NHNN bơm/hút ròng qua OMO và tín phiếu", "SBV net injection via OMO repos and bills", "VND tn per month", cut(H["omo"]),
                                "Change in (VBMA month-end repo outstanding − SBV bills outstanding); positive = net injection. From Feb-2025 only (finance.json sbv.operations); weekly reports are taken at the last Friday of the month, so month values carry timing noise. Before 2025 null (not in the model's index).",
                                "VBMA weekly bond-market reports via finance.json FINSYS.sbv.operations", "https://vbma.org.vn", "per finance.json", "sbv", basis=bas(HB["omo"]))
    S["sbv_fx_reserves_change_m"] = series("NHNN mua/bán ngoại tệ (thay đổi dự trữ, quy VND)", "SBV FX purchases (+) / sales (−), reserve change in VND", "VND tn per month", cut(H["sbv_fx"]),
                                           "BoP reserve-asset change × average rate / 12 per year (2021-2025). 2026 not published (SBV will publish net FX purchases with a 3-month lag from 2027): the model sets 0 (flagged). Reserves were ~87.6 bn USD on 18/6/2026 (SBV draft decree) vs 85.6 bn end-2025 (excl. gold).",
                                           "data/economy.json ECONFLOW.bop.s.reserves_change", "https://baodauthau.vn/den-186-du-tru-ngoai-hoi-dat-876-ty-usd-post201374.html", "Economy tab / search excerpt 2026-10-07", "sbv",
                                           basis=bas(HB["sbv_fx"]))
    S["credit_flow_m"] = series("Tín dụng tăng thêm trong tháng", "Credit increase in the month", "VND tn per month", cut(H["credit"]),
                                "Change in credit to the economy. 2025-2026: SBV month-end table levels (Aug/Sep-2026 rounded statement levels); 2021-2023: interval averages between dated SBV statements (cut-off mapped to its calendar month), anchored to year-end levels; 2020 & 2024: annual increase / 12. Pre-2022 year-end levels chained back from 2024 with finance.json annual credit growth (derived).",
                                "finance.json FINSYS.monthly / annual; SBV statements (credit_ytd observations)", "https://sbv.gov.vn/du-no-tin-dung-doi-voi-nen-kt-dttktt", "per finance.json", "credit",
                                basis=bas(HB["credit"]), year_end_levels={str(y): {"value_bn": round(annual["lvl"][y]), "basis": annual["lvl_basis"][y]} for y in range(2019, 2026)})
    S["re_credit_flow_m"] = series("Tín dụng bất động sản tăng thêm (gồm cho vay mua nhà)", "Real-estate credit increase (incl. home loans)", "VND tn per month", cut(H["re_credit"]),
                                   "Interval averages between SBV total real-estate credit points (business + home purchase/repair; finance.json flows): Dec-2024 3.35, Jul-2025 4.10, Nov-2025 4.50, Jun-2026 5.146 quadrillion VND. Developer ('kinh doanh BĐS', MoC quarterly) outstanding in 're_business_outstanding'.",
                                   "SBV via MoC/press (finance.json flows)", "per point", "per finance.json / search excerpts 2026-10-07", "credit",
                                   basis=bas(HB["re_credit"]), points=[{"date": d, "value_bn": v_, "url": u} for d, v_, u in annual["re_pts"]], re_business_outstanding=RE_BUSINESS)
    S["funding_gap_fg_m"] = series("Thiếu hụt vốn (mô hình), theo tháng", "Funding gap FG (model), monthly", "VND tn per month", cut(FG),
                                   "Model-derived: w_credit×credit − w_fx×net FX inflow − w_fiscal×fiscal injection − w_sbv×(SBV FX purchases + OMO net), weights from the annual regression in SIM.model2.estimation.liquidity (constant omitted, absorbed in the neutral P*).",
                                   "derived (model v2)", "derived", "derived", "liquidity", derived=True)
    S["funding_pressure_index"] = series("Chỉ số áp lực vốn P (12 tháng, % tiền gửi)", "Funding-pressure index P (12-month, % of deposits)", "pp of deposits", cut(Pidx),
                                         "Model-derived: sum of the last 12 monthly FG, each divided by customer deposits at the previous year-end (IMF-FSI) × 100. Comparable in spirit to credit growth minus deposit growth (y/y) but built from the levers; its neutral level P* is estimated.",
                                         "derived (model v2)", "derived", "derived", "liquidity", derived=True)
    re_m = [None] * N
    for o in SAVILLS_HN_YOY:
        for m in MONTHS:
            if o["from"] <= m <= o["to"]:
                re_m[IDX[m]] = o["value"]
    S["re_price_momentum"] = series("Đà tăng giá căn hộ sơ cấp Hà Nội (y/y)", "Hanoi primary apartment price momentum (y/y)", "% y/y", re_m,
                                    "Savills Hanoi primary asking price, % y/y; each published Q4 (or latest-quarter) value is applied to the months until the next one (stated in 'points'): a step series for the model, not monthly data.",
                                    "Savills via finance.json alternatives.real_estate", "see real_estate series URLs", "per finance.json", "savings", points=SAVILLS_HN_YOY)
    return S


def model2_block(Pn):
    H, HB, annual = v2_inputs(Pn)
    yrs = list(range(2016, 2026))
    liq = v2_liquidity_regression(annual, yrs)
    liq_oos = v2_liquidity_regression(annual, list(range(2016, 2025)))
    W = v2_weights(liq)
    FG = v2_fg(H, W)
    pp, Pidx = v2_pressure(FG, annual["dep_fsi"])
    neutral = v2_neutrals(Pn)
    rows = v2_interval_rows(Pn, Pidx, "2021-01", "2026-09")
    fit = v2_fit_rate(rows)
    c = fit["coef"]
    pstar = v2_pstar(c, neutral)
    # ---- out-of-sample: liquidity weights from 2016-2024, rate equation fitted to 2025-03, dynamic 2025-04..2026-09
    W_e = v2_weights(liq_oos)
    FG_e = v2_fg(H, W_e)
    _, Pidx_e = v2_pressure(FG_e, annual["dep_fsi"])
    rows_e = v2_interval_rows(Pn, Pidx_e, "2021-01", "2025-03")
    fit_e = v2_fit_rate(rows_e)
    pstar_e = v2_pstar(fit_e["coef"], neutral)
    sv = lambda k: v(Pn, k)
    fedc = [None] + [None if sv("fed_funds_upper")[i] is None or sv("fed_funds_upper")[i - 1] is None else sv("fed_funds_upper")[i] - sv("fed_funds_upper")[i - 1] for i in range(1, N)]
    re_m = [None] * N
    for o in SAVILLS_HN_YOY:
        for m in MONTHS:
            if o["from"] <= m <= o["to"]:
                re_m[IDX[m]] = o["value"]
    addons_hist = {"cpi_yoy": (V2_CALIBRATED["cpi_yoy"]["coef"], neutral["cpi_yoy"], sv("cpi_yoy")),
                   "usdvnd_12m": (V2_CALIBRATED["usdvnd_12m"]["coef"], V2_CALIBRATED["usdvnd_12m"]["neutral"], sv("usdvnd_12m")),
                   "re_price_momentum": (V2_CALIBRATED["re_price_momentum"]["coef"], V2_CALIBRATED["re_price_momentum"]["neutral"], re_m)}
    # fed change is contemporaneous: shift the series by one month so the history simulator's x[i-1] picks month i
    addons_hist["fed_change"] = (V2_CALIBRATED["fed_change"]["coef"], 0.0, fedc[1:] + [None])
    oos = v2_simulate_history(Pn, Pidx_e, fit_e["coef"], pstar_e, neutral, "2025-03", "2026-09")
    oos_add = v2_simulate_history(Pn, Pidx_e, fit_e["coef"], pstar_e, neutral, "2025-03", "2026-09", addons_hist)
    naive = [{"month": p_["month"], "predicted": at(Pn, "dep12_vcb", "2025-03"), "actual": p_["actual"]} for p_ in oos]
    # 2022-Q4 episode: in-sample replay and leave-episode-out refit
    ep_in = v2_simulate_history(Pn, Pidx, c, pstar, neutral, "2022-01", "2023-01")
    rows_lo = v2_interval_rows(Pn, Pidx, "2021-01", "2026-09", exclude=("2022-02", "2023-01"))
    fit_lo = v2_fit_rate(rows_lo)
    pstar_lo = v2_pstar(fit_lo["coef"], neutral)
    ep_lo = v2_simulate_history(Pn, Pidx, fit_lo["coef"], pstar_lo, neutral, "2022-01", "2023-01")
    ep_lo_nopol = v2_simulate_history(Pn, Pidx, dict(fit_lo["coef"], pol=0.0), pstar_lo, neutral, "2022-01", "2023-01")
    replay_26 = v2_simulate_history(Pn, Pidx, c, pstar, neutral, "2025-09", "2026-09")
    replay_26_add = v2_simulate_history(Pn, Pidx, c, pstar, neutral, "2025-09", "2026-09", addons_hist)

    # ---- levers -------------------------------------------------------------------------------------------------
    S_conv = at(Pn, "usdvnd_central", "2026-09")
    D_tn = annual["dep_fsi"][2025] / 1000.0
    last3 = lambda k: sum(H[k][V2_IDX[m]] for m in ("2026-07", "2026-08", "2026-09")) / 3
    seas = ["2025-10", "2025-11", "2025-12", "2026-01", "2026-02", "2026-03"]
    land_base = 474.2 / 12
    dev_b = _seasonal(H, "dev", seas)
    exp_b = _seasonal(H, "exp", seas)
    rev_b = _seasonal(H, "rev", seas)
    cred_b = _seasonal(H, "credit", seas)
    re_latest = H["re_credit"][V2_IDX["2026-06"]]
    sjc_sep = at(Pn, "sjc_gold_sell", "2026-09")
    gw = at(Pn, "gold_world_usd", "2026-09")
    sjc_prem_sep = sjc_sep - gw * S_conv * 37.5 / 31.1035 / 1e6
    prem_dec24 = [o for o in Pn["series"]["sjc_premium"]["observations"] if o["date"] == "2024-12"][0]["premium_mvnd"]
    wfx = W["fx"] * S_conv / 1000.0
    wsbv = W["sbv"] * S_conv / 1000.0

    def lever(key, group, lv_, le_, unit, freq, latest, lperiod, src, url, base, mn, mx, st, lw, rc, neu, lag, note, tor, basis):
        return {"key": key, "group": group, "label_vi": lv_, "label_en": le_, "unit": unit, "frequency": freq,
                "latest_value": _r(latest, 4), "latest_period": lperiod, "source": src, "url": url,
                "baseline_path": [_r(x, 4) for x in base], "baseline_basis": basis, "min": mn, "max": mx, "step": st,
                "liquidity_weight": _r(lw, 6), "rate_coef": _r(rc, 6), "neutral": _r(neu, 4), "lag": lag, "note": note,
                "tornado": {"low": [_r(x, 4) for x in tor[0]], "high": [_r(x, 4) for x in tor[1]]}}
    flat = lambda x: [x] * 6
    add = lambda base, d: [b + d for b in base]
    fdi3, tb3 = last3("fdi"), last3("tb")
    remit_l, tour_l, inc_l = H["remit"][V2_IDX["2026-09"]], H["tour"][V2_IDX["2026-09"]], H["income"][V2_IDX["2026-09"]]
    cal = V2_CALIBRATED
    levers = [
        lever("fdi_disbursed", "fx", "FDI giải ngân", "FDI disbursed", "USD bn/month", "monthly (NSO cumulative)", H["fdi"][V2_IDX["2026-09"]], "2026-09",
              "NSO via press", FDI_CUM["2026-09"][1], flat(fdi3), 0.5, 5.0, 0.1, -wfx, None, None, 0,
              f"Each USD 1 bn of extra net FX inflow narrows the funding gap by {_r(W['fx'],3)} × {S_conv} / 1000 = {_r(wfx,3)} VND tn (conversion share, estimated).",
              (flat(1.5), flat(3.5)), "average of Jul-Sep 2026 held flat"),
        lever("remittances", "fx", "Kiều hối (BoP chuyển giao vãng lai)", "Remittances (BoP current transfers)", "USD bn/month", "annual (BoP)", remit_l, "2025 (avg)",
              "BoP secondary income credit (data/economy.json)", "data/economy.json ECONFLOW.bop", flat(remit_l), 0.5, 3.0, 0.05, -wfx, None, None, 0,
              "No 2026 national figure; HCMC H1-2026 −22.8% y/y (SBV Region 2) is the only 2026 signal - downside risk.", (flat(1.2), flat(2.0)), "2025 monthly average held (assumption)"),
        lever("tourism_receipts", "fx", "Thu từ khách quốc tế", "International tourism receipts", "USD bn/month", "9M-2026 total", tour_l, "2026-01..09 (avg)",
              "NSO 9M-2026 report", "https://www.nso.gov.vn/bai-top/2026/10/bao-cao-tinh-hinh-kinh-te-xa-hoi-quy-iii-va-9-thang-nam-2026/", flat(tour_l), 0.5, 2.5, 0.05, -wfx, None, None, 0,
              "Travel receipts 13.06 bn in 9M-2026 (+17.4%).", (flat(1.0), flat(1.9)), "9M-2026 monthly average held"),
        lever("trade_balance", "fx", "Cán cân thương mại hàng hóa", "Goods trade balance", "USD bn/month", "monthly (customs)", H["tb"][V2_IDX["2026-09"]], "2026-09",
              "Customs/NSO via press", TRADE_CUM["2026-09"][1], flat(tb3), -6.0, 6.0, 0.25, -wfx, None, None, 0,
              "2026 turned to deficit (−19.4 bn in 9M) on 36.7% import growth; each USD 1 bn of deficit adds to the funding gap like an FX outflow.", (flat(-4.0), flat(3.0)), "average of Jul-Sep 2026 held flat"),
        lever("income_outflow", "fx", "Chuyển lợi nhuận/lãi ra nước ngoài (ròng)", "Profit & interest repatriation (net)", "USD bn/month", "annual (BoP)", inc_l, "2025 (avg)",
              "BoP primary income (data/economy.json)", "data/economy.json ECONFLOW.bop", flat(inc_l), 0.0, 3.0, 0.05, wfx, None, None, 0,
              "Added driver: FDI firms' profit remittances are the largest FX outflow after imports (14.4 bn net in 2025).", (flat(0.8), flat(1.6)), "2025 monthly average held (assumption)"),
        lever("dev_investment", "fiscal", "Chi đầu tư phát triển (NSNN)", "Development investment spending", "VND tn/month", "monthly (MoF cumulative)", H["dev"][V2_IDX["2026-09"]], "2026-09",
              "Ministry of Finance via press", BUDGET_CUM["2026-09"]["url"], dev_b, 0, 400, 5, -W["fiscal"], None, None, 0,
              "Spending moves Treasury money into customer deposits. Baseline = same month a year earlier (seasonal, no scaling); 2026 plan 1,120 tn vs 657.5 tn spent in 9M.",
              (add(dev_b, -30), add(dev_b, 80)), "same month one year earlier (Oct-2025..Mar-2026 actual/interval averages)"),
        lever("recurrent_spending", "fiscal", "Chi thường xuyên và chi khác", "Recurrent & other spending", "VND tn/month", "monthly (MoF cumulative)", H["exp"][V2_IDX["2026-09"]] - H["dev"][V2_IDX["2026-09"]], "2026-09",
              "Ministry of Finance via press", BUDGET_CUM["2026-09"]["url"], [e - d for e, d in zip(exp_b, dev_b)], 0, 500, 5, -W["fiscal"], None, None, 0,
              "Total spending minus development investment (incl. interest).", (add([e - d for e, d in zip(exp_b, dev_b)], -30), add([e - d for e, d in zip(exp_b, dev_b)], 30)), "same month one year earlier"),
        lever("tax_revenue", "fiscal", "Thu thuế, phí và thu khác (trừ tiền sử dụng đất)", "Taxes & other revenue (excl. land-use fees)", "VND tn/month", "monthly (MoF cumulative)", H["rev"][V2_IDX["2026-09"]] - land_base, "2026-09",
              "Ministry of Finance via press", BUDGET_CUM["2026-09"]["url"], [x - land_base for x in rev_b], 0, 500, 5, W["fiscal"], None, None, 0,
              "Revenue drains customer deposits into Treasury accounts. Split from land fees assumes land fees at the 2026 plan pace (no national monthly land-fee data).",
              (add([x - land_base for x in rev_b], -30), add([x - land_base for x in rev_b], 30)), "same month one year earlier, minus the land-fee assumption"),
        lever("land_use_fees", "fiscal", "Thu tiền sử dụng đất", "Land-use fees", "VND tn/month", "annual (plan)", land_base, "2026 plan / 12",
              "NA 2026 budget plan (data/economy.json)", "data/economy.json ECONFLOW.budget.revenue.land_use_fees", flat(land_base), 0, 150, 5, W["fiscal"], None, None, 0,
              "2026 plan 474.2 tn (2024 actual 232.9). Monthly national collections are not published: the baseline is an assumption.", (flat(20), flat(80)), "2026 plan / 12 (assumption)"),
        lever("omo_net_injection", "sbv", "NHNN bơm ròng OMO/tín phiếu", "SBV net OMO/bill injection", "VND tn/month", "monthly (VBMA month-end)", H["omo"][V2_IDX["2026-09"]], "2026-09",
              "VBMA weekly reports (finance.json)", "https://vbma.org.vn", flat(0.0), -200, 200, 10, -W["sbv"], None, 0.0, 0,
              "Assumed to act like SBV FX purchases (same per-VND weight); OMO is short-term, so a lasting effect needs the injection to be rolled over.", (flat(-50), flat(50)), "0 (outstanding unchanged)"),
        lever("sbv_fx_sales", "sbv", "NHNN bán ngoại tệ (+) / mua (−)", "SBV FX sales (+) / purchases (−)", "USD bn/month", "not published (annual BoP)", None, None,
              "BoP reserve change (data/economy.json)", "data/economy.json ECONFLOW.bop", flat(0.0), -3.0, 5.0, 0.25, wsbv, None, 0.0, 0,
              f"Each USD 1 bn sold drains {_r(wsbv,2)} VND tn of funding (estimated reserve-change weight × central rate). 2026 sales not published; 0 = beyond what the FX-flow levers imply.", (flat(-1.0), flat(2.0)), "0 (no extra intervention)"),
        lever("re_credit", "credit", "Tín dụng bất động sản tăng thêm (gồm cho vay mua nhà)", "Real-estate credit increase (incl. home loans)", "VND tn/month", "interval (SBV points)", re_latest, "2025-12..2026-06 avg",
              "SBV via press (finance.json flows)", annual["re_pts"][-1][2], flat(re_latest), 0, 250, 5, W["credit"], None, None, 0,
              "Real-estate credit 5.146 quadrillion VND at end-Jun-2026 (25.5% of credit). Same funding weight as other credit (no evidence of a different deposit return).", (flat(50), flat(160)), "latest interval average held"),
        lever("other_credit", "credit", "Tín dụng khác tăng thêm", "Other credit increase", "VND tn/month", "monthly (SBV table)", H["credit"][V2_IDX["2026-09"]] - re_latest, "2026-09",
              "SBV (finance.json monthly)", "https://sbv.gov.vn/du-no-tin-dung-doi-voi-nen-kt-dttktt", [x - re_latest for x in cred_b], -200, 600, 10, W["credit"], None, None, 0,
              "Total credit baseline = same month a year earlier (Oct-25..Mar-26 SBV table): 2026 would end at ~16% y/y vs the 15% target.", (add([x - re_latest for x in cred_b], -80), add([x - re_latest for x in cred_b], 80)), "same month one year earlier minus the real-estate baseline"),
        lever("gold_world_12m", "substitution", "Vàng thế giới, lợi suất 12 tháng", "World gold 12-month return", "%", "monthly", at(Pn, "gold_world_12m", "2026-09"), "2026-09",
              "World Bank Pink Sheet (panel)", "https://raw.githubusercontent.com/datasets/gold-prices/main/data/monthly.csv", flat(at(Pn, "gold_world_12m", "2026-09")), -30, 80, 5, None, c["gold"], neutral["gold_world_12m"], 1,
              "Estimated jointly with the funding index: ~0 once the funding gap is included (v1's gold effect was the funding gap in disguise).", (flat(0), flat(50)), "latest held"),
        lever("sjc_premium", "substitution", "Chênh lệch vàng SJC – thế giới", "SJC premium over world gold", "million VND/tael", "points", sjc_prem_sep, "2026-09 (derived)",
              "derived: SJC month-end sell − world monthly average × central rate", "panel sjc_gold_sell / gold_world_usd", flat(sjc_prem_sep), 0, 30, 0.5, None, cal["sjc_premium"]["coef"], prem_dec24, 1,
              "Derived at the central rate (press quotes at bank USD rates are lower: ~3 m on 28/8, ~9.3 m on 19/9/2026). Calibrated coefficient.", (flat(3), flat(20)), "latest held"),
        lever("vnindex_12m", "substitution", "VN-Index, lợi suất 12 tháng", "VN-Index 12-month return", "%", "monthly", at(Pn, "vnindex_12m", "2026-09"), "2026-09",
              "vnstock VCI (panel)", "https://trading.vietcap.com.vn", flat(at(Pn, "vnindex_12m", "2026-09")), -40, 80, 5, None, c["vni"], neutral["vnindex_12m"], 1,
              "Estimated: stronger stock returns pull savings out of term deposits.", (flat(-20), flat(40)), "latest held"),
        lever("re_price_momentum", "substitution", "Đà tăng giá căn hộ (Hà Nội, y/y)", "Apartment price momentum (Hanoi, y/y)", "% y/y", "quarterly/annual", SAVILLS_HN_YOY[-1]["value"], "2026-Q2",
              "Savills (finance.json)", "https://cafef.vn/chung-cu-ha-noi-vang-bong-can-ho-duoi-70-trieu-dong-m2-188260814143655062.chn", flat(SAVILLS_HN_YOY[-1]["value"]), -20, 60, 5, None, cal["re_price_momentum"]["coef"], cal["re_price_momentum"]["neutral"], 1,
              "Calibrated; not a constant-quality index.", (flat(10), flat(45)), "latest held"),
        lever("cpi_yoy", "prices", "Lạm phát CPI (y/y)", "CPI inflation (y/y)", "%", "monthly", at(Pn, "cpi_yoy", "2026-09"), "2026-09",
              "NSO (data/economy.json)", "https://www.nso.gov.vn/tin-tuc-thong-ke/2026/10/thong-cao-bao-chi-ve-tinh-hinh-gia-thang-chin-quy-iii-va-9-than", flat(at(Pn, "cpi_yoy", "2026-09")), 1, 9, 0.1, None, cal["cpi_yoy"]["coef"], neutral["cpi_yoy"], 1,
              "Calibrated (see estimation.calibrated).", (flat(3.5), flat(6.5)), "latest held"),
        lever("usdvnd_12m", "external", "Tỷ giá trung tâm, thay đổi 12 tháng", "USD/VND central rate, 12-month change", "%", "monthly", at(Pn, "usdvnd_12m", "2026-09"), "2026-09",
              "SBV central rate (finance.json)", "per finance.json usdvnd_monthly", flat(at(Pn, "usdvnd_12m", "2026-09")), -3, 10, 0.25, None, cal["usdvnd_12m"]["coef"], cal["usdvnd_12m"]["neutral"], 1,
              "Calibrated. Central rate 25,627 at end-Sep-2026; record 25,643 on 6/10/2026 (finance.json sbv.constraints).", (flat(0.0), flat(5.0)), "latest held"),
        lever("fed_change", "external", "Fed thay đổi lãi suất trong tháng", "Fed rate change in the month", "pp", "event", 0.0, "2026-09 (+0.25 on 16/9)",
              "FOMC (panel)", "https://www.federalreserve.gov/monetarypolicy/openmarket.htm", flat(0.0), -0.5, 0.5, 0.25, None, cal["fed_change"]["coef"], 0.0, 0,
              "Calibrated level shift. FOMC 27-28 Oct and 8-9 Dec 2026.", (flat(0.0), [0.25, 0, 0.25, 0, 0, 0]), "no change"),
        lever("policy_rate_change", "policy", "NHNN thay đổi lãi suất điều hành", "SBV policy-rate change", "pp", "event", 0.0, "2026-09",
              "SBV (panel refi_rate)", "data/policy.json", flat(0.0), -1.0, 1.0, 0.25, None, c["pol"], 0.0, 0,
              "Estimated pass-through to the VCB 12-month rate in the month of the change.", ([0, 0, 0, -0.5, 0, 0], [0, 0, 0.5, 0, 0, 0]), "no change"),
    ]
    model = {"params": {"r0": at(Pn, "dep12_vcb", "2026-09"), "P0": Pidx[V2_IDX["2026-09"]], "kappa": c["P"], "P_star": pstar, "D_tn": D_tn,
                        "rolloff_pp": [pp[V2_IDX[m]] for m in seas]}, "levers": levers}
    # round params once so the browser and Python use the same numbers
    model["params"] = {k: (_r(x, 6) if not isinstance(x, list) else [_r(y, 6) for y in x]) for k, x in model["params"].items()}
    base = project_v2(model)
    contrib = contributions_v2(model)
    sig = fit["sigma_month"]
    z = 1.2816
    low = [b - z * sig * math.sqrt(h + 1) for h, b in enumerate(base)]
    high = [b + z * sig * math.sqrt(h + 1) for h, b in enumerate(base)]
    tornado = []
    for lv in levers:
        lo = project_v2(model, {lv["key"]: lv["tornado"]["low"]})[5]
        hi = project_v2(model, {lv["key"]: lv["tornado"]["high"]})[5]
        tornado.append({"key": lv["key"], "low_setting": lv["tornado"]["low"], "high_setting": lv["tornado"]["high"], "rate_at_h6_low": _r(lo, 4), "rate_at_h6_high": _r(hi, 4),
                        "swing_pp": _r(abs(hi - lo), 4)})
    tornado.sort(key=lambda t: -t["swing_pp"])
    B_ = {lv["key"]: lv["baseline_path"] for lv in levers}
    lin = lambda a, b: [round(a + (b - a) * (h + 1) / 6, 4) for h in range(6)]
    scen_def = {
        "base": ({}, "Mọi đòn bẩy theo đường cơ sở: dòng tiền tín dụng và ngân sách như cùng tháng năm trước, dòng ngoại tệ theo trung bình 3 tháng gần nhất, giá vàng/cổ phiếu/CPI/tỷ giá giữ mức mới nhất, NHNN và Fed không đổi lãi suất.",
                 "All levers on their baseline: credit and budget flows as in the same month a year earlier, FX flows at their latest 3-month average, gold/stocks/CPI/FX held at latest values, no SBV or Fed rate change."),
        "easing": ({"policy_rate_change": [0, 0, 0, -0.5, 0, 0], "omo_net_injection": flat(40.0), "cpi_yoy": lin(B_["cpi_yoy"][0], 4.0)},
                   "NHNN giảm lãi suất điều hành 0,5 điểm % trong 1/2027, bơm ròng 40 nghìn tỷ/tháng qua OMO, CPI hạ dần về 4%.",
                   "SBV cuts policy rates by 0.5 pp in Jan-2027, injects a net VND 40 tn a month via OMO; CPI eases to 4%."),
        "fx_inflow_shock": ({"fdi_disbursed": [x * 0.7 for x in B_["fdi_disbursed"]], "remittances": [x * 0.75 for x in B_["remittances"]], "tourism_receipts": [x * 0.8 for x in B_["tourism_receipts"]],
                             "trade_balance": add(B_["trade_balance"], -1.5), "sbv_fx_sales": flat(1.0), "usdvnd_12m": lin(B_["usdvnd_12m"][0], 4.0)},
                            "FDI giải ngân −30%, kiều hối −25%, thu du lịch −20% so với cơ sở, nhập siêu thêm 1,5 tỷ USD/tháng; NHNN bán 1 tỷ USD/tháng để giữ tỷ giá; VND mất giá 4%/năm.",
                            "FDI disbursement −30%, remittances −25%, tourism receipts −20% vs baseline, trade balance 1.5 bn/month weaker; SBV sells USD 1 bn a month to steady the dong; 12-month depreciation rises to 4%."),
        "fiscal_push": ({"dev_investment": [b + d for b, d in zip(B_["dev_investment"], [55, 55, 55, 20, 20, 20])], "land_use_fees": add(B_["land_use_fees"], 30)},
                        "Đẩy nhanh đầu tư công: chi đầu tư phát triển tăng thêm 55 nghìn tỷ/tháng trong quý IV/2026 (tiến tới kế hoạch 1.120 nghìn tỷ) và 20 nghìn tỷ/tháng quý I/2027; đồng thời thu tiền sử dụng đất tăng thêm 30 nghìn tỷ/tháng.",
                        "Public-investment acceleration: development spending VND 55 tn a month above baseline in Q4-2026 (towards the 1,120 tn plan) and 20 tn in Q1-2027; at the same time land-use fees run 30 tn a month above baseline."),
        "real_estate_credit_boom": ({"re_credit": add(B_["re_credit"], 60), "re_price_momentum": lin(B_["re_price_momentum"][0], 40)},
                                    "Tín dụng bất động sản tăng thêm 60 nghìn tỷ/tháng so với cơ sở (khoảng 150 nghìn tỷ/tháng, như quý II/2026 của riêng tín dụng kinh doanh BĐS cộng cho vay mua nhà); giá căn hộ tăng tốc lên 40%/năm.",
                                    "Real-estate credit VND 60 tn a month above baseline (~150 tn a month); apartment-price momentum accelerates to 40% y/y."),
        "external_tightening": ({"fed_change": [0.25, 0, 0.25, 0, 0, 0], "usdvnd_12m": lin(B_["usdvnd_12m"][0], 4.5), "sbv_fx_sales": [1.5, 1.5, 1.5, 0, 0, 0], "omo_net_injection": flat(-30.0)},
                                "Fed tăng 0,25 điểm % trong 10 và 12/2026; VND mất giá 4,5%/năm; NHNN bán 1,5 tỷ USD/tháng trong quý IV và hút ròng 30 nghìn tỷ/tháng qua OMO.",
                                "Fed hikes 0.25 pp in Oct and Dec 2026; the dong's 12-month depreciation reaches 4.5%; SBV sells USD 1.5 bn a month in Q4 and drains a net VND 30 tn a month via OMO."),
    }
    scenarios = {}
    for k, (sett, avi, aen) in scen_def.items():
        pth = project_v2(model, sett)
        scenarios[k] = {"assumptions_vi": avi, "assumptions_en": aen, "lever_paths": {kk: [_r(x, 4) for x in vv] for kk, vv in sett.items()},
                        "path": [_r(x, 4) for x in pth], "change_h6_vs_base": _r(pth[5] - base[5], 4)}
    # ---- parameter sensitivity (base path at month 6) ---------------------------------------------------------------
    def with_params(**kw):
        m2 = json.loads(json.dumps(model))
        for kk, vv in kw.items():
            if kk in m2["params"]:
                m2["params"][kk] = vv
        return m2

    def scale_weights(group_keys, factor):
        m2 = json.loads(json.dumps(model))
        for lv in m2["levers"]:
            if lv["key"] in group_keys and lv["liquidity_weight"] is not None:
                lv["liquidity_weight"] *= factor
        return m2

    def addons_off():
        m2 = json.loads(json.dumps(model))
        for lv in m2["levers"]:
            if lv["key"] in ("cpi_yoy", "usdvnd_12m", "fed_change", "sjc_premium", "re_price_momentum"):
                lv["rate_coef"] = 0.0
        return m2
    fx_keys = ["fdi_disbursed", "remittances", "tourism_receipts", "trade_balance", "income_outflow"]
    fis_keys = ["dev_investment", "recurrent_spending", "tax_revenue", "land_use_fees"]
    sens = []
    k_se = fit["se"]["P"]
    for label, m2 in [("kappa − 1 SE", with_params(kappa=_r(c["P"] - k_se, 6))), ("kappa + 1 SE", with_params(kappa=_r(c["P"] + k_se, 6))),
                      ("P* from the 2025-03 fit", with_params(P_star=_r(pstar_e, 6))),
                      ("FX conversion share × 0 (FX flows irrelevant)", scale_weights(fx_keys, 0.0)), ("FX conversion share × 2", scale_weights(fx_keys, 2.0)),
                      ("fiscal weight × 0", scale_weights(fis_keys, 0.0)), ("fiscal weight × 2 (~0.68)", scale_weights(fis_keys, 2.0)),
                      ("calibrated add-ons off (CPI, USD/VND, Fed, SJC, real-estate prices)", addons_off())]:
        sens.append({"case": label, "rate_at_h6": _r(project_v2(m2)[5], 4), "vs_base": _r(project_v2(m2)[5] - base[5], 4)})
    # ---- test vectors -------------------------------------------------------------------------------------------------
    tv_cases = [("baseline", {}), ("easing", scen_def["easing"][0]), ("fx_inflow_shock", scen_def["fx_inflow_shock"][0]),
                ("mixed_custom", {"policy_rate_change": [0.25, 0, 0, 0, 0, 0], "cpi_yoy": flat(6.0), "fdi_disbursed": flat(2.0), "land_use_fees": flat(70.0),
                                  "re_credit": flat(150.0), "vnindex_12m": flat(30.0), "fed_change": [0, 0, 0.25, 0, 0, 0]}),
                ("all_liquidity_zero", {k: flat(0.0) for k in [lv["key"] for lv in levers if lv["liquidity_weight"] is not None]})]
    test_vectors = [{"id": i, "settings": s, "expected_path": [_r(x, 6) for x in project_v2(model, s)]} for i, s in tv_cases]
    # ---- observed check: model index vs SBV-basis credit-deposit y/y gap -------------------------------------------
    gap_obs = v(Pn, "credit_deposit_gap_yoy")
    chk = [{"month": m, "P_model": _r(Pidx[V2_IDX[m]], 3), "credit_minus_deposit_yoy": gap_obs[IDX[m]]} for m in MONTHS if gap_obs[IDX[m]] is not None]
    # share of listed FX inflows that ended in reserves
    tot_in = sum(r_["netfx"] for r_ in liq["rows"])
    tot_res = sum(r_["sbv_fx"] for r_ in liq["rows"])
    return {
        "H": H, "HB": HB, "FG": FG, "pp": pp, "Pidx": Pidx, "annual": annual, "model": model, "base": base, "low": low, "high": high, "contrib": contrib,
        "tornado": tornado, "scenarios": scenarios, "sens": sens, "test_vectors": test_vectors, "fit": fit, "fit_e": fit_e, "fit_lo": fit_lo, "liq": liq, "liq_oos": liq_oos,
        "W": W, "W_e": W_e, "pstar": pstar, "pstar_e": pstar_e, "pstar_lo": pstar_lo, "neutral": neutral, "oos": oos, "oos_add": oos_add, "naive": naive,
        "ep_in": ep_in, "ep_lo": ep_lo, "ep_lo_nopol": ep_lo_nopol, "replay_26": replay_26, "replay_26_add": replay_26_add, "chk": chk,
        "fx_res_share": tot_res / tot_in, "S_conv": S_conv, "wfx": wfx, "wsbv": wsbv, "sjc_prem_sep": sjc_prem_sep, "prem_dec24": prem_dec24,
    }


def v2_episode_terms(Pn, Pidx, coef, pstar, neutral, start, end):
    refi, gold, vni = v(Pn, "refi_rate"), v(Pn, "gold_world_12m"), v(Pn, "vnindex_12m")
    t = {"policy": 0.0, "funding_pressure": 0.0, "vnindex_12m": 0.0, "gold_world_12m": 0.0}
    for i in range(IDX[start] + 1, IDX[end] + 1):
        t["policy"] += coef["pol"] * (refi[i] - refi[i - 1])
        t["funding_pressure"] += coef["P"] * (Pidx[V2_IDX[MONTHS[i - 1]]] - pstar)
        t["vnindex_12m"] += coef["vni"] * (vni[i - 1] - neutral["vnindex_12m"])
        t["gold_world_12m"] += coef["gold"] * (gold[i - 1] - neutral["gold_world_12m"])
    return {k: round(x, 3) for k, x in t.items()}


def model2_json(R, Pn):
    fit, fe, fl, liq, lo = R["fit"], R["fit_e"], R["fit_lo"], R["liq"], R["liq_oos"]
    c = fit["coef"]
    m = R["model"]
    p = m["params"]
    nm = {"pol": "policy_rate_change (Δ refinancing rate, same month)", "P": "kappa: funding-pressure index P (t−1)", "gold": "world gold 12m return (t−1)",
          "vni": "VN-Index 12m return (t−1)", "c": "constant (per month)"}
    eq = ("Δr_t = {pi} × Δpolicy_t + {k} × (P_(t−1) − {ps}) + {gv} × (VNI12_(t−1) − {nv}) + {gg} × (gold12_(t−1) − {ng})"
          " + {cc} × (CPI_(t−1) − {nc}) + {cx} × (USDVND12_(t−1) − {nx}) + {cf} × ΔFed_t + {cs} × (SJCprem_(t−1) − {ns}) + {cr} × (REprice_(t−1) − {nr});"
          "  P_t = P_(t−1) + 100 × FG_t / D − (month leaving the 12-month window);"
          "  FG_t = {wc} × (RE credit + other credit) − {wf} × S/1000 × (FDI + remittances + tourism + trade balance − income outflow) − {wg} × (dev. investment + recurrent spending − taxes − land fees) − {ws} × (OMO net + S/1000 × SBV FX purchases)").format(
        pi=_r(c["pol"], 3), k=_r(c["P"], 4), ps=_r(R["pstar"], 2), gv=_r(c["vni"], 4), nv=_r(R["neutral"]["vnindex_12m"], 2), gg=_r(c["gold"], 4), ng=_r(R["neutral"]["gold_world_12m"], 2),
        cc=V2_CALIBRATED["cpi_yoy"]["coef"], nc=_r(R["neutral"]["cpi_yoy"], 2), cx=V2_CALIBRATED["usdvnd_12m"]["coef"], nx=V2_CALIBRATED["usdvnd_12m"]["neutral"], cf=V2_CALIBRATED["fed_change"]["coef"],
        cs=V2_CALIBRATED["sjc_premium"]["coef"], ns=_r(R["prem_dec24"], 2), cr=V2_CALIBRATED["re_price_momentum"]["coef"], nr=V2_CALIBRATED["re_price_momentum"]["neutral"],
        wc=_r(R["W"]["credit"], 3), wf=_r(R["W"]["fx"], 3), wg=_r(R["W"]["fiscal"], 3), ws=_r(R["W"]["sbv"], 3)).replace("+ -", "− ")
    oos_end = R["oos"][-1]
    ep_terms = v2_episode_terms(Pn, R["Pidx"], c, R["pstar"], R["neutral"], "2022-01", "2023-01")
    ep_lo_pi = v2_simulate_history(Pn, R["Pidx"], dict(fl["coef"], pol=c["pol"]), R["pstar_lo"], R["neutral"], "2022-01", "2023-01")
    chk = R["chk"]
    import statistics
    corr = None
    if len(chk) > 2:
        a_ = [x["P_model"] for x in chk]
        b_ = [x["credit_minus_deposit_yoy"] for x in chk]
        ma, mb = statistics.mean(a_), statistics.mean(b_)
        num = sum((x - ma) * (y - mb) for x, y in zip(a_, b_))
        den = math.sqrt(sum((x - ma) ** 2 for x in a_) * sum((y - mb) ** 2 for y in b_))
        corr = round(num / den, 2) if den else None
    est = {
        "liquidity": {
            "dep_var": "annual increase in SBV credit minus increase in customer deposits (IMF-FSI), VND tn",
            "regressors": {"credit": "increase in credit to the economy (SBV; pre-2022 levels chained back with finance.json credit_growth)",
                           "netfx": "(NSO FDI disbursed + BoP secondary income credit + BoP travel credit + customs goods balance + BoP net primary income) × annual average USD/VND",
                           "fiscal": "budget spending − revenue (final accounts; 2025 MoF execution estimate)",
                           "sbv_fx": "BoP reserve-asset change × annual average USD/VND (SBV FX purchases +)"},
            "coef": liq["coef"], "se": liq["se"], "r2": liq["r2"], "n": liq["n"], "sample": liq["sample"], "resid_sd_tn": liq["resid_sd_tn"], "data": liq["rows"],
            "weights_used": {k: _r(x, 4) for k, x in R["W"].items()},
            "basis": {"credit": "estimated (t≈3.8)", "fx": "estimated (t≈2.1)", "sbv": "estimated (t≈4.2)",
                      "fiscal": "point estimate used but NOT significant (t≈1.0): treat as calibrated; sensitivity ×0 / ×2",
                      "omo": "assumption: OMO net injection has the same weight as SBV FX purchases (no annual OMO series to estimate it)"},
            "stability": {"sample": lo["sample"], "coef": lo["coef"], "se": lo["se"],
                          "note": "Dropping 2025 changes the weights materially (credit 0.72, fiscal 0.76, SBV 0.36): 10 annual observations - treat weights as indicative."},
            "fx_into_reserves_share": _r(R["fx_res_share"], 3),
            "fx_into_reserves_note": "Over 2016-2025 the SBV's reserve purchases equalled ~15% of the listed net FX inflows (sum of BoP reserve change / sum of net FX inflow in VND); most of the inflow is offset by other outflows (errors & omissions −25.9 bn USD in 2025, portfolio, loans, residents' FX holdings).",
        },
        "rate_equation": {
            "method": "Interval regression: the target (VCB 12-month posted rate) is observed in irregular months, so each change between two consecutive observed months is regressed on the SUM over the interval's months of the monthly regressors (exact for a linear monthly equation).",
            "dep_var": "change in dep12_vcb between consecutive observed months (pp)",
            "coef": {k: _r(x, 5) for k, x in c.items()}, "se": {k: _r(x, 5) for k, x in fit["se"].items()}, "names": nm,
            "r2_centred": fit["r2"], "n_intervals": fit["n"], "sample": fit["sample"], "sigma_month": _r(fit["sigma_month"], 4),
            "P_star": _r(R["pstar"], 3), "neutrals_flat_window_2024_04_2025_09": {k: _r(x, 3) for k, x in R["neutral"].items()},
            "P_star_derivation": "constant = −kappa×P* − g_vni×neutral_vni − g_gold×neutral_gold  →  P* = −(constant + g_vni×12.46 + g_gold×33.37)/kappa",
            "intervals": fit["intervals"],
            "verdict_en": (f"Estimated: policy pass-through {_r(c['pol'],2)} (SE {_r(fit['se']['pol'],2)}), funding-pressure kappa {_r(c['P'],4)} pp per month per pp of P (SE {_r(fit['se']['P'],4)}, t≈{_r(c['P']/fit['se']['P'],1)}), "
                           f"VN-Index {_r(c['vni'],4)} (SE {_r(fit['se']['vni'],4)}); world gold ≈0 once the funding index is in (v1's gold effect was the funding gap in disguise). Fit to 2025-03 the neutral P* moves from {_r(R['pstar'],2)} to {_r(R['pstar_e'],2)}: "
                           "the level of 'normal' funding pressure is the least certain number in the model. CPI, USD/VND, Fed, SJC premium and real-estate prices cannot be estimated (gaps, wrong signs) and are calibrated add-ons."),
            "verdict_vi": (f"Ước lượng: truyền dẫn lãi suất điều hành {_r(c['pol'],2)} (SE {_r(fit['se']['pol'],2)}), hệ số áp lực vốn kappa {_r(c['P'],4)} điểm %/tháng cho mỗi điểm % của P (SE {_r(fit['se']['P'],4)}), "
                           f"VN-Index {_r(c['vni'],4)}; vàng thế giới ≈0 khi đã có chỉ số áp lực vốn (tác động 'vàng' ở v1 thực chất là thiếu hụt vốn). Ước lượng đến 3/2025 thì mức trung tính P* đổi từ {_r(R['pstar'],2)} lên {_r(R['pstar_e'],2)}: "
                           "mức áp lực vốn 'bình thường' là con số kém chắc chắn nhất. CPI, tỷ giá, Fed, chênh lệch SJC và giá nhà không ước lượng được (thiếu số liệu, sai dấu) nên được hiệu chỉnh."),
        },
        "rate_equation_fit_to_2025_03": {"coef": {k: _r(x, 5) for k, x in fe["coef"].items()}, "se": {k: _r(x, 5) for k, x in fe["se"].items()}, "r2_centred": fe["r2"],
                                         "n_intervals": fe["n"], "sample": fe["sample"], "P_star": _r(R["pstar_e"], 3), "liquidity_weights": {k: _r(x, 4) for k, x in R["W_e"].items()}},
        "calibrated": {k: {"coef": d["coef"], "neutral": (_r(R["neutral"]["cpi_yoy"], 3) if k == "cpi_yoy" else (_r(R["prem_dec24"], 2) if k == "sjc_premium" else d["neutral"])),
                           "lag": d["lag"], "basis": d["basis"], "sensitivity_range": d["sens"]} for k, d in V2_CALIBRATED.items()},
        "estimated_vs_calibrated": {
            "estimated": ["liquidity weights: credit, net FX inflow (conversion share), SBV FX purchases (annual 2016-2025)", "policy pass-through, kappa, P*, VN-Index, world gold (monthly interval regression 2021-2026)"],
            "weakly_estimated_used_as_calibrated": ["fiscal weight (t≈1.0)"],
            "assumed": ["OMO net injection weight = SBV FX purchase weight", "deposit base D fixed at end-2025 (IMF-FSI) for the projection", "conversion at the Sep-2026 central rate"],
            "calibrated": list(V2_CALIBRATED.keys()),
        },
        "observed_gap_check": {"note": "Model index P vs the SBV-basis credit-minus-deposit y/y gap (different construction; P excludes the regression constant, the gap includes it). Correlation over the overlap shown; the observed gap exists only Dec-2025..Jul-2026.",
                               "correlation": corr, "months": chk},
        "data_frequency_methods": {"annual_even": "annual total / 12 for each month of the year (2020-2024 FX, fiscal, credit 2020/2024; BoP items through 2025)",
                                   "interval_avg_<k>m": "when an intermediate cumulative figure was not found, the k-month total between two published cumulatives is spread evenly",
                                   "interval_avg_statements": "2021-2023 credit: increase between two dated SBV statements spread evenly over the months between them (cut-off date mapped to its calendar month)",
                                   "9M_even": "2026 tourism receipts: 9-month total / 9",
                                   "assumption_carry": "2026 remittances and income outflow: 2025 monthly average (no 2026 national data)",
                                   "step": "Savills y/y applied until the next published value"},
    }
    backtest = {
        "out_of_sample": {"kind": "true out-of-sample: liquidity weights estimated on 2016-2024, rate equation fitted to intervals ending ≤ 2025-03, then a dynamic monthly simulation 2025-04..2026-09 from the observed 4.6% using actual levers (no re-fitting)",
                          "start": {"month": "2025-03", "value": at(Pn, "dep12_vcb", "2025-03")},
                          "path": R["oos"], "rmse": _rmse(R["oos"]), "end_error_pp": _r(oos_end["predicted"] - oos_end["actual"], 3),
                          "naive_no_change_rmse": _rmse(R["naive"]),
                          "with_calibrated_addons": {"path": R["oos_add"], "rmse": _rmse(R["oos_add"]), "note": "CPI, USD/VND (from Oct-2025), Fed change and real-estate momentum add-ons applied where history exists; SJC premium has no monthly history"},
                          "v1_comparison": "v1's out-of-sample test (gold + VN-Index equation fitted to 2025-03) only covered the flat 2025-04..09 window (RMSE 0.053) and could not test the 2026 rise.",
                          "summary_en": (f"Fitted only on data to Mar-2025, the structural model predicts a rise from 4.6% to {oos_end['predicted']}% by Sep-2026 (actual 5.9%): right direction, about "
                                         f"{int(round(100 * (oos_end['predicted'] - 4.6) / 1.3))}% of the size, but about half a year late (Big-4 banks moved in Jan-Mar 2026; the model's pressure builds through mid-2026 and reaches 5.5% only in Sep-2026). RMSE {_rmse(R['oos'])} pp vs {_rmse(R['naive'])} for 'no change'."),
                          "summary_vi": (f"Chỉ dùng số liệu đến 3/2025, mô hình dự báo lãi suất tăng từ 4,6% lên {str(oos_end['predicted']).replace('.', ',')}% vào 9/2026 (thực tế 5,9%): đúng hướng, khoảng "
                                         f"{int(round(100 * (oos_end['predicted'] - 4.6) / 1.3))}% độ lớn nhưng trễ khoảng nửa năm (Big4 tăng trong 1-3/2026, mô hình chỉ đạt 5,5% vào 9/2026). RMSE {str(_rmse(R['oos'])).replace('.', ',')} điểm % so với {str(_rmse(R['naive'])).replace('.', ',')} nếu giả định 'không đổi'.")},
        "episode_2022_q4": {"in_sample_replay": {"path": R["ep_in"], "terms_sum_pp": ep_terms,
                                                  "note": "full-sample coefficients, dynamic from Jan-2022 (5.5%); observed: 6.4% (Sep-2022), 7.4% (Jan-2023). In-sample: the 2022 intervals are in the estimation."},
                            "leave_episode_out": {"fit": {"coef": {k: _r(x, 5) for k, x in fl["coef"].items()}, "r2_centred": fl["r2"], "n_intervals": fl["n"], "P_star": _r(R["pstar_lo"], 3)},
                                                  "path": R["ep_lo"], "path_with_full_sample_pass_through": ep_lo_pi,
                                                  "note": "Intervals overlapping Feb..Dec-2022 removed. Without them the pass-through is not identified (the only other policy moves, the 2023 cuts, coincide with falling funding pressure), and the 2022 stock crash pulls the predicted rate down: the model cannot reproduce the 2022 spike without seeing it. With the full-sample pass-through imposed, see path_with_full_sample_pass_through."},
                            "summary_en": "The 2022-Q4 spike is mainly the policy hikes (+2 pp refinancing) with rising funding pressure (P from ~3 to ~7 pp during 2022, SBV selling ~23 bn USD of reserves); the stock crash worked the other way. The episode is not predictable out-of-sample without the policy reaction.",
                            "summary_vi": "Đợt tăng quý IV/2022 chủ yếu do NHNN tăng lãi suất điều hành (+2 điểm %) cộng áp lực vốn tăng (P từ ~3 lên ~7 điểm % trong 2022, NHNN bán ~23 tỷ USD dự trữ); chứng khoán giảm sâu tác động ngược lại. Không dự báo được ngoài mẫu nếu không biết trước phản ứng chính sách."},
        "in_sample_replay_2025_10_2026_09": {"path": R["replay_26"], "rmse": _rmse(R["replay_26"]), "with_addons": R["replay_26_add"], "with_addons_rmse": _rmse(R["replay_26_add"]),
                                             "note": "In-sample. The model is above the posted VCB rate from Jul-2026 (6.25% vs 5.9% in Sep): Big-4 posted rates were held while private banks paid ~8.4% (MBS, Aug-2026) - a sign of administrative restraint the model does not capture; the projection inherits this upward tilt."},
    }
    levers = m["levers"]
    model2 = {
        "version": "v2.0 (2026-10-07)",
        "method_en": ("Structural monthly VND funding model. (A) Each month's funding gap FG (VND tn) = credit growth not matched by new deposits − the part of net FX inflows that becomes deposits (conversion share) − the fiscal net injection − SBV liquidity (FX purchases, OMO), with weights estimated on 2016-2025 annual data. "
                      "(B) The funding-pressure index P is the 12-month sum of FG in % of deposits. (C) The VCB 12-month deposit rate changes each month by pass-through × policy-rate change + kappa × (P last month − neutral P*) + substitution, inflation and external terms. "
                      "Pass-through, kappa, P* and the stock-return term are estimated by interval regression on 2021-2026; inflation, USD/VND, Fed, SJC premium and real-estate prices are calibrated (see estimation). v1 (calibrated 4-driver model) is kept in SIM.model for comparison."),
        "method_vi": ("Mô hình cấu trúc theo tháng về nguồn vốn VND. (A) Thiếu hụt vốn hằng tháng FG (nghìn tỷ) = phần tín dụng tăng không có tiền gửi tương ứng − phần dòng ngoại tệ ròng chuyển thành tiền gửi (tỷ lệ chuyển đổi) − bơm ròng ngân sách − thanh khoản NHNN (mua ngoại tệ, OMO); trọng số ước lượng từ số liệu năm 2016-2025. "
                      "(B) Chỉ số áp lực vốn P = tổng FG 12 tháng, tính theo % tiền gửi. (C) Lãi suất 12 tháng của VCB thay đổi mỗi tháng = hệ số truyền dẫn × thay đổi lãi suất điều hành + kappa × (P tháng trước − mức trung tính P*) + các yếu tố thay thế, lạm phát, bên ngoài. "
                      "Truyền dẫn, kappa, P* và yếu tố cổ phiếu được ước lượng (hồi quy theo khoảng 2021-2026); lạm phát, tỷ giá, Fed, chênh lệch SJC và giá nhà được hiệu chỉnh. Mô hình v1 giữ trong SIM.model để so sánh."),
        "equation": eq,
        "target": {"series": "dep12_vcb", "definition": "Vietcombank 12-month posted VND savings rate (counter), month-end; start 5.9% (Sep-2026)"},
        "params": p,
        "params_note": {"r0": "VCB 12M at end-Sep-2026", "P0": "funding-pressure index at Sep-2026 (Aug-Sep partly nowcast: credit from rounded statements, remittances/income/SBV FX assumed)",
                        "kappa": "estimated (rate_equation.coef.P)", "P_star": "estimated neutral P", "D_tn": "customer deposits end-2025 (IMF-FSI), VND tn",
                        "rolloff_pp": "100×FG/D of Oct-2025..Mar-2026, the months that leave the 12-month window in Oct-2026..Mar-2027"},
        "levers": levers,
        "lever_groups": {g: [lv["key"] for lv in levers if lv["group"] == g] for g in ["fx", "fiscal", "sbv", "credit", "substitution", "prices", "external", "policy"]},
        "liquidity": {"neutral_gap": _r(R["pstar"], 4), "neutral_gap_unit": "pp of deposits (12-month funding-gap sum, constant excluded)", "kappa": p["kappa"],
                      "conversion_share": _r(R["W"]["fx"], 4), "conversion_share_note": f"VND tn of funding gap removed per VND tn of net FX inflow (estimated, SE {liq['se']['netfx']}); per USD 1 bn at {R['S_conv']} VND/USD = {_r(R['wfx'],3)} VND tn. Separately, SBV FX purchases remove {_r(R['W']['sbv'],3)} per VND (SE {liq['se']['sbv_fx']}); only ~15% of listed inflows ended in reserves in 2016-2025.",
                      "credit_share": _r(R["W"]["credit"], 4), "fiscal_share": _r(R["W"]["fiscal"], 4), "sbv_share": _r(R["W"]["sbv"], 4),
                      "usdvnd_for_conversion": R["S_conv"], "deposit_base_tn": p["D_tn"], "P0": p["P0"], "rolloff_pp": p["rolloff_pp"],
                      "history_P": {"months": MONTHS, "values": [_r(R["Pidx"][V2_IDX[mm]], 3) for mm in MONTHS]},
                      "history_FG_tn": {"months": MONTHS, "values": [_r(R["FG"][V2_IDX[mm]], 2) for mm in MONTHS]}},
        "compute_order": [
            "1. Start from r = params.r0 (VCB 12M, Sep-2026) and P = params.P0 (funding-pressure index, Sep-2026).",
            "2. For each projection month h = 0..5 (Oct-2026..Mar-2027): rate change dr = kappa × (P − P_star).",
            "3. Add each rate lever: rate_coef × (x − neutral), where x = the lever's value LAST month (lag 1: latest_value for h = 0, path[h−1] after) or THIS month (lag 0: policy_rate_change, fed_change).",
            "4. r = r + dr; record r for month h.",
            "5. Update the funding index for next month: P = P + Σ over liquidity levers of 100 × liquidity_weight × path[h] / D_tn − rolloff_pp[h].",
            "6. Repeat. Bands: base ± 1.2816 × residual_sd × √(h+1). Contributions: SIM.projection2.contributions (sum = path − r0).",
        ],
        "estimation": est,
        "backtest": backtest,
        "residual_sd": _r(fit["sigma_month"], 4),
        "band_method": "80% band = base ± 1.2816 × σ × √h, σ = per-month residual s.d. of the interval regression (σ² = mean of e²/interval length) - parameter uncertainty (e.g. P*) is shown separately in projection2.sensitivity, not in the band.",
        "test_vectors": R["test_vectors"],
        "test_vector_note": "expected_path is unrounded (6 decimals). A JS port of project_v2 using params and levers from this file must reproduce each path to 1e-6. settings override baseline_path for the listed levers; others stay on baseline.",
        "not_modelled": [
            {"key": "ldr_cap_95_from_2026_12", "why_en": "Circular 50/2026 raises the LDR cap to 95% on 1 Dec 2026 and 50% of Treasury term deposits count as funding from 1 Aug 2026: regulatory room, not new deposits; no history to estimate its price effect - discuss as an easing risk.", "why_vi": "Nâng trần LDR lên 95% (1/12/2026) và tính 50% tiền gửi KBNN: tạo dư địa pháp lý, không phải tiền gửi mới; chưa có lịch sử để ước lượng."},
            {"key": "cash_leakage", "why_en": "Cash/M2 exists only from 2025 (methodology break Oct-2025); seasonal Tet swings (±150-200 tn) are visible but not in the annual regression.", "why_vi": "Tiền mặt/M2 chỉ có từ 2025, có gãy phương pháp 10/2025."},
            {"key": "gov_bond_issuance / corporate & bank bonds", "why_en": "Bank bond issuance substitutes for deposits and G-bond purchases absorb bank liquidity; monthly series incomplete before 2026.", "why_vi": "Trái phiếu ngân hàng thay thế tiền gửi, mua TPCP hút thanh khoản; thiếu chuỗi tháng trước 2026."},
            {"key": "administrative_guidance", "why_en": "SBV moral suasion on Big-4 posted rates (e.g. 2022 H1 and 2026 H2) keeps VCB below market pressure; not quantifiable.", "why_vi": "Chỉ đạo hành chính của NHNN với Big4 giữ lãi suất niêm yết thấp hơn áp lực thị trường; không định lượng được."},
        ],
    }
    return model2


def projection2_json(R):
    m = R["model"]
    base = R["base"]
    keys = [lv["key"] for lv in m["levers"]] + ["inherited_pressure", "funding_rolloff"]
    contrib = {k: [_r(x, 4) for x in R["contrib"][k]] for k in keys}
    total = [b - m["params"]["r0"] for b in base]
    check = [abs(sum(R["contrib"][k][h] for k in keys) - total[h]) for h in range(6)]
    return {
        "months": PROJ_MONTHS,
        "start": {"month": "2026-09", "value": m["params"]["r0"], "series": "dep12_vcb"},
        "base": [_r(x, 4) for x in base], "low": [_r(x, 4) for x in R["low"]], "high": [_r(x, 4) for x in R["high"]],
        "band": f"80% band = base ± 1.2816 × {round(R['fit']['sigma_month'], 4)} pp × √h (interval-regression residual s.d. per month)",
        "contributions": contrib,
        "contributions_note": "Cumulative pp vs the start (5.9%) for each month. Liquidity levers act through kappa × their accumulated addition to P (so their effect starts the month after); rate levers act directly. 'inherited_pressure' = kappa × (P0 − P*) each month (pressure already built up to Sep-2026); 'funding_rolloff' = the months of Oct-2025..Mar-2026 leaving the 12-month window. Sum over keys = base − start.",
        "contributions_check_max_abs_error": _r(max(check), 10),
        "tornado": R["tornado"],
        "tornado_note": "Each lever alone moved to its low/high setting (flat or baseline ± delta, see settings) with all others on baseline; rate at Mar-2027. Sorted by swing.",
        "scenarios": R["scenarios"],
        "sensitivity": R["sens"],
        "sensitivity_note": "Parameter uncertainty not in the band. The neutral P* is the biggest: with the P* estimated only on data to Mar-2025, the base path is roughly flat.",
        "kind": "dashboard model — not a forecast",
        "note_vi": "Mô hình cấu trúc của dashboard, không phải dự báo. Đường cơ sở dựa trên giả định đòn bẩy giữ như cùng kỳ hoặc mức mới nhất; mô hình có xu hướng cao hơn lãi suất niêm yết của Big4 khi NHNN chỉ đạo giữ lãi suất. Không phải khuyến nghị đầu tư.",
        "note_en": "Structural dashboard model, not a forecast. The base path assumes levers stay at same-month-last-year or latest values; the model tends to sit above Big-4 posted rates when the SBV guides banks to hold them. Not investment advice.",
    }


def v2_attach_panel(Pn, R):
    S = v2_panel_series(R["H"], R["HB"], R["FG"], R["pp"], R["Pidx"], R["annual"])
    # SJC premium: monthly 2026 derived points (same convention as the year-end points) + press quotes
    cen = dict(zip(MONTHS, v(Pn, "usdvnd_central")))
    gw = dict(zip(MONTHS, v(Pn, "gold_world_usd")))
    sj = dict(zip(MONTHS, v(Pn, "sjc_gold_sell")))
    pts = []
    for mm in MONTHS:
        if mm >= "2026-02" and sj.get(mm) and gw.get(mm) and cen.get(mm):
            w = gw[mm] * cen[mm] * 37.5 / 31.1035 / 1e6
            pts.append({"date": mm, "sjc": sj[mm], "world_converted": round(w, 2), "premium_mvnd": round(sj[mm] - w, 2)})
    Pn["series"]["sjc_premium"]["monthly_2026"] = {"points": pts, "press_quotes": SJC_PREMIUM_PRESS,
                                                   "note": "SJC month-end sell − world monthly AVERAGE × month-end central rate (approximate; same convention as the year-end points). Press quotes use spot world prices and bank USD rates and are lower (conflict of conventions, not of data)."}
    for k, s in S.items():
        Pn["series"][k] = s
    for g in ("fx", "sbv", "liquidity"):
        if g not in Pn["groups"]:
            Pn["groups"].append(g)
    Pn["lever_series"] = {"fx": ["fdi_disbursed_m", "remittances_bop_m", "tourism_receipts_m", "trade_balance_m", "income_outflow_m", "fx_net_inflow_vnd_m", "usdvnd_central", "usdvnd_12m"],
                          "fiscal": ["budget_revenue_m", "budget_spending_m", "dev_investment_m", "fiscal_net_injection_m", "treasury_deposits", "public_investment"],
                          "sbv": ["sbv_omo_net_m", "sbv_fx_reserves_change_m", "refi_rate", "omo_rate"],
                          "credit": ["credit_flow_m", "re_credit_flow_m", "credit_ytd", "credit_target"],
                          "substitution": ["gold_world_12m", "sjc_premium", "vnindex_12m", "new_stock_accounts", "margin_lending", "re_price_momentum"],
                          "prices": ["cpi_yoy", "core_cpi_yoy", "brent"],
                          "external": ["fed_funds_upper", "usdvnd_12m", "dxy"],
                          "liquidity": ["funding_gap_fg_m", "funding_pressure_index", "credit_deposit_gap_yoy"]}


# --------------------------------------------------------------------------------------------
# Story
# --------------------------------------------------------------------------------------------
def fv(x, k=1):
    """Vietnamese decimal comma."""
    s = f"{x:,.{k}f}"
    return s.replace(",", "§").replace(".", ",").replace("§", ".")


def fe(x, k=1):
    return f"{x:,.{k}f}"


def story(P):
    S = P["series"]
    g = lambda k, m: at(P, k, m)
    ag = S["annual_growth"]
    A = lambda field, y: ag[field][ag["periods"].index(y)]
    pi = {o["period"]: o for o in S["public_investment"]["observations"]}
    st = S["public_investment"]["state_sector_investment_nso"]
    stv = dict(zip(st["periods"], st["values_bn"]))
    mg = {o["date"]: o["value"] for o in S["margin_lending"]["observations"]}
    sjc = {o["date"]: o["value"] for o in S["sjc_gold_sell"]["observations"]}
    acc = {o["period"]: o["value"] for o in S["new_stock_accounts"]["annual"]}
    hn = {o["period"]: o["value"] for o in S["real_estate"]["series"]["hanoi_primary_apartment_price_mvnd_m2"]}
    sup = S["real_estate"]["series"]["supply_2025"]
    ib = {o["date"]: o["value"] for o in S["interbank_on"]["observations"]}
    gb = {o["date"]: o["value"] for o in S["gov_bond_10y"]["observations"]}
    cof = dict(zip(S["cof_listed"]["periods"], S["cof_listed"]["values"]))
    tr = {o["date"]: o["value"] for o in S["treasury_deposits"]["observations"]}
    mbs = S["dep12_private_mbs"]["values"]
    gap26 = [x for m, x in zip(MONTHS, v(P, "credit_deposit_gap_yoy")) if x is not None and m >= "2026-01"]
    vni_ye = lambda m: g("vnindex", m)
    pct = lambda a, b: (a / b - 1) * 100
    beats = []

    # 1
    beats.append({
        "id": "low_rates_2021", "period_from": "2021-01", "period_to": "2022-08",
        "title_vi": "2021–2022: lãi suất thấp kỷ lục", "title_en": "2021-22: record-low rates",
        "text_vi": (f"Lãi suất tái cấp vốn đứng ở {fv(g('refi_rate','2021-01'))}% từ 10/2020 đến 9/2022 và lãi suất 12 tháng của Vietcombank chỉ {fv(g('dep12_vcb','2021-01'))}% (1/2021) rồi {fv(g('dep12_vcb','2022-01'))}% (1/2022), "
                    f"trong khi lãi suất liên ngân hàng qua đêm quanh {fv(ib['2021-07-30'])}% (30/7/2021) và Fed giữ {fv(g('fed_funds_upper','2021-06'),2)}%. "
                    f"Với CPI chỉ {fv(g('cpi_yoy','2021-12'),2)}% (12/2021), gửi tiết kiệm vẫn có lãi thực khoảng {fv(g('real_dep_rate','2021-12'),1)} điểm %, nhưng VN-Index tăng {fv(g('vnindex_12m','2021-12'),1)}% trong năm 2021."),
        "text_en": (f"The refinancing rate sat at {fe(g('refi_rate','2021-01'))}% from Oct-2020 to Sep-2022 and Vietcombank's 12-month rate was only {fe(g('dep12_vcb','2021-01'))}% (Jan-21) and {fe(g('dep12_vcb','2022-01'))}% (Jan-22), "
                    f"with overnight interbank money near {fe(ib['2021-07-30'])}% (30 Jul 2021) and the Fed at {fe(g('fed_funds_upper','2021-06'),2)}%. "
                    f"With CPI at just {fe(g('cpi_yoy','2021-12'),2)}% (Dec-21), savers still earned a real {fe(g('real_dep_rate','2021-12'),1)} pp, while the VN-Index gained {fe(g('vnindex_12m','2021-12'),1)}% in 2021."),
        "highlight_series": ["refi_rate", "dep12_vcb", "interbank_on", "cpi_yoy", "fed_funds_upper"],
        "evidence": [{"series": "dep12_vcb", "from_value": g("dep12_vcb", "2021-01"), "to_value": g("dep12_vcb", "2022-01"), "period": "2021-01..2022-01"},
                     {"series": "refi_rate", "from_value": g("refi_rate", "2021-01"), "to_value": g("refi_rate", "2022-08"), "period": "2021-01..2022-08"},
                     {"series": "cpi_yoy", "from_value": g("cpi_yoy", "2021-04"), "to_value": g("cpi_yoy", "2021-12"), "period": "2021-04..2021-12"}],
        "data_check": {"supports": "yes", "note_en": "Deposit rates were low (5.5-5.6% vs 6.8% in Jan-2020 per finance.json). No lending-rate series exists for 2021-22 in reach, so the 'low lending rates' part is not shown.",
                       "note_vi": "Lãi suất huy động thấp (5,5–5,6% so với 6,8% tháng 1/2020). Không có chuỗi lãi suất cho vay 2021–22 nên phần 'lãi suất cho vay thấp' không được thể hiện."}})
    # 2
    beats.append({
        "id": "money_to_assets", "period_from": "2021-01", "period_to": "2022-03",
        "title_vi": "Tiền chảy vào cổ phiếu, vàng; tiền gửi chậm lại", "title_en": "Money flows into stocks and gold; deposits slow",
        "text_vi": (f"VN-Index lên {fv(vni_ye('2021-12'),0)} điểm cuối 2021 (+{fv(g('vnindex_12m','2021-12'),1)}%); nhà đầu tư mở hơn {fv(acc['2021']/1e6,1)} triệu tài khoản năm 2021 và gần {fv(acc['2022']/1e6,1)} triệu năm 2022, dư nợ cho vay của công ty chứng khoán đạt {fv(mg['2022-03']/1000,1)} nghìn tỷ đồng (quý I/2022). "
                    f"Tiền gửi chỉ tăng {fv(A('deposit',2021),1)}% năm 2021 (2020: {fv(A('deposit',2020),1)}%) và {fv(A('deposit',2022),2)}% năm 2022, trong khi tín dụng tăng {fv(A('credit',2021),1)}% và {fv(A('credit',2022),1)}%."),
        "text_en": (f"The VN-Index closed 2021 at {fe(vni_ye('2021-12'),0)} (+{fe(g('vnindex_12m','2021-12'),1)}%); investors opened over {fe(acc['2021']/1e6,1)} million accounts in 2021 and nearly {fe(acc['2022']/1e6,1)} million in 2022, and securities firms' loans reached VND {fe(mg['2022-03']/1000,1)} trn (Q1-22). "
                    f"Deposits grew only {fe(A('deposit',2021),1)}% in 2021 (2020: {fe(A('deposit',2020),1)}%) and {fe(A('deposit',2022),2)}% in 2022, while credit grew {fe(A('credit',2021),1)}% and {fe(A('credit',2022),1)}%."),
        "highlight_series": ["vnindex", "vnindex_12m", "new_stock_accounts", "margin_lending", "annual_growth"],
        "evidence": [{"series": "vnindex", "from_value": g("vnindex", "2021-01"), "to_value": g("vnindex", "2021-12"), "period": "2021-01..2021-12"},
                     {"series": "annual_growth.deposit", "from_value": A("deposit", 2020), "to_value": A("deposit", 2022), "period": "2020..2022"},
                     {"series": "margin_lending", "from_value": None, "to_value": mg["2022-03"], "period": "2022-03"}],
        "data_check": {"supports": "partly", "note_en": "Deposits did slow (13.3% -> 10.3% -> 5.99%), but 2022's 5.99% also reflects the late-2022 shock; real-estate price data for 2021 is missing (Hanoi Savills Q4-2021 y/y not found), so the real-estate leg is not evidenced here.",
                       "note_vi": "Tiền gửi chậm lại thật (13,3% → 10,3% → 5,99%), nhưng mức 5,99% năm 2022 còn do cú sốc cuối 2022; thiếu số giá BĐS 2021 nên vế bất động sản chưa có bằng chứng ở đây."}})
    # 3
    beats.append({
        "id": "shock_2022", "period_from": "2022-03", "period_to": "2023-01",
        "title_vi": "2022: cú sốc trái phiếu, SCB, tỷ giá", "title_en": "2022: bond, SCB and FX shock",
        "text_vi": (f"Fed nâng lãi suất từ {fv(g('fed_funds_upper','2022-02'),2)}% lên {fv(g('fed_funds_upper','2022-12'),2)}%, tỷ giá trung tâm tăng {fv(g('usdvnd_12m','2022-12'),2)}% trong năm, phát hành trái phiếu riêng lẻ chỉ còn {fv(CORP_BOND[0]['private_bn']/1000,0)} nghìn tỷ đồng (−65%) và SCB bị rút tiền từ 7/10/2022 trước khi bị kiểm soát đặc biệt. "
                    f"NHNN nâng tái cấp vốn từ {fv(g('refi_rate','2022-08'))}% lên {fv(g('refi_rate','2022-12'))}%, lãi suất qua đêm vọt lên {fv(ib['2022-10-04'],2)}% (4/10/2022), TPCP 10 năm lên {fv(gb['2022-12'],1)}% và lãi suất 12 tháng Vietcombank lên {fv(g('dep12_vcb','2023-01'))}% (1/2023)."),
        "text_en": (f"The Fed hiked from {fe(g('fed_funds_upper','2022-02'),2)}% to {fe(g('fed_funds_upper','2022-12'),2)}%, the central rate rose {fe(g('usdvnd_12m','2022-12'),2)}% over the year, private-placement bond issuance shrank to VND {fe(CORP_BOND[0]['private_bn']/1000,0)} trn (−65%) and SCB faced a run from 7 Oct 2022 before being placed under special control. "
                    f"The SBV lifted the refinancing rate from {fe(g('refi_rate','2022-08'))}% to {fe(g('refi_rate','2022-12'))}%, overnight interbank hit {fe(ib['2022-10-04'],2)}% (4 Oct 2022), the 10-year bond {fe(gb['2022-12'],1)}% and Vietcombank's 12-month rate {fe(g('dep12_vcb','2023-01'))}% (Jan-23)."),
        "highlight_series": ["refi_rate", "interbank_on", "gov_bond_10y", "dep12_vcb", "fed_funds_upper", "usdvnd_12m", "margin_lending", "vnindex"],
        "evidence": [{"series": "refi_rate", "from_value": g("refi_rate", "2022-08"), "to_value": g("refi_rate", "2022-12"), "period": "2022-08..2022-12"},
                     {"series": "dep12_vcb", "from_value": g("dep12_vcb", "2022-01"), "to_value": g("dep12_vcb", "2023-01"), "period": "2022-01..2023-01"},
                     {"series": "gov_bond_10y", "from_value": gb["2021-12"], "to_value": gb["2022-12"], "period": "2021-12..2022-12"},
                     {"series": "margin_lending", "from_value": mg["2022-03"], "to_value": mg["2022-12"], "period": "2022-03..2022-12"},
                     {"series": "vnindex", "from_value": vni_ye("2021-12"), "to_value": vni_ye("2022-12"), "period": "2021-12..2022-12"}],
        "context_sources": ["https://www.rfa.org/vietnamese/news/vietnamnews/vn-central-bank-places-scb-under-special-scrutiny-10152022084115.html",
                            "https://thitruongtaichinhtiente.vn/ngan-hang-nha-nuoc-khang-dinh-se-giu-vung-on-dinh-cua-scb-noi-rieng-va-he-thong-cac-tctd-noi-chung-42618.html"],
        "data_check": {"supports": "yes", "note_en": "Monthly VCB values for Oct-Dec 2022 are missing (step to 7.4% not dated); the jump is shown Sep-22 (6.4%) -> Jan-23 (7.4%).",
                       "note_vi": "Thiếu giá trị VCB tháng 10–12/2022; bước nhảy được thể hiện 9/2022 (6,4%) → 1/2023 (7,4%)."}})
    # 4
    beats.append({
        "id": "cuts_2023_24", "period_from": "2023-03", "period_to": "2025-09",
        "title_vi": "2023–2025: cắt giảm lãi suất, tín dụng vượt huy động", "title_en": "2023-25: rate cuts, credit outruns deposits",
        "text_vi": (f"NHNN hạ tái cấp vốn từ {fv(g('refi_rate','2023-03'))}% xuống {fv(g('refi_rate','2023-06'))}% (6/2023), qua đêm liên ngân hàng chỉ còn {fv(ib['2023-09-20'],2)}% (20/9/2023) và lãi suất 12 tháng Vietcombank giảm từ {fv(g('dep12_vcb','2023-01'))}% xuống {fv(g('dep12_vcb','2024-04'))}% (4/2024), đứng yên đến 9/2025. "
                    f"Năm 2023 huy động ({fv(A('deposit',2023),1)}%) vẫn theo kịp tín dụng ({fv(A('credit',2023),2)}%), nhưng năm 2024 tín dụng tăng {fv(A('credit',2024),2)}% so với huy động {fv(A('deposit',2024),2)}% và dư nợ margin tăng từ {fv(mg['2023-12']/1000,0)} lên {fv(mg['2024-12']/1000,1)} nghìn tỷ đồng."),
        "text_en": (f"The SBV cut the refinancing rate from {fe(g('refi_rate','2023-03'))}% to {fe(g('refi_rate','2023-06'))}% (Jun-23), overnight interbank fell to {fe(ib['2023-09-20'],2)}% (20 Sep 2023) and Vietcombank's 12-month rate dropped from {fe(g('dep12_vcb','2023-01'))}% to {fe(g('dep12_vcb','2024-04'))}% (Apr-24), where it stayed until Sep-25. "
                    f"In 2023 deposits ({fe(A('deposit',2023),1)}%) still kept pace with credit ({fe(A('credit',2023),2)}%), but in 2024 credit grew {fe(A('credit',2024),2)}% against deposits' {fe(A('deposit',2024),2)}% and margin loans rose from VND {fe(mg['2023-12']/1000,0)} to {fe(mg['2024-12']/1000,1)} trn."),
        "highlight_series": ["refi_rate", "dep12_vcb", "interbank_on", "annual_growth", "margin_lending"],
        "evidence": [{"series": "dep12_vcb", "from_value": g("dep12_vcb", "2023-01"), "to_value": g("dep12_vcb", "2025-09"), "period": "2023-01..2025-09"},
                     {"series": "annual_growth.gap", "from_value": A("gap", 2023), "to_value": A("gap", 2024), "period": "2023..2024"},
                     {"series": "margin_lending", "from_value": mg["2023-12"], "to_value": mg["2024-12"], "period": "2023-12..2024-12"}],
        "data_check": {"supports": "partly", "note_en": "'Deposits sluggish vs credit' holds for 2024 (gap 4.43 pp) but not 2023 (gap 0.58 pp): high 2022 rates kept money in banks during 2023.",
                       "note_vi": "'Huy động chậm hơn tín dụng' đúng cho 2024 (lệch 4,43 điểm %) nhưng không đúng cho 2023 (0,58 điểm %): lãi suất cao 2022 giữ tiền trong ngân hàng suốt 2023."}})
    # 5
    gap25_sbv = g("credit_deposit_gap_yoy", "2025-12")
    beats.append({
        "id": "boom_2025", "period_from": "2025-01", "period_to": "2025-12",
        "title_vi": "2025: đầu tư công bùng nổ, nhà đất và vàng tăng nóng", "title_en": "2025: public-investment surge, housing and gold boom",
        "text_vi": (f"Giải ngân đầu tư công năm 2025 đạt {fv(pi['2025']['value_bn']/1000,1)} nghìn tỷ đồng, tăng {fv(pct(pi['2025']['value_bn'], pi['2024']['value_bn']),1)}% so với 2024, giá căn hộ sơ cấp Hà Nội lên {fv(hn['2025-Q4'],0)} triệu đồng/m² theo Savills (Q4/2024: {fv(hn['2024-Q4'],0)}), {fv(sup['units'],0)} căn hộ được cấp phép (+{sup['yoy_pct']}%), vàng SJC tăng {fv(pct(sjc['2025-12'], sjc['2024-12']),1)}% và VN-Index {fv(pct(vni_ye('2025-12'), vni_ye('2024-12')),1)}%. "
                    f"Tín dụng tăng {fv(A('credit',2025),2)}% trong năm, nhanh hơn tiền gửi của dân cư và tổ chức ({fv(g('deposit_yoy','2025-12'),2)}% theo bảng NHNN, lệch {fv(gap25_sbv,2)} điểm %), nhưng lãi suất 12 tháng Vietcombank vẫn {fv(g('dep12_vcb','2025-09'))}% đến 9/2025."),
        "text_en": (f"Public-investment disbursement for 2025 reached VND {fe(pi['2025']['value_bn']/1000,1)} trn, {fe(pct(pi['2025']['value_bn'], pi['2024']['value_bn']),1)}% more than 2024; Hanoi primary apartment prices rose to VND {fe(hn['2025-Q4'],0)} m/m² per Savills (Q4-24: {fe(hn['2024-Q4'],0)}), {fe(sup['units'],0)} apartments were licensed (+{sup['yoy_pct']}%), SJC gold gained {fe(pct(sjc['2025-12'], sjc['2024-12']),1)}% and the VN-Index {fe(pct(vni_ye('2025-12'), vni_ye('2024-12')),1)}%. "
                    f"Credit grew {fe(A('credit',2025),2)}% in the year, faster than resident and corporate deposits ({fe(g('deposit_yoy','2025-12'),2)}% on SBV tables, a {fe(gap25_sbv,2)} pp gap), yet Vietcombank's 12-month rate was still {fe(g('dep12_vcb','2025-09'))}% in Sep-25."),
        "highlight_series": ["public_investment", "real_estate", "sjc_gold_sell", "vnindex", "credit_yoy", "deposit_yoy", "credit_deposit_gap_yoy", "ldr_sbv"],
        "evidence": [{"series": "public_investment", "from_value": pi["2024"]["value_bn"], "to_value": pi["2025"]["value_bn"], "period": "2024..2025"},
                     {"series": "real_estate.hanoi_primary_apartment_price_mvnd_m2", "from_value": hn["2024-Q4"], "to_value": hn["2025-Q4"], "period": "2024-Q4..2025-Q4"},
                     {"series": "sjc_gold_sell", "from_value": sjc["2024-12"], "to_value": sjc["2025-12"], "period": "2024-12..2025-12"},
                     {"series": "credit_deposit_gap_yoy", "from_value": None, "to_value": gap25_sbv, "period": "2025-12"},
                     {"series": "state_sector_investment_nso", "from_value": stv.get(2024), "to_value": stv.get(2025), "period": "2024..2025"}],
        "data_check": {"supports": "partly", "note_en": ("Public investment, housing prices/supply, gold and stocks all jumped. But (a) deposit growth for 2025 is 15.42% on the IMF-FSI basis used in finance.json annual vs 12.11% y/y on SBV monthly tables - "
                                                          "the 'deposits slowed' reading depends on the definition (conflict reported, not averaged); (b) the SBV-definition LDR was 76.0% at end-2025, well below the 85% cap - only the simple loans/deposits ratio (~110%) was high; "
                                                          "(c) the posted Big-4 rate did not rise until late 2025/Jan-2026."),
                       "note_vi": ("Đầu tư công, giá và nguồn cung nhà, vàng, cổ phiếu đều tăng mạnh. Nhưng (a) tăng trưởng tiền gửi 2025 là 15,42% theo cơ sở IMF-FSI so với 12,11% (y/y) theo bảng NHNN - kết luận 'tiền gửi chậm lại' tùy định nghĩa; "
                                   "(b) LDR theo định nghĩa NHNN là 76,0% cuối 2025, còn xa trần 85% - chỉ LDR đơn giản (~110%) mới cao; (c) lãi suất niêm yết của Big4 chỉ tăng từ cuối 2025/1/2026.")}})
    # 6
    beats.append({
        "id": "convergence_2026", "period_from": "2026-01", "period_to": "2026-09",
        "title_vi": "2026: các áp lực hội tụ, lãi suất buộc phải cao", "title_en": "2026: pressures converge, rates must stay high",
        "text_vi": (f"Chênh lệch tín dụng–huy động (y/y) ở {fv(min(gap26),2)}–{fv(max(gap26),2)} điểm % (1–7/2026), CPI lên {fv(g('cpi_yoy','2026-05'),2)}% (5/2026) và {fv(g('cpi_yoy','2026-09'),2)}% (9/2026), qua đêm liên ngân hàng {fv(g('interbank_on','2026-09'),1)}% cuối tháng 9/2026, chi phí vốn ngân hàng niêm yết lên {fv(cof['2026-Q2'],2)}% (Q2/2026, từ {fv(cof['2025-Q2'],2)}%) và tiền gửi KBNN khoảng {fv(tr['2026-06']/1000,0)} nghìn tỷ đồng (6/2026) được tính dần vào LDR. "
                    f"Lãi suất 12 tháng của Vietcombank tăng từ {fv(g('dep12_vcb','2025-09'))}% lên {fv(g('dep12_vcb','2026-09'))}% và của nhóm ngân hàng tư nhân (MBS) từ {fv(mbs[IDX['2025-12']],2)}% lên {fv(mbs[IDX['2026-08']],1)}%, dù lãi suất tái cấp vốn giữ {fv(g('refi_rate','2026-09'))}% và Fed đã hạ xuống {fv(g('fed_funds_upper','2026-08'),2)}%."),
        "text_en": (f"The credit-deposit gap (y/y) ran at {fe(min(gap26),2)}-{fe(max(gap26),2)} pp (Jan-Jul 2026), CPI reached {fe(g('cpi_yoy','2026-05'),2)}% (May) and {fe(g('cpi_yoy','2026-09'),2)}% (Sep), overnight interbank was {fe(g('interbank_on','2026-09'),1)}% at end-Sep, listed banks' cost of funds rose to {fe(cof['2026-Q2'],2)}% (Q2-26, from {fe(cof['2025-Q2'],2)}%) and roughly VND {fe(tr['2026-06']/1000,0)} trn of Treasury deposits (Jun-26) was being phased back into LDR funding. "
                    f"Vietcombank's 12-month rate rose from {fe(g('dep12_vcb','2025-09'))}% to {fe(g('dep12_vcb','2026-09'))}% and the private-bank average (MBS) from {fe(mbs[IDX['2025-12']],2)}% to {fe(mbs[IDX['2026-08']],1)}%, although the refinancing rate stayed at {fe(g('refi_rate','2026-09'))}% and the Fed had cut to {fe(g('fed_funds_upper','2026-08'),2)}%."),
        "highlight_series": ["credit_deposit_gap_yoy", "cpi_yoy", "interbank_on", "cof_listed", "treasury_deposits", "dep12_vcb", "dep12_private_mbs", "fed_funds_upper", "usdvnd_central"],
        "evidence": [{"series": "dep12_vcb", "from_value": g("dep12_vcb", "2025-09"), "to_value": g("dep12_vcb", "2026-09"), "period": "2025-09..2026-09"},
                     {"series": "dep12_private_mbs", "from_value": mbs[IDX["2025-12"]], "to_value": mbs[IDX["2026-08"]], "period": "2025-12..2026-08"},
                     {"series": "cpi_yoy", "from_value": g("cpi_yoy", "2026-01"), "to_value": g("cpi_yoy", "2026-09"), "period": "2026-01..2026-09"},
                     {"series": "credit_deposit_gap_yoy", "from_value": g("credit_deposit_gap_yoy", "2025-12"), "to_value": g("credit_deposit_gap_yoy", "2026-07"), "period": "2025-12..2026-07"},
                     {"series": "cof_listed", "from_value": cof["2025-Q2"], "to_value": cof["2026-Q2"], "period": "2025-Q2..2026-Q2"},
                     {"series": "usdvnd_central", "from_value": g("usdvnd_central", "2025-12"), "to_value": g("usdvnd_central", "2026-09"), "period": "2025-12..2026-09"}],
        "data_check": {"supports": "partly", "note_en": (f"Funding gap, inflation, cost of funds and Treasury flows support the beat. The 'Fed/USD gap' does not: the Fed cut to {fe(g('fed_funds_upper','2026-08'),2)}% (below the SBV refinancing rate) and only raised to {fe(g('fed_funds_upper','2026-09'),2)}% in Sep-2026; "
                                                          f"the central rate rose {fe(pct(g('usdvnd_central','2026-09'), g('usdvnd_central','2025-12')),1)}% Jan-Sep, a moderate move. In the regressions the Fed term has the wrong sign."),
                       "note_vi": (f"Chênh lệch vốn, lạm phát, chi phí vốn và dòng tiền KBNN ủng hộ nhận định. 'Chênh lệch Fed/USD' thì không: Fed hạ xuống {fv(g('fed_funds_upper','2026-08'),2)}% (thấp hơn tái cấp vốn) và chỉ nâng lên {fv(g('fed_funds_upper','2026-09'),2)}% trong 9/2026; "
                                   f"tỷ giá trung tâm tăng {fv(pct(g('usdvnd_central','2026-09'), g('usdvnd_central','2025-12')),1)}% từ đầu năm, mức vừa phải. Trong hồi quy, biến Fed sai dấu.")}})
    return beats


def story_next(P, proj):
    sq = proj["scenarios"]["status_quo"]["path"]
    cut = proj["scenarios"]["rates_cut"]["path"]
    tight = proj["scenarios"]["tighter"]["path"]
    reg = proj["regression_only"]["path"]
    r0 = proj["start"]["value"]
    return {
        "id": "next_6_months", "period_from": "2026-10", "period_to": "2027-03",
        "title_vi": "6 tháng tới: van xả áp và áp lực còn lại", "title_en": "Next 6 months: relief valves vs remaining pressure",
        "text_vi": (f"Trần LDR nâng lên 95% từ 1/12/2026 và 50% tiền gửi KBNN được tính vào LDR từ 1/8/2026 là các van xả áp, nhưng nếu chênh lệch vốn và lạm phát giữ nguyên, mô hình của dashboard cho lãi suất 12 tháng Vietcombank đi từ {fv(r0)}% lên {fv(sq[-1],2)}% vào 3/2027 (kịch bản: {fv(cut[-1],2)}–{fv(tight[-1],2)}%). "
                    f"Nếu mức tăng 2026 chủ yếu do lợi suất vàng và cổ phiếu như hồi quy gợi ý, lãi suất sẽ hạ dần về {fv(reg[-1],2)}% khi các lợi suất đó nguội đi - mô hình minh họa, không phải dự báo hay khuyến nghị đầu tư."),
        "text_en": (f"The 95% LDR cap from 1 Dec 2026 and counting 50% of Treasury deposits in LDR from 1 Aug 2026 are relief valves, but if the funding gap and inflation stay where they are the dashboard model takes Vietcombank's 12-month rate from {fe(r0)}% to {fe(sq[-1],2)}% by Mar-2027 (scenarios: {fe(cut[-1],2)}-{fe(tight[-1],2)}%). "
                    f"If the 2026 rise was mostly about gold and stock returns, as the regression suggests, the rate would ease to {fe(reg[-1],2)}% as those returns cool - an illustration, not a forecast or investment advice."),
        "highlight_series": ["dep12_vcb", "ldr_cap", "treasury_deposits"],
        "evidence": [{"series": "projection.status_quo", "from_value": r0, "to_value": sq[-1], "period": "2026-09..2027-03"},
                     {"series": "projection.rates_cut", "from_value": r0, "to_value": cut[-1], "period": "2026-09..2027-03"},
                     {"series": "projection.tighter", "from_value": r0, "to_value": tight[-1], "period": "2026-09..2027-03"},
                     {"series": "projection.regression_only", "from_value": r0, "to_value": reg[-1], "period": "2026-09..2027-03"}],
        "data_check": {"supports": "model", "note_en": "Model output (calibrated), see SIM.model.", "note_vi": "Kết quả mô hình (hiệu chỉnh), xem SIM.model."}}


INDICATORS_ADDED = [
    {"key": "rediscount_rate / sbv_overnight_rate", "why_en": "The SBV's corridor: the overnight lending rate caps interbank spikes (7% in late 2022), so it bounds how far funding stress can push short rates.", "why_vi": "Hành lang lãi suất của NHNN: lãi suất cho vay qua đêm là trần cho các đợt tăng vọt liên ngân hàng (7% cuối 2022)."},
    {"key": "real_dep_rate", "why_en": "What a saver actually earns after inflation; when it shrinks (0.3 pp in May-2026) gold, property and stocks look relatively better and deposits must pay more.", "why_vi": "Lợi suất thực của người gửi sau lạm phát; khi thu hẹp (0,3 điểm % tháng 5/2026), vàng, nhà đất, cổ phiếu hấp dẫn hơn tương đối."},
    {"key": "sjc_premium", "why_en": "The gap between SJC bars and world gold measures domestic gold fever (savings pulled out of banks), separate from the world price.", "why_vi": "Chênh lệch giá SJC so với thế giới đo 'cơn sốt vàng' trong nước (tiền rút khỏi ngân hàng), tách khỏi giá thế giới."},
    {"key": "cof_listed / casa_listed / nim_listed", "why_en": "Banks' cost of funds and the share of cheap current accounts show the pressure from the funding side directly; falling CASA forces more term deposits at higher rates.", "why_vi": "Chi phí vốn và tỷ lệ CASA cho thấy trực tiếp áp lực nguồn vốn; CASA giảm buộc huy động có kỳ hạn với lãi suất cao hơn."},
    {"key": "ldr_simple_listed vs ldr_sbv", "why_en": "Two definitions tell different stories: the SBV Circular-22 LDR (~77%) is far below the cap, while simple loans/deposits (~114%) shows how stretched balance sheets are.", "why_vi": "Hai định nghĩa cho hai câu chuyện: LDR theo TT22 (~77%) còn xa trần, còn cho vay/tiền gửi đơn giản (~114%) cho thấy bảng cân đối căng."},
    {"key": "treasury_deposits.ldr_inclusion", "why_en": "How much Treasury money banks may count as funding moved 50% -> 0% -> 50% (2023-2026); public-investment spending drains these deposits.", "why_vi": "Tỷ lệ tiền gửi KBNN được tính vào nguồn vốn thay đổi 50% → 0% → 50% (2023–2026); giải ngân đầu tư công rút bớt khoản tiền gửi này."},
    {"key": "gov_bond_10y", "why_en": "The 10-year government yield (4.0% end-2025 -> 4.43% at the 23-Sep-2026 auction) is the risk-free benchmark competing for the same savings and signals the fiscal financing need.", "why_vi": "Lợi suất TPCP 10 năm là chuẩn phi rủi ro cạnh tranh cùng nguồn tiết kiệm và phản ánh nhu cầu vốn của ngân sách."},
    {"key": "brent", "why_en": "Fuel drives Vietnam's CPI swings (transport +12.6% YTD in Sep-2026); oil therefore feeds the inflation driver.", "why_vi": "Giá xăng dầu chi phối biến động CPI (giao thông +12,6% từ đầu năm, 9/2026)."},
    {"key": "cash_to_m2", "why_en": "A rising cash share (12.1% in Feb-2026) means money circulating outside banks rather than as deposits.", "why_vi": "Tỷ trọng tiền mặt tăng (12,1% tháng 2/2026) nghĩa là tiền lưu thông ngoài ngân hàng thay vì thành tiền gửi."},
    {"key": "credit_target / ldr_cap.upcoming", "why_en": "Credit quotas (15% for 2026) and the LDR cap rise to 95% (1 Dec 2026) set how much more lending banks must fund.", "why_vi": "Chỉ tiêu tín dụng (15% cho 2026) và trần LDR 95% (1/12/2026) quyết định ngân hàng còn phải huy động bao nhiêu."},
    {"key": "margin_lending / new_stock_accounts", "why_en": "Direct measures of household money moving into stocks (margin VND 201.8 trn in Q1-2022, 453.8 trn in Jun-2026).", "why_vi": "Thước đo trực tiếp dòng tiền hộ gia đình vào cổ phiếu."},
]


def conflicts_block():
    return [
        {"topic": "2025 deposit growth", "a": "15.42% (finance.json annual, IMF-FSI customer deposits)", "b": "12.11% y/y Dec-2025 (SBV monthly deposits of residents + organisations)", "handling": "both kept, different definitions"},
        {"topic": "2022 credit growth", "a": "14.5% (Governor's year-end estimate)", "b": "14.2 (finance.json annual)", "handling": "both shown; story uses finance.json annual"},
        {"topic": "2023 credit growth", "a": "13.71% (policy.json)", "b": "~13.5% (excerpt), 13.78 (finance.json annual)", "handling": "not averaged"},
        {"topic": "VCB 12M Sep-2026", "a": "5.9% (vietbao, early Oct-2026)", "b": "Big-4 posted 6.8% (vietnamnet 28/9/2026)", "handling": "VCB 5.9% used; 6.8% is likely another Big-4 bank"},
        {"topic": "VCB 12M Jul-2026", "a": "5.9% counter (topi.vn)", "b": "5.5% (excerpt, late Jul-2026, channel unclear)", "handling": "5.9% used (counter definition)"},
        {"topic": "VCB 12M before Sep-2022 hike", "a": "5.5% counter (Jan-2022)", "b": "'+0.8 pp to 6.4%' implies 5.6%", "handling": "Feb-Aug 2022 null"},
        {"topic": "Margin lending scope", "a": "201,824 bn (106 securities firms, Q1-2022)", "b": "~230,000 bn whole-market estimate", "handling": "securities-firm figure used; tallies differ in scope over time"},
        {"topic": "Treasury deposits", "a": "460,000 bn at 3 Big-4 banks (Q3-2025)", "b": "406,000 bn at state banks (end-2025)", "handling": "kept as separate points with scope"},
        {"topic": "LDR 'near cap'", "a": "SBV TT22 LDR 77.1% (Jun-2026) vs 85% cap", "b": "simple loans/deposits ~114% (Q2-2026)", "handling": "different definitions, never merged"},
        {"topic": "Regulatory LDR Q2-2026", "a": "77.06% SBV system statistic (Circular 22, 30/6/2026)", "b": "~88% NSI estimate for its listed-bank sample", "handling": "77.06 used (official, whole system); 88 is a sample estimate - coverage differs, not merged"},
        {"topic": "10-year G-bond Sep-2026", "a": "4.43% at the 23/9/2026 auction (Reuters)", "b": "'4.67-4.80% in Sep' (vnbusiness excerpt, undated)", "handling": "b rejected: earlier-year article (5-year above 10-year, 31 sessions incl. VBSP)"},
        {"topic": "Corporate bond issuance 2026", "a": "VBMA 7M 289,911 / 8M ~349,000 bn", "b": "VBMA 5M 127,351 + Jun 43,876 = 171,227; other providers H1 ~273,500, 7M ~322,300", "handling": "VBMA cumulatives kept as stated; months shown as disclosed; not reconciled or summed"},
        {"topic": "Deposit growth Aug/Sep-2026", "a": "8.77% (22/8, VND only) and 9.78% (28/9, NSO) mobilisation statements", "b": "SBV table deposits of residents + organisations (latest Jul-2026 5.72% YTD)", "handling": "statement points stored as labelled observations of deposit_ytd; monthly values and y/y gap (model driver) stay on the table basis"},
    ]


def gaps_block(P):
    out = []
    for k, s in P["series"].items():
        c = s.get("coverage")
        if c and c["n"] < N:
            out.append({"series": k, "months_covered": c["n"], "of": N})
    return out


# --------------------------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--fetch", action="store_true", help="use the downloaded files given below for VN-Index / gold / Brent")
    ap.add_argument("--vnindex-json"); ap.add_argument("--gold-csv"); ap.add_argument("--brent-csv")
    ap.add_argument("--out", default=str(OUT))
    a = ap.parse_args()
    prev = json.loads(OUT.read_text(encoding="utf-8"))["SIM"] if OUT.exists() else None
    if not a.fetch:
        a.vnindex_json = a.gold_csv = a.brent_csv = None
        if prev is None:
            sys.exit("No cached data/simulation.json: run once with --fetch and the three download paths.")
    P = build_panel(prev, a)
    est = estimation_attempts(P)
    cal = calibrate(P)
    proj = project(P, cal, est)
    R2 = model2_block(P)
    v2_attach_panel(P, R2)
    beats = story(P) + [story_next(P, proj)]
    SIM = {
        "as_of": AS_OF,
        "panel": P,
        "story": beats,
        "model": model_block(P, cal, est),
        "backtest": backtest_block(P, cal, est),
        "projection": proj,
        "sliders": sliders_block(P, cal),
        "model2": model2_json(R2, P),
        "projection2": projection2_json(R2),
        "model_versions": {"main": "model2", "comparison": "model",
                           "note": "model2 (structural funding model, v2) is the main deposit-rate model from 2026-10-07; model/projection/sliders (v1, calibrated 4-driver) are kept unchanged for comparison."},
        "indicators_added": INDICATORS_ADDED,
        "conflicts": conflicts_block(),
        "gaps": gaps_block(P),
        "disclaimer_vi": "Mô hình minh họa của dashboard, không phải dự báo chính thức hay khuyến nghị đầu tư.",
        "disclaimer_en": "Illustrative dashboard model - not an official forecast or investment advice.",
    }
    out = {"_meta": {"owner": "Finance agent", "as_of": AS_OF,
                     "note": ("Simulation tab data. Built by tools/build/simulate.py from data/finance.json, economy.json, policy.json, invest_macro.json plus curated, sourced "
                              "observations in the script (search excerpts 2026-10-06; press/SBV pages are blocked from the sandbox) and three downloads cached here "
                              "(VN-Index via vnstock VCI; World Bank Pink Sheet gold and EIA Brent via github datasets). Nulls are gaps, never interpolated. "
                              "Main deposit-rate model: SIM.model2 / SIM.projection2 (structural monthly VND funding model, partly estimated - see SIM.model2.estimation); "
                              "v1 calibrated scenario model kept in SIM.model / projection / sliders for comparison. Not investment advice.")},
           "SIM": SIM}
    Path(a.out).write_text(json.dumps(out, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
    print("wrote", a.out)
    print("model:", SIM["model"]["equation"])
    print("base:", proj["base"], "low:", proj["low"], "high:", proj["high"])
    for k, sc in proj["scenarios"].items():
        print(k, sc["path"])
    print("v2:", SIM["model2"]["equation"])
    print("v2 base:", SIM["projection2"]["base"])
    for k, sc in SIM["projection2"]["scenarios"].items():
        print("v2", k, sc["path"])


if __name__ == "__main__":
    main()
