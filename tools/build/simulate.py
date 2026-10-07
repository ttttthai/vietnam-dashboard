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
    beats = story(P) + [story_next(P, proj)]
    SIM = {
        "as_of": AS_OF,
        "panel": P,
        "story": beats,
        "model": model_block(P, cal, est),
        "backtest": backtest_block(P, cal, est),
        "projection": proj,
        "sliders": sliders_block(P, cal),
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
                              "The deposit-rate projection is a calibrated scenario model (regressions were not credible; see SIM.model.estimation). Not investment advice.")},
           "SIM": SIM}
    Path(a.out).write_text(json.dumps(out, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
    print("wrote", a.out)
    print("model:", SIM["model"]["equation"])
    print("base:", proj["base"], "low:", proj["low"], "high:", proj["high"])
    for k, sc in proj["scenarios"].items():
        print(k, sc["path"])


if __name__ == "__main__":
    main()
