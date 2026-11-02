"""Server-side pulls of public macro APIs (proposals P-3 / P-4).

Writes snapshots to data/auto/<source>.json for the tab agents to read. Agents still decide what goes into
their own data files; nothing here touches data/<tab>.json, data/research/ or any other agent file.

Sources (no API key needed):
  world_bank      World Bank WDI API v2 — the WORLD_RANK indicators (data/society.json): latest VNM value,
                  the API's `lastupdated`, and Vietnam's rank among economies (aggregates excluded)
  imf_datamapper  IMF DataMapper API v1 — NGDP_RPCH, PCPIPCH, NGDPD, NGDPDPC for VNM (WEO vintage)
  fed_funds       FRED public CSV download (fredgraph.csv, no key) — DFEDTARU / DFEDTARL target range

Every network failure is caught: the previous snapshot is kept, and data/auto/_status.json records the attempt.
Stdlib only. Parsing functions are pure and unit-tested against fixtures (tests/fixtures/).
"""
from __future__ import annotations

import csv
import io
import json
import os
import socket
import tempfile
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

ROOT = Path(__file__).parent
AUTO_DIR = ROOT / "data" / "auto"
STATUS_FILE = AUTO_DIR / "_status.json"
UA = {"User-Agent": "vietnam-dashboard/1.0 (+server auto-fetch)", "Accept": "application/json, text/csv, */*"}
TIMEOUT = 20

# ─── World Bank WDI ────────────────────────────────────────────────
# key → (indicator code, label); keys match data/society.json WORLD_RANK (religion_unaffiliated is Pew, not WDI)
WB_INDICATORS: dict[str, tuple[str, str]] = {
    "pop":          ("SP.POP.TOTL",        "Population, total"),
    "area":         ("AG.SRF.TOTL.K2",     "Surface area (sq. km)"),
    "density":      ("EN.POP.DNST",        "Population density (people per sq. km of land area)"),
    "urban":        ("SP.URB.TOTL.IN.ZS",  "Urban population (% of total population)"),
    "tfr":          ("SP.DYN.TFRT.IN",     "Fertility rate, total (births per woman)"),
    "life_exp":     ("SP.DYN.LE00.IN",     "Life expectancy at birth, total (years)"),
    "cbr":          ("SP.DYN.CBRT.IN",     "Birth rate, crude (per 1,000 people)"),
    "cdr":          ("SP.DYN.CDRT.IN",     "Death rate, crude (per 1,000 people)"),
    "srb":          ("SP.POP.BRTH.MF",     "Sex ratio at birth (male births per female births)"),
    "age65":        ("SP.POP.65UP.TO.ZS",  "Population ages 65 and above (% of total population)"),
    "dependency":   ("SP.POP.DPND",        "Age dependency ratio (% of working-age population)"),
    "g_gdp":        ("NY.GDP.MKTP.CD",     "GDP (current US$)"),
    "g_gdp_pc":     ("NY.GDP.PCAP.CD",     "GDP per capita (current US$)"),
    "g_gdp_pc_ppp": ("NY.GDP.PCAP.PP.CD",  "GDP per capita, PPP (current international $)"),
    "g_growth":     ("NY.GDP.MKTP.KD.ZG",  "GDP growth (annual %)"),
}
WB_BASE = "https://api.worldbank.org/v2"
WB_COUNTRIES_URL = f"{WB_BASE}/country?format=json&per_page=400"


def wb_vnm_url(code: str, start: int, end: int) -> str:
    return f"{WB_BASE}/country/VNM/indicator/{code}?format=json&per_page=100&date={start}:{end}"


def wb_all_url(code: str, year: int) -> str:
    return f"{WB_BASE}/country/all/indicator/{code}?format=json&per_page=400&date={year}"


def parse_wb(payload: Any) -> dict:
    """World Bank v2 `?format=json` → {"lastupdated","sourcename","total","rows":[{iso3,year,value}]}.

    Success shape: [ {page, pages, per_page, total, sourceid, sourcename, lastupdated}, [ {indicator, country,
    countryiso3code, date, value, ...}, ... ] ]. Error shape: [ {"message": [{"id","key","value"}]} ].
    """
    if not isinstance(payload, list) or not payload or not isinstance(payload[0], dict):
        raise ValueError("unexpected World Bank payload")
    head = payload[0]
    if "message" in head:
        msgs = head.get("message") or []
        raise ValueError("World Bank API error: " + "; ".join(f"{m.get('key')}: {m.get('value')}" for m in msgs if isinstance(m, dict)))
    rows = []
    for r in (payload[1] if len(payload) > 1 and isinstance(payload[1], list) else []):
        if not isinstance(r, dict):
            continue
        try:
            year = int(r.get("date"))
        except (TypeError, ValueError):
            continue
        rows.append({"iso3": r.get("countryiso3code") or (r.get("country") or {}).get("id"),
                     "year": year, "value": r.get("value")})
    return {"lastupdated": head.get("lastupdated"), "sourcename": head.get("sourcename"),
            "total": head.get("total"), "pages": head.get("pages"), "rows": rows}


def parse_wb_economies(payload: Any) -> set[str]:
    """Country list → ISO3 codes of economies (aggregates have region.id == 'NA')."""
    if not isinstance(payload, list) or len(payload) < 2 or not isinstance(payload[1], list):
        raise ValueError("unexpected World Bank country payload")
    return {c["id"] for c in payload[1]
            if isinstance(c, dict) and c.get("id") and (c.get("region") or {}).get("id") not in (None, "NA")}


def wb_latest(rows: list[dict], iso3: str = "VNM") -> tuple[int | None, float | None]:
    vals = [(r["year"], r["value"]) for r in rows if r.get("iso3") == iso3 and r.get("value") is not None]
    return max(vals) if vals else (None, None)


def wb_rank(rows: list[dict], iso3: str, economies: set[str] | None) -> dict | None:
    """Competition rank (1 = highest value) of iso3 among economies with data in these rows."""
    vals = {r["iso3"]: r["value"] for r in rows
            if r.get("value") is not None and r.get("iso3") and (economies is None or r["iso3"] in economies)}
    if iso3 not in vals:
        return None
    v = vals[iso3]
    return {"rank": 1 + sum(1 for x in vals.values() if x > v), "of": len(vals)}


# ─── IMF DataMapper ────────────────────────────────────────────────
IMF_INDICATORS: dict[str, str] = {
    "NGDP_RPCH": "Real GDP growth (annual % change)",
    "PCPIPCH":   "Inflation, average consumer prices (annual % change)",
    "NGDPD":     "GDP, current prices (billions of U.S. dollars)",
    "NGDPDPC":   "GDP per capita, current prices (U.S. dollars per capita)",
}
IMF_BASE = "https://www.imf.org/external/datamapper/api/v1"


def imf_url(indicator: str, iso3: str = "VNM") -> str:
    return f"{IMF_BASE}/{indicator}/{iso3}"


def parse_imf(payload: Any, indicator: str, iso3: str = "VNM") -> dict[int, float]:
    """DataMapper `/api/v1/<ind>/<ISO3>` → {year: value}. Shape: {"values": {IND: {ISO3: {"1980": v, ...}}}, "api": {...}}."""
    if not isinstance(payload, dict):
        raise ValueError("unexpected IMF payload")
    series = ((payload.get("values") or {}).get(indicator) or {}).get(iso3)
    if not isinstance(series, dict) or not series:
        raise ValueError(f"IMF DataMapper: no {indicator} values for {iso3}")
    out = {}
    for y, v in series.items():
        try:
            if v is not None:
                out[int(y)] = float(v)
        except (TypeError, ValueError):
            continue
    return dict(sorted(out.items()))


# ─── FRED (public CSV, no key) ─────────────────────────────────────
FRED_SERIES = {"upper": "DFEDTARU", "lower": "DFEDTARL"}


def fred_url(series_id: str, start: str = "2022-01-01") -> str:
    return f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}&cosd={start}"


def parse_fred_csv(text: str) -> list[tuple[str, float]]:
    """fredgraph.csv → [(date, value)], skipping '.' (missing). Header is DATE or observation_date, then the id."""
    rdr = csv.reader(io.StringIO(text.lstrip("﻿")))
    head = next(rdr, None)
    if not head or len(head) < 2 or head[0].strip().lower() not in ("date", "observation_date"):
        raise ValueError("unexpected FRED CSV header")
    out = []
    for row in rdr:
        if len(row) < 2 or not row[0].strip():
            continue
        try:
            out.append((row[0].strip(), float(row[1])))
        except ValueError:
            continue                      # "." = no observation
    return out


def fred_changes(obs: list[tuple[str, float]]) -> list[dict]:
    ch, prev = [], None
    for d, v in obs:
        if prev is not None and v != prev:
            ch.append({"date": d, "from": prev, "to": v})
        prev = v
    return ch


# ─── HTTP ──────────────────────────────────────────────────────────
class NetworkDown(Exception):
    """Connection-level failure (DNS, refused, proxy block, timeout): stop calling the same host this run."""


def http_get(url: str, as_text: bool = False) -> Any:
    try:
        with urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=TIMEOUT) as r:
            raw = r.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        if e.code in (403, 407, 502, 503, 504):   # proxy/egress block or upstream down
            raise NetworkDown(f"HTTP {e.code} for {url}") from e
        raise
    except (urllib.error.URLError, socket.timeout, TimeoutError, ConnectionError, OSError) as e:
        raise NetworkDown(f"{type(e).__name__}: {str(e)[:160]}") from e
    return raw if as_text else json.loads(raw)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


# ─── Fetchers (get= is injectable for tests) ───────────────────────
def fetch_world_bank(get: Callable[..., Any] = http_get, with_ranks: bool = True, prev: dict | None = None) -> dict:
    y1 = datetime.now(timezone.utc).year
    snap: dict[str, Any] = {"source": "World Bank WDI API v2", "fetched_at": _now(), "country": "VNM",
                            "with_ranks": with_ranks, "indicators": {}, "errors": [], "urls": {}}
    economies = None
    if with_ranks:
        try:
            economies = parse_wb_economies(get(WB_COUNTRIES_URL))
            snap["urls"]["economies"] = WB_COUNTRIES_URL
            snap["economies_count"] = len(economies)
        except NetworkDown:
            raise
        except Exception as e:
            snap["errors"].append(f"economies: {str(e)[:160]}")
    prev_ind = (prev or {}).get("indicators") or {}
    for key, (code, label) in WB_INDICATORS.items():
        url = wb_vnm_url(code, y1 - 12, y1)
        try:
            p = parse_wb(get(url))
            year, value = wb_latest(p["rows"])
            ent = {"code": code, "label": label, "lastupdated": p["lastupdated"], "latest_year": year,
                   "latest_value": value, "series": {str(r["year"]): r["value"] for r in sorted(p["rows"], key=lambda r: r["year"])
                                                     if r["iso3"] == "VNM" and r["value"] is not None},
                   "url": url}
            if with_ranks and year is not None:
                rurl = wb_all_url(code, year)
                try:
                    pr = parse_wb(get(rurl))
                    if (pr.get("pages") or 1) > 1:
                        snap["errors"].append(f"{key}: rank page 1 of {pr['pages']} only")
                    rk = wb_rank(pr["rows"], "VNM", economies)
                    ent["rank"] = ({"year": year, **rk, "url": rurl, "direction": "desc",
                                    "universe": "WB economies, aggregates excluded" if economies else "all rows"} if rk else None)
                except NetworkDown:
                    raise
                except Exception as e:
                    snap["errors"].append(f"{key} rank: {str(e)[:160]}")
            snap["indicators"][key] = ent
        except NetworkDown:
            raise
        except Exception as e:
            snap["errors"].append(f"{key}: {str(e)[:160]}")
            if key in prev_ind:
                snap["indicators"][key] = {**prev_ind[key], "stale": True,
                                           "carried_from": (prev or {}).get("fetched_at")}
    lu = sorted({v.get("lastupdated") for v in snap["indicators"].values() if v.get("lastupdated")})
    snap["lastupdated"] = lu[-1] if lu else None
    return snap


def fetch_imf(get: Callable[..., Any] = http_get, prev: dict | None = None) -> dict:
    y = datetime.now(timezone.utc).year
    snap: dict[str, Any] = {"source": "IMF DataMapper API v1 (World Economic Outlook)", "fetched_at": _now(),
                            "country": "VNM", "indicators": {}, "errors": [],
                            "note": f"DataMapper mixes actuals and WEO projections without a flag; treat {y} and later "
                                    "as projections (the latest actual year depends on the vintage)."}
    prev_ind = (prev or {}).get("indicators") or {}
    for ind, label in IMF_INDICATORS.items():
        url = imf_url(ind)
        try:
            vals = parse_imf(get(url), ind)
            snap["indicators"][ind] = {"label": label, "url": url, "values": {str(k): v for k, v in vals.items()},
                                       "current_year": y, "current_year_value": vals.get(y),
                                       "next_year_value": vals.get(y + 1)}
        except NetworkDown:
            raise
        except Exception as e:
            snap["errors"].append(f"{ind}: {str(e)[:160]}")
            if ind in prev_ind:
                snap["indicators"][ind] = {**prev_ind[ind], "stale": True, "carried_from": (prev or {}).get("fetched_at")}
    return snap


def fetch_fed_funds(get: Callable[..., Any] = http_get, prev: dict | None = None) -> dict:
    snap: dict[str, Any] = {"source": "FRED (Federal Reserve Bank of St. Louis), public CSV; series DFEDTARU / DFEDTARL",
                            "fetched_at": _now(), "errors": [], "urls": {}}
    obs = {}
    for k, sid in FRED_SERIES.items():
        url = fred_url(sid)
        snap["urls"][k] = url
        try:
            obs[k] = parse_fred_csv(get(url, as_text=True))
        except NetworkDown:
            raise
        except Exception as e:
            snap["errors"].append(f"{sid}: {str(e)[:160]}")
    if not obs.get("upper"):
        raise ValueError("no DFEDTARU observations")
    up, lo = obs["upper"], obs.get("lower") or []
    snap.update({"as_of": up[-1][0], "upper": up[-1][1], "lower": lo[-1][1] if lo else None,
                 "unit": "% p.a.", "changes": fred_changes(up)[-12:]})
    snap["last_change"] = snap["changes"][-1]["date"] if snap["changes"] else None
    return snap


SOURCES: dict[str, Callable[..., dict]] = {
    "world_bank": fetch_world_bank,
    "imf_datamapper": fetch_imf,
    "fed_funds": fetch_fed_funds,
}


# ─── Storage (data/auto only) ──────────────────────────────────────
def _safe_path(name: str) -> Path:
    p = (AUTO_DIR / f"{name}.json").resolve()
    if p.parent != AUTO_DIR.resolve():
        raise ValueError(f"refusing to write outside data/auto: {p}")
    return p


def _write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=".tmp-", suffix=".json")
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=1)
    os.replace(tmp, path)


def read_snapshot(name: str) -> dict | None:
    try:
        return json.loads(_safe_path(name).read_text(encoding="utf-8"))
    except Exception:
        return None


def read_status() -> dict:
    try:
        return json.loads(STATUS_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def run(names: list[str] | None = None, log_event: Callable[..., None] | None = None,
        trigger: str = "auto", get: Callable[..., Any] = http_get) -> dict:
    """Fetch the given sources (default all), write data/auto/<name>.json on success, update _status.json."""
    names = names or list(SOURCES)
    status = read_status()
    results = {}
    for name in names:
        fn = SOURCES.get(name)
        if not fn:
            results[name] = {"status": "error", "error": "unknown source"}
            continue
        t0 = datetime.now(timezone.utc)
        prev = read_snapshot(name)
        entry: dict[str, Any] = {"attempted_at": t0.isoformat()}
        try:
            snap = fn(get=get, prev=prev)
            n = len(snap.get("indicators") or {}) if "indicators" in snap else (1 if snap.get("upper") is not None else 0)
            if n == 0:
                raise ValueError("no values parsed: " + "; ".join(snap.get("errors") or [])[:200])
            _write_json(_safe_path(name), snap)
            st = "partial" if snap.get("errors") else "ok"
            entry.update(status=st, fetched_at=snap["fetched_at"], items=n, errors=snap.get("errors") or [])
        except Exception as e:
            entry.update(status="error", error=f"{type(e).__name__}: {str(e)[:200]}",
                         kept_snapshot_from=(prev or {}).get("fetched_at"))
        entry["duration_ms"] = int((datetime.now(timezone.utc) - t0).total_seconds() * 1000)
        status[name] = {**status.get(name, {}), **entry}
        if entry["status"] != "error":
            status[name]["last_success"] = entry["fetched_at"]
            status[name].pop("error", None)
            status[name].pop("kept_snapshot_from", None)
        results[name] = status[name]
        if log_event:
            msg = (f"{name}: {entry.get('items')} mục" if entry["status"] != "error" else f"{name}: {entry.get('error')}")
            log_event(trigger, f"auto:{name}", entry["status"], duration_ms=entry["duration_ms"], message=msg[:200])
    try:
        _write_json(STATUS_FILE, status)
    except Exception:
        pass
    return results


def stale_sources(max_age_days: int = 31) -> list[str]:
    """Sources with no snapshot, or one older than max_age_days (used to back-fill on server start)."""
    out = []
    now = datetime.now(timezone.utc)
    for name in SOURCES:
        s = read_snapshot(name)
        try:
            age = (now - datetime.fromisoformat(s["fetched_at"])).days if s else None
        except Exception:
            age = None
        if age is None or age > max_age_days:
            out.append(name)
    return out
