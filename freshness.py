"""Data-freshness report for GET /api/freshness (proposal P-1).

Reads the Research agent's ops files at request time (they are small):
  data/research/inventory.json         — one row per figure family the site shows
                                          (id, tab, owner_agent, latest_period, calendar_id, last_checked, last_changed)
  data/research/release_calendar.json  — per publisher series: next_expected (list of free-text dates)

Status per series, computed against today in Asia/Ho_Chi_Minh:
  fresh    — the next expected release is more than LEAD_DAYS away (or every listed release is already captured)
  due      — today is inside the expected release window, or at most LEAD_DAYS before it
  overdue  — the window has passed and nobody checked the series since the window opened
  waiting  — the window has passed, the series was checked after it opened but the figure has not moved
             (publisher late), or the series has no fixed release date (ad hoc / unknown / no calendar entry)
Continuous series (daily / weekly / nightly) are fresh while last_checked is recent, otherwise overdue.

Stdlib only; every field is optional — a malformed row yields status "waiting" with a reason, never an error.
"""
from __future__ import annotations

import calendar as _cal
import json
import re
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

TZ = ZoneInfo("Asia/Ho_Chi_Minh")
LEAD_DAYS = 3                       # "due" starts this many days before a window opens
CONTINUOUS = {"daily": 1, "nightly": 1, "continuous": 1, "weekly": 7}
STATUSES = ("fresh", "due", "overdue", "waiting")


def today_vn() -> date:
    return datetime.now(TZ).date()


def _d(s: Any) -> date | None:
    """ISO date (or ISO datetime) → date; anything else → None."""
    if not isinstance(s, str):
        return None
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", s.strip())
    if not m:
        return None
    try:
        return date(int(m[1]), int(m[2]), int(m[3]))
    except ValueError:
        return None


def _month_end(y: int, m: int) -> int:
    return _cal.monthrange(y, m)[1]


def parse_window(text: Any) -> dict | None:
    """Free-text next_expected entry → {"start","end"} dates, {"every_days"} for continuous series, or None.

    Handles the calendar's forms: 2026-11-03, 2026-11-01..05, 2026-12 (early|mid|mid-late|late),
    2027-06/07, 2027-H1, 2027-mid, 2029-2030, 2027 (…), daily/weekly/nightly/continuous.
    "ad hoc …" and "unknown …" have no fixed date → None.
    """
    if not isinstance(text, str) or not text.strip():
        return None
    s = text.strip().lower()
    head = s.split("(")[0].strip()
    hint = s[len(head):]
    for word, days in CONTINUOUS.items():
        if head.startswith(word):
            return {"every_days": days}
    if s.startswith(("ad hoc", "ad_hoc", "unknown")):
        return None
    try:
        m = re.match(r"^(\d{4})-(\d{2})-(\d{2})\.\.(\d{2})", head)
        if m:
            y, mo = int(m[1]), int(m[2])
            return {"start": date(y, mo, int(m[3])), "end": date(y, mo, int(m[4]))}
        m = re.match(r"^(\d{4})-(\d{2})-(\d{2})", head)
        if m:
            d = date(int(m[1]), int(m[2]), int(m[3]))
            return {"start": d, "end": d}
        m = re.match(r"^(\d{4})-(\d{2})/(\d{2})", head)
        if m:
            y, a, b = int(m[1]), int(m[2]), int(m[3])
            return {"start": date(y, a, 1), "end": date(y, b, _month_end(y, b))}
        m = re.match(r"^(\d{4})-(\d{2})\b", head)
        if m:
            y, mo = int(m[1]), int(m[2])
            last = _month_end(y, mo)
            if "mid-late" in hint or "mid–late" in hint:
                a, b = 11, last
            elif "early" in hint:
                a, b = 1, 10
            elif "mid" in hint:
                a, b = 11, 20
            elif "late" in hint:
                a, b = 21, last
            else:
                a, b = 1, last
            return {"start": date(y, mo, a), "end": date(y, mo, b)}
        m = re.match(r"^(\d{4})-h([12])\b", head)
        if m:
            y = int(m[1])
            return {"start": date(y, 1, 1), "end": date(y, 6, 30)} if m[2] == "1" else \
                   {"start": date(y, 7, 1), "end": date(y, 12, 31)}
        m = re.match(r"^(\d{4})-mid\b", head)
        if m:
            y = int(m[1])
            return {"start": date(y, 6, 1), "end": date(y, 7, 31)}
        m = re.match(r"^(\d{4})-(\d{4})\b", head)
        if m:
            return {"start": date(int(m[1]), 1, 1), "end": date(int(m[2]), 12, 31)}
        m = re.match(r"^(\d{4})\b", head)
        if m:
            y = int(m[1])
            return {"start": date(y, 1, 1), "end": date(y, 12, 31)}
    except ValueError:
        return None
    return None


def series_status(row: dict, cal_entry: dict | None, today: date, cal_asof: date | None,
                  server_last_refresh: date | None = None) -> dict:
    """Freshness of one inventory row against its release-calendar entry."""
    last_checked = _d(row.get("last_checked"))
    last_changed = _d(row.get("last_changed"))
    out: dict[str, Any] = {
        "id": row.get("id"), "tab": row.get("tab"), "owner_agent": row.get("owner_agent"),
        "file": row.get("file"), "frequency": row.get("frequency"),
        "latest_period": row.get("latest_period"), "calendar_id": row.get("calendar_id"),
        "last_checked": row.get("last_checked"), "last_changed": row.get("last_changed"),
        "next_expected": None, "window": None, "status": "waiting", "reason": None, "days_overdue": None,
    }
    if not cal_entry:
        out["reason"] = "no release-calendar entry (fixed reference or not yet scheduled)"
        return out
    out["publisher"] = cal_entry.get("publisher")
    raw = cal_entry.get("next_expected")
    raw = raw if isinstance(raw, list) else ([raw] if raw else [])
    parsed = [(t, parse_window(t)) for t in raw]

    cont = next(((t, w) for t, w in parsed if w and "every_days" in w), None)
    if cont:
        every = cont[1]["every_days"]
        ref = last_checked
        if row.get("calendar_id") == "server_daily" and server_last_refresh:
            ref = server_last_refresh
        out["next_expected"] = cont[0]
        if ref is None:
            out.update(status="overdue", reason=f"{cont[0]} series with no last_checked date")
        else:
            age = (today - ref).days
            ok = age <= every + 1
            out.update(status="fresh" if ok else "overdue",
                       reason=f"{cont[0]} series, last checked {age} day(s) ago",
                       days_overdue=None if ok else age - every)
        return out

    dated = [(t, w) for t, w in parsed if w and "start" in w]
    if not dated:
        out["next_expected"] = raw[0] if raw else None
        out["reason"] = "no fixed release date" + (f": {raw[0]}" if raw else "")
        return out

    pending = None
    for t, w in dated:
        if cal_asof and w["end"] < cal_asof:
            continue                          # already past when the calendar was built → treated as captured
        if last_changed and w["start"] <= last_changed:
            continue                          # the figure moved after this window opened → captured
        pending = (t, w)
        break
    if pending is None:
        t, w = dated[-1]
        out.update(next_expected=t, window={"start": w["start"].isoformat(), "end": w["end"].isoformat()},
                   status="fresh", reason="all listed releases captured; release calendar needs rolling forward")
        return out

    t, w = pending
    out["next_expected"] = t
    out["window"] = {"start": w["start"].isoformat(), "end": w["end"].isoformat()}
    if today < w["start"] - timedelta(days=LEAD_DAYS):
        out.update(status="fresh", reason=f"next release in {(w['start'] - today).days} day(s)")
    elif today <= w["end"]:
        out.update(status="due", reason="inside (or just before) the expected release window")
    elif last_checked and last_checked >= w["start"]:
        out.update(status="waiting", days_overdue=(today - w["end"]).days,
                   reason=f"checked {last_checked.isoformat()} after the window opened; figure not moved (publisher late?)")
    else:
        out.update(status="overdue", days_overdue=(today - w["end"]).days,
                   reason="expected release window has passed and the series was not re-checked")
    return out


def _load(path: Path) -> tuple[dict, str | None]:
    try:
        d = json.loads(path.read_text(encoding="utf-8"))
        return (d if isinstance(d, dict) else {}), None
    except FileNotFoundError:
        return {}, f"{path.name} not found"
    except Exception as e:                   # malformed JSON → empty report, not a 500
        return {}, f"{path.name} unreadable: {str(e)[:120]}"


def report(research_dir: Path, today: date | None = None, server_last_refresh: date | None = None,
           tab: str | None = None, status: str | None = None) -> dict:
    today = today or today_vn()
    cal, e1 = _load(research_dir / "release_calendar.json")
    inv, e2 = _load(research_dir / "inventory.json")
    cal_rows = cal.get("series") if isinstance(cal.get("series"), list) else []
    by_id = {c.get("id"): c for c in cal_rows if isinstance(c, dict) and c.get("id")}
    cal_asof = _d(cal.get("as_of"))
    rows = inv.get("families") if isinstance(inv.get("families"), list) else []

    series = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        try:
            series.append(series_status(r, by_id.get(r.get("calendar_id")), today, cal_asof, server_last_refresh))
        except Exception as e:               # one bad row never breaks the report
            series.append({"id": r.get("id"), "tab": r.get("tab"), "owner_agent": r.get("owner_agent"),
                           "status": "waiting", "reason": f"could not evaluate: {str(e)[:120]}"})

    tabs: dict[str, dict] = {}
    for s in series:
        t = tabs.setdefault(s.get("tab") or "—", {"total": 0, **{k: 0 for k in STATUSES},
                                                   "overdue_ids": [], "due_ids": [], "next_window": None})
        t["total"] += 1
        st = s.get("status") if s.get("status") in STATUSES else "waiting"
        t[st] += 1
        if st == "overdue":
            t["overdue_ids"].append(s.get("id"))
        if st == "due":
            t["due_ids"].append(s.get("id"))
        w = s.get("window")
        if w and w.get("start") and w["start"] >= today.isoformat():
            if t["next_window"] is None or w["start"] < t["next_window"]:
                t["next_window"] = w["start"]
    for t in tabs.values():
        t["status"] = ("overdue" if t["overdue"] else "due" if t["due"] else
                       "fresh" if t["fresh"] else "waiting")

    if tab:
        series = [s for s in series if s.get("tab") == tab]
    if status:
        series = [s for s in series if s.get("status") == status]
    return {
        "today": today.isoformat(),
        "timezone": "Asia/Ho_Chi_Minh",
        "calendar_as_of": cal.get("as_of"),
        "inventory_as_of": inv.get("as_of"),
        "counts": {k: sum(t[k] for t in tabs.values()) for k in STATUSES},   # over all series (filters apply to "series" only)
        "tabs": tabs,
        "series": series,
        "errors": [e for e in (e1, e2) if e],
        "rules": {"lead_days": LEAD_DAYS, "statuses": {
            "fresh": "next expected release more than lead_days away, or all listed releases captured",
            "due": "inside the expected release window or within lead_days before it",
            "overdue": "window passed and the series was not re-checked since it opened",
            "waiting": "window passed but checked since (publisher late), or no fixed release date"}},
    }
