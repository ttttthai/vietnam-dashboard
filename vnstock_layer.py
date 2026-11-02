"""
Optional vnstock layer (sponsor library `vnstock_data`), used FIRST by server.py / invest.py when it is usable;
the direct Vietcap (VCI) code in invest.py stays the fallback.

Usable = `vnstock_data` imports AND an API key is present (env VNSTOCK_API_KEY or ~/.vnstock/api_key.json).
The key is never read, logged or copied here: the library (vnai) reads it itself. Telemetry is forced off
(VNSTOCK_TELEMETRY=off) before the import.

Install (never from public PyPI — see README "vnstock (optional)"). Without it every function here returns None
and callers fall back to the VCI endpoints.

All calls: retries with exponential backoff, plus a circuit breaker (after FAIL_LIMIT consecutive failures the
layer is skipped for COOLDOWN_S) so a broken upstream never slows the server.
"""
from __future__ import annotations

import contextlib
import io
import logging
import os
import threading
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Callable

os.environ["VNSTOCK_TELEMETRY"] = "off"

log = logging.getLogger("vn-dashboard.vnstock")

KEY_FILE = Path.home() / ".vnstock" / "api_key.json"
FAIL_LIMIT = 3
COOLDOWN_S = 15 * 60
# Index symbols: vnstock returns their levels unscaled. Stock prices come back in thousand VND (57.3) and are
# multiplied by 1000 here so callers get VND, like the direct VCI chart endpoint.
INDEX_SYMBOLS = {"VNINDEX", "VN30", "VN100", "HNXINDEX", "HNX30", "UPCOMINDEX", "HNXUPCOMINDEX", "VNMIDCAP",
                 "VNSMALLCAP", "VNALLSHARE", "VNFINLEAD", "VNFIN", "VNDIAMOND", "VNX50", "VNXALL"}
# Hosts behind each vnstock_data Finance source (decoded from vnstock_data 3.3.1 explorer constants)
FINANCE_HOSTS = {"VCI": "iq.vietcap.com.vn", "MAS": "masboard.masvn.com",
                 "KBS": "kbbuddywts.kbsec.com.vn", "MBK": "data.maybanktrade.com.vn"}

_lock = threading.Lock()
_state: dict[str, Any] = {"mod": None, "import_error": None, "checked": False, "fails": 0, "open_until": 0.0}


def key_present() -> bool:
    """True when a key is configured (env var set or key file exists). Does not read the key."""
    return bool(os.environ.get("VNSTOCK_API_KEY")) or KEY_FILE.is_file()


def _module():
    with _lock:
        if not _state["checked"]:
            _state["checked"] = True
            try:
                buf = io.StringIO()
                with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(buf):   # library banner
                    import vnstock_data  # type: ignore
                _state["mod"] = vnstock_data
                log.info("vnstock_data %s available (key %s)", getattr(vnstock_data, "__version__", _version()),
                         "configured" if key_present() else "missing")
            except BaseException as e:          # ImportError, or SystemExit from the library's own checks
                _state["import_error"] = f"{type(e).__name__}: {str(e)[:120]}"
                log.info("vnstock_data not available (%s) — using direct VCI endpoints", _state["import_error"])
        return _state["mod"]


def _version() -> str | None:
    try:
        from importlib.metadata import version
        return version("vnstock_data")
    except Exception:
        return None


def available() -> bool:
    """Library importable, key configured, circuit closed."""
    return key_present() and _module() is not None and time.time() >= _state["open_until"]


def status() -> dict:
    _module()
    return {"library": "vnstock_data", "version": _version() if _state["mod"] else None,
            "importable": _state["mod"] is not None, "import_error": _state["import_error"],
            "key_configured": key_present(), "telemetry": os.environ.get("VNSTOCK_TELEMETRY"),
            "circuit_open": time.time() < _state["open_until"], "active": available()}


def _ok() -> None:
    _state["fails"] = 0


def _fail(what: str, e: BaseException) -> None:
    _state["fails"] += 1
    log.info("vnstock %s failed (%s: %s)", what, type(e).__name__, str(e)[:120])
    if _state["fails"] >= FAIL_LIMIT:
        _state["open_until"] = time.time() + COOLDOWN_S
        _state["fails"] = 0
        log.warning("vnstock layer paused for %d min after %d consecutive failures — using direct VCI",
                    COOLDOWN_S // 60, FAIL_LIMIT)


def call(fn: Callable[[], Any], what: str, tries: int = 2, base_wait: float = 1.0, max_wait: float = 4.0,
         quiet: bool = True) -> Any:
    """Run fn() with retries + backoff. Returns None on failure (never raises). Library chatter is muted."""
    if not available():
        return None
    last: BaseException | None = None
    for i in range(tries):
        try:
            if quiet:
                buf = io.StringIO()
                with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(buf):
                    out = fn()
            else:
                out = fn()
            _ok()
            return out
        except BaseException as e:            # library raises SystemExit on rate limit
            last = e
            if i + 1 < tries:
                time.sleep(min(max_wait, base_wait * 2 ** i))
    _fail(what, last or RuntimeError("unknown"))
    return None


# ─── Prices ────────────────────────────────────────────────────────
def history(symbol: str, bars: int = 1500, **kw) -> dict[str, list] | None:
    """Daily OHLCV (oldest first) via vnstock_data Quote(source='VCI').history, in the same shape as
    invest.history: {t, o, h, l, c, v}; prices in VND (index levels unscaled). None when unavailable."""
    mod = _module() if available() else None
    if mod is None:
        return None
    sym = symbol.upper()
    start = (date.today() - timedelta(days=int(bars * 1.5) + 10)).isoformat()
    end = date.today().isoformat()
    df = call(lambda: mod.Quote(source="VCI", symbol=sym).history(start=start, end=end, interval="1D"),
              f"history {sym}", **kw)
    if df is None or not len(df) or "close" not in df.columns:
        return None
    df = df.tail(bars)
    k = 1.0 if sym in INDEX_SYMBOLS else 1000.0
    try:
        t = [(x.date() if hasattr(x, "date") else datetime.fromisoformat(str(x)[:10]).date()).isoformat() for x in df["time"]]
        out = {"t": t, **{s: [round(float(x) * k, 4) for x in df[c]] for s, c in
                          (("o", "open"), ("h", "high"), ("l", "low"), ("c", "close"))},
               "v": [float(x) for x in df["volume"]]}
    except Exception as e:
        log.info("vnstock history %s parse failed: %s", sym, str(e)[:120])
        return None
    if k != 1.0 and out["c"] and not (100 <= out["c"][-1] <= 10_000_000):     # unit sanity check
        log.warning("vnstock history %s: unexpected price scale (%s) — ignoring", sym, out["c"][-1])
        return None
    return out


def _pick(row: Any, *keys) -> Any:
    for k in keys:
        try:
            v = row[k]
        except Exception:
            continue
        if v is not None and not (isinstance(v, float) and v != v):
            return v
    return None


def price_board(symbols: list[str], **kw) -> dict[str, dict] | None:
    """Price board via vnstock_data Trading(source='VCI').price_board — listed shares, exchange, trading status,
    last/reference price (VND), session volume. Batched by 50. None when the layer is unusable."""
    mod = _module() if available() else None
    if mod is None or not symbols:
        return None
    out: dict[str, dict] = {}
    for k in range(0, len(symbols), 50):
        batch = [s.upper() for s in symbols[k:k + 50]]
        df = call(lambda: mod.Trading(source="VCI", symbol=batch[0]).price_board(batch), "price_board", **kw)
        if df is None or not len(df):
            continue
        for _, r in df.iterrows():
            sym = _pick(r, ("listing", "symbol"))
            if not sym:
                continue
            ls = _pick(r, ("listing", "listed_share"))
            out[str(sym)] = {
                "listed_share": int(ls) if ls else None,
                "exchange": _pick(r, ("listing", "exchange")),
                "organ_name": _pick(r, ("listing", "organ_name")),
                "trading_status": _pick(r, ("listing", "trading_status")),
                "is_delisted": bool(_pick(r, ("listing", "is_delisted")) or 0),
                "trading_date": _pick(r, ("listing", "trading_date")),
                "ref_price": _pick(r, ("listing", "ref_price")),
                "match_price": _pick(r, ("match", "match_price")),
                "volume": _pick(r, ("match", "accumulated_volume")),
            }
    return out or None


def listed_shares(symbols: list[str]) -> dict[str, int] | None:
    pb = price_board(symbols)
    if not pb:
        return None
    return {s: v["listed_share"] for s, v in pb.items() if v.get("listed_share")}


# ─── Reference / fundamentals (used by tools/build/banks_vnstock.py) ─
def symbols_by_exchange(**kw):
    mod = _module() if available() else None
    return None if mod is None else call(lambda: mod.Listing(source="VCI").symbols_by_exchange(), "listing", **kw)


def finance(symbol: str, report: str, period: str = "year", source: str = "VCI", **kw):
    """vnstock_data Finance(source, symbol, period).<report>() in long format (period, id, name, …, value)."""
    mod = _module() if available() else None
    if mod is None:
        return None
    return call(lambda: getattr(mod.Finance(source=source, symbol=symbol, period=period), report)(),
                f"finance {source} {symbol} {report} {period}", **kw)


def host_reachable(host: str, timeout: float = 10.0) -> tuple[bool, str]:
    """Cheap pre-flight for a data host (any HTTP answer = reachable; proxy 403 / reset / DNS = not)."""
    try:
        import requests
        r = requests.get(f"https://{host}/", timeout=timeout)
        return True, f"HTTP {r.status_code}"
    except Exception as e:
        return False, f"{type(e).__name__}: {str(e)[:100]}"
