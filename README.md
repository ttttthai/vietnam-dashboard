# Vietnam Dashboard

Interactive dashboard covering Vietnam's 34 post-merger provinces, macro/banking system, policy, strategy
and investing, plus every listed commercial bank (28 on HOSE / HNX / UPCoM, `data/banks_vnstock.json`).

## Stack

- **Backend:** FastAPI (`server.py`). Live VN-Index / VN30 / bank prices: the optional vnstock layer
  (`vnstock_layer.py`) first, then Vietcap (VCI) public endpoints via `invest.py` (standard library only) as the
  fallback; FX from open.er-api.com (frankfurter fallback).
- **Frontend:** vanilla HTML + D3 + inline SVG (`vietnam_dashboard.html`), no build step. Tab datasets are
  embedded in the page as constants so it also works from `file://`.
- **Scheduler:** APScheduler, timezone Asia/Ho_Chi_Minh
  - daily **00:00** — market snapshot refresh (also once on startup)
  - monthly, **15th 01:00** — public macro APIs (World Bank, IMF, FRED) → `data/auto/` (also on startup when a
    snapshot is missing or older than 31 days; set `AUTO_FETCH_ON_STARTUP=0` to skip)

## vnstock (optional)

The server works without it. When the sponsor library **`vnstock_data`** imports *and* an API key is configured,
`vnstock_layer.py` is used first for daily prices/volumes (`Quote.history`), listed shares (`Trading.price_board`)
and — via `tools/build/banks_vnstock.py` — the listed-bank universe and fundamentals; on any failure (or after 3
consecutive failures, for 15 min) the direct VCI code takes over. Responses carry `source` / `price_source`.

**Never install vnstock, vnai or vnstock_installer from public PyPI** — they were quarantined there on 2026-09-24
and PyPI hosts a dependency-confusion squatter (`vnstock_installer==99.0.0`). Nothing vnstock-related is in
`requirements.txt`. Install from the vnstocks sponsor index only, pinned and without dependency resolution, then
add the ordinary dependencies from PyPI:

```bash
python3 -m venv /tmp/vnenv && /tmp/vnenv/bin/pip install -U pip setuptools wheel
# 1) the installer and vnai — vnstocks index ONLY (never --extra-index-url: PyPI's squatter would win), pinned, no deps
/tmp/vnenv/bin/pip install --no-build-isolation --no-deps --index-url https://vnstocks.com/api/simple \
    "vnai==2.6.3" "vnstock_installer==3.1.3"
/tmp/vnenv/bin/pip install requests uv Eel cffi pandas psutil unidecode   # their ordinary deps (PyPI)
# 2) the sponsor library, fetched by the installer with your key (VNSTOCK_API_KEY or ~/.vnstock/api_key.json)
/tmp/vnenv/bin/vnstock-cli-installer --list-packages --non-interactive
/tmp/vnenv/bin/vnstock-cli-installer --packages vnstock_data --venv-path /tmp/vnenv \
    --python /tmp/vnenv/bin/python --non-interactive
/tmp/vnenv/bin/pip install -r requirements.txt        # server deps (fastapi, uvicorn, apscheduler…)
```
Verified 2026-10-06: installer 3.1.3 (sha256 2eef4b9a…c12e on the vnstocks index), vnstock_data 3.3.1. If an import
fails on a missing module (e.g. `unidecode`), install that ordinary library from PyPI.

- **Key:** `VNSTOCK_API_KEY` environment variable (on Render: a secret env var) or `~/.vnstock/api_key.json`.
  The library reads it itself; the server only checks that one of the two exists and never logs it. Never commit it.
- **Telemetry:** `vnstock_layer.py` forces `VNSTOCK_TELEMETRY=off` before importing the library; set it for any
  manual run too.
- **Bank universe:** `VNSTOCK_TELEMETRY=off /tmp/vnenv/bin/python tools/build/banks_vnstock.py` rebuilds
  `data/banks_vnstock.json` (retries, cache in `/tmp/vnstock_banks_cache`, prints a coverage table). Finance
  sources (VCI → `iq.vietcap.com.vn`, MAS, KBS, MBK) are pre-flighted; unreachable ones are recorded in
  `_meta.coverage.sources` and their fields stay null. Each period entry also carries `bs: {item_key: bn VND}`,
  the full balance sheet mapped to the TT49 bank template (`_meta.bs_items`); source rows that match no template
  item are listed per bank in `fetch.unmapped`.
- **Filling the balance-sheet history (run on your own machine):** the fundamentals hosts (`iq.vietcap.com.vn`,
  KBS, MAS, MBK, VNDirect, SSI, TCBS) are blocked from the agents' sandbox, so `data/banks_vnstock.json` has no
  `bs` data until the builder runs where they are reachable:

  ```bash
  VNSTOCK_TELEMETRY=off /tmp/vnenv/bin/python tools/build/banks_vnstock.py   # or your own venv's python with vnstock_data
  git add data/banks_vnstock.json && git commit -m "banks: full balance sheets (vnstock)"
  ```

  The coverage table it prints has a `bs` column (template items found per bank); check `fetch.unmapped` for rows
  worth adding to `BS_TEMPLATE`. Until then `/api/banks/bs_history` returns nulls with coverage 0.

## Run locally

```bash
pip install -r requirements.txt
uvicorn server:app --host 0.0.0.0 --port 8001 --reload
python3 -m unittest discover -s tests -v     # parsing / freshness tests (no network)
```

Open http://localhost:8001/

## Deploy to Render

Push this repo to GitHub and connect it to Render — `render.yaml` does the rest. Render has open egress, so the
monthly API pulls work there (the agents' sandbox blocks them; the server then keeps the previous snapshot).

## API

- `GET /` — dashboard HTML
- `GET /api/snapshot` — combined snapshot (indices, fx, rates, banks)
- `GET /api/rates` — SBV refinancing / OMO / rediscount rates and 12-month deposit / average lending rates, each
  with date and source, read from `data/policy.json` and `data/finance.json` (no hard-coded values)
- `GET /api/banks?period=year|quarter[&include_synthetic=1]` — every listed bank in `data/banks_vnstock.json`
  (falls back to the curated 17 when the file is missing). Reported periods only: vnstock periods where the file has
  them, else the curated FY2024 snapshot (17 banks), else nulls; histories are aligned on `periods` (padding entries
  are null with `missing: true`). Rows add `exchange`, `organ_name`, `state_owned`, `source`, `nii`, `toi`, `pbt`,
  `npat`, `listed_shares`, `market_cap_bn`, `price_source`, plus `latest_reported_loans` (30/6/2026, Finance's
  `listed_banks_by_sector`). `include_synthetic=1` adds the old extrapolated periods, each flagged `synthetic: true`
- `GET /api/banks/universe` — the listed-bank universe with per-bank coverage, finance-source reachability and the
  vnstock layer status
- `GET /api/banks/bs_history` — full balance sheet of every listed bank for the last 8 quarters (2024-Q3 → 2026-Q2),
  for the Appendix mini charts: `{periods, items, banks: {TICKER: {name, exchange, values: {item_key: [8]}, basis: [8],
  source: [8], url: [8]}}, aggregate: {values: {item_key: [8]}, coverage: {item_key: [8 bank counts]}, basis_mix},
  banks_total, banks_with_data, units, as_of, note}`. `items` is the ordered TT49 template (`key`, `side`
  asset/liability/equity, `vi`, `en`, `parent`, `total`). Values come from `data/banks_vnstock.json` (vnstock); a
  bank-quarter vnstock left empty is filled from `data/finance.json` `FINSYS.listed_banks_latest` (`customer_loans` →
  `customer_loans_gross`, `equity` → `equity_total`, `items` by key), never mixing consolidated and parent-only figures
  in one bank-quarter. `aggregate.values` sums the banks that report the item that quarter — always read it with
  `aggregate.coverage` (out of `banks_total`). Null = not reported
- `GET /api/banks/statements`, `/api/banks/breakdown`, `/api/banks/lineitem/{key}` — system BS / IS and breakdowns
  (`synthetic_history: true`: histories are modelled, not reported)
- `GET /api/banks/{symbol}/entities` — subsidiaries & affiliates
- `GET /api/freshness[?tab=…&status=fresh|due|overdue|waiting]` — per-series freshness (id, tab, owner_agent,
  latest_period, next_expected, status) and a per-tab summary, computed from `data/research/inventory.json` and
  `release_calendar.json` against today in Asia/Ho_Chi_Minh
- `GET /api/auto[?source=world_bank|imf_datamapper|fed_funds]` — latest server-fetched snapshots + fetch status
- `POST /api/auto/refresh[?source=…]` — re-pull the public APIs now
- `GET /api/refresh` — manual market refresh; `GET /api/logs` — activity log
- `GET /api/strategy` — `strategy_directives.json`; `GET /api/i18n/en` — English dictionary
- `GET /api/invest/context` — rules, macro context (`data/invest_macro.json`), VN-Index
- `GET /api/invest/research` — curated research notes (`invest_research.json`)
- `GET /api/invest/screen?group=VN30&horizon=63&target=0` — rank a basket by the 5 conditions
- `GET /api/invest/analyze?symbols=HPG,FPT&horizon=63&target=0` — deep dive (max 4 symbols, max 63 sessions)

## Data ownership

Each tab's data lives in one JSON file owned by one agent; top-level keys are the page constants it feeds and
`_meta` (owner, `as_of`, sources) is never embedded.

| File | Owner | Feeds |
|---|---|---|
| `data/economy.json` | Economy | `ECONFLOW`, `ECON_OFFICIAL`, `CPI_*` |
| `data/society.json` | Society | `SOC_PROV_DATA`, `POP_SERIES`, `SOC_GRDP`, `WORLD_RANK`, `PROV_PREV`, `RELIGION` |
| `data/policy.json` | Policy | `POLICY` (also `/api/rates`) |
| `data/finance.json` | Finance | `FINSYS` (also `/api/rates`, `/api/banks` loans, `/api/banks/bs_history` via `listed_banks_latest`) |
| `data/invest_macro.json` | Economy | `MACRO` for the Đầu tư tab, read by `invest.py` at call time (in-file fallback) |
| `strategy_directives.json` | Strategy | `/api/strategy` |
| `data/research/*` | Research | inventory, release calendar, source log, plans — ops files, served by `/api/freshness`, not embedded |
| `data/banks_vnstock.json` | server (vnstock) | `/api/banks`, `/api/banks/universe`, `/api/banks/bs_history` (built by `tools/build/banks_vnstock.py`) |
| `data/auto/*` | server | World Bank / IMF / FRED snapshots for agents to review; agents decide what to copy into their files |

After editing a tab file, embed it into the page:

```bash
python3 tools/build/embed_data.py               # all tab files (economy, society, policy, finance)
python3 tools/build/embed_data.py finance       # one file
python3 tools/build/embed_data.py --check       # show what would change + each file's _meta.as_of
python3 tools/build/embed_data.py --check --strict   # fail if a file's _meta.as_of is missing
```

`tools/build/make_preview.py [server_url]` snapshots the read-only API responses (including `/api/freshness` and
`/api/auto`) into a self-contained offline preview.

## Agents

Agent definitions live in `.claude/agents/`:

- **Strategy** (`strategy.md`) — Đảng & Chính phủ tab: directives, drafts, news → `strategy_directives.json`
- **Economy** (`economy.md`) — Kinh tế tab: GDP, BoP, budget, CPI → `data/economy.json`, `data/invest_macro.json`
- **Society** (`society.md`) — Xã hội tab: population, provinces, GRDP, world ranks, religion → `data/society.json`
- **Policy** (`policy.md`) — Chính sách tab: monetary/fiscal instrument registry → `data/policy.json`
- **Finance** (`finance.md`) — Hệ thống tài chính tab: money, credit, banks, rates → `data/finance.json`
- **Research** (`research.md`) — update planning, release calendar, consistency checks → `data/research/`
- **Ed** (`ed.md`) — chief editor: story layer, design and the published page (no data ownership)
