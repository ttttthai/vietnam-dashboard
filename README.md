# Vietnam Dashboard

Interactive dashboard covering Vietnam's 34 post-merger provinces, macro/banking system, policy, strategy
and investing, plus FY2024 fundamentals for 17 listed commercial banks.

## Stack

- **Backend:** FastAPI (`server.py`). Live VN-Index / VN30 / bank prices come straight from Vietcap (VCI)
  public endpoints via `invest.py` (standard library only); FX from open.er-api.com (frankfurter fallback).
- **Frontend:** vanilla HTML + D3 + inline SVG (`vietnam_dashboard.html`), no build step. Tab datasets are
  embedded in the page as constants so it also works from `file://`.
- **Scheduler:** APScheduler, timezone Asia/Ho_Chi_Minh
  - daily **00:00** — market snapshot refresh (also once on startup)
  - monthly, **15th 01:00** — public macro APIs (World Bank, IMF, FRED) → `data/auto/` (also on startup when a
    snapshot is missing or older than 31 days; set `AUTO_FETCH_ON_STARTUP=0` to skip)

**vnstock** is *not* installed and not required: it was quarantined on PyPI on 2026-09-24, and its heavy
dependencies exceed Render's free tier. `server.py` still uses it if present, otherwise the VCI endpoints.

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
- `GET /api/banks?period=year|quarter[&include_synthetic=1]` — 17 banks; reported periods only (FY2024) plus
  `latest_reported_loans` (30/6/2026, from Finance's `listed_banks_by_sector`). `include_synthetic=1` adds the old
  extrapolated periods, each flagged `synthetic: true`
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
| `data/finance.json` | Finance | `FINSYS` (also `/api/rates`, `/api/banks` loans) |
| `data/invest_macro.json` | Economy | `MACRO` for the Đầu tư tab, read by `invest.py` at call time (in-file fallback) |
| `strategy_directives.json` | Strategy | `/api/strategy` |
| `data/research/*` | Research | inventory, release calendar, source log, plans — ops files, served by `/api/freshness`, not embedded |
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
