# Vietnam Dashboard

Interactive dashboard covering Vietnam's 34 post-merger provinces, macro/banking system, and FY2024 financials for 17 listed commercial banks.

## Stack

- **Backend:** FastAPI + vnstock (live VN-Index / HNX-Index / FX)
- **Frontend:** vanilla HTML + D3 + inline SVG, no build step
- **Scheduler:** APScheduler — daily data refresh at 15:30 Asia/Ho_Chi_Minh

## Run locally

```bash
pip install -r requirements.txt
uvicorn server:app --host 0.0.0.0 --port 8001 --reload
```

Open http://localhost:8001/

## Deploy to Render

Push this repo to GitHub and connect it to Render — `render.yaml` does the rest.

## API

- `GET /` — dashboard HTML
- `GET /api/snapshot` — combined macro snapshot
- `GET /api/banks?period=year|quarter`
- `GET /api/banks/statements` — system BS + IS
- `GET /api/banks/breakdown` — lending/funding breakdowns
- `GET /api/banks/lineitem/{key}` — per-item drill-down
- `GET /api/banks/{symbol}/entities` — subsidiaries & affiliates
