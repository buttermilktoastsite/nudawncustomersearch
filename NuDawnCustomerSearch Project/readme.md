# Shopify Customer Finder

Discover 50–100 direct-to-consumer leads per product category, detect Shopify precisely, and export to XLSX. Single FastAPI web service serving frontend + API. Uses Upstash Redis for persistence. Optional SerpAPI (fallback to DuckDuckGo).

## Features
- Heuristic + precise Shopify detection (robots.txt, Shopify assets, theme files, HTML markers)
- SerpAPI (optional) with automatic fallback to DuckDuckGo HTML
- XLSX export (Brand Name, URL, Shopify?, Amazon?)
- Job queue with polling
- Redis persistence of job meta and XLSX bytes (no S3)

## Requirements
- Python 3.10+
- Redis (recommended: Upstash Redis)
- Optional SerpAPI key

## Environment variables
- `REDIS_URL` (required): `redis://default:<password>@<host>:6379`
- `SERPAPI_KEY` (optional): enables SerpAPI if `SEARCH_ENGINE=auto` or `serpapi`
- `SEARCH_ENGINE` (optional): `auto` (default) | `serpapi` | `duckduckgo`
- Tuning:
  - `SEARCH_CONCURRENCY` (default 4)
  - `SEARCH_PAGES_PER_QUERY` (default 3)
  - `DETECT_CONCURRENCY` (default 6)
  - `TARGET_LEADS_MIN` (default 50)
  - `TARGET_LEADS_MAX` (default 100)
  - `JOB_TTL_SECONDS` (default 3600)
  - `REQUEST_TIMEOUT` (default 15.0)

## Local run
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r app/requirements.txt
export REDIS_URL="redis://default:<password>@<host>:6379"
uvicorn app.main:app --host 0.0.0.0 --port 10000 --reload