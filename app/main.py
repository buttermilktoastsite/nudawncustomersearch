import asyncio
import io
import os
import time
import secrets
from typing import Dict, Optional, List, Tuple
from urllib.parse import urlparse, urlunparse, urlencode, parse_qs, urljoin

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, FileResponse
from starlette.staticfiles import StaticFiles

from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

import openpyxl
import httpx
from bs4 import BeautifulSoup
from redis import asyncio as aioredis

# -------- Configuration --------
FRONTEND_ORIGIN = os.getenv("FRONTEND_ORIGIN", "*")
AMAZON_CHECK_CONCURRENCY = int(os.getenv("AMAZON_CHECK_CONCURRENCY", "4"))
AMAZON_CHECK_DELAY_MS = int(os.getenv("AMAZON_CHECK_DELAY_MS", "250"))
REDIS_URL = os.getenv("REDIS_URL")
JOB_TTL_SECONDS = int(os.getenv("JOB_TTL_SECONDS", "3600"))

# Discovery tuning
SEARCH_CONCURRENCY = int(os.getenv("SEARCH_CONCURRENCY", "4"))
SEARCH_PAGES_PER_QUERY = int(os.getenv("SEARCH_PAGES_PER_QUERY", "3"))
TARGET_LEADS_MIN = int(os.getenv("TARGET_LEADS_MIN", "50"))
TARGET_LEADS_MAX = int(os.getenv("TARGET_LEADS_MAX", "100"))
REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "15.0"))
USER_AGENT = os.getenv("USER_AGENT", "Mozilla/5.0 (compatible; LeadFinderBot/1.3; +https://example.com/bot)")

# Search engine selection
SERPAPI_KEY = os.getenv("SERPAPI_KEY")
SEARCH_ENGINE = os.getenv("SEARCH_ENGINE", "auto").lower()

# Shopify detection concurrency
DETECT_CONCURRENCY = int(os.getenv("DETECT_CONCURRENCY", "6"))

if not REDIS_URL:
    raise RuntimeError("Missing REDIS_URL environment variable")

# Validate Redis URL scheme
if not REDIS_URL.lower().startswith(("redis://", "rediss://", "unix://")):
    raise RuntimeError("REDIS_URL must start with redis://, rediss://, or unix://")

# -------- App + Middleware --------
limiter = Limiter(key_func=get_remote_address)
app = FastAPI(title="Shopify Customer Finder")
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[FRONTEND_ORIGIN] if FRONTEND_ORIGIN != "*" else ["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# -------- Redis --------
def _redis_ssl_flag(url: str) -> bool:
    """Enable SSL/TLS only when URL starts with rediss://"""
    return url.strip().lower().startswith("rediss://")

redis = aioredis.StrictRedis.from_url(
    REDIS_URL,
    decode_responses=False,
    ssl=_redis_ssl_flag(REDIS_URL),
    socket_timeout=15,
    socket_connect_timeout=15,
    retry_on_timeout=True,
    health_check_interval=30,
)

# -------- Key helpers --------
def k_job(job_id: str) -> str:
    return f"job:{job_id}"

def k_job_lock(job_id: str) -> str:
    return f"job:{job_id}:lock"

def k_job_file(job_id: str) -> str:
    return f"job:{job_id}:file"

# -------- Utilities --------
async def sleep_ms(ms: int):
    await asyncio.sleep(ms / 1000)

def now_ms() -> int:
    return int(time.time() * 1000)

def normalize_url(url: str) -> str:
    """Remove tracking params and normalize URL"""
    try:
        u = urlparse(url)
        query = parse_qs(u.query)
        for k in list(query.keys()):
            if k.lower().startswith("utm_") or k.lower() in ("gclid", "fbclid"):
                query.pop(k, None)
        new_query = urlencode({k: v[0] if isinstance(v, list) else v for k, v in query.items()})
        scheme = u.scheme or "https"
        netloc = (u.netloc or "").lower()
        path = u.path if u.path.startswith("/") else f"/{u.path}"
        return urlunparse((scheme, netloc, path, "", new_query, ""))
    except:
        return url

def get_hostname(url: str) -> str:
    try:
        return (urlparse(url).hostname or "").lower()
    except:
        return ""

def get_root_domain(url: str) -> str:
    """Extract root domain for deduplication"""
    try:
        hostname = get_hostname(url).replace("www.", "")
        parts = hostname.split(".")
        if len(parts) >= 2:
            return parts[-2]
        return hostname
    except:
        return url

def is_marketplace_url(url: str) -> bool:
    """Filter out marketplace URLs"""
    host = get_hostname(url)
    if not host:
        return False
    bad_roots = ("amazon.", "walmart.", "target.", "etsy.", "ebay.")
    return host.startswith(bad_roots) or any(f".{b}" in host for b in bad_roots)

# -------- Shopify detection (heuristic + precise) --------
def looks_like_shopify_heuristic(url: str, snippet: str = "") -> bool:
    """Quick heuristic check for Shopify indicators"""
    url_l = url.lower()
    snip_l = (snippet or "").lower()
    if "cdn.shopify.com" in url_l or "cdn.shopify.com" in snip_l:
        return True
    if "powered by shopify" in url_l or "powered by shopify" in snip_l:
        return True
    if "/products/" in url_l or "/collections/" in url_l or "/cart" in url_l:
        return True
    return False

SHOPIFY_JS_PATTERNS = [
    r"window\.Shopify",
    r"ShopifyAnalytics",
    r"Shopify\.designMode",
]
SHOPIFY_META_NAMES = [
    "shopify-digital-wallet",
]
SHOPIFY_ASSET_HINTS = [
    "cdn.shopify.com",
    "shopifycloud.com",
    "/assets/theme.js",
    "/assets/theme.css",
    ".liquid",
]

def detect_shopify_from_html(html: str) -> bool:
    """Deep HTML inspection for Shopify markers"""
    if not html:
        return False
    low = html.lower()
    if "cdn.shopify.com" in low or "shopifycloud.com" in low:
        return True
    if any(pat in html for pat in SHOPIFY_JS_PATTERNS):
        return True
    for name in SHOPIFY_META_NAMES:
        if f'name="{name}"' in low or f"name='{name}'" in low:
            return True
    for hint in SHOPIFY_ASSET_HINTS:
        if hint in low:
            return True
    return False

async def fetch_text(client: httpx.AsyncClient, url: str, method: str = "GET") -> Optional[str]:
    """Fetch URL text with error handling"""
    try:
        if method == "HEAD":
            r = await client.head(url, follow_redirects=True)
            if r.status_code < 400:
                return ""
            return None
        r = await client.get(url, follow_redirects=True)
        if r.status_code < 400:
            return r.text
        return None
    except:
        return None

async def precise_shopify_check(base_url: str) -> Optional[bool]:
    """Multi-method precise Shopify detection"""
    base = normalize_url(base_url)
    if not base.startswith("http"):
        base = "https://" + base.lstrip("/")
    if not base.endswith("/"):
        base = base + "/"

    headers = {"User-Agent": USER_AGENT, "Accept-Language": "en-US,en;q=0.9"}
    
    try:
        async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT, headers=headers) as client:
            # Check robots.txt
            robots = await fetch_text(client, urljoin(base, "robots.txt"))
            if robots and ("shopify" in robots.lower()):
                return True

            # Check homepage HTML
            home_html = await fetch_text(client, base)
            if home_html is not None and detect_shopify_from_html(home_html):
                return True

            # Check theme assets
            for asset in ("assets/theme.js", "assets/theme.css"):
                txt = await fetch_text(client, urljoin(base, asset), method="HEAD")
                if txt is not None:
                    return True

            # If we got homepage but no Shopify markers, it's likely not Shopify
            if home_html is not None:
                return False
            
            return None
    except:
        return None

# -------- Amazon presence check --------
async def check_amazon_presence(brand: str) -> str:
    """Check if brand appears on Amazon search"""
    q = brand
    url = f"https://www.amazon.com/s?k={q}"
    headers = {
        "User-Agent": USER_AGENT,
        "Accept-Language": "en-US,en;q=0.9",
    }
    try:
        async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
            r = await client.get(url, headers=headers)
            if r.status_code != 200:
                return "No"
            txt = r.text.lower()
            if "results for" in txt or "s-result" in txt:
                return "Possibly Yes"
            return "No"
    except:
        return "Unknown"

# -------- Discovery via SerpAPI or DuckDuckGo --------
async def search_serpapi(query: str, page: int) -> List[Tuple[str, str, str]]:
    """Search using SerpAPI"""
    start = (page - 1) * 10
    url = "https://serpapi.com/search.json"
    params = {
        "engine": "google",
        "q": query,
        "hl": "en",
        "gl": "us",
        "num": "10",
        "start": str(start),
        "api_key": SERPAPI_KEY,
    }
    headers = {"User-Agent": USER_AGENT}
    out: List[Tuple[str, str, str]] = []
    try:
        async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
            r = await client.get(url, params=params, headers=headers)
            if r.status_code != 200:
                return out
            data = r.json()
            for item in (data.get("organic_results") or []):
                link = item.get("link") or ""
                title = (item.get("title") or "").strip()
                snippet = (item.get("snippet") or "").strip()
                if link:
                    out.append((link, title, snippet))
    except:
        pass
    return out

async def search_duckduckgo(query: str, page: int) -> List[Tuple[str, str, str]]:
    """Search using DuckDuckGo HTML"""
    offset = (page - 1) * 30
    params = {"q": query, "kl": "us-en", "s": str(offset)}
    url = "https://duckduckgo.com/html"
    headers = {"User-Agent": USER_AGENT}
    results: List[Tuple[str, str, str]] = []
    try:
        async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
            r = await client.post(url, data=params, headers=headers)
            if r.status_code != 200:
                return results
            soup = BeautifulSoup(r.text, "html.parser")
            for result in soup.select("div.result"):
                a = result.select_one("a.result__a")
                if not a:
                    continue
                href = a.get("href") or ""
                title = a.get_text(" ", strip=True)
                snippet_el = result.select_one(".result__snippet")
                snippet = snippet_el.get_text(" ", strip=True) if snippet_el else ""
                if href and href.startswith("http"):
                    results.append((href, title, snippet))
    except:
        pass
    return results

def build_queries(category: str) -> List[str]:
    """Build search queries for lead discovery"""
    base = category.strip()
    q = [
        f'"powered by shopify" "{base}"',
        f'"cdn.shopify.com" "{base}"',
        f'"{base}" "/products/" -amazon -walmart -target -etsy -ebay',
        f'"{base}" "/collections/" -amazon -walmart -target -etsy -ebay',
        f'"{base}" "add to cart" product -amazon -walmart -target -etsy -ebay',
        f'"{base}" "shop" "official site" -amazon -walmart -target -etsy -ebay',
    ]
    return q

async def discover_leads(category: str) -> List[Dict[str, str]]:
    """Discover leads via search engines"""
    queries = build_queries(category)
    leads: List[Dict[str, str]] = []
    seen_urls = set()
    sem = asyncio.Semaphore(SEARCH_CONCURRENCY)

    use_serpapi = (SEARCH_ENGINE == "serpapi") or (SEARCH_ENGINE == "auto" and SERPAPI_KEY)
    
    async def do_search(query: str, page: int) -> List[Tuple[str, str, str]]:
        if use_serpapi:
            return await search_serpapi(query, page)
        else:
            return await search_duckduckgo(query, page)

    async def fetch_query(q: str):
        nonlocal leads, seen_urls
        for page in range(1, SEARCH_PAGES_PER_QUERY + 1):
            async with sem:
                try:
                    results = await do_search(q, page)
                    for url, title, snippet in results:
                        if is_marketplace_url(url):
                            continue
                        nurl = normalize_url(url)
                        if nurl in seen_urls:
                            continue
                        seen_urls.add(nurl)
                        base_title = title.split(" - ")[0].split(" | ")[0].strip() if title else ""
                        brand = base_title or get_root_domain(nurl)
                        shopify_guess = "Yes" if looks_like_shopify_heuristic(nurl, f"{title} {snippet}") else "Unknown"
                        leads.append({
                            "Brand Name": brand or get_root_domain(nurl),
                            "URL": nurl,
                            "Is this a Shopify site?": shopify_guess,
                            "Already selling on Amazon?": "Unknown",
                        })
                        if len(leads) >= TARGET_LEADS_MAX:
                            return
                except Exception:
                    pass
            if len(leads) >= TARGET_LEADS_MAX:
                return

    tasks = [fetch_query(q) for q in queries]
    await asyncio.gather(*tasks)

    return leads[:max(TARGET_LEADS_MIN, min(TARGET_LEADS_MAX, len(leads)))]

def dedupe_by_root_domain(leads: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """Deduplicate leads by root domain"""
    seen = set()
    out: List[Dict[str, str]] = []
    for lead in leads:
        domain = get_root_domain(lead.get("URL") or "")
        if not domain:
            continue
        if domain in seen:
            continue
        seen.add(domain)
        out.append(lead)
    return out

def leads_to_xlsx_bytes(leads: List[Dict[str, str]]) -> bytes:
    """Convert leads to XLSX bytes"""
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Leads"
    headers = [
        "Brand Name",
        "URL",
        "Is this a Shopify site?",
        "Already selling on Amazon?"
    ]
    ws.append(headers)
    for row in leads:
        ws.append([
            row.get("Brand Name") or "",
            row.get("URL") or "",
            row.get("Is this a Shopify site?") or "",
            row.get("Already selling on Amazon?") or "Unknown",
        ])
    # Bold headers
    for cell in ws[1]:
        cell.font = openpyxl.styles.Font(bold=True)
    
    stream = io.BytesIO()
    wb.save(stream)
    return stream.getvalue()

async def refine_shopify_detection(leads: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """Refine Shopify detection with precise checks"""
    sem = asyncio.Semaphore(DETECT_CONCURRENCY)
    
    async def check_one(lead):
        url = lead.get("URL") or ""
        async with sem:
            verdict = await precise_shopify_check(url)
            if verdict is True:
                lead["Is this a Shopify site?"] = "Yes"
            elif verdict is False:
                lead["Is this a Shopify site?"] = "No"
            else:
                if lead.get("Is this a Shopify site?") not in ("Yes", "No"):
                    lead["Is this a Shopify site?"] = "Unknown"
    
    await asyncio.gather(*(check_one(l) for l in leads), return_exceptions=True)
    return leads

async def enrich_amazon_presence(leads: List[Dict[str, str]]):
    """Enrich leads with Amazon presence check"""
    sem = asyncio.Semaphore(AMAZON_CHECK_CONCURRENCY)
    
    async def check_one(lead):
        brand = (lead.get("Brand Name") or "").strip() or get_root_domain(lead.get("URL") or "")
        async with sem:
            lead["Already selling on Amazon?"] = await check_amazon_presence(brand)
            await sleep_ms(AMAZON_CHECK_DELAY_MS)
    
    await asyncio.gather(*(check_one(l) for l in leads), return_exceptions=True)
    return leads

# -------- Redis helpers --------
async def job_update(job_id: str, mapping: Dict[str, str]):
    """Update job metadata in Redis"""
    bmap = {k.encode(): v.encode() for k, v in mapping.items()}
    await redis.hset(k_job(job_id), mapping=bmap)
    await redis.expire(k_job(job_id), JOB_TTL_SECONDS)

async def job_read(job_id: str) -> Dict[str, str]:
    """Read job metadata from Redis"""
    data = await redis.hgetall(k_job(job_id))
    if not data:
        return {}
    return {k.decode(): v.decode() for k, v in data.items()}

async def job_set_file(job_id: str, blob: bytes):
    """Store job result file in Redis"""
    await redis.set(k_job_file(job_id), blob, ex=JOB_TTL_SECONDS)

async def job_get_file(job_id: str) -> Optional[bytes]:
    """Retrieve job result file from Redis"""
    return await redis.get(k_job_file(job_id))

async def job_claim_lock(job_id: str, ttl: int = 240) -> bool:
    """Claim a lock for job processing"""
    return await redis.set(k_job_lock(job_id), b"1", ex=ttl, nx=True) is True

async def job_release_lock(job_id: str):
    """Release job processing lock"""
    try:
        await redis.delete(k_job_lock(job_id))
    except:
        pass

# -------- API Routes --------
@app.post("/api/jobs")
@limiter.limit("20/minute")
async def create_job(request: Request):
    """Create a new lead discovery job"""
    data = await request.json()
    category = (data.get("category") or "").strip()
    include = bool(data.get("include_amazon_check"))
    
    if not category:
        raise HTTPException(status_code=400, detail="Missing category")

    job_id = f"{int(time.time())}_{secrets.token_urlsafe(8)}"
    await job_update(job_id, {
        "status": "queued",
        "progress": "0",
        "created_at": str(now_ms()),
        "updated_at": str(now_ms()),
        "category": category,
        "include_amazon_check": "true" if include else "false"
    })
    
    # Start job processing in background
    asyncio.create_task(run_job(job_id))
    
    return {"job_id": job_id}

@app.get("/api/jobs/{job_id}")
async def get_job(job_id: str):
    """Get job status"""
    meta = await job_read(job_id)
    if not meta:
        return {"status": "not_found"}
    
    allowed = {k: meta.get(k) for k in ["status", "message", "progress", "created_at", "updated_at"]}
    return allowed

@app.get("/api/jobs/{job_id}/result")
async def get_result(job_id: str):
    """Download job result XLSX"""
    meta = await job_read(job_id)
    if not meta:
        raise HTTPException(status_code=404, detail="Not found")
    if meta.get("status") != "completed":
        raise HTTPException(status_code=409, detail="Job not completed")
    
    blob = await job_get_file(job_id)
    if not blob:
        raise HTTPException(status_code=410, detail="Result expired")
    
    filename = meta.get("filename") or "leads.xlsx"
    return StreamingResponse(
        io.BytesIO(blob),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'}
    )

@app.get("/healthz")
async def healthz():
    """Health check endpoint"""
    try:
        await redis.ping()
        return {"status": "ok", "redis": "connected"}
    except Exception as e:
        return {"status": "degraded", "redis": "disconnected", "error": str(e)}

# -------- Job runner --------
async def run_job(job_id: str):
    """Background job processor"""
    if not await job_claim_lock(job_id):
        return
    
    try:
        await job_update(job_id, {"status": "running", "progress": "5", "updated_at": str(now_ms())})
        data = await job_read(job_id)
        category = data.get("category", "")
        include_amazon_check = data.get("include_amazon_check", "false") == "true"

        # Step 1: Discover leads
        leads = await discover_leads(category)
        await job_update(job_id, {"progress": "25", "updated_at": str(now_ms())})

        # Step 2: Deduplicate
        deduped = dedupe_by_root_domain(leads)
        await job_update(job_id, {"progress": "45", "updated_at": str(now_ms())})

        # Step 3: Refine Shopify detection
        await refine_shopify_detection(deduped)
        await job_update(job_id, {"progress": "70", "updated_at": str(now_ms())})

        # Step 4: Amazon presence check (if requested)
        if include_amazon_check:
            await enrich_amazon_presence(deduped)
        await job_update(job_id, {"progress": "85", "updated_at": str(now_ms())})

        # Step 5: Generate XLSX
        buffer = leads_to_xlsx_bytes(deduped[:TARGET_LEADS_MAX])
        await job_set_file(job_id, buffer)

        await job_update(job_id, {
            "status": "completed",
            "progress": "100",
            "filename": f"leads_{category.replace(' ', '_')}.xlsx",
            "updated_at": str(now_ms())
        })
    except Exception as e:
        await job_update(job_id, {
            "status": "failed",
            "message": str(e),
            "updated_at": str(now_ms())
        })
    finally:
        await job_release_lock(job_id)

# -------- Static files --------
public_dir = os.path.join(os.path.dirname(__file__), "public")

@app.get("/")
async def serve_index():
    """Serve the frontend"""
    return FileResponse(os.path.join(public_dir, "index.html"))

# Mount static assets at /static
app.mount("/static", StaticFiles(directory=public_dir, html=False), name="static")
