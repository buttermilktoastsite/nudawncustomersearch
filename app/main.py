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
REDIS_URL = os.getenv("REDIS_URL")
JOB_TTL_SECONDS = int(os.getenv("JOB_TTL_SECONDS", "3600"))

# Discovery tuning
SEARCH_CONCURRENCY = int(os.getenv("SEARCH_CONCURRENCY", "4"))
SEARCH_PAGES_PER_QUERY = int(os.getenv("SEARCH_PAGES_PER_QUERY", "3"))
TARGET_LEADS_MAX = int(os.getenv("TARGET_LEADS_MAX", "100"))
REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "15.0"))
USER_AGENT = os.getenv(
    "USER_AGENT",
    "Mozilla/5.0 (compatible; LeadFinderBot/1.3; +https://example.com/bot)"
)

# Search engine selection
SERPSTACK_KEY = os.getenv("SERPSTACK_KEY")
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
redis_kwargs = {
    "decode_responses": False,
    "socket_timeout": 15,
    "socket_connect_timeout": 15,
    "retry_on_timeout": True,
    "health_check_interval": 30,
}

redis = aioredis.StrictRedis.from_url(REDIS_URL, **redis_kwargs)

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

def get_parent_url(url: str) -> str:
    """Extract parent/root URL (scheme + netloc only)"""
    try:
        parsed = urlparse(url)
        scheme = parsed.scheme or "https"
        netloc = parsed.netloc or ""
        if netloc:
            return f"{scheme}://{netloc}"
        return url
    except:
        return url

def get_brand_from_url(url: str) -> str:
    """Extract brand/company name from URL"""
    try:
        hostname = get_hostname(url).replace("www.", "")
        parts = hostname.split(".")
        if len(parts) >= 2:
            brand = parts[-2]
            return brand.capitalize()
        return hostname
    except:
        return url

def extract_brand_from_title(title: str) -> str:
    """Extract brand name from page title"""
    if not title:
        return ""
    
    for sep in [" | ", " - ", " – ", " — ", ":"]:
        if sep in title:
            parts = title.split(sep)
            if len(parts) >= 2:
                brand = parts[-1].strip()
                for suffix in ["Shop", "Store", "Official Site", "Official Store", "Online Store"]:
                    if brand.endswith(suffix):
                        brand = brand[:-len(suffix)].strip()
                if brand and len(brand) > 2:
                    return brand
    
    words = title.split()
    if len(words) > 0:
        return " ".join(words[:min(3, len(words))])
    
    return ""

def is_marketplace_url(url: str) -> bool:
    """Filter out marketplace URLs"""
    host = get_hostname(url)
    if not host:
        return False
    bad_roots = ("amazon.", "walmart.", "target.", "etsy.", "ebay.")
    return any(host.startswith(b) or host.endswith(b[:-1]) for b in bad_roots)

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

async def fetch_brand_from_homepage(url: str) -> Optional[str]:
    """Try to extract brand name from homepage"""
    try:
        headers = {"User-Agent": USER_AGENT, "Accept-Language": "en-US,en;q=0.9"}
        async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT, headers=headers) as client:
            r = await client.get(url, follow_redirects=True)
            if r.status_code != 200:
                return None
            
            soup = BeautifulSoup(r.text, "html.parser")
            
            title_tag = soup.find("title")
            if title_tag:
                title = title_tag.get_text(strip=True)
                brand = extract_brand_from_title(title)
                if brand:
                    return brand
            
            og_site = soup.find("meta", property="og:site_name")
            if og_site and og_site.get("content"):
                return og_site["content"].strip()
            
            logo = soup.find("img", class_=lambda x: x and "logo" in x.lower())
            if logo and logo.get("alt"):
                return logo["alt"].strip()
            
            return None
    except:
        return None

async def precise_shopify_check(base_url: str) -> Optional[bool]:
    """Multi-method precise Shopify detection"""
    base = get_parent_url(base_url)
    if not base.startswith("http"):
        base = "https://" + base.lstrip("/")
    if not base.endswith("/"):
        base = base + "/"

    headers = {"User-Agent": USER_AGENT, "Accept-Language": "en-US,en;q=0.9"}
    
    try:
        async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT, headers=headers) as client:
            robots = await fetch_text(client, urljoin(base, "robots.txt"))
            if robots and ("shopify" in robots.lower()):
                return True

            home_html = await fetch_text(client, base)
            if home_html is not None and detect_shopify_from_html(home_html):
                return True

            for asset in ("assets/theme.js", "assets/theme.css"):
                txt = await fetch_text(client, urljoin(base, asset), method="HEAD")
                if txt is not None:
                    return True

            if home_html is not None:
                return False
            
            return None
    except:
        return None

# -------- Discovery via SerpStack or DuckDuckGo --------
async def search_serpstack(query: str, page: int) -> List[Tuple[str, str, str]]:
    """Search using SerpStack"""
    offset = (page - 1) * 10
    url = "http://api.serpstack.com/search"
    params = {
        "access_key": SERPSTACK_KEY,
        "query": query,
        "num": "10",
        "offset": str(offset),
        "gl": "us",
        "hl": "en",
    }
    headers = {"User-Agent": USER_AGENT}
    out: List[Tuple[str, str, str]] = []
    try:
        async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
            r = await client.get(url, params=params, headers=headers)
            if r.status_code != 200:
                print(f"[SerpStack] Error: status {r.status_code}")
                return out
            data = r.json()
            
            # Check for API errors
            if "error" in data:
                print(f"[SerpStack] API Error: {data['error']}")
                return out
            
            for item in (data.get("organic_results") or []):
                link = item.get("url") or ""
                title = (item.get("title") or "").strip()
                snippet = (item.get("snippet") or "").strip()
                if link:
                    out.append((link, title, snippet))
    except Exception as e:
        print(f"[SerpStack] Exception: {e}")
    return out

async def search_duckduckgo(query: str, page: int) -> List[Tuple[str, str, str]]:
    """Search using DuckDuckGo HTML with retry logic"""
    offset = (page - 1) * 30
    params = {"q": query, "kl": "us-en", "s": str(offset)}
    url = "https://duckduckgo.com/html"
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Referer": "https://duckduckgo.com/",
    }
    results: List[Tuple[str, str, str]] = []
    
    for attempt in range(3):
        try:
            async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT, follow_redirects=True) as client:
                r = await client.post(url, data=params, headers=headers)
                if r.status_code == 200:
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
                    if results:
                        return results
                if attempt < 2:
                    await asyncio.sleep(2 ** attempt)
        except Exception as e:
            print(f"DuckDuckGo search error (attempt {attempt + 1}): {e}")
            if attempt < 2:
                await asyncio.sleep(2 ** attempt)
    
    return results

def build_queries(category: str) -> List[str]:
    """Build search queries for lead discovery"""
    base = category.strip()
    return [
        f'"{base}" "buy" "online"',
        f'"{base}" "add to cart"',
        f'"{base}" "shop" -amazon -walmart -target -etsy -ebay',
        f'"{base}" "official site"',
        f'"{base}" "store" -amazon -walmart -target -etsy -ebay',
        f'"{base}" "products" -amazon -walmart -target -etsy -ebay',
    ]

async def discover_leads(category: str) -> List[Dict[str, str]]:
    """Discover leads via search engines"""
    queries = build_queries(category)
    leads: List[Dict[str, str]] = []
    seen_domains = set()
    sem = asyncio.Semaphore(SEARCH_CONCURRENCY)

    use_serpstack = (SEARCH_ENGINE == "serpstack") or (SEARCH_ENGINE == "auto" and SERPSTACK_KEY)
    print(f"[Discovery] Using SerpStack: {use_serpstack}, SERPSTACK_KEY present: {bool(SERPSTACK_KEY)}")
    
    async def do_search(query: str, page: int) -> List[Tuple[str, str, str]]:
        if use_serpstack:
            return await search_serpstack(query, page)
        else:
            return await search_duckduckgo(query, page)

    async def fetch_query(q: str):
        nonlocal leads, seen_domains
        for page in range(1, SEARCH_PAGES_PER_QUERY + 1):
            async with sem:
                try:
                    print(f"[Discovery] Query='{q}', page={page}")
                    results = await do_search(q, page)
                    print(f"[Discovery] Got {len(results)} raw results for query='{q}', page={page}")
                    for url, title, snippet in results:
                        if is_marketplace_url(url):
                            continue
                        
                        parent_url = get_parent_url(url)
                        domain = get_hostname(parent_url)
                        if not domain:
                            continue
                        if domain in seen_domains:
                            continue
                        seen_domains.add(domain)
                        
                        brand = extract_brand_from_title(title)
                        if not brand or len(brand) < 3:
                            brand = get_brand_from_url(parent_url)
                        
                        shopify_guess = "Yes" if looks_like_shopify_heuristic(url, f"{title} {snippet}") else "Unknown"
                        
                        leads.append({
                            "Brand Name": brand,
                            "URL": parent_url,
                            "Is this a Shopify site?": shopify_guess,
                        })
                        
                        if len(leads) >= TARGET_LEADS_MAX * 3:
                            # allow some extra before filtering
                            return
                except Exception as e:
                    print(f"[Discovery] Search error for query='{q}', page={page}: {e}")
            if len(leads) >= TARGET_LEADS_MAX * 3:
                return

    tasks = [fetch_query(q) for q in queries]
    await asyncio.gather(*tasks)

    return leads

def dedupe_by_root_domain(leads: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """Deduplicate leads by domain (already using parent URLs)"""
    seen = set()
    out: List[Dict[str, str]] = []
    for lead in leads:
        url = lead.get("URL") or ""
        domain = get_hostname(url)
        if not domain:
            continue
        if domain in seen:
            continue
        seen.add(domain)
        out.append(lead)
    return out

def filter_only_shopify(leads: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """Keep only leads that are confirmed Shopify sites."""
    return [l for l in leads if l.get("Is this a Shopify site?") == "Yes"]

def leads_to_xlsx_bytes(leads: List[Dict[str, str]]) -> bytes:
    """Convert leads to XLSX bytes"""
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Leads"
    headers = [
        "Brand Name",
        "URL",
        "Is this a Shopify site?",
    ]
    ws.append(headers)
    for row in leads:
        ws.append([
            row.get("Brand Name") or "",
            row.get("URL") or "",
            row.get("Is this a Shopify site?") or "",
        ])
    for cell in ws[1]:
        cell.font = openpyxl.styles.Font(bold=True)
    
    stream = io.BytesIO()
    wb.save(stream)
    return stream.getvalue()

async def refine_brand_names(leads: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """Refine brand names by fetching from homepage"""
    sem = asyncio.Semaphore(DETECT_CONCURRENCY)
    
    async def refine_one(lead):
        url = lead.get("URL") or ""
        current_brand = lead.get("Brand Name") or ""
        
        if current_brand and " " not in current_brand:
            async with sem:
                better_brand = await fetch_brand_from_homepage(url)
                if better_brand and len(better_brand) > 2:
                    lead["Brand Name"] = better_brand
    
    await asyncio.gather(*(refine_one(l) for l in leads), return_exceptions=True)
    return leads

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

# -------- Redis helpers --------
async def job_update(job_id: str, mapping: Dict[str, str]):
    bmap = {k.encode(): v.encode() for k, v in mapping.items()}
    await redis.hset(k_job(job_id), mapping=bmap)
    await redis.expire(k_job(job_id), JOB_TTL_SECONDS)

async def job_read(job_id: str) -> Dict[str, str]:
    data = await redis.hgetall(k_job(job_id))
    if not data:
        return {}
    return {k.decode(): v.decode() for k, v in data.items()}

async def job_set_file(job_id: str, blob: bytes):
    await redis.set(k_job_file(job_id), blob, ex=JOB_TTL_SECONDS)

async def job_get_file(job_id: str) -> Optional[bytes]:
    return await redis.get(k_job_file(job_id))

async def job_claim_lock(job_id: str, ttl: int = 240) -> bool:
    return await redis.set(k_job_lock(job_id), b"1", ex=ttl, nx=True) is True

async def job_release_lock(job_id: str):
    try:
        await redis.delete(k_job_lock(job_id))
    except:
        pass

# -------- API Routes --------
@app.post("/api/jobs")
@limiter.limit("20/minute")
async def create_job(request: Request):
    data = await request.json()
    category = (data.get("category") or "").strip()
    
    if not category:
        raise HTTPException(status_code=400, detail="Missing category")

    job_id = f"{int(time.time())}_{secrets.token_urlsafe(8)}"
    await job_update(job_id, {
        "status": "queued",
        "progress": "0",
        "created_at": str(now_ms()),
        "updated_at": str(now_ms()),
        "category": category,
    })
    
    asyncio.create_task(run_job(job_id))
    
    return {"job_id": job_id}

@app.get("/api/jobs/{job_id}")
async def get_job(job_id: str):
    meta = await job_read(job_id)
    if not meta:
        return {"status": "not_found"}
    
    allowed = {k: meta.get(k) for k in ["status", "message", "progress", "created_at", "updated_at"]}
    return allowed

@app.get("/api/jobs/{job_id}/result")
async def get_result(job_id: str):
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
        headers={"Content-Disposition": f'attachment; filename=\"{filename}\"'}
    )

@app.get("/healthz")
async def healthz():
    try:
        await redis.ping()
        return {"status": "ok", "redis": "connected"}
    except Exception as e:
        return {"status": "degraded", "redis": "disconnected", "error": str(e)}

# -------- Job runner --------
async def run_job(job_id: str):
    if not await job_claim_lock(job_id):
        return
    
    try:
        await job_update(job_id, {"status": "running", "progress": "5", "updated_at": str(now_ms())})
        data = await job_read(job_id)
        category = data.get("category", "")

        # Step 1: Discover leads
        print(f"[Job {job_id}] Starting discovery for category: {category}")
        leads = await discover_leads(category)
        print(f"[Job {job_id}] Found {len(leads)} initial leads")
        await job_update(job_id, {"progress": "25", "updated_at": str(now_ms())})

        if len(leads) == 0:
            await job_update(job_id, {
                "status": "completed",
                "progress": "100",
                "message": "No leads found. Try adjusting the category or using SERPSTACK_KEY.",
                "filename": f"leads_{category.replace(' ', '_')}.xlsx",
                "updated_at": str(now_ms())
            })
            buffer = leads_to_xlsx_bytes([])
            await job_set_file(job_id, buffer)
            return

        # Step 2: Deduplicate
        deduped = dedupe_by_root_domain(leads)
        print(f"[Job {job_id}] After deduplication: {len(deduped)} leads")
        await job_update(job_id, {"progress": "40", "updated_at": str(now_ms())})

        # Step 3: Refine brand names
        await refine_brand_names(deduped)
        await job_update(job_id, {"progress": "55", "updated_at": str(now_ms())})

        # Step 4: Refine Shopify detection
        await refine_shopify_detection(deduped)
        await job_update(job_id, {"progress": "75", "updated_at": str(now_ms())})

        # Step 5: Filter only confirmed Shopify and cap to 100
        shopify_only = filter_only_shopify(deduped)
        print(f"[Job {job_id}] After Shopify filter: {len(shopify_only)} leads")

        if not shopify_only:
            await job_update(job_id, {
                "status": "completed",
                "progress": "100",
                "message": "No confirmed Shopify stores found for this category.",
                "filename": f"leads_{category.replace(' ', '_')}.xlsx",
                "updated_at": str(now_ms())
            })
            buffer = leads_to_xlsx_bytes([])
            await job_set_file(job_id, buffer)
            return

        MAX_RESULTS = 100
        final_leads = shopify_only[:MAX_RESULTS]
        buffer = leads_to_xlsx_bytes(final_leads)
        await job_set_file(job_id, buffer)
        print(f"[Job {job_id}] Generated XLSX with {len(final_leads)} Shopify leads")

        await job_update(job_id, {
            "status": "completed",
            "progress": "100",
            "filename": f"leads_{category.replace(' ', '_')}.xlsx",
            "updated_at": str(now_ms())
        })
    except Exception as e:
        print(f"[Job {job_id}] Error: {str(e)}")
        import traceback
        traceback.print_exc()
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
    return FileResponse(os.path.join(public_dir, "index.html"))

app.mount("/static", StaticFiles(directory=public_dir, html=False), name="static")
