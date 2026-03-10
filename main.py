"""
NEM Dashboard - FastAPI backend
Serves scraped NEMWeb data and the mobile-friendly dashboard frontend.
"""

import asyncio
import logging
import traceback
from datetime import datetime, timezone
from pathlib import Path
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from scraper import scrape_all

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

cache = {"data": None, "last_updated": None, "error": None}
REFRESH_INTERVAL = 300


async def refresh_data():
    while True:
        try:
            logger.info("Refreshing NEMWeb data...")
            data = await asyncio.get_event_loop().run_in_executor(None, scrape_all)
            cache["data"] = data
            cache["last_updated"] = datetime.now(timezone.utc).isoformat()
            cache["error"] = None
            logger.info("Data refresh complete.")
        except Exception as e:
            logger.error(f"Error refreshing data: {e}")
            cache["error"] = str(e)
        await asyncio.sleep(REFRESH_INTERVAL)


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        logger.info("Initial NEMWeb data fetch...")
        data = await asyncio.get_event_loop().run_in_executor(None, scrape_all)
        cache["data"] = data
        cache["last_updated"] = datetime.now(timezone.utc).isoformat()
    except Exception as e:
        logger.error(f"Initial fetch failed: {e}")
        cache["error"] = str(e)
    task = asyncio.create_task(refresh_data())
    yield
    task.cancel()


app = FastAPI(title="NEM Dashboard", lifespan=lifespan)

static_dir = Path(__file__).parent / "static"
static_dir.mkdir(exist_ok=True)
app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")


@app.get("/api/data")
async def get_data():
    if cache["data"] is None:
        return JSONResponse(
            content={"error": cache.get("error", "Data not yet available"), "loading": True},
            status_code=503 if cache["error"] else 202,
        )
    return JSONResponse(content={
        **cache["data"],
        "last_updated": cache["last_updated"],
        "cache_error": cache.get("error"),
    })


@app.get("/api/health")
async def health():
    return {"status": "ok", "last_updated": cache["last_updated"], "has_data": cache["data"] is not None}


@app.get("/api/debug")
async def debug():
    """Inspect raw NEMWeb file structure to diagnose missing data."""
    from scraper import get_all_file_urls, fetch_zip_csv, TRADING_PRICE_URL, PREDISPATCH_URL
    from zoneinfo import ZoneInfo

    result = {}
    aest = ZoneInfo("Australia/Sydney")
    today_compact = datetime.now(aest).strftime("%Y%m%d")
    result["today_aest"] = datetime.now(aest).isoformat()
    result["today_compact"] = today_compact

    def peek_zip(url):
        rows = fetch_zip_csv(url)
        tables = {}
        current_table = None
        headers = None
        for row in rows:
            vals = list(row.values())
            if not vals:
                continue
            ind = str(vals[0]).strip().upper()
            if ind == "I" and len(vals) > 1:
                current_table = str(vals[1]).strip().upper()
                headers = [str(v).strip().upper() for v in vals]
                tables.setdefault(current_table, {"headers": headers, "sample": None})
            elif ind == "D" and headers and current_table:
                if tables[current_table]["sample"] is None:
                    tables[current_table]["sample"] = dict(zip(headers, [str(v) for v in vals]))
        return tables

    try:
        all_trading = get_all_file_urls(TRADING_PRICE_URL, "PUBLIC_TRADINGIS")
        today_trading = [u for u in all_trading if today_compact in u]
        result["trading_total_files"] = len(all_trading)
        result["trading_today_count"] = len(today_trading)
        result["trading_today_urls"] = today_trading[-3:]
        if today_trading:
            result["trading_structure"] = peek_zip(today_trading[0])
        elif all_trading:
            result["trading_latest_url"] = all_trading[-1]
            result["trading_structure"] = peek_zip(all_trading[-1])
    except Exception:
        result["trading_error"] = traceback.format_exc()

    try:
        all_pd = get_all_file_urls(PREDISPATCH_URL, "PUBLIC_PREDISPATCHIS")
        result["predispatch_total_files"] = len(all_pd)
        result["predispatch_latest_url"] = all_pd[-1] if all_pd else None
        if all_pd:
            result["predispatch_structure"] = peek_zip(all_pd[-1])
    except Exception:
        result["predispatch_error"] = traceback.format_exc()

    if cache["data"]:
        d = cache["data"]
        result["cache_summary"] = {
            "historical_regions": {r: len(v) for r, v in d.get("historical_prices", {}).items()},
            "predispatch_regions": {r: len(v) for r, v in d.get("predispatch_prices", {}).items()},
            "prices": d.get("prices", {}),
        }

    return JSONResponse(content=result)


@app.get("/", response_class=HTMLResponse)
async def dashboard():
    html_path = Path(__file__).parent / "static" / "index.html"
    if html_path.exists():
        return HTMLResponse(content=html_path.read_text())
    return HTMLResponse(content="<h1>Dashboard loading...</h1>")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
