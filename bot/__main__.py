import asyncio
import aiohttp
from bs4 import BeautifulSoup
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError
import logging
import os
import time
from dotenv import load_dotenv
import sys

# --- Logging Setup ---
log_formatter = logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s")
log_handler_file = logging.FileHandler("capitol_trades_bot.log", encoding='utf-8')
log_handler_file.setFormatter(log_formatter)
log_handler_stream = logging.StreamHandler(sys.stdout)

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)
log.addHandler(log_handler_file)
log.addHandler(log_handler_stream)

# --- Configuration ---
BASE_URL = "https://www.capitoltrades.com"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}
SEMAPHORE_LIMIT = 10
MAX_PAGES_TO_SCAN = 3
POLLING_INTERVAL_SECONDS = 3600
DB_CONNECTION_TIMEOUT_MS = 5000
REQUEST_TIMEOUT_SECONDS = 30

# --- Environment ---
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    log.critical("CRITICAL: MONGO_URI not set.")
    sys.exit(1)

# --- MongoDB Setup ---
try:
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=DB_CONNECTION_TIMEOUT_MS)
    client.admin.command('ismaster')
    db = client["capitol_trades"]
    politicians_collection = db["politicians"]
    recent_trades_collection = db["recent_trades"]
    politicians_collection.create_index("profile_url", unique=True)
    log.info("Connected to MongoDB.")
except Exception as e:
    log.critical(f"CRITICAL: MongoDB setup failed: {e}")
    sys.exit(1)

# --- Logic ---

def parse_trades(html):
    if not html: return []
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", class_="table--politician-trades") or \
            soup.find("table", class_="w-full caption-bottom text-size-3 text-txt")
    if not table or not table.find("tbody"): return []
    
    trades = []
    for row in (table.find("tbody").find_all("tr", class_="border-b") or table.find("tbody").find_all("tr")):
        cols = row.find_all("td")
        if len(cols) < 6: continue
        link = row.find("a", href=lambda h: h and ('/trades/' in h or '/disclosures/' in h))
        issuer_tag = cols[0].find('a', class_='issuer-ticker')
        trades.append({
            "issuer": issuer_tag.get_text(strip=True) if issuer_tag else cols[0].get_text(strip=True),
            "published": cols[1].get_text(strip=True),
            "traded": cols[2].get_text(strip=True),
            "type": cols[4].get_text(strip=True),
            "size": cols[5].get_text(strip=True),
            "trade_url": link["href"] if link else None
        })
    return trades

async def fetch_url(session, url, semaphore=None):
    async with (semaphore or asyncio.Lock()):
        try:
            async with session.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT_SECONDS) as resp:
                if resp.status == 200: return await resp.text()
        except Exception as e:
            log.debug(f"Fetch failed for {url}: {e}")
    return None

async def process_page(session, page_num, semaphore):
    listing_url = f"{BASE_URL}/politicians?page={page_num}&pageSize=96"
    html = await fetch_url(session, listing_url)
    if not html: return 0, 0, 0

    soup = BeautifulSoup(html, "html.parser")
    cards = soup.select("a.politician-index-card__link, a.index-card-link")
    if not cards: return 0, 0, 0

    politicians = []
    for card in cards:
        article = card.find("article")
        if not article or not card.get("href"): continue
        
        t_tag = article.select_one("div.politician-index-card__metric--trades .value, div.cell--count-trades .q-value")
        l_tag = article.select_one("div.politician-index-card__metric--last-traded time, div.cell--last-traded .time")
        
        politicians.append({
            "name": (article.select_one("h3, h2") or type('T',(),{'text':'Unknown'})).get_text(strip=True),
            "profile_url": card.get("href"),
            "trades_count_reported": int(t_tag.text.strip().replace(",", "")) if t_tag and t_tag.text.strip().replace(",", "").isdigit() else 0,
            "last_traded_reported": l_tag.get('datetime') or l_tag.get_text(strip=True) if l_tag else None,
            "party": (article.select_one("span.party, .politician-index-card__party") or type('T',(),{'text':''})).text.strip(),
            "state": (article.select_one("span.us-state-full, .politician-index-card__state") or type('T',(),{'text':''})).text.strip(),
            "image_url": BASE_URL + article.select_one("img")["src"] if article.select_one("img") and article.select_one("img").get("src", "").startswith("/") else None
        })

    # Optimization: Only fetch profiles if count or date changed
    urls = [p["profile_url"] for p in politicians]
    existing_map = {p["profile_url"]: p for p in politicians_collection.find({"profile_url": {"$in": urls}})}

    fetch_tasks, to_process = [], []
    for p in politicians:
        ext = existing_map.get(p["profile_url"])
        if not ext or ext.get("trades_count_reported") != p["trades_count_reported"] or ext.get("last_traded_reported") != p["last_traded_reported"]:
            fetch_tasks.append(fetch_url(session, BASE_URL + p["profile_url"], semaphore))
            to_process.append(p)

    if not fetch_tasks: return 0, 0, 0

    profiles_html = await asyncio.gather(*fetch_tasks)
    bulk_ops, recent_trades = [], []
    counts = {"new": 0, "trades": 0, "meta": 0}

    for p, html in zip(to_process, profiles_html):
        if not html: continue
        trades = parse_trades(html)
        ext = existing_map.get(p["profile_url"])
        
        new_trades = []
        if not ext:
            new_trades = trades
            counts["new"] += 1
            log.info(f"[NEW] Politician discovered: {p['name']}")
        else:
            ext_urls = {t.get("trade_url") for t in ext.get("trades_data", []) if t.get("trade_url")}
            new_trades = [t for t in trades if t.get("trade_url") not in ext_urls]
            
            # Check for actual meta changes
            meta_changed = any(p[k] != ext.get(k) for k in ["party", "state", "image_url", "trades_count_reported", "last_traded_reported"])
            if new_trades:
                counts["trades"] += 1
                log.info(f"[UPDATE] {len(new_trades)} new trade(s) for {p['name']}")
            elif meta_changed:
                counts["meta"] += 1
                log.info(f"[META] Metadata updated for {p['name']}")

        # Only update if there's a reason to write
        if not ext or new_trades or meta_changed:
            update = {"$set": p}
            update["$set"]["last_checked_timestamp"] = time.time()
            if new_trades:
                update["$push"] = {"trades_data": {"$each": list(reversed(new_trades)), "$position": 0}}
                for nt in new_trades:
                    recent_trades.append({"politician": {k: p[k] for k in ["name", "state", "profile_url", "image_url"]}, "trade": nt})
            bulk_ops.append(UpdateOne({"profile_url": p["profile_url"]}, update, upsert=True))

    if bulk_ops:
        try: politicians_collection.bulk_write(bulk_ops, ordered=False)
        except BulkWriteError as bwe: log.error(f"Bulk write error: {bwe.details}")

    if recent_trades:
        recent_trades.reverse()
        recent_trades_collection.update_one(
            {"_id": "recent_trades_stack_v1"},
            {"$push": {"trades": {"$each": recent_trades, "$position": 0, "$slice": 100}}, "$set": {"last_updated": time.time()}},
            upsert=True
        )

    return counts["new"], counts["trades"], counts["meta"]

async def run_cycle():
    start = time.time()
    sem = asyncio.Semaphore(SEMAPHORE_LIMIT)
    async with aiohttp.ClientSession() as session:
        tasks = [process_page(session, i, sem) for i in range(1, MAX_PAGES_TO_SCAN + 1)]
        results = await asyncio.gather(*tasks)
    
    totals = [sum(x) for x in zip(*results)] if results else [0, 0, 0]
    if any(totals):
        log.info(f"Cycle Results: {totals[0]} New, {totals[1]} Trade Updates, {totals[2]} Meta Updates (Duration: {time.time()-start:.2f}s)")
    return totals

async def main():
    log.info("Capitol Trades Bot Active.")
    while True:
        try: await run_cycle()
        except Exception as e: log.error(f"Cycle failure: {e}")
        await asyncio.sleep(POLLING_INTERVAL_SECONDS)

if __name__ == "__main__":
    try: asyncio.run(main())
    except KeyboardInterrupt: pass
    finally: client.close()
