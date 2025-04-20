import asyncio
import aiohttp
from bs4 import BeautifulSoup
from pymongo import MongoClient
import requests
import logging
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv

# --- Logging ---
logging.basicConfig(
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    level=logging.INFO
)
log = logging.getLogger(__name__)

# --- Config ---
base_url = "https://www.capitoltrades.com"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}
SEMAPHORE_LIMIT = 10
MAX_PAGES = 3

# --- Load Env ---
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")

# --- MongoDB ---
client = MongoClient(MONGO_URI)
db = client["capitol_trades"]
collection = db["politicians"]
recent_trades_collection = db["recent_trades"]

# --- Helpers ---

def parse_trades(html):
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", class_="w-full caption-bottom text-size-3 text-txt")
    trades = []
    if table:
        for row in table.find_all("tr", class_="border-b"):
            cols = row.find_all("td")
            if len(cols) >= 6:
                trades.append({
                    "issuer": cols[0].get_text(strip=True),
                    "published": cols[1].get_text(strip=True),
                    "traded": cols[2].get_text(strip=True),
                    "filed_after": cols[3].get_text(strip=True),
                    "type": cols[4].get_text(strip=True),
                    "size": cols[5].get_text(strip=True),
                })
    return trades

async def fetch_profile_limited(session, profile_url, semaphore):
    async with semaphore:
        async with session.get(base_url + profile_url, headers=headers) as response:
            return await response.text()

async def fetch_all_profiles(profile_urls):
    semaphore = asyncio.Semaphore(SEMAPHORE_LIMIT)
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_profile_limited(session, url, semaphore) for url in profile_urls]
        return await asyncio.gather(*tasks)

def process_page(page_number):
    created = 0
    updated = 0

    listing_url = f"{base_url}/politicians?page={page_number}&pageSize=96"
    resp = requests.get(listing_url, headers=headers)
    soup = BeautifulSoup(resp.text, "html.parser")
    cards = soup.select("a.index-card-link")

    if not cards:
        log.warning(f"Page {page_number} has no politician cards.")
        return 0, 0

    profile_urls = []
    base_data = {}

    for i, card in enumerate(cards, start=1):
        article = card.find("article", class_="politician-index-card")
        if not article:
            continue

        name = article.select_one("div.cell--name h2")
        party = article.select_one("span.party")
        state = article.select_one("span.us-state-full")
        trades = article.select_one("div.cell--count-trades .q-value")
        issuers = article.select_one("div.cell--count-issuers .q-value")
        volume = article.select_one("div.cell--volume .q-value")
        last_traded = article.select_one("div.cell--last-traded .time")
        image = article.select_one("div.cell--avatar img")
        profile_url = card["href"]

        entry = {
            "index": i,
            "name": name.text.strip() if name else None,
            "party": party.text.strip() if party else None,
            "state": state.text.strip() if state else None,
            "trades": int(trades.text.strip().replace(",", "")) if trades else None,
            "issuers": int(issuers.text.strip().replace(",", "")) if issuers else None,
            "volume": volume.text.strip() if volume else None,
            "last_traded": last_traded.text.strip() if last_traded else None,
            "image_url": base_url + image["src"] if image and "src" in image.attrs else None,
            "profile_url": profile_url
        }

        existing = collection.find_one({"profile_url": profile_url})
        if existing and entry["last_traded"] == existing.get("last_traded"):
            continue

        profile_urls.append(profile_url)
        base_data[profile_url] = entry

    if not profile_urls:
        log.info(f"No updated profiles on page {page_number}.")
        return 0, 0

    html_pages = asyncio.run(fetch_all_profiles(profile_urls))

    for profile_url, html in zip(profile_urls, html_pages):
        trades = parse_trades(html)
        if not trades:
            continue

        entry = base_data[profile_url]
        entry["trades_data"] = trades

        existing = collection.find_one({"profile_url": profile_url})

        # NEW POLITICIAN
        if not existing:
            collection.insert_one(entry)
            created += 1
            log.info(f"[NEW] {entry['name']} ({entry['state']}) — {len(trades)} trades added.")

            for trade in reversed(trades):
                recent_trades_collection.update_one(
                    {"_id": "stack"},
                    {
                        "$push": {
                            "trades": {
                                "$each": [{
                                    "politician": {
                                        "name": entry["name"],
                                        "state": entry["state"],
                                        "image_url": entry["image_url"],
                                        "profile_url": entry["profile_url"]
                                    },
                                    "trade": trade
                                }],
                                "$position": 0,
                                "$slice": 50
                            }
                        }
                    },
                    upsert=True
                )
            continue

        # EXISTING
        existing_trades = existing.get("trades_data", [])
        existing_set = set(map(lambda t: str(t), existing_trades))

        new_unique = []
        for trade in trades:
            if str(trade) in existing_set:
                break
            new_unique.append(trade)

        if new_unique:
            collection.update_one(
                {"profile_url": profile_url},
                {
                    "$push": {
                        "trades_data": {
                            "$each": list(reversed(new_unique)),
                            "$position": 0
                        }
                    }
                }
            )
            updated += 1
            log.info(f"[UPDATE] {entry['name']} — {len(new_unique)} new trades.")

            for trade in new_unique:
                recent_trades_collection.update_one(
                    {"_id": "stack"},
                    {
                        "$push": {
                            "trades": {
                                "$each": [{
                                    "politician": {
                                        "name": entry["name"],
                                        "state": entry["state"],
                                        "image_url": entry["image_url"],
                                        "profile_url": entry["profile_url"]
                                    },
                                    "trade": trade
                                }],
                                "$position": 0,
                                "$slice": 50
                            }
                        }
                    },
                    upsert=True
                )

        # Check for field updates
        updates = {}
        for field in ["name", "party", "state", "trades", "issuers", "volume", "last_traded", "image_url"]:
            if entry[field] != existing.get(field):
                updates[field] = entry[field]

        if updates:
            collection.update_one({"profile_url": profile_url}, {"$set": updates})
            log.debug(f"[META UPDATE] {entry['name']} — fields: {list(updates.keys())}")

    return created, updated

def main():
    log.info("Bot started.")
    with ThreadPoolExecutor() as executor:
        results = list(executor.map(process_page, range(1, MAX_PAGES + 1)))

    total_created = sum(r[0] for r in results)
    total_updated = sum(r[1] for r in results)
    log.info(f"Created: {total_created} | Updated: {total_updated}")

if __name__ == "__main__":
    main()
