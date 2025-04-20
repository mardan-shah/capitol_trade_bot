import asyncio
import aiohttp
from bs4 import BeautifulSoup
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError
import requests
import logging
import os
import time
from dotenv import load_dotenv
import sys # Import sys for checking Python version potentially

# --- Logging Setup ---
# Define log format with aligned levels
log_formatter = logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s")
log_handler_file = logging.FileHandler("capitol_trades_bot.log", encoding='utf-8')
log_handler_file.setFormatter(log_formatter)
log_handler_stream = logging.StreamHandler(sys.stdout) # Use stdout for console
log_handler_stream.setFormatter(log_formatter)

log = logging.getLogger(__name__)
log.setLevel(logging.INFO) # Set base level for the logger
log.addHandler(log_handler_file)
log.addHandler(log_handler_stream)

# --- Configuration ---
BASE_URL = "https://www.capitoltrades.com"
HEADERS = {
    # Using a common browser User-Agent
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36"
}
SEMAPHORE_LIMIT = 10 # Max concurrent profile page fetches
MAX_PAGES_TO_SCAN = 3 # Max politician *listing* pages to check per cycle
POLLING_INTERVAL_SECONDS = 3600  # 10 minutes between scan cycles
DB_CONNECTION_TIMEOUT_MS = 5000 # Timeout for MongoDB connection attempt
REQUEST_TIMEOUT_SECONDS = 30 # Timeout for HTTP requests

# --- Environment Variables Loading ---
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")

if not MONGO_URI:
    log.critical("CRITICAL: MONGO_URI environment variable not set. Please create a .env file or set the environment variable.")
    sys.exit(1) # Exit if DB connection string is missing


# --- MongoDB Connection and Setup ---
client = None # Initialize client to None
try:
    log.info(f"Attempting to connect to MongoDB (Timeout: {DB_CONNECTION_TIMEOUT_MS}ms)...")
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=DB_CONNECTION_TIMEOUT_MS)
    # The ismaster command is cheap and does not require auth. Validates connection.
    client.admin.command('ismaster')
    db = client["capitol_trades"] # Use your desired database name

    # Define collections
    politicians_collection = db["politicians"]
    recent_trades_collection = db["recent_trades"] # For the capped recent trades list

    # Create indexes for performance (if they don't exist)
    # Unique index on profile_url is crucial for finding/updating politicians
    politicians_collection.create_index("profile_url", unique=True, name="profile_url_unique_idx")
    politicians_collection.create_index("last_checked", name="last_checked_idx") # Index for potential future queries
    # Optional: Indexing trade URLs within the array might be slow if arrays get huge. Consider if needed.
    # politicians_collection.create_index("trades_data.trade_url", name="trade_url_idx")
    log.info("Successfully connected to MongoDB and collections/indexes are ready.")

except Exception as e:
    log.critical(f"CRITICAL: Failed to connect to MongoDB or setup collections/indexes: {e}", exc_info=True)
    if client:
        client.close()
    sys.exit(1) # Exit if DB connection fails


# --- Helper Functions ---

def parse_trades(html_content):
    """
    Parses trades from the politician's profile page HTML.

    Args:
        html_content (str): The HTML content of the profile page.

    Returns:
        list: A list of trade dictionaries, ordered as they appear on the page
              (usually most recent first). Returns empty list if no table/trades found.
    """
    if not html_content:
        log.warning("Received empty HTML content in parse_trades.")
        return []

    soup = BeautifulSoup(html_content, "html.parser")
    # Try a more specific selector first, then fall back
    table = soup.find("table", class_="table--politician-trades")
    if not table:
         table = soup.find("table", class_="w-full caption-bottom text-size-3 text-txt") # Original fallback

    trades_list = []
    if table:
        tbody = table.find("tbody")
        if not tbody:
            log.warning("Trade table found but missing 'tbody' element.")
            return trades_list # Return empty list if table structure is unexpected

        rows = tbody.find_all("tr", class_="border-b") # Original row selector
        if not rows:
             rows = tbody.find_all("tr") # More generic fallback if class changes
             log.debug(f"Found {len(rows)} trade rows using generic 'tr' selector.")

        for row_index, row in enumerate(rows):
            cols = row.find_all("td")
            # Find the trade link specifically within the row
            link_tag = row.find("a", href=lambda href: href and ('/trades/' in href or '/disclosures/' in href)) # Look for trade or disclosure links
            trade_url_relative = link_tag["href"] if link_tag else None

            if len(cols) >= 6: # Expecting at least 6 columns for trade data
                try:
                    # Use .get_text() with strip=True for cleaner text extraction
                    # Try specific selectors within columns if structure is known
                    issuer_tag = cols[0].find('a', class_='issuer-ticker')
                    issuer_text = issuer_tag.get_text(strip=True) if issuer_tag else cols[0].get_text(strip=True)

                    trade_data = {
                        "issuer": issuer_text,
                        "published": cols[1].get_text(strip=True), # Publication date of the disclosure
                        "traded": cols[2].get_text(strip=True),    # Actual trade date
                        "filed_after": cols[3].get_text(strip=True), # Delay in filing
                        "type": cols[4].get_text(strip=True),      # e.g., 'purchase', 'sale_full', 'sale_partial'
                        "size": cols[5].get_text(strip=True),      # Estimated size range
                        "trade_url": trade_url_relative # Store the relative URL found
                    }
                    trades_list.append(trade_data)
                except IndexError:
                     log.warning(f"IndexError parsing columns in row {row_index+1}. Row content: {row.get_text('|', strip=True)}")
                except Exception as e:
                     log.error(f"Unexpected error parsing row {row_index+1}: {e}. Row content: {row.get_text('|', strip=True)}", exc_info=False) # Avoid full trace for common parse errors
            else:
                # Log if a row doesn't have enough columns, might indicate layout change or footer row
                log.warning(f"Skipping row {row_index+1} due to insufficient columns ({len(cols)}). Content: {row.get_text('|', strip=True)}")

    # Check for explicit "no trades" message outside the table as well
    elif soup.find(lambda tag: tag.name == "div" and "no trades found" in tag.get_text(strip=True).lower()):
        log.debug("Explicit 'No trades found' message detected on profile page.")
    else:
        # This might happen if the page layout changes significantly
        log.warning("Could not find the trades table or 'No trades found' message on profile page.")
        # Consider saving HTML for debugging:
        # try:
        #     with open("debug_no_table_found.html", "w", encoding="utf-8") as f:
        #         f.write(html_content)
        # except Exception as write_err:
        #     log.error(f"Failed to write debug HTML file: {write_err}")

    log.debug(f"Parsed {len(trades_list)} trades from HTML.")
    return trades_list


async def fetch_profile_limited(session, profile_url_relative, semaphore):
    """Fetches a single politician profile page respecting the semaphore."""
    full_url = BASE_URL + profile_url_relative
    async with semaphore: # Acquire semaphore before making the request
        log.debug(f"Acquired semaphore, fetching: {full_url}")
        try:
            async with session.get(full_url, headers=HEADERS, timeout=REQUEST_TIMEOUT_SECONDS) as response:
                response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
                html_text = await response.text()
                log.debug(f"Successfully fetched {full_url} (Status: {response.status})")
                return html_text
        except asyncio.TimeoutError:
            log.error(f"Timeout error fetching {full_url} after {REQUEST_TIMEOUT_SECONDS} seconds.")
            return None
        except aiohttp.ClientResponseError as http_err:
            log.error(f"HTTP Error {http_err.status} fetching {full_url}: {http_err.message}")
            return None
        except aiohttp.ClientError as e: # Catch other potential client errors (e.g., connection issues)
            log.error(f"HTTP Client error fetching {full_url}: {e}")
            return None
        except Exception as e:
            # Catch any other unexpected errors during fetch/read
            log.error(f"Unexpected error fetching {full_url}: {e}", exc_info=True)
            return None
        finally:
             log.debug(f"Released semaphore for: {full_url}")


async def fetch_all_profiles(profile_urls_relative):
    """Fetches multiple profile pages concurrently using aiohttp and a semaphore."""
    if not profile_urls_relative:
        log.info("fetch_all_profiles received an empty list of URLs.")
        return {} # Return empty dict if no URLs

    semaphore = asyncio.Semaphore(SEMAPHORE_LIMIT)
    # Consider using TCPConnector with limits if needed, but semaphore often sufficient
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_profile_limited(session, url, semaphore) for url in profile_urls_relative]
        # Gather results, exceptions are propagated by default
        results_html = await asyncio.gather(*tasks)

        # Create a dictionary mapping the original relative URL to the fetched HTML (or None if failed)
        # Important: Assumes results are returned in the same order as tasks were created
        profile_html_map = {url: html for url, html in zip(profile_urls_relative, results_html)}
        successful_fetches = sum(1 for html in results_html if html is not None)
        failed_fetches = len(profile_urls_relative) - successful_fetches
        log.info(f"Profile fetching complete. Success: {successful_fetches}, Failed: {failed_fetches}.")
        return profile_html_map


def process_page(page_number):
    """
    Processes a single page of the politician listing.
    Fetches politician details, checks for new trades based on the latest trade URL,
    and prepares bulk database operations.

    Args:
        page_number (int): The listing page number to process.

    Returns:
        tuple: (politicians_created, politicians_updated_trades, politicians_updated_meta) counts for this page.
    """
    politicians_created = 0
    politicians_updated_trades = 0 # Count of politicians with new trades added
    politicians_updated_meta = 0   # Count of politicians with only metadata updated

    listing_url = f"{BASE_URL}/politicians?page={page_number}&pageSize=96" # Assuming 96 per page
    log.info(f"Processing politician listing page: {page_number}")

    # --- Fetch Listing Page ---
    try:
        resp = requests.get(listing_url, headers=HEADERS, timeout=REQUEST_TIMEOUT_SECONDS)
        resp.raise_for_status() # Check for HTTP errors
        log.debug(f"Successfully fetched listing page {page_number}")
    except requests.exceptions.Timeout:
        log.error(f"Timeout fetching listing page {page_number}.")
        return 0, 0, 0 # Return zero counts if page fetch fails
    except requests.exceptions.RequestException as e:
        log.error(f"Error fetching listing page {page_number}: {e}")
        return 0, 0, 0

    # --- Parse Listing Page ---
    soup = BeautifulSoup(resp.text, "html.parser")
    # Use robust selectors, allowing for variations (e.g., different link classes)
    cards = soup.select("a.politician-index-card__link, a.index-card-link")

    if not cards:
        # Check if it's just an empty page (beyond the last page) or a real issue
        if "no politicians found" in resp.text.lower():
             log.info(f"No politician cards found on listing page {page_number} (likely end of results).")
        else:
             log.warning(f"No politician cards found using selectors on listing page {page_number}. Possible layout change.")
        return 0, 0, 0

    profiles_to_fetch = []
    base_data_map = {} # Maps profile_url -> base data dict from listing

    # --- Stage 1: Extract Base Data and Identify Profiles to Fetch ---
    log.info(f"Parsing {len(cards)} politician cards from listing page {page_number}...")
    for i, card in enumerate(cards, start=1):
        article = card.find("article") # Find the article tag within the link card
        if not article:
            log.warning(f"Card {i} on page {page_number} is missing the <article> tag. Skipping.")
            continue

        # Extract data using specific selectors with fallbacks
        name_tag = article.select_one("h3.politician-index-card__name, div.cell--name h2")
        party_tag = article.select_one("div.politician-index-card__party > span, span.party")
        state_tag = article.select_one("div.politician-index-card__state > span, span.us-state-full")
        trades_tag = article.select_one("div.politician-index-card__metric--trades .value, div.cell--count-trades .q-value")
        issuers_tag = article.select_one("div.politician-index-card__metric--issuers .value, div.cell--count-issuers .q-value")
        volume_tag = article.select_one("div.politician-index-card__metric--volume .value, div.cell--volume .q-value")
        last_traded_tag = article.select_one("div.politician-index-card__metric--last-traded time, div.cell--last-traded .time")
        image_tag = article.select_one("img.politician-index-card__avatar, div.cell--avatar img")
        profile_url_relative = card.get("href")

        # Validate extracted profile URL
        if not profile_url_relative or not profile_url_relative.startswith('/politicians/'):
            log.warning(f"Card {i} on page {page_number} has invalid or missing profile URL: '{profile_url_relative}'. Skipping.")
            continue

        # Clean extracted data, handle potential None values gracefully
        name = name_tag.text.strip() if name_tag else "Unknown Name"
        party = party_tag.text.strip() if party_tag else None
        state = state_tag.text.strip() if state_tag else None

        # Safely convert counts to integers
        trades_count = None
        if trades_tag:
            try: trades_count = int(trades_tag.text.strip().replace(",", ""))
            except ValueError: log.warning(f"Could not parse trade count for {name}: '{trades_tag.text}'. Setting to None.")
        issuers_count = None
        if issuers_tag:
            try: issuers_count = int(issuers_tag.text.strip().replace(",", ""))
            except ValueError: log.warning(f"Could not parse issuer count for {name}: '{issuers_tag.text}'. Setting to None.")

        volume = volume_tag.text.strip() if volume_tag else None
        # Prefer datetime attribute if available, otherwise use text
        last_traded = last_traded_tag.get('datetime', None) or (last_traded_tag.text.strip() if last_traded_tag else None)
        # Construct absolute image URL if source exists
        image_url = (BASE_URL + image_tag["src"]) if image_tag and image_tag.get("src") and image_tag["src"].startswith('/') else (image_tag.get("src") if image_tag else None)


        if name == "Unknown Name":
            log.warning(f"Skipping card {i} on page {page_number} due to missing name (URL: {profile_url_relative}).")
            continue

        # Store base data extracted from the listing page
        base_data = {
            "name": name,
            "party": party,
            "state": state,
            "trades_count_reported": trades_count, # Reported on listing page
            "issuers_count_reported": issuers_count, # Reported on listing page
            "volume_reported": volume,             # Reported on listing page
            "last_traded_reported": last_traded,     # Reported on listing page
            "image_url": image_url,
            "profile_url": profile_url_relative    # Store the relative URL as key
        }

        profiles_to_fetch.append(profile_url_relative)
        base_data_map[profile_url_relative] = base_data
        log.debug(f"Identified politician: {name} ({profile_url_relative}). Queued for profile fetch.")

    # --- Exit early if no profiles need fetching ---
    if not profiles_to_fetch:
        log.info(f"No valid profiles identified for fetching on page {page_number}.")
        return 0, 0, 0

    # --- Stage 2: Fetch Profile Pages Concurrently ---
    log.info(f"Fetching {len(profiles_to_fetch)} profiles details for page {page_number}...")
    try:
        # Run the async fetching function within the sync function context
        # This returns a map: {profile_url_relative: html_content_or_None}
        profile_html_map = asyncio.run(fetch_all_profiles(profiles_to_fetch))
    except Exception as e:
        # Handle potential errors during the asyncio run itself
        log.error(f"Error occurred during async profile fetching execution for page {page_number}: {e}", exc_info=True)
        return 0, 0, 0 # Abort processing for this page if batch fetch fails

    # --- Stage 3: Process Each Fetched Profile ---
    bulk_db_operations = [] # List to hold UpdateOne operations
    recent_trades_to_add_list = [] # List to hold trades for the recent stack

    log.info(f"Processing details for {len(profile_html_map)} fetched profiles...")
    for profile_url_relative, html_content in profile_html_map.items():
        # Get the base data associated with this URL
        base_data = base_data_map.get(profile_url_relative)
        if not base_data: # Should not happen if logic is correct, but safeguard
             log.error(f"Consistency error: No base data found for fetched profile {profile_url_relative}. Skipping.")
             continue

        # Skip processing if the fetch failed for this specific profile
        if html_content is None:
            log.warning(f"Skipping processing for {base_data['name']} ({profile_url_relative}) due to fetch failure.")
            continue

        # Parse trades from the fetched HTML content
        trades_from_page = parse_trades(html_content)
        log.debug(f"Parsed {len(trades_from_page)} trades for {base_data['name']}")

        # Get the existing record from the database
        try:
            existing_politician = politicians_collection.find_one({"profile_url": profile_url_relative})
        except Exception as db_err:
            log.error(f"Database error finding politician {profile_url_relative}: {db_err}. Skipping.", exc_info=True)
            continue # Skip this politician if DB read fails

        # --- Core Comparison Logic ---
        latest_trade_on_page = trades_from_page[0] if trades_from_page else None
        latest_trade_url_on_page = latest_trade_on_page.get("trade_url") if latest_trade_on_page else None

        perform_trade_comparison = True # Assume we need to compare unless optimization applies
        newly_found_unique_trades = [] # Trades found on page that are not in DB

        if existing_politician:
            existing_trades_data = existing_politician.get("trades_data", [])
            latest_trade_in_db = existing_trades_data[0] if existing_trades_data else None
            latest_trade_url_in_db = latest_trade_in_db.get("trade_url") if latest_trade_in_db else None

            # Optimization Check: If latest URLs match, no need for detailed trade comparison
            if latest_trade_url_on_page and latest_trade_url_in_db and latest_trade_url_on_page == latest_trade_url_in_db:
                log.info(f"No new trades suspected for {base_data['name']} based on latest trade URL match ({latest_trade_url_on_page}). Skipping detailed trade diff.")
                perform_trade_comparison = False
            # Edge Case: No trades on page and no trades in DB - nothing to compare
            elif not latest_trade_on_page and not latest_trade_in_db:
                 log.info(f"No trades found on page or in DB for {base_data['name']}. Skipping detailed trade diff.")
                 perform_trade_comparison = False
            # Else: URLs don't match, or one is missing, or DB has no trades yet -> Proceed with comparison

        # --- Build the Database Update Document ---
        db_update_doc = {}         # The document specifying $set, $push, $setOnInsert
        requires_db_update = False # Flag if any DB change is needed
        is_upsert_operation = False  # Will be set True for new politicians

        # 1. Determine New Trades (if comparison is needed)
        if perform_trade_comparison and trades_from_page:
            if not existing_politician:
                # New politician: all parsed trades are considered new initially
                newly_found_unique_trades = trades_from_page
                # Note: Trades will be added via $set during upsert construction (step 4)
            else:
                # Existing politician: Find trades on page not present in DB based on URL
                existing_trade_urls = {t.get("trade_url") for t in existing_politician.get("trades_data", []) if t.get("trade_url")}
                temp_new_trades = []
                for trade in trades_from_page:
                    trade_url = trade.get("trade_url")
                    # Stop if we find a trade URL that already exists in the DB (assumes page is ordered newest first)
                    if trade_url and trade_url in existing_trade_urls:
                        log.debug(f"Found existing trade URL ({trade_url}) for {base_data['name']}. Stopping trade diff.")
                        break
                    # If URL is new or None, add it as potentially unique
                    temp_new_trades.append(trade)

                if temp_new_trades:
                     newly_found_unique_trades = temp_new_trades # Assign if any new were found
                     log.info(f"[TRADE UPDATE] Found {len(newly_found_unique_trades)} potential new trade(s) for {base_data['name']}.")
                     # Prepare the $push part of the update document
                     db_update_doc["$push"] = {
                         "trades_data": {
                             "$each": list(reversed(newly_found_unique_trades)), # Add newest first in the DB array
                             "$position": 0
                         }
                     }
                     requires_db_update = True
                     politicians_updated_trades += 1 # Increment trade update counter
                else:
                     # This case occurs if comparison ran but found no unique trades (e.g., only old trades on page)
                     log.info(f"No unique new trades found for {base_data['name']} after detailed comparison.")

        # 2. Check for Metadata Updates (always compare if politician exists)
        metadata_updates_to_set = {}
        if existing_politician:
            # Compare fields from the listing page (base_data) with stored data
            fields_to_compare = ["name", "party", "state", "trades_count_reported",
                               "issuers_count_reported", "volume_reported",
                               "last_traded_reported", "image_url"]
            for field in fields_to_compare:
                # Use .get() for safe access on both dicts
                base_value = base_data.get(field)
                existing_value = existing_politician.get(field)
                if base_value != existing_value:
                    # Handle None vs "" comparison if needed, but direct compare often okay
                    metadata_updates_to_set[field] = base_value # Update with the value from listing page
                    log.debug(f"Metadata diff found for {base_data['name']}: Field '{field}' changed from '{existing_value}' to '{base_value}'")

            if metadata_updates_to_set:
                 log.info(f"[META UPDATE] Metadata changes detected for {base_data['name']}. Fields: {list(metadata_updates_to_set.keys())}")
                 # Ensure $set operator exists in the update doc and add changes
                 db_update_doc.setdefault("$set", {}).update(metadata_updates_to_set)
                 requires_db_update = True
                 # Increment meta counter only if no new trades were found in *this cycle*
                 if not newly_found_unique_trades:
                      politicians_updated_meta += 1

        # 3. Prepare Recent Trades Addition
        if newly_found_unique_trades:
            # Create a minimal dict for the politician info in the recent trades stack
            politician_info_for_recent = {
                "name": base_data.get("name"),
                "state": base_data.get("state"),
                "image_url": base_data.get("image_url"),
                "profile_url": base_data.get("profile_url") # Include link back to profile
            }
            # Add trades to the list (will be reversed before DB update)
            for trade in newly_found_unique_trades: # Add in order found (newest first)
                 recent_trades_to_add_list.append({
                     "politician": politician_info_for_recent,
                     "trade": trade
                 })

        # 4. Handle Upsert Case (New Politician)
        if not existing_politician:
            is_upsert_operation = True
            requires_db_update = True # Must perform an insert/upsert
            politicians_created += 1

            # Prepare $set for the initial insert: include all base data + parsed trades
            initial_set_data = base_data.copy() # Start with listing page data
            # Store the full initial list of trades found
            initial_set_data["trades_data"] = newly_found_unique_trades # Store as parsed (newest first)
            # Merge any $set operations already prepared (e.g., metadata if logic changed)
            initial_set_data.update(db_update_doc.get("$set", {}))
            db_update_doc["$set"] = initial_set_data

            # Add $setOnInsert operator for fields only set on creation
            db_update_doc["$setOnInsert"] = {"first_seen_timestamp": time.time()}

            # Remove $push if it exists; trades are included in $set for the initial insert
            db_update_doc.pop("$push", None)
            log.info(f"[NEW] Preparing insert for: {base_data['name']} ({profile_url_relative}) with {len(newly_found_unique_trades)} trades.")

        # 5. Always Update 'last_checked' Timestamp if any update occurs
        if requires_db_update:
             # Ensure $set exists and add/update the timestamp
             db_update_doc.setdefault("$set", {})["last_checked_timestamp"] = time.time()

        # 6. Add the Final UpdateOne Operation to the Bulk List (if needed)
        if requires_db_update:
             if db_update_doc: # Ensure there's actually something to update
                 bulk_db_operations.append(UpdateOne(
                     {"profile_url": profile_url_relative}, # Filter document
                     db_update_doc,                      # Update document ($set, $push, etc.)
                     upsert=is_upsert_operation          # Upsert flag (True for new politicians)
                 ))
             else:
                 # This case should ideally not happen if requires_db_update is True
                 log.warning(f"Internal state warning: requires_db_update was True but db_update_doc was empty for {profile_url_relative}. No DB operation added.")
        elif existing_politician:
             # If no data changed, we don't update 'last_checked' here, but could add logic to do so if desired
             log.debug(f"No data changes detected for {base_data['name']}. No DB operation needed.")


    # --- Stage 4: Execute Bulk Database Operations ---
    # Execute updates for the Politicians collection
    if bulk_db_operations:
        log.info(f"Executing {len(bulk_db_operations)} bulk write operations for politicians collection...")
        try:
            bulk_write_result = politicians_collection.bulk_write(bulk_db_operations, ordered=False) # ordered=False allows operations to proceed even if one fails
            log.info(f"Politician bulk write complete. Matched: {bulk_write_result.matched_count}, Modified: {bulk_write_result.modified_count}, Upserted: {bulk_write_result.upserted_count}")
            if bulk_write_result.bulk_api_result.get('writeErrors'):
                 log.error(f"Politician bulk write encountered errors: {bulk_write_result.bulk_api_result['writeErrors']}")
        except BulkWriteError as bwe:
            # Log detailed errors from BulkWriteError exception
            log.error(f"Politician bulk write failed: {bwe.details}", exc_info=True)
        except Exception as e:
            # Catch other potential errors during bulk write execution
            log.error(f"Unexpected error during politician bulk write: {e}", exc_info=True)

    # Update the Recent Trades collection (single document acting as a capped list)
    if recent_trades_to_add_list:
        log.info(f"Adding {len(recent_trades_to_add_list)} trades to recent_trades collection...")
        try:
             # Reverse the list: we want the newest trades added last so $position:0 makes them first in the array
             recent_trades_to_add_list.reverse()
             # Use a consistent _id for the document holding the recent trades stack
             recent_trades_doc_id = "recent_trades_stack_v1"
             update_result = recent_trades_collection.update_one(
                 {"_id": recent_trades_doc_id},
                 {
                     "$push": {
                         "trades": {
                             "$each": recent_trades_to_add_list, # Add the list of new trades
                             "$position": 0,          # Add to the beginning of the array
                             "$slice": 100            # Keep only the latest 100 trades in the array
                         }
                     },
                     "$set": {"last_updated_timestamp": time.time()} # Track when it was last updated
                 },
                 upsert=True # Create the document if it doesn't exist
             )
             log.info(f"Recent trades update complete. Matched: {update_result.matched_count}, Modified: {update_result.modified_count}, UpsertedId: {update_result.upserted_id}")
        except Exception as e:
             log.error(f"Error updating recent_trades collection: {e}", exc_info=True)

    # Return the counts for this specific page
    return politicians_created, politicians_updated_trades, politicians_updated_meta


# --- Main Execution Logic ---

def run_once():
    """Runs a single scan cycle across the defined number of listing pages."""
    log.info("="*20 + " Starting Scan Cycle " + "="*20)
    total_cycle_created = 0
    total_cycle_trade_updates = 0
    total_cycle_meta_updates = 0
    start_cycle_time = time.time()

    # Process listing pages sequentially
    for page_num in range(1, MAX_PAGES_TO_SCAN + 1):
        try:
            created, updated_t, updated_m = process_page(page_num)
            total_cycle_created += created
            total_cycle_trade_updates += updated_t
            total_cycle_meta_updates += updated_m
            # Optional short delay between processing listing pages to be polite
            if page_num < MAX_PAGES_TO_SCAN:
                 time.sleep(1.5) # Be nice to the server
        except Exception as e:
             # Catch unexpected errors from process_page itself
             log.error(f"Critical error processing listing page {page_num}: {e}", exc_info=True)
             # Depending on severity, could 'break' the loop or just 'continue'
             log.warning(f"Continuing scan cycle despite error on page {page_num}.")
             continue

    end_cycle_time = time.time()
    cycle_duration = end_cycle_time - start_cycle_time
    log.info(f"Scan Cycle Complete ({cycle_duration:.2f}s).")
    log.info(f"  Politicians Created: {total_cycle_created}")
    log.info(f"  Trade Updates Recorded: {total_cycle_trade_updates}")
    log.info(f"  Metadata Updates Recorded: {total_cycle_meta_updates}")
    log.info("="*50)

    # Return combined updates count for the main loop's summary log
    return total_cycle_created, total_cycle_trade_updates + total_cycle_meta_updates


def main():
    """Main function to run the bot loop."""
    log.info("Capitol Trades Bot started. Press Ctrl+C to stop.")
    total_script_runs = 0
    total_politicians_ever_created = 0
    total_updates_ever_recorded = 0 # Combined trade and meta updates over all runs
    script_start_time = time.time()

    try:
        while True:
            total_script_runs += 1
            log.info(f"--- Starting Run #{total_script_runs} ---")
            run_start_time = time.time()

            # Execute one full scan cycle
            created_in_run, updates_in_run = run_once()

            run_end_time = time.time()
            total_politicians_ever_created += created_in_run
            total_updates_ever_recorded += updates_in_run

            # Calculate overall script runtime
            total_runtime_seconds = time.time() - script_start_time
            hours = int(total_runtime_seconds // 3600)
            minutes = int((total_runtime_seconds % 3600) // 60)
            seconds = int(total_runtime_seconds % 60)

            # Log status update after each run
            log.info(f"Bot Status Summary:")
            log.info(f"  Uptime: {hours}h {minutes}m {seconds}s")
            log.info(f"  Total Cycles Completed: {total_script_runs}")
            log.info(f"  Last Cycle Duration: {run_end_time - run_start_time:.2f}s")
            log.info(f"  Total Politicians Added (all runs): {total_politicians_ever_created}")
            log.info(f"  Total Updates Recorded (all runs): {total_updates_ever_recorded}")

            # Wait before the next cycle
            log.info(f"--- Sleeping for {POLLING_INTERVAL_SECONDS} seconds ({POLLING_INTERVAL_SECONDS/60:.1f} minutes) ---")
            time.sleep(POLLING_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        # Handle graceful shutdown on Ctrl+C
        log.info("SIGINT received (Ctrl+C). Stopping bot gracefully...")
    except Exception as e:
        # Catch any unexpected critical errors in the main loop
        log.critical(f"An unexpected critical error occurred in the main loop: {e}", exc_info=True)
        log.info("Bot is shutting down due to a critical error.")
    finally:
        # Cleanup actions: Close DB connection
        script_end_time = time.time()
        total_script_runtime = script_end_time - script_start_time
        log.info(f"Bot stopped.")
        log.info(f"  Total runtime: {total_script_runtime:.2f} seconds.")
        log.info(f"  Final Summary: Ran {total_script_runs} cycles, added {total_politicians_ever_created} politicians, "
                 f"and recorded {total_updates_ever_recorded} updates (trades + metadata).")

        if client: # Check if client was successfully initialized
            try:
                client.close()
                log.info("MongoDB connection closed successfully.")
            except Exception as db_close_err:
                 log.error(f"Error while closing MongoDB connection: {db_close_err}")

        log.info("Shutdown complete.")


# --- Script Entry Point ---
if __name__ == "__main__":
    # Optional: Add check for Python version if using very new features
    # if sys.version_info < (3, 8):
    #     log.critical("Python 3.8 or higher is recommended for asyncio features.")
    #     sys.exit(1)

    # Optional: Set asyncio loop policy for Windows compatibility if needed (often not required anymore)
    # if os.name == 'nt' and sys.version_info < (3, 8):
    #    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    main() # Start the main bot loop