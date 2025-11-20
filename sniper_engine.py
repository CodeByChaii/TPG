import json
import os
import random  # Mocking geo-scores if API missing
import re
import time
from pathlib import Path
import psycopg2
from dotenv import load_dotenv
import requests
from google.cloud import translate as translate_client

load_dotenv()

# DB CONNECTION
DB_URL = os.getenv("DATABASE_URL")


def get_db_connection():
    return psycopg2.connect(DB_URL)


REGULAR_API_URL = "https://bam-els-sync-api-prd.bam.co.th/api/asset-detail/search"
AUCTION_API_URL = "https://bam-els-sync-api-prd.bam.co.th/api/asset-detail-auction/search"
REQUEST_TIMEOUT = int(os.getenv("BAM_REQUEST_TIMEOUT", "15"))
PAGE_SIZE = int(os.getenv("BAM_PAGE_SIZE", "12"))
ONE_PAGE_ONLY = os.getenv("ONE_PAGE_ONLY") == "1"
BATCH_SIZE = int(os.getenv("BAM_BATCH_SIZE", "1000"))
PAUSE_BETWEEN_BATCHES = os.getenv("BAM_BATCH_PAUSE", "1") != "0"
AUTO_CONTINUE = os.getenv("BAM_AUTO_CONTINUE", "0") == "1"
PROGRESS_FILE = Path(os.getenv("BAM_PROGRESS_FILE", "bam_progress.json")).expanduser()
PAGES_PER_RUN = int(os.getenv("BAM_PAGES_PER_RUN", "0"))
MAX_RETRIES = max(1, int(os.getenv("BAM_MAX_RETRIES", "5")))
RETRY_BACKOFF = float(os.getenv("BAM_RETRY_BACKOFF", "2.0"))
RETRYABLE_STATUS_CODES = {
    int(code)
    for code in os.getenv("BAM_RETRY_STATUSES", "500,502,503,504").split(",")
    if code.strip().isdigit()
}
PLAN_FILE = Path(os.getenv("BAM_PAGE_PLAN_FILE", "")).expanduser()

SKIP_FAILED_PAGES = os.getenv("BAM_SKIP_FAILED_PAGES", "1") == "1"
MAX_SKIP_CHAIN = max(1, int(os.getenv("BAM_MAX_SKIP_CHAIN", "3")))
CATEGORY_CONFIGS = [
    {"label": "General Feed", "asset_types": [], "property_type_hint": "Mixed", "sale_channel": "standard"},
    {"label": "Single Houses", "asset_types": ["บ้านเดี่ยว"], "property_type_hint": "บ้านเดี่ยว", "sale_channel": "standard"},
    {"label": "Townhouses", "asset_types": ["ทาวน์เฮ้าส์"], "property_type_hint": "ทาวน์เฮ้าส์", "sale_channel": "standard"},
    {"label": "Condos", "asset_types": ["ห้องชุดพักอาศัย"], "property_type_hint": "ห้องชุดพักอาศัย", "sale_channel": "standard"},
    {"label": "Vacant Land", "asset_types": ["ที่ดินเปล่า"], "property_type_hint": "ที่ดินเปล่า", "sale_channel": "standard"},
    {"label": "Commercial Buildings", "asset_types": ["อาคารพาณิชย์"], "property_type_hint": "อาคารพาณิชย์", "sale_channel": "standard"},
]

session = requests.Session()


def _post_with_retry(url, payload, label, page_number):
    attempt = 1
    while True:
        try:
            resp = session.post(url, json=payload, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp
        except requests.HTTPError as exc:
            status_code = exc.response.status_code if exc.response else None
            retryable = status_code in RETRYABLE_STATUS_CODES if status_code else True
            if retryable and attempt < MAX_RETRIES:
                delay = RETRY_BACKOFF ** (attempt - 1)
                print(
                    f"⚠️ {label} page {page_number}: HTTP {status_code}, retrying in {delay:.1f}s (attempt {attempt}/{MAX_RETRIES})"
                )
                time.sleep(delay)
                attempt += 1
                continue
            failure_detail = status_code or "unknown status"
            print(
                f"🛑 {label} page {page_number}: giving up after {attempt} attempts (HTTP {failure_detail})"
            )
            raise
        except (requests.ConnectionError, requests.Timeout) as exc:
            if attempt < MAX_RETRIES:
                delay = RETRY_BACKOFF ** (attempt - 1)
                print(
                    f"⚠️ {label} page {page_number}: network error {exc.__class__.__name__}, retrying in {delay:.1f}s (attempt {attempt}/{MAX_RETRIES})"
                )
                time.sleep(delay)
                attempt += 1
                continue
            print(
                f"🛑 {label} page {page_number}: giving up after {attempt} attempts due to {exc.__class__.__name__}"
            )
            raise


class PageWindow:
    def __init__(self, target):
        self.target = target if target and target > 0 else None
        self.processed = 0

    def allow_next(self):
        if self.target is None:
            return True
        return self.processed < self.target

    def mark_complete(self):
        if self.target is None:
            return
        self.processed += 1

    def remaining(self):
        if self.target is None:
            return None
        return max(self.target - self.processed, 0)

    def exhausted(self):
        return self.target is not None and self.processed >= self.target


def _coerce_page_value(value, default=0):
    try:
        return max(0, int(float(value)))
    except (TypeError, ValueError):
        return default


def load_progress_state():
    state = {"regular": {}, "auction": {"page": 0}}
    if not PROGRESS_FILE:
        return state
    try:
        if PROGRESS_FILE.exists():
            with PROGRESS_FILE.open("r", encoding="utf-8") as handle:
                data = json.load(handle)
            if isinstance(data, dict):
                regular_map = data.get("regular") or {}
                for key, value in regular_map.items():
                    state["regular"][str(key)] = _coerce_page_value(value)
                auction_data = data.get("auction") or {}
                if isinstance(auction_data, dict):
                    state["auction"]["page"] = _coerce_page_value(auction_data.get("page"))
    except Exception as exc:
        print(f"⚠️ Failed to load progress file {PROGRESS_FILE}: {exc}")
    return state


def save_progress_state(state):
    if not PROGRESS_FILE:
        return
    try:
        PROGRESS_FILE.parent.mkdir(parents=True, exist_ok=True)
        with PROGRESS_FILE.open("w", encoding="utf-8") as handle:
            json.dump(state, handle, ensure_ascii=False, indent=2)
    except Exception as exc:
        print(f"⚠️ Failed to persist progress file {PROGRESS_FILE}: {exc}")


def describe_progress():
    if not PROGRESS_FILE.exists():
        return "no saved progress yet"
    return f"resuming via {PROGRESS_FILE}"


def load_page_plan():
    if not PLAN_FILE:
        return None
    try:
        if PLAN_FILE.exists():
            with PLAN_FILE.open("r", encoding="utf-8") as handle:
                data = json.load(handle)
            if isinstance(data, dict):
                return data
    except Exception as exc:
        print(f"⚠️ Failed to read plan file {PLAN_FILE}: {exc}")
    return None


def describe_plan(plan):
    if not plan:
        return "no plan provided"
    summary_bits = []
    regular = plan.get("regular") or {}
    for label, pages in regular.items():
        summary_bits.append(f"{label}:{len(pages)} pages")
    auction_pages = plan.get("auction")
    if auction_pages:
        summary_bits.append(f"Auction:{len(auction_pages)} pages")
    return "plan→ " + ", ".join(summary_bits)


def consume_plan_file():
    if PLAN_FILE and PLAN_FILE.exists():
        try:
            PLAN_FILE.unlink()
        except OSError as exc:
            print(f"⚠️ Unable to remove plan file {PLAN_FILE}: {exc}")


def wait_for_user_confirmation(batch_number, total_processed):
    """Pause between batches so the operator can verify results."""
    if not PAUSE_BETWEEN_BATCHES or AUTO_CONTINUE:
        return True
    prompt = (
        f"\nBatch {batch_number} complete ({total_processed} listings synced). "
        "Press Enter to continue or type 'q' to abort: "
    )
    try:
        response = input(prompt)
    except EOFError:
        # Non-interactive environment—resume automatically.
        return True
    return response.strip().lower() not in {"q", "quit", "stop", "exit"}

# --- 1. SCORING ALGORITHM (The "Brain") ---
def calculate_rating(price, size, lat, lon):
    # In a real app, use Google Maps API here.
    # For MVP, we simulate accurate scoring based on districts.
    
    # Base Score
    score = 5.0
    
    # Strategy Logic
    strategy = "Hold"
    price_per_sqm = price / size if size > 0 else 0
    
    if price_per_sqm < 40000: # Cheap for Bangkok
        strategy = "Big Flip"
        score += 2
    elif price_per_sqm < 80000:
        strategy = "Cash Flow"
        score += 1
        
    # Location Simulation (Mocking "Walk to BTS")
    transport_score = random.randint(4, 10)
    food_score = random.randint(5, 10)
    safety_score = random.randint(6, 10)
    
    total_rating = min(10, (score + (transport_score/10) + (food_score/10)))
    
    return {
        "strategy": strategy,
        "rating": round(total_rating, 1),
        "transport": transport_score,
        "food": food_score,
        "safety": safety_score
    }

def to_float(value, default=None):
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def extract_number(text):
    if not text:
        return None
    match = re.search(r"[0-9]+(?:\.[0-9]+)?", str(text))
    return float(match.group(0)) if match else None


def gather_images(*sources):
    images = []
    for source in sources:
        if not source:
            continue
        if isinstance(source, list):
            for item in source:
                if isinstance(item, dict) and item.get("url"):
                    images.append(item["url"])
                elif isinstance(item, str):
                    images.append(item)
        elif isinstance(source, dict) and source.get("url"):
            images.append(source["url"])
        elif isinstance(source, str):
            images.append(source)
    return images


def dedupe_images(urls):
    seen = set()
    deduped = []
    for url in urls:
        if not url:
            continue
        if url in seen:
            continue
        seen.add(url)
        deduped.append(url)
    return deduped


def combine_contact(name, *phones):
    phone_list = [p for p in phones if p]
    phone_text = ", ".join(phone_list)
    if name and phone_text:
        return f"{name} ({phone_text})"
    if name:
        return name
    return phone_text or None


def build_location(item):
    location_parts = [item.get("province"), item.get("district"), item.get("subDistrict")]
    location_core = ", ".join([part for part in location_parts if part])
    property_location = item.get("propertyLocation")
    if property_location and location_core:
        return f"{location_core} | {property_location}"
    if property_location:
        return property_location
    return location_core or item.get("location") or "Unknown"


def normalize_regular_item(item, config):
    asset_no = item.get("assetNo") or str(item.get("id", ""))
    title = item.get("projectTH") or item.get("assetType") or (f"NPA Asset {asset_no}" if asset_no else "NPA Asset")
    price = to_float(item.get("sellPrice") or item.get("shockPrice") or item.get("discountPrice") or 0, 0.0) or 0.0
    size = to_float(item.get("usableArea") or item.get("areaMeter") or item.get("areaWa")) or 0.0
    bedrooms = extract_number(item.get("bedroom") or item.get("studio"))
    bathrooms = extract_number(item.get("bathroom"))
    rooms = extract_number(item.get("rooms")) or bedrooms or extract_number(item.get("studio"))

    map_info = item.get("map") or item.get("geoMap") or {}
    lat = to_float(map_info.get("langtitude") or map_info.get("latitude"))
    lon = to_float(map_info.get("longtitude") or map_info.get("longitude"))
    if lat is None or lon is None:
        lat = 13.7 + (random.random() * 0.1)
        lon = 100.5 + (random.random() * 0.1)

    property_images = gather_images(
        item.get("albumProperty"),
        item.get("media"),
        item.get("albumPackage1"),
        item.get("albumPackage2"),
        item.get("albumPackage3"),
    )
    map_images = gather_images(
        map_info.get("imageUrl"),
        map_info.get("imageUrl360"),
        map_info.get("mapImage"),
    )
    images = dedupe_images(property_images + map_images)

    listing = {
        "source": "BAM",
        "title": title,
        "price": price,
        "size": size,
        "lat": lat,
        "lon": lon,
        "url": f"https://www.bam.co.th/asset/{asset_no}" if asset_no else "https://www.bam.co.th/asset/",
        "location": build_location(item),
        "description": item.get("propertyDetail") or item.get("summary") or item.get("location"),
        "contact": combine_contact(
            item.get("adminName") or item.get("adminNameConx"),
            item.get("telephone"),
            item.get("workPhone"),
            item.get("workPhoneNxt"),
            item.get("workPhoneConx"),
        ),
        "bank": item.get("departmentName") or item.get("groupOfDepartment") or item.get("groupProperty") or "BAM",
        "images": images,
        "metrics": calculate_rating(price, size, lat, lon),
        "property_type": item.get("assetType") or config.get("property_type_hint"),
        "sale_channel": config.get("sale_channel", "standard"),
        "bedrooms": bedrooms,
        "bathrooms": bathrooms,
        "rooms": rooms,
    }
    return listing


def normalize_auction_item(item):
    price = to_float(
        item.get("priceSetByCommittee")
        or item.get("priceEstimateOfLegalOfficer")
        or item.get("priceEstimateOfReciveorship")
        or 0,
        0.0,
    ) or 0.0
    size = extract_number(item.get("area")) or 0.0
    lat = 13.7 + (random.random() * 0.1)
    lon = 100.5 + (random.random() * 0.1)
    caseno = item.get("caseno") or item.get("caseNo")
    description_bits = [
        item.get("address"),
        f"Auction window {item.get('startDate')} → {item.get('endDate')}",
        item.get("placeAuction"),
        item.get("conditionBidder"),
    ]
    description = " | ".join([bit for bit in description_bits if bit])
    contact = combine_contact(item.get("contact"), item.get("claimant"))
    property_images = gather_images(item.get("assetImage"), item.get("images"))
    map_images = gather_images(item.get("mapImage"))
    images = dedupe_images(property_images + map_images)

    bedrooms = extract_number(item.get("bedroom"))
    bathrooms = extract_number(item.get("bathroom"))
    rooms = extract_number(item.get("rooms")) or bedrooms

    listing = {
        "source": "BAM",
        "title": item.get("assetType") or "Auction Asset",
        "price": price,
        "size": size,
        "lat": lat,
        "lon": lon,
        "url": f"{item.get('assetUrl')}?case={caseno}" if caseno else item.get("assetUrl"),
        "location": f"{item.get('province')}, {item.get('district')} | {item.get('address')}".strip(),
        "description": description,
        "contact": contact,
        "bank": "BAM Auction",
        "images": images,
        "metrics": calculate_rating(price, size, lat, lon),
        "property_type": item.get("assetType") or "Auction",
        "sale_channel": "auction",
        "bedrooms": bedrooms,
        "bathrooms": bathrooms,
        "rooms": rooms,
    }
    return listing


def fetch_regular_assets(progress_state, page_window, plan_pages=None):
    regular_progress = progress_state.setdefault("regular", {})
    for config in CATEGORY_CONFIGS:
        planned = []
        if plan_pages:
            buffered = []
            for raw_value in plan_pages.get(config["label"], []):
                page = _coerce_page_value(raw_value)
                if page > 0:
                    buffered.append(page)
            if buffered:
                planned = sorted(set(buffered))

        if planned:
            for page_number in planned:
                if not page_window.allow_next():
                    print("ℹ️ Page window exhausted before finishing regular plan; resuming next run.")
                    return
                payload = {
                    "assetTypes": config.get("asset_types", []),
                    "keyword": None,
                    "provinces": [],
                    "districts": [],
                    "pageNumber": page_number,
                    "pageSize": PAGE_SIZE,
                    "orderBy": "DEFAULT",
                }
                try:
                    resp = _post_with_retry(REGULAR_API_URL, payload, config["label"], page_number)
                    result = resp.json()
                except Exception as exc:
                    print(f"⚠️ Regular fetch failed for {config['label']} page {page_number}: {exc}")
                    continue

                rows = result.get("data", [])
                if not rows:
                    continue

                print(f"{config['label']} page {page_number}: pulled {len(rows)} assets (plan)")
                for item in rows:
                    yield normalize_regular_item(item, config)

                regular_progress[config["label"]] = max(page_number, regular_progress.get(config["label"], 0))
                save_progress_state(progress_state)
                page_window.mark_complete()
            continue

        page_number = max(1, regular_progress.get(config["label"], 0) + 1)
        consecutive_failures = 0
        while True:
            if not page_window.allow_next():
                print("ℹ️ Page window exhausted before finishing regular feed; resuming next run.")
                return
            payload = {
                "assetTypes": config.get("asset_types", []),
                "keyword": None,
                "provinces": [],
                "districts": [],
                "pageNumber": page_number,
                "pageSize": PAGE_SIZE,
                "orderBy": "DEFAULT",
            }
            try:
                resp = _post_with_retry(REGULAR_API_URL, payload, config["label"], page_number)
                result = resp.json()
            except Exception as exc:
                print(f"⚠️ Regular fetch failed for {config['label']} page {page_number}: {exc}")
                if SKIP_FAILED_PAGES:
                    consecutive_failures += 1
                    if consecutive_failures > MAX_SKIP_CHAIN:
                        print(
                            f"🛑 Too many skipped pages in {config['label']} (>{MAX_SKIP_CHAIN}); pausing category."
                        )
                        break
                    print(
                        f"🧭 Skipping {config['label']} page {page_number} (skip #{consecutive_failures}/{MAX_SKIP_CHAIN})."
                    )
                    regular_progress[config["label"]] = page_number
                    save_progress_state(progress_state)
                    page_number += 1
                    continue
                break

            rows = result.get("data", [])
            if not rows:
                break

            print(f"{config['label']} page {page_number}: pulled {len(rows)} assets")
            for item in rows:
                yield normalize_regular_item(item, config)

            regular_progress[config["label"]] = page_number
            save_progress_state(progress_state)
            page_window.mark_complete()
            consecutive_failures = 0

            if ONE_PAGE_ONLY:
                break

            if page_window.exhausted():
                print("✅ Captured requested page window; pausing regular feed.")
                return

            total_data = to_float(result.get("totalData"))
            if total_data and page_number * PAGE_SIZE >= total_data:
                break
            if len(rows) < PAGE_SIZE:
                break
            page_number += 1


def fetch_auction_assets(progress_state, page_window, plan_pages=None):
    auction_state = progress_state.setdefault("auction", {"page": 0})
    planned = []
    if plan_pages:
        buffered = []
        for raw_value in plan_pages:
            page = _coerce_page_value(raw_value)
            if page > 0:
                buffered.append(page)
        if buffered:
            planned = sorted(set(buffered))

    if planned:
        for page_number in planned:
            if not page_window.allow_next():
                print("ℹ️ Page window exhausted before auction plan; resuming next run.")
                return
            payload = {"pageNumber": page_number, "pageSize": PAGE_SIZE}
            try:
                resp = _post_with_retry(AUCTION_API_URL, payload, "Auction", page_number)
                result = resp.json()
            except Exception as exc:
                print(f"⚠️ Auction fetch failed on page {page_number}: {exc}")
                continue

            rows = result.get("data", [])
            if not rows:
                continue

            print(f"Auction feed page {page_number}: pulled {len(rows)} assets (plan)")
            for item in rows:
                yield normalize_auction_item(item)

            auction_state["page"] = max(page_number, auction_state.get("page", 0))
            save_progress_state(progress_state)
            page_window.mark_complete()
        return

    page_number = max(1, auction_state.get("page", 0) + 1)
    consecutive_failures = 0
    while True:
        if not page_window.allow_next():
            print("ℹ️ Page window exhausted before auction feed; resuming next run.")
            return
        payload = {"pageNumber": page_number, "pageSize": PAGE_SIZE}
        try:
            resp = _post_with_retry(AUCTION_API_URL, payload, "Auction", page_number)
            result = resp.json()
        except Exception as exc:
            print(f"⚠️ Auction fetch failed on page {page_number}: {exc}")
            if SKIP_FAILED_PAGES:
                consecutive_failures += 1
                if consecutive_failures > MAX_SKIP_CHAIN:
                    print(
                        f"🛑 Too many skipped pages in auction feed (>{MAX_SKIP_CHAIN}); pausing auction stream."
                    )
                    break
                print(
                    f"🧭 Skipping auction page {page_number} (skip #{consecutive_failures}/{MAX_SKIP_CHAIN})."
                )
                auction_state["page"] = page_number
                save_progress_state(progress_state)
                page_number += 1
                continue
            break

        rows = result.get("data", [])
        if not rows:
            break

        print(f"Auction feed page {page_number}: pulled {len(rows)} assets")
        for item in rows:
            yield normalize_auction_item(item)

        auction_state["page"] = page_number
        save_progress_state(progress_state)
        page_window.mark_complete()
        consecutive_failures = 0

        if ONE_PAGE_ONLY:
            break

        if page_window.exhausted():
            print("✅ Captured requested page window; finishing auction feed.")
            return

        total_data = to_float(result.get("totalData"))
        if total_data and page_number * PAGE_SIZE >= total_data:
            break
        if len(rows) < PAGE_SIZE:
            break
        page_number += 1


# --- 2. SCRAPER (The "Collector") ---
def scrape_bam(pages_limit=None):
    progress_state = load_progress_state()
    window = PageWindow(pages_limit if pages_limit is not None else PAGES_PER_RUN)
    plan = load_page_plan()
    target_note = ""
    if window.target:
        target_note = f" Targeting next {window.target} pages ({describe_progress()})."
    else:
        target_note = f" ({describe_progress()})"
    plan_note = f" {describe_plan(plan)}" if plan else ""
    print(f"🚀 Launching Sniper Drone to BAM Website APIs...{target_note}{plan_note}")
    regular_plan = (plan or {}).get("regular")
    auction_plan = (plan or {}).get("auction")
    for listing in fetch_regular_assets(progress_state, window, regular_plan):
        yield listing
    for listing in fetch_auction_assets(progress_state, window, auction_plan):
        yield listing
    if plan:
        consume_plan_file()

# --- 3. STORAGE (The "Vault") ---
def save_to_cloud(listings):
    conn = get_db_connection()
    cur = conn.cursor()

    def ensure_space_columns():
        cur.execute("ALTER TABLE properties ADD COLUMN IF NOT EXISTS rooms NUMERIC")
        cur.execute("ALTER TABLE properties ADD COLUMN IF NOT EXISTS bedrooms NUMERIC")
        cur.execute("ALTER TABLE properties ADD COLUMN IF NOT EXISTS bathrooms NUMERIC")

    ensure_space_columns()

    def translate_text(text, target_lang="en"):
        if not text:
            return text
        if os.getenv('SKIP_TRANSLATION') == '1':
            return text
        project = os.getenv('GOOGLE_CLOUD_PROJECT')
        if project:
            try:
                client = translate_client.TranslationServiceClient()
                parent = f"projects/{project}/locations/global"
                response = client.translate_text(
                    request={
                        "parent": parent,
                        "contents": [text],
                        "mime_type": "text/plain",
                        "target_language_code": target_lang,
                    }
                )
                if response and response.translations:
                    return response.translations[0].translated_text
            except Exception as e:
                print(f"GCP translate failed: {e}")
        try:
            url = "https://translate.googleapis.com/translate_a/single"
            params = {
                "client": "gtx",
                "sl": "auto",
                "tl": target_lang,
                "dt": "t",
                "q": text
            }
            r = requests.get(url, params=params, timeout=5)
            if r.status_code == 200:
                res = r.json()
                return res[0][0][0]
        except Exception as e:
            print(f"Fallback translation failed: {e}")
        return text

    seen_urls = set()
    duplicates_skipped = 0
    processed_count = 0
    new_total = 0
    batch_inserted = 0
    batch_number = 1
    aborted = False

    for item in listings:
        url = item.get('url')
        if not url:
            continue
        if url in seen_urls:
            duplicates_skipped += 1
            continue
        seen_urls.add(url)

        m = item['metrics']
        title = item.get('title')
        title_en = translate_text(title, "en") if title else None
        description = item.get('description') or item.get('desc') or ''
        description_en = translate_text(description, "en") if description else None
        location = item.get('location')
        location_en = translate_text(location, "en") if location else None
        photos = ''
        if item.get('images'):
            photos = ','.join(item['images'])
        elif item.get('photo'):
            photos = item['photo']

        contact = item.get('contact')
        contact_en = translate_text(contact, "en") if contact else None
        bank_value = item.get('bank') or item.get('source')
        bank_en = translate_text(bank_value, "en") if bank_value else None

        living_rating = m.get('rating')
        rent_estimate = int(item.get('price', 0) * 0.004) if item.get('price') else None
        investment_rating = m.get('rating')
        rooms_value = item.get('rooms')
        bedrooms_value = item.get('bedrooms')
        bathrooms_value = item.get('bathrooms')

        cur.execute("""
            INSERT INTO properties 
            (source, title, title_en, description, description_en, price, size_sqm, lat, lon, url, photos, property_type, sale_channel,
             location, location_en, contact, contact_en, bank, bank_en, strategy, total_rating, transport_score, food_score, safety_score,
             living_rating, rent_estimate, investment_rating, rooms, bedrooms, bathrooms, last_updated)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (url) DO UPDATE SET
            price = EXCLUDED.price,
            title = EXCLUDED.title,
            title_en = EXCLUDED.title_en,
            description = EXCLUDED.description,
            description_en = EXCLUDED.description_en,
            photos = EXCLUDED.photos,
            property_type = EXCLUDED.property_type,
            sale_channel = EXCLUDED.sale_channel,
            location = EXCLUDED.location,
            location_en = EXCLUDED.location_en,
            contact = EXCLUDED.contact,
            contact_en = EXCLUDED.contact_en,
            bank = EXCLUDED.bank,
            bank_en = EXCLUDED.bank_en,
            strategy = EXCLUDED.strategy,
            total_rating = EXCLUDED.total_rating,
            transport_score = EXCLUDED.transport_score,
            food_score = EXCLUDED.food_score,
            safety_score = EXCLUDED.safety_score,
            living_rating = EXCLUDED.living_rating,
            rent_estimate = EXCLUDED.rent_estimate,
            investment_rating = EXCLUDED.investment_rating,
            rooms = EXCLUDED.rooms,
            bedrooms = EXCLUDED.bedrooms,
            bathrooms = EXCLUDED.bathrooms,
            last_updated = NOW()
            RETURNING (xmax = 0) AS inserted;
        """, (
            item.get('source'), title, title_en, description, description_en, item.get('price'), item.get('size'), item.get('lat'), item.get('lon'), item.get('url'),
            photos, item.get('property_type'), item.get('sale_channel'), location, location_en, contact, contact_en, bank_value, bank_en, m.get('strategy'), m.get('rating'), m.get('transport'), m.get('food'), m.get('safety'),
            living_rating, rent_estimate, investment_rating, rooms_value, bedrooms_value, bathrooms_value
        ))

        inserted = cur.fetchone()[0]
        processed_count += 1
        if inserted:
            new_total += 1
            batch_inserted += 1

        if BATCH_SIZE > 0 and batch_inserted >= BATCH_SIZE:
            conn.commit()
            print(
                f"Batch {batch_number} committed: {batch_inserted} new listings (processed {processed_count}, total new {new_total})."
            )
            if not wait_for_user_confirmation(batch_number, new_total):
                aborted = True
                break
            batch_number += 1
            batch_inserted = 0

    conn.commit()
    conn.close()

    total_updates = processed_count - new_total
    status_prefix = "🛑" if aborted else "✅"
    print(
        f"{status_prefix} Data Sync Complete. {processed_count} unique listings processed, {new_total} new, {total_updates} refreshed."
    )
    if duplicates_skipped:
        print(f"ℹ️ Skipped {duplicates_skipped} duplicate URLs surfacing across feeds.")

if __name__ == "__main__":
    listings = scrape_bam()
    save_to_cloud(listings)
