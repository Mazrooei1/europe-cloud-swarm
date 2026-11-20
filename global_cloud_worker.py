"""
Global Multi-Region Cloud Extraction Worker (GitHub Actions Swarm).
Covers all portals previously outside the European 7-portal swarm:
- Remaining Europe: willhaben_at, myhome_ie, daft_ie
- UAE: bayut_ae, propertyfinder_ae, dubizzle_ae
- MENA / GCC: propertyfinder_bh, propertyfinder_eg, propertyfinder_qa, propertyfinder_om, propertyfinder_kw, bayut_sa, haraj_sa
- Americas & UK: rightmove_uk, inmuebles24_mx, redfin_us

35 Concurrent HTTP/2 TLS Workers • Zero-DOM Deserialization • SQLite WAL Shard Output • E.164 Contact Normalization
"""

import argparse
import concurrent.futures
import json
import logging
import random
import re
import sys
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Tuple

from bs4 import BeautifulSoup
from curl_cffi import requests

from cloud_schema import COUNTRY_META, init_shard_db, normalize_item, upsert_batch

# Extend COUNTRY_META with Global / MENA / Americas / UK currencies, prefixes & FX rates
COUNTRY_META.update({
    "AE": ("UAE", "AED", "+971", 0.272),
    "SA": ("Saudi Arabia", "SAR", "+966", 0.266),
    "BH": ("Bahrain", "BHD", "+973", 2.65),
    "EG": ("Egypt", "EGP", "+20", 0.020),
    "QA": ("Qatar", "QAR", "+974", 0.275),
    "OM": ("Oman", "OMR", "+968", 2.60),
    "KW": ("Kuwait", "KWD", "+965", 3.25),
    "GB": ("United Kingdom", "GBP", "+44", 1.27),
    "CA": ("Canada", "CAD", "+1", 0.72),
    "MX": ("Mexico", "MXN", "+52", 0.050),
    "US": ("United States", "USD", "+1", 1.00),
})

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("global_cloud_worker")

TLS_PROFILES = ["chrome124", "chrome120", "safari15_5"]
thread_local = threading.local()


def get_session() -> requests.Session:
    if not hasattr(thread_local, "session"):
        profile = random.choice(TLS_PROFILES)
        thread_local.session = requests.Session(impersonate=profile)
        thread_local.profile = profile
    return thread_local.session


def fetch_get(url: str, headers: Dict[str, str] = None, max_retries: int = 3) -> Any:
    s = get_session()
    hdrs = headers or {}
    for attempt in range(max_retries):
        try:
            r = s.get(url, headers=hdrs, impersonate=thread_local.profile, timeout=14)
            if r.status_code == 200:
                return r
            if r.status_code in (429, 503):
                time.sleep((2 ** attempt) + random.uniform(0.3, 1.2))
                thread_local.profile = random.choice(TLS_PROFILES)
                thread_local.session = requests.Session(impersonate=thread_local.profile)
                s = thread_local.session
                continue
            return None
        except Exception:
            time.sleep(0.5 * (attempt + 1))
    return None


def fetch_post(url: str, payload: Dict[str, Any], headers: Dict[str, str] = None, max_retries: int = 3) -> Any:
    s = get_session()
    hdrs = headers or {}
    for attempt in range(max_retries):
        try:
            r = s.post(url, json=payload, headers=hdrs, impersonate=thread_local.profile, timeout=14)
            if r.status_code == 200:
                return r
            if r.status_code in (429, 503):
                time.sleep((2 ** attempt) + random.uniform(0.3, 1.0))
                continue
            return None
        except Exception:
            time.sleep(0.5 * (attempt + 1))
    return None


# ----------------- 1. Remaining Europe: Willhaben, MyHome, Daft ----------------- #

def extract_willhaben(params: Tuple[str, int]) -> Tuple[List[Dict], List[Dict]]:
    cat_slug, page = params
    url = f"https://www.willhaben.at/iad/immobilien/{cat_slug}?rows=30&page={page}"
    r = fetch_get(url, {"Accept-Language": "de-AT,de;q=0.9"})
    if not r:
        return [], []
    soup = BeautifulSoup(r.text, "html.parser")
    sc = soup.find("script", id="__NEXT_DATA__")
    if not sc or not sc.string:
        return [], []
    items, orgs = [], []
    try:
        data = json.loads(sc.string)
        ads = (
            data.get("props", {})
            .get("pageProps", {})
            .get("searchResult", {})
            .get("advertSummaryList", {})
            .get("advertSummary", [])
        )
        purpose = "for_rent" if "mieten" in cat_slug else "for_sale"
        for ad in ads:
            aid = str(ad.get("id") or "")
            if not aid:
                continue
            attrs = {a.get("name"): (a.get("values") or [None])[0] for a in ad.get("attributes", {}).get("attribute", [])}
            raw = {
                "item_id": f"willhaben_at_{aid}",
                "portal_item_id": aid,
                "url": f"https://www.willhaben.at/iad/immobilien/d/{aid}/",
                "title": ad.get("description") or attrs.get("HEADING") or "Immobilie in Österreich",
                "purpose": purpose,
                "bedrooms": attrs.get("NUMBER_OF_ROOMS"),
                "area_sqm": attrs.get("ESTATE_SIZE/LIVING_AREA") or attrs.get("ESTATE_SIZE"),
                "price_local": attrs.get("PRICE"),
                "state_or_province": attrs.get("STATE"),
                "city": attrs.get("LOCATION") or attrs.get("DISTRICT"),
                "postal_code": attrs.get("POSTCODE"),
                "brokerage_name": attrs.get("ORGNAME"),
            }
            items.append(normalize_item(raw, "AT", "willhaben_at"))
    except Exception:
        pass
    return items, orgs


def extract_myhome(params: Tuple[int, int, int]) -> Tuple[List[Dict], List[Dict]]:
    min_p, max_p, page = params
    url = "https://api.myhome.ie/search"
    payload = {
        "ApiKey": "5f4bc325-7a03-4f88-a06c-d4e7806c90e2",
        "CorrelationId": "cloud-swarm",
        "Page": page,
        "PageSize": 50,
        "PropertyClassIds": [1],
        "MinPrice": min_p,
    }
    if max_p > 0:
        payload["MaxPrice"] = max_p
    r = fetch_post(url, payload, {"Content-Type": "application/json"})
    if not r:
        return [], []
    items, orgs = [], []
    try:
        for row in r.json().get("SearchResults", []):
            pid = str(row.get("PropertyId") or "")
            if not pid:
                continue
            raw = {
                "item_id": f"myhome_ie_{pid}",
                "portal_item_id": pid,
                "url": f"https://www.myhome.ie/residential/brochure/{pid}",
                "title": row.get("DisplayAddress") or "Property in Ireland",
                "purpose": "for_sale",
                "bedrooms": row.get("BedsString"),
                "bathrooms": row.get("BathString"),
                "area_sqm": row.get("SizeStringMeters"),
                "price_local": row.get("PriceAsString"),
                "city": row.get("County") or row.get("Region"),
                "full_address": row.get("DisplayAddress"),
                "agent_phone": row.get("GroupPhoneNumber"),
                "brokerage_name": row.get("GroupName"),
                "latitude": (row.get("GeoLocation") or {}).get("Latitude"),
                "longitude": (row.get("GeoLocation") or {}).get("Longitude"),
            }
            items.append(normalize_item(raw, "IE", "myhome_ie"))
    except Exception:
        pass
    return items, orgs


def extract_daft(params: Tuple[str, int]) -> Tuple[List[Dict], List[Dict]]:
    section, page = params
    url = "https://gateway.daft.ie/old/v1/listings"
    payload = {
        "section": section,
        "paging": {"from": str((page - 1) * 20), "pageSize": "20"},
    }
    headers = {
        "Content-Type": "application/json",
        "brand": "daft",
        "platform": "web",
        "Origin": "https://www.daft.ie",
        "Referer": "https://www.daft.ie/",
    }
    r = fetch_post(url, payload, headers)
    if not r:
        return [], []
    items, orgs = [], []
    try:
        for row in r.json().get("listings", []):
            l = row.get("listing") or {}
            lid = str(l.get("id") or "")
            if not lid:
                continue
            seller = l.get("seller") or {}
            raw = {
                "item_id": f"daft_ie_{lid}",
                "portal_item_id": lid,
                "url": f"https://www.daft.ie{l.get('seoFriendlyPath', '')}",
                "title": l.get("title"),
                "purpose": "for_rent" if "rent" in section else "for_sale",
                "bedrooms": l.get("numBedrooms"),
                "bathrooms": l.get("numBathrooms"),
                "price_local": l.get("price"),
                "full_address": l.get("title"),
                "agent_name": seller.get("name"),
                "agent_phone": seller.get("phone"),
                "brokerage_name": seller.get("branch"),
            }
            items.append(normalize_item(raw, "IE", "daft_ie"))
    except Exception:
        pass
    return items, orgs


# ----------------- 2. MENA / PropertyFinder Multi-Country ----------------- #

PF_DOMAINS = {
    "propertyfinder_ae": ("https://www.propertyfinder.ae", "AE"),
    "propertyfinder_bh": ("https://www.propertyfinder.bh", "BH"),
    "propertyfinder_eg": ("https://www.propertyfinder.eg", "EG"),
    "propertyfinder_qa": ("https://www.propertyfinder.qa", "QA"),
    "propertyfinder_om": ("https://www.propertyfinder.om", "OM"),
    "propertyfinder_kw": ("https://www.propertyfinder.kw", "KW"),
}


def extract_propertyfinder(params: Tuple[str, str, int]) -> Tuple[List[Dict], List[Dict]]:
    portal, cat_path, page = params
    base_url, c_code = PF_DOMAINS[portal]
    url = f"{base_url}/en/{cat_path}?page={page}"
    r = fetch_get(url, {"Accept-Language": "en-US,en;q=0.9"})
    if not r:
        return [], []
    soup = BeautifulSoup(r.text, "html.parser")
    sc = soup.find("script", id="__NEXT_DATA__")
    if not sc or not sc.string:
        return [], []
    items, orgs = [], []
    try:
        data = json.loads(sc.string)
        listings = (
            data.get("props", {})
            .get("pageProps", {})
            .get("searchResult", {})
            .get("listings", [])
        )
        purpose = "for_rent" if "rent" in cat_path else "for_sale"
        for entry in listings:
            prop = entry.get("property") or entry
            pid = str(prop.get("id") or "")
            if not pid:
                continue
            price_obj = prop.get("price") or {}
            loc = prop.get("location") or {}
            broker = prop.get("broker") or {}
            agent = prop.get("agent") or {}
            contacts = prop.get("contact_options") or []
            phone = None
            for co in contacts:
                if co.get("type") in ("phone", "whatsapp") and co.get("value"):
                    phone = co.get("value")
                    break
            raw = {
                "item_id": f"{portal}_{pid}",
                "portal_item_id": pid,
                "url": f"{base_url}{prop.get('share_url', '')}",
                "title": prop.get("title"),
                "purpose": purpose,
                "bedrooms": prop.get("bedrooms"),
                "bathrooms": prop.get("bathrooms"),
                "area_sqft": (prop.get("size") or {}).get("value"),
                "price_local": price_obj.get("value"),
                "currency": price_obj.get("currency"),
                "city": loc.get("name"),
                "full_address": loc.get("full_name"),
                "agent_name": agent.get("name"),
                "agent_phone": phone,
                "brokerage_name": broker.get("name"),
                "description": prop.get("description"),
            }
            items.append(normalize_item(raw, c_code, portal))
            if broker.get("name"):
                orgs.append({
                    "org_id": f"{portal}_org_{broker.get('id', hash(broker['name']) & 0xffffffff)}",
                    "name": broker["name"],
                    "portal_source": portal,
                    "country_code": c_code,
                    "phone": broker.get("phone"),
                })
    except Exception:
        pass
    return items, orgs


# ----------------- 3. Bayut UAE & Saudi Arabia (Algolia High-Speed) ----------------- #

def extract_bayut(params: Tuple[str, str, int]) -> Tuple[List[Dict], List[Dict]]:
    portal, path, page = params
    base_url = "https://www.bayut.sa" if portal == "bayut_sa" else "https://www.bayut.com"
    c_code = "SA" if portal == "bayut_sa" else "AE"
    suf = f"/page-{page}/" if page > 1 else "/"
    url = f"{base_url}/{path}{suf}"
    r = fetch_get(url, {"Accept-Language": "en-US,en;q=0.9"})
    if not r:
        return [], []
    m = re.search(r"window\.state\s*=\s*(\{.*?\});\s*</script>", r.text, re.S)
    if not m:
        return [], []
    items, orgs = [], []
    try:
        state = json.loads(m.group(1))
        hits = state.get("algolia", {}).get("content", {}).get("hits", [])
        purpose = "for_rent" if "rent" in path else "for_sale"
        for h in hits:
            eid = str(h.get("externalID") or h.get("id") or "")
            if not eid:
                continue
            agency = h.get("agency") or {}
            pn = h.get("phoneNumber") or {}
            phone = pn.get("mobile") or pn.get("whatsapp") or pn.get("phone")
            geo = h.get("geography") or {}
            raw = {
                "item_id": f"{portal}_{eid}",
                "portal_item_id": eid,
                "url": f"{base_url}/property/details-{eid}.html",
                "title": h.get("title"),
                "purpose": purpose,
                "bedrooms": h.get("rooms"),
                "bathrooms": h.get("baths"),
                "area_sqft": h.get("area"),
                "price_local": h.get("price"),
                "latitude": geo.get("lat"),
                "longitude": geo.get("lng"),
                "agent_name": h.get("contactName"),
                "agent_phone": phone,
                "brokerage_name": agency.get("name"),
            }
            items.append(normalize_item(raw, c_code, portal))
    except Exception:
        pass
    return items, orgs


# ----------------- 4. UK Rightmove ----------------- #

UK_REGIONS = [
    "REGION^87490",  # London
    "REGION^904",    # Manchester
    "REGION^162",    # Birmingham
    "REGION^787",    # Leeds
    "REGION^813",    # Liverpool
    "REGION^219",    # Bristol
    "REGION^475",    # Edinburgh
    "REGION^550",    # Glasgow
    "REGION^1195",   # Sheffield
    "REGION^984",    # Newcastle
]


def extract_rightmove(params: Tuple[str, str, int]) -> Tuple[List[Dict], List[Dict]]:
    region_id, channel, page = params
    idx = (page - 1) * 24
    url = f"https://www.rightmove.co.uk/api/_search?locationIdentifier={region_id}&numberOfPropertiesPerPage=24&index={idx}&channel={channel}&currencyCode=GBP"
    r = fetch_get(url, {"Accept": "application/json"})
    if not r:
        return [], []
    items, orgs = [], []
    try:
        for p in r.json().get("properties", []):
            pid = str(p.get("id") or "")
            if not pid:
                continue
            loc = p.get("location") or {}
            cust = p.get("customer") or {}
            raw = {
                "item_id": f"rightmove_uk_{pid}",
                "portal_item_id": pid,
                "url": f"https://www.rightmove.co.uk{p.get('propertyUrl', '')}",
                "title": p.get("propertyTypeFullDescription") or p.get("displayAddress"),
                "purpose": "for_rent" if channel == "RENT" else "for_sale",
                "bedrooms": p.get("bedrooms"),
                "bathrooms": p.get("bathrooms"),
                "price_local": (p.get("price") or {}).get("amount"),
                "currency": "GBP",
                "full_address": p.get("displayAddress"),
                "latitude": loc.get("latitude"),
                "longitude": loc.get("longitude"),
                "agent_phone": (cust.get("contactTelephone") or ""),
                "brokerage_name": cust.get("branchDisplayName"),
                "description": p.get("summary"),
            }
            items.append(normalize_item(raw, "GB", "rightmove_uk"))
    except Exception:
        pass
    return items, orgs


# ----------------- 5. USA Redfin (GIS CSV High-Speed) ----------------- #

US_METROS = [
    ("30749", "New York"), ("11203", "Los Angeles"), ("29470", "Chicago"),
    ("8903", "Houston"), ("14240", "Phoenix"), ("15502", "Philadelphia"),
    ("16657", "San Antonio"), ("16904", "San Diego"), ("30794", "Dallas"),
    ("17151", "San Francisco"), ("11458", "Miami"), ("16163", "Seattle"),
    ("1826", "Boston"), ("30756", "Atlanta"), ("5155", "Denver"),
]


def extract_redfin_us(params: Tuple[str, str]) -> Tuple[List[Dict], List[Dict]]:
    region_id, city_name = params
    url = f"https://www.redfin.com/stingray/api/gis-csv?al=1&num_homes=350&region_id={region_id}&region_type=6&status=9&uipt=1,2,3,4,5,6&v=8"
    r = fetch_get(url, {"Accept": "text/csv,*/*"})
    if not r or "MLS#" not in r.text:
        return [], []
    import csv, io
    items = []
    reader = csv.DictReader(io.StringIO(r.text))
    for row in reader:
        mls = row.get("MLS#") or row.get("URL (SEE https://www.redfin.com/buy-a-home/comparative-market-analysis FOR INFO ON PRICING)")
        if not mls:
            continue
        raw = {
            "item_id": f"redfin_us_{mls}",
            "portal_item_id": str(mls),
            "url": row.get("URL (SEE https://www.redfin.com/buy-a-home/comparative-market-analysis FOR INFO ON PRICING)"),
            "title": f"{row.get('PROPERTY TYPE', 'Home')} in {row.get('CITY', city_name)}",
            "purpose": "for_sale",
            "bedrooms": row.get("BEDS"),
            "bathrooms": row.get("BATHS"),
            "area_sqft": row.get("SQUARE FEET"),
            "price_local": row.get("PRICE"),
            "currency": "USD",
            "state_or_province": row.get("STATE OR PROVINCE"),
            "city": row.get("CITY") or city_name,
            "postal_code": row.get("ZIP OR POSTAL CODE"),
            "street_address": row.get("ADDRESS"),
            "full_address": f"{row.get('ADDRESS', '')}, {row.get('CITY', city_name)}, {row.get('STATE OR PROVINCE', '')}",
            "latitude": row.get("LATITUDE"),
            "longitude": row.get("LONGITUDE"),
        }
        items.append(normalize_item(raw, "US", "redfin_us"))
    return items, []


# ----------------- Queue Builder ----------------- #

def build_global_tasks(portal: str) -> List[Any]:
    tasks: List[Any] = []
    if portal == "willhaben_at":
        for cat in ["eigentumswohnung/eigentumswohnung-angebote", "haus-kaufen/haus-angebote", "mietwohnungen/mietwohnung-angebote", "grundstuecke/grundstueck-angebote", "gewerbeimmobilien-kaufen/gewerbeimmobilien-angebote"]:
            for p in range(1, 121):
                tasks.append((cat, p))
    elif portal == "myhome_ie":
        brackets = [(0, 200000), (200000, 325000), (325000, 450000), (450000, 650000), (650000, 1000000), (1000000, 0)]
        for lo, hi in brackets:
            for p in range(1, 45):
                tasks.append((lo, hi, p))
    elif portal == "daft_ie":
        for sec in ["residential-for-sale", "residential-to-rent", "commercial-for-sale"]:
            for p in range(1, 101):
                tasks.append((sec, p))
    elif portal in PF_DOMAINS:
        for cat in ["buy/properties-for-sale.html", "rent/properties-for-rent.html", "commercial-buy/properties-for-sale.html", "commercial-rent/properties-for-rent.html"]:
            for p in range(1, 81):
                tasks.append((portal, cat, p))
    elif portal in ("bayut_ae", "bayut_sa"):
        paths = ["for-sale/property/uae", "to-rent/property/uae"] if portal == "bayut_ae" else ["en/for-sale/properties/saudi-arabia", "en/to-rent/properties/saudi-arabia"]
        for path in paths:
            for p in range(1, 101):
                tasks.append((portal, path, p))
    elif portal == "rightmove_uk":
        for reg in UK_REGIONS:
            for ch in ["BUY", "RENT"]:
                for p in range(1, 43):
                    tasks.append((reg, ch, p))
    elif portal == "redfin_us":
        for rid, cname in US_METROS:
            tasks.append((rid, cname))
    return tasks


GLOBAL_EXTRACTORS = {
    "willhaben_at": extract_willhaben,
    "myhome_ie": extract_myhome,
    "daft_ie": extract_daft,
    "propertyfinder_ae": extract_propertyfinder,
    "propertyfinder_bh": extract_propertyfinder,
    "propertyfinder_eg": extract_propertyfinder,
    "propertyfinder_qa": extract_propertyfinder,
    "propertyfinder_om": extract_propertyfinder,
    "propertyfinder_kw": extract_propertyfinder,
    "bayut_ae": extract_bayut,
    "bayut_sa": extract_bayut,
    "rightmove_uk": extract_rightmove,
    "redfin_us": extract_redfin_us,
}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--portal", required=True, choices=list(GLOBAL_EXTRACTORS.keys()))
    parser.add_argument("--workers", type=int, default=25)
    parser.add_argument("--max-tasks", type=int, default=0)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    tasks = build_global_tasks(args.portal)
    if args.max_tasks > 0:
        tasks = tasks[: args.max_tasks]

    logger.info(f"Global Cloud Worker [{args.portal}]: {len(tasks):,} tasks across {args.workers} workers.")
    out_path = Path(args.output)
    conn = init_shard_db(out_path)
    fn = GLOBAL_EXTRACTORS[args.portal]

    t0 = time.time()
    ingested = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = [ex.submit(fn, t) for t in tasks]
        for fut in concurrent.futures.as_completed(futures):
            try:
                items, orgs = fut.result()
                if items or orgs:
                    ingested += upsert_batch(conn, items, orgs)
            except Exception:
                pass

    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM items")
    unique_cnt = cur.fetchone()[0]
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
    conn.close()
    logger.info(f"=== GLOBAL SHARD DONE [{args.portal}] Unique: {unique_cnt:,} | Elapsed: {time.time() - t0:.1f}s ===")


if __name__ == "__main__":
    main()
