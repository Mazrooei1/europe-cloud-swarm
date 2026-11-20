"""
Cloud-Native SQLite WAL Schema, E.164 Contact Normalizer, and Thread-Safe Batch Upserter.
1-to-1 Schema Parity with Master europe_master.sqlite (38 columns in items + orgs + agents + audit_logs).
"""

import re
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List, Optional

DB_LOCK = threading.Lock()

COUNTRY_META = {
    "ES": ("Spain", "EUR", "+34", 1.06),
    "IT": ("Italy", "EUR", "+39", 1.06),
    "DE": ("Germany", "EUR", "+49", 1.06),
    "PL": ("Poland", "PLN", "+48", 0.25),
    "PT": ("Portugal", "EUR", "+351", 1.06),
    "NO": ("Norway", "NOK", "+47", 0.091),
    "NL": ("Netherlands", "EUR", "+31", 1.06),
    "AT": ("Austria", "EUR", "+43", 1.06),
    "IE": ("Ireland", "EUR", "+353", 1.06),
}

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS items (
    item_id TEXT PRIMARY KEY,
    portal_source TEXT NOT NULL,
    portal_item_id TEXT NOT NULL,
    title TEXT,
    purpose TEXT DEFAULT 'for_sale',
    category TEXT DEFAULT 'residential',
    unit_subtype TEXT,
    bedrooms REAL,
    bathrooms REAL,
    area_sqft REAL,
    area_sqm REAL,
    price_local REAL,
    currency TEXT NOT NULL,
    price_usd REAL,
    price_per_sqft REAL,
    rent_frequency TEXT,
    property_status TEXT DEFAULT 'active',
    country TEXT NOT NULL,
    country_code TEXT NOT NULL,
    state_or_province TEXT,
    city TEXT,
    county_or_borough TEXT,
    neighborhood TEXT,
    postal_code TEXT,
    street_address TEXT,
    full_address TEXT,
    latitude REAL,
    longitude REAL,
    photos_count INTEGER DEFAULT 0,
    primary_photo_url TEXT,
    photos_json TEXT,
    description TEXT,
    url TEXT,
    agent_name TEXT,
    agent_phone TEXT,
    brokerage_name TEXT,
    date_posted TEXT,
    scraped_at_utc TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS organizations (
    org_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    portal_source TEXT,
    country_code TEXT,
    phone TEXT,
    address TEXT,
    website TEXT,
    created_at_utc TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sellers_or_agents (
    agent_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    portal_source TEXT,
    country_code TEXT,
    phone TEXT,
    email TEXT,
    brokerage_name TEXT,
    org_id TEXT,
    created_at_utc TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS audit_logs (
    log_id INTEGER PRIMARY KEY AUTOINCREMENT,
    portal_source TEXT NOT NULL,
    country_code TEXT NOT NULL,
    items_extracted INTEGER,
    status TEXT,
    details TEXT,
    timestamp_utc TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_items_country ON items(country_code);
CREATE INDEX IF NOT EXISTS idx_items_portal ON items(portal_source);
CREATE INDEX IF NOT EXISTS idx_items_city ON items(city);
CREATE INDEX IF NOT EXISTS idx_items_purpose ON items(purpose);
CREATE INDEX IF NOT EXISTS idx_items_category ON items(category);
CREATE INDEX IF NOT EXISTS idx_items_price_usd ON items(price_usd);
CREATE INDEX IF NOT EXISTS idx_items_bedrooms ON items(bedrooms);
"""

ITEM_COLS = [
    "item_id", "portal_source", "portal_item_id", "title", "purpose", "category",
    "unit_subtype", "bedrooms", "bathrooms", "area_sqft", "area_sqm", "price_local",
    "currency", "price_usd", "price_per_sqft", "rent_frequency", "property_status",
    "country", "country_code", "state_or_province", "city", "county_or_borough",
    "neighborhood", "postal_code", "street_address", "full_address", "latitude",
    "longitude", "photos_count", "primary_photo_url", "photos_json", "description",
    "url", "agent_name", "agent_phone", "brokerage_name", "date_posted", "scraped_at_utc"
]


def init_shard_db(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), timeout=30, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.executescript(SCHEMA_SQL)
    conn.commit()
    return conn


def to_float(val: Any) -> Optional[float]:
    if val is None or val == "":
        return None
    if isinstance(val, (int, float)):
        return float(val)
    s = re.sub(r"[^\d\.\,]", "", str(val))
    if not s:
        return None
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        parts = s.split(",")
        if len(parts[-1]) == 3:
            s = s.replace(",", "")
        else:
            s = s.replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


def normalize_phone_e164(phone: Optional[str], country_code: str) -> Optional[str]:
    if not phone:
        return None
    raw = str(phone).strip()
    digits = re.sub(r"[^\d+]", "", raw)
    if not digits or len(re.sub(r"\D", "", digits)) < 7:
        return None
    if digits.startswith("00"):
        digits = "+" + digits[2:]
    if digits.startswith("+"):
        return digits
    prefix = COUNTRY_META.get(country_code, ("", "", "+34", 1.0))[2]
    digits = digits.lstrip("0")
    return f"{prefix}{digits}"


def normalize_item(raw: Dict[str, Any], country_code: str, portal_source: str) -> Dict[str, Any]:
    c_name, default_curr, _, fx = COUNTRY_META.get(country_code, ("Europe", "EUR", "+34", 1.06))
    price_local = to_float(raw.get("price_local"))
    price_usd = round(price_local * fx, 2) if price_local is not None else None
    area_sqm = to_float(raw.get("area_sqm"))
    area_sqft = round(area_sqm * 10.7639, 2) if area_sqm is not None else to_float(raw.get("area_sqft"))
    price_per_sqft = round(price_usd / area_sqft, 2) if (price_usd and area_sqft and area_sqft > 0) else None

    phone = normalize_phone_e164(raw.get("agent_phone"), country_code)
    if not phone and raw.get("description"):
        m = re.search(r"(\+?\d[\d\s\-]{8,15}\d)", str(raw.get("description")))
        if m:
            phone = normalize_phone_e164(m.group(1), country_code)

    now_utc = datetime.now(timezone.utc).isoformat()
    norm = {
        "item_id": str(raw.get("item_id") or f"{portal_source}_{raw.get('portal_item_id')}"),
        "portal_source": portal_source,
        "portal_item_id": str(raw.get("portal_item_id", "")),
        "title": raw.get("title"),
        "purpose": raw.get("purpose") or "for_sale",
        "category": raw.get("category") or "residential",
        "unit_subtype": raw.get("unit_subtype"),
        "bedrooms": to_float(raw.get("bedrooms")),
        "bathrooms": to_float(raw.get("bathrooms")),
        "area_sqft": area_sqft,
        "area_sqm": area_sqm,
        "price_local": price_local,
        "currency": raw.get("currency") or default_curr,
        "price_usd": price_usd,
        "price_per_sqft": price_per_sqft,
        "rent_frequency": raw.get("rent_frequency"),
        "property_status": raw.get("property_status") or "active",
        "country": raw.get("country") or c_name,
        "country_code": country_code,
        "state_or_province": raw.get("state_or_province"),
        "city": raw.get("city"),
        "county_or_borough": raw.get("county_or_borough"),
        "neighborhood": raw.get("neighborhood"),
        "postal_code": raw.get("postal_code"),
        "street_address": raw.get("street_address"),
        "full_address": raw.get("full_address"),
        "latitude": to_float(raw.get("latitude")),
        "longitude": to_float(raw.get("longitude")),
        "photos_count": int(raw.get("photos_count") or 0),
        "primary_photo_url": raw.get("primary_photo_url"),
        "photos_json": raw.get("photos_json"),
        "description": raw.get("description"),
        "url": raw.get("url"),
        "agent_name": raw.get("agent_name"),
        "agent_phone": phone,
        "brokerage_name": raw.get("brokerage_name"),
        "date_posted": raw.get("date_posted"),
        "scraped_at_utc": raw.get("scraped_at_utc") or now_utc,
    }
    return norm


def upsert_batch(conn: sqlite3.Connection, items: List[Dict[str, Any]], orgs: List[Dict[str, Any]]) -> int:
    if not items and not orgs:
        return 0
    now_utc = datetime.now(timezone.utc).isoformat()
    placeholders = ", ".join([f":{c}" for c in ITEM_COLS])
    cols_str = ", ".join(ITEM_COLS)
    update_str = ", ".join([f"{c}=excluded.{c}" for c in ITEM_COLS if c != "item_id"])
    sql_items = f"INSERT INTO items ({cols_str}) VALUES ({placeholders}) ON CONFLICT(item_id) DO UPDATE SET {update_str}"

    sql_orgs = """
        INSERT INTO organizations (org_id, name, portal_source, country_code, phone, address, website, created_at_utc)
        VALUES (:org_id, :name, :portal_source, :country_code, :phone, :address, :website, :created_at_utc)
        ON CONFLICT(org_id) DO UPDATE SET name=excluded.name, phone=COALESCE(excluded.phone, organizations.phone)
    """
    for o in orgs:
        o.setdefault("created_at_utc", now_utc)
        o.setdefault("phone", None)
        o.setdefault("address", None)
        o.setdefault("website", None)

    with DB_LOCK:
        cur = conn.cursor()
        if items:
            cur.executemany(sql_items, items)
        if orgs:
            cur.executemany(sql_orgs, orgs)
        conn.commit()
    return len(items)
