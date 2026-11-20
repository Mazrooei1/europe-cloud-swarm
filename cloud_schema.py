"""
Cloud-Native SQLite WAL Schema, E.164 Contact Normalizer, and Thread-Safe Batch Upserter.
1-to-1 Schema Parity with Master europe_master.sqlite (38 columns in items + orgs + agents + audit_logs).
"""

import csv
import hashlib
import json
import re
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List, Optional

DB_LOCK = threading.Lock()

# All 109 Countries: ISO-2 -> (Country Name, Currency, E.164 Prefix, Approx FX to USD)
COUNTRY_META = {
    # MENA (16)
    "AE": ("United Arab Emirates", "AED", "+971", 0.272),
    "SA": ("Saudi Arabia", "SAR", "+966", 0.266),
    "QA": ("Qatar", "QAR", "+974", 0.275),
    "KW": ("Kuwait", "KWD", "+965", 3.25),
    "BH": ("Bahrain", "BHD", "+973", 2.65),
    "OM": ("Oman", "OMR", "+968", 2.60),
    "EG": ("Egypt", "EGP", "+20", 0.020),
    "JO": ("Jordan", "JOD", "+962", 1.41),
    "LB": ("Lebanon", "LBP", "+961", 0.000011),
    "IQ": ("Iraq", "IQD", "+964", 0.00076),
    "MA": ("Morocco", "MAD", "+212", 0.10),
    "TN": ("Tunisia", "TND", "+216", 0.32),
    "DZ": ("Algeria", "DZD", "+213", 0.0074),
    "IL": ("Israel", "ILS", "+972", 0.27),
    "TR": ("Turkey", "TRY", "+90", 0.029),
    "IR": ("Iran", "IRR", "+98", 0.000024),
    # North America (3)
    "US": ("United States", "USD", "+1", 1.00),
    "CA": ("Canada", "CAD", "+1", 0.72),
    "MX": ("Mexico", "MXN", "+52", 0.050),
    # Europe (34)
    "GB": ("United Kingdom", "GBP", "+44", 1.27),
    "IE": ("Ireland", "EUR", "+353", 1.06),
    "FR": ("France", "EUR", "+33", 1.06),
    "DE": ("Germany", "EUR", "+49", 1.06),
    "ES": ("Spain", "EUR", "+34", 1.06),
    "PT": ("Portugal", "EUR", "+351", 1.06),
    "IT": ("Italy", "EUR", "+39", 1.06),
    "NL": ("Netherlands", "EUR", "+31", 1.06),
    "BE": ("Belgium", "EUR", "+32", 1.06),
    "CH": ("Switzerland", "CHF", "+41", 1.13),
    "AT": ("Austria", "EUR", "+43", 1.06),
    "PL": ("Poland", "PLN", "+48", 0.25),
    "SE": ("Sweden", "SEK", "+46", 0.092),
    "NO": ("Norway", "NOK", "+47", 0.091),
    "DK": ("Denmark", "DKK", "+45", 0.142),
    "FI": ("Finland", "EUR", "+358", 1.06),
    "GR": ("Greece", "EUR", "+30", 1.06),
    "RO": ("Romania", "RON", "+40", 0.213),
    "UA": ("Ukraine", "UAH", "+380", 0.024),
    "RU": ("Russia", "RUB", "+7", 0.010),
    "CZ": ("Czech Republic", "CZK", "+420", 0.042),
    "HU": ("Hungary", "HUF", "+36", 0.0026),
    "BG": ("Bulgaria", "BGN", "+359", 0.54),
    "HR": ("Croatia", "EUR", "+385", 1.06),
    "RS": ("Serbia", "RSD", "+381", 0.0091),
    "SK": ("Slovakia", "EUR", "+421", 1.06),
    "SI": ("Slovenia", "EUR", "+386", 1.06),
    "EE": ("Estonia", "EUR", "+372", 1.06),
    "LV": ("Latvia", "EUR", "+371", 1.06),
    "LT": ("Lithuania", "EUR", "+370", 1.06),
    "CY": ("Cyprus", "EUR", "+357", 1.06),
    "MT": ("Malta", "EUR", "+356", 1.06),
    "LU": ("Luxembourg", "EUR", "+352", 1.06),
    "IS": ("Iceland", "ISK", "+354", 0.0072),
    # Asia-Pacific (24)
    "IN": ("India", "INR", "+91", 0.0119),
    "PK": ("Pakistan", "PKR", "+92", 0.0036),
    "BD": ("Bangladesh", "BDT", "+880", 0.0084),
    "LK": ("Sri Lanka", "LKR", "+94", 0.0034),
    "NP": ("Nepal", "NPR", "+977", 0.0074),
    "CN": ("China", "CNY", "+86", 0.138),
    "JP": ("Japan", "JPY", "+81", 0.0065),
    "KR": ("South Korea", "KRW", "+82", 0.00072),
    "TW": ("Taiwan", "TWD", "+886", 0.031),
    "HK": ("Hong Kong", "HKD", "+852", 0.128),
    "SG": ("Singapore", "SGD", "+65", 0.745),
    "MY": ("Malaysia", "MYR", "+60", 0.225),
    "ID": ("Indonesia", "IDR", "+62", 0.000063),
    "TH": ("Thailand", "THB", "+66", 0.029),
    "VN": ("Vietnam", "VND", "+84", 0.000039),
    "PH": ("Philippines", "PHP", "+63", 0.017),
    "KH": ("Cambodia", "KHR", "+855", 0.000245),
    "AU": ("Australia", "AUD", "+61", 0.65),
    "NZ": ("New Zealand", "NZD", "+64", 0.59),
    "KZ": ("Kazakhstan", "KZT", "+7", 0.0020),
    "UZ": ("Uzbekistan", "UZS", "+998", 0.000078),
    "GE": ("Georgia", "GEL", "+995", 0.365),
    "AZ": ("Azerbaijan", "AZN", "+994", 0.588),
    "AM": ("Armenia", "AMD", "+374", 0.00255),
    # Latin America (14)
    "BR": ("Brazil", "BRL", "+55", 0.173),
    "AR": ("Argentina", "ARS", "+54", 0.0010),
    "CO": ("Colombia", "COP", "+57", 0.00023),
    "CL": ("Chile", "CLP", "+56", 0.00103),
    "PE": ("Peru", "PEN", "+51", 0.265),
    "EC": ("Ecuador", "USD", "+593", 1.00),
    "UY": ("Uruguay", "UYU", "+598", 0.0235),
    "PY": ("Paraguay", "PYG", "+595", 0.000128),
    "BO": ("Bolivia", "BOB", "+591", 0.145),
    "VE": ("Venezuela", "VES", "+58", 0.022),
    "CR": ("Costa Rica", "CRC", "+506", 0.00195),
    "PA": ("Panama", "USD", "+507", 1.00),
    "DO": ("Dominican Republic", "DOP", "+1", 0.0166),
    "GT": ("Guatemala", "GTQ", "+502", 0.129),
    # Africa (18)
    "ZA": ("South Africa", "ZAR", "+27", 0.055),
    "NG": ("Nigeria", "NGN", "+234", 0.00060),
    "KE": ("Kenya", "KES", "+254", 0.0077),
    "GH": ("Ghana", "GHS", "+233", 0.062),
    "UG": ("Uganda", "UGX", "+256", 0.00027),
    "TZ": ("Tanzania", "TZS", "+255", 0.00037),
    "ET": ("Ethiopia", "ETB", "+251", 0.0080),
    "SN": ("Senegal", "XOF", "+221", 0.00162),
    "CI": ("Ivory Coast", "XOF", "+225", 0.00162),
    "CM": ("Cameroon", "XAF", "+237", 0.00162),
    "MU": ("Mauritius", "MUR", "+230", 0.0215),
    "NA": ("Namibia", "NAD", "+264", 0.055),
    "BW": ("Botswana", "BWP", "+267", 0.073),
    "ZW": ("Zimbabwe", "USD", "+263", 1.00),
    "ZM": ("Zambia", "ZMW", "+260", 0.037),
    "RW": ("Rwanda", "RWF", "+250", 0.00073),
    "MZ": ("Mozambique", "MZN", "+258", 0.0156),
    "AO": ("Angola", "AOA", "+244", 0.0011),
}

COUNTRY_NAME_TO_CODE = {v[0].lower(): k for k, v in COUNTRY_META.items()}

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
    conn.execute("PRAGMA cache_size=-64000;")
    conn.execute("PRAGMA temp_store=MEMORY;")
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


def md5_file(path: Path) -> str:
    h = hashlib.md5()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def export_shard_deliverables(
    db_path: Path,
    out_dir: Path,
    portal_domain: str,
    shard_index: int,
    extra_meta: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Exports a finalized SQLite shard into uncompressed .csv, .jsonl, and
    extraction_audit_summary.json with bitwise MD5 checksums.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    safe_portal = re.sub(r"[^a-zA-Z0-9_.-]+", "_", portal_domain.lower())
    stem = f"{safe_portal}_shard_{shard_index:04d}"

    # Checkpoint WAL into main .sqlite file before hashing
    conn = sqlite3.connect(str(db_path), timeout=30)
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    csv_path = out_dir / f"{stem}.csv"
    jsonl_path = out_dir / f"{stem}.jsonl"
    summary_path = out_dir / "extraction_audit_summary.json"

    total_rows = 0
    with_phone = 0
    category_counts: Dict[str, int] = {}

    cols_sql = ", ".join(ITEM_COLS)
    cur.execute(f"SELECT {cols_sql} FROM items")

    with csv_path.open("w", encoding="utf-8", newline="") as f_csv, jsonl_path.open("w", encoding="utf-8") as f_jsonl:
        writer = csv.writer(f_csv)
        writer.writerow(ITEM_COLS)
        while True:
            rows = cur.fetchmany(5000)
            if not rows:
                break
            for row in rows:
                vals = [row[c] for c in ITEM_COLS]
                writer.writerow(vals)
                rec = dict(zip(ITEM_COLS, vals))
                f_jsonl.write(json.dumps(rec, ensure_ascii=False) + "\n")
                total_rows += 1
                if rec.get("agent_phone"):
                    with_phone += 1
                cat = rec.get("category") or "uncategorized"
                category_counts[cat] = category_counts.get(cat, 0) + 1

    conn.close()

    # Copy/move sqlite if not already inside out_dir
    target_sqlite = out_dir / f"{stem}.sqlite"
    if db_path.resolve() != target_sqlite.resolve():
        import shutil
        shutil.copy2(db_path, target_sqlite)

    contact_pct = round((with_phone / total_rows) * 100.0, 2) if total_rows else 0.0
    summary = {
        "portal_domain": portal_domain,
        "shard_index": shard_index,
        "total_listings": total_rows,
        "with_phone_e164": with_phone,
        "contact_coverage_pct": contact_pct,
        "category_breakdown": category_counts,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "files": {
            "sqlite": {"name": target_sqlite.name, "bytes": target_sqlite.stat().st_size, "md5": md5_file(target_sqlite)},
            "csv": {"name": csv_path.name, "bytes": csv_path.stat().st_size, "md5": md5_file(csv_path)},
            "jsonl": {"name": jsonl_path.name, "bytes": jsonl_path.stat().st_size, "md5": md5_file(jsonl_path)},
        },
    }
    if extra_meta:
        summary["meta"] = extra_meta

    summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    return summary

