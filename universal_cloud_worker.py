"""
Universal Resumable 5-Hour Checkpointed Cloud Worker (327-Portal Global Swarm).
- Full detail-page extraction + E.164 phone/email contact intelligence
- All classifieds categories (Real Estate, Autos, Jobs, Goods, Services)
- 32 Concurrent curl_cffi HTTP/2 TLS Workers (threading.local session pooling)
- Zero-DOM streaming regex deserialization (XML sitemaps, JSON-LD, __NEXT_DATA__, OpenGraph)
- SQLite WAL mode with 5,000-row parameterized executemany batches
- Resumable checkpointing + 100k-record shard rotation -> uncompressed .sqlite/.csv/.jsonl/audit.json
  uploaded directly to Google Drive via rclone --checksum + rclone check (zero local disk bloat).
"""

import argparse
import concurrent.futures
import gzip
import hashlib
import json
import logging
import random
import re
import shutil
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import urlparse

from curl_cffi import requests

from cloud_schema import (
    COUNTRY_META,
    COUNTRY_NAME_TO_CODE,
    export_shard_deliverables,
    init_shard_db,
    normalize_item,
    normalize_phone_e164,
    upsert_batch,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("universal_cloud_worker")

TLS_PROFILES = ["chrome124", "chrome120", "safari15_5"]
thread_local = threading.local()

# Compiled zero-DOM streaming regexes
LOC_RE = re.compile(r"<loc>\s*([^<\s]+)\s*</loc>", re.I)
ROBOTS_SITEMAP_RE = re.compile(r"^\s*Sitemap:\s*(\S+)", re.I | re.M)
JSON_LD_RE = re.compile(
    r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
    re.I | re.S,
)
NEXT_DATA_RE = re.compile(
    r'<script[^>]+id=["\']__NEXT_DATA__["\'][^>]*>(.*?)</script>',
    re.I | re.S,
)
TITLE_RE = re.compile(r"<title[^>]*>(.*?)</title>", re.I | re.S)
META_RE = re.compile(
    r'<meta[^>]+(?:property|name)=["\']([^"\']+)["\'][^>]+content=["\']([^"\']*)["\']',
    re.I,
)
TEL_HREF_RE = re.compile(r'href=["\']tel:([^"\']+)["\']', re.I)
WA_HREF_RE = re.compile(r"(?:wa\.me/|whatsapp\.com/send\?phone=)(\+?\d{8,15})", re.I)
PHONE_TEXT_RE = re.compile(r"(?:\+|00)\d{1,3}[\s\-.\(\)]?\d{2,4}[\s\-.\(\)]?\d{3,4}[\s\-.\(\)]?\d{2,4}")
EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")
PRICE_RE = re.compile(
    r"(?:USD|EUR|GBP|AED|SAR|CAD|AUD|INR|BRL|MXN|PLN|CHF|JPY|CNY|TRY|ZAR|\$|€|£)\s*([0-9][0-9,\.]{2,14})",
    re.I,
)

# Skip obvious non-listing boilerplate URLs while retaining ALL classifieds categories
SKIP_URL_RE = re.compile(
    r"(?:/(?:login|signin|signup|register|privacy|terms|cookies|about-us|contact-us|help|faq|blog|news|press|careers|sitemap|wp-admin|wp-content|static|assets)/|\.(?:jpg|jpeg|png|gif|webp|svg|pdf|css|js|ico|woff2?)(?:\?|$))",
    re.I,
)

SKIP_SITEMAP_RE = re.compile(
    r"(?:blog|news|article|editorial|guide|static|cms|page-sitemap|author|tag|image-sitemap|video-sitemap)",
    re.I,
)


def get_session() -> requests.Session:
    if not hasattr(thread_local, "session"):
        prof = random.choice(TLS_PROFILES)
        thread_local.session = requests.Session(impersonate=prof)
        thread_local.profile = prof
    return thread_local.session


def fetch_url(url: str, timeout: int = 12, max_retries: int = 2) -> Optional[Tuple[int, str]]:
    s = get_session()
    for attempt in range(max_retries):
        try:
            r = s.get(
                url,
                timeout=timeout,
                allow_redirects=True,
                headers={
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.9",
                },
            )
            if r.status_code == 200:
                content = r.content
                if content[:2] == b"\x1f\x8b":
                    text = gzip.decompress(content).decode("utf-8", errors="ignore")
                else:
                    text = r.text or ""
                return 200, text
            if r.status_code in (429, 503):
                time.sleep((1.5 ** attempt) + random.uniform(0.2, 0.8))
                prof = random.choice(TLS_PROFILES)
                thread_local.session = requests.Session(impersonate=prof)
                thread_local.profile = prof
                s = thread_local.session
                continue
            return r.status_code, ""
        except Exception:
            if attempt + 1 < max_retries:
                time.sleep(0.4)
    return None


def classify_category(url: str, title: str, ld_type: str) -> Tuple[str, str]:
    """Classifies across ALL classifieds verticals: Real Estate, Autos, Jobs, Goods, Services."""
    blob = f"{ld_type} {url} {title}".lower()
    purpose = "for_rent" if any(k in blob for k in ("rent", "lease", "alquiler", "location", "miete", "affitto", "to-let")) else "for_sale"

    if any(k in blob for k in ("vehicle", "car", "auto", "motor", "truck", "suv", "bike", "motorcycle", "coche", "voiture", "fahrzeuge")):
        return "vehicles", purpose
    if any(k in blob for k in ("jobposting", "/jobs", "/job/", "empleo", "emploi", "vacanc", "hiring", "salary")):
        return "jobs", "hiring"
    if any(k in blob for k in ("commercial", "office", "warehouse", "retail", "shop", "bureaux", "oficina")):
        return "commercial", purpose
    if any(k in blob for k in ("land", "plot", "terreno", "terrain", "grundstueck")):
        return "land", purpose
    if any(k in blob for k in (
        "realestate", "residence", "apartment", "house", "villa", "condo", "flat",
        "property", "inmueble", "immobilier", "wohnung", "haus", "casa", "piso", "imovel", "bedroom",
    )):
        return "residential", purpose
    if any(k in blob for k in ("phone", "iphone", "laptop", "electronics", "computer", "tv", "camera", "console")):
        return "electronics", purpose
    if any(k in blob for k in ("service", "repair", "moving", "cleaning", "tutor")):
        return "services", "service"
    return "classifieds", purpose


def flatten_json_ld(obj: Any) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    if isinstance(obj, dict):
        if "@graph" in obj and isinstance(obj["@graph"], list):
            for g in obj["@graph"]:
                out.extend(flatten_json_ld(g))
        else:
            out.append(obj)
    elif isinstance(obj, list):
        for item in obj:
            out.extend(flatten_json_ld(item))
    return out


def extract_detail_record(url: str, html: str, domain: str, country_code: str) -> Optional[Dict[str, Any]]:
    if not html or len(html) < 400:
        return None

    # Meta tags
    metas: Dict[str, str] = {}
    for k, v in META_RE.findall(html[:35000]):
        kl = k.lower()
        if kl not in metas and v:
            metas[kl] = v.strip()

    title = metas.get("og:title") or metas.get("twitter:title")
    if not title:
        tm = TITLE_RE.search(html[:12000])
        if tm:
            title = re.sub(r"\s+", " ", tm.group(1)).strip()

    description = metas.get("og:description") or metas.get("description") or ""
    primary_photo = metas.get("og:image")
    price_local: Optional[float] = None
    currency: Optional[str] = None
    bedrooms = None
    bathrooms = None
    area_sqm = None
    city = None
    state = None
    postal = None
    street = None
    lat = None
    lon = None
    agent_name = None
    agent_phone = None
    brokerage = None
    date_posted = None
    ld_type = ""
    photos: List[str] = []

    # 1. Zero-DOM JSON-LD extraction
    for raw_ld in JSON_LD_RE.findall(html):
        raw_ld = raw_ld.strip()
        if not raw_ld:
            continue
        try:
            parsed = json.loads(raw_ld)
        except Exception:
            continue
        for node in flatten_json_ld(parsed):
            ntype = str(node.get("@type") or "")
            if isinstance(node.get("@type"), list):
                ntype = " ".join(str(x) for x in node["@type"])
            if not ntype or ntype.lower() in ("breadcrumblist", "website", "organization", "webpage"):
                # Still grab phone/name from Organization if missing
                if ntype.lower() == "organization":
                    if not brokerage and node.get("name"):
                        brokerage = str(node.get("name"))[:120]
                    if not agent_phone and node.get("telephone"):
                        agent_phone = str(node.get("telephone"))
                continue

            ld_type = ntype
            if node.get("name") and not title:
                title = str(node["name"])[:250]
            if node.get("description") and len(str(node["description"])) > len(description):
                description = str(node["description"])[:2500]
            if node.get("datePosted") and not date_posted:
                date_posted = str(node["datePosted"])[:40]
            if node.get("numberOfRooms") and bedrooms is None:
                bedrooms = node.get("numberOfRooms")
            if node.get("numberOfBedrooms") and bedrooms is None:
                bedrooms = node.get("numberOfBedrooms")
            if node.get("numberOfBathroomsTotal") and bathrooms is None:
                bathrooms = node.get("numberOfBathroomsTotal")
            if isinstance(node.get("floorSize"), dict) and area_sqm is None:
                area_sqm = node["floorSize"].get("value")

            # Offers / price
            offers = node.get("offers")
            if isinstance(offers, list) and offers:
                offers = offers[0]
            if isinstance(offers, dict):
                if offers.get("price") is not None and price_local is None:
                    price_local = offers.get("price")
                if offers.get("priceCurrency") and not currency:
                    currency = str(offers.get("priceCurrency"))[:8]
                seller = offers.get("seller")
                if isinstance(seller, dict):
                    if not agent_name and seller.get("name"):
                        agent_name = str(seller["name"])[:120]
                    if not agent_phone and seller.get("telephone"):
                        agent_phone = str(seller["telephone"])

            # Address & geo
            addr = node.get("address")
            if isinstance(addr, dict):
                city = city or addr.get("addressLocality")
                state = state or addr.get("addressRegion")
                postal = postal or addr.get("postalCode")
                street = street or addr.get("streetAddress")
            geo = node.get("geo")
            if isinstance(geo, dict):
                lat = lat if lat is not None else geo.get("latitude")
                lon = lon if lon is not None else geo.get("longitude")

            # Images
            img = node.get("image")
            if isinstance(img, str):
                photos.append(img)
            elif isinstance(img, list):
                for im in img[:15]:
                    if isinstance(im, str):
                        photos.append(im)
                    elif isinstance(im, dict) and im.get("url"):
                        photos.append(str(im["url"]))

            # Direct phone
            if not agent_phone and node.get("telephone"):
                agent_phone = str(node["telephone"])

    # 2. Contact Intelligence: tel: href, WhatsApp href, and international phone regex
    if not agent_phone:
        m_tel = TEL_HREF_RE.search(html)
        if m_tel:
            agent_phone = m_tel.group(1)
    if not agent_phone:
        m_wa = WA_HREF_RE.search(html)
        if m_wa:
            agent_phone = m_wa.group(1)
    if not agent_phone and description:
        m_ph = PHONE_TEXT_RE.search(description)
        if m_ph:
            agent_phone = m_ph.group(0)

    # Price fallback from title/meta/description
    if price_local is None:
        pm = PRICE_RE.search(f"{title or ''} {description[:300]}")
        if pm:
            price_local = pm.group(1)

    if not title and price_local is None and not description:
        return None

    category, purpose = classify_category(url, title or "", ld_type)
    portal_item_id = hashlib.md5(url.encode("utf-8")).hexdigest()[:16]

    if not primary_photo and photos:
        primary_photo = photos[0]

    raw_item = {
        "item_id": f"{domain}_{portal_item_id}",
        "portal_item_id": portal_item_id,
        "title": (title or "")[:300],
        "purpose": purpose,
        "category": category,
        "unit_subtype": ld_type[:80] if ld_type else None,
        "bedrooms": bedrooms,
        "bathrooms": bathrooms,
        "area_sqm": area_sqm,
        "price_local": price_local,
        "currency": currency,
        "city": str(city)[:100] if city else None,
        "state_or_province": str(state)[:100] if state else None,
        "postal_code": str(postal)[:30] if postal else None,
        "street_address": str(street)[:200] if street else None,
        "latitude": lat,
        "longitude": lon,
        "photos_count": len(photos) if photos else (1 if primary_photo else 0),
        "primary_photo_url": primary_photo,
        "photos_json": json.dumps(photos[:10]) if photos else None,
        "description": description[:2500] if description else None,
        "url": url,
        "agent_name": agent_name,
        "agent_phone": normalize_phone_e164(agent_phone, country_code),
        "brokerage_name": brokerage,
        "date_posted": date_posted,
    }
    return normalize_item(raw_item, country_code, domain)


def discover_child_sitemaps(domain: str, max_sitemaps: int = 50000) -> List[str]:
    """Discovers all child XML sitemaps for a domain via redirected origin + robots.txt & sitemap_index.xml."""
    roots: List[str] = []
    candidate_origins: List[str] = []

    # Resolve homepage redirect first to capture exact canonical origin (e.g. www. or regional subdomain)
    try:
        s = get_session()
        r = s.get(f"https://{domain}/", timeout=10, allow_redirects=True)
        m = re.match(r"^(https?://[^/]+)", str(r.url))
        if m:
            candidate_origins.append(m.group(1))
    except Exception:
        pass

    for fallback_base in (f"https://{domain}", f"https://www.{domain}"):
        if fallback_base not in candidate_origins:
            candidate_origins.append(fallback_base)

    origin = candidate_origins[0]
    for base in candidate_origins:
        res = fetch_url(f"{base}/robots.txt", timeout=10)
        if res and res[0] == 200:
            origin = base
            for sm in ROBOTS_SITEMAP_RE.findall(res[1]):
                sm = sm.strip()
                if sm.startswith("http") and sm not in roots:
                    roots.append(sm)
            if roots:
                break

    if not roots:
        for base in candidate_origins:
            for path in ("/sitemap_index.xml", "/sitemap.xml", "/sitemaps.xml"):
                res = fetch_url(f"{base}{path}", timeout=10)
                if res and res[0] == 200 and "<loc>" in res[1].lower():
                    roots.append(f"{base}{path}")
                    break
            if roots:
                break

    child_sitemaps: List[str] = []
    seen: Set[str] = set()
    queue = list(roots)

    while queue and len(child_sitemaps) < max_sitemaps:
        sm_url = queue.pop(0)
        if sm_url in seen or SKIP_SITEMAP_RE.search(sm_url):
            continue
        seen.add(sm_url)

        res = fetch_url(sm_url, timeout=14)
        if not res or res[0] != 200:
            continue
        body = res[1]
        if "<sitemapindex" in body[:2000].lower():
            for loc in LOC_RE.findall(body):
                if loc not in seen and not SKIP_SITEMAP_RE.search(loc):
                    queue.append(loc)
        elif "<urlset" in body[:2000].lower() or "<loc>" in body[:4000].lower():
            child_sitemaps.append(sm_url)

    return child_sitemaps


def rclone_cmd(args: List[str], check: bool = False, retries: int = 3) -> bool:
    for attempt in range(retries):
        try:
            proc = subprocess.run(["rclone"] + args, capture_output=True, text=True, timeout=900)
            if proc.returncode == 0:
                return True
            logger.warning("rclone %s failed (attempt %d/%d, rc=%d): %s",
                           " ".join(args[:3]), attempt + 1, retries, proc.returncode, proc.stderr[:300])
            if attempt + 1 < retries:
                time.sleep(5 * (attempt + 1))
            elif check:
                raise RuntimeError(proc.stderr)
        except Exception as e:
            logger.warning("rclone exception (attempt %d/%d): %s", attempt + 1, retries, e)
            if attempt + 1 < retries:
                time.sleep(5 * (attempt + 1))
            elif check:
                raise
    return False


def flush_and_upload_shard(
    conn,
    db_path: Path,
    work_dir: Path,
    domain: str,
    shard_index: int,
    rclone_dest: str,
    checkpoint: Dict[str, Any],
) -> int:
    conn.close()
    shard_dir = work_dir / f"shard_{shard_index:04d}"
    summary = export_shard_deliverables(
        db_path=db_path,
        out_dir=shard_dir,
        portal_domain=domain,
        shard_index=shard_index,
        extra_meta={"country_code": checkpoint["country_code"], "mode": "universal_detail_all_categories"},
    )
    shard_rows = summary["total_listings"]
    logger.info(
        "[%s] Exported shard %04d: %d listings | phone_cov=%.1f%% | files=%s",
        domain, shard_index, shard_rows, summary["contact_coverage_pct"],
        {k: v["bytes"] for k, v in summary["files"].items()},
    )

    if rclone_dest and shard_rows > 0:
        remote_shard = f"{rclone_dest}/shard_{shard_index:04d}"
        rclone_cmd(["copy", "--checksum", "-v", str(shard_dir), remote_shard], check=True)
        rclone_cmd(["check", str(shard_dir), remote_shard], check=False)

        # Update portal_totals.json and checkpoint.json on Drive
        checkpoint["next_shard_index"] = shard_index + 1
        checkpoint["total_extracted"] = checkpoint.get("total_extracted", 0) + shard_rows
        checkpoint["shards_uploaded"] = checkpoint.get("shards_uploaded", 0) + 1
        checkpoint["updated_at_utc"] = datetime.now(timezone.utc).isoformat()

        ckpt_file = work_dir / "checkpoint.json"
        ckpt_file.write_text(json.dumps(checkpoint, indent=2), encoding="utf-8")
        rclone_cmd(["copyto", "--checksum", str(ckpt_file), f"{rclone_dest}/checkpoint.json"], check=False)

        totals_file = work_dir / "portal_totals.json"
        totals_file.write_text(json.dumps({
            "portal_domain": domain,
            "country_code": checkpoint["country_code"],
            "total_listings": checkpoint["total_extracted"],
            "shards_uploaded": checkpoint["shards_uploaded"],
            "sitemaps_done": len(checkpoint.get("completed_sitemaps", [])),
            "sitemaps_total": checkpoint.get("total_sitemaps", 0),
            "status": checkpoint.get("status", "IN_PROGRESS"),
            "updated_at_utc": checkpoint["updated_at_utc"],
        }, indent=2), encoding="utf-8")
        rclone_cmd(["copyto", "--checksum", str(totals_file), f"{rclone_dest}/portal_totals.json"], check=False)

    # Free runner disk immediately
    shutil.rmtree(shard_dir, ignore_errors=True)
    for ext in ("", "-wal", "-shm"):
        p = Path(f"{db_path}{ext}")
        if p.exists():
            p.unlink(missing_ok=True)
    return shard_rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Universal Resumable 5-Hour Checkpointed Cloud Worker")
    parser.add_argument("--domain", required=True, help="Portal domain, e.g. subito.it")
    parser.add_argument("--country-code", default="", help="ISO-2 country code (e.g. IT, BR, JP)")
    parser.add_argument("--country-name", default="", help="Country name if code not passed")
    parser.add_argument("--workers", type=int, default=32, help="Concurrent HTTP/2 detail workers")
    parser.add_argument("--shard-size", type=int, default=100000, help="Rotate shard every N rows")
    parser.add_argument("--max-minutes", type=int, default=290, help="Soft stop budget in minutes (<360m GH cap)")
    parser.add_argument("--max-urls", type=int, default=0, help="Optional cap on URLs for smoke tests (0=unlimited)")
    parser.add_argument("--work-dir", default="/tmp/universal_work", help="Local scratch directory")
    parser.add_argument("--rclone-dest", default="", help="Remote rclone dest, e.g. gdrive:Global_Cloud_Swarm_Exports/portals/subito.it")
    args = parser.parse_args()

    domain = args.domain.strip().lower()
    cc = args.country_code.strip().upper()
    if not cc and args.country_name:
        cc = COUNTRY_NAME_TO_CODE.get(args.country_name.strip().lower(), "US")
    if cc not in COUNTRY_META:
        cc = "US"

    work_dir = Path(args.work_dir) / re.sub(r"[^a-zA-Z0-9_.-]+", "_", domain)
    work_dir.mkdir(parents=True, exist_ok=True)
    start_ts = time.time()
    deadline_ts = start_ts + (args.max_minutes * 60)

    # 1. Load resumable checkpoint from Google Drive if present
    ckpt_path = work_dir / "checkpoint.json"
    if args.rclone_dest:
        rclone_cmd(["copyto", f"{args.rclone_dest}/checkpoint.json", str(ckpt_path)], check=False)

    if ckpt_path.exists():
        checkpoint = json.loads(ckpt_path.read_text(encoding="utf-8"))
        logger.info("[%s] Resuming from checkpoint: shard=%d, extracted=%d, completed_sitemaps=%d",
                    domain, checkpoint.get("next_shard_index", 1),
                    checkpoint.get("total_extracted", 0), len(checkpoint.get("completed_sitemaps", [])))
    else:
        checkpoint = {
            "portal_domain": domain,
            "country_code": cc,
            "next_shard_index": 1,
            "total_extracted": 0,
            "shards_uploaded": 0,
            "completed_sitemaps": [],
            "total_sitemaps": 0,
            "status": "IN_PROGRESS",
        }

    completed_sm: Set[str] = set(checkpoint.get("completed_sitemaps", []))

    # 2. Discover child XML sitemaps
    child_sitemaps = discover_child_sitemaps(domain)
    checkpoint["total_sitemaps"] = len(child_sitemaps)
    pending_sitemaps = [s for s in child_sitemaps if s not in completed_sm]
    logger.info("[%s] Discovered %d child sitemaps (%d pending).", domain, len(child_sitemaps), len(pending_sitemaps))

    if not pending_sitemaps:
        checkpoint["status"] = "COMPLETED" if child_sitemaps else "NO_SITEMAP_FOUND"
        ckpt_path.write_text(json.dumps(checkpoint, indent=2), encoding="utf-8")
        if args.rclone_dest:
            rclone_cmd(["copyto", "--checksum", str(ckpt_path), f"{args.rclone_dest}/checkpoint.json"], check=False)
        logger.info("[%s] Nothing pending (status=%s). Exiting cleanly.", domain, checkpoint["status"])
        return

    # 3. Stream sitemaps -> 32-worker detail extraction -> 5,000-row WAL batches -> 100k shard rotation
    shard_index = int(checkpoint.get("next_shard_index", 1))
    db_path = work_dir / f"active_shard_{shard_index:04d}.sqlite"
    conn = init_shard_db(db_path)
    shard_count = 0
    total_urls_processed = 0
    batch_buf: List[Dict[str, Any]] = []
    waf_consecutive_403 = 0

    def process_detail(u: str) -> Tuple[Optional[Dict[str, Any]], int]:
        res = fetch_url(u, timeout=12, max_retries=2)
        if not res:
            return None, 0
        code, html = res
        if code != 200:
            return None, code
        return extract_detail_record(u, html, domain, cc), 200

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
        for sm_idx, sm_url in enumerate(pending_sitemaps, 1):
            if time.time() >= deadline_ts:
                logger.info("[%s] Soft time budget (%dm) reached. Flushing final shard...", domain, args.max_minutes)
                break

            res = fetch_url(sm_url, timeout=15)
            if not res or res[0] != 200:
                continue

            urls = [u for u in LOC_RE.findall(res[1]) if not SKIP_URL_RE.search(u)]
            if args.max_urls > 0:
                remaining = args.max_urls - total_urls_processed
                if remaining <= 0:
                    break
                urls = urls[:remaining]

            logger.info("[%s] Sitemap %d/%d (%s): %d candidate listing URLs",
                        domain, sm_idx, len(pending_sitemaps), sm_url, len(urls))

            # Process in windows of 2,000 URLs to bound memory and react fast to deadline/WAF
            sm_aborted = False
            for start in range(0, len(urls), 2000):
                if time.time() >= deadline_ts:
                    sm_aborted = True
                    break
                chunk = urls[start:start + 2000]
                futs = [executor.submit(process_detail, u) for u in chunk]
                for fut in concurrent.futures.as_completed(futs):
                    rec, status_code = fut.result()
                    total_urls_processed += 1
                    if status_code in (403, 429):
                        waf_consecutive_403 += 1
                    elif status_code == 200:
                        waf_consecutive_403 = 0
                    if rec:
                        batch_buf.append(rec)

                    if len(batch_buf) >= 5000:
                        inserted = upsert_batch(conn, batch_buf, [])
                        shard_count += inserted
                        batch_buf.clear()
                        elapsed = max(1.0, time.time() - start_ts)
                        live_total = checkpoint.get("total_extracted", 0) + shard_count
                        logger.info("[%s] Committed WAL batch | shard=%d | live_total=%d | rate=%.1f items/sec",
                                    domain, shard_count, live_total, live_total / elapsed)
                        if args.rclone_dest:
                            hb_file = work_dir / "portal_totals.json"
                            hb_file.write_text(json.dumps({
                                "portal_domain": domain,
                                "country_code": checkpoint["country_code"],
                                "total_listings": live_total,
                                "shards_uploaded": checkpoint.get("shards_uploaded", 0),
                                "sitemaps_done": len(completed_sm),
                                "sitemaps_total": checkpoint.get("total_sitemaps", 0),
                                "status": "IN_PROGRESS",
                                "updated_at_utc": datetime.now(timezone.utc).isoformat(),
                            }, indent=2), encoding="utf-8")
                            rclone_cmd(["copyto", str(hb_file), f"{args.rclone_dest}/portal_totals.json"], check=False)

                    if shard_count >= args.shard_size:
                        if batch_buf:
                            shard_count += upsert_batch(conn, batch_buf, [])
                            batch_buf.clear()
                        flush_and_upload_shard(conn, db_path, work_dir, domain, shard_index, args.rclone_dest, checkpoint)
                        shard_index = int(checkpoint["next_shard_index"])
                        db_path = work_dir / f"active_shard_{shard_index:04d}.sqlite"
                        conn = init_shard_db(db_path)
                        shard_count = 0

                if 50 <= waf_consecutive_403 < 300:
                    logger.warning("[%s] Rate-limit burst (%d consecutive 403/429). Cooling down 15s...", domain, waf_consecutive_403)
                    time.sleep(15)
                elif waf_consecutive_403 >= 300:
                    logger.warning("[%s] Hit 300 consecutive 403/429 responses -> WAF_BLOCKED_DETAIL. Stopping cleanly.", domain)
                    checkpoint["status"] = "WAF_BLOCKED_DETAIL"
                    sm_aborted = True
                    break

            if not sm_aborted:
                completed_sm.add(sm_url)
                checkpoint["completed_sitemaps"] = list(completed_sm)
            else:
                break

            if args.max_urls > 0 and total_urls_processed >= args.max_urls:
                break

    # Final flush
    if batch_buf:
        shard_count += upsert_batch(conn, batch_buf, [])
        batch_buf.clear()

    if len(completed_sm) >= len(child_sitemaps) and checkpoint.get("status") != "WAF_BLOCKED_DETAIL":
        checkpoint["status"] = "COMPLETED"

    if shard_count > 0:
        flush_and_upload_shard(conn, db_path, work_dir, domain, shard_index, args.rclone_dest, checkpoint)
    else:
        conn.close()
        ckpt_path.write_text(json.dumps(checkpoint, indent=2), encoding="utf-8")
        if args.rclone_dest:
            rclone_cmd(["copyto", "--checksum", str(ckpt_path), f"{args.rclone_dest}/checkpoint.json"], check=False)

    logger.info("[%s] Finished run | status=%s | total_extracted=%d | elapsed=%.1fs",
                domain, checkpoint["status"], checkpoint.get("total_extracted", 0), time.time() - start_ts)


if __name__ == "__main__":
    main()
