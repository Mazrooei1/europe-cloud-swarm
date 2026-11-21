"""
Phase 0 Cloud Reconnaissance Probe: All 327 Portals across 109 Countries.
Runs directly on GitHub Actions (Azure IPs) with 32 concurrent curl_cffi TLS workers
to classify every portal into REACHABLE_* vs WAF_BLOCKED / GEO_BLOCKED / UNREACHABLE,
and discover XML sitemaps + SSR hydration payloads (__NEXT_DATA__, JSON-LD, etc.).
"""

from __future__ import annotations

import argparse
import concurrent.futures
import csv
import gzip
import json
import logging
import random
import re
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    from curl_cffi import requests
except ImportError:
    requests = None  # Only needed for probing, not --merge-dir

from cloud_schema import COUNTRY_NAME_TO_CODE

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("probe_327")

TLS_PROFILES = ["chrome124", "chrome120", "safari15_5"]
thread_local = threading.local()

WAF_HEADER_SIGNATURES = [
    ("datadome", "DataDome"),
    ("x-datadome", "DataDome"),
    ("x-px-", "PerimeterX"),
    ("perimeterx", "PerimeterX"),
    ("akamai", "Akamai"),
    ("x-akamai", "Akamai"),
    ("incap_ses", "Imperva/Incapsula"),
    ("visid_incap", "Imperva/Incapsula"),
    ("x-iinfo", "Imperva/Incapsula"),
    ("cf-mitigated", "Cloudflare"),
    ("x-sucuri", "Sucuri"),
]

WAF_BODY_PATTERNS = [
    (re.compile(r"captcha-delivery\.com|geo\.captcha-delivery|datadome", re.I), "DataDome"),
    (re.compile(r"px-captcha|_pxhd|perimeterx|human\s+verification", re.I), "PerimeterX"),
    (re.compile(r"cf-turnstile|challenges\.cloudflare\.com|just a moment\.\.\.|attention required! \| cloudflare", re.I), "Cloudflare"),
    (re.compile(r"incapsula incident|_incapsula_resource", re.I), "Imperva/Incapsula"),
    (re.compile(r"access denied.*akamai|reference&#32;#", re.I), "Akamai"),
    (re.compile(r"not available in your country|geo-?restricted|access from your region|unavailable in your region", re.I), "Geo-Block"),
]

SITEMAP_LOC_RE = re.compile(r"<loc>\s*([^<\s]+)\s*</loc>", re.I)
ROBOTS_SITEMAP_RE = re.compile(r"^\s*Sitemap:\s*(\S+)", re.I | re.M)


def get_session(profile: Optional[str] = None) -> requests.Session:
    if profile:
        return requests.Session(impersonate=profile)
    if not hasattr(thread_local, "session"):
        p = random.choice(TLS_PROFILES)
        thread_local.session = requests.Session(impersonate=p)
        thread_local.profile = p
    return thread_local.session


def load_327_portals(csv_path: Path) -> List[Dict[str, str]]:
    portals: List[Dict[str, str]] = []
    seen = set()
    with csv_path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            country = (row.get("Country") or "").strip()
            cc = COUNTRY_NAME_TO_CODE.get(country.lower(), "XX")
            for tier_col in [
                "Top Real Estate Portal",
                "Secondary Real Estate / General Portal",
                "Top General Classifieds",
            ]:
                raw_domain = (row.get(tier_col) or "").strip()
                if not raw_domain:
                    continue
                domain = raw_domain.replace(" ", "").lower()
                key = (country, domain)
                if key in seen:
                    continue
                seen.add(key)
                portals.append({
                    "country": country,
                    "country_code": cc,
                    "domain": domain,
                    "raw_domain": raw_domain,
                    "tier": tier_col,
                })
    return portals


def detect_waf(status_code: int, headers: Dict[str, str], body: str) -> Optional[str]:
    hdr_blob = " ".join(f"{k}:{v}" for k, v in headers.items()).lower()
    for sig, vendor in WAF_HEADER_SIGNATURES:
        if sig in hdr_blob and status_code in (401, 403, 405, 429, 503):
            return vendor
    head_body = body[:30000]
    for pat, vendor in WAF_BODY_PATTERNS:
        if pat.search(head_body):
            if status_code in (401, 403, 405, 429, 503) or len(body) < 18000:
                return vendor
    if status_code in (403, 429, 503):
        return f"HTTP_{status_code}_BLOCK"
    return None


def safe_get(url: str, timeout: int = 12) -> Tuple[Optional[Any], Optional[str]]:
    for prof in ("chrome124", "safari15_5"):
        try:
            s = get_session(prof)
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
                return r, None
            # Try second TLS profile if 403/429/503
            if prof == "chrome124" and r.status_code in (403, 429, 503):
                continue
            return r, None
        except Exception as e:
            err = f"{type(e).__name__}: {str(e)[:120]}"
            if prof == "safari15_5":
                return None, err
    return None, "request_failed"


def read_response_text(r: Any) -> str:
    try:
        content = r.content
        if content[:2] == b"\x1f\x8b":
            return gzip.decompress(content).decode("utf-8", errors="ignore")
        return r.text or ""
    except Exception:
        return ""


def probe_single_portal(entry: Dict[str, str]) -> Dict[str, Any]:
    domain = entry["domain"]
    t0 = time.time()
    result: Dict[str, Any] = {
        "country": entry["country"],
        "country_code": entry["country_code"],
        "domain": domain,
        "tier": entry["tier"],
        "probed_at_utc": datetime.now(timezone.utc).isoformat(),
        "homepage_status": None,
        "final_url": None,
        "waf_vendor": None,
        "hydration_signals": [],
        "robots_sitemaps": [],
        "sitemap_xml_count": 0,
        "sitemap_sample_urls": [],
        "classification": "UNREACHABLE",
        "error": None,
    }

    # 1. Probe homepage (apex then www)
    resp = None
    last_err = None
    for candidate in (f"https://{domain}/", f"https://www.{domain}/"):
        resp, last_err = safe_get(candidate, timeout=12)
        if resp is not None:
            break

    if resp is None:
        result["error"] = last_err
        result["elapsed_sec"] = round(time.time() - t0, 2)
        return result

    body = read_response_text(resp)
    hdrs = {k.lower(): v for k, v in resp.headers.items()}
    result["homepage_status"] = resp.status_code
    result["final_url"] = str(resp.url)
    base_origin = re.match(r"^(https?://[^/]+)", result["final_url"])
    origin = base_origin.group(1) if base_origin else f"https://{domain}"

    waf = detect_waf(resp.status_code, hdrs, body)
    result["waf_vendor"] = waf

    # 2. Check SSR / JSON hydration signals on homepage
    signals = []
    if "__NEXT_DATA__" in body:
        signals.append("__NEXT_DATA__")
    if "self.__next_f.push" in body:
        signals.append("NEXT_RSC")
    if "__INITIAL_STATE__" in body or "__PRELOADED_STATE__" in body:
        signals.append("__INITIAL_STATE__")
    if "__NUXT__" in body:
        signals.append("__NUXT__")
    if "application/ld+json" in body:
        signals.append("JSON_LD")
    result["hydration_signals"] = signals

    # 3. Probe robots.txt & XML sitemaps (even if homepage had soft WAF, sitemaps are often open)
    discovered_sitemaps: List[str] = []
    r_rob, _ = safe_get(f"{origin}/robots.txt", timeout=8)
    if r_rob is not None and r_rob.status_code == 200:
        rob_txt = read_response_text(r_rob)
        for m in ROBOTS_SITEMAP_RE.findall(rob_txt):
            sm = m.strip()
            if sm.startswith("http") and sm not in discovered_sitemaps:
                discovered_sitemaps.append(sm)

    if not discovered_sitemaps:
        for fallback in (f"{origin}/sitemap.xml", f"{origin}/sitemap_index.xml"):
            r_sm, _ = safe_get(fallback, timeout=8)
            if r_sm is not None and r_sm.status_code == 200:
                sm_txt = read_response_text(r_sm)
                if "<loc>" in sm_txt.lower():
                    discovered_sitemaps.append(fallback)
                    break

    result["robots_sitemaps"] = discovered_sitemaps[:20]

    xml_count = 0
    sample_urls: List[str] = []
    for sm_url in discovered_sitemaps[:3]:
        r_sm, _ = safe_get(sm_url, timeout=10)
        if r_sm is None or r_sm.status_code != 200:
            continue
        sm_txt = read_response_text(r_sm)
        locs = SITEMAP_LOC_RE.findall(sm_txt)
        if locs:
            xml_count += len(locs)
            if not sample_urls:
                sample_urls = locs[:5]
    result["sitemap_xml_count"] = xml_count
    result["sitemap_sample_urls"] = sample_urls

    # 4. Final classification
    if waf == "Geo-Block":
        result["classification"] = "GEO_BLOCKED"
    elif waf is not None:
        # Check if sitemap is actually open despite homepage WAF
        if xml_count > 0:
            result["classification"] = "WAF_HOMEPAGE_OPEN_SITEMAP"
        else:
            result["classification"] = "WAF_BLOCKED"
    elif resp.status_code == 200:
        if xml_count > 0 and signals:
            result["classification"] = "REACHABLE_SITEMAP_SSR"
        elif xml_count > 0:
            result["classification"] = "REACHABLE_SITEMAP"
        elif signals:
            result["classification"] = "REACHABLE_SSR"
        else:
            result["classification"] = "REACHABLE_HTML"
    else:
        result["classification"] = f"HTTP_{resp.status_code}"

    result["elapsed_sec"] = round(time.time() - t0, 2)
    return result


def merge_probe_shards(merge_dir: Path, out_path: Path) -> None:
    all_portals: List[Dict[str, Any]] = []
    for p in sorted(merge_dir.glob("probe_shard_*.json")):
        data = json.loads(p.read_text(encoding="utf-8"))
        all_portals.extend(data.get("portals", []))

    counts: Dict[str, int] = {}
    waf_counts: Dict[str, int] = {}
    for row in all_portals:
        c = row.get("classification", "UNKNOWN")
        counts[c] = counts.get(c, 0) + 1
        w = row.get("waf_vendor")
        if w:
            waf_counts[w] = waf_counts.get(w, 0) + 1

    manifest = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "total_portals_probed": len(all_portals),
        "classification_summary": counts,
        "waf_summary": waf_counts,
        "portals": all_portals,
    }
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")
    logger.info("Merged %d portals -> %s | Summary: %s", len(all_portals), out_path, counts)


def main() -> None:
    parser = argparse.ArgumentParser(description="Phase 0: 327-Portal Cloud Reconnaissance Probe")
    parser.add_argument("--csv", default="portals_327.csv", help="Path to portals_327.csv")
    parser.add_argument("--shard-index", type=int, default=0, help="0-based shard index")
    parser.add_argument("--shard-total", type=int, default=1, help="Total number of shards")
    parser.add_argument("--workers", type=int, default=24, help="Concurrent HTTP/2 workers")
    parser.add_argument("--output", default="/tmp/probe_shard_0.json", help="Output JSON path")
    parser.add_argument("--merge-dir", default="", help="If set, merge probe_shard_*.json from this dir")
    args = parser.parse_args()

    if args.merge_dir:
        merge_probe_shards(Path(args.merge_dir), Path(args.output))
        return

    portals = load_327_portals(Path(args.csv))
    my_portals = [p for idx, p in enumerate(portals) if idx % args.shard_total == args.shard_index]
    logger.info("Shard %d/%d probing %d portals (total=%d) with %d workers...",
                args.shard_index, args.shard_total, len(my_portals), len(portals), args.workers)

    results: List[Dict[str, Any]] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = {ex.submit(probe_single_portal, p): p for p in my_portals}
        for i, fut in enumerate(concurrent.futures.as_completed(futs), 1):
            res = fut.result()
            results.append(res)
            if i % 15 == 0 or i == len(my_portals):
                logger.info("Progress: %d/%d | last=%s -> %s (sitemap_locs=%d)",
                            i, len(my_portals), res["domain"], res["classification"], res["sitemap_xml_count"])

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps({
        "shard_index": args.shard_index,
        "shard_total": args.shard_total,
        "probed_at_utc": datetime.now(timezone.utc).isoformat(),
        "count": len(results),
        "portals": results,
    }, indent=2, ensure_ascii=False), encoding="utf-8")
    logger.info("Wrote shard probe manifest -> %s", out_path)


if __name__ == "__main__":
    main()
