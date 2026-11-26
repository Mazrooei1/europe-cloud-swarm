"""
Engine v4.5 Dynamic Sharded Turbo Cloud Extraction Worker (Standalone — v2.0 & v3.0 untouched).
Combines ALL 5 Engine v3.0 Quality Safeguards (>85% verified accuracy, 0% brand pollution,
0 empty stubs, 110% capacity circuit breaker, canonical dedup) with v4.5 Zero-Stall Throughput:
  - Multi-Runner Sitemap Sharding (--partition-id P --num-partitions K) with collision-free
    shard_p{P}_XXXX & checkpoint_p{P}.json (zero write contention across concurrent runners!)
  - Cross-Sitemap Producer-Consumer Queue (background sitemap XML prefetcher -> zero drains!)
  - Continuous Sliding-Window Queue (64 HTTP/2 workers, max_in_flight=128, FIRST_COMPLETED)
  - Async Background Shard & Heartbeat Uploader (exports .csv/.jsonl + scorecard + rclone MD5
    in a background thread while all 64 HTTP/2 workers keep scraping uninterrupted!)
  - Fast-fail 7s HTTP/2 timeout (1 retry max) + 10,000-row SQLite WAL executemany batches
"""

import argparse
import concurrent.futures
import json
import logging
import queue
import re
import shutil
import sqlite3
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from cloud_schema import (
    COUNTRY_META,
    COUNTRY_NAME_TO_CODE,
    export_shard_deliverables,
    init_shard_db,
    upsert_batch,
)
from universal_cloud_worker_v3 import (
    LOC_RE,
    compute_db_quality_scorecard,
    cross_reference_contacts,
    discover_child_sitemaps,
    extract_v3_record,
    fetch_url,
    is_probable_listing_url,
    rclone_cmd,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("engine_v45_sharded")

EXTRA_JUNK_URL_RE = re.compile(r"/(?:kodepos|panduan-properti|kpr-kalkulator|artikel)(?:/|$|\?)", re.I)


def is_v45_listing_url(u: str) -> bool:
    if EXTRA_JUNK_URL_RE.search(u):
        return False
    return is_probable_listing_url(u)


def _upload_heartbeat_bg(
    work_dir: Path,
    domain: str,
    cc: str,
    live_total: int,
    shards_uploaded: int,
    sitemaps_done: int,
    sitemaps_total: int,
    status: str,
    rclone_dest: str,
    totals_filename: str,
) -> None:
    if not rclone_dest:
        return
    hb_file = work_dir / totals_filename
    hb_file.write_text(json.dumps({
        "portal_domain": domain,
        "country_code": cc,
        "engine": "v4.5_sharded",
        "total_listings": live_total,
        "shards_uploaded": shards_uploaded,
        "sitemaps_done": sitemaps_done,
        "sitemaps_total": sitemaps_total,
        "status": status,
        "updated_at_utc": datetime.now(timezone.utc).isoformat(),
    }, indent=2), encoding="utf-8")
    rclone_cmd(["copyto", str(hb_file), f"{rclone_dest}/{totals_filename}"], check=False, retries=2)


def _flush_and_upload_shard_bg(
    db_path: Path,
    work_dir: Path,
    domain: str,
    cc: str,
    shard_index: int,
    shard_prefix: str,
    rclone_dest: str,
    ckpt_filename: str,
    totals_filename: str,
    ckpt_snapshot: Dict[str, Any],
    ckpt_lock: threading.Lock,
    live_ckpt_ref: Dict[str, Any],
) -> int:
    conn = sqlite3.connect(str(db_path), timeout=60)
    xref_count = cross_reference_contacts(conn)
    conn.close()

    scorecard = compute_db_quality_scorecard(db_path, domain)
    shard_name = f"{shard_prefix}{shard_index:04d}"
    shard_dir = work_dir / shard_name
    summary = export_shard_deliverables(
        db_path=db_path,
        out_dir=shard_dir,
        portal_domain=domain,
        shard_index=shard_index,
        extra_meta={
            "country_code": cc,
            "engine": "v4.5_sharded_turbo",
            "shard_name": shard_name,
            "contacts_cross_referenced": xref_count,
            "quality_scorecard": scorecard,
        },
    )
    shard_rows = summary["total_listings"]
    logger.info(
        "[%s] v4.5 %s: %d rows | verdict=%s | avail_acc=%.1f%% (%d/12 cols) | phone_cov=%.1f%% | brand_poll=%d",
        domain, shard_name, shard_rows, scorecard["verdict"],
        scorecard["available_field_accuracy_pct"], scorecard["available_columns"],
        summary["contact_coverage_pct"], scorecard["brand_pollution_count"],
    )

    if rclone_dest and shard_rows > 0:
        remote_shard = f"{rclone_dest}/{shard_name}"
        rclone_cmd(["copy", "--checksum", "-v", str(shard_dir), remote_shard], check=True)
        rclone_cmd(["check", str(shard_dir), remote_shard], check=False)

    with ckpt_lock:
        live_ckpt_ref["next_shard_index"] = max(int(live_ckpt_ref.get("next_shard_index", 1)), shard_index + 1)
        live_ckpt_ref["total_extracted"] = int(live_ckpt_ref.get("total_extracted", 0)) + shard_rows
        live_ckpt_ref["shards_uploaded"] = int(live_ckpt_ref.get("shards_uploaded", 0)) + (1 if shard_rows > 0 else 0)
        live_ckpt_ref["last_verdict"] = scorecard["verdict"]
        live_ckpt_ref["last_avail_accuracy_pct"] = scorecard["available_field_accuracy_pct"]
        live_ckpt_ref["updated_at_utc"] = datetime.now(timezone.utc).isoformat()
        ckpt_copy = dict(live_ckpt_ref)
        ckpt_copy["completed_sitemaps"] = list(live_ckpt_ref.get("completed_sitemaps", []))

    if rclone_dest and shard_rows > 0:
        ckpt_file = work_dir / ckpt_filename
        ckpt_file.write_text(json.dumps(ckpt_copy, indent=2), encoding="utf-8")
        rclone_cmd(["copyto", "--checksum", str(ckpt_file), f"{rclone_dest}/{ckpt_filename}"], check=False)

        totals_file = work_dir / totals_filename
        totals_file.write_text(json.dumps({
            "portal_domain": domain,
            "country_code": cc,
            "engine": "v4.5_sharded",
            "total_listings": ckpt_copy["total_extracted"],
            "shards_uploaded": ckpt_copy["shards_uploaded"],
            "sitemaps_done": len(ckpt_copy.get("completed_sitemaps", [])),
            "sitemaps_total": ckpt_copy.get("total_sitemaps", 0),
            "verdict": scorecard["verdict"],
            "available_field_accuracy_pct": scorecard["available_field_accuracy_pct"],
            "status": ckpt_copy.get("status", "IN_PROGRESS"),
            "updated_at_utc": ckpt_copy["updated_at_utc"],
        }, indent=2), encoding="utf-8")
        rclone_cmd(["copyto", "--checksum", str(totals_file), f"{rclone_dest}/{totals_filename}"], check=False)

    shutil.rmtree(shard_dir, ignore_errors=True)
    for ext in ("", "-wal", "-shm"):
        p = Path(f"{db_path}{ext}")
        if p.exists():
            p.unlink(missing_ok=True)
    return shard_rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Engine v4.5 Dynamic Sharded Turbo Cloud Worker")
    parser.add_argument("--domain", required=True)
    parser.add_argument("--country-code", default="")
    parser.add_argument("--country-name", default="")
    parser.add_argument("--capacity", type=int, default=0, help="Audited capacity for 110%% circuit breaker")
    parser.add_argument("--workers", type=int, default=64, help="Concurrent HTTP/2 workers (default 64)")
    parser.add_argument("--shard-size", type=int, default=50000)
    parser.add_argument("--batch-size", type=int, default=10000)
    parser.add_argument("--max-minutes", type=int, default=290)
    parser.add_argument("--max-urls", type=int, default=0)
    parser.add_argument("--partition-id", type=int, default=0, help="0-based partition index for multi-runner sharding")
    parser.add_argument("--num-partitions", type=int, default=1, help="Total concurrent runners sharing this portal")
    parser.add_argument("--work-dir", default="/tmp/v45_work")
    parser.add_argument("--rclone-dest", default="")
    args = parser.parse_args()

    domain = args.domain.strip().lower()
    cc = args.country_code.strip().upper()
    if not cc and args.country_name:
        cc = COUNTRY_NAME_TO_CODE.get(args.country_name.strip().lower(), "US")
    if cc not in COUNTRY_META:
        cc = "US"

    num_parts = max(1, int(args.num_partitions))
    part_id = max(0, min(int(args.partition_id), num_parts - 1))
    is_sharded = num_parts > 1

    shard_prefix = f"shard_p{part_id}_" if is_sharded else "shard_"
    ckpt_filename = f"checkpoint_p{part_id}.json" if is_sharded else "checkpoint.json"
    totals_filename = f"portal_totals_p{part_id}.json" if is_sharded else "portal_totals.json"

    cap_limit = int(args.capacity * 1.10) if args.capacity > 0 else 0
    part_dir_tag = f"{domain}_p{part_id}" if is_sharded else domain
    work_dir = Path(args.work_dir) / re.sub(r"[^a-zA-Z0-9_.-]+", "_", part_dir_tag)
    work_dir.mkdir(parents=True, exist_ok=True)
    start_ts = time.time()
    deadline_ts = start_ts + (args.max_minutes * 60)

    # 1. Sync base checkpoint.json + all partition checkpoints + 24h sitemaps_cache.json from Drive
    ckpt_sync_dir = work_dir / "_ckpts"
    ckpt_sync_dir.mkdir(parents=True, exist_ok=True)
    if args.rclone_dest:
        rclone_cmd(
            [
                "copy",
                "--include", "checkpoint*.json",
                "--include", "sitemaps_cache.json",
                args.rclone_dest,
                str(ckpt_sync_dir),
            ],
            check=False,
            retries=2,
        )

    global_completed_sm: Set[str] = set()
    base_total_extracted = 0
    base_ckpt_file = ckpt_sync_dir / "checkpoint.json"
    if base_ckpt_file.exists():
        try:
            base_c = json.loads(base_ckpt_file.read_text(encoding="utf-8"))
            if base_c.get("status") in ("COMPLETED", "CAPACITY_CAP_REVIEW"):
                logger.info("[%s] Base checkpoint already %s (%d rows). Exiting cleanly.",
                            domain, base_c["status"], base_c.get("total_extracted", 0))
                return
            global_completed_sm.update(base_c.get("completed_sitemaps", []))
            base_total_extracted = int(base_c.get("total_extracted", 0))
        except Exception:
            pass

    for pf in ckpt_sync_dir.glob("checkpoint_p*.json"):
        try:
            pc = json.loads(pf.read_text(encoding="utf-8"))
            global_completed_sm.update(pc.get("completed_sitemaps", []))
        except Exception:
            pass

    my_ckpt_file = ckpt_sync_dir / ckpt_filename
    if my_ckpt_file.exists():
        checkpoint = json.loads(my_ckpt_file.read_text(encoding="utf-8"))
        logger.info("[%s][p%d/%d] Resuming v4.5: shard=%d, part_extracted=%d, global_sm_done=%d",
                    domain, part_id, num_parts, checkpoint.get("next_shard_index", 1),
                    checkpoint.get("total_extracted", 0), len(global_completed_sm))
    else:
        checkpoint = {
            "portal_domain": domain,
            "country_code": cc,
            "engine": "v4.5_sharded",
            "partition_id": part_id,
            "num_partitions": num_parts,
            "next_shard_index": 1,
            "total_extracted": 0,
            "shards_uploaded": 0,
            "completed_sitemaps": [],
            "total_sitemaps": 0,
            "status": "IN_PROGRESS",
        }

    # Partition-local completed sitemaps + global completed sitemaps
    completed_sm: Set[str] = set(checkpoint.get("completed_sitemaps", []))

    # 24-hour Sitemaps Cache (skips 6-10 min discovery on mega-portals like craigslist.org!)
    child_sitemaps: List[str] = []
    sm_cache_file = ckpt_sync_dir / "sitemaps_cache.json"
    cache_ttl_s = 86400
    if sm_cache_file.exists():
        try:
            sc = json.loads(sm_cache_file.read_text(encoding="utf-8"))
            age_s = time.time() - float(sc.get("cached_at_ts", 0))
            cached_list = sc.get("sitemaps", [])
            if isinstance(cached_list, list) and len(cached_list) > 0 and age_s < cache_ttl_s:
                child_sitemaps = cached_list
                logger.info(
                    "[%s][p%d/%d] sitemaps_cache.json HIT (%d sitemaps, age=%.1fh) -> skipped discovery!",
                    domain, part_id, num_parts, len(child_sitemaps), age_s / 3600.0,
                )
        except Exception:
            pass

    if not child_sitemaps:
        child_sitemaps = discover_child_sitemaps(domain, max_sitemaps=50000, workers=min(args.workers, 32))
        if child_sitemaps and (not is_sharded or part_id == 0):
            cache_payload = {
                "portal_domain": domain,
                "cached_at_ts": time.time(),
                "cached_at_utc": datetime.now(timezone.utc).isoformat(),
                "count": len(child_sitemaps),
                "sitemaps": child_sitemaps,
            }
            out_cache = work_dir / "sitemaps_cache.json"
            out_cache.write_text(json.dumps(cache_payload, separators=(",", ":")), encoding="utf-8")
            if args.rclone_dest:
                rclone_cmd(["copyto", "--checksum", str(out_cache), f"{args.rclone_dest}/sitemaps_cache.json"], check=False, retries=2)
                logger.info("[%s][p%d/%d] Uploaded 24h sitemaps_cache.json (%d sitemaps)", domain, part_id, num_parts, len(child_sitemaps))

    checkpoint["total_sitemaps"] = len(child_sitemaps)

    all_pending = [s for s in child_sitemaps if s not in global_completed_sm and s not in completed_sm]
    my_pending = [s for idx, s in enumerate(all_pending) if idx % num_parts == part_id] if is_sharded else all_pending

    logger.info(
        "[%s][p%d/%d] v4.5 discovered %d sitemaps (%d pending global -> %d assigned to this partition) | workers=%d | cap_limit=%d",
        domain, part_id, num_parts, len(child_sitemaps), len(all_pending), len(my_pending), args.workers, cap_limit,
    )

    if not my_pending:
        checkpoint["status"] = "COMPLETED" if child_sitemaps else "NO_SITEMAP_FOUND"
        checkpoint["updated_at_utc"] = datetime.now(timezone.utc).isoformat()
        out_ckpt = work_dir / ckpt_filename
        out_ckpt.write_text(json.dumps(checkpoint, indent=2), encoding="utf-8")
        if args.rclone_dest:
            rclone_cmd(["copyto", "--checksum", str(out_ckpt), f"{args.rclone_dest}/{ckpt_filename}"], check=False)
        return

    # 2. Cross-Sitemap Producer-Consumer Thread (Zero Inter-Sitemap Drains!)
    url_queue: "queue.Queue[Optional[Tuple[str, str]]]" = queue.Queue(maxsize=4096)
    sm_pending_counts: Dict[str, int] = {}
    sm_counts_lock = threading.Lock()
    stop_producer = threading.Event()

    def sitemap_producer() -> None:
        produced_urls = 0
        for sm_idx, sm_url in enumerate(my_pending, 1):
            if stop_producer.is_set() or time.time() >= deadline_ts:
                break
            res = fetch_url(sm_url, timeout=12, max_retries=2)
            if not res or res[0] != 200:
                continue
            all_locs = LOC_RE.findall(res[1])
            urls = [u.strip() for u in all_locs if is_v45_listing_url(u.strip())]
            if not urls:
                with sm_counts_lock:
                    completed_sm.add(sm_url)
                continue
            if args.max_urls > 0:
                rem = (args.max_urls * 2) - produced_urls
                if rem <= 0:
                    break
                urls = urls[:rem]
            with sm_counts_lock:
                sm_pending_counts[sm_url] = len(urls)
            if sm_idx <= 5 or sm_idx % 25 == 0:
                logger.info("[%s][p%d/%d] Prefetched sitemap %d/%d: %d/%d URLs queued",
                            domain, part_id, num_parts, sm_idx, len(my_pending), len(urls), len(all_locs))
            for u in urls:
                if stop_producer.is_set() or time.time() >= deadline_ts:
                    break
                while not stop_producer.is_set() and time.time() < deadline_ts:
                    try:
                        url_queue.put((sm_url, u), timeout=0.5)
                        produced_urls += 1
                        break
                    except queue.Full:
                        continue
        # Sentinel to signal consumer that all sitemaps are enqueued
        url_queue.put(None)

    producer_thread = threading.Thread(target=sitemap_producer, daemon=True)
    producer_thread.start()

    # 3. Async Background Shard & Heartbeat Uploader (Zero Upload Stalls!)
    ckpt_lock = threading.Lock()
    uploader = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    upload_futures: List[concurrent.futures.Future] = []

    shard_index = int(checkpoint.get("next_shard_index", 1))
    db_path = work_dir / f"active_{shard_prefix}{shard_index:04d}.sqlite"
    conn = init_shard_db(db_path)
    shard_count = 0
    run_ingested = 0
    batch_buf: List[Dict[str, Any]] = []
    waf_consecutive_403 = 0
    stop_reason: Optional[str] = None
    max_in_flight = args.workers * 2
    producer_done = False

    # Effective per-partition capacity share so sharded runners don't overshoot 110%
    effective_base = base_total_extracted if is_sharded else int(checkpoint.get("total_extracted", 0))
    part_cap_allowance = max(1000, (cap_limit - effective_base) // num_parts) if (cap_limit > 0 and is_sharded) else 0

    def process_detail_fast(sm_url: str, u: str) -> Tuple[str, Optional[Dict[str, Any]], int]:
        res = fetch_url(u, timeout=7, max_retries=1)
        if not res:
            return sm_url, None, 0
        code, html = res
        if code != 200:
            return sm_url, None, code
        return sm_url, extract_v3_record(u, html, domain, cc), 200

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
        in_flight: Set[concurrent.futures.Future] = set()

        def refill() -> None:
            nonlocal producer_done
            while len(in_flight) < max_in_flight and not producer_done and time.time() < deadline_ts and not stop_reason:
                try:
                    item = url_queue.get(timeout=0.05 if in_flight else 0.5)
                except queue.Empty:
                    if not producer_thread.is_alive():
                        producer_done = True
                    break
                if item is None:
                    producer_done = True
                    break
                sm_u, target_u = item
                in_flight.add(executor.submit(process_detail_fast, sm_u, target_u))

        refill()
        while in_flight or (not producer_done and time.time() < deadline_ts and not stop_reason):
            if not in_flight:
                refill()
                continue

            done, in_flight = concurrent.futures.wait(
                in_flight,
                timeout=1.0,
                return_when=concurrent.futures.FIRST_COMPLETED,
            )
            for fut in done:
                try:
                    sm_u, rec, code = fut.result()
                except Exception:
                    sm_u, rec, code = "", None, 0

                if sm_u:
                    with sm_counts_lock:
                        if sm_u in sm_pending_counts:
                            sm_pending_counts[sm_u] -= 1
                            if sm_pending_counts[sm_u] <= 0:
                                del sm_pending_counts[sm_u]
                                completed_sm.add(sm_u)
                                with ckpt_lock:
                                    checkpoint["completed_sitemaps"] = list(completed_sm)

                if code in (403, 429):
                    waf_consecutive_403 += 1
                elif code == 200:
                    waf_consecutive_403 = 0

                if rec:
                    batch_buf.append(rec)

                if len(batch_buf) >= args.batch_size:
                    inserted = upsert_batch(conn, batch_buf, [])
                    shard_count += inserted
                    run_ingested += inserted
                    batch_buf.clear()
                    elapsed = max(1.0, time.time() - start_ts)
                    with ckpt_lock:
                        live_part_total = int(checkpoint.get("total_extracted", 0)) + shard_count
                        sh_up = int(checkpoint.get("shards_uploaded", 0))
                    logger.info(
                        "[%s][p%d/%d] v4.5 WAL commit | shard=%d | part_total=%d | sm_done=%d | rate=%.1f items/sec",
                        domain, part_id, num_parts, shard_count, live_part_total, len(completed_sm), run_ingested / elapsed,
                    )
                    uploader.submit(
                        _upload_heartbeat_bg,
                        work_dir, domain, cc, live_part_total, sh_up,
                        len(completed_sm), len(child_sitemaps), "IN_PROGRESS",
                        args.rclone_dest, totals_filename,
                    )

                    if cap_limit > 0:
                        if (is_sharded and run_ingested >= part_cap_allowance) or (not is_sharded and live_part_total >= cap_limit):
                            logger.warning("[%s][p%d/%d] Hit 110%% capacity cap -> CAPACITY_CAP_REVIEW.", domain, part_id, num_parts)
                            stop_reason = "CAPACITY_CAP_REVIEW"
                            break

                if shard_count >= args.shard_size:
                    if batch_buf:
                        inserted = upsert_batch(conn, batch_buf, [])
                        shard_count += inserted
                        run_ingested += inserted
                        batch_buf.clear()
                    conn.close()
                    closed_db = db_path
                    closed_idx = shard_index
                    with ckpt_lock:
                        checkpoint["completed_sitemaps"] = list(completed_sm)
                        snap = dict(checkpoint)
                    upload_futures.append(uploader.submit(
                        _flush_and_upload_shard_bg,
                        closed_db, work_dir, domain, cc, closed_idx, shard_prefix,
                        args.rclone_dest, ckpt_filename, totals_filename, snap, ckpt_lock, checkpoint,
                    ))
                    shard_index += 1
                    db_path = work_dir / f"active_{shard_prefix}{shard_index:04d}.sqlite"
                    conn = init_shard_db(db_path)
                    shard_count = 0

                if args.max_urls > 0 and (run_ingested + len(batch_buf)) >= args.max_urls:
                    stop_reason = "MAX_URLS_TEST"
                    break

            if waf_consecutive_403 >= 200:
                logger.warning("[%s][p%d/%d] 200 consecutive 403/429 -> WAF_BLOCKED_DETAIL (zero stubs written).", domain, part_id, num_parts)
                stop_reason = "WAF_BLOCKED_DETAIL"
                break

            if stop_reason or time.time() >= deadline_ts:
                break

            refill()

        stop_producer.set()
        for f in in_flight:
            f.cancel()

    # 4. Final flush + wait for all background shard uploads to finish & MD5-verify
    if batch_buf:
        inserted = upsert_batch(conn, batch_buf, [])
        shard_count += inserted
        run_ingested += inserted
        batch_buf.clear()

    with ckpt_lock:
        checkpoint["completed_sitemaps"] = list(completed_sm)
        if stop_reason and stop_reason != "MAX_URLS_TEST":
            checkpoint["status"] = stop_reason
        elif len(completed_sm) + len(global_completed_sm) >= len(child_sitemaps) and len(child_sitemaps) > 0:
            checkpoint["status"] = "COMPLETED"
        elif not is_sharded and len(completed_sm) >= len(child_sitemaps) and len(child_sitemaps) > 0:
            checkpoint["status"] = "COMPLETED"

    if shard_count > 0:
        conn.close()
        with ckpt_lock:
            snap = dict(checkpoint)
        upload_futures.append(uploader.submit(
            _flush_and_upload_shard_bg,
            db_path, work_dir, domain, cc, shard_index, shard_prefix,
            args.rclone_dest, ckpt_filename, totals_filename, snap, ckpt_lock, checkpoint,
        ))
    else:
        conn.close()
        for ext in ("", "-wal", "-shm"):
            p = Path(f"{db_path}{ext}")
            if p.exists():
                p.unlink(missing_ok=True)

    # Wait for every background shard upload to complete cleanly
    for uf in upload_futures:
        uf.result()
    uploader.shutdown(wait=True)

    # Always publish final checkpoint state (e.g. WAF_BLOCKED_DETAIL / COMPLETED / IN_PROGRESS)
    with ckpt_lock:
        checkpoint["updated_at_utc"] = datetime.now(timezone.utc).isoformat()
        final_ckpt_path = work_dir / ckpt_filename
        final_ckpt_path.write_text(json.dumps(checkpoint, indent=2), encoding="utf-8")
    if args.rclone_dest:
        rclone_cmd(["copyto", "--checksum", str(final_ckpt_path), f"{args.rclone_dest}/{ckpt_filename}"], check=False)

    elapsed = max(1.0, time.time() - start_ts)
    logger.info(
        "[%s][p%d/%d] Finished v4.5 Sharded | status=%s | run_ingested=%d | part_total=%d | sm_done=%d | avg_rate=%.1f items/sec",
        domain, part_id, num_parts, checkpoint["status"], run_ingested,
        checkpoint.get("total_extracted", 0), len(completed_sm), run_ingested / elapsed,
    )


if __name__ == "__main__":
    main()
