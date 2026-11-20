"""
Cloud-Native SQLite Shard Merger & 1,000-Listing Gap Auditor.
- Merges all shard_*.sqlite files into europe_master.sqlite via SQLite ATTACH DATABASE.
- Checkpoints WAL so europe_master.sqlite is 100% self-contained.
- Generates extraction_audit_summary.json (Master .sqlite + audit JSON only, maximizing Drive quota).
- Computes bitwise MD5 checksums for verification after rclone upload.
"""

import argparse
import hashlib
import json
import logging
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

from cloud_schema import init_shard_db, ITEM_COLS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("cloud_merger")

TARGET_POOL = 1_428_780


def compute_md5(path: Path) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8 * 1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def merge_shards(master_db: Path, shards_dir: Path, summary_out: Path) -> None:
    conn = init_shard_db(master_db)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM items")
    before_count = cur.fetchone()[0]
    logger.info(f"Master DB count before merge: {before_count:,}")

    shard_files = sorted(shards_dir.glob("*.sqlite"))
    logger.info(f"Found {len(shard_files)} shard database(s) in {shards_dir}")

    cols_str = ", ".join(ITEM_COLS)

    for shard in shard_files:
        if shard.resolve() == master_db.resolve():
            continue
        try:
            cur.execute("ATTACH DATABASE ? AS shard_db", (str(shard),))
            cur.execute(
                f"INSERT OR REPLACE INTO main.items ({cols_str}) "
                f"SELECT {cols_str} FROM shard_db.items"
            )
            cur.execute(
                "INSERT OR REPLACE INTO main.organizations "
                "(org_id, name, portal_source, country_code, phone, address, website, created_at_utc) "
                "SELECT org_id, name, portal_source, country_code, phone, address, website, created_at_utc "
                "FROM shard_db.organizations"
            )
            conn.commit()
            cur.execute("DETACH DATABASE shard_db")
            logger.info(f"Merged shard: {shard.name}")
        except Exception as e:
            logger.error(f"Error merging {shard.name}: {e}")
            try:
                cur.execute("DETACH DATABASE shard_db")
            except Exception:
                pass

    # Final stats & WAL checkpoint
    cur.execute("SELECT COUNT(*) FROM items")
    total_items = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM organizations")
    total_orgs = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM sellers_or_agents")
    total_agents = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM items WHERE agent_phone IS NOT NULL AND agent_phone != ''")
    phone_count = cur.fetchone()[0]
    cur.execute("SELECT portal_source, COUNT(*) FROM items GROUP BY portal_source ORDER BY COUNT(*) DESC")
    portal_counts = {row[0]: row[1] for row in cur.fetchall()}
    cur.execute("SELECT country, COUNT(*) FROM items GROUP BY country ORDER BY COUNT(*) DESC")
    country_counts = {row[0]: row[1] for row in cur.fetchall()}

    conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
    conn.close()

    gap = max(TARGET_POOL - total_items, 0)
    pct = round((total_items / TARGET_POOL) * 100, 2)
    master_md5 = compute_md5(master_db)

    summary = {
        "export_timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "target_european_pool": TARGET_POOL,
        "total_listings": total_items,
        "net_added_this_cycle": total_items - before_count,
        "remaining_gap": gap,
        "completion_pct": pct,
        "gap_rule_triggered": gap > 1000,
        "total_brokerages_orgs": total_orgs,
        "total_agents": total_agents,
        "phone_coverage_count": phone_count,
        "phone_coverage_pct": round((phone_count / total_items) * 100, 2) if total_items else 0.0,
        "portal_breakdown": portal_counts,
        "country_breakdown": country_counts,
        "master_sqlite_size_bytes": master_db.stat().st_size,
        "master_sqlite_md5": master_md5,
    }

    summary_out.parent.mkdir(parents=True, exist_ok=True)
    with open(summary_out, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    logger.info(
        f"=== MERGE COMPLETE | Total: {total_items:,} ({pct}%) | "
        f"Added: +{total_items - before_count:,} | Gap: {gap:,} | MD5: {master_md5} ==="
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--master-db", required=True)
    parser.add_argument("--shards-dir", required=True)
    parser.add_argument("--summary-out", required=True)
    args = parser.parse_args()
    merge_shards(Path(args.master_db), Path(args.shards_dir), Path(args.summary_out))


if __name__ == "__main__":
    main()
