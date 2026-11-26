"""
Engine v4.5 Dynamic 20-Runner Work-Queue Dispatcher & Post-Wave Checkpoint Merger.

Subcommands:
  build  --manifest v4_global_portals.json --state-dir /tmp/v4_state --max-runners 20
         Reads live checkpoint states from Drive, skips COMPLETED / CAPACITY_CAP_REVIEW /
         WAF_BLOCKED_DETAIL / NO_SITEMAP_FOUND portals, and dynamically allocates all 20
         GitHub Actions runners across IN_PROGRESS mega-portals (multi-runner sharding) +
         unstarted portals -> 85-95% fleet utilization instead of 2.9%!

  merge  --state-dir /tmp/v4_state --rclone-root gdrive:Global_Cloud_Swarm_Exports/portals_v4
         Idempotently merges checkpoint_p*.json + portal_totals_p*.json into canonical
         checkpoint.json and portal_totals.json for all sharded portals so the live dashboard
         always shows exact combined totals with zero double-counting.
"""

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Set

SKIP_STATUSES = {"COMPLETED", "CAPACITY_CAP_REVIEW", "WAF_BLOCKED_DETAIL", "NO_SITEMAP_FOUND"}


def load_portal_state(portal_dir: Path) -> Dict[str, Any]:
    """Read merged state for a portal from base checkpoint.json + any checkpoint_p*.json."""
    if not portal_dir.exists():
        return {"exists": False, "status": "UNSTARTED", "rows": 0, "sm_done": 0, "sm_total": 0, "rem_sm": 0}

    base_status = "IN_PROGRESS"
    completed_sm: Set[str] = set()
    sm_total = 0
    rows = 0
    part_totals: Dict[str, int] = {}

    base_cp = portal_dir / "checkpoint.json"
    if base_cp.exists():
        try:
            c = json.loads(base_cp.read_text(encoding="utf-8"))
            base_status = c.get("status", "IN_PROGRESS")
            completed_sm.update(c.get("completed_sitemaps", []))
            sm_total = max(sm_total, int(c.get("total_sitemaps", 0)))
            rows = max(rows, int(c.get("total_extracted", 0)))
            part_totals = dict(c.get("partition_totals", {}))
        except Exception:
            pass

    for pf in sorted(portal_dir.glob("checkpoint_p*.json")):
        try:
            pc = json.loads(pf.read_text(encoding="utf-8"))
            completed_sm.update(pc.get("completed_sitemaps", []))
            sm_total = max(sm_total, int(pc.get("total_sitemaps", 0)))
            p_key = pf.stem
            p_rows = int(pc.get("total_extracted", 0))
            if p_rows > int(part_totals.get(p_key, 0)):
                rows += p_rows - int(part_totals.get(p_key, 0))
        except Exception:
            pass

    # Fallback to portal_totals.json if checkpoint didn't exist
    pt = portal_dir / "portal_totals.json"
    if pt.exists() and rows == 0:
        try:
            t = json.loads(pt.read_text(encoding="utf-8"))
            rows = max(rows, int(t.get("total_listings", 0)))
            sm_total = max(sm_total, int(t.get("sitemaps_total", 0)))
            if base_status == "IN_PROGRESS" and t.get("status"):
                base_status = t["status"]
        except Exception:
            pass

    sm_done = len(completed_sm)
    rem_sm = max(0, sm_total - sm_done) if sm_total > 0 else 0
    return {
        "exists": True,
        "status": base_status,
        "rows": rows,
        "sm_done": sm_done,
        "sm_total": sm_total,
        "rem_sm": rem_sm,
    }


def build_dynamic_matrix(manifest_path: Path, state_dir: Path, max_runners: int = 20) -> List[Dict[str, Any]]:
    data = json.loads(manifest_path.read_text(encoding="utf-8"))
    # Flatten all unique portals across v4_batch_1..v4_batch_8
    portals_by_domain: Dict[str, Dict[str, Any]] = {}
    for batch in data.get("batches", {}).values():
        for entry in batch:
            dom = entry["domain"].strip().lower()
            if dom not in portals_by_domain:
                portals_by_domain[dom] = {
                    "domain": dom,
                    "cc": entry.get("cc", "US"),
                    "capacity": int(entry.get("capacity", 0)),
                }

    active_with_data: List[Dict[str, Any]] = []
    unstarted: List[Dict[str, Any]] = []
    active_zero_rows: List[Dict[str, Any]] = []
    skipped_counts: Dict[str, int] = {}

    for dom, meta in portals_by_domain.items():
        st = load_portal_state(state_dir / dom)
        status = st["status"]
        if status in SKIP_STATUSES:
            skipped_counts[status] = skipped_counts.get(status, 0) + 1
            continue

        item = {**meta, **st}
        if not st["exists"]:
            unstarted.append(item)
        elif st["rows"] > 0 and (st["rem_sm"] > 0 or st["sm_total"] == 0):
            active_with_data.append(item)
        elif st["rem_sm"] > 0 or st["sm_total"] == 0:
            active_zero_rows.append(item)
        else:
            skipped_counts["DONE_ALL_SM"] = skipped_counts.get("DONE_ALL_SM", 0) + 1

    # Priority ordering:
    # 1. Active mega-portals with proven extraction (sorted by remaining sitemaps, then capacity)
    active_with_data.sort(key=lambda x: (-x["rem_sm"], -x["capacity"]))
    # 2. Unstarted portals (sorted by capacity descending)
    unstarted.sort(key=lambda x: -x["capacity"])
    # 3. In-progress portals with 0 rows yet (sorted by remaining sitemaps descending)
    active_zero_rows.sort(key=lambda x: (-x["rem_sm"], -x["capacity"]))

    matrix: List[Dict[str, Any]] = []

    def add_sharded(portal: Dict[str, Any], desired_parts: int) -> None:
        slots_left = max_runners - len(matrix)
        if slots_left <= 0:
            return
        k = min(desired_parts, slots_left)
        if portal["rem_sm"] > 0:
            k = min(k, portal["rem_sm"])
        k = max(1, k)
        for p in range(k):
            matrix.append({
                "domain": portal["domain"],
                "cc": portal["cc"],
                "capacity": portal["capacity"],
                "partition_id": p,
                "num_partitions": k,
                "label": f"{portal['domain']} (p{p + 1}/{k})" if k > 1 else portal["domain"],
            })

    # Tier A: Allocate multi-runner partitions to proven IN_PROGRESS mega-portals
    for p in active_with_data:
        if len(matrix) >= max_runners:
            break
        rem = p["rem_sm"]
        if rem >= 5000:
            parts = 6
        elif rem >= 200:
            parts = 3
        elif rem >= 50:
            parts = 2
        else:
            parts = 1
        add_sharded(p, parts)

    # Tier B: Fill remaining slots with UNSTARTED portals (1 runner each)
    for p in unstarted:
        if len(matrix) >= max_runners:
            break
        add_sharded(p, 1)

    # Tier C: Fill any leftover slots with IN_PROGRESS zero-row portals
    for p in active_zero_rows:
        if len(matrix) >= max_runners:
            break
        add_sharded(p, 1)

    print(
        f"[v4.5 Dispatcher] Total={len(portals_by_domain)} | Skipped={skipped_counts} | "
        f"ActiveData={len(active_with_data)} | Unstarted={len(unstarted)} | ActiveZero={len(active_zero_rows)} | "
        f"Scheduled={len(matrix)}/{max_runners} runners",
        file=sys.stderr,
    )
    return matrix


def merge_sharded_checkpoints(state_dir: Path, rclone_root: str) -> None:
    """Idempotently merge checkpoint_p*.json into canonical checkpoint.json + portal_totals.json."""
    if not state_dir.exists():
        return

    merged_count = 0
    for portal_dir in sorted(state_dir.iterdir()):
        if not portal_dir.is_dir():
            continue
        part_files = sorted(portal_dir.glob("checkpoint_p*.json"))
        if not part_files:
            continue

        domain = portal_dir.name
        base_cp_path = portal_dir / "checkpoint.json"
        if base_cp_path.exists():
            try:
                base = json.loads(base_cp_path.read_text(encoding="utf-8"))
            except Exception:
                base = {}
        else:
            base = {}

        completed_sm: Set[str] = set(base.get("completed_sitemaps", []))
        sm_total = int(base.get("total_sitemaps", 0))
        base_pre_shard_rows = int(base.get("base_pre_shard_extracted", base.get("total_extracted", 0)))
        base_pre_shard_shards = int(base.get("base_pre_shard_shards", base.get("shards_uploaded", 0)))
        part_totals: Dict[str, int] = dict(base.get("partition_totals", {}))
        part_shards: Dict[str, int] = dict(base.get("partition_shards", {}))

        # If base had already incorporated partition_totals previously, keep base_pre_shard_extracted intact
        if "base_pre_shard_extracted" not in base:
            base_pre_shard_rows = int(base.get("total_extracted", 0))
            base_pre_shard_shards = int(base.get("shards_uploaded", 0))

        cc = base.get("country_code", "US")
        verdicts: List[str] = []
        accs: List[float] = []
        statuses: Set[str] = set()

        for pf in part_files:
            try:
                pc = json.loads(pf.read_text(encoding="utf-8"))
            except Exception:
                continue
            pkey = pf.stem
            cc = pc.get("country_code", cc)
            completed_sm.update(pc.get("completed_sitemaps", []))
            sm_total = max(sm_total, int(pc.get("total_sitemaps", 0)))
            part_totals[pkey] = max(int(part_totals.get(pkey, 0)), int(pc.get("total_extracted", 0)))
            part_shards[pkey] = max(int(part_shards.get(pkey, 0)), int(pc.get("shards_uploaded", 0)))
            if pc.get("last_verdict"):
                verdicts.append(pc["last_verdict"])
            if isinstance(pc.get("last_avail_accuracy_pct"), (int, float)):
                accs.append(float(pc["last_avail_accuracy_pct"]))
            if pc.get("status"):
                statuses.add(pc["status"])

        total_rows = base_pre_shard_rows + sum(part_totals.values())
        total_shards = base_pre_shard_shards + sum(part_shards.values())
        avg_acc = round(sum(accs) / len(accs), 1) if accs else base.get("last_avail_accuracy_pct")
        verdict = "PASS" if (avg_acc is not None and avg_acc >= 85.0) else (verdicts[-1] if verdicts else base.get("last_verdict", "-"))

        if "CAPACITY_CAP_REVIEW" in statuses:
            final_status = "CAPACITY_CAP_REVIEW"
        elif sm_total > 0 and len(completed_sm) >= sm_total:
            final_status = "COMPLETED"
        elif statuses and statuses == {"WAF_BLOCKED_DETAIL"}:
            final_status = "WAF_BLOCKED_DETAIL"
        else:
            final_status = "IN_PROGRESS"

        now_iso = datetime.now(timezone.utc).isoformat()
        merged_cp = {
            "portal_domain": domain,
            "country_code": cc,
            "engine": "v4.5_sharded",
            "base_pre_shard_extracted": base_pre_shard_rows,
            "base_pre_shard_shards": base_pre_shard_shards,
            "partition_totals": part_totals,
            "partition_shards": part_shards,
            "total_extracted": total_rows,
            "shards_uploaded": total_shards,
            "completed_sitemaps": sorted(completed_sm),
            "total_sitemaps": sm_total,
            "last_verdict": verdict,
            "last_avail_accuracy_pct": avg_acc,
            "status": final_status,
            "updated_at_utc": now_iso,
        }
        base_cp_path.write_text(json.dumps(merged_cp, indent=2), encoding="utf-8")

        totals_path = portal_dir / "portal_totals.json"
        totals_path.write_text(json.dumps({
            "portal_domain": domain,
            "country_code": cc,
            "engine": "v4.5_sharded",
            "total_listings": total_rows,
            "shards_uploaded": total_shards,
            "sitemaps_done": len(completed_sm),
            "sitemaps_total": sm_total,
            "verdict": verdict,
            "available_field_accuracy_pct": avg_acc,
            "status": final_status,
            "updated_at_utc": now_iso,
        }, indent=2), encoding="utf-8")

        if rclone_root:
            subprocess.run(["rclone", "copyto", "--checksum", str(base_cp_path), f"{rclone_root}/{domain}/checkpoint.json"], check=False)
            subprocess.run(["rclone", "copyto", "--checksum", str(totals_path), f"{rclone_root}/{domain}/portal_totals.json"], check=False)

        print(f"[v4.5 Merger] {domain}: rows={total_rows:,} | shards={total_shards} | sm={len(completed_sm)}/{sm_total} | status={final_status}", file=sys.stderr)
        merged_count += 1

    print(f"[v4.5 Merger] Merged {merged_count} sharded portals.", file=sys.stderr)


def main() -> None:
    parser = argparse.ArgumentParser(description="Engine v4.5 Dynamic Dispatcher & Checkpoint Merger")
    sub = parser.add_subparsers(dest="cmd", required=True)

    b = sub.add_parser("build")
    b.add_argument("--manifest", default="v4_global_portals.json")
    b.add_argument("--state-dir", required=True)
    b.add_argument("--max-runners", type=int, default=20)

    m = sub.add_parser("merge")
    m.add_argument("--state-dir", required=True)
    m.add_argument("--rclone-root", default="")

    args = parser.parse_args()
    if args.cmd == "build":
        matrix = build_dynamic_matrix(Path(args.manifest), Path(args.state_dir), args.max_runners)
        print(json.dumps({"include": matrix}, separators=(",", ":")))
    elif args.cmd == "merge":
        merge_sharded_checkpoints(Path(args.state_dir), args.rclone_root)


if __name__ == "__main__":
    main()
