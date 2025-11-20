"""BAM metadata snapshot + page-plan generator.

This utility hits the same BAM endpoints as the scraper, records feed
metadata into Postgres, and emits a delta plan (bam_delta_plan.json by
default) for sniper_engine.py to follow on the next run.
"""

from __future__ import annotations

import json
import math
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, MutableMapping, Sequence

from dotenv import load_dotenv
from zoneinfo import ZoneInfo

from sniper_engine import (  # type: ignore
    AUCTION_API_URL,
    CATEGORY_CONFIGS,
    PAGE_SIZE,
    REGULAR_API_URL,
    _post_with_retry,
    get_db_connection,
    to_float,
)

load_dotenv()

PLAN_FILE = Path(os.getenv("BAM_PAGE_PLAN_FILE", "bam_delta_plan.json")).expanduser()
TAIL_RECHECK_PAGES = max(0, int(os.getenv("BAM_TAIL_RECHECK_PAGES", "3")))
HEAD_REFRESH_PAGES = max(0, int(os.getenv("BAM_HEAD_REFRESH_PAGES", "2")))
TH_ZONE = ZoneInfo("Asia/Bangkok")
SNAPSHOT_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS bam_feed_snapshot (
    id SERIAL PRIMARY KEY,
    feed_type TEXT NOT NULL,
    category TEXT NOT NULL,
    total_records INT,
    page_count INT,
    checked_at TIMESTAMPTZ DEFAULT NOW()
);
"""
def ensure_snapshot_table(conn) -> None:
    cur = conn.cursor()
    cur.execute(SNAPSHOT_TABLE_SQL)
    conn.commit()



@dataclass
class FeedSnapshot:
    feed_type: str
    category: str
    total_records: int
    page_count: int


def _build_regular_payload(config: Mapping[str, object], page_number: int = 1) -> Dict[str, object]:
    return {
        "assetTypes": config.get("asset_types", []),
        "keyword": None,
        "provinces": [],
        "districts": [],
        "pageNumber": page_number,
        "pageSize": PAGE_SIZE,
        "orderBy": "DEFAULT",
    }


def _build_auction_payload(page_number: int = 1) -> Dict[str, object]:
    return {"pageNumber": page_number, "pageSize": PAGE_SIZE}


def _extract_totals(result: Mapping[str, object]) -> int:
    total = to_float(result.get("totalData"), 0) or 0
    if total <= 0:
        data = result.get("data") or []
        if isinstance(data, Sequence):
            total = len(data)
    return int(total)


def collect_current_metadata() -> List[FeedSnapshot]:
    snapshots: List[FeedSnapshot] = []
    for config in CATEGORY_CONFIGS:
        label = config["label"]
        payload = _build_regular_payload(config)
        response = _post_with_retry(REGULAR_API_URL, payload, label, 1)
        result = response.json()
        total_records = _extract_totals(result)
        page_count = math.ceil(total_records / PAGE_SIZE) if total_records else 0
        snapshots.append(
            FeedSnapshot(
                feed_type="regular",
                category=label,
                total_records=total_records,
                page_count=page_count,
            )
        )

    payload = _build_auction_payload()
    response = _post_with_retry(AUCTION_API_URL, payload, "Auction", 1)
    result = response.json()
    total_records = _extract_totals(result)
    page_count = math.ceil(total_records / PAGE_SIZE) if total_records else 0
    snapshots.append(
        FeedSnapshot(
            feed_type="auction",
            category="Auction",
            total_records=total_records,
            page_count=page_count,
        )
    )
    return snapshots


def fetch_latest_snapshot_map(conn) -> Dict[tuple, FeedSnapshot]:
    cur = conn.cursor()
    cur.execute(
        """
        WITH ranked AS (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY feed_type, category ORDER BY checked_at DESC) AS rk
            FROM bam_feed_snapshot
        )
        SELECT feed_type, category, total_records, page_count
        FROM ranked
        WHERE rk = 1;
        """
    )
    rows = cur.fetchall()
    latest: Dict[tuple, FeedSnapshot] = {}
    for feed_type, category, total_records, page_count in rows:
        latest[(feed_type, category)] = FeedSnapshot(
            feed_type=feed_type,
            category=category,
            total_records=int(total_records or 0),
            page_count=int(page_count or 0),
        )
    return latest


def persist_snapshots(conn, snapshots: Iterable[FeedSnapshot]) -> None:
    cur = conn.cursor()
    params = [
        (snap.feed_type, snap.category, snap.total_records, snap.page_count)
        for snap in snapshots
    ]
    cur.executemany(
        """
        INSERT INTO bam_feed_snapshot (feed_type, category, total_records, page_count, checked_at)
        VALUES (%s, %s, %s, %s, NOW());
        """,
        params,
    )
    conn.commit()


def _range_set(start: int, end: int) -> Iterable[int]:
    return range(start, end + 1)


def _compute_plan_pages(current: FeedSnapshot, previous: FeedSnapshot | None) -> List[int]:
    pages: MutableMapping[int, None] = {}
    current_pages = max(0, current.page_count)
    if current_pages <= 0:
        return []

    head_limit = min(current_pages, HEAD_REFRESH_PAGES)
    if head_limit > 0:
        for page in _range_set(1, head_limit):
            pages.setdefault(page, None)

    tail_limit = min(current_pages, TAIL_RECHECK_PAGES)
    if tail_limit > 0:
        tail_start = max(1, current_pages - tail_limit + 1)
        for page in _range_set(tail_start, current_pages):
            pages.setdefault(page, None)

    if previous:
        prev_pages = max(0, previous.page_count)
        if current_pages > prev_pages:
            start = prev_pages + 1
            if start <= current_pages:
                for page in _range_set(start, current_pages):
                    pages.setdefault(page, None)
        elif prev_pages > current_pages:
            shrink_tail = min(current_pages, TAIL_RECHECK_PAGES * 2)
            if shrink_tail > 0:
                shrink_start = max(1, current_pages - shrink_tail + 1)
                if shrink_start <= current_pages:
                    for page in _range_set(shrink_start, current_pages):
                        pages.setdefault(page, None)

    return sorted(pages.keys())


def build_page_plan(
    previous: Mapping[tuple, FeedSnapshot],
    current: Sequence[FeedSnapshot],
) -> Dict[str, object]:
    plan: Dict[str, object] = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "generated_at_th": datetime.now(TH_ZONE).isoformat(),
        "page_size": PAGE_SIZE,
        "head_refresh_pages": HEAD_REFRESH_PAGES,
        "tail_recheck_pages": TAIL_RECHECK_PAGES,
        "regular": {},
        "auction": [],
    }

    regular_plan: Dict[str, List[int]] = {}
    auction_plan: List[int] = []

    for snap in current:
        prev = previous.get((snap.feed_type, snap.category))
        pages = _compute_plan_pages(snap, prev)
        if not pages:
            continue
        if snap.feed_type == "regular":
            regular_plan[snap.category] = pages
        else:
            auction_plan = pages

    if regular_plan:
        plan["regular"] = regular_plan
    if auction_plan:
        plan["auction"] = auction_plan
    return plan


def write_plan_file(plan: Mapping[str, object]) -> None:
    PLAN_FILE.parent.mkdir(parents=True, exist_ok=True)
    with PLAN_FILE.open("w", encoding="utf-8") as handle:
        json.dump(plan, handle, ensure_ascii=False, indent=2)


def main() -> None:
    print("📊 Capturing BAM feed metadata…")
    conn = get_db_connection()
    try:
        ensure_snapshot_table(conn)
        previous_map = fetch_latest_snapshot_map(conn)
        current_snapshots = collect_current_metadata()
        persist_snapshots(conn, current_snapshots)
    finally:
        conn.close()

    plan = build_page_plan(previous_map, current_snapshots)
    write_plan_file(plan)

    print(
        "✅ Snapshot complete — plan written to"
        f" {PLAN_FILE} (head={HEAD_REFRESH_PAGES}, tail={TAIL_RECHECK_PAGES})."
    )


if __name__ == "__main__":
    main()
