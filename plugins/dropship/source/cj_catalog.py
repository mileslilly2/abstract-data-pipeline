#!/usr/bin/env python3
# ADP Source: CJ Dropshipping product catalog

import json
import csv
from pathlib import Path
import time
from typing import Dict, Any, Iterator, Optional, List

from cj_client import make_client_from_env, CJClient


# ─────────────────────────────────────────────
# STATE HANDLING
# ─────────────────────────────────────────────

def _load_state(path: Path) -> Optional[int]:
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text())
        return data.get("last_time_start_ms")
    except Exception:
        return None


def _save_state(path: Path, ms: int) -> None:
    path.write_text(json.dumps({"last_time_start_ms": ms}, indent=2))


# ─────────────────────────────────────────────
# KEYWORD LOADING  ← NEW
# ─────────────────────────────────────────────

def load_keywords(file_path: str) -> List[str]:
    fp = Path(file_path)
    if not fp.exists():
        raise FileNotFoundError(f"keywords file not found: {file_path}")
    kws = []
    for line in fp.read_text().splitlines():
        kw = line.strip()
        if kw:
            kws.append(kw)
    return kws


# ─────────────────────────────────────────────
# MAIN SOURCE (for ADP)
# ─────────────────────────────────────────────

def run_source(config: Dict[str, Any]) -> Iterator[Dict[str, Any]]:
    """
    ADP Source:
    Streams normalized CJ Dropshipping products via listV2 search,
    looping through ALL keywords in a keywords.txt file.
    """

    # NEW: Load keywords
    keywords_file = config.get("keywords_file")
    if keywords_file:
        keywords = load_keywords(keywords_file)
    else:
        # fallback to single keyword
        kw = config.get("keyword", "")
        keywords = [kw] if kw else []

    page_start = int(config.get("page_start", 1))
    page_end = int(config.get("page_end", 1000))
    size = int(config.get("size", 100))
    state_path = Path(config.get("state_path", ".cj_state.json"))
    incremental = bool(config.get("incremental", True))

    client: CJClient = make_client_from_env()

    time_start_ms = _load_state(state_path) if incremental else None
    now_ms = int(time.time() * 1000)

    print(f"[cj_source] Starting CJ multi-keyword catalog search")
    print(f"[cj_source] keywords={len(keywords)}, pages={page_start}-{page_end}, size={size}")
    print(f"[cj_source] incremental={incremental}, last_state={time_start_ms}")

    # NEW: Loop through ALL keywords
    for keyword in keywords:
        print(f"[cj_source] Searching keyword: '{keyword}'")

        for product in client.iter_hybrid_catalog(
            keyword=keyword,
            page_start=page_start,
            page_end=page_end,
            size=size,
            time_start_ms=time_start_ms,
            time_end_ms=None,
            sleep_between_pages=0.5,
        ):
            # tag product with originating keyword
            product["search_keyword"] = keyword
            yield product

    if incremental:
        _save_state(state_path, now_ms)
        print(f"[cj_source] Saved timestamp={now_ms}")


# ─────────────────────────────────────────────
# STANDALONE MODE (BATCH SAVING)
# ─────────────────────────────────────────────

def save_batch_jsonl(path: Path, batch: list):
    with path.open("a") as f:
        for record in batch:
            f.write(json.dumps(record) + "\n")


def save_batch_csv(path: Path, batch: list, field_order=None):
    write_header = not path.exists()
    if field_order is None:
        field_order = sorted(batch[0].keys())
    with path.open("a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=field_order)
        if write_header:
            writer.writeheader()
        for row in batch:
            writer.writerow(row)


if __name__ == "__main__":
    """
    Standalone test: Reads keywords.txt and saves all products.
    """

    batch_size = 500
    output_path = Path("cj_products.jsonl")
    use_csv = False

    # Remove previous output
    output_path.unlink(missing_ok=True)

    test_config = {
        "keywords_file": "super_keywords.txt",    # ← NEW: use your keyword list
        "page_start": 1,
        "page_end": 3,
        "size": 50,
        "incremental": False,
    }

    print("[cj_source] Running multi-keyword batch-save...")

    batch = []
    count = 0

    for product in run_source(test_config):
        batch.append(product)
        count += 1

        if len(batch) >= batch_size:
            print(f"[cj_source] Saving batch of {len(batch)}...")
            if use_csv:
                save_batch_csv(output_path, batch)
            else:
                save_batch_jsonl(output_path, batch)
            batch = []

    if batch:
        print(f"[cj_source] Saving final batch of {len(batch)}...")
        if use_csv:
            save_batch_csv(output_path, batch)
        else:
            save_batch_jsonl(output_path, batch)

    print(f"[cj_source] DONE. Saved total {count} records → {output_path}")
