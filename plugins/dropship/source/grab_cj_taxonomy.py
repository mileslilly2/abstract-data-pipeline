#!/usr/bin/env python3
# Advanced CJ Taxonomy Crawler: Dedupe + Parquet + Checkpoints

import json
import time
import requests
import pandas as pd
from pathlib import Path
from typing import List, Dict, Any

import pyarrow as pa
import pyarrow.parquet as pq

from cj_client import make_client_from_env, CJRateLimitError

# ------------------------------------------------------------
# CONSTANTS
# ------------------------------------------------------------
CHECKPOINT_PATH = Path("cj_taxonomy_checkpoint.json")
PARQUET_PATH = Path("cj_taxonomy_products.parquet")
PARQUET_GROUP_SIZE = 500  # flush after this many rows


# ------------------------------------------------------------
# 429-SAFE RETRY WRAPPER (kept for future use if needed)
# ------------------------------------------------------------
def safe_get_with_retry(session, url, params, headers, max_retries=8):
    for attempt in range(max_retries):
        try:
            resp = session.get(url, params=params, headers=headers, timeout=30)

            if resp.status_code != 429:   # not rate-limited
                resp.raise_for_status()
                return resp

            retry_after = int(resp.headers.get("Retry-After", 0))
            backoff = min(2 ** attempt, 30)
            wait_time = max(retry_after, backoff)

            print(f"[CJ 429] Rate limited. Waiting {wait_time}s (retry {attempt+1}/{max_retries})")
            time.sleep(wait_time)

        except requests.exceptions.RequestException as e:
            print(f"[CJ WARN] Network error: {e}. Retrying...")
            time.sleep(min(2 ** attempt, 30))

    raise RuntimeError(f"[CJ ERROR] Max retries exceeded for GET {url}")


# ------------------------------------------------------------
# LOAD TAXONOMY FILE
# ------------------------------------------------------------
def load_taxonomy(path: str) -> List[str]:
    fp = Path(path)
    if not fp.exists():
        raise FileNotFoundError(f"Taxonomy file not found: {path}")

    categories = []
    for line in fp.read_text().splitlines():
        line = line.strip()

        # Skip comments or headers
        if not line or line.startswith("#") or line.startswith("//"):
            continue

        # Skip any lines with version tags
        if "Taxonomy" in line or "Version" in line:
            continue

        categories.append(line)

    return categories


# ------------------------------------------------------------
# CHECKPOINT HANDLING
# ------------------------------------------------------------
def load_checkpoint() -> Dict[str, Any]:
    if CHECKPOINT_PATH.exists():
        try:
            return json.loads(CHECKPOINT_PATH.read_text())
        except Exception:
            pass
    return {"last_category_index": 0, "product_count": 0}


def save_checkpoint(idx: int, count: int):
    CHECKPOINT_PATH.write_text(
        json.dumps({"last_category_index": idx, "product_count": count}, indent=2)
    )


# ------------------------------------------------------------
# PARQUET WRITER
# ------------------------------------------------------------
parquet_writer = None
parquet_schema = None


def flush_parquet(buffer: List[Dict[str, Any]]):
    """Flushes buffer to Parquet using ParquetWriter (fully compatible)."""
    global parquet_writer, parquet_schema

    if not buffer:
        return

    df = pd.DataFrame(buffer)
    table = pa.Table.from_pandas(df, preserve_index=False)

    # First batch initializes writer
    if parquet_writer is None:
        parquet_schema = table.schema
        parquet_writer = pq.ParquetWriter(
            PARQUET_PATH,
            parquet_schema,
            compression="snappy",
        )
        print(f"[CJ] Created Parquet file: {PARQUET_PATH}")

    parquet_writer.write_table(table)
    print(f"[CJ] Wrote {len(buffer)} rows → Parquet")

    buffer.clear()


# ------------------------------------------------------------
# MAIN CRAWLER
# ------------------------------------------------------------
def grab_by_taxonomy_advanced(
    taxonomy_path: str,
    page_start: int = 1,
    page_end: int = 3,
    size: int = 100,
    sleep: float = 0.5,
):

    client = make_client_from_env()
    categories = load_taxonomy(taxonomy_path)
    checkpoint = load_checkpoint()

    print(f"[CJ] Loaded {len(categories)} taxonomy categories")
    print(f"[CJ] Resuming at index {checkpoint['last_category_index']}")

    seen_ids = set()
    parquet_buffer: List[Dict[str, Any]] = []
    total_saved = checkpoint["product_count"]

    # Start from checkpoint index
    for idx in range(checkpoint["last_category_index"], len(categories)):
        cat = categories[idx]
        keyword = cat.split(">")[-1].strip()

        print(f"\n[CJ] ({idx}/{len(categories)}) Searching keyword: {keyword}")

        try:
            # iterate pages for this keyword
            for product in client.iter_hybrid_catalog(
                keyword=keyword,
                page_start=page_start,
                page_end=page_end,
                size=size,
                sleep_between_pages=sleep,
            ):
                if not isinstance(product, dict):
                    continue

                pid = product.get("id")
                if not pid:
                    continue

                if pid in seen_ids:
                    continue  # dedupe

                seen_ids.add(pid)

                # tag with taxonomy info
                product["google_category"] = cat
                product["google_keyword"] = keyword

                parquet_buffer.append(product)
                total_saved += 1

                if len(parquet_buffer) >= PARQUET_GROUP_SIZE:
                    flush_parquet(parquet_buffer)

        except CJRateLimitError as e:
            # If CJ keeps 429-ing for this keyword, log it, checkpoint, and move on.
            print(
                f"[CJ WARN] Rate limit encountered for keyword '{keyword}' "
                f"(category index {idx}). Skipping this category for now.\n"
                f"Details: {e}"
            )
            # optional: short sleep to avoid hammering if it's a global cooldown
            time.sleep(60)

        # Always checkpoint after each category (success or skipped)
        save_checkpoint(idx + 1, total_saved)
        print(f"[CJ] Checkpoint saved at category index {idx+1}, total_saved={total_saved}")

    # Final flush
    flush_parquet(parquet_buffer)

    if parquet_writer is not None:
        parquet_writer.close()
        print("[CJ] Parquet writer closed.")

    print(f"[CJ] DONE. Saved {total_saved} unique products.")


# ------------------------------------------------------------
# ENTRYPOINT
# ------------------------------------------------------------
if __name__ == "__main__":
    grab_by_taxonomy_advanced(
        taxonomy_path="datasets/google_taxonomy.txt",
        page_start=1,
        page_end=2,
        size=100,
        sleep=0.5,
    )
