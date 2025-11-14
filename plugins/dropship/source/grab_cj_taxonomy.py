#!/usr/bin/env python3
# Advanced CJ Taxonomy Crawler: Dedupe + Parquet + Checkpoints

import json
import time

import requests
import pandas as pd
from pathlib import Path
from cj_client import make_client_from_env
from typing import List, Dict, Any

CHECKPOINT_PATH = Path("cj_taxonomy_checkpoint.json")
PARQUET_PATH = Path("cj_taxonomy_products.parquet")
PARQUET_GROUP_SIZE = 500  # rows before flushing to Parquet



def safe_get_with_retry(session, url, params, headers, max_retries=8):
    """
    Wrap GET requests with retry + exponential backoff on 429 Too Many Requests.
    Works with CJ Dropshipping rate limits.
    """
    for attempt in range(max_retries):

        try:
            resp = session.get(url, params=params, headers=headers, timeout=30)

            # If not rate-limited, return immediately
            if resp.status_code != 429:
                resp.raise_for_status()
                return resp

            # Handle 429 Too Many Requests
            retry_after = int(resp.headers.get("Retry-After", 0))
            backoff = min(2 ** attempt, 30)  # exponential backoff, capped at 30s
            wait_time = max(retry_after, backoff)

            print(f"[CJ 429] Rate limited. Waiting {wait_time}s (retry {attempt+1}/{max_retries})")
            time.sleep(wait_time)

        except requests.exceptions.RequestException as e:
            # Network / transient failures
            print(f"[CJ WARN] Request failed: {e}. Retrying...")
            time.sleep(min(2 ** attempt, 30))

    # If all retries exhausted
    raise RuntimeError(f"[CJ ERROR] Max retries reached for GET {url}")


# ------------------------------------------------------------
# Load Google Taxonomy
# ------------------------------------------------------------
def load_taxonomy(path: str) -> List[str]:
    fp = Path(path)
    if not fp.exists():
        raise FileNotFoundError(f"Taxonomy file not found: {path}")
    lines = fp.read_text().splitlines()
    return [l.strip() for l in lines if l.strip()]


# ------------------------------------------------------------
# Checkpoint Handling
# ------------------------------------------------------------
def load_checkpoint() -> Dict[str, Any]:
    if CHECKPOINT_PATH.exists():
        try:
            return json.loads(CHECKPOINT_PATH.read_text())
        except:
            pass
    return {"last_category_index": 0, "product_count": 0}


def save_checkpoint(index: int, count: int):
    data = {"last_category_index": index, "product_count": count}
    CHECKPOINT_PATH.write_text(json.dumps(data, indent=2))


# ------------------------------------------------------------
# Flush parquet buffer
# ------------------------------------------------------------
def flush_parquet(buffer: List[Dict[str, Any]]):
    """
    Convert buffer to Arrow table and write using ParquetWriter.
    This works even on older pyarrow versions that don't support append=True.
    """
    global parquet_schema, parquet_writer

    if not buffer:
        return

    # Convert list of dicts → Arrow table
    df = pd.DataFrame(buffer)
    table = pd.Table.from_pandas(df, preserve_index=False)

    # First time: initialize writer with schema
    if parquet_writer is None:
        parquet_schema = table.schema
        parquet_writer = pq.ParquetWriter(
            PARQUET_PATH,
            parquet_schema,
            compression="snappy",
        )
        print(f"[CJ] Created new Parquet file: {PARQUET_PATH}")

    # Add row group to existing parquet file
    parquet_writer.write_table(table)
    print(f"[CJ] Wrote {len(buffer)} rows.")

    # Clear buffer
    buffer.clear()
# ------------------------------------------------------------
# MAIN
# ------------------------------------------------------------
def grab_by_taxonomy_advanced(
    taxonomy_path: str,
    page_start=1,
    page_end=5,
    size=100,
    sleep=0.4,
):

    client = make_client_from_env()
    categories = load_taxonomy(taxonomy_path)
    checkpoint = load_checkpoint()

    print(f"[CJ] Loaded taxonomy categories: {len(categories)}")
    print(f"[CJ] Resuming at index: {checkpoint['last_category_index']}")

    seen_ids = set()
    parquet_buffer = []
    total_saved = checkpoint["product_count"]

    # Resume from last category index
    for idx in range(checkpoint["last_category_index"], len(categories)):
        cat = categories[idx]
        keyword = cat.split(">")[-1].strip()  # leaf keyword

        print(f"\n[CJ] ({idx}/{len(categories)}) Searching for: {keyword}")

        for product in client.iter_hybrid_catalog(
            keyword=keyword,
            page_start=page_start,
            page_end=page_end,
            size=size,
            sleep_between_pages=sleep,
        ):
            pid = product.get("id")
            if not pid:
                continue

            # Dedupe
            if pid in seen_ids:
                continue
            seen_ids.add(pid)

            # Add metadata
            product["google_category"] = cat
            product["google_keyword"] = keyword

            parquet_buffer.append(product)
            total_saved += 1

            # flush buffer
            if len(parquet_buffer) >= PARQUET_GROUP_SIZE:
                print(f"[CJ] Flushing {len(parquet_buffer)} rows to parquet…")
                flush_parquet(parquet_buffer)

        # Save checkpoint after each category
        save_checkpoint(idx, total_saved)

    # Final flush
    flush_parquet(parquet_buffer)

    print(f"\n[CJ] DONE. Total unique products saved: {total_saved}")
    print(f"[CJ] Output → {PARQUET_PATH}")
        # final flush
    flush_parquet(parquet_buffer)

    # close writer
    if parquet_writer is not None:
        parquet_writer.close()
        print("[CJ] Parquet writer closed.")



# ------------------------------------------------------------
# Run
# ------------------------------------------------------------
if __name__ == "__main__":
    grab_by_taxonomy_advanced(
        taxonomy_path="datasets/google_taxonomy.txt",
        page_start=1,
        page_end=3,
        size=100,
        sleep=0.4,
    )
