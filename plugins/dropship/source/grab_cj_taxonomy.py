#!/usr/bin/env python3
# Advanced CJ Taxonomy Crawler: Dedupe + Parquet + Checkpoints

import json
import time
import pandas as pd
from pathlib import Path
from cj_client import make_client_from_env
from typing import List, Dict, Any

CHECKPOINT_PATH = Path("cj_taxonomy_checkpoint.json")
PARQUET_PATH = Path("cj_taxonomy_products.parquet")
PARQUET_GROUP_SIZE = 500  # rows before flushing to Parquet


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
    if not buffer:
        return
    df = pd.DataFrame(buffer)
    df.to_parquet(
        PARQUET_PATH,
        engine="pyarrow",
        append=PARQUET_PATH.exists(),
    )
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
