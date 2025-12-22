#!/usr/bin/env python3

import json
import time
from pathlib import Path
from typing import Dict, List

from cj_client import make_client_from_env

OUT = Path("cj_dump")
OUT.mkdir(exist_ok=True)
OUT_JSONL = OUT / "electronics.jsonl"

# VERY GENERIC CATEGORY WORDS (CJ WILL RETURN PRODUCTS FOR THESE)
CATEGORY_PROBES = [
    "electronics",
    "video",
    "audio",
    "computer",
    "accessory",
    "controller",
    "game",
    "console",
    "adapter",
    "cable",
    "charger",
]

def main():
    client = make_client_from_env()
    seen = set()

    with OUT_JSONL.open("a", encoding="utf-8") as f:
        for keyword in CATEGORY_PROBES:
            print(f"[CJ] Pulling broad category: {keyword}")

            page = 1
            while page <= 20:   # <-- BIG PULL
                products = client.search_products(
                    keyword=keyword,
                    page=page,
                    size=100,
                )

                if not products:
                    break

                for p in products:
                    pid = p.get("id") or p.get("pid")
                    if not pid or pid in seen:
                        continue

                    seen.add(pid)
                    p["_source_keyword"] = keyword
                    f.write(json.dumps(p) + "\n")

                print(f"  page {page} → total {len(seen)}")
                page += 1
                time.sleep(0.5)

    print(f"DONE. Saved {len(seen)} products.")

if __name__ == "__main__":
    main()
