#!/usr/bin/env python3
"""
cj_pull_retro_products.py
Pull RETRO GAMING products directly from CJ Dropshipping.
No database. No taxonomy. Just direct retro searches.

- Searches retro-related keywords
- Fetches full product detail for each pid
- Dedupe
- Save JSONL, Parquet, Shopify CSV
"""

import os
import json
import time
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from typing import List, Dict, Any

import requests
from cj_client import make_client_from_env


# ---------------------------------------------------------------
# Retro keywords for direct CJ API searching
# ---------------------------------------------------------------

RETRO_KEYWORDS = [
    "retro gaming",
    "retro console",
    "game console",
    "handheld game",
    "gamepad",
    "controller",
    "arcade",
    "joystick",
    "emulator",
    "n64",
    "snes",
    "nes",
    "ps1",
    "ps2",
    "sega",
    "genesis",
    "dreamcast",
    "gba",
    "gbc",
    "game boy",
    "raspberry pi",
]


# ---------------------------------------------------------------
# Output paths
# ---------------------------------------------------------------

OUT_DIR = Path("cj_retro_outputs")
OUT_DIR.mkdir(exist_ok=True)

JSONL_PATH = OUT_DIR / "cj_retro.jsonl"
PARQUET_PATH = OUT_DIR / "cj_retro.parquet"
SHOPIFY_CSV_PATH = OUT_DIR / "cj_retro_shopify.csv"


# ---------------------------------------------------------------
# CJ API wrappers
# ---------------------------------------------------------------

def cj_search(client, keyword: str, page=1, size=50):
    """
    Call CJ /product/list (search)
    """
    url = f"{client.base_url}/product/list"
    headers = {"CJ-Access-Token": client.access_token}
    params = {"pageNum": page, "pageSize": size, "keyword": keyword}

    resp = client.session.get(url, params=params, headers=headers, timeout=20)
    if resp.status_code == 429:
        raise RuntimeError("Rate limited (429). Try again later.")

    resp.raise_for_status()
    data = resp.json()
    result = data.get("data", {}).get("result", [])
    return result


def cj_detail(client, pid: str):
    """
    Full CJ product detail
    """
    url = f"{client.base_url}/product/detail"
    headers = {"CJ-Access-Token": client.access_token}
    params = {"pid": pid}

    resp = client.session.get(url, params=params, headers=headers, timeout=20)
    resp.raise_for_status()
    return resp.json().get("data", {})


# ---------------------------------------------------------------
# Main retro crawler
# ---------------------------------------------------------------

def crawl_retro_products():
    client = make_client_from_env()

    all_products: List[Dict[str, Any]] = []
    seen = set()

    for kw in RETRO_KEYWORDS:
        print(f"[CJ] Searching keyword: {kw}")

        page = 1
        while True:
            try:
                results = cj_search(client, kw, page=page, size=50)
            except Exception as e:
                print(f"[CJ WARN] Error: {e}")
                break

            if not results:
                break

            for item in results:
                pid = item.get("pid")
                if not pid or pid in seen:
                    continue

                seen.add(pid)

                # Fetch detail
                try:
                    detail = cj_detail(client, pid)
                except Exception as e:
                    print(f"[CJ WARN] Detail error {pid}: {e}")
                    continue

                detail["_keyword"] = kw
                all_products.append(detail)

            # If fewer than 50, no more pages
            if len(results) < 50:
                break

            page += 1
            time.sleep(0.5)  # small delay to avoid 429

    print(f"[CJ] Total retro products collected: {len(all_products)}")
    return all_products


# ---------------------------------------------------------------
# Save outputs
# ---------------------------------------------------------------

def save_jsonl(products):
    with JSONL_PATH.open("w", encoding="utf-8") as f:
        for p in products:
            f.write(json.dumps(p) + "\n")
    print(f"[OUT] JSONL → {JSONL_PATH}")


def save_parquet(products):
    df = pd.json_normalize(products)
    pq.write_table(pa.Table.from_pandas(df), PARQUET_PATH)
    print(f"[OUT] Parquet → {PARQUET_PATH}")


def save_shopify(products):
    rows = []

    for p in products:
        title = p.get("nameEn") or p.get("name") or "Retro Item"
        desc = p.get("description") or ""
        category = p.get("categoryName") or "Retro Gaming"
        pid = p.get("pid")
        price = p.get("sellPrice") or p.get("productPrice") or ""

        # first image
        imgs = p.get("image") or p.get("imageList") or []
        img_src = imgs[0] if isinstance(imgs, list) and imgs else ""

        # handle
        handle = (
            title.lower()
            .replace(" ", "-")
            .replace("/", "-")
            .replace("&", "and")
        )
        handle = "".join(ch for ch in handle if ch.isalnum() or ch == "-")

        rows.append({
            "Handle": handle,
            "Title": title,
            "Body (HTML)": desc,
            "Vendor": "CJ Dropshipping",
            "Type": category,
            "Tags": "retro-gaming",
            "Published": "TRUE",
            "Option1 Name": "Title",
            "Option1 Value": "Default Title",
            "Variant SKU": pid,
            "Variant Price": price,
            "Variant Inventory Qty": 0,
            "Variant Requires Shipping": "TRUE",
            "Variant Taxable": "TRUE",
            "Image Src": img_src,
        })

    df = pd.DataFrame(rows)
    df.to_csv(SHOPIFY_CSV_PATH, index=False)
    print(f"[OUT] Shopify CSV → {SHOPIFY_CSV_PATH}")


# ---------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------

def main():
    products = crawl_retro_products()

    save_jsonl(products)
    save_parquet(products)
    save_shopify(products)

    print("[CJ RETRO] DONE.")


if __name__ == "__main__":
    main()
