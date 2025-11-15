#!/usr/bin/env python3
"""
cj_retro_catalog_pipeline.py — CJ Dropshipping retro electronics bulk scraper
- Search retro gaming products from CJ
- Fetch full product details
- Dedupe + basic normalize
- Save JSONL, Parquet, Shopify CSV
"""

import os
import json
import time
import hashlib
from pathlib import Path
from typing import List, Dict, Any

import requests
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa

from cj_client import make_client_from_env, CJClient


# ──────────────────────────────────────────────
# Retro gaming keywords for CJ
# ──────────────────────────────────────────────

RETRO_KEYWORDS = [
    "retro gaming",
    "game console",
    "handheld game",
    "n64 controller",
    "snes controller",
    "usb controller",
    "retro arcade",
    "arcade stick",
    "sega",
    "nes",
    "ps1",
]

# ──────────────────────────────────────────────
# Output paths
# ──────────────────────────────────────────────

OUTPUT_DIR = Path("cj_retro_outputs")
OUTPUT_DIR.mkdir(exist_ok=True)
IMAGES_DIR = OUTPUT_DIR / "images"
IMAGES_DIR.mkdir(exist_ok=True)

RAW_JSONL = OUTPUT_DIR / "cj_retro_raw.jsonl"
PARQUET_PATH = OUTPUT_DIR / "cj_retro_catalog.parquet"
SHOPIFY_CSV_PATH = OUTPUT_DIR / "shopify_cj_retro_catalog.csv"


# ──────────────────────────────────────────────
# CJ API helpers
# ──────────────────────────────────────────────

def cj_search_products(client: CJClient, keyword: str, page: int = 1, size: int = 50):
    """Call CJ product.listV2"""
    url = f"{client.base_url}/product/list"
    params = {"pageNum": page, "pageSize": size, "keyword": keyword}
    resp = client.session.get(url, params=params, headers={"CJ-Access-Token": client.access_token})
    resp.raise_for_status()
    return resp.json().get("data", {}).get("result", [])


def cj_get_detail(client: CJClient, product_id: str):
    """Fetch full product detail"""
    url = f"{client.base_url}/product/detail"
    resp = client.session.get(url, params={"pid": product_id}, headers={"CJ-Access-Token": client.access_token})
    resp.raise_for_status()
    return resp.json().get("data", {})


# ──────────────────────────────────────────────
# Crawl CJ retro products
# ──────────────────────────────────────────────

def crawl_cj_retro(client: CJClient, keywords: List[str]):
    results = []

    for kw in keywords:
        print(f"[CJ] Searching keyword: {kw}")
        page = 1

        while True:
            items = cj_search_products(client, kw, page=page, size=50)
            if not items:
                break

            for it in items:
                pid = it.get("pid")
                if not pid:
                    continue

                try:
                    detail = cj_get_detail(client, pid)
                except Exception as e:
                    print(f"  ! detail error {pid}: {e}")
                    continue

                detail["_keyword"] = kw
                results.append(detail)

            if len(items) < 50:
                break

            page += 1

    return results


# ──────────────────────────────────────────────
# Dedupe + Image extraction
# ──────────────────────────────────────────────

def dedupe(items: List[Dict[str, Any]]):
    out = []
    seen = set()

    for p in items:
        pid = p.get("pid")
        if not pid:
            continue
        if pid in seen:
            continue
        seen.add(pid)
        out.append(p)

    print(f"[DEDUPE] kept {len(out)} items")
    return out


def extract_images(p: Dict[str, Any]):
    imgs = []
    main = p.get("image") or []
    if isinstance(main, list):
        imgs.extend([x for x in main if x.startswith("http")])

    # gallery
    gal = p.get("imageList") or []
    if isinstance(gal, list):
        imgs.extend([x for x in gal if isinstance(x, str) and x.startswith("http")])

    return list(dict.fromkeys(imgs))


# ──────────────────────────────────────────────
# Normalize CJ → basic Shopify-ready shape
# ──────────────────────────────────────────────

def normalize_cj(p: Dict[str, Any]):
    title = p.get("nameEn") or p.get("name") or ""
    desc = p.get("description") or ""

    price = None
    if p.get("sellPrice"):
        price = p["sellPrice"]
    elif p.get("productPrice"):
        price = p["productPrice"]

    imgs = extract_images(p)

    norm = {
        "id": p.get("pid"),
        "source": "cj",
        "title": title,
        "description": desc,
        "vendor": "CJ Dropshipping",
        "category": p.get("categoryName") or "Electronics / Retro",
        "tags": [p.get("_keyword", "")],
        "price": price,
        "currency": "USD",
        "url": p.get("productUrl"),
        "image_urls": imgs,
        "raw": p,
    }
    return norm


# ──────────────────────────────────────────────
# Save outputs
# ──────────────────────────────────────────────

def save_jsonl(items):
    with RAW_JSONL.open("w", encoding="utf8") as f:
        for row in items:
            f.write(json.dumps(row) + "\n")
    print(f"[OUT] JSONL -> {RAW_JSONL}")


def save_parquet(items):
    df = pd.json_normalize(items)
    table = pa.Table.from_pandas(df)
    pq.write_table(table, PARQUET_PATH)
    print(f"[OUT] Parquet -> {PARQUET_PATH}")


def save_shopify_csv(items):
    rows = []
    for p in items:
        handle = (
            p["title"].lower()
            .replace(" ", "-")
            .replace("/", "-")
        )
        handle = "".join(ch for ch in handle if ch.isalnum() or ch == "-")

        img = p["image_urls"][0] if p["image_urls"] else ""

        rows.append({
            "Handle": handle,
            "Title": p["title"],
            "Body (HTML)": p["description"],
            "Vendor": "CJ Dropshipping",
            "Type": p["category"],
            "Tags": ",".join(filter(None, p["tags"])),
            "Published": "TRUE",
            "Option1 Name": "Title",
            "Option1 Value": "Default Title",
            "Variant SKU": p["id"],
            "Variant Price": p["price"] or "",
            "Variant Inventory Qty": 0,
            "Variant Requires Shipping": "TRUE",
            "Variant Taxable": "TRUE",
            "Image Src": img,
        })

    df = pd.DataFrame(rows)
    df.to_csv(SHOPIFY_CSV_PATH, index=False)
    print(f"[OUT] Shopify CSV -> {SHOPIFY_CSV_PATH}")


# ──────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────

def main():
    client = make_client_from_env()

    print("[CJ] Crawling retro gaming products…")
    raw = crawl_cj_retro(client, RETRO_KEYWORDS)

    print(f"[CJ] Raw count: {len(raw)}")
    raw = dedupe(raw)

    normalized = [normalize_cj(p) for p in raw]

    save_jsonl(normalized)
    save_parquet(normalized)
    save_shopify_csv(normalized)

    print("\n[DONE] CJ retro catalog pipeline complete.\n")


if __name__ == "__main__":
    main()
