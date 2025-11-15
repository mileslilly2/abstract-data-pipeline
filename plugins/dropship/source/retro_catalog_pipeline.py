#!/usr/bin/env python3
"""
retro_catalog_pipeline.py — DSers retro gaming bulk scraper
- Fetch retro gaming products from DSers
- Dedupe + normalize
- Download images
- Save JSONL, Parquet, Shopify CSV
"""

import os
import json
import hashlib
from pathlib import Path
from typing import List, Dict, Any, Iterable, Optional

import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# ──────────────────────────────────────────────
# DSers Client
# ──────────────────────────────────────────────

class DSersClient:
    def __init__(self, api_key: str, base_url: str = "https://api.dsers.com/v1"):
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Accept": "application/json",
        }

    def search_products(self, q: str, limit: int = 50, extra_params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        NOTE: DSers' *actual* public API parameters may differ.
        Adjust `extra_params` / URL / pagination fields to match their docs or
        whatever endpoint you’re hitting (AliExpress, Banggood, etc.).
        """
        params: Dict[str, Any] = {"q": q, "limit": limit}
        if extra_params:
            params.update(extra_params)

        url = f"{self.base_url}/products"
        resp = self.session.get(url, params=params, headers=self.headers, timeout=30)
        resp.raise_for_status()
        payload = resp.json()
        return payload.get("data") or payload.get("products") or []

# ──────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────

RETRO_KEYWORDS = [
    "retro gaming",
    "n64 controller",
    "snes controller",
    "ps1 controller usb",
    "ps2 controller usb",
    "retro console",
    "handheld game console",
    "mini arcade",
    "arcade stick",
    "raspberry pi retro",
    "retro gamepad",
]

OUTPUT_DIR = Path("retro_outputs")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
IMAGES_DIR = OUTPUT_DIR / "images"
IMAGES_DIR.mkdir(parents=True, exist_ok=True)

RAW_JSONL_PATH = OUTPUT_DIR / "dsers_retro_raw.jsonl"
PARQUET_PATH = OUTPUT_DIR / "dsers_retro_catalog.parquet"
SHOPIFY_CSV_PATH = OUTPUT_DIR / "shopify_retro_catalog.csv"

# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────

def iter_retro_products(client: DSersClient, keywords: List[str], limit_per_kw: int = 50) -> Iterable[Dict[str, Any]]:
    """
    Simple keyword crawler.
    If DSers supports pagination, you can add page/offset-based loops here.
    """
    for kw in keywords:
        print(f"[DSers] Fetching keyword: {kw!r}")
        products = client.search_products(q=kw, limit=limit_per_kw)
        print(f"  -> got {len(products)} products")
        for p in products:
            p["_source_keyword"] = kw
            yield p

def dedupe_products(products: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Dedupe by a stable product ID if present, otherwise hash a few key fields.
    This is intentionally defensive because we don’t know DSers' exact schema.
    """
    seen = set()
    deduped: List[Dict[str, Any]] = []

    for p in products:
        pid = (
            str(p.get("id"))
            or str(p.get("productId"))
            or str(p.get("product_id"))
        )

        if not pid:
            # Fallback: hash title + first image URL
            title = str(p.get("title") or p.get("name") or "").strip()
            img = ""
            imgs = extract_image_urls(p)
            if imgs:
                img = imgs[0]
            pid = hashlib.sha1(f"{title}|{img}".encode("utf-8")).hexdigest()

        if pid in seen:
            continue
        seen.add(pid)
        p["_canonical_id"] = pid
        deduped.append(p)

    print(f"[DEDUPE] kept {len(deduped)} unique products")
    return deduped

def extract_image_urls(p: Dict[str, Any]) -> List[str]:
    """
    Tries multiple common patterns to extract image URLs from unknown schemas.
    You may want to adjust this once you see actual DSers payloads.
    """
    urls: List[str] = []

    # Common flat keys
    for key in ["image", "thumbnail", "mainImage", "imgUrl"]:
        v = p.get(key)
        if isinstance(v, str) and v.startswith("http"):
            urls.append(v)

    # images: [ { url }, ... ] or [ "http..." ]
    images = p.get("images") or p.get("gallery") or []
    if isinstance(images, list):
        for item in images:
            if isinstance(item, str) and item.startswith("http"):
                urls.append(item)
            elif isinstance(item, dict):
                for k in ["url", "src", "image", "img"]:
                    v = item.get(k)
                    if isinstance(v, str) and v.startswith("http"):
                        urls.append(v)

    # Deduplicate while preserving order
    seen = set()
    uniq = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            uniq.append(u)
    return uniq

def download_product_images(products: List[Dict[str, Any]], out_dir: Path, max_images_per_product: int = 3) -> None:
    """
    Downloads up to `max_images_per_product` images per product.
    Stores local paths in `_local_images` so we can map them to Shopify CSV.
    """
    out_dir.mkdir(parents=True, exist_ok=True)

    for idx, p in enumerate(products, start=1):
        urls = extract_image_urls(p)[:max_images_per_product]
        local_paths = []
        if not urls:
            continue

        print(f"[IMG] {idx}/{len(products)} downloading {len(urls)} images")

        for i, url in enumerate(urls, start=1):
            try:
                resp = requests.get(url, timeout=30)
                resp.raise_for_status()
                ext = ".jpg"
                if "image/png" in resp.headers.get("Content-Type", ""):
                    ext = ".png"
                # stable filename based on product ID + index
                base = p.get("_canonical_id") or hashlib.sha1(url.encode("utf-8")).hexdigest()
                fname = f"{base}_{i}{ext}"
                fpath = out_dir / fname
                fpath.write_bytes(resp.content)
                local_paths.append(str(fpath))
            except Exception as e:
                print(f"  ! failed to download {url}: {e}")

        p["_local_images"] = local_paths

# ──────────────────────────────────────────────
# Normalization: internal schema
# ──────────────────────────────────────────────

def normalize_product(p: Dict[str, Any], source: str = "dsers") -> Dict[str, Any]:
    """
    Map DSers-ish product into a common schema.
    Later, your CJ products should map into the same keys.
    """
    title = str(p.get("title") or p.get("name") or "").strip()
    desc = str(p.get("description") or p.get("desc") or "").strip()

    # Try common price locations
    price = (
        p.get("price")
        or p.get("salePrice")
        or p.get("minPrice")
        or (p.get("pricing") or {}).get("price")
    )

    vendor = (
        p.get("storeName")
        or p.get("sellerName")
        or p.get("vendor")
        or "AliExpress via DSers"
    )

    tags = []

    kw = p.get("_source_keyword")
    if kw:
        tags.append(kw)

    category = (
        p.get("categoryName")
        or p.get("category")
        or "Electronics / Retro Gaming"
    )

    imgs = extract_image_urls(p)
    local_imgs = p.get("_local_images") or []

    normalized = {
        "id": p.get("_canonical_id"),
        "raw_id": p.get("id") or p.get("productId") or p.get("product_id"),
        "source": source,              # <- THIS lets you merge CJ later
        "title": title,
        "description": desc,
        "vendor": vendor,
        "category": category,
        "tags": tags,
        "price": price,
        "currency": p.get("currency") or "USD",  # adjust/override as needed
        "url": p.get("url") or p.get("productUrl"),
        "image_urls": imgs,
        "local_images": local_imgs,
        "raw": p,  # keep full original
    }
    return normalized

def normalize_products(products: List[Dict[str, Any]], source: str = "dsers") -> List[Dict[str, Any]]:
    return [normalize_product(p, source=source) for p in products]

# ──────────────────────────────────────────────
# Outputs: JSONL + Parquet + Shopify CSV
# ──────────────────────────────────────────────

def save_jsonl(records: Iterable[Dict[str, Any]], path: Path) -> None:
    with path.open("w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    print(f"[OUT] JSONL -> {path}")

def save_parquet(records: List[Dict[str, Any]], path: Path) -> None:
    df = pd.json_normalize(records)
    table = pa.Table.from_pandas(df)
    pq.write_table(table, path)
    print(f"[OUT] Parquet -> {path}")

def build_shopify_rows(products: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Convert normalized products into Shopify CSV rows (one row per product).
    This is simplified — you can later expand to variants/options.
    """
    rows: List[Dict[str, Any]] = []

    for p in products:
        handle = (
            p["title"]
            .lower()
            .replace(" ", "-")
            .replace("/", "-")
            .replace("&", "and")
        )
        handle = "".join(ch for ch in handle if ch.isalnum() or ch in "-")

        primary_img = None
        if p["local_images"]:
            # For Shopify Import, we usually want URLs;
            # but you can upload these to a CDN and then rewrite later.
            primary_img = p["local_images"][0]
        elif p["image_urls"]:
            primary_img = p["image_urls"][0]

        tags = ",".join(sorted(set(p.get("tags") or [])))

        row = {
            "Handle": handle,
            "Title": p["title"],
            "Body (HTML)": p["description"],
            "Vendor": p["vendor"],
            "Type": p["category"],
            "Tags": tags,
            "Published": "TRUE",
            "Option1 Name": "Title",
            "Option1 Value": "Default Title",
            "Variant SKU": p["id"],
            "Variant Price": p["price"] or "",
            "Variant Inventory Qty": 0,
            "Variant Requires Shipping": "TRUE",
            "Variant Taxable": "TRUE",
            "Image Src": primary_img or "",
        }
        rows.append(row)

    return rows

def save_shopify_csv(products: List[Dict[str, Any]], path: Path) -> None:
    rows = build_shopify_rows(products)
    df = pd.DataFrame(rows)
    df.to_csv(path, index=False)
    print(f"[OUT] Shopify CSV -> {path}")

# ──────────────────────────────────────────────
# Future: merging CJ + DSers
# ──────────────────────────────────────────────

def merge_sources(*sources: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Later, when you pull CJ products and normalize them with the same schema,
    you can merge here and dedupe by `id` / (source, raw_id).
    """
    merged: List[Dict[str, Any]] = []
    seen = set()

    for src_list in sources:
        for p in src_list:
            key = (p.get("source"), p.get("raw_id") or p.get("id"))
            if key in seen:
                continue
            seen.add(key)
            merged.append(p)

    print(f"[MERGE] merged catalog size = {len(merged)}")
    return merged

# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────

def main():
    api_key = os.environ.get("DSERS_API_KEY")
    if not api_key:
        raise SystemExit("Set DSERS_API_KEY in your environment.")

    client = DSersClient(api_key=api_key)

    # 1) Fetch raw products from DSers
    raw_products = list(iter_retro_products(client, RETRO_KEYWORDS, limit_per_kw=50))

    # 2) Dedupe
    raw_deduped = dedupe_products(raw_products)

    # 3) Download a few images per product
    download_product_images(raw_deduped, IMAGES_DIR, max_images_per_product=3)

    # 4) Normalize into internal schema
    normalized = normalize_products(raw_deduped, source="dsers")

    # 5) Save JSONL + Parquet
    save_jsonl(normalized, RAW_JSONL_PATH)
    save_parquet(normalized, PARQUET_PATH)

    # 6) Build Shopify CSV catalog
    save_shopify_csv(normalized, SHOPIFY_CSV_PATH)

    print("[DONE] Retro catalog pipeline complete.")

if __name__ == "__main__":
    main()
