#!/usr/bin/env python3
"""
SuperDataset Auto Downloader + Merger
- Downloads datasets automatically (skips if fails)
- Extracts keyword-rich fields
- Outputs: super_keywords.txt, parquet, sqlite
"""

import os
import json
import requests
import pandas as pd
import sqlite3
from pathlib import Path
from io import StringIO

# ------------------------------------------------------------
# Setup
# ------------------------------------------------------------
DATA = Path("datasets")
OUT = Path("super_keywords")
DATA.mkdir(exist_ok=True)
OUT.mkdir(exist_ok=True)

def safe_download(url, dest_path, binary=False):
    """Download file; fail silently."""
    try:
        print(f"Downloading: {url}")
        resp = requests.get(url, timeout=20)
        if resp.status_code != 200:
            print(f"  ❌ Failed: HTTP {resp.status_code}")
            return False
        mode = "wb" if binary else "w"
        with open(dest_path, mode) as f:
            f.write(resp.content if binary else resp.text)
        print(f"  ✅ Saved: {dest_path}")
        return True
    except Exception as e:
        print(f"  ⚠️  Skipped ({e})")
        return False


def clean(text):
    if not text:
        return ""
    return text.replace("\n", " ").replace("\t", " ").strip()


def extract_fields(df, fields):
    texts = []
    for f in fields:
        if f in df.columns:
            texts.extend(df[f].astype(str).tolist())
    return texts


# ------------------------------------------------------------
# 1. Download all datasets (best-effort)
# ------------------------------------------------------------

sources = {
    "amazon": {
        "url": "https://raw.githubusercontent.com/selva86/datasets/master/amazon_reviews_sample.csv",
        "file": DATA / "amazon.csv"
    },
    "aliexpress": {
        "url": "https://raw.githubusercontent.com/datawhalechina/ai-product-dataset/master/data/aliexpress_products.csv",
        "file": DATA / "aliexpress.csv"
    },
    "wish": {
        "url": "https://raw.githubusercontent.com/PromptCloudHQ/flipkart-products-dataset/master/flipkart_com-ecommerce_sample.csv",
        "file": DATA / "wish.csv"
    },
    "flipkart": {
        "url": "https://raw.githubusercontent.com/PromptCloudHQ/flipkart-products-dataset/master/flipkart_com-ecommerce_sample.csv",
        "file": DATA / "flipkart.csv"
    },
    "google_taxonomy": {
        "url": "https://www.google.com/basepages/producttype/taxonomy.en-US.txt",
        "file": DATA / "google_taxonomy.txt"
    },
    "wikipedia_ngrams": {
        "url": "https://raw.githubusercontent.com/dwyl/english-words/master/words_alpha.txt",
        "file": DATA / "ngrams.txt"
    }
}

downloaded = {}

print("\n===== DOWNLOADING DATASETS =====")
for key, info in sources.items():
    ok = safe_download(info["url"], info["file"])
    downloaded[key] = ok

print("\nDownload status:")
for k, v in downloaded.items():
    print("  ", k, "→", "OK" if v else "SKIPPED")

# ------------------------------------------------------------
# 2. Build SuperDataset
# ------------------------------------------------------------
print("\n===== BUILDING SUPERDATASET =====")

all_text = []

# AMAZON
if downloaded["amazon"]:
    try:
        df = pd.read_csv(sources["amazon"]["file"])
        all_text.extend(extract_fields(df, ["title", "reviewText", "product_title", "description"]))
    except Exception as e:
        print("  ⚠️ Amazon parse failed:", e)

# ALIEXPRESS
if downloaded["aliexpress"]:
    try:
        df = pd.read_csv(sources["aliexpress"]["file"])
        all_text.extend(extract_fields(df, ["product_title", "detail", "meta"]))
    except Exception as e:
        print("  ⚠️ AliExpress parse failed:", e)

# WISH
if downloaded["wish"]:
    try:
        df = pd.read_csv(sources["wish"]["file"])
        all_text.extend(extract_fields(df, ["product_title", "product_description"]))
    except Exception as e:
        print("  ⚠️ Wish parse failed:", e)

# FLIPKART
if downloaded["flipkart"]:
    try:
        df = pd.read_csv(sources["flipkart"]["file"])
        all_text.extend(extract_fields(df, ["product_title", "description"]))
    except Exception as e:
        print("  ⚠️ Flipkart parse failed:", e)

# GOOGLE TAXONOMY
if downloaded["google_taxonomy"]:
    try:
        with open(sources["google_taxonomy"]["file"], "r") as f:
            tax = f.read().splitlines()
            all_text.extend(tax)
    except Exception as e:
        print("  ⚠️ Google taxonomy parse failed:", e)

# NGRAMS (Wikipedia word list)
if downloaded["wikipedia_ngrams"]:
    try:
        with open(sources["wikipedia_ngrams"]["file"]) as f:
            ngrams = f.read().splitlines()
            all_text.extend(ngrams)
    except Exception as e:
        print("  ⚠️ N-grams parse failed:", e)

# CLEAN
all_text = [clean(t) for t in all_text if isinstance(t, str) and len(t.strip()) > 0]

print(f"Total records: {len(all_text):,}")


# ------------------------------------------------------------
# 3. OUTPUTS
# ------------------------------------------------------------

# TXT
txt_path = OUT / "super_keywords.txt"
with open(txt_path, "w", encoding="utf-8") as f:
    for t in all_text:
        f.write(t + "\n")

print("Saved:", txt_path)

# PARQUET
df_out = pd.DataFrame({"text": all_text})
parquet_path = OUT / "super_keywords.parquet"
df_out.to_parquet(parquet_path, index=False)
print("Saved:", parquet_path)

# SQLITE
sqlite_path = OUT / "super_keywords.sqlite"
conn = sqlite3.connect(sqlite_path)
df_out.to_sql("keywords", conn, if_exists="replace", index=False)
conn.close()
print("Saved:", sqlite_path)

# METADATA
meta = {
    "downloaded": downloaded,
    "total_entries": len(all_text)
}
with open(OUT / "metadata.json", "w") as f:
    json.dump(meta, f, indent=2)

print("\n===== DONE =====")
