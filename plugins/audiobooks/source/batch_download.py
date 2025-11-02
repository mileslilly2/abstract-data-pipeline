#!/usr/bin/env python3
# source_gutenberg.py
# Source stage: fetch and cache multiple Gutenberg books by ID.

import requests, time
from pathlib import Path

DATA_DIR = Path("data/gutenberg_raw")
DATA_DIR.mkdir(exist_ok=True, parents=True)

BASE_URL = "https://www.gutenberg.org/cache/epub/{}/pg{}.html"

def fetch_gutenberg_book(book_id: int) -> Path:
    """Download a single book and cache it locally."""
    url = BASE_URL.format(book_id, book_id)
    out_path = DATA_DIR / f"{book_id}.html"

    if out_path.exists():
        print(f"[{time.strftime('%H:%M:%S')}] Cached: {book_id}")
        return out_path

    r = requests.get(url, timeout=20)
    if r.status_code != 200:
        raise RuntimeError(f"Failed to download book {book_id}: {r.status_code}")

    out_path.write_text(r.text, encoding="utf-8")
    print(f"[{time.strftime('%H:%M:%S')}] ✅ Downloaded: {book_id}")
    return out_path

def batch_fetch(book_ids):
    """Download multiple Gutenberg books."""
    return [fetch_gutenberg_book(bid) for bid in book_ids]

if __name__ == "__main__":
    books = [1342, 84, 98]  # Example: Pride and Prejudice, Frankenstein, A Tale of Two Cities
    batch_fetch(books)
