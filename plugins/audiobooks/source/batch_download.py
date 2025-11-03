#!/usr/bin/env python3
# source_gutenberg.py
# Source stage: fetch and cache multiple Gutenberg books by author or ID.

import requests, time, sqlite3, json
from pathlib import Path

# ──────────────────────────────────────────────
# Paths and constants
# ──────────────────────────────────────────────
DATA_DIR = Path("data/gutenberg_raw")
DB_PATH = Path("data/gutenberg_index.db")
DATA_DIR.mkdir(exist_ok=True, parents=True)


BASE_URL = "https://www.gutenberg.org/cache/epub/{}/pg{}-images.html"

# ──────────────────────────────────────────────
# Fetch functions
# ──────────────────────────────────────────────
def fetch_gutenberg_book(book_id: int) -> Path:
    """Download a single Gutenberg HTML book and cache it locally."""
    url = BASE_URL.format(book_id, book_id)
    out_path = DATA_DIR / f"{book_id}.html"

    if out_path.exists():
        print(f"[{time.strftime('%H:%M:%S')}] Cached: {book_id}")
        return out_path

    try:
        r = requests.get(url, timeout=30)
        if r.status_code != 200:
            print(f"[{time.strftime('%H:%M:%S')}] ⚠️ Failed {book_id}: {r.status_code}")
            return None
        out_path.write_text(r.text, encoding="utf-8")
        print(f"[{time.strftime('%H:%M:%S')}] ✅ Downloaded: {book_id}")
        return out_path
    except Exception as e:
        print(f"[{time.strftime('%H:%M:%S')}] ❌ Error fetching {book_id}: {e}")
        return None


def batch_fetch(book_ids):
    """Download multiple Gutenberg books by ID list."""
    results = []
    for bid in book_ids:
        p = fetch_gutenberg_book(bid)
        if p:
            results.append(p)
    print(f"[{time.strftime('%H:%M:%S')}] ✅ Fetched {len(results)} of {len(book_ids)} books.")
    return results


# ──────────────────────────────────────────────
# Database query: find all books by author
# ──────────────────────────────────────────────
def get_book_ids_by_author(author_name: str, limit=None):
    """Query SQLite DB for all book IDs matching an author."""
    if not DB_PATH.exists():
        raise FileNotFoundError(f"Database not found at {DB_PATH}. Run source_gutenberg_sqlite.py first.")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    sql = "SELECT id, title FROM books WHERE LOWER(author) LIKE ?"
    params = [f"%{author_name.lower()}%"]
    if limit:
        sql += f" LIMIT {limit}"

    cur.execute(sql, params)
    rows = cur.fetchall()
    conn.close()

    if not rows:
        print(f"[{time.strftime('%H:%M:%S')}] ⚠️ No books found for author: {author_name}")
        return []

    print(f"[{time.strftime('%H:%M:%S')}] Found {len(rows)} books by {author_name}.")
    for r in rows[:5]:
        print(f"• {r['id']} — {r['title']}")
    return [r["id"] for r in rows]


# ──────────────────────────────────────────────
# Main entry point
# ──────────────────────────────────────────────
if __name__ == "__main__":
    # Example 1: download books by author
    author = "Dick, Philip K"
    book_ids = get_book_ids_by_author(author, limit=10)
    if book_ids:
        batch_fetch(book_ids)

    # Example 2 (optional): specific IDs
    # books = [1342, 84, 98]
    # batch_fetch(books)
