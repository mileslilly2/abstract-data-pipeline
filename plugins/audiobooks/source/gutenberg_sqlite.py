#!/usr/bin/env python3
# source_gutenberg_sqlite.py
# Build & query a compact SQLite index of English + Public Domain Gutenberg books.

import sqlite3, json, time
from pathlib import Path
from source_gutenberg_rdf import stream_rdf_entries

DB_PATH = Path("data/gutenberg_index.db")
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

# ──────────────────────────────────────────────
# DB setup
# ──────────────────────────────────────────────
def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS books (
            id INTEGER PRIMARY KEY,
            title TEXT,
            author TEXT,
            subjects TEXT,
            language TEXT,
            rights TEXT
        )
    """)
    conn.commit()
    conn.close()


# ──────────────────────────────────────────────
# Populate database (English + Public Domain)
# ──────────────────────────────────────────────
def populate_db(limit=None):
    """
    Stream metadata from Gutenberg RDF and insert
    only English-language, public-domain books.
    """
    init_db()
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    count = 0

    for book in stream_rdf_entries(limit=limit, language="en"):
        # Heuristic filter for public domain
        rights_texts = book.get("rights", []) if isinstance(book.get("rights"), list) else [book.get("rights", "")]
        rights_combined = " ".join(r.lower() for r in rights_texts)
        if not any(x in rights_combined for x in ["public domain", "public-domain", "public-domain in the usa"]):
            continue

        cur.execute("""
            INSERT OR IGNORE INTO books (id, title, author, subjects, language, rights)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            book["id"],
            book.get("title", ""),
            book.get("author", ""),
            json.dumps(book.get("subjects", [])),
            json.dumps(book.get("language", [])),
            rights_combined
        ))

        count += 1
        if count % 100 == 0:
            conn.commit()
            print(f"[{time.strftime('%H:%M:%S')}] Inserted {count}…")

    conn.commit()
    conn.close()
    print(f"✅ Indexed {count} English + Public Domain books → {DB_PATH}")


# ──────────────────────────────────────────────
# Query interface
# ──────────────────────────────────────────────
def query_books(author=None, keyword=None, topic=None, limit=20):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    conditions, params = [], []
    if author:
        conditions.append("LOWER(author) LIKE ?")
        params.append(f"%{author.lower()}%")
    if keyword:
        conditions.append("(LOWER(title) LIKE ? OR LOWER(subjects) LIKE ?)")
        params.extend([f"%{keyword.lower()}%", f"%{keyword.lower()}%"])
    if topic:
        conditions.append("LOWER(subjects) LIKE ?")
        params.append(f"%{topic.lower()}%")

    where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
    sql = f"SELECT * FROM books {where_clause} LIMIT {limit}"
    cur.execute(sql, params)

    results = [dict(r) for r in cur.fetchall()]
    conn.close()
    print(f"[{time.strftime('%H:%M:%S')}] Found {len(results)} matches.")
    return results


if __name__ == "__main__":
    # Build index (≈ 35–50 MB for full English Public Domain)
    # populate_db()  # Uncomment to build

    results = query_books(author="Edgar Allan Poe", keyword="short story")
    for r in results:
        print(f"{r['id']} — {r['title']} by {r['author']}")
