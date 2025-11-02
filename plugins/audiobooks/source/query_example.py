from gutenberg_sqlite import query_books
import sqlite3, time
from pathlib import Path

DB_PATH = Path("data/gutenberg_index.db")

def list_authors(limit=100):
    """Print all unique authors in the database."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT author
        FROM books
        WHERE author IS NOT NULL AND TRIM(author) != ''
        ORDER BY author COLLATE NOCASE ASC
        LIMIT ?
    """, (limit,))
    authors = [a[0] for a in cur.fetchall()]
    conn.close()

    print(f"[{time.strftime('%H:%M:%S')}] Found {len(authors)} unique authors (showing up to {limit}):")
    for a in authors:
        print("•", a)
    print("\n")
    return authors


# ──────────────────────────────────────────────
# Run queries
# ──────────────────────────────────────────────

# Print all unique authors
list_authors(limit=6000)

# Find all books by Edgar Allan Poe
results = query_books(author="Poe, Edgar Allan", limit=10)

for r in results:
    print(f"{r['id']} — {r['title']} by {r['author']}")
