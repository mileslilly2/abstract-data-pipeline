from gutenberg_sqlite import query_books

# find all books by Edgar Allan Poe
results = query_books(author="Poe, Edgar Allan", limit=10)
results2 = query_books(author="Dick, Philip K", limit=10)
for r in results:
    print(f"{r['id']} — {r['title']} by {r['author']}")

for r in results2:
    print(f"{r['id']} — {r['title']} by {r['author']}")

