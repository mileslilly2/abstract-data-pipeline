import json
import os
import hashlib
from collections import defaultdict

# -------------------------
# Paths
# -------------------------

INPUT_PATH = (
    "data/wvu_oai/oai_dc/processed/records.jsonl"
    
)

OUT_DIR = "data/wvu_oai/oai_dc/kr"


os.makedirs(OUT_DIR, exist_ok=True)

WORKS_OUT = os.path.join(OUT_DIR, "works.jsonl")
AUTHORS_OUT = os.path.join(OUT_DIR, "authors.jsonl")
SUBJECTS_OUT = os.path.join(OUT_DIR, "subjects.jsonl")
EDGES_OUT = os.path.join(OUT_DIR, "edges.jsonl")


# -------------------------
# Helpers
# -------------------------

def stable_hash(*parts: str) -> str:
    """
    Deterministic ID from text content.
    """
    joined = "||".join(p.strip().lower() for p in parts if p)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()[:16]


def normalize(text: str) -> str:
    return text.strip() if text else ""


# -------------------------
# In-memory registries
# -------------------------

works = {}
authors = {}
subjects = {}
edges = []

# -------------------------
# Build KR
# -------------------------

with open(INPUT_PATH, "r", encoding="utf-8") as f:
    for line_num, line in enumerate(f, start=1):
        record = json.loads(line)

        title = normalize(record.get("title"))
        creators = [normalize(c) for c in record.get("creators", []) if c]
        subject_labels = [normalize(s) for s in record.get("subjects", []) if s]

        # ---- Work entity (1 per record for now)
        work_id = "work_" + stable_hash(title, "|".join(creators))

        if work_id not in works:
            works[work_id] = {
                "work_id": work_id,
                "title": title,
                "source": record.get("source"),
                "collection": record.get("collection"),
                "raw_doc_id": record.get("doc_id"),
            }

        # ---- Author entities + edges
        for name in creators:
            author_id = "author_" + stable_hash(name)

            if author_id not in authors:
                authors[author_id] = {
                    "author_id": author_id,
                    "name": name,
                }

            edges.append({
                "from": author_id,
                "to": work_id,
                "type": "AUTHORED",
            })

        # ---- Subject entities + edges
        for label in subject_labels:
            subject_id = "subject_" + stable_hash(label)

            if subject_id not in subjects:
                subjects[subject_id] = {
                    "subject_id": subject_id,
                    "label": label,
                }

            edges.append({
                "from": subject_id,
                "to": work_id,
                "type": "ABOUT",
            })

        if line_num % 1000 == 0:
            print(f"Processed {line_num} records")

# -------------------------
# Write outputs
# -------------------------

def write_jsonl(path, items):
    with open(path, "w", encoding="utf-8") as out:
        for item in items:
            out.write(json.dumps(item, ensure_ascii=False) + "\n")


write_jsonl(WORKS_OUT, works.values())
write_jsonl(AUTHORS_OUT, authors.values())
write_jsonl(SUBJECTS_OUT, subjects.values())
write_jsonl(EDGES_OUT, edges)

print("KR build complete:")
print(f"  works:   {len(works)}")
print(f"  authors: {len(authors)}")
print(f"  subjects:{len(subjects)}")
print(f"  edges:   {len(edges)}")
