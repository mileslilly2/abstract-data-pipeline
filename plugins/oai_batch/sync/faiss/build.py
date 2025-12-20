from pathlib import Path
import json
from indexer import build_faiss_index

# build.py is in: oai_batch/sync/faiss/
PLUGIN_DIR = Path(__file__).resolve().parents[2]  # oai_batch

WORKS_PATH = PLUGIN_DIR / "transform/kr/works.jsonl"

print("PLUGIN_DIR =", PLUGIN_DIR)
print("WORKS_PATH =", WORKS_PATH)
print("EXISTS =", WORKS_PATH.exists())

records = []

with open(WORKS_PATH, "r", encoding="utf-8") as f:
    for line in f:
        w = json.loads(line)
        records.append({
            "id": w["work_id"],
            "text": w["title"],
            "title": w["title"],
            "source": w.get("source"),
            "collection": w.get("collection"),
        })

build_faiss_index(
    records,
    index_path=PLUGIN_DIR / "sync/faiss/index.faiss",
    metadata_path=PLUGIN_DIR / "sync/faiss/metadata.pkl",
)
