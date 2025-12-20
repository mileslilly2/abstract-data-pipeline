import json
from app.core.indexer import build_faiss_index

records = []
with open("data/wvu_oai/oai_dc/processed/records.jsonl") as f:
    for line in f:
        r = json.loads(line)
        records.append({
            "title": r["title"],
            "description": r["description"],
            "link": r["link"],
            "subject": r["subjects"]
        })

build_faiss_index(
    records,
    index_path="data/wvu_oai/faiss/index.faiss",
    metadata_path="data/wvu_oai/faiss/metadata.pkl"
)
