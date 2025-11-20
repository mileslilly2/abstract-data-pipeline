"""
Flatten events for Parquet storage.
"""

def flatten_events(narrative):
    rows = []
    for ev in narrative.events:
        row = ev.model_dump()
        row["doc_id"] = narrative.doc_id
        rows.append(row)
    return rows
