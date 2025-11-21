"""
Store CivilNarrative in Postgres JSONB
"""

import json

def save_jsonb(conn, narrative):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO civil_narrative_documents (doc_id, data)
            VALUES (%s, %s)
            """,
            (narrative.doc_id, json.dumps(narrative.model_dump()))
        )
        conn.commit()
