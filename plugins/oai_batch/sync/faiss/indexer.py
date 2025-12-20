import os
import pickle
from typing import List, Dict

import faiss
import numpy as np
from sentence_transformers import SentenceTransformer


def build_faiss_index(
    records: List[Dict],
    index_path: str,
    metadata_path: str,
    model_name: str = "sentence-transformers/all-MiniLM-L6-v2",
    normalize_embeddings: bool = True,
):
    """
    Build a FAISS index from semantic records.

    Parameters
    ----------
    records : list of dict
        Each record must contain:
          - id   : stable semantic ID (e.g. work_id)
          - text : text to embed
        Any additional keys are preserved in metadata.

    index_path : str
        Path to write FAISS index.

    metadata_path : str
        Path to write metadata pickle.

    model_name : str
        SentenceTransformers model to use.

    normalize_embeddings : bool
        If True, embeddings are L2-normalized and
        cosine similarity is implemented via inner product.
    """

    if not records:
        raise ValueError("No records provided to build_faiss_index")

    # Ensure output directories exist
    os.makedirs(os.path.dirname(index_path), exist_ok=True)
    os.makedirs(os.path.dirname(metadata_path), exist_ok=True)

    # -------------------------
    # Load embedding model
    # -------------------------
    print(f"[FAISS] Loading model: {model_name}")
    model = SentenceTransformer(model_name)

    texts = [r["text"] for r in records]
    ids = [r["id"] for r in records]

    # -------------------------
    # Generate embeddings
    # -------------------------
    print(f"[FAISS] Embedding {len(texts)} records")
    embeddings = model.encode(
        texts,
        batch_size=32,
        show_progress_bar=True,
        convert_to_numpy=True,
    )

    if normalize_embeddings:
        faiss.normalize_L2(embeddings)

    dim = embeddings.shape[1]
    print(f"[FAISS] Embedding dimension: {dim}")

    # -------------------------
    # Build FAISS index
    # -------------------------
    if normalize_embeddings:
        index = faiss.IndexFlatIP(dim)   # cosine similarity
    else:
        index = faiss.IndexFlatL2(dim)   # L2 distance

    index.add(embeddings)
    print(f"[FAISS] Index size: {index.ntotal}")

    # -------------------------
    # Persist index + metadata
    # -------------------------
    faiss.write_index(index, str(index_path))

    metadata = {
        "ids": ids,              # FAISS row -> work_id
        "records": records,      # preserved metadata
        "model": model_name,
        "normalized": normalize_embeddings,
        "dim": dim,
    }

    with open(metadata_path, "wb") as f:
        pickle.dump(metadata, f)

    print(f"[FAISS] Index written to: {index_path}")
    print(f"[FAISS] Metadata written to: {metadata_path}")
