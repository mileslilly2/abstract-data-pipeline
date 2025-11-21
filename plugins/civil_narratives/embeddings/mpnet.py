"""
MPNet embeddings
"""
from sentence_transformers import SentenceTransformer

mpnet = SentenceTransformer("sentence-transformers/all-mpnet-base-v2")

def embed_texts(texts):
    return mpnet.encode(texts, normalize_embeddings=True)
