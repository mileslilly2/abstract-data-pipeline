"""
Embed opinion text with mpnet.
"""

from ..embeddings.mpnet import embed_texts

def embed_opinion(text: str):
    return embed_texts([text])[0]
