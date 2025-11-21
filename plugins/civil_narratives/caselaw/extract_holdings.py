"""
Extract paragraphs containing key legal concepts.
"""

def extract_holdings(text: str, keywords):
    lines = text.split("\n")
    return [ln.strip() for ln in lines if any(k in ln.lower() for k in keywords)]
