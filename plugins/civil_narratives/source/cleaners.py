"""
Various text cleaners / normalizers
"""

def clean_unicode(s: str) -> str:
    return s.replace("\u200b", "").strip()

def collapse_spaces(s: str) -> str:
    return " ".join(s.split())
