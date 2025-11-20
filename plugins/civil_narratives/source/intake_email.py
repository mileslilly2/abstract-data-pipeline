"""
Email intake placeholder.
Future: MIME parsing
"""

def parse_email(raw_email: str) -> dict:
    return {
        "raw": raw_email,
        "cleaned": raw_email.strip()
    }
